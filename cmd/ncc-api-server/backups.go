package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Server-side backup management lives under <install>/backups. Two producers
// write here: the in-app updater (pre-update-<stamp>.tar.gz, our rollback
// points) and the admin "create snapshot" action (manual-<stamp>.tar.gz).
// These endpoints let an admin browse, restore (incl. roll back to the latest
// pre-update snapshot), download, and prune them. All are admin-only via the
// /api/v1/settings/ prefix.

const backupFileSuffix = ".tar.gz"

func (s *apiServer) backupsDir() string {
	return filepath.Join(s.maintenanceInstallDir(), "backups")
}

// backupEntry describes one archive in the backups directory.
type backupEntry struct {
	Name              string `json:"name"`
	Kind              string `json:"kind"` // "pre-update" | "manual" | "other"
	Size              int64  `json:"size"`
	ModTime           string `json:"mod_time"`
	RollbackCandidate bool   `json:"rollback_candidate,omitempty"`
}

func backupKind(name string) string {
	switch {
	case strings.HasPrefix(name, "pre-update-"):
		return "pre-update"
	case strings.HasPrefix(name, "manual-"), strings.HasPrefix(name, "ncc-backup-"):
		return "manual"
	default:
		return "other"
	}
}

// safeBackupPath resolves a caller-supplied backup name to an absolute path
// inside the backups directory, rejecting any path traversal. The name must be
// a bare filename (no separators) ending in .tar.gz.
func (s *apiServer) safeBackupPath(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" || name != filepath.Base(name) || strings.ContainsAny(name, `/\`) {
		return "", false
	}
	if !strings.HasSuffix(name, backupFileSuffix) {
		return "", false
	}
	dir := s.backupsDir()
	full := filepath.Join(dir, name)
	// Defense-in-depth: ensure the cleaned path is still within the dir.
	if rel, err := filepath.Rel(dir, full); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return full, true
}

func (s *apiServer) listBackupEntries() ([]backupEntry, error) {
	dir := s.backupsDir()
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []backupEntry{}, nil
		}
		return nil, err
	}
	out := make([]backupEntry, 0, len(ents))
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), backupFileSuffix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, backupEntry{
			Name:    e.Name(),
			Kind:    backupKind(e.Name()),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	// Newest first.
	sort.Slice(out, func(i, j int) bool { return out[i].ModTime > out[j].ModTime })
	// Mark the most recent pre-update snapshot as the rollback candidate.
	for i := range out {
		if out[i].Kind == "pre-update" {
			out[i].RollbackCandidate = true
			break
		}
	}
	return out, nil
}

// handleBackups dispatches GET (list) and POST (create a server-side snapshot)
// for /api/v1/settings/backups.
func (s *apiServer) handleBackups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleBackupsList(w, r)
	case http.MethodPost:
		s.handleBackupCreate(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleBackupsList(w http.ResponseWriter, _ *http.Request) {
	entries, err := s.listBackupEntries()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "list backups: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"backups": entries}})
}

// handleBackupCreate writes a persistent snapshot into <install>/backups so it
// shows up in the list (and can later be restored or downloaded).
func (s *apiServer) handleBackupCreate(w http.ResponseWriter, r *http.Request) {
	installDir := s.maintenanceInstallDir()
	dir := s.backupsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "create backups dir: " + err.Error()})
		return
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	name := "manual-" + stamp + backupFileSuffix
	outPath := filepath.Join(dir, name)
	if out, err := s.runOrchestrator([]string{"v2-backup", "--install-dir", installDir, "--output-file", outPath}, 2*time.Minute); err != nil {
		s.audit(r, "settings.backup.create", false, map[string]interface{}{"install_dir": installDir})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "snapshot failed: " + strings.TrimSpace(out)})
		return
	}
	var entry backupEntry
	if info, err := os.Stat(outPath); err == nil {
		entry = backupEntry{Name: name, Kind: "manual", Size: info.Size(), ModTime: info.ModTime().UTC().Format(time.RFC3339)}
	} else {
		entry = backupEntry{Name: name, Kind: "manual"}
	}
	s.audit(r, "settings.backup.create", true, map[string]interface{}{"install_dir": installDir, "name": name, "bytes": entry.Size})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Snapshot created.", Data: map[string]interface{}{"backup": entry}})
}

// handleBackupRestoreNamed restores a server-side backup selected by name (so
// the archive never has to be re-uploaded), then restarts the stack — the same
// flow as the upload-based restore. This also powers the post-update rollback.
func (s *apiServer) handleBackupRestoreNamed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	archivePath, ok := s.safeBackupPath(body.Name)
	if !ok {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid backup name", ErrorCode: "NCC_API_INVALID_INPUT"})
		return
	}
	if _, err := os.Stat(archivePath); err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "backup not found"})
		return
	}

	installDir := s.maintenanceInstallDir()
	out, err := s.runOrchestrator([]string{"v2-restore", "--install-dir", installDir, "--input-file", archivePath, "--force", "--no-restart"}, 3*time.Minute)
	if err != nil {
		s.audit(r, "settings.backup.restore", false, map[string]interface{}{"install_dir": installDir, "name": body.Name})
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "restore failed: " + strings.TrimSpace(out)})
		return
	}
	s.audit(r, "settings.backup.restore", true, map[string]interface{}{"install_dir": installDir, "name": filepath.Base(archivePath)})

	restarting := s.spawnDetachedRestart(installDir)
	msg := "Backup restored. The stack is restarting now to load the restored config, accounts, and token — this page will reconnect in a few seconds."
	if !restarting {
		msg = "Backup restored. Restart the stack (v2-stop then v2-start) for the restored config, accounts, and token to take effect."
	}
	writeJSON(w, http.StatusOK, envelope{
		Success: true,
		Message: msg,
		Data: map[string]interface{}{
			"install_dir":      installDir,
			"restarting":       restarting,
			"restart_required": !restarting,
			"output":           strings.TrimSpace(out),
		},
	})
}

// handleBackupDelete removes a server-side backup archive by name.
func (s *apiServer) handleBackupDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	path, ok := s.safeBackupPath(body.Name)
	if !ok {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid backup name", ErrorCode: "NCC_API_INVALID_INPUT"})
		return
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "backup not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "delete failed: " + err.Error()})
		return
	}
	s.audit(r, "settings.backup.delete", true, map[string]interface{}{"name": filepath.Base(path)})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Backup deleted."})
}

// handleBackupDownloadNamed streams an existing server-side backup by name.
func (s *apiServer) handleBackupDownloadNamed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	path, ok := s.safeBackupPath(r.URL.Query().Get("name"))
	if !ok {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid backup name", ErrorCode: "NCC_API_INVALID_INPUT"})
		return
	}
	f, err := os.Open(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "backup not found"})
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "stat backup: " + err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(path)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
	s.audit(r, "settings.backup.download", true, map[string]interface{}{"name": filepath.Base(path), "bytes": fi.Size()})
}
