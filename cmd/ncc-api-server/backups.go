package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// backupEncSuffix marks a server-side snapshot that was sealed at rest with
// AES-256-GCM (v2-backup --encrypt). Downloads stream it as an opaque blob and
// restores require the original passphrase.
const backupEncSuffix = ".tar.gz.enc"

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
	Encrypted         bool   `json:"encrypted,omitempty"`
}

// isBackupArchiveName reports whether name is a backup archive we manage
// (plaintext .tar.gz or encrypted .tar.gz.enc).
func isBackupArchiveName(name string) bool {
	return strings.HasSuffix(name, backupFileSuffix) || strings.HasSuffix(name, backupEncSuffix)
}

// backupKeyConfigured reports whether a non-interactive backup encryption key is
// available in the server's environment (a key file, a raw key, or a
// passphrase). When set, automated backups (the updater's pre-update rollback
// points) are sealed at rest with AES-256-GCM — v2-backup/v2-restore read the
// same NCC_BACKUP_* variables from the inherited child environment, so no key
// material is ever placed on argv.
func backupKeyConfigured() bool {
	for _, k := range []string{"NCC_BACKUP_KEY_FILE", "NCC_BACKUP_KEY", "NCC_BACKUP_PASSPHRASE"} {
		if strings.TrimSpace(os.Getenv(k)) != "" {
			return true
		}
	}
	return false
}

// backupsRetainLimit reads the optional NCC_BACKUPS_RETAIN cap (number of manual
// snapshots to keep). 0/unset/invalid means "keep all".
func backupsRetainLimit() int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("NCC_BACKUPS_RETAIN")))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// pruneManualBackups keeps at most retain newest manual-* snapshots in dir
// (both .tar.gz and .tar.gz.enc), deleting older ones. It deliberately never
// touches pre-update-* rollback points or other files. Returns names pruned.
func pruneManualBackups(dir string, retain int) []string {
	return pruneBackupsByPrefix(dir, "manual-", retain)
}

// pruneBackupsByPrefix keeps at most retain newest archives whose name starts
// with prefix (matching both .tar.gz and .tar.gz.enc), deleting older ones. It
// only ever touches archives with that exact prefix, so callers can prune one
// producer's snapshots (manual-, ncc-backup-) without disturbing the
// pre-update-* rollback points. Returns the names pruned (newest kept).
func pruneBackupsByPrefix(dir, prefix string, retain int) []string {
	if retain <= 0 {
		return nil
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	type bk struct {
		path string
		mod  time.Time
	}
	var list []bk
	for _, e := range ents {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, prefix) || !isBackupArchiveName(n) {
			continue
		}
		if info, err := e.Info(); err == nil {
			list = append(list, bk{filepath.Join(dir, n), info.ModTime()})
		}
	}
	if len(list) <= retain {
		return nil
	}
	sort.Slice(list, func(i, j int) bool { return list[i].mod.After(list[j].mod) })
	var pruned []string
	for _, b := range list[retain:] {
		if os.Remove(b.path) == nil {
			pruned = append(pruned, filepath.Base(b.path))
		}
	}
	return pruned
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
	if !isBackupArchiveName(name) {
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
		if e.IsDir() || !isBackupArchiveName(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, backupEntry{
			Name:      e.Name(),
			Kind:      backupKind(e.Name()),
			Size:      info.Size(),
			ModTime:   info.ModTime().UTC().Format(time.RFC3339),
			Encrypted: strings.HasSuffix(e.Name(), backupEncSuffix),
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
// shows up in the list (and can later be restored or downloaded). An optional
// JSON body {"passphrase":"..."} seals the snapshot at rest with AES-256-GCM
// (stored as manual-<stamp>.tar.gz.enc); restoring/downloading then needs the
// same passphrase. The passphrase is handed to v2-backup via the environment
// (NCC_BACKUP_PASSPHRASE), never argv, so it stays out of the process list and
// audit log.
func (s *apiServer) handleBackupCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Passphrase string `json:"passphrase"`
	}
	// Body is optional (the UI's plain "Create snapshot" sends none); ignore a
	// decode error on an empty/absent body.
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body)
	passphrase := strings.TrimSpace(body.Passphrase)
	if len(passphrase) > 1024 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "passphrase too long"})
		return
	}
	encrypt := passphrase != ""

	installDir := s.maintenanceInstallDir()
	dir := s.backupsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "create backups dir: " + err.Error()})
		return
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	name := "manual-" + stamp + backupFileSuffix
	if encrypt {
		name = "manual-" + stamp + backupEncSuffix
	}
	outPath := filepath.Join(dir, name)
	args := []string{"v2-backup", "--install-dir", installDir, "--output-file", outPath}
	var extraEnv []string
	if encrypt {
		args = append(args, "--encrypt")
		extraEnv = []string{"NCC_BACKUP_PASSPHRASE=" + passphrase}
	}
	if out, err := s.runOrchestratorEnv(args, 2*time.Minute, extraEnv); err != nil {
		s.audit(r, "settings.backup.create", false, map[string]interface{}{"install_dir": installDir, "encrypted": encrypt})
		s.notifyOperationalFailure("backup_failure", "NCC backup snapshot failed", map[string]interface{}{
			"install_dir": installDir,
			"encrypted":   encrypt,
			"error":       firstNonEmptyLine(out, "snapshot failed"),
		})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "snapshot failed: " + strings.TrimSpace(out)})
		return
	}
	entry := backupEntry{Name: name, Kind: "manual", Encrypted: encrypt}
	if info, err := os.Stat(outPath); err == nil {
		entry.Size = info.Size()
		entry.ModTime = info.ModTime().UTC().Format(time.RFC3339)
	}
	// Optional retention: keep at most N newest manual snapshots (rollback
	// points are never pruned). Best-effort; failures don't fail the create.
	if pruned := pruneManualBackups(dir, backupsRetainLimit()); len(pruned) > 0 {
		s.audit(r, "settings.backup.prune", true, map[string]interface{}{"pruned": pruned, "retain": backupsRetainLimit()})
	}
	s.audit(r, "settings.backup.create", true, map[string]interface{}{"install_dir": installDir, "name": name, "bytes": entry.Size, "encrypted": encrypt})
	msg := "Snapshot created."
	if encrypt {
		msg = "Encrypted snapshot created."
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{"backup": entry}})
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
		Name       string `json:"name"`
		Passphrase string `json:"passphrase"`
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
	passphrase := strings.TrimSpace(body.Passphrase)
	if len(passphrase) > 1024 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "passphrase too long"})
		return
	}

	installDir := s.maintenanceInstallDir()
	// v2-restore auto-detects encryption by magic header; we only supply the key
	// (via env, not argv). An unencrypted archive ignores it.
	var extraEnv []string
	if passphrase != "" {
		extraEnv = []string{"NCC_BACKUP_PASSPHRASE=" + passphrase}
	}
	out, err := s.runOrchestratorEnv([]string{"v2-restore", "--install-dir", installDir, "--input-file", archivePath, "--force", "--no-restart"}, 3*time.Minute, extraEnv)
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

// handleBackupVerifyNamed validates a server-side backup by name without
// restoring it: it runs `v2-restore --verify-only`, which checks gzip+tar
// integrity, the manifest, and confined paths (and, for an encrypted snapshot,
// decrypts it first — so a correct passphrase is also confirmed). This lets an
// admin confirm a snapshot is actually restorable before trusting it as a
// recovery point, with no risk to the live stack. Admin-only via the
// /settings/ prefix.
func (s *apiServer) handleBackupVerifyNamed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		Name       string `json:"name"`
		Passphrase string `json:"passphrase"`
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
	passphrase := strings.TrimSpace(body.Passphrase)
	if len(passphrase) > 1024 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "passphrase too long"})
		return
	}
	var extraEnv []string
	if passphrase != "" {
		extraEnv = []string{"NCC_BACKUP_PASSPHRASE=" + passphrase}
	}
	out, err := s.runOrchestratorEnv([]string{"v2-restore", "--input-file", archivePath, "--verify-only"}, 2*time.Minute, extraEnv)
	if err != nil {
		s.audit(r, "settings.backup.verify", false, map[string]interface{}{"name": filepath.Base(archivePath)})
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "verification failed: " + firstNonEmptyLine(out, err.Error()), Data: map[string]interface{}{"output": strings.TrimSpace(out)}})
		return
	}
	s.audit(r, "settings.backup.verify", true, map[string]interface{}{"name": filepath.Base(archivePath)})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Backup verified: the archive is intact and restorable.", Data: map[string]interface{}{"output": strings.TrimSpace(out)}})
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
	contentType := "application/gzip"
	if strings.HasSuffix(path, backupEncSuffix) {
		// Encrypted snapshots are opaque AES-256-GCM envelopes, not gzip.
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(path)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
	s.audit(r, "settings.backup.download", true, map[string]interface{}{"name": filepath.Base(path), "bytes": fi.Size()})
}
