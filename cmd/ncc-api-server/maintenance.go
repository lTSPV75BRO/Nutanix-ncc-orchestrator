package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// maintenanceInstallDir resolves the install directory backup/restore operate
// on: the directory that holds config.yaml. For a standard v2-start install
// this also colocates the user database, API token, and scheduler/notification
// state, so a single directory captures all recoverable state.
func (s *apiServer) maintenanceInstallDir() string {
	return filepath.Dir(s.absPath(s.configPath))
}

// handleBackup builds a backup archive (delegating to the orchestrator's
// v2-backup, which the api-server already shells out to for runs) and streams
// it to the caller as a downloadable .tar.gz. Admin-only via the
// /api/v1/settings/ prefix. The archive contains secrets (API token, password
// hashes, SAML key), so it is generated 0600 in a temp dir and removed after
// streaming.
func (s *apiServer) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	installDir := s.maintenanceInstallDir()
	tmpDir, err := os.MkdirTemp("", "ncc-backup-*")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "create temp dir: " + err.Error()})
		return
	}
	defer os.RemoveAll(tmpDir)

	stamp := time.Now().UTC().Format("20060102T150405Z")
	outPath := filepath.Join(tmpDir, "ncc-backup-"+stamp+".tar.gz")
	out, err := s.runOrchestrator([]string{"v2-backup", "--install-dir", installDir, "--output-file", outPath}, 2*time.Minute)
	if err != nil {
		s.audit(r, "settings.backup", false, map[string]interface{}{"install_dir": installDir})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "backup failed: " + strings.TrimSpace(out)})
		return
	}
	f, err := os.Open(outPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "open backup archive: " + err.Error()})
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "stat backup archive: " + err.Error()})
		return
	}
	filename := "ncc-backup-" + stamp + ".tar.gz"
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
	s.audit(r, "settings.backup", true, map[string]interface{}{"install_dir": installDir, "bytes": fi.Size()})
}

// maxRestoreUploadBytes caps the uploaded archive size. Backups are small
// (config + user DB + token + state), so a few MB is generous.
const maxRestoreUploadBytes = 50 << 20 // 50 MiB

// handleRestore accepts a backup archive uploaded as multipart/form-data (field
// "archive") and restores it into the install directory via the orchestrator's
// v2-restore with --force (overwriting existing files and proceeding even
// though this server is live). Admin-only via the /api/v1/settings/ prefix.
//
// Because the running api-server caches accounts, config, and the API token in
// memory, the restore only fully takes effect after a restart — the response
// says so, and the UI surfaces a prominent warning before invoking it.
func (s *apiServer) handleRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRestoreUploadBytes+(1<<20))
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid upload (expected multipart/form-data with an 'archive' file): " + err.Error()})
		return
	}
	file, header, err := r.FormFile("archive")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "missing 'archive' file in upload"})
		return
	}
	defer file.Close()
	if header.Size > maxRestoreUploadBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, envelope{Success: false, Error: "archive too large"})
		return
	}

	tmpDir, err := os.MkdirTemp("", "ncc-restore-*")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "create temp dir: " + err.Error()})
		return
	}
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, "upload.tar.gz")
	dst, err := os.OpenFile(archivePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "stage upload: " + err.Error()})
		return
	}
	if _, err := io.Copy(dst, io.LimitReader(file, maxRestoreUploadBytes)); err != nil {
		dst.Close()
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "stage upload: " + err.Error()})
		return
	}
	if err := dst.Close(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "stage upload: " + err.Error()})
		return
	}

	installDir := s.maintenanceInstallDir()
	// Restore the files but do NOT let v2-restore restart the stack inline:
	// this api-server is part of that stack, so an inline restart would kill us
	// mid-request. We apply the (OS/version-agnostic) restore here, then kick
	// off the restart as a detached process that survives our own shutdown.
	out, err := s.runOrchestrator([]string{"v2-restore", "--install-dir", installDir, "--input-file", archivePath, "--force", "--no-restart"}, 3*time.Minute)
	if err != nil {
		s.audit(r, "settings.restore", false, map[string]interface{}{"install_dir": installDir, "filename": header.Filename})
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "restore failed: " + strings.TrimSpace(out)})
		return
	}
	s.audit(r, "settings.restore", true, map[string]interface{}{"install_dir": installDir, "filename": header.Filename})

	// Launch the restart detached and slightly delayed so this HTTP response
	// reaches the browser before v2-stop terminates us. The orchestrator binary
	// performs the actual stop + start --detach, so the stack comes back with
	// the restored config/accounts/token loaded — no manual step.
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

// spawnDetachedRestart launches `ncc-orchestrator v2-restart` as a detached,
// orphan-surviving process that stops and re-starts the stack (including this
// api-server). It returns false when the orchestrator binary isn't available,
// in which case the caller tells the operator to restart manually. A short
// delay lets the HTTP response flush before the stop signal lands.
func (s *apiServer) spawnDetachedRestart(installDir string) bool {
	base := s.orchestratorBaseCommand()
	if len(base) == 0 {
		return false
	}
	// Require a real built binary; the `go run` dev fallback is too slow/fragile
	// to hand a self-restart to.
	if base[0] == "go" {
		return false
	}
	go func() {
		// Give the response time to reach the client before we tear down.
		time.Sleep(2 * time.Second)
		args := append(append([]string{}, base[1:]...), "v2-restart", "--install-dir", installDir)
		cmd := exec.Command(base[0], args...)
		cmd.Dir = s.absPath(s.repoRoot)
		// Detach from this process group so a group-directed stop signal can't
		// take the restarter down with us, and so it outlives our shutdown.
		detachProcess(cmd)
		logf := filepath.Join(installDir, "logs", "v2-restart.log")
		if f, err := os.OpenFile(logf, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			cmd.Stdout, cmd.Stderr = f, f
			defer f.Close()
		}
		if err := cmd.Start(); err != nil {
			return
		}
		// Release so we don't wait on it; it continues after we exit.
		_ = cmd.Process.Release()
	}()
	return true
}
