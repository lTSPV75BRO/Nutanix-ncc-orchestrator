package main

import (
	"encoding/json"
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

	// Optional client-side encryption. The passphrase arrives in a header (not a
	// query param) so it never lands in the URL/access log, and we hand it to
	// v2-backup via the environment (NCC_BACKUP_PASSPHRASE) rather than argv so
	// it stays out of the process listing and audit log too.
	passphrase := strings.TrimSpace(r.Header.Get("X-NCC-Backup-Passphrase"))
	if len(passphrase) > 1024 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "passphrase too long"})
		return
	}
	encrypt := passphrase != ""

	stamp := time.Now().UTC().Format("20060102T150405Z")
	outName := "ncc-backup-" + stamp + ".tar.gz"
	if encrypt {
		outName += ".enc"
	}
	outPath := filepath.Join(tmpDir, outName)
	args := []string{"v2-backup", "--install-dir", installDir, "--output-file", outPath}
	var extraEnv []string
	if encrypt {
		args = append(args, "--encrypt")
		extraEnv = []string{"NCC_BACKUP_PASSPHRASE=" + passphrase}
	}
	out, err := s.runOrchestratorEnv(args, 2*time.Minute, extraEnv)
	if err != nil {
		s.audit(r, "settings.backup", false, map[string]interface{}{"install_dir": installDir, "encrypted": encrypt})
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
	contentType := "application/gzip"
	if encrypt {
		// Encrypted archives are opaque AES-256-GCM envelopes, not gzip.
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", outName))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
	s.audit(r, "settings.backup", true, map[string]interface{}{"install_dir": installDir, "bytes": fi.Size(), "encrypted": encrypt})
}

// maxRestoreUploadBytes caps the uploaded archive size. Backups are small
// (config + user DB + token + state), so a few MB is generous.
const maxRestoreUploadBytes = 50 << 20 // 50 MiB
const startStateFileName = ".ncc-v2-start.json"
const scheduleStateFileName = ".ncc-api-schedule.json"

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

	// Optional decryption passphrase for an encrypted archive (v2-backup
	// --encrypt). v2-restore auto-detects encryption by magic header; we only
	// need to supply the key. Passed via the environment, not argv, so it stays
	// out of the process list and audit log. An unencrypted archive ignores it.
	passphrase := strings.TrimSpace(r.FormValue("passphrase"))
	if len(passphrase) > 1024 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "passphrase too long"})
		return
	}

	installDir := s.maintenanceInstallDir()
	// Restore the files but do NOT let v2-restore restart the stack inline:
	// this api-server is part of that stack, so an inline restart would kill us
	// mid-request. We apply the (OS/version-agnostic) restore here, then kick
	// off the restart as a detached process that survives our own shutdown.
	var extraEnv []string
	if passphrase != "" {
		extraEnv = []string{"NCC_BACKUP_PASSPHRASE=" + passphrase}
	}
	out, err := s.runOrchestratorEnv([]string{"v2-restore", "--install-dir", installDir, "--input-file", archivePath, "--force", "--no-restart"}, 3*time.Minute, extraEnv)
	if err != nil {
		s.audit(r, "settings.restore", false, map[string]interface{}{"install_dir": installDir, "filename": header.Filename})
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "restore failed: " + strings.TrimSpace(out)})
		return
	}
	selfHealNotes := s.postRestoreValidateAndSelfHeal(installDir)
	s.audit(r, "settings.restore", true, map[string]interface{}{"install_dir": installDir, "filename": header.Filename})
	if len(selfHealNotes) > 0 {
		s.audit(r, "settings.restore.selfheal", true, map[string]interface{}{"install_dir": installDir, "notes": selfHealNotes})
	}

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
			"self_heal_notes":  selfHealNotes,
		},
	})
}

// postRestoreValidateAndSelfHeal applies small, safe fixes for common
// post-restore boot issues (e.g. restored start-state pointing at ui.crt/ui.key
// while only ui-selfsigned.* exists on disk after a clean install).
func (s *apiServer) postRestoreValidateAndSelfHeal(installDir string) []string {
	var notes []string

	// Normalize persisted scheduler state paths before health checks/readback.
	// Restored state can carry stale short paths (/root/config.yaml, /root/logs)
	// from older installs, which makes Scheduler Health look stale even when the
	// host timer/service was recreated correctly.
	if normalized := normalizeScheduleStatePaths(filepath.Join(installDir, scheduleStateFileName), installDir); normalized {
		notes = append(notes, "normalized scheduler state paths to install-root defaults")
	}

	// Validate and heal UI TLS file paths referenced by the persisted start state.
	statePath := filepath.Join(installDir, startStateFileName)
	raw, err := os.ReadFile(statePath)
	if err == nil && len(raw) > 0 {
		var st map[string]interface{}
		if json.Unmarshal(raw, &st) == nil {
			certPath := strings.TrimSpace(fmt.Sprintf("%v", st["ui_tls_cert_file"]))
			keyPath := strings.TrimSpace(fmt.Sprintf("%v", st["ui_tls_key_file"]))
			if certPath != "" && keyPath != "" {
				if healed := healMissingTLSPair(certPath, keyPath, installDir); healed {
					notes = append(notes, "repaired missing UI TLS cert/key from self-signed fallback")
				}
			}
		}
	}

	// Validate and heal persisted scheduler artifacts (systemd/cron wrapper)
	// that can be absent after restore when generated helper scripts lived under
	// logs/ and were not present in the restored image.
	if healed, note := s.healMissingScheduleArtifacts(installDir); healed {
		notes = append(notes, note)
	}

	return notes
}

type persistedScheduleState struct {
	Type     string `json:"type"`
	Action   string `json:"action"`
	Every    string `json:"every"`
	Config   string `json:"config"`
	LogPath  string `json:"log_path"`
	WithLock bool   `json:"with_lock"`
	TaskName string `json:"task_name"`
}

func (s *apiServer) healMissingScheduleArtifacts(installDir string) (bool, string) {
	statePath := filepath.Join(installDir, scheduleStateFileName)
	raw, err := os.ReadFile(statePath)
	if err != nil || len(raw) == 0 {
		return false, ""
	}
	var st persistedScheduleState
	if err := json.Unmarshal(raw, &st); err != nil {
		return false, ""
	}
	if strings.TrimSpace(st.Action) != "" && !strings.EqualFold(strings.TrimSpace(st.Action), "create") {
		return false, ""
	}
	schedType := strings.ToLower(strings.TrimSpace(st.Type))
	if schedType != "systemd" && schedType != "cron" {
		return false, ""
	}
	taskName := strings.TrimSpace(st.TaskName)
	if taskName == "" {
		taskName = "ncc-orchestrator"
	}
	// If the generated runner already exists, we don't need to recreate.
	// (systemd: logs/ncc-sched-<task>.sh, cron: logs/ncc-scheduler.sh)
	systemdRunner := filepath.Join(installDir, "logs", fmt.Sprintf("ncc-sched-%s.sh", taskName))
	cronRunner := filepath.Join(installDir, "logs", "ncc-scheduler.sh")
	if fileExists(systemdRunner) || fileExists(cronRunner) {
		return false, ""
	}

	configPath := strings.TrimSpace(st.Config)
	if configPath == "" {
		configPath = filepath.Join(installDir, "config.yaml")
	}
	logPath := strings.TrimSpace(st.LogPath)
	if logPath == "" {
		logPath = filepath.Join(installDir, "logs", "ncc-scheduler.log")
	}
	every := strings.TrimSpace(st.Every)
	if every == "" {
		every = "4h"
	}
	args := []string{
		"--config", configPath, "create-schedule",
		"--type", schedType,
		"--every", every,
		"--log-path", logPath,
		"--task-name", taskName,
		"--with-lock", fmt.Sprintf("%t", st.WithLock),
		"--print-only=false",
	}
	if _, err := s.runOrchestratorEnv(args, 30*time.Second, nil); err != nil {
		return false, ""
	}
	return true, "repaired missing scheduler artifacts from persisted schedule state"
}

func normalizeScheduleStatePaths(statePath, installDir string) bool {
	raw, err := os.ReadFile(statePath)
	if err != nil || len(raw) == 0 {
		return false
	}
	var st map[string]interface{}
	if err := json.Unmarshal(raw, &st); err != nil {
		return false
	}
	changed := false
	// Keep scheduler state aligned with current install-root so health/readback
	// and future repairs point at the actual config/log locations.
	wantCfg := filepath.Join(installDir, "config.yaml")
	if cfg := strings.TrimSpace(fmt.Sprintf("%v", st["config"])); cfg != "" {
		if strings.HasPrefix(cfg, "/root/") && !strings.HasPrefix(cfg, installDir+"/") {
			st["config"] = wantCfg
			changed = true
		}
	}
	wantLog := filepath.Join(installDir, "logs", "ncc-scheduler.log")
	if lp := strings.TrimSpace(fmt.Sprintf("%v", st["log_path"])); lp != "" {
		if strings.HasPrefix(lp, "/root/") && !strings.HasPrefix(lp, installDir+"/") {
			st["log_path"] = wantLog
			changed = true
		}
	}
	if !changed {
		return false
	}
	updated, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return false
	}
	return os.WriteFile(statePath, updated, 0o600) == nil
}

func healMissingTLSPair(certPath, keyPath, installDir string) bool {
	if fileExists(certPath) && fileExists(keyPath) {
		return false
	}
	fallbackCert := filepath.Join(installDir, "tls", "ui-selfsigned.crt")
	fallbackKey := filepath.Join(installDir, "tls", "ui-selfsigned.key")
	if !fileExists(fallbackCert) || !fileExists(fallbackKey) {
		return false
	}
	if err := os.MkdirAll(filepath.Dir(certPath), 0o700); err != nil {
		return false
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return false
	}
	if !fileExists(certPath) {
		if b, err := os.ReadFile(fallbackCert); err == nil {
			if err := os.WriteFile(certPath, b, 0o600); err != nil {
				return false
			}
		}
	}
	if !fileExists(keyPath) {
		if b, err := os.ReadFile(fallbackKey); err == nil {
			if err := os.WriteFile(keyPath, b, 0o600); err != nil {
				return false
			}
		}
	}
	return fileExists(certPath) && fileExists(keyPath)
}

func fileExists(p string) bool {
	if strings.TrimSpace(p) == "" {
		return false
	}
	if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
		return true
	}
	return false
}

func hasSystemdStackService() bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	// `systemctl cat` succeeds only when the unit is known to systemd.
	return exec.Command("systemctl", "cat", "ncc-orchestrator.service").Run() == nil
}

func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

// spawnDetachedRestart launches a detached restart that preserves the current
// runtime mode after restore:
//   - if the stack is installed as a systemd service, restart that service
//     (keeps v2-supervise/service mode and supervisor PID visibility),
//   - otherwise fall back to `ncc-orchestrator v2-restart`.
//
// It returns false when neither restart path is available, in which case the
// caller tells the operator to restart manually. A short delay lets the HTTP
// response flush before the stop signal lands.
func (s *apiServer) spawnDetachedRestart(installDir string) bool {
	base := s.orchestratorBaseCommand()
	useSystemd := hasSystemdStackService()
	if !useSystemd && len(base) == 0 {
		return false
	}
	// Require a real built binary; the `go run` dev fallback is too slow/fragile
	// to hand a self-restart to.
	if !useSystemd && base[0] == "go" {
		return false
	}
	go func() {
		// Give the response time to reach the client before we tear down.
		time.Sleep(2 * time.Second)
		var cmd *exec.Cmd
		if useSystemd {
			// Restart the service in a detached shell so we can do a best-effort
			// pre-clean first:
			//  1) relabel binaries for SELinux (fixes status=203/EXEC),
			//  2) stop detached leftovers that can hold :8080/:8081 and cause
			//     supervisor child bind failures during restart.
			installQ := shellSingleQuote(installDir)
			orchBin := filepath.Join(installDir, "bin", "ncc-orchestrator")
			orchBinQ := shellSingleQuote(orchBin)
			script := strings.Join([]string{
				"set -e",
				"if command -v chcon >/dev/null 2>&1; then",
				"  chcon -t bin_t " + shellSingleQuote(filepath.Join(installDir, "bin", "ncc-orchestrator")) + " " + shellSingleQuote(filepath.Join(installDir, "bin", "ncc-api-server")) + " " + shellSingleQuote(filepath.Join(installDir, "bin", "ncc-ui-server")) + " >/dev/null 2>&1 || true",
				"fi",
				"systemctl stop ncc-orchestrator.service >/dev/null 2>&1 || true",
				"if [ -x " + orchBinQ + " ]; then " + orchBinQ + " v2-stop --install-dir " + installQ + " >/dev/null 2>&1 || true; fi",
				"systemctl restart ncc-orchestrator.service",
			}, "; ")
			cmd = exec.Command("/bin/sh", "-c", script)
		} else {
			args := append(append([]string{}, base[1:]...), "v2-restart", "--install-dir", installDir)
			cmd = exec.Command(base[0], args...)
			cmd.Dir = s.absPath(s.repoRoot)
		}
		cmd.Env = childEnv()
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
