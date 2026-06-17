package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file implements scheduled, optionally-encrypted server-side backups.
//
// Rather than installing a systemd timer / cron entry, the api-server runs the
// schedule itself on an in-process ticker. That is deliberate: only this
// process holds the backup-encryption key material (NCC_BACKUP_* in its
// environment), which a detached systemd unit would not inherit — so an
// in-process loop is the one place that can produce *encrypted* scheduled
// backups without writing secrets into a unit file. It also keeps the feature
// cross-platform and reuses the exact same `v2-backup` path as manual
// snapshots. The trade-off vs. a systemd timer is that a backup missed while
// the stack is fully down is not replayed; the next tick after recovery simply
// runs (state persists last_run_at so cadence is honored across restarts).

// backupScheduleState is the persisted desired-state + last-run record.
type backupScheduleState struct {
	Enabled    bool   `json:"enabled"`
	Every      string `json:"every"` // 15m, 4h, 1d (validated by reBackupEvery)
	Encrypt    bool   `json:"encrypt"`
	Retain     int    `json:"retain"` // keep newest N scheduled archives (0 = keep all)
	LastRunAt  string `json:"last_run_at,omitempty"`
	LastStatus string `json:"last_status,omitempty"` // "ok" | "error"
	LastError  string `json:"last_error,omitempty"`
	LastFile   string `json:"last_file,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

// reBackupEvery mirrors the run scheduler's interval grammar: a positive number
// followed by m (minutes), h (hours), or d (days).
var reBackupEvery = regexp.MustCompile(`^[1-9][0-9]*[mhd]$`)

// parseEveryDuration converts "15m"/"4h"/"1d" to a time.Duration. Go's
// time.ParseDuration has no day unit, so days are handled explicitly.
func parseEveryDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if !reBackupEvery.MatchString(s) {
		return 0, errors.New("every must match patterns like 15m, 4h, or 1d")
	}
	n, err := strconv.Atoi(s[:len(s)-1])
	if err != nil || n <= 0 {
		return 0, errors.New("every must be a positive interval like 15m, 4h, or 1d")
	}
	switch s[len(s)-1] {
	case 'm':
		return time.Duration(n) * time.Minute, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return 0, errors.New("invalid interval unit")
}

const backupScheduleMinInterval = 5 * time.Minute

func (s *apiServer) loadBackupSchedule() (backupScheduleState, error) {
	path := s.absPath(s.backupScheduleStatePath)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return backupScheduleState{Enabled: false, Every: "24h", Retain: 7}, nil
		}
		return backupScheduleState{}, err
	}
	var st backupScheduleState
	if err := json.Unmarshal(b, &st); err != nil {
		return backupScheduleState{}, err
	}
	return st, nil
}

func (s *apiServer) saveBackupSchedule(st backupScheduleState) error {
	path := s.absPath(s.backupScheduleStatePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// backupScheduleMu serializes state writes and prevents overlapping runs.
var backupScheduleMu sync.Mutex

func validateBackupScheduleInput(st backupScheduleState) error {
	if !st.Enabled {
		return nil
	}
	d, err := parseEveryDuration(st.Every)
	if err != nil {
		return err
	}
	if d < backupScheduleMinInterval {
		return fmt.Errorf("interval too small; minimum is %s", backupScheduleMinInterval)
	}
	if st.Retain < 0 {
		return errors.New("retain must be >= 0")
	}
	if st.Encrypt && !backupKeyConfigured() {
		return errors.New("encryption requested but no backup key is configured (set NCC_BACKUP_KEY_FILE, NCC_BACKUP_KEY, or NCC_BACKUP_PASSPHRASE on the API server)")
	}
	return nil
}

// runScheduledBackupOnce produces one scheduled snapshot via `v2-backup`,
// inheriting the api-server's environment (so NCC_BACKUP_* supplies the
// encryption key when Encrypt is set), then prunes older scheduled archives to
// Retain. It records the outcome in the persisted state and alerts on failure.
func (s *apiServer) runScheduledBackupOnce(ctx context.Context, st backupScheduleState) {
	installDir := s.maintenanceInstallDir()
	dir := s.backupsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		s.finishScheduledBackup(st, "", fmt.Errorf("create backups dir: %w", err))
		return
	}
	args := []string{"v2-backup", "--install-dir", installDir, "--output-dir", dir}
	if st.Encrypt {
		args = append(args, "--encrypt")
	}
	out, err := s.runOrchestratorEnv(args, 5*time.Minute, nil)
	if err != nil {
		s.finishScheduledBackup(st, "", fmt.Errorf("%s", firstNonEmptyLine(out, err.Error())))
		return
	}
	// Prune older scheduled archives (ncc-backup-*), leaving manual-* and the
	// pre-update-* rollback points untouched.
	if st.Retain > 0 {
		pruneBackupsByPrefix(dir, "ncc-backup-", st.Retain)
	}
	s.finishScheduledBackup(st, scheduledBackupFileFromOutput(out), nil)
	s.auditEvent("schedule.backup.run", true, map[string]interface{}{"encrypted": st.Encrypt})
}

// scheduledBackupFileFromOutput extracts the written archive path from
// v2-backup's "Backup written: <path>" line (best-effort, for display only).
func scheduledBackupFileFromOutput(out string) string {
	for _, ln := range strings.Split(out, "\n") {
		ln = strings.TrimSpace(ln)
		if strings.HasPrefix(ln, "Backup written:") {
			return strings.TrimSpace(strings.TrimPrefix(ln, "Backup written:"))
		}
	}
	return ""
}

func (s *apiServer) finishScheduledBackup(st backupScheduleState, file string, runErr error) {
	backupScheduleMu.Lock()
	defer backupScheduleMu.Unlock()
	// Re-load so a concurrent config change (Enabled/Every/...) is preserved;
	// we only overwrite the last-run fields.
	cur, err := s.loadBackupSchedule()
	if err != nil {
		cur = st
	}
	cur.LastRunAt = time.Now().UTC().Format(time.RFC3339)
	if runErr != nil {
		cur.LastStatus = "error"
		cur.LastError = runErr.Error()
		cur.LastFile = ""
		s.notifyOperationalFailure("backup_failure", "NCC scheduled backup failed", map[string]interface{}{
			"error":   runErr.Error(),
			"encrypt": cur.Encrypt,
		})
	} else {
		cur.LastStatus = "ok"
		cur.LastError = ""
		cur.LastFile = file
	}
	if saveErr := s.saveBackupSchedule(cur); saveErr != nil {
		log.Printf("scheduled backup: save state error: %v", saveErr)
	}
}

// scheduledBackupDue reports whether a run is due given the persisted state and
// the current time.
func scheduledBackupDue(st backupScheduleState, now time.Time) bool {
	if !st.Enabled {
		return false
	}
	d, err := parseEveryDuration(st.Every)
	if err != nil {
		return false
	}
	if strings.TrimSpace(st.LastRunAt) == "" {
		return true // never run since enabled — take the first snapshot soon
	}
	last, err := time.Parse(time.RFC3339, st.LastRunAt)
	if err != nil {
		return true
	}
	return now.Sub(last) >= d
}

// startBackupScheduleLoop launches the ticker that triggers scheduled backups.
// It always runs (cheap, no-ops when disabled) so the schedule can be toggled
// at runtime via the API without a restart.
func (s *apiServer) startBackupScheduleLoop(ctx context.Context) {
	go func() {
		// Small initial delay so a backup isn't taken mid-startup.
		select {
		case <-ctx.Done():
			return
		case <-time.After(45 * time.Second):
		}
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			s.maybeRunScheduledBackup(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *apiServer) maybeRunScheduledBackup(ctx context.Context) {
	st, err := s.loadBackupSchedule()
	if err != nil {
		return
	}
	if !scheduledBackupDue(st, time.Now().UTC()) {
		return
	}
	if !backupScheduleMu.TryLock() {
		return // a run is already in progress
	}
	backupScheduleMu.Unlock()
	s.runScheduledBackupOnce(ctx, st)
}

// handleBackupSchedule serves GET (current config + last-run) and PUT (update
// config, optionally triggering an immediate run). Admin-only via /settings/.
func (s *apiServer) handleBackupSchedule(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		st, err := s.loadBackupSchedule()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		var nextRun string
		if st.Enabled {
			if d, derr := parseEveryDuration(st.Every); derr == nil && strings.TrimSpace(st.LastRunAt) != "" {
				if last, perr := time.Parse(time.RFC3339, st.LastRunAt); perr == nil {
					nextRun = last.Add(d).UTC().Format(time.RFC3339)
				}
			}
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"schedule":       st,
			"key_configured": backupKeyConfigured(),
			"next_run":       nextRun,
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req struct {
			Enabled bool   `json:"enabled"`
			Every   string `json:"every"`
			Encrypt bool   `json:"encrypt"`
			Retain  int    `json:"retain"`
			RunNow  bool   `json:"run_now"`
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		cur, _ := s.loadBackupSchedule()
		next := backupScheduleState{
			Enabled:   req.Enabled,
			Every:     strings.TrimSpace(req.Every),
			Encrypt:   req.Encrypt,
			Retain:    req.Retain,
			LastRunAt: cur.LastRunAt,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		if next.Every == "" {
			next.Every = "24h"
		}
		if err := validateBackupScheduleInput(next); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.saveBackupSchedule(next); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "schedule.backup.update", true, map[string]interface{}{
			"enabled": next.Enabled, "every": next.Every, "encrypt": next.Encrypt, "retain": next.Retain,
		})
		if req.RunNow {
			// Run synchronously so the immediate result (and any error) is
			// reflected in the response the operator sees.
			s.runScheduledBackupOnce(r.Context(), next)
			next, _ = s.loadBackupSchedule()
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "scheduled backups updated", Data: map[string]interface{}{"schedule": next}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}
