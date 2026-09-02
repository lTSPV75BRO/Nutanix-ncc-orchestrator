package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// targetVersionPattern bounds the optional target-version hint the UI may pass
// for an in-app update. We accept a semver-ish tag (digits/dots, optional
// pre-release/build suffix) and reject anything else before shelling out, so
// the value can never be mistaken for a flag or smuggle extra arguments.
var targetVersionPattern = regexp.MustCompile(`^v?[0-9]+(\.[0-9]+){0,2}([-+][0-9A-Za-z.\-]+)?$`)

// Software-update phases reported to the UI while an in-app update runs.
const (
	updPhaseIdle      = "idle"
	updPhaseBackingUp = "backing_up"
	updPhaseUpdating  = "updating"
	updPhaseRestart   = "restarting"
	updPhaseDone      = "done"
	updPhaseError     = "error"
)

// updateJobState tracks a single in-flight in-app update across requests. The
// api-server is a singleton process, so package-level state guarded by a mutex
// is sufficient and avoids threading new fields through apiServer. The state is
// intentionally in-memory: once the update reaches the restart phase this very
// process is replaced, by which point the UI is already in "reconnecting" mode.
type updateJobState struct {
	mu         sync.Mutex
	inProgress bool
	phase      string
	message    string
	errMsg     string
	target     string
	backupPath string
	startedAt  time.Time
	finishedAt time.Time
}

var updateJob = &updateJobState{phase: updPhaseIdle}

func (j *updateJobState) snapshot() map[string]interface{} {
	j.mu.Lock()
	defer j.mu.Unlock()
	m := map[string]interface{}{
		"in_progress": j.inProgress,
		"phase":       j.phase,
		"message":     j.message,
	}
	if j.errMsg != "" {
		m["error"] = j.errMsg
	}
	if j.target != "" {
		m["target_version"] = j.target
	}
	if j.backupPath != "" {
		m["backup_path"] = j.backupPath
	}
	if !j.startedAt.IsZero() {
		m["started_at"] = j.startedAt.UTC().Format(time.RFC3339)
	}
	if !j.finishedAt.IsZero() {
		m["finished_at"] = j.finishedAt.UTC().Format(time.RFC3339)
	}
	return m
}

func (j *updateJobState) set(phase, message string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.phase = phase
	j.message = message
}

func (j *updateJobState) fail(message string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.inProgress = false
	j.phase = updPhaseError
	j.errMsg = message
	j.finishedAt = time.Now()
}

// tryBegin claims the job for a new run, returning false if one is already
// running.
func (j *updateJobState) tryBegin(target string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.inProgress {
		return false
	}
	j.inProgress = true
	j.phase = updPhaseBackingUp
	j.message = "Taking a pre-update backup…"
	j.errMsg = ""
	j.target = target
	j.backupPath = ""
	j.startedAt = time.Now()
	j.finishedAt = time.Time{}
	return true
}

// updateApplyRequest is the optional JSON body for POST .../update/apply.
//
// Checksum verification is intentionally NOT exposable over the network: the
// orchestrator always verifies downloaded assets against the release
// checksums.txt. The air-gapped/mirrored bypass remains a local CLI-only flag
// so a compromised admin session can't install an unverified release.
type updateApplyRequest struct {
	TargetVersion string `json:"target_version,omitempty"`
}

// handleUpdateCheck (GET /api/v1/settings/update) reports the current version,
// the latest available release in the supported track, and whether an update is
// available — by shelling out to the orchestrator's `update --check`. While an
// in-app update is running it skips the (networked) check and returns the live
// job status so the UI can poll progress. Admin-only via the /settings/ prefix.
func (s *apiServer) handleUpdateCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	// A self-update requires a real built orchestrator binary; the `go run`
	// dev fallback can't replace itself.
	supported := true
	if base := s.orchestratorBaseCommand(); len(base) == 0 || base[0] == "go" {
		supported = false
	}
	data := map[string]interface{}{
		"current_version": Version,
		"supported":       supported,
		"job":             updateJob.snapshot(),
	}

	updateJob.mu.Lock()
	busy := updateJob.inProgress
	updateJob.mu.Unlock()

	// The networked release check only runs when explicitly requested
	// (?check=1) and no update is in progress. Plain GETs are a cheap status
	// poll the UI can call frequently without hitting GitHub on every render.
	q := r.URL.Query().Get("check")
	wantCheck := q != "" && q != "0" && q != "false"
	if busy {
		data["check_skipped"] = "update in progress"
	} else if wantCheck && !supported {
		data["check_error"] = "in-app update requires a built ncc-orchestrator binary (not the dev `go run` fallback)"
	} else if wantCheck {
		out, err := s.runOrchestrator([]string{"update", "--check", "--json"}, 40*time.Second)
		// Prefer the machine-readable sentinel line; fall back to scraping the
		// human-readable output for older orchestrator binaries that predate
		// `--json`.
		if !parseUpdateCheckJSON(out, data) {
			parseUpdateCheck(out, data)
		}
		if err != nil {
			data["check_error"] = "could not check for updates: " + firstNonEmptyLine(out, err.Error())
		} else {
			// Cache the result so /metrics can expose update-availability
			// without re-running the slow subprocess at scrape time.
			avail, _ := data["update_available"].(bool)
			latest, _ := data["latest_version"].(string)
			s.setUpdateCheck(avail, latest)
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// setUpdateCheck caches the most recent update-availability result for /metrics.
func (s *apiServer) setUpdateCheck(available bool, latest string) {
	s.updateCheckMu.Lock()
	s.updateCheckAt = time.Now().UTC()
	s.updateCheckAvailable = available
	s.updateLatestVersion = strings.TrimSpace(latest)
	s.updateCheckMu.Unlock()
}

// updateCheckSnapshot returns the cached update-check result and the time it was
// taken (zero time when no check has run yet).
func (s *apiServer) updateCheckSnapshot() (available bool, latest string, at time.Time) {
	s.updateCheckMu.RLock()
	defer s.updateCheckMu.RUnlock()
	return s.updateCheckAvailable, s.updateLatestVersion, s.updateCheckAt
}

// updateCheckJSONPrefix mirrors the orchestrator's sentinel for the
// machine-readable `update --check --json` result line.
const updateCheckJSONPrefix = "NCC_UPDATE_JSON "

// parseUpdateCheckJSON looks for the sentinel-prefixed JSON line emitted by
// `update --check --json` in the (combined stdout+stderr) output and folds it
// into data. It returns true when a well-formed result was found, so the
// caller can skip the brittle text-scraping fallback.
func parseUpdateCheckJSON(out string, data map[string]interface{}) bool {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, updateCheckJSONPrefix) {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, updateCheckJSONPrefix))
		var parsed struct {
			CurrentVersion  string `json:"current_version"`
			LatestVersion   string `json:"latest_version"`
			LatestOverall   string `json:"latest_overall"`
			UpdateAvailable bool   `json:"update_available"`
			HasPackage      bool   `json:"has_package"`
		}
		if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
			return false
		}
		if parsed.CurrentVersion != "" {
			data["current_version"] = parsed.CurrentVersion
		}
		if parsed.LatestVersion != "" {
			data["latest_version"] = parsed.LatestVersion
		}
		if parsed.LatestOverall != "" {
			data["latest_overall"] = parsed.LatestOverall
		}
		data["update_available"] = parsed.UpdateAvailable
		data["has_package"] = parsed.HasPackage
		return true
	}
	return false
}

// parseUpdateCheck extracts version/availability info from `update --check`
// human-readable output into data (best-effort; the raw text is also included).
func parseUpdateCheck(out string, data map[string]interface{}) {
	data["raw"] = strings.TrimSpace(out)
	available := strings.Contains(out, "Update available")
	data["update_available"] = available
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "Current version:"):
			data["current_version"] = strings.TrimSpace(strings.TrimPrefix(line, "Current version:"))
		case strings.HasPrefix(line, "Selected release:"):
			v := strings.TrimSpace(strings.TrimPrefix(line, "Selected release:"))
			data["latest_version"] = strings.TrimPrefix(v, "v")
		case strings.HasPrefix(line, "Update available in track:"):
			// "Update available in track: X -> Y"
			if idx := strings.LastIndex(line, "->"); idx != -1 {
				data["latest_version"] = strings.TrimPrefix(strings.TrimSpace(line[idx+2:]), "v")
			}
		}
	}
	if !available {
		if _, ok := data["latest_version"]; !ok {
			if cv, ok := data["current_version"]; ok {
				data["latest_version"] = cv
			}
		}
	}
}

func firstNonEmptyLine(primary, fallback string) string {
	for _, line := range strings.Split(primary, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			return t
		}
	}
	return fallback
}

// handleUpdateApply (POST /api/v1/settings/update/apply) starts an in-app
// update: it takes a pre-update backup, applies the package update (orchestrator
// + api + ui binaries + frontend, checksum-verified), and then restarts the
// stack so the new versions load. The work runs in the background and the UI
// polls GET /api/v1/settings/update for phase transitions; this handler returns
// 202 once the job is accepted. Admin-only via the /settings/ prefix.
func (s *apiServer) handleUpdateApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := s.capabilities.RejectHostOperation("in-place software updates"); err != nil {
		writeJSON(w, http.StatusConflict, envelope{
			Success:   false,
			Error:     err.Error() + "; update the container image and roll out the Deployment",
			ErrorCode: "NCC_KUBERNETES_IMMUTABLE_UPDATE",
		})
		return
	}

	// A self-update needs a real built binary; reject the dev `go run` fallback
	// up front so we never half-apply.
	if base := s.orchestratorBaseCommand(); len(base) == 0 || base[0] == "go" {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "in-app update requires a built ncc-orchestrator binary"})
		return
	}

	var req updateApplyRequest
	if r.Body != nil {
		// Body is optional; ignore decode errors for an empty/whitespace body.
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req)
	}
	req.TargetVersion = strings.TrimSpace(req.TargetVersion)
	if req.TargetVersion != "" && !targetVersionPattern.MatchString(req.TargetVersion) {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid target_version (expected a version like 2.1.0)", ErrorCode: "NCC_API_INVALID_INPUT"})
		return
	}

	if !updateJob.tryBegin(req.TargetVersion) {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "an update is already in progress", Data: updateJob.snapshot()})
		return
	}

	s.audit(r, "settings.update", true, map[string]interface{}{
		"target_version": req.TargetVersion,
	})

	go s.runUpdateJob(req)

	writeJSON(w, http.StatusAccepted, envelope{
		Success: true,
		Message: "Update started: taking a backup, applying the update, then restarting the stack. This page will reconnect when the new version is live.",
		Data:    updateJob.snapshot(),
	})
}

// runUpdateJob performs backup -> update -> detached restart, advancing the
// shared job state through each phase. It is launched in its own goroutine.
func (s *apiServer) runUpdateJob(req updateApplyRequest) {
	installDir := s.maintenanceInstallDir()

	// 1. Pre-update backup. We refuse to apply the update if the backup fails,
	//    since the whole point is to have a rollback point first.
	backupDir := filepath.Join(installDir, "backups")
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		s.updateFailedTotal.Add(1)
		updateJob.fail("could not create backup directory: " + err.Error())
		return
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	// Seal the rollback point at rest when a backup key is configured, so the
	// pre-update secrets bundle isn't left plaintext on disk. v2-backup reads the
	// key from the inherited NCC_BACKUP_* environment; rollback (v2-restore) picks
	// up the same key the same way, so the auto-restore stays transparent.
	encrypt := backupKeyConfigured()
	backupName := "pre-update-" + stamp + backupFileSuffix
	if encrypt {
		backupName = "pre-update-" + stamp + backupEncSuffix
	}
	backupPath := filepath.Join(backupDir, backupName)
	updateJob.set(updPhaseBackingUp, "Taking a pre-update backup…")
	backupArgs := []string{"v2-backup", "--install-dir", installDir, "--output-file", backupPath}
	if encrypt {
		backupArgs = append(backupArgs, "--encrypt")
	}
	if out, err := s.runOrchestrator(backupArgs, 3*time.Minute); err != nil {
		s.updateFailedTotal.Add(1)
		s.notifyOperationalFailure("backup_failure", "NCC pre-update backup failed — update aborted", map[string]interface{}{
			"error":   firstNonEmptyLine(out, err.Error()),
			"encrypt": encrypt,
		})
		updateJob.fail("pre-update backup failed (update not applied): " + firstNonEmptyLine(out, err.Error()))
		return
	}
	updateJob.mu.Lock()
	updateJob.backupPath = backupPath
	updateJob.mu.Unlock()

	// 2. Apply the package update (downloads, checksum-verifies, installs all
	//    stack binaries + frontend, and self-replaces the orchestrator).
	updateJob.set(updPhaseUpdating, "Downloading and installing the update…")
	args := []string{"update"}
	if req.TargetVersion != "" {
		args = append(args, "--target-version", req.TargetVersion)
	}
	out, err := s.runOrchestrator(args, 12*time.Minute)
	if err != nil {
		s.updateFailedTotal.Add(1)
		updateJob.fail("update failed (a pre-update backup was saved at " + backupPath + "): " + firstNonEmptyLine(out, err.Error()))
		return
	}
	// "Already on the latest version" is a success with nothing to restart.
	if strings.Contains(out, "already on the latest version") || strings.Contains(out, "No upgrade needed") {
		updateJob.mu.Lock()
		updateJob.inProgress = false
		updateJob.phase = updPhaseDone
		updateJob.message = "Already on the latest version — nothing to update."
		updateJob.finishedAt = time.Now()
		updateJob.mu.Unlock()
		return
	}

	// An update was actually installed at this point (the "already latest" and
	// error paths returned above), so count it before the restart replaces us.
	s.updateAppliedTotal.Add(1)

	// 3. Restart the stack (detached) so the freshly-installed api/ui binaries
	//    and frontend take effect. This kills and re-launches this process.
	updateJob.set(updPhaseRestart, "Update installed. Restarting the stack to load the new version…")
	if !s.spawnDetachedRestart(installDir) {
		updateJob.mu.Lock()
		updateJob.inProgress = false
		updateJob.phase = updPhaseDone
		updateJob.message = "Update installed, but an automatic restart is unavailable. Restart the stack (v2-restart) to load the new version."
		updateJob.finishedAt = time.Now()
		updateJob.mu.Unlock()
		return
	}
	// Leave inProgress true: the restart will replace this process shortly.
}
