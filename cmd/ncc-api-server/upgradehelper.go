package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The update helper (cmd/ncc-orchestrator/upgrade_*.go) carries upgrades out
// from outside the service. The API server starts them, shows their progress,
// offers the rollback, and brings a stack that an older release upgraded up to
// this version (adoption).

// helperUpgradePhases mirrors helperPhaseActive in the orchestrator.
var helperUpgradePhases = map[string]bool{
	"downloading": true, "helper_install": true, "helper_check": true, "handover": true, "staging": true, "saving": true, "stopping": true,
	"swapping": true, "migrating": true, "starting": true, "checking": true, "restoring": true,
}

// upgradeHeartbeatLimit: the helper and the preparation touch their job every
// 15 seconds; after this long without it, nobody is at work.
const upgradeHeartbeatLimit = 5 * time.Minute

// helperJob reports whether the job is (or was) not the supervisor's own.
func (j *upgradeJob) helperJob() bool { return j.Runner == "helper" || j.Runner == "stage" }

// outOfTouch reports whether the job was not touched for too long.
func (j *upgradeJob) outOfTouch(now time.Time) bool {
	at, err := time.Parse(time.RFC3339, j.UpdatedAt)
	return err != nil || now.Sub(at) > upgradeHeartbeatLimit
}

// upgradeInProgress says why a run must not start now ("" when it may): the
// upgrade stops the API server, and the run with it.
func (s *apiServer) upgradeInProgress() string {
	if s.capabilities.Kubernetes {
		return ""
	}
	if up := s.readUpgradeJob(); up != nil && upgradeJobActive(up) {
		return "an upgrade is in progress; try again when it has finished"
	}
	return ""
}

func envOff(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "0", "false", "off", "no":
		return true
	}
	return false
}

// handStartedStack reports whether v2-start --detach started this server: the
// update helper can then stop and start the stack the same way.
func (s *apiServer) handStartedStack() bool {
	if s.capabilities.Kubernetes || envOff("NCC_UPGRADE_HELPER") {
		return false
	}
	if base := s.orchestratorBaseCommand(); len(base) == 0 || base[0] == "go" {
		return false
	}
	if _, supervised := s.supervisorPID(); supervised {
		return false
	}
	b, err := os.ReadFile(filepath.Join(s.maintenanceInstallDir(), "run", "v2-api.pid"))
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	return err == nil && pid == os.Getpid()
}

// startHandStartedUpgrade runs `upgrade` as a process of its own.
func (s *apiServer) startHandStartedUpgrade(req upgradeRequest) error {
	base := s.orchestratorBaseCommand()
	if len(base) == 0 {
		return errors.New("no ncc-orchestrator program")
	}
	installDir := s.maintenanceInstallDir()
	args := append(append([]string{}, base[1:]...), "upgrade", "--no-wait", "--install-dir", installDir,
		"--id", req.ID, "--backup-path", req.BackupPath, "--requested-by", req.RequestedBy)
	if req.TargetVersion != "" {
		args = append(args, "--to", req.TargetVersion)
	}
	if req.ReplaceEdited {
		args = append(args, "--replace-edited")
	}
	logDir := filepath.Join(installDir, "logs")
	_ = os.MkdirAll(logDir, 0o755) // #nosec G301 -- same mode as the stack's own log folder
	f, err := os.OpenFile(filepath.Join(logDir, "upgrade-helper.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	cmd, err := startDetached(func() *exec.Cmd {
		c := exec.Command(base[0], args...) // #nosec G204 -- our own program; the version was checked by the handler
		c.Dir = s.absPath(s.repoRoot)
		c.Env = childEnv()
		c.Stdout, c.Stderr = f, f
		return c
	})
	if err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

// handleUpdatePlan (GET /api/v1/settings/update/plan) shows what an upgrade
// would do, without doing it.
func (s *apiServer) handleUpdatePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := s.capabilities.RejectHostOperation("in-place software updates"); err != nil {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: "NCC_KUBERNETES_IMMUTABLE_UPDATE"})
		return
	}
	target := strings.TrimSpace(r.URL.Query().Get("target_version"))
	if target != "" && !targetVersionPattern.MatchString(target) {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid target_version (expected a version like 2.1.0)", ErrorCode: "NCC_API_INVALID_INPUT"})
		return
	}
	args := []string{"upgrade", "--plan", "--json", "--install-dir", s.maintenanceInstallDir()}
	if !s.loadFeatureFlags().UpdateChecks {
		args = append(args, "--offline")
	}
	if target != "" {
		args = append(args, "--to", target)
	}
	if v := r.URL.Query().Get("replace_edited"); v == "1" || v == "true" {
		args = append(args, "--replace-edited")
	}
	out, err := s.runOrchestrator(args, 60*time.Second)
	var plan map[string]interface{}
	if start, end := strings.Index(out, "{"), strings.LastIndex(out, "}"); err != nil || start < 0 || end < start || json.Unmarshal([]byte(out[start:end+1]), &plan) != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "could not plan the upgrade: " + redactPaths(lastErrorLine(out))})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: plan})
}

// rollbackOffer is the saved previous version a Roll back puts back: after an
// upgrade, an adoption or `update` (run/upgrade/rollback.json, kept 24 hours).
type rollbackOffer struct {
	To    string `json:"to"`   // the version that comes back
	From  string `json:"from"` // the version now installed
	Of    string `json:"of"`   // upgrade | adopt | update
	Until string `json:"until"`
}

// rollbackKeep mirrors helperKeepSnapshot in the orchestrator.
const rollbackKeep = 24 * time.Hour

func (s *apiServer) readRollbackOffer(now time.Time) *rollbackOffer {
	dir := filepath.Join(s.maintenanceInstallDir(), "run", "upgrade")
	b, err := os.ReadFile(filepath.Join(dir, "rollback.json"))
	if err != nil || len(b) > 1<<20 {
		return nil
	}
	var info struct {
		From   string `json:"from"`
		To     string `json:"to"`
		At     string `json:"at"`
		Action string `json:"action"`
	}
	if json.Unmarshal(b, &info) != nil {
		return nil
	}
	at, err := time.Parse(time.RFC3339, info.At)
	if err != nil || now.Sub(at) > rollbackKeep {
		return nil
	}
	if st, err := os.Stat(filepath.Join(dir, "snapshot")); err != nil || !st.IsDir() {
		return nil
	}
	return &rollbackOffer{To: info.From, From: info.To, Of: info.Action, Until: at.Add(rollbackKeep).UTC().Format(time.RFC3339)}
}

// handleUpdateRollback (POST /api/v1/settings/update/rollback) puts the version
// before the last upgrade back (kept for 24 hours).
func (s *apiServer) handleUpdateRollback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := s.capabilities.RejectHostOperation("in-place software updates"); err != nil {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: "NCC_KUBERNETES_IMMUTABLE_UPDATE"})
		return
	}
	if s.hasInFlightRuns() {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "a run is in progress; roll back when it finishes"})
		return
	}
	if msg := s.upgradeInProgress(); msg != "" {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: msg})
		return
	}
	requestedBy := ""
	if p, ok := principalFromContext(r.Context()); ok {
		requestedBy = p.subject
	}
	before := s.readUpgradeJob()
	out, err := s.runOrchestrator([]string{"upgrade", "--rollback", "--no-wait", "--install-dir", s.maintenanceInstallDir(), "--requested-by", requestedBy}, 90*time.Second)
	if err != nil {
		// The helper may already be stopping the service, and with it the
		// program that started it.
		if job := s.readUpgradeJob(); job != nil && job.Action == "rollback" && job.Runner == "helper" && (before == nil || before.ID != job.ID) {
			err = nil
		}
	}
	s.audit(r, "settings.update.rollback", err == nil, nil)
	if err != nil {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "could not roll back: " + redactPaths(strings.TrimPrefix(lastErrorLine(out), "Error: "))})
		return
	}
	writeJSON(w, http.StatusAccepted, envelope{Success: true, Message: "The update helper puts the previous version back and restarts the stack. This page will reconnect."})
}

// startUpgradeFollowUp runs after a start: it records how an upgrade that
// restarted this server ended, and adopts the installation.
func (s *apiServer) startUpgradeFollowUp() {
	if s.capabilities.Kubernetes || s.capabilities.ImmutableUpdates {
		return
	}
	if up := s.readUpgradeJob(); up != nil && up.helperJob() && helperUpgradePhases[up.Phase] {
		go guard("upgrade result", func() { s.auditUpgradeResult(up.ID) })
	}
	if base := s.orchestratorBaseCommand(); len(base) == 0 || base[0] == "go" || envOff("NCC_UPGRADE_ADOPT") || envOff("NCC_UPGRADE_HELPER") {
		return
	}
	go guard("upgrade adoption", s.adoptInstallation)
}

// auditUpgradeResult waits for the job's end and records it.
func (s *apiServer) auditUpgradeResult(id string) {
	for deadline := time.Now().Add(30 * time.Minute); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
		up := s.readUpgradeJob()
		if up == nil || up.ID != id {
			return
		}
		if up.FinishedAt == "" {
			continue
		}
		ok := up.Phase == "done" || (up.Phase == "rolled_back" && up.Action == "rollback")
		if ok && up.Action == "upgrade" {
			s.updateAppliedTotal.Add(1)
		} else if !ok {
			s.updateFailedTotal.Add(1)
		}
		s.auditEvent("settings.update.result", ok, map[string]interface{}{
			"operation": up.Action, "phase": up.Phase, "from_version": up.FromVersion, "to_version": up.ToVersion, "requested_by": up.RequestedBy,
		})
		return
	}
}

// Waits before adoption; variables for tests.
var (
	adoptFirstWait = 2 * time.Minute
	adoptRetryWait = 5 * time.Minute
)

// adoptInstallation brings the update helper, the service definitions and the
// supervisor up to this version, once nothing else is going on. The
// orchestrator decides what is needed, tries it once per version, and puts
// everything back when the service does not come up.
func (s *apiServer) adoptInstallation() {
	wait := adoptFirstWait
	for i := 0; i < 24; i++ {
		time.Sleep(wait)
		wait = adoptRetryWait
		updateJob.mu.Lock()
		busy := updateJob.inProgress
		updateJob.mu.Unlock()
		if busy || s.hasInFlightRuns() || s.upgradeInProgress() != "" {
			continue
		}
		out, err := s.runOrchestrator([]string{"upgrade-adopt", "--auto", "--install-dir", s.maintenanceInstallDir()}, 4*time.Minute)
		if err != nil {
			serverLog.Info("the installation was not brought up to this version", "reason", redactPaths(strings.TrimPrefix(lastErrorLine(out), "Error: ")))
		}
		return
	}
}
