package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Supervised upgrades. The API server takes the pre-update backup and then
// hands the upgrade to the supervisor by writing
// run/v2-supervisor-upgrade.request. The supervisor is the one process that
// outlives the API and UI restarts, so it can see the upgrade through:
//
//	preflight -> snapshot -> installing -> restarting -> verifying -> done
//	                                          \-> rolling_back -> rolled_back
//
// snapshot copies the current binaries and frontend aside; installing runs
// `update`; restarting restarts the API and then the UI, each of which must
// start, pass its health probe and stay up before the next step; verifying
// checks that all components report the same new version and runs the safe
// doctor fixes. If a server does not come back healthy the snapshot is put
// back and the servers are restarted on the previous version.
//
// Progress is written to run/v2-upgrade-state.json after every step, so the
// UI can follow it across the API restart and a supervisor that is itself
// restarted mid-upgrade (for example by systemd) resumes where it left off.

const (
	upgradeRequestFile = "v2-supervisor-upgrade.request"
	upgradeStateFile   = "v2-upgrade-state.json"
	upgradeSnapshotDir = "upgrade-rollback"
)

const (
	upgPreflight   = "preflight"
	upgSnapshot    = "snapshot"
	upgInstalling  = "installing"
	upgRestarting  = "restarting"
	upgVerifying   = "verifying"
	upgDone        = "done"
	upgRollingBack = "rolling_back"
	upgRolledBack  = "rolled_back"
	upgFailed      = "failed"
)

// upgradeActive reports whether a job in this phase is still running.
func upgradeActive(phase string) bool {
	switch phase {
	case upgPreflight, upgSnapshot, upgInstalling, upgRestarting, upgVerifying, upgRollingBack:
		return true
	}
	return false
}

func upgradeRequestPath(installDir string) string {
	return filepath.Join(installDir, "run", upgradeRequestFile)
}

func upgradeStatePath(installDir string) string {
	return filepath.Join(installDir, "run", upgradeStateFile)
}

var upgradeTargetPattern = regexp.MustCompile(`^v?[0-9]+(\.[0-9]+){0,2}([-+][0-9A-Za-z.\-]+)?$`)

// upgradeRequest is what the API server writes.
type upgradeRequest struct {
	ID            string `json:"id"`
	TargetVersion string `json:"target_version,omitempty"`
	BackupPath    string `json:"backup_path,omitempty"`
	RequestedBy   string `json:"requested_by,omitempty"`
	RequestedAt   string `json:"requested_at,omitempty"`
}

type upgradeStep struct {
	At      string `json:"at"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
}

// upgradeJob is the persisted state of the current or last upgrade.
type upgradeJob struct {
	ID            string             `json:"id"`
	Phase         string             `json:"phase"`
	Message       string             `json:"message"`
	Error         string             `json:"error,omitempty"`
	TargetVersion string             `json:"target_version,omitempty"`
	FromVersion   string             `json:"from_version,omitempty"`
	ToVersion     string             `json:"to_version,omitempty"`
	BackupPath    string             `json:"backup_path,omitempty"`
	RequestedBy   string             `json:"requested_by,omitempty"`
	RequestedAt   string             `json:"requested_at,omitempty"`
	StartedAt     string             `json:"started_at"`
	UpdatedAt     string             `json:"updated_at"`
	FinishedAt    string             `json:"finished_at,omitempty"`
	Resumed       bool               `json:"resumed,omitempty"`
	Steps         []upgradeStep      `json:"steps"`
	Doctor        *upgradeDoctorNote `json:"doctor,omitempty"`
	// SupervisorRestartPending: the supervisor itself still runs the old
	// binary; it switches at the next service restart.
	SupervisorRestartPending bool `json:"supervisor_restart_pending,omitempty"`
}

// upgradeDoctorNote summarises the doctor run after a successful upgrade.
type upgradeDoctorNote struct {
	Summary map[string]int `json:"summary,omitempty"`
	Fixed   []string       `json:"fixed,omitempty"`
	Failing []string       `json:"failing,omitempty"`
	Error   string         `json:"error,omitempty"`
}

func readUpgradeJob(installDir string) (*upgradeJob, error) {
	b, err := os.ReadFile(upgradeStatePath(installDir))
	if err != nil {
		return nil, err
	}
	var j upgradeJob
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// upgrader runs one supervised upgrade at a time. Its OS-facing parts are
// fields so tests can drive it without a real stack.
type upgrader struct {
	installDir string
	selfPath   string
	children   []string // restart order: api, then ui
	childBin   map[string]string

	runCmd     func(timeout time.Duration, name string, args ...string) (string, error)
	childState func(name string) (superviseChildState, bool)
	restart    func(name string)
	logf       func(string, ...any)

	pauseWatcher, resumeWatcher func()

	poll          time.Duration
	verifyWindow  time.Duration // a restarted server must stay up this long
	verifyTimeout time.Duration // ... and get there within this

	mu  sync.Mutex
	job *upgradeJob
}

func newUpgrader(cfg *superviseConfig, logf func(string, ...any)) *upgrader {
	self, _ := os.Executable()
	u := &upgrader{
		installDir: cfg.installDir,
		selfPath:   self,
		childBin:   map[string]string{},
		logf:       logf,
		poll:       cfg.requestPoll,
		// Long enough for at least one health probe after the start grace.
		verifyWindow:  maxDuration(45*time.Second, cfg.probeInterval+15*time.Second),
		verifyTimeout: maxDuration(3*time.Minute, cfg.startGrace+cfg.probeInterval*time.Duration(cfg.unhealthyThreshold+2)),
		childState:    cfg.state.child,
		restart: func(name string) {
			_ = os.WriteFile(supervisorRestartRequestPath(cfg.installDir, name), []byte(nowRFC3339()+"\n"), 0o644)
		},
		runCmd: runUpgradeCommand,
	}
	for _, name := range []string{"api", "ui"} {
		for _, c := range cfg.children {
			if c.name == name {
				u.children = append(u.children, name)
				u.childBin[name] = c.bin
			}
		}
	}
	u.pauseWatcher, u.resumeWatcher = systemdUpdateWatcherToggles()
	return u
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// runUpgradeCommand runs a step of the upgrade. It is deliberately not tied
// to the supervisor's context: a stop signal must not kill `update` halfway
// through replacing files.
func runUpgradeCommand(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), err
}

// systemdUpdateWatcherToggles pauses the Linux apply-update watcher during a
// supervised upgrade: it restarts the whole service a few seconds after the
// binaries change, which would cut the install and the health checks short.
func systemdUpdateWatcherToggles() (pause, resume func()) {
	noop := func() {}
	if runtime.GOOS != "linux" {
		return noop, noop
	}
	unit := applyUpdateWatcherName(supervisorServiceName) + ".path"
	if _, err := os.Stat(filepath.Join(systemdUnitDir, unit)); err != nil {
		return noop, noop
	}
	return func() { _, _ = runUpgradeCommand(15*time.Second, "systemctl", "stop", unit) },
		func() { _, _ = runUpgradeCommand(15*time.Second, "systemctl", "start", unit) }
}

// save writes the job atomically; the caller holds no lock.
func (u *upgrader) save() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.job == nil {
		return
	}
	u.job.UpdatedAt = nowRFC3339()
	b, err := json.MarshalIndent(u.job, "", "  ")
	if err != nil {
		return
	}
	path := upgradeStatePath(u.installDir)
	tmp := path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, path)
	}
}

// step moves the job to phase and records it.
func (u *upgrader) step(phase, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	u.mu.Lock()
	u.job.Phase = phase
	u.job.Message = msg
	u.job.Steps = append(u.job.Steps, upgradeStep{At: nowRFC3339(), Phase: phase, Message: msg})
	if !upgradeActive(phase) {
		u.job.FinishedAt = nowRFC3339()
	}
	u.mu.Unlock()
	u.logf("upgrade %s: %s", phase, msg)
	u.save()
}

// loop resumes an interrupted upgrade, then takes new requests until ctx ends.
func (u *upgrader) loop(ctx context.Context) {
	if j, err := readUpgradeJob(u.installDir); err == nil && upgradeActive(j.Phase) {
		u.job = j
		u.resume(ctx)
	}
	for ctx.Err() == nil {
		if req, ok := u.takeRequest(); ok {
			u.run(ctx, req)
		}
		if !sleepCtx(ctx, u.poll) {
			return
		}
	}
}

// takeRequest consumes a pending request file, if any.
func (u *upgrader) takeRequest() (upgradeRequest, bool) {
	path := upgradeRequestPath(u.installDir)
	b, err := os.ReadFile(path)
	if err != nil {
		return upgradeRequest{}, false
	}
	_ = os.Remove(path)
	var req upgradeRequest
	if json.Unmarshal(b, &req) != nil {
		u.logf("upgrade: ignoring an unreadable request")
		return upgradeRequest{}, false
	}
	req.TargetVersion = strings.TrimSpace(req.TargetVersion)
	if req.TargetVersion != "" && !upgradeTargetPattern.MatchString(req.TargetVersion) {
		u.logf("upgrade: ignoring request with invalid target version %q", req.TargetVersion)
		return upgradeRequest{}, false
	}
	if strings.TrimSpace(req.ID) == "" {
		req.ID = time.Now().UTC().Format("20060102T150405Z")
	}
	return req, true
}

// run carries one upgrade from request to done, rolled back or failed.
func (u *upgrader) run(ctx context.Context, req upgradeRequest) {
	u.mu.Lock()
	u.job = &upgradeJob{
		ID: req.ID, TargetVersion: req.TargetVersion, BackupPath: req.BackupPath,
		RequestedBy: req.RequestedBy, RequestedAt: req.RequestedAt, StartedAt: nowRFC3339(),
	}
	u.mu.Unlock()
	u.job.FromVersion = u.binVersion(u.childBin["api"])

	u.step(upgPreflight, "Checking free disk space")
	if err := u.preflight(); err != nil {
		u.step(upgFailed, "Not started: %v. Nothing was changed.", err)
		u.setError(err.Error())
		return
	}
	u.step(upgSnapshot, "Saving the current version so it can be put back")
	if err := u.snapshot(); err != nil {
		u.step(upgFailed, "Not started: could not save the current version (%v). Nothing was changed.", err)
		u.setError(err.Error())
		u.removeSnapshot()
		return
	}

	u.pauseWatcher()
	defer u.resumeWatcher()

	target := "the latest release"
	if req.TargetVersion != "" {
		target = "v" + strings.TrimPrefix(req.TargetVersion, "v")
	}
	u.step(upgInstalling, "Downloading and installing %s (checksum-verified)", target)
	args := []string{"update"}
	if req.TargetVersion != "" {
		args = append(args, "--target-version", req.TargetVersion)
	}
	out, err := u.runCmd(12*time.Minute, u.selfPath, args...)
	if err != nil {
		// update rolls its own file changes back when it fails.
		u.setError(upgradeErrorLine(out, err))
		u.step(upgFailed, "The update could not be installed; the current version keeps running: %s", upgradeErrorLine(out, err))
		u.removeSnapshot()
		return
	}
	if strings.Contains(out, "already on the latest version") || strings.Contains(out, "No upgrade needed") {
		u.step(upgDone, "Already on the latest version; nothing to change")
		u.removeSnapshot()
		return
	}
	u.mu.Lock()
	u.job.ToVersion = u.binVersion(u.childBin["api"])
	u.mu.Unlock()
	u.save()

	u.restartAndVerify(ctx, time.Now())
}

// restartAndVerify restarts the servers one by one on the new version and
// finishes the job; since is when the restarts were asked for.
func (u *upgrader) restartAndVerify(ctx context.Context, since time.Time) {
	for _, name := range u.children {
		u.step(upgRestarting, "Restarting the %s on the new version", childTitle(name))
		started := time.Now()
		if started.Before(since) {
			started = since
		}
		u.restart(name)
		if err := u.awaitHealthy(ctx, name, started); err != nil {
			if ctx.Err() != nil {
				return // supervisor stopping; resumed on the next start
			}
			u.rollback(ctx, fmt.Sprintf("the %s did not come back healthy: %v", childTitle(name), err))
			return
		}
	}
	u.step(upgVerifying, "Checking that every component runs the same version")
	if err := u.versionsMatch(); err != nil {
		u.rollback(ctx, err.Error())
		return
	}
	u.step(upgVerifying, "Running the safe self-heal checks for the new version")
	u.postUpgradeDoctor()
	u.removeSnapshot()
	u.mu.Lock()
	u.job.SupervisorRestartPending = stripGoBuildGitSuffix(Version) != stripGoBuildGitSuffix(u.job.ToVersion) && u.job.ToVersion != ""
	u.mu.Unlock()
	msg := "Upgrade complete"
	if u.job.ToVersion != "" {
		msg = fmt.Sprintf("Upgraded from v%s to v%s; the API and UI are healthy", strings.TrimPrefix(u.job.FromVersion, "v"), strings.TrimPrefix(u.job.ToVersion, "v"))
	}
	u.step(upgDone, "%s", msg)
}

// resume continues a job the previous supervisor did not finish.
func (u *upgrader) resume(ctx context.Context) {
	u.mu.Lock()
	u.job.Resumed = true
	phase := u.job.Phase
	u.mu.Unlock()
	u.logf("upgrade: resuming job %s from phase %s", u.job.ID, phase)
	switch phase {
	case upgPreflight, upgSnapshot:
		u.step(upgFailed, "Interrupted before anything was installed; nothing was changed. Start the update again.")
		u.removeSnapshot()
	case upgRollingBack:
		u.rollback(ctx, u.job.Error)
	default:
		// The servers were started fresh by this supervisor, on whatever is
		// installed now; an install cut short shows up as mixed versions.
		u.pauseWatcher()
		defer u.resumeWatcher()
		u.step(upgRestarting, "The supervisor restarted during the upgrade; checking the servers on the installed version")
		u.restartAndVerify(ctx, time.Now())
	}
}

func (u *upgrader) setError(msg string) {
	u.mu.Lock()
	u.job.Error = msg
	u.mu.Unlock()
	u.save()
}

func childTitle(name string) string {
	switch name {
	case "api":
		return "API server"
	case "ui":
		return "web UI server"
	}
	return name
}

// preflight refuses to start without room for the snapshot and the download.
func (u *upgrader) preflight() error {
	need := dirSize(filepath.Join(u.installDir, "bin")) + dirSize(filepath.Join(u.installDir, "frontend-dist"))
	need = need*3 + 200<<20
	if free, ok := diskFreeBytes(u.installDir); ok && free < need {
		return fmt.Errorf("only %s free on %s, need about %s", humanBytes(free), u.installDir, humanBytes(need))
	}
	return nil
}

func dirSize(dir string) uint64 {
	var n uint64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n += uint64(info.Size())
		}
		return nil
	})
	return n
}

func (u *upgrader) snapshotDir() string {
	return filepath.Join(u.installDir, "run", upgradeSnapshotDir)
}

// snapshot copies bin/, frontend-dist/ and a self binary outside bin/.
func (u *upgrader) snapshot() error {
	snap := u.snapshotDir()
	_ = os.RemoveAll(snap)
	if err := os.MkdirAll(filepath.Join(snap, "bin"), 0o755); err != nil {
		return err
	}
	binDir := filepath.Join(u.installDir, "bin")
	entries, _ := os.ReadDir(binDir)
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if err := copyFilePlain(filepath.Join(binDir, e.Name()), filepath.Join(snap, "bin", e.Name()), 0o755); err != nil {
			return err
		}
	}
	if front := filepath.Join(u.installDir, "frontend-dist"); existingDir(front) {
		if err := copyDir(front, filepath.Join(snap, "frontend-dist")); err != nil {
			return err
		}
	}
	if u.selfPath != "" && !sameDir(filepath.Dir(u.selfPath), binDir) {
		if err := os.MkdirAll(filepath.Join(snap, "self"), 0o755); err != nil {
			return err
		}
		if err := copyFilePlain(u.selfPath, filepath.Join(snap, "self", filepath.Base(u.selfPath)), 0o755); err != nil {
			return err
		}
	}
	return nil
}

func (u *upgrader) removeSnapshot() { _ = os.RemoveAll(u.snapshotDir()) }

// rollback puts the snapshot back and restarts the servers on it.
func (u *upgrader) rollback(ctx context.Context, reason string) {
	u.setError(reason)
	snap := u.snapshotDir()
	if !existingDir(filepath.Join(snap, "bin")) {
		u.step(upgFailed, "%s. No saved copy of the previous version was found; restore the pre-update backup %s from Updates & backups → Backups.", reason, filepath.Base(u.job.BackupPath))
		return
	}
	u.step(upgRollingBack, "%s. Putting the previous version back", reason)
	if err := installStackComponents(snap, u.installDir); err != nil {
		u.step(upgFailed, "%s. Putting the previous version back failed (%v); restore the pre-update backup %s.", reason, err, filepath.Base(u.job.BackupPath))
		return
	}
	if selfCopy := filepath.Join(snap, "self", filepath.Base(u.selfPath)); fileExists(selfCopy) {
		if body, err := os.ReadFile(selfCopy); err == nil {
			_ = replaceRunningExecutable(u.selfPath, body)
		}
	}
	healthy := true
	for _, name := range u.children {
		started := time.Now()
		u.restart(name)
		if err := u.awaitHealthy(ctx, name, started); err != nil {
			if ctx.Err() != nil {
				return
			}
			healthy = false
			u.logf("upgrade: %s not healthy after rollback: %v", name, err)
		}
	}
	u.removeSnapshot()
	if !healthy {
		u.step(upgFailed, "%s. The previous version was put back but does not start cleanly; see Status → Supervisor, or restore the pre-update backup %s.", reason, filepath.Base(u.job.BackupPath))
		return
	}
	u.step(upgRolledBack, "%s. The previous version (v%s) was put back and is running.", reason, strings.TrimPrefix(u.job.FromVersion, "v"))
}

// awaitHealthy waits for a server started at or after since to be running,
// to pass its health probe, and to stay up for verifyWindow.
func (u *upgrader) awaitHealthy(ctx context.Context, name string, since time.Time) error {
	since = since.Truncate(time.Second)
	deadline := time.Now().Add(u.verifyTimeout)
	seenExits := map[string]bool{}
	var lastExit string
	for {
		if st, ok := u.childState(name); ok {
			started, serr := time.Parse(time.RFC3339, st.StartedAt)
			if exitAt, err := time.Parse(time.RFC3339, st.LastExitAt); err == nil && !exitAt.Before(since) && st.LastExitReason != "requested" {
				key := st.LastExitAt + st.LastExitReason
				if !seenExits[key] {
					seenExits[key] = true
					lastExit = st.LastExitReason
					if st.LastExitDetail != "" {
						lastExit += " (" + st.LastExitDetail + ")"
					}
					if line := exitSummaryLine(st.LastExitLogTail); line != "" {
						lastExit += ": " + line
					}
				}
			}
			switch {
			case st.State == "cooldown":
				return fmt.Errorf("it kept stopping (%s)", lastExit)
			case len(seenExits) >= 2:
				return fmt.Errorf("it stopped %d times (%s)", len(seenExits), lastExit)
			case st.State == "running" && serr == nil && !started.Before(since) && time.Since(started) >= u.verifyWindow:
				probeOK := !st.Probed
				if st.LastProbeOK != nil && *st.LastProbeOK {
					if at, err := time.Parse(time.RFC3339, st.LastProbeAt); err == nil && !at.Before(started) {
						probeOK = true
					}
				}
				if probeOK {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			if lastExit != "" {
				return fmt.Errorf("not healthy after %s (last stop: %s)", shortDuration(u.verifyTimeout), lastExit)
			}
			return fmt.Errorf("not healthy after %s", shortDuration(u.verifyTimeout))
		}
		if !sleepCtx(ctx, u.poll) {
			return ctx.Err()
		}
	}
}

var versionLine = regexp.MustCompile(`(?im)^\s*version:\s*(\S+)`)

// binVersion asks a stack binary for its version ("" when unknown).
func (u *upgrader) binVersion(bin string) string {
	if strings.TrimSpace(bin) == "" {
		return ""
	}
	out, err := u.runCmd(15*time.Second, bin, "version")
	if err != nil {
		return ""
	}
	if m := versionLine.FindStringSubmatch(out); m != nil {
		return stripGoBuildGitSuffix(strings.TrimPrefix(m[1], "v"))
	}
	return ""
}

// versionsMatch checks the orchestrator, API and UI on disk agree.
func (u *upgrader) versionsMatch() error {
	seen := map[string]string{}
	bins := map[string]string{"orchestrator": u.selfPath}
	for name, bin := range u.childBin {
		bins[childTitle(name)] = bin
	}
	var parts []string
	for title, bin := range bins {
		v := u.binVersion(bin)
		if v == "" {
			continue
		}
		seen[v] = title
		parts = append(parts, title+" v"+v)
	}
	if len(seen) > 1 {
		return fmt.Errorf("the installed components report different versions (%s)", strings.Join(parts, ", "))
	}
	return nil
}

// postUpgradeDoctor runs the non-disruptive doctor checks with fixes, so a
// new release's checks (service definition, config schema, ...) apply at once.
func (u *upgrader) postUpgradeDoctor() {
	note := &upgradeDoctorNote{}
	out, err := u.runCmd(3*time.Minute, u.selfPath, "doctor", "--json", "--fix", "--no-disruptive", "--install-dir", u.installDir)
	var rep healReport
	if jerr := json.Unmarshal([]byte(jsonObject(out)), &rep); jerr != nil {
		note.Error = upgradeErrorLine(out, err)
	} else {
		note.Summary = rep.Summary
		for _, r := range rep.Results {
			if r.Fixed {
				note.Fixed = append(note.Fixed, r.Title)
			}
			if r.Status == healFail {
				note.Failing = append(note.Failing, r.Title)
			}
		}
	}
	u.mu.Lock()
	u.job.Doctor = note
	u.mu.Unlock()
	u.save()
}

// jsonObject trims anything printed around a JSON object.
func jsonObject(s string) string {
	start, end := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if start < 0 || end < start {
		return ""
	}
	return s[start : end+1]
}

// upgradeErrorLine picks the line of command output that explains a failure.
func upgradeErrorLine(out string, err error) string {
	lines := strings.Split(strings.ReplaceAll(out, "\r", ""), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		low := strings.ToLower(l)
		if l != "" && (strings.Contains(low, "error") || strings.Contains(low, "failed") || strings.Contains(low, "denied")) {
			return l
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if l := strings.TrimSpace(lines[i]); l != "" {
			return l
		}
	}
	if err != nil {
		return err.Error()
	}
	return "unknown error"
}
