package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"goncc/internal/plural"
)

// errNoStack: upgrade has nothing to work on.
var errNoStack = errors.New("no NCC stack runs from this folder as a service, or was started with v2-start; use `update --programs-only` to replace the programs only")

// resolveStackDir turns the --install-dir flag into an absolute folder.
func resolveStackDir(flag string) (string, error) {
	dir := strings.TrimSpace(flag)
	if dir == "" {
		dir = defaultV2InstallDir()
	}
	return filepath.Abs(dir)
}

// stackServiceToUpgrade finds how the stack in installDir is run. A supervisor
// that no service manager started cannot be stopped and started again for it.
func stackServiceToUpgrade(installDir string) (stackService, error) {
	if svc := findStackService(installDir); svc != nil {
		return svc, nil
	}
	if st, err := readSupervisorState(installDir); err == nil && processIsAlive(st.PID) {
		return nil, errors.New("the supervisor of this stack was not started by a service manager; install the service (v2-install-service) first, or use `update --programs-only` and restart it yourself")
	}
	m := newManualService(installDir)
	if !m.running() {
		return nil, errNoStack
	}
	return m, nil
}

// newJob starts a progress file for a job.
func newJob(id, action, target, backup, by string) upgradeJob {
	now := nowRFC3339()
	if id == "" {
		id = time.Now().UTC().Format("20060102T150405Z")
	}
	return upgradeJob{ID: id, Action: action, Runner: "stage", TargetVersion: strings.TrimPrefix(target, "v"), BackupPath: backup, RequestedBy: by, RequestedAt: now, StartedAt: now}
}

// jobRunning returns the job that is being worked on, if any.
func jobRunning(installDir string) (*upgradeJob, bool) {
	job, err := readUpgradeJob(installDir)
	if err != nil || (!upgradeActive(job.Phase) && !helperPhaseActive(job.Phase)) {
		return nil, false
	}
	if at, err := time.Parse(time.RFC3339, job.UpdatedAt); err == nil && time.Since(at) > helperStale && !helperUnfinished(installDir) {
		return nil, false
	}
	return job, true
}

// helperStale: a job that was not touched for this long is not being worked on.
const helperStale = 5 * time.Minute

type upgradeOptions struct {
	InstallDir, To, Bundle         string
	Prerelease, SkipBackup, NoWait bool
	ID, BackupPath, RequestedBy    string // from the API server
	// ReplaceEdited replaces definitions edited on this host (Decision 3 keeps
	// them by default).
	ReplaceEdited bool
}

// runStackUpgrade is the `upgrade` command.
func runStackUpgrade(ctx context.Context, o upgradeOptions, out io.Writer) error {
	installDir, err := resolveStackDir(o.InstallDir)
	if err != nil {
		return err
	}
	if o.To != "" && !upgradeTargetPattern.MatchString(o.To) {
		return fmt.Errorf("invalid version %q", o.To)
	}
	svc, err := stackServiceToUpgrade(installDir)
	if err != nil {
		return err
	}
	if err := svc.privileged(); err != nil {
		return err
	}
	if job, busy := jobRunning(installDir); busy {
		return fmt.Errorf("another upgrade is in progress (%s)", job.Phase)
	}
	if o.ReplaceEdited {
		printReplacements(out, svc.plan(false))
	}
	if o.BackupPath == "" && !o.SkipBackup {
		fmt.Fprintln(out, "Taking a pre-update backup ...")
		if o.BackupPath, err = preUpgradeBackup(installDir); err != nil {
			return fmt.Errorf("the pre-update backup failed (nothing was changed): %w; --skip-backup goes on without one", err)
		}
		fmt.Fprintln(out, "Saved", o.BackupPath)
	}
	logf := func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }
	if o.RequestedBy == "" {
		o.RequestedBy = "command line"
		logf = func(string, ...any) {} // the progress is printed below
	}
	log := newJobLog(installDir, logf, newJob(o.ID, helperActionUpgrade, o.To, o.BackupPath, o.RequestedBy))
	s := newStageOne(installDir, svc, log, logf)
	s.bundle, s.prerelease, s.replaceEdited = o.Bundle, o.Prerelease, o.ReplaceEdited
	if o.Bundle != "" {
		if s.bundle, err = filepath.Abs(o.Bundle); err != nil {
			return err
		}
	}
	if o.NoWait {
		return s.run(ctx, o.To)
	}
	id := log.snapshot().ID
	done := make(chan error, 1)
	go func() { done <- s.run(ctx, o.To) }()
	return followJob(ctx, installDir, id, out, done)
}

// printReplacements shows what --replace-edited replaces, before it does.
func printReplacements(out io.Writer, changes []defChange) {
	n := 0
	for _, c := range changes {
		if c.Action != defKeep {
			continue
		}
		n++
		fmt.Fprintf(out, "Replacing %s (%s):\n", c.Title, c.Detail)
		if d := c.Diff(); d != "" {
			for _, l := range strings.Split(strings.TrimRight(d, "\n"), "\n") {
				fmt.Fprintln(out, "  "+l)
			}
		}
	}
	if n == 0 {
		fmt.Fprintln(out, "No definition was edited on this host; --replace-edited changes nothing.")
	}
}

// preUpgradeBackup saves a backup with the programs in it.
func preUpgradeBackup(installDir string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(installDir, "backups")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	name := "pre-update-" + time.Now().UTC().Format("20060102T150405Z") + ".tar.gz"
	args := []string{"v2-backup", "--install-dir", installDir, "--include-stack"}
	for _, env := range []string{"NCC_BACKUP_KEY_FILE", "NCC_BACKUP_KEY", "NCC_BACKUP_PASSPHRASE"} {
		if strings.TrimSpace(os.Getenv(env)) != "" {
			name += ".enc"
			args = append(args, "--encrypt")
			break
		}
	}
	path := filepath.Join(dir, name)
	out, err := runUpgradeCommand(10*time.Minute, self, append(args, "--output-file", path)...)
	if err != nil {
		return "", errors.New(upgradeErrorLine(out, err))
	}
	return path, nil
}

// followJob prints a job's steps until it ends. stage reports the end of stage
// 1, whose failure ends the job too.
func followJob(ctx context.Context, installDir, id string, out io.Writer, stage <-chan error) error {
	printed := 0
	var stageErr error
	staged := stage == nil
	lastLife := time.Now()
	for {
		job, err := readUpgradeJob(installDir)
		if err == nil && job.ID == id {
			for ; printed < len(job.Steps); printed++ {
				fmt.Fprintf(out, "  %-14s %s\n", job.Steps[printed].Phase, job.Steps[printed].Message)
			}
			if at, err := time.Parse(time.RFC3339, job.UpdatedAt); err == nil && at.After(lastLife) {
				lastLife = at
			}
			if job.FinishedAt != "" && staged {
				return jobOutcome(job)
			}
		}
		if staged && stageErr != nil {
			return stageErr
		}
		if staged && time.Since(lastLife) > helperStale {
			return fmt.Errorf("no progress for %s; see %s", shortDuration(helperStale), helperLogPath(installDir))
		}
		if !staged {
			select {
			case stageErr = <-stage:
				staged = true
				continue
			default:
			}
		}
		if !sleepCtx(ctx, 500*time.Millisecond) {
			return errors.New("stopped watching; the upgrade goes on (see `ncc-orchestrator upgrade --status`)")
		}
	}
}

func jobOutcome(job *upgradeJob) error {
	switch {
	case job.Phase == upgDone, job.Phase == upgRolledBack && job.Action == helperActionRollback:
		return nil
	case job.Error != "":
		return errors.New(job.Error)
	}
	return errors.New(job.Message)
}

// runStackRollback puts the version before the last upgrade back.
func runStackRollback(ctx context.Context, installDirFlag, id, by string, wait bool, out io.Writer) error {
	installDir, err := resolveStackDir(installDirFlag)
	if err != nil {
		return err
	}
	info, ok := rollbackAvailable(installDir)
	if !ok {
		return errors.New("there is no upgrade to roll back (the previous version is kept for 24 hours); restore a pre-update backup with v2-restore instead")
	}
	svc, err := stackServiceFor(info.Kind, installDir)
	if err != nil {
		return err
	}
	if err := svc.privileged(); err != nil {
		return err
	}
	if job, busy := jobRunning(installDir); busy {
		return fmt.Errorf("another upgrade is in progress (%s)", job.Phase)
	}
	unlock, err := acquireSupervisorLock(helperFile(installDir, "stage.lock"))
	if err != nil {
		return errors.New("an upgrade is being prepared")
	}
	defer unlock()
	// After `update` or on a host the helper never ran on, it is not there yet.
	if err := ensureHelper(ctx, installDir, svc); err != nil {
		return fmt.Errorf("the update helper could not be installed (nothing was changed): %w", err)
	}
	if by == "" {
		by = "command line"
	}
	job := newJob(id, helperActionRollback, info.From, info.Backup, by)
	job.FromVersion = info.To
	log := newJobLog(installDir, func(string, ...any) {}, job)
	if info.Action == helperActionAdopt {
		log.step(upgHandover, "The update helper puts the service definitions from before v%s back", info.To)
	} else {
		log.step(upgHandover, "The update helper puts v%s back", info.From)
	}
	req := helperRequest{ID: job.ID, Action: helperActionRollback, Kind: info.Kind, From: info.From, Target: info.To,
		BackupPath: info.Backup, RequestedBy: by, RequestedAt: job.RequestedAt, StartedAt: job.StartedAt, Steps: log.snapshot().Steps}
	if err := giveToHelper(installDir, svc, req, time.Minute, 250*time.Millisecond); err != nil {
		log.fail(upgFailed, err, "Not rolled back: %v. Nothing was changed.", err)
		return err
	}
	if !wait {
		return nil
	}
	return followJob(ctx, installDir, job.ID, out, nil)
}

// ensureHelper installs this program as the update helper when none is
// installed, and proves that it works. An installed one stays: it may be
// newer than this program (after a rollback).
func ensureHelper(ctx context.Context, installDir string, svc stackService) error {
	helper := helperBinaryPath(installDir)
	if fileExists(helper) && svc.helperCurrent(helper) {
		return nil
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if err := prepareHelperDir(installDir); err != nil {
		return err
	}
	body, err := os.ReadFile(self)
	if err != nil {
		return err
	}
	version := stripGoBuildGitSuffix(Version)
	undo, err := installHelperProgram(installDir, svc, body, version, runUpgradeCommand)
	if err != nil {
		return err
	}
	if err := probeInstalledHelper(ctx, installDir, svc, version, 90*time.Second, 250*time.Millisecond); err != nil {
		undo()
		return fmt.Errorf("it does not work on this host: %w", err)
	}
	waitForHelperExit(ctx, installDir, 15*time.Second)
	return nil
}

// upgradePlan is what an upgrade would do (`upgrade --plan --json`).
type upgradePlan struct {
	Current       string   `json:"current_version"`
	Target        string   `json:"target_version,omitempty"`
	UpToDate      bool     `json:"up_to_date,omitempty"`
	Service       string   `json:"service,omitempty"`
	Kind          string   `json:"kind,omitempty"`
	Helper        string   `json:"helper"` // ready | missing | off | unavailable
	Problems      []string `json:"problems,omitempty"`
	Definitions   []string `json:"definitions,omitempty"`
	DiskNeeded    uint64   `json:"disk_needed_bytes,omitempty"`
	DiskFree      uint64   `json:"disk_free_bytes,omitempty"`
	RollbackTo    string   `json:"rollback_to,omitempty"`
	RollbackUntil string   `json:"rollback_until,omitempty"`
	// RollbackOf: what a rollback undoes ("upgrade", "adopt" or "update").
	RollbackOf     string `json:"rollback_of,omitempty"`
	ReleaseProblem string `json:"release_problem,omitempty"`

	// Checks: what was checked before anything happens, passed or not.
	Checks []planCheck `json:"checks,omitempty"`
	// Changes: each service definition, with the diff of what changes.
	Changes []planChange `json:"changes,omitempty"`
	// Edited: definitions edited on this host, kept unless ReplaceEdited.
	Edited        int  `json:"edited,omitempty"`
	ReplaceEdited bool `json:"replace_edited,omitempty"`
	// Programs and Config: what is replaced, and what is saved to be put back.
	Programs []string `json:"programs,omitempty"`
	Config   []string `json:"config,omitempty"`
	// Downtime: how long the services are expected to be down.
	Downtime string `json:"downtime,omitempty"`
}

type planCheck struct {
	Title  string `json:"title"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

type planChange struct {
	Title  string `json:"title"`
	Action string `json:"action"`
	Detail string `json:"detail,omitempty"`
	Diff   string `json:"diff,omitempty"`
}

func planUpgrade(installDir, to string, prerelease, online, replace bool) upgradePlan {
	p := upgradePlan{Current: stripGoBuildGitSuffix(Version), Helper: "unavailable", ReplaceEdited: replace, Downtime: defaultDowntime}
	check := func(title string, err error, okDetail string) {
		c := planCheck{Title: title, OK: err == nil, Detail: okDetail}
		if err != nil {
			c.Detail = err.Error()
			p.Problems = append(p.Problems, err.Error())
		}
		p.Checks = append(p.Checks, c)
	}
	svc, err := stackServiceToUpgrade(installDir)
	if svc != nil {
		check("Service", nil, svc.title())
	} else {
		check("Service", err, "")
	}
	if svc != nil {
		p.Kind, p.Service, p.Helper = svc.kind(), svc.title(), "missing"
		if fileExists(helperBinaryPath(installDir)) && svc.helperCurrent(helperBinaryPath(installDir)) {
			p.Helper = "ready"
		}
		if !upgradeHelperEnabled() {
			p.Helper = "off"
		}
		check("Permissions", svc.privileged(), "may stop the service and change its definitions")
		var busy error
		if job, running := jobRunning(installDir); running {
			busy = fmt.Errorf("another upgrade is in progress (%s)", job.Phase)
		}
		check("No other upgrade", busy, "")
		changes := svc.plan(false)
		p.Edited = editedKept(changes)
		if replace {
			changes = replaceEdited(changes)
		}
		for _, c := range changes {
			p.Definitions = append(p.Definitions, c.String())
			p.Changes = append(p.Changes, planChange{Title: c.Title, Action: c.Action, Detail: c.Detail, Diff: c.Diff()})
		}
	}
	need := dirSize(filepath.Join(installDir, "bin")) + dirSize(filepath.Join(installDir, "frontend-dist"))
	p.DiskNeeded = need*4 + 250<<20
	if free, ok := diskFreeBytes(installDir); ok {
		p.DiskFree = free
		var low error
		if free < p.DiskNeeded {
			low = fmt.Errorf("only %s free, about %s needed", humanBytes(free), humanBytes(p.DiskNeeded))
		}
		check("Disk space", low, fmt.Sprintf("%s free, about %s needed", humanBytes(free), humanBytes(p.DiskNeeded)))
	}
	if entries, err := os.ReadDir(filepath.Join(installDir, "bin")); err == nil {
		for _, en := range entries {
			if !en.IsDir() && !strings.HasPrefix(en.Name(), ".") && !strings.Contains(en.Name(), updateAsideMarker) {
				p.Programs = append(p.Programs, en.Name())
			}
		}
	}
	if existingDir(filepath.Join(installDir, "frontend-dist")) {
		p.Programs = append(p.Programs, "web UI (frontend-dist)")
	}
	for _, f := range configFiles(installDir) {
		if rel, err := filepath.Rel(installDir, f); err == nil {
			p.Config = append(p.Config, filepath.ToSlash(rel))
		}
	}
	if info, ok := rollbackAvailable(installDir); ok {
		p.RollbackTo, p.RollbackOf = info.From, info.Action
		if at, err := time.Parse(time.RFC3339, info.At); err == nil {
			p.RollbackUntil = at.Add(helperKeepSnapshot).UTC().Format(time.RFC3339)
		}
	}
	if !online {
		return p
	}
	client := &http.Client{Timeout: 20 * time.Second}
	rel, err := findUpgradeRelease(client, p.Current, to, prerelease)
	switch {
	case errors.Is(err, errUpToDate):
		p.UpToDate = true
	case err != nil:
		p.ReleaseProblem = err.Error()
	default:
		p.Target = strings.TrimPrefix(rel.TagName, "v")
		var m *upgradeManifest
		sums, _, err := fetchReleaseChecksums(client, rel)
		if err == nil {
			m, err = fetchReleaseManifest(client, rel, sums)
		}
		if err == nil && m != nil {
			err = m.allows(p.Current)
			if m.DowntimeHint != "" {
				p.Downtime = m.DowntimeHint
			}
		}
		c := planCheck{Title: "Release v" + p.Target, OK: err == nil, Detail: "signature, checksums and upgrade path"}
		if err != nil {
			p.ReleaseProblem, c.Detail = err.Error(), err.Error()
		}
		p.Checks = append(p.Checks, c)
	}
	return p
}

func (p upgradePlan) print(out io.Writer) {
	fmt.Fprintf(out, "Installed version:  v%s\n", p.Current)
	switch {
	case p.UpToDate:
		fmt.Fprintln(out, "Newest version:     this one")
	case p.Target != "":
		fmt.Fprintf(out, "Upgrade to:         v%s\n", p.Target)
	}
	if p.Service != "" {
		fmt.Fprintf(out, "Runs as:            %s (%s)\n", p.Service, p.Kind)
	}
	fmt.Fprintf(out, "Update helper:      %s\n", p.Helper)
	for _, c := range p.Checks {
		fmt.Fprintf(out, "Check:              %s: %s%s\n", c.Title, ifThen(c.OK, "ok", "FAILED"), ifThen(c.Detail != "", " ("+c.Detail+")", ""))
	}
	for _, c := range p.Changes {
		fmt.Fprintf(out, "Service definition: %s: %s%s\n", c.Title, c.Action, ifThen(c.Detail != "", " ("+c.Detail+")", ""))
		if c.Diff != "" && c.Action != defUnchanged {
			for _, l := range strings.Split(strings.TrimRight(c.Diff, "\n"), "\n") {
				fmt.Fprintf(out, "                      %s\n", l)
			}
		}
	}
	if p.Edited > 0 && !p.ReplaceEdited {
		fmt.Fprintf(out, "Edited on this host: %s kept as they are (upgrade --replace-edited replaces them)\n", plural.Count(p.Edited, "definition"))
	}
	if len(p.Programs) > 0 {
		fmt.Fprintf(out, "Replaced:           %s\n", strings.Join(p.Programs, ", "))
	}
	if len(p.Config) > 0 {
		fmt.Fprintf(out, "Saved for rollback: %s\n", strings.Join(p.Config, ", "))
	}
	fmt.Fprintf(out, "Expected downtime:  %s\n", p.Downtime)
	if p.RollbackTo != "" {
		fmt.Fprintf(out, "Roll back:          to v%s until %s (upgrade --rollback)\n", p.RollbackTo, p.RollbackUntil)
	}
	for _, x := range p.Problems {
		fmt.Fprintf(out, "Problem:            %s\n", x)
	}
	if p.ReleaseProblem != "" {
		fmt.Fprintf(out, "Problem:            %s\n", p.ReleaseProblem)
	}
}

// ---- adoption ----

// adoptRecord notes that this version was adopted, or that it was tried.
type adoptRecord struct {
	Version string `json:"version"`
	At      string `json:"at"`
}

// runUpgradeAdopt brings a stack that an older release installed (or upgraded)
// up to this version: the update helper, the service definitions, and the
// supervisor, which such an upgrade left on the old program.
func runUpgradeAdopt(ctx context.Context, installDir string, auto, checkOnly, replace bool, out io.Writer) error {
	version := stripGoBuildGitSuffix(Version)
	if !upgradeTargetPattern.MatchString(version) {
		return fmt.Errorf("this is a development build (%s)", version)
	}
	svc := findStackService(installDir)
	if svc == nil {
		fmt.Fprintln(out, "No service runs the stack in", installDir+"; nothing to do.")
		return nil
	}
	if err := svc.privileged(); err != nil {
		return err
	}
	if _, busy := jobRunning(installDir); busy || helperUnfinished(installDir) {
		return errors.New("an upgrade is in progress")
	}
	changes := svc.plan(replace)
	pending := pendingChanges(changes)
	oldSupervisor := ""
	if st, err := readSupervisorState(installDir); err == nil && processIsAlive(st.PID) && st.Version != "" && st.Version != version {
		oldSupervisor = st.Version
	}
	if v := programVersion(runUpgradeCommand, svc.orchestratorBin()); v != version {
		return fmt.Errorf("the service starts v%s, not v%s", v, version)
	}
	helper := helperBinaryPath(installDir)
	self, err := os.Executable()
	if err != nil {
		return err
	}
	helperStaleNow := !sameContent(self, helper) || !svc.helperCurrent(helper)
	fmt.Fprintf(out, "Service definitions: %s\n", describeChanges(changes))
	if replace {
		printReplacements(out, svc.plan(false))
	}
	if oldSupervisor != "" {
		fmt.Fprintf(out, "The supervisor still runs v%s\n", oldSupervisor)
	}
	fmt.Fprintf(out, "Update helper: %s\n", ifThen(helperStaleNow, "to be installed", "ready"))
	if checkOnly {
		return nil
	}
	if err := prepareHelperDir(installDir); err != nil {
		return err
	}
	unlock, err := acquireSupervisorLock(helperFile(installDir, "stage.lock"))
	if err != nil {
		return errors.New("an upgrade is being prepared")
	}
	defer unlock()
	if helperStaleNow {
		body, err := os.ReadFile(self)
		if err != nil {
			return err
		}
		undo, err := installHelperProgram(installDir, svc, body, version, runUpgradeCommand)
		if err != nil {
			return fmt.Errorf("the update helper could not be installed: %w", err)
		}
		if err := probeInstalledHelper(ctx, installDir, svc, version, 90*time.Second, 250*time.Millisecond); err != nil {
			undo()
			return fmt.Errorf("the update helper does not work on this host: %w", err)
		}
		fmt.Fprintln(out, "Installed the update helper.")
		waitForHelperExit(ctx, installDir, 15*time.Second)
	}
	if pending == 0 && oldSupervisor == "" {
		return nil
	}
	var rec adoptRecord
	if auto && readHelperJSON(helperAdoptPath(installDir), &rec) == nil && rec.Version == version {
		// It was rolled back: do not restart the stack for it over and over.
		return fmt.Errorf("the service definitions of v%s were tried on %s already; run `ncc-orchestrator upgrade-adopt` to try again", version, rec.At)
	}
	if err := writeHelperJSON(helperAdoptPath(installDir), adoptRecord{Version: version, At: nowRFC3339()}); err != nil {
		return err
	}
	job := newJob("", helperActionAdopt, version, "", "this version's first start")
	if !auto {
		job.RequestedBy = "command line"
	}
	job.FromVersion = version
	log := newJobLog(installDir, func(string, ...any) {}, job)
	log.step(upgHandover, "Bringing the service up to v%s: %s", version, describeChanges(changes))
	req := helperRequest{ID: job.ID, Action: helperActionAdopt, Kind: svc.kind(), From: version, Target: version, ReplaceEdited: replace,
		RequestedBy: job.RequestedBy, RequestedAt: job.RequestedAt, StartedAt: job.StartedAt, Steps: log.snapshot().Steps}
	if err := giveToHelper(installDir, svc, req, time.Minute, 250*time.Millisecond); err != nil {
		log.fail(upgFailed, err, "The service was not updated: %v. Nothing was changed.", err)
		return err
	}
	fmt.Fprintln(out, "The update helper restarts the service now.")
	if auto {
		return nil
	}
	return followJob(ctx, installDir, job.ID, out, nil)
}

// sameContent reports whether two files have the same content.
func sameContent(a, b string) bool {
	x, err1 := sha256File(a)
	y, err2 := sha256File(b)
	return err1 == nil && err2 == nil && x == y
}

// installHelperForService installs the helper together with the service, so the
// first upgrade does not have to.
func installHelperForService(o installServiceOptions) error {
	if !upgradeHelperEnabled() {
		return nil
	}
	svc := findStackService(o.InstallDir)
	if svc == nil {
		return errors.New("the service was not found")
	}
	if err := prepareHelperDir(o.InstallDir); err != nil {
		return err
	}
	body, err := os.ReadFile(o.OrchestratorBin)
	if err != nil {
		return err
	}
	_, err = installHelperProgram(o.InstallDir, svc, body, stripGoBuildGitSuffix(Version), runUpgradeCommand)
	return err
}

// removeHelperForService removes the helper with the service.
func removeHelperForService(installDir string) {
	if svc := findStackService(installDir); svc != nil {
		svc.removeHelper()
	}
	_ = os.RemoveAll(filepath.Dir(helperBinaryPath(installDir)))
}

// ---- Kubernetes ----

// runUpgradeCheck (Helm hooks) compares the versions the API and UI report.
func runUpgradeCheck(ctx context.Context, api, ui, expect string, timeout time.Duration, out io.Writer) error {
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		// A new connection each time: a Service then picks any of the replicas.
		DisableKeepAlives: true,
		// Only a version is read. The UI's certificate is often self-signed.
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}, // #nosec G402 -- see above; nothing secret is sent
	}}
	for deadline, good := time.Now().Add(timeout), 0; ; {
		err := checkStackVersions(ctx, client, api, ui, expect, out)
		if err == nil {
			if good++; good == upgradeCheckInARow {
				return nil
			}
		} else {
			good = 0
			if time.Now().After(deadline) {
				return err
			}
			fmt.Fprintln(out, "waiting:", err)
		}
		if !sleepCtx(ctx, upgradeCheckEvery) {
			return ctx.Err()
		}
	}
}

// During a rollout old and new replicas answer side by side.
const upgradeCheckInARow = 5

var upgradeCheckEvery = 2 * time.Second

func checkStackVersions(ctx context.Context, client *http.Client, api, ui, expect string, out io.Writer) error {
	got, err := healthVersion(ctx, client, strings.TrimRight(api, "/")+"/api/v1/health")
	if err != nil {
		return fmt.Errorf("API: %w", err)
	}
	if expect != "" && got != strings.TrimPrefix(expect, "v") {
		return fmt.Errorf("the API runs v%s, not v%s", got, strings.TrimPrefix(expect, "v"))
	}
	fmt.Fprintf(out, "API: v%s, healthy\n", got)
	if ui == "" {
		return nil
	}
	// Through the UI, so its proxy to the API is checked too.
	via, err := healthVersion(ctx, client, strings.TrimRight(ui, "/")+"/api/v1/health")
	if err != nil {
		return fmt.Errorf("UI: %w", err)
	}
	if via != got {
		return fmt.Errorf("the UI reaches an API of v%s, not v%s", via, got)
	}
	fmt.Fprintln(out, "UI: healthy")
	return nil
}

func healthVersion(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := readBounded(resp.Body, 1<<20)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var v struct {
		Success bool `json:"success"`
		Data    struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &v); err != nil || !v.Success {
		return "", errors.New("it does not report a healthy API")
	}
	return stripGoBuildGitSuffix(strings.TrimPrefix(v.Data.Version, "v")), nil
}

// ---- commands ----

func newUpgradeCommands() []*cobra.Command {
	upgrade := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade the whole stack, or roll the last upgrade back",
		Long: `Upgrades the stack that runs from this install folder: programs, web UI and
service definitions.

A pre-update backup is taken, the release is downloaded and verified, and the
new version's update helper is installed and tested. Only then does the helper
stop the service, replace everything and start the new version. If it does not
come up healthy, the helper puts the previous version back by itself, also after
a crash or a reboot in between.

The previous version is kept for 24 hours: upgrade --rollback returns to it.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			get := func(n string) string { v, _ := cmd.Flags().GetString(n); return strings.TrimSpace(v) }
			is := func(n string) bool { v, _ := cmd.Flags().GetBool(n); return v }
			dir, err := resolveStackDir(get("install-dir"))
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			switch {
			case is("plan"):
				p := planUpgrade(dir, get("to"), is("include-prerelease"), !is("offline"), is("replace-edited"))
				if is("json") {
					return json.NewEncoder(out).Encode(p)
				}
				p.print(out)
				return nil
			case is("status"):
				job, err := readUpgradeJob(dir)
				if os.IsNotExist(err) {
					fmt.Fprintln(out, "No upgrade has run here.")
					return nil
				} else if err != nil {
					return err
				}
				if is("json") {
					return json.NewEncoder(out).Encode(job)
				}
				for _, s := range job.Steps {
					fmt.Fprintf(out, "%s  %-14s %s\n", s.At, s.Phase, s.Message)
				}
				return nil
			case is("rollback"):
				return runStackRollback(cmd.Context(), dir, get("id"), get("requested-by"), !is("no-wait"), out)
			}
			return runStackUpgrade(cmd.Context(), upgradeOptions{
				InstallDir: dir, To: get("to"), Bundle: get("bundle"),
				Prerelease: is("include-prerelease"), SkipBackup: is("skip-backup"), NoWait: is("no-wait"),
				ID: get("id"), BackupPath: get("backup-path"), RequestedBy: get("requested-by"), ReplaceEdited: is("replace-edited"),
			}, out)
		},
	}
	f := upgrade.Flags()
	f.String("install-dir", "", "Install folder (default: auto-detect, fallback .ncc-v2)")
	f.String("to", "", "Version to install (default: the newest of this major version)")
	f.String("bundle", "", "Install a downloaded release archive; checksums.txt (and checksums.txt.sig) lie beside it")
	f.Bool("include-prerelease", false, "Also consider release candidates (an install that runs one always does)")
	f.Bool("plan", false, "Show what an upgrade would do, and change nothing")
	f.Bool("status", false, "Show the current or last upgrade")
	f.Bool("rollback", false, "Put the version before the last upgrade back")
	f.Bool("skip-backup", false, "Do not take a pre-update backup")
	f.Bool("no-wait", false, "Return when the update helper has taken over")
	f.Bool("offline", false, "With --plan: do not look for a newer release")
	f.Bool("json", false, "With --plan or --status: machine-readable output")
	f.Bool("replace-edited", false, "Replace service definitions edited on this host instead of keeping them (shows the diff first; a rollback puts them back)")
	for _, n := range []string{"id", "backup-path", "requested-by"} {
		f.String(n, "", "Set by the API server")
		_ = f.MarkHidden(n)
	}

	helper := &cobra.Command{
		Use: "upgrade-helper", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "Carry out a requested upgrade (started by the service manager)",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _ := cmd.Flags().GetString("install-dir")
			if strings.TrimSpace(dir) == "" {
				return errors.New("--install-dir is required")
			}
			return runUpgradeHelper(dir)
		},
	}
	helper.Flags().String("install-dir", "", "Install folder")

	adopt := &cobra.Command{
		Use: "upgrade-adopt", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "Bring the service definitions and the update helper up to this version",
		RunE: func(cmd *cobra.Command, args []string) error {
			flag, _ := cmd.Flags().GetString("install-dir")
			dir, err := resolveStackDir(flag)
			if err != nil {
				return err
			}
			auto, _ := cmd.Flags().GetBool("auto")
			check, _ := cmd.Flags().GetBool("check")
			replace, _ := cmd.Flags().GetBool("replace-edited")
			return runUpgradeAdopt(cmd.Context(), dir, auto, check, replace, cmd.OutOrStdout())
		},
	}
	adopt.Flags().String("install-dir", "", "Install folder (default: auto-detect)")
	adopt.Flags().Bool("auto", false, "Try once per version, and do not wait")
	adopt.Flags().Bool("replace-edited", false, "Replace service definitions edited on this host (shows the diff first)")
	adopt.Flags().Bool("check", false, "Only show what would change")

	check := &cobra.Command{
		Use: "upgrade-check", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "Check that a deployment's API and UI run the expected version (Helm hooks)",
		RunE: func(cmd *cobra.Command, args []string) error {
			get := func(n string) string { v, _ := cmd.Flags().GetString(n); return strings.TrimSpace(v) }
			timeout, _ := cmd.Flags().GetDuration("timeout")
			if get("api") == "" {
				return errors.New("--api is required")
			}
			return runUpgradeCheck(cmd.Context(), get("api"), get("ui"), get("expect"), timeout, cmd.OutOrStdout())
		},
	}
	check.Flags().String("api", "", "Base URL of the API")
	check.Flags().String("ui", "", "Base URL of the UI (optional)")
	check.Flags().String("expect", "", "Expected version (optional)")
	check.Flags().Duration("timeout", 3*time.Minute, "How long to wait")

	return append([]*cobra.Command{upgrade, helper, adopt, check}, newK8sCheckCommands()...)
}
