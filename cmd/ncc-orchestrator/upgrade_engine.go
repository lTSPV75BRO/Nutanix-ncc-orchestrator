package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/plural"
)

// helperEngine is stage 2 of an upgrade, run by the new version's helper from
// outside the service:
//
//	staging -> saving -> stopping -> swapping -> migrating -> starting -> checking -> done
//	                        \__________\___________\____________\___________\-> restoring -> rolled_back
//
// The journal is written before each step, so a helper that is started again
// after a crash or a reboot knows what may already have happened. Until
// "stopping" nothing of the stack has changed. From there on every failure puts
// the snapshot back: programs, frontend, service definitions and configuration.
type helperEngine struct {
	installDir string
	version    string // of this helper
	svc        stackService
	log        *jobLog
	logf       func(string, ...any)

	runCmd func(timeout time.Duration, name string, args ...string) (string, error)
	check  func(ctx context.Context, since time.Time, version string) error
	doctor func() *upgradeDoctorNote
	// entered is called after a step was written to the journal (tests).
	entered func(step string)

	j *helperJournal
	// switched: when this process started the stack (monotonic).
	switched time.Time
}

// rollbackInfo describes the kept snapshot (run/upgrade/rollback.json).
type rollbackInfo struct {
	From     string   `json:"from"` // what a rollback brings back
	To       string   `json:"to"`
	Kind     string   `json:"kind"`
	At       string   `json:"at"`
	Action   string   `json:"action"`
	NewFiles []string `json:"new_files,omitempty"`
	Backup   string   `json:"backup,omitempty"`
	// Config: the configuration files as the upgrade left them. A manual
	// rollback keeps a file that was changed since.
	Config map[string]string `json:"config,omitempty"`
}

func helperRollbackPath(installDir string) string { return helperFile(installDir, "rollback.json") }
func helperSnapshotSums(installDir string) string { return helperFile(installDir, "snapshot.json") }
func helperAcceptedPath(installDir string) string { return helperFile(installDir, "accepted.json") }

// rollbackAvailable returns the upgrade that can still be rolled back by hand.
func rollbackAvailable(installDir string) (rollbackInfo, bool) {
	var info rollbackInfo
	if readHelperJSON(helperRollbackPath(installDir), &info) != nil {
		return info, false
	}
	at, err := time.Parse(time.RFC3339, info.At)
	if err != nil || time.Since(at) > helperKeepSnapshot || !existingDir(helperSnapshotDir(installDir)) {
		return info, false
	}
	return info, true
}

// maxHelperAttempts: how often one job is started again after a crash.
const maxHelperAttempts = 3

// enter writes step to the journal and the progress list.
func (e *helperEngine) enter(step, format string, a ...any) error {
	e.j.Step = step
	if err := e.saveJournal(); err != nil {
		return fmt.Errorf("cannot write the upgrade journal: %w", err)
	}
	e.log.step(step, format, a...)
	if e.entered != nil {
		e.entered(step)
	}
	return nil
}

func (e *helperEngine) saveJournal() error {
	return writeHelperJSON(helperJournalPath(e.installDir), e.j)
}

// finish removes the journal: nothing is left to do for this job.
func (e *helperEngine) finish() {
	if err := removeWithRetry(helperJournalPath(e.installDir)); err != nil && !os.IsNotExist(err) {
		e.logf("upgrade: remove the journal: %v", err)
	}
	_ = os.RemoveAll(helperStageDir(e.installDir))
	syncDir(helperDir(e.installDir))
}

// run carries the job out from the step in the journal.
func (e *helperEngine) run(ctx context.Context) {
	stop := e.log.keepAlive(15 * time.Second)
	defer stop()
	switch e.j.Step {
	case "":
		e.begin(ctx)
	case upgStaging, upgSaving:
		// Nothing has changed yet: start over.
		e.resumed()
		if e.j.Attempts > maxHelperAttempts {
			e.unchanged(fmt.Errorf("the update helper was interrupted %d times", e.j.Attempts))
			return
		}
		e.begin(ctx)
	case upgStopping, upgSwapping, upgMigrating:
		e.resumed()
		e.restore(ctx, errors.New("the upgrade was interrupted while files were being replaced"))
	case upgStarting, upgChecking:
		e.resumed()
		if e.j.Attempts > maxHelperAttempts {
			e.restore(ctx, fmt.Errorf("the host or the update helper stopped %d times while the new version was starting", e.j.Attempts))
			return
		}
		e.startAndCheck(ctx)
	case upgRestoring:
		e.resumed()
		e.restore(ctx, errors.New(e.j.Reason))
	default:
		e.unchanged(fmt.Errorf("unknown step %q in the journal", e.j.Step))
	}
}

func (e *helperEngine) resumed() {
	e.j.Attempts++
	e.log.edit(func(j *upgradeJob) { j.Resumed = true })
	e.logf("upgrade: continuing job %s at %s (attempt %d)", e.j.ID, e.j.Step, e.j.Attempts)
	if err := e.saveJournal(); err != nil {
		e.logf("upgrade: %v", err)
	}
}

// unchanged ends a job before anything was touched.
func (e *helperEngine) unchanged(reason error) {
	e.log.fail(upgFailed, reason, "Not upgraded: %v. Nothing was changed.", reason)
	e.finish()
}

func (e *helperEngine) begin(ctx context.Context) {
	if e.j.Action == helperActionRollback {
		info, ok := rollbackAvailable(e.installDir)
		if !ok {
			e.log.fail(upgFailed, errors.New("no saved version"), "Not rolled back: there is no saved previous version any more.")
			e.finish()
			return
		}
		e.j.UserRollback, e.j.NewFiles, e.j.RollbackOf = true, info.NewFiles, info.Action
		e.j.From, e.j.Target = info.From, info.To
		if info.Action == helperActionAdopt {
			e.restore(ctx, fmt.Errorf("the service definitions from before v%s were asked for", info.To))
			return
		}
		e.restore(ctx, fmt.Errorf("a rollback to v%s was requested", info.From))
		return
	}

	if e.j.Action == helperActionUpgrade {
		if err := e.enter(upgStaging, "Checking and unpacking v%s", e.j.Target); err != nil {
			e.unchanged(err)
			return
		}
		if err := e.stage(); err != nil {
			e.unchanged(err)
			return
		}
	}
	if err := e.enter(upgSaving, "Saving the current version so it can be put back"); err != nil {
		e.unchanged(err)
		return
	}
	// A run that started meanwhile (the API refuses new ones, a schedule's
	// timer does not ask) would be cut off by the stop.
	if r, ok := e.svc.(interface{ runActive() string }); ok {
		if what := r.runActive(); what != "" {
			e.unchanged(fmt.Errorf("%s is in progress; upgrade when it has finished", what))
			return
		}
	}
	if err := e.snapshot(); err != nil {
		e.dropSnapshot()
		e.unchanged(fmt.Errorf("could not save the current version (%w)", err))
		return
	}
	if err := e.change(ctx); err != nil {
		e.restore(ctx, err)
		return
	}
	e.startAndCheck(ctx)
}

// change stops the stack and replaces programs and definitions. An error means
// that the snapshot has to be put back.
func (e *helperEngine) change(ctx context.Context) error {
	release := e.svc.holdRestarts()
	defer release()
	if err := e.enter(upgStopping, "Stopping %s", e.svc.title()); err != nil {
		return err
	}
	if err := e.svc.stop(); err != nil {
		return fmt.Errorf("the stack could not be stopped: %w", err)
	}
	if e.j.Action == helperActionUpgrade {
		root := e.stageRoot()
		e.j.NewFiles = newPrograms(root, e.installDir)
		if err := e.enter(upgSwapping, "Installing v%s", e.j.Target); err != nil {
			return err
		}
		if err := e.swap(root); err != nil {
			return fmt.Errorf("the new version could not be installed: %w", err)
		}
	}
	if err := e.enter(upgMigrating, "Updating the service definitions"); err != nil {
		return err
	}
	changes, err := e.svc.migrate(e.j.ReplaceEdited)
	if err != nil {
		return fmt.Errorf("the service definitions could not be updated: %w", err)
	}
	e.j.Changes = nil
	for _, c := range changes {
		if c.Action != defUnchanged {
			e.j.Changes = append(e.j.Changes, c.String())
		}
	}
	e.log.edit(func(j *upgradeJob) { j.Migrations = e.j.Changes })
	if len(e.j.Changes) > 0 {
		e.log.step(upgMigrating, "%s", strings.Join(e.j.Changes, "; "))
	} else if e.svc.kind() != kindManual {
		e.log.step(upgMigrating, "The service definitions are up to date")
	}
	return ctx.Err()
}

// startAndCheck starts the stack on the new files and finishes the job.
func (e *helperEngine) startAndCheck(ctx context.Context) {
	target := e.j.Target
	if e.j.Step != upgChecking || !e.svc.running() {
		e.j.SwitchedAt, e.switched = nowRFC3339(), time.Now()
		if err := e.enter(upgStarting, "Starting v%s", target); err != nil {
			e.restore(ctx, err)
			return
		}
		release := e.svc.holdRestarts()
		err := e.svc.start()
		release()
		if err != nil {
			e.restore(ctx, fmt.Errorf("the new version could not be started: %w", err))
			return
		}
	}
	if err := e.enter(upgChecking, "Checking that the supervisor, API and UI run v%s and stay healthy", target); err != nil {
		e.restore(ctx, err)
		return
	}
	since, err := time.Parse(time.RFC3339, e.j.SwitchedAt)
	if err != nil {
		since = time.Now()
	} else if !e.switched.IsZero() {
		since = since.Add(clockJump(e.switched, e.switched)) // set while it started
	}
	if err := e.check(ctx, since, target); err != nil {
		if ctx.Err() != nil {
			return // stopping (shutdown); the journal says where to go on
		}
		e.restore(ctx, err)
		return
	}
	if note := e.doctor(); note != nil {
		e.log.edit(func(j *upgradeJob) { j.Doctor = note })
	}
	recordTrustedStackQuiet(e.installDir, "upgrade")
	e.keepSnapshot()
	e.log.edit(func(j *upgradeJob) { j.ToVersion = target; j.Error = "" })
	// The result is written first: a helper that dies in between checks again.
	defer e.finish()
	switch {
	case e.j.Action == helperActionAdopt:
		e.log.step(upgDone, "The service now runs v%s with its new definitions", target)
	case e.j.From != "":
		e.log.step(upgDone, "Upgraded from v%s to v%s; the supervisor, API and UI are healthy", e.j.From, target)
	default:
		e.log.step(upgDone, "Upgraded to v%s; the supervisor, API and UI are healthy", target)
	}
}

// restore puts the snapshot back and starts the previous version.
func (e *helperEngine) restore(ctx context.Context, reason error) {
	if ctx.Err() != nil {
		return // shutting down: resumed from the journal
	}
	e.j.Reason = reason.Error()
	e.log.edit(func(j *upgradeJob) { j.Error = reason.Error() })
	stuck := func(format string, a ...any) {
		msg := fmt.Sprintf(format, a...)
		hint := "."
		if e.j.BackupPath != "" {
			hint = "; restore the pre-update backup " + filepath.Base(e.j.BackupPath) + " with v2-restore."
		}
		e.log.step(upgFailed, "%s. %s%s", reason, msg, hint)
		e.finish()
	}
	title := "Putting the previous version back"
	switch {
	case e.j.UserRollback && e.j.RollbackOf == helperActionAdopt:
		title = "Putting the service definitions back"
	case e.j.UserRollback:
		title = "Putting v" + e.j.From + " back"
	}
	if err := e.enter(upgRestoring, "%s. %s", reason, title); err != nil {
		e.logf("upgrade: %v", err) // a full disk must not prevent the restore
	}
	if e.j.Attempts > 2*maxHelperAttempts {
		stuck("Putting the previous version back was interrupted %d times; it was given up", e.j.Attempts)
		return
	}
	snap := helperSnapshotDir(e.installDir)
	if err := e.checkSnapshot(snap); err != nil {
		stuck("The saved copy of the previous version cannot be used (%v)", err)
		return
	}
	defs, err := readSavedDefinitions(snap)
	if err != nil {
		stuck("The saved service definitions cannot be read (%v)", err)
		return
	}

	release := e.svc.holdRestarts()
	var errs []error
	if err := e.svc.stop(); err != nil {
		e.logf("upgrade: stop before the restore: %v", err)
	}
	if existingDir(filepath.Join(snap, "bin")) {
		if err := installStackComponents(snap, e.installDir); err != nil {
			errs = append(errs, err)
		}
		errs = append(errs, e.restoreSelf(snap))
		for _, f := range e.j.NewFiles {
			if filepath.Base(f) == f && f != "." {
				_ = removeWithRetry(filepath.Join(e.installDir, "bin", f))
			}
		}
	}
	errs = append(errs, e.restoreConfig(snap))
	if err := e.svc.restore(defs); err != nil {
		errs = append(errs, fmt.Errorf("service definitions: %w", err))
	}
	since := time.Now()
	startErr := e.svc.start()
	release()
	if err := errors.Join(errs...); err != nil {
		stuck("Putting the previous version back failed (%v)", err)
		return
	}
	if startErr == nil {
		startErr = e.check(ctx, since, e.j.From)
	}
	if ctx.Err() != nil {
		return
	}
	if startErr != nil {
		stuck("The previous version was put back but does not start cleanly (%v)", startErr)
		return
	}
	recordTrustedStackQuiet(e.installDir, "rollback")
	switch {
	case e.j.UserRollback && e.j.RollbackOf == helperActionAdopt:
		e.log.edit(func(j *upgradeJob) { j.Error = ""; j.ToVersion = e.j.From; j.FromVersion = e.j.Target })
		e.log.step(upgRolledBack, "The service definitions from before v%s are back; the supervisor, API and UI are healthy", e.j.Target)
	case e.j.UserRollback:
		e.log.edit(func(j *upgradeJob) { j.Error = ""; j.ToVersion = e.j.From; j.FromVersion = e.j.Target })
		e.log.step(upgRolledBack, "Rolled back to v%s; the supervisor, API and UI are healthy", e.j.From)
	case e.j.From == "":
		e.log.step(upgRolledBack, "%s. The previous version was put back and is running.", reason)
	default:
		e.log.step(upgRolledBack, "%s. The previous version (v%s) was put back and is running.", reason, e.j.From)
	}
	// In this order: without its journal the job is over, without its snapshot
	// it could not be repeated.
	e.finish()
	e.dropSnapshot()
}

// ---- staging ----

// stagedRelease is what stage 1 saved (run/upgrade/stage/stage.json).
type stagedRelease struct {
	Version string `json:"version"`
	Archive string `json:"archive"` // file name, as listed in checksums.txt
}

func stageInfoPath(installDir string) string {
	return filepath.Join(helperStageDir(installDir), "stage.json")
}

const (
	stagedChecksums = "checksums.txt"
	stagedSignature = "checksums.txt.sig"
)

func (e *helperEngine) stageRoot() string { return filepath.Join(helperStageDir(e.installDir), "root") }

// verifyStaged checks the saved archive again, with this program's own key.
func verifyStaged(installDir string) (stagedRelease, []byte, error) {
	var st stagedRelease
	dir := helperStageDir(installDir)
	if err := readHelperJSON(stageInfoPath(installDir), &st); err != nil {
		return st, nil, fmt.Errorf("the downloaded release is incomplete: %w", err)
	}
	if st.Archive == "" || filepath.Base(st.Archive) != st.Archive || strings.ContainsAny(st.Archive, `/\:`) {
		return st, nil, errors.New("the downloaded release names an invalid archive")
	}
	sums, err := os.ReadFile(filepath.Join(dir, stagedChecksums))
	if err != nil {
		return st, nil, err
	}
	if _, haveKey := embeddedReleasePublicKey(); haveKey {
		sig, err := os.ReadFile(filepath.Join(dir, stagedSignature))
		if err != nil {
			return st, nil, errors.New("the downloaded release has no signature")
		}
		if _, err := verifyReleaseSignature(sums, strings.TrimSpace(string(sig))); err != nil {
			return st, nil, fmt.Errorf("the signature of the downloaded release is wrong: %w", err)
		}
	}
	body, err := os.ReadFile(filepath.Join(dir, st.Archive))
	if err != nil {
		return st, nil, err
	}
	if err := matchChecksum(sums, st.Archive, body); err != nil {
		return st, nil, err
	}
	return st, body, nil
}

func (e *helperEngine) stage() error {
	st, body, err := verifyStaged(e.installDir)
	if err != nil {
		return err
	}
	if st.Version != e.j.Target {
		return fmt.Errorf("the downloaded release is v%s, not v%s", st.Version, e.j.Target)
	}
	if e.version != e.j.Target {
		return fmt.Errorf("this update helper is v%s, not v%s", e.version, e.j.Target)
	}
	root := e.stageRoot()
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil { // #nosec G301 -- inside the private helper folder; the modes are copied to the install folder
		return err
	}
	if err := extractArchiveByAssetName(body, st.Archive, root); err != nil {
		return fmt.Errorf("unpack %s: %w", st.Archive, err)
	}
	if !hasBootstrappedV2Layout(root) || existingBinaryInInstallDir(root, "ncc-orchestrator") == "" {
		return fmt.Errorf("%s is not a complete release", st.Archive)
	}
	// What is about to be installed has to run on this host, and be what its
	// file name says.
	for _, base := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		bin := existingBinaryInInstallDir(root, base)
		_ = os.Chmod(bin, 0o755) // #nosec G302 -- a program
		if v := programVersion(e.runCmd, bin); v != e.j.Target {
			return fmt.Errorf("the new %s reports version %q instead of %s", base, v, e.j.Target)
		}
	}
	return nil
}

// programVersion asks a stack program for its version ("" when unknown).
func programVersion(run func(time.Duration, string, ...string) (string, error), bin string) string {
	out, err := run(30*time.Second, bin, "version")
	if err != nil {
		return ""
	}
	if m := versionLine.FindStringSubmatch(out); m != nil {
		return stripGoBuildGitSuffix(strings.TrimPrefix(m[1], "v"))
	}
	return ""
}

// newPrograms lists the programs in root that installDir does not have.
func newPrograms(root, installDir string) []string {
	var out []string
	entries, _ := os.ReadDir(filepath.Join(root, "bin"))
	for _, en := range entries {
		if !en.IsDir() && !fileExists(filepath.Join(installDir, "bin", en.Name())) {
			out = append(out, en.Name())
		}
	}
	return out
}

// swap installs the new programs; a service program outside bin/ too.
func (e *helperEngine) swap(root string) error {
	if err := installStackComponents(root, e.installDir); err != nil {
		return err
	}
	if self := e.outsideProgram(); self != "" {
		body, err := os.ReadFile(existingBinaryInInstallDir(root, "ncc-orchestrator"))
		if err != nil {
			return err
		}
		return replaceRunningExecutable(self, body)
	}
	return nil
}

// outsideProgram is the program the service starts, when that is not the one
// in bin/ ("" otherwise).
func (e *helperEngine) outsideProgram() string {
	bin := e.svc.orchestratorBin()
	if resolved, err := filepath.EvalSymlinks(bin); err == nil {
		bin = resolved
	}
	if bin == "" || !fileExists(bin) || sameDir(filepath.Dir(bin), filepath.Join(e.installDir, "bin")) {
		return ""
	}
	return bin
}

// ---- snapshot ----

func readSavedDefinitions(snap string) ([]savedDefinition, error) {
	var defs []savedDefinition
	err := readHelperJSON(filepath.Join(snap, "definitions.json"), &defs)
	return defs, err
}

func (e *helperEngine) snapshot() error {
	e.dropSnapshot()
	snap := helperSnapshotDir(e.installDir)
	if err := os.MkdirAll(snap, 0o700); err != nil {
		return err
	}
	defs, err := e.svc.save()
	if err != nil {
		return err
	}
	if err := writeHelperJSON(filepath.Join(snap, "definitions.json"), defs); err != nil {
		return err
	}
	if e.j.Action == helperActionUpgrade {
		if err := e.snapshotPrograms(snap); err != nil {
			return err
		}
	}
	if err := copyConfigFiles(e.installDir, filepath.Join(snap, "config")); err != nil {
		return err
	}
	sums, err := snapshotHashes(snap)
	if err != nil {
		return err
	}
	return writeHelperJSON(helperSnapshotSums(e.installDir), sums)
}

func (e *helperEngine) snapshotPrograms(snap string) error {
	binDir := filepath.Join(e.installDir, "bin")
	if err := os.MkdirAll(filepath.Join(snap, "bin"), 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(binDir)
	if err != nil {
		return err
	}
	for _, en := range entries {
		if en.IsDir() || strings.HasPrefix(en.Name(), ".") || strings.Contains(en.Name(), updateAsideMarker) {
			continue
		}
		if err := copyFilePlain(filepath.Join(binDir, en.Name()), filepath.Join(snap, "bin", en.Name()), 0o755); err != nil {
			return err
		}
	}
	if front := filepath.Join(e.installDir, "frontend-dist"); existingDir(front) {
		if err := copyDir(front, filepath.Join(snap, "frontend-dist")); err != nil {
			return err
		}
	}
	if ex := filepath.Join(e.installDir, "example_config.yaml"); existingFile(ex) {
		if err := copyFilePlain(ex, filepath.Join(snap, "example_config.yaml"), 0o644); err != nil {
			return err
		}
	}
	if self := e.outsideProgram(); self != "" {
		if err := os.MkdirAll(filepath.Join(snap, "self"), 0o700); err != nil {
			return err
		}
		return copyFilePlain(self, filepath.Join(snap, "self", filepath.Base(self)), 0o755)
	}
	return nil
}

func (e *helperEngine) restoreSelf(snap string) error {
	self := e.outsideProgram()
	if self == "" {
		return nil
	}
	body, err := os.ReadFile(filepath.Join(snap, "self", filepath.Base(self)))
	if os.IsNotExist(err) {
		return nil
	}
	if err == nil {
		err = replaceRunningExecutable(self, body)
	}
	return err
}

// configFiles lists the configuration files a new version could change.
func configFiles(installDir string) []string {
	var out []string
	for _, pattern := range []string{"*.yaml", "*.yml", "configs/*.yaml", "configs/*.yml", ".ncc-v2-start.json"} {
		m, _ := filepath.Glob(filepath.Join(installDir, filepath.FromSlash(pattern)))
		for _, p := range m {
			if st, err := os.Lstat(p); err == nil && st.Mode().IsRegular() && st.Size() <= 8<<20 && filepath.Base(p) != "example_config.yaml" {
				out = append(out, p)
			}
		}
	}
	return out
}

func copyConfigFiles(installDir, dst string) error {
	for _, p := range configFiles(installDir) {
		rel, err := filepath.Rel(installDir, p)
		if err != nil {
			return err
		}
		to := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(to), 0o700); err != nil {
			return err
		}
		if err := copyFilePlain(p, to, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func configSums(installDir string) map[string]string {
	out := map[string]string{}
	for _, p := range configFiles(installDir) {
		rel, err := filepath.Rel(installDir, p)
		if err != nil {
			continue
		}
		if sum, err := sha256File(p); err == nil {
			out[filepath.ToSlash(rel)] = sum
		}
	}
	return out
}

// restoreConfig puts configuration files back that the new version changed. A
// rollback somebody asked for later keeps the files people edited since.
func (e *helperEngine) restoreConfig(snap string) error {
	root := filepath.Join(snap, "config")
	if !existingDir(root) {
		return nil
	}
	var after map[string]string
	if e.j.UserRollback {
		info, _ := rollbackAvailable(e.installDir)
		after = info.Config
		if after == nil {
			after = map[string]string{}
		}
	}
	now := configSums(e.installDir)
	return filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if sum, err := sha256File(path); err == nil && sum == now[key] {
			return nil
		}
		dst := filepath.Join(e.installDir, rel)
		if after != nil && now[key] != after[key] {
			e.log.step(upgRestoring, "%s was edited since the upgrade and is kept as it is", key)
			return nil
		}
		mode := os.FileMode(0o600)
		if st, err := os.Stat(dst); err == nil {
			mode = st.Mode().Perm()
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// In place when it exists, so owner and permissions stay.
		if fileExists(dst) {
			return os.WriteFile(dst, body, mode)
		}
		return writeHelperFile(dst, body)
	})
}

// checkSnapshot reports why the snapshot must not be put back.
func (e *helperEngine) checkSnapshot(snap string) error {
	var want map[string]string
	if err := readHelperJSON(helperSnapshotSums(e.installDir), &want); err != nil {
		return err
	}
	got, err := snapshotHashes(snap)
	if err != nil {
		return err
	}
	if len(want) == 0 || !sameFiles(got, want) {
		return errors.New("its files are not the ones that were saved")
	}
	return nil
}

// keepSnapshot leaves the previous version for a rollback by hand: after an
// upgrade the programs and definitions, after an adoption the definitions.
func (e *helperEngine) keepSnapshot() {
	if e.j.Action != helperActionUpgrade && e.j.Action != helperActionAdopt {
		e.dropSnapshot()
		return
	}
	info := rollbackInfo{From: e.j.From, To: e.j.Target, Kind: e.j.Kind, At: nowRFC3339(), Action: e.j.Action,
		NewFiles: e.j.NewFiles, Backup: e.j.BackupPath, Config: configSums(e.installDir)}
	if err := writeHelperJSON(helperRollbackPath(e.installDir), info); err != nil {
		e.logf("upgrade: keep the previous version: %v", err)
		e.dropSnapshot()
		return
	}
	e.log.edit(func(j *upgradeJob) { j.RollbackUntil = time.Now().Add(helperKeepSnapshot).UTC().Format(time.RFC3339) })
}

func (e *helperEngine) dropSnapshot() { dropHelperSnapshot(e.installDir) }

func dropHelperSnapshot(installDir string) {
	_ = os.Remove(helperRollbackPath(installDir))
	_ = os.RemoveAll(helperSnapshotDir(installDir))
	_ = os.Remove(helperSnapshotSums(installDir))
}

// ---- health ----

// stackCheck waits until the stack runs a version and stays healthy.
type stackCheck struct {
	installDir   string
	svc          stackService
	poll         time.Duration
	window       time.Duration // servers must stay up this long
	timeout      time.Duration
	manualProbes func() error
}

// wallClock reads the wall clock, which can jump; time.Since and deadlines use
// the monotonic one. A variable for tests.
var wallClock = time.Now

// clockJump is how far the wall clock moved beyond the time that passed since
// t0 (negative: it was set back).
func clockJump(t0wall, t0 time.Time) time.Duration {
	return wallClock().Round(0).Sub(t0wall.Round(0)) - time.Since(t0)
}

func (c *stackCheck) wait(ctx context.Context, since time.Time, version string) error {
	since = since.Truncate(time.Second)
	// The servers stamp their start with the wall clock. Should it be set
	// while we wait, the stamps move with it, and so does since.
	t0, t0wall, start := time.Now(), wallClock(), since
	deadline := time.Now().Add(c.timeout)
	var last error
	watches := map[string]*childWatch{}
	pids := map[int]bool{}
	healthySince, stoppedSince := time.Time{}, time.Time{}
	for {
		if jump := clockJump(t0wall, t0); jump < -time.Second || jump > time.Second {
			since = start.Add(jump).Truncate(time.Second)
		}
		ok, fatal := false, error(nil)
		if c.svc.kind() == kindManual {
			if err := c.manualProbes(); err != nil {
				last, healthySince = err, time.Time{}
				if !errors.Is(err, errServerStopped) {
					stoppedSince = time.Time{}
				} else if stoppedSince.IsZero() {
					stoppedSince = time.Now()
				} else if time.Since(stoppedSince) >= c.window {
					fatal = err // nothing starts it again
				}
			} else {
				stoppedSince = time.Time{}
				if healthySince.IsZero() {
					healthySince = time.Now()
				}
				last = errors.New("the servers have just started")
				ok = time.Since(healthySince) >= c.window
			}
		} else {
			ok, last, fatal = c.supervised(since, version, watches, pids)
		}
		if fatal != nil {
			return fatal
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not healthy after %s: %w", shortDuration(c.timeout), last)
		}
		if !sleepCtx(ctx, c.poll) {
			return ctx.Err()
		}
	}
}

// supervised looks at the supervisor's published state once. last says why it
// is not healthy yet; fatal that it will not be.
func (c *stackCheck) supervised(since time.Time, version string, watches map[string]*childWatch, pids map[int]bool) (ok bool, last, fatal error) {
	st, err := readSupervisorState(c.installDir)
	if err != nil || st.PID == 0 {
		return false, errors.New("the supervisor is not running"), nil
	}
	started, err := time.Parse(time.RFC3339, st.StartedAt)
	if err != nil || started.Before(since) {
		return false, errors.New("the supervisor has not started yet"), nil
	}
	if pids[st.PID] = true; len(pids) > 3 {
		return false, nil, fmt.Errorf("the supervisor stopped and started %d times", len(pids)-1)
	}
	if !processIsAlive(st.PID) {
		return false, errors.New("the supervisor stopped again"), nil
	}
	if version != "" && st.Version != "" && stripGoBuildGitSuffix(strings.TrimPrefix(st.Version, "v")) != version {
		return false, nil, fmt.Errorf("the service started v%s instead of v%s", st.Version, version)
	}
	if len(st.Children) == 0 {
		return false, errors.New("no server has started yet"), nil
	}
	ok = true
	for name, cs := range st.Children {
		if cs == nil {
			continue
		}
		w := watches[name]
		if w == nil {
			w = &childWatch{since: since, window: c.window}
			watches[name] = w
		}
		good, err := w.look(*cs)
		if err != nil {
			return false, nil, fmt.Errorf("the %s did not come up: %w", childTitle(name), err)
		}
		if !good {
			ok = false
			last = fmt.Errorf("the %s is %s", childTitle(name), cs.State)
			if w.lastExit != "" {
				last = fmt.Errorf("%w (last stop: %s)", last, w.lastExit)
			}
		}
	}
	return ok, last, nil
}

// errServerStopped: a server that nothing restarts has ended.
var errServerStopped = errors.New("it has stopped")

// manualStackProbe checks servers that no supervisor looks after.
func manualStackProbe(installDir string) error {
	st, _ := loadV2StartState(installDir)
	want := []struct{ pid, base string }{{"v2-api.pid", "ncc-api-server"}}
	if !st.APIOnly {
		want = append(want, struct{ pid, base string }{"v2-ui.pid", "ncc-ui-server"})
	}
	for _, w := range want {
		pid, err := readPIDFromFile(filepath.Join(installDir, "run", w.pid))
		if err != nil || !processIsExpected(pid, w.base) {
			return fmt.Errorf("the %s is not running: %w", strings.TrimPrefix(w.base, "ncc-"), errServerStopped)
		}
	}
	listen := strings.TrimSpace(st.APIListen)
	if listen == "" {
		listen = ":8081"
	}
	api := existingBinaryInInstallDir(installDir, "ncc-api-server")
	args := buildAPIHealthProbeArgs(api, listen, filepath.Join(installDir, ".ncc-api-token"), installDir)
	if out, err := runUpgradeCommand(20*time.Second, args[0], args[1:]...); err != nil {
		return fmt.Errorf("the API server does not answer: %s", upgradeErrorLine(out, err))
	}
	return nil
}

// runDoctorFixes runs the non-disruptive self-heal checks of the new version.
func runDoctorFixes(run func(time.Duration, string, ...string) (string, error), installDir, bin string) *upgradeDoctorNote {
	note := &upgradeDoctorNote{}
	out, err := run(3*time.Minute, bin, "doctor", "--json", "--fix", "--no-disruptive", "--install-dir", installDir)
	var rep healReport
	if jerr := json.Unmarshal([]byte(jsonObject(out)), &rep); jerr != nil {
		note.Error = upgradeErrorLine(out, err)
		return note
	}
	note.Summary = rep.Summary
	for _, r := range rep.Results {
		if r.Fixed {
			note.Fixed = append(note.Fixed, r.Title)
		}
		if r.Status == healFail {
			note.Failing = append(note.Failing, r.Title)
		}
	}
	return note
}

// describeChanges says in one line what a plan would do.
func describeChanges(changes []defChange) string {
	var parts []string
	for _, c := range changes {
		if c.Action != defUnchanged {
			parts = append(parts, c.String())
		}
	}
	if len(parts) == 0 {
		return "up to date"
	}
	return plural.Count(len(parts), "change") + ": " + strings.Join(parts, "; ")
}
