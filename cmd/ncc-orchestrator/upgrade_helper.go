package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"goncc/internal/runtimecaps"
)

// helperProbe asks the helper to prove that it can do its work, before anything
// depends on it.
type helperProbe struct {
	Nonce string `json:"nonce"`
	Kind  string `json:"kind"`
}

type helperProbeResult struct {
	Nonce   string `json:"nonce"`
	Version string `json:"version"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// probeSettle: the helper has to stay up this long before it reports.
var probeSettle = 5 * time.Second

func newNonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// runUpgradeHelper is the helper's whole life: it finishes an interrupted job,
// or takes a new one, or answers a probe; otherwise it exits at once. It takes
// no instructions other than the files in its private folder.
func runUpgradeHelper(installDir string) error {
	if err := runtimecaps.Detect().RejectHostOperation("the update helper"); err != nil {
		return err
	}
	installDir, err := filepath.Abs(installDir)
	if err != nil {
		return err
	}
	if err := prepareHelperDir(installDir); err != nil {
		return err
	}
	unlock, err := acquireSupervisorLock(helperLockPath(installDir))
	if err != nil {
		return nil // another helper is at work
	}
	defer unlock()

	logf, closeLog := helperLogger(installDir)
	defer closeLog()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer func() {
		// Last: while the wake file exists the service manager starts the helper
		// again, which is what an unfinished job needs.
		if !helperUnfinished(installDir) && !fileExists(helperRequestPath(installDir)) && !fileExists(helperProbePath(installDir)) {
			_ = os.Remove(helperWakePath(installDir))
			if svc, ok := findStackService(installDir).(interface{ disarmHelper() }); ok {
				svc.disarmHelper()
			}
		}
	}()

	j, found, err := takeHelperJob(installDir)
	if err != nil {
		logf("upgrade: %v", err)
		failJob(installDir, logf, err)
		return nil
	}
	if found {
		runHelperJob(ctx, installDir, j, logf)
		return nil
	}
	return answerProbe(ctx, installDir, logf)
}

// helperLogger writes to the helper's log file and to stderr (the journal).
func helperLogger(installDir string) (logf func(string, ...any), done func()) {
	w := io.Writer(os.Stderr)
	path := helperLogPath(installDir)
	done = func() {}
	_ = os.MkdirAll(filepath.Dir(path), 0o755) // #nosec G301 -- same mode as the stack's own log folder
	if st, err := os.Stat(path); err == nil && st.Size() > 2<<20 {
		_ = os.Rename(path, path+".1")
	}
	if f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640); err == nil {
		done = func() { _ = f.Close() }
		w = io.MultiWriter(os.Stderr, f)
		// A hand-started stack's helper has the log as its stderr already.
		if a, err := os.Stderr.Stat(); err == nil {
			if b, err := f.Stat(); err == nil && os.SameFile(a, b) {
				w = f
			}
		}
	}
	return func(format string, a ...any) {
		fmt.Fprintf(w, "%s helper v%s %s\n", nowRFC3339(), stripGoBuildGitSuffix(Version), fmt.Sprintf(format, a...))
	}, done
}

// takeHelperJob returns the job to work on: the one in the journal, or a new
// request. A request is taken by renaming it, so it is either taken or can
// still be withdrawn, never both.
func takeHelperJob(installDir string) (*helperJournal, bool, error) {
	journal, accepted := helperJournalPath(installDir), helperAcceptedPath(installDir)
	if fileExists(journal) {
		var j helperJournal
		err := readHelperJSON(journal, &j)
		if err == nil {
			err = j.validate()
		}
		if err != nil {
			_ = renameWithRetry(journal, journal+".bad")
			return nil, false, fmt.Errorf("the upgrade journal cannot be read (%w)", err)
		}
		_ = os.Remove(accepted)
		return &j, true, nil
	}
	if !fileExists(accepted) {
		if err := os.Rename(helperRequestPath(installDir), accepted); err != nil {
			if os.IsNotExist(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
	}
	var req helperRequest
	err := readHelperJSON(accepted, &req)
	if err == nil {
		err = req.validate()
	}
	if err != nil {
		_ = os.Remove(accepted)
		return nil, false, fmt.Errorf("the upgrade request is invalid (%w)", err)
	}
	j := &helperJournal{helperRequest: req}
	if err := writeHelperJSON(journal, j); err != nil {
		return nil, false, err
	}
	_ = os.Remove(accepted)
	return j, true, nil
}

// failJob ends the job in the progress file when its own data is unusable.
func failJob(installDir string, logf func(string, ...any), reason error) {
	job, err := readUpgradeJob(installDir)
	if err != nil || !helperPhaseActive(job.Phase) {
		return
	}
	hint := ""
	if job.BackupPath != "" {
		hint = " The pre-update backup is " + filepath.Base(job.BackupPath) + "."
	}
	newJobLog(installDir, logf, *job).fail(upgFailed, reason, "The upgrade stopped: %v.%s", reason, hint)
}

// helperJob is the progress of a job the helper takes over: what stage 1 wrote,
// or the request's copy of it.
func helperJob(installDir string, j *helperJournal) upgradeJob {
	job := upgradeJob{ID: j.ID, Steps: j.Steps, StartedAt: j.StartedAt}
	if old, err := readUpgradeJob(installDir); err == nil && old.ID == j.ID {
		job = *old
	}
	if job.StartedAt == "" {
		job.StartedAt = nowRFC3339()
	}
	job.Runner, job.Action = "helper", j.Action
	job.TargetVersion, job.FromVersion, job.BackupPath, job.RequestedBy, job.RequestedAt = j.Target, j.From, j.BackupPath, j.RequestedBy, j.RequestedAt
	job.FinishedAt = ""
	return job
}

func runHelperJob(ctx context.Context, installDir string, j *helperJournal, logf func(string, ...any)) {
	log := newJobLog(installDir, logf, helperJob(installDir, j))
	svc, err := stackServiceFor(j.Kind, installDir)
	e := &helperEngine{
		installDir: installDir, version: stripGoBuildGitSuffix(Version), svc: svc, log: log, logf: logf,
		runCmd: runUpgradeCommand, j: j,
	}
	if err == nil {
		err = svc.privileged()
	}
	if err != nil {
		log.fail(upgFailed, err, "The upgrade cannot go on: %v.", err)
		e.finish()
		return
	}
	check := newStackCheck(installDir, svc)
	e.check = check.wait
	e.doctor = func() *upgradeDoctorNote { return runDoctorFixes(runUpgradeCommand, installDir, svc.orchestratorBin()) }
	e.run(ctx)
}

func newStackCheck(installDir string, svc stackService) *stackCheck {
	return &stackCheck{
		installDir: installDir, svc: svc,
		poll: 2 * time.Second, window: 45 * time.Second, timeout: 6 * time.Minute,
		manualProbes: func() error { return manualStackProbe(installDir) },
	}
}

// answerProbe checks that this helper can do an upgrade on this host.
func answerProbe(ctx context.Context, installDir string, logf func(string, ...any)) error {
	var p helperProbe
	if err := readHelperJSON(helperProbePath(installDir), &p); err != nil {
		if !os.IsNotExist(err) {
			_ = os.Remove(helperProbePath(installDir))
		}
		return nil
	}
	res := helperProbeResult{Nonce: p.Nonce, Version: stripGoBuildGitSuffix(Version)}
	err := probeHelper(installDir, p.Kind)
	if err == nil && !sleepCtx(ctx, probeSettle) {
		err = errors.New("the helper was stopped")
	}
	if err != nil {
		res.Error = err.Error()
	}
	res.OK = err == nil
	logf("probe: ok=%v %s", res.OK, res.Error)
	_ = os.Remove(helperProbePath(installDir))
	return writeHelperJSON(helperResultPath(installDir), res)
}

func probeHelper(installDir, kind string) error {
	svc, err := stackServiceFor(kind, installDir)
	if err != nil {
		return err
	}
	if err := svc.privileged(); err != nil {
		return err
	}
	for _, d := range []string{installDir, filepath.Join(installDir, "bin"), filepath.Join(installDir, "run")} {
		if err := dirWritable(d); err != nil {
			return fmt.Errorf("cannot write %s: %w", d, err)
		}
	}
	return svc.probe()
}
