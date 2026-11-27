package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// The update helper carries an upgrade from outside the service it upgrades
// (docs/UPGRADE_HELPER_PLAN.md). This file holds what it keeps on disk:
//
//	run/upgrade/request.json   work for the helper (written last, in stage 1)
//	run/upgrade/journal.json   the job it works on; present = not finished
//	run/upgrade/probe.json     asks the helper to prove that it can do the work
//	run/upgrade/wake           present = the service manager starts the helper
//	run/upgrade/stage/         the verified release archive
//	run/upgrade/snapshot/      what a rollback puts back
//
// The folder is private to root (SYSTEM): what is in it decides what is run.
// Progress goes to run/v2-upgrade-state.json, the file the supervisor already
// uses, under phase names that older supervisors do not resume.

const (
	helperActionUpgrade  = "upgrade"
	helperActionAdopt    = "adopt"    // same programs, new service definitions
	helperActionRollback = "rollback" // put the last snapshot back
	// helperActionUpdate is not a job: it names a snapshot that `update`
	// (programs only) kept, which a rollback puts back like any other.
	helperActionUpdate = "update"
)

// Phases of a job the helper carries out (or, for the first four, that the
// supervisor or the CLI prepares for it).
const (
	upgDownloading   = "downloading"
	upgHelperInstall = "helper_install"
	upgHelperCheck   = "helper_check"
	upgHandover      = "handover"
	upgStaging       = "staging"
	upgSaving        = "saving"
	upgStopping      = "stopping"
	upgSwapping      = "swapping"
	upgMigrating     = "migrating"
	upgStarting      = "starting"
	upgChecking      = "checking"
	upgRestoring     = "restoring"
)

// helperPhaseActive reports whether a helper job in this phase is running.
func helperPhaseActive(phase string) bool {
	switch phase {
	case upgDownloading, upgHelperInstall, upgHelperCheck, upgHandover, upgStaging, upgSaving, upgStopping,
		upgSwapping, upgMigrating, upgStarting, upgChecking, upgRestoring:
		return true
	}
	return false
}

// helperKeepSnapshot: how long a finished upgrade can be rolled back by hand.
const helperKeepSnapshot = 24 * time.Hour

func helperDir(installDir string) string { return filepath.Join(installDir, "run", "upgrade") }
func helperFile(installDir, name string) string {
	return filepath.Join(helperDir(installDir), name)
}
func helperWakePath(installDir string) string    { return helperFile(installDir, "wake") }
func helperJournalPath(installDir string) string { return helperFile(installDir, "journal.json") }
func helperRequestPath(installDir string) string { return helperFile(installDir, "request.json") }
func helperProbePath(installDir string) string   { return helperFile(installDir, "probe.json") }
func helperResultPath(installDir string) string  { return helperFile(installDir, "probe-result.json") }
func helperStageDir(installDir string) string    { return helperFile(installDir, "stage") }
func helperSnapshotDir(installDir string) string { return helperFile(installDir, "snapshot") }
func helperLogPath(installDir string) string {
	return filepath.Join(installDir, "logs", "upgrade-helper.log")
}
func helperLockPath(installDir string) string  { return helperFile(installDir, "lock") }
func helperAdoptPath(installDir string) string { return helperFile(installDir, "adopt.json") }
func helperUnfinished(installDir string) bool  { return fileExists(helperJournalPath(installDir)) }
func helperBinaryPath(installDir string) string {
	return filepath.Join(installDir, "helper", "ncc-upgrade-helper"+exeSuffix())
}

// helperRequest is what stage 1 hands to the helper. It names no path, URL or
// command: those come from the helper's own definition.
type helperRequest struct {
	ID          string `json:"id"`
	Action      string `json:"action"`
	Kind        string `json:"kind"` // service manager (stackService.kind)
	From        string `json:"from,omitempty"`
	Target      string `json:"target,omitempty"`
	BackupPath  string `json:"backup_path,omitempty"`
	RequestedBy string `json:"requested_by,omitempty"`
	RequestedAt string `json:"requested_at,omitempty"`
	// Steps taken so far, so the progress list does not start over.
	Steps     []upgradeStep `json:"steps,omitempty"`
	StartedAt string        `json:"started_at,omitempty"`
	// ReplaceEdited: definitions edited on this host are replaced, not kept
	// (upgrade --replace-edited). Their saved copy comes back on a rollback.
	ReplaceEdited bool `json:"replace_edited,omitempty"`
}

// helperJournal is the job the helper is working on.
type helperJournal struct {
	helperRequest
	Step     string `json:"step"`
	Attempts int    `json:"attempts"`
	// SwitchedAt: when the stack was started on the new files.
	SwitchedAt string   `json:"switched_at,omitempty"`
	Reason     string   `json:"reason,omitempty"`    // why it is being restored
	NewFiles   []string `json:"new_files,omitempty"` // programs the previous version did not have
	Changes    []string `json:"changes,omitempty"`   // what was migrated
	// UserRollback: a rollback somebody asked for, not one after a failure.
	UserRollback bool `json:"user_rollback,omitempty"`
	// RollbackOf: what that rollback undoes (rollbackInfo.Action).
	RollbackOf string `json:"rollback_of,omitempty"`
}

var helperIDPattern = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z._-]{0,63}$`)

func (r helperRequest) validate() error {
	switch r.Action {
	case helperActionUpgrade, helperActionAdopt, helperActionRollback:
	default:
		return fmt.Errorf("unknown action %q", r.Action)
	}
	if !helperIDPattern.MatchString(r.ID) {
		return errors.New("invalid id")
	}
	if r.Action == helperActionUpgrade && r.Target == "" {
		return errors.New("no target version")
	}
	for _, v := range []string{r.Target, r.From} {
		if v != "" && !upgradeTargetPattern.MatchString(v) {
			return fmt.Errorf("invalid version %q", v)
		}
	}
	return nil
}

// decodeManifest reads a manifest; fields of later releases are ignored.
func decodeManifest(b []byte, m *upgradeManifest) error {
	if err := json.Unmarshal(b, m); err != nil {
		return err
	}
	if m.Schema < 1 || !upgradeTargetPattern.MatchString(m.Version) {
		return errors.New("not an upgrade manifest")
	}
	return nil
}

// readHelperJSON reads a small file of the helper folder, strictly.
func readHelperJSON(path string, v any) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", filepath.Base(path))
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := readBounded(f, 1<<20)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// writeHelperJSON writes durably: the journal must survive a power cut.
func writeHelperJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeHelperFile(path, b)
}

func writeHelperFile(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	staged, err := stageBytes(b, path, 0o600) // fsyncs
	if err != nil {
		return err
	}
	if err := renameWithRetry(staged, path); err != nil {
		_ = os.Remove(staged)
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// syncDir makes a rename durable where the OS supports it.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// prepareHelperDir creates the helper's private folder.
func prepareHelperDir(installDir string) error {
	dir := helperDir(installDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	st, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a folder", dir)
	}
	if err := checkFolderOwner(dir, st); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil { // #nosec G302 -- a folder
		return err
	}
	restrictNewSecret(dir)
	return nil
}

// jobLog writes a job's progress for the UI. Its methods are safe for use
// from several goroutines.
type jobLog struct {
	installDir string
	logf       func(string, ...any)

	mu  sync.Mutex
	job upgradeJob
}

func newJobLog(installDir string, logf func(string, ...any), job upgradeJob) *jobLog {
	return &jobLog{installDir: installDir, logf: logf, job: job}
}

func (l *jobLog) edit(fn func(*upgradeJob)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn(&l.job)
	l.saveLocked()
}

func (l *jobLog) saveLocked() {
	l.job.UpdatedAt = nowRFC3339()
	b, err := json.MarshalIndent(l.job, "", "  ")
	if err != nil {
		return
	}
	path := upgradeStatePath(l.installDir)
	tmp := path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = renameWithRetry(tmp, path)
	}
}

// step moves the job to phase and records it.
func (l *jobLog) step(phase, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	l.logf("upgrade %s: %s", phase, msg)
	l.edit(func(j *upgradeJob) {
		j.Phase, j.Message = phase, msg
		j.Steps = append(j.Steps, upgradeStep{At: nowRFC3339(), Phase: phase, Message: msg})
		if !upgradeActive(phase) && !helperPhaseActive(phase) {
			j.FinishedAt = nowRFC3339()
		}
	})
}

// fail ends the job with err as its reason.
func (l *jobLog) fail(phase string, reason error, format string, a ...any) {
	l.edit(func(j *upgradeJob) { j.Error = reason.Error() })
	l.step(phase, format, a...)
}

// touch tells readers that the job is still being worked on.
func (l *jobLog) touch() { l.edit(func(*upgradeJob) {}) }

// keepAlive touches the job until the returned function is called.
func (l *jobLog) keepAlive(every time.Duration) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				l.touch()
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }); wg.Wait() }
}

func (l *jobLog) snapshot() upgradeJob {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.job
}
