package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeStack simulates the supervised servers and the commands an upgrade runs.
type fakeStack struct {
	mu        sync.Mutex
	dir       string
	version   string // what `version` reports
	updateOut string
	updateErr error
	// unhealthy: restarts of these servers fail until rolled back.
	unhealthy map[string]bool
	states    map[string]superviseChildState
	restarts  map[string]int
	watcher   []string
	commands  []string
}

func newFakeStack(t *testing.T) *fakeStack {
	dir := t.TempDir()
	for _, p := range []string{"bin", "frontend-dist", "run"} {
		if err := os.MkdirAll(filepath.Join(dir, p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		if err := os.WriteFile(filepath.Join(dir, "bin", b), []byte("old "+b), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "frontend-dist", "index.html"), []byte("old ui"), 0o644); err != nil {
		t.Fatal(err)
	}
	return &fakeStack{dir: dir, version: "2.3.0", updateOut: "Installed package", unhealthy: map[string]bool{},
		states: map[string]superviseChildState{}, restarts: map[string]int{}}
}

func (f *fakeStack) upgrader() *upgrader {
	return &upgrader{
		installDir: f.dir,
		selfPath:   filepath.Join(f.dir, "bin", "ncc-orchestrator"),
		children:   []string{"api", "ui"},
		childBin:   map[string]string{"api": filepath.Join(f.dir, "bin", "ncc-api-server"), "ui": filepath.Join(f.dir, "bin", "ncc-ui-server")},
		runCmd:     f.run,
		childState: func(name string) (superviseChildState, bool) {
			f.mu.Lock()
			defer f.mu.Unlock()
			st, ok := f.states[name]
			return st, ok
		},
		restart:       f.restart,
		logf:          func(string, ...any) {},
		pauseWatcher:  func() { f.watcher = append(f.watcher, "pause") },
		resumeWatcher: func() { f.watcher = append(f.watcher, "resume") },
		poll:          5 * time.Millisecond,
		verifyWindow:  0,
		verifyTimeout: 2 * time.Second,
	}
}

func (f *fakeStack) run(_ time.Duration, name string, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, filepath.Base(name)+" "+strings.Join(args, " "))
	switch args[0] {
	case "version":
		return "ncc\n  version: " + f.version + "\n", nil
	case "update":
		if f.updateErr != nil {
			return "Downloading...\nerror: checksum mismatch for ncc-v2-stack.tar.gz", f.updateErr
		}
		if !strings.Contains(f.updateOut, "latest") {
			f.version = "2.4.0"
			_ = os.WriteFile(filepath.Join(f.dir, "bin", "ncc-api-server"), []byte("new api"), 0o755)
		}
		return f.updateOut, nil
	case "doctor":
		return `{"summary":{"ok":30,"warn":1},"results":[{"id":"service-config","title":"Boot service definition","status":"ok","fixed":true}]}`, nil
	}
	return "", errors.New("unexpected command")
}

// restart behaves like the supervisor acting on a restart request.
func (f *fakeStack) restart(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restarts[name]++
	now := nowRFC3339()
	ok := true
	newBinary := f.version != "2.3.0"
	if f.unhealthy[name] && newBinary {
		f.states[name] = superviseChildState{Name: name, State: "cooldown", Probed: true, LastExitAt: now, LastExitReason: "crashed", LastExitDetail: "exit status 2", LastExitLogTail: []string{"panic: new schema"}}
		return
	}
	f.states[name] = superviseChildState{Name: name, State: "running", StartedAt: now, Probed: true, LastProbeAt: now, LastProbeOK: &ok}
}

func readJob(t *testing.T, dir string) *upgradeJob {
	t.Helper()
	j, err := readUpgradeJob(dir)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestUpgradeHappyPath(t *testing.T) {
	f := newFakeStack(t)
	f.upgrader().run(context.Background(), upgradeRequest{ID: "u1", BackupPath: "/b/pre-update.tar.gz"})
	j := readJob(t, f.dir)
	if j.Phase != upgDone || j.FromVersion != "2.3.0" || j.ToVersion != "2.4.0" || j.Error != "" {
		t.Fatalf("job = %+v", j)
	}
	if f.restarts["api"] != 1 || f.restarts["ui"] != 1 {
		t.Fatalf("restarts = %v", f.restarts)
	}
	if j.Doctor == nil || len(j.Doctor.Fixed) != 1 || j.Doctor.Summary["ok"] != 30 {
		t.Fatalf("doctor note = %+v", j.Doctor)
	}
	if strings.Join(f.watcher, ",") != "pause,resume" {
		t.Fatalf("watcher = %v", f.watcher)
	}
	if existingDir(filepath.Join(f.dir, "run", upgradeSnapshotDir)) {
		t.Fatal("snapshot left behind")
	}
	phases := []string{}
	for _, s := range j.Steps {
		phases = append(phases, s.Phase)
	}
	if got := strings.Join(phases, ","); got != "preflight,snapshot,installing,restarting,restarting,verifying,verifying,done" {
		t.Fatalf("steps = %s", got)
	}
}

func TestUpgradeRollsBackWhenAServerDoesNotComeBack(t *testing.T) {
	f := newFakeStack(t)
	f.unhealthy["api"] = true
	u := f.upgrader()
	// Rollback restarts must see the old version again.
	u.restart = func(name string) {
		if b, _ := os.ReadFile(filepath.Join(f.dir, "bin", "ncc-api-server")); strings.HasPrefix(string(b), "old") {
			f.mu.Lock()
			f.version = "2.3.0"
			f.mu.Unlock()
		}
		f.restart(name)
	}
	u.run(context.Background(), upgradeRequest{ID: "u2", BackupPath: "/b/pre-update-x.tar.gz"})
	j := readJob(t, f.dir)
	if j.Phase != upgRolledBack || !strings.Contains(j.Error, "API server did not come back healthy") || !strings.Contains(j.Error, "panic: new schema") {
		t.Fatalf("job = %+v", j)
	}
	if b, _ := os.ReadFile(filepath.Join(f.dir, "bin", "ncc-api-server")); string(b) != "old ncc-api-server" {
		t.Fatalf("api binary after rollback = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(f.dir, "frontend-dist", "index.html")); string(b) != "old ui" {
		t.Fatalf("frontend after rollback = %q", b)
	}
	if f.restarts["ui"] != 1 {
		t.Fatalf("ui should only restart during rollback: %v", f.restarts)
	}
}

func TestUpgradeInstallFailureChangesNothing(t *testing.T) {
	f := newFakeStack(t)
	f.updateErr = errors.New("exit status 1")
	f.upgrader().run(context.Background(), upgradeRequest{ID: "u3"})
	j := readJob(t, f.dir)
	if j.Phase != upgFailed || !strings.Contains(j.Error, "checksum mismatch") || len(f.restarts) != 0 {
		t.Fatalf("job = %+v restarts=%v", j, f.restarts)
	}
}

func TestUpgradeAlreadyLatest(t *testing.T) {
	f := newFakeStack(t)
	f.updateOut = "You are already on the latest version (2.3.0)"
	f.upgrader().run(context.Background(), upgradeRequest{ID: "u4"})
	if j := readJob(t, f.dir); j.Phase != upgDone || len(f.restarts) != 0 {
		t.Fatalf("job = %+v", j)
	}
}

func TestUpgradeResumesAfterSupervisorRestart(t *testing.T) {
	f := newFakeStack(t)
	f.version = "2.4.0"
	b, _ := json.Marshal(upgradeJob{ID: "u5", Phase: upgRestarting, FromVersion: "2.3.0", ToVersion: "2.4.0", StartedAt: nowRFC3339()})
	if err := os.WriteFile(upgradeStatePath(f.dir), b, 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	u := f.upgrader()
	go func() { u.loop(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if j, err := readUpgradeJob(f.dir); err == nil && !upgradeActive(j.Phase) {
			if j.Phase != upgDone || !j.Resumed {
				t.Fatalf("job = %+v", j)
			}
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("resumed upgrade did not finish")
}

func TestUpgradeRequestValidation(t *testing.T) {
	f := newFakeStack(t)
	u := f.upgrader()
	if err := os.WriteFile(upgradeRequestPath(f.dir), []byte(`{"id":"x","target_version":"--repo=evil"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := u.takeRequest(); ok || fileExists(upgradeRequestPath(f.dir)) {
		t.Fatal("invalid target accepted or request left behind")
	}
	if err := os.WriteFile(upgradeRequestPath(f.dir), []byte(`{"target_version":"2.4.1"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	req, ok := u.takeRequest()
	if !ok || req.TargetVersion != "2.4.1" || req.ID == "" {
		t.Fatalf("req = %+v ok=%v", req, ok)
	}
}

func TestUpgradeVersionMismatchRollsBack(t *testing.T) {
	f := newFakeStack(t)
	u := f.upgrader()
	inner := u.runCmd
	u.runCmd = func(timeout time.Duration, name string, args ...string) (string, error) {
		if args[0] == "version" && strings.HasSuffix(name, "ncc-ui-server") {
			return "version: 2.3.0", nil
		}
		return inner(timeout, name, args...)
	}
	u.run(context.Background(), upgradeRequest{ID: "u6"})
	if j := readJob(t, f.dir); j.Phase != upgRolledBack || !strings.Contains(j.Error, "different versions") {
		t.Fatalf("job = %+v", j)
	}
}
