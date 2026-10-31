package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestSuperviseRestartsAndStops verifies the native supervisor (a) restarts a
// child that keeps exiting and (b) stops cleanly (terminating the child and
// removing its pid file) when the context is cancelled.
func TestSuperviseRestartsAndStops(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("supervisor child uses /bin/sh; not portable to windows")
	}
	dir := t.TempDir()
	counter := filepath.Join(dir, "starts")
	pidPath := filepath.Join(dir, "x.pid")
	logPath := filepath.Join(dir, "x.log")

	// Child records a start then exits quickly, so the supervisor must keep
	// restarting it. With a tiny initial backoff several restarts happen fast.
	child := &superviseChild{
		name:    "x",
		bin:     "/bin/sh",
		args:    []string{"-c", "printf x >> " + counter + "; sleep 0.05"},
		pidPath: pidPath,
		logPath: logPath,
	}
	cfg := superviseConfig{
		installDir:     dir,
		children:       []*superviseChild{child},
		maxRestarts:    1000,
		window:         time.Minute,
		initialBackoff: 10 * time.Millisecond,
		backoffCap:     10 * time.Millisecond,
		stopGrace:      500 * time.Millisecond,
	}
	cfg.applyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		superviseRun(ctx, &cfg, func(string, ...any) {})
		close(done)
	}()

	time.Sleep(600 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not stop within 5s of cancel")
	}

	b, err := os.ReadFile(counter)
	if err != nil {
		t.Fatalf("read starts counter: %v", err)
	}
	if len(b) < 2 {
		t.Fatalf("expected the child to be (re)started at least twice, got %d start(s)", len(b))
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("expected child pid file %s to be removed after shutdown, stat err=%v", pidPath, err)
	}
}

// TestSuperviseRefusesSecondInstance verifies the single-instance guard: when a
// live supervisor pid file already exists, a second runV2Supervise aborts
// immediately instead of launching children and clobbering shared pid files.
func TestSuperviseRefusesSecondInstance(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh to hold a live pid; not portable to windows")
	}
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run: %v", err)
	}
	unlock, err := acquireSupervisorLock(filepath.Join(runDir, "v2-supervisor.lock"))
	if err != nil {
		t.Fatalf("acquire supervisor lock: %v", err)
	}
	defer unlock()

	// Hold a real, live pid to stand in for an already-running supervisor.
	holder := exec.Command("/bin/sh", "-c", "sleep 30")
	if err := holder.Start(); err != nil {
		t.Fatalf("start holder: %v", err)
	}
	defer func() { _ = holder.Process.Kill(); _, _ = holder.Process.Wait() }()

	supPID := filepath.Join(runDir, "v2-supervisor.pid")
	if err := os.WriteFile(supPID, []byte(fmt.Sprintf("%d\n", holder.Process.Pid)), 0o644); err != nil {
		t.Fatalf("write supervisor pid: %v", err)
	}

	started := filepath.Join(dir, "started")
	cfg := superviseConfig{
		installDir: dir,
		children: []*superviseChild{{
			name:    "x",
			bin:     "/bin/sh",
			args:    []string{"-c", "printf 1 >> " + started + "; sleep 30"},
			pidPath: filepath.Join(runDir, "v2-api.pid"),
			logPath: filepath.Join(dir, "x.log"),
		}},
		initialBackoff: 10 * time.Millisecond,
		backoffCap:     10 * time.Millisecond,
		stopGrace:      500 * time.Millisecond,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- runV2Supervise(cfg) }()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "already running") {
			t.Fatalf("expected 'already running' error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runV2Supervise did not abort on existing live supervisor pid")
	}

	// The guard must not have launched the child nor overwritten the live pid file.
	if _, err := os.Stat(started); !os.IsNotExist(err) {
		t.Fatalf("child was launched despite existing supervisor (stat err=%v)", err)
	}
	if got, err := readPIDFromFile(supPID); err != nil || got != holder.Process.Pid {
		t.Fatalf("supervisor pid file was clobbered: got=%d err=%v want=%d", got, err, holder.Process.Pid)
	}
}

// TestSuperviseWaitTokenGatesStart verifies a child with waitToken does not
// launch until the token file appears.
func TestSuperviseWaitTokenGatesStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("supervisor child uses /bin/sh; not portable to windows")
	}
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	started := filepath.Join(dir, "started")

	child := &superviseChild{
		name:      "gated",
		bin:       "/bin/sh",
		args:      []string{"-c", "printf 1 >> " + started + "; sleep 0.05"},
		pidPath:   filepath.Join(dir, "gated.pid"),
		logPath:   filepath.Join(dir, "gated.log"),
		waitToken: token,
	}
	cfg := superviseConfig{
		installDir:     dir,
		children:       []*superviseChild{child},
		initialBackoff: 10 * time.Millisecond,
		backoffCap:     10 * time.Millisecond,
		stopGrace:      500 * time.Millisecond,
	}
	cfg.applyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		superviseRun(ctx, &cfg, func(string, ...any) {})
		close(done)
	}()

	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(started); !os.IsNotExist(err) {
		t.Fatalf("child started before token file existed (stat err=%v)", err)
	}
	if err := os.WriteFile(token, []byte("ok"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(started); err != nil {
		t.Fatalf("child did not start after token file appeared: %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not stop within 5s of cancel")
	}
}
