package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestGuardRecoversAndCounts(t *testing.T) {
	buf := captureServerLog(t, slog.LevelDebug)
	before := backgroundPanicTotal.Load()
	if guard("ok task", func() {}) {
		t.Fatal("no panic should report false")
	}
	var m map[string]int
	if !guard("bad task", func() { m["x"] = 1 }) {
		t.Fatal("a panic should report true")
	}
	if backgroundPanicTotal.Load() != before+1 {
		t.Fatal("panic not counted")
	}
	if out := buf.String(); !strings.Contains(out, "background task panic") || !strings.Contains(out, "bad task") {
		t.Fatalf("panic not logged: %s", out)
	}
}

// A run whose launch or finalization panicked must not stay "running" for
// ever: it is finished as failed, its process is stopped and its clusters
// are released. A second call (the run already left) does nothing.
func TestFailRunAfterPanicReleasesRun(t *testing.T) {
	captureServerLog(t, slog.LevelDebug)
	dir := t.TempDir()
	cancelled := 0
	rec := &runRecord{id: "r1", status: "running", liveOut: newTailBuffer(1024), outDir: dir, cancel: func() { cancelled++ }, startedAt: time.Now()}
	s := &apiServer{
		repoRoot:             dir,
		outputDir:            dir,
		runAutoRetryDisabled: true,
		runs:                 map[string]*runRecord{"r1": rec},
		runOrder:             []string{"r1"},
		clusterOwners:        map[string]string{"c1": "r1", "c2": "other"},
	}
	s.failRunAfterPanic(rec)
	s.mu.Lock()
	_, still := s.runs["r1"]
	owner1, owner2 := s.clusterOwners["c1"], s.clusterOwners["c2"]
	s.mu.Unlock()
	if still || owner1 != "" || owner2 != "other" {
		t.Fatalf("run not released: active=%v owners=%q,%q", still, owner1, owner2)
	}
	if rec.status != "done" || !strings.Contains(rec.errStr, "internal error") || cancelled != 1 {
		t.Fatalf("status=%q err=%q cancelled=%d", rec.status, rec.errStr, cancelled)
	}
	s.failRunAfterPanic(rec)
	if s.runsCompletedTotal.Load() != 1 {
		t.Fatalf("finished twice: %d", s.runsCompletedTotal.Load())
	}
}

// A refresh that panics must not leave the entry stuck in "loading": the old
// value keeps being served and the next request retries.
func TestDerivedAsyncSurvivesPanic(t *testing.T) {
	captureServerLog(t, slog.LevelDebug)
	c := newArtifactCache(1 << 20)
	if got := c.derivedAsync("k", nil, time.Nanosecond, 1, func() interface{} { return "v1" }); got != "v1" {
		t.Fatalf("first load: %v", got)
	}
	time.Sleep(2 * time.Millisecond)
	if got := c.derivedAsync("k", nil, time.Nanosecond, 1, func() interface{} { panic("boom") }); got != "v1" {
		t.Fatalf("stale value expected while refreshing: %v", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		e := c.entry("k")
		e.mu.Lock()
		loading := e.loading
		e.mu.Unlock()
		if !loading {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("entry stayed in loading after a panic")
		}
		time.Sleep(5 * time.Millisecond)
	}
	done := make(chan struct{})
	c.derivedAsync("k", nil, time.Hour, 1, func() interface{} { close(done); return "v2" })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh was not retried after the panic")
	}
}
