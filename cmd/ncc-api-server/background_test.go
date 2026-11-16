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
