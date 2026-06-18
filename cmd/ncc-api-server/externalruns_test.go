package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func writeHeartbeat(t *testing.T, dir string, pid int, source string) string {
	t.Helper()
	b, _ := json.Marshal(map[string]interface{}{
		"pid":        pid,
		"started_at": "2026-06-18T18:00:00Z",
		"clusters":   []string{"10.0.0.1"},
		"source":     source,
	})
	p := filepath.Join(dir, ".ncc-run-active-"+itoa(pid)+".json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestExternalActiveRunsLiveAndStale(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, outputDir: dir, configPath: filepath.Join(dir, "config.yaml")}

	// A live, unmanaged run (our own pid) should surface as a scheduled entry.
	live := os.Getpid()
	writeHeartbeat(t, dir, live, "scheduled")
	// A dead pid should be skipped and its stale file removed.
	deadPath := writeHeartbeat(t, dir, 2147483646, "scheduled")

	got := s.externalActiveRuns(map[int]bool{})
	if len(got) != 1 {
		t.Fatalf("expected 1 external run, got %d", len(got))
	}
	if got[0]["status"] != "running" || got[0]["source"] != "scheduled" || got[0]["external"] != true {
		t.Fatalf("unexpected entry: %+v", got[0])
	}
	if _, err := os.Stat(deadPath); !os.IsNotExist(err) {
		t.Fatalf("stale heartbeat for dead pid should have been removed")
	}

	// When the pid is managed (already shown), it should be excluded.
	if got := s.externalActiveRuns(map[int]bool{live: true}); len(got) != 0 {
		t.Fatalf("managed pid should be excluded, got %d", len(got))
	}
}

func TestExternalActiveRunsRemovesPidReuseHeartbeat(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, outputDir: dir, configPath: filepath.Join(dir, "config.yaml")}

	pid := os.Getpid()
	hbPath := writeHeartbeat(t, dir, pid, "scheduled")

	orig := processCmdline
	processCmdline = func(p int) string {
		if p == pid {
			return "bash -lc sleep 1"
		}
		return orig(p)
	}
	defer func() { processCmdline = orig }()

	got := s.externalActiveRuns(map[int]bool{})
	if len(got) != 0 {
		t.Fatalf("expected reused non-orchestrator pid heartbeat to be filtered, got %d entries", len(got))
	}
	if _, err := os.Stat(hbPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale heartbeat %s to be removed", strconv.Itoa(pid))
	}
}
