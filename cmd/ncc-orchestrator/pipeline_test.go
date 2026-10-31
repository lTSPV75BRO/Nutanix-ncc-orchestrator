package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAutoMaxParallel(t *testing.T) {
	for _, tc := range []struct{ n, want int }{{0, 1}, {1, 1}, {40, 40}, {500, 500}, {7000, autoMaxParallelCap}} {
		if got := autoMaxParallel(tc.n); got != tc.want {
			t.Errorf("autoMaxParallel(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

// The processing pool must keep up with the cluster slots: sized to CPUs it
// made a run with slow webhooks slower than before (fetched clusters queued
// while holding their slots).
func TestProcessingWorkersKeepUpWithSlots(t *testing.T) {
	if got := processingWorkers(500); got < 500 {
		t.Fatalf("processingWorkers(500) = %d", got)
	}
	if got := processingWorkers(1); got < 1 {
		t.Fatalf("processingWorkers(1) = %d", got)
	}
}

func TestRetryableClusterError(t *testing.T) {
	err := errors.New("boom")
	for class, want := range map[string]bool{"timeout": true, "network": true, "rate_limit": true, "auth": false, "parser": false, "": false} {
		if got := retryableClusterError(ClusterResult{Err: err, ErrorClass: class}); got != want {
			t.Errorf("class %q: retry=%v, want %v", class, got, want)
		}
	}
	if retryableClusterError(ClusterResult{ErrorClass: "timeout"}) {
		t.Error("a success is never retried")
	}
	cancelled := fmt.Errorf("%w: context canceled", errRunCancelled)
	if retryableClusterError(ClusterResult{Err: cancelled, ErrorClass: "timeout"}) {
		t.Error("a cluster stopped by shutdown is not retried")
	}
}

func TestNextPollDelay(t *testing.T) {
	interval := 15 * time.Second
	// Early on, or without progress: the configured interval (plus jitter).
	for _, pct := range []int{0, 1} {
		if d := nextPollDelay(interval, 2*time.Second, time.Minute, pct); d < interval || d > interval+2*time.Second {
			t.Errorf("pct %d: %v", pct, d)
		}
	}
	// Far from done: never more often than the interval.
	if d := nextPollDelay(interval, 0, 30*time.Second, 20); d != interval {
		t.Errorf("20%% after 30s: %v, want %v", d, interval)
	}
	// Close to done (90% after 180s ≈ 20s left → interval; 97% ≈ 5.6s left).
	if d := nextPollDelay(interval, 0, 180*time.Second, 97); d < 5*time.Second || d > 7*time.Second {
		t.Errorf("97%% after 180s: %v, want just after the ~5.6s estimate", d)
	}
	// Never more often than interval/5 (3s here).
	if d := nextPollDelay(interval, 0, 180*time.Second, 99); d < 3*time.Second {
		t.Errorf("99%%: %v, want >= 3s", d)
	}
	// Never faster than once a second, even with a short interval.
	if d := nextPollDelay(2*time.Second, 0, 100*time.Second, 99); d < time.Second {
		t.Errorf("short interval: %v", d)
	}
}

func TestRunJournalResume(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{OutputDirFiltered: dir, Clusters: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}}
	start := time.Now()
	j, err := openRunJournal(cfg, false, start)
	if err != nil {
		t.Fatal(err)
	}
	j.markDone("10.0.0.1")
	j.markDone("10.0.0.3")
	j.close(false) // interrupted: kept

	done, why := readRunJournal(runJournalPath(cfg), cfg.Clusters, start.Add(time.Hour))
	if why != "" || !done["10.0.0.1"] || done["10.0.0.2"] || !done["10.0.0.3"] {
		t.Fatalf("done=%v why=%q", done, why)
	}
	// Order of the cluster list does not matter; its contents do.
	if _, why := readRunJournal(runJournalPath(cfg), []string{"10.0.0.3", "10.0.0.2", "10.0.0.1"}, start); why != "" {
		t.Fatalf("reordered list rejected: %s", why)
	}
	if _, why := readRunJournal(runJournalPath(cfg), []string{"10.0.0.1"}, start); !strings.Contains(why, "cluster list changed") {
		t.Fatalf("different list: %q", why)
	}
	if _, why := readRunJournal(runJournalPath(cfg), cfg.Clusters, start.Add(resumeMaxAge+time.Minute)); !strings.Contains(why, "ago") {
		t.Fatalf("stale journal: %q", why)
	}

	// Resuming appends to the same journal and keeps its header.
	j2, err := openRunJournal(cfg, true, start.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	j2.markDone("10.0.0.2")
	j2.close(false)
	if done, _ := readRunJournal(runJournalPath(cfg), cfg.Clusters, start.Add(2*time.Hour)); len(done) != 3 {
		t.Fatalf("after resume: %v", done)
	}

	// A run that completes removes the journal.
	j3, _ := openRunJournal(cfg, false, start)
	j3.close(true)
	if _, err := os.Stat(runJournalPath(cfg)); !os.IsNotExist(err) {
		t.Fatalf("journal left after a complete run: %v", err)
	}
}

func TestResumedClusterResultFromRawLog(t *testing.T) {
	logs := t.TempDir()
	cfg := Config{OutputDirLogs: logs, SeverityFilter: []string{"FAIL"}}
	raw := "Cluster Name: prod-a\nNCC Version: 5.0.2\n" +
		"Detailed information for disk_usage_check:\nFAIL: disk 95%\nRefer to KB 1 for details.\n" +
		"Detailed information for ntp_check:\nWARN: drift\nRefer to KB 2 for details.\n"
	if err := os.WriteFile(filepath.Join(logs, "10.0.0.1.log"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	r, ok := resumedClusterResult(cfg, "10.0.0.1")
	if !ok || r.Cluster != "10.0.0.1" || r.Meta.ClusterName != "prod-a" {
		t.Fatalf("result = %+v ok=%v", r, ok)
	}
	// The same filters as a live run apply.
	if len(r.Blocks) != 1 || r.Blocks[0].Severity != "FAIL" {
		t.Fatalf("blocks = %+v", r.Blocks)
	}
	if _, ok := resumedClusterResult(cfg, "10.0.0.9"); ok {
		t.Fatal("a cluster without a saved summary must be run again")
	}
}
