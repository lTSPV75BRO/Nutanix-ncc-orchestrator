package main

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// API-archived runs are named by run ID (timestamp plus "-NNN"); retention
// must prune them like engine-named timestamp directories.
func TestRunHistoryRetentionCountsRunIDDirectories(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"20261101T000000Z",     // engine-named, oldest
		"20261102T000000Z-001", // API run IDs
		"20261103T000000Z-001",
		"20261103T000000Z-002", // same second, later sequence
		"notes",                // not a run: never touched
		"20261103T000000Z-abc", // not a run ID
	}
	for _, n := range names {
		if err := os.MkdirAll(filepath.Join(dir, n), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got, err := listHistoryRunDirs(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"20261103T000000Z-002", "20261103T000000Z-001", "20261102T000000Z-001", "20261101T000000Z"}
	if len(got) != len(want) {
		t.Fatalf("listHistoryRunDirs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listHistoryRunDirs = %v, want %v", got, want)
		}
	}

	if err := applyRunHistoryRetention(dir, 2, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	left, _ := os.ReadDir(dir)
	var kept []string
	for _, e := range left {
		kept = append(kept, e.Name())
	}
	sort.Strings(kept)
	wantKept := []string{"20261103T000000Z-001", "20261103T000000Z-002", "20261103T000000Z-abc", "notes"}
	if len(kept) != len(wantKept) {
		t.Fatalf("kept %v, want %v", kept, wantKept)
	}
	for i := range wantKept {
		if kept[i] != wantKept[i] {
			t.Fatalf("kept %v, want %v", kept, wantKept)
		}
	}

	// Age-based retention also reads the run-ID timestamp.
	now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	if err := applyRunHistoryRetention(dir, 0, 10, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "20261103T000000Z-001")); !os.IsNotExist(err) {
		t.Fatalf("10-day-old run ID directory was not pruned: %v", err)
	}
}
