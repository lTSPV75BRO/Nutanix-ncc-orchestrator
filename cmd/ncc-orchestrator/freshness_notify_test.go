package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeHistoryRun(t *testing.T, dir string, at time.Time, clusters string) {
	t.Helper()
	runDir := filepath.Join(dir, at.UTC().Format("20060102T150405Z"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"timestamp":"` + at.UTC().Format(time.RFC3339) + `","clusters":[` + clusters + `]}`
	if err := os.WriteFile(filepath.Join(runDir, "run-summary.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestUnreachedClusterLines(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	writeHistoryRun(t, dir, now.Add(-time.Minute), `{"address":"pe1","ok":false},{"address":"pe2","ok":false},{"address":"pe3","ok":true}`)
	writeHistoryRun(t, dir, now.Add(-5*time.Hour), `{"address":"PE1","ok":true},{"address":"pe2","ok":false}`)
	writeHistoryRun(t, dir, now.Add(-80*time.Hour), `{"address":"pe1","ok":true}`)

	got := unreachedClusterLines(dir, []string{"pe1", "pe2"}, now)
	want := []string{
		"pe1 was not reached; its data is from 5 hours ago.",
		"pe2 was not reached; no run has returned data for it yet.",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("lines:\n got %q\nwant %q", got, want)
	}
	if lines := unreachedClusterLines(dir, nil, now); len(lines) != 0 {
		t.Fatalf("every cluster reached: want no lines, got %q", lines)
	}
}

func TestUnreachedClusterLinesCapped(t *testing.T) {
	var failed []string
	for i := 0; i < unreachedNoticeMax+1; i++ {
		failed = append(failed, "pe"+string(rune('a'+i)))
	}
	got := unreachedClusterLines(t.TempDir(), failed, time.Now())
	if len(got) != unreachedNoticeMax+1 || got[len(got)-1] != "… and 1 more cluster not reached." {
		t.Fatalf("lines: %q", got)
	}
}

func TestDataAgeText(t *testing.T) {
	for d, want := range map[time.Duration]string{
		30 * time.Second: "less than a minute",
		time.Minute:      "1 minute",
		90 * time.Minute: "1 hour",
		49 * time.Hour:   "2 days",
	} {
		if got := dataAgeText(d); got != want {
			t.Errorf("dataAgeText(%s) = %q, want %q", d, got, want)
		}
	}
}
