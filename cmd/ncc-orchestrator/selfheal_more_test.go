package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckConfigLayoutTidiesDuplicates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	raw := "schema-version: 1\ntimeout: 20m\nrunner:\n  execution:\n    timeout: 15m\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	hc := &healContext{InstallDir: dir, ConfigPath: path}
	res := checkConfigLayout(hc)
	if res.Status != healWarn || !strings.Contains(res.Message, "timeout (uses timeout = \"20m\"; ignored: runner.execution.timeout = 15m)") {
		t.Fatalf("read-only: %+v", res)
	}
	if b, _ := os.ReadFile(path); string(b) != raw {
		t.Fatal("read-only check wrote the file")
	}
	hc.Fix = true
	res = checkConfigLayout(hc)
	if res.Status != healOK || !res.Fixed {
		t.Fatalf("fix: %+v", res)
	}
	b, _ := os.ReadFile(path)
	if strings.Contains(string(b), "15m") || !strings.Contains(string(b), "timeout: 20m") {
		t.Fatalf("fixed file = %s", b)
	}
	if again := checkConfigLayout(&healContext{InstallDir: dir, ConfigPath: path}); again.Status != healOK {
		t.Fatalf("re-check: %+v", again)
	}
}

func TestFindTempLeftoversSkipsFreshAndUserFiles(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-7 * time.Hour)
	mk := func(name string, isDir bool, mod time.Time) {
		p := filepath.Join(dir, name)
		if isDir {
			if err := os.MkdirAll(filepath.Join(p, "sub"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(p, "sub", "f"), []byte("12345"), 0o600); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	mk("ncc-backup-123", true, old)              // orphaned staging folder
	mk("ncc-backup-keep.tar.gz", false, old)     // a backup the user saved here
	mk("ncc-restore-456", true, time.Now())      // a restore still running
	mk("ncc-restore-dec-789.tar.gz", false, old) // decrypted copy left behind
	mk("unrelated", true, old)
	left := findTempLeftovers(map[string][]tempPattern{dir: systemTempPatterns}, time.Now())
	var names []string
	for _, l := range left {
		names = append(names, filepath.Base(l.path))
	}
	if strings.Join(names, ",") != "ncc-backup-123,ncc-restore-dec-789.tar.gz" {
		t.Fatalf("leftovers = %v", names)
	}
	if left[0].bytes != 5 {
		t.Fatalf("folder size = %d", left[0].bytes)
	}
}

func TestRuntimeTuningFindings(t *testing.T) {
	if got := runtimeTuningFindings(map[string]string{}); len(got) != 0 {
		t.Fatalf("defaults flagged: %v", got)
	}
	got := runtimeTuningFindings(map[string]string{
		"timeout": "1m", "request-timeout": "2m", "poll-interval": "1s",
		"retry-base-delay": "2s", "retry-max-delay": "1s", "max-conns-per-host": "1",
		"log-http": "true", "log-level": "debug",
	})
	joined := strings.Join(got, "\n")
	for _, want := range []string{"request-timeout (2m) is not shorter than the per-cluster timeout (1m)", "poll-interval 1s", "retry-max-delay (1s) is below retry-base-delay (2s)", "max-conns-per-host 1", "log-http", "log-level debug"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in:\n%s", want, joined)
		}
	}
	if shortDuration(20*time.Minute) != "20m" || shortDuration(90*time.Minute) != "1h30m" || shortDuration(2*time.Hour) != "2h" || shortDuration(1500*time.Millisecond) != "1.5s" {
		t.Fatalf("shortDuration: %s %s %s %s", shortDuration(20*time.Minute), shortDuration(90*time.Minute), shortDuration(2*time.Hour), shortDuration(1500*time.Millisecond))
	}
}

func TestCheckOutputRetentionAppliesPolicy(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-40 * 24 * time.Hour)
	for _, name := range []string{"cluster-a.csv", "cluster-b.html", "index.html"} {
		p := filepath.Join(out, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(p, old, old)
	}
	hc := &healContext{InstallDir: dir, CfgLoaded: true, Cfg: Config{OutputDirFiltered: out, ArtifactRetainDays: 30}}
	res := checkOutputRetention(hc)
	if res.Status != healWarn || !strings.Contains(res.Message, "2 report file(s)") {
		t.Fatalf("read-only: %+v", res)
	}
	hc.Fix = true
	res = checkOutputRetention(hc)
	if !res.Fixed || res.Status != healOK {
		t.Fatalf("fix: %+v", res)
	}
	if _, err := os.Stat(filepath.Join(out, "index.html")); err != nil {
		t.Fatal("index.html is protected and must stay")
	}
}

func TestHealEngineRecoversVerifiesAndTracksRecurrence(t *testing.T) {
	dir := t.TempDir()
	crash := healCheck{ID: "boom", Title: "Boom", Category: "x", Run: func(*healContext) healResult { panic("nil map") }}
	res := runHealCheckSafely(crash, &healContext{})
	if res.Status != healFail || !strings.Contains(res.Message, "check crashed: nil map") {
		t.Fatalf("panic result: %+v", res)
	}

	// A fix that does not hold: the re-check still fails.
	flaky := healCheck{ID: "flaky", Title: "Flaky", Category: "x", Run: func(hc *healContext) healResult {
		if hc.Fix {
			return healResult{ID: "flaky", Status: healOK, Fixed: true, FixMsg: "did a thing"}
		}
		return healResult{ID: "flaky", Status: healWarn, Message: "still broken"}
	}}
	hc := &healContext{InstallDir: dir, Fix: true}
	v := verifyHealFix(flaky, hc, runHealCheckSafely(flaky, hc))
	if v.Verified == nil || *v.Verified || v.Status != healWarn {
		t.Fatalf("verify: %+v", v)
	}

	for i := 0; i < recurringFixThreshold; i++ {
		results := []healResult{{ID: "flaky", Title: "Flaky", Fixed: true, FixMsg: "did a thing", Status: healOK}}
		markRecurringFixes(dir, results)
		appendHealHistory(dir, historyEntriesFor(results))
		if want := i+1 >= recurringFixThreshold; results[0].Recurring != want || results[0].Fixes24h != i+1 {
			t.Fatalf("pass %d: %+v", i, results[0])
		}
	}
	if got := readHealHistory(dir, time.Time{}); len(got) != recurringFixThreshold || got[0].ID != "flaky" {
		t.Fatalf("history = %+v", got)
	}
}
