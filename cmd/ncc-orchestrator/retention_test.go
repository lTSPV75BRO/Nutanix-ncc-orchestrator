package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func retWriteAged(t *testing.T, path string, age time.Duration, now time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("data for "+filepath.Base(path)), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := now.Add(-age)
	if err := os.Chtimes(path, mt, mt); err != nil {
		t.Fatal(err)
	}
}

func retPathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func retBaseNames(paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, filepath.Base(p))
	}
	sort.Strings(out)
	return out
}

func TestRawLogRetentionKeepsNewestAndReferenced(t *testing.T) {
	now := time.Now()
	root := t.TempDir()
	logs := filepath.Join(root, "nccfiles")
	report := filepath.Join(root, "outputfiles")
	day := 24 * time.Hour

	// All logs are old. c-ref is in the latest report, c-newest is the newest
	// log; only c-old-1 and c-old-2 may go. The young log stays too.
	retWriteAged(t, filepath.Join(logs, "c-newest.log"), 40*day, now)
	retWriteAged(t, filepath.Join(logs, "c-ref.log"), 90*day, now)
	retWriteAged(t, filepath.Join(logs, "c-old-1.log"), 60*day, now)
	retWriteAged(t, filepath.Join(logs, "c-old-2.log"), 70*day, now)
	retWriteAged(t, filepath.Join(logs, "notes.txt"), 70*day, now)
	retWriteAged(t, filepath.Join(logs, ".ncc-writecheck"), 70*day, now)
	sum := RunSummaryJSON{Clusters: []RunClusterSummary{{Address: "C-REF", OK: true}}}
	b, _ := json.Marshal(sum)
	retWriteAged(t, filepath.Join(report, "run-summary.json"), 0, now)
	if err := os.WriteFile(filepath.Join(report, "run-summary.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := rawLogRetentionCandidates(logs, report, 30, now)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"c-old-1.log", "c-old-2.log"}
	if g := retBaseNames(got); len(g) != 2 || g[0] != want[0] || g[1] != want[1] {
		t.Fatalf("candidates = %v, want %v", g, want)
	}

	// Off by default.
	if got, _ := rawLogRetentionCandidates(logs, report, 0, now); len(got) != 0 {
		t.Fatalf("raw-log-retain-days=0 listed %v", got)
	}
	// Never when the log folder is the report folder.
	if got, _ := rawLogRetentionCandidates(report, report, 1, now); len(got) != 0 {
		t.Fatalf("same folder listed %v", got)
	}

	n, err := applyRawLogRetention(logs, report, 30, now)
	if err != nil || n != 2 {
		t.Fatalf("applyRawLogRetention = %d, %v", n, err)
	}
	for _, name := range []string{"c-newest.log", "c-ref.log", "notes.txt", ".ncc-writecheck"} {
		if !retPathExists(filepath.Join(logs, name)) {
			t.Errorf("%s was removed", name)
		}
	}
	for _, name := range want {
		if retPathExists(filepath.Join(logs, name)) {
			t.Errorf("%s was kept", name)
		}
	}
}

func TestRawLogRetentionKeepsYoungLogs(t *testing.T) {
	now := time.Now()
	logs := t.TempDir()
	retWriteAged(t, filepath.Join(logs, "a.log"), time.Hour, now)
	retWriteAged(t, filepath.Join(logs, "b.log"), 2*24*time.Hour, now)
	got, err := rawLogRetentionCandidates(logs, "", 7, now)
	if err != nil || len(got) != 0 {
		t.Fatalf("candidates = %v, %v; want none", got, err)
	}
}

func TestRetentionPlanDryRunDeletesNothingAndMatchesApply(t *testing.T) {
	now := time.Now()
	root := t.TempDir()
	day := 24 * time.Hour
	cfg := Config{
		OutputDirFiltered:  filepath.Join(root, "outputfiles"),
		OutputDirLogs:      filepath.Join(root, "nccfiles"),
		RunHistoryEnabled:  true,
		RunHistoryDir:      filepath.Join(root, "runs"),
		RetainLastRuns:     2,
		ArtifactRetainDays: 10,
		RawLogRetainDays:   10,
	}
	// Run history: 3 runs, retain-last 2 drops the oldest.
	for i, ts := range []time.Time{now.Add(-3 * day), now.Add(-2 * day), now.Add(-1 * day)} {
		dir := filepath.Join(cfg.RunHistoryDir, runHistoryTimestamp(ts))
		retWriteAged(t, filepath.Join(dir, "run-summary.json"), time.Duration(3-i)*day, now)
	}
	// Report files: one old, one new, one protected.
	retWriteAged(t, filepath.Join(cfg.OutputDirFiltered, "old.html"), 20*day, now)
	retWriteAged(t, filepath.Join(cfg.OutputDirFiltered, "new.html"), day, now)
	retWriteAged(t, filepath.Join(cfg.OutputDirFiltered, "index.html"), 20*day, now)
	// Raw logs: newest kept, one old removed.
	retWriteAged(t, filepath.Join(cfg.OutputDirLogs, "newest.log"), 15*day, now)
	retWriteAged(t, filepath.Join(cfg.OutputDirLogs, "stale.log"), 30*day, now)

	plan := buildRetentionPlan(cfg, now)
	if !plan.DryRun {
		t.Fatal("plan should start as a dry run")
	}
	var listed []string
	for _, r := range plan.Rules {
		if r.Count != len(r.Items) {
			t.Errorf("%s: count %d, items %d", r.ID, r.Count, len(r.Items))
		}
		for _, it := range r.Items {
			listed = append(listed, it.Path)
			if !retPathExists(it.Path) {
				t.Errorf("dry run removed %s", it.Path)
			}
			if it.Bytes <= 0 {
				t.Errorf("%s: no size", it.Path)
			}
		}
	}
	if plan.TotalCount != 3 || len(listed) != 3 {
		t.Fatalf("plan lists %v (total %d), want 3 items", listed, plan.TotalCount)
	}
	if plan.TotalBytes <= 0 {
		t.Fatal("plan has no total size")
	}
	wantNames := []string{"old.html", runHistoryTimestamp(now.Add(-3 * day)), "stale.log"}
	sort.Strings(wantNames)
	if g := retBaseNames(listed); g[0] != wantNames[0] || g[1] != wantNames[1] || g[2] != wantNames[2] {
		t.Fatalf("plan lists %v, want %v", g, wantNames)
	}

	// A second plan is the same set, so nothing changed on disk.
	again := buildRetentionPlan(cfg, now)
	if again.TotalCount != plan.TotalCount || again.TotalBytes != plan.TotalBytes {
		t.Fatalf("second dry run differs: %d/%d vs %d/%d", again.TotalCount, again.TotalBytes, plan.TotalCount, plan.TotalBytes)
	}

	applyRetentionPlan(&plan)
	if plan.DryRun || plan.Deleted != 3 || len(plan.Errors) != 0 {
		t.Fatalf("apply: dry_run=%v deleted=%d errors=%v", plan.DryRun, plan.Deleted, plan.Errors)
	}
	for _, p := range listed {
		if retPathExists(p) {
			t.Errorf("%s was not removed", p)
		}
	}
	for _, p := range []string{
		filepath.Join(cfg.OutputDirFiltered, "new.html"),
		filepath.Join(cfg.OutputDirFiltered, "index.html"),
		filepath.Join(cfg.OutputDirLogs, "newest.log"),
	} {
		if !retPathExists(p) {
			t.Errorf("%s was removed", p)
		}
	}
	if after := buildRetentionPlan(cfg, now); after.TotalCount != 0 {
		t.Fatalf("after apply the plan still lists %d items", after.TotalCount)
	}
}

func TestRetentionPlanRulesOffKeepEverything(t *testing.T) {
	now := time.Now()
	root := t.TempDir()
	cfg := Config{OutputDirFiltered: filepath.Join(root, "out"), OutputDirLogs: filepath.Join(root, "logs")}
	retWriteAged(t, filepath.Join(cfg.OutputDirFiltered, "old.html"), 400*24*time.Hour, now)
	retWriteAged(t, filepath.Join(cfg.OutputDirLogs, "a.log"), 400*24*time.Hour, now)
	retWriteAged(t, filepath.Join(cfg.OutputDirLogs, "b.log"), 400*24*time.Hour, now)
	plan := buildRetentionPlan(cfg, now)
	if plan.TotalCount != 0 {
		t.Fatalf("defaults should keep everything, plan lists %d", plan.TotalCount)
	}
	for _, r := range plan.Rules {
		if r.Enabled {
			t.Errorf("%s enabled with default settings", r.ID)
		}
	}
}

func TestRawLogRetainDaysConfigKey(t *testing.T) {
	cfg := Config{
		Clusters:          []string{"10.0.1.1"},
		Username:          "admin",
		Timeout:           15 * time.Minute,
		RequestTimeout:    20 * time.Second,
		MaxParallel:       4,
		RetryMaxAttempts:  6,
		RetryBaseDelay:    400 * time.Millisecond,
		RetryMaxDelay:     8 * time.Second,
		OutputFormats:     []string{"html"},
		OutputDirLogs:     "nccfiles",
		OutputDirFiltered: "outputfiles",
		LogFile:           "logs/ncc-runner.log",
		RawLogRetainDays:  30,
	}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("raw-log-retain-days=30: %v", err)
	}
	cfg.RawLogRetainDays = -1
	if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), "raw-log-retain-days") {
		t.Fatalf("negative raw-log-retain-days: got %v", err)
	}
}

// The retention command with --dry-run --json prints the plan and deletes
// nothing; the folder flags override the config.
func TestRetentionCommandDryRunJSON(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	now := time.Now()
	root := t.TempDir()
	logs := filepath.Join(root, "logs")
	out := filepath.Join(root, "out")
	retWriteAged(t, filepath.Join(logs, "new.log"), 0, now)
	retWriteAged(t, filepath.Join(logs, "old.log"), 50*24*time.Hour, now)
	cfgPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("clusters: \"10.0.0.1\"\nusername: admin\nraw-log-retain-days: 30\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := newRetentionCmd()
	cmd.Flags().String("config", "", "")
	_ = cmd.Flags().Set("config", cfgPath)
	_ = cmd.Flags().Set("dry-run", "true")
	_ = cmd.Flags().Set("json", "true")
	_ = cmd.Flags().Set("output-dir-logs", logs)
	_ = cmd.Flags().Set("output-dir-filtered", out)

	r, w, _ := os.Pipe()
	stdout := os.Stdout
	os.Stdout = w
	err := runRetentionCommand(cmd, nil)
	w.Close()
	os.Stdout = stdout
	if err != nil {
		t.Fatal(err)
	}
	var plan retentionPlan
	if err := json.NewDecoder(r).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if !plan.DryRun || plan.TotalCount != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	if !retPathExists(filepath.Join(logs, "old.log")) {
		t.Fatal("dry run removed old.log")
	}
}
