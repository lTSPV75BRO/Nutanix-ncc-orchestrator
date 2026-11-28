package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// copyFile streams src to dst (run-history snapshots include the aggregated
// rows file, ~80 MB at 7,000 clusters, so it is not loaded into memory).
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func runHistoryTimestamp(now time.Time) string {
	return now.UTC().Format("20060102T150405Z")
}

// historyDirTime parses a run-history directory name. The engine names them
// by timestamp; the API names archived runs by run ID, which is the timestamp
// plus a sequence suffix (20260102T150405Z-001). Both must count for
// retention, or API runs are never pruned.
func historyDirTime(name string) (time.Time, bool) {
	const layout = "20060102T150405Z"
	if len(name) > len(layout) && name[len(layout)] == '-' {
		if _, err := strconv.Atoi(name[len(layout)+1:]); err != nil {
			return time.Time{}, false
		}
		name = name[:len(layout)]
	}
	ts, err := time.Parse(layout, name)
	return ts, err == nil
}

func listHistoryRunDirs(historyDir string) ([]string, error) {
	ents, err := os.ReadDir(historyDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	type item struct {
		name string
		t    time.Time
	}
	var items []item
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		ts, ok := historyDirTime(e.Name())
		if !ok {
			continue
		}
		items = append(items, item{name: e.Name(), t: ts})
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].t.Equal(items[j].t) {
			return items[i].t.After(items[j].t)
		}
		return items[i].name > items[j].name // same second: higher sequence is newer
	})
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.name)
	}
	return out, nil
}

func applyRunHistoryRetention(historyDir string, retainLast, retainDays int, now time.Time) error {
	dirs, err := historyRetentionCandidates(historyDir, retainLast, retainDays, now)
	if err != nil {
		return err
	}
	for _, d := range dirs {
		if err := os.RemoveAll(filepath.Join(historyDir, d)); err != nil {
			return err
		}
	}
	return nil
}

// historyRetentionCandidates lists the run snapshots retain-last/retain-days
// would delete, without deleting them.
func historyRetentionCandidates(historyDir string, retainLast, retainDays int, now time.Time) ([]string, error) {
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return nil, err
	}
	var out []string
	for i, d := range dirs {
		remove := retainLast > 0 && i >= retainLast
		if retainDays > 0 {
			if ts, ok := historyDirTime(d); ok && int(now.UTC().Sub(ts).Hours()/24) >= retainDays {
				remove = true
			}
		}
		if remove {
			out = append(out, d)
		}
	}
	return out, nil
}

func applyArtifactRetentionPolicies(outputDir string, retainDays, retainMaxFiles int, now time.Time) (int, error) {
	paths, err := artifactRetentionCandidates(outputDir, retainDays, retainMaxFiles, now)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

// artifactRetentionCandidates lists the report files the retention policies
// would delete, without deleting them.
func artifactRetentionCandidates(outputDir string, retainDays, retainMaxFiles int, now time.Time) ([]string, error) {
	if retainDays <= 0 && retainMaxFiles <= 0 {
		return nil, nil
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	protected := map[string]bool{
		"index.html":              true,
		"ncc-report-single.html":  true,
		"run-summary.json":        true,
		"ncc-run-record.json":     true,
		"regression-summary.json": true,
		"checks-snapshot.json":    true,
		"drilldown-diff.json":     true,
		"flaky-checks.json":       true,
		"slo-dashboard.json":      true,
		"policy-gates.txt":        true,
		"excluded-alerts.json":    true,
		// Large runs move the aggregated rows out of index.html into this
		// sidecar; index.html is useless without it.
		"aggregated-data.json": true,
		// Server-side state the api-server keeps in the output directory.
		// Pruning these deleted operator acknowledge/resolve marks and
		// feature-flag overrides after every run.
		"ncc-alert-dispositions.json": true,
		"feature-flags.json":          true,
		// Per-channel notification delivery status (runner + api-server).
		".ncc-notification-status.json":      true,
		".ncc-notification-status.json.lock": true,
	}
	type item struct {
		name string
		path string
		mod  time.Time
	}
	files := make([]item, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if protected[name] || name == ".ncc-writecheck" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, item{
			name: name,
			path: filepath.Join(outputDir, name),
			mod:  info.ModTime(),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	var out []string
	retainDuration := time.Duration(retainDays) * 24 * time.Hour
	for i, f := range files {
		if (retainDays > 0 && now.Sub(f.mod) >= retainDuration) || (retainMaxFiles > 0 && i >= retainMaxFiles) {
			out = append(out, f.path)
		}
	}
	return out, nil
}

func writeRunHistorySnapshot(cfg Config) (string, error) {
	ts := runHistoryTimestamp(time.Now())
	runDir := filepath.Join(cfg.RunHistoryDir, ts)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return "", err
	}
	candidates := []string{
		"index.html",
		// Runs above 20,000 findings keep their rows here instead of in
		// index.html; without it a large run's history snapshot had none.
		"aggregated-data.json",
		"run-summary.json",
		"ncc-run-record.json",
		"regression-summary.json",
		"checks-snapshot.json",
		"drilldown-diff.json",
		"flaky-checks.json",
		"slo-dashboard.json",
	}
	for _, rel := range candidates {
		src := filepath.Join(cfg.OutputDirFiltered, rel)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := copyFile(src, filepath.Join(runDir, rel)); err != nil {
			return "", err
		}
	}
	if err := os.WriteFile(filepath.Join(cfg.RunHistoryDir, "latest.txt"), []byte(ts+"\n"), 0644); err != nil {
		return "", err
	}
	if err := applyRunHistoryRetention(cfg.RunHistoryDir, cfg.RetainLastRuns, cfg.RetainDays, time.Now()); err != nil {
		return "", err
	}
	return runDir, nil
}
