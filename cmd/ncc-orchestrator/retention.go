package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"goncc/internal/plural"
)

// Retention covers three kinds of files:
//
//	runs      run-history snapshots (retain-last, retain-days)
//	artifacts generated report files in output-dir-filtered
//	          (artifact-retain-days, artifact-retain-max-files)
//	raw-logs  raw NCC output per cluster in output-dir-logs (raw-log-retain-days)
//
// buildRetentionPlan lists what each rule would delete; the retention
// command prints it (--dry-run) or applies it, and the API server's
// /api/v1/settings/retention/preview returns the same plan as JSON.

// retentionItem is one file or run folder a rule would delete.
type retentionItem struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	Bytes    int64  `json:"bytes"`
	Modified string `json:"modified,omitempty"`
}

// retentionRule is one rule's settings and the items past it.
type retentionRule struct {
	ID       string          `json:"id"`
	Title    string          `json:"title"`
	Dir      string          `json:"dir"`
	Enabled  bool            `json:"enabled"`
	Settings map[string]int  `json:"settings"`
	Count    int             `json:"count"`
	Bytes    int64           `json:"bytes"`
	Items    []retentionItem `json:"items"`
	Error    string          `json:"error,omitempty"`
}

// retentionPlan is the result of the retention command, dry run or not.
type retentionPlan struct {
	DryRun      bool            `json:"dry_run"`
	GeneratedAt string          `json:"generated_at"`
	Rules       []retentionRule `json:"rules"`
	TotalCount  int             `json:"total_count"`
	TotalBytes  int64           `json:"total_bytes"`
	Deleted     int             `json:"deleted,omitempty"`
	Errors      []string        `json:"errors,omitempty"`
}

// rawLogRetentionCandidates lists the raw NCC logs older than retainDays in
// logDir, without deleting them. It never lists the newest log (the latest
// run's output), nor the log of any cluster in the latest report
// (run-summary.json in reportDir), nor anything when logDir is the report
// folder itself. retainDays <= 0 keeps everything.
func rawLogRetentionCandidates(logDir, reportDir string, retainDays int, now time.Time) ([]string, error) {
	if retainDays <= 0 || strings.TrimSpace(logDir) == "" {
		return nil, nil
	}
	if reportDir != "" && sameAbsDir(logDir, reportDir) {
		return nil, nil
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	keep := latestReportRawLogs(reportDir)
	type item struct {
		path string
		mod  time.Time
	}
	files := make([]item, 0, len(entries))
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, ".") || !strings.EqualFold(filepath.Ext(name), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, item{path: filepath.Join(logDir, name), mod: info.ModTime()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	cutoff := time.Duration(retainDays) * 24 * time.Hour
	var out []string
	for i, f := range files {
		if i == 0 { // newest run's log
			continue
		}
		if keep[strings.ToLower(filepath.Base(f.path))] {
			continue
		}
		if now.Sub(f.mod) >= cutoff {
			out = append(out, f.path)
		}
	}
	return out, nil
}

// latestReportRawLogs returns the raw log names (lower case) of the clusters
// in the latest report's run-summary.json.
func latestReportRawLogs(reportDir string) map[string]bool {
	keep := map[string]bool{}
	if strings.TrimSpace(reportDir) == "" {
		return keep
	}
	b, err := os.ReadFile(filepath.Join(reportDir, "run-summary.json"))
	if err != nil {
		return keep
	}
	var sum RunSummaryJSON
	if json.Unmarshal(b, &sum) != nil {
		return keep
	}
	add := func(cluster string) {
		if c := strings.TrimSpace(cluster); c != "" {
			keep[strings.ToLower(c+".log")] = true
		}
	}
	for _, c := range sum.Clusters {
		add(c.Address)
	}
	for _, c := range sum.FailedClusters {
		add(c)
	}
	return keep
}

func sameAbsDir(a, b string) bool {
	aa, err1 := filepath.Abs(a)
	bb, err2 := filepath.Abs(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return sameDir(aa, bb)
}

func applyRawLogRetention(logDir, reportDir string, retainDays int, now time.Time) (int, error) {
	paths, err := rawLogRetentionCandidates(logDir, reportDir, retainDays, now)
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

func retentionItemFor(path string, isDir bool) retentionItem {
	it := retentionItem{Path: path, Name: filepath.Base(path)}
	if info, err := os.Stat(path); err == nil {
		it.Modified = info.ModTime().UTC().Format(time.RFC3339)
		if isDir {
			b, _, _ := dirUsage(path)
			it.Bytes = b
		} else {
			it.Bytes = info.Size()
		}
	}
	return it
}

// buildRetentionPlan lists what every retention rule in cfg would delete now.
// It deletes nothing.
func buildRetentionPlan(cfg Config, now time.Time) retentionPlan {
	plan := retentionPlan{DryRun: true, GeneratedAt: now.UTC().Format(time.RFC3339)}
	add := func(r retentionRule, paths []string, isDir bool, err error) {
		r.Items = []retentionItem{}
		if err != nil {
			r.Error = err.Error()
			plan.Errors = append(plan.Errors, fmt.Sprintf("%s: %v", r.ID, err))
		}
		for _, p := range paths {
			it := retentionItemFor(p, isDir)
			r.Items = append(r.Items, it)
			r.Bytes += it.Bytes
		}
		r.Count = len(r.Items)
		plan.TotalCount += r.Count
		plan.TotalBytes += r.Bytes
		plan.Rules = append(plan.Rules, r)
	}

	runs := retentionRule{
		ID: "runs", Title: "Run history", Dir: cfg.RunHistoryDir,
		Enabled:  cfg.RunHistoryEnabled && cfg.RunHistoryDir != "" && (cfg.RetainLastRuns > 0 || cfg.RetainDays > 0),
		Settings: map[string]int{"retain-last": cfg.RetainLastRuns, "retain-days": cfg.RetainDays},
	}
	var runPaths []string
	var runErr error
	if runs.Enabled {
		var names []string
		names, runErr = historyRetentionCandidates(cfg.RunHistoryDir, cfg.RetainLastRuns, cfg.RetainDays, now)
		for _, n := range names {
			runPaths = append(runPaths, filepath.Join(cfg.RunHistoryDir, n))
		}
	}
	add(runs, runPaths, true, runErr)

	arts := retentionRule{
		ID: "artifacts", Title: "Report files", Dir: cfg.OutputDirFiltered,
		Enabled:  cfg.ArtifactRetainDays > 0 || cfg.ArtifactRetainMaxFiles > 0,
		Settings: map[string]int{"artifact-retain-days": cfg.ArtifactRetainDays, "artifact-retain-max-files": cfg.ArtifactRetainMaxFiles},
	}
	artPaths, artErr := artifactRetentionCandidates(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, now)
	add(arts, artPaths, false, artErr)

	raw := retentionRule{
		ID: "raw-logs", Title: "Raw NCC logs", Dir: cfg.OutputDirLogs,
		Enabled:  cfg.RawLogRetainDays > 0,
		Settings: map[string]int{"raw-log-retain-days": cfg.RawLogRetainDays},
	}
	rawPaths, rawErr := rawLogRetentionCandidates(cfg.OutputDirLogs, cfg.OutputDirFiltered, cfg.RawLogRetainDays, now)
	add(raw, rawPaths, false, rawErr)
	return plan
}

// applyRetentionPlan deletes the items in plan and records how many went.
func applyRetentionPlan(plan *retentionPlan) {
	plan.DryRun = false
	for _, r := range plan.Rules {
		for _, it := range r.Items {
			var err error
			if r.ID == "runs" {
				err = os.RemoveAll(it.Path)
			} else {
				err = os.Remove(it.Path)
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				plan.Errors = append(plan.Errors, fmt.Sprintf("%s: %v", it.Path, err))
				continue
			}
			plan.Deleted++
		}
	}
}

func retentionSettingsText(r retentionRule) string {
	keys := make([]string, 0, len(r.Settings))
	for k := range r.Settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, r.Settings[k]))
	}
	return strings.Join(parts, ", ")
}

func printRetentionPlan(plan retentionPlan) {
	if plan.DryRun {
		fmt.Println("Retention preview (dry run): nothing was deleted.")
	} else {
		fmt.Println("Retention applied.")
	}
	for _, r := range plan.Rules {
		fmt.Printf("\n%s (%s)\n", r.Title, retentionSettingsText(r))
		if r.Dir != "" {
			fmt.Printf("  Folder: %s\n", r.Dir)
		}
		switch {
		case r.Error != "":
			fmt.Printf("  Could not check: %s\n", r.Error)
		case !r.Enabled:
			fmt.Println("  Off: keeps everything.")
		case r.Count == 0:
			fmt.Println("  Nothing past the limit.")
		default:
			for _, it := range r.Items {
				mod := it.Modified
				if len(mod) >= 10 {
					mod = mod[:10]
				}
				fmt.Printf("  %10s  %s  %s\n", humanBytes(uint64(it.Bytes)), mod, it.Path)
			}
			fmt.Printf("  %s, %s\n", plural.Count(r.Count, "item"), humanBytes(uint64(r.Bytes)))
		}
	}
	verb := "would be removed"
	if !plan.DryRun {
		verb = "removed"
	}
	fmt.Printf("\nTotal: %s, %s %s.\n", plural.Count(plan.TotalCount, "item"), humanBytes(uint64(plan.TotalBytes)), verb)
	for _, e := range plan.Errors {
		fmt.Printf("Error: %s\n", e)
	}
}

// loadRetentionConfig reads cfgPath and applies the folder overrides. It
// needs no cluster credentials (no password, no secrets), only a config that
// loads.
func loadRetentionConfig(cfgPath string, overrides map[string]string) (Config, error) {
	if _, err := os.Stat(cfgPath); err != nil {
		return Config{}, fmt.Errorf("config path %s: %w", cfgPath, err)
	}
	viper.Set("config", cfgPath)
	for k, v := range overrides {
		if strings.TrimSpace(v) != "" {
			viper.Set(k, v)
		}
	}
	cfg, err := bindConfig()
	if err != nil {
		return Config{}, fmt.Errorf("configuration: %w", err)
	}
	for name, v := range map[string]int{
		"retain-last": cfg.RetainLastRuns, "retain-days": cfg.RetainDays,
		"artifact-retain-days": cfg.ArtifactRetainDays, "artifact-retain-max-files": cfg.ArtifactRetainMaxFiles,
		"raw-log-retain-days": cfg.RawLogRetainDays,
	} {
		if v < 0 {
			return Config{}, fmt.Errorf("%s must be >= 0", name)
		}
	}
	return cfg, nil
}

func runRetentionCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		return exitConfig(errors.New("--config is required"))
	}
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	asJSON, _ := cmd.Flags().GetBool("json")
	overrides := map[string]string{}
	for _, f := range []string{"output-dir-filtered", "output-dir-logs", "run-history-dir"} {
		if v, _ := cmd.Flags().GetString(f); strings.TrimSpace(v) != "" {
			overrides[f] = v
		}
	}
	cfg, err := loadRetentionConfig(cfgPath, overrides)
	if err != nil {
		return exitConfig(err)
	}
	plan := buildRetentionPlan(cfg, time.Now())
	if !dryRun {
		applyRetentionPlan(&plan)
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}
	printRetentionPlan(plan)
	if !dryRun && len(plan.Errors) > 0 {
		return fmt.Errorf("retention finished with %s", plural.Count(len(plan.Errors), "error"))
	}
	return nil
}

func newRetentionCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "retention",
		Short: "Show or apply the retention rules for run history, report files and raw NCC logs",
		Long: `retention applies retain-last, retain-days, artifact-retain-days,
artifact-retain-max-files and raw-log-retain-days now, the same way a run
does when it finishes. With --dry-run it only lists what would be removed,
with sizes and totals.`,
		Example: `  ncc-orchestrator retention --config config.yaml --dry-run
  ncc-orchestrator retention --config config.yaml --dry-run --json
  ncc-orchestrator retention --config config.yaml`,
		RunE: runRetentionCommand,
	}
	c.Flags().Bool("dry-run", false, "List what would be removed without deleting anything")
	c.Flags().Bool("json", false, "Print the result as JSON")
	c.Flags().String("output-dir-filtered", "", "Report folder (default: from config)")
	c.Flags().String("output-dir-logs", "", "Raw NCC log folder (default: from config)")
	c.Flags().String("run-history-dir", "", "Run history folder (default: from config)")
	return c
}
