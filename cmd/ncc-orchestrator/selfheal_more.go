package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// More self-heal checks: duplicate settings, leftover temporary files, report
// growth and run performance settings. Same rules as selfheal.go: read-only
// unless hc.Fix, and only fixes that are safe unattended.

// checkConfigLayout finds settings the file spells twice (flat and nested, or
// two nested spellings). Only one of them is used, so editing the other one
// silently does nothing.
func checkConfigLayout(hc *healContext) healResult {
	res := healResult{ID: "config-layout", Title: "Settings spelled once", Category: "config"}
	raw, err := os.ReadFile(hc.ConfigPath)
	if err != nil {
		res.Status = healOK
		res.Message = "skipped: config file not readable (see Configuration validity)"
		return res
	}
	_, mig, err := migrateConfigToNested(raw)
	if err != nil {
		res.Status = healOK
		res.Message = "skipped: " + err.Error()
		return res
	}
	if len(mig.Duplicates) == 0 {
		res.Status = healOK
		switch mig.Layout {
		case "flat":
			res.Message = "flat layout; every setting is set once. Updates & backups → Migrations can convert it to the nested layout."
		case "mixed":
			res.Message = fmt.Sprintf("nested layout with %d top-level flat key(s); every setting is set once", len(mig.Moved))
		default:
			res.Message = "every setting is set once"
		}
		return res
	}
	var parts []string
	differ := 0
	for _, d := range mig.Duplicates {
		if d.Differs {
			differ++
			parts = append(parts, fmt.Sprintf("%s (uses %s = %q; ignored: %s)", d.Flat, d.Winner, d.Value, strings.Join(d.Shadowed, ", ")))
		} else {
			parts = append(parts, fmt.Sprintf("%s (%s, same value)", d.Flat, strings.Join(d.Paths, " and ")))
		}
	}
	res.Status = healWarn
	res.Message = fmt.Sprintf("%d setting(s) are set more than once, %d with different values: %s", len(mig.Duplicates), differ, strings.Join(parts, "; "))
	if !hc.Fix {
		res.Hint = "Heal keeps the value the runner uses now and removes the other spelling (the original file is kept as a .bak)."
		return res
	}
	out, err := migrateConfigFile(hc.ConfigPath, false)
	if err != nil {
		res.Hint = "Could not tidy the file: " + err.Error()
		return res
	}
	res.Status = healOK
	res.Fixed = true
	res.FixMsg = fmt.Sprintf("%s; runner settings unchanged; original saved as %s", out.Message, filepath.Base(out.Backup))
	res.Message = "every setting is set once"
	return res
}

// tempFileAge is how old a leftover must be before it is treated as orphaned;
// backup, restore and update finish well within it.
const tempFileAge = 6 * time.Hour

// tempPattern is a prefix NCC gives its temporary files or folders. dirOnly
// patterns never match a file: a backup saved as /tmp/ncc-backup-x.tar.gz is
// the user's, not a leftover staging folder.
type tempPattern struct {
	prefix  string
	dirOnly bool
}

// Temporary files and folders NCC creates. System temp holds the big ones
// (backup staging, restore extraction, update packages).
var (
	systemTempPatterns = []tempPattern{
		{prefix: "ncc-backup-", dirOnly: true},
		{prefix: "ncc-restore-", dirOnly: true},
		{prefix: "ncc-update-package-", dirOnly: true},
		{prefix: "ncc-restore-dec-"}, // decrypted copy of an encrypted backup
		{prefix: "ncc-pc-discover-"},
	}
	installTempPatterns = []tempPattern{
		{prefix: ".ncc-backup-seal-"}, {prefix: ".ncc-update-"}, {prefix: ".ncc-heal-probe-"},
		{prefix: ".ncc-health-"}, {prefix: ".ncc-ready-"}, {prefix: ".ncc-migrate-"},
	}
)

type tempLeftover struct {
	path  string
	bytes int64
}

func pathSize(p string) int64 {
	var total int64
	_ = filepath.WalkDir(p, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

func findTempLeftovers(dirs map[string][]tempPattern, now time.Time) []tempLeftover {
	var out []tempLeftover
	seen := map[string]bool{}
	for dir, patterns := range dirs {
		ents, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range ents {
			name := e.Name()
			match := false
			for _, p := range patterns {
				if strings.HasPrefix(name, p.prefix) && (e.IsDir() || !p.dirOnly) {
					match = true
					break
				}
			}
			if !match {
				continue
			}
			info, err := e.Info()
			if err != nil || now.Sub(info.ModTime()) < tempFileAge {
				continue
			}
			full := filepath.Join(dir, name)
			if seen[full] {
				continue
			}
			seen[full] = true
			out = append(out, tempLeftover{path: full, bytes: pathSize(full)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

func checkTempFiles(hc *healContext) healResult {
	res := healResult{ID: "temp-files", Title: "Leftover temporary files", Category: "storage"}
	dirs := map[string][]tempPattern{
		os.TempDir():                         systemTempPatterns,
		hc.InstallDir:                        installTempPatterns,
		filepath.Join(hc.InstallDir, "logs"): installTempPatterns,
	}
	if hc.ConfigPath != "" {
		dirs[filepath.Dir(hc.ConfigPath)] = installTempPatterns
	}
	if hc.CfgLoaded && hc.Cfg.OutputDirFiltered != "" {
		dirs[hc.Cfg.OutputDirFiltered] = installTempPatterns
	}
	left := findTempLeftovers(dirs, time.Now())
	if len(left) == 0 {
		res.Status = healOK
		res.Message = "no leftovers from interrupted backups, restores or updates"
		return res
	}
	var total, freed int64
	var names, removed []string
	for _, l := range left {
		total += l.bytes
		if hc.Fix && os.RemoveAll(l.path) == nil {
			freed += l.bytes
			removed = append(removed, filepath.Base(l.path))
			continue
		}
		names = append(names, filepath.Base(l.path))
	}
	if len(names) == 0 {
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = fmt.Sprintf("removed %d leftover(s), freed %s: %s", len(removed), humanBytes(uint64(freed)), strings.Join(removed, ", "))
		res.Message = "leftover temporary files removed"
		return res
	}
	res.Status = healWarn
	res.Message = fmt.Sprintf("%d leftover(s) older than %s using %s: %s", len(left), humanDuration(tempFileAge), humanBytes(uint64(total)), strings.Join(names, ", "))
	res.Hint = "Left by an interrupted backup, restore or update. Heal deletes them."
	return res
}

// Report-folder sizes past which unbounded growth is worth a warning.
const (
	outputGrowthBytes = int64(5) << 30
	outputGrowthFiles = 20000
	historyGrowthRuns = 500
	outputWalkCap     = 200000
)

func dirUsage(dir string) (bytes int64, files int, capped bool) {
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		files++
		if info, err := d.Info(); err == nil {
			bytes += info.Size()
		}
		if files >= outputWalkCap {
			capped = true
			return filepath.SkipAll
		}
		return nil
	})
	return
}

// checkOutputRetention applies the configured retention to reports and run
// history that are past it, and warns when nothing limits their growth.
func checkOutputRetention(hc *healContext) healResult {
	res := healResult{ID: "output-retention", Title: "Report and history growth", Category: "storage"}
	if !hc.CfgLoaded {
		res.Status = healOK
		res.Message = "skipped: config did not load (see Configuration validity)"
		return res
	}
	cfg := hc.Cfg
	now := time.Now()
	artifacts, _ := artifactRetentionCandidates(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, now)
	var snapshots []string
	if cfg.RunHistoryEnabled && cfg.RunHistoryDir != "" {
		snapshots, _ = historyRetentionCandidates(cfg.RunHistoryDir, cfg.RetainLastRuns, cfg.RetainDays, now)
	}
	if len(artifacts)+len(snapshots) > 0 {
		if hc.Fix {
			deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, now)
			herr := error(nil)
			if len(snapshots) > 0 {
				herr = applyRunHistoryRetention(cfg.RunHistoryDir, cfg.RetainLastRuns, cfg.RetainDays, now)
			}
			if err == nil && herr == nil {
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = fmt.Sprintf("applied your retention settings: removed %d report file(s) and %d run snapshot(s)", deleted, len(snapshots))
				res.Message = "reports and run history are within retention"
				return res
			}
			res.Hint = fmt.Sprintf("Retention could not finish: %v %v", err, herr)
		}
		res.Status = healWarn
		res.Message = fmt.Sprintf("%d report file(s) and %d run snapshot(s) are past your retention settings", len(artifacts), len(snapshots))
		if res.Hint == "" {
			res.Hint = "Runs prune these when they finish; heal prunes them now with the same rules."
		}
		return res
	}
	bytes, files, capped := dirUsage(cfg.OutputDirFiltered)
	usage := fmt.Sprintf("%s in %d file(s)", humanBytes(uint64(bytes)), files)
	if capped {
		usage = fmt.Sprintf("over %s in more than %d files", humanBytes(uint64(bytes)), files)
	}
	var warns []string
	if cfg.ArtifactRetainDays <= 0 && cfg.ArtifactRetainMaxFiles <= 0 && (bytes > outputGrowthBytes || files > outputGrowthFiles) {
		warns = append(warns, fmt.Sprintf("the report folder holds %s and nothing limits it", usage))
	}
	if cfg.RunHistoryEnabled && cfg.RetainLastRuns <= 0 && cfg.RetainDays <= 0 {
		if runs, _ := listHistoryRunDirs(cfg.RunHistoryDir); len(runs) > historyGrowthRuns {
			warns = append(warns, fmt.Sprintf("run history keeps all %d snapshots", len(runs)))
		}
	}
	if len(warns) > 0 {
		res.Status = healWarn
		res.Message = strings.Join(warns, "; ")
		res.Hint = "Set artifact-retain-days (reports) and retain-last or retain-days (run history) under Clusters & config → Reports & history."
		return res
	}
	res.Status = healOK
	res.Message = "report folder: " + usage
	return res
}

func tuningDuration(settings map[string]string, key string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(settings[key]); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// shortDuration prints 20m rather than 20m0s.
func shortDuration(d time.Duration) string {
	s := d.String()
	s = strings.TrimSuffix(s, "m0s")
	if s != d.String() {
		s += "m"
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

func tuningInt(settings map[string]string, key string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(settings[key]))
	return n, err == nil
}

// runtimeTuningFindings lists settings that slow runs down or make them fail
// sooner than they need to. Each names the current and a suggested value.
func runtimeTuningFindings(settings map[string]string) []string {
	var out []string
	timeout := tuningDuration(settings, "timeout", defaultTimeout)
	req := tuningDuration(settings, "request-timeout", defaultRequestTimeout)
	if req >= timeout {
		out = append(out, fmt.Sprintf("request-timeout (%s) is not shorter than the per-cluster timeout (%s), so one stuck request uses the whole budget; try %s", shortDuration(req), shortDuration(timeout), shortDuration(defaultRequestTimeout)))
	}
	if poll := tuningDuration(settings, "poll-interval", defaultPollInterval); poll < 5*time.Second {
		out = append(out, fmt.Sprintf("poll-interval %s polls every cluster very often and adds load on Prism; %s is usually enough", shortDuration(poll), shortDuration(defaultPollInterval)))
	}
	base := tuningDuration(settings, "retry-base-delay", defaultRetryBaseDelay)
	maxDelay := tuningDuration(settings, "retry-max-delay", defaultRetryMaxDelay)
	if maxDelay < base {
		out = append(out, fmt.Sprintf("retry-max-delay (%s) is below retry-base-delay (%s), so backoff never grows; raise it to at least %s", shortDuration(maxDelay), shortDuration(base), shortDuration(base*4)))
	}
	if n, ok := tuningInt(settings, "retry-max-attempts"); ok && n > 10 {
		out = append(out, fmt.Sprintf("retry-max-attempts %d keeps retrying a dead cluster for a long time; 6 is the default", n))
	}
	if n, ok := tuningInt(settings, "max-conns-per-host"); ok && n == 1 {
		out = append(out, "max-conns-per-host 1 sends one request at a time to each cluster; use 0 (no limit) or at least 4")
	}
	if strings.EqualFold(strings.TrimSpace(settings["log-http"]), "true") {
		out = append(out, "log-http logs every API call, which slows runs and fills the disk; turn it off unless you are debugging")
	}
	switch strings.ToLower(strings.TrimSpace(settings["log-level"])) {
	case "trace", "debug":
		out = append(out, fmt.Sprintf("log-level %s is meant for debugging and makes logs much larger; use info", settings["log-level"]))
	}
	return out
}

func checkRuntimeTuning(hc *healContext) healResult {
	res := healResult{ID: "runtime-tuning", Title: "Run performance settings", Category: "performance"}
	raw, err := os.ReadFile(hc.ConfigPath)
	if err != nil {
		res.Status = healOK
		res.Message = "skipped: config file not readable (see Configuration validity)"
		return res
	}
	settings, err := effectiveConfigSettings(raw)
	if err != nil {
		res.Status = healOK
		res.Message = "skipped: config is not valid YAML"
		return res
	}
	if found := runtimeTuningFindings(settings); len(found) > 0 {
		res.Status = healWarn
		res.Message = strings.Join(found, "; ")
		res.Hint = "Change these under Clusters & config → Run behavior or Logging. Self-heal does not change run settings for you."
		return res
	}
	res.Status = healOK
	res.Message = fmt.Sprintf("timeouts, polling, retries and logging are balanced (timeout %s, request %s, poll %s)",
		shortDuration(tuningDuration(settings, "timeout", defaultTimeout)), shortDuration(tuningDuration(settings, "request-timeout", defaultRequestTimeout)), shortDuration(tuningDuration(settings, "poll-interval", defaultPollInterval)))
	return res
}
