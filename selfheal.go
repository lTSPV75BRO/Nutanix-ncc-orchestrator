package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"goncc/internal/selfsigned"
)

// This file implements the orchestrator's active self-heal subsystem: a small
// registry of checks, each of which detects a class of latent operational fault
// and (when --fix is requested) applies a *safe* remediation. It backs the
// `ncc-orchestrator doctor` command and the api-server's diagnostics endpoint.
//
// Design rules for a check's Run:
//   - Read-only by default. Mutate the system only when hc.Fix is true.
//   - A remediation must be safe to apply unattended (idempotent, reversible or
//     non-destructive). Anything risky should warn with a Hint instead.
//   - Never panic; turn errors into a fail/warn result.

type healStatus string

const (
	healOK   healStatus = "ok"
	healWarn healStatus = "warn"
	healFail healStatus = "fail"
)

// healResult is the outcome of a single check.
type healResult struct {
	ID       string     `json:"id"`
	Title    string     `json:"title"`
	Category string     `json:"category"`
	Status   healStatus `json:"status"`
	Message  string     `json:"message"`
	Hint     string     `json:"hint,omitempty"`
	Fixed    bool       `json:"fixed,omitempty"`
	FixMsg   string     `json:"fix_message,omitempty"`
}

// healContext carries shared, lazily-resolved state into each check.
type healContext struct {
	InstallDir string
	ConfigPath string
	Cfg        Config
	CfgLoaded  bool
	Fix        bool
}

type healCheck struct {
	ID       string
	Title    string
	Category string
	Run      func(hc *healContext) healResult
}

// healReport is the aggregate, JSON-serializable result of a self-heal run.
type healReport struct {
	GeneratedAt string         `json:"generated_at"`
	InstallDir  string         `json:"install_dir"`
	ConfigPath  string         `json:"config_path"`
	FixApplied  bool           `json:"fix_applied"`
	Summary     map[string]int `json:"summary"`
	Results     []healResult   `json:"results"`
}

// Worst returns the most severe status across all results, defaulting to ok.
func (r healReport) Worst() healStatus {
	worst := healOK
	for _, res := range r.Results {
		if res.Status == healFail {
			return healFail
		}
		if res.Status == healWarn {
			worst = healWarn
		}
	}
	return worst
}

// selfHealChecks is the check registry. Order here is the report order.
func selfHealChecks() []healCheck {
	return []healCheck{
		{ID: "config-valid", Title: "Configuration validity", Category: "config", Run: checkConfigValid},
		{ID: "config-output-routing", Title: "Output-dir path routing", Category: "config", Run: checkConfigOutputRouting},
		{ID: "output-dirs-writable", Title: "Output directories writable", Category: "storage", Run: checkOutputDirsWritable},
		{ID: "disk-space", Title: "Free disk space", Category: "storage", Run: checkDiskSpace},
		{ID: "secrets-perms", Title: "Secret file permissions", Category: "encryption", Run: checkSecretsPerms},
		{ID: "backup-staleness", Title: "Backup freshness", Category: "backups", Run: checkBackupStaleness},
		{ID: "backup-restorable", Title: "Newest backup restorable", Category: "backups", Run: checkBackupRestorable},
		{ID: "recent-run-health", Title: "Most recent run health", Category: "runs", Run: checkRecentRunHealth},
		{ID: "run-output-freshness", Title: "Run output freshness", Category: "runs", Run: checkRunOutputFreshness},
		{ID: "tls-cert-expiry", Title: "TLS certificate validity", Category: "tls", Run: checkTLSCertExpiry},
		{ID: "stale-pids", Title: "Stale PID files", Category: "process", Run: checkStalePIDs},
		{ID: "log-sizes", Title: "Log file sizes", Category: "storage", Run: checkLogSizes},
	}
}

func parseCertFile(path string) (*x509.Certificate, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("no PEM certificate data")
	}
	return x509.ParseCertificate(block.Bytes)
}

// checkTLSCertExpiry validates the UI TLS certificate's validity window and,
// for the stack-managed self-signed cert only, auto-renews it under --fix
// (regenerating with the same SANs). Operator-supplied certs are never
// rewritten — they warn so an admin can renew and re-upload.
func checkTLSCertExpiry(hc *healContext) healResult {
	res := healResult{ID: "tls-cert-expiry", Title: "TLS certificate validity", Category: "tls"}
	certPath, keyPath := "", ""
	if st, ok := loadV2StartState(hc.InstallDir); ok && strings.TrimSpace(st.UITLSCertFile) != "" {
		certPath, keyPath = st.UITLSCertFile, st.UITLSKeyFile
	}
	if certPath == "" {
		certPath = filepath.Join(hc.InstallDir, "tls", "ui-selfsigned.crt")
		keyPath = filepath.Join(hc.InstallDir, "tls", "ui-selfsigned.key")
	}
	if !fileExists(certPath) {
		res.Status = healOK
		res.Message = "no UI TLS certificate present (HTTP, or stack not started)"
		return res
	}
	managed := filepath.Base(certPath) == "ui-selfsigned.crt"
	cert, err := parseCertFile(certPath)
	if err != nil {
		res.Status = healWarn
		res.Message = fmt.Sprintf("could not parse %s: %v", filepath.Base(certPath), err)
		return res
	}
	left := time.Until(cert.NotAfter)
	res.Message = fmt.Sprintf("%s valid until %s (%s left)", filepath.Base(certPath), cert.NotAfter.UTC().Format(time.RFC3339), humanDuration(left))
	if left > 30*24*time.Hour {
		res.Status = healOK
		return res
	}
	if managed && hc.Fix {
		hosts := append([]string{}, cert.DNSNames...)
		for _, ip := range cert.IPAddresses {
			hosts = append(hosts, ip.String())
		}
		certPEM, keyPEM, gerr := selfsigned.Generate(hosts, 0)
		if gerr == nil &&
			os.WriteFile(certPath, certPEM, 0o600) == nil &&
			os.WriteFile(keyPath, keyPEM, 0o600) == nil {
			res.Status = healOK
			res.Fixed = true
			res.FixMsg = "regenerated the self-signed UI certificate (same SANs)"
			res.Message = "self-signed certificate renewed"
			res.Hint = "Restart the stack (v2-restart) to load the renewed certificate."
			return res
		}
	}
	if left <= 0 {
		res.Status = healFail
	} else {
		res.Status = healWarn
	}
	if managed {
		res.Hint = "Self-signed UI cert is expiring; re-run with --fix to regenerate it, then restart the stack."
	} else {
		res.Hint = "Operator-supplied cert is expiring; renew it and update TLS settings (cannot be auto-renewed)."
	}
	return res
}

// checkStalePIDs detects pid files whose process is dead (a crashed stack) and,
// under --fix, removes them so a restart isn't blocked by a phantom "running".
func checkStalePIDs(hc *healContext) healResult {
	res := healResult{ID: "stale-pids", Title: "Stale PID files", Category: "process"}
	runDir := filepath.Join(hc.InstallDir, "run")
	pidFiles, _ := filepath.Glob(filepath.Join(runDir, "*.pid"))
	var stale, cleaned []string
	for _, pf := range pidFiles {
		pid, err := readPIDFromFile(pf)
		if err != nil {
			continue
		}
		if processIsAlive(pid) {
			continue
		}
		if hc.Fix {
			if os.Remove(pf) == nil {
				cleaned = append(cleaned, filepath.Base(pf))
				continue
			}
		}
		stale = append(stale, fmt.Sprintf("%s (pid %d dead)", filepath.Base(pf), pid))
	}
	switch {
	case len(stale) == 0 && len(cleaned) == 0:
		res.Status = healOK
		res.Message = "no stale pid files"
	case len(stale) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "removed stale pid file(s): " + strings.Join(cleaned, ", ")
		res.Message = "stale pid files cleaned"
	default:
		res.Status = healWarn
		res.Message = "stale pid file(s): " + strings.Join(stale, ", ")
		res.Hint = "Re-run with --fix to remove them so the stack can restart cleanly."
	}
	return res
}

// checkLogSizes flags oversized log files and, under --fix, rotates them
// (renaming to <name>.1) so an unbounded log can't fill the volume — the
// self-heal cooldown loop in particular can be chatty.
func checkLogSizes(hc *healContext) healResult {
	res := healResult{ID: "log-sizes", Title: "Log file sizes", Category: "storage"}
	logDir := filepath.Join(hc.InstallDir, "logs")
	logs, _ := filepath.Glob(filepath.Join(logDir, "*.log"))
	const cap = int64(50) << 20 // 50 MiB
	var big, rotated []string
	for _, lg := range logs {
		st, err := os.Stat(lg)
		if err != nil || st.Size() < cap {
			continue
		}
		if hc.Fix {
			rot := lg + ".1"
			_ = os.Remove(rot)
			if os.Rename(lg, rot) == nil {
				rotated = append(rotated, filepath.Base(lg))
				continue
			}
		}
		big = append(big, fmt.Sprintf("%s (%s)", filepath.Base(lg), humanBytes(uint64(st.Size()))))
	}
	switch {
	case len(big) == 0 && len(rotated) == 0:
		res.Status = healOK
		res.Message = "all log files under 50 MiB"
	case len(big) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "rotated oversized log(s): " + strings.Join(rotated, ", ")
		res.Message = "oversized logs rotated"
	default:
		res.Status = healWarn
		res.Message = "oversized log file(s): " + strings.Join(big, ", ")
		res.Hint = "Re-run with --fix to rotate them (renames to <name>.1)."
	}
	return res
}

// latestRunSummary reads <output-dir-filtered>/run-summary.json, the
// machine-readable result the orchestrator writes after every run.
func latestRunSummary(hc *healContext) (RunSummaryJSON, string, bool) {
	if !hc.CfgLoaded || strings.TrimSpace(hc.Cfg.OutputDirFiltered) == "" {
		return RunSummaryJSON{}, "", false
	}
	path := filepath.Join(hc.Cfg.OutputDirFiltered, "run-summary.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return RunSummaryJSON{}, path, false
	}
	var sum RunSummaryJSON
	if json.Unmarshal(raw, &sum) != nil {
		return RunSummaryJSON{}, path, false
	}
	return sum, path, true
}

// runClassMitigation describes the recommended remediation for a failure class
// in operator-facing terms (the api-server applies these automatically on a
// bounded auto-retry; here we only advise).
func runClassMitigation(class string) string {
	switch class {
	case "auth":
		return "verify Prism credentials / secret:// source and account lock state (preflight-check)"
	case "rate_limit":
		return "lower --max-parallel and raise --retry-max-attempts (auto-retried once by the api-server)"
	case "timeout", "network":
		return "raise --request-timeout, lower --max-parallel, check routing/firewall (auto-retried once)"
	case "parser":
		return "inspect raw NCC logs under output-dir-logs for unexpected payloads"
	default:
		return "inspect the run log for the failing cluster"
	}
}

func checkRecentRunHealth(hc *healContext) healResult {
	res := healResult{ID: "recent-run-health", Title: "Most recent run health", Category: "runs"}
	sum, _, ok := latestRunSummary(hc)
	if !ok {
		res.Status = healWarn
		res.Message = "no run-summary.json found; no completed run to evaluate"
		res.Hint = "Trigger a run (or wait for the scheduled one) to populate run history."
		return res
	}
	if sum.ClustersFailed == 0 {
		res.Status = healOK
		res.Message = fmt.Sprintf("last run %s: %d cluster(s) OK, 0 failed", sum.Timestamp, sum.ClustersOK)
		return res
	}
	// Identify the dominant failure class to advise the right mitigation.
	dominant, max := "unknown", 0
	for class, n := range sum.FailureClasses {
		if n > max {
			dominant, max = class, n
		}
	}
	res.Message = fmt.Sprintf("last run %s: %d failed / %d OK; dominant cause: %s", sum.Timestamp, sum.ClustersFailed, sum.ClustersOK, dominant)
	res.Hint = runClassMitigation(dominant)
	if sum.ClustersOK == 0 {
		res.Status = healFail // total failure
	} else {
		res.Status = healWarn // partial failure
	}
	return res
}

func checkRunOutputFreshness(hc *healContext) healResult {
	res := healResult{ID: "run-output-freshness", Title: "Run output freshness", Category: "runs"}
	sum, _, ok := latestRunSummary(hc)
	if !ok {
		res.Status = healWarn
		res.Message = "no run-summary.json; cannot assess freshness"
		return res
	}
	ts, err := time.Parse(time.RFC3339, sum.Timestamp)
	if err != nil {
		res.Status = healWarn
		res.Message = "run-summary.json timestamp unparseable: " + sum.Timestamp
		return res
	}
	age := time.Since(ts)
	res.Message = fmt.Sprintf("last run completed %s ago (%s)", humanDuration(age), sum.Timestamp)
	// A scan that hasn't landed in over a day usually means a broken schedule —
	// e.g. cron writing output to the wrong dir (the classic relative-path bug)
	// or the scheduled job not firing at all.
	const staleAfter = 24 * time.Hour
	if age > staleAfter {
		res.Status = healWarn
		res.Hint = "Scheduled runs may not be landing here. Check the cron/Task schedule and that its output-dir-* matches the served stack (doctor --fix anchors relative paths)."
		return res
	}
	res.Status = healOK
	return res
}

// runSelfHeal executes every registered check and returns an aggregate report.
// When fix is true, checks may apply their safe remediations.
func runSelfHeal(installDir, configPath string, fix bool) healReport {
	installDir = strings.TrimSpace(installDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		configPath = resolveDoctorConfigPath(installDir)
	}

	hc := &healContext{InstallDir: installDir, ConfigPath: configPath, Fix: fix}
	// Best-effort config load so downstream checks can read resolved paths.
	if configPath != "" {
		if cfg, err := loadConfigForValidation(configPath); err == nil {
			hc.Cfg = cfg
			hc.CfgLoaded = true
		}
	}

	report := healReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		InstallDir:  installDir,
		ConfigPath:  configPath,
		FixApplied:  fix,
		Summary:     map[string]int{"ok": 0, "warn": 0, "fail": 0},
	}
	for _, c := range selfHealChecks() {
		res := c.Run(hc)
		if res.ID == "" {
			res.ID = c.ID
		}
		if res.Title == "" {
			res.Title = c.Title
		}
		if res.Category == "" {
			res.Category = c.Category
		}
		report.Summary[string(res.Status)]++
		report.Results = append(report.Results, res)
	}
	return report
}

// resolveDoctorConfigPath mirrors v2-start's config resolution: prefer
// <install-dir>/config.yaml, fall back to example_config.yaml.
func resolveDoctorConfigPath(installDir string) string {
	primary := filepath.Join(installDir, "config.yaml")
	if fileExists(primary) {
		return primary
	}
	if alt := filepath.Join(installDir, "example_config.yaml"); fileExists(alt) {
		return alt
	}
	// Also try the current working directory for a bare CLI invocation.
	if fileExists("config.yaml") {
		if abs, err := filepath.Abs("config.yaml"); err == nil {
			return abs
		}
	}
	return primary // report the expected path even if absent
}

func fileExists(p string) bool {
	if strings.TrimSpace(p) == "" {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// ---- checks -------------------------------------------------------------

func checkConfigValid(hc *healContext) healResult {
	res := healResult{ID: "config-valid", Title: "Configuration validity", Category: "config"}
	if !fileExists(hc.ConfigPath) {
		res.Status = healFail
		res.Message = fmt.Sprintf("config file not found at %s", hc.ConfigPath)
		res.Hint = "Run `ncc-orchestrator quickstart` or point --config at a valid file."
		return res
	}
	if _, err := loadConfigForValidation(hc.ConfigPath); err == nil {
		res.Status = healOK
		res.Message = "config parses and validates"
		return res
	} else {
		res.Status = healFail
		res.Message = err.Error()
	}
	if hc.Fix {
		// The one repair that is always safe: strip inline-comment artifacts
		// from known duration keys (a frequent hand-edit mistake).
		if changed, ferr := repairConfigInlineCommentValues(hc.ConfigPath); ferr == nil && changed {
			if cfg, err := loadConfigForValidation(hc.ConfigPath); err == nil {
				hc.Cfg = cfg
				hc.CfgLoaded = true
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "repaired inline-comment artifacts in duration values; config now valid"
				res.Message = "config repaired and validated"
				return res
			}
		}
		res.Hint = "Automatic repair could not make the config valid; run `ncc-orchestrator validate-config --config <file>` for details."
	} else {
		res.Hint = "Re-run with --fix to attempt automatic repair, or `validate-config` for details."
	}
	return res
}

// checkConfigOutputRouting flags relative output-dir-* in the on-disk config —
// a scheduled (cron) run executes with an arbitrary cwd, so relative output
// dirs silently diverge from where the served stack reads results. The loader
// anchors them to the config dir at runtime; this rewrites the file so the
// stored config is unambiguous too.
func checkConfigOutputRouting(hc *healContext) healResult {
	res := healResult{ID: "config-output-routing", Title: "Output-dir path routing", Category: "config"}
	if !fileExists(hc.ConfigPath) {
		res.Status = healWarn
		res.Message = "config absent; skipped"
		return res
	}
	raw, err := os.ReadFile(hc.ConfigPath)
	if err != nil {
		res.Status = healWarn
		res.Message = "could not read config: " + err.Error()
		return res
	}
	rel := relativeOutputDirKeys(string(raw))
	if len(rel) == 0 {
		res.Status = healOK
		res.Message = "output directories are absolute (cron-safe)"
		return res
	}
	res.Status = healWarn
	res.Message = fmt.Sprintf("relative output dir(s) %s resolve against the working directory; scheduled runs may write elsewhere", strings.Join(rel, ", "))
	if !hc.Fix {
		res.Hint = "Re-run with --fix to rewrite them as absolute paths anchored to the config directory."
		return res
	}
	cfgDir := filepath.Dir(hc.ConfigPath)
	newText, changed := absolutizeOutputDirKeys(string(raw), cfgDir)
	if !changed {
		res.Hint = "Could not rewrite paths automatically; edit the config to use absolute output dirs."
		return res
	}
	if err := os.WriteFile(hc.ConfigPath, []byte(newText), 0o600); err != nil {
		res.Hint = "Rewrite failed: " + err.Error()
		return res
	}
	res.Status = healOK
	res.Fixed = true
	res.FixMsg = fmt.Sprintf("rewrote %s to absolute paths under %s", strings.Join(rel, ", "), cfgDir)
	res.Message = "output directories anchored to absolute paths"
	return res
}

func checkOutputDirsWritable(hc *healContext) healResult {
	res := healResult{ID: "output-dirs-writable", Title: "Output directories writable", Category: "storage"}
	if !hc.CfgLoaded {
		res.Status = healWarn
		res.Message = "config not loaded; skipped"
		return res
	}
	dirs := map[string]string{
		"output-dir-logs":     hc.Cfg.OutputDirLogs,
		"output-dir-filtered": hc.Cfg.OutputDirFiltered,
	}
	var problems, fixed []string
	for name, dir := range dirs {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		err := dirWritable(dir)
		if err == nil {
			continue
		}
		if hc.Fix {
			if mkErr := os.MkdirAll(dir, 0o755); mkErr == nil && dirWritable(dir) == nil {
				fixed = append(fixed, fmt.Sprintf("%s (%s)", name, dir))
				continue
			}
		}
		problems = append(problems, fmt.Sprintf("%s (%s): %v", name, dir, err))
	}
	switch {
	case len(problems) == 0 && len(fixed) == 0:
		res.Status = healOK
		res.Message = "output directories exist and are writable"
	case len(problems) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "created/repaired: " + strings.Join(fixed, ", ")
		res.Message = "output directories writable after repair"
	default:
		res.Status = healFail
		res.Message = strings.Join(problems, "; ")
		if !hc.Fix {
			res.Hint = "Re-run with --fix to create missing output directories."
		}
	}
	return res
}

func checkDiskSpace(hc *healContext) healResult {
	res := healResult{ID: "disk-space", Title: "Free disk space", Category: "storage"}
	target := hc.InstallDir
	if hc.CfgLoaded && strings.TrimSpace(hc.Cfg.OutputDirFiltered) != "" {
		if _, err := os.Stat(hc.Cfg.OutputDirFiltered); err == nil {
			target = hc.Cfg.OutputDirFiltered
		}
	}
	free, ok := diskFreeBytes(target)
	if !ok {
		res.Status = healWarn
		res.Message = "could not determine free space for " + target
		return res
	}
	const minFree = uint64(1) << 30 // 1 GiB
	res.Message = fmt.Sprintf("%s free on %s", humanBytes(free), target)
	if free < minFree {
		res.Status = healWarn
		res.Hint = "Low disk space can cause runs to write partial output. Prune old backups/run-history or grow the volume."
		return res
	}
	res.Status = healOK
	return res
}

// checkSecretsPerms flags secret files that are group/world-readable and (with
// --fix) tightens them to 0600. No-op on Windows where POSIX mode bits do not
// carry the same meaning.
func checkSecretsPerms(hc *healContext) healResult {
	res := healResult{ID: "secrets-perms", Title: "Secret file permissions", Category: "encryption"}
	if runtime.GOOS == "windows" {
		res.Status = healOK
		res.Message = "skipped on windows (NTFS ACLs, not POSIX mode bits)"
		return res
	}
	candidates := []string{
		filepath.Join(hc.InstallDir, ".ncc-api-users.json"),
		filepath.Join(hc.InstallDir, ".ncc-api-token"),
		filepath.Join(hc.InstallDir, ".ncc-initial-admin-password"),
	}
	if st, ok := loadV2StartState(hc.InstallDir); ok {
		if strings.TrimSpace(st.UITLSKeyFile) != "" {
			candidates = append(candidates, st.UITLSKeyFile)
		}
	}
	if hc.CfgLoaded && strings.TrimSpace(hc.Cfg.SecretsFile) != "" {
		candidates = append(candidates, hc.Cfg.SecretsFile)
	}
	var loose, fixed []string
	for _, p := range candidates {
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		if info.Mode().Perm()&0o077 == 0 {
			continue // already 0600-ish
		}
		if hc.Fix {
			if chErr := os.Chmod(p, 0o600); chErr == nil {
				fixed = append(fixed, filepath.Base(p))
				continue
			}
		}
		loose = append(loose, fmt.Sprintf("%s (%04o)", filepath.Base(p), info.Mode().Perm()))
	}
	switch {
	case len(loose) == 0 && len(fixed) == 0:
		res.Status = healOK
		res.Message = "secret files are owner-only (0600)"
	case len(loose) == 0:
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "tightened to 0600: " + strings.Join(fixed, ", ")
		res.Message = "secret file permissions repaired"
	default:
		res.Status = healWarn
		res.Message = "group/world-accessible secret files: " + strings.Join(loose, ", ")
		if !hc.Fix {
			res.Hint = "Re-run with --fix to chmod them to 0600."
		}
	}
	return res
}

func checkBackupStaleness(hc *healContext) healResult {
	res := healResult{ID: "backup-staleness", Title: "Backup freshness", Category: "backups"}
	dir := filepath.Join(hc.InstallDir, "backups")
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tar.gz"))
	const staleAfter = 7 * 24 * time.Hour

	if len(matches) == 0 {
		if hc.Fix {
			if out, err := selfHealCreateBackup(hc.InstallDir); err == nil {
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "no backup existed; created and verified " + filepath.Base(out)
				res.Message = "auto-backup created"
				return res
			}
		}
		res.Status = healWarn
		res.Message = "no backups found under " + dir
		res.Hint = "Re-run with --fix to take one now, or create from Settings → Maintenance."
		return res
	}
	var newest time.Time
	var newestName string
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.ModTime().After(newest) {
			newest = info.ModTime()
			newestName = filepath.Base(m)
		}
	}
	age := time.Since(newest)
	res.Message = fmt.Sprintf("%d backup(s); newest %s is %s old", len(matches), newestName, humanDuration(age))
	if age > staleAfter {
		if hc.Fix {
			if out, err := selfHealCreateBackup(hc.InstallDir); err == nil {
				res.Status = healOK
				res.Fixed = true
				res.FixMsg = "newest backup was stale; created and verified " + filepath.Base(out)
				res.Message = "auto-backup created"
				return res
			}
		}
		res.Status = healWarn
		res.Hint = "Latest backup is over 7 days old; re-run with --fix to take a fresh one."
		return res
	}
	res.Status = healOK
	return res
}

// selfHealCreateBackup builds a verified backup into <installDir>/backups and
// prunes to the newest 7. It writes nothing to stdout (the doctor controls
// output) and removes any archive that fails verification.
func selfHealCreateBackup(installDir string) (string, error) {
	entries, _ := collectBackupEntries(installDir)
	if len(entries) == 0 {
		return "", fmt.Errorf("nothing to back up under %s", installDir)
	}
	dir := filepath.Join(installDir, "backups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	out := filepath.Join(dir, fmt.Sprintf("ncc-backup-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z")))
	rels := make([]string, 0, len(entries))
	for _, e := range entries {
		rels = append(rels, e.Rel)
	}
	manifest := backupManifest{
		Tool:       "ncc-orchestrator",
		Version:    Version,
		Stream:     Stream,
		BuildDate:  BuildDate,
		GoVersion:  GoVersion,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		InstallDir: installDir,
		Files:      rels,
		Auth:       summarizeAuthProviders(filepath.Join(installDir, ".ncc-api-users.json")),
	}
	if err := writeBackupArchive(out, manifest, entries); err != nil {
		return "", err
	}
	if _, err := verifyBackupArchive(out); err != nil {
		_ = os.Remove(out)
		return "", fmt.Errorf("verification failed: %w", err)
	}
	_, _ = pruneOldBackups(dir, 7)
	return out, nil
}

// checkBackupRestorable validates the newest backup archive end-to-end so a
// silently-corrupt backup is caught before it is needed for a restore.
func checkBackupRestorable(hc *healContext) healResult {
	res := healResult{ID: "backup-restorable", Title: "Newest backup restorable", Category: "backups"}
	dir := filepath.Join(hc.InstallDir, "backups")
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tar.gz"))
	if len(matches) == 0 {
		res.Status = healWarn
		res.Message = "no backups to validate"
		return res
	}
	var newest string
	var newestMod time.Time
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.ModTime().After(newestMod) {
			newest, newestMod = m, info.ModTime()
		}
	}
	vr, err := verifyBackupArchive(newest)
	if err != nil {
		res.Status = healFail
		res.Message = fmt.Sprintf("%s is not restorable: %v", filepath.Base(newest), err)
		res.Hint = "This backup cannot be restored. Take a fresh backup (`v2-backup`) and investigate the storage."
		return res
	}
	res.Status = healOK
	res.Message = fmt.Sprintf("%s verified: %d file(s), created by %s at %s", filepath.Base(newest), vr.DataFiles, vr.Manifest.Version, vr.Manifest.CreatedAt)
	return res
}

// ---- helpers ------------------------------------------------------------

func dirWritable(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	f, err := os.CreateTemp(dir, ".ncc-heal-probe-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

var outputDirKeyNames = []string{"output-dir-logs", "output-dir-filtered", "run-history-dir"}

// relativeOutputDirKeys returns the output-dir keys whose YAML value is a
// non-empty relative path.
func relativeOutputDirKeys(yaml string) []string {
	var out []string
	for _, line := range strings.Split(yaml, "\n") {
		key, val, ok := parseSimpleYAMLKV(line)
		if !ok || !strInList(outputDirKeyNames, key) {
			continue
		}
		if val != "" && !filepath.IsAbs(val) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// absolutizeOutputDirKeys rewrites relative output-dir-* values to absolute
// paths anchored at base, preserving the rest of each line.
func absolutizeOutputDirKeys(yaml, base string) (string, bool) {
	lines := strings.Split(yaml, "\n")
	changed := false
	for i, line := range lines {
		key, val, ok := parseSimpleYAMLKV(line)
		if !ok || !strInList(outputDirKeyNames, key) || val == "" || filepath.IsAbs(val) {
			continue
		}
		abs := filepath.Join(base, val)
		// Replace just the value token, keeping indentation and any comment.
		idx := strings.Index(line, val)
		if idx < 0 {
			continue
		}
		lines[i] = line[:idx] + abs + line[idx+len(val):]
		changed = true
	}
	return strings.Join(lines, "\n"), changed
}

// parseSimpleYAMLKV parses a `key: value` line, stripping surrounding quotes
// and trailing inline comments from the value. Returns ok=false for blanks,
// comments, and list items.
func parseSimpleYAMLKV(line string) (key, val string, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "-") {
		return "", "", false
	}
	colon := strings.Index(trimmed, ":")
	if colon < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(trimmed[:colon])
	val = strings.TrimSpace(trimmed[colon+1:])
	// Strip an unquoted trailing comment.
	if !strings.HasPrefix(val, "\"") && !strings.HasPrefix(val, "'") {
		if h := strings.Index(val, " #"); h >= 0 {
			val = strings.TrimSpace(val[:h])
		}
	}
	val = strings.Trim(val, "\"'")
	return key, val, true
}

func strInList(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func humanDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}
