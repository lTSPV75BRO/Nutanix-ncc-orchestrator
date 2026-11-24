package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

type preflightCheck struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	Title           string `json:"title"`
	Message         string `json:"message"`
	RemediationCode string `json:"remediation_code,omitempty"`
	Hint            string `json:"hint,omitempty"`
	Output          string `json:"output,omitempty"`
}

type preflightReport struct {
	OK              bool             `json:"ok"`
	Failed          int              `json:"failed"`
	Warn            int              `json:"warn"`
	ConfigPath      string           `json:"config_path"`
	Checks          []preflightCheck `json:"checks"`
	ActionableHints []string         `json:"actionableHints"`
}

func defaultPreflightRemediationCode(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "NCC_PREFLIGHT_GENERIC"
	}
	replacer := strings.NewReplacer(".", "_", "-", "_", "/", "_", " ", "_")
	return "NCC_PREFLIGHT_" + strings.ToUpper(replacer.Replace(id))
}

func preflightProbePath(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	// Keep a persistent probe sentinel and validate RW access on each preflight run.
	probe := filepath.Join(path, ".ncc-preflight-check")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	// Write one byte at offset 0 (fixed-size probe), then read it back to verify RW.
	if _, err := f.WriteAt([]byte{'1'}, 0); err != nil {
		return err
	}
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if buf[0] != '1' {
		return fmt.Errorf("preflight probe verification failed for %s", probe)
	}
	// Backward-compat cleanup for older typo probe file name.
	legacyProbe := filepath.Join(path, ".ncc-prefight-check")
	if err := os.Remove(legacyProbe); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func preflightResolveClusterTarget(cluster string) error {
	target, err := normalizeClusterAddress(cluster)
	if err != nil {
		return err
	}
	if net.ParseIP(target) != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, target)
	if err != nil {
		return err
	}
	if len(addrs) == 0 {
		return errors.New("resolver returned no addresses")
	}
	return nil
}

func buildPreflightReport(cfgPath string) preflightReport {
	report := preflightReport{
		OK:         true,
		ConfigPath: cfgPath,
		Checks:     []preflightCheck{},
	}
	hints := []string{}
	add := func(c preflightCheck) {
		if c.Status != "pass" && strings.TrimSpace(c.RemediationCode) == "" {
			c.RemediationCode = defaultPreflightRemediationCode(c.ID)
		}
		report.Checks = append(report.Checks, c)
		switch c.Status {
		case "fail":
			report.Failed++
			report.OK = false
			if strings.TrimSpace(c.Hint) != "" {
				hints = append(hints, c.Hint)
			}
		case "warn":
			report.Warn++
		}
	}

	if strings.TrimSpace(cfgPath) == "" {
		add(preflightCheck{
			ID:      "validate-config",
			Status:  "warn",
			Title:   "validate-config",
			Message: "config path not provided; file-based preflight checks skipped",
			Hint:    "Run with --config to enable full preflight checks.",
		})
		report.ActionableHints = dedupeStringsKeepOrder(hints)
		return report
	}

	var loadedCfg Config
	cfgLoaded := false
	if cfg, err := loadConfigForValidation(cfgPath); err != nil {
		add(preflightCheck{
			ID:      "validate-config",
			Status:  "fail",
			Title:   "validate-config",
			Message: err.Error(),
			Hint:    "Fix configuration schema/values and rerun preflight.",
		})
	} else {
		loadedCfg = cfg
		cfgLoaded = true
		add(preflightCheck{ID: "validate-config", Status: "pass", Title: "validate-config", Message: "config is valid"})
		schemaCheck := checkConfigSchema(&healContext{ConfigPath: cfgPath})
		schemaStatus := "pass"
		switch schemaCheck.Status {
		case healWarn:
			schemaStatus = "warn"
		case healFail:
			schemaStatus = "fail"
		}
		add(preflightCheck{
			ID: schemaCheck.ID, Status: schemaStatus, Title: schemaCheck.Title,
			Message: schemaCheck.Message, Hint: schemaCheck.Hint,
		})
		dirs := []struct {
			id    string
			path  string
			title string
			hint  string
		}{
			{"path.output-dir-logs", cfg.OutputDirLogs, "Output logs path permission", "Grant write permission to output-dir-logs."},
			{"path.output-dir-filtered", cfg.OutputDirFiltered, "Output filtered path permission", "Grant write permission to output-dir-filtered."},
			{"path.log-file-dir", filepath.Dir(cfg.LogFile), "Log file directory permission", "Grant write permission to log-file directory."},
		}
		if cfg.PromEnabled {
			dirs = append(dirs, struct {
				id    string
				path  string
				title string
				hint  string
			}{"path.prom-dir", cfg.PromDir, "Prometheus path permission", "Grant write permission to prom-dir."})
		}
		if cfg.RunHistoryEnabled {
			dirs = append(dirs, struct {
				id    string
				path  string
				title string
				hint  string
			}{"path.run-history-dir", cfg.RunHistoryDir, "Run history path permission", "Grant write permission to run-history-dir."})
		}
		for _, d := range dirs {
			if err := preflightProbePath(d.path); err != nil {
				add(preflightCheck{ID: d.id, Status: "fail", Title: d.title, Message: err.Error(), Hint: d.hint})
			} else {
				add(preflightCheck{ID: d.id, Status: "pass", Title: d.title, Message: d.path})
			}
		}
		// Additional file-level checks for optional inputs when configured.
		checkFile := func(id string, title string, p string, required bool) {
			path := strings.TrimSpace(p)
			if path == "" {
				if required {
					add(preflightCheck{
						ID:      id,
						Status:  "fail",
						Title:   title,
						Message: "required file path is empty",
						Hint:    "Set this file path in config.",
					})
				} else {
					add(preflightCheck{ID: id, Status: "warn", Title: title, Message: "not set"})
				}
				return
			}
			st, err := os.Stat(path)
			if err != nil {
				add(preflightCheck{
					ID:      id,
					Status:  "fail",
					Title:   title,
					Message: err.Error(),
					Hint:    "Create the file or fix the configured path.",
				})
				return
			}
			if st.IsDir() {
				add(preflightCheck{
					ID:      id,
					Status:  "fail",
					Title:   title,
					Message: "path points to a directory",
					Hint:    "Use a regular file path.",
				})
				return
			}
			add(preflightCheck{ID: id, Status: "pass", Title: title, Message: path})
		}
		checkFile("file.clusters-file", "Clusters file", cfg.ClustersFile, false)
		checkFile("file.exclude-alert-titles-file", "Exclude alert titles file", cfg.ExcludeAlertTitlesFile, false)
		checkFile("file.secrets-file", "Secrets file", cfg.SecretsFile, strings.EqualFold(strings.TrimSpace(cfg.SecretsProvider), "file"))
		for i, cluster := range cfg.Clusters {
			id := fmt.Sprintf("cluster.target.%d", i+1)
			title := fmt.Sprintf("Cluster target %d resolution", i+1)
			if err := preflightResolveClusterTarget(cluster); err != nil {
				add(preflightCheck{
					ID:      id,
					Status:  "fail",
					Title:   title,
					Message: fmt.Sprintf("%s: %v", cluster, err),
					Hint:    "Use a reachable IP/FQDN, or verify DNS/network from this runtime.",
				})
			} else {
				add(preflightCheck{ID: id, Status: "pass", Title: title, Message: cluster})
			}
		}
		// Security posture advisories.
		if cfg.InsecureSkipVerify {
			add(preflightCheck{
				ID:      "safety.insecure-skip-verify",
				Status:  "warn",
				Title:   "TLS verification disabled",
				Message: "insecure-skip-verify=true",
				Hint:    "Use false in production and install valid certificates.",
			})
		}
		if cfg.LogHTTP {
			add(preflightCheck{
				ID:      "safety.log-http",
				Status:  "warn",
				Title:   "HTTP request/response logging enabled",
				Message: "log-http=true",
				Hint:    "Disable log-http for production runs to reduce sensitive output exposure.",
			})
		}
		if cfg.MaxParallel > 20 {
			add(preflightCheck{
				ID:      "safety.max-parallel",
				Status:  "warn",
				Title:   "High max-parallel setting",
				Message: fmt.Sprintf("max-parallel=%d", cfg.MaxParallel),
				Hint:    "Lower max-parallel if you hit API rate limits or network instability.",
			})
		}
	}

	var (
		secretRefs int
		provider   string
		secErr     error
	)
	if cfgLoaded {
		secretRefs, provider, secErr = validateSecretsWithConfig(cfgPath, loadedCfg)
	} else {
		secretRefs, provider, secErr = validateSecretsForPath(cfgPath)
	}
	if secErr != nil {
		add(preflightCheck{
			ID:      "validate-secrets",
			Status:  "fail",
			Title:   "validate-secrets",
			Message: secErr.Error(),
			Hint:    "Set secrets-provider and ensure secret sources are accessible.",
		})
	} else if secretRefs == 0 {
		add(preflightCheck{ID: "validate-secrets", Status: "warn", Title: "validate-secrets", Message: "no secret:// references found"})
	} else {
		add(preflightCheck{ID: "validate-secrets", Status: "pass", Title: "validate-secrets", Message: fmt.Sprintf("refs=%d provider=%s", secretRefs, provider)})
	}

	report.ActionableHints = dedupeStringsKeepOrder(hints)
	return report
}

func runPreflightCheckCommand(cmd *cobra.Command, args []string) error {
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	cfgPath, _ := cmd.Flags().GetString("config")
	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "json"
	}

	report := buildPreflightReport(cfgPath)
	data, _ := json.MarshalIndent(report, "", "  ")
	if format == "json" {
		fmt.Println(string(data))
		return nil
	}
	return exitConfig(fmt.Errorf("unsupported format: %s (supported: json)", format))
}

type automationLevel string

const (
	automationLevelAdvisory automationLevel = "advisory"
	automationLevelSafeFix  automationLevel = "safe-fix"
	automationLevelFullAuto automationLevel = "full-auto"
)

func parseAutomationLevel(raw string) (automationLevel, error) {
	v := automationLevel(strings.ToLower(strings.TrimSpace(raw)))
	if v == "" {
		return automationLevelSafeFix, nil
	}
	switch v {
	case automationLevelAdvisory, automationLevelSafeFix, automationLevelFullAuto:
		return v, nil
	default:
		return "", fmt.Errorf("invalid automation-level %q (valid: advisory, safe-fix, full-auto)", raw)
	}
}

func recommendationForPreflightCheck(c preflightCheck) string {
	msg := strings.TrimSpace(c.Message)
	switch c.ID {
	case "validate-config":
		return "Fix config schema/values, then rerun with --auto for guided retries."
	case "validate-secrets":
		return "Set secrets-provider and verify secret sources are reachable."
	case "path.output-dir-logs", "path.output-dir-filtered", "path.log-file-dir", "path.prom-dir", "path.run-history-dir":
		return "Grant write permissions or let safe automation create/repair directory permissions."
	case "file.clusters-file", "file.exclude-alert-titles-file", "file.secrets-file":
		return "Create the configured file or update config path; safe automation can create a starter file."
	}
	if strings.Contains(strings.ToLower(msg), "permission") {
		return "Check file ownership and writable permissions for configured paths."
	}
	return "Review check output and apply the listed hint."
}

func printAutomationRunbook(report preflightReport, level automationLevel) {
	if report.Failed == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "Automation runbook:")
	for _, c := range report.Checks {
		if c.Status != "fail" {
			continue
		}
		code := c.RemediationCode
		if strings.TrimSpace(code) == "" {
			code = defaultPreflightRemediationCode(c.ID)
		}
		fmt.Fprintf(os.Stderr, "- [%s] %s\n", code, recommendationForPreflightCheck(c))
	}
	if level == automationLevelAdvisory {
		fmt.Fprintln(os.Stderr, "Next step: re-run with --auto --automation-level safe-fix to allow automatic safe repairs.")
	}
}

func applySafePreflightFixes(cfg Config, cfgPath string, report preflightReport, level automationLevel) (int, []string) {
	if level == automationLevelAdvisory {
		return 0, nil
	}
	fixed := 0
	actions := []string{}
	fixPath := func(id string, path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if err := os.MkdirAll(path, 0o755); err == nil {
			fixed++
			actions = append(actions, fmt.Sprintf("%s -> ensured dir %s", id, path))
		}
	}
	fixFile := func(id string, path string, content string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return
		}
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if err := os.WriteFile(path, []byte(content), 0o600); err == nil {
				fixed++
				actions = append(actions, fmt.Sprintf("%s -> created file %s", id, path))
			}
		}
	}
	for _, c := range report.Checks {
		if c.Status != "fail" {
			continue
		}
		switch c.ID {
		case "validate-config":
			if strings.TrimSpace(cfgPath) != "" {
				if _, err := os.Stat(cfgPath); errors.Is(err, os.ErrNotExist) {
					if err := writeDummyConfig(cfgPath); err == nil {
						fixed++
						actions = append(actions, fmt.Sprintf("%s -> generated starter config %s", c.ID, cfgPath))
					}
				}
			}
		case "path.output-dir-logs":
			fixPath(c.ID, cfg.OutputDirLogs)
		case "path.output-dir-filtered":
			fixPath(c.ID, cfg.OutputDirFiltered)
		case "path.log-file-dir":
			fixPath(c.ID, filepath.Dir(cfg.LogFile))
		case "path.prom-dir":
			fixPath(c.ID, cfg.PromDir)
		case "path.run-history-dir":
			fixPath(c.ID, cfg.RunHistoryDir)
		case "file.clusters-file":
			fixFile(c.ID, cfg.ClustersFile, "# one cluster per line\n")
		case "file.exclude-alert-titles-file":
			fixFile(c.ID, cfg.ExcludeAlertTitlesFile, "# one alert title per line\n")
		case "file.secrets-file":
			fixFile(c.ID, cfg.SecretsFile, "{}\n")
		case "validate-secrets":
			if strings.TrimSpace(cfgPath) != "" {
				b, err := os.ReadFile(cfgPath)
				if err == nil {
					content := string(b)
					updated := upsertYAMLScalar(content, "secrets-provider", "env")
					if updated != content {
						if err := os.WriteFile(cfgPath, []byte(updated), 0644); err == nil {
							fixed++
							actions = append(actions, fmt.Sprintf("%s -> set secrets-provider to %q in %s", c.ID, "env", cfgPath))
						}
					}
				}
			}
		}
	}
	return fixed, actions
}

func applyFullAutoRuntimeTuning(cfg *Config) []string {
	if cfg == nil {
		return nil
	}
	changes := []string{}
	if cfg.MaxParallel > 12 || cfg.MaxParallel == 0 { // 0 = auto, which can go far higher
		old := cfg.MaxParallel
		cfg.MaxParallel = 12
		changes = append(changes, fmt.Sprintf("max-parallel %d -> %d", old, cfg.MaxParallel))
	}
	if cfg.Timeout < 20*time.Minute {
		old := cfg.Timeout
		cfg.Timeout = 20 * time.Minute
		changes = append(changes, fmt.Sprintf("timeout %s -> %s", old, cfg.Timeout))
	}
	if cfg.RequestTimeout < 30*time.Second {
		old := cfg.RequestTimeout
		cfg.RequestTimeout = 30 * time.Second
		changes = append(changes, fmt.Sprintf("request-timeout %s -> %s", old, cfg.RequestTimeout))
	}
	if cfg.RetryMaxAttempts < 8 {
		old := cfg.RetryMaxAttempts
		cfg.RetryMaxAttempts = 8
		changes = append(changes, fmt.Sprintf("retry-max-attempts %d -> %d", old, cfg.RetryMaxAttempts))
	}
	if cfg.RetryCircuitBreaker < 4 {
		old := cfg.RetryCircuitBreaker
		cfg.RetryCircuitBreaker = 4
		changes = append(changes, fmt.Sprintf("retry-circuit-breaker %d -> %d", old, cfg.RetryCircuitBreaker))
	}
	if !cfg.AdaptiveParallelism {
		cfg.AdaptiveParallelism = true
		changes = append(changes, "adaptive-parallelism false -> true")
	}
	return changes
}
