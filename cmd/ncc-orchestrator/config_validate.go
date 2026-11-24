package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// ==================== Validation Functions ====================

// validateClusterAddress validates cluster IP or hostname
func validateClusterAddress(cluster string) error {
	cluster, err := normalizeClusterAddress(cluster)
	if err != nil {
		return err
	}
	if len(cluster) > maxClusterNameLen {
		return fmt.Errorf("cluster address too long (max %d chars)", maxClusterNameLen)
	}

	// Check if it's a valid IP or hostname
	if net.ParseIP(cluster) != nil {
		return nil // Valid IP
	}

	// Validate hostname format (basic check)
	if strings.Contains(cluster, "..") || strings.HasPrefix(cluster, ".") || strings.HasSuffix(cluster, ".") {
		return errors.New("invalid cluster hostname format")
	}

	// Check for valid characters in hostname
	for _, r := range cluster {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '-' || r == '_') {
			return fmt.Errorf("invalid character in cluster address: %c", r)
		}
	}

	return nil
}

// validateClusters validates all cluster addresses
func validateClusters(clusters []string) error {
	if len(clusters) == 0 {
		return errors.New("at least one cluster must be provided")
	}

	seen := make(map[string]bool)
	for i, cluster := range clusters {
		norm, err := normalizeClusterAddress(cluster)
		if err != nil {
			return fmt.Errorf("cluster %d (%s): %w", i+1, cluster, err)
		}
		if err := validateClusterAddress(norm); err != nil {
			return fmt.Errorf("cluster %d (%s): %w", i+1, cluster, err)
		}
		if seen[norm] {
			return fmt.Errorf("duplicate cluster address: %s", norm)
		}
		seen[norm] = true
	}

	return nil
}

// rejectSampleConfig stops a generated or example config from being executed
// until the operator replaces the sample targets. The error names the field
// and what to type instead of a low-level parse failure.
func rejectSampleConfig(cfg Config) error {
	var lines []string
	checkTarget := func(kind, value string) {
		v := strings.TrimSpace(value)
		if v == "" || !sampleClusterValue(v) {
			return
		}
		lines = append(lines, fmt.Sprintf("%s %q is still a sample. Replace it with a real Prism IP or DNS name, such as 10.20.30.40 or pe-prod.corp.example.", kind, v))
	}
	for _, c := range cfg.Clusters {
		checkTarget("Cluster", c)
	}
	for _, pc := range cfg.PCs {
		checkTarget("Prism Central", pc)
	}
	if sampleClusterValue(cfg.PrismCentralURL) {
		checkTarget("prism-central-url", cfg.PrismCentralURL)
	}
	if cfg.EmailEnabled && sampleEndpoint(cfg.SMTPServer) {
		lines = append(lines, "Email is turned on, but smtp-server is still the sample host. Set your mail server, or set email-enabled to false.")
	}
	if cfg.WebhookEnabled && sampleEndpoint(cfg.WebhookURL) {
		lines = append(lines, "The webhook is turned on, but webhook-url is still the sample address. Set a real URL, or set webhook-enabled to false.")
	}
	if cfg.SlackEnabled && sampleEndpoint(cfg.SlackWebhookURL) {
		lines = append(lines, "Slack is turned on, but slack-webhook-url is still empty or a sample. Paste the incoming-webhook URL, or set slack-enabled to false.")
	}
	if len(lines) == 0 {
		return nil
	}
	return fmt.Errorf("this config still has sample values, so no cluster was contacted:\n- %s\nEdit the config file, then run the command again", strings.Join(lines, "\n- "))
}

func sampleClusterValue(v string) bool {
	s := strings.ToLower(strings.TrimSpace(v))
	if s == "" {
		return false
	}
	return strings.Contains(s, "xx.xx") || strings.Contains(s, "replace_with_") || strings.Contains(s, "changeme")
}

func sampleEndpoint(v string) bool {
	s := strings.ToLower(strings.TrimSpace(v))
	return s == "" || strings.Contains(s, "example.com") || strings.Contains(s, "changeme") || strings.Contains(s, "replace_with_")
}

// validateURL validates webhook/Slack URLs
func validateURL(urlStr string) error {
	if urlStr == "" {
		return errors.New("URL cannot be empty")
	}
	if len(urlStr) > maxURLLength {
		return fmt.Errorf("URL too long (max %d chars)", maxURLLength)
	}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL must use http or https scheme, got: %s", parsed.Scheme)
	}

	if parsed.Host == "" {
		return errors.New("URL must have a host")
	}

	return nil
}

// validateEmailAddress validates email address format
func validateEmailAddress(email string) error {
	if email == "" {
		return errors.New("email address cannot be empty")
	}

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return errors.New("invalid email format: must contain exactly one @")
	}

	local, domain := parts[0], parts[1]
	if len(local) == 0 || len(domain) == 0 {
		return errors.New("invalid email format: local or domain part is empty")
	}

	if !strings.Contains(domain, ".") {
		return errors.New("invalid email format: domain must contain a dot")
	}

	return nil
}

// nutanixV4PathSegment returns the API path segment for Nutanix v4 routes (clustermgmt / monitoring), defaulting to v4.2.
func nutanixV4PathSegment(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return defaultNutanixV4APIVersion
	}
	return s
}

// normalizeNCCAPIVersion maps config values to internal "v4" or "v1" (Legacy). Accepts v4, Legacy (any case), or v1 as alias for Legacy.
func normalizeNCCAPIVersion(s string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	if t == "" {
		return "v4", nil
	}
	switch t {
	case "v4":
		return "v4", nil
	case "v1", "legacy":
		return "v1", nil
	default:
		return "", fmt.Errorf("must be v4 or Legacy (v1 is accepted as an alias for Legacy), got %q", s)
	}
}

// validateNutanixV4APIVersion checks the path revision string (e.g. v4.2, v4.0.a1) used in /api/clustermgmt/{ver}/ and /api/monitoring/{ver}/.
func validateNutanixV4APIVersion(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.ToLower(s)
	if len(s) > 48 {
		return errors.New("nutanix-v4-api-version must be at most 48 characters")
	}
	if strings.ContainsAny(s, `/\`) || strings.Contains(s, "..") {
		return errors.New("nutanix-v4-api-version must not contain path separators or '..'")
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("nutanix-v4-api-version: invalid character %q (examples: v4.2, v4.1, v4.0.a1)", r)
	}
	if !strings.HasPrefix(s, "v") {
		return errors.New("nutanix-v4-api-version must start with v (e.g. v4.2, v4.1, v4.0.a1)")
	}
	return nil
}

func normalizeExcludeAlertMatchMode(s string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(s))
	if mode == "" {
		return defaultExcludeMatchMode, nil
	}
	switch mode {
	case "exact", "contains", "regex":
		return mode, nil
	default:
		return "", fmt.Errorf("exclude-alert-match-mode must be one of exact, contains, regex")
	}
}

// validateConfig performs comprehensive configuration validation
func validateConfig(cfg Config) error {
	if err := rejectSampleConfig(cfg); err != nil {
		return err
	}
	sourceMode, err := normalizeClusterSourceMode(cfg.ClusterSourceMode)
	if err != nil {
		return err
	}
	if sourceMode == "pc" {
		if len(cfg.PCs) == 0 && strings.TrimSpace(cfg.PrismCentralURL) == "" {
			return errors.New("pc mode requires pcs, pcs-file, or prism-central-url")
		}
		if strings.TrimSpace(cfg.Username) == "" {
			return errors.New("username is required in pc mode")
		}
		for i, pc := range cfg.PCs {
			if _, err := normalizePCBaseURL(pc, cfg.InsecureSkipVerify); err != nil {
				return fmt.Errorf("pc target %d (%s): %w", i+1, pc, err)
			}
		}
	} else {
		// Validate direct clusters input.
		if err := validateClusters(cfg.Clusters); err != nil {
			return fmt.Errorf("cluster validation failed: %w", err)
		}
		// Validate effective credentials (global or per-cluster from clusters-file).
		if err := validateClusterCredentialCoverage(cfg); err != nil {
			return err
		}
	}

	if _, err := normalizeNCCAPIVersion(cfg.NCCAPIVersion); err != nil {
		return fmt.Errorf("ncc-api-version: %w", err)
	}
	if err := validateNutanixV4APIVersion(cfg.NutanixV4APIVersion); err != nil {
		return fmt.Errorf("nutanix-v4-api-version: %w", err)
	}
	mode, err := normalizeExcludeAlertMatchMode(cfg.ExcludeAlertMatchMode)
	if err != nil {
		return err
	}
	if mode == "regex" {
		for _, pattern := range cfg.ExcludeAlertTitles {
			p := strings.TrimSpace(pattern)
			if p == "" {
				continue
			}
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("exclude-alert-titles regex %q: %w", p, err)
			}
		}
	}

	// Validate timeouts
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}
	if cfg.RequestTimeout <= 0 {
		return errors.New("request-timeout must be greater than 0")
	}
	if cfg.RequestTimeout > cfg.Timeout {
		return errors.New("request-timeout cannot be greater than overall timeout")
	}

	// Validate concurrency
	// 0 = auto: sized to the fleet when the run starts (autoMaxParallel).
	if cfg.MaxParallel < 0 {
		return errors.New("max-parallel must be 0 (auto) or greater")
	}
	if cfg.RetryFailedClusters < 0 || cfg.RetryFailedClusters > 5 {
		return errors.New("retry-failed-clusters must be between 0 and 5")
	}
	// Each worker mostly waits on its cluster's NCC task (polling every
	// poll-interval), so hundreds in flight are cheap; the old limit of 100
	// meant ~70 waves (12–23 h of NCC time) for a 7,000-cluster fleet.
	if cfg.MaxParallel > maxParallelLimit {
		return fmt.Errorf("max-parallel cannot exceed %d (safety limit)", maxParallelLimit)
	}

	// Validate output formats
	validFormats := map[string]bool{"html": true, "csv": true, "json": true, "markdown": true, "sarif": true}
	for _, format := range cfg.OutputFormats {
		if !validFormats[strings.ToLower(format)] {
			return fmt.Errorf("invalid output format: %s (valid: html, csv, json, markdown, sarif)", format)
		}
	}

	// Validate severity filter
	validSeverities := map[string]bool{"FAIL": true, "WARN": true, "ERR": true, "INFO": true}
	for _, sev := range cfg.SeverityFilter {
		if !validSeverities[strings.ToUpper(sev)] {
			return fmt.Errorf("invalid severity: %s (valid: FAIL, WARN, ERR, INFO)", sev)
		}
	}

	// Validate email settings if enabled
	if cfg.EmailEnabled {
		if cfg.SMTPServer == "" {
			return errors.New("smtp-server is required when email is enabled")
		}
		if cfg.EmailFrom == "" {
			return errors.New("email-from is required when email is enabled")
		}
		if len(cfg.EmailTo) == 0 {
			return errors.New("email-to is required when email is enabled")
		}
		if cfg.SMTPPort <= 0 || cfg.SMTPPort > 65535 {
			return errors.New("smtp-port must be between 1 and 65535")
		}

		if err := validateEmailAddress(cfg.EmailFrom); err != nil {
			return fmt.Errorf("invalid email-from: %w", err)
		}
		for i, to := range cfg.EmailTo {
			if err := validateEmailAddress(to); err != nil {
				return fmt.Errorf("invalid email-to[%d]: %w", i, err)
			}
		}
	}

	// Validate webhook if enabled
	if cfg.WebhookEnabled {
		if err := validateURL(cfg.WebhookURL); err != nil {
			return fmt.Errorf("invalid webhook-url: %w", err)
		}
	}

	// Validate Slack if enabled
	if cfg.SlackEnabled {
		if err := validateURL(cfg.SlackWebhookURL); err != nil {
			return fmt.Errorf("invalid slack-webhook-url: %w", err)
		}
	}

	// Validate output paths (non-empty)
	if strings.TrimSpace(cfg.OutputDirLogs) == "" {
		return errors.New("output-dir-logs cannot be empty")
	}
	if strings.TrimSpace(cfg.OutputDirFiltered) == "" {
		return errors.New("output-dir-filtered cannot be empty")
	}
	if strings.TrimSpace(cfg.LogFile) == "" {
		return errors.New("log-file cannot be empty")
	}
	if err := validateLogFilePath("log-file", cfg.LogFile); err != nil {
		return err
	}
	if cfg.PromEnabled && strings.TrimSpace(cfg.PromDir) == "" {
		return errors.New("prom-dir cannot be empty when prom-enabled=true")
	}
	if cfg.RetainLastRuns < 0 {
		return errors.New("retain-last must be >= 0")
	}
	if cfg.RetainDays < 0 {
		return errors.New("retain-days must be >= 0")
	}
	if cfg.ArtifactRetainDays < 0 {
		return errors.New("artifact-retain-days must be >= 0")
	}
	if cfg.ArtifactRetainMaxFiles < 0 {
		return errors.New("artifact-retain-max-files must be >= 0")
	}
	if cfg.RawLogRetainDays < 0 {
		return errors.New("raw-log-retain-days must be >= 0")
	}
	if cfg.RunHistoryEnabled && strings.TrimSpace(cfg.RunHistoryDir) == "" {
		return errors.New("run-history-dir cannot be empty when run-history is enabled")
	}
	if err := validateLogLevelStrict(cfg.LogLevel); err != nil {
		return err
	}
	if cfg.FlakyLookbackRuns < 0 {
		return errors.New("flaky-lookback-runs must be >= 0")
	}
	if cfg.FlakyLookbackRuns > 0 && cfg.FlakyLookbackRuns < 2 {
		return errors.New("flaky-lookback-runs must be >= 2")
	}
	if cfg.FlakyLookbackRuns > 200 {
		return errors.New("flaky-lookback-runs must be <= 200")
	}
	if cfg.FlakyMinTransitions < 0 {
		return errors.New("flaky-min-transitions must be >= 0")
	}
	if cfg.FlakyMinTransitions > 0 && cfg.FlakyMinTransitions < 1 {
		return errors.New("flaky-min-transitions must be >= 1")
	}
	if cfg.FlakyMinTransitions > 20 {
		return errors.New("flaky-min-transitions must be <= 20")
	}
	if _, err := inQuietHours(time.Now(), cfg.QuietHours); err != nil {
		return fmt.Errorf("quiet-hours: %w", err)
	}
	if _, err := inMaintenanceWindow(time.Now(), cfg.MaintenanceWindows); err != nil {
		return fmt.Errorf("maintenance-windows: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(cfg.SecretsProvider)) {
	case "", "env", "file":
	default:
		return errors.New("secrets-provider must be one of: env, file")
	}
	if strings.EqualFold(strings.TrimSpace(cfg.SecretsProvider), "file") && strings.TrimSpace(cfg.SecretsFile) == "" {
		return errors.New("secrets-file is required when secrets-provider=file")
	}

	// Validate retry settings
	if cfg.RetryMaxAttempts <= 0 {
		return errors.New("retry-max-attempts must be greater than 0")
	}
	if cfg.RetryMaxAttempts > 50 {
		return errors.New("retry-max-attempts cannot exceed 50 (safety limit)")
	}
	if cfg.RetryBaseDelay <= 0 {
		return errors.New("retry-base-delay must be greater than 0")
	}
	if cfg.RetryMaxDelay <= 0 {
		return errors.New("retry-max-delay must be greater than 0")
	}
	if cfg.RetryBaseDelay > cfg.RetryMaxDelay {
		return errors.New("retry-base-delay cannot be greater than retry-max-delay")
	}
	if cfg.RetryCircuitBreaker < 0 {
		return errors.New("retry-circuit-breaker must be >= 0")
	}
	if cfg.RetryCircuitBreaker > 0 && cfg.RetryCircuitBreaker > cfg.RetryMaxAttempts {
		return errors.New("retry-circuit-breaker cannot exceed retry-max-attempts")
	}

	// Security warning for insecure skip verify
	if cfg.InsecureSkipVerify {
		log.Warn().Msg("WARNING: TLS verification is disabled. This should only be used in trusted lab environments")
	}

	return nil
}

// maskPassword returns a masked version of password for logging
func maskPassword(pwd string) string {
	if pwd == "" {
		return "(empty)"
	}
	if len(pwd) <= 4 {
		return "****"
	}
	return pwd[:2] + "****" + pwd[len(pwd)-2:]
}
