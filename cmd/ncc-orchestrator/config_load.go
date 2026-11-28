package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"goncc/internal/httpclient"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

func parseLogLevel(s string) zerolog.Level {
	if s == "" {
		if env := os.Getenv("LOG_LEVEL"); env != "" {
			s = env
		} else {
			return zerolog.InfoLevel
		}
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace", "0":
		return zerolog.TraceLevel
	case "debug", "1":
		return zerolog.DebugLevel
	case "info", "2":
		return zerolog.InfoLevel
	case "warn", "warning", "3":
		return zerolog.WarnLevel
	case "error", "4":
		return zerolog.ErrorLevel
	case "fatal", "5":
		return zerolog.FatalLevel
	default:
		if n, err := strconv.Atoi(s); err == nil {
			if n >= math.MinInt8 && n <= math.MaxInt8 {
				return zerolog.Level(n)
			}
		}
		return zerolog.InfoLevel
	}
}

func parseBoolStrict(v interface{}) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		p, err := strconv.ParseBool(strings.TrimSpace(b))
		if err != nil {
			return false, err
		}
		return p, nil
	default:
		return false, fmt.Errorf("expected bool/string, got %T", v)
	}
}

func parseIntStrict(v interface{}) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		if math.Trunc(n) != n {
			return 0, fmt.Errorf("expected integer value, got %v", n)
		}
		return int(n), nil
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		if err != nil {
			return 0, err
		}
		return i, nil
	default:
		return 0, fmt.Errorf("expected int/string, got %T", v)
	}
}

func parseStringStrict(v interface{}) (string, error) {
	switch s := v.(type) {
	case string:
		return s, nil
	default:
		return "", fmt.Errorf("expected string, got %T", v)
	}
}

func parseMapStringStringStrict(v interface{}) error {
	switch m := v.(type) {
	case map[string]interface{}:
		for k, vv := range m {
			if _, ok := vv.(string); !ok {
				return fmt.Errorf("key %q must be string value, got %T", k, vv)
			}
		}
		return nil
	case map[string]string:
		return nil
	default:
		return fmt.Errorf("expected object/map, got %T", v)
	}
}

func validateLogLevelStrict(s string) error {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "" {
		return nil
	}
	switch v {
	case "trace", "debug", "info", "warn", "warning", "error", "fatal",
		"0", "1", "2", "3", "4", "5":
		return nil
	default:
		return fmt.Errorf("invalid log-level %q (valid: trace/debug/info/warn/error/fatal or 0..5)", s)
	}
}

// validateConfigFileRawTypes validates values as written in the config file (before env/flag overrides)
// so typos like insecure-skip-verify: flse are surfaced clearly.
func validateConfigFileRawTypes() error {
	if viper.ConfigFileUsed() == "" {
		return nil
	}
	allowedTopKeys := map[string]bool{
		"config":         true,
		"update":         true,
		"schema-version": true,
		"runner":         true, "storage": true, "api": true, "ui": true, "deployment": true, "notifications": true, "logging": true, "prometheus": true, "secrets": true,
		"skip-preflight-check": true,
		"auto":                 true, "automation-level": true,
		"clusters": true, "clusters-file": true, "cluster-source-mode": true, "pcs": true, "pcs-file": true, "prism-central-url": true, "discover-api-version": true,
		"username": true, "password": true, "ncc-api-version": true, "nutanix-v4-api-version": true, "pc-alerts-cache-ttl": true,
		"insecure-skip-verify": true, "ca-bundle": true, "pin-sha256": true, "timeout": true, "request-timeout": true, "poll-interval": true, "poll-jitter": true,
		"max-parallel": true, "outputs": true, "output-dir-logs": true, "output-dir-filtered": true,
		"single-report": true, "run-history": true, "run-history-dir": true, "retain-last": true, "retain-days": true, "artifact-retain-days": true, "artifact-retain-max-files": true, "raw-log-retain-days": true,
		"notify-on-regression": true, "adaptive-parallelism": true, "retry-failed-clusters": true, "resume": true,
		"policy-gates": true, "quiet-hours": true, "maintenance-windows": true,
		"flaky-lookback-runs": true, "flaky-min-transitions": true,
		"log-file": true, "log-level": true, "log-http": true,
		"retry-max-attempts": true, "retry-base-delay": true, "retry-max-delay": true, "retry-circuit-breaker": true,
		"prom-enabled": true, "prom-dir": true, "severity-filter": true, "exclude-alert-titles": true, "exclude-alert-titles-file": true, "exclude-alert-match-mode": true, "dry-run": true, "replay": true,
		"max-idle-conns": true, "max-idle-conns-per-host": true, "max-conns-per-host": true, "idle-conn-timeout": true,
		"gen-test-agg":  true,
		"email-enabled": true, "email-attach-html": true, "notify-digest": true,
		"smtp-server": true, "smtp-port": true, "smtp-user": true, "smtp-password": true,
		"email-from": true, "email-to": true, "email-use-tls": true, "smtp-insecure-skip-verify": true,
		"email-subject-template": true, "email-body-template": true,
		"webhook-enabled": true, "webhook-include-html": true, "webhook-url": true, "webhook-headers": true, "webhook-template": true, "webhook-secret": true,
		"slack-enabled": true, "slack-webhook-url": true, "slack-channel": true,
		"notification-deadletter-dir": true,
		"secrets-provider":            true, "secrets-file": true,
	}
	for key := range viper.AllSettings() {
		// Only enforce unknown-key checks for keys that actually come from config file.
		// Viper may contain extra keys set from flags/env/programmatic overrides.
		if !viper.InConfig(key) {
			continue
		}
		if !allowedTopKeys[key] {
			return fmt.Errorf("unknown config key %q", key)
		}
	}

	stringKeys := []string{
		"clusters", "clusters-file", "cluster-source-mode", "pcs", "pcs-file", "prism-central-url", "discover-api-version",
		"username", "password", "ncc-api-version", "nutanix-v4-api-version",
		"timeout", "request-timeout", "poll-interval", "poll-jitter",
		"outputs", "output-dir-logs", "output-dir-filtered", "run-history-dir",
		"log-file", "log-level", "retry-base-delay", "retry-max-delay", "prom-dir",
		"severity-filter", "exclude-alert-titles", "exclude-alert-titles-file", "exclude-alert-match-mode", "idle-conn-timeout", "policy-gates", "quiet-hours", "maintenance-windows",
		"ca-bundle", "pin-sha256",
		"smtp-server", "smtp-user", "smtp-password", "email-from", "email-to",
		"email-subject-template", "email-body-template",
		"webhook-url", "webhook-template", "webhook-secret", "notification-deadletter-dir", "slack-webhook-url", "slack-channel", "secrets-provider", "secrets-file",
		"automation-level",
	}
	for _, key := range stringKeys {
		if !viper.InConfig(key) {
			continue
		}
		if _, err := parseStringStrict(viper.Get(key)); err != nil {
			return fmt.Errorf("%s: invalid string value (%v)", key, err)
		}
	}

	boolKeys := []string{
		"update",
		"skip-preflight-check", "auto",
		"insecure-skip-verify", "dry-run", "replay", "log-http",
		"prom-enabled",
		"run-history", "single-report", "notify-on-regression", "adaptive-parallelism", "resume",
		"email-enabled", "email-attach-html", "notify-digest", "email-use-tls", "smtp-insecure-skip-verify",
		"webhook-enabled", "webhook-include-html", "slack-enabled",
	}
	for _, key := range boolKeys {
		if !viper.InConfig(key) {
			continue
		}
		if _, err := parseBoolStrict(viper.Get(key)); err != nil {
			return fmt.Errorf("%s: invalid boolean value (%v)", key, err)
		}
	}
	intKeys := []string{
		"schema-version", "max-parallel", "retry-failed-clusters", "retry-max-attempts", "retry-circuit-breaker", "max-idle-conns", "max-idle-conns-per-host",
		"max-conns-per-host", "smtp-port", "retain-last", "retain-days", "artifact-retain-days", "artifact-retain-max-files", "raw-log-retain-days", "gen-test-agg",
		"flaky-lookback-runs", "flaky-min-transitions",
	}
	for _, key := range intKeys {
		if !viper.InConfig(key) {
			continue
		}
		if _, err := parseIntStrict(viper.Get(key)); err != nil {
			return fmt.Errorf("%s: invalid integer value (%v)", key, err)
		}
	}
	durationKeys := []string{
		"timeout", "request-timeout", "poll-interval", "poll-jitter",
		"retry-base-delay", "retry-max-delay", "idle-conn-timeout", "pc-alerts-cache-ttl",
	}
	for _, key := range durationKeys {
		if !viper.InConfig(key) {
			continue
		}
		raw := viper.Get(key)
		s, ok := raw.(string)
		if !ok {
			return fmt.Errorf("%s: expected duration string, got %T", key, raw)
		}
		if _, err := time.ParseDuration(strings.TrimSpace(s)); err != nil {
			return fmt.Errorf("%s: invalid duration %q (%v)", key, s, err)
		}
	}
	if viper.InConfig("webhook-headers") {
		if err := parseMapStringStringStrict(viper.Get("webhook-headers")); err != nil {
			return fmt.Errorf("webhook-headers: invalid map value (%v)", err)
		}
	}
	if viper.InConfig("log-level") {
		if err := validateLogLevelStrict(viper.GetString("log-level")); err != nil {
			return err
		}
	}
	// Checked in both spellings: a bad level otherwise surfaces only when a run starts.
	for _, key := range []string{"automation-level", "runner.automation.level"} {
		if viper.InConfig(key) {
			if _, err := parseAutomationLevel(viper.GetString(key)); err != nil {
				return err
			}
		}
	}
	return nil
}

// configBaseDir returns the absolute directory of the loaded config file. It is
// used to anchor relative output paths so they resolve consistently regardless
// of the process working directory. Returns "" when no config file is in use
// (flag-only runs), in which case the legacy cwd-relative behavior is kept.
func configBaseDir(cfgFile string) string {
	p := strings.TrimSpace(cfgFile)
	if p == "" {
		p = strings.TrimSpace(viper.ConfigFileUsed())
	}
	if p == "" {
		return ""
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return ""
	}
	return filepath.Dir(abs)
}

// resolveUnderBase makes a relative path absolute by anchoring it to base. An
// already-absolute path, an empty path, or an empty base is returned unchanged.
// This is the core of the "self-healing" output-path resolution: a scheduled
// (cron) run executes with an arbitrary working directory (often the invoking
// user's home), so a relative output-dir like "outputfiles" would otherwise be
// created beside that cwd and silently diverge from where the API/UI reads
// results. Anchoring to the config file's directory keeps scheduled runs and
// interactive runs writing to the same place.
func resolveUnderBase(p, base string) string {
	p = strings.TrimSpace(p)
	if p == "" || base == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(base, p)
}

// normalizeCanonicalConfig bridges the versioned nested YAML shape to the
// legacy flat keys consumed by the existing runner. Flat keys win whenever
// they are present in the file or environment, which keeps old deployments
// compatible while allowing new deployments to use one canonical structure.
func normalizeCanonicalConfig() {
	if version := viper.GetInt("schema-version"); version != 0 && version != 1 {
		return
	}
	// A flat key with two nested spellings takes the preferred one when the
	// file sets both (map order used to pick at random).
	for flat, paths := range configNestedPathsByFlat() {
		nested := ""
		for _, p := range paths {
			if viper.IsSet(p) {
				nested = p
				break
			}
		}
		if nested == "" || viper.InConfig(flat) {
			continue
		}
		if _, ok := os.LookupEnv("NCC_" + strings.ToUpper(strings.ReplaceAll(flat, "-", "_"))); ok {
			continue
		}
		// Legacy flags represent list-valued settings as comma-separated
		// strings, while the canonical YAML schema represents them as arrays.
		// Normalize arrays before the strict legacy type validation runs.
		value := viper.Get(nested)
		switch values := value.(type) {
		case []string:
			value = strings.Join(values, ",")
		case []interface{}:
			items := make([]string, 0, len(values))
			for _, item := range values {
				items = append(items, fmt.Sprint(item))
			}
			value = strings.Join(items, ",")
		}
		viper.Set(flat, value)
	}
}

// canonicalConfigMappings maps each nested (schema-version: 1) path to the
// flat key the runner reads. Some flat keys have two nested spellings.
func canonicalConfigMappings() map[string]string {
	return map[string]string{
		"runner.targets.mode":                        "cluster-source-mode",
		"runner.targets.clusters":                    "clusters",
		"runner.targets.clusters-file":               "clusters-file",
		"runner.targets.pcs":                         "pcs",
		"runner.targets.pcs-file":                    "pcs-file",
		"runner.targets.prism-central-url":           "prism-central-url",
		"runner.targets.discover-api-version":        "discover-api-version",
		"runner.credentials.username":                "username",
		"runner.credentials.password":                "password",
		"runner.connection.insecure-skip-verify":     "insecure-skip-verify",
		"runner.connection.ca-bundle":                "ca-bundle",
		"runner.connection.pin-sha256":               "pin-sha256",
		"runner.connection.ncc-api-version":          "ncc-api-version",
		"runner.connection.nutanix-v4-api-version":   "nutanix-v4-api-version",
		"runner.execution.timeout":                   "timeout",
		"runner.execution.request-timeout":           "request-timeout",
		"runner.execution.max-parallel":              "max-parallel",
		"runner.execution.poll-interval":             "poll-interval",
		"runner.execution.poll-jitter":               "poll-jitter",
		"runner.execution.adaptive-parallelism":      "adaptive-parallelism",
		"runner.execution.retry-failed-clusters":     "retry-failed-clusters",
		"runner.execution.resume":                    "resume",
		"runner.execution.dry-run":                   "dry-run",
		"runner.execution.skip-preflight-check":      "skip-preflight-check",
		"runner.execution.replay":                    "replay",
		"runner.automation.enabled":                  "auto",
		"runner.automation.level":                    "automation-level",
		"runner.retry.max-attempts":                  "retry-max-attempts",
		"runner.retry.base-delay":                    "retry-base-delay",
		"runner.retry.max-delay":                     "retry-max-delay",
		"runner.retry.circuit-breaker":               "retry-circuit-breaker",
		"runner.filtering.severity":                  "severity-filter",
		"runner.filtering.policy-gates":              "policy-gates",
		"runner.filtering.exclude-alert-titles":      "exclude-alert-titles",
		"runner.filtering.flaky-lookback-runs":       "flaky-lookback-runs",
		"runner.filtering.flaky-min-transitions":     "flaky-min-transitions",
		"runner.filtering.exclude-alert-titles-file": "exclude-alert-titles-file",
		"runner.filtering.exclude-alert-match-mode":  "exclude-alert-match-mode",
		"runner.policy.flaky-lookback-runs":          "flaky-lookback-runs",
		"runner.policy.flaky-min-transitions":        "flaky-min-transitions",
		"runner.outputs.formats":                     "outputs",
		"runner.outputs.single-report":               "single-report",
		"runner.outputs.gen-test-agg":                "gen-test-agg",
		"runner.history.enabled":                     "run-history",
		"runner.history.retain-last":                 "retain-last",
		"runner.history.retain-days":                 "retain-days",
		"runner.history.artifact-retain-days":        "artifact-retain-days",
		"runner.history.artifact-retain-max-files":   "artifact-retain-max-files",
		"runner.history.raw-log-retain-days":         "raw-log-retain-days",
		"runner.policy.gates":                        "policy-gates",
		"runner.policy.notify-on-regression":         "notify-on-regression",
		"runner.http-pool.max-idle-conns":            "max-idle-conns",
		"runner.http-pool.max-idle-conns-per-host":   "max-idle-conns-per-host",
		"runner.http-pool.max-conns-per-host":        "max-conns-per-host",
		"runner.http-pool.idle-conn-timeout":         "idle-conn-timeout",
		"storage.output-dir":                         "output-dir-filtered",
		"storage.logs-dir":                           "output-dir-logs",
		"storage.run-history-dir":                    "run-history-dir",
		"storage.prom-dir":                           "prom-dir",
		"storage.notification-deadletter-dir":        "notification-deadletter-dir",
		"logging.file":                               "log-file",
		"logging.level":                              "log-level",
		"logging.http":                               "log-http",
		"prometheus.enabled":                         "prom-enabled",
		"notifications.quiet-hours":                  "quiet-hours",
		"notifications.maintenance-windows":          "maintenance-windows",
		"notifications.email-enabled":                "email-enabled",
		"notifications.smtp-server":                  "smtp-server",
		"notifications.smtp-port":                    "smtp-port",
		"notifications.webhook-url":                  "webhook-url",
		"notifications.email.enabled":                "email-enabled",
		"notifications.email.attach-html":            "email-attach-html",
		"notifications.email.digest":                 "notify-digest",
		"notifications.email.smtp-user":              "smtp-user",
		"notifications.email.smtp-server":            "smtp-server",
		"notifications.email.smtp-port":              "smtp-port",
		"notifications.email.smtp-password":          "smtp-password",
		"notifications.email.from":                   "email-from",
		"notifications.email.to":                     "email-to",
		"notifications.email.use-tls":                "email-use-tls",
		"notifications.email.insecure-skip-verify":   "smtp-insecure-skip-verify",
		"notifications.email.subject-template":       "email-subject-template",
		"notifications.email.body-template":          "email-body-template",
		"notifications.webhook.enabled":              "webhook-enabled",
		"notifications.webhook.url":                  "webhook-url",
		"notifications.webhook.include-html":         "webhook-include-html",
		"notifications.webhook.headers":              "webhook-headers",
		"notifications.webhook.template":             "webhook-template",
		"notifications.webhook.secret":               "webhook-secret",
		"notifications.slack.enabled":                "slack-enabled",
		"notifications.slack.webhook-url":            "slack-webhook-url",
		"notifications.slack.channel":                "slack-channel",
		"secrets.provider":                           "secrets-provider",
		"secrets.file":                               "secrets-file",
		"api.cache.pc-alerts-cache-ttl":              "pc-alerts-cache-ttl",
	}
}

func bindConfig() (Config, error) {
	cfgFile := viper.GetString("config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if _, err := os.Stat(cfgFile); errors.Is(err, os.ErrNotExist) {
			if err := writeDummyConfig(cfgFile); err != nil {
				return Config{}, fmt.Errorf("failed to create dummy config at %s: %w", cfgFile, err)
			}
			fmt.Printf("Created dummy config at %s. Please edit it according to your Nutanix environment and re-run.\n", cfgFile)
			return Config{}, errors.New("dummy config created; edit and re-run")
		}
		if err := viper.ReadInConfig(); err != nil {
			var nf viper.ConfigFileNotFoundError
			if !errors.As(err, &nf) {
				return Config{}, fmt.Errorf("read config: %w", err)
			}
		}
		if err := validateConfigFileRawTypes(); err != nil {
			return Config{}, fmt.Errorf("invalid config file values: %w", err)
		}
	}

	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	normalizeCanonicalConfig()

	clustersFromFlag := splitCSVValue(viper.Get("clusters"))
	clustersFile := strings.TrimSpace(viper.GetString("clusters-file"))
	pcsFromFlag := splitCSVValue(viper.Get("pcs"))
	pcsFile := strings.TrimSpace(viper.GetString("pcs-file"))
	clusterSourceMode := strings.TrimSpace(viper.GetString("cluster-source-mode"))
	clusterCreds := map[string]ClusterCredential{}
	if clustersFile != "" {
		lines, fileCreds, err := readClusterFile(clustersFile)
		if err != nil {
			return Config{}, fmt.Errorf("clusters-file %s: %w", clustersFile, err)
		}
		if len(lines) > 0 {
			clustersFromFlag = lines
		}
		clusterCreds = fileCreds
	}
	if len(clustersFromFlag) > 0 {
		normalizedClusters, err := normalizeClusters(clustersFromFlag)
		if err != nil {
			return Config{}, err
		}
		clustersFromFlag = normalizedClusters
	}
	if len(clusterCreds) > 0 {
		normalizedCreds := make(map[string]ClusterCredential, len(clusterCreds))
		for cluster, cred := range clusterCreds {
			norm, err := normalizeClusterAddress(cluster)
			if err != nil {
				return Config{}, fmt.Errorf("clusters-file credential key %s: %w", cluster, err)
			}
			if existing, ok := normalizedCreds[norm]; ok {
				if (strings.TrimSpace(existing.Username) != "" && strings.TrimSpace(cred.Username) != "" && existing.Username != cred.Username) ||
					(strings.TrimSpace(existing.Password) != "" && strings.TrimSpace(cred.Password) != "" && existing.Password != cred.Password) {
					return Config{}, fmt.Errorf("clusters-file has conflicting credentials for %s after normalization", norm)
				}
			}
			normalizedCreds[norm] = cred
		}
		clusterCreds = normalizedCreds
	}
	if pcsFile != "" {
		lines, err := readLineValuesFile(pcsFile)
		if err != nil {
			return Config{}, fmt.Errorf("pcs-file %s: %w", pcsFile, err)
		}
		if len(lines) > 0 {
			pcsFromFlag = lines
		}
	}
	nccAPIVer, err := normalizeNCCAPIVersion(viper.GetString("ncc-api-version"))
	if err != nil {
		return Config{}, fmt.Errorf("ncc-api-version: %w", err)
	}
	cfg := Config{
		SchemaVersion:             viper.GetInt("schema-version"),
		Clusters:                  clustersFromFlag,
		ClustersFile:              clustersFile,
		ClusterCredentials:        clusterCreds,
		ClusterSourceMode:         clusterSourceMode,
		PCs:                       pcsFromFlag,
		PCsFile:                   pcsFile,
		PrismCentralURL:           strings.TrimSpace(viper.GetString("prism-central-url")),
		DiscoverAPIVersion:        strings.TrimSpace(viper.GetString("discover-api-version")),
		Username:                  viper.GetString("username"),
		Password:                  viper.GetString("password"),
		InsecureSkipVerify:        viper.GetBool("insecure-skip-verify"),
		CABundle:                  strings.TrimSpace(viper.GetString("ca-bundle")),
		PinSHA256:                 splitCSVValue(viper.Get("pin-sha256")),
		Timeout:                   mustParseDur(viper.GetString("timeout"), defaultTimeout),
		RequestTimeout:            mustParseDur(viper.GetString("request-timeout"), defaultRequestTimeout),
		PollInterval:              mustParseDur(viper.GetString("poll-interval"), defaultPollInterval),
		PollJitter:                mustParseDur(viper.GetString("poll-jitter"), defaultPollJitter),
		OutputDirLogs:             viper.GetString("output-dir-logs"),
		OutputDirFiltered:         viper.GetString("output-dir-filtered"),
		OutputFormats:             splitCSVValue(viper.Get("outputs")),
		MaxParallel:               viper.GetInt("max-parallel"),
		TLSMinVersion:             tls.VersionTLS12,
		LogFile:                   viper.GetString("log-file"),
		LogLevel:                  viper.GetString("log-level"),
		LogHTTP:                   viper.GetBool("log-http"),
		RetryMaxAttempts:          viper.GetInt("retry-max-attempts"),
		RetryBaseDelay:            mustParseDur(viper.GetString("retry-base-delay"), defaultRetryBaseDelay),
		RetryMaxDelay:             mustParseDur(viper.GetString("retry-max-delay"), defaultRetryMaxDelay),
		RetryCircuitBreaker:       viper.GetInt("retry-circuit-breaker"),
		MaxIdleConns:              viper.GetInt("max-idle-conns"),
		MaxIdleConnsPerHost:       viper.GetInt("max-idle-conns-per-host"),
		MaxConnsPerHost:           viper.GetInt("max-conns-per-host"),
		IdleConnTimeout:           mustParseDur(viper.GetString("idle-conn-timeout"), httpclient.DefaultIdleConnTimeout),
		EmailEnabled:              viper.GetBool("email-enabled"),
		EmailAttachHTML:           viper.GetBool("email-attach-html"),
		NotifyDigest:              viper.GetBool("notify-digest"),
		SMTPServer:                viper.GetString("smtp-server"),
		SMTPPort:                  viper.GetInt("smtp-port"),
		SMTPUser:                  viper.GetString("smtp-user"),
		SMTPPassword:              viper.GetString("smtp-password"),
		EmailFrom:                 viper.GetString("email-from"),
		EmailTo:                   splitCSVValue(viper.Get("email-to")),
		EmailUseTLS:               viper.GetBool("email-use-tls"),
		SMTPInsecureSkipVerify:    viper.GetBool("smtp-insecure-skip-verify"),
		EmailSubjectTemplate:      viper.GetString("email-subject-template"),
		EmailBodyTemplate:         viper.GetString("email-body-template"),
		WebhookEnabled:            viper.GetBool("webhook-enabled"),
		WebhookIncludeHTML:        viper.GetBool("webhook-include-html"),
		WebhookURL:                viper.GetString("webhook-url"),
		WebhookHeaders:            viper.GetStringMapString("webhook-headers"),
		WebhookTemplate:           viper.GetString("webhook-template"),
		WebhookSecret:             viper.GetString("webhook-secret"),
		NotificationDeadLetterDir: strings.TrimSpace(viper.GetString("notification-deadletter-dir")),
		SeverityFilter:            splitCSVValue(viper.Get("severity-filter")),
		ExcludeAlertTitles:        splitCSVValue(viper.Get("exclude-alert-titles")),
		ExcludeAlertTitlesFile:    strings.TrimSpace(viper.GetString("exclude-alert-titles-file")),
		ExcludeAlertMatchMode:     strings.TrimSpace(viper.GetString("exclude-alert-match-mode")),
		DryRun:                    viper.GetBool("dry-run"),
		RunHistoryEnabled:         viper.GetBool("run-history"),
		RunHistoryDir:             strings.TrimSpace(viper.GetString("run-history-dir")),
		RetainLastRuns:            viper.GetInt("retain-last"),
		RetainDays:                viper.GetInt("retain-days"),
		ArtifactRetainDays:        viper.GetInt("artifact-retain-days"),
		ArtifactRetainMaxFiles:    viper.GetInt("artifact-retain-max-files"),
		RawLogRetainDays:          viper.GetInt("raw-log-retain-days"),
		SingleReport:              viper.GetBool("single-report"),
		NotifyOnRegression:        viper.GetBool("notify-on-regression"),
		AdaptiveParallelism:       viper.GetBool("adaptive-parallelism"),
		RetryFailedClusters:       viper.GetInt("retry-failed-clusters"),
		Resume:                    viper.GetBool("resume"),
		PolicyGates:               splitCSV(viper.GetString("policy-gates")),
		QuietHours:                strings.TrimSpace(viper.GetString("quiet-hours")),
		MaintenanceWindows:        splitCSVValue(viper.Get("maintenance-windows")),
		FlakyLookbackRuns:         viper.GetInt("flaky-lookback-runs"),
		FlakyMinTransitions:       viper.GetInt("flaky-min-transitions"),
		SlackEnabled:              viper.GetBool("slack-enabled"),
		SlackWebhookURL:           viper.GetString("slack-webhook-url"),
		SlackChannel:              viper.GetString("slack-channel"),
		SecretsProvider:           strings.TrimSpace(viper.GetString("secrets-provider")),
		SecretsFile:               strings.TrimSpace(viper.GetString("secrets-file")),
		NCCAPIVersion:             nccAPIVer,
		NutanixV4APIVersion:       strings.ToLower(strings.TrimSpace(viper.GetString("nutanix-v4-api-version"))),
		PCAlertsCacheTTL:          mustParseDur(viper.GetString("pc-alerts-cache-ttl"), 5*time.Minute),
	}
	// Apply defaults
	if cfg.NutanixV4APIVersion == "" {
		cfg.NutanixV4APIVersion = defaultNutanixV4APIVersion
	}
	if cfg.OutputDirLogs == "" {
		cfg.OutputDirLogs = defaultOutputDirLogs
	}
	if cfg.OutputDirFiltered == "" {
		cfg.OutputDirFiltered = defaultOutputDirFiltered
	}
	// Self-healing path resolution: anchor relative output directories to the
	// config file's directory so a scheduled run (cron, arbitrary cwd) writes to
	// the same place an interactive run from the install dir does. Resolved here
	// (before RunHistoryDir is derived from OutputDirFiltered) so the history dir
	// inherits the absolute base too. Absolute paths are left untouched.
	if cfgBase := configBaseDir(cfgFile); cfgBase != "" {
		cfg.OutputDirLogs = resolveUnderBase(cfg.OutputDirLogs, cfgBase)
		cfg.OutputDirFiltered = resolveUnderBase(cfg.OutputDirFiltered, cfgBase)
	}
	if len(cfg.OutputFormats) == 0 {
		cfg.OutputFormats = []string{defaultOutputFormat}
	}
	// max-parallel 0 means auto and stays 0 until the run knows its fleet
	// (autoMaxParallel); only an invalid negative value falls back.
	if cfg.MaxParallel < 0 {
		cfg.MaxParallel = defaultMaxParallel
	}
	// 0 is a valid choice (no retry pass), so only an unset value defaults.
	if !viper.IsSet("retry-failed-clusters") {
		cfg.RetryFailedClusters = defaultRetryFailedClusters
	}
	if cfg.LogFile == "" {
		cfg.LogFile = defaultLogFile
	}
	if viper.IsSet("prom-enabled") {
		cfg.PromEnabled = viper.GetBool("prom-enabled")
	} else {
		cfg.PromEnabled = false
	}
	cfg.PromDir = viper.GetString("prom-dir")
	if cfg.PromDir == "" {
		cfg.PromDir = defaultPromDir
	}
	if cfg.ExcludeAlertMatchMode == "" {
		cfg.ExcludeAlertMatchMode = defaultExcludeMatchMode
	}
	mode, err := normalizeClusterSourceMode(cfg.ClusterSourceMode)
	if err != nil {
		return cfg, err
	}
	cfg.ClusterSourceMode = mode
	if cfg.DiscoverAPIVersion == "" {
		cfg.DiscoverAPIVersion = defaultDiscoverAPIVersion
	}
	cfg.DiscoverAPIVersion = strings.ToLower(strings.TrimSpace(cfg.DiscoverAPIVersion))
	if cfg.DiscoverAPIVersion != "v3" && cfg.DiscoverAPIVersion != "v4" {
		return cfg, fmt.Errorf("discover-api-version must be v3 or v4, got %q", cfg.DiscoverAPIVersion)
	}
	if cfg.ClusterSourceMode == "pc" {
		if len(cfg.PCs) == 0 && strings.TrimSpace(cfg.PrismCentralURL) != "" {
			cfg.PCs = []string{cfg.PrismCentralURL}
		}
	}
	if cfg.ExcludeAlertTitlesFile != "" {
		fileTitles, err := loadExcludeAlertTitlesFromFile(cfg.ExcludeAlertTitlesFile)
		if err != nil {
			return cfg, fmt.Errorf("exclude-alert-titles-file: %w", err)
		}
		cfg.ExcludeAlertTitles = mergeUniqueStrings(cfg.ExcludeAlertTitles, fileTitles)
	}
	excludeMode, err := normalizeExcludeAlertMatchMode(cfg.ExcludeAlertMatchMode)
	if err != nil {
		return cfg, err
	}
	cfg.ExcludeAlertMatchMode = excludeMode
	if cfg.RunHistoryDir == "" {
		cfg.RunHistoryDir = filepath.Join(cfg.OutputDirFiltered, "runs")
	} else if cfgBase := configBaseDir(cfgFile); cfgBase != "" {
		cfg.RunHistoryDir = resolveUnderBase(cfg.RunHistoryDir, cfgBase)
	}
	if cfg.FlakyLookbackRuns <= 0 {
		cfg.FlakyLookbackRuns = defaultFlakyLookbackRuns
	}
	if cfg.FlakyMinTransitions <= 0 {
		cfg.FlakyMinTransitions = defaultFlakyTransitions
	}
	if cfg.RetryMaxAttempts <= 0 {
		cfg.RetryMaxAttempts = defaultRetryAttempts
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = defaultRetryBaseDelay
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = defaultRetryMaxDelay
	}
	if cfg.RetryCircuitBreaker <= 0 {
		cfg.RetryCircuitBreaker = defaultRetryCircuitBreaker
	}
	if err := applySecretsToConfig(&cfg); err != nil {
		return cfg, fmt.Errorf("secret resolution failed: %w", err)
	}

	// Validate configuration
	if err := validateConfig(cfg); err != nil {
		return cfg, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}
