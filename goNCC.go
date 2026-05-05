package main

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/smtp"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
	"golang.org/x/term"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// ==================== Configuration ====================

type Config struct {
	Clusters           []string
	ClustersFile       string                       // Optional: cluster file; lines are cluster or cluster,username[,password] (overrides/supplements clusters when set)
	ClusterCredentials map[string]ClusterCredential `mapstructure:"-"`
	Username           string
	Password           string
	InsecureSkipVerify bool
	Timeout            time.Duration // per-cluster overall timeout
	RequestTimeout     time.Duration // per HTTP request timeout
	PollInterval       time.Duration
	PollJitter         time.Duration
	OutputDirLogs      string
	OutputDirFiltered  string
	OutputFormats      []string // html,csv,json
	MaxParallel        int
	TLSMinVersion      uint16
	LogFile            string

	// Filtering
	SeverityFilter         []string // Only include these severities (FAIL, WARN, ERR, INFO)
	ExcludeAlertTitles     []string // Exclude findings whose alert title matches one of these values
	ExcludeAlertTitlesFile string   // Optional file with one alert title per line to exclude
	ExcludeAlertMatchMode  string   // exact (default), contains, regex

	// Logging options
	LogLevel string // 0..5 or names
	LogHTTP  bool   // dump HTTP request/response

	// Dry-run mode
	DryRun bool // Don't actually run checks, just validate config

	// History + regression
	RunHistoryEnabled        bool
	RunHistoryDir            string
	RetainLastRuns           int
	RetainDays               int
	ArtifactRetainDays       int
	ArtifactRetainMaxFiles   int
	SingleReport             bool
	NotifyOnRegression       bool
	AdaptiveParallelism      bool
	PreviousClusterFailCount map[string]int `mapstructure:"-"`
	PolicyGates              []string
	QuietHours               string
	MaintenanceWindows       []string
	FlakyLookbackRuns        int
	FlakyMinTransitions      int

	// Retry tuning
	RetryMaxAttempts    int
	RetryBaseDelay      time.Duration
	RetryMaxDelay       time.Duration
	RetryCircuitBreaker int // Fail fast after N consecutive retryable failures

	// HTTP connection pooling
	MaxIdleConns        int           // Max idle connections per host
	MaxIdleConnsPerHost int           // Max idle connections per host
	MaxConnsPerHost     int           // Max total connections per host
	IdleConnTimeout     time.Duration // Idle connection timeout

	// Prometheus metrics
	PromDir string `mapstructure:"prom-dir"`

	// Email
	EmailEnabled    bool
	EmailAttachHTML bool // Attach per-cluster (or digest) HTML report to email
	NotifyDigest    bool // When true: one email/webhook/slack per run (with run overview); when false: per-cluster
	SMTPServer      string
	SMTPPort        int
	SMTPUser        string
	SMTPPassword    string
	EmailFrom       string
	EmailTo         []string
	EmailUseTLS     bool

	// Webhook
	WebhookEnabled     bool
	WebhookIncludeHTML bool // Include per-cluster HTML report as base64 in webhook payload
	WebhookURL         string
	WebhookHeaders     map[string]string `mapstructure:"webhook-headers"`

	// Slack
	SlackEnabled    bool
	SlackWebhookURL string `mapstructure:"slack-webhook-url"`
	SlackChannel    string `mapstructure:"slack-channel"`

	// Secrets
	SecretsProvider string
	SecretsFile     string

	// NCCAPIVersion is normalized to "v4" or "v1" (Legacy). Config accepts v4, Legacy, or v1 (alias for Legacy).
	NCCAPIVersion string `mapstructure:"ncc-api-version"`

	// NutanixV4APIVersion is the v4 REST API path revision (e.g. v4.2, v4.1, v4.0.a1) for /api/clustermgmt/{ver}/ and /api/monitoring/{ver}/.
	NutanixV4APIVersion string `mapstructure:"nutanix-v4-api-version"`
}

type ClusterCredential struct {
	Username string
	Password string
}

type NotificationSummary struct {
	Cluster          string
	StartedAt        time.Time
	FinishedAt       time.Time
	FailCount        int
	WarnCount        int
	ErrCount         int
	InfoCount        int
	TotalChecks      int
	OutputFiles      []string
	Overview         string // Brief text summary for email body and webhook
	ReportHTMLBase64 string `json:"ReportHTMLBase64,omitempty"` // Optional: base64-encoded HTML when WebhookIncludeHTML
}

type HTMLMeta struct {
	ClusterName    string
	ClusterVersion string
	NCCVersion     string
}

type HTMLData struct {
	Rows    []Row
	Now     string
	Meta    HTMLMeta
	Summary SummaryCounts
}

// SummaryCounts holds per-severity counts for the report header.
type SummaryCounts struct {
	FAIL, WARN, ERR, INFO int
}

const termsText = `
This script is created by Prajwal Vernekar (prajwal.vernekar@nutanix.com).

Script Description:
Nutanix NCC Orchestrator is a CLI tool to run NCC checks across multiple clusters in parallel, aggregate results, and generate HTML/CSV reports.

How the Script Works:
- Reads configuration from config file, environment variables, or CLI flags.
- Starts NCC checks on each cluster via API.
- Polls for completion and fetches summaries.
- Generates per-cluster and aggregated reports in specified formats.

Usage:
./ncc-orchestrator [flags]
./ncc-orchestrator --help for more details.

Instructions for config.yaml File:
Create a config.yaml with keys like:
# Required
clusters: "10.0.XX.XX,10.1.XX.XX"      	  # Comma-separated list of Prism cluster IPs/hosts  
username: "admin"                         # Prism username  
password: ""                              # Prefer env NCC_PASSWORD in CI; leave empty here if using env  
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only)
nutanix-v4-api-version: v4.2              # v4 path revision: v4.2 (default), v4.1, v4.0.a1, etc.

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed  
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
artifact-retain-days: 0                   # Remove generated artifacts older than N days (0 = unlimited)
artifact-retain-max-files: 0              # Keep only N newest generated artifacts (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s
policy-gates: ""                          # e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0
quiet-hours: ""                           # Local HH:MM-HH:MM notification quiet window
maintenance-windows: ""                   # RFC3339 windows start/end[,start/end...]
flaky-lookback-runs: 6                    # Runs to inspect for flaky checks
flaky-min-transitions: 2                  # Minimum severity transitions to mark flaky
exclude-alert-titles: ""                  # Comma-separated NCC alert titles to exclude from final reports/HTML
exclude-alert-titles-file: ""             # Optional file with one alert title per line
exclude-alert-match-mode: "exact"         # exact (default), contains, regex

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  
 
# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true

# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
secrets-provider: ""                      # env or file
secrets-file: ""                          # YAML/JSON key-value map when secrets-provider=file

Use --config to specify file path.

Nutanix APIs used:

With ncc-api-version v4 (default): start checks via POST .../api/monitoring/{nutanix-v4-api-version}/serviceability/clusters/{uuid}/$actions/run-system-defined-checks (cluster UUID from GET .../api/clustermgmt/{nutanix-v4-api-version}/config/clusters, with fallback to Prism Gateway v1 cluster). Poll task via GET .../api/prism/{nutanix-v4-api-version}/config/tasks/{extId} when extId is returned, else v2.0/tasks. Configure nutanix-v4-api-version (default v4.2) for API revisions such as v4.1 or v4.0.a1. If v4 start-checks is unavailable (404), the tool falls back to Legacy start-checks.

With ncc-api-version Legacy: POST https://{cluster_IP}:9440/PrismGateway/services/rest/v1/ncc/checks only.

Task polling: with ncc-api-version v4 and a task extId from start-checks, GET https://{cluster_IP}:9440/api/prism/{nutanix-v4-api-version}/config/tasks/{extId} (extId URL-encoded); on HTTP 404, falls back to Prism Gateway GET .../v2.0/tasks/{uuid}. Summary: GET https://{cluster_IP}:9440/PrismGateway/services/rest/v1/ncc/{uuid}.

Disclaimer:
     Use at your own risk. Running this program implies acceptance of associated risks.
     The developer or Nutanix shall not be held liable for any consequences resulting from its use.
`

// ==================== Constants ====================

const (
	// Default values
	defaultTimeout             = 15 * time.Minute
	defaultRequestTimeout      = 20 * time.Second
	defaultPollInterval        = 15 * time.Second
	defaultPollJitter          = 2 * time.Second
	defaultMaxParallel         = 4
	defaultRetryAttempts       = 6
	defaultRetryBaseDelay      = 400 * time.Millisecond
	defaultRetryMaxDelay       = 8 * time.Second
	defaultRetryCircuitBreaker = 3
	defaultOutputDirLogs       = "nccfiles"
	defaultOutputDirFiltered   = "outputfiles"
	defaultPromDir             = "promfiles"
	defaultLogFile             = "logs/ncc-runner.log"
	defaultOutputFormat        = "html"
	defaultExcludeMatchMode    = "exact"
	maxSecretsFileBytes        = 1 << 20 // 1 MiB

	// HTTP connection pooling defaults
	defaultMaxIdleConns        = 100
	defaultMaxIdleConnsPerHost = 10
	defaultMaxConnsPerHost     = 0 // 0 = unlimited
	defaultIdleConnTimeout     = 90 * time.Second

	// Graceful shutdown
	shutdownTimeout = 30 * time.Second

	// Security
	minPasswordLength = 1
	maxClusterNameLen = 255
	maxURLLength      = 2048

	// Prism Gateway port
	prismGatewayPort = 9440

	// defaultNutanixV4APIVersion is the default Nutanix v4 API path segment (clustermgmt / monitoring).
	defaultNutanixV4APIVersion = "v4.2"
	defaultFlakyLookbackRuns   = 6
	defaultFlakyTransitions    = 2
)

// ==================== Exit Codes ====================
// Exit code 0 = success, 1 = run/execution error, 2 = config/validation error (see README).

type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }
func (e *exitCodeError) Unwrap() error { return e.err }

func exitConfig(err error) error { return &exitCodeError{code: 2, err: err} }

// exitPartial indicates some clusters succeeded and some failed (exit code 3).
func exitPartial(err error) error { return &exitCodeError{code: 3, err: err} }

// ==================== Utility Functions ====================

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func splitCSVTrimLower(s string) []string {
	in := splitCSV(s)
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strings.ToLower(strings.TrimSpace(v)))
	}
	return out
}

func loadExcludeAlertTitlesFromFile(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(b), "\n")
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		v := strings.TrimSpace(ln)
		if v == "" || strings.HasPrefix(v, "#") {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

func mergeUniqueStrings(items ...[]string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, arr := range items {
		for _, raw := range arr {
			v := strings.TrimSpace(raw)
			if v == "" {
				continue
			}
			k := strings.ToLower(v)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, v)
		}
	}
	return out
}

func parseHHMM(s string) (int, int, error) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected HH:MM")
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, 0, fmt.Errorf("time out of range")
	}
	return h, m, nil
}

func inQuietHours(now time.Time, quietHours string) (bool, error) {
	q := strings.TrimSpace(quietHours)
	if q == "" {
		return false, nil
	}
	parts := strings.Split(q, "-")
	if len(parts) != 2 {
		return false, fmt.Errorf("quiet-hours must be HH:MM-HH:MM")
	}
	sh, sm, err := parseHHMM(parts[0])
	if err != nil {
		return false, fmt.Errorf("quiet-hours start: %w", err)
	}
	eh, em, err := parseHHMM(parts[1])
	if err != nil {
		return false, fmt.Errorf("quiet-hours end: %w", err)
	}
	curMins := now.Hour()*60 + now.Minute()
	startMins := sh*60 + sm
	endMins := eh*60 + em
	if startMins == endMins {
		return true, nil // full-day quiet window
	}
	if startMins < endMins {
		return curMins >= startMins && curMins < endMins, nil
	}
	return curMins >= startMins || curMins < endMins, nil
}

func inMaintenanceWindow(now time.Time, windows []string) (bool, error) {
	for _, w := range windows {
		w = strings.TrimSpace(w)
		if w == "" {
			continue
		}
		parts := strings.Split(w, "/")
		if len(parts) != 2 {
			return false, fmt.Errorf("maintenance window %q must be start/end RFC3339", w)
		}
		start, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[0]))
		if err != nil {
			return false, fmt.Errorf("maintenance window start %q: %w", parts[0], err)
		}
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(parts[1]))
		if err != nil {
			return false, fmt.Errorf("maintenance window end %q: %w", parts[1], err)
		}
		if !end.After(start) {
			return false, fmt.Errorf("maintenance window %q end must be after start", w)
		}
		if (now.Equal(start) || now.After(start)) && now.Before(end) {
			return true, nil
		}
	}
	return false, nil
}

func notificationsSuppressedNow(cfg Config, now time.Time) (bool, string) {
	if ok, err := inQuietHours(now, cfg.QuietHours); err == nil && ok {
		return true, "quiet-hours"
	}
	if ok, err := inMaintenanceWindow(now, cfg.MaintenanceWindows); err == nil && ok {
		return true, "maintenance-window"
	}
	return false, ""
}

func validateSecretsFileHardening(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return errors.New("secrets-file must not be a symlink")
	}
	if !st.Mode().IsRegular() {
		return errors.New("secrets-file must be a regular file")
	}
	if st.Size() > maxSecretsFileBytes {
		return fmt.Errorf("secrets-file exceeds max size (%d bytes)", maxSecretsFileBytes)
	}
	if st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("secrets-file permissions are too open (%#o); expected owner-only (e.g. 0600)", st.Mode().Perm())
	}
	return nil
}

func loadSecretMapFile(path string) (map[string]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("secrets-file is required for provider=file")
	}
	if err := validateSecretsFileHardening(path); err != nil {
		return nil, fmt.Errorf("secrets-file hardening check failed: %w", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out map[string]string
	if err := json.Unmarshal(b, &out); err == nil {
		return out, nil
	}
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("parse secrets-file: %w", err)
	}
	raw := v.AllSettings()
	out = make(map[string]string, len(raw))
	for k, val := range raw {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out, nil
}

func resolveSecretRef(ref string, provider string, fileSecrets map[string]string) (string, error) {
	v := strings.TrimSpace(ref)
	if !strings.HasPrefix(v, "secret://") {
		return v, nil
	}
	name := strings.TrimPrefix(v, "secret://")
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("empty secret reference")
	}
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "env":
		if val := os.Getenv(name); val != "" {
			return val, nil
		}
		if val := os.Getenv("NCC_SECRET_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))); val != "" {
			return val, nil
		}
		return "", fmt.Errorf("secret %q not found in env", name)
	case "file":
		if val, ok := fileSecrets[name]; ok {
			return val, nil
		}
		return "", fmt.Errorf("secret %q not found in secrets-file", name)
	default:
		return "", fmt.Errorf("secrets-provider must be env or file when secret:// refs are used")
	}
}

func applySecretsToConfig(cfg *Config) error {
	provider := strings.ToLower(strings.TrimSpace(cfg.SecretsProvider))
	var fileSecrets map[string]string
	var err error
	if provider == "file" {
		fileSecrets, err = loadSecretMapFile(cfg.SecretsFile)
		if err != nil {
			return fmt.Errorf("load secrets-file: %w", err)
		}
	}
	cfg.Password, err = resolveSecretRef(cfg.Password, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("password: %w", err)
	}
	cfg.SMTPPassword, err = resolveSecretRef(cfg.SMTPPassword, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("smtp-password: %w", err)
	}
	cfg.WebhookURL, err = resolveSecretRef(cfg.WebhookURL, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("webhook-url: %w", err)
	}
	cfg.SlackWebhookURL, err = resolveSecretRef(cfg.SlackWebhookURL, provider, fileSecrets)
	if err != nil {
		return fmt.Errorf("slack-webhook-url: %w", err)
	}
	for cluster, cred := range cfg.ClusterCredentials {
		if strings.TrimSpace(cred.Username) != "" {
			cred.Username, err = resolveSecretRef(cred.Username, provider, fileSecrets)
			if err != nil {
				return fmt.Errorf("clusters-file username for cluster %s: %w", cluster, err)
			}
		}
		if strings.TrimSpace(cred.Password) != "" {
			cred.Password, err = resolveSecretRef(cred.Password, provider, fileSecrets)
			if err != nil {
				return fmt.Errorf("clusters-file password for cluster %s: %w", cluster, err)
			}
		}
		cfg.ClusterCredentials[cluster] = cred
	}
	return nil
}

// readClusterFile reads cluster targets from file.
// Supported formats per non-comment line:
//  1. cluster
//  2. cluster,username
//  3. cluster,username,password
//
// Blank and # lines are ignored.
func readClusterFile(path string) ([]string, map[string]ClusterCredential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var out []string
	creds := make(map[string]ClusterCredential)
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cluster, user, pass, err := parseClusterFileLine(line)
		if err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		out = append(out, cluster)
		if user != "" || pass != "" {
			creds[cluster] = ClusterCredential{Username: user, Password: pass}
		}
	}
	return out, creds, nil
}

func parseClusterFileLine(line string) (cluster string, username string, password string, err error) {
	r := csv.NewReader(strings.NewReader(line))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	rec, err := r.Read()
	if err != nil {
		return "", "", "", fmt.Errorf("invalid cluster entry: %w", err)
	}
	for i := range rec {
		rec[i] = strings.TrimSpace(rec[i])
	}
	switch len(rec) {
	case 1:
		if rec[0] == "" {
			return "", "", "", errors.New("cluster value is empty")
		}
		return rec[0], "", "", nil
	case 2:
		if rec[0] == "" {
			return "", "", "", errors.New("cluster value is empty")
		}
		if rec[1] == "" {
			return "", "", "", errors.New("username is empty")
		}
		return rec[0], rec[1], "", nil
	case 3:
		if rec[0] == "" {
			return "", "", "", errors.New("cluster value is empty")
		}
		if rec[1] == "" {
			return "", "", "", errors.New("username is empty")
		}
		return rec[0], rec[1], rec[2], nil
	default:
		return "", "", "", fmt.Errorf("expected 1, 2, or 3 CSV fields (cluster[,username[,password]]), got %d", len(rec))
	}
}

func credentialsForCluster(cfg Config, cluster string) (string, string) {
	user := strings.TrimSpace(cfg.Username)
	pass := cfg.Password
	if cred, ok := cfg.ClusterCredentials[cluster]; ok {
		if strings.TrimSpace(cred.Username) != "" {
			user = strings.TrimSpace(cred.Username)
		}
		if strings.TrimSpace(cred.Password) != "" {
			pass = cred.Password
		}
	}
	return user, pass
}

func validateClusterCredentialCoverage(cfg Config) error {
	for _, cluster := range cfg.Clusters {
		user, _ := credentialsForCluster(cfg, cluster)
		if strings.TrimSpace(user) == "" {
			return fmt.Errorf("missing username for cluster %s (set global username or provide cluster,username[,password] in clusters-file)", cluster)
		}
		if len(user) > 255 {
			return fmt.Errorf("username too long for cluster %s (max 255 characters)", cluster)
		}
	}
	return nil
}

func needsPasswordPrompt(cfg Config) (bool, string) {
	if strings.TrimSpace(cfg.Password) != "" {
		return false, ""
	}
	for _, cluster := range cfg.Clusters {
		user, pass := credentialsForCluster(cfg, cluster)
		if strings.TrimSpace(pass) == "" {
			return true, user
		}
	}
	return false, ""
}

func mustParseDur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

// ==================== Validation Functions ====================

// validateClusterAddress validates cluster IP or hostname
func validateClusterAddress(cluster string) error {
	if cluster == "" {
		return errors.New("cluster address cannot be empty")
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
		if err := validateClusterAddress(cluster); err != nil {
			return fmt.Errorf("cluster %d (%s): %w", i+1, cluster, err)
		}
		if seen[cluster] {
			return fmt.Errorf("duplicate cluster address: %s", cluster)
		}
		seen[cluster] = true
	}

	return nil
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
	// Validate clusters
	if err := validateClusters(cfg.Clusters); err != nil {
		return fmt.Errorf("cluster validation failed: %w", err)
	}

	// Validate effective credentials (global or per-cluster from clusters-file)
	if err := validateClusterCredentialCoverage(cfg); err != nil {
		return err
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
	if cfg.MaxParallel <= 0 {
		return errors.New("max-parallel must be greater than 0")
	}
	if cfg.MaxParallel > 100 {
		return errors.New("max-parallel cannot exceed 100 (safety limit)")
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
	if strings.TrimSpace(cfg.PromDir) == "" {
		return errors.New("prom-dir cannot be empty")
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

func writeDummyConfig(path string) error {
	ext := strings.ToLower(filepath.Ext(path))
	dummy := ""
	switch ext {
	case ".yaml", ".yml":
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
# clusters-file: ""                        # Optional: cluster or cluster,username[,password] per line; overrides clusters when set
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias)
nutanix-v4-api-version: v4.2              # v4 path revision (v4.2 default; e.g. v4.1, v4.0.a1)

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
artifact-retain-days: 0                   # Remove generated artifacts older than N days (0 = unlimited)
artifact-retain-max-files: 0              # Keep only N newest generated artifacts (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s
policy-gates: ""                          # e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0
quiet-hours: ""                           # Local HH:MM-HH:MM notification quiet window
maintenance-windows: ""                   # RFC3339 windows start/end[,start/end...]
flaky-lookback-runs: 6                    # Runs to inspect for flaky checks
flaky-min-transitions: 2                  # Minimum severity transitions to mark flaky
exclude-alert-titles: ""                  # Comma-separated NCC alert titles to exclude from final reports/HTML
exclude-alert-titles-file: ""             # Optional file with one alert title per line
exclude-alert-match-mode: "exact"         # exact (default), contains, regex

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  

# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true

# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
secrets-provider: ""                      # env or file
secrets-file: ""                          # YAML/JSON key-value map when secrets-provider=file

`
	case ".json":
		dummy = `{
  "clusters": "10.0.0.1,10.0.0.2",
  "clusters-file": "",
  "username": "admin",
  "password": "",
  "ncc-api-version": "v4",
  "nutanix-v4-api-version": "v4.2",
  "insecure-skip-verify": false,
  "timeout": "15m",
  "request-timeout": "30s",
  "poll-interval": "15s",
  "poll-jitter": "2s",
  "max-parallel": 4,
  "outputs": "html,csv",
  "output-dir-logs": "nccfiles",
  "output-dir-filtered": "outputfiles",
  "single-report": false,
  "run-history": false,
  "run-history-dir": "outputfiles/runs",
  "retain-last": 0,
  "retain-days": 0,
  "artifact-retain-days": 0,
  "artifact-retain-max-files": 0,
  "notify-on-regression": false,
  "adaptive-parallelism": true,
  "policy-gates": "",
  "quiet-hours": "",
  "maintenance-windows": "",
  "flaky-lookback-runs": 6,
  "flaky-min-transitions": 2,
  "exclude-alert-titles": "",
  "exclude-alert-titles-file": "",
  "exclude-alert-match-mode": "exact",
  "log-file": "logs/ncc-runner.log",
  "log-level": "2",
  "log-http": false,
  "retry-max-attempts": 6,
  "retry-base-delay": "400ms",
  "retry-max-delay": "8s",
  "retry-circuit-breaker": 3,
  "email-enabled": false,
  "email-attach-html": false,
  "notify-digest": false,
  "smtp-server": "smtp.example.com",
  "smtp-port": 587,
  "smtp-user": "ncc@example.com",
  "smtp-password": "",
  "email-from": "ncc@example.com",
  "email-to": "ops@example.com,sre@example.com",
  "email-use-tls": true,
  "webhook-enabled": false,
  "webhook-include-html": false,
  "webhook-url": "https://hooks.example.com/ncc",
  "webhook-headers": {
    "X-Auth-Token": "changeme"
  },
  "slack-enabled": false,
  "slack-webhook-url": "",
  "slack-channel": "",
  "secrets-provider": "",
  "secrets-file": ""
}
`
	default:
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
# clusters-file: ""                        # Optional: cluster or cluster,username[,password] per line; overrides clusters when set
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias)
nutanix-v4-api-version: v4.2              # v4 path revision (v4.2 default; e.g. v4.1, v4.0.a1)

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  

# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true

# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
`
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(dummy), 0644)
}

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
		"config":   true,
		"update":   true,
		"clusters": true, "clusters-file": true, "prism-central-url": true, "discover-api-version": true,
		"username": true, "password": true, "ncc-api-version": true, "nutanix-v4-api-version": true,
		"insecure-skip-verify": true, "timeout": true, "request-timeout": true, "poll-interval": true, "poll-jitter": true,
		"max-parallel": true, "outputs": true, "output-dir-logs": true, "output-dir-filtered": true,
		"single-report": true, "run-history": true, "run-history-dir": true, "retain-last": true, "retain-days": true, "artifact-retain-days": true, "artifact-retain-max-files": true,
		"notify-on-regression": true, "adaptive-parallelism": true,
		"policy-gates": true, "quiet-hours": true, "maintenance-windows": true,
		"flaky-lookback-runs": true, "flaky-min-transitions": true,
		"log-file": true, "log-level": true, "log-http": true,
		"retry-max-attempts": true, "retry-base-delay": true, "retry-max-delay": true, "retry-circuit-breaker": true,
		"prom-dir": true, "severity-filter": true, "exclude-alert-titles": true, "exclude-alert-titles-file": true, "exclude-alert-match-mode": true, "dry-run": true, "replay": true,
		"max-idle-conns": true, "max-idle-conns-per-host": true, "max-conns-per-host": true, "idle-conn-timeout": true,
		"gen-test-agg":  true,
		"email-enabled": true, "email-attach-html": true, "notify-digest": true,
		"smtp-server": true, "smtp-port": true, "smtp-user": true, "smtp-password": true,
		"email-from": true, "email-to": true, "email-use-tls": true,
		"webhook-enabled": true, "webhook-include-html": true, "webhook-url": true, "webhook-headers": true,
		"slack-enabled": true, "slack-webhook-url": true, "slack-channel": true,
		"secrets-provider": true, "secrets-file": true,
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
		"clusters", "clusters-file", "prism-central-url", "discover-api-version",
		"username", "password", "ncc-api-version", "nutanix-v4-api-version",
		"timeout", "request-timeout", "poll-interval", "poll-jitter",
		"outputs", "output-dir-logs", "output-dir-filtered", "run-history-dir",
		"log-file", "log-level", "retry-base-delay", "retry-max-delay", "prom-dir",
		"severity-filter", "exclude-alert-titles", "exclude-alert-titles-file", "exclude-alert-match-mode", "idle-conn-timeout", "policy-gates", "quiet-hours", "maintenance-windows",
		"smtp-server", "smtp-user", "smtp-password", "email-from", "email-to",
		"webhook-url", "slack-webhook-url", "slack-channel", "secrets-provider", "secrets-file",
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
		"insecure-skip-verify", "dry-run", "replay", "log-http",
		"run-history", "single-report", "notify-on-regression", "adaptive-parallelism",
		"email-enabled", "email-attach-html", "notify-digest", "email-use-tls",
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
		"max-parallel", "retry-max-attempts", "retry-circuit-breaker", "max-idle-conns", "max-idle-conns-per-host",
		"max-conns-per-host", "smtp-port", "retain-last", "retain-days", "artifact-retain-days", "artifact-retain-max-files", "gen-test-agg",
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
		"retry-base-delay", "retry-max-delay", "idle-conn-timeout",
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
	return nil
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

	clustersFromFlag := splitCSV(viper.GetString("clusters"))
	clustersFile := strings.TrimSpace(viper.GetString("clusters-file"))
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
	nccAPIVer, err := normalizeNCCAPIVersion(viper.GetString("ncc-api-version"))
	if err != nil {
		return Config{}, fmt.Errorf("ncc-api-version: %w", err)
	}
	cfg := Config{
		Clusters:               clustersFromFlag,
		ClustersFile:           clustersFile,
		ClusterCredentials:     clusterCreds,
		Username:               viper.GetString("username"),
		Password:               viper.GetString("password"),
		InsecureSkipVerify:     viper.GetBool("insecure-skip-verify"),
		Timeout:                mustParseDur(viper.GetString("timeout"), defaultTimeout),
		RequestTimeout:         mustParseDur(viper.GetString("request-timeout"), defaultRequestTimeout),
		PollInterval:           mustParseDur(viper.GetString("poll-interval"), defaultPollInterval),
		PollJitter:             mustParseDur(viper.GetString("poll-jitter"), defaultPollJitter),
		OutputDirLogs:          viper.GetString("output-dir-logs"),
		OutputDirFiltered:      viper.GetString("output-dir-filtered"),
		OutputFormats:          splitCSV(viper.GetString("outputs")),
		MaxParallel:            viper.GetInt("max-parallel"),
		TLSMinVersion:          tls.VersionTLS12,
		LogFile:                viper.GetString("log-file"),
		LogLevel:               viper.GetString("log-level"),
		LogHTTP:                viper.GetBool("log-http"),
		RetryMaxAttempts:       viper.GetInt("retry-max-attempts"),
		RetryBaseDelay:         mustParseDur(viper.GetString("retry-base-delay"), defaultRetryBaseDelay),
		RetryMaxDelay:          mustParseDur(viper.GetString("retry-max-delay"), defaultRetryMaxDelay),
		RetryCircuitBreaker:    viper.GetInt("retry-circuit-breaker"),
		MaxIdleConns:           viper.GetInt("max-idle-conns"),
		MaxIdleConnsPerHost:    viper.GetInt("max-idle-conns-per-host"),
		MaxConnsPerHost:        viper.GetInt("max-conns-per-host"),
		IdleConnTimeout:        mustParseDur(viper.GetString("idle-conn-timeout"), defaultIdleConnTimeout),
		EmailEnabled:           viper.GetBool("email-enabled"),
		EmailAttachHTML:        viper.GetBool("email-attach-html"),
		NotifyDigest:           viper.GetBool("notify-digest"),
		SMTPServer:             viper.GetString("smtp-server"),
		SMTPPort:               viper.GetInt("smtp-port"),
		SMTPUser:               viper.GetString("smtp-user"),
		SMTPPassword:           viper.GetString("smtp-password"),
		EmailFrom:              viper.GetString("email-from"),
		EmailTo:                splitCSV(viper.GetString("email-to")),
		EmailUseTLS:            viper.GetBool("email-use-tls"),
		WebhookEnabled:         viper.GetBool("webhook-enabled"),
		WebhookIncludeHTML:     viper.GetBool("webhook-include-html"),
		WebhookURL:             viper.GetString("webhook-url"),
		WebhookHeaders:         viper.GetStringMapString("webhook-headers"),
		SeverityFilter:         splitCSV(viper.GetString("severity-filter")),
		ExcludeAlertTitles:     splitCSV(viper.GetString("exclude-alert-titles")),
		ExcludeAlertTitlesFile: strings.TrimSpace(viper.GetString("exclude-alert-titles-file")),
		ExcludeAlertMatchMode:  strings.TrimSpace(viper.GetString("exclude-alert-match-mode")),
		DryRun:                 viper.GetBool("dry-run"),
		RunHistoryEnabled:      viper.GetBool("run-history"),
		RunHistoryDir:          strings.TrimSpace(viper.GetString("run-history-dir")),
		RetainLastRuns:         viper.GetInt("retain-last"),
		RetainDays:             viper.GetInt("retain-days"),
		ArtifactRetainDays:     viper.GetInt("artifact-retain-days"),
		ArtifactRetainMaxFiles: viper.GetInt("artifact-retain-max-files"),
		SingleReport:           viper.GetBool("single-report"),
		NotifyOnRegression:     viper.GetBool("notify-on-regression"),
		AdaptiveParallelism:    viper.GetBool("adaptive-parallelism"),
		PolicyGates:            splitCSV(viper.GetString("policy-gates")),
		QuietHours:             strings.TrimSpace(viper.GetString("quiet-hours")),
		MaintenanceWindows:     splitCSV(viper.GetString("maintenance-windows")),
		FlakyLookbackRuns:      viper.GetInt("flaky-lookback-runs"),
		FlakyMinTransitions:    viper.GetInt("flaky-min-transitions"),
		SlackEnabled:           viper.GetBool("slack-enabled"),
		SlackWebhookURL:        viper.GetString("slack-webhook-url"),
		SlackChannel:           viper.GetString("slack-channel"),
		SecretsProvider:        strings.TrimSpace(viper.GetString("secrets-provider")),
		SecretsFile:            strings.TrimSpace(viper.GetString("secrets-file")),
		NCCAPIVersion:          nccAPIVer,
		NutanixV4APIVersion:    strings.ToLower(strings.TrimSpace(viper.GetString("nutanix-v4-api-version"))),
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
	if len(cfg.OutputFormats) == 0 {
		cfg.OutputFormats = []string{defaultOutputFormat}
	}
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = defaultMaxParallel
	}
	if cfg.LogFile == "" {
		cfg.LogFile = defaultLogFile
	}
	cfg.PromDir = viper.GetString("prom-dir")
	if cfg.PromDir == "" {
		cfg.PromDir = defaultPromDir
	}
	if cfg.ExcludeAlertMatchMode == "" {
		cfg.ExcludeAlertMatchMode = defaultExcludeMatchMode
	}
	if cfg.ExcludeAlertTitlesFile != "" {
		fileTitles, err := loadExcludeAlertTitlesFromFile(cfg.ExcludeAlertTitlesFile)
		if err != nil {
			return cfg, fmt.Errorf("exclude-alert-titles-file: %w", err)
		}
		cfg.ExcludeAlertTitles = mergeUniqueStrings(cfg.ExcludeAlertTitles, fileTitles)
	}
	mode, err := normalizeExcludeAlertMatchMode(cfg.ExcludeAlertMatchMode)
	if err != nil {
		return cfg, err
	}
	cfg.ExcludeAlertMatchMode = mode
	if cfg.RunHistoryDir == "" {
		cfg.RunHistoryDir = filepath.Join(cfg.OutputDirFiltered, "runs")
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

// checkOutputPermissions verifies the process can create/open files for write in the log path
// and in each output directory. Returns a clear error on first failure so permission issues
// are reported early instead of during normal writes.
func checkOutputPermissions(cfg *Config) error {
	probeName := ".ncc-writecheck"
	indexHTML := filepath.Join(cfg.OutputDirFiltered, "index.html")
	checks := []struct {
		label    string
		path     string
		remove   bool
		truncate bool // open with O_TRUNC so we can overwrite existing file (e.g. NFS stale ownership)
	}{
		{"log file", cfg.LogFile, false, false},
		{"output dir (raw logs)", filepath.Join(cfg.OutputDirLogs, probeName), true, false},
		{"output dir (filtered)", filepath.Join(cfg.OutputDirFiltered, probeName), true, false},
		{"aggregated index.html", indexHTML, false, true},
		{"prom dir", filepath.Join(cfg.PromDir, probeName), true, false},
	}
	for _, c := range checks {
		dir := filepath.Dir(c.path)
		if dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("cannot create directory %s: %w", dir, err)
			}
		}
		flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
		if c.truncate {
			flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
		}
		f, err := os.OpenFile(c.path, flags, 0644)
		if err != nil {
			return fmt.Errorf("cannot open/create file for write (%s): %w", c.label, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close probe file %s: %w", c.path, err)
		}
		if c.remove {
			_ = os.Remove(c.path)
		}
	}
	return nil
}

// ==================== Logging ====================

// In setupFileLogger, add the new version fields to the global logger context
func setupFileLogger(logPath string, lvl zerolog.Level) error {
	dir := filepath.Dir(logPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	fileWriter := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    20, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	var gitRevision string
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				gitRevision = s.Value
				break
			}
		}
		log.Logger = zerolog.New(fileWriter).Level(lvl).With().
			Timestamp().
			Str("git_revision", gitRevision).
			Str("go_version", bi.GoVersion).
			Str("Version", Version).
			Str("stream", Stream).
			Logger()
	} else {
		log.Logger = zerolog.New(fileWriter).Level(lvl).With().Timestamp().Logger()
	}
	return nil
}

// ==================== Retry Helpers ====================

func jitteredBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	capDelay := time.Duration(exp)
	if capDelay > maxDelay {
		capDelay = maxDelay
	}
	if capDelay <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(capDelay)))
}

func isRetryableStatus(code int) bool {
	switch code {
	case 408, 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

// maxRateLimitBackoff caps Retry-After / computed backoff for HTTP 429 so a misbehaving server cannot sleep unbounded.
const maxRateLimitBackoff = 2 * time.Minute

var (
	adaptiveCurrentParallel int32
	adaptiveMaxParallel     int32
)

func resetAdaptiveParallelism(maxParallel int) {
	if maxParallel < 1 {
		maxParallel = 1
	}
	atomic.StoreInt32(&adaptiveMaxParallel, int32(maxParallel))
	atomic.StoreInt32(&adaptiveCurrentParallel, int32(maxParallel))
}

func currentAdaptiveParallel(maxParallel int) int {
	cur := int(atomic.LoadInt32(&adaptiveCurrentParallel))
	if cur < 1 {
		return 1
	}
	if cur > maxParallel {
		return maxParallel
	}
	return cur
}

func noteHTTPStatusForAdaptiveParallelism(status int, cfg Config) {
	if !cfg.AdaptiveParallelism {
		return
	}
	maxP := int(atomic.LoadInt32(&adaptiveMaxParallel))
	if maxP <= 1 {
		return
	}
	switch status {
	case http.StatusTooManyRequests:
		for {
			cur := atomic.LoadInt32(&adaptiveCurrentParallel)
			if cur <= 1 {
				return
			}
			next := cur - 1
			if atomic.CompareAndSwapInt32(&adaptiveCurrentParallel, cur, next) {
				log.Warn().Int("adaptive_parallel", int(next)).Int("max_parallel", maxP).Msg("adaptive parallelism reduced after 429")
				return
			}
		}
	default:
		if status >= 200 && status < 300 {
			for {
				cur := atomic.LoadInt32(&adaptiveCurrentParallel)
				max := atomic.LoadInt32(&adaptiveMaxParallel)
				if cur >= max {
					return
				}
				next := cur + 1
				if atomic.CompareAndSwapInt32(&adaptiveCurrentParallel, cur, next) {
					return
				}
			}
		}
	}
}

func capRateLimitWait(d time.Duration) time.Duration {
	if d > maxRateLimitBackoff {
		return maxRateLimitBackoff
	}
	return d
}

func logRateLimitHeaders(op string, resp *http.Response) {
	if resp == nil {
		return
	}
	rem := resp.Header.Get("X-RateLimit-Remaining")
	reset := resp.Header.Get("X-RateLimit-Reset")
	apiRem := resp.Header.Get("X-Api-Ratelimit-Remaining")
	if rem == "" && reset == "" && apiRem == "" {
		return
	}
	log.Warn().Str("op", op).Str("X-RateLimit-Remaining", rem).Str("X-RateLimit-Reset", reset).
		Str("X-Api-Ratelimit-Remaining", apiRem).Msg("rate limit headers")
}

func retryAfterDelay(resp *http.Response) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(ra); err == nil {
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(ra); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

// ==================== HTTP Client and File System ====================

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type LoggingTransport struct {
	Base    http.RoundTripper
	MaxBody int // bytes; 0 = unlimited
}

// redactHTTPDump masks Authorization and other sensitive headers/body in HTTP dumps for logging.
func redactHTTPDump(dump []byte, maxBody int) []byte {
	lines := bytes.SplitAfter(dump, []byte("\n"))
	var out []byte
	for _, line := range lines {
		if bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("Authorization:")) ||
			bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("authorization:")) {
			out = append(out, []byte("Authorization: [REDACTED]\r\n")...)
			continue
		}
		if bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("Cookie:")) ||
			bytes.HasPrefix(bytes.TrimLeft(line, " \t"), []byte("cookie:")) {
			out = append(out, []byte("Cookie: [REDACTED]\r\n")...)
			continue
		}
		out = append(out, line...)
	}
	// If body looks like JSON and contains password field, mask the value
	if bytes.Contains(out, []byte(`"password"`)) || bytes.Contains(out, []byte(`"Password"`)) {
		out = redactJSONPasswordValue(out)
	}
	if maxBody > 0 && len(out) > maxBody {
		out = append(append([]byte(nil), out[:maxBody]...), []byte("...[truncated]")...)
	}
	return out
}

func redactJSONPasswordValue(b []byte) []byte {
	// Replace "password":"<anything>" or "password": "<anything>" with "password":"[REDACTED]"
	i := 0
	for {
		idx := bytes.Index(b[i:], []byte(`"password"`))
		if idx < 0 {
			idx = bytes.Index(b[i:], []byte(`"Password"`))
		}
		if idx < 0 {
			break
		}
		start := i + idx
		i = start + 10 // past the key
		// Skip whitespace and colon
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == ':') {
			i++
		}
		for i < len(b) && (b[i] == ' ' || b[i] == '\t') {
			i++
		}
		if i >= len(b) {
			break
		}
		// Find value end (string or number)
		valueStart := i
		if b[i] == '"' {
			i++
			for i < len(b) && b[i] != '"' {
				if b[i] == '\\' {
					i++
				}
				i++
			}
			if i < len(b) {
				i++
			}
		} else {
			for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != '\n' && b[i] != '\r' {
				i++
			}
		}
		// Replace value with [REDACTED]
		replacement := []byte(`"[REDACTED]"`)
		b = append(append(append([]byte(nil), b[:valueStart]...), replacement...), b[i:]...)
		i = valueStart + len(replacement)
	}
	return b
}

func (t *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if d, err := httputil.DumpRequestOut(req, true); err == nil {
		dump := redactHTTPDump(d, t.MaxBody)
		log.Debug().
			Str("method", req.Method).
			Str("url", req.URL.String()).
			RawJSON("request_dump", dump).
			Msg("http request")
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		log.Error().Err(err).Str("url", req.URL.String()).Msg("http roundtrip error")
		return nil, err
	}
	if resp != nil {
		if d, err := httputil.DumpResponse(resp, true); err == nil {
			dump := redactHTTPDump(d, t.MaxBody)
			log.Debug().
				Int("status", resp.StatusCode).
				RawJSON("response_dump", dump).
				Msg("http response")
		}
	}
	return resp, nil
}

func NewHTTPClient(cfg Config) *http.Client {
	// Apply defaults for connection pooling
	maxIdleConns := cfg.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = defaultMaxIdleConns
	}
	maxIdleConnsPerHost := cfg.MaxIdleConnsPerHost
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}
	idleConnTimeout := cfg.IdleConnTimeout
	if idleConnTimeout <= 0 {
		idleConnTimeout = defaultIdleConnTimeout
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		MinVersion:         cfg.TLSMinVersion,
	}
	// When insecure mode is on, use a custom verifier that accepts any cert so we bypass
	// strict x509 "not standards compliant" checks that some Prism certs trigger.
	if cfg.InsecureSkipVerify {
		tlsCfg.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error { return nil }
	}
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsCfg,
		// Production-ready connection pooling
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		MaxConnsPerHost:     cfg.MaxConnsPerHost, // 0 = unlimited
		IdleConnTimeout:     idleConnTimeout,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true, // Enable HTTP/2 for better performance
	}
	rt := http.RoundTripper(tr)
	if cfg.LogHTTP || os.Getenv("LOG_HTTP") == "1" {
		rt = &LoggingTransport{Base: tr, MaxBody: 64 * 1024}
	}
	return &http.Client{
		Timeout:   cfg.RequestTimeout, // Use request timeout, not overall timeout
		Transport: rt,
	}
}

// ==================== File System Interface ====================

type FS interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(path string, data []byte, perm os.FileMode) error
	ReadFile(path string) ([]byte, error)
	ReadDir(path string) ([]os.DirEntry, error)
	Create(path string) (*os.File, error)
}

type OSFS struct{}

func (OSFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }
func (OSFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}
func (OSFS) ReadFile(path string) ([]byte, error)       { return os.ReadFile(path) }
func (OSFS) ReadDir(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }
func (OSFS) Create(path string) (*os.File, error)       { return os.Create(path) }

// ==================== Prometheus Metrics ====================
// sanitizeLabel ensures Prometheus label values are safe-ish (no newlines, quotes escaped).
func sanitizeLabel(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

func writePrometheusFile(fs FS, promDir, cluster string, blocks []ParsedBlock) error {
	if err := fs.MkdirAll(promDir, 0755); err != nil {
		return err
	}
	filename := filepath.Join(promDir, fmt.Sprintf("%s.prom", cluster))

	var b strings.Builder

	// Metric headers.
	b.WriteString(`# HELP nutanix_ncc_check_result Result of an NCC check (1 = present)` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_result gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_summary_total Number of NCC checks per severity` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_summary_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_total Total NCC checks for this cluster` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_total gauge` + "\n")

	// Per-check result metrics.
	counts := map[string]int{
		"FAIL": 0,
		"WARN": 0,
		"ERR":  0,
		"INFO": 0,
		"PASS": 0, // in case parser ever maps PASS
	}

	for _, pb := range blocks {
		sev := pb.Severity
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; !ok {
			counts[sev] = 0
		}
		counts[sev]++

		// one sample per check
		b.WriteString(fmt.Sprintf(
			`nutanix_ncc_check_result{cluster="%s",check="%s",severity="%s"} 1`+"\n",
			sanitizeLabel(cluster),
			sanitizeLabel(pb.CheckName),
			sanitizeLabel(sev),
		))
	}

	// Summary per severity.
	for sev, c := range counts {
		if c == 0 {
			continue
		}
		b.WriteString(fmt.Sprintf(
			`nutanix_ncc_check_summary_total{cluster="%s",severity="%s"} %d`+"\n",
			sanitizeLabel(cluster),
			sanitizeLabel(sev),
			c,
		))
	}

	// Total checks.
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_total{cluster="%s"} %d`+"\n",
		sanitizeLabel(cluster),
		len(blocks),
	))

	return fs.WriteFile(filename, []byte(b.String()), 0644)
}

// ==================== API Types ====================

type TaskStatus struct {
	PercentageComplete int    `json:"percentage_complete"`
	ProgressStatus     string `json:"progress_status"`
}

type NCCSummary struct {
	RunSummary string `json:"runSummary"`
}

// ==================== Parser ====================

var (
	reBlockStart = regexp.MustCompile(`^Detailed information for .*`)
	reBlockEnd   = regexp.MustCompile(`^Refer to.*`)
	reSeverity   = regexp.MustCompile(`\b(FAIL|WARN|INFO|ERR)\s*:`)
	rePluginSev  = regexp.MustCompile(`\[\s*(FAIL|WARN|ERR|INFO)\s*\]`)
)

type Row struct {
	Severity  string
	CheckName string
	Detail    template.HTML
}

type ParsedBlock struct {
	Severity  string
	CheckName string
	DetailRaw string
}

func splitLines(s string) []string {
	sc := bufio.NewScanner(strings.NewReader(s))
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lines := []string{}
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(s) > 0 && strings.HasSuffix(s, "\n") {
		lines = append(lines, "")
	}
	return lines
}

func detectSeverity(s string) string {
	loc := reSeverity.FindStringSubmatch(s)
	if len(loc) > 1 {
		return loc[1]
	}
	switch {
	case strings.Contains(s, "FAIL:"):
		return "FAIL"
	case strings.Contains(s, "WARN:"):
		return "WARN"
	case strings.Contains(s, "ERR:"):
		return "ERR"
	case strings.Contains(s, "INFO:"):
		return "INFO"
	default:
		return "INFO"
	}
}

func ParseSummary(text string) ([]ParsedBlock, error) {
	lines := splitLines(text)
	var blocks []ParsedBlock
	for i := 0; i < len(lines); i++ {
		if reBlockStart.MatchString(lines[i]) {
			checkName := lines[i]
			i++
			var buf []string
			for i < len(lines) && !reBlockEnd.MatchString(lines[i]) {
				buf = append(buf, lines[i])
				i++
			}
			if i < len(lines) {
				buf = append(buf, lines[i])
			}
			joined := strings.Join(buf, "\n")
			blocks = append(blocks, ParsedBlock{
				Severity:  detectSeverity(joined),
				CheckName: checkName,
				DetailRaw: joined,
			})
		}
	}
	return blocks, nil
}

func countParsedSeverities(blocks []ParsedBlock) map[string]int {
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range blocks {
		sev := strings.ToUpper(strings.TrimSpace(b.Severity))
		if _, ok := counts[sev]; !ok {
			continue
		}
		counts[sev]++
	}
	return counts
}

func parsePluginResultsSeverities(raw string) map[string]int {
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, ln := range splitLines(raw) {
		m := rePluginSev.FindStringSubmatch(strings.ToUpper(ln))
		if len(m) != 2 {
			continue
		}
		sev := m[1]
		if _, ok := counts[sev]; ok {
			counts[sev]++
		}
	}
	return counts
}

// validateParsedAlertsAgainstPluginResults ensures parser output remains
// aligned with NCC plugin-result severities in raw logs.
func validateParsedAlertsAgainstPluginResults(raw string, blocks []ParsedBlock) error {
	plugin := parsePluginResultsSeverities(raw)
	totalPlugin := plugin["FAIL"] + plugin["WARN"] + plugin["ERR"] + plugin["INFO"]
	if totalPlugin == 0 {
		return nil
	}
	parsed := countParsedSeverities(blocks)
	if parsed["FAIL"] != plugin["FAIL"] ||
		parsed["WARN"] != plugin["WARN"] ||
		parsed["ERR"] != plugin["ERR"] ||
		parsed["INFO"] != plugin["INFO"] {
		return fmt.Errorf("parsed alerts mismatch plugin results: parsed={FAIL:%d WARN:%d ERR:%d INFO:%d} plugin_results={FAIL:%d WARN:%d ERR:%d INFO:%d}",
			parsed["FAIL"], parsed["WARN"], parsed["ERR"], parsed["INFO"],
			plugin["FAIL"], plugin["WARN"], plugin["ERR"], plugin["INFO"])
	}
	return nil
}

func parseNCCHeader(path string) (HTMLMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return HTMLMeta{}, err
	}

	var meta HTMLMeta
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		switch {
		case strings.HasPrefix(line, "Cluster Name:"):
			meta.ClusterName = strings.TrimSpace(strings.TrimPrefix(line, "Cluster Name:"))
		case strings.HasPrefix(line, "Cluster Version:"):
			meta.ClusterVersion = strings.TrimSpace(strings.TrimPrefix(line, "Cluster Version:"))
		case strings.HasPrefix(line, "NCC Version:"):
			meta.NCCVersion = strings.TrimSpace(strings.TrimPrefix(line, "NCC Version:"))
		}
	}
	if err := scanner.Err(); err != nil {
		return HTMLMeta{}, err
	}
	return meta, nil
}

// ==================== Email Notifications ====================

func sendEmail(cfg Config, subj string, body string, attachPath string) error {
	if !cfg.EmailEnabled || cfg.SMTPServer == "" || len(cfg.EmailTo) == 0 {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", cfg.SMTPServer, cfg.SMTPPort)
	auth := smtp.PlainAuth("", cfg.SMTPUser, cfg.SMTPPassword, cfg.SMTPServer)

	var msg bytes.Buffer
	attachHTML := cfg.EmailAttachHTML && attachPath != ""
	if attachHTML {
		attachBody, err := os.ReadFile(attachPath)
		if err != nil {
			return fmt.Errorf("read attachment %s: %w", attachPath, err)
		}
		boundary := "ncc-report-boundary"
		msg.WriteString(fmt.Sprintf("From: %s\r\n", cfg.EmailFrom))
		msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(cfg.EmailTo, ",")))
		msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subj))
		msg.WriteString("MIME-Version: 1.0\r\n")
		msg.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=%s\r\n", boundary))
		msg.WriteString("\r\n")
		msg.WriteString(fmt.Sprintf("--%s\r\n", boundary))
		msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		msg.WriteString("\r\n")
		msg.WriteString(body)
		msg.WriteString("\r\n")
		msg.WriteString(fmt.Sprintf("--%s\r\n", boundary))
		msg.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
		msg.WriteString("Content-Disposition: attachment; filename=\"" + filepath.Base(attachPath) + "\"\r\n")
		msg.WriteString("\r\n")
		msg.Write(attachBody)
		msg.WriteString("\r\n")
		msg.WriteString(fmt.Sprintf("--%s--\r\n", boundary))
	} else {
		msg.WriteString(fmt.Sprintf("From: %s\r\n", cfg.EmailFrom))
		msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(cfg.EmailTo, ",")))
		msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subj))
		msg.WriteString("MIME-Version: 1.0\r\n")
		msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		msg.WriteString("\r\n")
		msg.WriteString(body)
	}

	if cfg.EmailUseTLS {
		// STARTTLS-style connection:
		c, err := smtp.Dial(addr)
		if err != nil {
			return err
		}
		defer c.Close()

		if err := c.StartTLS(&tls.Config{ServerName: cfg.SMTPServer, InsecureSkipVerify: cfg.InsecureSkipVerify}); err != nil {
			return err
		}
		if err := c.Auth(auth); err != nil {
			return err
		}
		if err := c.Mail(cfg.EmailFrom); err != nil {
			return err
		}
		for _, rcpt := range cfg.EmailTo {
			if err := c.Rcpt(rcpt); err != nil {
				return err
			}
		}
		w, err := c.Data()
		if err != nil {
			return err
		}
		if _, err := w.Write(msg.Bytes()); err != nil {
			return err
		}
		return w.Close()
	}

	return smtp.SendMail(addr, auth, cfg.EmailFrom, cfg.EmailTo, msg.Bytes())
}

const notificationRetryAttempts = 3

func sendEmailWithRetry(cfg Config, subj string, body string, attachPath string) error {
	var lastErr error
	for attempt := 1; attempt <= notificationRetryAttempts; attempt++ {
		lastErr = sendEmail(cfg, subj, body, attachPath)
		if lastErr == nil {
			return nil
		}
		if attempt < notificationRetryAttempts {
			backoff := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
			time.Sleep(backoff)
		}
	}
	return lastErr
}

// ==================== Webhook Notifications ====================

func sendWebhook(ctx context.Context, client HTTPClient, cfg Config, summary NotificationSummary) error {
	if !cfg.WebhookEnabled || cfg.WebhookURL == "" {
		return nil
	}

	payload, err := json.Marshal(summary)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.WebhookHeaders {
		req.Header.Set(k, v)
	}

	// simple retry loop using existing helpers
	for attempt := 1; attempt <= cfg.RetryMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("webhook request failed: %w", err)
			}
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if !isRetryableStatus(resp.StatusCode) || attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("webhook status %d", resp.StatusCode)
			}
			if d, ok := retryAfterDelay(resp); ok {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(d):
					continue
				}
			}
		}
		backoff := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("webhook exhausted retries")
}

func sendWebhookWithRetry(ctx context.Context, client HTTPClient, cfg Config, summary NotificationSummary) error {
	var lastErr error
	for attempt := 1; attempt <= notificationRetryAttempts; attempt++ {
		lastErr = sendWebhook(ctx, client, cfg, summary)
		if lastErr == nil {
			return nil
		}
		if attempt < notificationRetryAttempts {
			backoff := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}

// ==================== Slack Notifications ====================

func sendSlack(ctx context.Context, client HTTPClient, cfg Config, summary NotificationSummary) error {
	if !cfg.SlackEnabled || cfg.SlackWebhookURL == "" {
		return nil
	}

	// Determine color based on severity
	color := "#36a64f" // green
	if summary.FailCount > 0 {
		color = "#ff0000" // red
	} else if summary.WarnCount > 0 {
		color = "#ffaa00" // orange
	}

	// Build Slack message
	attachment := map[string]interface{}{
		"color": color,
		"title": fmt.Sprintf("NCC Report: %s", summary.Cluster),
		"fields": []map[string]string{
			{"title": "FAIL", "value": fmt.Sprintf("%d", summary.FailCount), "short": "true"},
			{"title": "WARN", "value": fmt.Sprintf("%d", summary.WarnCount), "short": "true"},
			{"title": "ERR", "value": fmt.Sprintf("%d", summary.ErrCount), "short": "true"},
			{"title": "INFO", "value": fmt.Sprintf("%d", summary.InfoCount), "short": "true"},
			{"title": "Total Checks", "value": fmt.Sprintf("%d", summary.TotalChecks), "short": "false"},
		},
		"footer": "NCC Orchestrator",
		"ts":     summary.FinishedAt.Unix(),
	}

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{attachment},
	}

	if cfg.SlackChannel != "" {
		payload["channel"] = cfg.SlackChannel
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.SlackWebhookURL, bytes.NewReader(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Simple retry loop
	for attempt := 1; attempt <= cfg.RetryMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("slack request failed: %w", err)
			}
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if !isRetryableStatus(resp.StatusCode) || attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("slack status %d", resp.StatusCode)
			}
		}
		backoff := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("slack exhausted retries")
}

func sendSlackWithRetry(ctx context.Context, client HTTPClient, cfg Config, summary NotificationSummary) error {
	var lastErr error
	for attempt := 1; attempt <= notificationRetryAttempts; attempt++ {
		lastErr = sendSlack(ctx, client, cfg, summary)
		if lastErr == nil {
			return nil
		}
		if attempt < notificationRetryAttempts {
			backoff := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}

// ==================== Report Renderers ====================

// func generateHTMLNoMeta(fs FS, rows []Row, filename string) error {
// 	return generateHTML(fs, rows, filename, HTMLMeta{})
// }

// htmlNowForReport supplies the "Generated" timestamp for per-cluster HTML reports.
// Tests may replace it with a fixed clock for golden-file stability.
var htmlNowForReport = func() time.Time { return time.Now() }

func generateHTML(fs FS, rows []Row, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC Report - {{.Meta.ClusterName}}</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjI4IiBoZWlnaHQ9IjI4IiByeD0iOCIgZmlsbD0iIzBCMTIyMCIvPgo8cGF0aCBkPSJNOSAyM1Y5SDEyTDIwIDE5VjlIMjNWMjNIMjBMMTIgMTNWMjNIOVoiIGZpbGw9IiMyMkM1NUUiLz4KPGNpcmNsZSBjeD0iMjQiIGN5PSI4IiByPSIyIiBmaWxsPSIjMzhCREY4Ii8+CjxjaXJjbGUgY3g9IjgiIGN5PSIyNCIgcj0iMiIgZmlsbD0iI0Y1OUUwQiIvPgo8L3N2Zz4=">
  <style>
    :root {
      --fail: #ef4444;
      --warn: #f59e0b;
      --info: #3b82f6;
      --err:  #374151;
      --border: #d1d5db;
      --thead: #f3f4f6;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: #111827;
      background: #f9fafb;
    }
    .page { max-width: 1100px; margin: 24px auto; padding: 0 16px 24px; }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
      margin-bottom: 16px;
      flex-wrap: wrap;
    }
    h1 { margin: 0; font-size: 22px; }
    .subtitle { margin-top: 4px; font-size: 13px; color: #6b7280; }
    .summary-line { font-size: 13px; color: #6b7280; margin-top: 6px; }
    .summary-line strong { color: #111827; }
    .tags { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
    .tag {
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 11px;
      display: inline-flex;
      gap: 4px;
    }
    .tag .label { color: #6b7280; }
    .tag .value { font-weight: 600; color: #111827; }
    table { border-collapse: collapse; width: 100%; border: 1px solid var(--border); }
    thead th {
      position: sticky; top: 0; background: var(--thead);
      border-bottom: 1px solid var(--border);
      padding: 10px; text-align: left; font-size: 13px;
    }
    tbody td { border-bottom: 1px solid var(--border); padding: 10px; vertical-align: top; }
    tbody tr:nth-child(odd) { background: #fafafa; }
    .sev { display: inline-block; padding: 2px 8px; border-radius: 999px; font-weight: 600; font-size: 12px; }
    .sev.FAIL { color: #fff; background: var(--fail); }
    .sev.WARN { color: #111827; background: #fde68a; }
    .sev.INFO { color: #fff; background: var(--info); }
    .sev.ERR  { color: #111827; background: #e5e7eb; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; white-space: pre-wrap; word-break: break-word; }
  </style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div>
        <h1>NCC Report</h1>
        <div class="subtitle">{{.Meta.ClusterName}}</div>
        <div class="summary-line">
          {{.Summary.FAIL}} FAIL, {{.Summary.WARN}} WARN, {{.Summary.ERR}} ERR, {{.Summary.INFO}} INFO
        </div>
      </div>
      <div class="tags">
        <div class="tag"><span class="label">Cluster</span><span class="value">{{.Meta.ClusterName}}</span></div>
        <div class="tag"><span class="label">Cluster Version</span><span class="value">{{.Meta.ClusterVersion}}</span></div>
        <div class="tag"><span class="label">NCC Version</span><span class="value">{{.Meta.NCCVersion}}</span></div>
        <div class="tag"><span class="label">Generated</span><span class="value">{{.Now}}</span></div>
      </div>
    </header>
    <main>
      <table>
        <thead>
          <tr>
            <th style="width:110px">Severity</th>
            <th style="width:320px">NCC Check Name</th>
            <th>Detail Information</th>
          </tr>
        </thead>
        <tbody>
        {{range .Rows}}
          <tr>
            <td><span class="sev {{.Severity}}">{{.Severity}}</span></td>
            <td class="mono">{{.CheckName}}</td>
            <td class="mono">{{.Detail}}</td>
          </tr>
        {{end}}
        </tbody>
      </table>
    </main>
  </div>
</body>
</html>`
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	sum := SummaryCounts{}
	for _, r := range rows {
		switch r.Severity {
		case "FAIL":
			sum.FAIL++
		case "WARN":
			sum.WARN++
		case "ERR":
			sum.ERR++
		default:
			sum.INFO++
		}
	}
	data := HTMLData{
		Rows:    rows,
		Now:     htmlNowForReport().Format(time.RFC3339),
		Meta:    meta,
		Summary: sum,
	}
	t := template.Must(template.New("table").Parse(tmpl))
	return t.Execute(f, data)
}

func generateCSV(fs FS, blocks []ParsedBlock, filename string) error {
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"Severity", "CheckName", "Detail"}); err != nil {
		return err
	}
	for _, b := range blocks {
		if err := w.Write([]string{b.Severity, b.CheckName, b.DetailRaw}); err != nil {
			return err
		}
	}
	return w.Error()
}

func generateMarkdown(fs FS, blocks []ParsedBlock, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# NCC Report\n\n")
	if meta.ClusterName != "" || meta.ClusterVersion != "" || meta.NCCVersion != "" {
		b.WriteString("| Field | Value |\n|-------|-------|\n")
		if meta.ClusterName != "" {
			b.WriteString(fmt.Sprintf("| Cluster | %s |\n", escapeMarkdown(meta.ClusterName)))
		}
		if meta.ClusterVersion != "" {
			b.WriteString(fmt.Sprintf("| Version | %s |\n", escapeMarkdown(meta.ClusterVersion)))
		}
		if meta.NCCVersion != "" {
			b.WriteString(fmt.Sprintf("| NCC Version | %s |\n", escapeMarkdown(meta.NCCVersion)))
		}
		b.WriteString("\n")
	}
	b.WriteString("| Severity | Check | Detail |\n|----------|-------|--------|\n")
	for _, block := range blocks {
		sev := block.Severity
		if sev == "" {
			sev = "INFO"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n",
			escapeMarkdown(sev),
			escapeMarkdown(block.CheckName),
			escapeMarkdown(strings.ReplaceAll(block.DetailRaw, "\n", " "))))
	}
	return fs.WriteFile(filename, []byte(b.String()), 0644)
}

func escapeMarkdown(s string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ").Replace(s)
}

type JSONOutput struct {
	GeneratedAt string      `json:"generated_at"`
	Checks      []JSONCheck `json:"checks"`
	Summary     JSONSummary `json:"summary"`
}

type JSONCheck struct {
	Severity  string `json:"severity"`
	CheckName string `json:"check_name"`
	Detail    string `json:"detail"`
}

type JSONSummary struct {
	Total int            `json:"total"`
	Count map[string]int `json:"count"`
}

// RunSummaryJSON is the machine-readable run result written to run-summary.json.
type RunSummaryJSON struct {
	Timestamp      string              `json:"timestamp"`
	DurationS      float64             `json:"duration_s"`
	ClustersOK     int                 `json:"clusters_ok"`
	ClustersFailed int                 `json:"clusters_failed"`
	FailedClusters []string            `json:"failed_clusters,omitempty"`
	Clusters       []RunClusterSummary `json:"clusters,omitempty"`
	ExitCode       int                 `json:"exit_code,omitempty"`
	IndexHTML      string              `json:"index_html"`
	TotalChecks    int                 `json:"total_checks,omitempty"`
	AvgHealthScore int                 `json:"avg_health_score,omitempty"`
	MinHealthScore int                 `json:"min_health_score,omitempty"`
	FailureClasses map[string]int      `json:"failure_classes,omitempty"`
}

// RunClusterSummary is per-cluster stats for automation (run-summary.json).
type RunClusterSummary struct {
	Address     string `json:"address"`
	OK          bool   `json:"ok"`
	Error       string `json:"error,omitempty"`
	ErrorClass  string `json:"error_class,omitempty"`
	FailCount   int    `json:"fail_count,omitempty"`
	WarnCount   int    `json:"warn_count,omitempty"`
	ErrCount    int    `json:"err_count,omitempty"`
	InfoCount   int    `json:"info_count,omitempty"`
	ChecksTotal int    `json:"checks_total,omitempty"`
	HealthScore int    `json:"health_score,omitempty"`
}

type CheckSnapshotEntry struct {
	CheckName string `json:"check_name"`
	Severity  string `json:"severity"`
}

type ClusterChecksSnapshot struct {
	Address     string               `json:"address"`
	Checks      []CheckSnapshotEntry `json:"checks,omitempty"`
	FailCount   int                  `json:"fail_count,omitempty"`
	WarnCount   int                  `json:"warn_count,omitempty"`
	ErrCount    int                  `json:"err_count,omitempty"`
	InfoCount   int                  `json:"info_count,omitempty"`
	ChecksTotal int                  `json:"checks_total,omitempty"`
	HealthScore int                  `json:"health_score,omitempty"`
}

type ChecksSnapshotJSON struct {
	Timestamp string                  `json:"timestamp"`
	Clusters  []ClusterChecksSnapshot `json:"clusters,omitempty"`
}

type SeverityChange struct {
	CheckName string `json:"check_name"`
	From      string `json:"from"`
	To        string `json:"to"`
}

type ClusterDiffSummary struct {
	Address          string           `json:"address"`
	NewFailures      []string         `json:"new_failures,omitempty"`
	ResolvedFailures []string         `json:"resolved_failures,omitempty"`
	NewChecks        []string         `json:"new_checks,omitempty"`
	RemovedChecks    []string         `json:"removed_checks,omitempty"`
	SeverityChanges  []SeverityChange `json:"severity_changes,omitempty"`
}

type DrillDownDiffJSON struct {
	Timestamp         string               `json:"timestamp"`
	PreviousTimestamp string               `json:"previous_timestamp,omitempty"`
	NewFailCount      int                  `json:"new_fail_count"`
	ResolvedFailCount int                  `json:"resolved_fail_count"`
	Clusters          []ClusterDiffSummary `json:"clusters,omitempty"`
}

type FlakyCheckSummary struct {
	Cluster      string   `json:"cluster"`
	CheckName    string   `json:"check_name"`
	Transitions  int      `json:"transitions"`
	Observations int      `json:"observations"`
	States       []string `json:"states,omitempty"`
	Current      string   `json:"current"`
}

type FlakyChecksJSON struct {
	Timestamp        string              `json:"timestamp"`
	LookbackRuns     int                 `json:"lookback_runs"`
	MinTransitions   int                 `json:"min_transitions"`
	TotalFlakyChecks int                 `json:"total_flaky_checks"`
	Checks           []FlakyCheckSummary `json:"checks,omitempty"`
}

type SLOClusterExport struct {
	Address         string  `json:"address"`
	ChecksTotal     int     `json:"checks_total"`
	FailCount       int     `json:"fail_count"`
	WarnCount       int     `json:"warn_count"`
	ErrCount        int     `json:"err_count"`
	InfoCount       int     `json:"info_count"`
	FailRatePercent float64 `json:"fail_rate_percent"`
	HealthScore     int     `json:"health_score"`
	Status          string  `json:"status"`
}

type SLODashboardJSON struct {
	Timestamp string             `json:"timestamp"`
	DurationS float64            `json:"duration_s"`
	Clusters  []SLOClusterExport `json:"clusters,omitempty"`
}

func writeRunSummaryJSON(fs FS, outDir string, summary RunSummaryJSON) error {
	path := filepath.Join(outDir, "run-summary.json")
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// NCCRunRecord is a versioned machine-readable bundle (ncc-run-record.json) for automation pipelines.
type NCCRunRecord struct {
	SchemaVersion       string         `json:"schema_version"`
	OrchestratorVersion string         `json:"orchestrator_version"`
	GitRevision         string         `json:"git_revision,omitempty"`
	Hostname            string         `json:"hostname,omitempty"`
	SchedulerSource     string         `json:"scheduler_source,omitempty"`
	Stream              string         `json:"stream,omitempty"`
	Run                 RunSummaryJSON `json:"run"`
}

func writeNCCRunRecordJSON(fs FS, outDir string, summary RunSummaryJSON) error {
	path := filepath.Join(outDir, "ncc-run-record.json")
	hostname, _ := os.Hostname()
	schedulerSource := strings.TrimSpace(os.Getenv("NCC_SCHEDULER_SOURCE"))
	if schedulerSource == "" {
		schedulerSource = "manual"
	}
	gitRevision := ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				gitRevision = s.Value
				break
			}
		}
	}
	rec := NCCRunRecord{
		SchemaVersion:       "1.0",
		OrchestratorVersion: Version,
		GitRevision:         gitRevision,
		Hostname:            hostname,
		SchedulerSource:     schedulerSource,
		Stream:              Stream,
		Run:                 summary,
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// RegressionSummary captures change in FAIL counts vs the previous run-summary.json.
type RegressionSummary struct {
	Timestamp         string         `json:"timestamp"`
	PreviousTimestamp string         `json:"previous_timestamp,omitempty"`
	Current           RunSummaryJSON `json:"current"`
	PreviousFailTotal int            `json:"previous_fail_total"`
	CurrentFailTotal  int            `json:"current_fail_total"`
	DeltaFailTotal    int            `json:"delta_fail_total"`
	HasRegression     bool           `json:"has_regression"`
	IncreasedClusters []string       `json:"increased_clusters,omitempty"`
	DecreasedClusters []string       `json:"decreased_clusters,omitempty"`
	UnchangedClusters []string       `json:"unchanged_clusters,omitempty"`
}

func failCountByCluster(summary RunSummaryJSON) map[string]int {
	out := make(map[string]int, len(summary.Clusters))
	for _, c := range summary.Clusters {
		out[strings.TrimSpace(c.Address)] = c.FailCount
	}
	return out
}

func loadRunSummaryJSON(path string) (RunSummaryJSON, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RunSummaryJSON{}, false, nil
		}
		return RunSummaryJSON{}, false, err
	}
	var s RunSummaryJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return RunSummaryJSON{}, false, err
	}
	return s, true, nil
}

func computeRegressionSummary(previous RunSummaryJSON, hasPrevious bool, current RunSummaryJSON) RegressionSummary {
	reg := RegressionSummary{
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		Current:           current,
		CurrentFailTotal:  0,
		PreviousFailTotal: 0,
	}
	if hasPrevious {
		reg.PreviousTimestamp = previous.Timestamp
	}

	prevMap := failCountByCluster(previous)
	currMap := failCountByCluster(current)
	seen := make(map[string]bool, len(prevMap)+len(currMap))

	for k := range prevMap {
		seen[k] = true
	}
	for k := range currMap {
		seen[k] = true
	}

	for cluster := range seen {
		prev := prevMap[cluster]
		curr := currMap[cluster]
		reg.PreviousFailTotal += prev
		reg.CurrentFailTotal += curr
		switch {
		case curr > prev:
			reg.IncreasedClusters = append(reg.IncreasedClusters, cluster)
		case curr < prev:
			reg.DecreasedClusters = append(reg.DecreasedClusters, cluster)
		default:
			reg.UnchangedClusters = append(reg.UnchangedClusters, cluster)
		}
	}
	sort.Strings(reg.IncreasedClusters)
	sort.Strings(reg.DecreasedClusters)
	sort.Strings(reg.UnchangedClusters)
	reg.DeltaFailTotal = reg.CurrentFailTotal - reg.PreviousFailTotal
	reg.HasRegression = len(reg.IncreasedClusters) > 0
	return reg
}

func writeRegressionSummaryJSON(fs FS, outDir string, reg RegressionSummary) error {
	path := filepath.Join(outDir, "regression-summary.json")
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeChecksSnapshotJSON(fs FS, outDir string, snap ChecksSnapshotJSON) error {
	path := filepath.Join(outDir, "checks-snapshot.json")
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func loadChecksSnapshotJSON(path string) (ChecksSnapshotJSON, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ChecksSnapshotJSON{}, false, nil
		}
		return ChecksSnapshotJSON{}, false, err
	}
	var s ChecksSnapshotJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return ChecksSnapshotJSON{}, false, err
	}
	return s, true, nil
}

func writeDrillDownDiffJSON(fs FS, outDir string, diff DrillDownDiffJSON) error {
	path := filepath.Join(outDir, "drilldown-diff.json")
	data, err := json.MarshalIndent(diff, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeFlakyChecksJSON(fs FS, outDir string, flaky FlakyChecksJSON) error {
	path := filepath.Join(outDir, "flaky-checks.json")
	data, err := json.MarshalIndent(flaky, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeSLODashboardJSON(fs FS, outDir string, slo SLODashboardJSON) error {
	path := filepath.Join(outDir, "slo-dashboard.json")
	data, err := json.MarshalIndent(slo, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

type ExcludedAlertsClusterAudit struct {
	Cluster       string          `json:"cluster"`
	ExcludedCount int             `json:"excluded_count"`
	Alerts        []ExcludedAlert `json:"alerts,omitempty"`
}

type ExcludedAlertsAudit struct {
	SchemaVersion      string                       `json:"schema_version"`
	Timestamp          string                       `json:"timestamp"`
	MatchMode          string                       `json:"match_mode"`
	ExcludeAlertTitles []string                     `json:"exclude_alert_titles"`
	TotalExcluded      int                          `json:"total_excluded"`
	Clusters           []ExcludedAlertsClusterAudit `json:"clusters"`
}

func writeExcludedAlertsAuditJSON(fs FS, outDir string, matchMode string, titles []string, perCluster map[string][]ExcludedAlert) error {
	path := filepath.Join(outDir, "excluded-alerts.json")
	clusters := make([]ExcludedAlertsClusterAudit, 0, len(perCluster))
	total := 0
	for cluster, alerts := range perCluster {
		cp := make([]ExcludedAlert, len(alerts))
		copy(cp, alerts)
		clusters = append(clusters, ExcludedAlertsClusterAudit{
			Cluster:       cluster,
			ExcludedCount: len(cp),
			Alerts:        cp,
		})
		total += len(cp)
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Cluster < clusters[j].Cluster })
	payload := ExcludedAlertsAudit{
		SchemaVersion:      "1.0",
		Timestamp:          time.Now().UTC().Format(time.RFC3339),
		MatchMode:          matchMode,
		ExcludeAlertTitles: append([]string{}, titles...),
		TotalExcluded:      total,
		Clusters:           clusters,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func clusterHealthScore(failCount, warnCount, errCount, total int) int {
	if total <= 0 {
		return 100
	}
	penalty := (float64(failCount)*1.0 + float64(errCount)*0.8 + float64(warnCount)*0.3) / float64(total) * 100.0
	score := int(math.Round(100.0 - penalty))
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func buildChecksSnapshot(results []ClusterResult) ChecksSnapshotJSON {
	snap := ChecksSnapshotJSON{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Clusters:  make([]ClusterChecksSnapshot, 0, len(results)),
	}
	for _, r := range results {
		cluster := ClusterChecksSnapshot{Address: r.Cluster}
		if r.Err != nil {
			snap.Clusters = append(snap.Clusters, cluster)
			continue
		}
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		cluster.Checks = make([]CheckSnapshotEntry, 0, len(r.Blocks))
		for _, b := range r.Blocks {
			sev := strings.ToUpper(strings.TrimSpace(b.Severity))
			if sev == "" {
				sev = "INFO"
			}
			if _, ok := counts[sev]; !ok {
				sev = "INFO"
			}
			counts[sev]++
			cluster.Checks = append(cluster.Checks, CheckSnapshotEntry{
				CheckName: strings.TrimSpace(b.CheckName),
				Severity:  sev,
			})
		}
		sort.Slice(cluster.Checks, func(i, j int) bool {
			if cluster.Checks[i].CheckName == cluster.Checks[j].CheckName {
				return cluster.Checks[i].Severity < cluster.Checks[j].Severity
			}
			return cluster.Checks[i].CheckName < cluster.Checks[j].CheckName
		})
		cluster.FailCount = counts["FAIL"]
		cluster.WarnCount = counts["WARN"]
		cluster.ErrCount = counts["ERR"]
		cluster.InfoCount = counts["INFO"]
		cluster.ChecksTotal = len(r.Blocks)
		cluster.HealthScore = clusterHealthScore(cluster.FailCount, cluster.WarnCount, cluster.ErrCount, cluster.ChecksTotal)
		snap.Clusters = append(snap.Clusters, cluster)
	}
	sort.Slice(snap.Clusters, func(i, j int) bool { return snap.Clusters[i].Address < snap.Clusters[j].Address })
	return snap
}

func checksMapForCluster(c ClusterChecksSnapshot) map[string]string {
	out := make(map[string]string, len(c.Checks))
	for _, ch := range c.Checks {
		name := strings.TrimSpace(ch.CheckName)
		if name == "" {
			continue
		}
		out[name] = strings.ToUpper(strings.TrimSpace(ch.Severity))
	}
	return out
}

func computeDrillDownDiff(previous ChecksSnapshotJSON, hasPrevious bool, current ChecksSnapshotJSON) DrillDownDiffJSON {
	diff := DrillDownDiffJSON{
		Timestamp: current.Timestamp,
	}
	if !hasPrevious {
		return diff
	}
	if hasPrevious {
		diff.PreviousTimestamp = previous.Timestamp
	}
	prevClusters := map[string]ClusterChecksSnapshot{}
	currClusters := map[string]ClusterChecksSnapshot{}
	for _, c := range previous.Clusters {
		prevClusters[c.Address] = c
	}
	for _, c := range current.Clusters {
		currClusters[c.Address] = c
	}
	allClusters := map[string]bool{}
	for k := range prevClusters {
		allClusters[k] = true
	}
	for k := range currClusters {
		allClusters[k] = true
	}
	for addr := range allClusters {
		prevMap := checksMapForCluster(prevClusters[addr])
		currMap := checksMapForCluster(currClusters[addr])
		cd := ClusterDiffSummary{Address: addr}
		for name, currSev := range currMap {
			prevSev, hadPrev := prevMap[name]
			if !hadPrev {
				cd.NewChecks = append(cd.NewChecks, name)
				if currSev == "FAIL" {
					cd.NewFailures = append(cd.NewFailures, name)
					diff.NewFailCount++
				}
				continue
			}
			if currSev != prevSev {
				cd.SeverityChanges = append(cd.SeverityChanges, SeverityChange{
					CheckName: name, From: prevSev, To: currSev,
				})
			}
			if prevSev != "FAIL" && currSev == "FAIL" {
				cd.NewFailures = append(cd.NewFailures, name)
				diff.NewFailCount++
			}
			if prevSev == "FAIL" && currSev != "FAIL" {
				cd.ResolvedFailures = append(cd.ResolvedFailures, name)
				diff.ResolvedFailCount++
			}
		}
		for name := range prevMap {
			if _, ok := currMap[name]; !ok {
				cd.RemovedChecks = append(cd.RemovedChecks, name)
				if prevMap[name] == "FAIL" {
					cd.ResolvedFailures = append(cd.ResolvedFailures, name)
					diff.ResolvedFailCount++
				}
			}
		}
		sort.Strings(cd.NewFailures)
		sort.Strings(cd.ResolvedFailures)
		sort.Strings(cd.NewChecks)
		sort.Strings(cd.RemovedChecks)
		sort.Slice(cd.SeverityChanges, func(i, j int) bool { return cd.SeverityChanges[i].CheckName < cd.SeverityChanges[j].CheckName })
		if len(cd.NewFailures) > 0 || len(cd.ResolvedFailures) > 0 || len(cd.NewChecks) > 0 || len(cd.RemovedChecks) > 0 || len(cd.SeverityChanges) > 0 {
			diff.Clusters = append(diff.Clusters, cd)
		}
	}
	sort.Slice(diff.Clusters, func(i, j int) bool { return diff.Clusters[i].Address < diff.Clusters[j].Address })
	return diff
}

func computeFlakyChecks(snapshots []ChecksSnapshotJSON, minTransitions int) FlakyChecksJSON {
	report := FlakyChecksJSON{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		LookbackRuns:   len(snapshots),
		MinTransitions: minTransitions,
		Checks:         []FlakyCheckSummary{},
	}
	if len(snapshots) < 2 {
		return report
	}
	type key struct{ cluster, check string }
	series := map[key][]string{}
	for _, snap := range snapshots {
		for _, c := range snap.Clusters {
			for _, ch := range c.Checks {
				k := key{cluster: c.Address, check: strings.TrimSpace(ch.CheckName)}
				if k.check == "" {
					continue
				}
				series[k] = append(series[k], strings.ToUpper(strings.TrimSpace(ch.Severity)))
			}
		}
	}
	for k, states := range series {
		if len(states) < 2 {
			continue
		}
		transitions := 0
		uniq := map[string]bool{}
		prev := states[0]
		uniq[prev] = true
		for i := 1; i < len(states); i++ {
			cur := states[i]
			uniq[cur] = true
			if cur != prev {
				transitions++
			}
			prev = cur
		}
		if transitions >= minTransitions && len(uniq) > 1 {
			report.Checks = append(report.Checks, FlakyCheckSummary{
				Cluster:      k.cluster,
				CheckName:    k.check,
				Transitions:  transitions,
				Observations: len(states),
				States:       states,
				Current:      states[len(states)-1],
			})
		}
	}
	sort.Slice(report.Checks, func(i, j int) bool {
		if report.Checks[i].Transitions == report.Checks[j].Transitions {
			if report.Checks[i].Cluster == report.Checks[j].Cluster {
				return report.Checks[i].CheckName < report.Checks[j].CheckName
			}
			return report.Checks[i].Cluster < report.Checks[j].Cluster
		}
		return report.Checks[i].Transitions > report.Checks[j].Transitions
	})
	report.TotalFlakyChecks = len(report.Checks)
	return report
}

func loadRecentCheckSnapshots(historyDir string, maxRuns int) ([]ChecksSnapshotJSON, error) {
	if maxRuns <= 0 {
		return nil, nil
	}
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return nil, err
	}
	if len(dirs) > maxRuns {
		dirs = dirs[:maxRuns]
	}
	outNewest := make([]ChecksSnapshotJSON, 0, len(dirs))
	for _, d := range dirs {
		path := filepath.Join(historyDir, d, "checks-snapshot.json")
		s, ok, err := loadChecksSnapshotJSON(path)
		if err != nil {
			log.Warn().Err(err).Str("path", path).Msg("failed to load checks snapshot from history (ignored)")
			continue
		}
		if ok {
			outNewest = append(outNewest, s)
		}
	}
	// Return oldest -> newest
	for i, j := 0, len(outNewest)-1; i < j; i, j = i+1, j-1 {
		outNewest[i], outNewest[j] = outNewest[j], outNewest[i]
	}
	return outNewest, nil
}

func buildSLODashboard(run RunSummaryJSON) SLODashboardJSON {
	out := SLODashboardJSON{
		Timestamp: run.Timestamp,
		DurationS: run.DurationS,
		Clusters:  make([]SLOClusterExport, 0, len(run.Clusters)),
	}
	for _, c := range run.Clusters {
		failRate := 0.0
		if c.ChecksTotal > 0 {
			failRate = float64(c.FailCount) * 100.0 / float64(c.ChecksTotal)
		}
		status := "ok"
		if !c.OK {
			status = "error"
		} else if c.FailCount > 0 {
			status = "degraded"
		} else if c.WarnCount > 0 {
			status = "warn"
		}
		out.Clusters = append(out.Clusters, SLOClusterExport{
			Address:         c.Address,
			ChecksTotal:     c.ChecksTotal,
			FailCount:       c.FailCount,
			WarnCount:       c.WarnCount,
			ErrCount:        c.ErrCount,
			InfoCount:       c.InfoCount,
			FailRatePercent: math.Round(failRate*100) / 100,
			HealthScore:     c.HealthScore,
			Status:          status,
		})
	}
	sort.Slice(out.Clusters, func(i, j int) bool { return out.Clusters[i].Address < out.Clusters[j].Address })
	return out
}

type parsedPolicyGate struct {
	Raw       string
	Metric    string
	Operator  string
	Threshold float64
}

var policyGateRe = regexp.MustCompile(`^\s*([a-zA-Z0-9\-_]+)\s*(>=|<=|==|!=|>|<)\s*([0-9]+(?:\.[0-9]+)?)\s*$`)

func parsePolicyGate(expr string) (parsedPolicyGate, error) {
	expr = strings.TrimSpace(expr)
	m := policyGateRe.FindStringSubmatch(expr)
	if len(m) != 4 {
		return parsedPolicyGate{}, fmt.Errorf("invalid policy gate %q (expected metric<op>number)", expr)
	}
	th, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return parsedPolicyGate{}, err
	}
	return parsedPolicyGate{
		Raw:       expr,
		Metric:    strings.ToLower(strings.TrimSpace(m[1])),
		Operator:  m[2],
		Threshold: th,
	}, nil
}

func compareFloat(lhs float64, op string, rhs float64) bool {
	switch op {
	case ">":
		return lhs > rhs
	case ">=":
		return lhs >= rhs
	case "<":
		return lhs < rhs
	case "<=":
		return lhs <= rhs
	case "==":
		return lhs == rhs
	case "!=":
		return lhs != rhs
	default:
		return false
	}
}

func evaluatePolicyGates(gates []string, metrics map[string]float64) ([]string, error) {
	var violations []string
	for _, g := range gates {
		if strings.TrimSpace(g) == "" {
			continue
		}
		p, err := parsePolicyGate(g)
		if err != nil {
			return nil, err
		}
		val, ok := metrics[p.Metric]
		if !ok {
			return nil, fmt.Errorf("unsupported policy gate metric %q", p.Metric)
		}
		if compareFloat(val, p.Operator, p.Threshold) {
			violations = append(violations, fmt.Sprintf("%s violated (actual=%.2f)", p.Raw, val))
		}
	}
	return violations, nil
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0644)
}

func runHistoryTimestamp(now time.Time) string {
	return now.UTC().Format("20060102T150405Z")
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
		ts, err := time.Parse("20060102T150405Z", e.Name())
		if err != nil {
			continue
		}
		items = append(items, item{name: e.Name(), t: ts})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].t.After(items[j].t) })
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.name)
	}
	return out, nil
}

func applyRunHistoryRetention(historyDir string, retainLast, retainDays int, now time.Time) error {
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return err
	}
	for i, d := range dirs {
		remove := false
		if retainLast > 0 && i >= retainLast {
			remove = true
		}
		if retainDays > 0 {
			ts, err := time.Parse("20060102T150405Z", d)
			if err == nil {
				ageDays := int(now.UTC().Sub(ts).Hours() / 24)
				if ageDays >= retainDays {
					remove = true
				}
			}
		}
		if remove {
			if err := os.RemoveAll(filepath.Join(historyDir, d)); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyArtifactRetentionPolicies(outputDir string, retainDays, retainMaxFiles int, now time.Time) (int, error) {
	if retainDays <= 0 && retainMaxFiles <= 0 {
		return 0, nil
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
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
	deleted := 0
	retainDuration := time.Duration(retainDays) * 24 * time.Hour
	for i, f := range files {
		remove := false
		if retainDays > 0 && now.Sub(f.mod) >= retainDuration {
			remove = true
		}
		if retainMaxFiles > 0 && i >= retainMaxFiles {
			remove = true
		}
		if !remove {
			continue
		}
		if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func writeRunHistorySnapshot(cfg Config) (string, error) {
	ts := runHistoryTimestamp(time.Now())
	runDir := filepath.Join(cfg.RunHistoryDir, ts)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return "", err
	}
	candidates := []string{
		"index.html",
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

func classifyClusterError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "timed out"), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "http 401"), strings.Contains(msg, "http 403"), strings.Contains(msg, "unauthorized"), strings.Contains(msg, "forbidden"), strings.Contains(msg, "authentication"):
		return "auth"
	case strings.Contains(msg, "retry circuit breaker opened"), strings.Contains(msg, "http 429"), strings.Contains(msg, "rate limit"):
		return "rate_limit"
	case strings.Contains(msg, "parse filtered"), strings.Contains(msg, "parse summary"), strings.Contains(msg, "parser"):
		return "parser"
	case strings.Contains(msg, "transport error"), strings.Contains(msg, "connection refused"), strings.Contains(msg, "no such host"), strings.Contains(msg, "dial tcp"), strings.Contains(msg, "tls"):
		return "network"
	case strings.Contains(msg, "http 4"), strings.Contains(msg, "http 5"), strings.Contains(msg, "start checks failed"), strings.Contains(msg, "get summary failed"), strings.Contains(msg, "poll failed"):
		return "api"
	default:
		return "unknown"
	}
}

func buildRunClusterSummary(r ClusterResult) RunClusterSummary {
	s := RunClusterSummary{Address: r.Cluster, OK: r.Err == nil}
	if r.Err != nil {
		s.Error = r.Err.Error()
		if strings.TrimSpace(r.ErrorClass) != "" {
			s.ErrorClass = r.ErrorClass
		} else {
			s.ErrorClass = classifyClusterError(r.Err)
		}
		return s
	}
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range r.Blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; ok {
			counts[sev]++
		} else {
			counts["INFO"]++
		}
	}
	s.FailCount = counts["FAIL"]
	s.WarnCount = counts["WARN"]
	s.ErrCount = counts["ERR"]
	s.InfoCount = counts["INFO"]
	s.ChecksTotal = len(r.Blocks)
	s.HealthScore = clusterHealthScore(s.FailCount, s.WarnCount, s.ErrCount, s.ChecksTotal)
	return s
}

func failureClassCounts(results []ClusterResult) map[string]int {
	counts := map[string]int{
		"timeout":    0,
		"auth":       0,
		"network":    0,
		"api":        0,
		"parser":     0,
		"rate_limit": 0,
		"unknown":    0,
	}
	for _, r := range results {
		if r.Err == nil {
			continue
		}
		class := strings.TrimSpace(r.ErrorClass)
		if class == "" {
			class = classifyClusterError(r.Err)
		}
		if _, ok := counts[class]; !ok {
			class = "unknown"
		}
		counts[class]++
	}
	return counts
}

func generateJSON(fs FS, blocks []ParsedBlock, filename string, meta HTMLMeta) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f, err := fs.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	checks := make([]JSONCheck, 0, len(blocks))
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		counts[sev]++
		checks = append(checks, JSONCheck{
			Severity:  sev,
			CheckName: b.CheckName,
			Detail:    b.DetailRaw,
		})
	}

	output := JSONOutput{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Checks:      checks,
		Summary: JSONSummary{
			Total: len(blocks),
			Count: counts,
		},
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

type sarifLog struct {
	Version string     `json:"version"`
	Schema  string     `json:"$schema"`
	Runs    []sarifRun `json:"runs"`
}
type sarifRun struct {
	Tool    sarifTool     `json:"tool"`
	Results []sarifResult `json:"results"`
}
type sarifTool struct {
	Driver sarifDriver `json:"driver"`
}
type sarifDriver struct {
	Name           string      `json:"name"`
	Version        string      `json:"version,omitempty"`
	InformationURI string      `json:"informationUri,omitempty"`
	Rules          []sarifRule `json:"rules,omitempty"`
}
type sarifRule struct {
	ID               string `json:"id"`
	ShortDescription struct {
		Text string `json:"text"`
	} `json:"shortDescription"`
}
type sarifResult struct {
	RuleID  string `json:"ruleId"`
	Level   string `json:"level"`
	Message struct {
		Text string `json:"text"`
	} `json:"message"`
}

func sarifLevelForSeverity(sev string) string {
	switch strings.ToUpper(strings.TrimSpace(sev)) {
	case "FAIL":
		return "error"
	case "WARN":
		return "warning"
	case "ERR":
		return "error"
	default:
		return "note"
	}
}

func generateSARIF(fs FS, blocks []ParsedBlock, filename string) error {
	if err := fs.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	ruleMap := map[string]bool{}
	rules := make([]sarifRule, 0, len(blocks))
	results := make([]sarifResult, 0, len(blocks))
	for _, b := range blocks {
		ruleID := strings.TrimSpace(b.CheckName)
		if ruleID == "" {
			ruleID = "NCC_CHECK"
		}
		if !ruleMap[ruleID] {
			ruleMap[ruleID] = true
			r := sarifRule{ID: ruleID}
			r.ShortDescription.Text = ruleID
			rules = append(rules, r)
		}
		res := sarifResult{
			RuleID: ruleID,
			Level:  sarifLevelForSeverity(b.Severity),
		}
		res.Message.Text = strings.TrimSpace(b.DetailRaw)
		if res.Message.Text == "" {
			res.Message.Text = "NCC finding"
		}
		results = append(results, res)
	}

	payload := sarifLog{
		Version: "2.1.0",
		Schema:  "https://json.schemastore.org/sarif-2.1.0.json",
		Runs: []sarifRun{{
			Tool: sarifTool{
				Driver: sarifDriver{
					Name:           "ncc-orchestrator",
					Version:        Version,
					InformationURI: "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
					Rules:          rules,
				},
			},
			Results: results,
		}},
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(filename, data, 0644)
}

func filterBlocksBySeverity(blocks []ParsedBlock, allowedSeverities []string) []ParsedBlock {
	if len(allowedSeverities) == 0 {
		return blocks
	}
	allowed := make(map[string]bool)
	for _, s := range allowedSeverities {
		allowed[strings.ToUpper(strings.TrimSpace(s))] = true
	}
	filtered := make([]ParsedBlock, 0, len(blocks))
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		if allowed[sev] {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

type ExcludedAlert struct {
	Severity   string `json:"severity"`
	CheckName  string `json:"check_name"`
	Detail     string `json:"detail,omitempty"`
	MatchMode  string `json:"match_mode"`
	MatchValue string `json:"match_value"`
}

func filterBlocksByTitle(blocks []ParsedBlock, excludedTitles []string, matchMode string) ([]ParsedBlock, []ExcludedAlert, error) {
	if len(excludedTitles) == 0 {
		return blocks, nil, nil
	}
	mode, err := normalizeExcludeAlertMatchMode(matchMode)
	if err != nil {
		return blocks, nil, err
	}
	cleanedTitles := make([]string, 0, len(excludedTitles))
	for _, title := range excludedTitles {
		t := strings.TrimSpace(title)
		if t == "" {
			continue
		}
		cleanedTitles = append(cleanedTitles, t)
	}
	if len(cleanedTitles) == 0 {
		return blocks, nil, nil
	}
	regexPatterns := make([]*regexp.Regexp, 0, len(cleanedTitles))
	if mode == "regex" {
		for _, pattern := range cleanedTitles {
			re, err := regexp.Compile(pattern)
			if err != nil {
				return blocks, nil, fmt.Errorf("exclude-alert-titles regex %q: %w", pattern, err)
			}
			regexPatterns = append(regexPatterns, re)
		}
	}
	match := func(checkName string) (bool, string) {
		checkLower := strings.ToLower(strings.TrimSpace(checkName))
		switch mode {
		case "exact":
			for _, title := range cleanedTitles {
				if checkLower == strings.ToLower(title) {
					return true, title
				}
			}
		case "contains":
			for _, title := range cleanedTitles {
				if strings.Contains(checkLower, strings.ToLower(title)) {
					return true, title
				}
			}
		case "regex":
			for i, re := range regexPatterns {
				if re.MatchString(checkName) {
					return true, cleanedTitles[i]
				}
			}
		}
		return false, ""
	}

	filtered := make([]ParsedBlock, 0, len(blocks))
	excluded := make([]ExcludedAlert, 0)
	for _, b := range blocks {
		if ok, matchedBy := match(b.CheckName); ok {
			excluded = append(excluded, ExcludedAlert{
				Severity:   b.Severity,
				CheckName:  b.CheckName,
				Detail:     b.DetailRaw,
				MatchMode:  mode,
				MatchValue: matchedBy,
			})
			continue
		}
		filtered = append(filtered, b)
	}
	return filtered, excluded, nil
}

func rowsFromBlocks(blocks []ParsedBlock) []Row {
	rows := make([]Row, 0, len(blocks))
	for _, b := range blocks {
		detail := template.HTML(strings.ReplaceAll(html.EscapeString(b.DetailRaw), "\n", "<br>"))
		rows = append(rows, Row{
			Severity:  b.Severity,
			CheckName: html.EscapeString(strings.ReplaceAll(b.CheckName, "\n", " ")),
			Detail:    detail,
		})
	}
	return rows
}

// ==================== Aggregation ====================

// generateTestAgg produces a test index.html with n clusters and ~10–15 rows per cluster for scalability testing.
func generateTestAgg(n int, outDir string) error {
	severities := []string{"FAIL", "WARN", "ERR", "INFO"}
	checks := []string{
		"AHV host time sync", "CVM memory", "Disk health", "Network connectivity",
		"Storage pool", "Prism connectivity", "NCC version", "Cluster health",
		"Data resilience", "VM placement", "Controller VM", "License validity",
	}
	agg := make([]AggBlock, 0, n*12)
	clusterFiles := make([]struct{ Cluster, HTML, CSV string }, 0, n)
	snapshotClusters := make([]ClusterChecksSnapshot, 0, n)
	runClusters := make([]RunClusterSummary, 0, n)
	totalChecks := 0
	for i := 1; i <= n; i++ {
		cluster := fmt.Sprintf("10.0.%d.%d", (i-1)/255, (i-1)%255+1)
		clusterName := fmt.Sprintf("cluster-%03d", i)
		clusterVersion := "6.5.2"
		nccVersion := "4.2.0"
		clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
			Cluster: cluster,
			HTML:    cluster + ".html",
			CSV:     cluster + ".csv",
		})
		numChecks := 8 + (i % 8)
		clusterChecks := make([]CheckSnapshotEntry, 0, numChecks)
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		for j := 0; j < numChecks; j++ {
			sev := severities[(i+j)%len(severities)]
			check := checks[j%len(checks)]
			agg = append(agg, AggBlock{
				Cluster:        cluster,
				Severity:       sev,
				Check:          check,
				Detail:         fmt.Sprintf("Test detail for %s on %s. See https://portal.nutanix.com/kb/%d for more.", check, clusterName, 1000+i+j),
				ClusterName:    clusterName,
				ClusterVersion: clusterVersion,
				NCCVersion:     nccVersion,
			})
			clusterChecks = append(clusterChecks, CheckSnapshotEntry{
				CheckName: check,
				Severity:  sev,
			})
			counts[sev]++
		}
		sort.Slice(clusterChecks, func(a, b int) bool {
			if clusterChecks[a].CheckName == clusterChecks[b].CheckName {
				return clusterChecks[a].Severity < clusterChecks[b].Severity
			}
			return clusterChecks[a].CheckName < clusterChecks[b].CheckName
		})
		total := len(clusterChecks)
		health := clusterHealthScore(counts["FAIL"], counts["WARN"], counts["ERR"], total)
		snapshotClusters = append(snapshotClusters, ClusterChecksSnapshot{
			Address:     cluster,
			Checks:      clusterChecks,
			FailCount:   counts["FAIL"],
			WarnCount:   counts["WARN"],
			ErrCount:    counts["ERR"],
			InfoCount:   counts["INFO"],
			ChecksTotal: total,
			HealthScore: health,
		})
		runClusters = append(runClusters, RunClusterSummary{
			Address:     cluster,
			OK:          true,
			FailCount:   counts["FAIL"],
			WarnCount:   counts["WARN"],
			ErrCount:    counts["ERR"],
			InfoCount:   counts["INFO"],
			ChecksTotal: total,
			HealthScore: health,
		})
		totalChecks += total
	}
	fs := OSFS{}
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return err
	}
	sort.Slice(snapshotClusters, func(i, j int) bool { return snapshotClusters[i].Address < snapshotClusters[j].Address })
	sort.Slice(runClusters, func(i, j int) bool { return runClusters[i].Address < runClusters[j].Address })

	now := time.Now().UTC()
	currTS := now.Format(time.RFC3339)
	prevTS := now.Add(-4 * time.Hour).Format(time.RFC3339)

	avgHealth := 0
	minHealth := 100
	for _, c := range runClusters {
		avgHealth += c.HealthScore
		if c.HealthScore < minHealth {
			minHealth = c.HealthScore
		}
	}
	if len(runClusters) > 0 {
		avgHealth /= len(runClusters)
	} else {
		minHealth = 0
	}

	runSummary := RunSummaryJSON{
		Timestamp:      currTS,
		DurationS:      137.7,
		ClustersOK:     len(runClusters),
		ClustersFailed: 0,
		Clusters:       runClusters,
		IndexHTML:      filepath.ToSlash(filepath.Join(filepath.Base(outDir), "index.html")),
		TotalChecks:    totalChecks,
		AvgHealthScore: avgHealth,
		MinHealthScore: minHealth,
	}
	currentSnapshot := ChecksSnapshotJSON{
		Timestamp: currTS,
		Clusters:  snapshotClusters,
	}

	// Build a previous snapshot with small deterministic differences so
	// drill-down/regression/flaky artifacts are populated for UI simulation.
	previousSnapshot := ChecksSnapshotJSON{
		Timestamp: prevTS,
		Clusters:  make([]ClusterChecksSnapshot, 0, len(snapshotClusters)),
	}
	for idx, c := range snapshotClusters {
		cp := c
		cp.Checks = append([]CheckSnapshotEntry(nil), c.Checks...)
		if len(cp.Checks) > 0 {
			switch cp.Checks[0].Severity {
			case "FAIL":
				cp.Checks[0].Severity = "WARN"
			case "WARN":
				cp.Checks[0].Severity = "INFO"
			case "ERR":
				cp.Checks[0].Severity = "WARN"
			default:
				cp.Checks[0].Severity = "WARN"
			}
		}
		if idx%3 == 0 && len(cp.Checks) > 1 {
			cp.Checks = cp.Checks[:len(cp.Checks)-1]
		}
		if idx%4 == 0 {
			cp.Checks = append(cp.Checks, CheckSnapshotEntry{
				CheckName: "Legacy replication guard",
				Severity:  "FAIL",
			})
		}
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		for _, ch := range cp.Checks {
			sev := strings.ToUpper(strings.TrimSpace(ch.Severity))
			if _, ok := counts[sev]; !ok {
				sev = "INFO"
			}
			counts[sev]++
		}
		cp.FailCount = counts["FAIL"]
		cp.WarnCount = counts["WARN"]
		cp.ErrCount = counts["ERR"]
		cp.InfoCount = counts["INFO"]
		cp.ChecksTotal = len(cp.Checks)
		cp.HealthScore = clusterHealthScore(cp.FailCount, cp.WarnCount, cp.ErrCount, cp.ChecksTotal)
		previousSnapshot.Clusters = append(previousSnapshot.Clusters, cp)
	}

	prevRunClusters := make([]RunClusterSummary, 0, len(previousSnapshot.Clusters))
	prevTotalChecks := 0
	prevAvgHealth := 0
	prevMinHealth := 100
	for _, c := range previousSnapshot.Clusters {
		prevRunClusters = append(prevRunClusters, RunClusterSummary{
			Address:     c.Address,
			OK:          true,
			FailCount:   c.FailCount,
			WarnCount:   c.WarnCount,
			ErrCount:    c.ErrCount,
			InfoCount:   c.InfoCount,
			ChecksTotal: c.ChecksTotal,
			HealthScore: c.HealthScore,
		})
		prevTotalChecks += c.ChecksTotal
		prevAvgHealth += c.HealthScore
		if c.HealthScore < prevMinHealth {
			prevMinHealth = c.HealthScore
		}
	}
	if len(prevRunClusters) > 0 {
		prevAvgHealth /= len(prevRunClusters)
	} else {
		prevMinHealth = 0
	}
	previousSummary := RunSummaryJSON{
		Timestamp:      prevTS,
		DurationS:      129.4,
		ClustersOK:     len(prevRunClusters),
		ClustersFailed: 0,
		Clusters:       prevRunClusters,
		IndexHTML:      filepath.ToSlash(filepath.Join(filepath.Base(outDir), "index.html")),
		TotalChecks:    prevTotalChecks,
		AvgHealthScore: prevAvgHealth,
		MinHealthScore: prevMinHealth,
	}

	regression := computeRegressionSummary(previousSummary, true, runSummary)
	drillDown := computeDrillDownDiff(previousSnapshot, true, currentSnapshot)

	// Build synthetic history to ensure flaky-checks.json has representative data.
	olderA := previousSnapshot
	olderA.Timestamp = now.Add(-8 * time.Hour).Format(time.RFC3339)
	olderB := previousSnapshot
	olderB.Timestamp = now.Add(-6 * time.Hour).Format(time.RFC3339)
	for i := range olderA.Clusters {
		if len(olderA.Clusters[i].Checks) > 0 {
			olderA.Clusters[i].Checks[0].Severity = "WARN"
		}
		if len(olderB.Clusters[i].Checks) > 0 {
			olderB.Clusters[i].Checks[0].Severity = "FAIL"
		}
	}
	flaky := computeFlakyChecks([]ChecksSnapshotJSON{olderA, olderB, currentSnapshot}, 2)
	slo := buildSLODashboard(runSummary)

	if err := writeRunSummaryJSON(fs, outDir, runSummary); err != nil {
		return err
	}
	if err := writeChecksSnapshotJSON(fs, outDir, currentSnapshot); err != nil {
		return err
	}
	if err := writeDrillDownDiffJSON(fs, outDir, drillDown); err != nil {
		return err
	}
	if err := writeFlakyChecksJSON(fs, outDir, flaky); err != nil {
		return err
	}
	if err := writeRegressionSummaryJSON(fs, outDir, regression); err != nil {
		return err
	}
	if err := writeSLODashboardJSON(fs, outDir, slo); err != nil {
		return err
	}
	if err := writeAggregatedHTMLSingle(fs, outDir, agg, clusterFiles, Config{}); err != nil {
		return err
	}
	return nil
}

type AggBlock struct {
	Cluster        string
	Severity       string
	Check          string
	Detail         string
	ClusterName    string
	ClusterVersion string
	NCCVersion     string
}

// writeAllClustersFailedHTML writes a minimal index.html when every cluster failed, so the report page exists.
func writeAllClustersFailedHTML(fs FS, outDir string, failedClusters []string) error {
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, "index.html")
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>NCC Aggregated Report</title></head>
<body style="font-family: system-ui; max-width: 800px; margin: 2rem auto; padding: 1rem; background: #0f172a; color: #e5e7eb;">
<h1>NCC Aggregated Report</h1>
<p style="color: #f59e0b;">All clusters failed. No data was collected.</p>
<p>Failed clusters:</p>
<ul>{{range .Failed}}
<li><code>{{.}}</code></li>{{end}}
</ul>
<p style="color: #9ca3af; font-size: 14px;">Generated at {{.GeneratedAt}}</p>
</body>
</html>`
	t := template.Must(template.New("allfailed").Parse(tmpl))
	type data struct {
		Failed      []string
		GeneratedAt string
	}
	d := data{Failed: failedClusters, GeneratedAt: time.Now().Format(time.RFC3339)}
	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	return t.Execute(f, d)
}

func writeAggregatedHTMLSingle(fs FS, outDir string, rows []AggBlock, perCluster []struct{ Cluster, HTML, CSV string }, cfg Config) error {
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, "index.html")
	abs, _ := filepath.Abs(path)
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>NCC Aggregated Report</title>
	<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjI4IiBoZWlnaHQ9IjI4IiByeD0iOCIgZmlsbD0iIzBCMTIyMCIvPgo8cGF0aCBkPSJNOSAyM1Y5SDEyTDIwIDE5VjlIMjNWMjNIMjBMMTIgMTNWMjNIOVoiIGZpbGw9IiMyMkM1NUUiLz4KPGNpcmNsZSBjeD0iMjQiIGN5PSI4IiByPSIyIiBmaWxsPSIjMzhCREY4Ii8+CjxjaXJjbGUgY3g9IjgiIGN5PSIyNCIgcj0iMiIgZmlsbD0iI0Y1OUUwQiIvPgo8L3N2Zz4=">
	<style>
	:root {
	  --bg: #0f172a;
	  --card: #111827;
	  --text: #e5e7eb;
	  --muted: #9ca3af;
	  --accent: #2563eb;
	  --row1: #0b1224;
	  --row2: #0e1630;
	  --border: #1f2937;
	  --fail: #ef4444;
	  --warn: #f59e0b;
	  --info: #3b82f6;
	  --details: #aaa;
	  --err:  #94a3b8;
	}
	* { box-sizing: border-box; }
	html, body { height: 100%; }
	body {
	  margin: 0;
	  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
	  background: linear-gradient(180deg,#0b1224,#0e1630);
	  color: var(--text);
    background-color: var(--row1);
	}
	.container { max-width: 1400px; margin: 0 auto; padding: 7px 12px; }
	.header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
	.title { flex: 1; min-width: 0; }
	.title h1 { margin: 0; font-size: 22px; font-weight: 700; }
	.title .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }
	.header-actions { flex-shrink: 0; }
	.controls { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 12px 0 18px 0; }
	.control { background: #0d152b; border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; display: flex; gap: 8px; align-items: center; }
	.control label { font-size: 12px; color: var(--muted); margin-right: 6px; }
	input[type="text"] { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; outline: none; width: 280px; }
	select, button { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; outline: none; }
	button:hover { border-color: var(--accent); cursor: pointer; }
	.badge { display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 999px; background:#0a1123; border:1px solid var(--border); user-select:none; }
	.badge .dot { width: 8px; height: 8px; border-radius: 999px; display:inline-block; }
	.dot.fail{ background: var(--fail); } .dot.warn{ background: var(--warn); }
	.dot.info{ background: var(--info); } .dot.err{ background: var(--err); }
	.legend { display:flex; gap:8px; flex-wrap: wrap; }
	.card { padding: 12px; }
	
	 
	.summary { display:grid; grid-template-columns: repeat(5, 1fr); gap:12px; margin: 16px 0; }
	.sum-item { background: #0a1123; border: 1px solid var(--border); border-radius: 10px; padding: 10px; }
	.sum-item .label { font-size: 12px; color: var(--muted); }
	.sum-item .count { font-size: 18px; font-weight: 700; margin-top: 6px; }
	.progress { height: 6px; border-radius: 999px; background: #0d152b; margin-top: 8px; overflow: hidden; border:1px solid var(--border); }
	.progress > span { display:block; height:100%; }
	.progress.fail > span { background: var(--fail); } .progress.warn > span { background: var(--warn); }
	.progress.err  > span { background: var(--err); }  .progress.info > span { background: var(--info); }
	
.scroll {
  overflow-x: auto;
  overflow-y: hidden;
  -webkit-overflow-scrolling: touch; /* smooth mobile scroll */
  border-radius: 12px;
  border: 1px solid var(--border);
  margin: 0 -12px 16px -12px; /* full width minus padding */
  padding: 12px;
}

@media (max-width: 1024px) {
  .scroll {
    margin: 0 -8px 16px -8px;
    padding: 8px;
  }
}


	.scroll::-webkit-scrollbar { height: 10px; }
	.scroll::-webkit-scrollbar-thumb { background: #22304d; border-radius: 8px; }
	.scroll::-webkit-scrollbar-track { background: #0a1123; }
	
	 
	table { width: 100%; border-collapse: collapse; table-layout: fixed; }
	thead th {
	  position: sticky; top: 0; z-index: 1;
	  background: #0d152b; border-bottom: 1px solid var(--border);
	  padding: 10px; text-align: left; font-size: 12px; color: var(--muted);
	}
	tbody td { padding: 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
	thead th, tbody td { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	
	tbody tr:nth-child(odd) { background: var(--row1); }
	tbody tr:nth-child(even){ background: var(--row2); }
	
	td .severity { padding: 2px 8px; border-radius: 999px; font-size: 12px; }
	.sev-FAIL { background: #2b0d0d; color: var(--fail); border: 1px solid #4c1d1d; }
	.sev-WARN { background: #2b1f0d; color: var(--warn); border: 1px solid #4a3112; }
	.sev-INFO { background: #0c1f35; color: var(--info); border: 1px solid #173e6d; }
	.sev-ERR  { background: #1b2130; color: var(--err);  border: 1px solid #2c354a; }
	
	small.mono { color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
	.highlight { background: #3b82f655; }
	
	 
	th.col-cname, td.col-cname { width: 190px; }
	th.col-cluster, td.col-cluster   { width: 140px; }
	th.col-sev,     td.col-sev       { width: 96px; }
	th.col-title,   td.col-title     { width: 240px; }
	th.col-kb,      td.col-kb        { width: 110px; }
	th.col-detail,  td.col-detail    { width: 640px; }
	th.col-actions, td.col-actions   { width: 220px; }
	
	td.col-detail { white-space: normal; overflow: visible; }
	.detail-full { color: var(--details); font-size: 13px; line-height: 1.35; }
	
	 
	tbody tr.selected { outline: 2px solid var(--accent); outline-offset: -2px; }
	.actions { white-space: nowrap; display: inline-flex; gap: 6px; flex-wrap: wrap; }
	.actions button { background:#0a1123; border:1px solid var(--border); color:var(--text); padding:6px 8px; border-radius:8px; }
	.actions button:hover { border-color: var(--accent); cursor:pointer; }
	
	 
	a { color: #93c5fd; text-decoration: none; }
	a:hover { text-decoration: underline; color: #bfdbfe; }
	a:visited { color: #a5b4fc; }
	a[href^="http"]::after {
	  content: "↗";
	  font-size: 11px;
	  margin-left: 4px;
	  color: #64748b;
	}
	   
.control input[type="checkbox"] {
  position: absolute;
  opacity: 0;
  cursor: pointer;
  height: 0;
  width: 0;
}


.control span {
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  padding-left: 24px;
  min-height: 16px;  
  cursor: pointer;
  color: var(--muted);
}


.control span::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 0;
  transform: translateY(-50%);  
  height: 16px;
  width: 16px;
  background-color: #0a1123;
  border: 1px solid var(--border);
  border-radius: 4px;
  box-sizing: border-box;  
}


.control span::after {
  content: "";
  width: 9px;
  height: 9px;
  background-color: var(--muted);
  position: absolute;
  top: 50%;
  left: 8px;  
  transform: translate(-50%, -50%) scale(0);  
  transition: transform 0.2s ease-in-out;
  border-radius: 2px;
}


.control input[type="checkbox"]:checked ~ span::after {
  transform: translate(-50%, -50%) scale(1);
}


 
.control span:hover::before {
  border-color: var(--accent);
}


 
.control input[type="checkbox"]:focus + span::before {
  outline: 2px solid var(--accent);
}

.cluster-meta {
  font-size: 10px;
  color: var(--muted);
  margin-top: 2px;
}

.cluster-wrapper {
  display: flex;
  gap: 8px;
  align-items: center;
  flex: 1;
}

.cluster-display {
  flex: 1;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 14px;
  min-height: 20px;
  cursor: pointer;
  position: relative;
  font-size: 13px;
}

.cluster-display:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.05);
}

.cluster-placeholder {
  color: var(--text);
}

.cluster-toggle {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--text);
  padding: 8px 12px;
  border-radius: 8px;
  font-size: 12px;
  cursor: pointer;
  min-width: 70px;
}

.cluster-toggle:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-modal {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0,0,0,0.8);
  z-index: 10000;
  display: flex;
  align-items: center;
  justify-content: center;
}

.cluster-modal-content {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 24px;
  max-width: 500px;
  width: 90%;
  max-height: 80vh;  /* ✅ Fixed height */
  overflow-y: auto;  /* ✅ Scroll whole modal */
}

.cluster-list {
  max-height: 400px;        /* ✅ Scroll container */
  overflow-y: auto;         /* ✅ Vertical scroll */
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 8px;
  margin-bottom: 16px;
  background: #0a1123;
}

.cluster-list::-webkit-scrollbar {
  width: 8px;
}

.cluster-list::-webkit-scrollbar-track {
  background: #0a1123;
  border-radius: 4px;
}

.cluster-list::-webkit-scrollbar-thumb {
  background: #334155;
  border-radius: 4px;
}

.cluster-list::-webkit-scrollbar-thumb:hover {
  background: var(--accent);
}


.cluster-item {
  display: flex;
  align-items: center;
  padding: 12px;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  margin-bottom: 4px;  /* ✅ Spacing */
}

.cluster-item:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-item.active {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.2);
  color: var(--accent);
}
.cluster-load-more {
  width: 100%;
  padding: 10px;
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--accent);
  border-radius: 8px;
  cursor: pointer;
  font-size: 13px;
}
.cluster-load-more:hover { border-color: var(--accent); background: rgba(37,99,235,0.1); }

.modal-buttons {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--border);
}

.cluster-status-wrapper {
  display: flex;
  gap: 8px;
  align-items: center;
  flex: 1;
}

.cluster-status {
  flex: 1;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 16px;
  font-size: 13px;
  color: var(--text);
  cursor: pointer;
  position: relative;
  transition: all 0.2s ease;
  min-height: 20px;
  display: flex;
  align-items: center;
}

.cluster-status:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-status::after {
  content: "▼";
  margin-left: 8px;
  color: var(--muted);
  font-size: 11px;
}

.cluster-status-empty {
  color: var(--muted);
}

.cluster-edit-btn {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--text);
  padding: 10px 14px;
  border-radius: 8px;
  font-size: 12px;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.2s ease;
}

.cluster-edit-btn:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}



.tooltip {
  position: absolute;
  z-index: 1000;
  background: var(--card);
  color: var(--text);
  padding: 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.4;
  max-width: 450px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.4);
  white-space: pre-wrap;
  pointer-events: none;
}

.cluster-expand-btn {
  background: #0a1123 !important;
  border: 1px solid var(--border) !important;
  color: var(--accent) !important;
  padding: 8px 12px !important;
  border-radius: 6px !important;
  font-size: 12px !important;
  cursor: pointer !important;
  margin-top: 8px !important;
  width: 100% !important;
  text-align: left !important;
}

.cluster-expand-btn:hover {
  background: rgba(37,99,235,0.1) !important;
}

.full-cluster-table table th,
.full-cluster-table table td {
  padding: 6px 4px !important;
  text-align: right !important;
}

.full-cluster-table table th:first-child,
.full-cluster-table table td:first-child {
  text-align: left !important;
}


.compact-cluster-table {
  width: 100%;
  font-size: 12px;
  margin-top: 12px;
}

.compact-cluster-table th,
.compact-cluster-table td {
  padding: 6px 4px;
  text-align: right;
}

.compact-cluster-table th:first-child,
.compact-cluster-table td:first-child {
  text-align: left;
}

.compact-cluster-table tr:hover {
  background: rgba(37, 99, 235, 0.05);
}

.full-cluster-table {
  display: none;
  margin-top: 12px;
  max-height: 300px;
  overflow-y: auto;
}

.full-cluster-table.show {
  display: block;
}

#detailsToggle {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--accent);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 12px;
  cursor: pointer;
  margin-top: 8px;
}

#detailsToggle:hover {
  background: rgba(37, 99, 235, 0.1);
}

.expand-clusters-btn {
  width: 100% !important;
  background: #0a1123 !important;
  border: 1px solid var(--border) !important;
  color: var(--accent) !important;
  padding: 10px 12px !important;
  border-radius: 8px !important;
  font-size: 13px !important;
  cursor: pointer !important;
  margin-top: 12px !important;
}

.expand-clusters-btn:hover {
  background: rgba(37,99,235,0.15) !important;
  border-color: var(--accent) !important;
}

.compact-cluster-table a,
.full-cluster-table a {
  color: #93c5fd !important;
  text-decoration: none !important;
}

.compact-cluster-table a:hover,
.full-cluster-table a:hover {
  text-decoration: underline !important;
  color: #bfdbfe !important;
}


@media (max-width: 768px) {
  .controls {
    flex-direction: column !important;
    gap: 8px;
    align-items: stretch;
  }
  
  .control {
    flex: 1;
    min-width: 0; /* allow shrinking */
  }
  
  .control input[type="text"] {
    width: 100%;
    font-size: 16px; /* mobile zoom fix */
  }
}

@media (max-width: 900px) {
  th.col-kb, td.col-kb,
  th.col-actions, td.col-actions {
    display: none; /* Hide KB/Actions on small screens */
  }
  
  th.col-detail, td.col-detail {
    min-width: 200px; /* Priority for details */
  }
}

@media (max-width: 600px) {
  th.col-cluster, td.col-cluster {
    display: none; /* Hide IP on tiny screens */
  }
  
  th.col-cname, td.col-cname {
    width: 140px !important; /* Fixed width for cluster names */
  }
}

/* 6. PRINT STYLES (clean PDF export) */
@media print {
  .controls, .actions, .summary, footer {
    display: none !important;
  }
  
  .scroll {
    overflow: visible !important;
    border: none !important;
    margin: 0 !important;
  }
  
  table {
    font-size: 9px !important;
    width: 100% !important;
  }
  
  th, td {
    padding: 4px 2px !important;
    border-bottom: 1px solid #ccc !important;
  }
}

/* 7. MOBILE SUMMARY CARDS (stack vertically) */
@media (max-width: 768px) {
  .summary {
    grid-template-columns: 1fr !important;
    gap: 8px;
    margin: 12px 0;
  }
}

/* 8. TOUCH TARGETS (min 44px for mobile) */
button, .cluster-display, .cluster-toggle {
  min-height: 44px;
  min-width: 44px;
}

@media (hover: none) and (pointer: coarse) {
  .actions button {
    padding: 12px 16px; /* bigger touch targets */
    font-size: 14px;
  }
}

*:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.skip-link { position: absolute; top: -40px; left: 8px; background: var(--accent); color: #fff; padding: 8px 12px; border-radius: 6px; z-index: 100; text-decoration: none; font-size: 14px; }
.skip-link:focus { top: 8px; }
.report-footer { font-size: 0.8125rem; color: var(--muted); margin-top: 16px; padding: 12px 0; border-top: 1px solid var(--border); }
.report-meta-line { margin-top: 8px; line-height: 1.5; word-break: break-word; }
.sum-item.clickable { cursor: pointer; transition: background 0.15s, border-color 0.15s; }
.sum-item.clickable:hover { background: #0d152b; border-color: var(--accent); }
.sum-item.clickable:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.th-sort { cursor: pointer; user-select: none; }
.th-sort:hover { color: var(--text); }
.th-sort .sort-arrow { font-size: 10px; margin-left: 4px; opacity: 0.7; }
.table-info { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
.pagination { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 12px; padding: 8px 10px; border: 1px solid var(--border); border-radius: 10px; background: #0a1123; }
.pagination button { background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; min-width: 38px; transition: border-color 0.15s, background 0.15s; }
.pagination button:hover:not(:disabled) { border-color: var(--accent); }
.pagination button:disabled { opacity: 0.5; cursor: not-allowed; }
.pagination-info { font-size: 12px; color: var(--muted); }
.pagination-left { display: inline-flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.pagination-right { display: inline-flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; }
.pagination-pages { display: inline-flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.pagination-page.active { border-color: var(--accent); background: rgba(37,99,235,0.18); color: #bfdbfe; }
.pagination-ellipsis { color: var(--muted); padding: 0 2px; font-size: 12px; }
.pagination-size { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.pagination-size select { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 6px; font-size: 12px; }
.pagination-jump { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.pagination-jump input { width: 62px; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 6px; font-size: 12px; }
.empty-state { text-align: center; padding: 48px 16px; color: var(--muted); }
.empty-state p { margin: 0 0 12px 0; font-size: 14px; }
.per-cluster-btn { font-size: 12px; padding: 8px 14px; background: transparent; border: 1px solid var(--border); border-radius: 8px; color: var(--muted); cursor: pointer; display: inline-flex; align-items: center; gap: 6px; transition: border-color 0.15s, color 0.15s; }
.per-cluster-btn:hover { border-color: var(--accent); color: #93c5fd; }
.per-cluster-btn::after { content: "↗"; font-size: 11px; opacity: 0.8; }
.per-cluster-links { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.per-cluster-links a { font-size: 12px; padding: 4px 10px; background: #0a1123; border: 1px solid var(--border); border-radius: 6px; color: #93c5fd; text-decoration: none; }
.per-cluster-links a:hover { border-color: var(--accent); background: rgba(37,99,235,0.1); }
.row-badges { display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }
.ux-tag { font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); background:#0a1123; color: var(--muted); }
.ux-tag.tag-new { border-color: #7f1d1d; color: #fecaca; background: #3b1111; }
.ux-tag.tag-resolved { border-color: #14532d; color: #bbf7d0; background: #0f2d1d; }
.ux-tag.tag-changed { border-color: #1e3a8a; color: #bfdbfe; background: #10244f; }
.ux-tag.tag-flaky { border-color: #92400e; color: #fde68a; background: #3a2609; }
.insights { display:grid; grid-template-columns: repeat(3, 1fr); gap:12px; margin: 0 0 16px 0; }
.insights .sum-item .meta { font-size: 11px; color: var(--muted); margin-top: 6px; line-height: 1.4; max-height: 84px; overflow: auto; }
.meta { font-size: 12px; color: var(--muted); margin-top: 6px; line-height: 1.4; }
.health-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:10px; margin-top: 10px; }
.health-card { border: 1px solid var(--border); background: #0a1123; border-radius: 10px; padding: 10px; }
.health-top { display:flex; justify-content: space-between; align-items: center; gap: 10px; }
.health-name { font-size: 13px; color: var(--text); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.health-score { font-size: 12px; font-weight: 700; border-radius: 999px; padding: 3px 8px; border: 1px solid transparent; }
.health-score.good { color: #86efac; border-color: #14532d; background: #0f2d1d; }
.health-score.warn { color: #fde68a; border-color: #854d0e; background: #36250b; }
.health-score.bad { color: #fca5a5; border-color: #7f1d1d; background: #3b1111; }
.health-trend { margin-top: 8px; color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
.health-bar { margin-top: 8px; height: 6px; background: #111827; border: 1px solid var(--border); border-radius: 999px; overflow: hidden; }
.health-bar > span { display: block; height: 100%; background: linear-gradient(90deg, #ef4444 0%, #f59e0b 45%, #22c55e 100%); }

@media (max-width: 768px) {
  .insights { grid-template-columns: 1fr !important; gap: 8px; }
  .pagination { padding: 8px; }
  .pagination-left, .pagination-right { width: 100%; }
  .pagination-right { justify-content: flex-start; }
}

	</style>
	<script>

	const AGG = {{.JSON}};
	var CLUSTER_LINKS = {{.ClusterLinksJSON}};
	const DIFF_FLAGS = {{.DiffFlagsJSON}};
	const FLAKY_KEYS = {{.FlakyKeysJSON}};
	const POLICY_VIOLATIONS = {{.PolicyViolationsJSON}};
	const RUN_SUMMARY = {{.RunSummaryJSON}};
	const HEALTH_TRENDS = {{.HealthTrendsJSON}};
	const ARTIFACT_LINKS = {{.ArtifactLinksJSON}};
	const REPORT_META = {{.ReportMetaJSON}};

	let state = {
  sortKey: "severity",
  sortDir: "asc",
  filterSev: new Set(["FAIL","WARN","ERR","INFO"]),
  filterClusters: new Set(),
  search: "",
  showClusterModal: false,
  allClusters: [],
  pageSize: 100,
  currentPage: 0,
  filteredRows: [],
  compareMode: "all",
  clusterModalSearch: "",
  clusterListVisible: 100,
  showPerClusterModal: false,
  perClusterSearch: "",
  perClusterListVisible: 50
};
	
	const sevRank = { FAIL: 1, WARN: 2, ERR: 3, INFO: 4 };
	let selIndex = -1;

function init() {
  initClusters();
  renderMetadata();
  renderHealthWidget();
  renderArtifactLinks();
  updateAndRender();
  document.addEventListener("keydown", onKey);
  var statusEl = document.getElementById('clusterStatus');
  if (statusEl) {
    statusEl.onclick = toggleClusterFilter;
    statusEl.onkeydown = function(e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleClusterFilter(); } };
  }
}

function rowKeyFor(cluster, check) {
  return ((cluster || "").trim().toLowerCase() + "||" + (check || "").trim().toLowerCase());
}

function renderInsights(rows) {
  var policyViolations = Array.isArray(POLICY_VIOLATIONS) ? POLICY_VIOLATIONS : [];
  var changed = 0;
  var flaky = 0;
  (rows || []).forEach(function(r) {
    var k = rowKeyFor(r.cluster, r.check);
    var d = DIFF_FLAGS[k];
    if (d && (d.new_fail || d.resolved_fail || d.severity_changed)) changed++;
    if (FLAKY_KEYS[k]) flaky++;
  });
  var changedEl = document.getElementById("changedChecksCount");
  if (changedEl) changedEl.textContent = String(changed);
  var flakyEl = document.getElementById("flakyChecksCount");
  if (flakyEl) flakyEl.textContent = String(flaky);
  var policyStatus = document.getElementById("policyStatus");
  var policyMeta = document.getElementById("policyMeta");
  if (policyStatus) {
    if (policyViolations.length > 0) {
      policyStatus.textContent = "FAILED";
      policyStatus.style.color = "var(--fail)";
    } else {
      policyStatus.textContent = "PASS/NOT_CONFIGURED";
      policyStatus.style.color = "#86efac";
    }
  }
  if (policyMeta) {
    if (policyViolations.length > 0) {
      policyMeta.innerHTML = policyViolations.map(function(v) { return "• " + escapeHtml(v); }).join("<br>");
    } else {
      policyMeta.textContent = "No policy gate violations file for this run.";
    }
  }
}

function sparkline(values) {
  if (!values || !values.length) return "";
  var blocks = ["▁","▂","▃","▄","▅","▆","▇","█"];
  return values.map(function(v) {
    var n = Math.max(0, Math.min(100, Number(v) || 0));
    var idx = Math.round((n / 100) * (blocks.length - 1));
    return blocks[idx];
  }).join("");
}

function renderMetadata() {
  var meta = document.getElementById("reportMetaFooter");
  if (!meta) return;
  var parts = [];
  if (REPORT_META.version) parts.push("Version: " + REPORT_META.version);
  if (REPORT_META.git_revision) parts.push("Git: " + REPORT_META.git_revision);
  if (REPORT_META.stream) parts.push("Stream: " + REPORT_META.stream);
  if (REPORT_META.build_date) parts.push("Build: " + REPORT_META.build_date);
  if (RUN_SUMMARY && RUN_SUMMARY.duration_s) parts.push("Run duration: " + Number(RUN_SUMMARY.duration_s).toFixed(1) + "s");
  if (REPORT_META.quiet_hours || REPORT_META.maintenance_windows) {
    var q = REPORT_META.quiet_hours ? ("quiet-hours=" + REPORT_META.quiet_hours) : "";
    var m = REPORT_META.maintenance_windows ? ("maintenance=" + REPORT_META.maintenance_windows) : "";
    parts.push([q, m].filter(Boolean).join(" "));
  } else {
    parts.push("Quiet-hours/maintenance: not configured");
  }
  meta.textContent = parts.join(" | ");
}

function renderHealthWidget() {
  var root = document.getElementById("healthWidget");
  if (!root) return;
  var clusters = (RUN_SUMMARY && RUN_SUMMARY.clusters) ? RUN_SUMMARY.clusters.slice() : [];
  // Keep health widget aligned with currently selected cluster filters.
  if (state && state.filterClusters) {
    var selectedAddresses = new Set();
    (AGG || []).forEach(function(r) {
      var clusterLabel = displayClusterName(r);
      if (state.filterClusters.has(clusterLabel)) {
        selectedAddresses.add((r.cluster || "").trim());
      }
    });
    clusters = clusters.filter(function(c) {
      return selectedAddresses.has((c.address || "").trim());
    });
  }
  if (!clusters.length) {
    root.innerHTML = '<div class="meta">No per-cluster health data available for selected clusters.</div>';
    return;
  }
  clusters.sort(function(a, b) {
    var sa = Math.max(0, Math.min(100, Number(a.health_score || 0)));
    var sb = Math.max(0, Math.min(100, Number(b.health_score || 0)));
    if (sa !== sb) return sa - sb; // worst first
    return (a.address || "").localeCompare(b.address || "");
  });
  var visible = clusters.slice(0, 10);
  var rows = visible.map(function(c) {
    var k = (c.address || "").toLowerCase();
    var trend = HEALTH_TRENDS[k] || [];
    var score = Math.max(0, Math.min(100, Number(c.health_score || 0)));
    var cls = score >= 90 ? "good" : (score >= 75 ? "warn" : "bad");
    var trendText = trend.length ? sparkline(trend) : "n/a";
    return '<div class="health-card">' +
      '<div class="health-top">' +
        '<div class="health-name" title="' + escapeHtml(c.address || "") + '">' + escapeHtml(c.address || "-") + '</div>' +
        '<div class="health-score ' + cls + '">' + score.toFixed(0) + '/100</div>' +
      '</div>' +
      '<div class="health-trend">Trend: ' + escapeHtml(trendText) + '</div>' +
      '<div class="health-bar"><span style="width:' + score.toFixed(0) + '%"></span></div>' +
    '</div>';
  });
  var note = clusters.length > 10
    ? '<div class="meta">Showing 10 worst clusters out of ' + clusters.length + '.</div>'
    : '<div class="meta">Showing all ' + clusters.length + ' clusters.</div>';
  root.innerHTML = note + '<div class="health-grid">' + rows.join("") + '</div>';
}

function renderArtifactLinks() {
  var root = document.getElementById("artifactLinks");
  if (!root) return;
  var links = [];
  Object.keys(ARTIFACT_LINKS || {}).forEach(function(name) {
    var href = ARTIFACT_LINKS[name];
    if (!href) return;
    links.push('<a href="' + escapeHtml(href) + '" target="_blank" rel="noopener">' + escapeHtml(name) + '</a>');
  });
  root.innerHTML = links.length ? links.join("") : '<span class="meta">No artifact links available.</span>';
}

function setCompareMode(v) {
  state.compareMode = v || "all";
  updateAndRender();
}

function parseSearchQuery(raw) {
  var out = { terms: [], sev: null, cluster: null, changed: null, flaky: null };
  if (!raw) return out;
  raw.split(/\s+/).forEach(function(tok) {
    if (!tok) return;
    var m = tok.match(/^([^:]+):(.*)$/);
    if (!m) { out.terms.push(tok.toLowerCase()); return; }
    var key = m[1].toLowerCase();
    var val = m[2].toLowerCase();
    if (key === "sev" || key === "severity") out.sev = val.toUpperCase();
    else if (key === "cluster") out.cluster = val;
    else if (key === "changed") out.changed = (val === "true" || val === "1" || val === "yes");
    else if (key === "flaky") out.flaky = (val === "true" || val === "1" || val === "yes");
    else out.terms.push(tok.toLowerCase());
  });
  return out;
}

function displayClusterName(r) {
  var name = (r && r.clusterName ? String(r.clusterName).trim() : "");
  if (name) return name;
  var version = (r && r.clusterVersion ? String(r.clusterVersion).trim().toLowerCase() : "");
  if (version.indexOf("pc.") === 0) return "Prism Central";
  return (r && r.cluster ? String(r.cluster) : "-");
}

function getSelectedClusterAddresses() {
  var selectedAddresses = new Set();
  (AGG || []).forEach(function(r) {
    var label = displayClusterName(r);
    if (state.filterClusters.has(label)) {
      selectedAddresses.add((r.cluster || "").trim());
    }
  });
  return selectedAddresses;
}

function getClusterFilteredRows() {
  return (AGG || []).filter(function(r) {
    return state.filterClusters.has(displayClusterName(r));
  });
}

function initClusters() {
  const clusters = Array.from(new Set(AGG.map(function(r) { 
    return displayClusterName(r);
  }))).sort();
  console.log("Found clusters:", clusters); 
  state.allClusters = clusters;
  state.filterClusters = new Set(clusters);
  updateClusterStatus();
}

function updateClusterStatus() {
  const statusEl = document.getElementById('clusterStatus');
  if (!statusEl) return;
  const count = state.filterClusters.size;
  const total = state.allClusters.length || 0;
  let text, className;
  if (count === 0) {
    text = 'No clusters selected';
    className = 'cluster-status cluster-status-empty';
  } else if (count === total && total > 0) {
    text = total > 1 ? 'All clusters (' + total + ')' : 'All clusters selected';
    className = 'cluster-status';
  } else {
    if (total > 8) {
      text = count + ' of ' + total + ' clusters selected';
    } else {
      const names = Array.from(state.filterClusters).slice(0, 2);
      text = names.join(', ') + (count > 2 ? ' +' + (count - 2) : '') + ' (' + count + '/' + total + ')';
    }
    className = 'cluster-status';
  }
  statusEl.textContent = text;
  statusEl.className = className;
}


function toggleCluster(name) {
  if (state.filterClusters.has(name)) {
    state.filterClusters.delete(name);
  } else {
    state.filterClusters.add(name);
  }
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function selectAllClusters() {
  state.filterClusters = new Set(state.allClusters);
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function clearAllClusters() {
  state.filterClusters.clear();
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function toggleClusterFilter() {
  state.showClusterModal = !state.showClusterModal;
  if (state.showClusterModal) {
    showClusterModal();
  } else {
    hideClusterModal();
  }
}

function showClusterModal() {
  if (document.getElementById('clusterModal')) return;
  state.clusterModalSearch = "";
  state.clusterListVisible = 100;
  const modal = document.createElement('div');
  modal.id = 'clusterModal';
  modal.className = 'cluster-modal';
  modal.innerHTML = [
    '<div class="cluster-modal-content">',
      '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">',
        '<h3 style="margin: 0; font-size: 18px;">Select Clusters</h3>',
        '<button type="button" onclick="toggleClusterFilter()" style="background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: 6px; cursor: pointer;" aria-label="Close">✕</button>',
      '</div>',
      '<div style="margin-bottom: 12px;"><input type="text" id="clusterModalSearch" placeholder="Filter clusters..." style="width:100%; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; font-size: 13px;" aria-label="Filter cluster list"></div>',
      '<div class="cluster-list" id="clusterList"></div>',
      '<div id="clusterListMore" style="margin-top: 8px;"></div>',
      '<div class="modal-buttons">',
        '<button type="button" onclick="selectAllClusters()" style="padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: #0a1123; color: var(--text);">Select All</button>',
        '<button type="button" onclick="clearAllClusters()" style="padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: #0a1123; color: var(--text);">Clear All</button>',
        '<button type="button" onclick="toggleClusterFilter()" style="padding: 8px 16px; border-radius: 6px; background: var(--accent); color: white; border: none;">Done</button>',
      '</div>',
    '</div>'
  ].join('');
  document.body.appendChild(modal);
  var searchEl = document.getElementById('clusterModalSearch');
  if (searchEl) {
    searchEl.oninput = function() { state.clusterModalSearch = this.value.trim(); renderClusterList(); };
    searchEl.onkeydown = function(e) { if (e.key === 'Escape') { this.value = ''; state.clusterModalSearch = ''; renderClusterList(); this.focus(); } };
  }
  renderClusterList();
}

function hideClusterModal() {
  const modal = document.getElementById('clusterModal');
  if (modal) modal.remove();
}
function showPerClusterModal() {
  if (typeof CLUSTER_LINKS === 'undefined' || !CLUSTER_LINKS.length) return;
  if (document.getElementById('perClusterModal')) return;
  state.perClusterSearch = "";
  state.perClusterListVisible = 50;
  var modal = document.createElement('div');
  modal.id = 'perClusterModal';
  modal.className = 'cluster-modal';
  modal.innerHTML = [
    '<div class="cluster-modal-content" style="max-width: 560px;">',
      '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">',
        '<h3 style="margin: 0; font-size: 18px;">Per-cluster reports</h3>',
        '<button type="button" onclick="hidePerClusterModal()" style="background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: 6px; cursor: pointer;" aria-label="Close">✕</button>',
      '</div>',
      '<div style="margin-bottom: 12px;"><input type="text" id="perClusterSearch" placeholder="Filter by cluster name or IP..." style="width:100%; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; font-size: 13px;" aria-label="Filter per-cluster links"></div>',
      '<div class="cluster-list" id="perClusterList"></div>',
      '<div id="perClusterListMore" style="margin-top: 8px;"></div>',
    '</div>'
  ].join('');
  document.body.appendChild(modal);
  var searchEl = document.getElementById('perClusterSearch');
  if (searchEl) {
    searchEl.oninput = function() { state.perClusterSearch = this.value.trim(); renderPerClusterList(); };
    searchEl.onkeydown = function(e) { if (e.key === 'Escape') { this.value = ''; state.perClusterSearch = ''; renderPerClusterList(); this.focus(); } };
    searchEl.focus();
  }
  state.showPerClusterModal = true;
  renderPerClusterList();
}
function hidePerClusterModal() {
  var modal = document.getElementById('perClusterModal');
  if (modal) modal.remove();
  state.showPerClusterModal = false;
}
function renderPerClusterList() {
  var list = document.getElementById('perClusterList');
  var moreEl = document.getElementById('perClusterListMore');
  if (!list || typeof CLUSTER_LINKS === 'undefined') return;
  var needle = state.perClusterSearch.toLowerCase();
  var filtered = needle ? CLUSTER_LINKS.filter(function(item) { return (item.Cluster || '').toLowerCase().indexOf(needle) !== -1; }) : CLUSTER_LINKS.slice();
  var visible = filtered.slice(0, state.perClusterListVisible);
  var items = visible.map(function(item) {
    var name = escapeHtml(item.Cluster || '');
    var href = escapeHtml(item.HTML || '#');
    return '<a href="' + href + '" target="_blank" rel="noopener" class="cluster-item" style="display:block; text-decoration: none; color: inherit;">' + name + '</a>';
  });
  list.innerHTML = items.join('');
  if (moreEl) {
    var remaining = filtered.length - state.perClusterListVisible;
    if (remaining > 0) {
      moreEl.innerHTML = '<button type="button" class="cluster-load-more" onclick="state.perClusterListVisible += 50; renderPerClusterList();">Show more (' + remaining + ' remaining)</button>';
      moreEl.style.display = 'block';
    } else {
      moreEl.innerHTML = '';
      moreEl.style.display = 'none';
    }
  }
}

function renderClusterList() {
  const list = document.getElementById('clusterList');
  const moreEl = document.getElementById('clusterListMore');
  if (!list) return;
  var needle = state.clusterModalSearch.toLowerCase();
  var filtered = needle ? state.allClusters.filter(function(name) { return name.toLowerCase().indexOf(needle) !== -1; }) : state.allClusters.slice();
  var visible = filtered.slice(0, state.clusterListVisible);
  var items = visible.map(function(name) {
    var active = state.filterClusters.has(name) ? 'active' : '';
    var safeName = escapeHtml(name);
    var safeJs = jsStrEsc(name);
    return '<div class="cluster-item ' + active + '" onclick="toggleCluster(\'' + safeJs + '\')">' + safeName + '</div>';
  });
  list.innerHTML = items.join('');
  if (moreEl) {
    var remaining = filtered.length - state.clusterListVisible;
    if (remaining > 0) {
      moreEl.innerHTML = '<button type="button" class="cluster-load-more" onclick="state.clusterListVisible += 100; renderClusterList();">Show more (' + remaining + ' remaining)</button>';
      moreEl.style.display = 'block';
    } else {
      moreEl.innerHTML = '';
      moreEl.style.display = 'none';
    }
  }
}
	
	function filterBySev(sev) {
	  if (sev === null) {
	    state.filterSev = new Set(["FAIL","WARN","ERR","INFO"]);
	  } else {
	    state.filterSev = new Set([sev]);
	  }
	  document.querySelectorAll('.control input[type="checkbox"]').forEach(function(cb) {
	    var m = (cb.getAttribute('onchange') || '').match(/'([^']+)'/);
	    if (m) cb.checked = state.filterSev.has(m[1]);
	  });
	  updateAndRender();
	}
	function clearFilters() {
	  state.search = "";
	  var sb = document.getElementById("searchBox");
	  if (sb) sb.value = "";
	  state.filterClusters = new Set(state.allClusters);
	  state.filterSev = new Set(["FAIL","WARN","ERR","INFO"]);
	  document.querySelectorAll('.control input[type="checkbox"]').forEach(function(cb) { cb.checked = true; });
	  renderClusterList();
	  updateClusterStatus();
	  updateAndRender();
	  if (state.showClusterModal) { state.showClusterModal = false; hideClusterModal(); }
	}
	function setSev(checked, sev) {
	  if (checked) state.filterSev.add(sev); else state.filterSev.delete(sev);
	  updateAndRender();
	}
	
	function onSearch(inp) {
	  state.search = inp.value.trim();
	  updateAndRender();
	}
	
	let debounceTimer;
	function onSearchDebounced(inp) {
	  clearTimeout(debounceTimer);
	  debounceTimer = setTimeout(() => onSearch(inp), 150);
	}
	
	function sortBy(key) {
	  if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
	  else { state.sortKey = key; state.sortDir = "asc"; }
	  updateSortIndicators();
	  updateAndRender();
	}
	function updateSortIndicators() {
	  document.querySelectorAll('.th-sort .sort-arrow').forEach(function(el) {
	    var key = el.id ? el.id.replace('arrow-','') : '';
	    if (key === 'clusterName') key = 'clusterName';
	    if (key === 'cluster') key = 'cluster';
	    if (key === 'severity') key = 'severity';
	    if (key === 'check') key = 'check';
	    el.textContent = state.sortKey === key ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
	  });
	  var keys = ['clusterName','cluster','severity','check'];
	  keys.forEach(function(key) {
	    var el = document.getElementById('arrow-' + key);
	    if (el) el.textContent = state.sortKey === key ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
	  });
	}
	
function filterData() {
  const q = parseSearchQuery(state.search);
  return AGG.filter(r => {
    if (!state.filterSev.has(r.severity)) return false;
    const clusterId = displayClusterName(r);
    if (!state.filterClusters.has(clusterId)) return false;
    const k = rowKeyFor(r.cluster, r.check);
    const d = DIFF_FLAGS[k] || {};
    const isChanged = !!(d.new_fail || d.resolved_fail || d.severity_changed);
    const isFlaky = !!FLAKY_KEYS[k];

    if (state.compareMode === "changed" && !isChanged) return false;
    if (state.compareMode === "flaky" && !isFlaky) return false;

    if (q.sev && r.severity !== q.sev) return false;
    if (q.cluster && clusterId.toLowerCase().indexOf(q.cluster) === -1) return false;
    if (q.changed !== null && isChanged !== q.changed) return false;
    if (q.flaky !== null && isFlaky !== q.flaky) return false;

    if (!q.terms.length) return true;
    const hay = (clusterId + " " + r.severity + " " + r.check + " " + r.detail).toLowerCase();
    return q.terms.every(function(t){ return hay.indexOf(t) !== -1; });
  });
}


	function sortData(rows) {
	  const k = state.sortKey, dir = state.sortDir;
	  const mul = dir === "asc" ? 1 : -1;
	  rows.sort((a,b) => {
    let av = a[k], bv = b[k];
    if (k === "clusterName") {
      av = displayClusterName(a);
      bv = displayClusterName(b);
    }
		if (k === "severity") { av = sevRank[av] || 99; bv = sevRank[bv] || 99; }
		return (av > bv ? 1 : av < bv ? -1 : 0) * mul;
	  });
	  return rows;
	}
	
function updateCounts(rows) {
  const total = rows.length;
  const cnt = { FAIL:0, WARN:0, ERR:0, INFO:0 };
  rows.forEach(r => { if (cnt[r.severity] !== undefined) cnt[r.severity]++; });

  // Update main summary counts ONLY
  document.getElementById("countTotal").textContent = total;
  document.getElementById("countFail").textContent = cnt.FAIL;
  document.getElementById("countWarn").textContent = cnt.WARN;
  document.getElementById("countErr").textContent  = cnt.ERR;
  document.getElementById("countInfo").textContent = cnt.INFO;

  const pct = {};
  Object.keys(cnt).forEach(k => pct[k] = total ? Math.round(cnt[k]*100/total) : 0);
  document.getElementById("barFail").style.width = pct.FAIL + "%";
  document.getElementById("barWarn").style.width = pct.WARN + "%";
  document.getElementById("barErr").style.width  = pct.ERR  + "%";
  document.getElementById("barInfo").style.width = pct.INFO + "%";

  // const pc = document.getElementById("perCluster"); pc.innerHTML = "";
}

	
	function extractKB(detail) {
	  const text = detail || "";
	  const re = /(https?:\/\/[^\s)]+portal\.nutanix\.com\/kb\/\d+|https?:\/\/[^\s)]+)/i;
	  const m = text.match(re);
	  return m ? m[0] : "";
	}
	function kbLabel(url) {
	  if (!url) return "";
	  const m = url.match(/\/kb\/(\d+)\b/i);
	  return m ? ('KB-' + m[1]) : 'KB';
	}
	
	function escapeHtml(s) {
	  return (s || "").toString()
		.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")
		.replaceAll('"',"&quot;").replaceAll("'","&#39;");
	}
	function jsStrEsc(s) {
	  return (s || "").toString()
		.replace(/\\/g, "\\\\").replace(/'/g, "\\'").replace(/\r/g, " ").replace(/\n/g, " ");
	}
	
	function highlight(text, needle) {
	  if (!needle) return escapeHtml(text);
	  const re = new RegExp("(" + needle.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&") + ")", "ig");
	  return escapeHtml(text).replace(re, '<span class="highlight">$1</span>');
	}
	
	function formatCheckTitle(s) {
 	 s = s || "";
  	return s.replace(/^detailed information for\s*/i, "").replace(/:$/, "");
	}

	function jsEscape(s) {
	  return (s || "").toString()
		.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll("\r", " ")
		.replaceAll("'", "\\'").replaceAll("\"", "\\\"");
	}
	
	async function copyText(text) {
	  try { await navigator.clipboard.writeText(text); }
	  catch {
		const ta = document.createElement("textarea");
		ta.value = text; document.body.appendChild(ta);
		ta.select(); document.execCommand("copy");
		document.body.removeChild(ta);
	  }
	}

	
  function renderTable(rows) {
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = "";
  const needle = state.search;
  const frag = document.createDocumentFragment();
  
  rows.forEach((r, idx) => {
    const tr = document.createElement("tr");
    tr.setAttribute("tabindex", "0");
    tr.dataset.index = idx.toString();
    
    const detailEsc = (r.detail || "").replaceAll("\\n","<br>");
    const nameLine = escapeHtml(displayClusterName(r));
    const verLine = escapeHtml(r.clusterVersion || "");
    const nccLine = escapeHtml(r.nccVersion || "");
    const checkTitle = formatCheckTitle(r.check || "");
    const kb = extractKB(r.detail);
    const kbCell = kb ? ('<a href="' + kb + '" target="_blank" rel="noopener">' + kbLabel(kb) + '</a>') : '';
    const clusterUrl = 'https://' + encodeURIComponent(r.cluster) + ':9440';
    const rowText = (r.cluster + " " + r.severity + " " + r.check + " " + (r.detail || "")).trim();
    const actHTML = '<div class="actions">' +
      '<button onclick="copyText(\'' + jsEscape(rowText) + '\')">Copy row</button>' +
      '<button onclick="copyText(\'' + jsEscape(r.detail || "") + '\')">Copy detail</button>' +
      '</div>';
    
    
    const nameCell = '<td class="col-cname">' +
      '<div class="mono" style="color: var(--text); font-weight: 600;">' + highlight(nameLine, needle) + '</div>' +
      '<div class="cluster-meta mono">' +
      (verLine ? ('<div>Version: ' + highlight(verLine, needle) + '</div>') : '') +
      (nccLine ? ('<div>NCC: ' + highlight(nccLine, needle) + '</div>') : '') +
      '</div></td>';
    
    const clusterCell = '<td class="col-cluster">' +
      '<small class="mono has-tooltip" data-fulltext="' + escapeHtml(r.cluster) + '">' +
      '<a href="' + clusterUrl + '" target="_blank" rel="noopener">' + highlight(r.cluster, needle) + '</a>' +
      '</small></td>';
    
    const titleCell = '<td class="col-title">' +
      '<small class="mono has-tooltip" data-fulltext="' + escapeHtml(checkTitle) + '">' +
      highlight(checkTitle, needle) +
      '</small>' +
      (function() {
        var k = rowKeyFor(r.cluster, r.check);
        var d = DIFF_FLAGS[k] || {};
        var tags = [];
        if (d.new_fail) tags.push('<span class="ux-tag tag-new">+new FAIL</span>');
        if (d.resolved_fail) tags.push('<span class="ux-tag tag-resolved">resolved FAIL</span>');
        if (d.severity_changed) tags.push('<span class="ux-tag tag-changed">severity changed</span>');
        if (FLAKY_KEYS[k]) tags.push('<span class="ux-tag tag-flaky">flaky</span>');
        return tags.length ? ('<div class="row-badges">' + tags.join('') + '</div>') : '';
      })() +
      '</td>';
    
    const detailCell = '<td class="col-detail">' +
      '<div class="detail-full has-tooltip" data-fulltext="' + jsEscape(r.detail || "") + '"' +
      ' style="max-height: 60px; overflow-y: auto;">' +
      highlight(detailEsc, needle) +
      '</div></td>';
    
    tr.innerHTML = nameCell + clusterCell +
      '<td class="col-sev"><span class="severity sev-' + r.severity + '">' + r.severity + '</span></td>' +
      titleCell + '<td class="col-kb">' + kbCell + '</td>' + detailCell +
      '<td class="col-actions">' + actHTML + '</td>';
    
    tr.addEventListener("focus", () => selectRow(tr));
    frag.appendChild(tr);
  });
  
  tbody.appendChild(frag);
  setTimeout(initTooltips, 0);
}


let tooltip = null;
let tooltipTimeout = null;

function showTooltip(event, text) {
  
  if (tooltipTimeout) clearTimeout(tooltipTimeout);
  
  if (!text || text.length < 20) return; 
  
  
  if (tooltip) {
    tooltip.remove();
    tooltip = null;
  }
  
  tooltip = document.createElement('div');
  tooltip.className = 'tooltip';
  tooltip.textContent = text;
  document.body.appendChild(tooltip);
  
  
  const rect = event.target.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  
  
  let top = event.clientY - tooltipRect.height - 10;
  let left = event.clientX - tooltipRect.width / 2;
  
  
  if (top < 10) top = event.clientY + 20;
  if (left < 10) left = 10;
  if (left + tooltipRect.width > window.innerWidth - 10) {
    left = window.innerWidth - tooltipRect.width - 10;
  }
  
  tooltip.style.position = 'fixed';
  tooltip.style.left = left + 'px';
  tooltip.style.top = top + 'px';
}

function hideTooltip() {
  if (tooltipTimeout) clearTimeout(tooltipTimeout);
  tooltipTimeout = setTimeout(() => {
    if (tooltip) {
      tooltip.remove();
      tooltip = null;
    }
  }, 50); 
}

function initTooltips() {
  
  document.querySelectorAll('.has-tooltip').forEach(el => {
    el.removeEventListener('mouseenter', el._ttEnter);
    el.removeEventListener('mouseleave', el._ttLeave);
    el.removeEventListener('mousemove', el._ttMove);
  });
  
  
  document.querySelectorAll('.has-tooltip').forEach(el => {
    const text = el.dataset.fulltext || '';
    if (text.length < 20) return; 
    
    el._ttEnter = (e) => showTooltip(e, text);
    el._ttLeave = hideTooltip;
    el._ttMove = (e) => {
      if (tooltip && text.length >= 20) {
        showTooltip(e, text); 
      }
    };
    
    el.addEventListener('mouseenter', el._ttEnter);
    el.addEventListener('mouseleave', el._ttLeave);
    el.addEventListener('mousemove', el._ttMove);
  });
}
	function selectRow(tr) {
	  const tbody = document.getElementById("tbody");
	  Array.from(tbody.querySelectorAll("tr.selected")).forEach(x => x.classList.remove("selected"));
	  tr.classList.add("selected");
	  selIndex = parseInt(tr.dataset.index || "-1", 10);
	}
	
	function focusRow(i) {
	  const rows = document.querySelectorAll("#tbody tr");
	  if (!rows.length) return;
	  if (i < 0) i = 0;
	  if (i >= rows.length) i = rows.length - 1;
	  selIndex = i;
	  const tr = rows[i];
	  tr.focus({preventScroll:false});
	  selectRow(tr);
	  tr.scrollIntoView({block:"nearest", inline:"nearest"});
	}
	
	function onKey(e) {
	  const k = e.key;
	  if (k === "/") {
		e.preventDefault();
		const sb = document.getElementById("searchBox");
		if (sb) { sb.focus(); sb.select(); }
		return;
	  }
	  if (k === "Escape") {
		if (state.showPerClusterModal) {
		  state.showPerClusterModal = false;
		  hidePerClusterModal();
		  return;
		}
		if (state.showClusterModal) {
		  state.showClusterModal = false;
		  hideClusterModal();
		  return;
		}
		if (state.search) {
		  state.search = ""; var sb = document.getElementById("searchBox"); if (sb) sb.value = "";
		  updateAndRender();
		}
		return;
	  }
	  if (k === "ArrowDown") { e.preventDefault(); focusRow(selIndex + 1); return; }
	  if (k === "ArrowUp")   { e.preventDefault(); focusRow(selIndex - 1); return; }
	}
	
	function updateAndRender() {
	  let rows = filterData();
	  var clusterScopedRows = getClusterFilteredRows();
	  renderInsights(clusterScopedRows);
	  
	  const total = rows.length;
	  const cnt = { FAIL:0, WARN:0, ERR:0, INFO:0 };
	  rows.forEach(r => { if (cnt[r.severity] !== undefined) cnt[r.severity]++; });
	  document.getElementById("countTotal").textContent = total;
	  document.getElementById("countFail").textContent = cnt.FAIL;
	  document.getElementById("countWarn").textContent = cnt.WARN;
	  document.getElementById("countErr").textContent  = cnt.ERR;
	  document.getElementById("countInfo").textContent = cnt.INFO;
	  const pct = {};
	  Object.keys(cnt).forEach(k => pct[k] = total ? Math.round(cnt[k]*100/total) : 0);
	  document.getElementById("barFail").style.width = pct.FAIL + "%";
	  document.getElementById("barWarn").style.width = pct.WARN + "%";
	  document.getElementById("barErr").style.width  = pct.ERR  + "%";
	  document.getElementById("barInfo").style.width = pct.INFO + "%";
	
	  
	  updateCounts(rows);
	  renderHealthWidget();
	  rows = sortData(rows.slice());
	  state.filteredRows = rows;
	  var totalRows = rows.length;
	  var totalPages = state.pageSize > 0 ? Math.max(1, Math.ceil(totalRows / state.pageSize)) : 1;
	  if (state.currentPage >= totalPages) state.currentPage = Math.max(0, totalPages - 1);
	  var pageStart = state.currentPage * state.pageSize;
	  var pageEnd = Math.min(pageStart + state.pageSize, totalRows);
	  var pageRows = totalRows > 0 ? rows.slice(pageStart, pageEnd) : [];
	  var infoEl = document.getElementById("tableInfo");
	  var emptyEl = document.getElementById("emptyState");
	  var tableEl = document.getElementById("main-table");
	  var paginationEl = document.getElementById("pagination");
	  if (infoEl) {
	    if (totalRows === 0) infoEl.textContent = "Showing 0 rows";
	    else if (totalRows <= state.pageSize) infoEl.textContent = "Showing " + totalRows + " row" + (totalRows === 1 ? "" : "s");
	    else infoEl.textContent = "Showing " + (pageStart + 1) + "–" + pageEnd + " of " + totalRows + " rows";
	  }
	  if (emptyEl && tableEl) {
	    if (totalRows === 0) { tableEl.style.display = "none"; emptyEl.style.display = "block"; if (paginationEl) paginationEl.style.display = "none"; }
	    else { tableEl.style.display = ""; emptyEl.style.display = "none"; if (paginationEl) paginationEl.style.display = "flex"; }
	  }
	  if (paginationEl && totalRows > 0) {
	    var pageButtons = buildPageButtons(totalPages, state.currentPage);
	    paginationEl.innerHTML =
	      '<div class="pagination-left">' +
	      '<button type="button" onclick="goToPage(0)" ' + (state.currentPage === 0 ? 'disabled' : '') + ' aria-label="First page">First</button>' +
	      '<button type="button" onclick="goToPage(' + (state.currentPage - 1) + ')" ' + (state.currentPage === 0 ? 'disabled' : '') + ' aria-label="Previous page">Prev</button>' +
	      '<span class="pagination-pages">' + pageButtons + '</span>' +
	      '<button type="button" onclick="goToPage(' + (state.currentPage + 1) + ')" ' + (state.currentPage >= totalPages - 1 ? 'disabled' : '') + ' aria-label="Next page">Next</button>' +
	      '<button type="button" onclick="goToPage(' + (totalPages - 1) + ')" ' + (state.currentPage >= totalPages - 1 ? 'disabled' : '') + ' aria-label="Last page">Last</button>' +
	      '<span class="pagination-info">Page ' + (state.currentPage + 1) + ' of ' + totalPages + '</span>' +
	      '</div>' +
	      '<div class="pagination-right">' +
	      '<label class="pagination-jump"><span>Go</span><input id="pageJumpInput" type="number" min="1" max="' + totalPages + '" value="' + (state.currentPage + 1) + '" onkeydown="if(event.key===&#39;Enter&#39;){goToPageInput(this.value,' + totalPages + ')}" aria-label="Go to page number"><button type="button" onclick="goToPageInput(document.getElementById(&#39;pageJumpInput&#39;).value,' + totalPages + ')">Go</button></label>' +
	      '<label class="pagination-size"><span>Rows</span><select id="pageSizeSelect" onchange="setPageSize(parseInt(this.value,10))">' +
	      (state.pageSize === 50 ? '<option value="50" selected>50</option>' : '<option value="50">50</option>') +
	      (state.pageSize === 100 ? '<option value="100" selected>100</option>' : '<option value="100">100</option>') +
	      (state.pageSize === 200 ? '<option value="200" selected>200</option>' : '<option value="200">200</option>') +
	      (state.pageSize === 500 ? '<option value="500" selected>500</option>' : '<option value="500">500</option>') +
	      '</select></label>' +
	      '</div>';
	  }
	  updateSortIndicators();
	  var fc = document.getElementById("footerClusterCount");
	  if (fc) fc.textContent = "Clusters: " + state.filterClusters.size + "/" + state.allClusters.length;
	  renderTable(pageRows);
	}
	function goToPage(p) {
	  state.currentPage = Math.max(0, Math.min(p, Math.ceil(state.filteredRows.length / state.pageSize) - 1));
	  updateAndRender();
	}
	function setPageSize(n) {
	  state.pageSize = n;
	  state.currentPage = 0;
	  updateAndRender();
	}
	function buildPageButtons(totalPages, currentPage) {
	  if (totalPages <= 1) return '<button type="button" class="pagination-page active" aria-current="page">1</button>';
	  var pages = [0];
	  var start = Math.max(1, currentPage - 1);
	  var end = Math.min(totalPages - 2, currentPage + 1);
	  if (start > 1) pages.push("...");
	  for (var i = start; i <= end; i++) pages.push(i);
	  if (end < totalPages - 2) pages.push("...");
	  pages.push(totalPages - 1);
	  var html = "";
	  for (var p = 0; p < pages.length; p++) {
	    var item = pages[p];
	    if (item === "...") {
	      html += '<span class="pagination-ellipsis" aria-hidden="true">...</span>';
	    } else {
	      html += '<button type="button" class="pagination-page ' + (item === currentPage ? 'active' : '') + '" onclick="goToPage(' + item + ')" ' + (item === currentPage ? 'aria-current="page"' : '') + ' aria-label="Page ' + (item + 1) + '">' + (item + 1) + '</button>';
	    }
	  }
	  return html;
	}
	function goToPageInput(raw, totalPages) {
	  var n = parseInt(raw, 10);
	  if (isNaN(n)) return;
	  if (n < 1) n = 1;
	  if (n > totalPages) n = totalPages;
	  goToPage(n - 1);
	}
	
	function downloadCSV() {
		const rows = filterData();
		const headers = ["Cluster","Severity","NCC Alert Title","Detail"];
		const lines = [headers.join(",")];
		rows.forEach(r => {
		  const title = formatCheckTitle(r.check || "");
		  const row = [r.cluster, r.severity, title, r.detail || ""].map(v => {
		    const s = (v ?? "").toString().replaceAll('"','""').replaceAll("\r"," ").replaceAll("\n","\\n");
		    return '"' + s + '"';
		  }).join(",");
		  lines.push(row);
		});
	  const blob = new Blob([lines.join("\n")], {type: "text/csv;charset=utf-8;"});
	  triggerDownload(blob, "aggregated_filtered.csv");
	}
	
	function downloadJSON() {
	  const rows = filterData();
	  const blob = new Blob([JSON.stringify(rows, null, 2)], {type: "application/json;charset=utf-8;"});
	  triggerDownload(blob, "aggregated_filtered.json");
	}
	
	function triggerDownload(blob, name) {
	  const a = document.createElement("a");
	  a.href = URL.createObjectURL(blob);
	  a.download = name;
	  document.body.appendChild(a);
	  a.click();
	  document.body.removeChild(a);
	}
	</script>
	</head>
	<body onload="init()">
	<a href="#main-table" class="skip-link">Skip to table</a>
	<div class="container">
	  <div class="header">
		<div class="title">
		  <h1>NCC Aggregated Report</h1>
		  <div class="sub">Generated at {{.GeneratedAt}}{{if .Clusters}} · {{len .Clusters}} cluster{{if eq (len .Clusters) 1}}{{else}}s{{end}}{{end}}</div>
		</div>
		{{if .Clusters}}
		<div class="header-actions">
		  <button type="button" class="per-cluster-btn" onclick="showPerClusterModal()" aria-label="Open per-cluster report links">Per-cluster reports ({{len .Clusters}})</button>
		</div>
		{{end}}
	  </div>
	
	  <div class="controls">
		<div class="control">
		  <label for="searchBox">Search</label>
		  <input id="searchBox" type="text" placeholder="Type to filter... (tokens: sev:FAIL cluster:10.1 changed:true flaky:true)" oninput="onSearchDebounced(this)" aria-label="Filter rows by search text or tokens" />
		</div>
		<div class="control">
		  <label>Severity</label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'FAIL')"> <span style="color: var(--fail);">FAIL</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'WARN')"> <span style="color: var(--warn);">WARN</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'ERR')"> <span style="color: var(--err);">ERR</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'INFO')"> <span style="color: var(--info);">INFO</span></label>
		</div>
<div class="control">
  <label for="clusterStatus">Clusters</label>
  <div class="cluster-status-wrapper">
    <div id="clusterStatus" class="cluster-status" role="button" tabindex="0" aria-label="Select clusters to filter" aria-haspopup="dialog">All clusters selected</div>
  </div>
</div>
		<div class="control">
		  <label for="compareMode">Compare</label>
		  <select id="compareMode" onchange="setCompareMode(this.value)" aria-label="Comparison mode">
		    <option value="all">All rows</option>
		    <option value="changed">Changed only</option>
		    <option value="flaky">Flaky only</option>
		  </select>
		</div>
		<div class="control">
		  <button type="button" onclick="downloadCSV()" aria-label="Export filtered rows as CSV">Export CSV</button>
		  <button type="button" onclick="downloadJSON()" aria-label="Export filtered rows as JSON">Export JSON</button>
		</div>
		<div class="control" style="min-width: 280px;">
		  <label>Artifacts</label>
		  <div class="per-cluster-links" id="artifactLinks" aria-label="Artifact quick links"></div>
		</div>
	  </div>

	  <div class="summary">
		<div class="sum-item clickable" id="sumTotal" role="button" tabindex="0" onclick="filterBySev(null)" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev(null); }" aria-label="Show all severities">
		  <div class="label">Total</div>
		  <div class="count" id="countTotal">0</div>
		</div>
		<div class="sum-item clickable" id="sumFail" role="button" tabindex="0" onclick="filterBySev('FAIL')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('FAIL'); }" aria-label="Show only FAIL">
		  <div class="label">FAIL</div>
		  <div class="count" id="countFail">0</div>
		  <div class="progress fail"><span id="barFail" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumWarn" role="button" tabindex="0" onclick="filterBySev('WARN')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('WARN'); }" aria-label="Show only WARN">
		  <div class="label">WARN</div>
		  <div class="count" id="countWarn">0</div>
		  <div class="progress warn"><span id="barWarn" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumErr" role="button" tabindex="0" onclick="filterBySev('ERR')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('ERR'); }" aria-label="Show only ERR">
		  <div class="label">ERR</div>
		  <div class="count" id="countErr">0</div>
		  <div class="progress err"><span id="barErr" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumInfo" role="button" tabindex="0" onclick="filterBySev('INFO')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('INFO'); }" aria-label="Show only INFO">
		  <div class="label">INFO</div>
		  <div class="count" id="countInfo">0</div>
		  <div class="progress info"><span id="barInfo" style="width:0%"></span></div>
		</div>
	  </div>

	  <div class="insights">
		<div class="sum-item">
		  <div class="label">Policy Gates</div>
		  <div class="count" id="policyStatus">-</div>
		  <div class="meta" id="policyMeta"></div>
		</div>
		<div class="sum-item">
		  <div class="label">Changed checks</div>
		  <div class="count" id="changedChecksCount">0</div>
		  <div class="meta">Rows with diff markers (+new FAIL, resolved FAIL, or severity change).</div>
		</div>
		<div class="sum-item">
		  <div class="label">Flaky checks</div>
		  <div class="count" id="flakyChecksCount">0</div>
		  <div class="meta">Rows flagged as severity-oscillating in flaky analysis.</div>
		</div>
	  </div>

	  <div class="card" style="margin-bottom: 12px;">
		<div class="label">Cluster health (score + trend)</div>
		<div class="meta">Sparkline is built from recent run-history snapshots when available.</div>
		<div id="healthWidget" style="margin-top:8px;"></div>
	  </div>

	  <div class="table-info" id="tableInfo" aria-live="polite">Showing 0 rows</div>
	  <div id="pagination" class="pagination" style="display:none;"></div>
	  <div class="card">
		<div class="scroll">
		  <table id="main-table" aria-label="NCC aggregated findings table">
			<thead>
			  <tr>
			  	<th class="col-cname th-sort" data-sort="clusterName" onclick="sortBy('clusterName')">Cluster Name <span class="sort-arrow" id="arrow-clusterName"></span></th>
				<th class="col-cluster th-sort" data-sort="cluster" onclick="sortBy('cluster')">Cluster <span class="sort-arrow" id="arrow-cluster"></span></th>
				<th class="col-sev th-sort" data-sort="severity" onclick="sortBy('severity')">Severity <span class="sort-arrow" id="arrow-severity"></span></th>
				<th class="col-title th-sort" data-sort="check" onclick="sortBy('check')">NCC Alert Title <span class="sort-arrow" id="arrow-check"></span></th>
				<th class="col-kb">KB</th>
				<th class="col-detail">Detail</th>
				<th class="col-actions">Actions</th>
			  </tr>
			</thead>
			<tbody id="tbody"></tbody>
		  </table>
		  <div id="emptyState" class="empty-state" style="display:none;">
		    <p>No rows match your filters.</p>
		    <button type="button" onclick="clearFilters()">Clear filters</button>
		  </div>
		</div>
	  </div>
	
     <footer class="report-footer">
    <strong>Keyboard:</strong> / focus search · ↑/↓ move row · Esc clear search or close modal. <span id="footerClusterCount"></span><br>
    <strong>Search tokens:</strong> <code>sev:FAIL</code> <code>cluster:10.1.1.5</code> <code>changed:true</code> <code>flaky:true</code>
    <div class="report-meta-line" id="reportMetaFooter" aria-live="polite"></div>
</footer>
	</div>
	</body>
	</html>`

	// Build data for template with embedded JSON
	type tmplRow struct {
		Cluster        string `json:"cluster"`
		Severity       string `json:"severity"`
		Check          string `json:"check"`
		Detail         string `json:"detail"`
		ClusterName    string `json:"clusterName"`
		ClusterVersion string `json:"clusterVersion"`
		NCCVersion     string `json:"nccVersion"`
	}

	aggRows := make([]tmplRow, 0, len(rows))
	for _, r := range rows {
		aggRows = append(aggRows, tmplRow{
			Cluster:        r.Cluster,
			Severity:       r.Severity,
			Check:          r.Check,
			Detail:         r.Detail,
			ClusterName:    r.ClusterName,
			ClusterVersion: r.ClusterVersion,
			NCCVersion:     r.NCCVersion,
		})
	}

	jsonBytes, err := json.Marshal(aggRows)
	if err != nil {
		return fmt.Errorf("marshal agg json: %w", err)
	}

	clusterLinksJSON, _ := json.Marshal(perCluster)
	if clusterLinksJSON == nil {
		clusterLinksJSON = []byte("[]")
	}

	type diffFlags struct {
		NewFail         bool `json:"new_fail"`
		ResolvedFail    bool `json:"resolved_fail"`
		SeverityChanged bool `json:"severity_changed"`
	}
	rowKey := func(cluster, check string) string {
		return strings.ToLower(strings.TrimSpace(cluster)) + "||" + strings.ToLower(strings.TrimSpace(check))
	}
	diffFlagMap := map[string]diffFlags{}
	diffPath := filepath.Join(outDir, "drilldown-diff.json")
	if b, err := os.ReadFile(diffPath); err == nil {
		var d DrillDownDiffJSON
		if err := json.Unmarshal(b, &d); err == nil {
			for _, c := range d.Clusters {
				for _, name := range c.NewFailures {
					k := rowKey(c.Address, name)
					v := diffFlagMap[k]
					v.NewFail = true
					diffFlagMap[k] = v
				}
				for _, name := range c.ResolvedFailures {
					k := rowKey(c.Address, name)
					v := diffFlagMap[k]
					v.ResolvedFail = true
					diffFlagMap[k] = v
				}
				for _, ch := range c.SeverityChanges {
					k := rowKey(c.Address, ch.CheckName)
					v := diffFlagMap[k]
					v.SeverityChanged = true
					diffFlagMap[k] = v
				}
			}
		}
	}
	diffFlagsJSON, _ := json.Marshal(diffFlagMap)
	if diffFlagsJSON == nil {
		diffFlagsJSON = []byte("{}")
	}

	flakyKeys := map[string]bool{}
	flakyPath := filepath.Join(outDir, "flaky-checks.json")
	if b, err := os.ReadFile(flakyPath); err == nil {
		var f FlakyChecksJSON
		if err := json.Unmarshal(b, &f); err == nil {
			for _, c := range f.Checks {
				flakyKeys[rowKey(c.Cluster, c.CheckName)] = true
			}
		}
	}
	flakyKeysJSON, _ := json.Marshal(flakyKeys)
	if flakyKeysJSON == nil {
		flakyKeysJSON = []byte("{}")
	}

	policyViolations := []string{}
	policyPath := filepath.Join(outDir, "policy-gates.txt")
	if b, err := os.ReadFile(policyPath); err == nil {
		for _, ln := range strings.Split(string(b), "\n") {
			ln = strings.TrimSpace(ln)
			if ln != "" {
				policyViolations = append(policyViolations, ln)
			}
		}
	}
	policyViolationsJSON, _ := json.Marshal(policyViolations)
	if policyViolationsJSON == nil {
		policyViolationsJSON = []byte("[]")
	}

	var runSummary RunSummaryJSON
	if b, err := os.ReadFile(filepath.Join(outDir, "run-summary.json")); err == nil {
		_ = json.Unmarshal(b, &runSummary)
	}
	runSummaryJSON, _ := json.Marshal(runSummary)
	if runSummaryJSON == nil {
		runSummaryJSON = []byte("{}")
	}

	historyDir := strings.TrimSpace(cfg.RunHistoryDir)
	if historyDir == "" {
		historyDir = filepath.Join(outDir, "runs")
	}
	healthTrends := map[string][]int{}
	if dirs, err := listHistoryRunDirs(historyDir); err == nil && len(dirs) > 0 {
		limit := 20
		if len(dirs) > limit {
			dirs = dirs[:limit]
		}
		// oldest -> newest
		for i, j := 0, len(dirs)-1; i < j; i, j = i+1, j-1 {
			dirs[i], dirs[j] = dirs[j], dirs[i]
		}
		for _, d := range dirs {
			p := filepath.Join(historyDir, d, "run-summary.json")
			b, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			var s RunSummaryJSON
			if err := json.Unmarshal(b, &s); err != nil {
				continue
			}
			for _, c := range s.Clusters {
				key := strings.ToLower(strings.TrimSpace(c.Address))
				if key == "" {
					continue
				}
				healthTrends[key] = append(healthTrends[key], c.HealthScore)
			}
		}
	}
	for _, c := range runSummary.Clusters {
		key := strings.ToLower(strings.TrimSpace(c.Address))
		if key == "" {
			continue
		}
		healthTrends[key] = append(healthTrends[key], c.HealthScore)
	}
	healthTrendsJSON, _ := json.Marshal(healthTrends)
	if healthTrendsJSON == nil {
		healthTrendsJSON = []byte("{}")
	}

	artifactLinks := map[string]string{}
	for _, name := range []string{"run-summary.json", "slo-dashboard.json", "drilldown-diff.json", "flaky-checks.json", "checks-snapshot.json", "regression-summary.json"} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err == nil {
			artifactLinks[name] = name
		}
	}
	artifactLinksJSON, _ := json.Marshal(artifactLinks)
	if artifactLinksJSON == nil {
		artifactLinksJSON = []byte("{}")
	}

	reportMeta := map[string]string{
		"version":             Version,
		"build_date":          BuildDate,
		"stream":              Stream,
		"quiet_hours":         strings.TrimSpace(cfg.QuietHours),
		"maintenance_windows": strings.Join(cfg.MaintenanceWindows, ","),
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				reportMeta["git_revision"] = s.Value
				break
			}
		}
	}
	reportMetaJSON, _ := json.Marshal(reportMeta)
	if reportMetaJSON == nil {
		reportMetaJSON = []byte("{}")
	}

	data := struct {
		JSON                 template.JS
		ClusterLinksJSON     template.JS
		DiffFlagsJSON        template.JS
		FlakyKeysJSON        template.JS
		PolicyViolationsJSON template.JS
		RunSummaryJSON       template.JS
		HealthTrendsJSON     template.JS
		ArtifactLinksJSON    template.JS
		ReportMetaJSON       template.JS
		Clusters             []struct{ Cluster, HTML, CSV string }
		GeneratedAt          string
		ClusterName          string
		ClusterVersion       string
		NCCVersion           string
	}{
		JSON:                 template.JS(jsonBytes),
		ClusterLinksJSON:     template.JS(clusterLinksJSON),
		DiffFlagsJSON:        template.JS(diffFlagsJSON),
		FlakyKeysJSON:        template.JS(flakyKeysJSON),
		PolicyViolationsJSON: template.JS(policyViolationsJSON),
		RunSummaryJSON:       template.JS(runSummaryJSON),
		HealthTrendsJSON:     template.JS(healthTrendsJSON),
		ArtifactLinksJSON:    template.JS(artifactLinksJSON),
		ReportMetaJSON:       template.JS(reportMetaJSON),
		Clusters:             perCluster,
		GeneratedAt:          time.Now().Format(time.RFC3339),
	}

	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	t := template.Must(template.New("index").Parse(tmpl))
	if err := t.Execute(f, data); err != nil {
		return fmt.Errorf("template execute %s: %w", path, err)
	}
	log.Info().Str("file", abs).Int("rows", len(rows)).Int("clusters", len(perCluster)).Msg("aggregated HTML generated")
	return nil
}

// ==================== Retryable HTTP Wrappers ====================

func doWithRetry(ctx context.Context, client HTTPClient, req *http.Request, cfg Config, op string) (*http.Response, []byte, error) {
	attempts := cfg.RetryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	breaker := cfg.RetryCircuitBreaker
	if breaker <= 0 {
		breaker = defaultRetryCircuitBreaker
	}
	var lastErr error
	var resp *http.Response
	var body []byte
	consecutiveRetryableFailures := 0

	// Snapshot original body if present
	var origBody []byte
	var hasBody bool
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, nil, err
		}
		_ = req.Body.Close()
		origBody = b
		hasBody = true
		req.Body = io.NopCloser(bytes.NewReader(origBody))
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		reqClone := req.Clone(reqCtx)
		if hasBody {
			reqClone.Body = io.NopCloser(bytes.NewReader(origBody))
		}

		resp, lastErr = client.Do(reqClone)
		if lastErr != nil {
			consecutiveRetryableFailures++
			cancel()
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if consecutiveRetryableFailures >= breaker {
				return nil, nil, fmt.Errorf("%s retry circuit breaker opened after %d consecutive transport failures: %w", op, consecutiveRetryableFailures, lastErr)
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("transport error, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(back):
				}
				continue
			}
			return nil, nil, lastErr
		}

		func() {
			defer cancel()
			defer resp.Body.Close()
			var err error
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				lastErr = err
			} else {
				lastErr = nil
			}
		}()
		if lastErr != nil {
			consecutiveRetryableFailures++
			if consecutiveRetryableFailures >= breaker {
				return resp, nil, fmt.Errorf("%s retry circuit breaker opened after %d consecutive body read failures: %w", op, consecutiveRetryableFailures, lastErr)
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("read body failed, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(back):
				}
				continue
			}
			return resp, nil, lastErr
		}

		status := resp.StatusCode
		if status >= 200 && status < 300 {
			consecutiveRetryableFailures = 0
			noteHTTPStatusForAdaptiveParallelism(status, cfg)
			log.Debug().Str("op", op).Int("status", status).Msg("request succeeded")
			return resp, body, nil
		}
		noteHTTPStatusForAdaptiveParallelism(status, cfg)

		retryable := isRetryableStatus(status)
		var back time.Duration
		if status == 429 {
			logRateLimitHeaders(op, resp)
			if ra, ok := retryAfterDelay(resp); ok {
				back = capRateLimitWait(ra)
				if ra > maxRateLimitBackoff {
					log.Warn().Str("op", op).Dur("requested", ra).Dur("capped", back).Msg("rate limit wait capped")
				}
			}
		}
		if back == 0 {
			back = jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		}

		if retryable && attempt < attempts {
			consecutiveRetryableFailures++
			if consecutiveRetryableFailures >= breaker {
				return resp, body, fmt.Errorf("%s retry circuit breaker opened after %d consecutive retryable failures (last HTTP %d)", op, consecutiveRetryableFailures, status)
			}
			log.Warn().Str("op", op).Int("attempt", attempt).Int("status", status).Dur("backoff", back).Msg("retryable status, retrying")
			select {
			case <-ctx.Done():
				return resp, body, ctx.Err()
			case <-time.After(back):
			}
			continue
		}

		log.Error().Str("op", op).Int("status", status).Int("attempts", attempt).Msg("request failed, not retrying")
		return resp, body, fmt.Errorf("%s HTTP %d", op, status)
	}

	if lastErr != nil {
		return nil, nil, lastErr
	}
	return resp, body, fmt.Errorf("%s exhausted retries", op)
}

// ==================== NCC Client ====================

type NCCClient struct {
	baseURL    string
	cluster    string // host:port or IP for building v4 API URL
	user       string
	pass       string
	http       HTTPClient
	cfg        Config
	apiVersion string // "v1" or "v4"
	// taskExtID is the full Nutanix extId from start-checks v4 (e.g. "base:uuid"); used for GET /api/prism/{ver}/config/tasks/{extId}.
	taskExtID string
}

func NewNCCClient(cluster, user, pass string, httpc HTTPClient, cfg Config) *NCCClient {
	apiVer := strings.ToLower(strings.TrimSpace(cfg.NCCAPIVersion))
	if apiVer == "" {
		apiVer = "v4"
	}
	return &NCCClient{
		baseURL:    fmt.Sprintf("https://%s:9440/PrismGateway/services/rest", cluster),
		cluster:    cluster,
		user:       user,
		pass:       pass,
		http:       httpc,
		cfg:        cfg,
		apiVersion: apiVer,
	}
}

// generateRequestID returns a UUID v4-style string for NTNX-Request-Id header.
func generateRequestID() (string, error) {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// getClusterUUID fetches the cluster UUID from Prism (v1 cluster endpoint or v4 clustermgmt list).
func (c *NCCClient) getClusterUUID(ctx context.Context) (string, error) {
	if c.apiVersion == "v4" {
		uuid, err := c.getClusterUUIDV4(ctx)
		if err != nil && strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("v4 cluster list not available (404), using v1 cluster for uuid")
			return c.getClusterUUIDV1(ctx)
		}
		return uuid, err
	}
	return c.getClusterUUIDV1(ctx)
}

func (c *NCCClient) getClusterUUIDV1(ctx context.Context) (string, error) {
	url := c.baseURL + "/v1/cluster"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)
	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get cluster uuid")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("get cluster uuid failed")
		return "", fmt.Errorf("get cluster uuid: %w", err)
	}
	_ = resp
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", fmt.Errorf("parse cluster response: %w", err)
	}
	uuid, _ := data["uuid"].(string)
	if uuid == "" {
		if id, ok := data["id"].(string); ok && id != "" {
			uuid = id
		}
	}
	if uuid == "" {
		return "", errors.New("cluster response missing uuid")
	}
	return uuid, nil
}

func (c *NCCClient) getClusterUUIDV4(ctx context.Context) (string, error) {
	extID, _, err := c.resolveClusterV4ForNCC(ctx)
	return extID, err
}

// resolveClusterV4ForNCC finds the clustermgmt entity matching c.cluster (IP, name, extId, or CVM IP),
// returns its extId and CVM IPv4s for run-system-defined-checks. Prism Central often registers multiple
// clusters (e.g. AOS + PC); using only data[0] paired with nodeIps={c.cluster} caused NCC-40023 when
// the first cluster was not the one the user addressed.
func (c *NCCClient) resolveClusterV4ForNCC(ctx context.Context) (clusterExtID string, nodeIPv4s []string, err error) {
	ref := strings.TrimSpace(c.cluster)
	if ref == "" {
		return "", nil, errors.New("cluster address is empty")
	}
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	base := fmt.Sprintf("https://%s:9440/api/clustermgmt/%s/config/clusters", c.cluster, ver)
	const pageSize = 100
	var scanned int

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return "", nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return "", nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(c.user, c.pass)
		resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get cluster uuid v4")
		if err != nil {
			log.Error().Err(err).Str("url", u.String()).Msg("get cluster uuid v4 failed")
			return "", nil, fmt.Errorf("get cluster uuid v4: %w", err)
		}
		_ = resp

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return "", nil, fmt.Errorf("parse cluster list response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, entity := range payload.Data {
			scanned++
			if entity == nil {
				continue
			}
			if !clusterEntityMatchesUserRef(ref, entity) {
				continue
			}
			extID := extractClusterExtIDV4(entity)
			if extID == "" {
				return "", nil, errors.New("matched cluster entity missing extId")
			}
			nodeIPs := extractCVMIPv4sFromClusterEntity(entity)
			if len(nodeIPs) == 0 {
				if a := extractClusterAddressV4(entity); a != "" {
					nodeIPs = []string{a}
				}
			}
			return extID, dedupeStringsKeepOrder(nodeIPs), nil
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && scanned >= total {
			break
		}
	}

	return "", nil, fmt.Errorf("no cluster registered in Prism matches %q (use an IP, name, or extId from discover-clusters)", ref)
}

func (c *NCCClient) StartChecks(ctx context.Context) (string, []byte, error) {
	if c.apiVersion == "v4" {
		taskID, body, err := c.startChecksV4(ctx)
		if err != nil && strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("v4 endpoint not available (404), falling back to v1")
			return c.startChecksV1(ctx)
		}
		if err != nil {
			return "", body, err
		}
		return taskID, body, nil
	}
	return c.startChecksV1(ctx)
}

func (c *NCCClient) startChecksV1(ctx context.Context) (string, []byte, error) {
	url := c.baseURL + "/v1/ncc/checks"
	payload := []byte(`{"sendEmail":false}`)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "start checks")
	if err != nil {
		log.Error().Err(err).Str("url", url).Str("method", "POST").Msg("http do error")
		return "", body, fmt.Errorf("start checks: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("start checks response")

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", body, err
	}
	uuid, _ := data["taskUuid"].(string)
	if uuid == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			uuid = alt
		}
	}
	if uuid == "" {
		return "", body, errors.New("missing taskUuid in response")
	}
	return uuid, body, nil
}

// v4RunChecksRequest is the body for monitoring v4 run-system-defined-checks.
type v4RunChecksRequest struct {
	ShouldAnonymize                        bool       `json:"shouldAnonymize"`
	ShouldSendReportToConfiguredRecipients bool       `json:"shouldSendReportToConfiguredRecipients"`
	AdditionalRecipients                   []string   `json:"additionalRecipients"`
	NodeIps                                []v4NodeIP `json:"nodeIps"`
	ShouldRunAllChecks                     bool       `json:"shouldRunAllChecks"`
}

type v4NodeIP struct {
	Value        string `json:"value"`
	PrefixLength int    `json:"prefixLength"`
}

func (c *NCCClient) startChecksV4(ctx context.Context) (string, []byte, error) {
	clusterUUID, nodeIPs, err := c.resolveClusterV4ForNCC(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("get cluster uuid for v4: %w", err)
	}
	if len(nodeIPs) == 0 {
		nodeIPs = []string{c.cluster}
	}
	var nodeIpsReq []v4NodeIP
	for _, ip := range nodeIPs {
		nodeIpsReq = append(nodeIpsReq, v4NodeIP{Value: ip, PrefixLength: 32})
	}
	requestID, err := generateRequestID()
	if err != nil {
		return "", nil, fmt.Errorf("generate request id: %w", err)
	}
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	v4Base := fmt.Sprintf("https://%s:9440/api/monitoring/%s/serviceability/clusters/%s/$actions/run-system-defined-checks", c.cluster, ver, clusterUUID)
	// Prism Central v4 requires additionalRecipients to have at least one element matching email regex (user@domain.tld); use placeholder when not sending report
	additionalRecipients := []string{"noreply@example.com"}
	body := v4RunChecksRequest{
		ShouldAnonymize:                        false,
		ShouldSendReportToConfiguredRecipients: false,
		AdditionalRecipients:                   additionalRecipients,
		NodeIps:                                nodeIpsReq,
		ShouldRunAllChecks:                     true,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", v4Base, bytes.NewReader(payload))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("NTNX-Request-Id", requestID)
	req.SetBasicAuth(c.user, c.pass)

	resp, respBody, err := doWithRetry(ctx, c.http, req, c.cfg, "start checks v4")
	if err != nil {
		log.Error().Err(err).Str("url", v4Base).Str("method", "POST").Msg("http do error")
		return "", respBody, fmt.Errorf("start checks v4: %w", err)
	}
	_ = resp
	log.Debug().Str("url", v4Base).RawJSON("body", respBody).Msg("start checks v4 response")

	var data map[string]interface{}
	if err := json.Unmarshal(respBody, &data); err != nil {
		return "", respBody, err
	}
	c.taskExtID = ""
	var taskID string
	if dataObj, _ := data["data"].(map[string]interface{}); dataObj != nil {
		if extID, _ := dataObj["extId"].(string); extID != "" {
			c.taskExtID = extID
			if idx := strings.LastIndex(extID, ":"); idx >= 0 && idx+1 < len(extID) {
				taskID = extID[idx+1:]
			} else {
				taskID = extID
			}
		}
	}
	if taskID == "" {
		taskID, _ = data["taskUuid"].(string)
	}
	if taskID == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			taskID = alt
		}
	}
	if taskID == "" {
		if extID, ok := data["executionId"].(string); ok && extID != "" {
			taskID = extID
		}
	}
	if taskID == "" {
		return "", respBody, errors.New("v4 response missing taskUuid or executionId")
	}
	return taskID, respBody, nil
}

// mapPrismTaskJSONToTaskStatus maps Prism v4 GET .../api/prism/{ver}/config/tasks/{extId} JSON to legacy TaskStatus for the poll loop.
func mapPrismTaskJSONToTaskStatus(raw []byte) (TaskStatus, error) {
	var wrap struct {
		Data struct {
			Status             string `json:"status"`
			ProgressPercentage int    `json:"progressPercentage"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return TaskStatus{}, err
	}
	st := strings.ToUpper(strings.TrimSpace(wrap.Data.Status))
	pct := wrap.Data.ProgressPercentage
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	var ps string
	switch st {
	case "SUCCEEDED", "SUCCESS", "COMPLETED", "DONE", "COMPLETE", "SUCCEED":
		ps = "Succeeded"
		if pct < 100 {
			pct = 100
		}
	case "FAILED", "FAILURE", "ABORTED", "CANCELED", "CANCELLED", "ERROR":
		ps = "Failed"
	default:
		ps = "Running"
	}
	return TaskStatus{PercentageComplete: pct, ProgressStatus: ps}, nil
}

func (c *NCCClient) getTaskPrismV4(ctx context.Context, extID string) (TaskStatus, []byte, error) {
	ver := nutanixV4PathSegment(c.cfg.NutanixV4APIVersion)
	u := fmt.Sprintf("https://%s:9440/api/prism/%s/config/tasks/%s", c.cluster, ver, url.PathEscape(extID))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return TaskStatus{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get task prism v4")
	if err != nil {
		log.Error().Err(err).Str("url", u).Msg("http do error")
		return TaskStatus{}, body, fmt.Errorf("get task prism v4: %w", err)
	}
	_ = resp
	log.Debug().Str("url", u).RawJSON("body", body).Msg("get task prism v4 response")

	st, err := mapPrismTaskJSONToTaskStatus(body)
	if err != nil {
		return TaskStatus{}, body, fmt.Errorf("parse prism task response: %w", err)
	}
	return st, body, nil
}

func (c *NCCClient) getTaskV2Legacy(ctx context.Context, taskID string) (TaskStatus, []byte, error) {
	url := c.baseURL + "/v2.0/tasks/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return TaskStatus{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get task")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("http do error")
		return TaskStatus{}, body, fmt.Errorf("get task: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("get task response")

	var status TaskStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return TaskStatus{}, body, err
	}
	return status, body, nil
}

func (c *NCCClient) GetTask(ctx context.Context, taskID string) (TaskStatus, []byte, error) {
	if c.apiVersion == "v4" && c.taskExtID != "" {
		st, body, err := c.getTaskPrismV4(ctx, c.taskExtID)
		if err == nil {
			return st, body, nil
		}
		if strings.Contains(err.Error(), "404") {
			log.Info().Str("cluster", c.cluster).Msg("prism v4 task API unavailable (404), falling back to Prism Gateway v2.0/tasks")
			return c.getTaskV2Legacy(ctx, taskID)
		}
		return TaskStatus{}, body, err
	}
	return c.getTaskV2Legacy(ctx, taskID)
}

func (c *NCCClient) GetRunSummary(ctx context.Context, taskID string) (NCCSummary, []byte, error) {
	url := c.baseURL + "/v1/ncc/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return NCCSummary{}, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := doWithRetry(ctx, c.http, req, c.cfg, "get summary")
	if err != nil {
		log.Error().Err(err).Str("url", url).Msg("http do error")
		return NCCSummary{}, body, fmt.Errorf("get summary: %w", err)
	}
	_ = resp
	log.Debug().Str("url", url).RawJSON("body", body).Msg("get summary response")

	var summary NCCSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return NCCSummary{}, body, err
	}
	return summary, body, nil
}

// ==================== Orchestration ====================

func sanitizeSummary(s string) string {
	return strings.ReplaceAll(s, "\\n", "\n")
}

func writeSummary(fs FS, folder, cluster, summary string) (string, error) {
	if err := fs.MkdirAll(folder, 0755); err != nil {
		return "", err
	}
	outPath := filepath.Join(folder, fmt.Sprintf("%s.log", cluster))
	log.Debug().Str("path", outPath).Int("bytes", len(summary)).Msg("writing summary")
	if err := fs.WriteFile(outPath, []byte(sanitizeSummary(summary)), 0644); err != nil {
		return "", err
	}
	return outPath, nil
}

func filterBlocksToFile(fs FS, inputPath, outputPath string) ([]ParsedBlock, error) {
	data, err := fs.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	log.Debug().Str("path", inputPath).Int("bytes", len(data)).Msg("read raw log")
	blocks, err := ParseSummary(string(data))
	if err != nil {
		return nil, err
	}
	if err := validateParsedAlertsAgainstPluginResults(string(data), blocks); err != nil {
		log.Warn().Err(err).Str("path", inputPath).Msg("parser validation: mismatch between parsed alerts and NCC plugin results")
	}
	if err := fs.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, err
	}
	var b strings.Builder
	for _, pb := range blocks {
		b.WriteString(pb.CheckName)
		b.WriteString("\n")
		b.WriteString(pb.DetailRaw)
		b.WriteString("\n\n---------------------------------------\n")
	}
	if err := fs.WriteFile(outputPath, []byte(b.String()), 0644); err != nil {
		return nil, err
	}
	log.Debug().Str("path", outputPath).Int("bytes", len(b.String())).Msg("wrote filtered")
	return blocks, nil
}

func runClusterWithBars(
	ctx context.Context,
	cfg Config,
	fs FS,
	httpc HTTPClient,
	cluster string,
	username string,
	password string,
	onPct func(int),
	setPhase func(string),
) (ClusterRunOutput, error) {
	l := log.With().Str("cluster", cluster).Logger()
	client := NewNCCClient(cluster, username, password, httpc, cfg)
	var clusterStart = time.Now()
	setPhase("starting")
	l.Info().Msg("starting NCC checks")
	taskID, body, err := client.StartChecks(ctx)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("start checks failed")
		return ClusterRunOutput{}, fmt.Errorf("start checks failed: %w", err)
	}
	l.Info().Str("taskID", taskID).Msg("ncc task started")
	onPct(1)

	last := 1
	setPhase("polling")
	for {
		select {
		case <-ctx.Done():
			l.Warn().Err(ctx.Err()).Msg("context cancelled during polling, stopping gracefully")
			return ClusterRunOutput{}, fmt.Errorf("operation cancelled: %w", ctx.Err())
		case <-time.After(cfg.PollInterval + time.Duration(rand.Int63n(int64(cfg.PollJitter)))):
			if dl, ok := ctx.Deadline(); ok {
				rem := time.Until(dl)
				if rem < 10*time.Second {
					l.Warn().Dur("remaining", rem).Msg("cluster deadline near")
				}
			}
			status, body, err := client.GetTask(ctx, taskID)
			if err != nil {
				l.Error().Err(err).RawJSON("response_body", body).Msg("poll failed")
				return ClusterRunOutput{}, fmt.Errorf("poll failed: %w", err)
			}
			pct := status.PercentageComplete
			if pct < last {
				pct = last
			}
			if pct > 100 {
				pct = 100
			}
			onPct(pct)
			l.Debug().Int("pct", pct).Str("progress", status.ProgressStatus).Msg("task status")
			last = pct

			if status.ProgressStatus == "Failed" {
				return ClusterRunOutput{}, fmt.Errorf("ncc task failed")
			}
			if pct >= 100 {
				goto SUMMARY
			}
		}
	}

SUMMARY:
	setPhase("summary")
	summary, body, err := client.GetRunSummary(ctx, taskID)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("get summary failed")
		return ClusterRunOutput{}, fmt.Errorf("get summary failed: %w", err)
	}

	setPhase("writing")
	logPath, err := writeSummary(fs, cfg.OutputDirLogs, cluster, summary.RunSummary)
	if err != nil {
		l.Error().Err(err).Msg("write summary failed")
		return ClusterRunOutput{}, err
	}
	l.Info().Str("logPath", logPath).Msg("summary written")

	// Parse blocks and write filtered file in one operation to avoid duplicate parsing
	filteredPath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
	blocks, err := filterBlocksToFile(fs, logPath, filteredPath)
	if err != nil {
		l.Error().Err(err).Msg("filter blocks failed")
		return ClusterRunOutput{}, err
	}
	l.Info().Str("filteredPath", filteredPath).Msg("filtered written")

	// Apply severity filtering if configured
	if len(cfg.SeverityFilter) > 0 {
		originalCount := len(blocks)
		blocks = filterBlocksBySeverity(blocks, cfg.SeverityFilter)
		l.Info().Int("original", originalCount).Int("filtered", len(blocks)).Strs("severities", cfg.SeverityFilter).Msg("applied severity filter")
	}
	excludedByTitle := make([]ExcludedAlert, 0)
	if len(cfg.ExcludeAlertTitles) > 0 {
		originalCount := len(blocks)
		filteredBlocks, excludedAlerts, err := filterBlocksByTitle(blocks, cfg.ExcludeAlertTitles, cfg.ExcludeAlertMatchMode)
		if err != nil {
			return ClusterRunOutput{}, err
		}
		blocks = filteredBlocks
		excludedByTitle = excludedAlerts
		l.Info().
			Int("original", originalCount).
			Int("filtered", len(blocks)).
			Int("excluded", len(excludedByTitle)).
			Str("mode", cfg.ExcludeAlertMatchMode).
			Strs("titles", cfg.ExcludeAlertTitles).
			Msg("applied alert title exclusion filter")
	}

	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		counts[sev]++
	}
	// Write Prometheus file and output formats (HTML/CSV/JSON) before notifications so we can attach HTML
	if err := writePrometheusFile(fs, cfg.PromDir, cluster, blocks); err != nil {
		l.Error().Err(err).Msg("write Prometheus .prom failed")
	} else {
		log.Info().Str("cluster", cluster).Str("prom_dir", cfg.PromDir).Msg("Prometheus .prom written")
	}

	var htmlPathForNotify string
	base := filteredPath
	for _, f := range cfg.OutputFormats {
		switch strings.ToLower(strings.TrimSpace(f)) {
		case "html":
			htmlFile := base + ".html"
			rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
			meta, _ := parseNCCHeader(rawPath) // ignore error if file missing
			rows := rowsFromBlocks(blocks)
			if err := generateHTML(fs, rows, htmlFile, meta); err != nil {
				l.Error().Err(err).Str("file", htmlFile).Msg("write HTML failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", htmlFile).Msg("HTML generated")
			htmlPathForNotify = htmlFile
		case "csv":
			csvFile := base + ".csv"
			if err := generateCSV(fs, blocks, csvFile); err != nil {
				l.Error().Err(err).Str("file", csvFile).Msg("write CSV failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", csvFile).Msg("CSV generated")
		case "json":
			jsonFile := base + ".json"
			rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
			meta, _ := parseNCCHeader(rawPath) // ignore error if file missing
			if err := generateJSON(fs, blocks, jsonFile, meta); err != nil {
				l.Error().Err(err).Str("file", jsonFile).Msg("write JSON failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", jsonFile).Msg("JSON generated")
		case "markdown":
			mdFile := base + ".md"
			rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
			meta, _ := parseNCCHeader(rawPath)
			if err := generateMarkdown(fs, blocks, mdFile, meta); err != nil {
				l.Error().Err(err).Str("file", mdFile).Msg("write Markdown failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", mdFile).Msg("Markdown generated")
		case "sarif":
			sarifFile := base + ".sarif"
			if err := generateSARIF(fs, blocks, sarifFile); err != nil {
				l.Error().Err(err).Str("file", sarifFile).Msg("write SARIF failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", sarifFile).Msg("SARIF generated")
		default:
			l.Warn().Str("format", f).Msg("unknown output format")
		}
	}

	if len(blocks) == 0 {
		l.Warn().Str("path", filteredPath).Msg("no blocks parsed from summary")
	}

	// Build notification summary with brief overview; optionally include HTML in webhook
	overview := fmt.Sprintf("NCC run completed for cluster %s. FAIL: %d, WARN: %d, ERR: %d, INFO: %d. Total: %d checks.",
		cluster, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], len(blocks))
	summaryNotify := NotificationSummary{
		Cluster:     cluster,
		StartedAt:   clusterStart,
		FinishedAt:  time.Now(),
		FailCount:   counts["FAIL"],
		WarnCount:   counts["WARN"],
		ErrCount:    counts["ERR"],
		InfoCount:   counts["INFO"],
		TotalChecks: len(blocks),
		OutputFiles: []string{filteredPath},
		Overview:    overview,
	}
	if cfg.WebhookIncludeHTML && htmlPathForNotify != "" {
		if b, err := os.ReadFile(htmlPathForNotify); err == nil {
			summaryNotify.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
		}
	}

	subj := fmt.Sprintf("NCC %s: FAIL=%d WARN=%d", summaryNotify.Cluster,
		summaryNotify.FailCount, summaryNotify.WarnCount)
	bodyEmail := overview + "\n\n" + fmt.Sprintf("FAIL: %d | WARN: %d | Total: %d\nFiltered: %s",
		summaryNotify.FailCount, summaryNotify.WarnCount, len(blocks), filteredPath)

	if !cfg.NotifyDigest {
		if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
			l.Info().Str("reason", reason).Msg("notifications suppressed by quiet-hours/maintenance-window")
			setPhase("done")
			return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle}, nil
		}
		if cfg.NotifyOnRegression {
			prevFail := cfg.PreviousClusterFailCount[cluster]
			if summaryNotify.FailCount <= prevFail {
				l.Info().
					Int("current_fail", summaryNotify.FailCount).
					Int("previous_fail", prevFail).
					Msg("notify-on-regression enabled: skipping non-regression cluster notification")
				setPhase("done")
				return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle}, nil
			}
		}
		if err := sendEmailWithRetry(cfg, subj, bodyEmail, htmlPathForNotify); err != nil {
			l.Error().Err(err).Msg("email failed")
		}
		if err := sendWebhookWithRetry(ctx, httpc, cfg, summaryNotify); err != nil {
			l.Error().Err(err).Msg("webhook failed")
		}
		if err := sendSlackWithRetry(ctx, httpc, cfg, summaryNotify); err != nil {
			l.Error().Err(err).Msg("slack notification failed")
		}
		l.Info().Int("fail", summaryNotify.FailCount).Int("warn", summaryNotify.WarnCount).Msg("notifications sent")
	}

	setPhase("done")
	if len(excludedByTitle) > 0 {
		l.Info().Int("excluded_alerts", len(excludedByTitle)).Msg("alerts excluded by title filters")
	}
	return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle}, nil
}

// ==================== CLI ====================

type ClusterResult struct {
	Cluster         string
	Blocks          []ParsedBlock
	ExcludedByTitle []ExcludedAlert
	Err             error
	ErrorClass      string
}

type ClusterRunOutput struct {
	Blocks          []ParsedBlock
	ExcludedByTitle []ExcludedAlert
}

type proxyDecorator struct{ text string }

func (p *proxyDecorator) Decor(ctx decor.Statistics) string { return p.text }
func (p *proxyDecorator) Sync() (chan int, bool)            { return nil, false }
func (p *proxyDecorator) GetConf() decor.WC                 { return decor.WC{} }
func (p *proxyDecorator) SetConf(wc decor.WC)               {}
func (p *proxyDecorator) SetText(s string)                  { p.text = s }

func promptPasswordIfEmpty(p string, Username string) (string, error) {
	if p != "" {
		return p, nil
	}
	fmt.Printf("Prism Password (%s): ", Username)
	bytePw, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytePw)), nil
}

// githubRepo is used by --update to fetch latest release (format: owner/repo).
const githubRepo = "lTSPV75BRO/Nutanix-ncc-orchestrator"

// stripGoBuildGitSuffix removes a trailing -<hex> from ldflags/git injection (e.g. 1.0.0-deadbeef...).
// Does not strip semver prereleases like 1.0.0-rc1 (suffix is not all hex).
func stripGoBuildGitSuffix(s string) string {
	i := strings.LastIndex(s, "-")
	if i <= 0 {
		return s
	}
	tail := s[i+1:]
	if len(tail) < 7 || len(tail) > 64 {
		return s
	}
	for _, c := range tail {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			continue
		}
		return s
	}
	return s[:i]
}

// versionLess returns true if a is semantically less than b (e.g. "0.9.0" < "1.0.0").
// Uses semver when both strings parse; otherwise falls back to simple numeric parsing.
func versionLess(a, b string) bool {
	a = strings.TrimPrefix(strings.TrimSpace(a), "v")
	b = strings.TrimPrefix(strings.TrimSpace(b), "v")
	if va, ea := semver.NewVersion(a); ea == nil {
		if vb, eb := semver.NewVersion(b); eb == nil {
			return va.LessThan(vb)
		}
	}
	a2 := stripGoBuildGitSuffix(a)
	b2 := stripGoBuildGitSuffix(b)
	if va, ea := semver.NewVersion(a2); ea == nil {
		if vb, eb := semver.NewVersion(b2); eb == nil {
			return va.LessThan(vb)
		}
	}
	return versionLessLegacy(a2, b2)
}

func versionLessLegacy(a, b string) bool {
	parse := func(s string) (major, minor, patch int) {
		parts := strings.SplitN(s, ".", 4)
		if len(parts) >= 1 {
			major, _ = strconv.Atoi(parts[0])
		}
		if len(parts) >= 2 {
			minor, _ = strconv.Atoi(parts[1])
		}
		if len(parts) >= 3 {
			patch, _ = strconv.Atoi(strings.SplitN(parts[2], "-", 2)[0])
		}
		return major, minor, patch
	}
	ma, mia, pa := parse(a)
	mb, mib, pb := parse(b)
	if ma != mb {
		return ma < mb
	}
	if mia != mib {
		return mia < mib
	}
	return pa < pb
}

var (
	Version   string
	BuildDate string
	GoVersion string
	Stream    string // e.g., "prod", "dev", "beta"
)

func init() {
	// Defaults
	if Version == "" {
		Version = "2.0.0"
	}
	if BuildDate == "" {
		BuildDate = "unknown"
	}
	if Stream == "" {
		Stream = "dev"
	}

	// Optional build info enrichment
	if bi, ok := debug.ReadBuildInfo(); ok {
		if GoVersion == "" {
			GoVersion = bi.GoVersion
		}

		var gitRevision string
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				gitRevision = s.Value
				break
			}
		}
		if gitRevision != "" {
			Version = Version + "-" + gitRevision
		}
	}

	if GoVersion == "" {
		GoVersion = "unknown"
	}
}

// githubRelease represents the minimal GitHub releases/latest API response.
type githubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []githubAsset `json:"assets"`
	HTMLURL string        `json:"html_url"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// runUpdate fetches the latest release from GitHub, downloads the binary for the current OS/arch if available, and replaces the running executable.
func runUpdate() error {
	apiURL := "https://api.github.com/repos/" + githubRepo + "/releases/latest"
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch latest release: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		if remaining := resp.Header.Get("X-RateLimit-Remaining"); remaining == "0" {
			fmt.Fprintln(os.Stderr, "GitHub API rate limited. Set GITHUB_TOKEN for higher limits or try again later.")
		}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, string(body))
	}

	var rel githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return fmt.Errorf("parse release: %w", err)
	}

	// Normalize tag (e.g. v0.1.12 -> 0.1.12) for display
	latestVer := strings.TrimPrefix(rel.TagName, "v")
	fmt.Fprintf(os.Stderr, "Latest release: %s\n", rel.TagName)

	// Compare with current version (strip optional git revision suffix from ldflags) using semver
	currentVer := stripGoBuildGitSuffix(Version)
	if !versionLess(currentVer, latestVer) {
		if versionLess(latestVer, currentVer) {
			fmt.Fprintln(os.Stderr, "You have a newer version than the latest release (dev build).")
		} else {
			fmt.Fprintln(os.Stderr, "You are already on the latest version.")
		}
		return nil
	}

	goos := runtime.GOOS
	goarch := runtime.GOARCH
	// Match asset names like ncc-orchestrator-linux-amd64 or ncc-orchestrator_0.1.12_linux_amd64 (raw binary only for self-replace)
	wantOS := goos
	wantArch := goarch

	var downloadURL string
	var chosenAssetName string
	for _, a := range rel.Assets {
		name := strings.ToLower(a.Name)
		if strings.Contains(name, wantOS) && strings.Contains(name, wantArch) {
			// Prefer raw binary (no archive) so we can replace self
			if strings.HasSuffix(name, ".tar.gz") || strings.HasSuffix(name, ".zip") {
				if downloadURL == "" {
					downloadURL = a.BrowserDownloadURL
					chosenAssetName = a.Name
				}
				continue
			}
			downloadURL = a.BrowserDownloadURL
			chosenAssetName = a.Name
			break
		}
	}
	if downloadURL == "" {
		// No raw binary; use archive if present so we can at least point to it
		for _, a := range rel.Assets {
			name := strings.ToLower(a.Name)
			if strings.Contains(name, wantOS) && strings.Contains(name, wantArch) {
				downloadURL = a.BrowserDownloadURL
				chosenAssetName = a.Name
				break
			}
		}
	}

	if downloadURL == "" {
		fmt.Fprintf(os.Stderr, "No binary found for %s/%s. Download manually: %s\n", goos, goarch, rel.HTMLURL)
		return nil
	}

	// If only an archive is available, we cannot replace self; tell user to download and extract
	if strings.HasSuffix(strings.ToLower(downloadURL), ".tar.gz") || strings.HasSuffix(strings.ToLower(downloadURL), ".zip") {
		fmt.Fprintf(os.Stderr, "Binary for %s/%s is only available as archive. Download and extract: %s\n", goos, goarch, downloadURL)
		return nil
	}

	// Download
	fmt.Fprintf(os.Stderr, "Downloading %s ...\n", downloadURL)
	dlReq, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("create download request: %w", err)
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && (strings.Contains(downloadURL, "github.com") || strings.Contains(downloadURL, "githubusercontent.com")) {
		dlReq.Header.Set("Authorization", "Bearer "+token)
	}
	dlResp, err := client.Do(dlReq)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer dlResp.Body.Close()
	if dlResp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned %d", dlResp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(dlResp.Body, 200*1024*1024)) // 200 MiB cap
	if err != nil {
		return fmt.Errorf("read download: %w", err)
	}

	// Optional checksum verification: look for checksums.txt or *.sha256 asset
	if chosenAssetName != "" {
		for _, a := range rel.Assets {
			an := strings.ToLower(a.Name)
			if strings.Contains(an, "checksum") || strings.Contains(an, "sha256") || strings.HasSuffix(an, ".sha256") {
				csBody, err := fetchURL(a.BrowserDownloadURL, client)
				if err != nil {
					log.Debug().Err(err).Str("asset", a.Name).Msg("skip checksum verification")
					break
				}
				expectedHash := parseChecksumFile(csBody, chosenAssetName)
				if expectedHash != "" {
					sum := sha256.Sum256(body)
					got := hex.EncodeToString(sum[:])
					if !strings.EqualFold(got, expectedHash) {
						return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedHash, got)
					}
					fmt.Fprintln(os.Stderr, "Checksum verified.")
				}
				break
			}
		}
	}

	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}
	dir := filepath.Dir(selfPath)

	// On Windows, overwriting the running exe often fails; write to .new.exe and instruct user
	if runtime.GOOS == "windows" {
		newPath := selfPath + ".new.exe"
		if err := os.WriteFile(newPath, body, 0755); err != nil {
			return fmt.Errorf("write %s: %w", newPath, err)
		}
		fmt.Fprintf(os.Stderr, "Update saved as %s. Exit this program, then replace %s with it and run again.\n", newPath, selfPath)
		return nil
	}

	tmpPath := filepath.Join(dir, ".ncc-orchestrator-update."+strconv.Itoa(os.Getpid()))
	if err := os.WriteFile(tmpPath, body, 0755); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := os.Rename(tmpPath, selfPath); err != nil {
		_ = os.Remove(tmpPath)
		// Fallback: write to cwd so user can replace manually
		fallback := filepath.Join(".", "ncc-orchestrator-"+latestVer)
		if wErr := os.WriteFile(fallback, body, 0755); wErr != nil {
			return fmt.Errorf("replace binary failed (%v); write to %s failed: %w", err, fallback, wErr)
		}
		fmt.Fprintf(os.Stderr, "Could not replace running binary. New binary saved as %s — move it to replace the current one.\n", fallback)
		return nil
	}

	fmt.Fprintln(os.Stderr, "Update complete. Run the binary again to use the new version.")
	return nil
}

func fetchURL(url string, client *http.Client) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && strings.Contains(url, "github") {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
}

// parseChecksumFile finds a line in checksum body that matches filename (or *filename) and returns the hex hash.
func parseChecksumFile(body []byte, filename string) string {
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Format: "hash  filename" or "hash *filename"
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		hashStr := fields[0]
		name := fields[1]
		if strings.TrimPrefix(name, "*") == filename || name == filename {
			hashStr = strings.TrimPrefix(hashStr, "0x")
			if len(hashStr) == 64 {
				return hashStr
			}
			return ""
		}
	}
	return ""
}

// pcListClustersResponse represents a minimal Prism Central v3 clusters/list response.
type pcListClustersResponse struct {
	Entities []map[string]interface{} `json:"entities"`
}

// errDiscoverV4Unavailable signals that the v4 clustermgmt API is not available (e.g. HTTP 404).
type errDiscoverV4Unavailable struct {
	status int
	body   string
}

func (e errDiscoverV4Unavailable) Error() string {
	return fmt.Sprintf("Prism Central clustermgmt v4 returned HTTP %d", e.status)
}

func discoverHTTPClient(insecure bool) *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}
}

// fetchPCClustersV3 lists clusters via legacy POST /api/nutanix/v3/clusters/list.
func fetchPCClustersV3(pcURL, username, password string, insecure bool) ([]string, error) {
	u := pcURL + "/api/nutanix/v3/clusters/list"
	body := []byte(`{}`)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := discoverHTTPClient(insecure)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Prism Central: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
	}

	var list pcListClustersResponse
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var addresses []string
	for _, e := range list.Entities {
		addr := extractClusterAddressV3(e)
		if addr != "" {
			addresses = append(addresses, addr)
		}
	}
	return addresses, nil
}

// fetchPCClustersV4 lists clusters via GET /api/clustermgmt/{ver}/config/clusters with $page / $limit pagination.
func fetchPCClustersV4(pcURL, username, password string, insecure bool, v4APIVer string) ([]string, error) {
	ver := nutanixV4PathSegment(v4APIVer)
	base := strings.TrimSuffix(pcURL, "/") + "/api/clustermgmt/" + ver + "/config/clusters"
	client := discoverHTTPClient(insecure)
	const pageSize = 100
	seen := make(map[string]bool)
	var out []string

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request to Prism Central: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, errDiscoverV4Unavailable{status: resp.StatusCode, body: string(data)}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
		}

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, e := range payload.Data {
			addr := extractClusterAddressV4(e)
			if addr != "" && !seen[addr] {
				seen[addr] = true
				out = append(out, addr)
			}
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && len(out) >= total {
			break
		}
		// Next page until empty; do not stop on len(data) < $limit (server may cap below our limit).
	}
	return out, nil
}

// DiscoverClusterRow is one registered cluster from discover-clusters (table/json output).
type DiscoverClusterRow struct {
	Name    string `json:"name"`
	ExtID   string `json:"ext_id"`
	Address string `json:"address"`
	API     string `json:"api"`
}

func discoverRowFromV4Entity(e map[string]interface{}) DiscoverClusterRow {
	if e == nil {
		return DiscoverClusterRow{}
	}
	name, _ := e["name"].(string)
	return DiscoverClusterRow{
		Name:    strings.TrimSpace(name),
		ExtID:   extractClusterExtIDV4(e),
		Address: extractClusterAddressV4(e),
		API:     "v4",
	}
}

func extractClusterExtIDV3(e map[string]interface{}) string {
	if m, _ := e["metadata"].(map[string]interface{}); m != nil {
		if u, _ := m["uuid"].(string); strings.TrimSpace(u) != "" {
			return strings.TrimSpace(u)
		}
	}
	return ""
}

func discoverRowFromV3Entity(e map[string]interface{}) DiscoverClusterRow {
	if e == nil {
		return DiscoverClusterRow{}
	}
	name := ""
	if m, _ := e["metadata"].(map[string]interface{}); m != nil {
		if n, _ := m["name"].(string); strings.TrimSpace(n) != "" {
			name = strings.TrimSpace(n)
		}
	}
	if name == "" {
		if spec, _ := e["spec"].(map[string]interface{}); spec != nil {
			if n, _ := spec["name"].(string); strings.TrimSpace(n) != "" {
				name = strings.TrimSpace(n)
			}
		}
	}
	return DiscoverClusterRow{
		Name:    name,
		ExtID:   extractClusterExtIDV3(e),
		Address: extractClusterAddressV3(e),
		API:     "v3",
	}
}

// fetchDiscoverClusterRowsV4 returns cluster rows from GET clustermgmt v4 (paginated).
func fetchDiscoverClusterRowsV4(pcURL, username, password string, insecure bool, v4APIVer string) ([]DiscoverClusterRow, error) {
	ver := nutanixV4PathSegment(v4APIVer)
	base := strings.TrimSuffix(pcURL, "/") + "/api/clustermgmt/" + ver + "/config/clusters"
	client := discoverHTTPClient(insecure)
	const pageSize = 100
	seen := make(map[string]bool)
	var out []DiscoverClusterRow

	for page := 0; page < 1000; page++ {
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(pageSize))
		q.Set("$page", strconv.Itoa(page))
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request to Prism Central: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, errDiscoverV4Unavailable{status: resp.StatusCode, body: string(data)}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
		}

		var payload struct {
			Metadata struct {
				TotalAvailableResults int `json:"totalAvailableResults"`
			} `json:"metadata"`
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, e := range payload.Data {
			row := discoverRowFromV4Entity(e)
			key := row.ExtID
			if key == "" {
				key = row.Address + row.Name
			}
			if key != "" && !seen[key] {
				seen[key] = true
				out = append(out, row)
			}
		}
		total := payload.Metadata.TotalAvailableResults
		if total > 0 && len(out) >= total {
			break
		}
	}
	return out, nil
}

// fetchDiscoverClusterRowsV3 returns cluster rows from POST v3 clusters/list.
func fetchDiscoverClusterRowsV3(pcURL, username, password string, insecure bool) ([]DiscoverClusterRow, error) {
	u := pcURL + "/api/nutanix/v3/clusters/list"
	body := []byte(`{}`)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(username, password)

	client := discoverHTTPClient(insecure)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Prism Central: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Prism Central returned %d: %s", resp.StatusCode, string(bytes.TrimSpace(data)))
	}

	var list pcListClustersResponse
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var out []DiscoverClusterRow
	for _, e := range list.Entities {
		out = append(out, discoverRowFromV3Entity(e))
	}
	return out, nil
}

// runDiscoverClusters lists clusters from Prism Central (default v4 GET clustermgmt; optional v3 or v4→v3 fallback).
func runDiscoverClusters(cmd *cobra.Command, args []string) error {
	// Load config file if set (so prism-central-url, username, etc. can come from config)
	if cfgFile := viper.GetString("config"); cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		_ = viper.ReadInConfig()
	}
	pcURL := strings.TrimSuffix(strings.TrimSpace(viper.GetString("prism-central-url")), "/")
	if pcURL == "" {
		return exitConfig(errors.New("prism-central-url is required (flag or config)"))
	}
	if !strings.HasPrefix(pcURL, "https://") && !strings.HasPrefix(pcURL, "http://") {
		pcURL = "https://" + pcURL
	}
	// Discover defines its own username/password/insecure flags; those must NOT be
	// viper.BindPFlag'd to the same keys as the root command (the last bind wins and
	// breaks the main ncc-orchestrator run). Merge: flag when set, else viper/config.
	username := viper.GetString("username")
	if cmd.Flags().Changed("username") {
		username, _ = cmd.Flags().GetString("username")
	}
	if strings.TrimSpace(username) == "" {
		username = "admin"
	}
	password := viper.GetString("password")
	if cmd.Flags().Changed("password") {
		password, _ = cmd.Flags().GetString("password")
	}
	if password == "" {
		password = os.Getenv("NCC_PASSWORD")
	}
	if password == "" {
		var err error
		password, err = promptPasswordIfEmpty("", username)
		if err != nil {
			return err
		}
	}
	insecure := viper.GetBool("insecure-skip-verify")
	if cmd.Flags().Changed("insecure-skip-verify") {
		insecure, _ = cmd.Flags().GetBool("insecure-skip-verify")
	}

	discoverVer, _ := cmd.Flags().GetString("discover-api-version")
	discoverVer = strings.ToLower(strings.TrimSpace(discoverVer))
	if discoverVer == "" {
		discoverVer = strings.ToLower(strings.TrimSpace(viper.GetString("discover-api-version")))
	}
	if discoverVer == "" {
		discoverVer = "v4"
	}
	if discoverVer != "v3" && discoverVer != "v4" {
		return exitConfig(fmt.Errorf("discover-api-version must be v3 or v4, got %q", discoverVer))
	}
	if discoverVer == "v4" {
		if err := validateNutanixV4APIVersion(viper.GetString("nutanix-v4-api-version")); err != nil {
			return exitConfig(fmt.Errorf("nutanix-v4-api-version: %w", err))
		}
	}

	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "lines"
	}
	switch format {
	case "lines", "table", "json":
	default:
		return exitConfig(fmt.Errorf("format must be lines, table, or json, got %q", format))
	}

	var rows []DiscoverClusterRow
	var err error
	if discoverVer == "v4" {
		rows, err = fetchDiscoverClusterRowsV4(pcURL, username, password, insecure, viper.GetString("nutanix-v4-api-version"))
		if err != nil {
			var v4un errDiscoverV4Unavailable
			if errors.As(err, &v4un) && v4un.status == http.StatusNotFound {
				fmt.Fprintf(os.Stderr, "discover-clusters: v4 API not found (%d), falling back to v3\n", v4un.status)
				rows, err = fetchDiscoverClusterRowsV3(pcURL, username, password, insecure)
			}
		}
	} else {
		rows, err = fetchDiscoverClusterRowsV3(pcURL, username, password, insecure)
	}
	if err != nil {
		return err
	}

	outPath, _ := cmd.Flags().GetString("output")
	writeOut := func(content string) error {
		if outPath != "" {
			if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
				return fmt.Errorf("write %s: %w", outPath, err)
			}
			fmt.Fprintf(os.Stderr, "Wrote %d cluster(s) to %s\n", len(rows), outPath)
			return nil
		}
		fmt.Print(content)
		return nil
	}

	switch format {
	case "json":
		b, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		return writeOut(string(b) + "\n")
	case "table":
		var buf strings.Builder
		tw := tabwriter.NewWriter(&buf, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "NAME\tEXT_ID\tADDRESS\tAPI")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Name, r.ExtID, r.Address, r.API)
		}
		_ = tw.Flush()
		return writeOut(buf.String())
	default: // lines
		var lines []string
		for _, r := range rows {
			if r.Address != "" {
				lines = append(lines, r.Address)
			} else if r.Name != "" {
				lines = append(lines, r.Name)
			}
		}
		return writeOut(strings.Join(lines, "\n") + "\n")
	}
}

func normalizeScheduleType(s string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "", "auto":
		if runtime.GOOS == "windows" {
			return "windows", nil
		}
		return "cron", nil
	case "cron":
		return "cron", nil
	case "windows", "windows-task":
		return "windows", nil
	default:
		return "", fmt.Errorf("type must be auto, cron, or windows (got %q)", s)
	}
}

func cronExprFromInterval(every time.Duration) (string, error) {
	if every <= 0 {
		return "", errors.New("every must be > 0")
	}
	if every%time.Minute != 0 {
		return "", errors.New("every must be a whole number of minutes for cron")
	}
	minutes := int(every / time.Minute)
	switch {
	case minutes < 60:
		return fmt.Sprintf("*/%d * * * *", minutes), nil
	case minutes%60 == 0 && minutes < 24*60:
		return fmt.Sprintf("0 */%d * * *", minutes/60), nil
	case minutes%(24*60) == 0:
		days := minutes / (24 * 60)
		return fmt.Sprintf("0 0 */%d * *", days), nil
	default:
		return "", errors.New("every must be an even minute/hour/day interval for cron (or provide --cron)")
	}
}

func shellQuotePOSIX(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func validateScheduleTaskName(taskName string) error {
	name := strings.TrimSpace(taskName)
	if name == "" {
		return errors.New("task-name must not be empty")
	}
	if len(name) > 128 {
		return errors.New("task-name too long")
	}
	for _, ch := range name {
		if !(ch == '.' || ch == '_' || ch == '-' || ch == ':' ||
			(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
			return errors.New("task-name contains invalid characters")
		}
	}
	return nil
}

func sanitizeScheduleCommand(runCmd string) (string, error) {
	v := strings.TrimSpace(runCmd)
	if v == "" {
		return "", errors.New("command cannot be empty")
	}
	if strings.ContainsAny(v, "&;|`$><\n\r\t") {
		return "", errors.New("command contains unsafe shell metacharacters")
	}
	return v, nil
}

func parseCommandLineStrict(raw string) (string, []string, error) {
	clean, err := sanitizeScheduleCommand(raw)
	if err != nil {
		return "", nil, err
	}
	parts := []string{}
	var current strings.Builder
	var quote rune
	flush := func() {
		if current.Len() == 0 {
			return
		}
		parts = append(parts, current.String())
		current.Reset()
	}
	for _, ch := range clean {
		if quote != 0 {
			if ch == quote {
				quote = 0
				continue
			}
			current.WriteRune(ch)
			continue
		}
		if ch == '"' || ch == '\'' {
			quote = ch
			continue
		}
		if ch == ' ' {
			flush()
			continue
		}
		current.WriteRune(ch)
	}
	if quote != 0 {
		return "", nil, errors.New("unterminated quotes in command")
	}
	flush()
	if len(parts) == 0 {
		return "", nil, errors.New("command cannot be empty")
	}
	name := strings.TrimSpace(parts[0])
	if name == "" {
		return "", nil, errors.New("command executable cannot be empty")
	}
	args := make([]string, 0, len(parts)-1)
	for _, p := range parts[1:] {
		args = append(args, strings.TrimSpace(p))
	}
	return name, args, nil
}

func scheduleMarker(taskName string) string {
	name := strings.TrimSpace(taskName)
	if name == "" {
		name = "default"
	}
	return "ncc-orchestrator:" + name
}

func defaultScheduleCommand(configPath string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	exe = filepath.Clean(exe)
	if runtime.GOOS == "windows" {
		if strings.TrimSpace(configPath) == "" {
			return fmt.Sprintf("\"%s\"", exe), nil
		}
		return fmt.Sprintf("\"%s\" --config \"%s\"", exe, configPath), nil
	}
	if strings.TrimSpace(configPath) == "" {
		return fmt.Sprintf("%s", shellQuotePOSIX(exe)), nil
	}
	return fmt.Sprintf("%s --config %s", shellQuotePOSIX(exe), shellQuotePOSIX(configPath)), nil
}

func upsertScheduleLine(content, marker, line string) string {
	lines := splitLines(content)
	out := make([]string, 0, len(lines)+1)
	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" {
			continue
		}
		if strings.Contains(ln, marker) {
			continue
		}
		out = append(out, ln)
	}
	out = append(out, line)
	return strings.Join(out, "\n") + "\n"
}

func removeScheduleLine(content, marker string) (string, bool) {
	lines := splitLines(content)
	out := make([]string, 0, len(lines))
	removed := false
	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" {
			continue
		}
		if strings.Contains(ln, marker) {
			removed = true
			continue
		}
		out = append(out, ln)
	}
	if len(out) == 0 {
		return "", removed
	}
	return strings.Join(out, "\n") + "\n", removed
}

func listCronSchedules(taskName string) error {
	marker := scheduleMarker(taskName)
	out, err := exec.Command("crontab", "-l").CombinedOutput()
	if err != nil {
		low := strings.ToLower(string(out))
		if strings.Contains(low, "no crontab") {
			fmt.Printf("No cron entries found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	lines := splitLines(string(out))
	found := false
	for _, ln := range lines {
		if strings.Contains(ln, marker) {
			fmt.Println(ln)
			found = true
		}
	}
	if !found {
		fmt.Printf("No cron entries found for marker %q\n", marker)
	}
	return nil
}

func removeCronSchedule(taskName string) error {
	marker := scheduleMarker(taskName)
	existingBytes, err := exec.Command("crontab", "-l").CombinedOutput()
	existing := string(existingBytes)
	if err != nil {
		low := strings.ToLower(existing)
		if strings.Contains(low, "no crontab") {
			fmt.Printf("No cron entries found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(existing))
	}
	updated, removed := removeScheduleLine(existing, marker)
	if !removed {
		fmt.Printf("No cron entries found for marker %q\n", marker)
		return nil
	}
	c := exec.Command("crontab", "-")
	c.Stdin = strings.NewReader(updated)
	if out, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("write updated crontab: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed cron entry for marker %q\n", marker)
	return nil
}

func listWindowsSchedule(taskName string) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be listed on Windows")
	}
	out, err := exec.Command("schtasks", "/Query", "/TN", taskName, "/FO", "LIST", "/V").CombinedOutput()
	if err != nil {
		return fmt.Errorf("query scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Print(string(out))
	return nil
}

func removeWindowsSchedule(taskName string) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be removed on Windows")
	}
	out, err := exec.Command("schtasks", "/Delete", "/TN", taskName, "/F").CombinedOutput()
	if err != nil {
		return fmt.Errorf("delete scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed Windows Scheduled Task %q\n%s\n", taskName, strings.TrimSpace(string(out)))
	return nil
}

func runScheduleCommandNow(runCmd string) error {
	name, args, err := parseCommandLineStrict(runCmd)
	if err != nil {
		return err
	}
	c := exec.Command(name, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin
	return c.Run()
}

func installCronSchedule(taskName, cronSpec, runCmd, logPath string) error {
	marker := scheduleMarker(taskName)
	line := fmt.Sprintf("%s %s >> %s 2>&1 # %s", cronSpec, runCmd, shellQuotePOSIX(logPath), marker)

	existingBytes, err := exec.Command("crontab", "-l").CombinedOutput()
	existing := string(existingBytes)
	if err != nil {
		// "no crontab for <user>" should be treated as empty content.
		low := strings.ToLower(existing)
		if !strings.Contains(low, "no crontab") {
			return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(existing))
		}
		existing = ""
	}

	updated := upsertScheduleLine(existing, marker, line)
	c := exec.Command("crontab", "-")
	c.Stdin = strings.NewReader(updated)
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("install crontab entry: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed cron schedule with marker %q\n", marker)
	fmt.Printf("%s\n", line)
	return nil
}

func installWindowsSchedule(taskName, runCmd string, every time.Duration) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be created on Windows")
	}
	if every <= 0 {
		return errors.New("every must be > 0")
	}
	if every%time.Minute != 0 {
		return errors.New("every must be a whole number of minutes")
	}
	minutes := int(every / time.Minute)
	if minutes < 1 {
		return errors.New("every must be at least 1 minute")
	}

	args := []string{
		"/Create",
		"/TN", taskName,
		"/TR", runCmd,
		"/SC", "MINUTE",
		"/MO", strconv.Itoa(minutes),
		"/F",
	}
	out, err := exec.Command("schtasks", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("create scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Created/updated Windows Scheduled Task %q (every %d minute(s))\n", taskName, minutes)
	return nil
}

func runCreateSchedule(cmd *cobra.Command, args []string) error {
	scheduleTypeRaw, _ := cmd.Flags().GetString("type")
	scheduleType, err := normalizeScheduleType(scheduleTypeRaw)
	if err != nil {
		return exitConfig(err)
	}

	taskName, _ := cmd.Flags().GetString("task-name")
	if err := validateScheduleTaskName(taskName); err != nil {
		return exitConfig(err)
	}
	logPath, _ := cmd.Flags().GetString("log-path")
	if strings.TrimSpace(logPath) == "" {
		logPath = "logs/ncc-scheduler.log"
	}

	runCmd, _ := cmd.Flags().GetString("command")
	if strings.TrimSpace(runCmd) == "" {
		configPath, _ := cmd.Flags().GetString("config")
		runCmd, err = defaultScheduleCommand(configPath)
		if err != nil {
			return fmt.Errorf("build schedule command: %w", err)
		}
	}
	runCmd, err = sanitizeScheduleCommand(runCmd)
	if err != nil {
		return exitConfig(err)
	}

	printOnly, _ := cmd.Flags().GetBool("print-only")
	every, _ := cmd.Flags().GetDuration("every")
	action, _ := cmd.Flags().GetString("action")
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = "create"
	}

	switch scheduleType {
	case "cron":
		switch action {
		case "list":
			return listCronSchedules(taskName)
		case "remove":
			if printOnly {
				fmt.Printf("Cron remove preview: marker=%q\n", scheduleMarker(taskName))
				return nil
			}
			return removeCronSchedule(taskName)
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}

		cronSpec, _ := cmd.Flags().GetString("cron")
		cronSpec = strings.TrimSpace(cronSpec)
		if cronSpec == "" {
			cronSpec, err = cronExprFromInterval(every)
			if err != nil {
				return exitConfig(fmt.Errorf("derive cron from --every: %w", err))
			}
		}
		line := fmt.Sprintf("%s %s >> %s 2>&1 # %s", cronSpec, runCmd, shellQuotePOSIX(logPath), scheduleMarker(taskName))
		if printOnly {
			fmt.Printf("Cron entry preview:\n%s\n", line)
			return nil
		}
		return installCronSchedule(taskName, cronSpec, runCmd, logPath)
	case "windows":
		switch action {
		case "list":
			return listWindowsSchedule(taskName)
		case "remove":
			if printOnly {
				fmt.Printf("Windows remove preview: schtasks /Delete /TN %q /F\n", taskName)
				return nil
			}
			return removeWindowsSchedule(taskName)
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}
		if printOnly {
			if every <= 0 {
				return exitConfig(errors.New("every must be > 0"))
			}
			fmt.Printf("Windows task preview:\nschtasks /Create /TN %q /TR %q /SC MINUTE /MO %d /F\n",
				taskName, runCmd, int(every/time.Minute))
			return nil
		}
		return installWindowsSchedule(taskName, runCmd, every)
	default:
		return exitConfig(fmt.Errorf("unsupported type %q", scheduleType))
	}
}

func configJSONSchema() map[string]interface{} {
	props := map[string]interface{}{
		"clusters":                  map[string]interface{}{"type": "string", "description": "Comma-separated cluster IPs/FQDNs"},
		"clusters-file":             map[string]interface{}{"type": "string"},
		"update":                    map[string]interface{}{"type": "boolean"},
		"username":                  map[string]interface{}{"type": "string"},
		"password":                  map[string]interface{}{"type": "string"},
		"ncc-api-version":           map[string]interface{}{"type": "string", "enum": []string{"v4", "Legacy", "v1"}},
		"nutanix-v4-api-version":    map[string]interface{}{"type": "string"},
		"insecure-skip-verify":      map[string]interface{}{"type": "boolean"},
		"timeout":                   map[string]interface{}{"type": "string"},
		"request-timeout":           map[string]interface{}{"type": "string"},
		"poll-interval":             map[string]interface{}{"type": "string"},
		"poll-jitter":               map[string]interface{}{"type": "string"},
		"max-parallel":              map[string]interface{}{"type": "integer", "minimum": 1},
		"outputs":                   map[string]interface{}{"type": "string", "description": "Comma-separated html,csv,json,markdown,sarif"},
		"output-dir-logs":           map[string]interface{}{"type": "string"},
		"output-dir-filtered":       map[string]interface{}{"type": "string"},
		"single-report":             map[string]interface{}{"type": "boolean"},
		"gen-test-agg":              map[string]interface{}{"type": "integer", "minimum": 0},
		"severity-filter":           map[string]interface{}{"type": "string"},
		"exclude-alert-titles":      map[string]interface{}{"type": "string"},
		"exclude-alert-titles-file": map[string]interface{}{"type": "string"},
		"exclude-alert-match-mode":  map[string]interface{}{"type": "string", "enum": []string{"exact", "contains", "regex"}},
		"dry-run":                   map[string]interface{}{"type": "boolean"},
		"replay":                    map[string]interface{}{"type": "boolean"},
		"log-file":                  map[string]interface{}{"type": "string"},
		"log-level":                 map[string]interface{}{"type": "string", "enum": []string{"trace", "debug", "info", "warn", "warning", "error", "fatal", "0", "1", "2", "3", "4", "5"}},
		"log-http":                  map[string]interface{}{"type": "boolean"},
		"retry-max-attempts":        map[string]interface{}{"type": "integer", "minimum": 1},
		"retry-base-delay":          map[string]interface{}{"type": "string"},
		"retry-max-delay":           map[string]interface{}{"type": "string"},
		"retry-circuit-breaker":     map[string]interface{}{"type": "integer", "minimum": 1},
		"prom-dir":                  map[string]interface{}{"type": "string"},
		"run-history":               map[string]interface{}{"type": "boolean"},
		"run-history-dir":           map[string]interface{}{"type": "string"},
		"retain-last":               map[string]interface{}{"type": "integer", "minimum": 0},
		"retain-days":               map[string]interface{}{"type": "integer", "minimum": 0},
		"artifact-retain-days":      map[string]interface{}{"type": "integer", "minimum": 0},
		"artifact-retain-max-files": map[string]interface{}{"type": "integer", "minimum": 0},
		"notify-on-regression":      map[string]interface{}{"type": "boolean"},
		"adaptive-parallelism":      map[string]interface{}{"type": "boolean"},
		"policy-gates":              map[string]interface{}{"type": "string"},
		"quiet-hours":               map[string]interface{}{"type": "string", "description": "HH:MM-HH:MM local time"},
		"maintenance-windows":       map[string]interface{}{"type": "string", "description": "comma-separated start/end RFC3339 windows"},
		"flaky-lookback-runs":       map[string]interface{}{"type": "integer", "minimum": 2},
		"flaky-min-transitions":     map[string]interface{}{"type": "integer", "minimum": 1},
		"email-enabled":             map[string]interface{}{"type": "boolean"},
		"email-attach-html":         map[string]interface{}{"type": "boolean"},
		"notify-digest":             map[string]interface{}{"type": "boolean"},
		"smtp-server":               map[string]interface{}{"type": "string"},
		"smtp-port":                 map[string]interface{}{"type": "integer"},
		"smtp-user":                 map[string]interface{}{"type": "string"},
		"smtp-password":             map[string]interface{}{"type": "string"},
		"email-from":                map[string]interface{}{"type": "string"},
		"email-to":                  map[string]interface{}{"type": "string"},
		"email-use-tls":             map[string]interface{}{"type": "boolean"},
		"webhook-enabled":           map[string]interface{}{"type": "boolean"},
		"webhook-include-html":      map[string]interface{}{"type": "boolean"},
		"webhook-url":               map[string]interface{}{"type": "string"},
		"webhook-headers": map[string]interface{}{
			"type":                 "object",
			"additionalProperties": map[string]interface{}{"type": "string"},
		},
		"slack-enabled":           map[string]interface{}{"type": "boolean"},
		"slack-webhook-url":       map[string]interface{}{"type": "string"},
		"slack-channel":           map[string]interface{}{"type": "string"},
		"secrets-provider":        map[string]interface{}{"type": "string", "enum": []string{"", "env", "file"}},
		"secrets-file":            map[string]interface{}{"type": "string"},
		"prism-central-url":       map[string]interface{}{"type": "string"},
		"discover-api-version":    map[string]interface{}{"type": "string", "enum": []string{"v3", "v4"}},
		"max-idle-conns":          map[string]interface{}{"type": "integer"},
		"max-idle-conns-per-host": map[string]interface{}{"type": "integer"},
		"max-conns-per-host":      map[string]interface{}{"type": "integer"},
		"idle-conn-timeout":       map[string]interface{}{"type": "string"},
	}
	return map[string]interface{}{
		"$schema":              "https://json-schema.org/draft/2020-12/schema",
		"title":                "NCC Orchestrator Config",
		"type":                 "object",
		"additionalProperties": false,
		"properties":           props,
	}
}

func runConfigSchema(cmd *cobra.Command, args []string) error {
	schema := configJSONSchema()
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}
	outPath, _ := cmd.Flags().GetString("output")
	if strings.TrimSpace(outPath) == "" {
		fmt.Println(string(data))
		return nil
	}
	return os.WriteFile(outPath, data, 0644)
}

func runValidateConfigCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		return exitConfig(errors.New("--config is required"))
	}
	if _, err := os.Stat(cfgPath); err != nil {
		return exitConfig(fmt.Errorf("config path %s: %w", cfgPath, err))
	}
	// validate-config is a standalone subcommand and does not share root --config binding,
	// so set viper key explicitly before calling bindConfig().
	viper.Set("config", cfgPath)
	cfg, err := bindConfig()
	if err != nil {
		return exitConfig(fmt.Errorf("configuration: %w", err))
	}
	if err := validateConfig(cfg); err != nil {
		return exitConfig(fmt.Errorf("validation: %w", err))
	}
	fmt.Printf("Config is valid: %s\n", cfgPath)
	return nil
}

func runValidateSecretsCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		return exitConfig(errors.New("--config is required"))
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return exitConfig(fmt.Errorf("config path %s: %w", cfgPath, err))
	}
	secretRefs := strings.Count(string(raw), "secret://")
	viper.Set("config", cfgPath)
	cfg, err := bindConfig()
	if err != nil {
		return exitConfig(fmt.Errorf("secret validation failed: %w", err))
	}
	provider := strings.TrimSpace(cfg.SecretsProvider)
	if secretRefs == 0 {
		fmt.Printf("No secret:// references found in config: %s\n", cfgPath)
		return nil
	}
	if provider == "" {
		return exitConfig(errors.New("secret:// references found but secrets-provider is empty"))
	}
	fmt.Printf("Secrets validation passed: refs=%d provider=%s config=%s\n", secretRefs, provider, cfgPath)
	return nil
}

// extractClusterAddressV4 returns a reachable cluster address from clustermgmt v4 config cluster JSON.
func extractClusterAddressV4(entity map[string]interface{}) string {
	if netw, _ := entity["network"].(map[string]interface{}); netw != nil {
		if ext, _ := netw["externalAddress"].(map[string]interface{}); ext != nil {
			if ipv4, _ := ext["ipv4"].(map[string]interface{}); ipv4 != nil {
				if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
			if ipv6, _ := ext["ipv6"].(map[string]interface{}); ipv6 != nil {
				if v, _ := ipv6["value"].(string); strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	if nodes, _ := entity["nodes"].(map[string]interface{}); nodes != nil {
		if list, _ := nodes["nodeList"].([]interface{}); len(list) > 0 {
			if first, _ := list[0].(map[string]interface{}); first != nil {
				if cvm, _ := first["controllerVmIp"].(map[string]interface{}); cvm != nil {
					if ipv4, _ := cvm["ipv4"].(map[string]interface{}); ipv4 != nil {
						if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
							return strings.TrimSpace(v)
						}
					}
				}
				if host, _ := first["hostIp"].(map[string]interface{}); host != nil {
					if ipv4, _ := host["ipv4"].(map[string]interface{}); ipv4 != nil {
						if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
							return strings.TrimSpace(v)
						}
					}
				}
			}
		}
	}
	if name, _ := entity["name"].(string); strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	return ""
}

// extractCVMIPv4sFromClusterEntity returns controller VM IPv4 addresses from a clustermgmt v4 cluster entity.
func extractCVMIPv4sFromClusterEntity(entity map[string]interface{}) []string {
	nodes, _ := entity["nodes"].(map[string]interface{})
	if nodes == nil {
		return nil
	}
	list, _ := nodes["nodeList"].([]interface{})
	var out []string
	for _, raw := range list {
		node, _ := raw.(map[string]interface{})
		if node == nil {
			continue
		}
		cvm, _ := node["controllerVmIp"].(map[string]interface{})
		if cvm == nil {
			continue
		}
		if ipv4, _ := cvm["ipv4"].(map[string]interface{}); ipv4 != nil {
			if v, _ := ipv4["value"].(string); strings.TrimSpace(v) != "" {
				out = append(out, strings.TrimSpace(v))
			}
		}
	}
	return out
}

func extractClusterExtIDV4(entity map[string]interface{}) string {
	if extID, _ := entity["extId"].(string); strings.TrimSpace(extID) != "" {
		return strings.TrimSpace(extID)
	}
	return ""
}

func dedupeStringsKeepOrder(in []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// clusterEntityMatchesUserRef reports whether the user's --clusters value refers to this
// registered cluster (name, extId, external address, or any CVM IP). Used so Prism Central
// does not always use data[0] (wrong cluster when multiple are registered).
func clusterEntityMatchesUserRef(ref string, entity map[string]interface{}) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	if name, _ := entity["name"].(string); strings.TrimSpace(name) != "" && strings.EqualFold(strings.TrimSpace(name), ref) {
		return true
	}
	if extID, _ := entity["extId"].(string); strings.TrimSpace(extID) != "" && strings.EqualFold(strings.TrimSpace(extID), ref) {
		return true
	}
	if a := extractClusterAddressV4(entity); a != "" && strings.EqualFold(a, ref) {
		return true
	}
	for _, ip := range extractCVMIPv4sFromClusterEntity(entity) {
		if strings.EqualFold(ip, ref) {
			return true
		}
	}
	return false
}

// extractClusterAddressV3 extracts external IP or name from a Prism Central cluster entity (v3).
func extractClusterAddressV3(entity map[string]interface{}) string {
	// spec.resources.network.external_ip or external_ip_address
	if spec, _ := entity["spec"].(map[string]interface{}); spec != nil {
		if res, _ := spec["resources"].(map[string]interface{}); res != nil {
			if netw, _ := res["network"].(map[string]interface{}); netw != nil {
				if ip, _ := netw["external_ip"].(string); ip != "" {
					return ip
				}
				if ip, _ := netw["external_ip_address"].(string); ip != "" {
					return ip
				}
			}
		}
	}
	// status.resources.network.external_ip
	if status, _ := entity["status"].(map[string]interface{}); status != nil {
		if res, _ := status["resources"].(map[string]interface{}); res != nil {
			if netw, _ := res["network"].(map[string]interface{}); netw != nil {
				if ip, _ := netw["external_ip"].(string); ip != "" {
					return ip
				}
			}
		}
		if name, _ := status["name"].(string); name != "" {
			return name
		}
	}
	return ""
}

func newRootCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "ncc-orchestrator",
		Short: "Nutanix NCC Orchestrator",
		Long: `A tool to run NCC checks on multiple clusters, aggregate results, and generate reports.
Use --config for setup.

Examples:

  # Basic usage with configuration file
  ncc-orchestrator --config config.yaml

  # Specify clusters and username via flags
  ncc-orchestrator --clusters 10.0.1.1,10.0.2.1 --username admin

  # Show all available environment variables
  ncc-orchestrator --env-info

Run 'ncc-orchestrator --help' for a full list of options.
`,
		Version: fmt.Sprintf(`
Version: %s
Stream: %s
Build Date: %s
Go Version: %s`, Version, Stream, BuildDate, GoVersion),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Setup console logger first for early error visibility
			consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
			consoleLogger := zerolog.New(consoleWriter).With().Timestamp().Logger()
			zerolog.SetGlobalLevel(zerolog.InfoLevel)

			// Check for latest release and optionally update binary
			if update, _ := cmd.Flags().GetBool("update"); update {
				if err := runUpdate(); err != nil {
					consoleLogger.Error().Err(err).Msg("update failed")
					return fmt.Errorf("update: %w", err)
				}
				return nil
			}

			// Generate test aggregated report (no config required)
			if genN, _ := cmd.Flags().GetInt("gen-test-agg"); genN > 0 {
				outDir := viper.GetString("output-dir-filtered")
				if outDir == "" {
					outDir = "outputfiles"
				}
				if err := generateTestAgg(genN, outDir); err != nil {
					consoleLogger.Error().Err(err).Int("clusters", genN).Msg("gen-test-agg failed")
					return fmt.Errorf("gen-test-agg: %w", err)
				}
				fmt.Printf("Generated test aggregated report: %d clusters, output in %s/index.html\n", genN, outDir)
				return nil
			}

			cfg, err := bindConfig()
			if err != nil {
				consoleLogger.Error().Err(err).Msg("configuration error")
				return exitConfig(fmt.Errorf("configuration: %w", err))
			}

			lvl := parseLogLevel(cfg.LogLevel)
			// Validate log level
			if lvl > zerolog.FatalLevel {
				consoleLogger.Warn().Int("level", int(lvl)).Msg("invalid log level, using info level")
				lvl = zerolog.InfoLevel
			}

			if err := checkOutputPermissions(&cfg); err != nil {
				consoleLogger.Error().Err(err).Msg("output permissions check failed (cannot open/create required files)")
				return exitConfig(fmt.Errorf("output permissions: %w", err))
			}

			if err := setupFileLogger(cfg.LogFile, lvl); err != nil {
				consoleLogger.Error().Err(err).Str("logFile", cfg.LogFile).Msg("failed to setup file logger")
				return exitConfig(fmt.Errorf("setup logger: %w", err))
			}

			// Set global log level after file logger is set up
			zerolog.SetGlobalLevel(lvl)
			log.Info().
				Strs("clusters", cfg.Clusters).
				Str("username", cfg.Username).
				Str("password", maskPassword(cfg.Password)).
				Bool("insecureSkipVerify", cfg.InsecureSkipVerify).
				Dur("timeout", cfg.Timeout).
				Dur("requestTimeout", cfg.RequestTimeout).
				Dur("pollInterval", cfg.PollInterval).
				Dur("pollJitter", cfg.PollJitter).
				Int("maxParallel", cfg.MaxParallel).
				Strs("outputs", cfg.OutputFormats).
				Str("logsDir", cfg.OutputDirLogs).
				Str("filteredDir", cfg.OutputDirFiltered).
				Str("logFile", cfg.LogFile).
				Str("logLevel", lvl.String()).
				Bool("logHTTP", cfg.LogHTTP || os.Getenv("LOG_HTTP") == "1").
				Int("retryMaxAttempts", cfg.RetryMaxAttempts).
				Dur("retryBaseDelay", cfg.RetryBaseDelay).
				Dur("retryMaxDelay", cfg.RetryMaxDelay).
				Msg("starting NCC orchestrator")

			if tc, _ := cmd.Flags().GetBool("tc"); tc {
				fmt.Print(termsText)
				return nil
			}
			// Validate required fields first
			if len(cfg.Clusters) == 0 {
				err := errors.New("no clusters provided (--clusters, --clusters-file, env, or config)")
				log.Error().Msg(err.Error())
				return exitConfig(err)
			}
			if err := validateClusterCredentialCoverage(cfg); err != nil {
				log.Error().Msg(err.Error())
				return exitConfig(err)
			}

			// Dry-run mode: perform full validation and exit
			if cfg.DryRun {
				// Perform comprehensive validation for dry-run
				if err := validateConfig(cfg); err != nil {
					return fmt.Errorf("dry-run validation failed: %w", err)
				}

				log.Info().Msg("DRY-RUN MODE: Configuration validated, no checks will be executed")
				fmt.Println("✓ Configuration is valid")
				fmt.Printf("  Clusters: %d configured\n", len(cfg.Clusters))
				if strings.TrimSpace(cfg.Username) != "" {
					fmt.Printf("  Username: %s\n", cfg.Username)
				} else {
					fmt.Println("  Username: per-cluster (clusters-file)")
				}
				fmt.Printf("  Output formats: %v\n", cfg.OutputFormats)
				if len(cfg.SeverityFilter) > 0 {
					fmt.Printf("  Severity filter: %v\n", cfg.SeverityFilter)
				}
				if len(cfg.ExcludeAlertTitles) > 0 {
					fmt.Printf("  Excluded alert titles: %v\n", cfg.ExcludeAlertTitles)
				}
				fmt.Printf("  Exclude alert match mode: %s\n", cfg.ExcludeAlertMatchMode)
				if cfg.ExcludeAlertTitlesFile != "" {
					fmt.Printf("  Exclude alert titles file: %s\n", cfg.ExcludeAlertTitlesFile)
				}
				if cfg.ArtifactRetainDays > 0 || cfg.ArtifactRetainMaxFiles > 0 {
					fmt.Printf("  Artifact retention: days=%d max_files=%d\n", cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles)
				}
				if cfg.InsecureSkipVerify {
					fmt.Println("  ⚠️  WARNING: TLS verification is disabled")
				}
				fmt.Println("  All settings validated successfully")
				return nil
			}

			if envInfo, err := cmd.Flags().GetBool("env-info"); err == nil && envInfo {
				fmt.Println("Possible Environment Variables (prefix: NCC_) and Current Values:")
				envKeys := []string{
					"CONFIG",
					"CLUSTERS",
					"CLUSTERS_FILE",
					"PRISM_CENTRAL_URL",
					"USERNAME",
					"PASSWORD",
					"INSECURE_SKIP_VERIFY",
					"TIMEOUT",
					"REQUEST_TIMEOUT",
					"POLL_INTERVAL",
					"POLL_JITTER",
					"MAX_PARALLEL",
					"OUTPUTS",
					"OUTPUT_DIR_LOGS",
					"OUTPUT_DIR_FILTERED",
					"LOG_FILE",
					"LOG_LEVEL",
					"LOG_HTTP",
					"RETRY_MAX_ATTEMPTS",
					"RETRY_BASE_DELAY",
					"RETRY_MAX_DELAY",
					"RETRY_CIRCUIT_BREAKER",
					"PROM_DIR",
					"RUN_HISTORY",
					"RUN_HISTORY_DIR",
					"RETAIN_LAST",
					"RETAIN_DAYS",
					"ARTIFACT_RETAIN_DAYS",
					"ARTIFACT_RETAIN_MAX_FILES",
					"SINGLE_REPORT",
					"NOTIFY_ON_REGRESSION",
					"ADAPTIVE_PARALLELISM",
					"SEVERITY_FILTER",
					"EXCLUDE_ALERT_TITLES",
					"EXCLUDE_ALERT_TITLES_FILE",
					"EXCLUDE_ALERT_MATCH_MODE",
					"DRY_RUN",
					"REPLAY",
					"MAX_IDLE_CONNS",
					"MAX_IDLE_CONNS_PER_HOST",
					"MAX_CONNS_PER_HOST",
					"IDLE_CONN_TIMEOUT",
					"EMAIL_ENABLED",
					"EMAIL_ATTACH_HTML",
					"NOTIFY_DIGEST",
					"SMTP_SERVER",
					"SMTP_PORT",
					"SMTP_USER",
					"SMTP_PASSWORD",
					"EMAIL_FROM",
					"EMAIL_TO",
					"EMAIL_USE_TLS",
					"WEBHOOK_ENABLED",
					"WEBHOOK_INCLUDE_HTML",
					"WEBHOOK_URL",
					"WEBHOOK_HEADERS",
					"SLACK_ENABLED",
					"SLACK_WEBHOOK_URL",
					"SLACK_CHANNEL",
					"UPDATE",
				}
				for _, key := range envKeys {
					envVar := "NCC_" + key
					val := os.Getenv(envVar)
					if val != "" {
						// Mask sensitive values
						if key == "PASSWORD" || key == "SMTP_PASSWORD" {
							fmt.Printf("%s = %s\n", envVar, maskPassword(val))
						} else {
							fmt.Printf("%s = %s\n", envVar, val)
						}
					} else {
						fmt.Printf("%s = (not set)\n", envVar)
					}
				}
				return nil // Exit after printing
			}

			if needPrompt, promptUser := needsPasswordPrompt(cfg); needPrompt {
				cfg.Password, err = promptPasswordIfEmpty(cfg.Password, promptUser)
				if err != nil {
					return err
				}
			}

			fs := OSFS{}
			httpc := NewHTTPClient(cfg)
			if err := fs.MkdirAll(cfg.OutputDirLogs, 0755); err != nil {
				return err
			}
			if err := fs.MkdirAll(cfg.OutputDirFiltered, 0755); err != nil {
				return err
			}
			if err := fs.MkdirAll(cfg.PromDir, 0755); err != nil {
				return err
			}

			previousSummaryPath := filepath.Join(cfg.OutputDirFiltered, "run-summary.json")
			previousSummary, hasPreviousSummary, err := loadRunSummaryJSON(previousSummaryPath)
			if err != nil {
				log.Warn().Err(err).Str("path", previousSummaryPath).Msg("failed to read previous run-summary; regression baseline disabled")
			}
			previousChecksSnapshotPath := filepath.Join(cfg.OutputDirFiltered, "checks-snapshot.json")
			previousChecksSnapshot, hasPreviousChecksSnapshot, err := loadChecksSnapshotJSON(previousChecksSnapshotPath)
			if err != nil {
				log.Warn().Err(err).Str("path", previousChecksSnapshotPath).Msg("failed to read previous checks snapshot; drill-down baseline disabled")
			}
			if hasPreviousSummary {
				cfg.PreviousClusterFailCount = failCountByCluster(previousSummary)
				log.Info().
					Str("previous_timestamp", previousSummary.Timestamp).
					Int("previous_clusters", len(previousSummary.Clusters)).
					Msg("loaded previous run-summary baseline")
			} else {
				cfg.PreviousClusterFailCount = map[string]int{}
			}

			// Fast replay mode: skip API, parse existing logs and render everything
			if cmd.Flags().Changed("replay") && viper.GetBool("replay") {
				var agg []AggBlock
				var clusterFiles []struct{ Cluster, HTML, CSV string }
				excludedByCluster := make(map[string][]ExcludedAlert)

				for _, cluster := range cfg.Clusters {
					// Ensure filtered log exists
					filtered := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
					if _, err := os.Stat(filtered); err != nil {
						// Try to build it from raw ncc log
						raw := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
						if _, err2 := os.Stat(raw); err2 == nil {
							if _, err3 := filterBlocksToFile(OSFS{}, raw, filtered); err3 != nil {
								log.Error().Str("cluster", cluster).Err(err3).Msg("replay: build filtered failed")
								continue
							}
							log.Info().Str("cluster", cluster).Str("filtered", filtered).Msg("replay: built filtered")
						} else {
							log.Warn().Str("cluster", cluster).Msg("replay: no filtered or raw log, skipping")
							continue
						}
					}
					// Parse filtered
					data, err := os.ReadFile(filtered)
					if err != nil {
						log.Error().Str("cluster", cluster).Err(err).Msg("replay: read filtered failed")
						continue
					}
					blocks, err := ParseSummary(string(data))
					if err != nil {
						log.Error().Str("cluster", cluster).Err(err).Msg("replay: parse filtered failed")
						continue
					}
					if len(cfg.SeverityFilter) > 0 {
						originalCount := len(blocks)
						blocks = filterBlocksBySeverity(blocks, cfg.SeverityFilter)
						log.Info().
							Str("cluster", cluster).
							Int("original", originalCount).
							Int("filtered", len(blocks)).
							Strs("severities", cfg.SeverityFilter).
							Msg("replay: applied severity filter")
					}
					if len(cfg.ExcludeAlertTitles) > 0 {
						originalCount := len(blocks)
						filteredBlocks, excludedAlerts, err := filterBlocksByTitle(blocks, cfg.ExcludeAlertTitles, cfg.ExcludeAlertMatchMode)
						if err != nil {
							log.Error().Str("cluster", cluster).Err(err).Msg("replay: apply alert title exclusion filter failed")
							continue
						}
						blocks = filteredBlocks
						excludedByCluster[cluster] = excludedAlerts
						log.Info().
							Str("cluster", cluster).
							Int("original", originalCount).
							Int("filtered", len(blocks)).
							Int("excluded", len(excludedAlerts)).
							Str("mode", cfg.ExcludeAlertMatchMode).
							Strs("titles", cfg.ExcludeAlertTitles).
							Msg("replay: applied alert title exclusion filter")
					}

					counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
					for _, b := range blocks {
						sev := b.Severity
						if sev == "" {
							sev = "INFO"
						}
						counts[sev]++
					}
					overviewReplay := fmt.Sprintf("NCC replay for cluster %s. FAIL: %d, WARN: %d, ERR: %d, INFO: %d. Total: %d checks (from existing log).",
						cluster, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], len(blocks))
					replaySummary := NotificationSummary{
						Cluster:     cluster,                           // from replay filename or param
						StartedAt:   time.Now().Add(-10 * time.Minute), // estimate
						FinishedAt:  time.Now(),
						FailCount:   counts["FAIL"],
						WarnCount:   counts["WARN"],
						ErrCount:    counts["ERR"],
						InfoCount:   counts["INFO"],
						TotalChecks: len(blocks),
						OutputFiles: []string{filtered},
						Overview:    overviewReplay,
					}

					subj := fmt.Sprintf("NCC REPLAY %s: FAIL=%d WARN=%d",
						replaySummary.Cluster, replaySummary.FailCount, replaySummary.WarnCount)
					body := overviewReplay + "\n\n" + fmt.Sprintf("REPLAY MODE - From existing log:\nFAIL: %d | WARN: %d | Total: %d\nLog: %s",
						replaySummary.FailCount, replaySummary.WarnCount, len(blocks), filtered)

					// Per-cluster outputs: generate HTML and CSV before notifications so we can attach HTML to email
					base := filtered
					var replayHTMLPath string
					for _, f := range cfg.OutputFormats {
						switch strings.ToLower(strings.TrimSpace(f)) {
						case "html":
							htmlFile := base + ".html"
							replayHTMLPath = htmlFile
							rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
							meta, err := parseNCCHeader(rawPath)
							if err != nil {
								log.Warn().Err(err).Str("rawPath", rawPath).Msg("replay: parseNCCHeader failed, using empty meta")
								meta = HTMLMeta{}
							}
							if err := generateHTML(OSFS{}, rowsFromBlocks(blocks), htmlFile, meta); err != nil {
								log.Error().Err(err).Str("file", htmlFile).Msg("replay: write HTML failed")
								replayHTMLPath = ""
							}
						case "csv":
							_ = generateCSV(OSFS{}, blocks, base+".csv")
						case "json":
							rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
							meta, _ := parseNCCHeader(rawPath)
							_ = generateJSON(OSFS{}, blocks, base+".json", meta)
						case "markdown":
							rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
							meta, _ := parseNCCHeader(rawPath)
							_ = generateMarkdown(OSFS{}, blocks, base+".md", meta)
						case "sarif":
							_ = generateSARIF(OSFS{}, blocks, base+".sarif")
						}
						if err := writePrometheusFile(OSFS{}, cfg.PromDir, cluster, blocks); err != nil {
							log.Error().Str("cluster", cluster).Err(err).Msg("replay write Prometheus .prom failed")
						}
						log.Info().Str("cluster", cluster).Str("prom_dir", cfg.PromDir).Msg("replay: Prometheus .prom written")
					}

					attachPath := ""
					if cfg.EmailAttachHTML && replayHTMLPath != "" {
						attachPath = replayHTMLPath
					}
					if cfg.WebhookIncludeHTML && replayHTMLPath != "" {
						if b, err := os.ReadFile(replayHTMLPath); err == nil {
							replaySummary.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
						}
					}
					ctx := context.Background()
					httpc := NewHTTPClient(cfg)
					if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
						log.Info().Str("cluster", cluster).Str("reason", reason).Msg("replay notifications suppressed")
					} else {
						if err := sendEmailWithRetry(cfg, subj, body, attachPath); err != nil {
							log.Error().Err(err).Str("cluster", cluster).Msg("replay email failed")
						}
						if err := sendWebhookWithRetry(ctx, httpc, cfg, replaySummary); err != nil {
							log.Error().Err(err).Str("cluster", cluster).Msg("replay webhook failed")
						}
						log.Info().Int("fail", replaySummary.FailCount).Int("warn", replaySummary.WarnCount).
							Str("cluster", cluster).Msg("replay notifications sent")
					}

					clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
						Cluster: cluster,
						HTML:    filepath.Base(base + ".html"),
						CSV:     filepath.Base(base + ".csv"),
					})
					rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
					meta, _ := parseNCCHeader(rawPath) // ignore error

					for _, b := range blocks {
						agg = append(agg, AggBlock{
							Cluster:        cluster,
							Severity:       b.Severity,
							Check:          b.CheckName,
							Detail:         b.DetailRaw,
							ClusterName:    meta.ClusterName,
							ClusterVersion: meta.ClusterVersion,
							NCCVersion:     meta.NCCVersion,
						})
					}
				}

				if err := writeAggregatedHTMLSingle(OSFS{}, cfg.OutputDirFiltered, agg, clusterFiles, cfg); err != nil {
					log.Error().Err(err).Msg("replay: write aggregated HTML failed")
					return err
				}
				if len(cfg.ExcludeAlertTitles) > 0 {
					if err := writeExcludedAlertsAuditJSON(OSFS{}, cfg.OutputDirFiltered, cfg.ExcludeAlertMatchMode, cfg.ExcludeAlertTitles, excludedByCluster); err != nil {
						log.Error().Err(err).Msg("replay: write excluded-alerts.json failed (non-fatal)")
					}
				}
				if deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, time.Now()); err != nil {
					log.Error().Err(err).Msg("replay: artifact retention failed (non-fatal)")
				} else if deleted > 0 {
					log.Info().Int("deleted", deleted).Int("retain_days", cfg.ArtifactRetainDays).Int("retain_max_files", cfg.ArtifactRetainMaxFiles).Msg("replay: artifact retention applied")
				}
				log.Info().Int("clusters", len(clusterFiles)).Int("rows", len(agg)).Msg("replay: aggregated page generated")
				return nil
			}

			// Inside RunE, after setting up cfg, fs, httpc...
			fmt.Println("You have accepted T&C, Check using --tc flag")

			// Create root context with graceful shutdown support
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Setup graceful shutdown signal handling
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
			go func() {
				sig := <-sigChan
				log.Warn().Str("signal", sig.String()).Msg("received shutdown signal, initiating graceful shutdown")
				fmt.Fprintf(os.Stderr, "\n⚠️  Received %s signal. Initiating graceful shutdown...\n", sig.String())
				cancel() // Cancel root context to stop all operations

				// Give operations time to finish
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
				defer shutdownCancel()

				select {
				case <-shutdownCtx.Done():
					log.Warn().Msg("graceful shutdown timeout exceeded, forcing exit")
					fmt.Fprintln(os.Stderr, "⚠️  Graceful shutdown timeout exceeded")
					os.Exit(1)
				case <-time.After(100 * time.Millisecond):
					// Allow time for cleanup
				}
			}()

			p := mpb.New(mpb.WithWidth(80))
			defer func() {
				// Ensure progress bars are cleaned up on exit
				p.Wait()
			}()
			resetAdaptiveParallelism(cfg.MaxParallel)
			sem := make(chan struct{}, cfg.MaxParallel)
			var activeWorkers int32
			var wg sync.WaitGroup
			results := make(chan ClusterResult, len(cfg.Clusters))
			runStart := time.Now()

			for _, cluster := range cfg.Clusters {
				clusterUser, clusterPass := credentialsForCluster(cfg, cluster)
				wg.Add(1)
				for {
					limit := currentAdaptiveParallel(cfg.MaxParallel)
					curActive := int(atomic.LoadInt32(&activeWorkers))
					if curActive < limit {
						break
					}
					if ctx.Err() != nil {
						break
					}
					time.Sleep(250 * time.Millisecond)
				}
				sem <- struct{}{}
				atomic.AddInt32(&activeWorkers, 1)

				mainBar := p.New(
					100,
					mpb.BarStyle().Rbound("|"),
					mpb.PrependDecorators(
						decor.Name(fmt.Sprintf("%-18s", cluster), decor.WC{W: 20, C: decor.DidentRight}),
					),
					mpb.AppendDecorators(
						decor.Percentage(decor.WC{W: 4}),
						decor.Name(" • "),
						decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
					),
				)

				phaseProxy := &proxyDecorator{text: "starting"}

				phaseBar := p.New(
					1,
					mpb.NopStyle(),
					mpb.PrependDecorators(decor.Name(strings.Repeat(" ", 20))),
					mpb.AppendDecorators(phaseProxy),
				)

				go func(cl string, user string, pass string, b *mpb.Bar, phase *proxyDecorator, phaseBar *mpb.Bar) {
					defer wg.Done()
					defer func() {
						<-sem
						atomic.AddInt32(&activeWorkers, -1)
					}()
					defer func() {
						if r := recover(); r != nil {
							b.Abort(false)
							b.SetTotal(b.Current(), true)
							phaseBar.SetCurrent(1)     // Set current to match total
							phaseBar.SetTotal(1, true) // Complete phaseBar on panic
							log.Error().Interface("panic", r).Stack().Str("cluster", cl).Msg("cluster goroutine panic")
							panicErr := fmt.Errorf("panic: %v", r)
							results <- ClusterResult{Cluster: cl, Blocks: nil, Err: panicErr, ErrorClass: classifyClusterError(panicErr)}
						}
					}()

					reqCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
					defer cancel()

					onPct := func(pct int) { b.SetCurrent(int64(pct)) }
					setPhase := func(text string) {
						phase.SetText(text)
						log.Info().Str("cluster", cl).Str("phase", text).Msg("phase change")
					}

					runOut, err := runClusterWithBars(reqCtx, cfg, fs, httpc, cl, user, pass, onPct, setPhase)
					if err != nil {
						b.Abort(false)
						b.SetTotal(b.Current(), true)
						setPhase("failed")
						phaseBar.SetCurrent(1)     // Set current to match total
						phaseBar.SetTotal(1, true) // Complete phaseBar on error
						// Log with detailed error information
						log.Error().
							Str("cluster", cl).
							Err(err).
							Str("error_type", fmt.Sprintf("%T", err)).
							Msg("cluster run failed")
						// Also print to console for visibility
						fmt.Fprintf(os.Stderr, "\n❌ Cluster %s failed: %v\n", cl, err)
						results <- ClusterResult{Cluster: cl, Blocks: nil, Err: err, ErrorClass: classifyClusterError(err)}
						return
					}

					b.SetCurrent(100)
					b.SetTotal(100, true)
					setPhase("done")
					phaseBar.SetCurrent(1)     // Set current to match total
					phaseBar.SetTotal(1, true) // Complete phaseBar on success
					log.Info().Str("cluster", cl).Msg("cluster run completed")
					results <- ClusterResult{Cluster: cl, Blocks: runOut.Blocks, ExcludedByTitle: runOut.ExcludedByTitle, Err: nil, ErrorClass: ""}
				}(cluster, clusterUser, clusterPass, mainBar, phaseProxy, phaseBar) // Pass phaseBar and per-cluster credentials
			}

			// Wait for workers with context cancellation support
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				// All workers completed normally
			case <-ctx.Done():
				// Context cancelled (shutdown signal received)
				log.Warn().Msg("context cancelled, waiting for workers to finish")
				fmt.Fprintln(os.Stderr, "⏳ Waiting for in-progress operations to complete...")
				// Wait with timeout for workers to finish
				waitCtx, waitCancel := context.WithTimeout(context.Background(), shutdownTimeout)
				defer waitCancel()
				select {
				case <-done:
					log.Info().Msg("all workers completed after cancellation")
				case <-waitCtx.Done():
					log.Error().Msg("timeout waiting for workers after cancellation")
					fmt.Fprintln(os.Stderr, "❌ Timeout waiting for operations to complete")
					return fmt.Errorf("graceful shutdown timeout: operations did not complete in time")
				}
			}

			close(results)

			var failed []string
			var agg []AggBlock
			var clusterFiles []struct{ Cluster, HTML, CSV string }
			var allResults []ClusterResult
			excludedByCluster := make(map[string][]ExcludedAlert)

			for r := range results {
				allResults = append(allResults, r)
				if r.Err != nil {
					failed = append(failed, r.Cluster)
					continue
				}
				if len(r.ExcludedByTitle) > 0 {
					excludedByCluster[r.Cluster] = append(excludedByCluster[r.Cluster], r.ExcludedByTitle...)
				}

				rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", r.Cluster))
				meta, _ := parseNCCHeader(rawPath) // ignore error

				for _, b := range r.Blocks {
					agg = append(agg, AggBlock{
						Cluster:        r.Cluster,
						Severity:       b.Severity,
						Check:          b.CheckName,
						Detail:         b.DetailRaw,
						ClusterName:    meta.ClusterName,
						ClusterVersion: meta.ClusterVersion,
						NCCVersion:     meta.NCCVersion,
					})
				}
				basePath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", r.Cluster))
				htmlPath := basePath + ".html"
				csvPath := basePath + ".csv"
				clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
					Cluster: r.Cluster,
					HTML:    filepath.Base(htmlPath),
					CSV:     filepath.Base(csvPath),
				})
			}

			runDuration := time.Since(runStart)
			indexPath := filepath.Join(cfg.OutputDirFiltered, "index.html")
			log.Info().
				Int("clusters_ok", len(clusterFiles)).
				Int("clusters_failed", len(failed)).
				Float64("duration_s", runDuration.Seconds()).
				Str("index_html", indexPath).
				Msg("run summary")

			exitCode := 0
			if len(failed) > 0 {
				if len(clusterFiles) > 0 {
					exitCode = 3
				} else {
					exitCode = 1
				}
			}
			perCluster := make([]RunClusterSummary, 0, len(allResults))
			for _, r := range allResults {
				perCluster = append(perCluster, buildRunClusterSummary(r))
			}
			avgHealth := 0
			minHealth := 100
			healthClusters := 0
			for _, c := range perCluster {
				if !c.OK {
					continue
				}
				avgHealth += c.HealthScore
				healthClusters++
				if c.HealthScore < minHealth {
					minHealth = c.HealthScore
				}
			}
			if healthClusters > 0 {
				avgHealth = int(math.Round(float64(avgHealth) / float64(healthClusters)))
			} else {
				minHealth = 0
			}

			runSummary := RunSummaryJSON{
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
				DurationS:      runDuration.Seconds(),
				ClustersOK:     len(clusterFiles),
				ClustersFailed: len(failed),
				FailedClusters: failed,
				Clusters:       perCluster,
				ExitCode:       exitCode,
				IndexHTML:      indexPath,
				TotalChecks:    len(agg),
				AvgHealthScore: avgHealth,
				MinHealthScore: minHealth,
				FailureClasses: failureClassCounts(allResults),
			}
			if err := writeRunSummaryJSON(fs, cfg.OutputDirFiltered, runSummary); err != nil {
				log.Error().Err(err).Msg("write run-summary.json failed (non-fatal)")
			}
			checksSnapshot := buildChecksSnapshot(allResults)
			if err := writeChecksSnapshotJSON(fs, cfg.OutputDirFiltered, checksSnapshot); err != nil {
				log.Error().Err(err).Msg("write checks-snapshot.json failed (non-fatal)")
			}
			if err := writeNCCRunRecordJSON(fs, cfg.OutputDirFiltered, runSummary); err != nil {
				log.Error().Err(err).Msg("write ncc-run-record.json failed (non-fatal)")
			}
			regression := computeRegressionSummary(previousSummary, hasPreviousSummary, runSummary)
			if err := writeRegressionSummaryJSON(fs, cfg.OutputDirFiltered, regression); err != nil {
				log.Error().Err(err).Msg("write regression-summary.json failed (non-fatal)")
			} else {
				log.Info().
					Bool("has_regression", regression.HasRegression).
					Int("delta_fail_total", regression.DeltaFailTotal).
					Msg("regression summary generated")
			}
			drillDownDiff := computeDrillDownDiff(previousChecksSnapshot, hasPreviousChecksSnapshot, checksSnapshot)
			if err := writeDrillDownDiffJSON(fs, cfg.OutputDirFiltered, drillDownDiff); err != nil {
				log.Error().Err(err).Msg("write drilldown-diff.json failed (non-fatal)")
			}
			historySnapshots, err := loadRecentCheckSnapshots(cfg.RunHistoryDir, cfg.FlakyLookbackRuns-1)
			if err != nil {
				log.Warn().Err(err).Str("history_dir", cfg.RunHistoryDir).Msg("load check snapshot history failed (non-fatal)")
				historySnapshots = nil
			}
			historySnapshots = append(historySnapshots, checksSnapshot)
			flaky := computeFlakyChecks(historySnapshots, cfg.FlakyMinTransitions)
			if err := writeFlakyChecksJSON(fs, cfg.OutputDirFiltered, flaky); err != nil {
				log.Error().Err(err).Msg("write flaky-checks.json failed (non-fatal)")
			}
			slo := buildSLODashboard(runSummary)
			if err := writeSLODashboardJSON(fs, cfg.OutputDirFiltered, slo); err != nil {
				log.Error().Err(err).Msg("write slo-dashboard.json failed (non-fatal)")
			}
			// Write index.html after run-summary/artifacts are written, so embedded RUN_SUMMARY
			// and artifact links in the page always represent the current run.
			if len(agg) > 0 {
				if err := writeAggregatedHTMLSingle(fs, cfg.OutputDirFiltered, agg, clusterFiles, cfg); err != nil {
					log.Error().Err(err).Msg("write aggregated HTML failed")
					return fmt.Errorf("write aggregated HTML: %w", err)
				}
			} else if len(failed) > 0 {
				if err := writeAllClustersFailedHTML(fs, cfg.OutputDirFiltered, failed); err != nil {
					log.Error().Err(err).Msg("write all-failed HTML failed (non-fatal)")
				}
			}
			if len(cfg.ExcludeAlertTitles) > 0 {
				if err := writeExcludedAlertsAuditJSON(fs, cfg.OutputDirFiltered, cfg.ExcludeAlertMatchMode, cfg.ExcludeAlertTitles, excludedByCluster); err != nil {
					log.Error().Err(err).Msg("write excluded-alerts.json failed (non-fatal)")
				}
			}
			if cfg.SingleReport {
				src := filepath.Join(cfg.OutputDirFiltered, "index.html")
				dst := filepath.Join(cfg.OutputDirFiltered, "ncc-report-single.html")
				if err := copyFile(src, dst); err != nil {
					log.Error().Err(err).Str("src", src).Str("dst", dst).Msg("single-report copy failed (non-fatal)")
				} else {
					log.Info().Str("file", dst).Msg("single-file report generated")
				}
			}
			if cfg.RunHistoryEnabled {
				if runDir, err := writeRunHistorySnapshot(cfg); err != nil {
					log.Error().Err(err).Str("history_dir", cfg.RunHistoryDir).Msg("write run history snapshot failed (non-fatal)")
				} else {
					log.Info().
						Str("history_dir", runDir).
						Int("retain_last", cfg.RetainLastRuns).
						Int("retain_days", cfg.RetainDays).
						Msg("run history snapshot created")
				}
			}
			if deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, time.Now()); err != nil {
				log.Error().Err(err).Msg("artifact retention failed (non-fatal)")
			} else if deleted > 0 {
				log.Info().Int("deleted", deleted).Int("retain_days", cfg.ArtifactRetainDays).Int("retain_max_files", cfg.ArtifactRetainMaxFiles).Msg("artifact retention applied")
			}

			if cfg.NotifyDigest {
				if cfg.NotifyOnRegression && !regression.HasRegression {
					log.Info().Msg("notify-on-regression enabled: skipping digest notification (no regression detected)")
				} else if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
					log.Info().Str("reason", reason).Msg("digest notifications suppressed by quiet-hours/maintenance-window")
				} else {
					digestCounts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
					for _, row := range agg {
						sev := strings.ToUpper(strings.TrimSpace(row.Severity))
						if sev == "" {
							sev = "INFO"
						}
						if _, ok := digestCounts[sev]; ok {
							digestCounts[sev]++
						} else {
							digestCounts["INFO"]++
						}
					}
					overview := fmt.Sprintf("Run completed in %s. Clusters OK: %d, Failed: %d. Alerts (filtered): FAIL=%d WARN=%d ERR=%d INFO=%d Total=%d.",
						runDuration.Round(time.Second), len(clusterFiles), len(failed),
						digestCounts["FAIL"], digestCounts["WARN"], digestCounts["ERR"], digestCounts["INFO"], len(agg))
					if len(failed) > 0 {
						overview += fmt.Sprintf(" Failed: %v.", failed)
					}
					subj := fmt.Sprintf("NCC Run: OK=%d FAIL=%d", len(clusterFiles), len(failed))
					bodyEmail := overview + "\n\nIndex report: " + indexPath
					attachPath := ""
					if cfg.EmailAttachHTML {
						if _, err := os.Stat(indexPath); err == nil {
							attachPath = indexPath
						}
					}
					if err := sendEmailWithRetry(cfg, subj, bodyEmail, attachPath); err != nil {
						log.Error().Err(err).Msg("digest email failed")
					}
					digestSummary := NotificationSummary{
						Cluster:     "run",
						StartedAt:   runStart,
						FinishedAt:  time.Now(),
						FailCount:   digestCounts["FAIL"],
						WarnCount:   digestCounts["WARN"],
						ErrCount:    digestCounts["ERR"],
						InfoCount:   digestCounts["INFO"],
						TotalChecks: len(agg),
						Overview:    overview,
					}
					if cfg.WebhookIncludeHTML {
						if b, err := os.ReadFile(indexPath); err == nil {
							digestSummary.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
						}
					}
					if err := sendWebhookWithRetry(ctx, httpc, cfg, digestSummary); err != nil {
						log.Error().Err(err).Msg("digest webhook failed")
					}
					if err := sendSlackWithRetry(ctx, httpc, cfg, digestSummary); err != nil {
						log.Error().Err(err).Msg("digest slack failed")
					}
				}
			}

			// Check if context was cancelled during execution
			if ctx.Err() != nil {
				log.Warn().Err(ctx.Err()).Msg("operation cancelled during execution")
				if len(failed) > 0 {
					return fmt.Errorf("operation cancelled: %d clusters failed: %v", len(failed), failed)
				}
				return fmt.Errorf("operation cancelled: %w", ctx.Err())
			}
			failureCounts := failureClassCounts(allResults)
			policyMetrics := map[string]float64{
				"new-fails":            float64(drillDownDiff.NewFailCount),
				"resolved-fails":       float64(drillDownDiff.ResolvedFailCount),
				"fail-rate":            0,
				"clusters-failed":      float64(len(failed)),
				"max-cluster-failures": float64(len(failed)),
				"regressions":          0,
				"flaky-checks":         float64(flaky.TotalFlakyChecks),
				"min-health-score":     float64(runSummary.MinHealthScore),
				"avg-health-score":     float64(runSummary.AvgHealthScore),
				"timeout-clusters":     float64(failureCounts["timeout"]),
				"auth-failures":        float64(failureCounts["auth"]),
				"network-failures":     float64(failureCounts["network"]),
				"api-failures":         float64(failureCounts["api"]),
				"parser-failures":      float64(failureCounts["parser"]),
				"rate-limit-failures":  float64(failureCounts["rate_limit"]),
				"unknown-failures":     float64(failureCounts["unknown"]),
			}
			if regression.HasRegression {
				policyMetrics["regressions"] = 1
			}
			if runSummary.TotalChecks > 0 {
				policyMetrics["fail-rate"] = (float64(regression.CurrentFailTotal) * 100.0) / float64(runSummary.TotalChecks)
			}
			if len(cfg.PolicyGates) > 0 {
				violations, err := evaluatePolicyGates(cfg.PolicyGates, policyMetrics)
				if err != nil {
					return exitConfig(fmt.Errorf("policy-gates: %w", err))
				}
				if len(violations) > 0 {
					_ = fs.WriteFile(filepath.Join(cfg.OutputDirFiltered, "policy-gates.txt"), []byte(strings.Join(violations, "\n")+"\n"), 0644)
					return fmt.Errorf("policy gate violations: %s", strings.Join(violations, "; "))
				}
			}

			if len(failed) > 0 {
				if len(clusterFiles) > 0 {
					log.Warn().Strs("failedClusters", failed).Int("succeeded", len(clusterFiles)).Msg("some clusters failed; aggregated report written for successful clusters")
					fmt.Fprintf(os.Stderr, "Some clusters failed: %v (report written for %d successful cluster(s)). Exit code 3.\n", failed, len(clusterFiles))
					return exitPartial(fmt.Errorf("some clusters failed: %v", failed))
				}
				log.Error().Strs("failedClusters", failed).Msg("all clusters failed")
				return fmt.Errorf("all clusters failed: %v", failed)
			}

			log.Info().Msg("all clusters processed successfully")
			fmt.Printf("All clusters processed successfully\n")
			return nil
		},
	}

	cmd.SilenceUsage = true

	cmd.PersistentFlags().String("nutanix-v4-api-version", defaultNutanixV4APIVersion, "Nutanix v4 REST API path revision for clustermgmt and monitoring (e.g. v4.2, v4.1, v4.0.a1)")
	_ = viper.BindPFlag("nutanix-v4-api-version", cmd.PersistentFlags().Lookup("nutanix-v4-api-version"))

	// flags
	cmd.Flags().BoolP("update", "u", false, "Fetch latest release from GitHub and update this binary if a matching asset exists")
	cmd.Flags().Bool("env-info", false, "Display possible environment variables and their current values")
	cmd.Flags().Bool("tc", false, "Display terms and conditions")
	cmd.Flags().String("config", "", "Config file path (yaml/json)")
	cmd.Flags().String("clusters", "", "Comma-separated cluster IPs or FQDNs")
	cmd.Flags().String("clusters-file", "", "Path to cluster file (cluster or cluster,username[,password] per line; overrides clusters when set)")
	cmd.Flags().String("username", "admin", "Username for Prism Gateway")
	cmd.Flags().String("password", "", "Password (omit to be prompted)")
	cmd.Flags().String("ncc-api-version", "v4", "NCC API mode: v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias); use --nutanix-v4-api-version for v4.2 vs v4.0.a1 etc.")
	cmd.Flags().Bool("insecure-skip-verify", false, "Skip TLS verify (only for trusted labs)")
	cmd.Flags().String("timeout", "15m", "Overall per-cluster timeout")
	cmd.Flags().String("request-timeout", "20s", "Per-request timeout")
	cmd.Flags().String("poll-interval", "15s", "Polling interval for task status")
	cmd.Flags().String("poll-jitter", "2s", "Additive jitter to polling interval")
	cmd.Flags().Int("max-parallel", 4, "Max concurrent clusters")
	cmd.Flags().String("outputs", "html,csv", "Comma-separated outputs: html,csv,json,markdown,sarif for per-cluster files")
	cmd.Flags().String("output-dir-logs", "nccfiles", "Directory for raw logs")
	cmd.Flags().String("output-dir-filtered", "outputfiles", "Directory for filtered and aggregated results")
	cmd.Flags().Bool("single-report", false, "Also write a single-file report copy at output-dir-filtered/ncc-report-single.html")
	cmd.Flags().String("severity-filter", "", "Comma-separated severities to include (FAIL,WARN,ERR,INFO). Empty = all")
	cmd.Flags().String("exclude-alert-titles", "", "Comma-separated NCC alert titles to exclude from generated reports/HTML")
	cmd.Flags().String("exclude-alert-titles-file", "", "Path to file containing alert titles to exclude (one title per line)")
	cmd.Flags().String("exclude-alert-match-mode", defaultExcludeMatchMode, "Alert title exclusion match mode: exact, contains, regex")
	cmd.Flags().Bool("dry-run", false, "Validate configuration without running checks")
	cmd.Flags().String("log-file", "logs/ncc-runner.log", "Path to log file (rotated)")
	cmd.Flags().String("log-level", "", "Log level (trace/debug/info/warn/error or 0..5)")
	cmd.Flags().Bool("log-http", false, "Enable HTTP request/response dump logs")
	cmd.Flags().Int("retry-max-attempts", 6, "Max retry attempts for HTTP calls")
	cmd.Flags().String("retry-base-delay", "400ms", "Base retry delay (with jitter, exponential)")
	cmd.Flags().String("retry-max-delay", "8s", "Max retry delay cap")
	cmd.Flags().Int("retry-circuit-breaker", defaultRetryCircuitBreaker, "Fail fast after N consecutive retryable failures")
	cmd.Flags().Bool("replay", false, "Replay from existing logs without running NCC")
	cmd.Flags().Int("gen-test-agg", 0, "Generate a test index.html with N clusters for scalability testing (no API calls)")
	cmd.Flags().String("prom-dir", "promfiles", "Directory for Prometheus metrics")
	cmd.Flags().Bool("run-history", false, "Store each run snapshot in run-history-dir")
	cmd.Flags().String("run-history-dir", "", "Run history directory (default: <output-dir-filtered>/runs)")
	cmd.Flags().Int("retain-last", 0, "When run-history is enabled, keep only the last N runs (0 = unlimited)")
	cmd.Flags().Int("retain-days", 0, "When run-history is enabled, keep runs newer than N days (0 = unlimited)")
	cmd.Flags().Int("artifact-retain-days", 0, "Remove generated artifacts older than N days from output-dir-filtered (0 = unlimited)")
	cmd.Flags().Int("artifact-retain-max-files", 0, "Keep only N newest generated artifacts in output-dir-filtered (0 = unlimited)")
	cmd.Flags().Bool("notify-on-regression", false, "Only send notifications when FAIL count increases vs previous run-summary")
	cmd.Flags().Bool("adaptive-parallelism", true, "Dynamically reduce/increase effective concurrency based on HTTP 429 responses")
	cmd.Flags().String("policy-gates", "", "Comma-separated policy gates (e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0)")
	cmd.Flags().String("quiet-hours", "", "Suppress notifications during local quiet hours, format HH:MM-HH:MM")
	cmd.Flags().String("maintenance-windows", "", "Suppress notifications during RFC3339 windows: start/end[,start/end...]")
	cmd.Flags().Int("flaky-lookback-runs", defaultFlakyLookbackRuns, "Number of recent runs to inspect for flaky check detection")
	cmd.Flags().Int("flaky-min-transitions", defaultFlakyTransitions, "Minimum severity transitions to mark a check as flaky")
	cmd.Flags().Bool("email-enabled", false, "Enable email notifications")
	cmd.Flags().Bool("email-attach-html", false, "Attach per-cluster (or digest) HTML report to notification email")
	cmd.Flags().Bool("notify-digest", false, "Send one email/webhook/slack per run with run overview (and optional index.html attach) instead of per-cluster")
	cmd.Flags().String("smtp-server", "", "SMTP server (smtp.gmail.com)")
	cmd.Flags().String("smtp-port", "587", "SMTP port (587=STARTTLS, 465=SSL)")
	cmd.Flags().String("smtp-user", "", "SMTP username")
	cmd.Flags().String("smtp-password", "", "SMTP password (use env NCC_SMTP_PASSWORD)")
	cmd.Flags().String("email-from", "", "From email address")
	cmd.Flags().String("email-to", "", "Comma-separated recipient emails")
	cmd.Flags().Bool("email-use-tls", true, "Use STARTTLS (recommended)")
	cmd.Flags().Bool("webhook-enabled", false, "Enable webhook notifications")
	cmd.Flags().Bool("webhook-include-html", false, "Include per-cluster HTML report as base64 in webhook JSON payload")
	cmd.Flags().String("webhook-url", "", "Webhook endpoint URL")
	cmd.Flags().StringToString("webhook-headers", map[string]string{}, "Webhook headers (key=value)")
	cmd.Flags().Bool("slack-enabled", false, "Enable Slack notifications")
	cmd.Flags().String("slack-webhook-url", "", "Slack webhook URL")
	cmd.Flags().String("slack-channel", "", "Slack channel (optional, uses webhook default if empty)")
	cmd.Flags().String("secrets-provider", "", "Secret source for secret:// refs: env or file")
	cmd.Flags().String("secrets-file", "", "Path to YAML/JSON secrets map when secrets-provider=file")

	// viper bindings
	_ = viper.BindPFlag("update", cmd.Flags().Lookup("update"))
	_ = viper.BindPFlag("config", cmd.Flags().Lookup("config"))
	_ = viper.BindPFlag("clusters", cmd.Flags().Lookup("clusters"))
	_ = viper.BindPFlag("clusters-file", cmd.Flags().Lookup("clusters-file"))
	_ = viper.BindPFlag("username", cmd.Flags().Lookup("username"))
	_ = viper.BindPFlag("password", cmd.Flags().Lookup("password"))
	_ = viper.BindPFlag("ncc-api-version", cmd.Flags().Lookup("ncc-api-version"))
	_ = viper.BindPFlag("insecure-skip-verify", cmd.Flags().Lookup("insecure-skip-verify"))
	_ = viper.BindPFlag("timeout", cmd.Flags().Lookup("timeout"))
	_ = viper.BindPFlag("request-timeout", cmd.Flags().Lookup("request-timeout"))
	_ = viper.BindPFlag("poll-interval", cmd.Flags().Lookup("poll-interval"))
	_ = viper.BindPFlag("poll-jitter", cmd.Flags().Lookup("poll-jitter"))
	_ = viper.BindPFlag("max-parallel", cmd.Flags().Lookup("max-parallel"))
	_ = viper.BindPFlag("outputs", cmd.Flags().Lookup("outputs"))
	_ = viper.BindPFlag("output-dir-logs", cmd.Flags().Lookup("output-dir-logs"))
	_ = viper.BindPFlag("output-dir-filtered", cmd.Flags().Lookup("output-dir-filtered"))
	_ = viper.BindPFlag("single-report", cmd.Flags().Lookup("single-report"))
	_ = viper.BindPFlag("gen-test-agg", cmd.Flags().Lookup("gen-test-agg"))
	_ = viper.BindPFlag("log-file", cmd.Flags().Lookup("log-file"))
	_ = viper.BindPFlag("log-level", cmd.Flags().Lookup("log-level"))
	_ = viper.BindPFlag("log-http", cmd.Flags().Lookup("log-http"))
	_ = viper.BindPFlag("retry-max-attempts", cmd.Flags().Lookup("retry-max-attempts"))
	_ = viper.BindPFlag("retry-base-delay", cmd.Flags().Lookup("retry-base-delay"))
	_ = viper.BindPFlag("retry-max-delay", cmd.Flags().Lookup("retry-max-delay"))
	_ = viper.BindPFlag("retry-circuit-breaker", cmd.Flags().Lookup("retry-circuit-breaker"))
	_ = viper.BindPFlag("replay", cmd.Flags().Lookup("replay"))
	_ = viper.BindPFlag("prom-dir", cmd.Flags().Lookup("prom-dir"))
	_ = viper.BindPFlag("run-history", cmd.Flags().Lookup("run-history"))
	_ = viper.BindPFlag("run-history-dir", cmd.Flags().Lookup("run-history-dir"))
	_ = viper.BindPFlag("retain-last", cmd.Flags().Lookup("retain-last"))
	_ = viper.BindPFlag("retain-days", cmd.Flags().Lookup("retain-days"))
	_ = viper.BindPFlag("artifact-retain-days", cmd.Flags().Lookup("artifact-retain-days"))
	_ = viper.BindPFlag("artifact-retain-max-files", cmd.Flags().Lookup("artifact-retain-max-files"))
	_ = viper.BindPFlag("notify-on-regression", cmd.Flags().Lookup("notify-on-regression"))
	_ = viper.BindPFlag("adaptive-parallelism", cmd.Flags().Lookup("adaptive-parallelism"))
	_ = viper.BindPFlag("policy-gates", cmd.Flags().Lookup("policy-gates"))
	_ = viper.BindPFlag("quiet-hours", cmd.Flags().Lookup("quiet-hours"))
	_ = viper.BindPFlag("maintenance-windows", cmd.Flags().Lookup("maintenance-windows"))
	_ = viper.BindPFlag("flaky-lookback-runs", cmd.Flags().Lookup("flaky-lookback-runs"))
	_ = viper.BindPFlag("flaky-min-transitions", cmd.Flags().Lookup("flaky-min-transitions"))
	_ = viper.BindPFlag("email-enabled", cmd.Flags().Lookup("email-enabled"))
	_ = viper.BindPFlag("email-attach-html", cmd.Flags().Lookup("email-attach-html"))
	_ = viper.BindPFlag("notify-digest", cmd.Flags().Lookup("notify-digest"))
	_ = viper.BindPFlag("smtp-server", cmd.Flags().Lookup("smtp-server"))
	_ = viper.BindPFlag("smtp-port", cmd.Flags().Lookup("smtp-port"))
	_ = viper.BindPFlag("smtp-user", cmd.Flags().Lookup("smtp-user"))
	_ = viper.BindPFlag("smtp-password", cmd.Flags().Lookup("smtp-password"))
	_ = viper.BindPFlag("email-from", cmd.Flags().Lookup("email-from"))
	_ = viper.BindPFlag("email-to", cmd.Flags().Lookup("email-to"))
	_ = viper.BindPFlag("email-use-tls", cmd.Flags().Lookup("email-use-tls"))
	_ = viper.BindPFlag("webhook-enabled", cmd.Flags().Lookup("webhook-enabled"))
	_ = viper.BindPFlag("webhook-include-html", cmd.Flags().Lookup("webhook-include-html"))
	_ = viper.BindPFlag("webhook-url", cmd.Flags().Lookup("webhook-url"))
	_ = viper.BindPFlag("webhook-headers", cmd.Flags().Lookup("webhook-headers"))
	_ = viper.BindPFlag("severity-filter", cmd.Flags().Lookup("severity-filter"))
	_ = viper.BindPFlag("exclude-alert-titles", cmd.Flags().Lookup("exclude-alert-titles"))
	_ = viper.BindPFlag("exclude-alert-titles-file", cmd.Flags().Lookup("exclude-alert-titles-file"))
	_ = viper.BindPFlag("exclude-alert-match-mode", cmd.Flags().Lookup("exclude-alert-match-mode"))
	_ = viper.BindPFlag("dry-run", cmd.Flags().Lookup("dry-run"))
	_ = viper.BindPFlag("slack-enabled", cmd.Flags().Lookup("slack-enabled"))
	_ = viper.BindPFlag("slack-webhook-url", cmd.Flags().Lookup("slack-webhook-url"))
	_ = viper.BindPFlag("slack-channel", cmd.Flags().Lookup("slack-channel"))
	_ = viper.BindPFlag("secrets-provider", cmd.Flags().Lookup("secrets-provider"))
	_ = viper.BindPFlag("secrets-file", cmd.Flags().Lookup("secrets-file"))

	// discover-clusters subcommand: Prism Central cluster list (default v4 clustermgmt API)
	discoverCmd := &cobra.Command{
		Use:   "discover-clusters",
		Short: "List clusters from Prism Central (v4 API by default)",
		Long: `Lists registered clusters from Prism Central. Default output is one address per line (IPv4 preferred).

Use --format table for NAME, EXT_ID, ADDRESS, API columns; --format json for machine-readable rows.

Default: GET /api/clustermgmt/{nutanix-v4-api-version}/config/clusters with pagination ($page, $limit); use global --nutanix-v4-api-version (default v4.2) to match your PC API (e.g. v4.1, v4.0.a1).
Use --discover-api-version v3 for legacy POST /api/nutanix/v3/clusters/list.

If v4 returns HTTP 404, the command falls back to v3 automatically.

Use --output to write to a file (e.g. for --clusters-file).`,
		RunE: runDiscoverClusters,
	}
	discoverCmd.Flags().String("prism-central-url", "", "Prism Central URL (e.g. https://10.0.0.1:9440)")
	discoverCmd.Flags().String("username", "admin", "Prism username")
	discoverCmd.Flags().String("password", "", "Prism password (or NCC_PASSWORD)")
	discoverCmd.Flags().String("output", "", "Write cluster list to file (one per line)")
	discoverCmd.Flags().String("format", "lines", "Output format: lines (address per line), table (NAME EXT_ID ADDRESS API), or json")
	discoverCmd.Flags().Bool("insecure-skip-verify", false, "Skip TLS verify for Prism Central")
	discoverCmd.Flags().String("discover-api-version", "v4", "Cluster list API: v4 (GET clustermgmt) or v3 (legacy POST); v4 path uses --nutanix-v4-api-version")
	_ = viper.BindPFlag("prism-central-url", discoverCmd.Flags().Lookup("prism-central-url"))
	// Do not BindPFlag username, password, or insecure-skip-verify here — they share keys
	// with the root command; the second bind would override viper and break the main run.
	_ = viper.BindPFlag("discover-api-version", discoverCmd.Flags().Lookup("discover-api-version"))
	cmd.AddCommand(discoverCmd)

	createScheduleCmd := &cobra.Command{
		Use:   "create-schedule",
		Short: "Create periodic scheduler for ncc-orchestrator",
		Long: `Creates a periodic schedule to run ncc-orchestrator.

On Linux/macOS, it installs (or replaces) a crontab entry.
On Windows, it creates or updates a Scheduled Task.

Examples:
  ncc-orchestrator create-schedule --type cron --cron "15 */4 * * *" --config config.yaml --print-only
  ncc-orchestrator create-schedule --type cron --every 4h --config config.yaml
  ncc-orchestrator create-schedule --type windows --every 4h --config C:\ncc\config.yaml
  ncc-orchestrator create-schedule --type auto --action list
  ncc-orchestrator create-schedule --type auto --action remove --print-only=false`,
		RunE: runCreateSchedule,
	}
	createScheduleCmd.Flags().String("type", "auto", "Scheduler type: auto, cron, or windows")
	createScheduleCmd.Flags().String("action", "create", "Action: create, list, remove, or run-now")
	createScheduleCmd.Flags().String("task-name", "ncc-orchestrator", "Schedule/task name marker")
	createScheduleCmd.Flags().String("config", "", "Config file path passed to ncc-orchestrator --config")
	createScheduleCmd.Flags().String("command", "", "Override full command used by scheduler (advanced)")
	createScheduleCmd.Flags().String("cron", "", "Cron expression (for --type cron). If empty, derived from --every")
	createScheduleCmd.Flags().Duration("every", 4*time.Hour, "Periodic interval used for cron derivation or Windows schedule (e.g. 30m, 4h, 24h)")
	createScheduleCmd.Flags().String("log-path", "logs/ncc-scheduler.log", "Log file path for cron redirect")
	createScheduleCmd.Flags().Bool("print-only", true, "Preview action without applying changes (used by create/remove)")
	cmd.AddCommand(createScheduleCmd)

	configSchemaCmd := &cobra.Command{
		Use:   "config-schema",
		Short: "Print JSON schema for config.yaml",
		RunE:  runConfigSchema,
	}
	configSchemaCmd.Flags().String("output", "", "Write schema JSON to file (default: stdout)")
	cmd.AddCommand(configSchemaCmd)

	validateConfigCmd := &cobra.Command{
		Use:   "validate-config",
		Short: "Validate configuration file and exit",
		RunE:  runValidateConfigCommand,
	}
	validateConfigCmd.Flags().String("config", "", "Config file path (yaml/json)")
	cmd.AddCommand(validateConfigCmd)

	validateSecretsCmd := &cobra.Command{
		Use:   "validate-secrets",
		Short: "Validate secret:// references and secret source accessibility",
		RunE:  runValidateSecretsCommand,
	}
	validateSecretsCmd.Flags().String("config", "", "Config file path (yaml/json)")
	cmd.AddCommand(validateSecretsCmd)

	return cmd
}

func main() {
	// Ensure logs are flushed on exit
	defer func() {
		// Give logger time to flush
		time.Sleep(100 * time.Millisecond)
	}()

	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		log.Error().Err(err).Msg("application error")
		code := 1
		var exitErr *exitCodeError
		if errors.As(err, &exitErr) {
			code = exitErr.code
		}
		os.Exit(code)
	}
	os.Exit(0)
}
