// Package model holds the foundational, dependency-free domain types shared
// across the orchestrator and its extracted subsystems (notifications,
// Prometheus textfile metrics, parser). It is a leaf package: it imports only
// the standard library so any internal/* package can depend on it without
// creating an import cycle back into package main.
//
// Package main re-exports these via type aliases (e.g. `type Config =
// model.Config`) so existing call sites continue to compile unchanged while
// the canonical definitions live here.
package model

import (
	"math"
	"net/http"
	"os"
	"time"
)

// Config is the fully-resolved runtime configuration for an NCC run.
type Config struct {
	Clusters           []string
	ClustersFile       string                       // Optional: cluster file; lines are cluster or cluster,username[,password] (overrides/supplements clusters when set)
	ClusterCredentials map[string]ClusterCredential `mapstructure:"-"`
	ClusterSourceMode  string                       `mapstructure:"cluster-source-mode"` // clusters (default) or pc
	PCs                []string                     `mapstructure:"-"`
	PCsFile            string                       `mapstructure:"pcs-file"` // Optional: one Prism Central IP/FQDN/URL per line
	PrismCentralURL    string                       `mapstructure:"prism-central-url"`
	DiscoverAPIVersion string                       `mapstructure:"discover-api-version"` // v4 (default) or v3 for PC cluster discovery
	Username           string
	Password           string
	InsecureSkipVerify bool
	// CABundle is an optional path to a PEM file of additional trusted CA
	// certificates (e.g. an internal Prism CA), a safer alternative to
	// InsecureSkipVerify.
	CABundle string `mapstructure:"ca-bundle"`
	// PinSHA256 is an optional set of allowed server leaf-certificate SHA-256
	// fingerprints (hex, with or without colons). When set, the server cert is
	// accepted only if its fingerprint matches one of these (certificate
	// pinning), independent of the system trust store.
	PinSHA256         []string      `mapstructure:"-"`
	Timeout           time.Duration // per-cluster overall timeout
	RequestTimeout    time.Duration // per HTTP request timeout
	PollInterval      time.Duration
	PollJitter        time.Duration
	OutputDirLogs     string
	OutputDirFiltered string
	OutputFormats     []string // html,csv,json
	MaxParallel       int
	TLSMinVersion     uint16
	LogFile           string

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
	PromEnabled bool   `mapstructure:"prom-enabled"`
	PromDir     string `mapstructure:"prom-dir"`

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
	// SMTPInsecureSkipVerify skips SMTP STARTTLS certificate verification,
	// decoupled from the Prism InsecureSkipVerify flag.
	SMTPInsecureSkipVerify bool `mapstructure:"smtp-insecure-skip-verify"`
	// Optional Go text/template overrides for the email subject/body. Empty
	// = built-in default. Rendered against NotificationSummary (.Cluster,
	// .FailCount, .WarnCount, .ErrCount, .InfoCount, .TotalChecks, .Overview,
	// .StartedAt, .FinishedAt, .OutputFiles).
	EmailSubjectTemplate string `mapstructure:"email-subject-template"`
	EmailBodyTemplate    string `mapstructure:"email-body-template"`

	// Webhook
	WebhookEnabled     bool
	WebhookIncludeHTML bool // Include per-cluster HTML report as base64 in webhook payload
	WebhookURL         string
	WebhookHeaders     map[string]string `mapstructure:"webhook-headers"`
	// Optional Go text/template override for the webhook request body. Empty
	// = default JSON encoding of NotificationSummary. The rendered output is
	// sent verbatim (the operator is responsible for producing valid JSON).
	WebhookTemplate string `mapstructure:"webhook-template"`
	// WebhookSecret, when set, makes the orchestrator sign the webhook body
	// with HMAC-SHA256 and send it as the X-NCC-Signature header
	// ("sha256=<hex>") so the receiver can verify provenance.
	WebhookSecret string `mapstructure:"webhook-secret"`

	// NotificationDeadLetterDir, when set, is a directory where notification
	// payloads that fail to deliver (after retries) are written so a transient
	// SMTP/webhook/Slack outage does not silently lose the alert.
	NotificationDeadLetterDir string `mapstructure:"notification-deadletter-dir"`

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

// ClusterCredential is an optional per-cluster username/password override.
type ClusterCredential struct {
	Username string
	Password string
}

// NotificationSummary is the per-cluster (or per-run digest) payload used for
// email/webhook/slack notifications and as the data context for notification
// templates.
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

// ParsedBlock is a single parsed NCC finding.
type ParsedBlock struct {
	Severity  string
	CheckName string
	DetailRaw string
}

// HTTPClient is the minimal HTTP surface used by the notification and NCC
// clients, so callers can inject a stub in tests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// FS abstracts the small set of filesystem operations the report/metrics
// writers need, so tests can supply an in-memory implementation.
type FS interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(path string, data []byte, perm os.FileMode) error
	ReadFile(path string) ([]byte, error)
	ReadDir(path string) ([]os.DirEntry, error)
	Create(path string) (*os.File, error)
}

// OSFS is the os-backed implementation of FS used in production.
type OSFS struct{}

func (OSFS) MkdirAll(path string, perm os.FileMode) error { return os.MkdirAll(path, perm) }
func (OSFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}
func (OSFS) ReadFile(path string) ([]byte, error)       { return os.ReadFile(path) }
func (OSFS) ReadDir(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }
func (OSFS) Create(path string) (*os.File, error)       { return os.Create(path) }

// ClusterHealthScore maps a run's severity counts to a 0-100 health score.
// FAIL is weighted heaviest, then ERR, then WARN; INFO does not penalize.
func ClusterHealthScore(failCount, warnCount, errCount, total int) int {
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
