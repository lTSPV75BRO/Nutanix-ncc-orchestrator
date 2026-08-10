package main

import (
	"context"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"io/fs"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	yaml "go.yaml.in/yaml/v3"
	"goncc/internal/promtext"
	"goncc/internal/v2layout"
)

// Build-time metadata. These are set via -ldflags at link time, e.g.:
//
//	go build -ldflags "-X main.Version=2.0.0 -X main.BuildDate=2026-05-21T12:34:56Z \
//	  -X main.Stream=Release -X main.GoVersion=go1.22" ./cmd/ncc-api-server
//
// They are surfaced on /api/v1/health so support teams can see the exact build
// the API server is running.
var (
	Version     string
	BuildDate   string
	Stream      string
	GoVersion   string
	GitRevision string
)

func init() {
	if Version == "" {
		Version = "2.1.0"
	}
	if BuildDate == "" {
		BuildDate = "unknown"
	}
	if Stream == "" {
		Stream = "dev"
	}
	if GoVersion == "" {
		GoVersion = runtime.Version()
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range bi.Settings {
			switch setting.Key {
			case "vcs.revision":
				if GitRevision == "" {
					GitRevision = setting.Value
				}
			case "vcs.time":
				if BuildDate == "unknown" {
					BuildDate = setting.Value
				}
			}
		}
	}
}

type apiServer struct {
	repoRoot                string
	configPath              string
	outputDir               string
	logDir                  string
	runnerLogPath           string
	scheduleStatePath       string
	backupScheduleStatePath string
	notificationStatePath   string
	auditLogPath            string
	auditLogMaxBytes        int64
	auditMu                 sync.Mutex
	// SIEM/syslog audit forwarding (flag-configured; nil when disabled).
	auditForwardHTTPURL    string
	auditForwardHTTPAuth   string
	auditForwardHTTPSplunk bool
	auditForwardSyslog     string
	auditForwardSyslogNet  string
	auditForwarder         *auditForwarder
	loginLockThreshold     int           // failed logins before per-account lockout (0 disables)
	loginLockWindow        time.Duration // window to accumulate failures
	loginLockDuration      time.Duration // how long a locked account stays locked
	loginGuard             *loginGuard
	orchestratorBin        string
	authToken              string
	// viewerToken, when set, grants a read-only role: holders can reach
	// safe GET endpoints but are denied settings/* and all mutating
	// requests (those require the full authToken / a session). When empty,
	// RBAC is disabled and the single authToken keeps full access
	// (backward compatible).
	viewerToken   string
	tokenFilePath string
	corsOrigin    string
	authMode      string
	// Interactive login: local accounts (users file) and/or SAML SSO. When
	// either is configured, browser cookie sessions carrying a role are
	// honored and the UI shows a login screen. Static tokens keep working
	// for automation.
	usersFilePath    string // optional read-only YAML seed (--users-file)
	usersDBPath      string // writable JSON file store (--users-db)
	usersDBSecret    string // Kubernetes Secret name backing the store (--users-db-secret)
	usersDBSecretKey string // data key inside that Secret (--users-db-secret-key)
	usersDBSecretNS  string // namespace override (--users-db-secret-namespace)
	// usersDBKeyFile points at a file holding a 32-byte master key (base64/hex)
	// used to envelope-encrypt the user store at rest. The NCC_MASTER_KEY env
	// takes precedence. Empty (and env unset) => plaintext store (default).
	usersDBKeyFile string
	// userStoreEncrypted records whether the file-backed user store is wrapped
	// with envelope encryption (a master key was configured). Surfaced on
	// /api/v1/health so operators can confirm secrets-at-rest is active.
	userStoreEncrypted bool
	// disableLocalAccounts opts out of the default-on user database. By
	// default (no flag, env, secret, or stack path) the server falls back to a
	// writable JSON file in the repo root so a bare run still bootstraps a
	// first-run admin and enables login. Set --disable-local-accounts (or
	// NCC_DISABLE_LOCAL_ACCOUNTS=1) for pure token-only automation.
	disableLocalAccounts bool
	users                *userDB
	samlEnabled          bool
	samlFromFlags        bool // SAML came from startup flags (not runtime-editable)
	samlMu               sync.RWMutex
	saml                 *samlProvider
	ldapEnabled          bool
	ldapFromFlags        bool // LDAP came from startup flags (not runtime-editable)
	ldapMu               sync.RWMutex
	ldap                 ldapAuthenticator
	// pcCacheMu guards the Prism Central -> managed-clusters discovery cache used
	// to expand cluster-group PC entries into their registered clusters.
	pcCacheMu  sync.Mutex
	pcCache    map[string]*pcCacheEntry
	pcInflight map[string]bool
	// cookieInsecure forces the Secure attribute OFF on session cookies even
	// when HTTPS is enabled (useful only behind a TLS-terminating proxy that
	// re-presents plain http to the stack). Insecure is already the default.
	cookieInsecure bool
	// cookieSecureForce forces the Secure attribute ON regardless of the
	// runtime HTTPS policy, for deployments that terminate TLS in front of the
	// stack (reverse proxy / load balancer) yet still want Secure cookies.
	cookieSecureForce  bool
	sessionSecret      string
	sessionTTL         time.Duration
	sessionIssuer      string
	runTimeout         time.Duration
	debugExpose        bool
	tlsCertFile        string
	tlsKeyFile         string
	tlsClientCAFile    string
	rateLimitPerMinute int
	rateLimiter        *fixedWindowRateLimiter
	adminResetMu       sync.Mutex              // guards lazy init of adminResetLimiter
	adminResetLimiter  *fixedWindowRateLimiter // per-IP cooldown for admin self-reset (GC-evicted)
	readTimeout        time.Duration
	writeTimeout       time.Duration
	idleTimeout        time.Duration
	metricsPublic      bool      // when true, /metrics bypasses token auth
	startedAt          time.Time // server boot timestamp (set in main)

	// Lifecycle counters surfaced on /metrics. Counters are
	// monotonic; they're cleared only by a process restart, which
	// matches Prometheus's expectation that counters reset to 0
	// on target restart.
	runsTriggeredTotal atomic.Int64
	runsCompletedTotal atomic.Int64
	runsFailedTotal    atomic.Int64
	// runAutoRetriesTotal counts self-heal auto-retries triggered after a run
	// failed with a recoverable class (rate-limit, timeout, network).
	runAutoRetriesTotal atomic.Int64
	// runAutoRetryDisabled turns off the single bounded auto-retry (--disable-run-auto-retry).
	runAutoRetryDisabled bool

	// Periodic self-heal: when selfHealInterval > 0 a background loop runs the
	// orchestrator's `doctor` checks on a timer, caching the report for the
	// diagnostics endpoint and metrics. When selfHealAutoFix is set the loop
	// passes --fix so safe remediations (path anchoring, missing dirs, secret
	// perms, config repair) are applied unattended.
	selfHealInterval   time.Duration
	selfHealAutoFix    bool
	selfHealRunMu      sync.Mutex
	selfHealMu         sync.RWMutex
	lastSelfHeal       *selfHealReport
	selfHealRunsTotal  atomic.Int64
	selfHealFixesTotal atomic.Int64
	// prevSelfHealFail holds the failing-check count from the previous self-heal
	// cycle so we alert only on a healthy->failing transition (no per-cycle spam).
	prevSelfHealFail atomic.Int64
	// Notification throttle state (in-memory): last send time per event and the
	// last send of any event, used by the dedup / min-interval controls.
	notifThrottleMu sync.Mutex
	notifLastSent   map[string]time.Time
	notifLastAny    time.Time
	// Authentication counters (interactive login: local + LDAP). SAML ACS has
	// its own path and is not counted here.
	loginSuccessTotal atomic.Int64
	loginFailureTotal atomic.Int64
	lockoutTotal      atomic.Int64 // accounts newly locked by the brute-force guard
	// In-app software-update outcomes.
	updateAppliedTotal atomic.Int64
	updateFailedTotal  atomic.Int64
	// Cached result of the most recent `update --check` (populated when the UI
	// polls GET /api/v1/settings/update?check=1). Lets /metrics expose
	// update-availability without running the ~40s subprocess at scrape time.
	updateCheckMu        sync.RWMutex
	updateCheckAt        time.Time
	updateCheckAvailable bool
	updateLatestVersion  string
	// Run-duration accumulator (sum is milliseconds to stay integer-atomic;
	// exposed as ncc_run_duration_seconds_{sum,count} so Prometheus can derive
	// rate()/avg over time, and the avg backs the UI queue-ETA estimate).
	runDurationMillisSum atomic.Int64
	runDurationCount     atomic.Int64
	lastRunDurationMs    atomic.Int64

	mu        sync.Mutex
	active    bool
	started   time.Time
	lastErr   string
	lastOut   string
	lastCfg   string
	lastCmd   []string
	lastCwd   string
	lastEnv   map[string]string
	lastPID   int
	cancelRun context.CancelFunc
	cancelled bool
	liveOut   *tailBuffer

	// Concurrent run engine. Multiple cluster-group runs can execute at once,
	// each scoped to a disjoint cluster subset (overlapping clusters are run
	// only once and shared). All fields below are guarded by s.mu.
	//
	//   runs          - every run that is queued or running, keyed by run id.
	//   runOrder      - run ids in submission order (stable listing).
	//   clusterOwners - normalized cluster name -> id of the run that owns it
	//                   (queued or running). Used to de-duplicate overlapping
	//                   clusters across concurrent triggers.
	//   runQueue      - ids of runs waiting for a concurrency slot (FIFO).
	//   wildcardRunID - id of an in-flight "run everything" (admin, no subset)
	//                   run; while set, it owns every cluster implicitly.
	maxConcurrentRuns int
	runs              map[string]*runRecord
	runOrder          []string
	clusterOwners     map[string]string
	runQueue          []string
	wildcardRunID     string
	runSeq            int64
	mergeMu           sync.Mutex // serializes merge-into-canonical of per-run artifacts
}

type envelope struct {
	Success   bool        `json:"success"`
	Message   string      `json:"message,omitempty"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
	ErrorCode string      `json:"error_code,omitempty"`
}

type routeMeta struct {
	Path        string   `json:"path"`
	Methods     []string `json:"methods"`
	Description string   `json:"description,omitempty"`
	SampleBody  string   `json:"sample_body,omitempty"`
	// MinRole is the least-privileged role that may call the route (the most
	// restrictive across its methods), surfaced so the API explorer can show
	// what access each endpoint needs. Populated by apiRouteCatalog.
	MinRole string `json:"min_role,omitempty"`
}

// routeRequiredRole returns the most restrictive minimum role across a route's
// methods (e.g. a GET+PUT settings route reports "admin" because the PUT is
// admin-only), as a display string ("viewer"/"operator"/"admin").
func routeRequiredRole(path string, methods []string) string {
	max := RoleViewer
	for _, m := range methods {
		isRead := m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
		if rr := routeMinRoleFor(path, isRead); rr > max {
			max = rr
		}
	}
	return max.String()
}

type configUpdateRequest struct {
	Path    string `json:"path,omitempty"`
	Content string `json:"content"`
}

type runConfigPreferenceRequest struct {
	Path string `json:"path,omitempty"`
}

type availableConfigFile struct {
	Path     string `json:"path"`
	Resolved string `json:"resolved"`
	Exists   bool   `json:"exists"`
	IsActive bool   `json:"is_active,omitempty"`
}

type configBatchOperation struct {
	Action  string `json:"action"`
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
}

type configBatchRequest struct {
	Operations []configBatchOperation `json:"operations"`
}

type configRelatedFile struct {
	Key          string `json:"key"`
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path"`
	Exists       bool   `json:"exists"`
	Size         int64  `json:"size,omitempty"`
}

type configRelatedFileUpdateRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type configRelatedFileBatchOperation struct {
	Action  string `json:"action"`
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
}

type configRelatedFileBatchRequest struct {
	Operations []configRelatedFileBatchOperation `json:"operations"`
}

type scheduleState struct {
	Type      string `json:"type"`
	Action    string `json:"action"`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	Config    string `json:"config"`
	LogPath   string `json:"log_path,omitempty"`
	WithLock  bool   `json:"with_lock,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly bool   `json:"print_only"`
	UpdatedAt string `json:"updated_at"`
}

type scheduleUpdateRequest struct {
	Type      string `json:"type"`
	Action    string `json:"action"`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	Config    string `json:"config,omitempty"`
	LogPath   string `json:"log_path,omitempty"`
	WithLock  *bool  `json:"with_lock,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly *bool  `json:"print_only,omitempty"`
	Apply     bool   `json:"apply"`
}

type runTriggerRequest struct {
	ConfigPath string   `json:"config_path,omitempty"`
	Password   string   `json:"password,omitempty"`
	ExtraArgs  []string `json:"extra_args,omitempty"`
	// Group optionally scopes the run to a single cluster group (by name).
	Group string `json:"group,omitempty"`
	// Clusters optionally scopes the run to an explicit cluster subset. Both are
	// further intersected with the caller's allowed clusters for non-admins.
	Clusters []string `json:"clusters,omitempty"`
}

type runPreflightRequest struct {
	ConfigPath string `json:"config_path,omitempty"`
}

type artifactInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

// runInfo is a single entry in the /api/v1/runs feed. The list combines:
//
//   - "history" entries archived under outputDir/runs/<id>/
//   - the current outputDir/run-summary.json + ncc-run-record.json ("summary")
//   - audit-log `runs.trigger` events that have no artifacts yet ("trigger")
//
// All numeric/metric fields are omitted from JSON when zero so a "trigger"-only
// record stays small and clients can detect the absence of full data.
type runInfo struct {
	ID       string `json:"id"`
	Path     string `json:"path,omitempty"`
	ModTime  string `json:"mod_time"`
	HasIndex bool   `json:"has_index"`

	// Where this row came from. One of: "history" | "summary" | "trigger".
	Source string `json:"source,omitempty"`
	// How the run was launched ("scheduled" | "manual"), from run-summary.json.
	RunSource string `json:"run_source,omitempty"`

	// Enrichment fields (populated from run-summary.json when available).
	Timestamp      string  `json:"timestamp,omitempty"`
	DurationS      float64 `json:"duration_s,omitempty"`
	ClustersOK     int     `json:"clusters_ok,omitempty"`
	ClustersFailed int     `json:"clusters_failed,omitempty"`
	TotalChecks    int     `json:"total_checks,omitempty"`
	AvgHealthScore int     `json:"avg_health_score,omitempty"`
	MinHealthScore int     `json:"min_health_score,omitempty"`
	FailTotal      int     `json:"fail_total,omitempty"`
	WarnTotal      int     `json:"warn_total,omitempty"`
	ErrTotal       int     `json:"err_total,omitempty"`
	InfoTotal      int     `json:"info_total,omitempty"`
	ExitCode       *int    `json:"exit_code,omitempty"`
	Success        *bool   `json:"success,omitempty"`

	// Optional provenance for "trigger"-only rows.
	Client    string `json:"client,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	AuthMode  string `json:"auth_mode,omitempty"`
}

func isInternalArtifactName(name string) bool {
	clean := strings.TrimSpace(name)
	switch clean {
	case ".ncc-preflight-check", ".ncc-prefight-check":
		return true
	default:
		return false
	}
}

type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newTailBuffer(max int) *tailBuffer {
	return &tailBuffer{max: max}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if t.max > 0 && len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

func main() {
	var s apiServer
	var listen string

	flag.StringVar(&listen, "listen", ":8081", "HTTP listen address")
	flag.StringVar(&s.repoRoot, "repo-root", ".", "Repository root path")
	flag.StringVar(&s.configPath, "config-path", "config.yaml", "Application config path")
	flag.StringVar(&s.outputDir, "output-dir", "outputfiles", "Artifacts output directory")
	flag.StringVar(&s.logDir, "log-dir", "nccfiles", "Raw logs directory")
	flag.StringVar(&s.runnerLogPath, "runner-log-path", "logs/ncc-runner.log", "Runner log file path")
	flag.StringVar(&s.scheduleStatePath, "schedule-state-path", ".ncc-api-schedule.json", "Schedule state file path")
	flag.StringVar(&s.backupScheduleStatePath, "backup-schedule-state-path", ".ncc-api-backup-schedule.json", "Scheduled-backup state file path")
	flag.StringVar(&s.notificationStatePath, "notifications-state-path", ".ncc-api-notifications.json", "Legacy notifications metadata path (delivery history/API-only fields)")
	flag.StringVar(&s.auditLogPath, "audit-log-path", "logs/ncc-audit.log", "JSONL audit log file path")
	flag.Int64Var(&s.auditLogMaxBytes, "audit-log-max-bytes", 5*1024*1024, "Audit log size before rotation (bytes); 0 disables rotation")
	flag.StringVar(&s.auditForwardHTTPURL, "audit-forward-http-url", "", "Forward each audit event (JSON) to this HTTP collector endpoint (Splunk HEC, Elastic, Loki, generic webhook). Empty disables.")
	flag.StringVar(&s.auditForwardHTTPAuth, "audit-forward-http-auth", "", "Authorization header value for the audit HTTP collector (e.g. 'Bearer <token>' or 'Splunk <hec-token>')")
	flag.BoolVar(&s.auditForwardHTTPSplunk, "audit-forward-http-splunk", false, "Wrap each forwarded audit event as a Splunk HEC payload {\"event\": ...}")
	flag.StringVar(&s.auditForwardSyslog, "audit-forward-syslog", "", "Forward each audit event as an RFC5424 syslog message to host:port (e.g. siem.example.com:514). Empty disables.")
	flag.StringVar(&s.auditForwardSyslogNet, "audit-forward-syslog-network", "udp", "Transport for --audit-forward-syslog: udp or tcp")
	flag.IntVar(&s.loginLockThreshold, "login-lockout-threshold", 5, "Failed logins per account before a temporary lockout (0 disables)")
	flag.DurationVar(&s.loginLockWindow, "login-lockout-window", 15*time.Minute, "Rolling window for accumulating failed logins toward a lockout")
	flag.DurationVar(&s.loginLockDuration, "login-lockout-duration", 15*time.Minute, "How long an account stays locked after exceeding the failure threshold")
	flag.StringVar(&s.orchestratorBin, "orchestrator-bin", "./ncc-orchestrator", "Path to ncc-orchestrator binary")
	flag.StringVar(&s.tokenFilePath, "token-file-path", ".ncc-api-token", "Token file path for UI proxy/frontend use")
	flag.StringVar(&s.corsOrigin, "cors-origin", "http://localhost:8080", "CORS allowed origin(s), comma-separated")
	flag.StringVar(&s.authMode, "auth-mode", "token", "Auth mode: token, session, hybrid")
	flag.StringVar(&s.sessionSecret, "session-secret", "", "Session token HMAC secret (required for session/hybrid unless generated)")
	flag.DurationVar(&s.sessionTTL, "session-ttl", 6*time.Hour, "Session token TTL (default 6h; admins can override at runtime in Settings → Access)")
	flag.StringVar(&s.sessionIssuer, "session-issuer", "ncc-api-server", "Session token issuer")
	flag.DurationVar(&s.runTimeout, "run-timeout", 90*time.Minute, "Max runtime for trigger-run command")
	flag.IntVar(&s.maxConcurrentRuns, "max-concurrent-runs", defaultMaxConcurrentRuns, "Max orchestrator runs executing at once; extra triggers queue and start as slots free")
	flag.BoolVar(&s.runAutoRetryDisabled, "disable-run-auto-retry", false, "Disable the single self-heal auto-retry of a failed run (with a safe mitigation) for recoverable failures (rate-limit/timeout/network)")
	flag.DurationVar(&s.selfHealInterval, "self-heal-interval", 0, "Periodically run the orchestrator doctor self-heal checks on this interval (e.g. 1h; 0 = disabled). Results feed /metrics and the System Health view")
	flag.BoolVar(&s.selfHealAutoFix, "self-heal-auto-fix", false, "When --self-heal-interval is set, apply safe remediations each cycle (anchor relative output paths, create missing dirs, tighten secret perms, repair config)")
	flag.BoolVar(&s.debugExpose, "debug-expose", false, "Expose debug internals in APIs (off by default)")
	flag.StringVar(&s.tlsCertFile, "tls-cert-file", "", "TLS certificate file for direct HTTPS")
	flag.StringVar(&s.tlsKeyFile, "tls-key-file", "", "TLS key file for direct HTTPS")
	flag.StringVar(&s.tlsClientCAFile, "tls-client-ca-file", "", "Optional client CA file (for mTLS verification)")
	flag.IntVar(&s.rateLimitPerMinute, "rate-limit-per-minute", 60, "Per-client rate limit for sensitive API routes (0 disables)")
	flag.DurationVar(&s.readTimeout, "read-timeout", 15*time.Second, "HTTP server read timeout")
	flag.DurationVar(&s.writeTimeout, "write-timeout", 60*time.Second, "HTTP server write timeout")
	flag.DurationVar(&s.idleTimeout, "idle-timeout", 60*time.Second, "HTTP server idle timeout")
	flag.BoolVar(&s.metricsPublic, "metrics-public", false, "Allow unauthenticated GET /metrics for Prometheus scrapers (off by default; on private networks behind a service mesh this is safe)")
	flag.StringVar(&s.usersFilePath, "users-file", "", "Path to a read-only YAML seed of local accounts (imported into --users-db on first run)")
	flag.StringVar(&s.usersDBPath, "users-db", "", "Path to the writable JSON user database file; enables login, first-run admin bootstrap, and runtime user/SSO management")
	flag.StringVar(&s.usersDBSecret, "users-db-secret", "", "Kubernetes Secret name to store the user database in (encrypted at rest by etcd); mutually exclusive with --users-db. Requires in-cluster execution + RBAC to get/create/patch the Secret")
	flag.StringVar(&s.usersDBSecretKey, "users-db-secret-key", "users.json", "Data key inside the Kubernetes Secret that holds the user-database JSON")
	flag.StringVar(&s.usersDBSecretNS, "users-db-secret-namespace", "", "Namespace of the Kubernetes Secret (defaults to the pod's own namespace)")
	flag.StringVar(&s.usersDBKeyFile, "users-db-key-file", "", "Path to a file holding a 32-byte master key (base64 or hex) to envelope-encrypt the user store at rest with AES-256-GCM. NCC_MASTER_KEY env takes precedence. Unset => plaintext (default). Keep this key off the protected disk/backup.")
	flag.BoolVar(&s.disableLocalAccounts, "disable-local-accounts", false, "Opt out of the default-on user database (no first-run admin bootstrap, login disabled). Use for pure token-only automation")
	flag.BoolVar(&s.cookieInsecure, "cookie-insecure", false, "Force the Secure attribute OFF on session cookies even when HTTPS is enabled (only behind a TLS-terminating proxy that talks plain http to the stack). Insecure is already the default.")
	flag.BoolVar(&s.cookieSecureForce, "cookie-secure", false, "Force the Secure attribute ON on session cookies regardless of the runtime HTTPS policy (for deployments terminating TLS in front of the stack)")
	var samlCfg samlConfig
	flag.StringVar(&samlCfg.RootURL, "saml-root-url", "", "External base URL of this server for SAML (e.g. https://ncc.example.com); enables SAML when set together with cert/key/idp-metadata")
	flag.StringVar(&samlCfg.IDPMetadata, "saml-idp-metadata", "", "SAML IdP metadata URL or local file path")
	flag.StringVar(&samlCfg.CertFile, "saml-cert", "", "SAML SP certificate (PEM)")
	flag.StringVar(&samlCfg.KeyFile, "saml-key", "", "SAML SP private key (PEM)")
	flag.StringVar(&samlCfg.EntityID, "saml-entity-id", "", "Optional SAML SP entity ID (default <root>/saml/metadata)")
	flag.StringVar(&samlCfg.UsernameAttr, "saml-username-attribute", "", "Assertion attribute used as username (default: NameID / common attrs)")
	flag.StringVar(&samlCfg.RoleAttr, "saml-role-attribute", "Role", "Assertion attribute carrying role/group values")
	flag.StringVar(&samlCfg.RoleMapRaw, "saml-role-map", "", "Mapping of IdP role/group values to local roles, e.g. 'ncc-admins=admin,ncc-ops=operator'")
	flag.StringVar(&samlCfg.DefaultRole, "saml-default-role", "viewer", "Role assigned when no SAML role mapping matches")
	flag.BoolVar(&samlCfg.AllowIDPInit, "saml-allow-idp-initiated", false, "Allow IdP-initiated SAML logins")

	var ldapCfg ldapPersisted
	var ldapCAFile string
	flag.StringVar(&ldapCfg.URL, "ldap-url", "", "LDAP/AD server URL(s), e.g. ldaps://dc1.corp:636 (comma-separated for failover); enables AD login when set with --ldap-base-dn")
	flag.StringVar(&ldapCfg.BaseDN, "ldap-base-dn", "", "LDAP search base DN for users, e.g. DC=corp,DC=example,DC=com")
	flag.StringVar(&ldapCfg.BindDN, "ldap-bind-dn", "", "DN of the read-only service account used to search for users")
	flag.StringVar(&ldapCfg.BindPassword, "ldap-bind-password", "", "Password for the LDAP service account (--ldap-bind-dn)")
	flag.StringVar(&ldapCfg.UserFilter, "ldap-user-filter", "", "User search filter; %s is the login name (default: AD sAMAccountName filter)")
	flag.StringVar(&ldapCfg.UsernameAttr, "ldap-username-attribute", "", "Attribute used as the canonical username (default: sAMAccountName)")
	flag.StringVar(&ldapCfg.GroupAttr, "ldap-group-attribute", "", "Attribute carrying group membership (default: memberOf)")
	flag.StringVar(&ldapCfg.RoleMapRaw, "ldap-role-map", "", "Mapping of group DNs/CNs to local roles, newline- or semicolon-separated, e.g. 'CN=NCC-Admins,OU=Groups,DC=corp,DC=com=admin'")
	flag.StringVar(&ldapCfg.DefaultRole, "ldap-default-role", "viewer", "Role assigned when no LDAP group mapping matches")
	flag.BoolVar(&ldapCfg.StartTLS, "ldap-start-tls", false, "Upgrade a plain ldap:// connection with StartTLS before binding")
	flag.BoolVar(&ldapCfg.InsecureSkipVerify, "ldap-insecure-skip-verify", false, "Skip TLS certificate verification for LDAPS/StartTLS (discouraged)")
	flag.StringVar(&ldapCAFile, "ldap-ca-file", "", "PEM file of CA certificate(s) used to verify the LDAP server certificate")
	var hashPasswordMode bool
	flag.BoolVar(&hashPasswordMode, "hash-password", false, "Read a password from stdin (or $NCC_PASSWORD) and print its bcrypt hash for the users file, then exit")
	var resetPasswordUser string
	var resetAdminMode bool
	flag.StringVar(&resetPasswordUser, "reset-password", "", "Reset the named local account to a new random temporary password (forced change at next login), write it to the configured user store, print it, and exit. Recovery path for a lost password; restart the api-server afterward.")
	flag.BoolVar(&resetAdminMode, "reset-admin", false, "Shortcut for --reset-password admin (recreates the built-in admin if it was wiped). Restart the api-server afterward.")
	// Probe mode: when --health-check is passed, the api-server does
	// NOT bind a port. Instead it reads the on-disk token, hits its
	// own /api/v1/health URL on --listen, and exits 0/1. Designed for
	// Docker HEALTHCHECK and Kubernetes liveness/readiness probes so
	// operators don't have to ship a separate curl/wget into the
	// container image.
	var healthCheckMode bool
	var healthCheckTimeout time.Duration
	flag.BoolVar(&healthCheckMode, "health-check", false, "Probe self at /api/v1/health and exit (0=healthy, 1=unhealthy); does NOT start a server")
	flag.DurationVar(&healthCheckTimeout, "health-check-timeout", 5*time.Second, "Connect+read timeout for --health-check")
	flag.Parse()

	if healthCheckMode {
		// Stack-aware defaults still apply here so a probe invocation
		// from <root>/bin/ncc-api-server --health-check picks the
		// matching token file without flags.
		applyStackAwareDefaults(&s, os.Args[1:])
		runHealthCheckProbe(&s, listen, healthCheckTimeout)
		return
	}

	if hashPasswordMode {
		runHashPassword()
		return
	}

	// Helpful subcommand handling. Without this, an invocation like
	//
	//     ./ncc-api-server update --check
	//
	// silently starts the server because Go's flag package treats
	// "update" as a positional arg and ignores the trailing flags.
	// We catch any positional arg here, special-case "version" to
	// print buildinfo, and otherwise redirect the user to the
	// orchestrator binary inside the same stack.
	handleSubcommandArgs(flag.Args())

	// Stack-aware defaults: if the api-server binary was launched from
	// inside an extracted v2 stack (`<root>/bin/<self>`) AND the user
	// did not pass an explicit value for a path flag, rewrite that
	// flag to the install-dir-relative path. Mirrors the v2.0.2
	// orchestrator auto-detect so that
	//
	//     cd ncc-v2-stack-<os>-<arch>/bin && ./ncc-api-server
	//
	// "just works" without the user having to re-derive every path
	// from the stack root.
	applyStackAwareDefaults(&s, os.Args[1:])

	s.authToken = strings.TrimSpace(os.Getenv("NCC_API_TOKEN"))
	s.viewerToken = strings.TrimSpace(os.Getenv("NCC_API_VIEWER_TOKEN"))
	if s.viewerToken != "" && s.authToken != "" && secureCompare(s.viewerToken, s.authToken) {
		log.Fatal("NCC_API_VIEWER_TOKEN must differ from the admin NCC_API_TOKEN")
	}
	if strings.Contains(s.corsOrigin, "*") {
		log.Fatal("wildcard cors-origin is not allowed in strict mode")
	}
	if s.authMode != "token" && s.authMode != "session" && s.authMode != "hybrid" {
		log.Fatal("auth-mode must be one of: token, session, hybrid")
	}
	if env := strings.TrimSpace(os.Getenv("NCC_USERS_FILE")); env != "" && strings.TrimSpace(s.usersFilePath) == "" {
		s.usersFilePath = env
	}
	if env := strings.TrimSpace(os.Getenv("NCC_USERS_DB")); env != "" && strings.TrimSpace(s.usersDBPath) == "" {
		s.usersDBPath = env
	}
	if env := strings.TrimSpace(os.Getenv("NCC_USERS_DB_SECRET")); env != "" && strings.TrimSpace(s.usersDBSecret) == "" {
		s.usersDBSecret = env
	}
	if env := strings.TrimSpace(os.Getenv("NCC_USERS_DB_SECRET_NAMESPACE")); env != "" && strings.TrimSpace(s.usersDBSecretNS) == "" {
		s.usersDBSecretNS = env
	}
	if strings.TrimSpace(s.usersDBSecret) != "" && strings.TrimSpace(s.usersDBPath) != "" {
		log.Fatal("--users-db and --users-db-secret are mutually exclusive; pick one user-database backend")
	}
	if v := strings.TrimSpace(os.Getenv("NCC_DISABLE_LOCAL_ACCOUNTS")); v == "1" || strings.EqualFold(v, "true") {
		s.disableLocalAccounts = true
	}

	// Default-on user database: when no backend was configured by a flag, env,
	// or the stack-aware default, fall back to a writable 0600 JSON file in the
	// repo root. This means a bare `./ncc-api-server` run still bootstraps a
	// first-run admin and enables login out of the box. Operators who want
	// pure token-only automation can opt out with --disable-local-accounts.
	if !s.disableLocalAccounts && strings.TrimSpace(s.usersDBSecret) == "" && strings.TrimSpace(s.usersDBPath) == "" {
		s.usersDBPath = defaultUsersDBPath(s.repoRoot)
		log.Printf("user database not configured; defaulting to %s (login + first-run admin bootstrap on; use --disable-local-accounts to opt out)", s.usersDBPath)
	}

	// Reset-password mode: an offline recovery path. Resolve the same backend
	// the server would use, reset the target account to a new random temporary
	// password (forced change at next login), print it, and exit without
	// binding a port. The admin must restart the api-server afterward so the
	// in-memory copy reloads.
	if resetAdminMode || strings.TrimSpace(resetPasswordUser) != "" {
		target := strings.TrimSpace(resetPasswordUser)
		if resetAdminMode {
			target = reservedAdminUsername
		}
		runResetPassword(&s, target)
		return
	}

	// Select the user-database backend. A Kubernetes Secret store (encrypted at
	// rest by etcd) is preferred in-cluster; otherwise a local 0600 JSON file.
	storeBackend, err := s.resolveUserStoreBackend()
	if err != nil {
		log.Fatalf("%v", err)
	}

	// When a backend is configured (explicitly, via env, or by the stack-aware
	// default when launched from a v2 stack), the server manages local
	// accounts: it imports an optional --users-file seed and, if still empty,
	// bootstraps an initial admin with a random password that the operator must
	// change on first login.
	if storeBackend != nil {
		db, err := openUserDBFromBackend(storeBackend)
		if err != nil {
			log.Fatalf("open user database (%s): %v", storeBackend.location(), err)
		}
		s.users = db
		if db.count() == 0 && strings.TrimSpace(s.usersFilePath) != "" {
			seed, err := loadUserStore(s.usersFilePath)
			if err != nil {
				log.Fatalf("load users seed file: %v", err)
			}
			if err := db.importSeed(seed); err != nil {
				log.Fatalf("import users seed: %v", err)
			}
			log.Printf("imported %d local account(s) from seed %s into %s", db.count(), filepath.Clean(s.usersFilePath), db.location())
		}
		if db.count() == 0 {
			pw, created, err := db.bootstrapAdminIfEmpty("admin")
			if err != nil {
				log.Fatalf("bootstrap admin: %v", err)
			}
			if created {
				hint := db.setInitialPassword("admin", pw)
				log.Printf("==================================================================")
				log.Printf(" FIRST-RUN ADMIN CREATED (store: %s)", db.location())
				log.Printf("   username: admin")
				log.Printf("   password: %s", pw)
				log.Printf("   You MUST change this password on first login.")
				if hint != "" {
					log.Printf("   retrieve it later via: %s", hint)
				}
				log.Printf("==================================================================")
			}
		}
	} else if strings.TrimSpace(s.usersFilePath) != "" {
		// Read-only seed without a writable db: load it in-memory (legacy
		// --users-file behavior, no runtime management).
		db, err := loadUserStore(s.usersFilePath)
		if err != nil {
			log.Fatalf("load users file: %v", err)
		}
		s.users = db
		log.Printf("loaded %d local account(s) from %s (read-only)", db.count(), filepath.Clean(s.usersFilePath))
	}

	// SAML: startup flags take precedence (and lock the runtime SSO settings
	// page) for backward compatibility. Otherwise, load any persisted runtime
	// SAML config from the user database.
	if samlCfg.configured() {
		prov, err := newSAMLProvider(context.Background(), samlCfg)
		if err != nil {
			log.Fatalf("init SAML: %v", err)
		}
		s.saml = prov
		s.samlEnabled = true
		s.samlFromFlags = true
		log.Printf("SAML SSO enabled via flags (root=%s)", strings.TrimSpace(samlCfg.RootURL))
	} else if strings.TrimSpace(samlCfg.RootURL) != "" || strings.TrimSpace(samlCfg.IDPMetadata) != "" {
		log.Fatal("SAML requires --saml-root-url, --saml-idp-metadata, --saml-cert, and --saml-key together")
	} else if s.users.writable() {
		if err := s.reloadSAMLFromStore(context.Background()); err != nil {
			log.Printf("WARNING: persisted SAML config failed to load (SSO disabled until fixed): %v", err)
		}
	}

	// LDAP/AD: startup flags take precedence (and lock the runtime settings
	// page). Otherwise load any persisted runtime LDAP config from the store.
	if strings.TrimSpace(ldapCfg.URL) != "" && strings.TrimSpace(ldapCfg.BaseDN) != "" {
		ldapCfg.Enabled = true
		if strings.TrimSpace(ldapCAFile) != "" {
			pem, err := os.ReadFile(ldapCAFile)
			if err != nil {
				log.Fatalf("read --ldap-ca-file: %v", err)
			}
			ldapCfg.CACertPEM = string(pem)
		}
	}
	if ldapCfg.configured() {
		prov, _, err := buildLDAPProvider(&ldapCfg)
		if err != nil {
			log.Fatalf("init LDAP: %v", err)
		}
		s.ldap = prov
		s.ldapEnabled = true
		s.ldapFromFlags = true
		log.Printf("LDAP/AD login enabled via flags (url=%s)", strings.TrimSpace(ldapCfg.URL))
	} else if strings.TrimSpace(ldapCfg.URL) != "" || strings.TrimSpace(ldapCfg.BaseDN) != "" {
		log.Fatal("LDAP requires --ldap-url and --ldap-base-dn together")
	} else if s.users.writable() {
		if err := s.reloadLDAPFromStore(context.Background()); err != nil {
			log.Printf("WARNING: persisted LDAP config failed to load (AD login disabled until fixed): %v", err)
		}
	}
	// Interactive login requires signed sessions. If the operator left
	// auth-mode at the token default, transparently upgrade to hybrid so
	// browser cookie sessions work alongside static tokens for automation.
	if s.loginEnabled() && s.authMode == "token" {
		s.authMode = "hybrid"
	}
	if s.sessionTTL <= 0 || s.sessionTTL > 24*time.Hour {
		log.Fatal("session-ttl must be > 0 and <= 24h")
	}
	if s.rateLimitPerMinute < 0 {
		log.Fatal("rate-limit-per-minute must be >= 0")
	}
	if s.readTimeout <= 0 || s.writeTimeout <= 0 || s.idleTimeout <= 0 {
		log.Fatal("read-timeout, write-timeout, and idle-timeout must be > 0")
	}
	if err := s.ensureAuthToken(); err != nil {
		log.Fatal(err)
	}
	if s.sessionsHonored() && strings.TrimSpace(s.sessionSecret) == "" {
		b := make([]byte, 32)
		if _, err := crand.Read(b); err != nil {
			log.Fatalf("generate session secret: %v", err)
		}
		s.sessionSecret = base64.RawURLEncoding.EncodeToString(b)
	}
	if err := s.validatePathConfig(); err != nil {
		log.Fatal(err)
	}
	if s.rateLimitPerMinute > 0 {
		s.rateLimiter = newFixedWindowRateLimiter(s.rateLimitPerMinute, time.Minute)
	}
	s.loginGuard = newLoginGuard(s.loginLockThreshold, s.loginLockWindow, s.loginLockDuration)
	s.ensureRunManager()
	s.startedAt = time.Now().UTC()
	s.auditForwarder = s.startAuditForwarder(context.Background())
	s.startSelfHealLoop(context.Background())
	s.startBackupScheduleLoop(context.Background())
	s.startNotificationDigestLoop(context.Background())

	handler := s.buildHandler()
	srv := &http.Server{
		Addr:         listen,
		Handler:      handler,
		ReadTimeout:  s.readTimeout,
		WriteTimeout: s.writeTimeout,
		IdleTimeout:  s.idleTimeout,
	}
	log.Printf("ncc-api-server listening on %s (auth_mode=%s, tls=%t)", listen, s.authMode, strings.TrimSpace(s.tlsCertFile) != "" && strings.TrimSpace(s.tlsKeyFile) != "")
	if strings.TrimSpace(s.tlsCertFile) != "" || strings.TrimSpace(s.tlsKeyFile) != "" {
		if strings.TrimSpace(s.tlsCertFile) == "" || strings.TrimSpace(s.tlsKeyFile) == "" {
			log.Fatal("both tls-cert-file and tls-key-file are required together")
		}
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if strings.TrimSpace(s.tlsClientCAFile) != "" {
			b, err := os.ReadFile(s.tlsClientCAFile)
			if err != nil {
				log.Fatalf("read tls-client-ca-file: %v", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				log.Fatal("failed to parse tls-client-ca-file")
			}
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		srv.TLSConfig = tlsCfg
		if err := srv.ListenAndServeTLS(s.tlsCertFile, s.tlsKeyFile); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func (s *apiServer) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || r.URL.Path == "/api/v1/health" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodGet && (r.URL.Path == "/api/v1/openapi.json" || r.URL.Path == "/api/v1/meta/routes" || r.URL.Path == "/api/v1/metrics/rate-limit") {
			next.ServeHTTP(w, r)
			return
		}
		// /metrics is publicly readable when the operator explicitly
		// opts in via --metrics-public so vanilla Prometheus scrapers
		// (which don't easily set X-Api-Token) can ingest the
		// endpoint. Off by default; same auth as everything else
		// otherwise.
		if r.Method == http.MethodGet && r.URL.Path == "/metrics" && s.metricsPublic {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/" || r.URL.Path == "/docs" || r.URL.Path == "/docs/ui" ||
			strings.HasPrefix(r.URL.Path, "/docs/assets/") {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/api/v1/auth/session" {
			next.ServeHTTP(w, r)
			return
		}
		// Login/logout/me, password change, and the SAML SP endpoints
		// authenticate (or report status) on their own and must be reachable
		// without a fully-privileged session.
		if r.URL.Path == "/api/v1/auth/login" ||
			r.URL.Path == "/api/v1/auth/logout" ||
			r.URL.Path == "/api/v1/auth/me" ||
			r.URL.Path == "/api/v1/auth/change-password" ||
			r.URL.Path == "/api/v1/auth/forgot-password" ||
			strings.HasPrefix(r.URL.Path, "/saml/") {
			next.ServeHTTP(w, r)
			return
		}

		p, ok := s.resolvePrincipal(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
			return
		}
		// Forced password change: a flagged local account may do nothing else
		// until it sets a new password (the allowlisted endpoints above let it
		// reach /auth/me, /auth/change-password, and /auth/logout).
		//
		// Exception: a backup restore is allowed during forced change so the
		// first-login admin can recover an existing deployment instead of
		// setting a new password — the restore replaces the user database with
		// the backed-up one (old admin hash, must_change=false), making a
		// password change pointless. The RBAC check below still confines this to
		// the admin role, and CSRF still applies, so a non-admin flagged account
		// cannot use it.
		if p.mustChange && r.URL.Path != "/api/v1/settings/restore" {
			writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "password change required", ErrorCode: "NCC_API_PASSWORD_CHANGE_REQUIRED"})
			return
		}
		// Role-based access control: the caller's role must meet the route's
		// minimum (viewer < operator < admin).
		if need := routeMinRole(r); p.role < need {
			writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: fmt.Sprintf("forbidden: this action requires the %q role", need.String())})
			return
		}
		// CSRF: browser cookie sessions must echo the double-submit token on
		// any mutating request. Token/bearer automation is exempt (no cookie).
		if p.method == authSessionCookie && isMutating(r) && !s.csrfValid(r) {
			writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "forbidden: missing or invalid CSRF token"})
			return
		}
		// Carry the resolved identity forward so audit entries are attributed to
		// the acting user + role (see apiServer.audit).
		next.ServeHTTP(w, withPrincipal(r, p))
	})
}

func (s *apiServer) withCORS(next http.Handler) http.Handler {
	allowedOrigins := parseAllowedOrigins(s.corsOrigin)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		// SAML SP endpoints are browser-mediated and IdP-originated: the IdP
		// returns its assertion as a top-level cross-site POST to /saml/acs, so
		// the browser sends Origin: <idp-host>, which can never be in the UI
		// origin allowlist. The CORS allowlist guards the SPA's XHR calls to
		// /api/; the SAML flow's security boundary is the signed assertion plus
		// the relay-state cookie, so exempt /saml/ from origin enforcement.
		isSAML := strings.HasPrefix(r.URL.Path, "/saml/")
		if origin != "" && !isSAML {
			if _, ok := allowedOrigins[origin]; !ok && !sameHostOrigin(origin, r) {
				writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "origin not allowed"})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
		// Content-Security-Policy. API/JSON (and Prometheus text) responses serve
		// no active content, so they get a maximally strict policy. The two HTML
		// help pages (landing + Swagger UI) need inline style/script; Swagger's
		// bundle/styles are now self-hosted (vendored, served from /docs/assets),
		// so the policy no longer trusts any external CDN origin.
		switch {
		case r.URL.Path == "/" || r.URL.Path == "/docs" || r.URL.Path == "/docs/ui" ||
			strings.HasPrefix(r.URL.Path, "/docs/assets/"):
			w.Header().Set("Content-Security-Policy",
				"default-src 'none'; base-uri 'none'; frame-ancestors 'none'; "+
					"img-src 'self' data:; "+
					"style-src 'self' 'unsafe-inline'; "+
					"script-src 'self' 'unsafe-inline'; "+
					"connect-src 'self'; font-src 'self' data:; worker-src 'self' blob:")
		default:
			w.Header().Set("Content-Security-Policy",
				"default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		}
		if r.TLS != nil {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Token, Authorization, X-CSRF-Token")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameHostOrigin allows a browser Origin when it targets the same host as the
// current request (hostname match, any port). This removes the need for users
// to hand-edit CORS origins just because API and UI listen on different ports
// on the same box (e.g. 8081 vs 8080).
func sameHostOrigin(origin string, r *http.Request) bool {
	ou, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if ou.Scheme != "http" && ou.Scheme != "https" {
		return false
	}
	originHost := strings.TrimSpace(strings.ToLower(ou.Hostname()))
	if originHost == "" {
		return false
	}
	reqHost := strings.TrimSpace(strings.ToLower(hostOnly(r.Host)))
	if reqHost == "" {
		// Fallback for proxied setups where Host can be rewritten.
		reqHost = strings.TrimSpace(strings.ToLower(hostOnly(r.Header.Get("X-Forwarded-Host"))))
	}
	return reqHost != "" && originHost == reqHost
}

func hostOnly(hostport string) string {
	hostport = strings.TrimSpace(hostport)
	if hostport == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	// Already host-only, or an unbracketed IPv6 literal.
	return hostport
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := map[string]interface{}{
		"status":                "ok",
		"time":                  time.Now().UTC().Format(time.RFC3339),
		"auth_mode":             s.authMode,
		"token_source":          tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN")),
		"rbac_enabled":          s.viewerToken != "" || s.loginEnabled(),
		"login_enabled":         s.loginEnabled(),
		"local_login":           s.users != nil && s.users.count() > 0,
		"saml_enabled":          s.samlEnabled,
		"ldap_enabled":          s.ldapIsEnabled(),
		"users_store_encrypted": s.userStoreEncrypted,
		"config_path":           s.absPath(s.configPath),
		"output_dir":            s.absPath(s.outputDir),
		"log_dir":               s.absPath(s.logDir),
		"token_file":            s.absPath(s.tokenFilePath),
		"orchestrator_bin":      s.absPath(s.orchestratorBin),
		"version":               Version,
		"git_revision":          GitRevision,
		"build_date":            BuildDate,
		"stream":                Stream,
		"go_version":            GoVersion,
		"os":                    runtime.GOOS,
		"arch":                  runtime.GOARCH,
	}
	if s.debugExpose {
		data["repo_root"] = s.absPath(s.repoRoot)
		data["schedule_state"] = s.absPath(s.scheduleStatePath)
		data["orchestrator_cmd"] = strings.Join(s.orchestratorBaseCommand(), " ")
	}
	writeJSON(w, http.StatusOK, envelope{
		Success: true,
		Data:    data,
	})
}

// handleAudit returns recent audit log entries (newest first) read from the
// JSONL file. Filters: ?limit=200, ?action=settings, ?failures=1.
func (s *apiServer) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	q := r.URL.Query()
	limit := 100
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid limit: must be a positive integer"})
			return
		}
		if n > 10000 {
			n = 10000
		}
		limit = n
	}
	filter := auditFilter{
		actionPrefix: strings.TrimSpace(q.Get("action")),
		user:         strings.TrimSpace(q.Get("user")),
		onlyFailures: q.Get("failures") == "1" || strings.EqualFold(q.Get("failures"), "true"),
	}
	// since/until accept either an RFC3339 timestamp or a YYYY-MM-DD date.
	if raw := strings.TrimSpace(q.Get("since")); raw != "" {
		t, err := parseAuditTime(raw, false)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid since: use RFC3339 or YYYY-MM-DD"})
			return
		}
		filter.since = t
	}
	if raw := strings.TrimSpace(q.Get("until")); raw != "" {
		t, err := parseAuditTime(raw, true)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid until: use RFC3339 or YYYY-MM-DD"})
			return
		}
		filter.until = t
	}

	entries, err := s.auditEntries(limit, filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "read audit log: " + err.Error()})
		return
	}

	// CSV export streams a downloadable file rather than the JSON envelope.
	if strings.EqualFold(strings.TrimSpace(q.Get("format")), "csv") {
		s.writeAuditCSV(w, entries)
		return
	}

	abs := s.absPath(s.auditLogPath)
	var size int64
	var modTime string
	if st, err := os.Stat(abs); err == nil {
		size = st.Size()
		modTime = st.ModTime().UTC().Format(time.RFC3339)
	}

	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"path":      abs,
		"size":      size,
		"mod_time":  modTime,
		"limit":     limit,
		"count":     len(entries),
		"max_bytes": s.auditLogMaxBytes,
		"entries":   entries,
		"filters": map[string]interface{}{
			"action":   filter.actionPrefix,
			"user":     filter.user,
			"failures": filter.onlyFailures,
			"since":    q.Get("since"),
			"until":    q.Get("until"),
		},
	}})
}

// parseAuditTime parses an RFC3339 timestamp or a YYYY-MM-DD date (UTC). For a
// bare date used as an upper bound, endOfDay extends it to the end of that day
// so "until=2026-06-05" includes the whole day.
func parseAuditTime(raw string, endOfDay bool) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	d, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, err
	}
	if endOfDay {
		return d.Add(24*time.Hour - time.Nanosecond), nil
	}
	return d, nil
}

// writeAuditCSV renders audit entries as a downloadable CSV with stable columns;
// any keys beyond the common ones are folded into a JSON "details" column.
func (s *apiServer) writeAuditCSV(w http.ResponseWriter, entries []map[string]interface{}) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"ncc-audit-%s.csv\"", time.Now().UTC().Format("20060102-150405")))
	cw := csv.NewWriter(w)
	cols := []string{"ts", "user", "role", "action", "success", "client", "method", "path", "user_agent"}
	_ = cw.Write(append(append([]string{}, cols...), "details"))
	known := map[string]bool{}
	for _, c := range cols {
		known[c] = true
	}
	for _, e := range entries {
		row := make([]string, 0, len(cols)+1)
		for _, c := range cols {
			row = append(row, auditCSVCell(e[c]))
		}
		details := map[string]interface{}{}
		for k, v := range e {
			if !known[k] {
				details[k] = v
			}
		}
		if len(details) > 0 {
			b, _ := json.Marshal(details)
			row = append(row, string(b))
		} else {
			row = append(row, "")
		}
		_ = cw.Write(row)
	}
	cw.Flush()
}

func auditCSVCell(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", t)
	}
}

func (s *apiServer) handleRateLimitMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.rateLimiter == nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"enabled": false,
			"config": map[string]interface{}{
				"rate_limit_per_minute": s.rateLimitPerMinute,
				"window_seconds":        60,
			},
		}})
		return
	}
	st := s.rateLimiter.stats(time.Now().UTC())
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"enabled": true,
		"config": map[string]interface{}{
			"rate_limit_per_minute": s.rateLimitPerMinute,
			"window_seconds":        st.WindowSeconds,
		},
		"metrics": st,
	}})
}

// handlePrometheusMetrics emits Prometheus exposition format on
// GET /metrics. Intentionally hand-rolled (no client_golang dep) so
// the api-server stays at its current dependency footprint and the
// scrape endpoint never needs reachability to a Prometheus library
// for build-time updates.
//
// Metric naming follows the Prometheus best practices: snake_case,
// `_total` suffix for monotonic counters, `_seconds` for time, and a
// `ncc_` namespace prefix to avoid collisions when scraped alongside
// other targets.
//
// HELP / TYPE comments are emitted so the endpoint passes
// `promtool check metrics`. The response is text/plain;
// version=0.0.4; charset=utf-8 — the canonical content type
// Prometheus advertises in its scrape Accept header.
func (s *apiServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	now := time.Now().UTC()
	uptime := now.Sub(s.startedAt).Seconds()

	s.mu.Lock()
	active := 0
	if s.active {
		active = 1
	}
	startedTS := float64(0)
	if !s.started.IsZero() {
		startedTS = float64(s.started.Unix())
	}
	runningNow := s.runningCountLocked()
	queuedNow := len(s.runQueue)
	maxConc := s.maxConcurrentRuns
	s.mu.Unlock()

	fmt.Fprintf(w, "# HELP ncc_build_info Build metadata for this api-server (always 1).\n")
	fmt.Fprintf(w, "# TYPE ncc_build_info gauge\n")
	fmt.Fprintf(w, "ncc_build_info{version=%q,stream=%q,go_version=%q,os=%q,arch=%q} 1\n",
		Version, Stream, GoVersion, runtime.GOOS, runtime.GOARCH)

	fmt.Fprintf(w, "# HELP ncc_process_start_time_seconds Unix epoch start time of the api-server process.\n")
	fmt.Fprintf(w, "# TYPE ncc_process_start_time_seconds gauge\n")
	fmt.Fprintf(w, "ncc_process_start_time_seconds %d\n", s.startedAt.Unix())

	fmt.Fprintf(w, "# HELP ncc_process_uptime_seconds Seconds since the api-server started.\n")
	fmt.Fprintf(w, "# TYPE ncc_process_uptime_seconds gauge\n")
	fmt.Fprintf(w, "ncc_process_uptime_seconds %.3f\n", uptime)

	fmt.Fprintf(w, "# HELP ncc_run_active 1 when an NCC run is currently executing, else 0.\n")
	fmt.Fprintf(w, "# TYPE ncc_run_active gauge\n")
	fmt.Fprintf(w, "ncc_run_active %d\n", active)

	fmt.Fprintf(w, "# HELP ncc_run_started_seconds Unix epoch start time of the currently-running NCC run (0 when idle).\n")
	fmt.Fprintf(w, "# TYPE ncc_run_started_seconds gauge\n")
	fmt.Fprintf(w, "ncc_run_started_seconds %.0f\n", startedTS)

	fmt.Fprintf(w, "# HELP ncc_runs_triggered_total Cumulative count of NCC runs triggered via /api/v1/runs/trigger since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_triggered_total counter\n")
	fmt.Fprintf(w, "ncc_runs_triggered_total %d\n", s.runsTriggeredTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_runs_completed_total Cumulative count of NCC runs that completed (any exit code) since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_completed_total counter\n")
	fmt.Fprintf(w, "ncc_runs_completed_total %d\n", s.runsCompletedTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_runs_failed_total Cumulative count of NCC runs that exited with a non-zero status since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_failed_total counter\n")
	fmt.Fprintf(w, "ncc_runs_failed_total %d\n", s.runsFailedTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_run_auto_retries_total Cumulative count of self-heal auto-retries triggered after a recoverable run failure (rate-limit/timeout/network).\n")
	fmt.Fprintf(w, "# TYPE ncc_run_auto_retries_total counter\n")
	fmt.Fprintf(w, "ncc_run_auto_retries_total %d\n", s.runAutoRetriesTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_selfheal_runs_total Cumulative count of periodic self-heal (doctor) cycles executed.\n")
	fmt.Fprintf(w, "# TYPE ncc_selfheal_runs_total counter\n")
	fmt.Fprintf(w, "ncc_selfheal_runs_total %d\n", s.selfHealRunsTotal.Load())
	fmt.Fprintf(w, "# HELP ncc_selfheal_fixes_total Cumulative count of remediations applied by periodic self-heal cycles.\n")
	fmt.Fprintf(w, "# TYPE ncc_selfheal_fixes_total counter\n")
	fmt.Fprintf(w, "ncc_selfheal_fixes_total %d\n", s.selfHealFixesTotal.Load())
	if rep := s.cachedSelfHeal(); rep != nil {
		fmt.Fprintf(w, "# HELP ncc_selfheal_checks Most recent self-heal check counts by status (ok/warn/fail).\n")
		fmt.Fprintf(w, "# TYPE ncc_selfheal_checks gauge\n")
		for _, status := range []string{"ok", "warn", "fail"} {
			fmt.Fprintf(w, "ncc_selfheal_checks{status=%q} %d\n", status, rep.Summary[status])
		}
	}

	fmt.Fprintf(w, "# HELP ncc_runs_running Number of NCC runs currently executing (concurrent engine).\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_running gauge\n")
	fmt.Fprintf(w, "ncc_runs_running %d\n", runningNow)

	fmt.Fprintf(w, "# HELP ncc_runs_queued Number of NCC runs waiting for a free concurrency slot.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_queued gauge\n")
	fmt.Fprintf(w, "ncc_runs_queued %d\n", queuedNow)

	fmt.Fprintf(w, "# HELP ncc_runs_max_concurrent Configured maximum number of concurrent NCC runs.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_max_concurrent gauge\n")
	fmt.Fprintf(w, "ncc_runs_max_concurrent %d\n", maxConc)

	// Run-duration summary. Emitting _sum and _count (no buckets) keeps the
	// hand-rolled exposition simple while still letting Prometheus compute the
	// average run time as rate(_sum)/rate(_count).
	durCount := s.runDurationCount.Load()
	durSumSeconds := float64(s.runDurationMillisSum.Load()) / 1000.0
	fmt.Fprintf(w, "# HELP ncc_run_duration_seconds Wall-clock duration of completed NCC runs (summary; sum+count only).\n")
	fmt.Fprintf(w, "# TYPE ncc_run_duration_seconds summary\n")
	fmt.Fprintf(w, "ncc_run_duration_seconds_sum %.3f\n", durSumSeconds)
	fmt.Fprintf(w, "ncc_run_duration_seconds_count %d\n", durCount)

	fmt.Fprintf(w, "# HELP ncc_run_last_duration_seconds Wall-clock duration of the most recently completed NCC run.\n")
	fmt.Fprintf(w, "# TYPE ncc_run_last_duration_seconds gauge\n")
	fmt.Fprintf(w, "ncc_run_last_duration_seconds %.3f\n", float64(s.lastRunDurationMs.Load())/1000.0)

	fmt.Fprintf(w, "# HELP ncc_auth_logins_total Cumulative successful interactive logins (local + LDAP) since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_auth_logins_total counter\n")
	fmt.Fprintf(w, "ncc_auth_logins_total %d\n", s.loginSuccessTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_auth_login_failures_total Cumulative failed interactive login attempts (bad credentials, locked, or directory unavailable).\n")
	fmt.Fprintf(w, "# TYPE ncc_auth_login_failures_total counter\n")
	fmt.Fprintf(w, "ncc_auth_login_failures_total %d\n", s.loginFailureTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_auth_lockouts_total Cumulative account lockouts triggered by the brute-force guard.\n")
	fmt.Fprintf(w, "# TYPE ncc_auth_lockouts_total counter\n")
	fmt.Fprintf(w, "ncc_auth_lockouts_total %d\n", s.lockoutTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_update_applied_total Cumulative in-app software updates successfully installed.\n")
	fmt.Fprintf(w, "# TYPE ncc_update_applied_total counter\n")
	fmt.Fprintf(w, "ncc_update_applied_total %d\n", s.updateAppliedTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_update_failed_total Cumulative in-app software updates that failed (backup or install error).\n")
	fmt.Fprintf(w, "# TYPE ncc_update_failed_total counter\n")
	fmt.Fprintf(w, "ncc_update_failed_total %d\n", s.updateFailedTotal.Load())

	if s.auditForwarder != nil {
		fmt.Fprintf(w, "# HELP ncc_audit_forward_dropped_total Audit events dropped (buffer full or sink error) instead of forwarded to the SIEM/syslog sink.\n")
		fmt.Fprintf(w, "# TYPE ncc_audit_forward_dropped_total counter\n")
		fmt.Fprintf(w, "ncc_audit_forward_dropped_total %d\n", s.auditForwarder.dropped.Load())
	}

	// Update-availability gauge from the cached `update --check` result (set
	// when the UI last polled). Omitted entirely until a check has run so a
	// stale 0 is never mistaken for "up to date".
	if avail, _, at := s.updateCheckSnapshot(); !at.IsZero() {
		availVal := 0
		if avail {
			availVal = 1
		}
		fmt.Fprintf(w, "# HELP ncc_update_available 1 when the last update check found a newer release in-track, else 0.\n")
		fmt.Fprintf(w, "# TYPE ncc_update_available gauge\n")
		fmt.Fprintf(w, "ncc_update_available %d\n", availVal)
		fmt.Fprintf(w, "# HELP ncc_update_check_timestamp_seconds Unix epoch of the most recent successful update check.\n")
		fmt.Fprintf(w, "# TYPE ncc_update_check_timestamp_seconds gauge\n")
		fmt.Fprintf(w, "ncc_update_check_timestamp_seconds %d\n", at.Unix())
	}

	// Server-side backup inventory (best-effort; a missing/unreadable backups
	// directory simply yields zeros). Lets Prometheus alert on "no recent
	// snapshot" and track encryption coverage.
	if entries, err := s.listBackupEntries(); err == nil {
		byKind := map[string]int{"manual": 0, "pre-update": 0, "other": 0}
		encrypted := 0
		var newest time.Time
		lastTS := int64(0)
		for _, e := range entries {
			byKind[e.Kind]++
			if e.Encrypted {
				encrypted++
			}
			if t, perr := time.Parse(time.RFC3339, e.ModTime); perr == nil && t.After(newest) {
				newest = t
			}
		}
		fmt.Fprintf(w, "# HELP ncc_backups Number of server-side backup snapshots present, by kind.\n")
		fmt.Fprintf(w, "# TYPE ncc_backups gauge\n")
		for _, k := range []string{"manual", "pre-update", "other"} {
			fmt.Fprintf(w, "ncc_backups{kind=%q} %d\n", k, byKind[k])
		}
		fmt.Fprintf(w, "# HELP ncc_backups_encrypted Number of server-side backup snapshots sealed at rest (AES-256-GCM).\n")
		fmt.Fprintf(w, "# TYPE ncc_backups_encrypted gauge\n")
		fmt.Fprintf(w, "ncc_backups_encrypted %d\n", encrypted)
		if !newest.IsZero() {
			lastTS = newest.Unix()
		}
		fmt.Fprintf(w, "# HELP ncc_backup_last_timestamp_seconds Unix epoch modification time of the most recent backup snapshot (0 when none).\n")
		fmt.Fprintf(w, "# TYPE ncc_backup_last_timestamp_seconds gauge\n")
		fmt.Fprintf(w, "ncc_backup_last_timestamp_seconds %d\n", lastTS)
	}

	fmt.Fprintf(w, "# HELP ncc_go_goroutines Number of goroutines in the running api-server process.\n")
	fmt.Fprintf(w, "# TYPE ncc_go_goroutines gauge\n")
	fmt.Fprintf(w, "ncc_go_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP ncc_go_memstats_alloc_bytes Currently allocated heap bytes (runtime.MemStats.Alloc).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(w, "ncc_go_memstats_alloc_bytes %d\n", ms.Alloc)

	fmt.Fprintf(w, "# HELP ncc_go_memstats_sys_bytes Total bytes obtained from the OS (runtime.MemStats.Sys).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(w, "ncc_go_memstats_sys_bytes %d\n", ms.Sys)

	fmt.Fprintf(w, "# HELP ncc_go_memstats_heap_inuse_bytes Heap in-use bytes (runtime.MemStats.HeapInuse).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(w, "ncc_go_memstats_heap_inuse_bytes %d\n", ms.HeapInuse)

	fmt.Fprintf(w, "# HELP ncc_go_gc_total Total GC cycles since process start (runtime.MemStats.NumGC).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_gc_total counter\n")
	fmt.Fprintf(w, "ncc_go_gc_total %d\n", ms.NumGC)

	if s.rateLimiter != nil {
		st := s.rateLimiter.stats(now)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_window_seconds Rate-limiter window length in seconds.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_window_seconds gauge\n")
		fmt.Fprintf(w, "ncc_ratelimit_window_seconds %d\n", st.WindowSeconds)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_active_buckets Number of distinct (client, route) buckets tracked in the current window.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_active_buckets gauge\n")
		fmt.Fprintf(w, "ncc_ratelimit_active_buckets %d\n", st.ActiveBuckets)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_allowed_total Cumulative requests allowed by the rate limiter.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_allowed_total counter\n")
		fmt.Fprintf(w, "ncc_ratelimit_allowed_total %d\n", st.AllowedTotal)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_blocked_total Cumulative requests blocked by the rate limiter.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_blocked_total counter\n")
		fmt.Fprintf(w, "ncc_ratelimit_blocked_total %d\n", st.BlockedTotal)
	}

	// NCC run metrics derived from the latest run-summary.json. Serving these
	// over /metrics lets Prometheus scrape per-cluster severity/health
	// directly from the api-server instead of relying on a node_exporter
	// textfile collector reading <cluster>.prom files. Best-effort: a missing
	// or unreadable summary simply omits this block.
	if v, ok := s.latestRunSummaryView(); ok {
		fmt.Fprint(w, promtext.RenderRunSummaryMetrics(v))
	}
}

// latestRunSummaryView loads the most recent run-summary.json into the view
// used to render Prometheus run metrics. It returns ok=false when no readable
// summary exists.
func (s *apiServer) latestRunSummaryView() (promtext.RunSummaryView, bool) {
	path := filepath.Join(s.absPath(s.outputDir), "run-summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return promtext.RunSummaryView{}, false
	}
	var v promtext.RunSummaryView
	if err := json.Unmarshal(data, &v); err != nil {
		return promtext.RunSummaryView{}, false
	}
	return v, true
}

func (s *apiServer) handleAPIDocsHome(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if r.URL.Path == "/docs/ui" {
		s.handleSwaggerUIPage(w, r)
		return
	}
	if r.URL.Path != "/" && r.URL.Path != "/docs" {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "not found"})
		return
	}
	authMode := html.EscapeString(strings.TrimSpace(s.authMode))
	if authMode == "" {
		authMode = "token"
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC API Docs</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 1.5rem auto; max-width: 1100px; line-height: 1.4; padding: 0 1rem; }
    h1, h2 { margin: 0.5rem 0; }
    a { color: #1677ff; text-decoration: none; }
    .muted { color: #666; }
    .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 0.75rem; margin: 1rem 0; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 0.8rem; background: rgba(127,127,127,0.05); }
    .label { font-size: 0.85rem; color: #666; margin-bottom: 0.25rem; }
    .value { font-size: 1.1rem; font-weight: 600; }
    .toolbar { display: flex; gap: 0.5rem; flex-wrap: wrap; margin: 0.75rem 0; }
    input { padding: 0.45rem 0.6rem; border: 1px solid #bbb; border-radius: 6px; min-width: 230px; }
    button { padding: 0.45rem 0.7rem; border: 1px solid #bbb; border-radius: 6px; background: transparent; cursor: pointer; }
    table { width: 100%%; border-collapse: collapse; margin-top: 0.6rem; }
    th, td { border: 1px solid #ddd; padding: 0.5rem; vertical-align: top; text-align: left; }
    th { background: rgba(127,127,127,0.09); }
    code, pre { background: rgba(127,127,127,0.12); border-radius: 6px; }
    code { padding: 0.1rem 0.35rem; }
    pre { margin: 0.4rem 0 0 0; padding: 0.6rem; white-space: pre-wrap; word-break: break-all; }
  </style>
</head>
<body>
  <h1>NCC API Docs + Live Status</h1>
  <p class="muted">Auth mode: <code>%s</code> | OpenAPI: <a href="/api/v1/openapi.json">/api/v1/openapi.json</a> | Swagger UI: <a href="/docs/ui">/docs/ui</a></p>

  <div class="row">
    <div class="card"><div class="label">Server status</div><div class="value" id="st-health">Loading...</div></div>
    <div class="card"><div class="label">API auth mode</div><div class="value" id="st-auth">-</div></div>
    <div class="card"><div class="label">Rate limit blocked total</div><div class="value" id="st-blocked">-</div></div>
    <div class="card"><div class="label">Active limiter buckets</div><div class="value" id="st-buckets">-</div></div>
  </div>

  <h2>Endpoint Explorer</h2>
  <div class="toolbar">
    <input id="route-filter" placeholder="Filter endpoints (path, method, description)">
    <input id="api-token" placeholder="Optional token for protected curl examples">
    <button id="refresh-btn" type="button">Refresh</button>
  </div>
  <table>
    <thead><tr><th>Path</th><th>Methods</th><th>Description</th><th>Try-it curl</th></tr></thead>
    <tbody id="routes-body"><tr><td colspan="4">Loading routes...</td></tr></tbody>
  </table>

  <script>
    const state = { routes: [], health: null, metrics: null };
    function esc(s) { return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", "\"":"&quot;", "'":"&#39;" }[c])); }
    function tokenHeader(token) {
      const t = String(token || "").trim();
      return t ? '-H "X-API-Token: ' + t + '" ' : "";
    }
    function curlForRoute(route, token) {
      const method = (route.methods && route.methods[0]) || "GET";
      const base = window.location.origin + route.path;
      const hdr = tokenHeader(token);
      if (route.sample_body && (method === "POST" || method === "PUT")) {
        return "curl -X " + method + " " + hdr + '-H "Content-Type: application/json" ' + base + " -d '" + String(route.sample_body) + "'";
      }
      return "curl -X " + method + " " + hdr + base;
    }
    function renderRoutes() {
      const body = document.getElementById("routes-body");
      const q = document.getElementById("route-filter").value.toLowerCase().trim();
      const token = document.getElementById("api-token").value;
      const rows = state.routes.filter(r => {
        if (!q) return true;
        const hay = ((r.path || "").toLowerCase() + " " + (r.description || "").toLowerCase() + " " + (r.methods || []).join(" ").toLowerCase());
        return hay.includes(q);
      });
      if (rows.length === 0) {
        body.innerHTML = "<tr><td colspan='4'>No routes match current filter.</td></tr>";
        return;
      }
      body.innerHTML = rows.map(r => {
        const curl = curlForRoute(r, token);
        const methods = (r.methods || []).map(m => "<code>" + esc(m) + "</code>").join(" ");
        return "<tr>" +
          "<td><code>" + esc(r.path) + "</code></td>" +
          "<td>" + methods + "</td>" +
          "<td>" + esc(r.description || "") + "</td>" +
          "<td><pre>" + esc(curl) + "</pre></td>" +
        "</tr>";
      }).join("");
    }
    async function refreshStatus() {
      try {
        const healthRes = await fetch("/api/v1/health");
        const healthJson = await healthRes.json();
        state.health = healthJson.data || {};
      } catch (_) {
        state.health = null;
      }
      try {
        const metricsRes = await fetch("/api/v1/metrics/rate-limit");
        const metricsJson = await metricsRes.json();
        state.metrics = metricsJson.data || {};
      } catch (_) {
        state.metrics = null;
      }
      document.getElementById("st-health").textContent = (state.health && state.health.status) || "unavailable";
      document.getElementById("st-auth").textContent = (state.health && state.health.auth_mode) || "-";
      document.getElementById("st-blocked").textContent = ((state.metrics && state.metrics.metrics && state.metrics.metrics.blocked_total) ?? "-");
      document.getElementById("st-buckets").textContent = ((state.metrics && state.metrics.metrics && state.metrics.metrics.active_buckets) ?? "-");
    }
    async function refreshRoutes() {
      const res = await fetch("/api/v1/meta/routes");
      const json = await res.json();
      state.routes = (json.data && json.data.routes) || [];
      renderRoutes();
    }
    async function refreshAll() {
      await Promise.all([refreshStatus(), refreshRoutes()]);
    }
    document.getElementById("route-filter").addEventListener("input", renderRoutes);
    document.getElementById("api-token").addEventListener("input", renderRoutes);
    document.getElementById("refresh-btn").addEventListener("click", refreshAll);
    refreshAll();
    setInterval(refreshStatus, 5000);
  </script>
</body>
</html>
`, authMode)
}

func (s *apiServer) handleSwaggerUIPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NCC API Swagger UI</title>
  <link rel="stylesheet" href="/docs/assets/swagger-ui.css">
</head>
<body>
  <div style="margin:10px 16px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">
    <a href="/">Back to API docs</a>
  </div>
  <div id="swagger-ui"></div>
  <script src="/docs/assets/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/api/v1/openapi.json",
      dom_id: "#swagger-ui",
      deepLinking: true,
      displayRequestDuration: true,
      persistAuthorization: true
    });
  </script>
</body>
</html>`)
}

func (s *apiServer) handleAuthSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !isLoopbackRequest(r) {
		s.audit(r, "auth.session.issue", false, map[string]interface{}{"reason": "non_loopback"})
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "session bootstrap allowed only from loopback"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		s.audit(r, "auth.session.issue", false, map[string]interface{}{"reason": "bad_token"})
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	token, exp, err := s.issueSessionToken(cleanClientIP(r))
	if err != nil {
		s.audit(r, "auth.session.issue", false, map[string]interface{}{"reason": "issue_failed", "error": err.Error()})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.session.issue", true, map[string]interface{}{"ttl_sec": int(s.sessionTTL.Seconds())})
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"token":      token,
		"expires_at": exp.Format(time.RFC3339),
		"ttl_sec":    int(s.sessionTTL.Seconds()),
	}})
}

func (s *apiServer) handleAuthRotate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		s.audit(r, "auth.token.rotate", false, map[string]interface{}{"reason": "bad_token"})
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		s.audit(r, "auth.token.rotate", false, map[string]interface{}{"reason": "rand_failed", "error": err.Error()})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: fmt.Sprintf("generate token: %v", err)})
		return
	}
	s.authToken = base64.RawURLEncoding.EncodeToString(b)
	if err := s.ensureAuthToken(); err != nil {
		s.audit(r, "auth.token.rotate", false, map[string]interface{}{"reason": "persist_failed", "error": err.Error()})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.token.rotate", true, nil)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "token rotated"})
}

func (s *apiServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		reqPath := strings.TrimSpace(r.URL.Query().Get("path"))
		targetPath := s.configPath
		if reqPath != "" {
			targetPath = reqPath
		}
		cfgPath, err := s.validateConfigPath(targetPath)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		b, err := os.ReadFile(cfgPath)
		if errors.Is(err, os.ErrNotExist) {
			// Bootstrap a dummy config through the root orchestrator flow.
			// validate-config currently requires an existing file, while root --config creates one when missing.
			out, runErr := s.runOrchestrator([]string{"--config", cfgPath}, 30*time.Second)
			b, err = os.ReadFile(cfgPath)
			if err == nil {
				s.audit(r, "settings.config.bootstrap", true, map[string]interface{}{"config_path": cfgPath})
			} else {
				data := map[string]interface{}{"output": tailString(redactSensitiveText(strings.TrimSpace(out)), 4000)}
				if runErr != nil {
					data["bootstrap_error"] = runErr.Error()
				}
				writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: fmt.Sprintf("config file not found: %s", cfgPath), Data: data})
				return
			}
		} else if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"path":             cfgPath,
			"content":          string(b),
			"content_redacted": redactSensitiveText(string(b)),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req configUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if strings.TrimSpace(req.Content) == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "content is required"})
			return
		}
		targetPath := s.configPath
		if strings.TrimSpace(req.Path) != "" {
			targetPath = strings.TrimSpace(req.Path)
		}
		cfgPath, err := s.validateConfigPath(targetPath)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		ext := filepath.Ext(cfgPath)
		base := strings.TrimSuffix(cfgPath, ext)
		if ext == "" {
			ext = ".yaml"
			base = cfgPath
		}
		// Keep temp file extension parseable by Viper during validate-config.
		tmpPath := base + ".tmp" + ext
		if err := os.WriteFile(tmpPath, []byte(req.Content), 0o600); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		defer os.Remove(tmpPath)
		cmd := s.makeOrchestratorCommand(context.TODO(), "validate-config", "--config", tmpPath)
		cmd.Dir = s.absPath(s.repoRoot)
		out, err := cmd.CombinedOutput()
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{
				Success: false,
				Error:   fmt.Sprintf("strict config validation failed: %v", err),
				Data:    map[string]string{"output": redactSensitiveText(string(out))},
			})
			return
		}
		if err := os.Rename(tmpPath, cfgPath); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.config.update", true, map[string]interface{}{"config_path": cfgPath})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "config updated", Data: map[string]interface{}{
			"path":     cfgPath,
			"validate": true,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) applyConfigBatchOperation(r *http.Request, op configBatchOperation) map[string]interface{} {
	action := strings.ToLower(strings.TrimSpace(op.Action))
	result := map[string]interface{}{
		"action": action,
		"path":   strings.TrimSpace(op.Path),
		"ok":     false,
	}
	cfgPath, err := s.validateConfigPath(op.Path)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	result["resolved"] = cfgPath
	activeCfgPath, _ := s.validateConfigPath(s.configPath)
	switch action {
	case "add", "update":
		if strings.TrimSpace(op.Content) == "" {
			result["error"] = "content is required for add/update"
			return result
		}
		if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
			result["error"] = err.Error()
			return result
		}
		ext := filepath.Ext(cfgPath)
		base := strings.TrimSuffix(cfgPath, ext)
		if ext == "" {
			ext = ".yaml"
			base = cfgPath
		}
		tmpPath := base + ".tmp" + ext
		if err := os.WriteFile(tmpPath, []byte(op.Content), 0o600); err != nil {
			result["error"] = err.Error()
			return result
		}
		defer os.Remove(tmpPath)
		cmd := s.makeOrchestratorCommand(context.TODO(), "validate-config", "--config", tmpPath)
		cmd.Dir = s.absPath(s.repoRoot)
		out, err := cmd.CombinedOutput()
		if err != nil {
			result["error"] = fmt.Sprintf("strict config validation failed: %v", err)
			result["validate_output"] = tailString(redactSensitiveText(string(out)), 2000)
			return result
		}
		if err := os.Rename(tmpPath, cfgPath); err != nil {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = true
		s.audit(r, "settings.config.batch_update", true, map[string]interface{}{"config_path": cfgPath, "action": action})
		return result
	case "remove", "delete":
		if activeCfgPath != "" && strings.EqualFold(cfgPath, activeCfgPath) {
			result["error"] = "cannot remove active config-path"
			return result
		}
		if err := os.Remove(cfgPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = false
		s.audit(r, "settings.config.batch_delete", true, map[string]interface{}{"config_path": cfgPath, "action": action})
		return result
	default:
		result["error"] = "action must be one of: add, update, remove, delete"
		return result
	}
}

func (s *apiServer) handleConfigBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req configBatchRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if len(req.Operations) == 0 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "operations must not be empty"})
		return
	}
	results := make([]map[string]interface{}, 0, len(req.Operations))
	okCount := 0
	failCount := 0
	for _, op := range req.Operations {
		res := s.applyConfigBatchOperation(r, op)
		if ok, _ := res["ok"].(bool); ok {
			okCount++
		} else {
			failCount++
		}
		results = append(results, res)
	}
	msg := fmt.Sprintf("processed %d operation(s): %d succeeded, %d failed", len(req.Operations), okCount, failCount)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{
		"total":   len(req.Operations),
		"ok":      okCount,
		"failed":  failCount,
		"results": results,
	}})
}

func (s *apiServer) discoverAvailableConfigFiles() ([]availableConfigFile, error) {
	activeCfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	rootAbs, err := filepath.Abs(s.repoRoot)
	if err != nil {
		return nil, err
	}
	rootAbs = filepath.Clean(rootAbs)
	candidates := map[string]bool{activeCfgPath: true}

	// "configs/" is the canonical home for additional per-cluster config files.
	configsDir := filepath.Join(rootAbs, "configs")
	_ = filepath.WalkDir(configsDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		if abs, err := s.validateConfigPath(path); err == nil {
			candidates[abs] = true
		}
		return nil
	})

	// Also include any immediate sibling .ya?ml files near the active config.
	// Operators commonly create per-cluster files with non-"config*" names.
	activeDir := filepath.Dir(activeCfgPath)
	if entries, readErr := os.ReadDir(activeDir); readErr == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := strings.ToLower(entry.Name())
			ext := strings.ToLower(filepath.Ext(name))
			if ext != ".yaml" && ext != ".yml" {
				continue
			}
			if abs, err := s.validateConfigPath(filepath.Join(activeDir, entry.Name())); err == nil {
				candidates[abs] = true
			}
		}
	}

	items := make([]availableConfigFile, 0, len(candidates))
	for abs := range candidates {
		rel, relErr := filepath.Rel(rootAbs, abs)
		pathVal := abs
		if relErr == nil && rel != "" && !strings.HasPrefix(rel, "..") {
			pathVal = filepath.ToSlash(rel)
		}
		_, statErr := os.Stat(abs)
		items = append(items, availableConfigFile{
			Path:     pathVal,
			Resolved: abs,
			Exists:   statErr == nil,
			IsActive: strings.EqualFold(abs, activeCfgPath),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].IsActive != items[j].IsActive {
			return items[i].IsActive
		}
		return items[i].Path < items[j].Path
	})
	return items, nil
}

func (s *apiServer) resolvePreferredRunConfigPath(r *http.Request, requestedPath string) string {
	if v := strings.TrimSpace(requestedPath); v != "" {
		return v
	}
	if p, ok := principalFromContext(r.Context()); ok && s.users != nil {
		if pref := strings.TrimSpace(s.users.getRunConfigPath(p.subject)); pref != "" {
			return pref
		}
	}
	return s.configPath
}

func (s *apiServer) handleConfigsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	items, err := s.discoverAvailableConfigFiles()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	defaultPath := ""
	if p, ok := principalFromContext(r.Context()); ok && s.users != nil {
		defaultPath = strings.TrimSpace(s.users.getRunConfigPath(p.subject))
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items":        items,
		"default_path": defaultPath,
	}})
}

func (s *apiServer) handleRunConfigPreference(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "user preferences require a writable user database"})
		return
	}
	p, ok := principalFromContext(r.Context())
	if !ok || strings.TrimSpace(p.subject) == "" {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"path": strings.TrimSpace(s.users.getRunConfigPath(p.subject)),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req runConfigPreferenceRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		val := strings.TrimSpace(req.Path)
		if val != "" {
			if _, err := s.validateConfigPath(val); err != nil {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
				return
			}
		}
		if err := s.users.setRunConfigPath(p.subject, val); err != nil {
			writeUserStoreError(w, err)
			return
		}
		s.audit(r, "runs.config_preference.update", true, map[string]interface{}{"path": val})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "run config preference updated", Data: map[string]interface{}{"path": val}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func parseScalarConfigValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case fmt.Stringer:
		return strings.TrimSpace(t.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func redactSensitiveText(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return raw
	}
	lines := strings.Split(raw, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			out = append(out, line)
			continue
		}
		lower := strings.ToLower(trimmed)
		if strings.Contains(lower, "password:") || strings.Contains(lower, "token:") || strings.Contains(lower, "secret:") {
			if idx := strings.Index(line, ":"); idx >= 0 {
				out = append(out, line[:idx+1]+" \"***\"")
				continue
			}
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func isUnsetConfigPathLiteral(val string) bool {
	v := strings.ToLower(strings.TrimSpace(val))
	return v == "" || v == "null" || v == "nil" || v == "<nil>" || v == "~"
}

func extractRelatedFilePathFromConfig(content string, key string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return ""
	}
	var ym map[string]interface{}
	if err := yaml.Unmarshal([]byte(content), &ym); err == nil {
		val := parseScalarConfigValue(ym[key])
		if isUnsetConfigPathLiteral(val) {
			return ""
		}
		return val
	}
	if strings.HasPrefix(trimmed, "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			val := parseScalarConfigValue(m[key])
			if isUnsetConfigPathLiteral(val) {
				return ""
			}
			return val
		}
	}
	pattern := fmt.Sprintf(`(?m)^\s*%s\s*:\s*(.+?)\s*$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(content)
	if len(matches) < 2 {
		return ""
	}
	val := strings.TrimSpace(matches[1])
	val = strings.Trim(val, `"'`)
	if isUnsetConfigPathLiteral(val) {
		return ""
	}
	return val
}

func validateClusterAddressForConfigFile(cluster string) error {
	cluster = strings.TrimSpace(cluster)
	if cluster == "" {
		return errors.New("cluster address cannot be empty")
	}
	if len(cluster) > 255 {
		return errors.New("cluster address too long")
	}
	if net.ParseIP(cluster) != nil {
		return nil
	}
	if strings.Contains(cluster, "..") || strings.HasPrefix(cluster, ".") || strings.HasSuffix(cluster, ".") {
		return errors.New("invalid cluster hostname format")
	}
	for _, r := range cluster {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("invalid character %q in cluster address", r)
	}
	return nil
}

func validateClustersFileContent(content string) error {
	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r := csv.NewReader(strings.NewReader(line))
		r.TrimLeadingSpace = true
		r.FieldsPerRecord = -1
		rec, err := r.Read()
		if err != nil {
			return fmt.Errorf("line %d: invalid CSV format: %w", i+1, err)
		}
		for idx := range rec {
			rec[idx] = strings.TrimSpace(rec[idx])
		}
		switch len(rec) {
		case 1:
			if err := validateClusterAddressForConfigFile(rec[0]); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
		case 2, 3:
			if err := validateClusterAddressForConfigFile(rec[0]); err != nil {
				return fmt.Errorf("line %d: %w", i+1, err)
			}
			if rec[1] == "" {
				return fmt.Errorf("line %d: username is empty", i+1)
			}
		default:
			return fmt.Errorf("line %d: expected cluster[,username[,password]]", i+1)
		}
	}
	return nil
}

func validateExcludeAlertTitlesFileContent(content string, matchMode string) error {
	mode := strings.ToLower(strings.TrimSpace(matchMode))
	if mode == "" {
		mode = "exact"
	}
	if mode != "exact" && mode != "contains" && mode != "regex" {
		return fmt.Errorf("unsupported exclude-alert-match-mode: %s", matchMode)
	}
	if mode != "regex" {
		return nil
	}
	for i, line := range strings.Split(content, "\n") {
		title := strings.TrimSpace(line)
		if title == "" || strings.HasPrefix(title, "#") {
			continue
		}
		if _, err := regexp.Compile(title); err != nil {
			return fmt.Errorf("line %d: invalid regex %q: %w", i+1, title, err)
		}
	}
	return nil
}

func validateSecretsFileContent(content string) error {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil
	}
	var jsonMap map[string]string
	if json.Unmarshal([]byte(trimmed), &jsonMap) == nil {
		return nil
	}
	var yamlMap map[string]interface{}
	if err := yaml.Unmarshal([]byte(trimmed), &yamlMap); err != nil {
		return fmt.Errorf("secrets file must be valid YAML/JSON map: %w", err)
	}
	for k, v := range yamlMap {
		if _, ok := v.(string); !ok {
			return fmt.Errorf("secret %q must be a string value", k)
		}
	}
	return nil
}

func (s *apiServer) configScalarValue(key string, def string) string {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return def
	}
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return def
	}
	val := strings.TrimSpace(extractRelatedFilePathFromConfig(string(content), key))
	if val == "" {
		return def
	}
	return val
}

func (s *apiServer) validateConfigRelatedFileContent(ref *configRelatedFile, content string) error {
	switch ref.Key {
	case "clusters-file":
		return validateClustersFileContent(content)
	case "exclude-alert-titles-file":
		return validateExcludeAlertTitlesFileContent(content, s.configScalarValue("exclude-alert-match-mode", "exact"))
	case "secrets-file":
		return validateSecretsFileContent(content)
	default:
		return nil
	}
}

func (s *apiServer) discoverConfigRelatedFiles() ([]configRelatedFile, error) {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	keys := []string{
		"clusters-file",
		"exclude-alert-titles-file",
		"secrets-file",
	}
	out := make([]configRelatedFile, 0, len(keys))
	for _, key := range keys {
		rawPath := extractRelatedFilePathFromConfig(string(content), key)
		if rawPath == "" {
			continue
		}
		resolved, err := s.normalizeAndConfinePath(rawPath)
		if err != nil {
			out = append(out, configRelatedFile{
				Key:          key,
				Path:         rawPath,
				ResolvedPath: "",
				Exists:       false,
			})
			continue
		}
		info, statErr := os.Stat(resolved)
		item := configRelatedFile{
			Key:          key,
			Path:         rawPath,
			ResolvedPath: resolved,
			Exists:       statErr == nil,
		}
		if statErr == nil {
			item.Size = info.Size()
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *apiServer) relatedConfigFileByPath(rawPath string) (*configRelatedFile, error) {
	target := strings.TrimSpace(rawPath)
	if isUnsetConfigPathLiteral(target) {
		return nil, errors.New("path is required")
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		return nil, err
	}
	for i := range items {
		item := items[i]
		if strings.EqualFold(item.Path, target) || (item.ResolvedPath != "" && strings.EqualFold(item.ResolvedPath, target)) {
			return &item, nil
		}
	}
	return nil, fmt.Errorf("path is not referenced by config: %s", target)
}

func (s *apiServer) handleConfigFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	items, err := s.discoverConfigRelatedFiles()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items": items,
	}})
}

func (s *apiServer) handleConfigFile(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pathArg := strings.TrimSpace(r.URL.Query().Get("path"))
		ref, err := s.relatedConfigFileByPath(pathArg)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if ref.ResolvedPath == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "path cannot be resolved inside repo root"})
			return
		}
		content, err := os.ReadFile(ref.ResolvedPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
					"key":      ref.Key,
					"path":     ref.Path,
					"resolved": ref.ResolvedPath,
					"exists":   false,
					"content":  "",
				}})
				return
			}
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"key":      ref.Key,
			"path":     ref.Path,
			"resolved": ref.ResolvedPath,
			"exists":   true,
			"content":  string(content),
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req configRelatedFileUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		ref, err := s.relatedConfigFileByPath(req.Path)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if ref.ResolvedPath == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "path cannot be resolved inside repo root"})
			return
		}
		if err := s.validateConfigRelatedFileContent(ref, req.Content); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("validation failed for %s: %v", ref.Key, err)})
			return
		}
		if err := os.MkdirAll(filepath.Dir(ref.ResolvedPath), 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := os.WriteFile(ref.ResolvedPath, []byte(req.Content), 0o600); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "settings.config_file.update", true, map[string]interface{}{"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "config file updated", Data: map[string]interface{}{
			"key":      ref.Key,
			"path":     ref.Path,
			"resolved": ref.ResolvedPath,
			"exists":   true,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) applyConfigFileOperation(r *http.Request, op configRelatedFileBatchOperation) map[string]interface{} {
	action := strings.ToLower(strings.TrimSpace(op.Action))
	result := map[string]interface{}{
		"action": action,
		"path":   strings.TrimSpace(op.Path),
		"ok":     false,
	}
	ref, err := s.relatedConfigFileByPath(op.Path)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	result["key"] = ref.Key
	result["resolved"] = ref.ResolvedPath
	if ref.ResolvedPath == "" {
		result["error"] = "path cannot be resolved inside repo root"
		return result
	}
	switch action {
	case "add", "update":
		if err := s.validateConfigRelatedFileContent(ref, op.Content); err != nil {
			result["error"] = fmt.Sprintf("validation failed for %s: %v", ref.Key, err)
			return result
		}
		if err := os.MkdirAll(filepath.Dir(ref.ResolvedPath), 0o755); err != nil {
			result["error"] = err.Error()
			return result
		}
		if err := os.WriteFile(ref.ResolvedPath, []byte(op.Content), 0o600); err != nil {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = true
		s.audit(r, "settings.config_file.update", true, map[string]interface{}{
			"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath, "action": action,
		})
		return result
	case "remove", "delete":
		if err := os.Remove(ref.ResolvedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result["error"] = err.Error()
			return result
		}
		result["ok"] = true
		result["exists"] = false
		s.audit(r, "settings.config_file.delete", true, map[string]interface{}{
			"key": ref.Key, "path": ref.Path, "resolved": ref.ResolvedPath, "action": action,
		})
		return result
	default:
		result["error"] = "action must be one of: add, update, remove, delete"
		return result
	}
}

func (s *apiServer) handleConfigFilesBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req configRelatedFileBatchRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if len(req.Operations) == 0 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "operations must not be empty"})
		return
	}
	results := make([]map[string]interface{}, 0, len(req.Operations))
	okCount := 0
	failCount := 0
	for _, op := range req.Operations {
		res := s.applyConfigFileOperation(r, op)
		if ok, _ := res["ok"].(bool); ok {
			okCount++
		} else {
			failCount++
		}
		results = append(results, res)
	}
	msg := fmt.Sprintf("processed %d operation(s): %d succeeded, %d failed", len(req.Operations), okCount, failCount)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: msg, Data: map[string]interface{}{
		"total":   len(req.Operations),
		"ok":      okCount,
		"failed":  failCount,
		"results": results,
	}})
}

func (s *apiServer) handleSchedule(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		st, err := s.loadSchedule()
		if err != nil {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: st})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req scheduleUpdateRequest
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		st := scheduleState{
			Type:      defaultIfEmpty(req.Type, "auto"),
			Action:    defaultIfEmpty(req.Action, "create"),
			Cron:      strings.TrimSpace(req.Cron),
			Every:     strings.TrimSpace(req.Every),
			Config:    defaultIfEmpty(req.Config, s.configPath),
			LogPath:   strings.TrimSpace(req.LogPath),
			TaskName:  strings.TrimSpace(req.TaskName),
			PrintOnly: true,
			WithLock:  true,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		if req.PrintOnly != nil {
			st.PrintOnly = *req.PrintOnly
		}
		if req.WithLock != nil {
			st.WithLock = *req.WithLock
		}
		if err := validateScheduleInput(st); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		// Anchor config + log paths to absolute, install-root paths before we
		// persist or apply them. Otherwise the orchestrator's create-schedule
		// resolves a relative path against ITS OWN working directory (e.g.
		// <install>/bin), producing a cron line / systemd unit whose runner
		// can't find config.yaml — the "at least one cluster must be provided"
		// failure that silently breaks scheduled runs. Resolving here makes the
		// generated schedule deterministic regardless of the api-server's cwd.
		resolvedCfg, cfgErr := s.validateConfigPath(st.Config)
		if cfgErr != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: cfgErr.Error()})
			return
		}
		st.Config = resolvedCfg
		if st.LogPath != "" {
			st.LogPath = s.absPath(st.LogPath)
		}
		if err := s.saveSchedule(st); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		resp := map[string]interface{}{"schedule": st, "applied": false}
		applyErr := error(nil)
		if req.Apply {
			args := []string{"create-schedule", "--type", st.Type, "--action", st.Action, "--config", st.Config, "--print-only=" + fmt.Sprintf("%t", st.PrintOnly)}
			if st.Cron != "" {
				args = append(args, "--cron", st.Cron)
			}
			if st.Every != "" {
				args = append(args, "--every", st.Every)
			}
			if st.LogPath != "" {
				args = append(args, "--log-path", st.LogPath)
			}
			args = append(args, fmt.Sprintf("--with-lock=%t", st.WithLock))
			if st.TaskName != "" {
				args = append(args, "--task-name", st.TaskName)
			}
			out, err := s.runOrchestrator(args, 60*time.Second)
			resp["applied"] = err == nil
			resp["command"] = append(s.orchestratorBaseCommand(), args...)
			resp["output"] = tailString(strings.TrimSpace(out), 4000)
			if err != nil {
				resp["apply_error"] = err.Error()
				applyErr = err
			}
		}
		s.audit(r, "schedule.update", applyErr == nil, map[string]interface{}{
			"applied": req.Apply,
			"type":    st.Type,
			"action":  st.Action,
		})
		// State was saved, but the apply step failed — surface that at the
		// envelope level instead of pretending the whole call succeeded.
		if applyErr != nil {
			writeJSON(w, http.StatusBadGateway, envelope{
				Success: false,
				Error:   fmt.Sprintf("schedule saved but apply failed: %v", applyErr),
				Data:    resp,
			})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "schedule updated", Data: resp})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func parseScheduleHealthFromLog(path string) map[string]string {
	out := map[string]string{
		"last_run":     "",
		"last_success": "",
		"last_error":   "",
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	lines := strings.Split(string(b), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if out["last_run"] == "" {
			out["last_run"] = line
		}
		lower := strings.ToLower(line)
		if out["last_success"] == "" && (strings.Contains(lower, "completed") || strings.Contains(lower, "success") || strings.Contains(lower, "report generated")) {
			out["last_success"] = line
		}
		if out["last_error"] == "" && (strings.Contains(lower, "error") || strings.Contains(lower, "failed") || strings.Contains(lower, "panic")) {
			out["last_error"] = line
		}
		if out["last_success"] != "" && out["last_error"] != "" && out["last_run"] != "" {
			break
		}
	}
	return out
}

func (s *apiServer) handleScheduleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	// loadSchedule returns a default state when no file is present, so we
	// cannot infer existence from a load error alone — stat the path directly
	// to give the UI an honest "state_file_exists" signal.
	stateFileExists := false
	if st, statErr := os.Stat(s.absPath(s.scheduleStatePath)); statErr == nil && !st.IsDir() {
		stateFileExists = true
	}
	st, err := s.loadSchedule()
	if err != nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"configured":        false,
			"saved":             false,
			"installed":         false,
			"state_file_exists": stateFileExists,
			"error":             err.Error(),
		}})
		return
	}
	saved := false
	if strings.TrimSpace(st.UpdatedAt) != "" {
		saved = true
	}
	taskName := strings.TrimSpace(st.TaskName)
	if taskName == "" {
		taskName = "ncc-orchestrator"
	}
	installed, installError := s.detectInstalledSchedule(taskName)

	logPath := strings.TrimSpace(st.LogPath)
	if logPath == "" {
		logPath = filepath.Join("logs", "ncc-scheduler.log")
	}
	logAbs := s.absPath(logPath)
	logInfo, statErr := os.Stat(logAbs)
	lockPath := filepath.Join(filepath.Dir(logAbs), ".ncc-scheduler.lock")
	parsed := parseScheduleHealthFromLog(logAbs)
	// Surface the exact config the schedule runs with (absolute) so the UI can
	// show — and operators can verify — which config.yaml the timer uses. A
	// relative/missing config here is precisely what silently breaks scheduled
	// runs ("at least one cluster must be provided").
	configDisp := strings.TrimSpace(st.Config)
	if configDisp != "" {
		configDisp = s.absPath(configDisp)
	}
	data := map[string]interface{}{
		"configured":        installed, // authoritative: schedule is actually installed in OS
		"saved":             saved,
		"installed":         installed,
		"state_file_exists": stateFileExists,
		"task_name":         taskName,
		"type":              st.Type,
		"action":            st.Action,
		"with_lock":         st.WithLock,
		"config":            configDisp,
		"every":             st.Every,
		"cron":              st.Cron,
		"log_path":          logAbs,
		"lock_path":         lockPath,
		"last_updated_at":   st.UpdatedAt,
		"last_run":          parsed["last_run"],
		"last_success":      parsed["last_success"],
		"last_error":        parsed["last_error"],
		"detector":          s.scheduleDetectorName(),
	}
	if installError != "" {
		data["install_check_error"] = installError
	}
	if statErr == nil {
		data["log_exists"] = true
		data["log_size"] = logInfo.Size()
		data["log_mod_time"] = logInfo.ModTime().UTC().Format(time.RFC3339)
	} else {
		data["log_exists"] = false
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// detectInstalledSchedule returns true iff a schedule entry tagged with the
// expected ncc-orchestrator marker is present in the host's scheduler.
//
// Detection is delegated to the orchestrator binary via
//
//	create-schedule --type=auto --action=list --task-name=<name>
//
// so that the API server never touches the host scheduler directly. The
// orchestrator prints the matching schedule line (containing the marker) when
// installed and a stable "No cron entries found" / "No scheduled tasks" line
// otherwise. Returns (installed, errorMessage). errorMessage is non-empty
// only when the orchestrator could not be invoked or returned an unexpected
// failure.
func (s *apiServer) detectInstalledSchedule(taskName string) (bool, string) {
	if taskName == "" {
		return false, ""
	}
	args := []string{
		"create-schedule",
		"--type", "auto",
		"--action", "list",
		"--task-name", taskName,
	}
	out, err := s.runOrchestrator(args, 8*time.Second)
	text := string(out)
	if err != nil {
		return false, fmt.Sprintf("orchestrator schedule list failed: %v: %s", err, strings.TrimSpace(tailString(text, 240)))
	}
	marker := fmt.Sprintf("# ncc-orchestrator:%s", taskName)
	if strings.Contains(text, marker) {
		return true, ""
	}
	// Defensive: also accept the marker without the leading hash in case the
	// orchestrator output format changes for the listing line. (Used by both
	// cron and schtasks list outputs which include the task name.)
	bareMarker := fmt.Sprintf("ncc-orchestrator:%s", taskName)
	if strings.Contains(text, bareMarker) && !strings.Contains(text, "No cron entries") && !strings.Contains(text, "No scheduled tasks") {
		return true, ""
	}
	return false, ""
}

func (s *apiServer) scheduleDetectorName() string {
	if runtime.GOOS == "windows" {
		return "schtasks (via orchestrator)"
	}
	return "crontab / systemd timer (via orchestrator)"
}

func (s *apiServer) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.artifactsRestricted(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "raw report artifacts include all clusters and are limited to administrators; use the dashboard for your cluster groups"})
		return
	}
	dir := s.absPath(s.outputDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	out := make([]artifactInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if isInternalArtifactName(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, artifactInfo{
			Name:    e.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
}

// artifactInlineMaxBytes caps the size of artifact bodies returned inline as
// JSON. Anything bigger is served as a truncated tail with a `truncated: true`
// flag so the UI can prompt the user to download instead. Direct downloads
// (?download=1) stream the full file via http.ServeFile and aren't subject to
// this cap.
const artifactInlineMaxBytes = 5 * 1024 * 1024

func (s *apiServer) handleArtifactByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.artifactsRestricted(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "raw report artifacts include all clusters and are limited to administrators; use the dashboard for your cluster groups"})
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, "..") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid artifact name"})
		return
	}
	if isInternalArtifactName(name) {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "artifact not found"})
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), name)
	info, statErr := os.Stat(path)
	if statErr != nil || info.IsDir() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "artifact not found"})
		return
	}
	if r.URL.Query().Get("download") == "1" {
		ct := mime.TypeByExtension(filepath.Ext(name))
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
		// http.ServeFile streams the response and supports Range requests,
		// avoiding an arbitrary-size buffer for large artifacts (NCC logs can
		// easily be tens of megabytes).
		http.ServeFile(w, r, path)
		return
	}
	// Inline JSON view: read up to the cap. Truncate large files so a
	// careless click on a huge log file can't OOM the server.
	f, openErr := os.Open(path)
	if openErr != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: openErr.Error()})
		return
	}
	defer f.Close()
	limited := io.LimitReader(f, artifactInlineMaxBytes+1)
	b, readErr := io.ReadAll(limited)
	if readErr != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: readErr.Error()})
		return
	}
	truncated := false
	if int64(len(b)) > artifactInlineMaxBytes {
		b = b[:artifactInlineMaxBytes]
		truncated = true
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"name":      name,
		"size":      info.Size(),
		"mod_time":  info.ModTime().UTC().Format(time.RFC3339),
		"content":   string(b),
		"truncated": truncated,
		"max_bytes": int64(artifactInlineMaxBytes),
	}})
}

// handleRuns returns a merged, enriched feed of historical orchestrator runs.
//
// Query params:
//   - limit (default 200, max 1000) — cap on entries returned (newest first).
//   - source=history|summary|trigger — keep only one source kind.
//   - since=RFC3339|2006-01-02 — drop entries older than this instant.
//
// Sources merged (deduped by minute-precision timestamp; "history" > "summary"
// > "trigger" when keeping a single representative for the same run):
//   - outputDir/runs/<id>/run-summary.json  (archived, full metrics)
//   - outputDir/run-summary.json            (the latest run on disk)
//   - audit-log runs.trigger events         (trigger-only, no artifacts)
func (s *apiServer) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}

	q := r.URL.Query()
	limit := 200
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid limit: must be a positive integer"})
			return
		}
		limit = n
	}
	if limit > 1000 {
		limit = 1000
	}
	sourceFilter := strings.TrimSpace(q.Get("source"))
	if sourceFilter != "" && sourceFilter != "history" && sourceFilter != "summary" && sourceFilter != "trigger" {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   "invalid source: must be one of history, summary, trigger",
		})
		return
	}
	var sinceTS time.Time
	if raw := strings.TrimSpace(q.Get("since")); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			sinceTS = t
		} else if t, err := time.Parse("2006-01-02", raw); err == nil {
			sinceTS = t
		} else {
			writeJSON(w, http.StatusBadRequest, envelope{
				Success: false,
				Error:   "invalid since: expected RFC3339 timestamp or YYYY-MM-DD date",
			})
			return
		}
	}

	outDir := s.absPath(s.outputDir)
	collected := []runInfo{}

	// 1) Archived history directories.
	runsDir := filepath.Join(outDir, "runs")
	if entries, err := os.ReadDir(runsDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			info, infoErr := e.Info()
			if infoErr != nil {
				continue
			}
			runPath := filepath.Join(runsDir, e.Name())
			ri := runInfo{
				ID:      e.Name(),
				Path:    runPath,
				ModTime: info.ModTime().UTC().Format(time.RFC3339),
				Source:  "history",
			}
			if _, err := os.Stat(filepath.Join(runPath, "index.html")); err == nil {
				ri.HasIndex = true
			}
			enrichRunInfoFromSummary(&ri, filepath.Join(runPath, "run-summary.json"))
			collected = append(collected, ri)
		}
	}

	// 2) Latest top-level run-summary.json (the in-place "current" run).
	latestSummary := filepath.Join(outDir, "run-summary.json")
	if info, err := os.Stat(latestSummary); err == nil {
		latestID := info.ModTime().UTC().Format("20060102T150405Z")
		latest := runInfo{
			ID:      latestID,
			Path:    outDir,
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
			Source:  "summary",
		}
		if _, err := os.Stat(filepath.Join(outDir, "index.html")); err == nil {
			latest.HasIndex = true
		}
		enrichRunInfoFromSummary(&latest, latestSummary)
		collected = append(collected, latest)
	}

	// 3) Audit-log `runs.trigger` events for triggers that may not have
	//    produced any artifacts (run still in flight, ephemeral storage, etc).
	if auditEntries, err := s.auditEntries(500, auditFilter{actionPrefix: "runs.trigger"}); err == nil {
		for _, e := range auditEntries {
			act, _ := e["action"].(string)
			if act != "runs.trigger" {
				continue
			}
			ts, _ := e["ts"].(string)
			if strings.TrimSpace(ts) == "" {
				continue
			}
			ri := runInfo{
				ID:      "trigger-" + ts,
				ModTime: ts,
				Source:  "trigger",
			}
			if ts != "" {
				ri.Timestamp = ts
			}
			if ok, present := e["success"].(bool); present {
				ri.Success = &ok
			}
			if v, _ := e["client"].(string); v != "" {
				ri.Client = v
			}
			if v, _ := e["user_agent"].(string); v != "" {
				ri.UserAgent = v
			}
			if v, _ := e["auth_mode"].(string); v != "" {
				ri.AuthMode = v
			}
			collected = append(collected, ri)
		}
	}

	// Dedupe by minute-precision timestamp; prefer richer sources.
	priority := map[string]int{"history": 3, "summary": 2, "trigger": 1}
	type dedupedEntry struct {
		ri  runInfo
		pri int
	}
	bucket := map[string]dedupedEntry{}
	bucketKey := func(ri runInfo) string {
		raw := ri.Timestamp
		if raw == "" {
			raw = ri.ModTime
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t.UTC().Format("2006-01-02T15:04")
		}
		return raw
	}
	for _, ri := range collected {
		k := bucketKey(ri)
		if k == "" {
			continue
		}
		p := priority[ri.Source]
		if existing, ok := bucket[k]; !ok || p > existing.pri {
			bucket[k] = dedupedEntry{ri: ri, pri: p}
		}
	}

	out := make([]runInfo, 0, len(bucket))
	for _, v := range bucket {
		if sourceFilter != "" && v.ri.Source != sourceFilter {
			continue
		}
		if !sinceTS.IsZero() {
			ref := v.ri.Timestamp
			if ref == "" {
				ref = v.ri.ModTime
			}
			if t, err := time.Parse(time.RFC3339, ref); err == nil && t.Before(sinceTS) {
				continue
			}
		}
		out = append(out, v.ri)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModTime > out[j].ModTime })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	// Cluster-group scoping: a run row aggregates every cluster in the run, so
	// for non-admins blank the cluster-level counts/metrics that would leak
	// global state. The run still appears (id/time/duration/outcome) and the
	// per-cluster detail comes from the filtered /report/data endpoint.
	if p, ok := principalFromContext(r.Context()); ok && !s.allowedClusters(p).unrestricted {
		for i := range out {
			redactRunInfoCounts(&out[i])
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
}

// redactRunInfoCounts zeroes the aggregate, all-cluster metric fields of a run
// row so a group-restricted caller cannot infer global cluster state from the
// runs feed. Identity/outcome fields are preserved for the listing.
func redactRunInfoCounts(ri *runInfo) {
	ri.ClustersOK = 0
	ri.ClustersFailed = 0
	ri.TotalChecks = 0
	ri.AvgHealthScore = 0
	ri.MinHealthScore = 0
	ri.FailTotal = 0
	ri.WarnTotal = 0
	ri.ErrTotal = 0
	ri.InfoTotal = 0
}

// handleRunsRouter dispatches /api/v1/runs/<id> requests that are not
// already claimed by more-specific routes ("summary", "active", "preflight",
// "trigger"). It supports GET (return archived run metadata + summary) and
// returns a clear 404 for unknown IDs. Path traversal is rejected.
func (s *apiServer) handleRunsRouter(w http.ResponseWriter, r *http.Request) {
	tail := strings.TrimPrefix(r.URL.Path, "/api/v1/runs/")
	if tail == "" {
		s.handleRuns(w, r)
		return
	}
	// Reserved sub-routes are claimed by their explicit registrations above;
	// anything else is treated as an archived run ID lookup.
	if strings.Contains(tail, "/") || strings.Contains(tail, "..") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid run id"})
		return
	}
	s.handleRunByID(w, r, tail)
}

// handleRunByID returns metadata + summary for a single archived run under
// outputDir/runs/<id>/. Only GET is supported.
func (s *apiServer) handleRunByID(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	runPath := filepath.Join(s.absPath(s.outputDir), "runs", id)
	info, err := os.Stat(runPath)
	if err != nil || !info.IsDir() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: fmt.Sprintf("run %q not found", id)})
		return
	}
	ri := runInfo{
		ID:      id,
		Path:    runPath,
		ModTime: info.ModTime().UTC().Format(time.RFC3339),
		Source:  "history",
	}
	if _, err := os.Stat(filepath.Join(runPath, "index.html")); err == nil {
		ri.HasIndex = true
	}
	enrichRunInfoFromSummary(&ri, filepath.Join(runPath, "run-summary.json"))

	// Embed the artifacts we can find — keeps the UI from making N follow-up
	// requests for a single drill-down.
	artifacts := map[string]interface{}{}
	for _, name := range []string{"run-summary.json", "ncc-run-record.json", "regression-summary.json", "checks-snapshot.json", "run-meta.json"} {
		if b, rerr := os.ReadFile(filepath.Join(runPath, name)); rerr == nil {
			var v interface{}
			if json.Unmarshal(b, &v) == nil {
				artifacts[name] = v
			} else {
				artifacts[name] = string(b)
			}
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"run":       ri,
		"artifacts": artifacts,
	}})
}

// enrichRunInfoFromSummary fills the metric fields on ri by parsing a
// run-summary.json file. Missing/invalid files are silently ignored.
func enrichRunInfoFromSummary(ri *runInfo, summaryPath string) {
	b, err := os.ReadFile(summaryPath)
	if err != nil {
		return
	}
	var s trendRunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return
	}
	if strings.TrimSpace(s.Timestamp) != "" {
		ri.Timestamp = s.Timestamp
		if ri.ModTime == "" {
			ri.ModTime = s.Timestamp
		}
	}
	ri.DurationS = s.DurationS
	ri.RunSource = strings.TrimSpace(s.Source)
	ri.ClustersOK = s.ClustersOK
	ri.ClustersFailed = s.ClustersFailed
	ri.TotalChecks = s.TotalChecks
	ri.AvgHealthScore = s.AvgHealthScore
	ri.MinHealthScore = s.MinHealthScore
	for _, c := range s.Clusters {
		ri.FailTotal += c.FailCount
		ri.WarnTotal += c.WarnCount
		ri.ErrTotal += c.ErrCount
		ri.InfoTotal += c.InfoCount
	}
	if s.ExitCode != nil {
		v := *s.ExitCode
		ri.ExitCode = &v
		success := v == 0
		ri.Success = &success
	}
}

func (s *apiServer) handleRunSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), "run-summary.json")
	b, err := os.ReadFile(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	var raw interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	// Apply the same on-read error-classification healing as
	// /api/v1/report/data so /runs/summary stays consistent.
	if m, ok := raw.(map[string]interface{}); ok {
		reclassifyRunSummaryInPlace(m)
		raw = m
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: raw})
}

func (s *apiServer) handleRunActive(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// fallthrough to snapshot code below
	case http.MethodDelete:
		s.cancelActiveRun(w, r)
		return
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	// Concurrent run engine: report the full set of queued/running runs as well
	// as the legacy single-run summary (mirroring the most-recent running run)
	// so older clients keep working.
	runs := s.activeRunsSnapshot()
	queued := s.queuedCount()
	s.mu.Lock()
	defer s.mu.Unlock()
	var elapsedSeconds int
	var elapsedHuman string
	var overdue bool
	var deadlineISO string
	if s.active && !s.started.IsZero() {
		elapsed := time.Since(s.started).Round(time.Second)
		elapsedSeconds = int(elapsed.Seconds())
		elapsedHuman = elapsed.String()
		if s.runTimeout > 0 {
			deadlineISO = s.started.Add(s.runTimeout).UTC().Format(time.RFC3339)
			overdue = time.Now().After(s.started.Add(s.runTimeout))
		}
	}
	runningCount := 0
	managedPIDs := map[int]bool{}
	for _, rec := range s.runs {
		if rec.status == "running" {
			runningCount++
		}
		if rec.pid > 0 {
			managedPIDs[rec.pid] = true
		}
	}
	// Surface runs this api-server didn't launch (scheduled systemd-timer/cron
	// runs, or a manual CLI run on the host) so they appear in Active Runs too.
	if ext := s.externalActiveRuns(managedPIDs); len(ext) > 0 {
		runs = append(runs, ext...)
		runningCount += len(ext)
	}
	data := map[string]interface{}{
		"active":            s.active,
		"started_at":        s.started.UTC().Format(time.RFC3339),
		"elapsed_seconds":   elapsedSeconds,
		"elapsed_human":     elapsedHuman,
		"expected_deadline": deadlineISO,
		"overdue":           overdue,
		"pid":               s.lastPID,
		"last_error":        s.lastErr,
		"last_output":       s.lastOut,
		"live_output":       s.currentLiveOutput(),
		"runner_log":        s.absPath(s.runnerLogPath),
		"output_dir":        s.absPath(s.outputDir),
		"config_path":       defaultIfEmpty(s.lastCfg, s.absPath(s.configPath)),
		// Concurrent-run fields.
		"runs":                 runs,
		"running_count":        runningCount,
		"queued_count":         queued,
		"max_concurrent":       s.maxConcurrentRuns,
		"avg_run_duration_sec": s.avgRunDurationSeconds(),
	}
	if s.debugExpose {
		data["command"] = s.lastCmd
		data["work_dir"] = s.lastCwd
		data["env"] = s.lastEnv
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// cancelActiveRun cancels in-flight orchestrator run(s). With ?id=<run-id> it
// cancels a single run (running: signals the process to exit; queued: removes it
// from the queue and frees its reserved clusters). Without an id, it cancels
// every queued and running run. The goroutine running cmd.Wait() observes the
// context cancellation, reaps the child, and routes the result through
// finishRun() exactly as it would for a normal exit.
//
// This is the user-facing escape hatch for runs that are stuck on a slow
// cluster, broken DNS, etc., without forcing the operator to restart the API
// server.
func (s *apiServer) cancelActiveRun(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id != "" {
		rec, ok := s.cancelRunByID(id)
		if !ok {
			writeJSON(w, http.StatusConflict, envelope{
				Success: false,
				Error:   fmt.Sprintf("no queued or running run with id %q", id),
				Data:    map[string]interface{}{"run_id": id},
			})
			return
		}
		s.audit(r, "runs.cancel", true, map[string]interface{}{"run_id": id, "pid": rec.pid})
		writeJSON(w, http.StatusAccepted, envelope{
			Success: true,
			Message: "cancellation signalled",
			Data: map[string]interface{}{
				"run_id":        rec.id,
				"pid":           rec.pid,
				"poll_endpoint": "/api/v1/runs/active",
			},
		})
		return
	}

	n := s.cancelAllRuns()
	if n == 0 {
		writeJSON(w, http.StatusConflict, envelope{
			Success: false,
			Error:   "no run is currently active",
			Data:    map[string]interface{}{"active": false},
		})
		return
	}
	s.audit(r, "runs.cancel", true, map[string]interface{}{"cancelled": n})
	writeJSON(w, http.StatusAccepted, envelope{
		Success: true,
		Message: fmt.Sprintf("cancellation signalled for %d run(s)", n),
		Data: map[string]interface{}{
			"cancelled":     n,
			"poll_endpoint": "/api/v1/runs/active",
		},
	})
}

func (s *apiServer) handleRunPreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var req runPreflightRequest
	if r.ContentLength > 0 {
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
	}
	cfgPath := s.resolvePreferredRunConfigPath(r, req.ConfigPath)
	resolvedCfgPath, err := s.validateConfigPath(cfgPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	out, err := s.runOrchestrator([]string{"preflight-check", "--config", resolvedCfgPath, "--format", "json"}, 120*time.Second)
	if err != nil {
		s.audit(r, "runs.preflight", false, map[string]interface{}{
			"config_path": resolvedCfgPath,
			"reason":      "orchestrator_failed",
			"error":       err.Error(),
		})
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   fmt.Sprintf("preflight-check failed: %v", err),
			Data:    map[string]string{"output": tailString(strings.TrimSpace(out), 4000)},
		})
		return
	}
	var payload map[string]interface{}
	if uerr := json.Unmarshal([]byte(strings.TrimSpace(out)), &payload); uerr != nil {
		s.audit(r, "runs.preflight", false, map[string]interface{}{
			"config_path": resolvedCfgPath,
			"reason":      "parse_failed",
			"error":       uerr.Error(),
		})
		writeJSON(w, http.StatusInternalServerError, envelope{
			Success: false,
			Error:   fmt.Sprintf("parse preflight-check output failed: %v", uerr),
			Data:    map[string]string{"output": tailString(strings.TrimSpace(out), 4000)},
		})
		return
	}
	// Surface ok/failed counts in the audit entry so admins can spot
	// preflight-failure trends without re-running.
	successFields := map[string]interface{}{"config_path": resolvedCfgPath}
	if v, present := payload["ok"]; present {
		successFields["ok"] = v
	}
	if v, present := payload["failed"]; present {
		successFields["failed"] = v
	}
	s.audit(r, "runs.preflight", true, successFields)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: payload})
}

func (s *apiServer) handleRunTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req runTriggerRequest
	if err := decodeJSON(r.Body, &req); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	cfgPath := s.resolvePreferredRunConfigPath(r, req.ConfigPath)
	resolvedCfgPath, err := s.validateConfigPath(cfgPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if len(req.Password) > 256 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "password too long"})
		return
	}
	cleanExtraArgs, err := sanitizeExtraArgs(req.ExtraArgs)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	// Cluster-group scoping: confine the run to the caller's allowed clusters
	// (admins/static tokens are unrestricted) and to any requested group/subset.
	p, _ := principalFromContext(r.Context())
	access := s.allowedClusters(p)
	scopedClusters, scopeErr := s.resolveRunClusterScope(req, access, cleanExtraArgs)
	if scopeErr != nil {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: scopeErr.Error()})
		return
	}
	if st, err := os.Stat(resolvedCfgPath); err != nil || st.IsDir() {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   fmt.Sprintf("config file not found: %s", cfgPath),
			Data: map[string]interface{}{
				"config_path": cfgPath,
				"resolved":    resolvedCfgPath,
			},
		})
		return
	}

	// Hand off to the concurrent run engine. It de-duplicates clusters that are
	// already being refreshed by another active run (so two cluster groups can
	// run at once without re-running shared clusters), and queues the request
	// when all concurrency slots are busy.
	res := s.submitRun(runStartParams{
		cfgPath:      resolvedCfgPath,
		password:     req.Password,
		extraArgs:    cleanExtraArgs,
		group:        strings.TrimSpace(req.Group),
		requested:    scopedClusters,
		unrestricted: access.unrestricted,
		auditSubject: p.subject,
		auditRole:    p.role.String(),
	})

	baseFields := map[string]interface{}{"config_path": resolvedCfgPath, "extra_args_count": len(cleanExtraArgs)}
	if strings.TrimSpace(req.Group) != "" {
		baseFields["group"] = strings.TrimSpace(req.Group)
	}

	switch {
	case res.noop:
		baseFields["reason"] = "already_running"
		baseFields["skipped"] = len(res.skipped)
		s.audit(r, "runs.trigger", true, baseFields)
		writeJSON(w, http.StatusOK, envelope{
			Success: true,
			Message: "all requested clusters are already being refreshed by an in-progress run",
			Data: map[string]interface{}{
				"started":          false,
				"queued":           false,
				"skipped_clusters": res.skipped,
				"skipped_owner":    res.skippedOwner,
			},
		})
		return
	case res.queued:
		baseFields["queued"] = true
		baseFields["queue_position"] = res.queuePos
		if len(res.skipped) > 0 {
			baseFields["skipped"] = len(res.skipped)
		}
		s.audit(r, "runs.trigger", true, baseFields)
		writeJSON(w, http.StatusAccepted, envelope{
			Success: true,
			Message: fmt.Sprintf("run queued (position %d) — will start automatically when a slot frees", res.queuePos),
			Data: map[string]interface{}{
				"run_id":           res.rec.id,
				"started":          false,
				"queued":           true,
				"queue_position":   res.queuePos,
				"config_path":      resolvedCfgPath,
				"skipped_clusters": res.skipped,
				"skipped_owner":    res.rec.skippedOwner,
			},
		})
		return
	default: // started immediately
		if len(res.rec.clusters) > 0 {
			baseFields["scoped_clusters"] = len(res.rec.clusters)
		}
		if len(res.skipped) > 0 {
			baseFields["skipped"] = len(res.skipped)
		}
		baseFields["run_id"] = res.rec.id
		s.audit(r, "runs.trigger", true, baseFields)
		writeJSON(w, http.StatusAccepted, envelope{Success: true, Message: "run triggered", Data: map[string]interface{}{
			"run_id":           res.rec.id,
			"started":          true,
			"queued":           false,
			"started_at":       res.rec.startedAt.Format(time.RFC3339),
			"config_path":      resolvedCfgPath,
			"used_password":    strings.TrimSpace(req.Password) != "",
			"clusters":         res.rec.clusters,
			"skipped_clusters": res.skipped,
			"skipped_owner":    res.rec.skippedOwner,
			"running_count":    res.runningCount,
		}})
		return
	}
}

func (s *apiServer) handleRunnerLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	logPath := s.absPath(s.runnerLogPath)
	content, err := readTailFile(logPath, 64000)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
				"path":    logPath,
				"content": "",
				"exists":  false,
			}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"path":    logPath,
		"content": strings.TrimSpace(content),
		"exists":  true,
	}})
}

func (s *apiServer) handleReportData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	pg, err := parseReportDataPagination(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	// Cluster-group scoping: non-admins who belong to one or more cluster groups
	// only see data for clusters in those groups; ungrouped non-admins (and
	// admins/static tokens) are unrestricted. The helper is a no-op when
	// unrestricted, so the filter calls below are cheap for those callers.
	p, _ := principalFromContext(r.Context())
	access := s.allowedClusters(p)
	outDir := s.selectBestReportOutDir()
	runSummary := readJSONArtifact(filepath.Join(outDir, "run-summary.json"), map[string]interface{}{})
	// Heal `error_class` / `failure_classes` for runs persisted by older
	// orchestrator builds that misclassified DNS-driven circuit-breaker errors
	// as `rate_limit`. The fix in goNCC.go covers future runs; this read-side
	// re-classification surfaces correct buckets for existing on-disk data
	// without forcing the user to re-trigger.
	if m, ok := runSummary.(map[string]interface{}); ok {
		reclassifyRunSummaryInPlace(m)
		runSummary = m
	}
	runSummary = deepFilterClusters(runSummary, access)
	checksSnapshot := deepFilterClusters(readJSONArtifact(filepath.Join(outDir, "checks-snapshot.json"), []interface{}{}), access)
	drilldownDiff := deepFilterClusters(readJSONArtifact(filepath.Join(outDir, "drilldown-diff.json"), map[string]interface{}{}), access)
	flakyChecks := deepFilterClusters(readJSONArtifact(filepath.Join(outDir, "flaky-checks.json"), map[string]interface{}{}), access)
	regressionSummary := deepFilterClusters(readJSONArtifact(filepath.Join(outDir, "regression-summary.json"), map[string]interface{}{}), access)
	sloDashboard := deepFilterClusters(readJSONArtifact(filepath.Join(outDir, "slo-dashboard.json"), map[string]interface{}{}), access)
	policyViolations := []string{}
	if b, err := os.ReadFile(filepath.Join(outDir, "policy-gates.txt")); err == nil {
		for _, ln := range strings.Split(string(b), "\n") {
			ln = strings.TrimSpace(ln)
			if ln == "" {
				continue
			}
			policyViolations = append(policyViolations, ln)
		}
	}
	pagination := map[string]interface{}{}
	aggRows := deepFilterClusters(readInlineJSONVar(filepath.Join(outDir, "index.html"), "AGG", []interface{}{}), access)
	if rows, ok := checksSnapshot.([]interface{}); ok && pg.enabled() {
		pageRows, meta := paginateAnySlice(rows, pg)
		checksSnapshot = pageRows
		pagination["checks_snapshot"] = meta
	}
	if rows, ok := aggRows.([]interface{}); ok && pg.enabled() {
		pageRows, meta := paginateAnySlice(rows, pg)
		aggRows = pageRows
		pagination["agg_rows"] = meta
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"run_summary":         runSummary,
		"checks_snapshot":     checksSnapshot,
		"drilldown_diff":      drilldownDiff,
		"flaky_checks":        flakyChecks,
		"regression_summary":  regressionSummary,
		"slo_dashboard":       sloDashboard,
		"policy_violations":   policyViolations,
		"agg_rows":            aggRows,
		"diff_flags":          readInlineJSONVar(filepath.Join(outDir, "index.html"), "DIFF_FLAGS", map[string]interface{}{}),
		"flaky_keys":          readInlineJSONVar(filepath.Join(outDir, "index.html"), "FLAKY_KEYS", map[string]interface{}{}),
		"cluster_links":       deepFilterClusters(readInlineJSONVar(filepath.Join(outDir, "index.html"), "CLUSTER_LINKS", []interface{}{}), access),
		"artifact_links":      readInlineJSONVar(filepath.Join(outDir, "index.html"), "ARTIFACT_LINKS", map[string]interface{}{}),
		"report_meta":         loadReportMeta(outDir),
		"ncc_logs":            listNCCLogs(s.absPath(s.logDir)),
		"ncc_summary_counts":  parseNCCSummaryCounts(s.absPath(s.logDir)),
		"ncc_cluster_summary": deepFilterClusters(parseNCCClusterSummary(s.absPath(s.logDir)), access),
		"trends":              collectTrendPoints(outDir, 30),
		"report_source_dir":   outDir,
		"pagination":          pagination,
	}})
}

type reportDataPagination struct {
	Limit  int
	Offset int
}

func (p reportDataPagination) enabled() bool {
	return p.Limit > 0 || p.Offset > 0
}

func parseReportDataPagination(r *http.Request) (reportDataPagination, error) {
	const maxLimit = 5000
	out := reportDataPagination{}
	limitRaw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if limitRaw != "" {
		v, err := strconv.Atoi(limitRaw)
		if err != nil || v < 0 {
			return out, errors.New("limit must be a non-negative integer")
		}
		if v > maxLimit {
			return out, fmt.Errorf("limit must be <= %d", maxLimit)
		}
		out.Limit = v
	}
	offsetRaw := strings.TrimSpace(r.URL.Query().Get("offset"))
	if offsetRaw != "" {
		v, err := strconv.Atoi(offsetRaw)
		if err != nil || v < 0 {
			return out, errors.New("offset must be a non-negative integer")
		}
		out.Offset = v
	}
	return out, nil
}

func paginateAnySlice(items []interface{}, p reportDataPagination) ([]interface{}, map[string]interface{}) {
	total := len(items)
	start := p.Offset
	if start > total {
		start = total
	}
	end := total
	if p.Limit > 0 && start+p.Limit < end {
		end = start + p.Limit
	}
	out := items[start:end]
	meta := map[string]interface{}{
		"total":    total,
		"offset":   p.Offset,
		"limit":    p.Limit,
		"count":    len(out),
		"has_more": end < total,
	}
	return out, meta
}

type trendClusterSummary struct {
	FailCount int `json:"fail_count"`
	WarnCount int `json:"warn_count"`
	ErrCount  int `json:"err_count"`
	InfoCount int `json:"info_count"`
}

type trendRunSummary struct {
	Timestamp      string                `json:"timestamp"`
	DurationS      float64               `json:"duration_s"`
	ClustersOK     int                   `json:"clusters_ok"`
	ClustersFailed int                   `json:"clusters_failed"`
	TotalChecks    int                   `json:"total_checks"`
	AvgHealthScore int                   `json:"avg_health_score"`
	MinHealthScore int                   `json:"min_health_score"`
	ExitCode       *int                  `json:"exit_code,omitempty"`
	Source         string                `json:"source,omitempty"`
	Clusters       []trendClusterSummary `json:"clusters"`
}

type trendPoint struct {
	Timestamp      string  `json:"timestamp"`
	DurationS      float64 `json:"duration_s"`
	ClustersOK     int     `json:"clusters_ok"`
	ClustersFailed int     `json:"clusters_failed"`
	TotalChecks    int     `json:"total_checks"`
	AvgHealthScore int     `json:"avg_health_score"`
	MinHealthScore int     `json:"min_health_score"`
	FailTotal      int     `json:"fail_total"`
	WarnTotal      int     `json:"warn_total"`
	ErrTotal       int     `json:"err_total"`
	InfoTotal      int     `json:"info_total"`
}

func parseTrendLimit(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 30
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 30
	}
	if n > 365 {
		return 365
	}
	return n
}

func readTrendRunSummary(path string) (trendPoint, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return trendPoint{}, false
	}
	var s trendRunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return trendPoint{}, false
	}
	failTotal, warnTotal, errTotal, infoTotal := 0, 0, 0, 0
	for _, c := range s.Clusters {
		failTotal += c.FailCount
		warnTotal += c.WarnCount
		errTotal += c.ErrCount
		infoTotal += c.InfoCount
	}
	return trendPoint{
		Timestamp:      s.Timestamp,
		DurationS:      s.DurationS,
		ClustersOK:     s.ClustersOK,
		ClustersFailed: s.ClustersFailed,
		TotalChecks:    s.TotalChecks,
		AvgHealthScore: s.AvgHealthScore,
		MinHealthScore: s.MinHealthScore,
		FailTotal:      failTotal,
		WarnTotal:      warnTotal,
		ErrTotal:       errTotal,
		InfoTotal:      infoTotal,
	}, true
}

func collectTrendPoints(outDir string, limit int) []trendPoint {
	points := make([]trendPoint, 0)
	seenTs := map[string]bool{}
	addPoint := func(p trendPoint) {
		if strings.TrimSpace(p.Timestamp) == "" {
			return
		}
		if seenTs[p.Timestamp] {
			return
		}
		seenTs[p.Timestamp] = true
		points = append(points, p)
	}
	if p, ok := readTrendRunSummary(filepath.Join(outDir, "run-summary.json")); ok {
		addPoint(p)
	}
	runsDir := filepath.Join(outDir, "runs")
	entries, err := os.ReadDir(runsDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if p, ok := readTrendRunSummary(filepath.Join(runsDir, e.Name(), "run-summary.json")); ok {
				addPoint(p)
			}
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	if limit > 0 && len(points) > limit {
		points = points[len(points)-limit:]
	}
	return points
}

func (s *apiServer) handleReportTrends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	outDir := s.selectBestReportOutDir()
	limit := parseTrendLimit(r.URL.Query().Get("limit"))
	points := collectTrendPoints(outDir, limit)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"points":            points,
		"count":             len(points),
		"limit":             limit,
		"report_source_dir": outDir,
	}})
}

func (s *apiServer) handleMetaRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	routes := apiRouteCatalog()
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"routes": routes,
		"count":  len(routes),
	}})
}

// apiRouteCatalog is the single source of truth for the REST surface: it powers
// both /api/v1/meta/routes and (by gap-filling) the OpenAPI spec, so new routes
// can't silently go undocumented in either. Each entry's MinRole is computed
// from the same routeMinRole logic the auth middleware enforces.
func apiRouteCatalog() []routeMeta {
	routes := []routeMeta{
		{Path: "/api/v1/health", Methods: []string{http.MethodGet}, Description: "Backend health, version, and resolved paths"},
		{Path: "/api/v1/audit", Methods: []string{http.MethodGet}, Description: "Read recent audit log entries (limit, action, failures filters)"},
		{Path: "/api/v1/metrics/rate-limit", Methods: []string{http.MethodGet}, Description: "Rate limiter configuration and counters"},
		{Path: "/metrics", Methods: []string{http.MethodGet}, Description: "Prometheus exposition (run/auth/update counters, build info, self-heal, backup inventory gauges, update-availability, audit-forward drops)"},
		{Path: "/api/v1/health/diagnostics", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: GET runs a read-only self-heal scan (doctor checks + live LDAP/SAML/clock probes) returning a ranked diagnostics list with an overall status; POST supports targeted fixes (check_ids), guardrails (no_disruptive), and post-fix verification."},
		{Path: "/api/v1/health/diagnostics/support-bundle", Methods: []string{http.MethodPost}, Description: "Admin-only: generate a redacted doctor support bundle and return its path on disk."},
		{Path: "/api/v1/auth/session", Methods: []string{http.MethodPost}, Description: "Issue short-lived session token"},
		{Path: "/api/v1/auth/rotate", Methods: []string{http.MethodPost}, Description: "Rotate API token"},
		{Path: "/api/v1/auth/login", Methods: []string{http.MethodPost}, Description: "Local-account login; sets the httpOnly session + CSRF cookies and reports role/must-change", SampleBody: "{\n  \"username\": \"admin\",\n  \"password\": \"\"\n}"},
		{Path: "/api/v1/auth/logout", Methods: []string{http.MethodPost}, Description: "Clear the session and CSRF cookies"},
		{Path: "/api/v1/auth/me", Methods: []string{http.MethodGet}, Description: "Caller identity, role, session expiry, and which login methods are enabled (no auth required)"},
		{Path: "/api/v1/auth/change-password", Methods: []string{http.MethodPost}, Description: "Self-service password change for the logged-in session (requires CSRF on cookie sessions); bumps token generation so other sessions are signed out", SampleBody: "{\n  \"current_password\": \"\",\n  \"new_password\": \"\"\n}"},
		{Path: "/api/v1/auth/refresh", Methods: []string{http.MethodPost}, Description: "Re-issue the current session cookie with a fresh expiry (used by the UI's inactivity 'stay logged in' prompt); session auth only"},
		{Path: "/api/v1/auth/forgot-password", Methods: []string{http.MethodPost}, Description: "Public self-service: queue a password-reset request for an admin to action; always returns a generic 200 (no account enumeration)", SampleBody: "{\n  \"username\": \"alice\"\n}"},
		{Path: "/api/v1/auth/tokens", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Self-service personal access tokens (any signed-in user): GET lists your tokens (metadata only); POST mints a bearer token inheriting your role, returned once. Use it as 'X-API-Token: <token>' or 'Authorization: Bearer <token>'.", SampleBody: "{\n  \"name\": \"laptop-cli\",\n  \"expires_in_days\": 90\n}"},
		{Path: "/api/v1/auth/tokens/{id}", Methods: []string{http.MethodDelete}, Description: "Self-service: revoke one of your own personal access tokens by id"},
		{Path: "/api/v1/settings/password-resets", Methods: []string{http.MethodGet}, Description: "Admin-only: list pending self-service password-reset requests"},
		{Path: "/api/v1/settings/password-resets/{name}", Methods: []string{http.MethodDelete}, Description: "Admin-only: dismiss a pending password-reset request without resetting the password (resetting it clears the request automatically)"},
		{Path: "/api/v1/settings/users", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: list local accounts / create one (last-admin + reserved-admin protection)", SampleBody: "{\n  \"username\": \"alice\",\n  \"password\": \"\",\n  \"role\": \"operator\",\n  \"must_change_password\": true\n}"},
		{Path: "/api/v1/settings/users/{name}", Methods: []string{http.MethodPut, http.MethodDelete}, Description: "Admin-only: update role / reset password / set must-change, or delete an account", SampleBody: "{\n  \"role\": \"viewer\",\n  \"password\": \"\",\n  \"must_change_password\": true\n}"},
		{Path: "/api/v1/settings/sso", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/update the runtime SAML SSO configuration (read-only when configured via startup flags)", SampleBody: "{\n  \"enabled\": true,\n  \"root_url\": \"https://ncc.example.com\",\n  \"idp_metadata_url\": \"https://idp.example.com/metadata\",\n  \"role_attribute\": \"groups\",\n  \"role_map\": \"ncc-admins=admin,ncc-ops=operator\",\n  \"default_role\": \"viewer\"\n}"},
		{Path: "/api/v1/settings/ldap", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/update the runtime LDAP/Active Directory configuration (bind password is write-only; read-only when configured via startup flags)", SampleBody: "{\n  \"enabled\": true,\n  \"url\": \"ldaps://dc1.corp.example.com:636\",\n  \"bind_dn\": \"CN=ncc-svc,OU=Service Accounts,DC=corp,DC=example,DC=com\",\n  \"bind_password\": \"service-account-secret\",\n  \"base_dn\": \"DC=corp,DC=example,DC=com\",\n  \"role_map\": \"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin\",\n  \"default_role\": \"viewer\"\n}"},
		{Path: "/api/v1/settings/ldap/test", Methods: []string{http.MethodPost}, Description: "Admin-only: validate an LDAP/AD configuration by authenticating sample credentials, without saving it", SampleBody: "{\n  \"url\": \"ldaps://dc1.corp.example.com:636\",\n  \"base_dn\": \"DC=corp,DC=example,DC=com\",\n  \"bind_dn\": \"CN=ncc-svc,DC=corp,DC=example,DC=com\",\n  \"bind_password\": \"service-account-secret\",\n  \"role_map\": \"CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin\",\n  \"test_username\": \"jdoe\",\n  \"test_password\": \"users-password\"\n}"},
		{Path: "/api/v1/settings/ldap/search", Methods: []string{http.MethodGet}, Description: "Admin-only: live AD/LDAP type-ahead search for groups and users (?q=<term>&type=group|user|all&limit=<n>) to assign to cluster groups"},
		{Path: "/api/v1/settings/session", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/set the session lifetime (ttl_sec or ttl_min; 0 restores the --session-ttl default)", SampleBody: "{\n  \"ttl_min\": 360\n}"},
		{Path: "/api/v1/settings/tls", Methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete}, Description: "Admin-only: manage HTTPS for the UI server. GET returns the installed certificate metadata; PUT installs a PEM cert+key, enables HTTPS, and restarts the stack (session cookies become Secure); DELETE removes the cert, disables HTTPS, and restarts back to HTTP", SampleBody: "{\n  \"cert\": \"-----BEGIN CERTIFICATE-----\\n...\\n-----END CERTIFICATE-----\\n\",\n  \"key\": \"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n\"\n}"},
		{Path: "/api/v1/settings/tls/generate", Methods: []string{http.MethodPost}, Description: "Admin-only: generate (or renew) a self-signed certificate for the UI server, enable HTTPS, and restart the stack. Body is optional: {hosts:[...], valid_days:N}; hosts default to the request host and always include localhost/loopback", SampleBody: "{\n  \"hosts\": [\"10.21.88.27\"],\n  \"valid_days\": 825\n}"},
		{Path: "/api/v1/settings/cluster-groups", Methods: []string{http.MethodGet, http.MethodPut}, Description: "GET operator+: read the cluster groups; PUT admin-only: replace the groups that confine non-admins to their clusters (members = local accounts + AD groups/users)", SampleBody: "{\n  \"groups\": [\n    {\n      \"name\": \"Platform\",\n      \"clusters\": [\"10.0.0.1\", \"pc-east\"],\n      \"local_users\": [\"alice\"],\n      \"ad_groups\": [\"CN=NCC-Platform,OU=Groups,DC=corp,DC=example,DC=com\"]\n    }\n  ]\n}"},
		{Path: "/api/v1/settings/tokens", Methods: []string{http.MethodGet}, Description: "Admin-only: inventory every user's personal access tokens (metadata only)"},
		{Path: "/api/v1/settings/tokens/{id}", Methods: []string{http.MethodDelete}, Description: "Admin-only: revoke any user's personal access token by id"},
		{Path: "/api/v1/settings/clusters", Methods: []string{http.MethodGet}, Description: "Operator+: list clusters known to the active config (for assigning to groups / scoping runs)"},
		{Path: "/api/v1/settings/pc-clusters", Methods: []string{http.MethodGet}, Description: "Admin-only: discover the clusters registered under a Prism Central (?pc=<url>) for assigning a PC to a cluster group; uses the active run config's credentials"},
		{Path: "/api/v1/settings/backup", Methods: []string{http.MethodGet}, Description: "Admin-only: download a .tar.gz backup of the install dir (config + referenced files, local user database, API token, scheduler/notifications state). Send an optional 'X-NCC-Backup-Passphrase' header to seal the download at rest with AES-256-GCM (served as .tar.gz.enc)."},
		{Path: "/api/v1/settings/restore", Methods: []string{http.MethodPost}, Description: "Admin-only: restore a backup archive uploaded as multipart/form-data (field 'archive'; optional 'passphrase' field decrypts an encrypted archive); overwrites install-dir files with --force. Restart the stack afterward for it to take effect."},
		{Path: "/api/v1/settings/backups", Methods: []string{http.MethodGet, http.MethodPost}, Description: "Admin-only: GET lists server-side backups under <install>/backups (manual snapshots + pre-update rollback points; each entry reports whether it is encrypted); POST creates a new persistent snapshot. Pass an optional {\"passphrase\":\"...\"} to encrypt the snapshot at rest with AES-256-GCM (stored as .tar.gz.enc).", SampleBody: "{\n  \"passphrase\": \"\"\n}"},
		{Path: "/api/v1/settings/backups/restore", Methods: []string{http.MethodPost}, Description: "Admin-only: restore a server-side backup by name (no re-upload), then restart the stack. Also powers post-update rollback. Supply 'passphrase' to restore an encrypted (.tar.gz.enc) snapshot.", SampleBody: "{\n  \"name\": \"pre-update-20260610T120000Z.tar.gz\",\n  \"passphrase\": \"\"\n}"},
		{Path: "/api/v1/settings/backups/verify", Methods: []string{http.MethodPost}, Description: "Admin-only: verify a server-side backup is intact and restorable (v2-restore --verify-only: gzip/tar integrity, manifest, confined paths) without restoring. For an encrypted (.tar.gz.enc) snapshot, supply 'passphrase' — a successful verify also confirms the passphrase decrypts it.", SampleBody: "{\n  \"name\": \"manual-20260610T120000Z.tar.gz\",\n  \"passphrase\": \"\"\n}"},
		{Path: "/api/v1/settings/backups/schedule", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: get/set the in-process scheduled backup (interval, optional AES-256-GCM encryption using the API server's configured key, and retention of newest N scheduled archives). PUT with run_now=true takes one snapshot immediately.", SampleBody: "{\n  \"enabled\": true,\n  \"every\": \"24h\",\n  \"encrypt\": false,\n  \"retain\": 7,\n  \"run_now\": false\n}"},
		{Path: "/api/v1/settings/backups/delete", Methods: []string{http.MethodPost}, Description: "Admin-only: delete a server-side backup by name", SampleBody: "{\n  \"name\": \"manual-20260610T120000Z.tar.gz\"\n}"},
		{Path: "/api/v1/settings/backups/download", Methods: []string{http.MethodGet}, Description: "Admin-only: download a server-side backup by name (?name=...). Encrypted snapshots stream as the opaque .tar.gz.enc envelope (decrypt with v2-restore + the passphrase)."},
		{Path: "/api/v1/settings/update", Methods: []string{http.MethodGet}, Description: "Admin-only: check for a newer release (current/latest version, update_available) and report any in-progress in-app update job/phase"},
		{Path: "/api/v1/settings/update/apply", Methods: []string{http.MethodPost}, Description: "Admin-only: apply an in-app update — takes a pre-update backup, installs the latest stack (orchestrator+api+ui+frontend, always checksum-verified against the release checksums.txt), then restarts the stack. Runs in the background; poll GET /api/v1/settings/update for progress.", SampleBody: "{\n  \"target_version\": \"\"\n}"},
		{Path: "/api/v1/settings/config", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write runtime config", SampleBody: "{\n  \"content\": \"clusters: \\\"10.0.0.1\\\"\\nusername: \\\"admin\\\"\\n\"\n}"},
		{Path: "/api/v1/settings/config/batch", Methods: []string{http.MethodPost}, Description: "Bulk add/update/remove YAML config files", SampleBody: "{\n  \"operations\": [\n    {\"action\": \"add\", \"path\": \"configs/dev-a.yaml\", \"content\": \"clusters: \\\"10.0.0.1\\\"\\nusername: \\\"admin\\\"\\n\"},\n    {\"action\": \"remove\", \"path\": \"configs/old.yaml\"}\n  ]\n}"},
		{Path: "/api/v1/settings/config-files", Methods: []string{http.MethodGet}, Description: "List config-referenced files (clusters, exclusions, secrets)"},
		{Path: "/api/v1/settings/config-file", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write one config-referenced file", SampleBody: "{\n  \"path\": \"alerts-exclude.txt\",\n  \"content\": \"AHV_MemoryUsage\\n\"\n}"},
		{Path: "/api/v1/settings/config-files/batch", Methods: []string{http.MethodPost}, Description: "Bulk add/update/remove config-referenced files", SampleBody: "{\n  \"operations\": [\n    {\"action\": \"add\", \"path\": \"clusters.txt\", \"content\": \"10.0.0.1\\n\"},\n    {\"action\": \"remove\", \"path\": \"exclude.txt\"}\n  ]\n}"},
		{Path: "/api/v1/settings/notifications", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Admin-only: read/write notification channels (slack/webhook/email), events, and delivery controls — quiet hours, maintenance windows, dedup/rate-limit throttle, and the scheduled health digest. Failures (run/backup/self-heal) reuse the run_failure toggle.", SampleBody: "{\n  \"enabled\": true,\n  \"events\": {\"run_failure\": true, \"run_success\": false, \"policy_violations\": true},\n  \"email\": {\"enabled\": true, \"smtp_host\": \"smtp.example.com\", \"smtp_port\": 587, \"from\": \"ncc@example.com\", \"to\": \"sre@example.com\"},\n  \"quiet\": {\"enabled\": true, \"start\": \"22:00\", \"end\": \"07:00\", \"timezone\": \"UTC\", \"allow_failures\": true},\n  \"maintenance\": [{\"start\": \"2026-07-01T01:00:00Z\", \"end\": \"2026-07-01T03:00:00Z\", \"note\": \"patching\"}],\n  \"throttle\": {\"dedup_window_sec\": 300, \"min_interval_sec\": 30},\n  \"digest\": {\"enabled\": true, \"every\": \"24h\"}\n}"},
		{Path: "/api/v1/settings/notifications/test", Methods: []string{http.MethodPost}, Description: "Operator+: send test notification(s) (delivery errors are URL-redacted)", SampleBody: "{\n  \"channel\": \"all\"\n}"},
		{Path: "/api/v1/schedule", Methods: []string{http.MethodGet, http.MethodPut}, Description: "GET viewer+: read scheduler state; PUT operator+: create/update/apply a recurring run", SampleBody: "{\n  \"type\": \"cron\",\n  \"action\": \"create\",\n  \"cron\": \"15 */4 * * *\",\n  \"config\": \"config.yaml\",\n  \"print_only\": true,\n  \"apply\": false\n}"},
		{Path: "/api/v1/schedule/health", Methods: []string{http.MethodGet}, Description: "Scheduler health snapshot (last run/success/error hints)"},
		{Path: "/api/v1/artifacts", Methods: []string{http.MethodGet}, Description: "List available artifacts"},
		{Path: "/api/v1/artifacts/{name}", Methods: []string{http.MethodGet}, Description: "Read artifact by name"},
		{Path: "/api/v1/runs", Methods: []string{http.MethodGet}, Description: "List historical runs with metrics (limit, source, since filters; merges archived runs, latest summary, and audit triggers)"},
		{Path: "/api/v1/runs/{id}", Methods: []string{http.MethodGet}, Description: "Read one archived run's metadata + embedded artifacts"},
		{Path: "/api/v1/runs/summary", Methods: []string{http.MethodGet}, Description: "Read latest run summary (with on-read error-class healing)"},
		{Path: "/api/v1/runs/active", Methods: []string{http.MethodGet, http.MethodDelete}, Description: "GET: read all queued/running runs (concurrent engine) plus the legacy single-run summary. DELETE: cancel a run by ?id=<run-id>, or all queued/running runs when no id is given."},
		{Path: "/api/v1/runs/preflight", Methods: []string{http.MethodPost}, Description: "Run preflight checks (config/secrets/path permissions)", SampleBody: "{\n  \"config_path\": \"config.yaml\"\n}"},
		{Path: "/api/v1/runs/trigger", Methods: []string{http.MethodPost}, Description: "Trigger an orchestrator run. Concurrent runs are allowed; clusters already being refreshed by another active run are skipped (reported in skipped_clusters) and the run executes only the remainder. When all concurrency slots are busy the run is queued and starts automatically.", SampleBody: "{\n  \"config_path\": \"config.yaml\",\n  \"password\": \"\",\n  \"group\": \"\",\n  \"clusters\": [],\n  \"extra_args\": [\"--no-html\"]\n}"},
		{Path: "/api/v1/report/data", Methods: []string{http.MethodGet}, Description: "Aggregated report payload (supports optional limit/offset pagination for large arrays)"},
		{Path: "/api/v1/report/trends", Methods: []string{http.MethodGet}, Description: "Historical trends from run summaries"},
		{Path: "/api/v1/logs/runner", Methods: []string{http.MethodGet}, Description: "Read tail of runner log"},
		{Path: "/api/v1/openapi.json", Methods: []string{http.MethodGet}, Description: "OpenAPI 3.0 specification"},
		{Path: "/api/v1/meta/routes", Methods: []string{http.MethodGet}, Description: "List available REST routes for API explorer"},
	}
	for i := range routes {
		routes[i].MinRole = routeRequiredRole(routes[i].Path, routes[i].Methods)
	}
	return routes
}

func (s *apiServer) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	spec := s.buildOpenAPISpec()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(spec)
}

// buildOpenAPISpec returns the hand-authored OpenAPI spec, then gap-fills it
// from the canonical route catalog so every registered route is present (with a
// summary, a request example where the catalog has one, and an x-required-role
// annotation) even if it wasn't hand-written. This keeps /docs/ui (Swagger) and
// the API explorer from drifting as routes are added.
func (s *apiServer) buildOpenAPISpec() map[string]interface{} {
	spec := s.buildOpenAPISpecBase()
	paths, ok := spec["paths"].(map[string]interface{})
	if !ok || paths == nil {
		paths = map[string]interface{}{}
		spec["paths"] = paths
	}
	for _, rt := range apiRouteCatalog() {
		item, exists := paths[rt.Path].(map[string]interface{})
		if !exists {
			item = openAPIItemFromRoute(rt)
			paths[rt.Path] = item
		}
		// Annotate the required role on each method operation (additive; never
		// clobbers a hand-authored field).
		role := routeRequiredRole(rt.Path, rt.Methods)
		for _, m := range rt.Methods {
			if op, okop := item[strings.ToLower(m)].(map[string]interface{}); okop {
				if _, has := op["x-required-role"]; !has {
					op["x-required-role"] = role
				}
			}
		}
	}
	return spec
}

// openAPIItemFromRoute builds a minimal OpenAPI path item from a catalog entry:
// a summary per method and, for mutating methods with a sample body, a JSON
// request example.
func openAPIItemFromRoute(rt routeMeta) map[string]interface{} {
	item := map[string]interface{}{}
	for _, m := range rt.Methods {
		op := map[string]interface{}{"summary": rt.Description}
		if rt.SampleBody != "" && (m == http.MethodPost || m == http.MethodPut) {
			var example interface{}
			if json.Unmarshal([]byte(rt.SampleBody), &example) == nil {
				op["requestBody"] = map[string]interface{}{
					"required": true,
					"content": map[string]interface{}{
						"application/json": map[string]interface{}{"example": example},
					},
				}
			}
		}
		item[strings.ToLower(m)] = op
	}
	return item
}

func (s *apiServer) buildOpenAPISpecBase() map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.3",
		"info": map[string]interface{}{
			"title":       "NCC Orchestrator API",
			"version":     "2.1.0",
			"description": "REST API for NCC orchestrator run control, artifacts, settings, and analytics.",
		},
		"servers": []map[string]interface{}{
			{"url": "/"},
		},
		"paths": map[string]interface{}{
			"/api/v1/health": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Backend health, version, build_date, and resolved paths"},
			},
			"/api/v1/audit": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Recent audit log entries (newest first).",
					"parameters": []map[string]interface{}{
						{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 10000, "default": 100}},
						{"name": "action", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "description": "Filter to entries whose action starts with this string (e.g. \"settings\", \"runs.trigger\")"}},
						{"name": "failures", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "enum": []string{"1", "true"}, "description": "Return only failed entries"}},
					},
				},
			},
			"/api/v1/metrics/rate-limit": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Rate limiter configuration and counters"},
			},
			"/metrics": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Prometheus exposition format (run/notification counters, build info)"},
			},
			"/api/v1/auth/session": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Issue short-lived session token"},
			},
			"/api/v1/auth/rotate": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Rotate API token"},
			},
			"/api/v1/auth/login": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Local-account login. On success sets the httpOnly session cookie + CSRF cookie and returns the role, session expiry, and whether a password change is required.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"username": "admin",
									"password": "",
								},
							},
						},
					},
				},
			},
			"/api/v1/auth/logout": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Clear the session and CSRF cookies"},
			},
			"/api/v1/auth/refresh": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Re-issue the current session cookie with a fresh expiry (extends the session by the effective TTL). Session auth only (cookie sessions require X-CSRF-Token); used by the UI's inactivity 'stay logged in' prompt."},
			},
			"/api/v1/auth/me": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Caller identity, role, session expiry, and enabled login methods (local/SAML). Reachable unauthenticated so the UI can decide whether to show a login screen."},
			},
			"/api/v1/auth/change-password": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Change the logged-in account's password. Requires a session (cookie sessions also require the X-CSRF-Token header). Verifies the current password, enforces a 12-char minimum, and bumps the token generation so all other sessions are signed out.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"current_password": "",
									"new_password":     "",
								},
							},
						},
					},
				},
			},
			"/api/v1/auth/forgot-password": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Public self-service password recovery: queue a reset request for an admin to action out-of-band. Always returns a generic 200 regardless of whether the account exists (no enumeration); a request is recorded only for an existing local account. Rate-limited.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{"username": "alice"},
							},
						},
					},
				},
			},
			"/api/v1/settings/password-resets": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list pending self-service password-reset requests (username, requested_at, client_ip)."},
			},
			"/api/v1/settings/password-resets/{name}": map[string]interface{}{
				"delete": map[string]interface{}{
					"summary": "Admin-only: dismiss a pending password-reset request without resetting the password (resetting the user's password via the users endpoint clears it automatically).",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
				},
			},
			"/api/v1/settings/users": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list local accounts (username, role, must-change, timestamps; password hashes are never returned)"},
				"post": map[string]interface{}{
					"summary": "Admin-only: create a local account. Role is admin|operator|viewer; new accounts default to must-change-password.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"username":             "alice",
									"password":             "",
									"role":                 "operator",
									"must_change_password": true,
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/users/{name}": map[string]interface{}{
				"put": map[string]interface{}{
					"summary": "Admin-only: update an account's role and/or reset its password and/or flip the must-change flag. The built-in admin cannot be demoted; the last admin cannot be demoted.",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"role":                 "viewer",
									"password":             "",
									"must_change_password": true,
								},
							},
						},
					},
				},
				"delete": map[string]interface{}{
					"summary": "Admin-only: delete an account. The built-in admin and the last remaining admin cannot be deleted.",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
				},
			},
			"/api/v1/settings/sso": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: read the runtime SAML SSO config (SP private key is never returned). Reports whether SSO is enabled and whether it is managed via startup flags or runtime."},
				"put": map[string]interface{}{
					"summary": "Admin-only: update the runtime SAML SSO config and hot-reload the SP. Returns 409 when SAML is managed via startup flags.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled":          true,
									"root_url":         "https://ncc.example.com",
									"idp_metadata_url": "https://idp.example.com/metadata",
									"role_attribute":   "groups",
									"role_map":         "ncc-admins=admin,ncc-ops=operator",
									"default_role":     "viewer",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/ldap": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: read the runtime LDAP/Active Directory config (the bind password is never returned; has_bind_password is reported instead). Reports whether LDAP is enabled and whether it is managed via startup flags or runtime."},
				"put": map[string]interface{}{
					"summary": "Admin-only: update the runtime LDAP/AD config and hot-reload the provider. The bind password is write-only (omit to keep the stored secret). Returns 409 when LDAP is managed via startup flags.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled":       true,
									"url":           "ldaps://dc1.corp.example.com:636",
									"bind_dn":       "CN=ncc-svc,OU=Service Accounts,DC=corp,DC=example,DC=com",
									"bind_password": "service-account-secret",
									"base_dn":       "DC=corp,DC=example,DC=com",
									"role_map":      "CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin",
									"default_role":  "viewer",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/ldap/search": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: live AD/LDAP type-ahead search for groups and users (query params q, type=group|user|all, limit) used when assigning AD principals to cluster groups. Returns an empty result set when LDAP is not enabled."},
			},
			"/api/v1/settings/pc-clusters": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: discover the clusters registered under a Prism Central (query param pc=<url>) so it can be assigned to a cluster group. Uses the active run config's credentials; results are cached and refreshed in the background."},
			},
			"/api/v1/settings/cluster-groups": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list the cluster groups. Membership (local accounts + AD groups + individual AD users) confines non-admins to the clusters in their groups (plus any clusters under an assigned Prism Central); ungrouped clusters are admin-only."},
				"put": map[string]interface{}{
					"summary": "Admin-only: replace the full set of cluster groups.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"groups": []map[string]interface{}{{
										"name":        "Platform",
										"clusters":    []string{"10.0.0.1", "pc-east"},
										"local_users": []string{"alice"},
										"ad_groups":   []string{"CN=NCC-Platform,OU=Groups,DC=corp,DC=example,DC=com"},
									}},
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/clusters": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list the clusters known to the active config (inline clusters + clusters-file) for assigning to groups."},
			},
			"/api/v1/settings/session": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: read the effective session lifetime, its source (default|runtime), and the allowed bounds"},
				"put": map[string]interface{}{
					"summary": "Admin-only: set the session lifetime. Provide ttl_sec or ttl_min; 0 clears the override and restores the server's --session-ttl default. New value applies to sessions minted afterward.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"ttl_min": 360,
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/backup": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Admin-only: download a .tar.gz backup of the install directory (config + referenced files, local user database, API token, scheduler/notifications state). The archive contains secrets — store it securely. Send an optional 'X-NCC-Backup-Passphrase' header to encrypt the download at rest with AES-256-GCM (served as .tar.gz.enc).",
					"parameters": []map[string]interface{}{
						{"name": "X-NCC-Backup-Passphrase", "in": "header", "required": false, "schema": map[string]interface{}{"type": "string"}, "description": "When set, the archive is sealed with AES-256-GCM and downloaded as .tar.gz.enc; restore needs the same passphrase."},
					},
				},
			},
			"/api/v1/settings/restore": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Admin-only: restore a backup archive uploaded as multipart/form-data (field 'archive'; optional 'passphrase' field decrypts an encrypted archive). Overwrites install-dir files with --force and proceeds even while the stack is live; restart the stack afterward for the restored config/accounts/token to take effect."},
			},
			"/api/v1/settings/backups": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Admin-only: list server-side backups under <install>/backups (manual snapshots + pre-update rollback points). Each entry reports name, kind, size, mod_time, and whether it is encrypted."},
				"post": map[string]interface{}{
					"summary": "Admin-only: create a persistent server-side snapshot. An optional passphrase seals it at rest with AES-256-GCM (stored as .tar.gz.enc).",
					"requestBody": map[string]interface{}{
						"required": false,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{"example": map[string]interface{}{"passphrase": ""}},
						},
					},
				},
			},
			"/api/v1/settings/backups/restore": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Admin-only: restore a server-side backup by name, then restart the stack. Supply 'passphrase' to restore an encrypted (.tar.gz.enc) snapshot.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{"example": map[string]interface{}{"name": "pre-update-20260610T120000Z.tar.gz", "passphrase": ""}},
						},
					},
				},
			},
			"/api/v1/settings/backups/delete": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Admin-only: delete a server-side backup by name.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{"example": map[string]interface{}{"name": "manual-20260610T120000Z.tar.gz"}},
						},
					},
				},
			},
			"/api/v1/settings/backups/download": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Admin-only: download a server-side backup by name. Encrypted snapshots stream as the opaque .tar.gz.enc envelope.",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "query", "required": true, "schema": map[string]interface{}{"type": "string"}, "description": "Backup filename (…tar.gz or …tar.gz.enc)"},
					},
				},
			},
			"/api/v1/health/diagnostics": map[string]interface{}{
				"get":  map[string]interface{}{"summary": "Admin-only: read-only self-heal scan (doctor checks + live LDAP/SAML/clock probes) with a ranked diagnostics list and overall status."},
				"post": map[string]interface{}{"summary": "Admin-only: run self-heal with optional targeted checks (check_ids), disruptive-fix guardrails, and post-fix verification loops."},
			},
			"/api/v1/health/diagnostics/support-bundle": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Admin-only: generate a redacted doctor support bundle and return the bundle path."},
			},
			"/api/v1/settings/config": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read runtime config"},
				"put": map[string]interface{}{
					"summary": "Write runtime config",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"content": "clusters: \"10.0.0.1\"\nusername: \"admin\"\n",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config/batch": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Bulk add/update/remove YAML config files",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"operations": []map[string]interface{}{
										{"action": "add", "path": "configs/dev-a.yaml", "content": "clusters: \"10.0.0.1\"\nusername: \"admin\"\n"},
										{"action": "remove", "path": "configs/old.yaml"},
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config-files": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List config-referenced files"},
			},
			"/api/v1/settings/config-file": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read one config-referenced file"},
				"put": map[string]interface{}{
					"summary": "Write one config-referenced file",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"path":    "alerts-exclude.txt",
									"content": "AHV_MemoryUsage\n",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/config-files/batch": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Bulk add/update/remove config-referenced files",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"operations": []map[string]interface{}{
										{"action": "add", "path": "clusters.txt", "content": "10.0.0.1\n"},
										{"action": "remove", "path": "exclude.txt"},
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/notifications": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read notifications state"},
				"put": map[string]interface{}{
					"summary": "Write notifications state",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"enabled": true,
									"channel": "webhook",
								},
							},
						},
					},
				},
			},
			"/api/v1/settings/notifications/test": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Send test notification(s)",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"channel": "all",
								},
							},
						},
					},
				},
			},
			"/api/v1/schedule": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read scheduler state"},
				"put": map[string]interface{}{
					"summary": "Write scheduler state",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"type":       "cron",
									"action":     "create",
									"cron":       "15 */4 * * *",
									"config":     "config.yaml",
									"print_only": true,
									"apply":      false,
								},
							},
						},
					},
				},
			},
			"/api/v1/schedule/health": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read scheduler health snapshot"},
			},
			"/api/v1/artifacts": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List available artifacts"},
			},
			"/api/v1/artifacts/{name}": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read artifact by name"},
			},
			"/api/v1/runs": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "List historical runs with metrics; merges archived runs, latest summary, and audit triggers.",
					"parameters": []map[string]interface{}{
						{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 1000, "default": 200}},
						{"name": "source", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "enum": []string{"history", "summary", "trigger"}}},
						{"name": "since", "in": "query", "required": false, "schema": map[string]interface{}{"type": "string", "description": "RFC3339 timestamp or YYYY-MM-DD date"}},
					},
				},
			},
			"/api/v1/runs/{id}": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Read one archived run's metadata + embedded artifacts (run-summary.json, ncc-run-record.json, regression-summary.json, checks-snapshot.json, run-meta.json).",
					"parameters": []map[string]interface{}{
						{"name": "id", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
				},
			},
			"/api/v1/runs/summary": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read latest run summary (with on-read error_class healing for legacy entries)"},
			},
			"/api/v1/runs/active": map[string]interface{}{
				"get":    map[string]interface{}{"summary": "Read all queued/running runs (runs[], running_count, queued_count, max_concurrent) plus the legacy single-run summary"},
				"delete": map[string]interface{}{"summary": "Cancel a run by ?id=<run-id>, or all queued/running runs when no id is given. Returns 409 if nothing is active."},
			},
			"/api/v1/runs/preflight": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Run preflight checks before trigger-run",
					"requestBody": map[string]interface{}{
						"required": false,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"config_path": "config.yaml",
								},
							},
						},
					},
				},
			},
			"/api/v1/runs/trigger": map[string]interface{}{
				"post": map[string]interface{}{
					"summary": "Trigger an orchestrator run. Concurrent runs are allowed; clusters already in flight elsewhere are skipped and only the remainder runs. Over the concurrency cap the run is queued.",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"example": map[string]interface{}{
									"config_path": "config.yaml",
									"password":    "",
									"extra_args":  []string{"--no-html"},
								},
							},
						},
					},
				},
			},
			"/api/v1/report/data": map[string]interface{}{
				"get": map[string]interface{}{
					"summary": "Aggregated report payload",
					"parameters": []map[string]interface{}{
						{"name": "limit", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 0, "maximum": 5000}},
						{"name": "offset", "in": "query", "required": false, "schema": map[string]interface{}{"type": "integer", "minimum": 0}},
					},
				},
			},
			"/api/v1/report/trends": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Historical trends from run summaries"},
			},
			"/api/v1/logs/runner": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read tail of runner log"},
			},
			"/api/v1/openapi.json": map[string]interface{}{
				"get": map[string]interface{}{"summary": "OpenAPI 3.0 specification"},
			},
			"/api/v1/meta/routes": map[string]interface{}{
				"get": map[string]interface{}{"summary": "List available REST routes for API explorer"},
			},
		},
	}
}

// extraArgsHaveFlag reports whether the caller-supplied extra args already
// include the given flag (matching `--flag` or `--flag=value`), so the
// api-server doesn't double-specify output directories it would otherwise pin.
func extraArgsHaveFlag(args []string, flag string) bool {
	want := "--" + flag
	for _, a := range args {
		a = strings.TrimSpace(a)
		if a == want || strings.HasPrefix(a, want+"=") {
			return true
		}
	}
	return false
}

func (s *apiServer) runOrchestrator(args []string, timeout time.Duration) (string, error) {
	return s.runOrchestratorEnv(args, timeout, nil)
}

// runOrchestratorEnv is runOrchestrator with extra environment variables
// appended to the (master-key-scrubbed) child environment. Used to pass a
// backup passphrase (NCC_BACKUP_PASSPHRASE) to v2-backup/v2-restore via the
// environment rather than argv, so it never lands in a process listing or the
// audit log.
func (s *apiServer) runOrchestratorEnv(args []string, timeout time.Duration, extraEnv []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	if len(extraEnv) > 0 {
		cmd.Env = append(cmd.Env, extraEnv...)
	}
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("command timed out after %s", timeout)
	}
	return string(out), err
}

func (s *apiServer) loadSchedule() (scheduleState, error) {
	path := s.absPath(s.scheduleStatePath)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return scheduleState{
				Type:      "auto",
				Action:    "create",
				Config:    s.configPath,
				LogPath:   "logs/ncc-scheduler.log",
				WithLock:  true,
				PrintOnly: true,
				UpdatedAt: "",
			}, nil
		}
		return scheduleState{}, err
	}
	var st scheduleState
	if err := json.Unmarshal(b, &st); err != nil {
		return scheduleState{}, err
	}
	if strings.TrimSpace(st.LogPath) == "" {
		st.LogPath = "logs/ncc-scheduler.log"
	}
	return st, nil
}

func (s *apiServer) saveSchedule(st scheduleState) error {
	path := s.absPath(s.scheduleStatePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (s *apiServer) absPath(p string) string {
	if abs, err := s.normalizeAndConfinePath(p); err == nil {
		return abs
	}
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Clean(filepath.Join(s.repoRoot, p))
}

func writeJSON(w http.ResponseWriter, status int, v envelope) {
	if !v.Success && strings.TrimSpace(v.Error) != "" && strings.TrimSpace(v.ErrorCode) == "" {
		v.ErrorCode = defaultAPIErrorCode(status)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func defaultAPIErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "NCC_API_BAD_REQUEST"
	case http.StatusUnauthorized:
		return "NCC_API_UNAUTHORIZED"
	case http.StatusForbidden:
		return "NCC_API_FORBIDDEN"
	case http.StatusNotFound:
		return "NCC_API_NOT_FOUND"
	case http.StatusMethodNotAllowed:
		return "NCC_API_METHOD_NOT_ALLOWED"
	case http.StatusConflict:
		return "NCC_API_CONFLICT"
	case http.StatusUnsupportedMediaType:
		return "NCC_API_UNSUPPORTED_MEDIA_TYPE"
	case http.StatusTooManyRequests:
		return "NCC_API_RATE_LIMITED"
	case http.StatusBadGateway:
		return "NCC_API_UPSTREAM_FAILURE"
	case http.StatusInternalServerError:
		return "NCC_API_INTERNAL"
	default:
		if status >= 400 && status < 500 {
			return "NCC_API_CLIENT_ERROR"
		}
		if status >= 500 {
			return "NCC_API_SERVER_ERROR"
		}
		return ""
	}
}

func defaultIfEmpty(v, def string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return def
	}
	return v
}

func (s *apiServer) orchestratorBaseCommand() []string {
	binPath, _ := filepath.Abs(s.absPath(s.orchestratorBin))
	if st, err := os.Stat(binPath); err == nil && !st.IsDir() {
		return []string{binPath}
	}
	// Dev-friendly fallback: run source directly when binary is not built yet.
	goNCCPath, _ := filepath.Abs(filepath.Join(s.absPath(s.repoRoot), "goNCC.go"))
	return []string{"go", "run", goNCCPath}
}

func (s *apiServer) makeOrchestratorCommand(ctx context.Context, args ...string) *exec.Cmd {
	base := s.orchestratorBaseCommand()
	full := append(append([]string{}, base...), args...)
	if len(full) == 0 {
		return exec.Command("true")
	}
	name := full[0]
	rest := full[1:]
	var cmd *exec.Cmd
	if ctx == nil {
		cmd = exec.Command(name, rest...)
	} else {
		cmd = exec.CommandContext(ctx, name, rest...)
	}
	// Never hand the user-store master key to an orchestrator child; it copies
	// the (already-sealed) user DB as opaque bytes and has no need for the key.
	cmd.Env = childEnv()
	return cmd
}

func tailString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}

func (s *apiServer) currentLiveOutput() string {
	if s.liveOut == nil {
		return ""
	}
	return strings.TrimSpace(s.liveOut.String())
}

func readTailFile(path string, maxBytes int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	var offset int64 = 0
	if st.Size() > maxBytes {
		offset = st.Size() - maxBytes
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func readJSONArtifact(path string, fallback interface{}) interface{} {
	b, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		return fallback
	}
	return out
}

// loadReportMeta reads REPORT_META embedded in index.html and enriches it with
// fields from ncc-run-record.json (hostname, scheduler_source, git_revision,
// orchestrator_version) when they are missing or empty. Older reports — produced
// before reportMeta carried these fields — are upgraded transparently.
func loadReportMeta(outDir string) map[string]interface{} {
	out := map[string]interface{}{}
	if v, ok := readInlineJSONVar(filepath.Join(outDir, "index.html"), "REPORT_META", map[string]interface{}{}).(map[string]interface{}); ok {
		for k, val := range v {
			out[k] = val
		}
	}
	recPath := filepath.Join(outDir, "ncc-run-record.json")
	if b, err := os.ReadFile(recPath); err == nil {
		var rec map[string]interface{}
		if err := json.Unmarshal(b, &rec); err == nil {
			fill := func(key string, recKey string) {
				if cur, ok := out[key].(string); ok && strings.TrimSpace(cur) != "" {
					return
				}
				if v, ok := rec[recKey].(string); ok && strings.TrimSpace(v) != "" {
					out[key] = v
				}
			}
			fill("hostname", "hostname")
			fill("scheduler_source", "scheduler_source")
			fill("git_revision", "git_revision")
			fill("stream", "stream")
			if cur, ok := out["version"].(string); !ok || strings.TrimSpace(cur) == "" {
				if v, ok := rec["orchestrator_version"].(string); ok && strings.TrimSpace(v) != "" {
					out["version"] = v
				}
			}
		}
	}
	return out
}

// inlineVarPrefixPattern builds a regex matching only the `const NAME =` /
// `var NAME =` declaration prefix (never the value itself). The value's end
// is located separately via findJSONValueEnd, which is JSON-aware — see its
// doc comment for why a naive `(.*?);` regex is unsafe here.
func inlineVarPrefixPattern(varName string) *regexp.Regexp {
	return regexp.MustCompile(`(?:const|var)\s+` + regexp.QuoteMeta(varName) + `\s*=\s*`)
}

// findJSONValueEnd scans the JSON value that starts at (or after) index i in
// data — skipping leading whitespace — and returns the index just past its
// final character.
//
// This exists because index.html embeds report data as
// `const AGG = <json>;` and a naive `(.*?);` regex (the previous approach)
// stops at the FIRST semicolon it finds, including one that appears inside a
// quoted JSON string. NCC check titles/details very commonly contain literal
// semicolons (e.g. "Description: X; Recommendation: Y"), so that regex would
// silently truncate the embedded JSON mid-string, corrupting the value on
// read AND, more seriously, on write: replaceInlineJSONVar used the same
// pattern to locate the OLD value to overwrite during report merges, so a
// semicolon anywhere in the previous AGG payload could cause a merge to
// splice fresh JSON together with a dangling fragment of the old JSON,
// permanently corrupting the canonical index.html until the next full
// (non-merged) report write. This scanner instead tracks bracket depth and
// string-escaping so it finds the true end of the array/object/string/
// primitive, regardless of any semicolons embedded inside it.
func findJSONValueEnd(data []byte, i int) (int, bool) {
	n := len(data)
	for i < n {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		}
		break
	}
	if i >= n {
		return 0, false
	}
	start := i
	switch data[i] {
	case '[', '{':
		depth := 0
		inString := false
		escaped := false
		for ; i < n; i++ {
			c := data[i]
			if inString {
				switch {
				case escaped:
					escaped = false
				case c == '\\':
					escaped = true
				case c == '"':
					inString = false
				}
				continue
			}
			switch c {
			case '"':
				inString = true
			case '[', '{':
				depth++
			case ']', '}':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
		}
		return 0, false
	case '"':
		escaped := false
		for i++; i < n; i++ {
			c := data[i]
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				return i + 1, true
			}
		}
		return 0, false
	default:
		// A bare primitive (number/true/false/null): ends at the next
		// statement-terminating character.
		for ; i < n; i++ {
			switch data[i] {
			case ';', ',', '\n', '\r', ' ', '\t':
				if i == start {
					return 0, false
				}
				return i, true
			}
		}
		if i == start {
			return 0, false
		}
		return i, true
	}
}

func readInlineJSONVar(path, varName string, fallback interface{}) interface{} {
	b, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	loc := inlineVarPrefixPattern(varName).FindIndex(b)
	if loc == nil {
		return fallback
	}
	end, ok := findJSONValueEnd(b, loc[1])
	if !ok {
		return fallback
	}
	raw := strings.TrimSpace(string(b[loc[1]:end]))
	if raw == "" {
		return fallback
	}
	var out interface{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return fallback
	}
	return out
}

func (s *apiServer) selectBestReportOutDir() string {
	seen := map[string]bool{}
	candidates := []string{}
	add := func(p string) {
		if strings.TrimSpace(p) == "" {
			return
		}
		abs := s.absPath(p)
		if seen[abs] {
			return
		}
		seen[abs] = true
		candidates = append(candidates, abs)
	}
	add(s.outputDir)
	add(filepath.Join(filepath.Dir(s.absPath(s.configPath)), "outputfiles"))
	if strings.TrimSpace(s.lastCfg) != "" {
		add(filepath.Join(filepath.Dir(s.lastCfg), "outputfiles"))
	}
	best := s.absPath(s.outputDir)
	bestScore := -1
	for _, c := range candidates {
		agg := readInlineJSONVar(filepath.Join(c, "index.html"), "AGG", []interface{}{})
		score := 0
		if rows, ok := agg.([]interface{}); ok {
			score = len(rows)
		}
		if score > bestScore {
			best = c
			bestScore = score
		}
	}
	return best
}

func listNCCLogs(logDir string) []map[string]string {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return []map[string]string{}
	}
	out := make([]map[string]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".log") {
			continue
		}
		out = append(out, map[string]string{
			"name": name,
			"path": filepath.Join(logDir, name),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"] < out[j]["name"] })
	return out
}

// classifyClusterErrorMessage mirrors goncc.classifyClusterError so the
// API server can heal stale `error_class` values written by older binaries.
// Keep this in sync with the orchestrator implementation in goNCC.go.
func classifyClusterErrorMessage(raw string) string {
	msg := strings.ToLower(strings.TrimSpace(raw))
	if msg == "" {
		return ""
	}
	switch {
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "timed out"), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "http 401"), strings.Contains(msg, "http 403"), strings.Contains(msg, "unauthorized"), strings.Contains(msg, "forbidden"), strings.Contains(msg, "authentication"):
		return "auth"
	case strings.Contains(msg, "no such host"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "host is down"),
		strings.Contains(msg, "tls"),
		strings.Contains(msg, "x509"):
		return "network"
	case strings.Contains(msg, "http 429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "rate limit"),
		strings.Contains(msg, "rate-limit"),
		strings.Contains(msg, "retry-after"):
		return "rate_limit"
	case strings.Contains(msg, "parse filtered"), strings.Contains(msg, "parse summary"), strings.Contains(msg, "parser"):
		return "parser"
	case strings.Contains(msg, "retry circuit breaker opened"),
		strings.Contains(msg, "transport error"),
		strings.Contains(msg, "dial tcp"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection reset"):
		return "network"
	case strings.Contains(msg, "http 4"), strings.Contains(msg, "http 5"), strings.Contains(msg, "start checks failed"), strings.Contains(msg, "get summary failed"), strings.Contains(msg, "poll failed"):
		return "api"
	default:
		return "unknown"
	}
}

// reclassifyRunSummaryInPlace walks the parsed run-summary map and rewrites
// each failed cluster's `error_class` using the current classifier, then
// rebuilds the top-level `failure_classes` histogram so the Insights "Run
// Reliability" panel reflects the corrected buckets even for legacy runs.
func reclassifyRunSummaryInPlace(m map[string]interface{}) {
	clusters, ok := m["clusters"].([]interface{})
	if !ok {
		return
	}
	counts := map[string]int{
		"timeout": 0, "auth": 0, "network": 0, "parser": 0,
		"rate_limit": 0, "api": 0, "unknown": 0,
	}
	for _, raw := range clusters {
		c, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if okFlag, _ := c["ok"].(bool); okFlag {
			continue
		}
		errMsg, _ := c["error"].(string)
		if strings.TrimSpace(errMsg) == "" {
			continue
		}
		class := classifyClusterErrorMessage(errMsg)
		if class == "" {
			class = "unknown"
		}
		c["error_class"] = class
		counts[class]++
	}
	m["failure_classes"] = map[string]interface{}{
		"timeout":    counts["timeout"],
		"auth":       counts["auth"],
		"network":    counts["network"],
		"parser":     counts["parser"],
		"rate_limit": counts["rate_limit"],
		"api":        counts["api"],
		"unknown":    counts["unknown"],
	}
}

// parseNCCSummaryCounts aggregates per-state counts from every *.log file in
// logDir, recognising the NCC summary table. The "Warning" row was previously
// omitted, which caused the UI to show a spurious "summary count mismatch"
// banner whenever NCC reported any WARN rows.
func parseNCCSummaryCounts(logDir string) map[string]int {
	totals := map[string]int{
		"fail":          0,
		"warn":          0,
		"pass":          0,
		"info":          0,
		"error":         0,
		"unknown":       0,
		"total_plugins": 0,
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return totals
	}
	parseCount := func(content, label string) int {
		re := regexp.MustCompile(`(?im)\|\s*` + regexp.QuoteMeta(label) + `\s*\|\s*(\d+)\s*\|`)
		m := re.FindStringSubmatch(content)
		if len(m) < 2 {
			return 0
		}
		v, _ := strconv.Atoi(m[1])
		return v
	}
	// Some NCC variants emit "Warning", others "Warn". Try both and take the
	// first that produced a non-zero value (or the last one, to be safe).
	parseWarn := func(content string) int {
		if v := parseCount(content, "Warning"); v > 0 {
			return v
		}
		return parseCount(content, "Warn")
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(logDir, e.Name()))
		if err != nil {
			continue
		}
		content := string(b)
		totals["fail"] += parseCount(content, "Fail")
		totals["warn"] += parseWarn(content)
		totals["pass"] += parseCount(content, "Pass")
		totals["info"] += parseCount(content, "Info")
		totals["error"] += parseCount(content, "Error")
		totals["unknown"] += parseCount(content, "Unknown")
		totals["total_plugins"] += parseCount(content, "Total Plugins")
	}
	return totals
}

func parseNCCClusterSummary(logDir string) []map[string]interface{} {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return []map[string]interface{}{}
	}
	out := make([]map[string]interface{}, 0, len(entries))
	parseCount := func(content, label string) int {
		re := regexp.MustCompile(`(?im)\|\s*` + regexp.QuoteMeta(label) + `\s*\|\s*(\d+)\s*\|`)
		m := re.FindStringSubmatch(content)
		if len(m) < 2 {
			return 0
		}
		v, _ := strconv.Atoi(m[1])
		return v
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".log") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(logDir, name))
		if err != nil {
			continue
		}
		content := string(b)
		fail := parseCount(content, "Fail")
		warn := parseCount(content, "Warning")
		if warn == 0 {
			warn = parseCount(content, "Warn")
		}
		pass := parseCount(content, "Pass")
		info := parseCount(content, "Info")
		errCount := parseCount(content, "Error")
		unknown := parseCount(content, "Unknown")
		total := parseCount(content, "Total Plugins")
		healthRate := 0.0
		if total > 0 {
			healthRate = (float64(pass) / float64(total)) * 100.0
		}
		addr := strings.TrimSuffix(name, ".log")
		out = append(out, map[string]interface{}{
			"address":       addr,
			"log_name":      name,
			"fail":          fail,
			"warn":          warn,
			"pass":          pass,
			"info":          info,
			"error":         errCount,
			"unknown":       unknown,
			"total_plugins": total,
			"health_rate":   healthRate,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["address"]) < fmt.Sprint(out[j]["address"])
	})
	return out
}

func redactedArgs(args []string) []string {
	if len(args) == 0 {
		return args
	}
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == "--password" {
			out[i+1] = "***"
		}
	}
	return out
}

func tokenSource(activeToken, envToken string) string {
	if strings.TrimSpace(envToken) != "" && strings.TrimSpace(activeToken) == strings.TrimSpace(envToken) {
		return "env"
	}
	return "generated"
}

// runHealthCheckProbe implements the --health-check mode. It does
// NOT bind a listening socket. Instead it:
//
//  1. Reads the on-disk token from --token-file-path (the api-server
//     wrote this on its first start; stack-aware defaults will have
//     pointed it at <root>/.ncc-api-token if applicable).
//  2. Issues GET /api/v1/health to the URL derived from --listen,
//     forcing IPv4 loopback when the bind addr is :PORT or
//     0.0.0.0:PORT (avoids the macOS IPv6/IPv4 mismatch the
//     orchestrator's wait-ready already works around).
//  3. Exits 0 on a 200 with `data.status == "ok"`, 1 on any other
//     outcome.
//
// Designed for `HEALTHCHECK CMD ["ncc-api-server", "--health-check"]`
// in Docker images and `livenessProbe.exec.command` in Kubernetes
// manifests, so operators don't have to ship curl/wget in the image.
func runHealthCheckProbe(s *apiServer, listenAddr string, timeout time.Duration) {
	tokenPath := s.tokenFilePath
	if !filepath.IsAbs(tokenPath) {
		tokenPath = filepath.Join(s.repoRoot, tokenPath)
	}
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: read token %s: %v\n", tokenPath, err)
		os.Exit(1)
	}
	token := strings.TrimSpace(string(tokenBytes))

	host, port, err := net.SplitHostPort(strings.TrimSpace(listenAddr))
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: parse listen %q: %v\n", listenAddr, err)
		os.Exit(1)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/api/v1/health"

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: build request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("X-Api-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-check: GET %s: %v\n", url, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health-check: %s -> HTTP %d\n", url, resp.StatusCode)
		os.Exit(1)
	}
	var env struct {
		Success bool `json:"success"`
		Data    struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		fmt.Fprintf(os.Stderr, "health-check: decode body: %v\n", err)
		os.Exit(1)
	}
	if !env.Success || env.Data.Status != "ok" {
		fmt.Fprintf(os.Stderr, "health-check: %s -> success=%v status=%q\n", url, env.Success, env.Data.Status)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "ncc-api-server health: ok (%s)\n", url)
	os.Exit(0)
}

// handleSubcommandArgs reacts to positional arguments left over after
// flag.Parse(). The api-server is a long-running daemon that takes
// only flags, but users routinely confuse it with the orchestrator
// (e.g. `./ncc-api-server update --check`) — without this guard,
// Go's flag package silently ignores the trailing args and the
// server starts up anyway, surprising the user. We:
//
//   - exit 0 with build metadata for `version` (so users get a quick
//     way to identify which build of the api-server they have);
//   - exit 2 with a redirect-to-orchestrator pointer for everything
//     else, including a hint about the orchestrator binary inside the
//     same v2 stack when one was detected.
func handleSubcommandArgs(args []string) {
	if len(args) == 0 {
		return
	}
	first := strings.ToLower(strings.TrimSpace(args[0]))
	switch first {
	case "version", "--version", "-version":
		fmt.Printf("ncc-api-server\n  version: %s\n  stream:  %s\n  build:   %s\n  go:      %s\n  os/arch: %s/%s\n",
			Version, Stream, BuildDate, GoVersion, runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	case "help", "--help", "-help", "-h":
		// flag.Parse already printed usage for "-h" / "--help"; treat
		// bare "help" the same way.
		flag.Usage()
		os.Exit(0)
	}
	// Anything else (update, v2-start, run, ...): redirect to the
	// orchestrator and exit 2 so scripts notice.
	fmt.Fprintf(os.Stderr,
		"ncc-api-server: unrecognized subcommand %q.\n"+
			"This binary is a sub-component of the Nutanix NCC Orchestrator stack and only accepts --flags.\n",
		args[0])
	if root, ok := v2layout.DetectStackRootFromExe(); ok {
		if orch := v2layout.FindBinary(root, "ncc-orchestrator"); orch != "" {
			fmt.Fprintf(os.Stderr,
				"For lifecycle commands like %q, run the orchestrator instead:\n  %s %s\n",
				args[0], orch, strings.Join(args, " "))
			os.Exit(2)
		}
	}
	fmt.Fprintf(os.Stderr,
		"For lifecycle commands like %q, run `ncc-orchestrator %s` instead.\n",
		args[0], strings.Join(args, " "))
	os.Exit(2)
}

// applyStackAwareDefaults rewrites the path flags on s when the
// api-server is running from inside an extracted v2 stack and the
// user did not provide an explicit value for the flag. Mirrors the
// orchestrator's v2.0.2 install-dir auto-detect so users who copy
// only the api-server out of the archive (or run it from
// <root>/bin/) get sensible defaults without having to re-derive
// every path.
//
// Detection is structural via internal/v2layout.DetectStackRootFromExe
// and only fires when the executable's directory is named "bin" AND
// the parent contains a v2 marker (frontend-dist/ or
// bin/ncc-api-server*). All other invocations (Docker images that
// just COPY bin/* into /usr/local/bin, manual extraction outside a
// bin/ subdirectory, dev runs from a checkout) fall through to the
// pre-existing CWD-relative defaults.
//
// argv is the post-program-name argv slice (i.e. os.Args[1:]); we use
// it to identify which flags the user explicitly set so that an
// explicit `--config-path /etc/foo.yaml` is never silently overridden
// by the auto-detect.
// buildHandler registers every route on a fresh mux and wraps it in the
// CORS, rate-limit, and auth middleware. It is shared by main() and the
// end-to-end tests so both exercise identical routing and middleware.
func (s *apiServer) buildHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/audit", s.handleAudit)
	mux.HandleFunc("/api/v1/metrics/rate-limit", s.handleRateLimitMetrics)
	mux.HandleFunc("/metrics", s.handlePrometheusMetrics)
	mux.HandleFunc("/api/v1/auth/session", s.handleAuthSession)
	mux.HandleFunc("/api/v1/auth/rotate", s.handleAuthRotate)
	mux.HandleFunc("/api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("/api/v1/auth/logout", s.handleLogout)
	mux.HandleFunc("/api/v1/auth/logout-all", s.handleLogoutAll)
	mux.HandleFunc("/api/v1/auth/me", s.handleMe)
	mux.HandleFunc("/api/v1/auth/change-password", s.handleChangePassword)
	mux.HandleFunc("/api/v1/auth/refresh", s.handleAuthRefresh)
	mux.HandleFunc("/api/v1/auth/forgot-password", s.handleForgotPassword)
	// Self-service personal access tokens: any authenticated user manages their
	// own (handlers 501 when the store is not writable).
	mux.HandleFunc("/api/v1/auth/tokens", s.handleAuthTokens)
	mux.HandleFunc("/api/v1/auth/tokens/", s.handleAuthTokenByID)
	// Register SAML endpoints when SAML is active now or could be enabled at
	// runtime (a writable user db is present). Handlers 503 when no provider.
	if s.samlEnabled || s.users.writable() {
		s.registerSAML(mux)
	}
	// Admin-only account and SSO management (under /api/v1/settings/*). Only
	// meaningful when the store can persist changes.
	if s.users.writable() {
		mux.HandleFunc("/api/v1/settings/users", s.handleUsers)
		mux.HandleFunc("/api/v1/settings/users/", s.handleUserByName)
		mux.HandleFunc("/api/v1/settings/sso", s.handleSSO)
		mux.HandleFunc("/api/v1/settings/ldap", s.handleLDAP)
		mux.HandleFunc("/api/v1/settings/ldap/test", s.handleLDAPTest)
		mux.HandleFunc("/api/v1/settings/ldap/search", s.handleLDAPSearch)
		mux.HandleFunc("/api/v1/settings/session", s.handleSessionPolicy)
		mux.HandleFunc("/api/v1/settings/tls", s.handleTLSSettings)
		mux.HandleFunc("/api/v1/settings/tls/generate", s.handleTLSGenerate)
		mux.HandleFunc("/api/v1/settings/password-resets", s.handlePasswordResets)
		mux.HandleFunc("/api/v1/settings/password-resets/", s.handlePasswordResetByName)
		mux.HandleFunc("/api/v1/settings/cluster-groups", s.handleClusterGroups)
		mux.HandleFunc("/api/v1/settings/tokens", s.handleAdminTokens)
		mux.HandleFunc("/api/v1/settings/tokens/", s.handleAdminTokenByID)
	}
	mux.HandleFunc("/api/v1/settings/clusters", s.handleClusterInventory)
	mux.HandleFunc("/api/v1/settings/pc-clusters", s.handlePCDiscover)
	mux.HandleFunc("/api/v1/settings/backup", s.handleBackup)
	mux.HandleFunc("/api/v1/settings/restore", s.handleRestore)
	mux.HandleFunc("/api/v1/settings/backups", s.handleBackups)
	mux.HandleFunc("/api/v1/settings/backups/restore", s.handleBackupRestoreNamed)
	mux.HandleFunc("/api/v1/settings/backups/verify", s.handleBackupVerifyNamed)
	mux.HandleFunc("/api/v1/settings/backups/schedule", s.handleBackupSchedule)
	mux.HandleFunc("/api/v1/settings/backups/delete", s.handleBackupDelete)
	mux.HandleFunc("/api/v1/settings/backups/download", s.handleBackupDownloadNamed)
	mux.HandleFunc("/api/v1/settings/update", s.handleUpdateCheck)
	mux.HandleFunc("/api/v1/settings/update/apply", s.handleUpdateApply)
	mux.HandleFunc("/api/v1/settings/config", s.handleConfig)
	mux.HandleFunc("/api/v1/settings/config/batch", s.handleConfigBatch)
	mux.HandleFunc("/api/v1/settings/configs", s.handleConfigsList)
	mux.HandleFunc("/api/v1/settings/config-files", s.handleConfigFiles)
	mux.HandleFunc("/api/v1/settings/config-file", s.handleConfigFile)
	mux.HandleFunc("/api/v1/settings/config-files/batch", s.handleConfigFilesBatch)
	mux.HandleFunc("/api/v1/settings/notifications", s.handleNotifications)
	mux.HandleFunc("/api/v1/settings/notifications/test", s.handleNotificationsTest)
	mux.HandleFunc("/api/v1/schedule", s.handleSchedule)
	mux.HandleFunc("/api/v1/schedule/health", s.handleScheduleHealth)
	mux.HandleFunc("/api/v1/health/diagnostics", s.handleHealthDiagnostics)
	mux.HandleFunc("/api/v1/health/diagnostics/support-bundle", s.handleHealthSupportBundle)
	mux.HandleFunc("/api/v1/artifacts", s.handleArtifacts)
	mux.HandleFunc("/api/v1/artifacts/", s.handleArtifactByName)
	mux.HandleFunc("/api/v1/runs", s.handleRuns)
	mux.HandleFunc("/api/v1/runs/", s.handleRunsRouter)
	mux.HandleFunc("/api/v1/runs/summary", s.handleRunSummary)
	mux.HandleFunc("/api/v1/runs/active", s.handleRunActive)
	mux.HandleFunc("/api/v1/runs/config-preference", s.handleRunConfigPreference)
	mux.HandleFunc("/api/v1/runs/configs", s.handleConfigsList)
	mux.HandleFunc("/api/v1/runs/preflight", s.handleRunPreflight)
	mux.HandleFunc("/api/v1/runs/trigger", s.handleRunTrigger)
	mux.HandleFunc("/api/v1/report/data", s.handleReportData)
	mux.HandleFunc("/api/v1/report/trends", s.handleReportTrends)
	mux.HandleFunc("/api/v1/logs/runner", s.handleRunnerLogs)
	mux.HandleFunc("/api/v1/openapi.json", s.handleOpenAPI)
	mux.HandleFunc("/api/v1/meta/routes", s.handleMetaRoutes)
	mux.HandleFunc("/docs/assets/", s.handleSwaggerAsset)
	mux.HandleFunc("/", s.handleAPIDocsHome)

	return s.withCORS(s.withRateLimit(s.withAuth(mux)))
}

// defaultUsersDBPath returns the writable user-database path used when no
// backend is configured. It lives alongside the other server state in the
// repo root so the default-on login flow "just works" for a bare run.
func defaultUsersDBPath(repoRoot string) string {
	root := strings.TrimSpace(repoRoot)
	if root == "" {
		root = "."
	}
	return v2layout.UsersDB(root)
}

func applyStackAwareDefaults(s *apiServer, argv []string) {
	root, ok := v2layout.DetectStackRootFromExe()
	if !ok {
		return
	}
	explicit := explicitFlagSet(argv)

	// Only auto-redirect a flag that's still at its compile-time
	// default. The flag package's default values match what the
	// CWD-relative invocation would have produced.
	type pathFlag struct {
		flagName  string
		current   *string
		zeroValue string
		stackPath string
	}
	// Pre-resolve every path through symlinks (matches the
	// orchestrator v2-start fix). Without this, /var on macOS
	// resolves to /private/var inside the api-server's path
	// sandbox, but the un-resolved /var paths we pass for
	// config-path/output-dir/log-dir get rejected as "path escapes
	// repo root".
	resolvedRoot := v2layout.ResolveToReal(root)
	cfg := v2layout.ConfigPath(root)
	if cfg == "" {
		// no config.yaml AND no example_config.yaml in stack — leave
		// the api-server's --config-path default alone so its existing
		// "config not found" error message kicks in.
		cfg = s.configPath
	} else {
		cfg = v2layout.ResolveToReal(cfg)
	}
	// Resolve the orchestrator binary inside <root>/bin (canonical
	// or platform-suffixed). FindBinary returns "" when the
	// orchestrator wasn't shipped (e.g. someone copied just the
	// api-server binary into <root>/bin/) — in that case we leave
	// --orchestrator-bin alone and the user gets the existing
	// "orchestrator binary not found" error path when triggering a
	// run.
	orchBin := v2layout.FindBinary(root, "ncc-orchestrator")
	if orchBin != "" {
		orchBin = v2layout.ResolveToReal(orchBin)
	}
	flags := []pathFlag{
		{"repo-root", &s.repoRoot, ".", resolvedRoot},
		{"config-path", &s.configPath, "config.yaml", cfg},
		{"output-dir", &s.outputDir, "outputfiles", v2layout.ResolveToReal(v2layout.OutputDir(root))},
		{"log-dir", &s.logDir, "nccfiles", v2layout.ResolveToReal(v2layout.LogDir(root))},
		{"token-file-path", &s.tokenFilePath, ".ncc-api-token", v2layout.ResolveToReal(v2layout.TokenFile(root))},
		{"users-db", &s.usersDBPath, "", v2layout.ResolveToReal(v2layout.UsersDB(root))},
		{"orchestrator-bin", &s.orchestratorBin, "./ncc-orchestrator", orchBin},
	}
	rewrote := []string{}
	for _, f := range flags {
		if explicit[f.flagName] {
			continue
		}
		if *f.current != f.zeroValue {
			continue
		}
		if f.stackPath == "" || f.stackPath == *f.current {
			continue
		}
		*f.current = f.stackPath
		rewrote = append(rewrote, f.flagName)
	}
	if len(rewrote) > 0 {
		fmt.Fprintf(os.Stderr,
			"[stack-aware] detected v2 stack at %s; auto-resolved %s\n",
			root, strings.Join(rewrote, ", "))
		fmt.Fprintln(os.Stderr,
			"             pass any of those flags explicitly to override.")
	}
}

// explicitFlagSet returns the set of flag names the user passed on
// the command line. We reparse argv ourselves rather than using
// flag.Visit because Visit reports flags whose values match the
// default when set from environment variables in some workflows;
// here we want strict "did the user type --foo on the command line".
func explicitFlagSet(argv []string) map[string]bool {
	out := map[string]bool{}
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		a = strings.TrimLeft(a, "-")
		if i := strings.IndexByte(a, '='); i >= 0 {
			a = a[:i]
		}
		if a == "" {
			continue
		}
		out[a] = true
	}
	return out
}

func (s *apiServer) ensureAuthToken() error {
	if strings.TrimSpace(s.authToken) == "" {
		b := make([]byte, 32)
		if _, err := crand.Read(b); err != nil {
			return fmt.Errorf("generate auth token: %w", err)
		}
		s.authToken = base64.RawURLEncoding.EncodeToString(b)
	}
	tokenPath := s.absPath(s.tokenFilePath)
	if err := os.MkdirAll(filepath.Dir(tokenPath), 0o700); err != nil {
		return fmt.Errorf("prepare token file dir: %w", err)
	}
	if err := os.WriteFile(tokenPath, []byte(s.authToken+"\n"), 0o600); err != nil {
		return fmt.Errorf("write token file: %w", err)
	}
	log.Printf("api auth token ready (source=%s, token_file=%s)", tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN")), tokenPath)
	return nil
}
