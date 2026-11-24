package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"goncc/internal/graceful"
	"goncc/internal/plural"
	"goncc/internal/runtimecaps"
	"goncc/internal/runtimetune"
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
		Version = "2.4.0"
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
	capabilities            runtimecaps.Capabilities
	repoRoot                string
	configPath              string
	outputDir               string
	logDir                  string
	runnerLogPath           string
	serverLogPath           string
	logLevel                string
	logFormat               string
	scheduleStatePath       string
	backupScheduleStatePath string
	notificationStatePath   string
	auditLogPath            string
	auditLogMaxBytes        int64
	auditLogKeep            int
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
	// Additional brute-force controls (0 = 4x the per-account threshold).
	loginLockAccountThreshold int
	loginLockSourceThreshold  int
	loginGuardStateFile       string
	trustedProxyCIDRs         string
	orchestratorBin           string
	authToken                 string
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
	// runnerCronJob names the Kubernetes CronJob that Settings > Schedule
	// reads and patches (--runner-cronjob, NCC_RUNNER_CRONJOB).
	runnerCronJob string
	// runnerImage caches the runner CronJob's image tag for the version-skew
	// check (versionskew.go).
	runnerImage runnerImageCache
	// onboardingMu serializes read-modify-write of the setup checklist state.
	onboardingMu sync.Mutex
	// scheduleK8sClient builds the Kubernetes API client for the schedule;
	// nil means in-cluster credentials. Tests point it at a fake API server.
	scheduleK8sClient func() (*k8sSecretClient, error)
	// usersDBKeyFile points at a file holding a 32-byte master key (base64/hex)
	// used to envelope-encrypt the user store at rest. The NCC_MASTER_KEY env
	// takes precedence. Empty (and env unset) => plaintext store (default).
	usersDBKeyFile string
	// userStoreEncrypted records whether the file-backed user store is wrapped
	// with envelope encryption (a master key was configured). Surfaced on
	// /api/v1/health so operators can confirm secrets-at-rest is active.
	userStoreEncrypted bool
	// requireEncryptedUserStore makes a missing master key fatal at startup.
	requireEncryptedUserStore bool
	// disableLocalAccounts opts out of the default-on user database. By
	// default (no flag, env, secret, or stack path) the server falls back to a
	// writable JSON file in the repo root so a bare run still bootstraps a
	// first-run admin and enables login. Set --disable-local-accounts (or
	// NCC_DISABLE_LOCAL_ACCOUNTS=1) for pure token-only automation.
	disableLocalAccounts bool
	readyCache           readinessCache
	httpMetrics          httpMetrics
	watch                clientWatch
	absCache             absPathCache
	// adminSelfResetEnabled enables the anonymous forgot-password recovery for
	// the built-in admin. Off by default: that path lets any network client
	// rotate the admin password (and end every admin session) once a minute
	// per source IP, while the offline v2-reset-password / --reset-admin
	// recovery needs the same host access reading the new password does.
	adminSelfResetEnabled bool
	users                 *userDB
	samlEnabled           bool
	samlFromFlags         bool // SAML came from startup flags (not runtime-editable)
	samlMu                sync.RWMutex
	saml                  *samlProvider
	oidcMu                sync.RWMutex
	oidc                  *oidcProvider // built lazily from the stored config
	ldapEnabled           bool
	ldapFromFlags         bool // LDAP came from startup flags (not runtime-editable)
	ldapMu                sync.RWMutex
	ldap                  ldapAuthenticator
	// pcCacheMu guards the Prism Central -> managed-clusters discovery cache used
	// to expand cluster-group PC entries into their registered clusters.
	pcCacheMu        sync.Mutex
	pcCache          map[string]*pcCacheEntry
	pcInflight       map[string]bool
	pcAlertsMu       sync.Mutex
	pcAlertsCache    *pcAlertsCacheEntry
	pcAlertsCacheTTL time.Duration
	// cookieInsecure forces the Secure attribute OFF on session cookies even
	// when HTTPS is enabled (useful only behind a TLS-terminating proxy that
	// re-presents plain http to the stack). Insecure is already the default.
	cookieInsecure bool
	// cookieSecureForce forces the Secure attribute ON regardless of the
	// runtime HTTPS policy, for deployments that terminate TLS in front of the
	// stack (reverse proxy / load balancer) yet still want Secure cookies.
	cookieSecureForce  bool
	sessionSecret      string
	jwtSecret          []byte // HS256 key for stateless session JWTs (NCC_JWT_SECRET)
	jwtSecretEphemeral bool   // true when the JWT key was generated in-process (not replica-safe)
	sessionTTL         time.Duration
	sessionIssuer      string
	runTimeout         time.Duration
	debugExpose        bool
	// healthPathCache holds the resolved display paths for /api/v1/health.
	healthPathCache    healthPathCache
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

	// NCC alert lifecycle (persistent acknowledge/resolve state + history).
	alertStoreMu          sync.Mutex
	alertStore            *alertLifecycleStore
	alertHistoryRetainDay int
}

func main() {
	// Size the Go heap to the container's memory limit (no-op on hosts).
	runtimetune.Apply()
	var s apiServer
	s.capabilities = runtimecaps.Detect()
	var listen string

	flag.StringVar(&listen, "listen", ":8081", "HTTP listen address")
	flag.StringVar(&s.repoRoot, "repo-root", ".", "Repository root path")
	flag.StringVar(&s.configPath, "config-path", "config.yaml", "Application config path")
	flag.StringVar(&s.outputDir, "output-dir", "outputfiles", "Artifacts output directory")
	flag.StringVar(&s.logDir, "log-dir", "nccfiles", "Raw logs directory")
	flag.StringVar(&s.runnerLogPath, "runner-log-path", "logs/ncc-runner.log", "Runner log file path")
	flag.StringVar(&s.serverLogPath, "server-log-path", envOrDefault("NCC_SERVER_LOG_PATH", defaultServerLogPath), "API server's own log (JSON lines, rotated at 50 MB, 10 kept). \"-\" logs to stderr only. Also via NCC_SERVER_LOG_PATH")
	flag.StringVar(&s.logLevel, "log-level", envOrDefault("NCC_LOG_LEVEL", "info"), "API server log level: debug, info, warn or error (changeable at run time in Settings → Logs). Also via NCC_LOG_LEVEL")
	flag.StringVar(&s.logFormat, "log-format", envOrDefault("NCC_LOG_FORMAT", "text"), "Console (stderr) log format: text or json. The log file is always JSON. Also via NCC_LOG_FORMAT")
	flag.StringVar(&s.scheduleStatePath, "schedule-state-path", ".ncc-api-schedule.json", "Schedule state file path")
	flag.StringVar(&s.backupScheduleStatePath, "backup-schedule-state-path", ".ncc-api-backup-schedule.json", "Scheduled-backup state file path")
	flag.StringVar(&s.notificationStatePath, "notifications-state-path", ".ncc-api-notifications.json", "Legacy notifications metadata path (delivery history/API-only fields)")
	flag.StringVar(&s.auditLogPath, "audit-log-path", "logs/ncc-audit.log", "JSONL audit log file path")
	flag.Int64Var(&s.auditLogMaxBytes, "audit-log-max-bytes", 5*1024*1024, "Audit log size before rotation (bytes); 0 disables rotation")
	flag.IntVar(&s.auditLogKeep, "audit-log-keep", 10, "Number of rotated audit log generations to keep (<path>.1 … <path>.N)")
	flag.StringVar(&s.auditForwardHTTPURL, "audit-forward-http-url", "", "Forward each audit event (JSON) to this HTTP collector endpoint (Splunk HEC, Elastic, Loki, generic webhook). Empty disables.")
	flag.StringVar(&s.auditForwardHTTPAuth, "audit-forward-http-auth", "", "Authorization header value for the audit HTTP collector (e.g. 'Bearer <token>' or 'Splunk <hec-token>')")
	flag.BoolVar(&s.auditForwardHTTPSplunk, "audit-forward-http-splunk", false, "Wrap each forwarded audit event as a Splunk HEC payload {\"event\": ...}")
	flag.StringVar(&s.auditForwardSyslog, "audit-forward-syslog", "", "Forward each audit event as an RFC5424 syslog message to host:port (e.g. siem.example.com:514). Empty disables.")
	flag.StringVar(&s.auditForwardSyslogNet, "audit-forward-syslog-network", "udp", "Transport for --audit-forward-syslog: udp or tcp")
	flag.IntVar(&s.alertHistoryRetainDay, "alert-history-retain-days", alertDefaultHistoryRetainDy, "Days to keep closed NCC finding instances and their lifecycle events (active alerts are never pruned; 0 keeps forever)")
	flag.IntVar(&s.loginLockThreshold, "login-lockout-threshold", 5, "Failed logins per account before a temporary lockout (0 disables)")
	flag.IntVar(&s.loginLockAccountThreshold, "login-lockout-account-threshold", 0, "Failed logins for one account from any source before an account-wide lockout (0 = 4x --login-lockout-threshold)")
	flag.IntVar(&s.loginLockSourceThreshold, "login-lockout-source-threshold", 0, "Failed logins from one client IP across all accounts before that source is locked (0 = 4x --login-lockout-threshold)")
	flag.StringVar(&s.loginGuardStateFile, "login-guard-state-file", os.Getenv("NCC_LOGIN_GUARD_STATE_FILE"), "Shared JSON file for login failure counters so limits hold across API replicas (default in Kubernetes: <repo-root>/.ncc-login-guard.json; empty elsewhere = in-memory)")
	flag.StringVar(&s.trustedProxyCIDRs, "trusted-proxy-cidrs", defaultIfEmpty(os.Getenv("NCC_TRUSTED_PROXY_CIDRS"), "private,loopback"), "Comma-separated peers whose X-Forwarded-For is honored for client-IP rate limiting: CIDRs, 'loopback', 'private', or 'none'. Narrow to your reverse proxy / ingress / UI pod CIDRs to prevent IP spoofing")
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
	flag.DurationVar(&s.pcAlertsCacheTTL, "pc-alerts-cache-ttl", 5*time.Minute, "Prism Central alerts cache TTL (config key: pc-alerts-cache-ttl; 0 disables caching)")
	flag.BoolVar(&s.metricsPublic, "metrics-public", false, "Allow unauthenticated GET /metrics for Prometheus scrapers (off by default; on private networks behind a service mesh this is safe)")
	flag.StringVar(&s.usersFilePath, "users-file", "", "Path to a read-only YAML seed of local accounts (imported into --users-db on first run)")
	flag.StringVar(&s.usersDBPath, "users-db", "", "Path to the writable JSON user database file; enables login, first-run admin bootstrap, and runtime user/SSO management")
	flag.StringVar(&s.usersDBSecret, "users-db-secret", "", "Kubernetes Secret name to store the user database in (encrypted at rest by etcd); mutually exclusive with --users-db. Requires in-cluster execution + RBAC to get/create/patch the Secret")
	flag.StringVar(&s.usersDBSecretKey, "users-db-secret-key", "users.json", "Data key inside the Kubernetes Secret that holds the user-database JSON")
	flag.StringVar(&s.usersDBSecretNS, "users-db-secret-namespace", "", "Namespace of the Kubernetes Secret (defaults to the pod's own namespace)")
	flag.StringVar(&s.runnerCronJob, "runner-cronjob", defaultIfEmpty(os.Getenv("NCC_RUNNER_CRONJOB"), kubernetesRunnerCronJob), "Kubernetes only: name of the runner CronJob that Settings > Schedule reads and updates (the Helm chart sets ncc-orchestrator)")
	flag.BoolVar(&s.requireEncryptedUserStore, "require-encrypted-user-store", envBool("NCC_REQUIRE_ENCRYPTED_USER_STORE"), "Refuse to start unless the user store (password/PAT hashes, SAML SP key, LDAP bind password) is envelope-encrypted with a master key (NCC_MASTER_KEY / --users-db-key-file). Also via NCC_REQUIRE_ENCRYPTED_USER_STORE=true")
	flag.StringVar(&s.usersDBKeyFile, "users-db-key-file", "", "Path to a file holding a 32-byte master key (base64 or hex) to envelope-encrypt the user store at rest with AES-256-GCM. NCC_MASTER_KEY env takes precedence. Unset => plaintext (default). Keep this key off the protected disk/backup.")
	flag.BoolVar(&s.disableLocalAccounts, "disable-local-accounts", false, "Opt out of the default-on user database (no first-run admin bootstrap, login disabled). Use for pure token-only automation")
	flag.BoolVar(&s.adminSelfResetEnabled, "allow-admin-self-reset", envBool("NCC_ALLOW_ADMIN_SELF_RESET"), "Let the login page's anonymous forgot-password request self-reset the built-in admin (new password written to the .ncc-initial-admin-password file / Kubernetes Secret). Off by default because any network client can then rotate the admin password once a minute per IP; recover with 'ncc-orchestrator v2-reset-password --user admin' or --reset-admin on the host instead. Also via NCC_ALLOW_ADMIN_SELF_RESET=true")
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

	{
		logFile := strings.TrimSpace(s.serverLogPath)
		if logFile != "" && logFile != "-" {
			// Replicas sharing a volume each keep their own file, so rotation
			// never races between pods.
			if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
				if host, err := os.Hostname(); err == nil && host != "" {
					ext := filepath.Ext(logFile)
					logFile = strings.TrimSuffix(logFile, ext) + "-" + host + ext
				}
			}
			logFile = s.absPath(logFile)
		}
		if _, err := setupServerLogging(logFile, s.logLevel, s.logFormat, os.Stderr); err != nil {
			serverLog.Warn("logging setup", "error", err.Error())
		}
	}

	s.authToken = strings.TrimSpace(os.Getenv("NCC_API_TOKEN"))
	if s.authToken == "" {
		s.authToken = strings.TrimSpace(os.Getenv("NCC_API_STATIC_TOKEN"))
	} else if static := strings.TrimSpace(os.Getenv("NCC_API_STATIC_TOKEN")); static != "" && !secureCompare(static, s.authToken) {
		log.Fatal("NCC_API_STATIC_TOKEN must match NCC_API_TOKEN when both are set")
	}
	s.viewerToken = strings.TrimSpace(os.Getenv("NCC_API_VIEWER_TOKEN"))
	if s.viewerToken != "" && s.authToken != "" && secureCompare(s.viewerToken, s.authToken) {
		log.Fatal("NCC_API_VIEWER_TOKEN must differ from the admin NCC_API_TOKEN")
	}
	if env := strings.TrimSpace(os.Getenv("NCC_CORS_ORIGIN")); env != "" {
		s.corsOrigin = env
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
	if err := s.checkUserStoreEncryptionPolicy(storeBackend != nil); err != nil {
		log.Fatal(err)
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
			log.Printf("imported %s from seed %s into %s", plural.Count(db.count(), "local account"), filepath.Clean(s.usersFilePath), db.location())
		}
		if db.count() == 0 {
			pw, created, err := db.bootstrapAdminIfEmpty("admin")
			if err != nil {
				log.Fatalf("bootstrap admin: %v", err)
			}
			if created {
				hint := db.setInitialPassword("admin", pw)
				// Console only: the password must not reach the log file.
				fmt.Fprintf(os.Stderr, "==================================================================\n")
				fmt.Fprintf(os.Stderr, " FIRST-RUN ADMIN CREATED (store: %s)\n", db.location())
				fmt.Fprintf(os.Stderr, "   username: admin\n")
				fmt.Fprintf(os.Stderr, "   password: %s\n", pw)
				fmt.Fprintf(os.Stderr, "   You MUST change this password on first login.\n")
				if hint != "" {
					fmt.Fprintf(os.Stderr, "   retrieve it later via: %s\n", hint)
				}
				fmt.Fprintf(os.Stderr, "==================================================================\n")
				serverLog.Warn("first-run admin account created; its temporary password was printed to the console", "store", db.location(), "retrieve_hint", hint)
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
		log.Printf("loaded %s from %s (read-only)", plural.Count(db.count(), "local account"), filepath.Clean(s.usersFilePath))
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
		prov, _, err := buildLDAPRouter(&ldapCfg)
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
	if env := strings.TrimSpace(os.Getenv("NCC_TOKEN_EXPIRY")); env != "" {
		d, err := time.ParseDuration(env)
		if err != nil {
			log.Fatalf("NCC_TOKEN_EXPIRY: parse %q: %v", env, err)
		}
		s.sessionTTL = d
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
	s.initJWTSecret()
	if err := s.validatePathConfig(); err != nil {
		log.Fatal(err)
	}
	if s.rateLimitPerMinute > 0 {
		s.rateLimiter = newFixedWindowRateLimiter(s.rateLimitPerMinute, time.Minute)
	}
	s.loginGuard = newLoginGuard(s.loginLockThreshold, s.loginLockWindow, s.loginLockDuration)
	if s.loginGuard != nil {
		if s.loginLockAccountThreshold > 0 {
			s.loginGuard.accountThreshold = s.loginLockAccountThreshold
		}
		if s.loginLockSourceThreshold > 0 {
			s.loginGuard.sourceThreshold = s.loginLockSourceThreshold
		}
		statePath := strings.TrimSpace(s.loginGuardStateFile)
		if statePath == "" && s.capabilities.Kubernetes {
			// Replicas share the PVC: enforce limits cluster-wide.
			statePath = filepath.Join(s.stateRoot(), ".ncc-login-guard.json")
		}
		s.loginGuard.configureSharedState(statePath)
	}
	if err := configureTrustedProxies(s.trustedProxyCIDRs); err != nil {
		log.Fatalf("trusted-proxy-cidrs: %v", err)
	}
	s.ensureRunManager()
	s.startedAt = time.Now().UTC()
	s.auditForwarder = s.startAuditForwarder(context.Background())
	s.startSelfHealLoop(context.Background())
	s.ensureApplyUpdateWatcher()
	s.startUpgradeFollowUp()
	s.startBackupScheduleLoop(context.Background())
	s.startEnvironmentScheduleLoop(context.Background())
	s.startNotificationDigestLoop(context.Background())
	s.startUserDBReloadLoop(context.Background())
	s.startAlertLifecycleSyncLoop(context.Background())
	// Prime report artifacts without delaying API startup. Expensive log
	// summaries are refreshed asynchronously and served from the previous
	// value while a shared/NFS-backed directory is being scanned.
	go s.warmReportCache()

	handler := s.buildHandler()
	s.logStartup(listen)
	srv := &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadTimeout:       s.readTimeout,
		ReadHeaderTimeout: 10 * time.Second, // slowloris guard
		WriteTimeout:      s.writeTimeout,
		IdleTimeout:       s.idleTimeout,
		MaxHeaderBytes:    64 << 10,
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
		if err := graceful.Serve(srv, func() error { return srv.ListenAndServeTLS(s.tlsCertFile, s.tlsKeyFile) }, graceful.DefaultGrace); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := graceful.Serve(srv, srv.ListenAndServe, graceful.DefaultGrace); err != nil {
		log.Fatal(err)
	}
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
	mux.HandleFunc("/api/v1/health/ready", s.handleReady)
	mux.HandleFunc("/api/v1/tls/public", s.handlePublicTLS)
	mux.HandleFunc("/api/v1/components", s.handleComponents)
	mux.HandleFunc("/api/v1/alerts", s.handleAlerts)
	mux.HandleFunc("/api/v1/features", s.handleFeatureFlags)
	mux.HandleFunc("/api/v1/onboarding", s.handleOnboarding)
	mux.HandleFunc("/api/v1/alerts/ncc-dispositions", s.handleNCCDispositions)
	mux.HandleFunc("/api/v1/alerts/ncc", s.handleNCCAlerts)
	mux.HandleFunc("/api/v1/alerts/ncc/", s.handleNCCAlerts)
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
	// Stateless hybrid-auth aliases for the same self-service PAT store
	// (POST mints a hashed PAT; DELETE revokes by id). Reachable with JWT,
	// PAT, or static token so horizontally scaled replicas behave the same.
	mux.HandleFunc("/api/v1/users/me/pats", s.handleAuthTokens)
	mux.HandleFunc("/api/v1/users/me/pats/", s.handleAuthTokenByID)
	// Register SAML endpoints when SAML is active now or could be enabled at
	// runtime (a writable user db is present). Handlers 503 when no provider.
	if s.samlEnabled || s.users.writable() {
		s.registerSAML(mux)
	}
	// OIDC sign-in; handlers send the browser back to the login page with an
	// error code when OIDC is not configured.
	mux.HandleFunc(oidcLoginPath, s.handleOIDCLogin)
	mux.HandleFunc(oidcCallbackPath, s.handleOIDCCallback)
	// Admin-only account and SSO management (under /api/v1/settings/*). Only
	// meaningful when the store can persist changes.
	if s.users.writable() {
		mux.HandleFunc("/api/v1/settings/users", s.handleUsers)
		mux.HandleFunc("/api/v1/settings/users/", s.handleUserByName)
		mux.HandleFunc("/api/v1/settings/sso", s.handleSSO)
		mux.HandleFunc("/api/v1/settings/oidc", s.handleOIDCSettings)
		mux.HandleFunc("/api/v1/settings/oidc/test", s.handleOIDCTest)
		mux.HandleFunc("/api/v1/settings/ldap", s.handleLDAP)
		mux.HandleFunc("/api/v1/settings/ldap/test", s.handleLDAPTest)
		mux.HandleFunc("/api/v1/settings/ldap/search", s.handleLDAPSearch)
		mux.HandleFunc("/api/v1/settings/session", s.handleSessionPolicy)
		mux.HandleFunc("/api/v1/settings/ui-versions", s.handleUIVersionPolicy)
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
	mux.HandleFunc("/api/v1/settings/retention/preview", s.handleRetentionPreview)
	mux.HandleFunc("/api/v1/settings/backups/delete", s.handleBackupDelete)
	mux.HandleFunc("/api/v1/settings/backups/download", s.handleBackupDownloadNamed)
	mux.HandleFunc("/api/v1/settings/update", s.handleUpdateCheck)
	mux.HandleFunc("/api/v1/settings/update/apply", s.handleUpdateApply)
	mux.HandleFunc("/api/v1/settings/update/plan", s.handleUpdatePlan)
	mux.HandleFunc("/api/v1/settings/update/rollback", s.handleUpdateRollback)
	mux.HandleFunc("/api/v1/settings/config", s.handleConfig)
	mux.HandleFunc("/api/v1/settings/config/batch", s.handleConfigBatch)
	mux.HandleFunc("/api/v1/settings/configs", s.handleConfigsList)
	mux.HandleFunc("/api/v1/settings/config-files", s.handleConfigFiles)
	mux.HandleFunc("/api/v1/settings/config-file", s.handleConfigFile)
	mux.HandleFunc("/api/v1/settings/config-files/batch", s.handleConfigFilesBatch)
	mux.HandleFunc("/api/v1/settings/notifications", s.handleNotifications)
	mux.HandleFunc("/api/v1/settings/notifications/test", s.handleNotificationsTest)
	mux.HandleFunc("/api/v1/settings/notifications/status", s.handleNotificationsStatus)
	mux.HandleFunc("/api/v1/schedule", s.handleSchedule)
	mux.HandleFunc("/api/v1/schedule/health", s.handleScheduleHealth)
	mux.HandleFunc("/api/v1/freshness", s.handleFreshness)
	mux.HandleFunc("/api/v1/health/diagnostics", s.handleHealthDiagnostics)
	mux.HandleFunc("/api/v1/health/diagnostics/support-bundle", s.handleHealthSupportBundle)
	mux.HandleFunc("/api/v1/health/history", s.handleHealHistory)
	mux.HandleFunc("/api/v1/health/autoheal", s.handleAutoHeal)
	mux.HandleFunc("/api/v1/health/supervisor", s.handleSupervisor)
	mux.HandleFunc("/api/v1/health/supervisor/restart", s.handleSupervisorRestart)
	mux.HandleFunc("/api/v1/health/platform", s.handlePlatform)
	mux.HandleFunc("/api/v1/health/migrations/config", s.handleConfigMigration)
	mux.HandleFunc("/api/v1/artifacts", s.handleArtifacts)
	mux.HandleFunc("/api/v1/artifacts/", s.handleArtifactByName)
	mux.HandleFunc("/api/v1/runs", s.handleRuns)
	mux.HandleFunc("/api/v1/runs/", s.handleRunsRouter)
	mux.HandleFunc("/api/v1/jobs/", s.handleJobs)
	mux.HandleFunc("/api/v1/runs/summary", s.handleRunSummary)
	mux.HandleFunc("/api/v1/runs/active", s.handleRunActive)
	mux.HandleFunc("/api/v1/runs/config-preference", s.handleRunConfigPreference)
	mux.HandleFunc("/api/v1/environments", s.handleEnvironments)
	mux.HandleFunc("/api/v1/environment-schedules", s.handleEnvironmentSchedules)
	mux.HandleFunc("/api/v1/settings/environments", s.handleEnvironmentSettings)
	mux.HandleFunc("/api/v1/settings/environments/", s.handleEnvironmentSettings)
	mux.HandleFunc("/api/v1/runs/configs", s.handleConfigsList)
	mux.HandleFunc("/api/v1/runs/extra-args", s.handleRunExtraArgs)
	mux.HandleFunc("/api/v1/runs/preflight", s.handleRunPreflight)
	mux.HandleFunc("/api/v1/runs/trigger", s.handleRunTrigger)
	mux.HandleFunc("/api/v1/report/data", s.handleReportData)
	mux.HandleFunc("/api/v1/report/trends", s.handleReportTrends)
	mux.HandleFunc("/api/v1/logs/runner", s.handleRunnerLogs)
	mux.HandleFunc("/api/v1/logs", s.handleLogs)
	mux.HandleFunc("/api/v1/logs/", s.handleLogs)
	mux.HandleFunc("/api/v1/settings/logging", s.handleLoggingSettings)
	mux.HandleFunc("/api/v1/openapi.json", s.handleOpenAPI)
	mux.HandleFunc("/api/v1/meta/routes", s.handleMetaRoutes)
	mux.HandleFunc("/docs/assets/", s.handleSwaggerAsset)
	mux.HandleFunc("/", s.handleAPIDocsHome)

	return s.withRequestMeta(s.withCompression(s.withCORS(s.withRateLimit(withErrorCapture(s.withAuth(mux))))))
}
