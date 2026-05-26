package main

import (
	"bytes"
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
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Build-time metadata. These are set via -ldflags at link time, e.g.:
//
//	go build -ldflags "-X main.Version=2.0.0 -X main.BuildDate=2026-05-21T12:34:56Z \
//	  -X main.Stream=Release -X main.GoVersion=go1.22" ./cmd/ncc-api-server
//
// They are surfaced on /api/v1/health so support teams can see the exact build
// the API server is running.
var (
	Version   string
	BuildDate string
	Stream    string
	GoVersion string
)

func init() {
	if Version == "" {
		Version = "2.0.0"
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
}

type apiServer struct {
	repoRoot              string
	configPath            string
	outputDir             string
	logDir                string
	runnerLogPath         string
	scheduleStatePath     string
	notificationStatePath string
	auditLogPath          string
	auditLogMaxBytes      int64
	auditMu               sync.Mutex
	orchestratorBin       string
	authToken             string
	tokenFilePath         string
	corsOrigin            string
	authMode              string
	sessionSecret         string
	sessionTTL            time.Duration
	sessionIssuer         string
	runTimeout            time.Duration
	debugExpose           bool
	tlsCertFile           string
	tlsKeyFile            string
	tlsClientCAFile       string
	rateLimitPerMinute    int
	rateLimiter           *fixedWindowRateLimiter
	readTimeout           time.Duration
	writeTimeout          time.Duration
	idleTimeout           time.Duration

	mu      sync.Mutex
	active  bool
	started time.Time
	lastErr string
	lastOut string
	lastCfg string
	lastCmd []string
	lastCwd string
	lastEnv map[string]string
	liveOut *tailBuffer
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
}

type configUpdateRequest struct {
	Content string `json:"content"`
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
}

type runPreflightRequest struct {
	ConfigPath string `json:"config_path,omitempty"`
}

type artifactInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

type runInfo struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	ModTime  string `json:"mod_time"`
	HasIndex bool   `json:"has_index"`
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
	flag.StringVar(&s.notificationStatePath, "notifications-state-path", ".ncc-api-notifications.json", "Notifications state file path")
	flag.StringVar(&s.auditLogPath, "audit-log-path", "logs/ncc-audit.log", "JSONL audit log file path")
	flag.Int64Var(&s.auditLogMaxBytes, "audit-log-max-bytes", 5*1024*1024, "Audit log size before rotation (bytes); 0 disables rotation")
	flag.StringVar(&s.orchestratorBin, "orchestrator-bin", "./ncc-orchestrator", "Path to ncc-orchestrator binary")
	flag.StringVar(&s.tokenFilePath, "token-file-path", ".ncc-api-token", "Token file path for UI proxy/frontend use")
	flag.StringVar(&s.corsOrigin, "cors-origin", "http://localhost:8080", "CORS allowed origin(s), comma-separated")
	flag.StringVar(&s.authMode, "auth-mode", "token", "Auth mode: token, session, hybrid")
	flag.StringVar(&s.sessionSecret, "session-secret", "", "Session token HMAC secret (required for session/hybrid unless generated)")
	flag.DurationVar(&s.sessionTTL, "session-ttl", 10*time.Minute, "Session token TTL")
	flag.StringVar(&s.sessionIssuer, "session-issuer", "ncc-api-server", "Session token issuer")
	flag.DurationVar(&s.runTimeout, "run-timeout", 90*time.Minute, "Max runtime for trigger-run command")
	flag.BoolVar(&s.debugExpose, "debug-expose", false, "Expose debug internals in APIs (off by default)")
	flag.StringVar(&s.tlsCertFile, "tls-cert-file", "", "TLS certificate file for direct HTTPS")
	flag.StringVar(&s.tlsKeyFile, "tls-key-file", "", "TLS key file for direct HTTPS")
	flag.StringVar(&s.tlsClientCAFile, "tls-client-ca-file", "", "Optional client CA file (for mTLS verification)")
	flag.IntVar(&s.rateLimitPerMinute, "rate-limit-per-minute", 60, "Per-client rate limit for sensitive API routes (0 disables)")
	flag.DurationVar(&s.readTimeout, "read-timeout", 15*time.Second, "HTTP server read timeout")
	flag.DurationVar(&s.writeTimeout, "write-timeout", 60*time.Second, "HTTP server write timeout")
	flag.DurationVar(&s.idleTimeout, "idle-timeout", 60*time.Second, "HTTP server idle timeout")
	flag.Parse()

	s.authToken = strings.TrimSpace(os.Getenv("NCC_API_TOKEN"))
	if strings.Contains(s.corsOrigin, "*") {
		log.Fatal("wildcard cors-origin is not allowed in strict mode")
	}
	if s.authMode != "token" && s.authMode != "session" && s.authMode != "hybrid" {
		log.Fatal("auth-mode must be one of: token, session, hybrid")
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
	if (s.authMode == "session" || s.authMode == "hybrid") && strings.TrimSpace(s.sessionSecret) == "" {
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

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/audit", s.handleAudit)
	mux.HandleFunc("/api/v1/metrics/rate-limit", s.handleRateLimitMetrics)
	mux.HandleFunc("/api/v1/auth/session", s.handleAuthSession)
	mux.HandleFunc("/api/v1/auth/rotate", s.handleAuthRotate)
	mux.HandleFunc("/api/v1/settings/config", s.handleConfig)
	mux.HandleFunc("/api/v1/settings/config-files", s.handleConfigFiles)
	mux.HandleFunc("/api/v1/settings/config-file", s.handleConfigFile)
	mux.HandleFunc("/api/v1/settings/notifications", s.handleNotifications)
	mux.HandleFunc("/api/v1/settings/notifications/test", s.handleNotificationsTest)
	mux.HandleFunc("/api/v1/schedule", s.handleSchedule)
	mux.HandleFunc("/api/v1/schedule/health", s.handleScheduleHealth)
	mux.HandleFunc("/api/v1/artifacts", s.handleArtifacts)
	mux.HandleFunc("/api/v1/artifacts/", s.handleArtifactByName)
	mux.HandleFunc("/api/v1/runs", s.handleRuns)
	mux.HandleFunc("/api/v1/runs/summary", s.handleRunSummary)
	mux.HandleFunc("/api/v1/runs/active", s.handleRunActive)
	mux.HandleFunc("/api/v1/runs/preflight", s.handleRunPreflight)
	mux.HandleFunc("/api/v1/runs/trigger", s.handleRunTrigger)
	mux.HandleFunc("/api/v1/report/data", s.handleReportData)
	mux.HandleFunc("/api/v1/report/trends", s.handleReportTrends)
	mux.HandleFunc("/api/v1/logs/runner", s.handleRunnerLogs)
	mux.HandleFunc("/api/v1/openapi.json", s.handleOpenAPI)
	mux.HandleFunc("/api/v1/meta/routes", s.handleMetaRoutes)
	mux.HandleFunc("/", s.handleAPIDocsHome)

	handler := s.withCORS(s.withRateLimit(s.withAuth(mux)))
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
		if r.URL.Path == "/" || r.URL.Path == "/docs" || r.URL.Path == "/docs/ui" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/api/v1/auth/session" {
			next.ServeHTTP(w, r)
			return
		}
		tokenOK := false
		if s.authMode == "token" || s.authMode == "hybrid" {
			token := strings.TrimSpace(r.Header.Get("X-API-Token"))
			tokenOK = secureCompare(token, s.authToken)
		}
		sessionOK := false
		if !tokenOK && (s.authMode == "session" || s.authMode == "hybrid") {
			authz := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
				bearer := strings.TrimSpace(authz[len("Bearer "):])
				sessionOK = s.verifySessionToken(bearer, cleanClientIP(r)) == nil
			}
		}
		if !tokenOK && !sessionOK {
			writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *apiServer) withCORS(next http.Handler) http.Handler {
	allowedOrigins := parseAllowedOrigins(s.corsOrigin)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			if _, ok := allowedOrigins[origin]; !ok {
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
		if r.TLS != nil {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Token, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := map[string]interface{}{
		"status":           "ok",
		"time":             time.Now().UTC().Format(time.RFC3339),
		"auth_mode":        s.authMode,
		"token_source":     tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN")),
		"config_path":      s.absPath(s.configPath),
		"output_dir":       s.absPath(s.outputDir),
		"log_dir":          s.absPath(s.logDir),
		"token_file":       s.absPath(s.tokenFilePath),
		"orchestrator_bin": s.absPath(s.orchestratorBin),
		"version":          Version,
		"build_date":       BuildDate,
		"stream":           Stream,
		"go_version":       GoVersion,
		"os":               runtime.GOOS,
		"arch":             runtime.GOARCH,
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
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	actionPrefix := strings.TrimSpace(q.Get("action"))
	onlyFailures := q.Get("failures") == "1" || strings.EqualFold(q.Get("failures"), "true")

	entries, err := s.auditEntries(limit, actionPrefix, onlyFailures)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "read audit log: " + err.Error()})
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
		"path":       abs,
		"size":       size,
		"mod_time":   modTime,
		"limit":      limit,
		"count":      len(entries),
		"max_bytes":  s.auditLogMaxBytes,
		"entries":    entries,
		"filters": map[string]interface{}{
			"action":   actionPrefix,
			"failures": onlyFailures,
		},
	}})
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
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div style="margin:10px 16px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif">
    <a href="/">Back to API docs</a>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
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
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "session bootstrap allowed only from loopback"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	token, exp, err := s.issueSessionToken(cleanClientIP(r))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
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
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: fmt.Sprintf("generate token: %v", err)})
		return
	}
	s.authToken = base64.RawURLEncoding.EncodeToString(b)
	if err := s.ensureAuthToken(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "token rotated"})
}

func (s *apiServer) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cfgPath, err := s.validateConfigPath(s.configPath)
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
		cfgPath, err := s.validateConfigPath(s.configPath)
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
		if _, err := s.validateConfigPath(st.Config); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.saveSchedule(st); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		resp := map[string]interface{}{"schedule": st, "applied": false}
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
			resp["output"] = out
			if err != nil {
				resp["apply_error"] = err.Error()
			}
		}
		s.audit(r, "schedule.update", true, map[string]interface{}{"applied": req.Apply, "type": st.Type, "action": st.Action})
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
	st, err := s.loadSchedule()
	saved := false
	stateFileExists := true
	if err != nil {
		stateFileExists = false
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"configured":        false,
			"saved":             false,
			"installed":         false,
			"state_file_exists": stateFileExists,
			"error":             err.Error(),
		}})
		return
	}
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
	data := map[string]interface{}{
		"configured":        installed, // authoritative: schedule is actually installed in OS
		"saved":             saved,
		"installed":         installed,
		"state_file_exists": stateFileExists,
		"task_name":         taskName,
		"type":              st.Type,
		"action":            st.Action,
		"with_lock":         st.WithLock,
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
	return "crontab (via orchestrator)"
}

func (s *apiServer) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
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

func (s *apiServer) handleArtifactByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
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
	b, err := os.ReadFile(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	if r.URL.Query().Get("download") == "1" {
		ct := mime.TypeByExtension(filepath.Ext(name))
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
		_, _ = w.Write(b)
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"name":    name,
		"content": string(b),
	}})
}

func (s *apiServer) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	runsDir := filepath.Join(s.absPath(s.outputDir), "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: []runInfo{}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	out := make([]runInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		runPath := filepath.Join(runsDir, e.Name())
		_, hasIndexErr := os.Stat(filepath.Join(runPath, "index.html"))
		out = append(out, runInfo{
			ID:       e.Name(),
			Path:     runPath,
			ModTime:  info.ModTime().UTC().Format(time.RFC3339),
			HasIndex: hasIndexErr == nil,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModTime > out[j].ModTime })
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
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
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: raw})
}

func (s *apiServer) handleRunActive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := map[string]interface{}{
		"active":      s.active,
		"started_at":  s.started.UTC().Format(time.RFC3339),
		"last_error":  s.lastErr,
		"last_output": s.lastOut,
		"live_output": s.currentLiveOutput(),
		"runner_log":  s.absPath(s.runnerLogPath),
		"output_dir":  s.absPath(s.outputDir),
		"config_path": defaultIfEmpty(s.lastCfg, s.absPath(s.configPath)),
	}
	if s.debugExpose {
		data["command"] = s.lastCmd
		data["work_dir"] = s.lastCwd
		data["env"] = s.lastEnv
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
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
	cfgPath := strings.TrimSpace(req.ConfigPath)
	if cfgPath == "" {
		cfgPath = s.configPath
	}
	resolvedCfgPath, err := s.validateConfigPath(cfgPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	out, err := s.runOrchestrator([]string{"preflight-check", "--config", resolvedCfgPath, "--format", "json"}, 120*time.Second)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   fmt.Sprintf("preflight-check failed: %v", err),
			Data:    map[string]string{"output": tailString(strings.TrimSpace(out), 4000)},
		})
		return
	}
	var payload map[string]interface{}
	if uerr := json.Unmarshal([]byte(strings.TrimSpace(out)), &payload); uerr != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{
			Success: false,
			Error:   fmt.Sprintf("parse preflight-check output failed: %v", uerr),
			Data:    map[string]string{"output": tailString(strings.TrimSpace(out), 4000)},
		})
		return
	}
	s.audit(r, "runs.preflight", true, map[string]interface{}{"config_path": resolvedCfgPath})
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
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "a run is already active"})
		return
	}
	s.active = true
	s.started = time.Now().UTC()
	s.lastErr = ""
	s.lastOut = ""
	s.lastCmd = nil
	s.lastCwd = ""
	s.lastEnv = nil
	s.liveOut = nil
	s.mu.Unlock()

	var req runTriggerRequest
	if err := decodeJSON(r.Body, &req); err != nil && !errors.Is(err, io.EOF) {
		s.setRunDone(err, "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	cfgPath := s.configPath
	if strings.TrimSpace(req.ConfigPath) != "" {
		cfgPath = strings.TrimSpace(req.ConfigPath)
	}
	resolvedCfgPath, err := s.validateConfigPath(cfgPath)
	if err != nil {
		s.setRunDone(err, "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if len(req.Password) > 256 {
		s.setRunDone(errors.New("password too long"), "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "password too long"})
		return
	}
	cleanExtraArgs, err := sanitizeExtraArgs(req.ExtraArgs)
	if err != nil {
		s.setRunDone(err, "")
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if st, err := os.Stat(resolvedCfgPath); err != nil || st.IsDir() {
		s.setRunDone(fmt.Errorf("config not found or invalid: %s", cfgPath), "")
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
	args := []string{"--config", resolvedCfgPath}
	args = append(args, cleanExtraArgs...)
	s.mu.Lock()
	s.lastCfg = resolvedCfgPath
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), s.runTimeout)
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	injectedEnv := map[string]string{}
	if strings.TrimSpace(req.Password) != "" {
		cmd.Env = append(os.Environ(), "NCC_PASSWORD="+req.Password)
		injectedEnv["NCC_PASSWORD"] = "***"
	}
	fullCmd := append(s.orchestratorBaseCommand(), redactedArgs(args)...)
	var runOut bytes.Buffer
	liveBuf := newTailBuffer(64000)
	mw := io.MultiWriter(&runOut, liveBuf)
	cmd.Stdout = mw
	cmd.Stderr = mw
	s.mu.Lock()
	s.lastCmd = fullCmd
	s.lastCwd = cmd.Dir
	s.lastEnv = injectedEnv
	s.liveOut = liveBuf
	s.mu.Unlock()
	if err := cmd.Start(); err != nil {
		cancel()
		s.setRunDone(err, "")
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	pid := cmd.Process.Pid
	go func() {
		defer cancel()
		err := cmd.Wait()
		if ctx.Err() == context.DeadlineExceeded {
			err = fmt.Errorf("run timed out after %s", s.runTimeout)
		}
		s.setRunDone(err, runOut.String())
		go s.notifyRunFinished(err)
	}()
	s.audit(r, "runs.trigger", true, map[string]interface{}{"config_path": resolvedCfgPath, "extra_args_count": len(cleanExtraArgs)})
	writeJSON(w, http.StatusAccepted, envelope{Success: true, Message: "run triggered", Data: map[string]interface{}{
		"pid":           pid,
		"command":       append(s.orchestratorBaseCommand(), redactedArgs(args)...),
		"started_at":    s.started.Format(time.RFC3339),
		"config_path":   resolvedCfgPath,
		"used_password": strings.TrimSpace(req.Password) != "",
	}})
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
	outDir := s.selectBestReportOutDir()
	runSummary := readJSONArtifact(filepath.Join(outDir, "run-summary.json"), map[string]interface{}{})
	checksSnapshot := readJSONArtifact(filepath.Join(outDir, "checks-snapshot.json"), []interface{}{})
	drilldownDiff := readJSONArtifact(filepath.Join(outDir, "drilldown-diff.json"), map[string]interface{}{})
	flakyChecks := readJSONArtifact(filepath.Join(outDir, "flaky-checks.json"), map[string]interface{}{})
	regressionSummary := readJSONArtifact(filepath.Join(outDir, "regression-summary.json"), map[string]interface{}{})
	sloDashboard := readJSONArtifact(filepath.Join(outDir, "slo-dashboard.json"), map[string]interface{}{})
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
	aggRows := readInlineJSONVar(filepath.Join(outDir, "index.html"), "AGG", []interface{}{})
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
		"run_summary":        runSummary,
		"checks_snapshot":    checksSnapshot,
		"drilldown_diff":     drilldownDiff,
		"flaky_checks":       flakyChecks,
		"regression_summary": regressionSummary,
		"slo_dashboard":      sloDashboard,
		"policy_violations":  policyViolations,
		"agg_rows":           aggRows,
		"diff_flags":         readInlineJSONVar(filepath.Join(outDir, "index.html"), "DIFF_FLAGS", map[string]interface{}{}),
		"flaky_keys":         readInlineJSONVar(filepath.Join(outDir, "index.html"), "FLAKY_KEYS", map[string]interface{}{}),
		"cluster_links":      readInlineJSONVar(filepath.Join(outDir, "index.html"), "CLUSTER_LINKS", []interface{}{}),
		"artifact_links":     readInlineJSONVar(filepath.Join(outDir, "index.html"), "ARTIFACT_LINKS", map[string]interface{}{}),
		"report_meta":        readInlineJSONVar(filepath.Join(outDir, "index.html"), "REPORT_META", map[string]interface{}{}),
		"ncc_logs":           listNCCLogs(s.absPath(s.logDir)),
		"ncc_summary_counts": parseNCCSummaryCounts(s.absPath(s.logDir)),
		"ncc_cluster_summary": parseNCCClusterSummary(s.absPath(s.logDir)),
		"trends":             collectTrendPoints(outDir, 30),
		"report_source_dir":  outDir,
		"pagination":         pagination,
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
	routes := []routeMeta{
		{Path: "/api/v1/health", Methods: []string{http.MethodGet}, Description: "Backend health, version, and resolved paths"},
		{Path: "/api/v1/audit", Methods: []string{http.MethodGet}, Description: "Read recent audit log entries (limit, action, failures filters)"},
		{Path: "/api/v1/metrics/rate-limit", Methods: []string{http.MethodGet}, Description: "Rate limiter configuration and counters"},
		{Path: "/api/v1/auth/session", Methods: []string{http.MethodPost}, Description: "Issue short-lived session token"},
		{Path: "/api/v1/auth/rotate", Methods: []string{http.MethodPost}, Description: "Rotate API token"},
		{Path: "/api/v1/settings/config", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write runtime config", SampleBody: "{\n  \"content\": \"clusters: \\\"10.0.0.1\\\"\\nusername: \\\"admin\\\"\\n\"\n}"},
		{Path: "/api/v1/settings/config-files", Methods: []string{http.MethodGet}, Description: "List config-referenced files (clusters, exclusions, secrets)"},
		{Path: "/api/v1/settings/config-file", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write one config-referenced file", SampleBody: "{\n  \"path\": \"alerts-exclude.txt\",\n  \"content\": \"AHV_MemoryUsage\\n\"\n}"},
		{Path: "/api/v1/settings/notifications", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write notifications state", SampleBody: "{\n  \"enabled\": true,\n  \"channel\": \"webhook\"\n}"},
		{Path: "/api/v1/settings/notifications/test", Methods: []string{http.MethodPost}, Description: "Send test notification(s)", SampleBody: "{\n  \"channel\": \"all\"\n}"},
		{Path: "/api/v1/schedule", Methods: []string{http.MethodGet, http.MethodPut}, Description: "Read/write scheduler state", SampleBody: "{\n  \"type\": \"cron\",\n  \"action\": \"create\",\n  \"cron\": \"15 */4 * * *\",\n  \"config\": \"config.yaml\",\n  \"print_only\": true,\n  \"apply\": false\n}"},
		{Path: "/api/v1/schedule/health", Methods: []string{http.MethodGet}, Description: "Scheduler health snapshot (last run/success/error hints)"},
		{Path: "/api/v1/artifacts", Methods: []string{http.MethodGet}, Description: "List available artifacts"},
		{Path: "/api/v1/artifacts/{name}", Methods: []string{http.MethodGet}, Description: "Read artifact by name"},
		{Path: "/api/v1/runs", Methods: []string{http.MethodGet}, Description: "List historical runs"},
		{Path: "/api/v1/runs/summary", Methods: []string{http.MethodGet}, Description: "Read latest run summary"},
		{Path: "/api/v1/runs/active", Methods: []string{http.MethodGet}, Description: "Read active run state"},
		{Path: "/api/v1/runs/preflight", Methods: []string{http.MethodPost}, Description: "Run preflight checks (config/secrets/path permissions)", SampleBody: "{\n  \"config_path\": \"config.yaml\"\n}"},
		{Path: "/api/v1/runs/trigger", Methods: []string{http.MethodPost}, Description: "Trigger orchestrator run", SampleBody: "{\n  \"config_path\": \"config.yaml\",\n  \"password\": \"\",\n  \"extra_args\": [\"--no-html\"]\n}"},
		{Path: "/api/v1/report/data", Methods: []string{http.MethodGet}, Description: "Aggregated report payload (supports optional limit/offset pagination for large arrays)"},
		{Path: "/api/v1/report/trends", Methods: []string{http.MethodGet}, Description: "Historical trends from run summaries"},
		{Path: "/api/v1/logs/runner", Methods: []string{http.MethodGet}, Description: "Read tail of runner log"},
		{Path: "/api/v1/openapi.json", Methods: []string{http.MethodGet}, Description: "OpenAPI 3.0 specification"},
		{Path: "/api/v1/meta/routes", Methods: []string{http.MethodGet}, Description: "List available REST routes for API explorer"},
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"routes": routes,
		"count":  len(routes),
	}})
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

func (s *apiServer) buildOpenAPISpec() map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.3",
		"info": map[string]interface{}{
			"title":       "NCC Orchestrator API",
			"version":     "2.0.0",
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
				"get": map[string]interface{}{"summary": "Recent audit log entries (filters: limit, action, failures)"},
			},
			"/api/v1/metrics/rate-limit": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Rate limiter configuration and counters"},
			},
			"/api/v1/auth/session": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Issue short-lived session token"},
			},
			"/api/v1/auth/rotate": map[string]interface{}{
				"post": map[string]interface{}{"summary": "Rotate API token"},
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
				"get": map[string]interface{}{"summary": "List historical runs"},
			},
			"/api/v1/runs/summary": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read latest run summary"},
			},
			"/api/v1/runs/active": map[string]interface{}{
				"get": map[string]interface{}{"summary": "Read active run state"},
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
					"summary": "Trigger orchestrator run",
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

func (s *apiServer) setRunDone(err error, output string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
	s.lastOut = tailString(strings.TrimSpace(output), 4000)
	s.liveOut = nil
	if err != nil {
		if s.lastOut != "" {
			s.lastErr = fmt.Sprintf("%s\n%s", err.Error(), s.lastOut)
			return
		}
		s.lastErr = err.Error()
		return
	}
	s.lastErr = ""
}

func (s *apiServer) runOrchestrator(args []string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
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
	if ctx == nil {
		return exec.Command(name, rest...)
	}
	return exec.CommandContext(ctx, name, rest...)
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

func readInlineJSONVar(path, varName string, fallback interface{}) interface{} {
	b, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	pattern := `(?s)(?:const|var)\s+` + regexp.QuoteMeta(varName) + `\s*=\s*(.*?);`
	re := regexp.MustCompile(pattern)
	m := re.FindSubmatch(b)
	if len(m) < 2 {
		return fallback
	}
	raw := strings.TrimSpace(string(m[1]))
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

func parseNCCSummaryCounts(logDir string) map[string]int {
	totals := map[string]int{
		"fail":          0,
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
