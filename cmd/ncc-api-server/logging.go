package main

// Server log for ncc-api-server.
//
// Every process line goes to two places:
//   - stderr, as readable text (console, journald, `kubectl logs`, and the
//     detached-mode logs/v2-api.log capture), and
//   - logs/ncc-api.log, as JSON lines rotated by size, so a problem can be
//     traced afterwards by request ID, user, run ID or subsystem.
//
// The standard library `log` package is routed through the same handler, so
// older log.Printf call sites land in both places too. Values under keys
// that look like secrets are replaced before anything is written.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// serverLogLevel is shared by every handler so the level can change at run
// time (Settings → Logs, or PUT /api/v1/settings/logging).
var serverLogLevel = new(slog.LevelVar)

// serverLog is the process logger; slog.Default() points at it after setup.
var serverLog = slog.Default()

const (
	defaultServerLogPath = "logs/ncc-api.log"
	slowRequestThreshold = 3 * time.Second
)

// parseLogLevel accepts debug, info, warn/warning and error (any case).
func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "trace":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return slog.LevelInfo, fmt.Errorf("unknown log level %q (use debug, info, warn or error)", s)
}

func logLevelName(l slog.Level) string {
	switch {
	case l <= slog.LevelDebug:
		return "debug"
	case l <= slog.LevelInfo:
		return "info"
	case l <= slog.LevelWarn:
		return "warn"
	}
	return "error"
}

// secretKey matches attribute keys whose values must never be written.
var secretKey = regexp.MustCompile(`(?i)(pass(word|wd)?|secret|token|authorization|cookie|api[_-]?key|private[_-]?key|credential|session[_-]?id|bind[_-]?pw)`)

// keysThatLookSecretButAreNot are identifiers, counts or flags, not values.
var keysThatLookSecretButAreNot = map[string]bool{
	"token_id": true, "token_name": true, "tokens": true, "token_count": true, "token_file": true,
	"password_set": true, "password_changed": true, "must_change_password": true, "has_password": true,
	"auth_mode": true, "secret_name": true, "secrets_provider": true, "credential_source": true,
}

var bearerValue = regexp.MustCompile(`(?i)\b(bearer|basic|splunk)\s+[A-Za-z0-9._~+/=-]{6,}`)

func isSecretKey(k string) bool {
	k = strings.ToLower(k)
	return !keysThatLookSecretButAreNot[k] && secretKey.MatchString(k)
}

// redactString hides credentials embedded in free text: Authorization
// values and passwords in URLs.
func redactString(s string) string {
	s = bearerValue.ReplaceAllString(s, "$1 [redacted]")
	if strings.Contains(s, "://") && strings.Contains(s, "@") {
		s = reURLUserinfo.ReplaceAllString(s, "://[redacted]@")
	}
	return s
}

var reURLUserinfo = regexp.MustCompile(`://[^/\s:@]+:[^/\s@]+@`)

var reURLPath = regexp.MustCompile(`(https?://[^/\s"'?]+)[^\s"']*`)

// redactURLPaths keeps only scheme and host of URLs in s. For errors that may
// carry webhook URLs, whose path is the secret (Slack, Teams, …).
func redactURLPaths(s string) string {
	return reURLPath.ReplaceAllString(s, "$1/…")
}

func redactAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindString {
		if isSecretKey(a.Key) && a.Value.String() != "" {
			return slog.String(a.Key, "[redacted]")
		}
		if v := a.Value.String(); strings.ContainsAny(v, "@ ") {
			return slog.String(a.Key, redactString(v))
		}
	} else if isSecretKey(a.Key) && a.Value.Kind() != slog.KindBool && a.Value.Kind() != slog.KindInt64 && a.Value.Kind() != slog.KindGroup {
		return slog.String(a.Key, "[redacted]")
	}
	return a
}

// redactQuery keeps a query string readable while hiding secret values.
func redactQuery(raw string) string {
	if raw == "" {
		return ""
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return "[unparsable]"
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := strings.Join(q[k], ",")
		if isSecretKey(k) || k == "code" || k == "state" || k == "SAMLResponse" {
			v = "[redacted]"
		} else if len(v) > 120 {
			v = v[:120] + "…"
		}
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, "&")
}

// fanoutHandler writes each record to every handler that wants it.
type fanoutHandler struct{ hs []slog.Handler }

func (f fanoutHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f.hs {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f.hs {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (f fanoutHandler) WithAttrs(as []slog.Attr) slog.Handler {
	out := make([]slog.Handler, len(f.hs))
	for i, h := range f.hs {
		out[i] = h.WithAttrs(as)
	}
	return fanoutHandler{out}
}

func (f fanoutHandler) WithGroup(name string) slog.Handler {
	out := make([]slog.Handler, len(f.hs))
	for i, h := range f.hs {
		out[i] = h.WithGroup(name)
	}
	return fanoutHandler{out}
}

// requestContextHandler adds the request ID, user and run ID carried by the
// context to every record logged with that context.
type requestContextHandler struct{ slog.Handler }

func (h requestContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if id := requestIDFromContext(ctx); id != "" && !recordHas(r, "request_id") {
			r.AddAttrs(slog.String("request_id", id))
		}
		if info := requestLogInfoFrom(ctx); info != nil && !recordHas(r, "user") {
			if u, _ := info.principal(); u != "" {
				r.AddAttrs(slog.String("user", u))
			}
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h requestContextHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return requestContextHandler{h.Handler.WithAttrs(as)}
}

func (h requestContextHandler) WithGroup(name string) slog.Handler {
	return requestContextHandler{h.Handler.WithGroup(name)}
}

func recordHas(r slog.Record, key string) bool {
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = true
			return false
		}
		return true
	})
	return found
}

// serverLogFile is the rotating writer behind logs/ncc-api.log (nil when the
// file log is off).
var serverLogFile *lumberjack.Logger

// setupServerLogging installs the process logger. filePath "" or "-" keeps
// logging to stderr only. Errors opening the file are reported and logging
// continues on stderr.
func setupServerLogging(filePath, level, format string, stderr io.Writer) (string, error) {
	lvl, lvlErr := parseLogLevel(level)
	serverLogLevel.Set(lvl)
	opts := &slog.HandlerOptions{Level: serverLogLevel, ReplaceAttr: redactAttr}
	var console slog.Handler
	if strings.EqualFold(strings.TrimSpace(format), "json") {
		console = slog.NewJSONHandler(stderr, opts)
	} else {
		console = slog.NewTextHandler(stderr, opts)
	}
	handlers := []slog.Handler{console}
	resolved := ""
	var fileErr error
	if p := strings.TrimSpace(filePath); p != "" && p != "-" {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			fileErr = fmt.Errorf("server log %s: %w", p, err)
		} else {
			serverLogFile = &lumberjack.Logger{Filename: p, MaxSize: 50, MaxBackups: 10, MaxAge: 30, Compress: true}
			fileOpts := &slog.HandlerOptions{Level: serverLogLevel, ReplaceAttr: redactAttr, AddSource: true}
			handlers = append(handlers, slog.NewJSONHandler(serverLogFile, fileOpts))
			resolved = p
		}
	}
	host, _ := os.Hostname()
	logger := slog.New(requestContextHandler{fanoutHandler{handlers}}).With(
		slog.String("component", "api-server"),
		slog.Int("pid", os.Getpid()),
		slog.String("host", host),
	)
	serverLog = logger
	slog.SetDefault(logger)
	// Older log.Printf call sites: pick a level from the wording, so
	// warnings and failures are not filed as INFO.
	log.SetFlags(0)
	log.SetOutput(stdlogBridge{})
	return resolved, errors.Join(lvlErr, fileErr)
}

// logStartup records how this process was started: version, paths, modes and
// the non-secret flags, so a log file alone answers "what was running".
func (s *apiServer) logStartup(listen string) {
	attrs := []any{
		"version", Version,
		"go", runtime.Version(),
		"os", runtime.GOOS, "arch", runtime.GOARCH,
		"listen", listen,
		"repo_root", s.repoRoot,
		"config_path", s.configPath,
		"output_dir", s.outputDir,
		"runner_log", s.runnerLogPath,
		"audit_log", s.auditLogPath,
		"auth_mode", s.authMode,
		"login_enabled", s.loginEnabled(),
		"kubernetes", s.capabilities.Kubernetes,
		"log_level", logLevelName(serverLogLevel.Level()),
		"rate_limit_per_minute", s.rateLimitPerMinute,
		"session_ttl", s.sessionTTL.String(),
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, st := range bi.Settings {
			if st.Key == "vcs.revision" {
				attrs = append(attrs, "git_revision", st.Value)
			}
		}
	}
	serverLog.Info("api-server starting", attrs...)
}

// stdlogBridge receives lines from the standard log package.
type stdlogBridge struct{}

var (
	reStdlogError = regexp.MustCompile(`(?i)\b(error|failed|fail|cannot|can't|could not|unable|panic|fatal)\b`)
	reStdlogWarn  = regexp.MustCompile(`(?i)\b(warn|warning|unavailable|ignored|suppressed|skipped|retry|retrying|ephemeral)\b`)
)

func (stdlogBridge) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\r\n")
	lvl := slog.LevelInfo
	switch {
	case reStdlogError.MatchString(msg):
		lvl = slog.LevelError
	case reStdlogWarn.MatchString(msg):
		lvl = slog.LevelWarn
	}
	serverLog.Log(context.Background(), lvl, redactString(msg), "source_kind", "stdlog")
	return len(p), nil
}

// ---- per-request log ----

// requestLogInfo is filled in while a request is handled (by withAuth and the
// error capture below) and read by the request log when it completes.
type requestLogInfo struct {
	mu      sync.Mutex
	user    string
	role    string
	method  string
	errMsg  string
	errCode string
	attrs   []any
}

type requestLogInfoKey struct{}

func requestLogInfoFrom(ctx context.Context) *requestLogInfo {
	v, _ := ctx.Value(requestLogInfoKey{}).(*requestLogInfo)
	return v
}

func (i *requestLogInfo) principal() (string, string) {
	if i == nil {
		return "", ""
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.user, i.role
}

func (i *requestLogInfo) setPrincipal(p principal) {
	if i == nil {
		return
	}
	i.mu.Lock()
	i.user, i.role, i.method = p.subject, p.role.String(), p.method.String()
	i.mu.Unlock()
}

// addRequestLogAttrs attaches extra fields (run_id, environment, …) to the
// request's log line.
func addRequestLogAttrs(r *http.Request, kv ...any) {
	if i := requestLogInfoFrom(r.Context()); i != nil {
		i.mu.Lock()
		i.attrs = append(i.attrs, kv...)
		i.mu.Unlock()
	}
}

// quietPaths are polled by the UI every few seconds; successful calls are
// logged at DEBUG so they don't bury everything else at INFO.
var quietPaths = map[string]bool{
	"/api/v1/health":                true,
	"/api/v1/health/ready":          true,
	"/metrics":                      true,
	"/api/v1/runs/active":           true,
	"/api/v1/auth/me":               true,
	"/api/v1/auth/session":          true,
	"/api/v1/auth/refresh":          true,
	"/api/v1/logs":                  true,
	"/api/v1/schedule/health":       true,
	"/api/v1/environment-schedules": true,
	"/api/v1/features":              true,
	"/api/v1/tls/public":            true,
}

func isQuietPath(p string) bool {
	p = strings.TrimSuffix(p, "/")
	return quietPaths[p] || strings.HasPrefix(p, "/api/v1/logs/") || strings.HasPrefix(p, "/api/v1/jobs/")
}

// logRequest writes one line per completed request.
func logRequest(r *http.Request, info *requestLogInfo, status int, bytes int64, d time.Duration) {
	lvl := slog.LevelInfo
	switch {
	case status >= 500:
		lvl = slog.LevelError
	case status >= 400:
		lvl = slog.LevelWarn
	case d >= slowRequestThreshold:
		lvl = slog.LevelWarn
	case r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions:
		if isQuietPath(r.URL.Path) {
			lvl = slog.LevelDebug
		}
	}
	ctx := r.Context()
	if !serverLog.Enabled(ctx, lvl) {
		return
	}
	attrs := []any{
		"method", r.Method,
		"path", r.URL.Path,
		"route", routeLabel(r.URL.Path),
		"status", status,
		"duration_ms", float64(d.Microseconds()) / 1000,
		"bytes", bytes,
		"client_ip", cleanClientIP(r),
	}
	if q := redactQuery(r.URL.RawQuery); q != "" {
		attrs = append(attrs, "query", q)
	}
	if r.ContentLength > 0 {
		attrs = append(attrs, "request_bytes", r.ContentLength)
	}
	if ua := strings.TrimSpace(r.UserAgent()); ua != "" {
		if len(ua) > 160 {
			ua = ua[:160]
		}
		attrs = append(attrs, "user_agent", ua)
	}
	if d >= slowRequestThreshold {
		attrs = append(attrs, "slow", true)
	}
	if info != nil {
		info.mu.Lock()
		if info.user != "" {
			attrs = append(attrs, "user", info.user, "role", info.role, "auth", info.method)
		}
		if info.errMsg != "" {
			attrs = append(attrs, "error", info.errMsg)
		}
		if info.errCode != "" {
			attrs = append(attrs, "error_code", info.errCode)
		}
		attrs = append(attrs, info.attrs...)
		info.mu.Unlock()
	}
	serverLog.Log(ctx, lvl, "http request", attrs...)
}

// errorCaptureWriter keeps the error text of a failed JSON response so the
// request log can say why it failed, not just that it did.
type errorCaptureWriter struct {
	http.ResponseWriter
	info   *requestLogInfo
	status int
	buf    []byte
}

const errorCaptureLimit = 4096

func (w *errorCaptureWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *errorCaptureWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.status >= 400 && len(w.buf) < errorCaptureLimit {
		n := errorCaptureLimit - len(w.buf)
		if n > len(b) {
			n = len(b)
		}
		w.buf = append(w.buf, b[:n]...)
	}
	return w.ResponseWriter.Write(b)
}

func (w *errorCaptureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *errorCaptureWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *errorCaptureWriter) record() {
	if w.info == nil || w.status < 400 || len(w.buf) == 0 {
		return
	}
	var env struct {
		Error     string `json:"error"`
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
	}
	msg, code := "", ""
	if json.Unmarshal(w.buf, &env) == nil {
		msg, code = env.Error, env.ErrorCode
		if msg == "" {
			msg = env.Message
		}
	} else {
		msg = strings.TrimSpace(string(w.buf))
	}
	if len(msg) > 500 {
		msg = msg[:500] + "…"
	}
	w.info.mu.Lock()
	if w.info.errMsg == "" {
		w.info.errMsg = redactString(msg)
	}
	if w.info.errCode == "" {
		w.info.errCode = code
	}
	w.info.mu.Unlock()
}

// withErrorCapture sits just outside the router (inside auth and compression)
// so it sees handler responses uncompressed.
func withErrorCapture(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := requestLogInfoFrom(r.Context())
		if info == nil {
			next.ServeHTTP(w, r)
			return
		}
		cw := &errorCaptureWriter{ResponseWriter: w, info: info}
		defer cw.record()
		next.ServeHTTP(cw, r)
	})
}

// recoverPanic turns a handler panic into a logged 500 with the stack, instead
// of net/http's bare stderr dump and a dropped connection.
func recoverPanic(w http.ResponseWriter, r *http.Request, rec *statusRecorder) {
	v := recover()
	if v == nil {
		return
	}
	if v == http.ErrAbortHandler {
		panic(v)
	}
	serverLog.ErrorContext(r.Context(), "handler panic",
		"panic", fmt.Sprint(v),
		"method", r.Method,
		"path", r.URL.Path,
		"stack", string(debug.Stack()),
	)
	if info := requestLogInfoFrom(r.Context()); info != nil {
		info.mu.Lock()
		info.errMsg = fmt.Sprintf("panic: %v", v)
		info.errCode = "NCC_API_INTERNAL"
		info.mu.Unlock()
	}
	if rec.status == 0 {
		writeJSON(rec, http.StatusInternalServerError, envelope{Success: false, Error: "internal error; see the server log for request " + requestIDFromContext(r.Context()), ErrorCode: "NCC_API_INTERNAL"})
	}
}

// ---- log files for the UI ----

// logSource is one log the Logs page can show.
type logSource struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
	// Format is "json" (one JSON object per line) or "text".
	Format string   `json:"format"`
	Files  []string `json:"files"`
	Admin  bool     `json:"admin_only"`
	// patterns are repo-relative globs; the first match that exists wins as
	// the default file.
	patterns []string
}

func (s *apiServer) logSources() []logSource {
	logsDir := filepath.Dir(s.runnerLogPath)
	if strings.TrimSpace(logsDir) == "" || logsDir == "." {
		logsDir = "logs"
	}
	serverPattern := s.serverLogPath
	if strings.TrimSpace(serverPattern) == "" || serverPattern == "-" {
		serverPattern = defaultServerLogPath
	}
	ext := filepath.Ext(serverPattern)
	serverGlob := strings.TrimSuffix(serverPattern, ext) + "*" + ext
	sources := []logSource{
		{ID: "runner", Label: "Runner", Description: "NCC runs: cluster connections, checks, reports and notifications.", Format: "json", patterns: []string{s.runnerLogPath}},
		{ID: "api", Label: "API server", Description: "Every API request, sign-ins, settings changes, background jobs and errors.", Format: "json", Admin: true, patterns: []string{serverGlob}},
		{ID: "api-console", Label: "API console", Description: "The API server's console output, including startup failures.", Format: "text", Admin: true, patterns: []string{filepath.Join(logsDir, "v2-api.log")}},
		{ID: "ui", Label: "UI server", Description: "Web UI server: proxied API calls, static files and TLS.", Format: "text", Admin: true, patterns: []string{filepath.Join(logsDir, "v2-ui.log")}},
		{ID: "supervisor", Label: "Supervisor", Description: "Process supervisor: starts, restarts, health checks and upgrades.", Format: "text", Admin: true, patterns: []string{filepath.Join(logsDir, "v2-supervisor.log"), filepath.Join(logsDir, "v2-api-supervisor.log"), filepath.Join(logsDir, "v2-ui-supervisor.log")}},
		{ID: "scheduler", Label: "Scheduler", Description: "Output of OS-scheduled runs (cron, systemd, Task Scheduler).", Format: "text", Admin: true, patterns: []string{filepath.Join(logsDir, "ncc-scheduler.log")}},
	}
	for i := range sources {
		seen := map[string]bool{}
		for _, pat := range sources[i].patterns {
			matches, _ := filepath.Glob(s.absPath(pat))
			sort.Strings(matches)
			for _, m := range matches {
				if strings.HasSuffix(m, ".gz") || seen[m] {
					continue
				}
				if st, err := os.Stat(m); err != nil || st.IsDir() {
					continue
				}
				seen[m] = true
				rel, err := filepath.Rel(s.absPath("."), m)
				if err != nil {
					rel = m
				}
				sources[i].Files = append(sources[i].Files, filepath.ToSlash(rel))
			}
		}
	}
	return sources
}

// handleLogs serves GET /api/v1/logs (the sources the caller may read) and
// GET /api/v1/logs/<source>?file=&lines= (the tail of one file).
func (s *apiServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	p, _ := principalFromContext(r.Context())
	isAdmin := p.role >= RoleAdmin
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/logs"), "/")
	sources := s.logSources()
	if id == "" {
		out := []logSource{}
		for _, src := range sources {
			if src.Admin && !isAdmin {
				continue
			}
			out = append(out, src)
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"sources": out,
			"level":   logLevelName(serverLogLevel.Level()),
		}})
		return
	}
	var src *logSource
	for i := range sources {
		if sources[i].ID == id {
			src = &sources[i]
		}
	}
	if src == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "unknown log " + id})
		return
	}
	if src.Admin && !isAdmin {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "only admins can read the " + src.Label + " log", ErrorCode: "NCC_API_FORBIDDEN"})
		return
	}
	file := strings.TrimSpace(r.URL.Query().Get("file"))
	if file == "" {
		if len(src.Files) == 0 {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"source": src.ID, "path": "", "content": "", "exists": false, "format": src.Format}})
			return
		}
		file = src.Files[0]
	}
	// Only files this source lists can be read; the name is never used to
	// build a path on its own.
	allowed := false
	for _, f := range src.Files {
		if f == file {
			allowed = true
		}
	}
	if !allowed {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "that file is not part of the " + src.Label + " log"})
		return
	}
	maxBytes := int64(256 << 10)
	if v := r.URL.Query().Get("bytes"); v != "" {
		var n int64
		if _, err := fmt.Sscan(v, &n); err == nil && n > 0 && n <= 4<<20 {
			maxBytes = n
		}
	}
	abs := s.absPath(file)
	content, err := readTailFile(abs, maxBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"source": src.ID, "path": file, "content": "", "exists": false, "format": src.Format}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	var size int64
	var mod string
	if st, err := os.Stat(abs); err == nil {
		size, mod = st.Size(), st.ModTime().UTC().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"source":    src.ID,
		"path":      file,
		"content":   strings.TrimRight(content, "\r\n"),
		"exists":    true,
		"format":    src.Format,
		"size":      size,
		"mod_time":  mod,
		"truncated": size > maxBytes,
	}})
}

// handleLoggingSettings serves GET/PUT /api/v1/settings/logging: the API
// server's log level, changeable without a restart (not persisted; the
// --log-level flag / NCC_LOG_LEVEL applies again on restart).
func (s *apiServer) handleLoggingSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	case http.MethodPut, http.MethodPost:
		var req struct {
			Level string `json:"level"`
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		lvl, err := parseLogLevel(req.Level)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		before := logLevelName(serverLogLevel.Level())
		serverLogLevel.Set(lvl)
		serverLog.InfoContext(r.Context(), "log level changed", "from", before, "to", logLevelName(lvl))
		s.audit(r, "settings.logging.update", true, map[string]interface{}{"from": before, "to": logLevelName(lvl)})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"level":       logLevelName(serverLogLevel.Level()),
		"server_log":  s.serverLogPath,
		"runner_log":  s.runnerLogPath,
		"persistence": "Until the API server restarts; set --log-level or NCC_LOG_LEVEL to keep it.",
	}})
}

// logAuditEntry mirrors an audit entry into the server log, so one file holds
// the request, the action it took and any error around it.
func logAuditEntry(payload map[string]interface{}) {
	keys := make([]string, 0, len(payload))
	for k := range payload {
		if k != "ts" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	attrs := make([]any, 0, len(keys)*2)
	for _, k := range keys {
		attrs = append(attrs, k, payload[k])
	}
	lvl := slog.LevelInfo
	if ok, _ := payload["success"].(bool); !ok {
		lvl = slog.LevelWarn
	}
	serverLog.Log(context.Background(), lvl, "audit", attrs...)
}

// logSubprocess records a helper command's outcome: DEBUG when it worked,
// WARN with the end of its output when it did not.
func logSubprocess(what string, args []string, d time.Duration, output string, err error) {
	attrs := []any{"args", strings.Join(args, " "), "took_ms", d.Milliseconds()}
	if len(args) > 0 {
		attrs = append(attrs, "subcommand", args[0])
	}
	if err == nil {
		serverLog.Debug(what+" finished", attrs...)
		return
	}
	attrs = append(attrs, "error", err.Error())
	if out := strings.TrimSpace(output); out != "" {
		attrs = append(attrs, "output_tail", tailString(out, 1500))
	}
	serverLog.Warn(what+" failed", attrs...)
}

// envOrDefault returns the trimmed environment variable, or def when unset.
func envOrDefault(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}
