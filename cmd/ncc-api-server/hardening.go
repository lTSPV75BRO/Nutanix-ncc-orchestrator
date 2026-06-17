package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"goncc/internal/v2layout"
)

const (
	maxJSONBodyBytes = 1 << 20 // 1 MiB
	maxExtraArgs     = 32
)

var (
	reTaskName = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	reCronSafe = regexp.MustCompile(`^[0-9*/,\- ]{1,120}$`)
	reEvery    = regexp.MustCompile(`^[1-9][0-9]{0,3}[mhd]$`)
	reSafeArg  = regexp.MustCompile(`^[A-Za-z0-9._:/=\-,@]+$`)
)

type sessionClaims struct {
	Exp  int64  `json:"exp"`
	Iat  int64  `json:"iat"`
	JTI  string `json:"jti"`
	CIP  string `json:"cip"`
	Iss  string `json:"iss"`
	Sub  string `json:"sub,omitempty"`  // subject (username / identity)
	Role string `json:"role,omitempty"` // viewer|operator|admin
	Gen  int    `json:"gen,omitempty"`  // token generation (bumped on password change/reset)
	// Grps carries the directory (AD/SAML) group values resolved at login so
	// cluster-group membership can be evaluated without a live directory lookup
	// on every request. Empty for local accounts (membership is resolved live
	// by username) and static-token callers.
	Grps []string `json:"grps,omitempty"`
}

type fixedWindowRateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	buckets map[string]rateLimitBucket
	lastGC  time.Time
	allowed uint64
	blocked uint64
	evicted uint64
}

type rateLimitBucket struct {
	count   int
	resetAt time.Time
}

func newFixedWindowRateLimiter(limit int, window time.Duration) *fixedWindowRateLimiter {
	return &fixedWindowRateLimiter{
		limit:   limit,
		window:  window,
		buckets: map[string]rateLimitBucket{},
	}
}

func (l *fixedWindowRateLimiter) allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gcExpiredLocked(now)
	b, ok := l.buckets[key]
	if !ok || !now.Before(b.resetAt) {
		l.buckets[key] = rateLimitBucket{count: 1, resetAt: now.Add(l.window)}
		l.allowed++
		return true, 0
	}
	if b.count >= l.limit {
		l.blocked++
		return false, b.resetAt.Sub(now)
	}
	b.count++
	l.buckets[key] = b
	l.allowed++
	return true, 0
}

func (l *fixedWindowRateLimiter) gcExpiredLocked(now time.Time) {
	if !l.lastGC.IsZero() && now.Sub(l.lastGC) < l.window {
		return
	}
	evicted := uint64(0)
	for k, b := range l.buckets {
		if !now.Before(b.resetAt) {
			delete(l.buckets, k)
			evicted++
		}
	}
	l.evicted += evicted
	l.lastGC = now
}

type rateLimiterStats struct {
	LimitPerWindow int    `json:"limit_per_window"`
	WindowSeconds  int64  `json:"window_seconds"`
	ActiveBuckets  int    `json:"active_buckets"`
	AllowedTotal   uint64 `json:"allowed_total"`
	BlockedTotal   uint64 `json:"blocked_total"`
	EvictedTotal   uint64 `json:"evicted_total"`
	LastGCAt       string `json:"last_gc_at,omitempty"`
}

func (l *fixedWindowRateLimiter) stats(now time.Time) rateLimiterStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gcExpiredLocked(now)
	st := rateLimiterStats{
		LimitPerWindow: l.limit,
		WindowSeconds:  int64(l.window / time.Second),
		ActiveBuckets:  len(l.buckets),
		AllowedTotal:   l.allowed,
		BlockedTotal:   l.blocked,
		EvictedTotal:   l.evicted,
	}
	if !l.lastGC.IsZero() {
		st.LastGCAt = l.lastGC.UTC().Format(time.RFC3339)
	}
	return st
}

func parseAllowedOrigins(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		out[v] = struct{}{}
	}
	return out
}

// scrubEnv returns a copy of env with any variable whose name matches one of
// drop removed. It never mutates the input slice.
func scrubEnv(env []string, drop ...string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		keep := true
		for _, d := range drop {
			if strings.HasPrefix(kv, d+"=") {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, kv)
		}
	}
	return out
}

// childEnv is the environment handed to any orchestrator child process. It
// strips the user-store master key (NCC_MASTER_KEY): only this api-server uses
// it to (un)seal the user database, while the orchestrator copies that DB as
// opaque bytes for backup/restore and never needs the key — so it must not
// leak into a child's environment (visible via /proc/<pid>/environ and
// inherited by anything the child itself spawns). Operators who want the key
// available to children should use the key-file source instead.
func childEnv() []string {
	return scrubEnv(os.Environ(), "NCC_MASTER_KEY")
}

func secureCompare(a, b string) bool {
	ab := []byte(strings.TrimSpace(a))
	bb := []byte(strings.TrimSpace(b))
	if len(ab) != len(bb) {
		return false
	}
	return subtle.ConstantTimeCompare(ab, bb) == 1
}

func decodeJSON(r io.Reader, out interface{}) error {
	dec := json.NewDecoder(io.LimitReader(r, maxJSONBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var trailing interface{}
	if err := dec.Decode(&trailing); err != io.EOF {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func requireJSONContentType(r *http.Request) error {
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if ct == "" {
		return errors.New("content-type is required")
	}
	if !strings.HasPrefix(ct, "application/json") {
		return fmt.Errorf("unsupported content-type: %s", ct)
	}
	return nil
}

func parseRemoteIP(remoteAddr string) net.IP {
	trimmed := strings.TrimSpace(remoteAddr)
	if trimmed == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(trimmed)
	if err == nil {
		if ip := net.ParseIP(strings.TrimSpace(host)); ip != nil {
			return ip
		}
	}
	if ip := net.ParseIP(trimmed); ip != nil {
		return ip
	}
	return nil
}

func trustForwardedHeaders(remoteIP net.IP) bool {
	if remoteIP == nil {
		return false
	}
	return remoteIP.IsLoopback() || remoteIP.IsPrivate()
}

func cleanClientIP(r *http.Request) string {
	remoteIP := parseRemoteIP(r.RemoteAddr)
	if trustForwardedHeaders(remoteIP) {
		xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
		if xff != "" {
			first := strings.TrimSpace(strings.Split(xff, ",")[0])
			if ip := net.ParseIP(first); ip != nil {
				return ip.String()
			}
		}
	}
	if remoteIP != nil {
		return remoteIP.String()
	}
	return ""
}

func isLoopbackRequest(r *http.Request) bool {
	ip := net.ParseIP(cleanClientIP(r))
	return ip != nil && ip.IsLoopback()
}

func isWithinBase(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func pathContainsSymlink(base, target string) (bool, error) {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false, err
	}
	if rel == "." {
		return false, nil
	}
	current := base
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		st, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return false, err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *apiServer) normalizeAndConfinePath(p string) (string, error) {
	rootAbs, err := filepath.Abs(s.repoRoot)
	if err != nil {
		return "", fmt.Errorf("resolve repo root: %w", err)
	}
	rootAbs = filepath.Clean(rootAbs)
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", fmt.Errorf("resolve repo root symlinks: %w", err)
	}
	rootReal = filepath.Clean(rootReal)

	var abs string
	if filepath.IsAbs(p) {
		abs = filepath.Clean(p)
	} else {
		abs = filepath.Clean(filepath.Join(rootReal, p))
	}
	if !isWithinBase(rootReal, abs) {
		return "", fmt.Errorf("path escapes repo root: %s", p)
	}
	hasLink, err := pathContainsSymlink(rootReal, abs)
	if err != nil {
		return "", fmt.Errorf("inspect path symlinks: %w", err)
	}
	if hasLink {
		return "", fmt.Errorf("path includes symlink and is not allowed: %s", p)
	}
	if _, err := os.Lstat(abs); err == nil {
		realAbs, err := filepath.EvalSymlinks(abs)
		if err != nil {
			return "", fmt.Errorf("resolve path symlinks: %w", err)
		}
		realAbs = filepath.Clean(realAbs)
		if !isWithinBase(rootReal, realAbs) {
			return "", fmt.Errorf("path resolves outside repo root: %s", p)
		}
	}
	return abs, nil
}

func (s *apiServer) validateConfigPath(p string) (string, error) {
	trimmed := strings.TrimSpace(p)
	if trimmed == "" {
		return "", errors.New("config path is required")
	}
	abs, err := s.normalizeAndConfinePath(trimmed)
	if err != nil {
		return "", err
	}
	ext := strings.ToLower(filepath.Ext(abs))
	if ext != ".yml" && ext != ".yaml" {
		return "", fmt.Errorf("config path must end with .yaml or .yml: %s", trimmed)
	}
	return abs, nil
}

func validateScheduleInput(st scheduleState) error {
	switch st.Type {
	case "auto", "cron", "systemd", "windows":
	default:
		return fmt.Errorf("invalid schedule type: %s", st.Type)
	}
	switch st.Action {
	case "create", "list", "remove", "run-now":
	default:
		return fmt.Errorf("invalid schedule action: %s", st.Action)
	}
	cron := strings.TrimSpace(st.Cron)
	every := strings.TrimSpace(st.Every)
	if cron != "" && !reCronSafe.MatchString(cron) {
		return errors.New("cron contains invalid characters")
	}
	if every != "" && !reEvery.MatchString(every) {
		return errors.New("every must match patterns like 15m, 4h, or 1d")
	}
	if strings.TrimSpace(st.TaskName) != "" && !reTaskName.MatchString(strings.TrimSpace(st.TaskName)) {
		return errors.New("task_name contains invalid characters")
	}
	// An "action: create" schedule must have a recurrence — otherwise the saved
	// state is meaningless and any later --apply will fail in the orchestrator.
	// "list", "remove", and "run-now" are introspective/one-shot and don't need
	// a cron/every spec.
	if st.Action == "create" && cron == "" && every == "" {
		return errors.New("schedule with action=create requires either cron or every")
	}
	return nil
}

func sanitizeExtraArgs(args []string) ([]string, error) {
	if len(args) > maxExtraArgs {
		return nil, fmt.Errorf("too many extra args: max %d", maxExtraArgs)
	}
	allowed := map[string]bool{
		"--output-dir":         true,
		"--log-dir":            true,
		"--cluster-file":       true,
		"--clusters":           true,
		"--prom-dir":           true,
		"--compare-with":       true,
		"--gen-test-agg":       false,
		"--no-html":            false,
		"--quiet-hours":        false,
		"--maintenance-window": false,
	}
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := strings.TrimSpace(args[i])
		if a == "" {
			continue
		}
		if strings.ContainsAny(a, "&;|`$><\n\r\t") {
			return nil, fmt.Errorf("unsafe characters in arg: %s", a)
		}
		if strings.HasPrefix(a, "--") {
			expectsVal, ok := allowed[a]
			if !ok {
				return nil, fmt.Errorf("extra arg not allowed: %s", a)
			}
			out = append(out, a)
			if expectsVal {
				if i+1 >= len(args) {
					return nil, fmt.Errorf("missing value for arg: %s", a)
				}
				val := strings.TrimSpace(args[i+1])
				if val == "" || strings.ContainsAny(val, "&;|`$><\n\r\t") || !reSafeArg.MatchString(val) {
					return nil, fmt.Errorf("unsafe value for arg: %s", a)
				}
				out = append(out, val)
				i++
			}
			continue
		}
		return nil, fmt.Errorf("standalone arg values are not allowed: %s", a)
	}
	return out, nil
}

// issueSessionToken mints a loopback-bootstrap admin session. It is kept for
// backward compatibility with the /api/v1/auth/session endpoint, which already
// gates issuance behind the admin token, so the resulting session is admin.
func (s *apiServer) issueSessionToken(clientIP string) (string, time.Time, error) {
	return s.issueRoleSessionToken(clientIP, "loopback-admin", RoleAdmin)
}

// issueRoleSessionToken mints a signed session token carrying a subject and a
// role. Used by password login and SAML SSO so the session itself encodes the
// caller's authorization level.
func (s *apiServer) issueRoleSessionToken(clientIP, subject string, role Role) (string, time.Time, error) {
	return s.issueRoleSessionTokenWithGroups(clientIP, subject, role, nil)
}

// issueRoleSessionTokenWithGroups is issueRoleSessionToken plus the directory
// group values resolved at login, embedded in the token so cluster-group
// membership can be evaluated per request without re-querying the directory.
func (s *apiServer) issueRoleSessionTokenWithGroups(clientIP, subject string, role Role, groups []string) (string, time.Time, error) {
	jb := make([]byte, 16)
	if _, err := rand.Read(jb); err != nil {
		return "", time.Time{}, fmt.Errorf("generate session id: %w", err)
	}
	now := time.Now().UTC()
	exp := now.Add(s.effectiveSessionTTL())
	// Stamp the subject's current token generation so a later password
	// change/reset (which bumps the generation) invalidates this token.
	gen := 0
	if s.users != nil {
		if acct, ok := s.users.lookup(strings.TrimSpace(subject)); ok {
			gen = acct.TokenGen
		}
	}
	claims := sessionClaims{
		Exp:  exp.Unix(),
		Iat:  now.Unix(),
		JTI:  base64.RawURLEncoding.EncodeToString(jb),
		CIP:  strings.TrimSpace(clientIP),
		Iss:  s.sessionIssuer,
		Sub:  strings.TrimSpace(subject),
		Role: role.String(),
		Gen:  gen,
		Grps: groups,
	}
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte(s.sessionSecret))
	_, _ = mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return payload + "." + sig, exp, nil
}

func (s *apiServer) validatePathConfig() error {
	candidates := []string{
		s.configPath,
		s.outputDir,
		s.logDir,
		s.runnerLogPath,
		s.scheduleStatePath,
		s.backupScheduleStatePath,
		s.notificationStatePath,
		s.tokenFilePath,
	}
	for _, p := range candidates {
		if _, err := s.normalizeAndConfinePath(p); err != nil {
			return err
		}
	}

	// Fail fast for explicit orchestrator binary paths (common in production),
	// while preserving dev fallback behavior when the default relative path is used.
	trimmedBin := strings.TrimSpace(s.orchestratorBin)
	if filepath.IsAbs(trimmedBin) {
		binPath := s.absPath(trimmedBin)
		st, err := os.Stat(binPath)
		if err != nil {
			return fmt.Errorf("orchestrator binary not found at %s (set --orchestrator-bin to a valid executable path): %w", binPath, err)
		}
		if st.IsDir() {
			return fmt.Errorf("orchestrator binary path is a directory: %s", binPath)
		}
		// Windows has no Unix executable bit, so a mode&0o111 test would
		// reject a perfectly valid ncc-orchestrator.exe and make the
		// api-server exit 1 during v2-start. v2layout.IsExecutable
		// branches on GOOS: PATHEXT-extension check on Windows, the
		// strict bit check on Unix.
		if !v2layout.IsExecutable(binPath) {
			return fmt.Errorf("orchestrator binary is not executable: %s", binPath)
		}
	}

	return nil
}

// verifySessionToken validates a session token's signature and time/issuer/IP
// bindings. It is kept as a boolean-style helper (error only) for existing
// callers; verifySession returns the decoded claims as well.
func (s *apiServer) verifySessionToken(token, clientIP string) error {
	_, err := s.verifySession(token, clientIP)
	return err
}

// verifySession validates a session token and returns its decoded claims
// (including subject and role) on success.
func (s *apiServer) verifySession(token, clientIP string) (sessionClaims, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 2 {
		return sessionClaims{}, errors.New("invalid session token format")
	}
	payload, sigHex := parts[0], parts[1]
	mac := hmac.New(sha256.New, []byte(s.sessionSecret))
	_, _ = mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	if !secureCompare(expectedSig, sigHex) {
		return sessionClaims{}, errors.New("invalid session token signature")
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return sessionClaims{}, errors.New("invalid session token payload")
	}
	var claims sessionClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return sessionClaims{}, errors.New("invalid session token claims")
	}
	now := time.Now().UTC().Unix()
	if claims.Exp <= now || claims.Iat > now+30 {
		return sessionClaims{}, errors.New("session token expired or invalid time")
	}
	if strings.TrimSpace(claims.Iss) != strings.TrimSpace(s.sessionIssuer) {
		return sessionClaims{}, errors.New("invalid session token issuer")
	}
	if claims.CIP != "" && clientIP != "" && claims.CIP != clientIP {
		return sessionClaims{}, errors.New("session token client binding mismatch")
	}
	return claims, nil
}

func (s *apiServer) withRateLimit(next http.Handler) http.Handler {
	limitedPaths := map[string]bool{
		"/api/v1/auth/session":                true,
		"/api/v1/auth/login":                  true,
		"/api/v1/auth/rotate":                 true,
		"/api/v1/runs/trigger":                true,
		"/api/v1/settings/config":             true,
		"/api/v1/settings/config-file":        true,
		"/api/v1/settings/notifications/test": true,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.rateLimiter == nil || r.Method == http.MethodOptions || r.URL.Path == "/api/v1/health" {
			next.ServeHTTP(w, r)
			return
		}
		if !limitedPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		clientIP := cleanClientIP(r)
		if clientIP == "" {
			clientIP = "unknown"
		}
		key := r.URL.Path + "|" + clientIP
		ok, retryAfter := s.rateLimiter.allow(key, time.Now().UTC())
		if ok {
			next.ServeHTTP(w, r)
			return
		}
		retrySeconds := int((retryAfter + time.Second - 1) / time.Second)
		if retrySeconds < 1 {
			retrySeconds = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
		writeJSON(w, http.StatusTooManyRequests, envelope{
			Success: false,
			Error:   "rate limit exceeded",
			Data: map[string]interface{}{
				"retry_after_sec": retrySeconds,
				"path":            r.URL.Path,
			},
		})
	})
}

// auditEvent records a server-originated (non-HTTP) audit entry — e.g. a
// background self-heal action that has no request context. It mirrors audit()
// but tags the actor as the system rather than a principal/IP.
func (s *apiServer) auditEvent(action string, success bool, fields map[string]interface{}) {
	payload := map[string]interface{}{
		"ts":        time.Now().UTC().Format(time.RFC3339),
		"action":    action,
		"success":   success,
		"client":    "system",
		"user":      "system",
		"auth_mode": s.authMode,
	}
	for k, v := range fields {
		payload[k] = v
	}
	b, _ := json.Marshal(payload)
	fmt.Printf("AUDIT %s\n", string(b))
	s.appendAuditLine(b)
}

func (s *apiServer) audit(r *http.Request, action string, success bool, fields map[string]interface{}) {
	payload := map[string]interface{}{
		"ts":        time.Now().UTC().Format(time.RFC3339),
		"action":    action,
		"success":   success,
		"path":      r.URL.Path,
		"method":    r.Method,
		"client":    cleanClientIP(r),
		"auth_mode": s.authMode,
	}
	if ua := strings.TrimSpace(r.Header.Get("User-Agent")); ua != "" {
		if len(ua) > 200 {
			ua = ua[:200]
		}
		payload["user_agent"] = ua
	}
	// Attribute the action to the authenticated principal (user + role) when it
	// was resolved by withAuth. Static-token automation surfaces as the
	// synthetic subject (e.g. "static-admin-token"); local/SSO sessions carry
	// the real username. The admin account always reports role "admin".
	if p, ok := principalFromContext(r.Context()); ok {
		if p.subject != "" {
			payload["user"] = p.subject
		}
		if rs := p.role.String(); rs != "" {
			payload["role"] = rs
		}
	}
	for k, v := range fields {
		payload[k] = v
	}
	b, _ := json.Marshal(payload)
	fmt.Printf("AUDIT %s\n", string(b))
	s.appendAuditLine(b)
}

// appendAuditLine writes a JSONL audit entry with a serialized lock so concurrent
// requests don't interleave bytes. It rotates the log when it exceeds
// auditLogMaxBytes (renaming current file to <path>.1).
func (s *apiServer) appendAuditLine(line []byte) {
	if strings.TrimSpace(s.auditLogPath) == "" {
		return
	}
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	abs := s.absPath(s.auditLogPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		log.Printf("audit log: ensure dir: %v", err)
		return
	}
	if s.auditLogMaxBytes > 0 {
		if st, err := os.Stat(abs); err == nil && st.Size() >= s.auditLogMaxBytes {
			rotated := abs + ".1"
			_ = os.Remove(rotated)
			if err := os.Rename(abs, rotated); err != nil {
				log.Printf("audit log: rotate: %v", err)
			}
		}
	}
	f, err := os.OpenFile(abs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Printf("audit log: open: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		log.Printf("audit log: write: %v", err)
	}
}

// auditEntries reads and parses up to `limit` most recent audit entries,
// optionally filtered by an action prefix or success flag.
// auditFilter narrows the audit entries returned by auditEntries. Zero-value
// fields are ignored (no filtering on that dimension).
type auditFilter struct {
	actionPrefix string    // match entries whose "action" has this prefix
	user         string    // case-insensitive exact match on "user"
	onlyFailures bool      // keep only failed actions
	since        time.Time // keep entries at or after this time
	until        time.Time // keep entries at or before this time
}

// matches reports whether a decoded audit entry passes the filter.
func (f auditFilter) matches(entry map[string]interface{}) bool {
	if f.actionPrefix != "" {
		act, _ := entry["action"].(string)
		if !strings.HasPrefix(act, f.actionPrefix) {
			return false
		}
	}
	if f.onlyFailures {
		if ok, _ := entry["success"].(bool); ok {
			return false
		}
	}
	if f.user != "" {
		u, _ := entry["user"].(string)
		if !strings.EqualFold(strings.TrimSpace(u), f.user) {
			return false
		}
	}
	if !f.since.IsZero() || !f.until.IsZero() {
		ts, _ := entry["ts"].(string)
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return false
		}
		if !f.since.IsZero() && t.Before(f.since) {
			return false
		}
		if !f.until.IsZero() && t.After(f.until) {
			return false
		}
	}
	return true
}

func (s *apiServer) auditEntries(limit int, f auditFilter) ([]map[string]interface{}, error) {
	if strings.TrimSpace(s.auditLogPath) == "" {
		return nil, nil
	}
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	if limit > 50000 {
		limit = 50000
	}
	// Always return a non-nil slice so JSON encodes [] (not null), keeping the
	// API shape stable for the UI even when the file doesn't exist yet.
	out := make([]map[string]interface{}, 0, limit)
	abs := s.absPath(s.auditLogPath)
	data, err := os.ReadFile(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return out, err
	}
	lines := bytes.Split(data, []byte{'\n'})
	// Walk newest-first.
	for i := len(lines) - 1; i >= 0 && len(out) < limit; i-- {
		ln := bytes.TrimSpace(lines[i])
		if len(ln) == 0 {
			continue
		}
		var entry map[string]interface{}
		if err := json.Unmarshal(ln, &entry); err != nil {
			continue
		}
		if !f.matches(entry) {
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}
