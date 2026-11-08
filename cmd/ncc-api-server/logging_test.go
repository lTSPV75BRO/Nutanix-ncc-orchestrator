package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureServerLog points serverLog at a JSON buffer for one test.
func captureServerLog(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	prev, prevLevel := serverLog, serverLogLevel.Level()
	buf := &bytes.Buffer{}
	serverLogLevel.Set(level)
	serverLog = slog.New(requestContextHandler{slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: serverLogLevel, ReplaceAttr: redactAttr})})
	t.Cleanup(func() { serverLog = prev; serverLogLevel.Set(prevLevel) })
	return buf
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, ln := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("not JSON: %s", ln)
		}
		out = append(out, m)
	}
	return out
}

func TestLogRedaction(t *testing.T) {
	buf := captureServerLog(t, slog.LevelDebug)
	serverLog.Info("x",
		"password", "hunter2",
		"api_key", "abc",
		"NCC_JWT_SECRET", "s3cret",
		"token_id", "tok_123",
		"must_change_password", true,
		"detail", "Authorization: Bearer abcdefghijklmnop sent to https://user:pw@example.com/x",
	)
	m := logLines(t, buf)[0]
	for _, k := range []string{"password", "api_key", "NCC_JWT_SECRET"} {
		if m[k] != "[redacted]" {
			t.Errorf("%s = %v, want redacted", k, m[k])
		}
	}
	if m["token_id"] != "tok_123" || m["must_change_password"] != true {
		t.Errorf("identifiers must stay readable: %v", m)
	}
	d := m["detail"].(string)
	if strings.Contains(d, "abcdefghijklmnop") || strings.Contains(d, "user:pw") {
		t.Errorf("credentials left in free text: %s", d)
	}
	if got := redactQuery("limit=5&token=zzz&code=abc&env=prod"); got != "code=[redacted]&env=prod&limit=5&token=[redacted]" {
		t.Errorf("redactQuery = %s", got)
	}
	if got := redactURLPaths(`Post "https://hooks.slack.com/services/T0/B0/XYZ": timeout`); strings.Contains(got, "T0/B0") || !strings.Contains(got, "hooks.slack.com") {
		t.Errorf("redactURLPaths = %s", got)
	}
}

func TestRequestLogNamesUserAndError(t *testing.T) {
	buf := captureServerLog(t, slog.LevelDebug)
	s := &apiServer{}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = withPrincipal(r, principal{role: RoleOperator, subject: "alex", method: authSessionCookie})
		addRequestLogAttrs(r, "run_id", "r1")
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "A run is already in progress.", ErrorCode: "NCC_API_RUN_IN_PROGRESS"})
	})
	h := s.withRequestMeta(withErrorCapture(inner))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger?password=x", nil))

	var line map[string]interface{}
	for _, m := range logLines(t, buf) {
		if m["msg"] == "http request" {
			line = m
		}
	}
	if line == nil {
		t.Fatalf("no request line: %s", buf)
	}
	want := map[string]interface{}{"level": "WARN", "user": "alex", "role": "operator", "auth": "session-cookie", "status": float64(409), "error": "A run is already in progress.", "error_code": "NCC_API_RUN_IN_PROGRESS", "run_id": "r1", "query": "password=[redacted]"}
	for k, v := range want {
		if line[k] != v {
			t.Errorf("%s = %v, want %v", k, line[k], v)
		}
	}
	if id, _ := line["request_id"].(string); id == "" || id != rr.Header().Get(requestIDHeader) {
		t.Errorf("request_id %v should match the response header %q", line["request_id"], rr.Header().Get(requestIDHeader))
	}
}

func TestQuietPollingLoggedAtDebug(t *testing.T) {
	buf := captureServerLog(t, slog.LevelInfo)
	s := &apiServer{}
	h := s.withRequestMeta(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/runs/active", nil))
	if strings.Contains(buf.String(), "http request") {
		t.Fatalf("polling GETs should not be logged at INFO: %s", buf)
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil))
	if !strings.Contains(buf.String(), "/api/v1/runs") {
		t.Fatalf("ordinary requests are logged at INFO: %s", buf)
	}
}

func TestPanicIsLoggedAndAnswered(t *testing.T) {
	buf := captureServerLog(t, slog.LevelInfo)
	s := &apiServer{}
	h := s.withRequestMeta(withErrorCapture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("boom") })))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", rr.Code)
	}
	out := buf.String()
	if !strings.Contains(out, "handler panic") || !strings.Contains(out, "boom") || !strings.Contains(out, "goroutine") {
		t.Fatalf("panic and stack should be logged: %s", out)
	}
}

func TestLogSourcesAndAccess(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"ncc-runner.log", "ncc-api.log", "ncc-api.log.1.gz", "v2-ui.log"} {
		if err := os.WriteFile(filepath.Join(root, "logs", f), []byte("line one\nline two\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s := &apiServer{repoRoot: root, runnerLogPath: "logs/ncc-runner.log", serverLogPath: "logs/ncc-api.log"}
	get := func(role Role, path string) (int, map[string]interface{}) {
		req := withPrincipal(httptest.NewRequest(http.MethodGet, path, nil), principal{role: role, subject: "u"})
		rr := httptest.NewRecorder()
		s.handleLogs(rr, req)
		var env struct {
			Data map[string]interface{} `json:"data"`
		}
		_ = json.Unmarshal(rr.Body.Bytes(), &env)
		return rr.Code, env.Data
	}
	_, list := get(RoleViewer, "/api/v1/logs")
	if n := len(list["sources"].([]interface{})); n != 1 {
		t.Fatalf("viewers see only the runner log, got %d sources", n)
	}
	_, list = get(RoleAdmin, "/api/v1/logs")
	var api map[string]interface{}
	for _, src := range list["sources"].([]interface{}) {
		if m := src.(map[string]interface{}); m["id"] == "api" {
			api = m
		}
	}
	if api == nil || len(api["files"].([]interface{})) != 1 {
		t.Fatalf("api source should list ncc-api.log only (no .gz): %v", api)
	}
	if code, _ := get(RoleOperator, "/api/v1/logs/api"); code != http.StatusForbidden {
		t.Fatalf("operator reading the API server log: %d", code)
	}
	code, data := get(RoleAdmin, "/api/v1/logs/ui")
	if code != http.StatusOK || data["content"] != "line one\nline two" {
		t.Fatalf("admin reading the UI log: %d %v", code, data)
	}
	if code, _ := get(RoleAdmin, "/api/v1/logs/ui?file=../config.yaml"); code != http.StatusBadRequest {
		t.Fatalf("a file outside the source must be refused: %d", code)
	}
}

func TestLoggingSettingsChangesLevel(t *testing.T) {
	captureServerLog(t, slog.LevelInfo)
	s := &apiServer{}
	req := withPrincipal(httptest.NewRequest(http.MethodPut, "/api/v1/settings/logging", strings.NewReader(`{"level":"debug"}`)), principal{role: RoleAdmin, subject: "admin"})
	rr := httptest.NewRecorder()
	s.handleLoggingSettings(rr, req)
	if rr.Code != http.StatusOK || serverLogLevel.Level() != slog.LevelDebug {
		t.Fatalf("level not changed: %d %s", rr.Code, rr.Body)
	}
	rr = httptest.NewRecorder()
	s.handleLoggingSettings(rr, withPrincipal(httptest.NewRequest(http.MethodPut, "/api/v1/settings/logging", strings.NewReader(`{"level":"loud"}`)), principal{role: RoleAdmin}))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad level accepted: %d", rr.Code)
	}
}
