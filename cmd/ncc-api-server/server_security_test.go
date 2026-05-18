package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWithAuthTokenMode(t *testing.T) {
	s := &apiServer{authToken: "topsecret", authMode: "token"}
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", rr.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req2.Header.Set("X-API-Token", "topsecret")
	rr2 := httptest.NewRecorder()
	next.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 with token, got %d", rr2.Code)
	}

	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	rr3 := httptest.NewRecorder()
	next.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("expected docs home to bypass auth, got %d", rr3.Code)
	}

	req4 := httptest.NewRequest(http.MethodGet, "/api/v1/openapi.json", nil)
	rr4 := httptest.NewRecorder()
	next.ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusOK {
		t.Fatalf("expected openapi endpoint to bypass auth, got %d", rr4.Code)
	}

	req5 := httptest.NewRequest(http.MethodGet, "/api/v1/meta/routes", nil)
	rr5 := httptest.NewRecorder()
	next.ServeHTTP(rr5, req5)
	if rr5.Code != http.StatusOK {
		t.Fatalf("expected meta routes endpoint to bypass auth, got %d", rr5.Code)
	}

	req6 := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/rate-limit", nil)
	rr6 := httptest.NewRecorder()
	next.ServeHTTP(rr6, req6)
	if rr6.Code != http.StatusOK {
		t.Fatalf("expected rate-limit metrics endpoint to bypass auth, got %d", rr6.Code)
	}

	req7 := httptest.NewRequest(http.MethodGet, "/docs/ui", nil)
	rr7 := httptest.NewRecorder()
	next.ServeHTTP(rr7, req7)
	if rr7.Code != http.StatusOK {
		t.Fatalf("expected swagger docs endpoint to bypass auth, got %d", rr7.Code)
	}
}

func TestHandleAPIDocsHome(t *testing.T) {
	s := &apiServer{authMode: "token"}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	s.handleAPIDocsHome(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "/api/v1/openapi.json") {
		t.Fatal("expected docs page to include OpenAPI link")
	}
	if !strings.Contains(body, "Endpoint Explorer") {
		t.Fatal("expected docs page to include endpoint explorer")
	}
}

func TestHandleSwaggerUIPage(t *testing.T) {
	s := &apiServer{}
	req := httptest.NewRequest(http.MethodGet, "/docs/ui", nil)
	rr := httptest.NewRecorder()
	s.handleSwaggerUIPage(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "swagger-ui-bundle.js") {
		t.Fatal("expected swagger UI script reference")
	}
	if !strings.Contains(body, "/api/v1/openapi.json") {
		t.Fatal("expected swagger UI to target openapi endpoint")
	}
}

func TestWithAuthSessionMode(t *testing.T) {
	s := &apiServer{
		authMode:      "session",
		sessionSecret: "session-secret",
		sessionTTL:    5 * time.Minute,
		sessionIssuer: "ncc-api-server",
	}
	token, _, err := s.issueSessionToken("127.0.0.1")
	if err != nil {
		t.Fatalf("issueSessionToken failed: %v", err)
	}
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid bearer session, got %d", rr.Code)
	}
}

func TestWithCORSDeniesUnknownOrigin(t *testing.T) {
	s := &apiServer{corsOrigin: "http://localhost:8080"}
	next := s.withCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set("Origin", "https://evil.example")
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for disallowed origin, got %d", rr.Code)
	}
}

func TestCleanClientIPTrustsForwardedOnlyFromTrustedProxy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.RemoteAddr = "198.51.100.10:12345"
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	if got := cleanClientIP(req); got != "198.51.100.10" {
		t.Fatalf("expected remote client IP for untrusted proxy, got %q", got)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req2.RemoteAddr = "10.0.0.2:12345"
	req2.Header.Set("X-Forwarded-For", "203.0.113.9")
	if got := cleanClientIP(req2); got != "203.0.113.9" {
		t.Fatalf("expected forwarded client IP from trusted proxy, got %q", got)
	}
}

func TestNormalizeAndConfinePathRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	linkDir := filepath.Join(root, "linked")
	if err := os.Symlink(outside, linkDir); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	s := &apiServer{repoRoot: root}
	if _, err := s.normalizeAndConfinePath("linked/secret.txt"); err == nil {
		t.Fatal("expected symlink path to be rejected")
	}

	if _, err := s.normalizeAndConfinePath("safe/path.txt"); err != nil {
		t.Fatalf("expected regular path inside repo root to pass, got %v", err)
	}
}

func TestWithRateLimitSensitivePath(t *testing.T) {
	s := &apiServer{
		rateLimiter: newFixedWindowRateLimiter(1, time.Minute),
	}
	next := s.withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil)
	req1.RemoteAddr = "127.0.0.1:9000"
	rr1 := httptest.NewRecorder()
	next.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected first request to pass, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil)
	req2.RemoteAddr = "127.0.0.1:9000"
	rr2 := httptest.NewRecorder()
	next.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request to be rate limited, got %d", rr2.Code)
	}
	if rr2.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header for rate-limited response")
	}
}

func TestWithRateLimitHealthBypass(t *testing.T) {
	s := &apiServer{
		rateLimiter: newFixedWindowRateLimiter(1, time.Minute),
	}
	next := s.withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
		req.RemoteAddr = "127.0.0.1:9000"
		rr := httptest.NewRecorder()
		next.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected health bypass, got %d at iter %d", rr.Code, i)
		}
	}
}

func TestWithRateLimitNotificationsTestPath(t *testing.T) {
	s := &apiServer{
		rateLimiter: newFixedWindowRateLimiter(1, time.Minute),
	}
	next := s.withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/settings/notifications/test", nil)
	req1.RemoteAddr = "127.0.0.1:9000"
	rr1 := httptest.NewRecorder()
	next.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected first request to pass, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/settings/notifications/test", nil)
	req2.RemoteAddr = "127.0.0.1:9000"
	rr2 := httptest.NewRecorder()
	next.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request to be rate limited, got %d", rr2.Code)
	}
}

func TestWriteJSONAddsDefaultErrorCode(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJSON(rr, http.StatusBadRequest, envelope{Success: false, Error: "invalid input"})
	var got map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["error_code"] != "NCC_API_BAD_REQUEST" {
		t.Fatalf("expected NCC_API_BAD_REQUEST, got %#v", got["error_code"])
	}
}

func TestHandleRateLimitMetrics(t *testing.T) {
	s := &apiServer{
		rateLimitPerMinute: 10,
		rateLimiter:        newFixedWindowRateLimiter(10, time.Minute),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/rate-limit", nil)
	rr := httptest.NewRecorder()
	s.handleRateLimitMetrics(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "\"enabled\":true") {
		t.Fatalf("expected enabled=true in body, got %s", body)
	}
	if !strings.Contains(body, "\"blocked_total\"") {
		t.Fatalf("expected blocked_total metric in body, got %s", body)
	}
}
