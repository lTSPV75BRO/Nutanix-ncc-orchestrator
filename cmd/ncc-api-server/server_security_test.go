package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
