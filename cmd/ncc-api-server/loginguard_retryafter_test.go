package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lockedLogin drives the login handler until it answers 429 for user and
// returns that response.
func lockedLogin(t *testing.T, s *apiServer, user, ip string) *httptest.ResponseRecorder {
	t.Helper()
	for i := 0; i < 10; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"`+user+`","password":"wrong"}`))
		req.RemoteAddr = ip + ":1234"
		s.handleLogin(rr, req)
		if rr.Code == http.StatusTooManyRequests {
			return rr
		}
	}
	t.Fatalf("%s was never locked out", user)
	return nil
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// The lockout response carries a machine-readable retry time (header and
// envelope field) and is identical in shape for real and unknown accounts.
func TestLoginLockoutCarriesRetryAfterForKnownAndUnknownUsers(t *testing.T) {
	s := newDBServer(t)
	s.loginGuard = newLoginGuard(2, 15*time.Minute, 15*time.Minute)
	hash, err := hashPassword("correct-horse-battery")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("admin", hash, RoleAdmin, false); err != nil {
		t.Fatal(err)
	}

	known := lockedLogin(t, s, "admin", "203.0.113.10")
	unknown := lockedLogin(t, s, "ghost-user-does-not-exist", "203.0.113.11")

	digits := regexp.MustCompile(`\d+`)
	var shapes [2][]string
	var texts [2]string
	for i, rr := range []*httptest.ResponseRecorder{known, unknown} {
		header := rr.Header().Get("Retry-After")
		secs, err := strconv.Atoi(header)
		if err != nil || secs < 1 {
			t.Fatalf("Retry-After header = %q, want a positive number of seconds", header)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		field, ok := body["retry_after_seconds"].(float64)
		if !ok || int(field) != secs {
			t.Fatalf("retry_after_seconds = %v, want %d (matching Retry-After)", body["retry_after_seconds"], secs)
		}
		if secs < 15*60-5 || secs > 15*60+1 {
			t.Fatalf("retry time %d s does not match the 15 min lockout", secs)
		}
		shapes[i] = sortedKeys(body)
		texts[i] = digits.ReplaceAllString(rr.Body.String(), "N")
		if strings.Contains(strings.ToLower(rr.Body.String()), "attempts left") || strings.Contains(rr.Body.String(), "admin") {
			t.Fatalf("lockout response leaks account details: %s", rr.Body.String())
		}
	}
	if strings.Join(shapes[0], ",") != strings.Join(shapes[1], ",") {
		t.Fatalf("lockout shape differs for known vs unknown user:\n%v\n%v", shapes[0], shapes[1])
	}
	if texts[0] != texts[1] {
		t.Fatalf("lockout body differs for known vs unknown user:\n%s\n%s", texts[0], texts[1])
	}
}

// "Stay signed in" (POST /auth/refresh) keeps the directory groups embedded in
// the session, so a refreshed AD/SAML user keeps cluster-group access.
func TestSessionRefreshKeepsDirectoryGroups(t *testing.T) {
	s := newDBServer(t)
	tok, _, err := s.issueRoleSessionTokenWithGroups("", "jdoe", RoleViewer, []string{"CN=ncc-ops"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr := httptest.NewRecorder()
	s.handleAuthRefresh(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("refresh: %d (%s)", rr.Code, rr.Body.String())
	}
	var next *http.Cookie
	for _, c := range rr.Result().Cookies() {
		if c.Name == sessionCookieName && strings.TrimSpace(c.Value) != "" {
			next = c
		}
	}
	if next == nil {
		t.Fatal("refresh did not re-issue a session cookie")
	}
	verify := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	verify.AddCookie(next)
	p, ok := s.resolvePrincipal(verify)
	if !ok {
		t.Fatal("refreshed session should authenticate")
	}
	if len(p.groups) != 1 || p.groups[0] != "CN=ncc-ops" {
		t.Fatalf("refreshed session groups = %v, want [CN=ncc-ops]", p.groups)
	}
}

// The per-client rate limit (login, forgot-password and other sensitive
// routes) reports its retry time in the envelope as well as the header.
func TestRateLimitResponseCarriesRetryAfterSeconds(t *testing.T) {
	s := &apiServer{rateLimiter: newFixedWindowRateLimiter(1, time.Minute)}
	h := s.withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	for _, path := range []string{"/api/v1/auth/login", "/api/v1/auth/forgot-password"} {
		var rr *httptest.ResponseRecorder
		for i := 0; i < 2; i++ {
			rr = httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			req.RemoteAddr = "198.51.100.20:1"
			h.ServeHTTP(rr, req)
		}
		if rr.Code != http.StatusTooManyRequests {
			t.Fatalf("%s: second request should be limited, got %d", path, rr.Code)
		}
		secs, err := strconv.Atoi(rr.Header().Get("Retry-After"))
		if err != nil || secs < 1 {
			t.Fatalf("%s: Retry-After = %q", path, rr.Header().Get("Retry-After"))
		}
		var body struct {
			RetryAfterSeconds int `json:"retry_after_seconds"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.RetryAfterSeconds != secs {
			t.Fatalf("%s: retry_after_seconds = %d, want %d", path, body.RetryAfterSeconds, secs)
		}
	}
}
