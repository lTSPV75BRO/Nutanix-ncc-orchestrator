package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestSessionRefresh verifies that POST /api/v1/auth/refresh re-issues a session
// cookie (with a fresh expiry) for an authenticated session, and rejects callers
// that have no refreshable session (static-token automation / no credential).
func TestSessionRefresh(t *testing.T) {
	s := newDBServer(t)
	hash, err := hashPassword("dave-strong-password")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("dave", hash, RoleOperator, false); err != nil {
		t.Fatal(err)
	}

	// A valid bearer session can be refreshed; the response re-issues a session
	// cookie whose token authenticates on a subsequent request.
	tok, _, err := s.issueRoleSessionToken("", "dave", RoleOperator)
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
	var newCookie *http.Cookie
	for _, c := range rr.Result().Cookies() {
		if c.Name == sessionCookieName && strings.TrimSpace(c.Value) != "" {
			newCookie = c
		}
	}
	if newCookie == nil {
		t.Fatal("refresh did not re-issue a session cookie")
	}
	verify := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	verify.AddCookie(newCookie)
	if _, ok := s.resolvePrincipal(verify); !ok {
		t.Fatal("re-issued session cookie should authenticate")
	}

	// Static-token automation has no session to refresh -> 400.
	reqStatic := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	reqStatic.Header.Set("X-API-Token", s.authToken)
	rrStatic := httptest.NewRecorder()
	s.handleAuthRefresh(rrStatic, reqStatic)
	if rrStatic.Code != http.StatusBadRequest {
		t.Fatalf("static-token refresh: want 400, got %d", rrStatic.Code)
	}

	// No credential at all -> 400 (nothing to refresh).
	reqNone := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	rrNone := httptest.NewRecorder()
	s.handleAuthRefresh(rrNone, reqNone)
	if rrNone.Code != http.StatusBadRequest {
		t.Fatalf("no-credential refresh: want 400, got %d", rrNone.Code)
	}
}

// TestSessionInvalidationOnPasswordChange verifies that changing (or resetting)
// an account's password bumps its token generation, immediately invalidating
// every session token minted before the change while a freshly minted one works.
func TestSessionInvalidationOnPasswordChange(t *testing.T) {
	s := newDBServer(t)
	hash, err := hashPassword("carol-strong-password")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.upsertUser("carol", hash, RoleOperator, false); err != nil {
		t.Fatal(err)
	}

	// Two independent sessions ("devices") for the same account. Mint with an
	// empty client IP so the CIP binding check doesn't interfere here.
	tok1, _, err := s.issueRoleSessionToken("", "carol", RoleOperator)
	if err != nil {
		t.Fatal(err)
	}
	tok2, _, err := s.issueRoleSessionToken("", "carol", RoleOperator)
	if err != nil {
		t.Fatal(err)
	}

	authed := func(tok string) bool {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		_, ok := s.resolvePrincipal(req)
		return ok
	}

	if !authed(tok1) || !authed(tok2) {
		t.Fatal("both sessions should be valid before the password change")
	}

	// Carol changes her password -> token generation is bumped.
	newHash, err := hashPassword("carol-newer-password")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.users.setPassword("carol", newHash, false); err != nil {
		t.Fatal(err)
	}

	if authed(tok1) || authed(tok2) {
		t.Fatal("old sessions must be invalidated after the password change")
	}

	// A session minted after the change (new generation) is valid again.
	tok3, _, err := s.issueRoleSessionToken("", "carol", RoleOperator)
	if err != nil {
		t.Fatal(err)
	}
	if !authed(tok3) {
		t.Fatal("a session minted after the change should be valid")
	}
}

// TestSecurityResponseHeaders pins the security headers and CSP applied by the
// api-server's CORS/headers middleware to both API (JSON) and HTML doc pages.
func TestSecurityResponseHeaders(t *testing.T) {
	s := newDBServer(t)
	s.corsOrigin = "http://localhost:8080"
	s.startedAt = time.Now().UTC()
	ts := httptest.NewServer(s.buildHandler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	want := map[string]string{
		"X-Content-Type-Options":     "nosniff",
		"X-Frame-Options":            "DENY",
		"Referrer-Policy":            "no-referrer",
		"Cross-Origin-Opener-Policy": "same-origin",
		"Cache-Control":              "no-store",
		"Content-Security-Policy":    "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
	}
	for k, v := range want {
		if got := resp.Header.Get(k); got != v {
			t.Errorf("API header %s = %q, want %q", k, got, v)
		}
	}
	// The SPA must be able to send the double-submit CSRF token cross-origin.
	if got := resp.Header.Get("Access-Control-Allow-Headers"); !strings.Contains(got, "X-CSRF-Token") {
		t.Errorf("Access-Control-Allow-Headers missing X-CSRF-Token: %q", got)
	}

	// The Swagger UI page gets a scoped CSP that permits its bundle/styles.
	resp2, err := http.Get(ts.URL + "/docs/ui")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	csp := resp2.Header.Get("Content-Security-Policy")
	if !strings.Contains(csp, "script-src") || !strings.Contains(csp, "unpkg.com") {
		t.Errorf("/docs/ui CSP not scoped for assets: %q", csp)
	}
	if strings.Contains(csp, "form-action 'none'") {
		t.Errorf("/docs/ui should not get the strict API CSP: %q", csp)
	}
}
