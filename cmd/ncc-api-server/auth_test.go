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

func TestRouteMinRole(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   Role
	}{
		{http.MethodGet, "/api/v1/runs", RoleViewer},
		{http.MethodGet, "/api/v1/report/data", RoleViewer},
		{http.MethodGet, "/api/v1/settings/config", RoleAdmin},
		{http.MethodPut, "/api/v1/settings/config", RoleAdmin},
		{http.MethodPost, "/api/v1/auth/rotate", RoleAdmin},
		{http.MethodPost, "/api/v1/runs/trigger", RoleOperator},
		{http.MethodPost, "/api/v1/runs/preflight", RoleOperator},
		{http.MethodDelete, "/api/v1/runs/active", RoleOperator},
		{http.MethodDelete, "/api/v1/runs/abc123", RoleOperator},
		// Operator-accessible operational endpoints (expanded scope).
		{http.MethodGet, "/api/v1/schedule", RoleViewer},
		{http.MethodPut, "/api/v1/schedule", RoleOperator},
		{http.MethodGet, "/api/v1/settings/clusters", RoleOperator},
		{http.MethodGet, "/api/v1/settings/cluster-groups", RoleOperator},
		{http.MethodPut, "/api/v1/settings/cluster-groups", RoleAdmin},
		{http.MethodPost, "/api/v1/settings/notifications/test", RoleOperator},
		// Secret-bearing settings reads stay admin-only.
		{http.MethodGet, "/api/v1/settings/notifications", RoleAdmin},
		{http.MethodGet, "/api/v1/settings/users", RoleAdmin},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.path, nil)
		if got := routeMinRole(r); got != c.want {
			t.Errorf("routeMinRole(%s %s) = %v, want %v", c.method, c.path, got, c.want)
		}
	}
}

func TestParseRole(t *testing.T) {
	for _, c := range []struct {
		in   string
		want Role
		ok   bool
	}{
		{"admin", RoleAdmin, true},
		{"ADMIN", RoleAdmin, true},
		{"operator", RoleOperator, true},
		{"viewer", RoleViewer, true},
		{"read-only", RoleViewer, true},
		{"", RoleNone, false},
		{"superuser", RoleNone, false},
	} {
		got, ok := parseRole(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("parseRole(%q) = (%v,%v), want (%v,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
	if RoleViewer >= RoleOperator || RoleOperator >= RoleAdmin {
		t.Fatal("role ordering must be viewer < operator < admin")
	}
}

func TestUserStoreVerify(t *testing.T) {
	hash, err := hashPassword("s3cret-pass")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "users.yaml")
	content := "users:\n" +
		"  - username: alice\n" +
		"    password_hash: \"" + hash + "\"\n" +
		"    role: admin\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := loadUserStore(path)
	if err != nil {
		t.Fatalf("loadUserStore: %v", err)
	}
	if store.count() != 1 {
		t.Fatalf("want 1 user, got %d", store.count())
	}
	if role, ok, _ := store.verify("alice", "s3cret-pass"); !ok || role != RoleAdmin {
		t.Fatalf("verify good password: ok=%v role=%v", ok, role)
	}
	if _, ok, _ := store.verify("alice", "wrong"); ok {
		t.Fatal("verify wrong password should fail")
	}
	if _, ok, _ := store.verify("ALICE", "s3cret-pass"); !ok {
		t.Fatal("username lookup should be case-insensitive")
	}
	if _, ok, _ := store.verify("ghost", "whatever"); ok {
		t.Fatal("unknown user should fail")
	}
}

func TestLoadUserStoreRejectsBadRole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.yaml")
	content := "users:\n  - username: bob\n    password_hash: \"" + dummyBcryptHash + "\"\n    role: wizard\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadUserStore(path); err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func newLoginServer(t *testing.T) *apiServer {
	t.Helper()
	hash, err := hashPassword("pw-admin")
	if err != nil {
		t.Fatal(err)
	}
	ophash, err := hashPassword("pw-op")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "users.yaml")
	content := "users:\n" +
		"  - username: admin1\n    password_hash: \"" + hash + "\"\n    role: admin\n" +
		"  - username: op1\n    password_hash: \"" + ophash + "\"\n    role: operator\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := loadUserStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return &apiServer{
		authMode:       "hybrid",
		authToken:      "admintoken",
		sessionSecret:  "test-session-secret",
		sessionTTL:     10 * time.Minute,
		sessionIssuer:  "ncc-api-server",
		users:          store,
		cookieInsecure: true,
	}
}

func TestHandleLoginAndSessionRole(t *testing.T) {
	s := newLoginServer(t)

	// Login as operator.
	body := strings.NewReader(`{"username":"op1","password":"pw-op"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", body)
	rr := httptest.NewRecorder()
	s.handleLogin(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("login: want 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	var sessionCookie, csrfCookie *http.Cookie
	for _, c := range rr.Result().Cookies() {
		switch c.Name {
		case sessionCookieName:
			sessionCookie = c
		case csrfCookieName:
			csrfCookie = c
		}
	}
	if sessionCookie == nil || csrfCookie == nil {
		t.Fatal("login must set session and csrf cookies")
	}
	if !sessionCookie.HttpOnly {
		t.Error("session cookie must be HttpOnly")
	}
	if csrfCookie.HttpOnly {
		t.Error("csrf cookie must be readable by JS (not HttpOnly)")
	}

	// The session must encode the operator role.
	claims, err := s.verifySession(sessionCookie.Value, "")
	if err != nil {
		t.Fatalf("verifySession: %v", err)
	}
	if claims.Role != "operator" || claims.Sub != "op1" {
		t.Fatalf("claims role/sub = %q/%q, want operator/op1", claims.Role, claims.Sub)
	}

	// Bad password is rejected.
	rr2 := httptest.NewRecorder()
	s.handleLogin(rr2, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"op1","password":"nope"}`)))
	if rr2.Code != http.StatusUnauthorized {
		t.Fatalf("bad password: want 401, got %d", rr2.Code)
	}
}

func TestWithAuthCookieSessionRBACAndCSRF(t *testing.T) {
	s := newLoginServer(t)
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Mint an operator cookie session directly.
	token, _, err := s.issueRoleSessionToken("", "op1", RoleOperator)
	if err != nil {
		t.Fatal(err)
	}
	csrf := "csrf-token-value"

	withCookies := func(req *http.Request, withCSRF bool) *httptest.ResponseRecorder {
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
		req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: csrf})
		if withCSRF {
			req.Header.Set(csrfHeaderName, csrf)
		}
		rr := httptest.NewRecorder()
		next.ServeHTTP(rr, req)
		return rr
	}

	// Operator GET (viewer-level) is allowed without CSRF.
	if rr := withCookies(httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil), false); rr.Code != http.StatusOK {
		t.Fatalf("operator GET /runs: want 200, got %d", rr.Code)
	}
	// Operator mutating request without CSRF header is rejected.
	if rr := withCookies(httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil), false); rr.Code != http.StatusForbidden {
		t.Fatalf("operator POST /trigger without CSRF: want 403, got %d", rr.Code)
	}
	// With CSRF header it succeeds.
	if rr := withCookies(httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil), true); rr.Code != http.StatusOK {
		t.Fatalf("operator POST /trigger with CSRF: want 200, got %d", rr.Code)
	}
	// Operator cannot reach admin-only settings even with CSRF.
	if rr := withCookies(httptest.NewRequest(http.MethodGet, "/api/v1/settings/config", nil), true); rr.Code != http.StatusForbidden {
		t.Fatalf("operator GET /settings: want 403, got %d", rr.Code)
	}
}

func TestStaticAdminTokenExemptFromCSRF(t *testing.T) {
	s := newLoginServer(t)
	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	// Automation using the static admin token can mutate without a CSRF token.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil)
	req.Header.Set("X-API-Token", "admintoken")
	rr := httptest.NewRecorder()
	next.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin token POST /trigger: want 200, got %d", rr.Code)
	}
}

func TestHandleMeAnonymousAndAuthenticated(t *testing.T) {
	s := newLoginServer(t)

	rr := httptest.NewRecorder()
	s.handleMe(rr, httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil))
	var env struct {
		Data struct {
			Authenticated bool   `json:"authenticated"`
			LoginEnabled  bool   `json:"login_enabled"`
			Role          string `json:"role"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Data.Authenticated {
		t.Error("anonymous /me must report not authenticated")
	}
	if !env.Data.LoginEnabled {
		t.Error("/me must report login_enabled when users configured")
	}

	// Authenticated via admin token.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	req.Header.Set("X-API-Token", "admintoken")
	rr2 := httptest.NewRecorder()
	s.handleMe(rr2, req)
	var env2 struct {
		Data struct {
			Authenticated bool   `json:"authenticated"`
			Role          string `json:"role"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr2.Body.Bytes(), &env2); err != nil {
		t.Fatal(err)
	}
	if !env2.Data.Authenticated || env2.Data.Role != "admin" {
		t.Fatalf("admin /me = auth:%v role:%q, want true/admin", env2.Data.Authenticated, env2.Data.Role)
	}
}

func TestParseRoleMap(t *testing.T) {
	m, err := parseRoleMap("ncc-admins=admin, ncc-ops=operator,readers=viewer")
	if err != nil {
		t.Fatalf("parseRoleMap: %v", err)
	}
	if m["ncc-admins"] != RoleAdmin || m["ncc-ops"] != RoleOperator || m["readers"] != RoleViewer {
		t.Fatalf("unexpected role map: %+v", m)
	}
	if _, err := parseRoleMap("bad-entry"); err == nil {
		t.Fatal("expected error for malformed mapping")
	}
}

func TestSAMLRoleFromValues(t *testing.T) {
	p := &samlProvider{
		roleMap:     map[string]Role{"ncc-admins": RoleAdmin, "ncc-ops": RoleOperator},
		defaultRole: RoleViewer,
	}
	if got := p.roleFromValues([]string{"ncc-ops", "ncc-admins"}); got != RoleAdmin {
		t.Errorf("highest role wins: got %v", got)
	}
	if got := p.roleFromValues([]string{"ncc-ops"}); got != RoleOperator {
		t.Errorf("operator mapping: got %v", got)
	}
	if got := p.roleFromValues([]string{"unmapped-group"}); got != RoleViewer {
		t.Errorf("default role fallback: got %v", got)
	}
	if got := p.roleFromValues(nil); got != RoleViewer {
		t.Errorf("nil values default: got %v", got)
	}
}
