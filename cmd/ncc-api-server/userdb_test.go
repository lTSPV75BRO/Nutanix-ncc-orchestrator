package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newDBServer(t *testing.T) *apiServer {
	t.Helper()
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	return &apiServer{
		authMode:       "hybrid",
		authToken:      "admintoken",
		sessionSecret:  "secret-secret",
		sessionTTL:     10 * time.Minute,
		sessionIssuer:  "ncc-api-server",
		users:          db,
		usersDBPath:    db.path,
		cookieInsecure: true,
	}
}

func TestSessionPolicyPersistAndEffectiveTTL(t *testing.T) {
	s := newDBServer(t)

	// With no override, the effective TTL is the server's --session-ttl flag.
	if got := s.effectiveSessionTTL(); got != 10*time.Minute {
		t.Fatalf("default effective TTL = %v, want 10m", got)
	}

	// PUT a runtime override via the admin API and confirm it is honored.
	body := strings.NewReader(`{"ttl_min":120}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings/session", body)
	rr := httptest.NewRecorder()
	s.handleSessionPolicy(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT session policy: got %d (%s)", rr.Code, rr.Body.String())
	}
	if got := s.effectiveSessionTTL(); got != 2*time.Hour {
		t.Fatalf("effective TTL after override = %v, want 2h", got)
	}

	// The override is persisted, so a freshly issued token expires ~2h out.
	_, exp, err := s.issueRoleSessionToken("127.0.0.1", "admin", RoleAdmin)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(exp); d < 110*time.Minute || d > 130*time.Minute {
		t.Fatalf("issued token TTL = %v, want ~2h", d)
	}

	// Reopen the store and confirm the policy round-trips.
	db2, err := openUserDB(s.users.path)
	if err != nil {
		t.Fatal(err)
	}
	if pol := db2.getSessionPolicy(); pol == nil || pol.TTLSeconds != 7200 {
		t.Fatalf("session policy did not round-trip: %+v", pol)
	}

	// Out-of-range values are rejected.
	rrBad := httptest.NewRecorder()
	s.handleSessionPolicy(rrBad, httptest.NewRequest(http.MethodPut, "/api/v1/settings/session", strings.NewReader(`{"ttl_sec":5}`)))
	if rrBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for below-min TTL, got %d", rrBad.Code)
	}

	// ttl_sec:0 clears the override and restores the server default.
	rrClear := httptest.NewRecorder()
	s.handleSessionPolicy(rrClear, httptest.NewRequest(http.MethodPut, "/api/v1/settings/session", strings.NewReader(`{"ttl_sec":0}`)))
	if rrClear.Code != http.StatusOK {
		t.Fatalf("clear session policy: got %d (%s)", rrClear.Code, rrClear.Body.String())
	}
	if got := s.effectiveSessionTTL(); got != 10*time.Minute {
		t.Fatalf("effective TTL after clear = %v, want 10m", got)
	}
}

func TestBootstrapAdminAndPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.json")
	db, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	pw, created, err := db.bootstrapAdminIfEmpty("admin")
	if err != nil || !created {
		t.Fatalf("bootstrap: created=%v err=%v", created, err)
	}
	if len(pw) < 16 {
		t.Fatalf("bootstrap password too short: %q", pw)
	}
	// Second call is a no-op.
	if _, created2, _ := db.bootstrapAdminIfEmpty("admin"); created2 {
		t.Fatal("bootstrap should not recreate admin")
	}
	// The admin must be flagged must-change and the file persisted.
	role, ok, mustChange := db.verify("admin", pw)
	if !ok || role != RoleAdmin || !mustChange {
		t.Fatalf("verify bootstrap admin: ok=%v role=%v mustChange=%v", ok, role, mustChange)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("users db file not persisted: %v", err)
	}
	// Reopen and confirm it round-trips.
	db2, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, mc := db2.verify("admin", pw); !ok || !mc {
		t.Fatalf("reopened db verify failed: ok=%v mustChange=%v", ok, mc)
	}
}

func TestForcedPasswordChangeFlow(t *testing.T) {
	s := newDBServer(t)
	pw, _, err := s.users.bootstrapAdminIfEmpty("admin")
	if err != nil {
		t.Fatal(err)
	}

	next := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Login as the bootstrap admin -> session cookie, must_change=true.
	rr := httptest.NewRecorder()
	s.handleLogin(rr, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"`+pw+`"}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("login: %d (%s)", rr.Code, rr.Body.String())
	}
	var sessionCookie, csrfCookie *http.Cookie
	for _, c := range rr.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionCookie = c
		}
		if c.Name == csrfCookieName {
			csrfCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("no session cookie")
	}

	// While must-change is set, a normal endpoint is blocked.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.AddCookie(sessionCookie)
	rrBlocked := httptest.NewRecorder()
	next.ServeHTTP(rrBlocked, req)
	if rrBlocked.Code != http.StatusForbidden {
		t.Fatalf("expected 403 while must-change, got %d", rrBlocked.Code)
	}

	// Change the password (cookie session + CSRF).
	chReq := httptest.NewRequest(http.MethodPost, "/api/v1/auth/change-password", strings.NewReader(`{"current_password":"`+pw+`","new_password":"brand-new-passw0rd"}`))
	chReq.AddCookie(sessionCookie)
	chReq.AddCookie(csrfCookie)
	chReq.Header.Set(csrfHeaderName, csrfCookie.Value)
	rrCh := httptest.NewRecorder()
	s.handleChangePassword(rrCh, chReq)
	if rrCh.Code != http.StatusOK {
		t.Fatalf("change-password: %d (%s)", rrCh.Code, rrCh.Body.String())
	}
	// The password change bumps the token generation, so the change-password
	// response re-issues a fresh session cookie (the browser swaps it in).
	var newSessionCookie *http.Cookie
	for _, c := range rrCh.Result().Cookies() {
		if c.Name == sessionCookieName && strings.TrimSpace(c.Value) != "" {
			newSessionCookie = c
		}
	}
	if newSessionCookie == nil {
		t.Fatal("change-password did not re-issue a session cookie")
	}

	// The OLD session cookie is now invalidated (every other device is signed out).
	reqOld := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	reqOld.AddCookie(sessionCookie)
	rrOld := httptest.NewRecorder()
	next.ServeHTTP(rrOld, reqOld)
	if rrOld.Code != http.StatusUnauthorized {
		t.Fatalf("expected old session to be invalidated (401), got %d", rrOld.Code)
	}

	// must-change cleared; the re-issued cookie works.
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req2.AddCookie(newSessionCookie)
	rrOK := httptest.NewRecorder()
	next.ServeHTTP(rrOK, req2)
	if rrOK.Code != http.StatusOK {
		t.Fatalf("expected 200 after password change, got %d", rrOK.Code)
	}
	// Old password no longer verifies.
	if _, ok, _ := s.users.verify("admin", pw); ok {
		t.Fatal("old password should no longer verify")
	}
}

func TestUserCRUDAndLastAdminProtection(t *testing.T) {
	s := newDBServer(t)
	if _, _, err := s.users.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}

	create := func(payload string) int {
		rr := httptest.NewRecorder()
		s.handleUsers(rr, httptest.NewRequest(http.MethodPost, "/api/v1/settings/users", strings.NewReader(payload)))
		return rr.Code
	}
	if code := create(`{"username":"bob","password":"bob-password-123","role":"operator"}`); code != http.StatusOK {
		t.Fatalf("create bob: %d", code)
	}
	if code := create(`{"username":"bob","password":"bob-password-123","role":"operator"}`); code != http.StatusConflict {
		t.Fatalf("duplicate create: want 409, got %d", code)
	}
	if code := create(`{"username":"weak","password":"short","role":"viewer"}`); code != http.StatusBadRequest {
		t.Fatalf("weak password: want 400, got %d", code)
	}

	// List should show admin + bob, no hashes.
	rrList := httptest.NewRecorder()
	s.handleUsers(rrList, httptest.NewRequest(http.MethodGet, "/api/v1/settings/users", nil))
	if !strings.Contains(rrList.Body.String(), "bob") || strings.Contains(rrList.Body.String(), "password_hash") {
		t.Fatalf("list unexpected: %s", rrList.Body.String())
	}

	// Deleting the built-in admin is refused (reserved account).
	rrDel := httptest.NewRecorder()
	s.handleUserByName(rrDel, httptest.NewRequest(http.MethodDelete, "/api/v1/settings/users/admin", nil))
	if rrDel.Code != http.StatusConflict {
		t.Fatalf("delete admin: want 409, got %d (%s)", rrDel.Code, rrDel.Body.String())
	}

	// Promote bob to admin; the built-in admin still cannot be deleted or
	// demoted even with another admin present.
	rrRole := httptest.NewRecorder()
	s.handleUserByName(rrRole, httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/bob", strings.NewReader(`{"role":"admin"}`)))
	if rrRole.Code != http.StatusOK {
		t.Fatalf("promote bob: %d", rrRole.Code)
	}
	rrDel2 := httptest.NewRecorder()
	s.handleUserByName(rrDel2, httptest.NewRequest(http.MethodDelete, "/api/v1/settings/users/admin", nil))
	if rrDel2.Code != http.StatusConflict {
		t.Fatalf("delete reserved admin (second admin present): want 409, got %d (%s)", rrDel2.Code, rrDel2.Body.String())
	}
	rrDemote := httptest.NewRecorder()
	s.handleUserByName(rrDemote, httptest.NewRequest(http.MethodPut, "/api/v1/settings/users/admin", strings.NewReader(`{"role":"viewer"}`)))
	if rrDemote.Code != http.StatusConflict {
		t.Fatalf("demote reserved admin: want 409, got %d (%s)", rrDemote.Code, rrDemote.Body.String())
	}

	// A non-reserved admin (bob) can still be deleted normally.
	rrDelBob := httptest.NewRecorder()
	s.handleUserByName(rrDelBob, httptest.NewRequest(http.MethodDelete, "/api/v1/settings/users/bob", nil))
	if rrDelBob.Code != http.StatusOK {
		t.Fatalf("delete bob: %d (%s)", rrDelBob.Code, rrDelBob.Body.String())
	}
}

func TestReservedAdminRoleImmutable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "users.json")
	db, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}
	// A second admin exists, so last-admin protection is NOT what enforces this.
	hash, err := hashPassword("second-admin-pw-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.upsertUser("carol", hash, RoleAdmin, false); err != nil {
		t.Fatal(err)
	}

	// The built-in admin can never be demoted (case-insensitive)...
	if err := db.setRole("admin", RoleViewer); !errors.Is(err, errReservedAdminRole) {
		t.Fatalf("setRole(admin, viewer): want errReservedAdminRole, got %v", err)
	}
	if err := db.setRole("ADMIN", RoleOperator); !errors.Is(err, errReservedAdminRole) {
		t.Fatalf("setRole(ADMIN, operator): want errReservedAdminRole, got %v", err)
	}
	// ...nor deleted.
	if err := db.deleteUser("admin"); !errors.Is(err, errReservedAdminDelete) {
		t.Fatalf("deleteUser(admin): want errReservedAdminDelete, got %v", err)
	}
	// Setting it (back) to admin remains a no-op success.
	if err := db.setRole("admin", RoleAdmin); err != nil {
		t.Fatalf("setRole(admin, admin): %v", err)
	}

	// upsert cannot create/replace "admin" with a non-admin role.
	if err := db.upsertUser("Admin", hash, RoleViewer, false); err != nil {
		t.Fatal(err)
	}
	if a, ok := db.lookup("admin"); !ok || a.Role != RoleAdmin.String() {
		t.Fatalf("upsert reserved admin role = %q, want admin", a.Role)
	}

	// Load coercion: a tampered store that demoted admin is corrected on open.
	tampered := usersDBFile{Users: []account{{Username: "admin", PasswordHash: hash, Role: "viewer"}}}
	b, err := json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := openUserDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := reopened.lookup("admin"); !ok || a.Role != RoleAdmin.String() {
		t.Fatalf("coerced admin role on load = %q, want admin", a.Role)
	}
}

func TestSSOConfigPersistAndCertGeneration(t *testing.T) {
	s := newDBServer(t)
	// Persist a SAML config with an inline (well-formed minimal) IdP metadata.
	idpXML := `<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/metadata">` +
		`<IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">` +
		`<SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>` +
		`</IDPSSODescriptor></EntityDescriptor>`
	body, _ := json.Marshal(map[string]interface{}{
		"enabled":          true,
		"root_url":         "https://ncc.example.com",
		"idp_metadata_xml": idpXML,
		"role_attribute":   "Role",
		"role_map":         "ncc-admins=admin",
		"default_role":     "viewer",
	})
	rr := httptest.NewRecorder()
	s.handleSSO(rr, httptest.NewRequest(http.MethodPut, "/api/v1/settings/sso", strings.NewReader(string(body))))
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT sso: %d (%s)", rr.Code, rr.Body.String())
	}
	if !s.samlIsEnabled() {
		t.Fatal("SAML should be enabled after PUT")
	}
	// SP keypair generated and persisted; private key never returned by GET.
	cfg := s.users.getSAML()
	if cfg == nil || cfg.SPKeyPEM == "" || cfg.SPCertPEM == "" {
		t.Fatal("SP keypair was not generated/persisted")
	}
	rrGet := httptest.NewRecorder()
	s.handleSSO(rrGet, httptest.NewRequest(http.MethodGet, "/api/v1/settings/sso", nil))
	if strings.Contains(rrGet.Body.String(), "sp_key_pem") || strings.Contains(rrGet.Body.String(), cfg.SPKeyPEM) {
		t.Fatal("GET sso must not expose the SP private key")
	}
	if !strings.Contains(rrGet.Body.String(), "/saml/metadata") {
		t.Fatalf("GET sso should expose sp_metadata_url: %s", rrGet.Body.String())
	}
}
