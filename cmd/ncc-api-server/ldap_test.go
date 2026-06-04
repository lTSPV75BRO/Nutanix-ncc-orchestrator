package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseLDAPRoleMap(t *testing.T) {
	// DN keys contain "=" and "," so the role is read off the LAST "=", and
	// entries are separated by newlines/semicolons (not commas).
	raw := "CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin\n" +
		"CN=NCC-Operators,OU=Groups,DC=corp,DC=example,DC=com=operator; readers=viewer"
	m, err := parseLDAPRoleMap(raw)
	if err != nil {
		t.Fatalf("parseLDAPRoleMap: %v", err)
	}
	if got := m["cn=ncc-admins,ou=groups,dc=corp,dc=example,dc=com"]; got != RoleAdmin {
		t.Errorf("admin DN mapping: got %v", got)
	}
	if got := m["cn=ncc-operators,ou=groups,dc=corp,dc=example,dc=com"]; got != RoleOperator {
		t.Errorf("operator DN mapping: got %v", got)
	}
	if got := m["readers"]; got != RoleViewer {
		t.Errorf("CN-only mapping: got %v", got)
	}
	if _, err := parseLDAPRoleMap("no-equals-sign"); err == nil {
		t.Fatal("expected error for malformed mapping")
	}
	if _, err := parseLDAPRoleMap("group=notarole"); err == nil {
		t.Fatal("expected error for invalid role")
	}
}

func TestLDAPConfiguredAndProviderBuild(t *testing.T) {
	if (&ldapPersisted{Enabled: true}).configured() {
		t.Error("config with no URL/base should not be configured")
	}
	if (&ldapPersisted{URL: "ldaps://x", BaseDN: "dc=x"}).configured() {
		t.Error("disabled config should not be configured")
	}
	cfg := &ldapPersisted{Enabled: true, URL: "ldaps://dc1:636", BaseDN: "DC=corp,DC=com"}
	if !cfg.configured() {
		t.Fatal("complete config should be configured")
	}
	p, enabled, err := buildLDAPProvider(cfg)
	if err != nil || !enabled || p == nil {
		t.Fatalf("buildLDAPProvider: enabled=%v err=%v", enabled, err)
	}
	// Defaults are applied.
	if p.userFilter != defaultLDAPUserFilter || p.usernameAttr != defaultLDAPUsernameAttr || p.groupAttr != defaultLDAPGroupAttr {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestLDAPRoleFromGroups(t *testing.T) {
	cfg := &ldapPersisted{
		Enabled:     true,
		URL:         "ldaps://dc1:636",
		BaseDN:      "DC=corp,DC=com",
		RoleMapRaw:  "CN=NCC-Admins,OU=Groups,DC=corp,DC=com=admin\nNCC-Ops=operator",
		DefaultRole: "viewer",
	}
	p, _, err := buildLDAPProvider(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Full-DN match (case-insensitive), highest role wins.
	groups := []string{
		"CN=NCC-Ops,OU=Groups,DC=corp,DC=com",
		"cn=ncc-admins,ou=groups,dc=corp,dc=com",
	}
	if got := p.roleFromGroups(groups); got != RoleAdmin {
		t.Errorf("highest role wins: got %v", got)
	}
	// CN-only mapping should match a full DN by its extracted CN.
	if got := p.roleFromGroups([]string{"CN=NCC-Ops,OU=Groups,DC=corp,DC=com"}); got != RoleOperator {
		t.Errorf("CN match within DN: got %v", got)
	}
	// No match falls back to the default role.
	if got := p.roleFromGroups([]string{"CN=Nobody,DC=corp,DC=com"}); got != RoleViewer {
		t.Errorf("default fallback: got %v", got)
	}
}

func TestLDAPEmptyPasswordRejected(t *testing.T) {
	p, _, err := buildLDAPProvider(&ldapPersisted{Enabled: true, URL: "ldaps://dc1:636", BaseDN: "DC=corp,DC=com"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// An empty password must be rejected before any network bind (no dial),
	// otherwise LDAP would treat it as an anonymous bind and falsely succeed.
	role, _, ok, err := p.authenticate("jdoe", "")
	if ok || err != nil || role != RoleNone {
		t.Fatalf("empty password should be rejected without error: role=%v ok=%v err=%v", role, ok, err)
	}
	role, _, ok, err = p.authenticate("", "secret")
	if ok || err != nil || role != RoleNone {
		t.Fatalf("empty username should be rejected: role=%v ok=%v err=%v", role, ok, err)
	}
}

// fakeLDAP is a stand-in authenticator for login-dispatch tests.
type fakeLDAP struct {
	role      Role
	canonical string
	ok        bool
	err       error
	called    bool
	lastUser  string
	lastPass  string
}

func (f *fakeLDAP) authenticate(user, pass string) (Role, string, bool, error) {
	f.called = true
	f.lastUser = user
	f.lastPass = pass
	return f.role, f.canonical, f.ok, f.err
}

func newLDAPLoginTestServer(t *testing.T, fake *fakeLDAP) (*apiServer, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := openUserDB(filepath.Join(dir, "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	adminPW, created, err := db.bootstrapAdminIfEmpty("admin")
	if err != nil || !created {
		t.Fatalf("bootstrap admin: created=%v err=%v", created, err)
	}
	s := &apiServer{
		authMode:       "hybrid",
		sessionSecret:  "test-session-secret-value",
		sessionTTL:     10 * time.Minute,
		sessionIssuer:  "ncc-api-server",
		users:          db,
		usersDBPath:    db.path,
		cookieInsecure: true,
		startedAt:      time.Now().UTC(),
		ldap:           fake,
		ldapEnabled:    true,
	}
	return s, adminPW
}

func loginRequest(t *testing.T, s *apiServer, username, password string) (int, map[string]any) {
	t.Helper()
	body := `{"username":"` + username + `","password":"` + password + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.handleLogin(rr, req)
	var env struct {
		Success bool           `json:"success"`
		Data    map[string]any `json:"data"`
		Error   string         `json:"error"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	out := env.Data
	if out == nil {
		out = map[string]any{}
	}
	out["__error"] = env.Error
	return rr.Code, out
}

func TestHandleLoginLocalFirstThenLDAP(t *testing.T) {
	t.Run("local account wins, LDAP not consulted", func(t *testing.T) {
		fake := &fakeLDAP{role: RoleAdmin, canonical: "admin", ok: true}
		s, adminPW := newLDAPLoginTestServer(t, fake)
		code, data := loginRequest(t, s, "admin", adminPW)
		if code != http.StatusOK {
			t.Fatalf("local login code=%d data=%+v", code, data)
		}
		if fake.called {
			t.Fatal("LDAP should not be called when the local account authenticates")
		}
		if data["role"] != "admin" {
			t.Fatalf("role=%v", data["role"])
		}
	})

	t.Run("AD fallback succeeds with mapped role", func(t *testing.T) {
		fake := &fakeLDAP{role: RoleOperator, canonical: "jdoe", ok: true}
		s, _ := newLDAPLoginTestServer(t, fake)
		code, data := loginRequest(t, s, "jdoe", "ad-password")
		if code != http.StatusOK {
			t.Fatalf("AD login code=%d data=%+v", code, data)
		}
		if !fake.called || fake.lastUser != "jdoe" || fake.lastPass != "ad-password" {
			t.Fatalf("LDAP not invoked correctly: %+v", fake)
		}
		if data["role"] != "operator" || data["username"] != "jdoe" {
			t.Fatalf("expected operator/jdoe, got %+v", data)
		}
		// AD users have no local password account; must_change stays false.
		if data["must_change_password"] != false {
			t.Fatalf("must_change_password=%v", data["must_change_password"])
		}
	})

	t.Run("AD rejection returns 401", func(t *testing.T) {
		fake := &fakeLDAP{ok: false}
		s, _ := newLDAPLoginTestServer(t, fake)
		code, _ := loginRequest(t, s, "jdoe", "wrong")
		if code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", code)
		}
	})

	t.Run("AD operational error returns generic 401", func(t *testing.T) {
		fake := &fakeLDAP{err: errLDAPTest}
		s, _ := newLDAPLoginTestServer(t, fake)
		code, data := loginRequest(t, s, "jdoe", "pw")
		if code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", code)
		}
		if msg, _ := data["__error"].(string); msg != "invalid username or password" {
			t.Fatalf("error should be generic, got %q", msg)
		}
	})
}

var errLDAPTest = errLDAP("dial tcp: connection refused")

type errLDAP string

func (e errLDAP) Error() string { return string(e) }
