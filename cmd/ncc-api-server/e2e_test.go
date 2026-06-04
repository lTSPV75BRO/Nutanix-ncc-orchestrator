package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestEndToEndFirstRunAdminFlow exercises the complete first-run experience
// against the real api-server handler stack (CORS + rate-limit + auth
// middleware + mux from buildHandler) over HTTP with a browser-like cookie
// jar: bootstrap admin -> login -> blocked by forced password change ->
// change password -> manage a user -> configure SSO -> logout.
func TestEndToEndFirstRunAdminFlow(t *testing.T) {
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
		authToken:      "admin-static-token",
		sessionSecret:  "test-session-secret-value",
		sessionTTL:     10 * time.Minute,
		sessionIssuer:  "ncc-api-server",
		users:          db,
		usersDBPath:    db.path,
		cookieInsecure: true, // httptest serves plain http
		corsOrigin:     "http://localhost:8080",
		startedAt:      time.Now().UTC(),
	}

	ts := httptest.NewServer(s.buildHandler())
	defer ts.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	baseURL, _ := url.Parse(ts.URL)

	csrfToken := func() string {
		for _, c := range jar.Cookies(baseURL) {
			if c.Name == csrfCookieName {
				return c.Value
			}
		}
		return ""
	}

	do := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		// Mimic the browser client: echo the readable CSRF cookie on mutations.
		if method != http.MethodGet && method != http.MethodHead {
			if tok := csrfToken(); tok != "" {
				req.Header.Set(csrfHeaderName, tok)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		var env map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&env)
		return resp.StatusCode, env
	}

	data := func(env map[string]any) map[string]any {
		d, _ := env["data"].(map[string]any)
		return d
	}

	// 1. Anonymous /me: login is enabled, local accounts present, not authed.
	code, env := do(http.MethodGet, "/api/v1/auth/me", "")
	if code != http.StatusOK {
		t.Fatalf("anon me: %d", code)
	}
	if d := data(env); d["authenticated"] != false || d["login_enabled"] != true || d["local_enabled"] != true {
		t.Fatalf("anon me data: %+v", d)
	}

	// 2. Login as the bootstrap admin -> session + csrf cookies, must-change set.
	code, env = do(http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"`+adminPW+`"}`)
	if code != http.StatusOK {
		t.Fatalf("login: %d (%+v)", code, env)
	}
	if data(env)["must_change_password"] != true {
		t.Fatalf("login must_change_password expected true: %+v", data(env))
	}
	if csrfToken() == "" {
		t.Fatal("login did not set a CSRF cookie")
	}

	// 3. /me now reflects the authenticated admin still owing a password change.
	code, env = do(http.MethodGet, "/api/v1/auth/me", "")
	if d := data(env); d["authenticated"] != true || d["role"] != "admin" || d["must_change_password"] != true {
		t.Fatalf("authed me data: %+v", d)
	}

	// 4. While must-change is set, any other endpoint is blocked.
	code, env = do(http.MethodGet, "/api/v1/runs", "")
	if code != http.StatusForbidden || env["error_code"] != "NCC_API_PASSWORD_CHANGE_REQUIRED" {
		t.Fatalf("expected forced-change block, got %d (%+v)", code, env)
	}

	// 5. A too-short new password is rejected.
	code, _ = do(http.MethodPost, "/api/v1/auth/change-password", `{"current_password":"`+adminPW+`","new_password":"short"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("short password: want 400, got %d", code)
	}

	// 6. Change the password successfully.
	code, env = do(http.MethodPost, "/api/v1/auth/change-password", `{"current_password":"`+adminPW+`","new_password":"a-brand-new-strong-password"}`)
	if code != http.StatusOK {
		t.Fatalf("change-password: %d (%+v)", code, env)
	}

	// 7. must-change cleared; previously-blocked endpoints are reachable.
	code, env = do(http.MethodGet, "/api/v1/auth/me", "")
	if data(env)["must_change_password"] != false {
		t.Fatalf("must_change should be cleared: %+v", data(env))
	}

	// 8. CSRF is enforced: a mutation without the header is rejected.
	noCSRF, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/settings/users", strings.NewReader(`{"username":"x","password":"password-1234","role":"viewer"}`))
	noCSRF.Header.Set("Content-Type", "application/json")
	respNoCSRF, err := client.Do(noCSRF)
	if err != nil {
		t.Fatal(err)
	}
	respNoCSRF.Body.Close()
	if respNoCSRF.StatusCode != http.StatusForbidden {
		t.Fatalf("create user without CSRF: want 403, got %d", respNoCSRF.StatusCode)
	}

	// 9. Admin creates an operator account (with CSRF).
	code, env = do(http.MethodPost, "/api/v1/settings/users", `{"username":"bob","password":"bob-strong-password","role":"operator"}`)
	if code != http.StatusOK {
		t.Fatalf("create user: %d (%+v)", code, env)
	}

	// 10. The new account shows up in the list without leaking its hash.
	code, env = do(http.MethodGet, "/api/v1/settings/users", "")
	if code != http.StatusOK {
		t.Fatalf("list users: %d", code)
	}
	users, _ := data(env)["users"].([]any)
	foundBob := false
	for _, u := range users {
		m, _ := u.(map[string]any)
		if m["username"] == "bob" {
			foundBob = true
			if _, leaked := m["password_hash"]; leaked {
				t.Fatal("list leaked password_hash")
			}
		}
	}
	if !foundBob {
		t.Fatalf("created user not listed: %+v", users)
	}

	// 11. Configure SAML at runtime; SP keypair is generated server-side.
	idpXML := `<EntityDescriptor xmlns=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"https://idp.example.com/metadata\">` +
		`<IDPSSODescriptor protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">` +
		`<SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\" Location=\"https://idp.example.com/sso\"/>` +
		`</IDPSSODescriptor></EntityDescriptor>`
	code, env = do(http.MethodPut, "/api/v1/settings/sso", `{"enabled":true,"root_url":"https://ncc.example.com","idp_metadata_xml":"`+idpXML+`","role_attribute":"Role","role_map":"ncc-admins=admin","default_role":"viewer"}`)
	if code != http.StatusOK {
		t.Fatalf("configure sso: %d (%+v)", code, env)
	}

	// 12. GET SSO exposes the SP metadata URL but never the private key.
	code, env = do(http.MethodGet, "/api/v1/settings/sso", "")
	d := data(env)
	if d["enabled"] != true {
		t.Fatalf("sso should be enabled: %+v", d)
	}
	if _, ok := d["sp_key_pem"]; ok {
		t.Fatal("GET sso must not expose sp_key_pem")
	}
	if spURL, _ := d["sp_metadata_url"].(string); !strings.HasSuffix(spURL, "/saml/metadata") {
		t.Fatalf("unexpected sp_metadata_url: %v", d["sp_metadata_url"])
	}

	// 13. Logout clears the session.
	code, _ = do(http.MethodPost, "/api/v1/auth/logout", "")
	if code != http.StatusOK {
		t.Fatalf("logout: %d", code)
	}
	code, env = do(http.MethodGet, "/api/v1/auth/me", "")
	if data(env)["authenticated"] != false {
		t.Fatalf("after logout, expected unauthenticated: %+v", data(env))
	}
}
