package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// fakeIdP is a minimal OpenID provider: discovery, JWKS, and a token endpoint
// that enforces PKCE and returns an RS256-signed ID token.
type fakeIdP struct {
	t      *testing.T
	srv    *httptest.Server
	key    *rsa.PrivateKey
	mu     sync.Mutex
	codes  map[string]fakeGrant // code -> grant
	claims map[string]interface{}
	// tamper lets a test corrupt the ID token (wrong nonce, audience, ...).
	tamper func(map[string]interface{})
}

type fakeGrant struct {
	challenge string
	nonce     string
	clientID  string
}

func newFakeIdP(t *testing.T) *fakeIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeIdP{t: t, key: key, codes: map[string]fakeGrant{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"issuer":                                f.srv.URL,
			"authorization_endpoint":                f.srv.URL + "/authorize",
			"token_endpoint":                        f.srv.URL + "/token",
			"jwks_uri":                              f.srv.URL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"code_challenge_methods_supported":      []string{"S256"},
			"scopes_supported":                      []string{"openid", "profile", "email", "groups"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.mu.Lock()
		g, ok := f.codes[r.Form.Get("code")]
		delete(f.codes, r.Form.Get("code"))
		f.mu.Unlock()
		sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if !ok || base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		claims := map[string]interface{}{
			"iss":   f.srv.URL,
			"aud":   g.clientID,
			"sub":   "user-123",
			"exp":   time.Now().Add(5 * time.Minute).Unix(),
			"iat":   time.Now().Unix(),
			"nonce": g.nonce,
		}
		for k, v := range f.claims {
			claims[k] = v
		}
		if f.tamper != nil {
			f.tamper(claims)
		}
		signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: f.key}, (&jose.SignerOptions{}).WithHeader("kid", "k1"))
		if err != nil {
			t.Error(err)
			return
		}
		idt, err := jwt.Signed(signer).Claims(claims).Serialize()
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "at", "token_type": "Bearer", "expires_in": 300, "id_token": idt})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// authorize plays the IdP's login page: it accepts the redirect our server
// produced and returns the callback URL the browser would be sent to.
func (f *fakeIdP) authorize(t *testing.T, authURL string) (callback *url.URL) {
	t.Helper()
	u, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" {
		t.Fatalf("authorization request lacks PKCE: %s", authURL)
	}
	if q.Get("nonce") == "" || q.Get("state") == "" {
		t.Fatalf("authorization request lacks state/nonce: %s", authURL)
	}
	if !strings.Contains(q.Get("scope"), "openid") {
		t.Fatalf("scope %q lacks openid", q.Get("scope"))
	}
	code := "code-" + q.Get("state")[:8]
	f.mu.Lock()
	f.codes[code] = fakeGrant{challenge: q.Get("code_challenge"), nonce: q.Get("nonce"), clientID: q.Get("client_id")}
	f.mu.Unlock()
	cb, err := url.Parse(q.Get("redirect_uri"))
	if err != nil {
		t.Fatal(err)
	}
	cq := cb.Query()
	cq.Set("code", code)
	cq.Set("state", q.Get("state"))
	cb.RawQuery = cq.Encode()
	return cb
}

func newOIDCServer(t *testing.T, idp *fakeIdP, mutate func(*oidcPersisted)) *apiServer {
	t.Helper()
	s := newDBServer(t)
	cfg := &oidcPersisted{
		Enabled:      true,
		IssuerURL:    idp.srv.URL,
		ClientID:     "ncc-client",
		ClientSecret: "client-secret",
		RootURL:      "http://127.0.0.1:8080",
		RoleMapRaw:   "ncc-admins=admin,ncc-ops=operator",
		ButtonLabel:  "Test IdP",
	}
	if mutate != nil {
		mutate(cfg)
	}
	if err := s.users.setOIDC(cfg); err != nil {
		t.Fatal(err)
	}
	return s
}

// runOIDCLogin drives login -> IdP -> callback and returns the final response.
func runOIDCLogin(t *testing.T, s *apiServer, idp *fakeIdP, editCallback func(*url.URL)) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	s.handleOIDCLogin(rr, httptest.NewRequest(http.MethodGet, oidcLoginPath, nil))
	if rr.Code != http.StatusFound {
		t.Fatalf("login: %d %s", rr.Code, rr.Body.String())
	}
	var flow *http.Cookie
	for _, c := range rr.Result().Cookies() {
		if c.Name == oidcFlowCookie {
			flow = c
		}
	}
	if flow == nil || !flow.HttpOnly || flow.SameSite != http.SameSiteLaxMode {
		t.Fatalf("flow cookie missing or not HttpOnly+Lax: %+v", flow)
	}
	cb := idp.authorize(t, rr.Header().Get("Location"))
	if cb.Path != oidcCallbackPath {
		t.Fatalf("redirect_uri path = %s", cb.Path)
	}
	if editCallback != nil {
		editCallback(cb)
	}
	req := httptest.NewRequest(http.MethodGet, cb.RequestURI(), nil)
	req.AddCookie(flow)
	out := httptest.NewRecorder()
	s.handleOIDCCallback(out, req)
	return out
}

func sessionFrom(t *testing.T, s *apiServer, rr *httptest.ResponseRecorder) (principal, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	for _, c := range rr.Result().Cookies() {
		req.AddCookie(c)
	}
	return s.resolvePrincipal(req)
}

func TestOIDCLoginMapsGroupsToRole(t *testing.T) {
	idp := newFakeIdP(t)
	idp.claims = map[string]interface{}{"preferred_username": "dana@example.com", "groups": []string{"staff", "ncc-ops"}}
	s := newOIDCServer(t, idp, nil)

	rr := runOIDCLogin(t, s, idp, nil)
	if rr.Code != http.StatusFound || rr.Header().Get("Location") != "/" {
		t.Fatalf("callback: %d -> %s", rr.Code, rr.Header().Get("Location"))
	}
	p, ok := sessionFrom(t, s, rr)
	if !ok || p.subject != "dana@example.com" || p.role != RoleOperator {
		t.Fatalf("session = %+v ok=%v, want dana@example.com as operator", p, ok)
	}
	if !s.loginEnabled() || !s.oidcIsEnabled() || s.oidcLabel() != "Test IdP" {
		t.Fatal("OIDC should enable interactive login and expose its label")
	}
}

func TestOIDCDefaultRoleAndDenyUnmapped(t *testing.T) {
	idp := newFakeIdP(t)
	idp.claims = map[string]interface{}{"email": "eve@example.com", "groups": "contractors"}

	s := newOIDCServer(t, idp, nil) // default role: viewer
	rr := runOIDCLogin(t, s, idp, nil)
	if p, ok := sessionFrom(t, s, rr); !ok || p.role != RoleViewer || p.subject != "eve@example.com" {
		t.Fatalf("unmapped user should get the default viewer role: %+v ok=%v", p, ok)
	}

	s = newOIDCServer(t, idp, func(c *oidcPersisted) { c.DefaultRole = "none" })
	rr = runOIDCLogin(t, s, idp, nil)
	if loc := rr.Header().Get("Location"); loc != "/?sso_error=no_role" {
		t.Fatalf("default_role none should refuse: %s", loc)
	}
	if _, ok := sessionFrom(t, s, rr); ok {
		t.Fatal("refused sign-in must not set a session")
	}
}

func TestOIDCRejectsTamperedFlows(t *testing.T) {
	idp := newFakeIdP(t)
	idp.claims = map[string]interface{}{"preferred_username": "mallory", "groups": []string{"ncc-admins"}}
	cases := []struct {
		name   string
		tamper func(map[string]interface{})
		editCB func(*url.URL)
		want   string
	}{
		{"state", nil, func(u *url.URL) { q := u.Query(); q.Set("state", "forged"); u.RawQuery = q.Encode() }, "state_mismatch"},
		{"nonce", func(c map[string]interface{}) { c["nonce"] = "replayed" }, nil, "token_invalid"},
		{"audience", func(c map[string]interface{}) { c["aud"] = "other-app" }, nil, "token_invalid"},
		{"issuer", func(c map[string]interface{}) { c["iss"] = "https://evil.example.com" }, nil, "token_invalid"},
		{"expired", func(c map[string]interface{}) { c["exp"] = time.Now().Add(-time.Hour).Unix() }, nil, "token_invalid"},
		{"idp error", nil, func(u *url.URL) { u.RawQuery = "error=access_denied&state=x" }, "idp_denied"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idp.tamper = tc.tamper
			defer func() { idp.tamper = nil }()
			s := newOIDCServer(t, idp, nil)
			rr := runOIDCLogin(t, s, idp, tc.editCB)
			if loc := rr.Header().Get("Location"); loc != "/?sso_error="+tc.want {
				t.Fatalf("got redirect %q, want sso_error=%s", loc, tc.want)
			}
			if _, ok := sessionFrom(t, s, rr); ok {
				t.Fatal("rejected flow must not set a session")
			}
		})
	}
}

func TestOIDCFlowCookieRequiredAndSigned(t *testing.T) {
	idp := newFakeIdP(t)
	s := newOIDCServer(t, idp, nil)
	rr := httptest.NewRecorder()
	s.handleOIDCCallback(rr, httptest.NewRequest(http.MethodGet, oidcCallbackPath+"?code=x&state=y", nil))
	if loc := rr.Header().Get("Location"); loc != "/?sso_error=flow_expired" {
		t.Fatalf("callback without flow cookie: %s", loc)
	}
	forged, _ := (&apiServer{sessionSecret: "other"}).signOIDCFlow(oidcFlow{State: "y", Nonce: "n", Verifier: "v", Exp: time.Now().Add(time.Minute).Unix()})
	req := httptest.NewRequest(http.MethodGet, oidcCallbackPath+"?code=x&state=y", nil)
	req.AddCookie(&http.Cookie{Name: oidcFlowCookie, Value: forged})
	rr = httptest.NewRecorder()
	s.handleOIDCCallback(rr, req)
	if loc := rr.Header().Get("Location"); loc != "/?sso_error=flow_expired" {
		t.Fatalf("callback with forged flow cookie: %s", loc)
	}
}

func TestOIDCRefusesLocalUsernameClash(t *testing.T) {
	idp := newFakeIdP(t)
	idp.claims = map[string]interface{}{"preferred_username": "admin", "groups": []string{"ncc-admins"}}
	s := newOIDCServer(t, idp, nil)
	if _, _, err := s.users.bootstrapAdminIfEmpty("admin"); err != nil {
		t.Fatal(err)
	}
	rr := runOIDCLogin(t, s, idp, nil)
	if loc := rr.Header().Get("Location"); loc != "/?sso_error=username_conflict" {
		t.Fatalf("clash with local admin should be refused: %s", loc)
	}
}

func TestOIDCSettingsAPI(t *testing.T) {
	idp := newFakeIdP(t)
	s := newDBServer(t)
	put := func(body string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		s.handleOIDCSettings(rr, httptest.NewRequest(http.MethodPut, "/api/v1/settings/oidc", strings.NewReader(body)))
		return rr
	}
	// http issuers are refused unless loopback; discovery runs before saving.
	if rr := put(`{"enabled":true,"issuer_url":"http://idp.example.com","client_id":"c","root_url":"https://ncc.example.com"}`); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "https") {
		t.Fatalf("non-loopback http issuer: %d %s", rr.Code, rr.Body.String())
	}
	if rr := put(`{"enabled":true,"issuer_url":"` + idp.srv.URL + `","client_id":"c","client_secret":"s3cret","root_url":"https://ncc.example.com/","role_map":"ncc-admins=admin"}`); rr.Code != http.StatusOK {
		t.Fatalf("valid config: %d %s", rr.Code, rr.Body.String())
	}
	// Saving without a secret keeps the stored one; GET never returns it.
	if rr := put(`{"enabled":true,"issuer_url":"` + idp.srv.URL + `","client_id":"c","root_url":"https://ncc.example.com","button_label":"Okta"}`); rr.Code != http.StatusOK {
		t.Fatalf("edit: %d %s", rr.Code, rr.Body.String())
	}
	if got := s.users.getOIDC().ClientSecret; got != "s3cret" {
		t.Fatalf("client secret = %q, want it preserved", got)
	}
	rr := httptest.NewRecorder()
	s.handleOIDCSettings(rr, httptest.NewRequest(http.MethodGet, "/api/v1/settings/oidc", nil))
	body := rr.Body.String()
	if strings.Contains(body, "s3cret") || !strings.Contains(body, `"has_client_secret":true`) ||
		!strings.Contains(body, `"redirect_url":"https://ncc.example.com/api/v1/auth/oidc/callback"`) {
		t.Fatalf("GET leaked the secret or lacks redirect_url: %s", body)
	}
	// Discovery check without saving.
	rr = httptest.NewRecorder()
	s.handleOIDCTest(rr, httptest.NewRequest(http.MethodPost, "/api/v1/settings/oidc/test", strings.NewReader(`{"issuer_url":"`+idp.srv.URL+`"}`)))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"pkce_s256":true`) {
		t.Fatalf("discovery test: %d %s", rr.Code, rr.Body.String())
	}
}

func TestOIDCRoutesArePublicAndAdminGated(t *testing.T) {
	s := newDBServer(t)
	for _, p := range []string{oidcLoginPath, oidcCallbackPath} {
		if !s.isPublicAuthPath(httptest.NewRequest(http.MethodGet, p, nil)) {
			t.Errorf("%s must be reachable before sign-in", p)
		}
	}
	for _, p := range []string{"/api/v1/settings/oidc", "/api/v1/settings/oidc/test"} {
		if routeMinRoleFor(p, true) != RoleAdmin || routeMinRoleFor(p, false) != RoleAdmin {
			t.Errorf("%s must be admin-only", p)
		}
	}
	// Not configured: the login button sends the user back with a reason.
	rr := httptest.NewRecorder()
	s.handleOIDCLogin(rr, httptest.NewRequest(http.MethodGet, oidcLoginPath, nil))
	if loc := rr.Header().Get("Location"); loc != "/?sso_error=oidc_unavailable" {
		t.Fatalf("unconfigured login: %s", loc)
	}
}
