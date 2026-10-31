package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OpenID Connect sign-in (authorization code flow with PKCE). Works with any
// standards-compliant provider: Microsoft Entra ID, Okta, Google Workspace,
// Keycloak, Auth0, Ping, ADFS 2019+. Like SAML, the provider's identity is
// turned into our own role-bearing session cookie; the IdP's tokens are
// verified and then discarded.
//
// The flow is stateless on the server (state, nonce and PKCE verifier travel
// in a short-lived HMAC-signed cookie), so it works across API replicas.

const (
	oidcLoginPath    = "/api/v1/auth/oidc/login"
	oidcCallbackPath = "/api/v1/auth/oidc/callback"
	oidcFlowCookie   = "ncc_oidc_flow"
	oidcFlowTTL      = 10 * time.Minute
	// Session tokens travel in a cookie; cap the groups embedded in them.
	oidcMaxGroups     = 64
	oidcMaxGroupBytes = 2048
)

// oidcPersisted is the admin-editable OIDC configuration stored in the user
// database. ClientSecret is write-only through the API.
type oidcPersisted struct {
	Enabled       bool   `json:"enabled"`
	IssuerURL     string `json:"issuer_url"`
	ClientID      string `json:"client_id"`
	ClientSecret  string `json:"client_secret,omitempty"`
	RootURL       string `json:"root_url"`
	Scopes        string `json:"scopes,omitempty"`
	UsernameClaim string `json:"username_claim,omitempty"`
	GroupsClaim   string `json:"groups_claim,omitempty"`
	RoleMapRaw    string `json:"role_map,omitempty"`
	DefaultRole   string `json:"default_role,omitempty"`
	ButtonLabel   string `json:"button_label,omitempty"`
}

func (c *oidcPersisted) configured() bool {
	return c != nil && c.Enabled &&
		strings.TrimSpace(c.IssuerURL) != "" &&
		strings.TrimSpace(c.ClientID) != "" &&
		strings.TrimSpace(c.RootURL) != ""
}

// fingerprint identifies a config version so each replica can tell when the
// shared store holds a newer one than the provider it built.
func (c *oidcPersisted) fingerprint() string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// redirectURL is the callback the IdP must have registered.
func (c *oidcPersisted) redirectURL() string {
	return strings.TrimRight(strings.TrimSpace(c.RootURL), "/") + oidcCallbackPath
}

func (c *oidcPersisted) label() string {
	if l := strings.TrimSpace(c.ButtonLabel); l != "" {
		return l
	}
	return "single sign-on"
}

type oidcProvider struct {
	oauth         oauth2.Config
	verifier      *oidc.IDTokenVerifier
	client        *http.Client
	usernameClaim string
	groupsClaim   string
	roleMap       map[string]Role
	defaultRole   Role // RoleNone: users matching no mapping are refused
	label         string
	fingerprint   string
}

// validateOIDCURL requires https, allowing http only for loopback hosts (a
// local test IdP or a dashboard on localhost).
func validateOIDCURL(field, raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return fmt.Errorf("%s must be an absolute URL", field)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if host == "localhost" || strings.HasSuffix(host, ".localhost") {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("%s must use https (http is allowed only for localhost)", field)
	default:
		return fmt.Errorf("%s must use https", field)
	}
}

func oidcHTTPClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second}
}

// discoverOIDC fetches the provider's discovery document. The returned
// provider keeps using client for later JWKS fetches, so it is bound to a
// background context rather than a request's.
func discoverOIDC(issuer string, client *http.Client) (*oidc.Provider, error) {
	ctx := oidc.ClientContext(context.Background(), client)
	p, err := oidc.NewProvider(ctx, strings.TrimSpace(issuer))
	if err != nil {
		return nil, fmt.Errorf("oidc discovery for %s: %w", strings.TrimSpace(issuer), err)
	}
	return p, nil
}

func buildOIDCProvider(c *oidcPersisted) (*oidcProvider, error) {
	if err := validateOIDCURL("issuer_url", c.IssuerURL); err != nil {
		return nil, err
	}
	if err := validateOIDCURL("root_url", c.RootURL); err != nil {
		return nil, err
	}
	roleMap, err := parseRoleMap(c.RoleMapRaw)
	if err != nil {
		return nil, err
	}
	defRole := RoleViewer
	switch d := strings.ToLower(strings.TrimSpace(c.DefaultRole)); d {
	case "":
	case "none":
		defRole = RoleNone
	default:
		r, ok := parseRole(d)
		if !ok {
			return nil, fmt.Errorf("invalid default_role %q (want admin|operator|viewer|none)", c.DefaultRole)
		}
		defRole = r
	}
	client := oidcHTTPClient()
	prov, err := discoverOIDC(c.IssuerURL, client)
	if err != nil {
		return nil, err
	}
	scopes := []string{oidc.ScopeOpenID, "profile", "email"}
	for _, sc := range strings.FieldsFunc(c.Scopes, func(r rune) bool { return r == ',' || r == ' ' }) {
		if sc = strings.TrimSpace(sc); sc != "" && !containsString(scopes, sc) {
			scopes = append(scopes, sc)
		}
	}
	groupsClaim := strings.TrimSpace(c.GroupsClaim)
	if groupsClaim == "" {
		groupsClaim = "groups"
	}
	return &oidcProvider{
		oauth: oauth2.Config{
			ClientID:     strings.TrimSpace(c.ClientID),
			ClientSecret: c.ClientSecret,
			Endpoint:     prov.Endpoint(),
			RedirectURL:  c.redirectURL(),
			Scopes:       scopes,
		},
		verifier:      prov.Verifier(&oidc.Config{ClientID: strings.TrimSpace(c.ClientID)}),
		client:        client,
		usernameClaim: strings.TrimSpace(c.UsernameClaim),
		groupsClaim:   groupsClaim,
		roleMap:       roleMap,
		defaultRole:   defRole,
		label:         c.label(),
		fingerprint:   c.fingerprint(),
	}, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// roleFor maps the user's groups to the highest matching role, else the
// default (RoleNone refuses the sign-in).
func (p *oidcProvider) roleFor(groups []string) Role {
	best := RoleNone
	for _, g := range groups {
		if r, ok := p.roleMap[strings.TrimSpace(g)]; ok && r > best {
			best = r
		}
	}
	if best == RoleNone {
		return p.defaultRole
	}
	return best
}

// oidcIsEnabled reports whether OIDC sign-in is configured and enabled.
func (s *apiServer) oidcIsEnabled() bool {
	return s.users != nil && s.users.getOIDC().configured()
}

// oidcLabel is the provider name shown on the sign-in button.
func (s *apiServer) oidcLabel() string {
	if s.users == nil {
		return ""
	}
	if c := s.users.getOIDC(); c.configured() {
		return c.label()
	}
	return ""
}

// currentOIDC returns a provider for the stored config, rebuilding it when the
// config changed (here or on another replica) or an earlier build failed.
func (s *apiServer) currentOIDC() (*oidcProvider, error) {
	if s.users == nil {
		return nil, errors.New("oidc requires a user database")
	}
	cfg := s.users.getOIDC()
	if !cfg.configured() {
		return nil, errors.New("oidc is not configured")
	}
	fp := cfg.fingerprint()
	s.oidcMu.RLock()
	p := s.oidc
	s.oidcMu.RUnlock()
	if p != nil && p.fingerprint == fp {
		return p, nil
	}
	built, err := buildOIDCProvider(cfg)
	if err != nil {
		return nil, err
	}
	s.oidcMu.Lock()
	s.oidc = built
	s.oidcMu.Unlock()
	return built, nil
}

// oidcFlow is the per-login state carried in the signed flow cookie.
type oidcFlow struct {
	State    string `json:"s"`
	Nonce    string `json:"n"`
	Verifier string `json:"v"`
	Exp      int64  `json:"e"`
}

func randomURLToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func (s *apiServer) signOIDCFlow(f oidcFlow) (string, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte("oidc-flow\x00"+s.sessionSecret))
	_, _ = mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (s *apiServer) parseOIDCFlow(v string) (oidcFlow, error) {
	payload, sig, ok := strings.Cut(v, ".")
	if !ok {
		return oidcFlow{}, errors.New("malformed flow cookie")
	}
	mac := hmac.New(sha256.New, []byte("oidc-flow\x00"+s.sessionSecret))
	_, _ = mac.Write([]byte(payload))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return oidcFlow{}, errors.New("flow cookie signature mismatch")
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return oidcFlow{}, err
	}
	var f oidcFlow
	if err := json.Unmarshal(raw, &f); err != nil {
		return oidcFlow{}, err
	}
	if time.Now().Unix() > f.Exp {
		return oidcFlow{}, errors.New("sign-in took too long; start again")
	}
	return f, nil
}

func (s *apiServer) setOIDCFlowCookie(w http.ResponseWriter, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     oidcFlowCookie,
		Value:    value,
		Path:     "/api/v1/auth/oidc/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   s.cookieSecure(),
		// The IdP redirects back with a top-level GET, which Lax allows.
		SameSite: http.SameSiteLaxMode,
	})
}

// oidcFail ends a failed sign-in on the login page with a short reason code
// the UI turns into a message.
func (s *apiServer) oidcFail(w http.ResponseWriter, r *http.Request, code string, err error, fields map[string]interface{}) {
	if fields == nil {
		fields = map[string]interface{}{}
	}
	fields["reason"] = code
	if err != nil {
		log.Printf("oidc sign-in failed (%s): %v", code, err)
	}
	s.audit(r, "auth.oidc.login", false, fields)
	http.Redirect(w, r, "/?sso_error="+url.QueryEscape(code), http.StatusFound)
}

// handleOIDCLogin starts the authorization code flow.
func (s *apiServer) handleOIDCLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	p, err := s.currentOIDC()
	if err != nil {
		s.oidcFail(w, r, "oidc_unavailable", err, nil)
		return
	}
	state, err1 := randomURLToken(32)
	nonce, err2 := randomURLToken(32)
	if err1 != nil || err2 != nil {
		s.oidcFail(w, r, "oidc_unavailable", errors.Join(err1, err2), nil)
		return
	}
	verifier := oauth2.GenerateVerifier()
	cookie, err := s.signOIDCFlow(oidcFlow{State: state, Nonce: nonce, Verifier: verifier, Exp: time.Now().Add(oidcFlowTTL).Unix()})
	if err != nil {
		s.oidcFail(w, r, "oidc_unavailable", err, nil)
		return
	}
	s.setOIDCFlowCookie(w, cookie, int(oidcFlowTTL.Seconds()))
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, p.oauth.AuthCodeURL(state, oidc.Nonce(nonce), oauth2.S256ChallengeOption(verifier)), http.StatusFound)
}

// handleOIDCCallback completes the flow: checks state, exchanges the code with
// the PKCE verifier, verifies the ID token (signature, issuer, audience,
// expiry, nonce) and mints our session.
func (s *apiServer) handleOIDCCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	c, cerr := r.Cookie(oidcFlowCookie)
	s.setOIDCFlowCookie(w, "", -1) // single use
	q := r.URL.Query()
	if e := q.Get("error"); e != "" {
		s.oidcFail(w, r, "idp_denied", fmt.Errorf("%s: %s", e, q.Get("error_description")), map[string]interface{}{"idp_error": e})
		return
	}
	if cerr != nil {
		s.oidcFail(w, r, "flow_expired", cerr, nil)
		return
	}
	flow, err := s.parseOIDCFlow(c.Value)
	if err != nil {
		s.oidcFail(w, r, "flow_expired", err, nil)
		return
	}
	if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(flow.State)) != 1 {
		s.oidcFail(w, r, "state_mismatch", errors.New("state does not match"), nil)
		return
	}
	code := q.Get("code")
	if code == "" {
		s.oidcFail(w, r, "idp_denied", errors.New("no authorization code"), nil)
		return
	}
	p, err := s.currentOIDC()
	if err != nil {
		s.oidcFail(w, r, "oidc_unavailable", err, nil)
		return
	}
	ctx, cancel := context.WithTimeout(oidc.ClientContext(r.Context(), p.client), 20*time.Second)
	defer cancel()
	tok, err := p.oauth.Exchange(ctx, code, oauth2.VerifierOption(flow.Verifier))
	if err != nil {
		s.oidcFail(w, r, "token_exchange", err, nil)
		return
	}
	rawID, _ := tok.Extra("id_token").(string)
	if rawID == "" {
		s.oidcFail(w, r, "token_invalid", errors.New("token response has no id_token"), nil)
		return
	}
	idt, err := p.verifier.Verify(ctx, rawID)
	if err != nil {
		s.oidcFail(w, r, "token_invalid", err, nil)
		return
	}
	if subtle.ConstantTimeCompare([]byte(idt.Nonce), []byte(flow.Nonce)) != 1 {
		s.oidcFail(w, r, "token_invalid", errors.New("nonce mismatch"), nil)
		return
	}
	var claims map[string]interface{}
	if err := idt.Claims(&claims); err != nil {
		s.oidcFail(w, r, "token_invalid", err, nil)
		return
	}
	username := oidcUsername(claims, p.usernameClaim, idt.Subject)
	// A directory identity must never borrow a local account's session
	// semantics (token generation, password change), so a clash is refused.
	if _, exists := s.users.lookup(username); exists {
		s.oidcFail(w, r, "username_conflict", fmt.Errorf("OIDC user %q matches a local account", username), map[string]interface{}{"username": username})
		return
	}
	groups := capGroups(claimStrings(claims[p.groupsClaim]))
	role := p.roleFor(groups)
	if role == RoleNone {
		s.oidcFail(w, r, "no_role", fmt.Errorf("user %q matched no role mapping", username), map[string]interface{}{"username": username})
		return
	}
	token, exp, err := s.issueRoleSessionTokenWithGroups(cleanClientIP(r), username, role, groups)
	if err != nil {
		s.oidcFail(w, r, "oidc_unavailable", err, nil)
		return
	}
	if err := s.setSessionCookies(w, token, exp); err != nil {
		s.oidcFail(w, r, "oidc_unavailable", err, nil)
		return
	}
	s.audit(r, "auth.oidc.login", true, map[string]interface{}{"username": username, "role": role.String(), "issuer": idt.Issuer})
	http.Redirect(w, r, "/", http.StatusFound)
}

// oidcUsername picks the configured claim, then the usual human-readable
// ones, then the subject.
func oidcUsername(claims map[string]interface{}, preferred, subject string) string {
	keys := []string{"preferred_username", "email", "upn", "name"}
	if preferred != "" {
		keys = append([]string{preferred}, keys...)
	}
	for _, k := range keys {
		if v, ok := claims[k].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return subject
}

// claimStrings accepts a claim that is a string, a list of strings, or
// missing.
func claimStrings(v interface{}) []string {
	switch t := v.(type) {
	case string:
		if s := strings.TrimSpace(t); s != "" {
			return []string{s}
		}
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	}
	return nil
}

func capGroups(groups []string) []string {
	out := make([]string, 0, len(groups))
	total := 0
	for _, g := range groups {
		if len(out) >= oidcMaxGroups || total+len(g) > oidcMaxGroupBytes {
			log.Printf("oidc: user has more groups than fit in a session; extra groups ignored for cluster-group membership")
			break
		}
		out = append(out, g)
		total += len(g)
	}
	return out
}

// handleOIDCSettings manages the runtime OIDC configuration (admin-only). GET
// never returns the client secret; PUT keeps the stored secret when the body
// leaves it empty.
func (s *apiServer) handleOIDCSettings(w http.ResponseWriter, r *http.Request) {
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "runtime OIDC config requires --users-db"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		cfg := s.users.getOIDC()
		data := map[string]interface{}{"enabled": false}
		if cfg != nil {
			data["enabled"] = cfg.Enabled
			data["issuer_url"] = cfg.IssuerURL
			data["client_id"] = cfg.ClientID
			data["has_client_secret"] = cfg.ClientSecret != ""
			data["root_url"] = cfg.RootURL
			data["scopes"] = cfg.Scopes
			data["username_claim"] = cfg.UsernameClaim
			data["groups_claim"] = cfg.GroupsClaim
			data["role_map"] = cfg.RoleMapRaw
			data["default_role"] = cfg.DefaultRole
			data["button_label"] = cfg.ButtonLabel
			if strings.TrimSpace(cfg.RootURL) != "" {
				data["redirect_url"] = cfg.redirectURL()
			}
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
	case http.MethodPut:
		var body oidcPersisted
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
			return
		}
		cfg := &oidcPersisted{
			Enabled:       body.Enabled,
			IssuerURL:     strings.TrimRight(strings.TrimSpace(body.IssuerURL), "/"),
			ClientID:      strings.TrimSpace(body.ClientID),
			ClientSecret:  body.ClientSecret,
			RootURL:       strings.TrimRight(strings.TrimSpace(body.RootURL), "/"),
			Scopes:        strings.TrimSpace(body.Scopes),
			UsernameClaim: strings.TrimSpace(body.UsernameClaim),
			GroupsClaim:   strings.TrimSpace(body.GroupsClaim),
			RoleMapRaw:    strings.TrimSpace(body.RoleMapRaw),
			DefaultRole:   strings.TrimSpace(body.DefaultRole),
			ButtonLabel:   strings.TrimSpace(body.ButtonLabel),
		}
		if cfg.ClientSecret == "" {
			if prev := s.users.getOIDC(); prev != nil {
				cfg.ClientSecret = prev.ClientSecret
			}
		}
		if cfg.Enabled {
			if !cfg.configured() {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "issuer_url, client_id and root_url are required to enable OIDC"})
				return
			}
			// Build (discovery included) before saving so a typo never
			// replaces a working configuration.
			if _, err := buildOIDCProvider(cfg); err != nil {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
				return
			}
		}
		if err := s.users.setOIDC(cfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.oidcMu.Lock()
		s.oidc = nil // rebuilt on next use
		s.oidcMu.Unlock()
		s.audit(r, "settings.oidc.update", true, map[string]interface{}{"enabled": cfg.Enabled, "issuer": cfg.IssuerURL})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "OIDC configuration updated", Data: map[string]interface{}{"redirect_url": cfg.redirectURL()}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// handleOIDCTest runs discovery against an issuer without saving anything, so
// an admin can check the URL and see what the provider supports.
func (s *apiServer) handleOIDCTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var body struct {
		IssuerURL string `json:"issuer_url"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	issuer := strings.TrimRight(strings.TrimSpace(body.IssuerURL), "/")
	if err := validateOIDCURL("issuer_url", issuer); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	prov, err := discoverOIDC(issuer, oidcHTTPClient())
	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: err.Error()})
		return
	}
	var meta struct {
		Scopes  []string `json:"scopes_supported"`
		Claims  []string `json:"claims_supported"`
		PKCE    []string `json:"code_challenge_methods_supported"`
		SigAlgs []string `json:"id_token_signing_alg_values_supported"`
	}
	_ = prov.Claims(&meta)
	ep := prov.Endpoint()
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Discovery succeeded", Data: map[string]interface{}{
		"authorization_endpoint": ep.AuthURL,
		"token_endpoint":         ep.TokenURL,
		"scopes_supported":       meta.Scopes,
		"claims_supported":       meta.Claims,
		"pkce_s256":              containsString(meta.PKCE, "S256"),
		"signing_algs":           meta.SigAlgs,
	}})
}
