package main

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/auth"
)

func (s *apiServer) withAuth(next http.Handler) http.Handler {
	// After AuthMiddleware injects jwt/pat/static identity, map it onto the
	// api-server principal and enforce RBAC + CSRF.
	afterStateless := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ident, ok := auth.IdentityFromContext(r.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
			return
		}
		p, ok := s.principalFromIdentity(&ident)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
			return
		}
		s.authorizePrincipal(next, w, r, p)
	})
	stateless := auth.AuthMiddleware(s.jwtSecret, s, s.staticTokenForMode())(afterStateless)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rejectInvalidMCPClient(w, r) {
			return
		}
		if s.rejectCompromisedUIClient(w, r) {
			return
		}
		if s.isPublicAuthPath(r) {
			next.ServeHTTP(w, r)
			return
		}
		// Legacy HMAC ncc_session cookies and the static viewer token. JWT,
		// hashed PATs, and the admin static bearer are handled below by
		// AuthMiddleware so every replica sharing NCC_JWT_SECRET can verify
		// them without a local session file.
		if p, err := s.resolveLegacyPrincipal(r); err == nil {
			auth.RecordSuccess()
			s.authorizePrincipal(next, w, r, p)
			return
		}
		stateless.ServeHTTP(w, r)
	})
}

func (s *apiServer) isPublicAuthPath(r *http.Request) bool {
	if r.Method == http.MethodOptions || r.URL.Path == "/api/v1/health" || r.URL.Path == "/api/v1/health/ready" {
		return true
	}
	if r.Method == http.MethodGet && r.URL.Path == "/api/v1/tls/public" {
		return true
	}
	if r.Method == http.MethodGet && (r.URL.Path == "/api/v1/openapi.json" || r.URL.Path == "/api/v1/meta/routes") {
		return true
	}
	if r.Method == http.MethodGet && r.URL.Path == "/metrics" && s.metricsPublic {
		return true
	}
	if r.URL.Path == "/" || r.URL.Path == "/docs" || r.URL.Path == "/docs/ui" ||
		strings.HasPrefix(r.URL.Path, "/docs/assets/") {
		return true
	}
	if r.URL.Path == "/api/v1/auth/session" {
		return true
	}
	if r.URL.Path == "/api/v1/auth/login" ||
		r.URL.Path == "/api/v1/auth/logout" ||
		r.URL.Path == "/api/v1/auth/me" ||
		r.URL.Path == "/api/v1/auth/change-password" ||
		r.URL.Path == "/api/v1/auth/forgot-password" ||
		r.URL.Path == oidcLoginPath || r.URL.Path == oidcCallbackPath ||
		strings.HasPrefix(r.URL.Path, "/saml/") {
		return true
	}
	return false
}

func (s *apiServer) authorizePrincipal(next http.Handler, w http.ResponseWriter, r *http.Request, p principal) {
	// Forced password change: a flagged local account may do nothing else
	// until it sets a new password (the allowlisted endpoints above let it
	// reach /auth/me, /auth/change-password, and /auth/logout).
	//
	// Exception: a backup restore is allowed during forced change so the
	// first-login admin can recover an existing deployment instead of
	// setting a new password — the restore replaces the user database with
	// the backed-up one (old admin hash, must_change=false), making a
	// password change pointless. The RBAC check below still confines this to
	// the admin role, and CSRF still applies, so a non-admin flagged account
	// cannot use it.
	if p.mustChange && r.URL.Path != "/api/v1/settings/restore" {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "password change required", ErrorCode: "NCC_API_PASSWORD_CHANGE_REQUIRED"})
		return
	}
	if need := routeMinRole(r); p.role < need {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: fmt.Sprintf("forbidden: this action requires the %q role", need.String())})
		return
	}
	if p.method == authSessionCookie && isMutating(r) && !s.csrfValid(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "forbidden: missing or invalid CSRF token"})
		return
	}
	next.ServeHTTP(w, withPrincipal(r, p))
}

func (s *apiServer) withCORS(next http.Handler) http.Handler {
	allowedOrigins := parseAllowedOrigins(s.corsOrigin)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		// SAML SP endpoints are browser-mediated and IdP-originated: the IdP
		// returns its assertion as a top-level cross-site POST to /saml/acs, so
		// the browser sends Origin: <idp-host>, which can never be in the UI
		// origin allowlist. The CORS allowlist guards the SPA's XHR calls to
		// /api/; the SAML flow's security boundary is the signed assertion plus
		// the relay-state cookie, so exempt /saml/ from origin enforcement.
		isSAML := strings.HasPrefix(r.URL.Path, "/saml/")
		if origin != "" && !isSAML {
			if _, ok := allowedOrigins[origin]; !ok && !sameHostOrigin(origin, r) {
				writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "origin not allowed"})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
		// Content-Security-Policy. API/JSON (and Prometheus text) responses serve
		// no active content, so they get a maximally strict policy. The two HTML
		// help pages (landing + Swagger UI) need inline style/script; Swagger's
		// bundle/styles are now self-hosted (vendored, served from /docs/assets),
		// so the policy no longer trusts any external CDN origin.
		switch {
		case r.URL.Path == "/" || r.URL.Path == "/docs" || r.URL.Path == "/docs/ui" ||
			strings.HasPrefix(r.URL.Path, "/docs/assets/"):
			w.Header().Set("Content-Security-Policy",
				"default-src 'none'; base-uri 'none'; frame-ancestors 'none'; "+
					"img-src 'self' data:; "+
					"style-src 'self' 'unsafe-inline'; "+
					"script-src 'self' 'unsafe-inline'; "+
					"connect-src 'self'; font-src 'self' data:; worker-src 'self' blob:")
		default:
			w.Header().Set("Content-Security-Policy",
				"default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		}
		if r.TLS != nil {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Token, Authorization, X-CSRF-Token, X-Requested-With, X-NCC-UI-Version, X-NCC-MCP-Version, X-NCC-MCP-Proof, X-NCC-MCP-Tool")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameHostOrigin allows a browser Origin when it targets the same host as the
// current request (hostname match, any port). This removes the need for users
// to hand-edit CORS origins just because API and UI listen on different ports
// on the same box (e.g. 8081 vs 8080).
func sameHostOrigin(origin string, r *http.Request) bool {
	ou, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if ou.Scheme != "http" && ou.Scheme != "https" {
		return false
	}
	originHost := strings.TrimSpace(strings.ToLower(ou.Hostname()))
	if originHost == "" {
		return false
	}
	reqHost := strings.TrimSpace(strings.ToLower(hostOnly(r.Host)))
	if reqHost == "" {
		// Fallback for proxied setups where Host can be rewritten.
		reqHost = strings.TrimSpace(strings.ToLower(hostOnly(r.Header.Get("X-Forwarded-Host"))))
	}
	return reqHost != "" && originHost == reqHost
}

func hostOnly(hostport string) string {
	hostport = strings.TrimSpace(hostport)
	if hostport == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	// Already host-only, or an unbracketed IPv6 literal.
	return hostport
}

// optionalPrincipal resolves the caller on a public endpoint without failing
// the request (and without counting a missing credential as an auth failure).
// It returns nil for anonymous callers.
func (s *apiServer) optionalPrincipal(r *http.Request) *principal {
	if p, err := s.resolveLegacyPrincipal(r); err == nil {
		return &p
	}
	if tok, _ := auth.ExtractToken(r); tok == "" {
		return nil
	}
	ident, err := auth.Authenticate(r, s.jwtSecret, s, s.staticTokenForMode())
	if err != nil || ident == nil {
		return nil
	}
	if p, ok := s.principalFromIdentity(ident); ok {
		return &p
	}
	return nil
}

func (s *apiServer) handleAuthSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !isLoopbackRequest(r) {
		s.audit(r, "auth.session.issue", false, map[string]interface{}{"reason": "non_loopback"})
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "session bootstrap allowed only from loopback"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		s.audit(r, "auth.session.issue", false, map[string]interface{}{"reason": "bad_token"})
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	token, exp, err := s.issueSessionToken(cleanClientIP(r))
	if err != nil {
		s.audit(r, "auth.session.issue", false, map[string]interface{}{"reason": "issue_failed", "error": err.Error()})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.session.issue", true, map[string]interface{}{"ttl_sec": int(s.sessionTTL.Seconds())})
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"token":      token,
		"expires_at": exp.Format(time.RFC3339),
		"ttl_sec":    int(s.sessionTTL.Seconds()),
	}})
}

func (s *apiServer) handleAuthRotate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !secureCompare(strings.TrimSpace(r.Header.Get("X-API-Token")), s.authToken) {
		s.audit(r, "auth.token.rotate", false, map[string]interface{}{"reason": "bad_token"})
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		s.audit(r, "auth.token.rotate", false, map[string]interface{}{"reason": "rand_failed", "error": err.Error()})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: fmt.Sprintf("generate token: %v", err)})
		return
	}
	s.authToken = base64.RawURLEncoding.EncodeToString(b)
	if err := s.ensureAuthToken(); err != nil {
		s.audit(r, "auth.token.rotate", false, map[string]interface{}{"reason": "persist_failed", "error": err.Error()})
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.token.rotate", true, nil)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "token rotated"})
}

// initJWTSecret loads NCC_JWT_SECRET (or reuses --session-secret) for HS256
// session JWTs. When neither is set, a 32-byte secret is generated in memory
// and a prominent warning is logged: replicas cannot share sessions without a
// configured secret. An empty --session-secret is filled from the JWT key so
// legacy HMAC cookies also verify across replicas that share NCC_JWT_SECRET.
func (s *apiServer) initJWTSecret() {
	raw := strings.TrimSpace(os.Getenv("NCC_JWT_SECRET"))
	if raw == "" {
		raw = strings.TrimSpace(s.sessionSecret)
	}
	if raw == "" {
		if s.capabilities.Kubernetes {
			log.Fatal("NCC_JWT_SECRET is required in Kubernetes so API replicas can validate the same session JWTs. Create the jwt-secret key in ncc-v2-secrets (or Helm secretName) and restart the API Deployment.")
		}
		b := make([]byte, 32)
		if _, err := crand.Read(b); err != nil {
			log.Fatalf("generate jwt secret: %v", err)
		}
		s.jwtSecret = b
		s.jwtSecretEphemeral = true
		log.Printf("WARNING: NCC_JWT_SECRET is not set; generated an ephemeral 32-byte HMAC-SHA256 signing secret in memory. Browser sessions will not validate across process restarts or additional replicas. Set NCC_JWT_SECRET to the same value on every replica for horizontal scaling.")
	} else {
		s.jwtSecret = []byte(raw)
	}
	if strings.TrimSpace(s.sessionSecret) == "" {
		s.sessionSecret = string(s.jwtSecret)
	}
}

func (s *apiServer) ensureAuthToken() error {
	if strings.TrimSpace(s.authToken) == "" {
		b := make([]byte, 32)
		if _, err := crand.Read(b); err != nil {
			return fmt.Errorf("generate auth token: %w", err)
		}
		s.authToken = base64.RawURLEncoding.EncodeToString(b)
	}
	tokenPath := s.absPath(s.tokenFilePath)
	if err := os.MkdirAll(filepath.Dir(tokenPath), 0o700); err != nil {
		return fmt.Errorf("prepare token file dir: %w", err)
	}
	if err := writeSecretFile(tokenPath, []byte(s.authToken+"\n")); err != nil {
		return fmt.Errorf("write token file: %w", err)
	}
	log.Printf("api auth token ready (source=%s, token_file=%s)", tokenSource(s.authToken, os.Getenv("NCC_API_TOKEN")), tokenPath)
	return nil
}
