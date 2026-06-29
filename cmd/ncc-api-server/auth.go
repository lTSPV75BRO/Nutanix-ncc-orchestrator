package main

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Role is an ordered authorization level. Higher values are strictly more
// privileged, so access checks reduce to a single `have >= need` comparison.
type Role int

const (
	RoleNone Role = iota
	RoleViewer
	RoleOperator
	RoleAdmin
)

func (r Role) String() string {
	switch r {
	case RoleViewer:
		return "viewer"
	case RoleOperator:
		return "operator"
	case RoleAdmin:
		return "admin"
	default:
		return ""
	}
}

// parseRole maps a case-insensitive role name to a Role. The bool is false for
// unknown/empty input so callers can decide on a default.
func parseRole(s string) (Role, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "viewer", "read", "readonly", "read-only":
		return RoleViewer, true
	case "operator", "op":
		return RoleOperator, true
	case "admin", "administrator":
		return RoleAdmin, true
	default:
		return RoleNone, false
	}
}

// Cookie names for the browser session and its CSRF double-submit token.
const (
	sessionCookieName = "ncc_session"
	csrfCookieName    = "ncc_csrf"
	csrfHeaderName    = "X-CSRF-Token"
)

// authMethod describes how a request authenticated, which drives CSRF
// enforcement (only browser cookie sessions are CSRF-eligible).
type authMethod int

const (
	authNone authMethod = iota
	authStaticAdminToken
	authStaticViewerToken
	authSessionCookie
	authSessionBearer
	authPAT // user-minted personal access token (bearer credential)
)

// principal is the resolved identity + role for a request.
type principal struct {
	role       Role
	subject    string
	method     authMethod
	mustChange bool      // local account flagged for forced password change
	expiresAt  time.Time // session expiry (zero for static-token auth)
	groups     []string  // directory (AD/SAML) groups from the session, for cluster-group membership
}

// principalCtxKey scopes the resolved principal stored on a request's context
// so the audit logger can attribute every action to the acting user + role.
type principalCtxKey struct{}

// withPrincipal returns a shallow copy of r carrying the resolved principal.
func withPrincipal(r *http.Request, p principal) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), principalCtxKey{}, p))
}

// principalFromContext retrieves a principal previously stored by withAuth.
func principalFromContext(ctx context.Context) (principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(principal)
	return p, ok
}

// samlIsEnabled reports whether SAML is currently active (read lock).
func (s *apiServer) samlIsEnabled() bool {
	s.samlMu.RLock()
	defer s.samlMu.RUnlock()
	return s.samlEnabled
}

// loginEnabled reports whether any interactive login path (local accounts or
// SAML SSO) is configured. When enabled, browser cookie sessions are honored
// and the UI presents a login screen.
func (s *apiServer) loginEnabled() bool {
	if s.users != nil && s.users.count() > 0 {
		return true
	}
	if s.samlIsEnabled() {
		return true
	}
	return s.ldapIsEnabled()
}

// ldapIsEnabled reports whether LDAP/AD login is currently active (read lock).
func (s *apiServer) ldapIsEnabled() bool {
	s.ldapMu.RLock()
	defer s.ldapMu.RUnlock()
	return s.ldapEnabled
}

// sessionsHonored reports whether signed session tokens (cookie or bearer) are
// accepted for this server configuration.
func (s *apiServer) sessionsHonored() bool {
	return s.authMode == "session" || s.authMode == "hybrid" || s.loginEnabled()
}

// cookieSecure reports whether session cookies should carry the Secure
// attribute. Insecure by default so the stack works over plain HTTP out of the
// box (browsers refuse to store a Secure cookie on a non-localhost http origin,
// which otherwise bounces every login back to the login screen). The Secure
// attribute is turned on automatically once an admin enables HTTPS from
// Settings → Access (TLS), and --cookie-secure forces it on for deployments
// that terminate TLS in front of the stack (reverse proxy / load balancer).
func (s *apiServer) cookieSecure() bool {
	if s.cookieInsecure {
		return false
	}
	if s.cookieSecureForce {
		return true
	}
	if s.users != nil {
		if p := s.users.getTLSPolicy(); p != nil && p.HTTPSEnabled {
			return true
		}
	}
	return false
}

// Session-lifetime bounds for the runtime-tunable session TTL. The admin can
// pick any duration between these via Settings → Access; values outside the
// range (or unset) fall back to the server's --session-ttl default.
const (
	sessionTTLMin     = time.Minute
	sessionTTLMax     = 24 * time.Hour
	sessionTTLDefault = 6 * time.Hour
)

// effectiveSessionTTL resolves the session lifetime to apply when minting a
// token: the admin-configured runtime policy if present and in-range, else the
// server's --session-ttl flag, else a safe built-in default.
func (s *apiServer) effectiveSessionTTL() time.Duration {
	if s.users != nil {
		if pol := s.users.getSessionPolicy(); pol != nil && pol.TTLSeconds > 0 {
			d := time.Duration(pol.TTLSeconds) * time.Second
			if d >= sessionTTLMin && d <= sessionTTLMax {
				return d
			}
		}
	}
	if s.sessionTTL >= sessionTTLMin && s.sessionTTL <= sessionTTLMax {
		return s.sessionTTL
	}
	if s.sessionTTL > 0 {
		return s.sessionTTL
	}
	return sessionTTLDefault
}

// sessionFromRequest extracts and verifies a session from the request, checking
// the session cookie first and falling back to an Authorization: Bearer token.
func (s *apiServer) sessionFromRequest(r *http.Request) (sessionClaims, authMethod, error) {
	if c, err := r.Cookie(sessionCookieName); err == nil && strings.TrimSpace(c.Value) != "" {
		claims, verr := s.verifySession(c.Value, cleanClientIP(r))
		if verr == nil {
			return claims, authSessionCookie, nil
		}
		return sessionClaims{}, authNone, verr
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		bearer := strings.TrimSpace(authz[len("Bearer "):])
		claims, verr := s.verifySession(bearer, cleanClientIP(r))
		if verr == nil {
			return claims, authSessionBearer, nil
		}
		return sessionClaims{}, authNone, verr
	}
	return sessionClaims{}, authNone, errNoSession
}

var errNoSession = &authError{"no session credential"}

type authError struct{ msg string }

func (e *authError) Error() string { return e.msg }

// resolvePrincipal determines the caller's role and how they authenticated.
// Resolution order: static admin token, signed session (cookie/bearer), then
// static viewer token. ok is false when no valid credential is present.
func (s *apiServer) resolvePrincipal(r *http.Request) (principal, bool) {
	if s.authMode == "token" || s.authMode == "hybrid" {
		tok := strings.TrimSpace(r.Header.Get("X-API-Token"))
		if tok != "" && secureCompare(tok, s.authToken) {
			return principal{role: RoleAdmin, subject: "static-admin-token", method: authStaticAdminToken}, true
		}
	}
	// Personal access tokens: user-minted bearer credentials carried in
	// X-API-Token or Authorization: Bearer. Honored in any auth mode (they are
	// explicit, revocable, owner-scoped credentials), but only when a writable
	// user store exists to verify and re-resolve them.
	if p, ok := s.principalFromPAT(r); ok {
		return p, true
	}
	if s.sessionsHonored() {
		if claims, method, err := s.sessionFromRequest(r); err == nil {
			role, ok := parseRole(claims.Role)
			if !ok {
				// Legacy sessions issued before role claims existed were
				// only ever minted for admins (loopback + admin token).
				role = RoleAdmin
			}
			subject := claims.Sub
			if subject == "" {
				subject = "session"
			}
			// Resolve the live must-change flag (and current role) from the
			// store so a password change or admin reset takes effect on the
			// next request without re-issuing the token.
			mustChange := false
			validSession := true
			if s.users != nil && subject != "" {
				if acct, ok := s.users.lookup(subject); ok {
					// A password change/reset bumps the account's token
					// generation; a session minted under an older generation is
					// no longer valid, so every other device is signed out.
					if claims.Gen != acct.TokenGen {
						validSession = false
					} else {
						mustChange = acct.MustChange
						if r2, ok := parseRole(acct.Role); ok {
							role = r2
						}
					}
				}
			}
			if validSession {
				exp := time.Time{}
				if claims.Exp > 0 {
					exp = time.Unix(claims.Exp, 0).UTC()
				}
				return principal{role: role, subject: subject, method: method, mustChange: mustChange, expiresAt: exp, groups: claims.Grps}, true
			}
		}
	}
	if s.viewerToken != "" {
		tok := strings.TrimSpace(r.Header.Get("X-API-Token"))
		if tok != "" && secureCompare(tok, s.viewerToken) {
			return principal{role: RoleViewer, subject: "static-viewer-token", method: authStaticViewerToken}, true
		}
	}
	return principal{}, false
}

// routeMinRole reports the minimum role required to access a request.
//
// The model is "viewer < operator < admin": viewers read non-secret data,
// operators additionally run and operate NCC day to day, and admins own
// secret-bearing configuration and identity/security management.
//
// Settings endpoints and token rotation are admin-only even for GET because
// they can expose or change secrets — with a small allowlist of operational
// endpoints carved out for operators (read-only cluster topology, the run
// schedule, and sending a test notification). These are evaluated first so the
// blanket /settings/ admin rule below does not shadow them.
func routeMinRole(r *http.Request) Role {
	isRead := r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions
	return routeMinRoleFor(r.URL.Path, isRead)
}

// routeMinRoleFor is the request-free core of routeMinRole, so the route catalog
// (meta/routes + OpenAPI) can advertise the minimum role for each path/method
// without synthesizing an *http.Request.
func routeMinRoleFor(p string, isRead bool) Role {

	// Operator-accessible operational endpoints. These either expose no secrets
	// (cluster topology is just names) or are operating actions adjacent to
	// running NCC (managing the run schedule, sending a test notification).
	switch {
	// System Health diagnostics + on-demand heal are admin-only: probes touch
	// directory/SSO connectivity and POST applies remediations.
	case strings.HasPrefix(p, "/api/v1/health/diagnostics"):
		return RoleAdmin
	// Personal access tokens are self-service: any authenticated user (viewer
	// included) may list, create, and revoke their OWN tokens. The handlers
	// scope every operation to the caller's subject, and a created token can
	// never exceed the caller's own role. Admin-wide token management lives
	// under /settings/tokens (covered by the admin rule below).
	case p == "/api/v1/auth/tokens" || strings.HasPrefix(p, "/api/v1/auth/tokens/"):
		return RoleViewer
	case isRead && p == "/api/v1/settings/clusters":
		return RoleOperator
	case isRead && p == "/api/v1/settings/cluster-groups":
		return RoleOperator
	case p == "/api/v1/settings/notifications/test":
		return RoleOperator
	case p == "/api/v1/runs/configs":
		return RoleOperator
	case p == "/api/v1/runs/config-preference":
		// Any authenticated caller may store their own default run config.
		return RoleViewer
	case p == "/api/v1/schedule":
		// Reading the schedule is a plain viewer read; creating/updating or
		// applying a recurring run is an operator action (it automates the same
		// runs an operator may already trigger by hand).
		if isRead {
			return RoleViewer
		}
		return RoleOperator
	}

	if strings.HasPrefix(p, "/api/v1/settings/") ||
		p == "/api/v1/auth/rotate" ||
		strings.HasPrefix(p, "/api/v1/auth/users") {
		return RoleAdmin
	}
	if isRead {
		return RoleViewer
	}
	// Mutating methods below.
	switch {
	case p == "/api/v1/runs/trigger",
		p == "/api/v1/runs/preflight",
		strings.HasPrefix(p, "/api/v1/runs/"):
		return RoleOperator
	}
	return RoleAdmin
}

// isMutating reports whether the HTTP method changes server state.
func isMutating(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

// csrfValid implements stateless double-submit-cookie validation: the
// X-CSRF-Token header must equal the ncc_csrf cookie. Only meaningful for
// cookie-authenticated browser requests.
func (s *apiServer) csrfValid(r *http.Request) bool {
	c, err := r.Cookie(csrfCookieName)
	if err != nil || strings.TrimSpace(c.Value) == "" {
		return false
	}
	header := strings.TrimSpace(r.Header.Get(csrfHeaderName))
	if header == "" {
		return false
	}
	return secureCompare(header, c.Value)
}

func randToken(nbytes int) (string, error) {
	b := make([]byte, nbytes)
	if _, err := crand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// setSessionCookies writes the httpOnly session cookie and the readable CSRF
// cookie. Both are SameSite=Strict and expire with the session.
func (s *apiServer) setSessionCookies(w http.ResponseWriter, token string, exp time.Time) error {
	csrf, err := randToken(24)
	if err != nil {
		return err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  exp,
		HttpOnly: true,
		Secure:   s.cookieSecure(),
		SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    csrf,
		Path:     "/",
		Expires:  exp,
		HttpOnly: false, // readable by the SPA so it can echo it back
		Secure:   s.cookieSecure(),
		SameSite: http.SameSiteStrictMode,
	})
	return nil
}

// clearSessionCookies expires the session and CSRF cookies (logout).
func (s *apiServer) clearSessionCookies(w http.ResponseWriter) {
	for _, name := range []string{sessionCookieName, csrfCookieName} {
		http.SetCookie(w, &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			Expires:  time.Unix(0, 0),
			MaxAge:   -1,
			HttpOnly: name == sessionCookieName,
			Secure:   s.cookieSecure(),
			SameSite: http.SameSiteStrictMode,
		})
	}
}

// handleLogin authenticates a local username/password against the user store
// and, on success, mints a role-bearing session cookie.
func (s *apiServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	hasLocal := s.users != nil && s.users.count() > 0
	ldapProv := s.currentLDAP()
	if !hasLocal && ldapProv == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "local accounts are not configured"})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	username := strings.TrimSpace(body.Username)

	// Per-account brute-force lockout: reject early (before touching the user
	// store / directory) when an account has too many recent failures.
	if locked, retryAfter := s.loginGuard.locked(username, time.Now()); locked {
		secs := int(retryAfter.Seconds()) + 1
		s.loginFailureTotal.Add(1)
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		s.audit(r, "auth.login", false, map[string]interface{}{"username": username, "error": "account_locked"})
		writeJSON(w, http.StatusTooManyRequests, envelope{
			Success:   false,
			Error:     fmt.Sprintf("too many failed attempts; this account is temporarily locked. Try again in about %ds.", secs),
			ErrorCode: "NCC_API_ACCOUNT_LOCKED",
		})
		return
	}

	// Local-first: the built-in admin and break-glass local accounts always
	// work even when AD is down or misconfigured.
	var (
		role       Role
		ok         bool
		mustChange bool
		method     = "local"
		ldapGroups []string
	)
	if hasLocal {
		role, ok, mustChange = s.users.verify(username, body.Password)
	}

	// AD fallback: only when the local store didn't authenticate the user.
	if !ok && ldapProv != nil {
		ldapRole, canonical, groups, authed, err := ldapProv.authenticate(username, body.Password)
		if err != nil {
			// Operational failure (dial/bind/search): log it but keep the
			// response generic so we don't leak directory topology.
			log.Printf("LDAP authentication error for %q: %v", username, err)
			s.loginFailureTotal.Add(1)
			s.audit(r, "auth.login", false, map[string]interface{}{"username": username, "method": "ldap", "error": "ldap_unavailable"})
			writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "invalid username or password"})
			return
		}
		if authed {
			if strings.TrimSpace(canonical) != "" {
				username = strings.TrimSpace(canonical)
			}
			role, ok, mustChange, method, ldapGroups = ldapRole, true, false, "ldap", groups
		}
	}

	if !ok {
		locked := s.loginGuard.recordFailure(username, time.Now())
		s.loginFailureTotal.Add(1)
		if locked {
			s.lockoutTotal.Add(1)
		}
		s.audit(r, "auth.login", false, map[string]interface{}{"username": username, "locked": locked})
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "invalid username or password"})
		return
	}
	// Successful auth clears any accumulated failure/lock state for the account.
	s.loginSuccessTotal.Add(1)
	s.loginGuard.reset(username)
	// AD/SAML group values are embedded in the session so cluster-group
	// membership is evaluated without re-querying the directory each request.
	token, exp, err := s.issueRoleSessionTokenWithGroups(cleanClientIP(r), username, role, ldapGroups)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if err := s.setSessionCookies(w, token, exp); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.login", true, map[string]interface{}{"username": username, "role": role.String(), "must_change": mustChange, "method": method})
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"username":             username,
		"role":                 role.String(),
		"must_change_password": mustChange,
		"expires_at":           exp.Format(time.RFC3339),
		"ttl_sec":              int(s.effectiveSessionTTL().Seconds()),
	}})
}

// handleChangePassword lets an authenticated local account set a new password,
// clearing the must-change flag. It requires a session (not a static token),
// verifies the current password, and enforces a minimum length.
func (s *apiServer) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.users == nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "local accounts are not configured"})
		return
	}
	p, ok := s.resolvePrincipal(r)
	if !ok || (p.method != authSessionCookie && p.method != authSessionBearer) {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "a logged-in session is required to change a password"})
		return
	}
	// Cookie sessions still require CSRF on this mutation.
	if p.method == authSessionCookie && !s.csrfValid(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "forbidden: missing or invalid CSRF token"})
		return
	}
	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	if err := dec.Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid JSON body"})
		return
	}
	if len(body.NewPassword) < minPasswordLen {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("new password must be at least %d characters", minPasswordLen)})
		return
	}
	if _, ok, _ := s.users.verify(p.subject, body.CurrentPassword); !ok {
		s.audit(r, "auth.password.change", false, map[string]interface{}{"username": p.subject, "reason": "bad_current"})
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "current password is incorrect"})
		return
	}
	hash, err := hashPassword(body.NewPassword)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if err := s.users.setPassword(p.subject, hash, false); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	// Re-issue the session cookie so the elevated session is fresh post-change.
	if token, exp, err := s.issueRoleSessionToken(cleanClientIP(r), p.subject, p.role); err == nil {
		_ = s.setSessionCookies(w, token, exp)
	}
	s.removeInitialPasswordFileIfBootstrap()
	s.audit(r, "auth.password.change", true, map[string]interface{}{"username": p.subject})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "password changed"})
}

// removeInitialPasswordFileIfBootstrap deletes the bootstrap password file once
// the admin password is changed.
func (s *apiServer) removeInitialPasswordFileIfBootstrap() {
	if s.users != nil {
		s.users.clearInitialPassword()
	}
}

// handleLogout clears the session and CSRF cookies.
func (s *apiServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	s.clearSessionCookies(w)
	s.audit(r, "auth.logout", true, nil)
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "logged out"})
}

// handleLogoutAll signs the caller out of every device by bumping their account
// token generation (invalidating all previously issued sessions) and clearing
// the local cookies. Only local password accounts have a revocable generation;
// SSO/static-token sessions report a clear error.
func (s *apiServer) handleLogoutAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	p, ok := principalFromContext(r.Context())
	if !ok {
		p, ok = s.resolvePrincipal(r)
	}
	if !ok {
		writeJSON(w, http.StatusUnauthorized, envelope{Success: false, Error: "unauthorized"})
		return
	}
	if s.users == nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "local accounts are not configured"})
		return
	}
	if _, found := s.users.lookup(p.subject); !found {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "only local password accounts can revoke all sessions"})
		return
	}
	if err := s.users.revokeSessions(p.subject); err != nil {
		writeUserStoreError(w, err)
		return
	}
	s.clearSessionCookies(w)
	s.audit(r, "auth.logout_all", true, map[string]interface{}{"username": p.subject})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "signed out of all sessions"})
}

// handleAuthRefresh re-issues the current session cookie with a fresh expiry
// (the effective session TTL) for the already-authenticated principal. The UI's
// inactivity "stay logged in" prompt calls it. withAuth has already validated
// the session (and CSRF for cookie POSTs); only browser/bearer sessions can be
// refreshed — static-token automation has no session and is rejected.
func (s *apiServer) handleAuthRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	p, ok := principalFromContext(r.Context())
	if !ok {
		// Not wrapped by withAuth (defensive); resolve directly.
		p, ok = s.resolvePrincipal(r)
	}
	if !ok || (p.method != authSessionCookie && p.method != authSessionBearer) {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "no refreshable session (sign in first)"})
		return
	}
	token, exp, err := s.issueRoleSessionToken(cleanClientIP(r), p.subject, p.role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if err := s.setSessionCookies(w, token, exp); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	s.audit(r, "auth.refresh", true, map[string]interface{}{"username": p.subject})
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"expires_at": exp.Format(time.RFC3339),
		"ttl_sec":    int(s.effectiveSessionTTL().Seconds()),
	}})
}

// handleForgotPassword queues a self-service password-reset request for an
// admin to action out-of-band. It is intentionally unauthenticated and never
// reveals whether the account exists (no enumeration): a request is recorded
// only for an existing local account, yet the response is always a generic 200.
// It inherits the global rate limiter applied to sensitive routes.
func (s *apiServer) handleForgotPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	const generic = "If that account exists, an administrator has been notified to reset it."
	var body struct {
		Username string `json:"username"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024))
	if err := dec.Decode(&body); err != nil {
		// Do not leak parsing detail; respond generically.
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: generic})
		return
	}
	username := strings.TrimSpace(body.Username)
	recorded := false
	adminReset := false
	if username != "" && s.users != nil {
		switch {
		case isReservedAdmin(username) && s.users.writable():
			// Admin lockout recovery: a queued reset request is useless when the
			// only admin is locked out, so self-service the built-in admin the
			// same way first-run setup does — regenerate a random password,
			// force a change at next login, invalidate existing admin sessions,
			// and surface the new password through the server logs and the
			// .ncc-initial-admin-password file (never over the network).
			//
			// A short per-IP cooldown blunts the force-rotation nuisance: an
			// anonymous caller cannot learn the new password, but without a
			// throttle they could repeatedly invalidate the real admin's
			// sessions. When throttled we respond 429 and skip the reset.
			if !s.allowAdminSelfReset(cleanClientIP(r), time.Now()) {
				s.audit(r, "auth.forgot_password", true, map[string]interface{}{"username": username, "admin_self_reset": false, "throttled": true})
				writeJSON(w, http.StatusTooManyRequests, envelope{
					Success:   false,
					Error:     "an admin password reset was requested very recently; wait a minute before trying again",
					ErrorCode: "NCC_API_RATE_LIMITED",
				})
				return
			}
			if pw, err := s.users.adminResetPassword(reservedAdminUsername); err == nil {
				hint := s.users.setInitialPassword(reservedAdminUsername, pw)
				s.loginGuard.reset(reservedAdminUsername) // recovery also clears any lockout
				logAdminPasswordReset("self-service via forgot-password", pw, hint)
				adminReset = true
			} else {
				log.Printf("forgot-password: admin self-reset failed: %v", err)
			}
		default:
			if _, ok := s.users.lookup(username); ok {
				s.users.addResetRequest(username, cleanClientIP(r))
				recorded = true
			}
		}
	}
	s.audit(r, "auth.forgot_password", true, map[string]interface{}{"username": username, "recorded": recorded, "admin_self_reset": adminReset})
	if adminReset {
		writeJSON(w, http.StatusOK, envelope{
			Success: true,
			Message: "A new temporary password for the admin account has been generated. Retrieve it from the ncc-api-server logs or the .ncc-initial-admin-password file on the server, then sign in and change it.",
			Data:    map[string]interface{}{"admin_reset": true},
		})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: generic})
}

// adminSelfResetCooldown bounds how often a single client may trigger the
// built-in admin's self-service password reset via forgot-password.
const adminSelfResetCooldown = 60 * time.Second

// allowAdminSelfReset returns true if the client IP has not triggered an admin
// self-reset within the cooldown window, recording the attempt when allowed. It
// reuses the fixed-window limiter (limit 1 per cooldown) so expired entries are
// garbage-collected and the per-IP map cannot grow without bound.
func (s *apiServer) allowAdminSelfReset(ip string, now time.Time) bool {
	s.adminResetMu.Lock()
	if s.adminResetLimiter == nil {
		s.adminResetLimiter = newFixedWindowRateLimiter(1, adminSelfResetCooldown)
	}
	lim := s.adminResetLimiter
	s.adminResetMu.Unlock()
	ok, _ := lim.allow("admin-self-reset:"+ip, now)
	return ok
}

// logAdminPasswordReset prints the first-run-style banner announcing a freshly
// generated admin password and where to retrieve it. The plaintext is only ever
// written to the server logs and the sibling .ncc-initial-admin-password file —
// it is never returned over the network for the anonymous recovery path.
func logAdminPasswordReset(reason, password, hint string) {
	log.Printf("==================================================================")
	log.Printf(" ADMIN PASSWORD RESET (%s)", reason)
	log.Printf("   username: %s", reservedAdminUsername)
	log.Printf("   password: %s", password)
	log.Printf("   You MUST change this password on next login.")
	if hint != "" {
		log.Printf("   retrieve it later from: %s", hint)
	}
	log.Printf("==================================================================")
}

// handleMe reports the current caller's identity, role, and which login
// methods are available. It is reachable without authentication so the UI can
// bootstrap and decide whether to show a login screen.
func (s *apiServer) handleMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := map[string]interface{}{
		"authenticated": false,
		"login_enabled": s.loginEnabled(),
		"local_enabled": s.users != nil && s.users.count() > 0,
		"saml_enabled":  s.samlIsEnabled(),
		"ldap_enabled":  s.ldapIsEnabled(),
		"auth_mode":     s.authMode,
	}
	// Surface (pre-login) whether the built-in admin still has its initial
	// bootstrap password, so the login screen can show the retrieval hint only
	// while it is relevant.
	if s.users != nil && s.users.bootstrapPending() {
		data["bootstrap_pending"] = true
	}
	data["session_ttl_sec"] = int(s.effectiveSessionTTL().Seconds())
	if p, ok := s.resolvePrincipal(r); ok {
		data["authenticated"] = true
		data["username"] = p.subject
		data["role"] = p.role.String()
		data["is_admin"] = p.role >= RoleAdmin
		data["can_operate"] = p.role >= RoleOperator
		data["must_change_password"] = p.mustChange
		// is_local marks sessions backed by a local password account (so the UI
		// can offer self-service password change). SSO/static-token sessions
		// have no local password and are excluded.
		if s.users != nil {
			if acct, ok := s.users.lookup(p.subject); ok {
				data["is_local"] = true
				if pref := strings.TrimSpace(acct.RunConfigPath); pref != "" {
					data["run_config_path"] = pref
				}
			}
		}
		if !p.expiresAt.IsZero() {
			data["expires_at"] = p.expiresAt.Format(time.RFC3339)
			if remaining := time.Until(p.expiresAt); remaining > 0 {
				data["expires_in_sec"] = int(remaining.Seconds())
			} else {
				data["expires_in_sec"] = 0
			}
		}
		// Cluster-group scope: tell the UI whether the caller sees all clusters
		// (admin/static token) or only a subset, and which ones, so it can hide
		// admin-only artifact downloads and label the visible scope.
		access := s.allowedClusters(p)
		data["cluster_access_unrestricted"] = access.unrestricted
		if !access.unrestricted {
			data["allowed_clusters"] = access.displayList()
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}
