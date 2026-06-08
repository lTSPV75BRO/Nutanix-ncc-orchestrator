package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// patPrefix tags every personal access token so it is easy to recognize in
// logs/secret scanners and so PAT verification can skip non-PAT credentials
// (static tokens, signed sessions) cheaply.
const patPrefix = "ncc_pat_"

// Personal access token expiry policy (in days). A bounded, non-zero expiry is
// required so a leaked token cannot live forever.
const (
	patDefaultExpiryDays = 90
	patMaxExpiryDays     = 365
)

// extractAPIToken returns the raw credential from X-API-Token, falling back to
// an Authorization: Bearer header. Empty when neither is present.
func extractAPIToken(r *http.Request) string {
	if t := strings.TrimSpace(r.Header.Get("X-API-Token")); t != "" {
		return t
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		return strings.TrimSpace(authz[len("bearer "):])
	}
	return ""
}

// patHash returns the SHA-256 hex digest of a token secret. The secret is
// high-entropy (256 bits), so a plain digest is a safe, fast lookup key — no
// per-request bcrypt is needed and we never persist the plaintext.
func patHash(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// principalFromPAT resolves a request authenticated by a personal access token.
// ok is false when no PAT credential is present or it is invalid/expired.
func (s *apiServer) principalFromPAT(r *http.Request) (principal, bool) {
	if s.users == nil {
		return principal{}, false
	}
	tok := extractAPIToken(r)
	if !strings.HasPrefix(tok, patPrefix) {
		return principal{}, false
	}
	pt, found := s.users.findTokenByHash(patHash(tok))
	if !found {
		return principal{}, false
	}
	if pt.ExpiresAt != "" {
		if exp, err := time.Parse(time.RFC3339, pt.ExpiresAt); err == nil && time.Now().After(exp) {
			return principal{}, false
		}
	}
	role, _ := parseRole(pt.Role)
	subject := pt.Owner
	groups := pt.Groups
	if pt.OwnerLocal {
		// Re-resolve the live state for local owners so role changes,
		// deletions, and forced password changes take effect immediately.
		acct, ok := s.users.lookup(subject)
		if !ok {
			return principal{}, false
		}
		if acct.MustChange {
			return principal{}, false
		}
		if r2, ok := parseRole(acct.Role); ok {
			role = r2
		}
		groups = nil
	}
	if role == RoleNone {
		return principal{}, false
	}
	// Record last-used time (throttled to at most once a minute per token inside,
	// so this rarely touches the store on the auth path).
	s.users.touchTokenLastUsed(pt.ID, cleanClientIP(r))
	return principal{role: role, subject: subject, method: authPAT, groups: groups}, true
}

// handleAuthTokens is the self-service personal-access-token endpoint:
//
//	GET  /api/v1/auth/tokens  -> list the caller's own tokens (metadata only)
//	POST /api/v1/auth/tokens  -> mint a new token (plaintext returned once)
func (s *apiServer) handleAuthTokens(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "personal access tokens require a writable user store (enable local accounts)"})
		return
	}
	p, _ := principalFromContext(r.Context())
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"tokens": s.users.listTokensForOwner(p.subject),
		}})
	case http.MethodPost:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var body struct {
			Name          string `json:"name"`
			ExpiresInDays *int   `json:"expires_in_days"`
		}
		if err := decodeJSON(r.Body, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		name := strings.TrimSpace(body.Name)
		if name == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "name is required"})
			return
		}
		if len(name) > 80 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "name must be 80 characters or fewer"})
			return
		}
		days := patDefaultExpiryDays
		if body.ExpiresInDays != nil {
			days = *body.ExpiresInDays
		}
		if days < 1 {
			days = 1
		}
		if days > patMaxExpiryDays {
			days = patMaxExpiryDays
		}
		secret, err := randToken(32)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "failed to generate token"})
			return
		}
		secret = patPrefix + secret
		id, err := randToken(9)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "failed to generate token id"})
			return
		}
		now := time.Now().UTC()
		_, ownerLocal := s.users.lookup(p.subject)
		pt := personalToken{
			ID:         id,
			Name:       name,
			Owner:      p.subject,
			OwnerLocal: ownerLocal,
			Role:       p.role.String(),
			Groups:     append([]string(nil), p.groups...),
			Hash:       patHash(secret),
			CreatedAt:  now.Format(time.RFC3339),
			ExpiresAt:  now.Add(time.Duration(days) * 24 * time.Hour).Format(time.RFC3339),
			CreatedIP:  cleanClientIP(r),
		}
		if err := s.users.addToken(pt); err != nil {
			s.audit(r, "auth.token.create", false, map[string]interface{}{"name": name, "error": err.Error()})
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "auth.token.create", true, map[string]interface{}{"id": id, "name": name, "role": pt.Role, "expires_at": pt.ExpiresAt})
		writeJSON(w, http.StatusCreated, envelope{Success: true, Message: "store this token now — it will not be shown again", Data: map[string]interface{}{
			"id":         id,
			"name":       name,
			"role":       pt.Role,
			"token":      secret,
			"created_at": pt.CreatedAt,
			"expires_at": pt.ExpiresAt,
		}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// handleAuthTokenByID revokes one of the caller's own tokens:
//
//	DELETE /api/v1/auth/tokens/<id>
func (s *apiServer) handleAuthTokenByID(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "personal access tokens require a writable user store (enable local accounts)"})
		return
	}
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/auth/tokens/")
	id = strings.TrimSpace(strings.Trim(id, "/"))
	if id == "" || strings.Contains(id, "/") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "token id is required"})
		return
	}
	p, _ := principalFromContext(r.Context())
	// Self-service: a user may revoke only their own tokens (isAdmin=false).
	_, removed, err := s.users.deleteToken(id, p.subject, false)
	if err == errTokenForbidden {
		s.audit(r, "auth.token.revoke", false, map[string]interface{}{"id": id, "reason": "not_owner"})
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "you can only revoke your own tokens"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if !removed {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "token not found"})
		return
	}
	s.audit(r, "auth.token.revoke", true, map[string]interface{}{"id": id})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "token revoked"})
}

// handleAdminTokens is the admin-wide token inventory:
//
//	GET /api/v1/settings/tokens -> list every user's tokens (metadata only)
func (s *apiServer) handleAdminTokens(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "personal access tokens require a writable user store (enable local accounts)"})
		return
	}
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"tokens": s.users.listAllTokens(),
	}})
}

// handleAdminTokenByID lets an admin revoke any user's token:
//
//	DELETE /api/v1/settings/tokens/<id>
func (s *apiServer) handleAdminTokenByID(w http.ResponseWriter, r *http.Request) {
	if s.users == nil || !s.users.writable() {
		writeJSON(w, http.StatusNotImplemented, envelope{Success: false, Error: "personal access tokens require a writable user store (enable local accounts)"})
		return
	}
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/settings/tokens/")
	id = strings.TrimSpace(strings.Trim(id, "/"))
	if id == "" || strings.Contains(id, "/") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "token id is required"})
		return
	}
	p, _ := principalFromContext(r.Context())
	removedTok, removed, err := s.users.deleteToken(id, p.subject, true)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	if !removed {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "token not found"})
		return
	}
	s.audit(r, "auth.token.revoke", true, map[string]interface{}{"id": id, "owner": removedTok.Owner, "by": "admin"})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "token revoked"})
}
