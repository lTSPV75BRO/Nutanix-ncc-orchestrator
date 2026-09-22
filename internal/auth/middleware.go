package auth

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Auth type values injected into request context.
const (
	AuthTypeJWT    = "jwt"
	AuthTypePAT    = "pat"
	AuthTypeStatic = "static"
)

// Failure reasons for ncc_api_auth_failure_total{reason=...}.
const (
	ReasonExpired = "expired"
	ReasonInvalid = "invalid"
	ReasonMissing = "missing"
)

// Source of the presented credential.
const (
	SourceHeader = "header"
	SourceCookie = "cookie"
)

type ctxKey int

const (
	ctxUserID ctxKey = iota
	ctxRole
	ctxAuthType
	ctxIdentity
)

// PATOwner is the identity resolved from a hashed personal access token.
type PATOwner struct {
	UserID string
	Role   string
	Groups []string
}

// UserStore looks up a PAT by the SHA-256 hex digest of the presented secret.
// Implementations must not log or return the plaintext token.
type UserStore interface {
	LookupPAT(tokenHash string) (PATOwner, bool)
}

// Identity is the resolved caller after Authenticate succeeds.
type Identity struct {
	UserID   string
	Role     string
	AuthType string
	Source   string // header or cookie (drives CSRF in the api-server)
	Gen      int
	Groups   []string
}

// UserIDFromContext returns the authenticated user id, or "".
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxUserID).(string)
	return v
}

// RoleFromContext returns the authenticated role, or "".
func RoleFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxRole).(string)
	return v
}

// AuthTypeFromContext returns jwt, pat, or static, or "".
func AuthTypeFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxAuthType).(string)
	return v
}

// IdentityFromContext returns the full Identity previously stored by AuthMiddleware.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	v, ok := ctx.Value(ctxIdentity).(Identity)
	return v, ok
}

func withIdentity(ctx context.Context, ident Identity) context.Context {
	ctx = context.WithValue(ctx, ctxUserID, ident.UserID)
	ctx = context.WithValue(ctx, ctxRole, ident.Role)
	ctx = context.WithValue(ctx, ctxAuthType, ident.AuthType)
	ctx = context.WithValue(ctx, ctxIdentity, ident)
	return ctx
}

// ExtractToken returns the presented credential and its source. Order:
// Authorization: Bearer, then X-API-Token (legacy clients), then the
// auth_token cookie.
func ExtractToken(r *http.Request) (token, source string) {
	if r == nil {
		return "", ""
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		if tok := strings.TrimSpace(authz[7:]); tok != "" {
			return tok, SourceHeader
		}
	}
	if tok := strings.TrimSpace(r.Header.Get("X-API-Token")); tok != "" {
		return tok, SourceHeader
	}
	if c, err := r.Cookie(CookieName); err == nil {
		if tok := strings.TrimSpace(c.Value); tok != "" {
			return tok, SourceCookie
		}
	}
	return "", ""
}

// Authenticate resolves the caller from JWT, hashed PAT, or a static bearer
// token. It never logs the raw token. err is one of ErrMissing, ErrExpired,
// ErrInvalid on failure.
func Authenticate(r *http.Request, jwtSecret []byte, userStore UserStore, staticToken string) (*Identity, error) {
	token, source := ExtractToken(r)
	if token == "" {
		return nil, ErrMissing
	}

	if static := strings.TrimSpace(staticToken); static != "" && constantTimeEqual(token, static) {
		return &Identity{
			UserID:   "static-admin-token",
			Role:     "admin",
			AuthType: AuthTypeStatic,
			Source:   source,
		}, nil
	}

	if LooksLikeJWT(token) {
		claims, err := ValidateJWT(token, jwtSecret)
		if err != nil {
			return nil, err
		}
		return &Identity{
			UserID:   claims.Subject,
			Role:     claims.Role,
			AuthType: AuthTypeJWT,
			Source:   source,
			Gen:      claims.Gen,
			Groups:   append([]string(nil), claims.Groups...),
		}, nil
	}

	if IsPAT(token) {
		if userStore == nil {
			return nil, ErrInvalid
		}
		owner, ok := userStore.LookupPAT(HashPAT(token))
		if !ok || strings.TrimSpace(owner.UserID) == "" {
			return nil, ErrInvalid
		}
		role := strings.TrimSpace(owner.Role)
		if role == "" {
			return nil, ErrInvalid
		}
		return &Identity{
			UserID:   strings.TrimSpace(owner.UserID),
			Role:     role,
			AuthType: AuthTypePAT,
			Source:   source,
			Groups:   append([]string(nil), owner.Groups...),
		}, nil
	}

	return nil, ErrInvalid
}

// AuthMiddleware authenticates each request with the hybrid JWT + hashed-PAT +
// static-token scheme and injects userID, role, and auth_type into the
// request context. Failures return 401 {"error":"Unauthorized"} and increment
// Prometheus counters. Raw tokens are never logged.
func AuthMiddleware(jwtSecret []byte, userStore UserStore, staticToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ident, err := Authenticate(r, jwtSecret, userStore, staticToken)
			if err != nil {
				reason := ReasonOf(err)
				RecordFailure(reason)
				logAuthFailure(r, err, reason)
				writeUnauthorized(w)
				return
			}
			RecordSuccess()
			next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), *ident)))
		})
	}
}

// ReasonOf maps an Authenticate error onto a Prometheus failure reason.
func ReasonOf(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrExpired):
		return ReasonExpired
	case errors.Is(err, ErrMissing):
		return ReasonMissing
	default:
		return ReasonInvalid
	}
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized"})
}

func logAuthFailure(r *http.Request, err error, reason string) {
	userID := ""
	authType := ""
	if r != nil {
		if tok, _ := ExtractToken(r); tok != "" {
			switch {
			case LooksLikeJWT(tok):
				authType = AuthTypeJWT
			case IsPAT(tok):
				authType = AuthTypePAT
			default:
				authType = AuthTypeStatic
			}
		}
	}
	log.Printf("auth failed reason=%s auth_type=%s user_id=%s remote_ip=%s err=%v",
		reason, authType, userID, remoteIP(r), err)
}

func remoteIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// NewSessionCookie builds the HttpOnly auth_token cookie (SameSite=Lax;
// Secure when production TLS is on).
func NewSessionCookie(jwt string, exp time.Time, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     CookieName,
		Value:    jwt,
		Path:     "/",
		Expires:  exp,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	}
}

// ExpiredSessionCookie clears auth_token.
func ExpiredSessionCookie(secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	}
}

// Prometheus counters. Hand-rolled (no client_golang) so the api-server can
// emit them next to its existing ncc_* series.
var (
	authSuccessTotal atomic.Int64
	authFailExpired  atomic.Int64
	authFailInvalid  atomic.Int64
	authFailMissing  atomic.Int64
)

// RecordSuccess increments ncc_api_auth_success_total.
func RecordSuccess() { authSuccessTotal.Add(1) }

// RecordFailure increments ncc_api_auth_failure_total for the given reason.
func RecordFailure(reason string) {
	switch reason {
	case ReasonExpired:
		authFailExpired.Add(1)
	case ReasonMissing:
		authFailMissing.Add(1)
	default:
		authFailInvalid.Add(1)
	}
}

// MetricsSnapshot is a point-in-time view of auth counters (process start = 0).
type MetricsSnapshot struct {
	Success        int64
	FailureExpired int64
	FailureInvalid int64
	FailureMissing int64
}

// Snapshot returns the current auth counter values.
func Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Success:        authSuccessTotal.Load(),
		FailureExpired: authFailExpired.Load(),
		FailureInvalid: authFailInvalid.Load(),
		FailureMissing: authFailMissing.Load(),
	}
}

// WritePrometheus emits ncc_api_auth_success_total and
// ncc_api_auth_failure_total{reason="..."} in Prometheus text format.
func WritePrometheus(w http.ResponseWriter) {
	if w == nil {
		return
	}
	s := Snapshot()
	_, _ = w.Write([]byte("# HELP ncc_api_auth_success_total Successful API authentication attempts since process start.\n"))
	_, _ = w.Write([]byte("# TYPE ncc_api_auth_success_total counter\n"))
	_, _ = w.Write([]byte("ncc_api_auth_success_total "))
	writeInt(w, s.Success)
	_, _ = w.Write([]byte("# HELP ncc_api_auth_failure_total Failed API authentication attempts by reason (expired|invalid|missing).\n"))
	_, _ = w.Write([]byte("# TYPE ncc_api_auth_failure_total counter\n"))
	writeLabeled(w, "ncc_api_auth_failure_total", ReasonExpired, s.FailureExpired)
	writeLabeled(w, "ncc_api_auth_failure_total", ReasonInvalid, s.FailureInvalid)
	writeLabeled(w, "ncc_api_auth_failure_total", ReasonMissing, s.FailureMissing)
}

func writeInt(w http.ResponseWriter, n int64) {
	_, _ = w.Write([]byte(itoa(n)))
	_, _ = w.Write([]byte("\n"))
}

func writeLabeled(w http.ResponseWriter, name, reason string, n int64) {
	_, _ = w.Write([]byte(name))
	_, _ = w.Write([]byte("{reason=\""))
	_, _ = w.Write([]byte(reason))
	_, _ = w.Write([]byte("\"} "))
	_, _ = w.Write([]byte(itoa(n)))
	_, _ = w.Write([]byte("\n"))
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
