// Package auth implements stateless hybrid authentication for the NCC API
// server: HMAC-SHA256 JWTs for browser sessions and SHA-256-hashed personal
// access tokens (PATs) for programmatic access. Verification is memory-first
// and identical on Linux, macOS, and Windows — there is no on-disk session
// store, so any replica that shares NCC_JWT_SECRET can validate a token.
package auth

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// TokenTypeSession is the JWT token_type claim for interactive UI sessions.
	TokenTypeSession = "session"

	// CookieName is the HttpOnly cookie that carries a session JWT.
	CookieName = "auth_token"

	// DefaultTokenExpiry is used when NCC_TOKEN_EXPIRY is unset.
	DefaultTokenExpiry = 24 * time.Hour
)

// Sentinel errors returned by ValidateJWT / Authenticate. Middleware maps
// these onto Prometheus failure reasons (expired|invalid|missing).
var (
	ErrMissing = errors.New("missing credentials")
	ErrExpired = errors.New("token expired")
	ErrInvalid = errors.New("invalid token")
)

// CustomClaims is the JWT payload for a browser session. Registered claims
// cover sub/iat/exp; Role and TokenType are required application claims.
// Gen and Groups are optional metadata used by the api-server for session
// revocation and cluster-group membership; they are omitted from tokens
// minted by GenerateSessionJWT.
type CustomClaims struct {
	jwt.RegisteredClaims
	Role      string   `json:"role"`
	TokenType string   `json:"token_type"`
	Gen       int      `json:"gen,omitempty"`
	Groups    []string `json:"groups,omitempty"`
}

// GenerateSessionJWT mints an HS256 JWT for an interactive session.
func GenerateSessionJWT(userID, role string, duration time.Duration, secret []byte) (string, error) {
	return GenerateSessionJWTWithMeta(userID, role, duration, secret, 0, nil)
}

// GenerateSessionJWTWithMeta is GenerateSessionJWT plus a token-generation
// counter (bumped on password change so every replica can reject stale
// sessions) and directory group values captured at login.
func GenerateSessionJWTWithMeta(userID, role string, duration time.Duration, secret []byte, gen int, groups []string) (string, error) {
	userID = strings.TrimSpace(userID)
	role = strings.TrimSpace(role)
	if userID == "" {
		return "", fmt.Errorf("jwt: user id is required")
	}
	if role == "" {
		return "", fmt.Errorf("jwt: role is required")
	}
	if duration <= 0 {
		return "", fmt.Errorf("jwt: duration must be positive")
	}
	if len(secret) == 0 {
		return "", fmt.Errorf("jwt: signing secret is required")
	}
	now := time.Now().UTC()
	claims := CustomClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(duration)),
		},
		Role:      role,
		TokenType: TokenTypeSession,
		Gen:       gen,
		Groups:    append([]string(nil), groups...),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("jwt: sign: %w", err)
	}
	return signed, nil
}

// ValidateJWT parses and verifies an HS256 session JWT. Expired tokens return
// ErrExpired; any other verification failure returns ErrInvalid.
func ValidateJWT(tokenStr string, secret []byte) (*CustomClaims, error) {
	tokenStr = strings.TrimSpace(tokenStr)
	if tokenStr == "" {
		return nil, ErrMissing
	}
	if len(secret) == 0 {
		return nil, ErrInvalid
	}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)
	parsed, err := parser.ParseWithClaims(tokenStr, &CustomClaims{}, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return secret, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpired
		}
		return nil, ErrInvalid
	}
	claims, ok := parsed.Claims.(*CustomClaims)
	if !ok || !parsed.Valid {
		return nil, ErrInvalid
	}
	if strings.TrimSpace(claims.TokenType) != TokenTypeSession {
		return nil, ErrInvalid
	}
	if strings.TrimSpace(claims.Subject) == "" || strings.TrimSpace(claims.Role) == "" {
		return nil, ErrInvalid
	}
	// Allow a small clock skew on iat so replicas with a few seconds of drift
	// still accept a freshly minted token. Expiry itself is fail-closed (no leeway).
	if claims.IssuedAt != nil && claims.IssuedAt.Time.After(time.Now().UTC().Add(30*time.Second)) {
		return nil, ErrInvalid
	}
	return claims, nil
}

// LooksLikeJWT reports whether s has the three-segment JOSE shape of a JWT.
// Used to skip HMAC-session verification on bearer credentials that are JWTs.
func LooksLikeJWT(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			n++
		}
	}
	return n == 2
}
