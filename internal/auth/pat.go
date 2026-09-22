package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
)

// PATPrefix tags every personal access token so logs/secret scanners can
// recognize it and verification can skip non-PAT credentials cheaply.
const PATPrefix = "ncc_pat_"

// GeneratePAT returns a high-entropy PAT (ncc_pat_ + 32 random bytes as hex)
// and the SHA-256 hex digest that should be persisted. The plaintext is never
// stored — callers must display it once at creation.
func GeneratePAT() (plainToken string, tokenHash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("pat: generate: %w", err)
	}
	plain := PATPrefix + hex.EncodeToString(b)
	return plain, HashPAT(plain), nil
}

// HashPAT returns the SHA-256 hex digest of a PAT secret. The secret is
// high-entropy (256 bits), so a plain digest is a safe, fast lookup key.
func HashPAT(plainToken string) string {
	sum := sha256.Sum256([]byte(plainToken))
	return hex.EncodeToString(sum[:])
}

// ValidatePATHash reports whether providedToken hashes to storedHash using
// constant-time comparison. Empty inputs never match.
func ValidatePATHash(providedToken, storedHash string) bool {
	if strings.TrimSpace(providedToken) == "" || strings.TrimSpace(storedHash) == "" {
		return false
	}
	got := HashPAT(providedToken)
	return constantTimeEqual(got, storedHash)
}

// IsPAT reports whether token uses the ncc_pat_ prefix.
func IsPAT(token string) bool {
	return strings.HasPrefix(strings.TrimSpace(token), PATPrefix)
}

func constantTimeEqual(a, b string) bool {
	ab := []byte(a)
	bb := []byte(b)
	if len(ab) != len(bb) {
		return false
	}
	return subtle.ConstantTimeCompare(ab, bb) == 1
}
