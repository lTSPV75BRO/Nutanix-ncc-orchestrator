// Package mcpclient is the shared gate between ncc-mcp-server, ncc-orchestrator
// and the API. Self-reported versions (ldflags, User-Agent, env) are not
// trusted. A 2.4.0+ client must present a time-bound HMAC proof keyed by
// NCC_MCP_TOKEN using protocol ncc-mcp-proof-v1, which 2.3.x cannot produce.
//
// This is a compatibility check, not authentication: the key is the PAT that
// the client also sends, so any holder of the PAT can compute a valid proof, and
// a caller that does not identify as an MCP client is not checked at all. The
// PAT's role and the audit log are what limit and record an MCP client.
package mcpclient

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Masterminds/semver/v3"
)

const (
	// MinVersion is the first MCP server the 2.4 stack will talk to.
	MinVersion = "2.4.0"
	// ProofProtocol is bound into every HMAC. Changing the product version
	// string on an old binary does not mint a valid proof.
	ProofProtocol = "ncc-mcp-proof-v1"
	// VersionEnv is set on orchestrator subprocesses by ncc-mcp-server 2.4.0+.
	VersionEnv = "NCC_MCP_CLIENT_VERSION"
	// ProofEnv carries the HMAC proof for subprocesses.
	ProofEnv = "NCC_MCP_CLIENT_PROOF"
	// TokenEnv is the personal access token the MCP host must configure.
	TokenEnv = "NCC_MCP_TOKEN"
	// VersionHeader identifies the claimed MCP product version on HTTP calls.
	VersionHeader = "X-NCC-MCP-Version"
	// ProofHeader carries the HMAC proof on HTTP calls.
	ProofHeader = "X-NCC-MCP-Proof"
	// ToolHeader names the MCP tool that caused an HTTP call (audit).
	ToolHeader = "X-NCC-MCP-Tool"
	// UserAgentPrefix is the User-Agent product token for ncc-mcp-server.
	UserAgentPrefix = "ncc-mcp-server/"
	// ErrorCode is returned when the MCP client is too old or unidentified.
	ErrorCode = "NCC_API_MCP_UPGRADE_REQUIRED"
	// ProofErrorCode is returned when a caller claims to be MCP but the proof is bad.
	ProofErrorCode = "NCC_API_MCP_PROOF_INVALID"
	// MinTokenBytes rejects trivial stand-ins for a PAT.
	MinTokenBytes = 16
	// MaxProofSkew is the allowed clock difference for a proof timestamp.
	MaxProofSkew = 2 * time.Minute
)

var (
	ErrUpgrade      = errors.New("mcp client upgrade required")
	ErrInvalidProof = errors.New("mcp client proof invalid")
	ErrMissingToken = errors.New("mcp token required")
)

// UpgradeMessage is the operator-facing error for a 2.3.x (or unidentified) MCP client.
func UpgradeMessage() string {
	return "this MCP server is too old for this stack; upgrade ncc-mcp-server to " + MinVersion + " or later and set " + TokenEnv + " to a personal access token"
}

// Remediation tells the operator how to recover from UpgradeMessage.
func Remediation() string {
	return "Replace the MCP host command with the " + MinVersion + " ncc-mcp-server binary, mint a PAT in the dashboard, set " + TokenEnv + " in the MCP env, and reload MCP."
}

// InvalidProofMessage is returned when a caller spoofs a 2.4 version without a valid HMAC.
func InvalidProofMessage() string {
	return "MCP client proof is invalid; a 2.3.x binary cannot impersonate " + MinVersion + " by setting the version string. Use the " + MinVersion + " ncc-mcp-server binary and a personal access token"
}

// VersionAllowed reports whether v is a 2.4.0+ MCP client version.
func VersionAllowed(v string) bool {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" {
		return false
	}
	got, err := semver.NewVersion(v)
	if err != nil {
		return false
	}
	min, err := semver.NewVersion(MinVersion)
	if err != nil {
		return false
	}
	return !got.LessThan(min)
}

// ParseRequestVersion reads the claimed MCP version from HTTP headers.
// isMCP is true when the caller identified as ncc-mcp-server.
func ParseRequestVersion(header, userAgent string) (version string, isMCP bool) {
	if v := strings.TrimSpace(header); v != "" {
		return v, true
	}
	ua := strings.TrimSpace(userAgent)
	lower := strings.ToLower(ua)
	prefix := strings.ToLower(UserAgentPrefix)
	if strings.HasPrefix(lower, prefix) {
		rest := ua
		if len(ua) >= len(UserAgentPrefix) {
			rest = ua[len(UserAgentPrefix):]
		}
		if i := strings.IndexAny(rest, " \t"); i >= 0 {
			rest = rest[:i]
		}
		return strings.TrimSpace(rest), true
	}
	return "", false
}

// HeaderSafeVersion returns a semver string safe to put on HTTP headers.
func HeaderSafeVersion(v string) string {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if !VersionAllowed(v) {
		return MinVersion
	}
	got, err := semver.NewVersion(v)
	if err != nil {
		return MinVersion
	}
	return got.String()
}

// UserAgent returns the User-Agent value for this MCP product version.
func UserAgent(productVersion string) string {
	return UserAgentPrefix + HeaderSafeVersion(productVersion)
}

// MissingTokenError is returned when a current MCP client has no PAT configured.
func MissingTokenError() error {
	return fmt.Errorf("%w: mint a personal access token, set %s, and use ncc-mcp-server %s or later", ErrMissingToken, TokenEnv, MinVersion)
}

// SignProof mints a time-bound HMAC proof. Only 2.4.0+ source includes this.
func SignProof(token, version string, now time.Time) (string, error) {
	token = strings.TrimSpace(token)
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if len(token) < MinTokenBytes {
		return "", MissingTokenError()
	}
	if !VersionAllowed(version) {
		return "", fmt.Errorf("%w: %s", ErrUpgrade, UpgradeMessage())
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ts := now.UTC().Unix()
	mac := proofMAC(token, version, ts)
	return "v1." + strconv.FormatInt(ts, 10) + "." + hex.EncodeToString(mac), nil
}

// VerifyProof checks token, claimed version, and HMAC. A 2.3.x binary that
// only changes its version string cannot satisfy this.
func VerifyProof(token, version, proof string, now time.Time) error {
	token = strings.TrimSpace(token)
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	proof = strings.TrimSpace(proof)
	if token == "" && proof == "" && version == "" {
		return fmt.Errorf("%w: %s", ErrUpgrade, UpgradeMessage())
	}
	if len(token) < MinTokenBytes {
		return MissingTokenError()
	}
	if proof == "" || !VersionAllowed(version) {
		return fmt.Errorf("%w: %s", ErrUpgrade, UpgradeMessage())
	}
	parts := strings.Split(proof, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return fmt.Errorf("%w: %s", ErrInvalidProof, InvalidProofMessage())
	}
	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || ts <= 0 {
		return fmt.Errorf("%w: %s", ErrInvalidProof, InvalidProofMessage())
	}
	got, err := hex.DecodeString(parts[2])
	if err != nil || len(got) != sha256.Size {
		return fmt.Errorf("%w: %s", ErrInvalidProof, InvalidProofMessage())
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	delta := now.UTC().Sub(time.Unix(ts, 0).UTC())
	if delta < 0 {
		delta = -delta
	}
	if delta > MaxProofSkew {
		return fmt.Errorf("%w: %s", ErrInvalidProof, InvalidProofMessage())
	}
	want := proofMAC(token, version, ts)
	if !hmac.Equal(got, want) {
		return fmt.Errorf("%w: %s", ErrInvalidProof, InvalidProofMessage())
	}
	return nil
}

// SanitizeToolName allows only MCP tool identifiers in audit logs and headers.
func SanitizeToolName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			b.WriteRune(r)
		}
		if b.Len() >= 64 {
			break
		}
	}
	return b.String()
}

func proofMAC(token, version string, unix int64) []byte {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = fmt.Fprintf(mac, "%s\n%s\n%d", ProofProtocol, version, unix)
	return mac.Sum(nil)
}
