package mcpclient

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestVersionAllowed(t *testing.T) {
	if VersionAllowed("") || VersionAllowed("2.3.0") || VersionAllowed("2.3.9") || VersionAllowed("latest") {
		t.Fatal("2.3.x and empty/non-semver must be refused")
	}
	if !VersionAllowed("2.4.0") || !VersionAllowed("v2.4.1") || !VersionAllowed("2.5.0") {
		t.Fatal("2.4.0 and later must be allowed")
	}
}

func TestParseRequestVersion(t *testing.T) {
	v, isMCP := ParseRequestVersion("2.3.0", "")
	if !isMCP || v != "2.3.0" {
		t.Fatalf("header: got %q %v", v, isMCP)
	}
	v, isMCP = ParseRequestVersion("", "ncc-mcp-server/2.4.0")
	if !isMCP || v != "2.4.0" {
		t.Fatalf("ua: got %q %v", v, isMCP)
	}
	v, isMCP = ParseRequestVersion("", "ncc-mcp-server/2.4.0 (Cursor)")
	if !isMCP || v != "2.4.0" {
		t.Fatalf("ua with comment: got %q %v", v, isMCP)
	}
	_, isMCP = ParseRequestVersion("", "curl/8.0")
	if isMCP {
		t.Fatal("non-MCP user-agent must not be flagged")
	}
}

func TestSignAndVerifyProof(t *testing.T) {
	now := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	token := "ncc_pat_testtoken12"
	proof, err := SignProof(token, "2.4.0", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyProof(token, "2.4.0", proof, now); err != nil {
		t.Fatal(err)
	}
}

func TestProofRejectsVersionStampWithoutHMAC(t *testing.T) {
	now := time.Now().UTC()
	token := "ncc_pat_testtoken12"
	err := VerifyProof(token, "2.4.0", "", now)
	if err == nil || !errors.Is(err, ErrUpgrade) {
		t.Fatalf("claimed 2.4.0 without proof must be upgrade, got %v", err)
	}
}

func TestProofRejectsStampedOldBinaryHMAC(t *testing.T) {
	now := time.Now().UTC()
	token := "ncc_pat_testtoken12"
	// Proof computed as if protocol were missing / version 2.3.0 cannot be
	// presented as 2.4.0: the HMAC binds protocol + version.
	proof, err := SignProof(token, "2.4.0", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyProof(token, "2.3.0", proof, now); err == nil {
		t.Fatal("proof for 2.4.0 must not validate a 2.3.0 claim")
	}
	if err := VerifyProof("ncc_pat_other_token1", "2.4.0", proof, now); err == nil {
		t.Fatal("proof must not validate a different token")
	}
}

func TestProofRejectsTamperAndSkew(t *testing.T) {
	now := time.Now().UTC()
	token := "ncc_pat_testtoken12"
	proof, err := SignProof(token, "2.4.0", now)
	if err != nil {
		t.Fatal(err)
	}
	tampered := proof[:len(proof)-2] + "00"
	if err := VerifyProof(token, "2.4.0", tampered, now); err == nil || !errors.Is(err, ErrInvalidProof) {
		t.Fatalf("tampered proof: %v", err)
	}
	if err := VerifyProof(token, "2.4.0", proof, now.Add(3*time.Minute)); err == nil {
		t.Fatal("expired proof must be rejected")
	}
	if err := VerifyProof(token, "2.4.0", "v1.notanint.abcd", now); err == nil {
		t.Fatal("malformed proof must be rejected")
	}
	if err := VerifyProof(token, "2.4.0", "v2.1.aa", now); err == nil {
		t.Fatal("unknown proof version must be rejected")
	}
}

func TestSignProofRejectsShortTokenAndOldVersion(t *testing.T) {
	now := time.Now().UTC()
	if _, err := SignProof("short", "2.4.0", now); err == nil || !errors.Is(err, ErrMissingToken) {
		t.Fatalf("short token: %v", err)
	}
	if _, err := SignProof("ncc_pat_testtoken12", "2.3.0", now); err == nil || !errors.Is(err, ErrUpgrade) {
		t.Fatalf("old version: %v", err)
	}
}

func TestHeaderSafeVersionStripsCRLF(t *testing.T) {
	if HeaderSafeVersion("2.4.0\r\nX-Evil: 1") != MinVersion {
		t.Fatal("CRLF version must not be forwarded")
	}
	if HeaderSafeVersion("2.4.0") != "2.4.0" {
		t.Fatal(HeaderSafeVersion("2.4.0"))
	}
}

func TestParseAPIURLRejectsCredentials(t *testing.T) {
	if _, err := ParseAPIURL("http://ncc_pat_x@127.0.0.1:8081", "NCC_MCP_API_URL"); err == nil {
		t.Fatal("userinfo")
	}
	if _, err := ParseAPIURL("https://ok.example:8081", "NCC_MCP_API_URL"); err != nil {
		t.Fatal(err)
	}
}

func TestProofProtocolIsBound(t *testing.T) {
	if ProofProtocol != "ncc-mcp-proof-v1" {
		t.Fatal(ProofProtocol)
	}
	now := time.Now().UTC()
	token := "ncc_pat_testtoken12"
	proof, _ := SignProof(token, "2.4.0", now)
	if strings.Count(proof, ".") != 2 || !strings.HasPrefix(proof, "v1.") {
		t.Fatalf("wire format %q", proof)
	}
}
