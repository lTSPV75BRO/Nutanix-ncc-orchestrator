package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

// withEmbeddedKey temporarily sets the package-level embedded public key for a
// test and restores it afterward.
func withEmbeddedKey(t *testing.T, b64 string) {
	t.Helper()
	prev := releaseSigningPublicKeyB64
	releaseSigningPublicKeyB64 = b64
	t.Cleanup(func() { releaseSigningPublicKeyB64 = prev })
}

func TestVerifyReleaseSignatureSkippedWithoutKey(t *testing.T) {
	withEmbeddedKey(t, "")
	status, err := verifyReleaseSignature([]byte("anything"), "")
	if err != nil {
		t.Fatalf("expected no error when no key embedded, got %v", err)
	}
	if status != sigSkipped {
		t.Fatalf("expected SKIPPED, got %s", status)
	}
}

func TestVerifyReleaseSignatureValidAndTampered(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	withEmbeddedKey(t, base64.StdEncoding.EncodeToString(pub))

	msg := []byte("ncc-orchestrator-linux-amd64  abc123\n")
	sig := base64.StdEncoding.EncodeToString(ed25519.Sign(priv, msg))

	status, err := verifyReleaseSignature(msg, sig)
	if err != nil || status != sigValid {
		t.Fatalf("expected VALID, got %s err=%v", status, err)
	}

	// Tampered message must fail.
	status, err = verifyReleaseSignature([]byte("tampered"), sig)
	if err == nil || status != sigInvalid {
		t.Fatalf("expected INVALID on tamper, got %s err=%v", status, err)
	}

	// Garbage signature must fail.
	if status, err := verifyReleaseSignature(msg, "!!notbase64!!"); err == nil || status != sigInvalid {
		t.Fatalf("expected INVALID on bad sig, got %s err=%v", status, err)
	}
}

func TestSignReleaseFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "k.key")
	pubB64, err := generateReleaseSigningKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	inPath := filepath.Join(dir, "checksums.txt")
	content := []byte("deadbeef  ncc-orchestrator-linux-amd64\n")
	if err := os.WriteFile(inPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	sigPath := filepath.Join(dir, "checksums.txt.sig")
	if err := signReleaseFile(keyPath, inPath, sigPath); err != nil {
		t.Fatal(err)
	}
	sigB64, err := os.ReadFile(sigPath)
	if err != nil {
		t.Fatal(err)
	}
	withEmbeddedKey(t, pubB64)
	status, err := verifyReleaseSignature(content, string(sigB64))
	if err != nil || status != sigValid {
		t.Fatalf("round-trip verify failed: status=%s err=%v", status, err)
	}
}
