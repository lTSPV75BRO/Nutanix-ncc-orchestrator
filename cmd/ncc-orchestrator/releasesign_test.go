package main

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
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

func TestSignedBuildRefusesReleaseWithoutSignatureAsset(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	withEmbeddedKey(t, base64.StdEncoding.EncodeToString(pub))
	rel := &githubRelease{TagName: "v9.9.9"} // no checksums.txt.sig asset
	if _, err := verifyChecksumSignature(rel, []byte("sums"), nil, true); err == nil {
		t.Fatal("a signed build must refuse a release that has no signature")
	}
	// The advisory mode (verify --online without --require-signature) only reports it.
	if status, err := verifyChecksumSignature(rel, []byte("sums"), nil, false); err != nil || status != sigMissing {
		t.Fatalf("advisory mode: status=%s err=%v", status, err)
	}
}

func releaseKeyForms(t *testing.T) (priv ed25519.PrivateKey, privForms, pubForms map[string]string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	b64 := base64.StdEncoding.EncodeToString
	privPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}))
	pubPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki}))
	privForms = map[string]string{
		"release-keygen":   b64(priv) + "\n",
		"64 bytes":         string(priv),
		"PEM":              privPEM,
		"PEM CRLF and BOM": "\ufeff" + strings.ReplaceAll(privPEM, "\n", "\r\n"),
		"PEM on one line":  strings.ReplaceAll(strings.TrimSpace(privPEM), "\n", " "),
		"DER":              string(pkcs8),
		"base64 DER":       b64(pkcs8),
	}
	pubForms = map[string]string{
		"one line":        b64(pub),
		"trailing break":  b64(pub) + "\r\n",
		"PEM":             pubPEM,
		"PEM CRLF":        strings.ReplaceAll(pubPEM, "\n", "\r\n"),
		"PEM on one line": strings.ReplaceAll(strings.TrimSpace(pubPEM), "\n", " "),
		"base64 DER":      b64(spki),
	}
	return priv, privForms, pubForms
}

func TestReleaseKeyForms(t *testing.T) {
	priv, privForms, pubForms := releaseKeyForms(t)
	want := base64.StdEncoding.EncodeToString(priv.Public().(ed25519.PublicKey))
	dir := t.TempDir()
	in := filepath.Join(dir, "checksums.txt")
	if err := os.WriteFile(in, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "k")
	for name, form := range privForms {
		t.Run("private "+name, func(t *testing.T) {
			if err := os.WriteFile(keyPath, []byte(form), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := signReleaseFile(keyPath, in, in+".sig"); err != nil {
				t.Fatal(err)
			}
			sig, err := os.ReadFile(in + ".sig")
			if err != nil {
				t.Fatal(err)
			}
			withEmbeddedKey(t, want)
			if st, err := verifyReleaseSignature([]byte("x"), string(sig)); st != sigValid {
				t.Fatalf("status %s: %v", st, err)
			}
			if got, err := releasePublicKey(keyPath, ""); err != nil || got != want {
				t.Fatalf("public key %q, %v; want %q", got, err, want)
			}
		})
	}
	for name, form := range pubForms {
		t.Run("public "+name, func(t *testing.T) {
			if got, err := releasePublicKey(keyPath, form); err != nil || got != want {
				t.Fatalf("got %q, %v; want %q", got, err, want)
			}
			if got, err := releasePublicKey("", form); err != nil || got != want {
				t.Fatalf("without the key: got %q, %v; want %q", got, err, want)
			}
		})
	}
}

func TestReleaseKeyRefusals(t *testing.T) {
	priv, privForms, pubForms := releaseKeyForms(t)
	_, otherPriv, otherPub := releaseKeyForms(t)
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ecDER, err := x509.MarshalPKCS8PrivateKey(ec)
	if err != nil {
		t.Fatal(err)
	}
	ecPub, err := x509.MarshalPKIXPublicKey(&ec.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	block := func(label string, b []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: label, Bytes: b}))
	}
	mixed := append(append([]byte{}, priv.Seed()...), make([]byte, ed25519.PublicKeySize)...)
	for name, c := range map[string]struct{ in, want string }{
		"empty":       {"", "must be an Ed25519 private key"},
		"text":        {"hello", "must be an Ed25519 private key"},
		"seed only":   {base64.StdEncoding.EncodeToString(priv.Seed()), "must be an Ed25519 private key"},
		"not a pair":  {base64.StdEncoding.EncodeToString(mixed), "not a key pair"},
		"public PEM":  {pubForms["PEM"], "not a \"PRIVATE KEY\""},
		"encrypted":   {block("ENCRYPTED PRIVATE KEY", []byte("x")), "encrypted"},
		"openssh":     {block("OPENSSH PRIVATE KEY", []byte("x")), "not a \"PRIVATE KEY\""},
		"ECDSA":       {block("PRIVATE KEY", ecDER), "ecdsa.PrivateKey"},
		"no END line": {"-----BEGIN PRIVATE KEY-----\nAAAA\n", "no END line"},
	} {
		if _, err := parseReleasePrivateKey([]byte(c.in)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("private %s: got %v, want %q", name, err, c.want)
		}
	}
	for name, c := range map[string]struct{ in, want string }{
		"private PEM": {privForms["PEM"], "not a \"PUBLIC KEY\""},
		"private":     {privForms["release-keygen"], "public key must be"},
		"ECDSA":       {block("PUBLIC KEY", ecPub), "must be Ed25519"},
		"text":        {"not base64!", "not valid base64"},
		"raw in PEM":  {block("PUBLIC KEY", priv.Public().(ed25519.PublicKey)), "public key must be"},
	} {
		_, err := releasePublicKey("", c.in)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("public %s: got %v, want %q", name, err, c.want)
			continue
		}
		if body := strings.TrimSpace(privForms["release-keygen"]); strings.Contains(err.Error(), body) {
			t.Errorf("public %s: the error contains the private key", name)
		}
	}
	keyPath := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(keyPath, []byte(otherPriv["PEM"]), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := releasePublicKey(keyPath, otherPub["PEM"]); err != nil {
		t.Errorf("matching pair refused: %v", err)
	}
	if _, err := releasePublicKey(keyPath, pubForms["PEM"]); err == nil || !strings.Contains(err.Error(), "does not belong") {
		t.Errorf("mismatch: got %v", err)
	}
	if _, err := releasePublicKey("", " "); err == nil {
		t.Error("no input accepted")
	}
}
