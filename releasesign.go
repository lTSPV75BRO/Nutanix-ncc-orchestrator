package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Release signing adds cryptographic provenance on top of the SHA-256
// checksums.txt: the maintainer signs checksums.txt with an Ed25519 private key
// kept offline, publishes the detached signature as `checksums.txt.sig` (base64
// of the 64-byte Ed25519 signature), and embeds the corresponding *public* key
// in the binary. `verify --online` and the in-app updater then verify that
// signature against the embedded key before trusting any hash — so a tampered
// or MITM'd checksums.txt (which would otherwise let an attacker swap both the
// binary and its checksum) is rejected.
//
// Ed25519 verification uses only the Go standard library, so verification works
// offline against the embedded key with no external tool (gpg/cosign) or
// network dependency beyond fetching the signature asset itself.
//
// releaseSigningPublicKeyB64 is intentionally empty by default and is set at
// release build time via -ldflags "-X main.releaseSigningPublicKeyB64=<b64>"
// (or by editing this value on a signed-release branch). With no key embedded,
// signature verification degrades to "skipped" so unsigned/dev builds keep
// working; --require-signature turns a skip into a hard failure.
var releaseSigningPublicKeyB64 = ""

// releaseSignatureAssetName is the conventional detached-signature asset that
// pairs with checksums.txt on a GitHub release.
const releaseSignatureAssetName = "checksums.txt.sig"

// signatureStatus is the outcome of a signature check, surfaced in `verify`.
type signatureStatus string

const (
	sigValid   signatureStatus = "VALID"
	sigInvalid signatureStatus = "INVALID"
	sigMissing signatureStatus = "MISSING"
	sigSkipped signatureStatus = "SKIPPED" // no public key embedded in this build
)

// embeddedReleasePublicKey decodes the embedded Ed25519 public key. ok=false
// when no key is embedded (the default for unsigned builds).
func embeddedReleasePublicKey() (ed25519.PublicKey, bool) {
	raw := strings.TrimSpace(releaseSigningPublicKeyB64)
	if raw == "" {
		return nil, false
	}
	key, err := decodeReleaseKey(raw)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, false
	}
	return ed25519.PublicKey(key), true
}

// decodeReleaseKey accepts std or url base64 (with or without padding).
func decodeReleaseKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, errors.New("not valid base64")
}

// verifyReleaseSignature checks sigB64 (base64 Ed25519 signature) over message
// against the embedded public key. It returns a status the caller renders:
//   - sigSkipped: no public key embedded (unsigned build) — not an error
//   - sigValid:   signature verified
//   - sigInvalid: signature present but does not verify (error)
func verifyReleaseSignature(message []byte, sigB64 string) (signatureStatus, error) {
	pub, ok := embeddedReleasePublicKey()
	if !ok {
		return sigSkipped, nil
	}
	sig, err := decodeReleaseKey(sigB64)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return sigInvalid, fmt.Errorf("malformed signature")
	}
	if !ed25519.Verify(pub, message, sig) {
		return sigInvalid, fmt.Errorf("signature does not verify against the embedded release public key")
	}
	return sigValid, nil
}

// fetchReleaseSignatureAsset downloads the checksums.txt.sig asset body (the
// base64 signature string). Returns ok=false when the release has no signature
// asset.
func fetchReleaseSignatureAsset(rel *githubRelease, client *http.Client) (sigB64 string, ok bool, err error) {
	if rel == nil {
		return "", false, errors.New("nil release")
	}
	for _, a := range rel.Assets {
		an := strings.ToLower(strings.TrimSpace(a.Name))
		if an == releaseSignatureAssetName || (strings.Contains(an, "checksums") && strings.HasSuffix(an, ".sig")) {
			body, ferr := fetchURL(a.BrowserDownloadURL, client)
			if ferr != nil {
				return "", true, fmt.Errorf("fetch signature asset %s: %w", a.Name, ferr)
			}
			return strings.TrimSpace(string(body)), true, nil
		}
	}
	return "", false, nil
}

// verifyChecksumSignature verifies the signature over the checksums.txt body.
// `require` forces a hard error when the build has an embedded key but the
// signature is missing/invalid (or, for the updater, when no key is embedded
// it stays advisory). Returns the rendered status and an error to fail on.
func verifyChecksumSignature(rel *githubRelease, csBody []byte, client *http.Client, require bool) (signatureStatus, error) {
	_, haveKey := embeddedReleasePublicKey()
	if !haveKey {
		if require {
			return sigSkipped, errors.New("signature required but this build has no embedded release public key")
		}
		return sigSkipped, nil
	}
	sigB64, present, err := fetchReleaseSignatureAsset(rel, client)
	if err != nil {
		return sigMissing, err
	}
	if !present {
		if require {
			return sigMissing, fmt.Errorf("no %s asset on the release (signature required)", releaseSignatureAssetName)
		}
		return sigMissing, nil
	}
	return verifyReleaseSignature(csBody, sigB64)
}

// --- maintainer-side signing helpers (used by the release build) ---

// generateReleaseSigningKey writes a new Ed25519 private key (base64, 64 bytes)
// to privPath (0600) and returns the matching public key as base64 std.
func generateReleaseSigningKey(privPath string) (publicB64 string, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	privB64 := base64.StdEncoding.EncodeToString(priv)
	if err := os.WriteFile(privPath, []byte(privB64+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write private key %s: %w", privPath, err)
	}
	return base64.StdEncoding.EncodeToString(pub), nil
}

// signReleaseFile signs the bytes of inPath with the base64 Ed25519 private key
// in keyPath and writes the base64 detached signature to outPath.
func signReleaseFile(keyPath, inPath, outPath string) error {
	keyRaw, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("read signing key: %w", err)
	}
	privBytes, err := decodeReleaseKey(string(keyRaw))
	if err != nil || len(privBytes) != ed25519.PrivateKeySize {
		return fmt.Errorf("signing key must be a base64 Ed25519 private key (%d bytes)", ed25519.PrivateKeySize)
	}
	msg, err := os.ReadFile(inPath)
	if err != nil {
		return fmt.Errorf("read input %s: %w", inPath, err)
	}
	sig := ed25519.Sign(ed25519.PrivateKey(privBytes), msg)
	sigB64 := base64.StdEncoding.EncodeToString(sig)
	if err := os.WriteFile(outPath, []byte(sigB64+"\n"), 0o644); err != nil {
		return fmt.Errorf("write signature %s: %w", outPath, err)
	}
	return nil
}
