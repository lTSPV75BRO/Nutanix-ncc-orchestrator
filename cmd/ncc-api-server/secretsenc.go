package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Secrets at rest. The file-backed user database (users.json) holds the most
// sensitive material the orchestrator manages: bcrypt password hashes, PAT
// SHA-256 hashes, the SAML SP private key, and the LDAP service-account bind
// password. On a plain-file install these live in a 0600 JSON document — safe
// from other local users, but plaintext to anyone who can read the disk or a
// backup archive. Operators who want defense-in-depth can supply a 32-byte
// master key; when present the api-server transparently wraps the whole
// document with AES-256-GCM (envelope-style: the master key encrypts the
// document encryption performed per-save with a fresh random nonce).
//
// Key sources (first match wins):
//  1. NCC_MASTER_KEY env — 32 bytes as base64 (std/raw/url) or hex.
//  2. --users-db-key-file / NCC_MASTER_KEY_FILE — a file whose contents are the
//     key (base64/hex, or exactly 32 raw bytes). Keep this OFF the protected
//     disk (e.g. a tmpfs, a secret mount, or a KMS-fetched file) so the key and
//     the ciphertext are not co-located in the same backup.
//
// When no key is configured the store stays plaintext (unchanged, fully
// backward compatible). Migration is automatic and transparent: a legacy
// plaintext store is read as-is and re-written encrypted on the next save.
//
// In Kubernetes the recommended path remains a Secret backend (encrypted at
// rest by etcd/KMS) — this file-level envelope is for non-k8s installs that
// still want the on-disk user store and backups to be unreadable without the
// master key.
const (
	masterKeyEnv     = "NCC_MASTER_KEY"
	masterKeyFileEnv = "NCC_MASTER_KEY_FILE"
)

// encMagic prefixes an encrypted document so load() can distinguish ciphertext
// from a legacy plaintext JSON store and migrate transparently. It is also used
// as GCM additional-authenticated-data so a stripped prefix is detected.
var encMagic = []byte("NCCENC1\n")

// loadMasterKey resolves a 32-byte key from the env var or a key file. Returns
// (nil, nil) when no key is configured (encryption disabled — the default).
func loadMasterKey(keyFile string) ([]byte, error) {
	if raw := strings.TrimSpace(os.Getenv(masterKeyEnv)); raw != "" {
		return decodeMasterKey([]byte(raw))
	}
	if strings.TrimSpace(keyFile) == "" {
		keyFile = strings.TrimSpace(os.Getenv(masterKeyFileEnv))
	}
	if strings.TrimSpace(keyFile) == "" {
		return nil, nil
	}
	b, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read master key file %q: %w", keyFile, err)
	}
	return decodeMasterKey(b)
}

// decodeMasterKey accepts a 32-byte key as base64 (std/raw/url), hex, or exactly
// 32 raw bytes (after trimming surrounding whitespace).
func decodeMasterKey(raw []byte) ([]byte, error) {
	s := strings.TrimSpace(string(raw))
	for _, dec := range []func(string) ([]byte, error){
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.URLEncoding.DecodeString,
		hex.DecodeString,
	} {
		if k, err := dec(s); err == nil && len(k) == 32 {
			return k, nil
		}
	}
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	return nil, fmt.Errorf("master key must decode to 32 bytes: provide base64 or hex of a 32-byte key (e.g. `openssl rand -base64 32`)")
}

// documentIsEncrypted reports whether a stored blob carries the envelope magic.
func documentIsEncrypted(data []byte) bool { return bytes.HasPrefix(data, encMagic) }

// sealDocument encrypts plaintext with AES-256-GCM under key, prefixing the
// envelope magic. Layout: magic || nonce || ciphertext+tag.
func sealDocument(key, plaintext []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := crand.Read(nonce); err != nil {
		return nil, err
	}
	ct := gcm.Seal(nil, nonce, plaintext, encMagic)
	out := make([]byte, 0, len(encMagic)+len(nonce)+len(ct))
	out = append(out, encMagic...)
	out = append(out, nonce...)
	out = append(out, ct...)
	return out, nil
}

// openDocument decrypts a sealed document produced by sealDocument.
func openDocument(key, data []byte) ([]byte, error) {
	if !documentIsEncrypted(data) {
		return nil, errors.New("not an encrypted user store")
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	body := data[len(encMagic):]
	ns := gcm.NonceSize()
	if len(body) < ns {
		return nil, errors.New("encrypted user store is truncated")
	}
	nonce, ct := body[:ns], body[ns:]
	pt, err := gcm.Open(nil, nonce, ct, encMagic)
	if err != nil {
		return nil, fmt.Errorf("decrypt user store failed (wrong %s / corrupt store?): %w", masterKeyEnv, err)
	}
	return pt, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// encryptingBackend wraps any userStoreBackend, encrypting the document at rest
// with a master key. Reads auto-detect (and tolerate) a legacy plaintext store
// so enabling encryption never locks an operator out: the store is upgraded to
// ciphertext on the next write. Non-document operations (initial-password hint,
// location) delegate to the inner backend.
type encryptingBackend struct {
	inner userStoreBackend
	key   []byte
}

func (b *encryptingBackend) load() ([]byte, error) {
	data, err := b.inner.load()
	if err != nil || len(data) == 0 {
		return data, err
	}
	if !documentIsEncrypted(data) {
		return data, nil // legacy plaintext — re-encrypted on next save()
	}
	return openDocument(b.key, data)
}

func (b *encryptingBackend) save(data []byte) error {
	sealed, err := sealDocument(b.key, data)
	if err != nil {
		return fmt.Errorf("encrypt user store: %w", err)
	}
	return b.inner.save(sealed)
}

func (b *encryptingBackend) setInitialPassword(username, password string) (string, error) {
	return b.inner.setInitialPassword(username, password)
}

func (b *encryptingBackend) clearInitialPassword() { b.inner.clearInitialPassword() }

func (b *encryptingBackend) location() string {
	return b.inner.location() + " (encrypted at rest, AES-256-GCM)"
}
