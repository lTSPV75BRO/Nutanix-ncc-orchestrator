package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return k
}

func TestSealOpenRoundTrip(t *testing.T) {
	key := testKey(t)
	plaintext := []byte(`{"users":[{"username":"admin","password_hash":"x"}],"ldap":{"bind_password":"s3cret"}}`)
	sealed, err := sealDocument(key, plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if !documentIsEncrypted(sealed) {
		t.Fatal("sealed document missing magic prefix")
	}
	if bytes.Contains(sealed, []byte("s3cret")) {
		t.Fatal("plaintext secret leaked into ciphertext")
	}
	out, err := openDocument(key, sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Fatalf("round-trip mismatch: got %q", out)
	}
}

func TestOpenWithWrongKeyFails(t *testing.T) {
	sealed, err := sealDocument(testKey(t), []byte("hello"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := openDocument(testKey(t), sealed); err == nil {
		t.Fatal("expected decryption with wrong key to fail")
	}
}

func TestNoncesAreUnique(t *testing.T) {
	key := testKey(t)
	a, _ := sealDocument(key, []byte("same"))
	b, _ := sealDocument(key, []byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("identical ciphertext for two seals — nonce reuse")
	}
}

func TestDecodeMasterKeyFormats(t *testing.T) {
	raw := testKey(t)
	cases := map[string][]byte{
		"base64-std": []byte(base64.StdEncoding.EncodeToString(raw)),
		"base64-raw": []byte(base64.RawStdEncoding.EncodeToString(raw)),
		"hex":        []byte(hex.EncodeToString(raw)),
		"raw32":      raw,
	}
	for name, enc := range cases {
		t.Run(name, func(t *testing.T) {
			k, err := decodeMasterKey(enc)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(k, raw) {
				t.Fatalf("key mismatch for %s", name)
			}
		})
	}
	if _, err := decodeMasterKey([]byte("too-short")); err == nil {
		t.Fatal("expected error for short key")
	}
}

// TestResolveUserStoreBackendFailsFastOnEncryptedWithoutKey is the lockout
// guard: an encrypted on-disk store with no master key configured must make the
// server refuse to start, rather than silently treat the ciphertext as a
// corrupt/empty store and bootstrap a fresh admin over the real accounts.
func TestResolveUserStoreBackendFailsFastOnEncryptedWithoutKey(t *testing.T) {
	// Ensure no key leaks in from the environment.
	t.Setenv(masterKeyEnv, "")
	t.Setenv(masterKeyFileEnv, "")

	dir := t.TempDir()
	storePath := filepath.Join(dir, ".ncc-api-users.json")
	sealed, err := sealDocument(testKey(t), []byte(`{"users":[{"username":"admin"}]}`))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := os.WriteFile(storePath, sealed, 0o600); err != nil {
		t.Fatalf("write store: %v", err)
	}

	s := &apiServer{usersDBPath: storePath}
	if _, err := s.resolveUserStoreBackend(); err == nil {
		t.Fatal("expected resolveUserStoreBackend to fail on an encrypted store with no key")
	} else if !strings.Contains(err.Error(), "no master key") {
		t.Fatalf("error should explain the missing key; got: %v", err)
	}

	// With the correct key the same store resolves to the encrypting backend
	// and decrypts cleanly.
	key := testKey(t)
	sealed2, _ := sealDocument(key, []byte(`{"users":[{"username":"admin"}]}`))
	if err := os.WriteFile(storePath, sealed2, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(masterKeyEnv, base64.StdEncoding.EncodeToString(key))
	be, err := s.resolveUserStoreBackend()
	if err != nil {
		t.Fatalf("resolve with correct key: %v", err)
	}
	if data, err := be.load(); err != nil || !bytes.Contains(data, []byte("admin")) {
		t.Fatalf("expected decrypted load to contain admin; data=%q err=%v", data, err)
	}
}

// memBackend is an in-memory userStoreBackend for testing the encrypting wrapper.
type memBackend struct {
	data []byte
	hint string
}

func (m *memBackend) load() ([]byte, error)                          { return m.data, nil }
func (m *memBackend) save(b []byte) error                            { m.data = append([]byte(nil), b...); return nil }
func (m *memBackend) setInitialPassword(_, _ string) (string, error) { return m.hint, nil }
func (m *memBackend) clearInitialPassword()                          {}
func (m *memBackend) location() string                               { return "mem" }

func TestEncryptingBackendMigratesPlaintext(t *testing.T) {
	plaintext := []byte(`{"users":[]}`)
	inner := &memBackend{data: plaintext}
	be := &encryptingBackend{inner: inner, key: testKey(t)}

	// Legacy plaintext store loads transparently.
	got, err := be.load()
	if err != nil {
		t.Fatalf("load plaintext: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("plaintext load mismatch: %q", got)
	}

	// Saving upgrades the at-rest document to ciphertext.
	updated := []byte(`{"users":[{"username":"admin"}]}`)
	if err := be.save(updated); err != nil {
		t.Fatalf("save: %v", err)
	}
	if !documentIsEncrypted(inner.data) {
		t.Fatal("inner store not encrypted after save")
	}

	// And reading back decrypts to the original.
	got, err = be.load()
	if err != nil {
		t.Fatalf("load ciphertext: %v", err)
	}
	if !bytes.Equal(got, updated) {
		t.Fatalf("ciphertext round-trip mismatch: %q", got)
	}
}
