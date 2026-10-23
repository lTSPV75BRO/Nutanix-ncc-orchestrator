package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFakeArchive(t *testing.T, dir string) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte("secret: prism-password"))
	_ = gz.Close()
	p := filepath.Join(dir, "b.tar.gz")
	if err := os.WriteFile(p, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBackupEnvelopeTamperMissingKeyAndWrongKey(t *testing.T) {
	for _, k := range []string{"NCC_BACKUP_KEY_FILE", "NCC_BACKUP_KEY", "NCC_BACKUP_PASSPHRASE"} {
		t.Setenv(k, "")
	}
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "backup.key")
	key := bytes.Repeat([]byte{7}, 32)
	if err := os.WriteFile(keyFile, []byte(base64.StdEncoding.EncodeToString(key)), 0o600); err != nil {
		t.Fatal(err)
	}
	arch := writeFakeArchive(t, dir)
	k, mode, salt, err := resolveBackupEncKey(keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := encryptBackupFile(arch, k, mode, salt); err != nil {
		t.Fatal(err)
	}
	sealed, _ := os.ReadFile(arch)
	if bytes.Contains(sealed, []byte("prism-password")) || !bytes.HasPrefix(sealed, []byte(backupEncMagic)) {
		t.Fatal("archive is not sealed")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 2 {
		t.Fatalf("sealing left temp files: %v", entries)
	}

	// Correct key decrypts.
	out, err := decryptBackupArchive(arch, keyFile, "")
	if err != nil {
		t.Fatalf("decrypt with the right key: %v", err)
	}
	_ = os.Remove(out)

	// Missing key fails closed.
	if _, err := decryptBackupArchive(arch, "", ""); err == nil {
		t.Fatal("decrypt without a key must fail")
	}
	// Wrong key fails.
	wrong := filepath.Join(dir, "wrong.key")
	_ = os.WriteFile(wrong, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))), 0o600)
	if _, err := decryptBackupArchive(arch, wrong, ""); err == nil || !strings.Contains(err.Error(), "decrypt failed") {
		t.Fatalf("wrong key must fail authentication, got %v", err)
	}
	// Ciphertext tampering is detected (GCM tag).
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-5] ^= 0x01
	tp := filepath.Join(dir, "tampered.enc")
	_ = os.WriteFile(tp, tampered, 0o600)
	if _, err := decryptBackupArchive(tp, keyFile, ""); err == nil {
		t.Fatal("tampered ciphertext must be rejected")
	}
	// Header tampering (mode byte is authenticated data) is detected.
	hdr := append([]byte(nil), sealed...)
	hdr[len(backupEncMagic)] = backupModePass
	hp := filepath.Join(dir, "hdr.enc")
	_ = os.WriteFile(hp, hdr, 0o600)
	if _, err := decryptBackupArchive(hp, "", "any passphrase"); err == nil {
		t.Fatal("tampered header must be rejected")
	}
	// Encryption with no key available fails instead of writing plaintext.
	if _, _, _, err := resolveBackupEncKey("", ""); err == nil {
		t.Fatal("encryption without a key must fail")
	}
}
