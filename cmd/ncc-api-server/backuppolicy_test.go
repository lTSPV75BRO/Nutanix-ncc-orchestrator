package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func clearBackupKeyEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"NCC_BACKUP_KEY_FILE", "NCC_BACKUP_KEY", "NCC_BACKUP_PASSPHRASE", backupEncryptionEnv} {
		t.Setenv(k, "")
	}
}

func TestBackupEncryptionPlan(t *testing.T) {
	clearBackupKeyEnv(t)
	if _, _, _, err := backupEncryptionPlan(""); err == nil {
		t.Fatal("default policy must refuse a plaintext backup")
	}
	enc, env, src, err := backupEncryptionPlan("correct horse battery staple")
	if err != nil || !enc || src != "request_passphrase" || len(env) != 1 || !strings.HasPrefix(env[0], "NCC_BACKUP_PASSPHRASE=") {
		t.Fatalf("passphrase plan = %v %v %q %v", enc, env, src, err)
	}
	t.Setenv("NCC_BACKUP_KEY_FILE", "/run/secrets/ncc-backup-key")
	if enc, env, src, err := backupEncryptionPlan(""); err != nil || !enc || src != "server_key" || len(env) != 0 {
		t.Fatalf("server key plan = %v %v %q %v", enc, env, src, err)
	}
	t.Setenv("NCC_BACKUP_KEY_FILE", "")
	t.Setenv(backupEncryptionEnv, "optional")
	if enc, _, src, err := backupEncryptionPlan(""); err != nil || enc || src != "none" {
		t.Fatalf("optional plan = %v %q %v", enc, src, err)
	}
}

func TestBackupFileEncryptedDetectsEnvelope(t *testing.T) {
	dir := t.TempDir()
	enc := filepath.Join(dir, "a.tar.gz.enc")
	plain := filepath.Join(dir, "b.tar.gz")
	if err := os.WriteFile(enc, append([]byte(backupEncMagicAPI), 'K', 1, 2, 3), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plain, []byte{0x1f, 0x8b, 8, 0}, 0o600); err != nil {
		t.Fatal(err)
	}
	if !backupFileEncrypted(enc) || backupFileEncrypted(plain) || backupFileEncrypted(filepath.Join(dir, "missing")) {
		t.Fatal("envelope detection is wrong")
	}
}

// Under the default policy the API refuses to create or download a plaintext
// archive (fail securely) and records why, without running v2-backup.
func TestBackupEndpointsRefusePlaintextByDefault(t *testing.T) {
	clearBackupKeyEnv(t)
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, auditLogPath: filepath.Join(dir, "audit.log")}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/backups", bytes.NewBufferString(`{}`))
	req = withPrincipal(req, principal{subject: "admin", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleBackupCreate(rec, req)
	if rec.Code != http.StatusPreconditionFailed || !strings.Contains(rec.Body.String(), "NCC_BACKUP_ENCRYPTION_REQUIRED") {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	req = withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/settings/backup", nil), principal{subject: "admin", role: RoleAdmin})
	rec = httptest.NewRecorder()
	s.handleBackup(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("download: %d %s", rec.Code, rec.Body.String())
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "audit.log")); !strings.Contains(string(b), `"encrypted":false`) || !strings.Contains(string(b), "encryption_required") {
		t.Fatalf("refusals must be audited with the real encryption status: %s", b)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "backups"))
	if len(entries) > 0 {
		t.Fatalf("no archive may be written: %v", entries)
	}
}
