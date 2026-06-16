package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSafeBackupPathAcceptsEncrypted pins that both plaintext (.tar.gz) and
// encrypted (.tar.gz.enc) snapshot names resolve, while traversal and other
// suffixes are rejected.
func TestSafeBackupPathAcceptsEncrypted(t *testing.T) {
	install := t.TempDir()
	s := &apiServer{repoRoot: install, configPath: filepath.Join(install, "config.yaml")}
	cases := []struct {
		name string
		want bool
	}{
		{"manual-20260610T120000Z.tar.gz", true},
		{"manual-20260610T120000Z.tar.gz.enc", true},
		{"pre-update-20260610T120000Z.tar.gz.enc", true},
		{"", false},
		{"evil.txt", false},
		{"../../etc/passwd", false},
		{"sub/dir.tar.gz", false},
		{"archive.tar.gz.enc.bak", false},
	}
	for _, c := range cases {
		_, ok := s.safeBackupPath(c.name)
		if ok != c.want {
			t.Errorf("safeBackupPath(%q) = %v, want %v", c.name, ok, c.want)
		}
	}
}

// TestListBackupEntriesMarksEncrypted pins that listing flags .enc archives as
// encrypted and includes both archive kinds.
func TestListBackupEntriesMarksEncrypted(t *testing.T) {
	install := t.TempDir()
	s := &apiServer{repoRoot: install, configPath: filepath.Join(install, "config.yaml")}
	dir := s.backupsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(dir, "manual-20260610T120000Z.tar.gz")
	enc := filepath.Join(dir, "manual-20260610T130000Z.tar.gz.enc")
	for _, p := range []string{plain, enc} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := s.listBackupEntries()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		got[e.Name] = e.Encrypted
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d (%v)", len(entries), entries)
	}
	if got["manual-20260610T120000Z.tar.gz"] {
		t.Error("plaintext archive should not be marked encrypted")
	}
	if !got["manual-20260610T130000Z.tar.gz.enc"] {
		t.Error("encrypted archive should be marked encrypted")
	}
}
