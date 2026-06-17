package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// TestPruneManualBackupsKeepsRollbackPoints pins that retention prunes only the
// oldest manual-* snapshots (both plaintext and encrypted) and never touches
// pre-update-* rollback points or non-backup files.
func TestPruneManualBackupsKeepsRollbackPoints(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	mk := func(name string, ageMin int) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		mt := base.Add(time.Duration(ageMin) * time.Minute)
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	// 4 manual snapshots (mixed plaintext/encrypted) at increasing mtimes, plus
	// two rollback points and an unrelated file.
	mk("manual-a.tar.gz", 0)
	mk("manual-b.tar.gz.enc", 10)
	mk("manual-c.tar.gz", 20)
	mk("manual-d.tar.gz.enc", 30)
	mk("pre-update-x.tar.gz", 5)
	mk("pre-update-y.tar.gz.enc", 40)
	mk("notes.txt", 0)

	pruned := pruneManualBackups(dir, 2)
	if len(pruned) != 2 {
		t.Fatalf("expected 2 pruned, got %d (%v)", len(pruned), pruned)
	}
	exists := func(n string) bool {
		_, err := os.Stat(filepath.Join(dir, n))
		return err == nil
	}
	// Two newest manual snapshots survive; two oldest are gone.
	if !exists("manual-d.tar.gz.enc") || !exists("manual-c.tar.gz") {
		t.Error("two newest manual snapshots should be kept")
	}
	if exists("manual-a.tar.gz") || exists("manual-b.tar.gz.enc") {
		t.Error("two oldest manual snapshots should be pruned")
	}
	// Rollback points and unrelated files are untouched.
	for _, n := range []string{"pre-update-x.tar.gz", "pre-update-y.tar.gz.enc", "notes.txt"} {
		if !exists(n) {
			t.Errorf("%s should never be pruned by manual retention", n)
		}
	}

	// retain<=0 is a no-op.
	if got := pruneManualBackups(dir, 0); got != nil {
		t.Errorf("retain=0 should prune nothing, got %v", got)
	}
}

// TestApiRouteCatalogInOpenAPI pins the route catalog as the single source of
// truth: every catalog path is present in the generated OpenAPI spec and is
// annotated with a required role.
func TestApiRouteCatalogInOpenAPI(t *testing.T) {
	s := &apiServer{}
	spec := s.buildOpenAPISpec()
	paths, ok := spec["paths"].(map[string]interface{})
	if !ok {
		t.Fatal("spec has no paths map")
	}
	for _, rt := range apiRouteCatalog() {
		if rt.MinRole == "" {
			t.Errorf("catalog route %s has no MinRole", rt.Path)
		}
		item, ok := paths[rt.Path].(map[string]interface{})
		if !ok {
			t.Errorf("OpenAPI spec is missing catalog path %s", rt.Path)
			continue
		}
		for _, m := range rt.Methods {
			op, ok := item[strings.ToLower(m)].(map[string]interface{})
			if !ok {
				continue // hand-authored item may not enumerate every method
			}
			if _, has := op["x-required-role"]; !has {
				t.Errorf("%s %s missing x-required-role annotation", m, rt.Path)
			}
		}
	}
}
