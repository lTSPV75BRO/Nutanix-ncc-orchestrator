package main

import (
	"os/exec"
	"path/filepath"
	"testing"

	"goncc/internal/winacl"
)

// In a folder every user can read, the token, the users database and the
// initial password are closed from the first write and stay closed.
func TestSecretFilesAreClosedOnWindows(t *testing.T) {
	dir := t.TempDir()
	if out, err := exec.Command("icacls", dir, "/grant", "*S-1-5-32-545:(OI)(CI)M").CombinedOutput(); err != nil {
		t.Fatalf("icacls: %v %s", err, out)
	}
	s := &apiServer{repoRoot: dir, tokenFilePath: filepath.Join(dir, ".ncc-api-token")}
	db := &fileStoreBackend{path: filepath.Join(dir, ".ncc-api-users.json")}
	for range 2 {
		if err := s.ensureAuthToken(); err != nil {
			t.Fatal(err)
		}
		if err := db.save([]byte("{}")); err != nil {
			t.Fatal(err)
		}
		pw, err := db.setInitialPassword("admin", "x")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{s.tokenFilePath, db.path, pw} {
			if broad, err := winacl.Check(p, winacl.Private); err != nil || len(broad) > 0 {
				t.Fatalf("%s: open to %v (%v)", p, broad, err)
			}
		}
	}
	if err := mkdirPrivate(filepath.Join(dir, "backups")); err != nil {
		t.Fatal(err)
	}
	if broad, err := winacl.Check(filepath.Join(dir, "backups"), winacl.Private); err != nil || len(broad) > 0 {
		t.Fatalf("backups: open to %v (%v)", broad, err)
	}
}
