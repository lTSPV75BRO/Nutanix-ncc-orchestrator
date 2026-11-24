//go:build !windows

package winacl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNoOpOnOtherPlatforms(t *testing.T) {
	if Supported {
		t.Fatal("Supported must be false")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	if err := WriteFile(path, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if changed, err := Restrict(path, Private); changed || err != nil {
		t.Fatalf("Restrict = %v, %v", changed, err)
	}
	if changed, errs := RestrictTree(dir, Private); changed != nil || errs != nil {
		t.Fatalf("RestrictTree = %v, %v", changed, errs)
	}
	if broad, err := Check(path, Private); broad != nil || err != nil {
		t.Fatalf("Check = %v, %v", broad, err)
	}
	if broad, errs := CheckTree(dir, Private); broad != nil || errs != nil {
		t.Fatalf("CheckTree = %v, %v", broad, errs)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0o644 {
		t.Fatalf("mode changed: %v %v", st.Mode(), err)
	}
	dst := filepath.Join(dir, "g")
	if err := Rename(path, dst); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "a" {
		t.Fatal("rename lost the content")
	}
}
