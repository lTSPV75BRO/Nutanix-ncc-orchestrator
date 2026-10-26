package main

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeRestoreTestArchive builds a minimal v2 backup (manifest + data/ entries, in
// the given order) and returns its path.
func writeRestoreTestArchive(t *testing.T, entries [][2]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "backup.tar.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	add := func(name, body string) {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	add("manifest.json", `{"tool":"ncc-orchestrator","version":"2.2.1"}`)
	for _, e := range entries {
		add("data/"+e[0], e[1])
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// Regression (DATA-02): a restore that fails part-way used to leave the
// install with some files already overwritten. It is now transactional: the
// failure rolls back every replaced file and leaves no staging debris.
func TestV2RestoreFailureRollsBack(t *testing.T) {
	install := t.TempDir()
	cfg := filepath.Join(install, "config.yaml")
	if err := os.WriteFile(cfg, []byte("clusters: original\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// "blocker" is a regular file, so the archive's blocker/state.json
	// cannot be written (its parent must be a directory) — after config.yaml
	// has already been replaced.
	if err := os.WriteFile(filepath.Join(install, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "clusters: restored\n"},
		{"blocker/state.json", "{}"},
	})

	err := runV2Restore(v2RestoreOptions{InstallDir: install, InputFile: archive, Force: true, NoRestart: true})
	if err == nil {
		t.Fatal("expected the restore to fail")
	}
	if !strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("error should say the restore was rolled back: %v", err)
	}
	got, readErr := os.ReadFile(cfg)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != "clusters: original\n" {
		t.Fatalf("config.yaml not rolled back: %q", got)
	}
	ents, _ := os.ReadDir(install)
	for _, e := range ents {
		if strings.Contains(e.Name(), ".ncc-restore-stage-") || strings.Contains(e.Name(), updateAsideMarker) {
			t.Fatalf("restore left debris behind: %s", e.Name())
		}
	}
}

func TestV2RestoreSuccessReplacesFiles(t *testing.T) {
	install := t.TempDir()
	cfg := filepath.Join(install, "config.yaml")
	if err := os.WriteFile(cfg, []byte("clusters: original\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "clusters: restored\n"},
		{"nested/state.json", "{}"},
	})
	if err := runV2Restore(v2RestoreOptions{InstallDir: install, InputFile: archive, Force: true, NoRestart: true}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got, _ := os.ReadFile(cfg); string(got) != "clusters: restored\n" {
		t.Fatalf("config.yaml = %q", got)
	}
	if _, err := os.Stat(filepath.Join(install, "nested", "state.json")); err != nil {
		t.Fatalf("new file not restored: %v", err)
	}
	ents, _ := os.ReadDir(install)
	for _, e := range ents {
		if strings.Contains(e.Name(), ".ncc-restore-stage-") || strings.Contains(e.Name(), updateAsideMarker) {
			t.Fatalf("restore left debris behind: %s", e.Name())
		}
	}
}
