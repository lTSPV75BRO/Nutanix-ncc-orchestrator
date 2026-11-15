package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeInstallFile(t *testing.T, install, rel, body string) {
	t.Helper()
	p := filepath.Join(install, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func readInstallFile(t *testing.T, install, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(install, filepath.FromSlash(rel)))
	if err != nil {
		return "<missing>"
	}
	return string(b)
}

func restoreForTest(t *testing.T, install, archive string, trust bool) error {
	t.Helper()
	return runV2Restore(v2RestoreOptions{InstallDir: install, InputFile: archive, Force: true, NoRestart: true, TrustStack: trust})
}

// SEC-07: a backup's program files are not installed unless this host has
// installed them before; the data still restores.
func TestRestoreSkipsUntrustedStack(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "clusters: old\n")
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "clusters: restored\n"},
		{"bin/ncc-api-server", "crafted binary"},
		{"frontend-dist/index.html", "<script>crafted</script>"},
	})
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	if got := readInstallFile(t, install, "config.yaml"); got != "clusters: restored\n" {
		t.Fatalf("data should restore: %q", got)
	}
	for _, rel := range []string{"bin/ncc-api-server", "frontend-dist/index.html"} {
		if got := readInstallFile(t, install, rel); got != "<missing>" {
			t.Fatalf("%s must not be restored from an untrusted backup, got %q", rel, got)
		}
	}
}

func TestRestorePutsBackRecordedStack(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "clusters: a\n")
	writeInstallFile(t, install, "bin/ncc-api-server", "api v1")
	writeInstallFile(t, install, "frontend-dist/index.html", "ui v1")
	if err := recordTrustedStack(install, "test"); err != nil {
		t.Fatal(err)
	}
	// The host moves on to v2; a rollback point holds v1.
	writeInstallFile(t, install, "bin/ncc-api-server", "api v2")
	writeInstallFile(t, install, "frontend-dist/index.html", "ui v2")
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "clusters: b\n"},
		{"bin/ncc-api-server", "api v1"},
		{"frontend-dist/index.html", "ui v1"},
	})
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	if readInstallFile(t, install, "bin/ncc-api-server") != "api v1" || readInstallFile(t, install, "frontend-dist/index.html") != "ui v1" {
		t.Fatal("a stack this host recorded should be restored")
	}
}

// One untrusted file keeps the whole stack out, so versions never mix.
func TestRestoreStackIsAllOrNothing(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "x: 1\n")
	writeInstallFile(t, install, "bin/ncc-api-server", "api v1")
	writeInstallFile(t, install, "bin/ncc-ui-server", "ui-server v1")
	if err := recordTrustedStack(install, "test"); err != nil {
		t.Fatal(err)
	}
	writeInstallFile(t, install, "bin/ncc-api-server", "api v2")
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "x: 2\n"},
		{"bin/ncc-api-server", "api v1"},          // trusted
		{"bin/ncc-ui-server", "crafted ui-server"}, // not
	})
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	if got := readInstallFile(t, install, "bin/ncc-api-server"); got != "api v2" {
		t.Fatalf("no stack file may be restored when one is untrusted, api server is %q", got)
	}
	if got := readInstallFile(t, install, "bin/ncc-ui-server"); got != "ui-server v1" {
		t.Fatalf("ui server changed: %q", got)
	}
	if got := readInstallFile(t, install, "config.yaml"); got != "x: 2\n" {
		t.Fatalf("data should still restore: %q", got)
	}
}

// The ledger is this host's record: an archive can't replace it to vouch for
// its own binaries.
func TestRestoreIgnoresLedgerInArchive(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "x: 1\n")
	sum := "7b6f4f3e9b3c0a0e1a1c1d1e1f202122232425262728292a2b2c2d2e2f303132"
	forged := fmt.Sprintf(`{"sets":[{"recorded_at":"2026-01-01T00:00:00Z","files":{"bin/ncc-api-server":%q}}]}`, sum)
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "x: 2\n"},
		{trustedStackFile, forged},
		{"bin/ncc-api-server", "crafted"},
	})
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	if got := readInstallFile(t, install, trustedStackFile); got != "<missing>" {
		t.Fatalf("ledger must not come from the archive: %q", got)
	}
	if got := readInstallFile(t, install, "bin/ncc-api-server"); got != "<missing>" {
		t.Fatalf("binary restored: %q", got)
	}
}

func TestRestoreTrustStackFlag(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "x: 1\n")
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "x: 2\n"},
		{"bin/ncc-api-server", "operator-approved build"},
	})
	if err := restoreForTest(t, install, archive, true); err != nil {
		t.Fatal(err)
	}
	if got := readInstallFile(t, install, "bin/ncc-api-server"); got != "operator-approved build" {
		t.Fatalf("--trust-stack should restore the program files: %q", got)
	}
}

func TestRestoreOnlyUntrustedStackFails(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "bin/ncc-api-server", "installed")
	archive := writeRestoreTestArchive(t, [][2]string{{"bin/ncc-api-server", "crafted"}})
	if err := restoreForTest(t, install, archive, false); err == nil {
		t.Fatal("an archive with nothing trusted to restore should fail")
	}
	if got := readInstallFile(t, install, "bin/ncc-api-server"); got != "installed" {
		t.Fatalf("install changed: %q", got)
	}
}

// The file installed now is always trusted, even with no ledger.
func TestRestoreTrustsInstalledFile(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "x: 1\n")
	writeInstallFile(t, install, "bin/ncc-api-server", "same build")
	archive := writeRestoreTestArchive(t, [][2]string{
		{"config.yaml", "x: 2\n"},
		{"bin/ncc-api-server", "same build"},
	})
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	if got := readInstallFile(t, install, "config.yaml"); got != "x: 2\n" {
		t.Fatalf("config: %q", got)
	}
}

func TestRecordTrustedStackDedupesAndCaps(t *testing.T) {
	install := t.TempDir()
	if err := recordTrustedStack(install, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(install, trustedStackFile)); !os.IsNotExist(err) {
		t.Fatal("no stack installed: nothing should be written")
	}
	writeInstallFile(t, install, "bin/ncc-orchestrator", "build 0")
	for i := 0; i < 2; i++ {
		if err := recordTrustedStack(install, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(loadTrustedStack(install).Sets); n != 1 {
		t.Fatalf("recording the same files twice should keep one set, got %d", n)
	}
	for i := 1; i <= trustedStackMaxSets+5; i++ {
		writeInstallFile(t, install, "bin/ncc-orchestrator", fmt.Sprintf("build %d", i))
		if err := recordTrustedStack(install, "test"); err != nil {
			t.Fatal(err)
		}
	}
	l := loadTrustedStack(install)
	if len(l.Sets) != trustedStackMaxSets {
		t.Fatalf("ledger should keep the newest %d sets, has %d", trustedStackMaxSets, len(l.Sets))
	}
	latest, _ := sha256File(filepath.Join(install, "bin", "ncc-orchestrator"))
	if l.Sets[len(l.Sets)-1].Files["bin/ncc-orchestrator"] != latest {
		t.Fatal("newest set should be last")
	}
	if info, err := os.Stat(filepath.Join(install, trustedStackFile)); err == nil && info.Mode().Perm()&0o077 != 0 && os.PathSeparator == '/' {
		t.Fatalf("ledger should be 0600, is %v", info.Mode().Perm())
	}
}
