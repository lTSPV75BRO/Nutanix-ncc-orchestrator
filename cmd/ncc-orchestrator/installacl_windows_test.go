package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"goncc/internal/winacl"
)

// broadInstall is an install dir with the permissions a folder under
// C:\ProgramData inherits, and then some.
func broadInstall(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ncc")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("icacls", dir, "/grant", "*S-1-5-32-545:(OI)(CI)M", "*S-1-5-11:(OI)(CI)M").CombinedOutput(); err != nil {
		t.Fatalf("icacls: %v %s", err, out)
	}
	for _, rel := range []string{
		".ncc-api-token", ".ncc-api-users.json", ".ncc-initial-admin-password", ".ncc-v2-start.json",
		"config.yaml", "config.yaml.pre-migrate-1.bak", "secrets.json", `backups\a.tar.gz`, `tls\ui.key`,
		`configs\environments\prod.yaml`, `logs\ncc-audit.log`, `bin\ncc-orchestrator.exe`,
		`frontend-dist\assets\app.js`, `run\v2-api.pid`, `outputfiles\report.html`, "README.md",
	} {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func isBroad(t *testing.T, path string, mode winacl.Mode) bool {
	t.Helper()
	broad, err := winacl.Check(path, mode)
	if err != nil {
		t.Fatal(err)
	}
	return len(broad) > 0
}

func TestSecureInstallACLs(t *testing.T) {
	dir := broadInstall(t)
	outside := filepath.Join(filepath.Dir(dir), "shared")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("icacls", outside, "/grant", "*S-1-5-32-545:(OI)(CI)M").CombinedOutput(); err != nil {
		t.Fatalf("icacls: %v %s", err, out)
	}
	for _, name := range []string{"site.key", "users.json", "report.html"} {
		if err := os.WriteFile(filepath.Join(outside, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	opts := v2StartOptions{
		ConfigPath:   filepath.Join(dir, "config.yaml"),
		TokenFile:    filepath.Join(dir, ".ncc-api-token"),
		UsersDB:      filepath.Join(outside, "users.json"),
		UITLSKeyFile: filepath.Join(outside, "site.key"),
		OutputDir:    outside,
	}
	secureInstallACLs(dir, opts)

	for _, rel := range []string{
		".ncc-api-token", ".ncc-api-users.json", ".ncc-initial-admin-password", ".ncc-v2-start.json",
		"config.yaml", "config.yaml.pre-migrate-1.bak", "secrets.json", "backups", `backups\a.tar.gz`,
		"tls", `tls\ui.key`, "configs", `configs\environments\prod.yaml`, `logs\ncc-audit.log`,
	} {
		if isBroad(t, filepath.Join(dir, rel), winacl.Private) {
			t.Errorf("%s is still open", rel)
		}
	}
	for _, rel := range []string{"bin", `bin\ncc-orchestrator.exe`, `frontend-dist\assets\app.js`, "run", `run\v2-api.pid`} {
		p := filepath.Join(dir, rel)
		if isBroad(t, p, winacl.NoBroadWrite) {
			t.Errorf("%s is still writable", rel)
		}
		if !isBroad(t, p, winacl.Private) {
			t.Errorf("%s should stay readable", rel)
		}
	}
	// The install dir, its output and files that are not ours stay as they were.
	for _, p := range []string{
		dir, filepath.Join(dir, "outputfiles", "report.html"), filepath.Join(dir, "README.md"),
		outside, filepath.Join(outside, "report.html"), filepath.Join(outside, "site.key"),
	} {
		if !isBroad(t, p, winacl.NoBroadWrite) {
			t.Errorf("%s was changed", p)
		}
	}
	if isBroad(t, filepath.Join(outside, "users.json"), winacl.Private) {
		t.Error("the users database is ours wherever it is")
	}

	in := aclInstall{dir: dir, configPath: opts.ConfigPath, secrets: []string{opts.TokenFile, opts.UsersDB}}
	if o := in.apply(true); len(o.changed)+len(o.failed)+len(o.broad) != 0 {
		t.Errorf("second pass: %+v", o)
	}
	in.keys = []string{opts.UITLSKeyFile}
	msgs := strings.Join(in.apply(true).startMessages(dir), "\n")
	for _, want := range []string{"site.key", "install folder " + dir} {
		if !strings.Contains(msgs, want) {
			t.Errorf("no warning about %q in %q", want, msgs)
		}
	}
	// Files rewritten later keep their restricted permissions.
	if err := recordTrustedStack(dir, "test"); err != nil {
		t.Fatal(err)
	}
	if err := writeV2StartState(dir, v2StartOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{trustedStackFile, v2StartStateFile} {
		if isBroad(t, filepath.Join(dir, name), winacl.Private) {
			t.Errorf("%s went back to broad access", name)
		}
	}
}

func TestSecretsPermsCheckOnWindows(t *testing.T) {
	dir := broadInstall(t)
	hc := &healContext{InstallDir: dir, ConfigPath: filepath.Join(dir, "config.yaml")}
	res := checkSecretsPerms(hc)
	if res.Status != healWarn || res.Fixed || !strings.Contains(res.Hint, "--fix") {
		t.Fatalf("broad install: %+v", res)
	}
	hc.Fix = true
	if res = checkSecretsPerms(hc); !res.Fixed || res.FixMsg == "" {
		t.Fatalf("fix: %+v", res)
	}
	// Only the install folder itself is left, which the product never changes.
	hc.Fix = false
	if res = checkSecretsPerms(hc); res.Status != healWarn || !strings.Contains(res.Hint, "icacls") || strings.Contains(res.Hint, "--fix") {
		t.Fatalf("after fix: %+v", res)
	}
	if out, err := exec.Command("icacls", dir, "/remove:g", "*S-1-5-32-545", "*S-1-5-11").CombinedOutput(); err != nil {
		t.Fatalf("icacls: %v %s", err, out)
	}
	if res = checkSecretsPerms(hc); res.Status != healOK {
		t.Fatalf("closed install: %+v", res)
	}
}

func TestACLOutcomeMessages(t *testing.T) {
	var o aclOutcome
	o.fail(`E:\ncc\a`, winacl.ErrUnsupported)
	o.fail(`E:\ncc\b`, winacl.ErrUnsupported)
	o.fail(`C:\ncc\c`, errors.New("Access is denied."))
	msgs := o.startMessages(t.TempDir())
	if len(msgs) != 2 || !strings.Contains(msgs[0], `C:\ncc\c: Access is denied.`) || !strings.Contains(msgs[1], `E:\ncc\a`) {
		t.Fatalf("%q", msgs)
	}
}

func TestRefreshInstallACLsAfterReplace(t *testing.T) {
	dir := broadInstall(t)
	refreshInstallACLs(dir)
	staged := filepath.Join(dir, ".ncc-stage-frontend-1")
	if err := os.MkdirAll(staged, 0o755); err != nil {
		t.Fatal(err)
	}
	txn := newInstallTxn()
	front := filepath.Join(dir, "frontend-dist")
	if err := txn.replace(staged, front); err != nil {
		t.Fatal(err)
	}
	txn.commit()
	if !isBroad(t, front, winacl.NoBroadWrite) {
		t.Log("the staged folder was already closed")
	}
	refreshInstallACLs(dir)
	if isBroad(t, front, winacl.NoBroadWrite) {
		t.Fatal("frontend-dist is writable after an update")
	}
}

func TestVolumeRootInstallIsLeftAlone(t *testing.T) {
	for dir, want := range map[string]bool{`C:\`: true, `\\srv\share\`: true, `C:\ncc`: false, t.TempDir(): false} {
		if got := isVolumeRoot(dir); got != want {
			t.Errorf("isVolumeRoot(%s) = %v", dir, got)
		}
	}
}
