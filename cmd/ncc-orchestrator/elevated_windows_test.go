package main

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"goncc/internal/winacl"

	"golang.org/x/sys/windows"
)

// CI sets NCC_TEST_EXPECT_ELEVATED=1 / NCC_TEST_EXPECT_SYSTEM=1 where the token
// is guaranteed; then a missing token fails the test instead of skipping it.

func requireToken(t *testing.T, system bool) {
	t.Helper()
	tok := windows.GetCurrentProcessToken()
	tu, err := tok.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	user := tu.User.Sid.String()
	switch {
	case system && user != sidSystemS:
		if os.Getenv("NCC_TEST_EXPECT_SYSTEM") == "1" {
			t.Fatalf("NCC_TEST_EXPECT_SYSTEM is set but this process runs as %s", user)
		}
		t.Skip("needs to run as SYSTEM (CI sets NCC_TEST_EXPECT_SYSTEM=1)")
	case !system && !tok.IsElevated():
		if os.Getenv("NCC_TEST_EXPECT_ELEVATED") == "1" || os.Getenv("NCC_TEST_EXPECT_SYSTEM") == "1" {
			t.Fatal("NCC_TEST_EXPECT_ELEVATED is set but this process is not elevated")
		}
		t.Skip("needs an elevated process (CI sets NCC_TEST_EXPECT_ELEVATED=1)")
	}
}

// aceMasks sums the allow entries that apply to path itself, by SID.
func aceMasks(t *testing.T, path string) map[string]uint32 {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	acl, _, err := sd.DACL()
	if err != nil || acl == nil {
		t.Fatalf("%s: no DACL (%v)", path, err)
	}
	out := map[string]uint32{}
	for i := uint32(0); i < uint32(acl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, i, &ace); err != nil {
			t.Fatal(err)
		}
		if ace.Header.AceType != 0 || ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 {
			continue
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		out[sid.String()] |= uint32(ace.Mask)
	}
	return out
}

const (
	sidSystemS  = "S-1-5-18"
	sidAdminsS  = "S-1-5-32-544"
	sidUsersS   = "S-1-5-32-545"
	fullControl = 0x1F01FF
	// Rights that change, create, delete or take over.
	writeBits = windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA | windows.FILE_WRITE_EA |
		windows.FILE_WRITE_ATTRIBUTES | 0x40 | windows.DELETE | windows.WRITE_DAC | windows.WRITE_OWNER |
		windows.GENERIC_WRITE | windows.GENERIC_ALL
)

func assertFullControl(t *testing.T, rel string, m map[string]uint32) {
	t.Helper()
	for _, sid := range []string{sidSystemS, sidAdminsS} {
		if m[sid]&fullControl != fullControl {
			t.Errorf("%s: %s has %#x, want full control", rel, sid, m[sid])
		}
	}
}

// doctor --fix on a stack folder in the layout the installer unpacks must leave
// SYSTEM and Administrators with full control, Users with read and run only on
// the folder and program files, and nothing at all on secrets. Run as SYSTEM in
// CI, which is the account the Task Scheduler service runs the stack as.
func TestDoctorFixInstallerLayoutAsSystem(t *testing.T) {
	requireToken(t, true)
	dir := broadInstall(t)
	hc := &healContext{InstallDir: dir, ConfigPath: filepath.Join(dir, "config.yaml"), Fix: true}
	if res := checkSecretsPerms(hc); !res.Fixed {
		t.Fatalf("doctor --fix changed nothing: %+v", res)
	}
	hc.Fix = false
	if res := checkSecretsPerms(hc); res.Status != healOK {
		t.Fatalf("doctor after fix: %+v", res)
	}

	for _, rel := range []string{".", "bin", `bin\ncc-orchestrator.exe`, `frontend-dist\assets\app.js`, "run"} {
		m := aceMasks(t, filepath.Join(dir, rel))
		assertFullControl(t, rel, m)
		if m[sidUsersS] == 0 {
			t.Errorf("%s: Users lost read access", rel)
		}
		if m[sidUsersS]&writeBits != 0 {
			t.Errorf("%s: Users can write (%#x)", rel, m[sidUsersS])
		}
	}
	for _, rel := range []string{"config.yaml", "secrets.json", ".ncc-api-token", ".ncc-api-users.json", "backups", `tls\ui.key`, `logs\ncc-audit.log`} {
		m := aceMasks(t, filepath.Join(dir, rel))
		assertFullControl(t, rel, m)
		for _, sid := range []string{sidUsersS, "S-1-1-0", "S-1-5-11"} {
			if m[sid] != 0 {
				t.Errorf("%s: broad group %s has %#x", rel, sid, m[sid])
			}
		}
	}
	if isBroad(t, dir, winacl.NoBroadWrite) {
		t.Error("install folder still writable for broad groups")
	}
	if !isBroad(t, dir, winacl.Private) {
		t.Error("install folder should stay readable")
	}
}

// Elevated (not necessarily SYSTEM): doctor --fix runs the code path that also
// takes ownership of files.
func TestDoctorFixElevated(t *testing.T) {
	requireToken(t, false)
	dir := broadInstall(t)
	hc := &healContext{InstallDir: dir, ConfigPath: filepath.Join(dir, "config.yaml"), Fix: true}
	if res := checkSecretsPerms(hc); !res.Fixed {
		t.Fatalf("doctor --fix changed nothing: %+v", res)
	}
	hc.Fix = false
	if res := checkSecretsPerms(hc); res.Status != healOK {
		t.Fatalf("after fix: %+v", res)
	}
	m := aceMasks(t, filepath.Join(dir, "secrets.json"))
	assertFullControl(t, "secrets.json", m)
	if m[sidUsersS] != 0 {
		t.Fatalf("secrets.json masks: %v", m)
	}
}
