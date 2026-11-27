package winacl

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// These tests need an elevated token (and one needs SYSTEM). On a developer's
// machine they skip with a message. CI sets NCC_TEST_EXPECT_ELEVATED=1 and
// NCC_TEST_EXPECT_SYSTEM=1 for the runs where the token is guaranteed, and
// then a missing token is a failure, not a skip: a green run must mean the
// elevated code path really ran.

func expectEnv(name string) bool { return os.Getenv(name) == "1" }

func requireElevated(t *testing.T) identity {
	t.Helper()
	id, err := currentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if !id.elevated {
		if expectEnv("NCC_TEST_EXPECT_ELEVATED") || expectEnv("NCC_TEST_EXPECT_SYSTEM") {
			t.Fatal("NCC_TEST_EXPECT_ELEVATED is set but this process is not elevated")
		}
		t.Skip("needs an elevated process (set NCC_TEST_EXPECT_ELEVATED=1 in CI to make this a failure)")
	}
	return id
}

func requireSystem(t *testing.T) identity {
	t.Helper()
	id, err := currentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if id.user != sidSystem {
		if expectEnv("NCC_TEST_EXPECT_SYSTEM") {
			t.Fatalf("NCC_TEST_EXPECT_SYSTEM is set but this process runs as %s", id.user)
		}
		t.Skip("needs to run as SYSTEM (set NCC_TEST_EXPECT_SYSTEM=1 in CI to make this a failure)")
	}
	return id
}

func enablePrivilege(t *testing.T, name string) {
	t.Helper()
	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, windows.StringToUTF16Ptr(name), &luid); err != nil {
		t.Fatal(err)
	}
	tp := windows.Tokenprivileges{PrivilegeCount: 1}
	tp.Privileges[0] = windows.LUIDAndAttributes{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}
	if err := windows.AdjustTokenPrivileges(windows.GetCurrentProcessToken(), false, &tp, 0, nil, nil); err != nil {
		t.Fatalf("enable %s: %v", name, err)
	}
	if windows.GetLastError() == windows.ERROR_NOT_ALL_ASSIGNED {
		t.Fatalf("token does not hold %s", name)
	}
}

// setOwner gives path to sid, which an elevated token may do with SeRestore.
func setOwner(t *testing.T, path, sid string) {
	t.Helper()
	enablePrivilege(t, "SeRestorePrivilege")
	enablePrivilege(t, "SeTakeOwnershipPrivilege")
	s, err := windows.StringToSid(sid)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION, s, nil, nil, nil); err != nil {
		t.Fatalf("set owner of %s: %v", path, err)
	}
	if got := read(t, path).owner; got != sid {
		t.Fatalf("owner of %s is %s, want %s", path, got, sid)
	}
}

func mustSID(t *testing.T, s string) *windows.SID {
	t.Helper()
	sid, err := windows.StringToSid(s)
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

// The runner or session is the identity the rest of the suite assumes.
func TestTokenMatchesExpectation(t *testing.T) {
	id, err := currentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("user=%s elevated=%v", id.user, id.elevated)
	if !expectEnv("NCC_TEST_EXPECT_ELEVATED") && !expectEnv("NCC_TEST_EXPECT_SYSTEM") {
		t.Skip("no expectation set")
	}
	if expectEnv("NCC_TEST_EXPECT_ELEVATED") {
		requireElevated(t)
		if member, err := windows.GetCurrentProcessToken().IsMember(mustSID(t, sidAdministrators)); err != nil || !member {
			t.Fatalf("not an enabled member of Administrators (member=%v err=%v)", member, err)
		}
	}
	if expectEnv("NCC_TEST_EXPECT_SYSTEM") {
		requireSystem(t)
	}
}

// A file another account created first must not stay theirs: the owner can
// rewrite the DACL. Only an elevated process can take it over.
func TestElevatedRestrictTakesOwnership(t *testing.T) {
	id := requireElevated(t)
	dir := broadDir(t)
	f := write(t, filepath.Join(dir, "secret.json"))
	setOwner(t, f, localService)

	changed, err := Restrict(f, Private)
	if err != nil || !changed {
		t.Fatalf("Restrict: changed=%v err=%v", changed, err)
	}
	assertPrivate(t, f)
	if got := read(t, f).owner; got != id.owner.String() {
		t.Fatalf("owner is %s, want %s", got, id.owner.String())
	}
}

// Children of a folder whose ACL needed work are taken over even when their
// own ACL is already closed.
func TestElevatedRestrictTreeTakesOwnershipOfChildren(t *testing.T) {
	id := requireElevated(t)
	dir := broadDir(t)
	child := write(t, filepath.Join(dir, "sub", "keys.json"))
	// Closed already: only the DACL is clean, the owner is someone else's.
	setSDDL(t, child, "D:P(A;;FA;;;SY)(A;;FA;;;BA)")
	setOwner(t, child, localService)

	changed, errs := RestrictTree(dir, Private)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if len(changed) == 0 {
		t.Fatal("nothing changed")
	}
	assertPrivate(t, dir)
	if got := read(t, child).owner; got != id.owner.String() {
		t.Fatalf("child owner is %s, want %s", got, id.owner.String())
	}
}

// An owner we trust (Administrators, SYSTEM, ourselves) is left alone.
func TestElevatedRestrictKeepsTrustedOwner(t *testing.T) {
	requireElevated(t)
	dir := broadDir(t)
	f := write(t, filepath.Join(dir, "a.json"))
	setOwner(t, f, sidAdministrators)
	if _, err := Restrict(f, Private); err != nil {
		t.Fatal(err)
	}
	if got := read(t, f).owner; got != sidAdministrators {
		t.Fatalf("owner changed to %s", got)
	}
}

// As SYSTEM the account running the process is SYSTEM itself, so the required
// set must not grow a duplicate entry, and a second pass changes nothing.
func TestSystemRestrictLeavesSystemAndAdmins(t *testing.T) {
	requireSystem(t)
	dir := broadDir(t)
	f := write(t, filepath.Join(dir, "token"))
	if changed, err := Restrict(f, Private); err != nil || !changed {
		t.Fatalf("Restrict: changed=%v err=%v", changed, err)
	}
	assertPrivate(t, f)
	for _, a := range read(t, f).aces {
		if a.sid != sidSystem && a.sid != sidAdministrators && a.sid != localService {
			t.Errorf("unexpected entry for %s", a.sid)
		}
	}
	if changed, err := Restrict(f, Private); err != nil || changed {
		t.Fatalf("second pass changed=%v err=%v", changed, err)
	}
}
