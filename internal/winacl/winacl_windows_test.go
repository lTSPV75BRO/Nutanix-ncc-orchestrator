package winacl

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// LOCAL SERVICE stands for a service account an administrator added.
const (
	localService = "S-1-5-19"
	users        = "S-1-5-32-545"
	everyone     = "S-1-1-0"
	authUsers    = "S-1-5-11"
)

// setSDDL puts a DACL on path. The current user always gets full control so
// the test can clean up.
func setSDDL(t *testing.T, path, dacl string) {
	t.Helper()
	sd, err := windows.SecurityDescriptorFromString(dacl + "(A;OICI;FA;;;" + me(t) + ")")
	if err != nil {
		t.Fatal(err)
	}
	acl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	info := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.UNPROTECTED_DACL_SECURITY_INFORMATION)
	if strings.HasPrefix(dacl, "D:P") {
		info = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, info, nil, nil, acl, nil); err != nil {
		t.Fatal(err)
	}
}

func me(t *testing.T) string {
	t.Helper()
	id, err := currentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return id.user
}

func read(t *testing.T, path string) security {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, infoFlags)
	if err != nil {
		t.Fatal(err)
	}
	s, err := parseSD(sd)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// mask is the access a SID is allowed on path itself.
func mask(t *testing.T, path, sid string) uint32 {
	t.Helper()
	var m uint32
	for _, a := range read(t, path).aces {
		if a.allow() && a.sid == sid && a.flags&windows.INHERIT_ONLY_ACE == 0 {
			m |= a.mask
		}
	}
	return m
}

func assertPrivate(t *testing.T, path string) {
	t.Helper()
	s := read(t, path)
	if broad := s.excess(Private); len(broad) > 0 {
		t.Fatalf("%s: still open to %v", path, broad)
	}
	for _, a := range s.aces {
		if creatorSID(a.sid) {
			t.Fatalf("%s: CREATOR OWNER entry left", path)
		}
	}
	for _, sid := range []string{sidSystem, sidAdministrators, me(t)} {
		if mask(t, path, sid) != fileAllAccess {
			t.Fatalf("%s: %s has %#x, want full control", path, sid, mask(t, path, sid))
		}
	}
}

// broadDir is a folder open the way C:\ProgramData subfolders are, plus one
// deliberately added service account.
func broadDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	setSDDL(t, dir, "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICIIO;GA;;;CO)(A;OICI;0x1200a9;;;BU)(A;CI;0x116;;;BU)(A;OICI;0x1301bf;;;AU)(A;OICI;FA;;;WD)(A;OICI;FR;;;"+localService+")")
	return dir
}

func write(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRestrictFile(t *testing.T) {
	path := write(t, filepath.Join(broadDir(t), "token"))
	if broad, err := Check(path, Private); err != nil || !slices.Contains(broad, "Users") {
		t.Fatalf("the file should start broad: %v %v", broad, err)
	}
	if changed, err := Restrict(path, Private); err != nil || !changed {
		t.Fatalf("Restrict = %v, %v", changed, err)
	}
	assertPrivate(t, path)
	s := read(t, path)
	if !s.protected {
		t.Fatal("the DACL should not inherit any more")
	}
	if mask(t, path, localService) == 0 {
		t.Fatal("the service account lost its access")
	}
	if s.owner == "" {
		t.Fatal("no owner")
	}
	if changed, err := Restrict(path, Private); err != nil || changed {
		t.Fatalf("second Restrict = %v, %v; want no change", changed, err)
	}
	if _, err := os.ReadFile(path); err != nil {
		t.Fatal(err)
	}
}

func TestRestrictKeepsExplicitGrants(t *testing.T) {
	path := write(t, filepath.Join(broadDir(t), "users"))
	const custom = "S-1-5-21-1111-2222-3333-1234"
	setSDDL(t, path, "D:(D;;FW;;;"+localService+")(A;;FR;;;"+custom+")(A;;FR;;;S-1-5-21-1111-2222-3333-513)(A;;FR;;;IU)(A;;FR;;;NU)(A;;FR;;;AN)(A;;FR;;;BG)")
	if _, err := Restrict(path, Private); err != nil {
		t.Fatal(err)
	}
	assertPrivate(t, path)
	if mask(t, path, custom) != windows.FILE_GENERIC_READ {
		t.Fatalf("custom account has %#x", mask(t, path, custom))
	}
	aces := read(t, path).aces
	if !aces[0].deny() || aces[0].sid != localService {
		t.Fatal("the deny entry should be kept and come first")
	}
}

func TestRestrictOpenDACL(t *testing.T) {
	path := write(t, filepath.Join(t.TempDir(), "open"))
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !read(t, path).open {
		t.Skip("could not set a null DACL here")
	}
	if changed, err := Restrict(path, Private); err != nil || !changed {
		t.Fatalf("Restrict = %v, %v", changed, err)
	}
	assertPrivate(t, path)
}

// Nothing is rewritten when the folder is already closed to broad groups, so
// an install in a user profile keeps inheriting.
func TestRestrictLeavesClosedPathsAlone(t *testing.T) {
	dir := t.TempDir()
	setSDDL(t, dir, "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;"+localService+")")
	path := write(t, filepath.Join(dir, "f"))
	if changed, err := Restrict(path, Private); err != nil || changed {
		t.Fatalf("Restrict = %v, %v", changed, err)
	}
	if read(t, path).protected {
		t.Fatal("inheritance was turned off for nothing")
	}
}

func TestRestrictTreePrivate(t *testing.T) {
	dir := broadDir(t)
	root := filepath.Join(dir, "backups")
	inner := write(t, filepath.Join(root, "sub", "a.tar.gz"))
	explicit := write(t, filepath.Join(root, "sub", "b.tar.gz"))
	setSDDL(t, explicit, "D:P(A;;FR;;;BU)")
	outside := write(t, filepath.Join(dir, "other"))

	// A junction to a folder outside must neither be followed nor changed.
	target := filepath.Join(dir, "target")
	targetFile := write(t, filepath.Join(target, "t"))
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", filepath.Join(root, "j"), target).CombinedOutput(); err != nil {
		t.Logf("no junction: %v %s", err, out)
	}

	changed, errs := RestrictTree(root, Private)
	if len(errs) > 0 || len(changed) == 0 {
		t.Fatalf("RestrictTree = %v, %v", changed, errs)
	}
	for _, p := range []string{root, filepath.Join(root, "sub"), inner, explicit} {
		assertPrivate(t, p)
	}
	assertPrivate(t, write(t, filepath.Join(root, "new", "c.tar.gz")))
	if mask(t, inner, localService) == 0 {
		t.Fatal("the service account lost its access")
	}
	for _, p := range []string{outside, target, targetFile} {
		if len(read(t, p).excess(Private)) == 0 {
			t.Fatalf("%s is outside the tree and must not change", p)
		}
	}
	if broad, errs := CheckTree(root, Private); len(broad) > 0 || len(errs) > 0 {
		t.Fatalf("CheckTree = %v, %v", broad, errs)
	}
	if changed, errs := RestrictTree(root, Private); len(errs) > 0 || len(changed) > 0 {
		t.Fatalf("second RestrictTree = %v, %v", changed, errs)
	}
}

func TestRestrictTreeNoBroadWrite(t *testing.T) {
	bin := filepath.Join(broadDir(t), "bin")
	exe := write(t, filepath.Join(bin, "ncc.exe"))
	if broad, _ := Check(bin, NoBroadWrite); len(broad) == 0 {
		t.Fatal("bin should start writable")
	}
	if _, errs := RestrictTree(bin, NoBroadWrite); len(errs) > 0 {
		t.Fatal(errs)
	}
	for _, p := range []string{bin, exe, write(t, filepath.Join(bin, "new.exe"))} {
		for _, sid := range []string{users, everyone, authUsers} {
			m := mask(t, p, sid)
			if m&writeRights != 0 {
				t.Fatalf("%s: %s can still write (%#x)", p, sid, m)
			}
			if m&windows.FILE_READ_DATA == 0 || m&windows.FILE_EXECUTE == 0 {
				t.Fatalf("%s: %s should still read and execute (%#x)", p, sid, m)
			}
		}
		for _, sid := range []string{sidSystem, sidAdministrators, me(t)} {
			if mask(t, p, sid) != fileAllAccess {
				t.Fatalf("%s: %s lost access", p, sid)
			}
		}
		for _, a := range read(t, p).aces {
			if creatorSID(a.sid) {
				t.Fatalf("%s: CREATOR OWNER entry left", p)
			}
		}
	}
	if changed, errs := RestrictTree(bin, NoBroadWrite); len(changed) > 0 || len(errs) > 0 {
		t.Fatalf("second run = %v, %v", changed, errs)
	}
}

// An inherited entry that CREATOR OWNER produced is dropped; one the parent
// grants by name is kept.
func TestCreatorOwnerEntryIsNotKept(t *testing.T) {
	inherited := func(sid string) ace {
		raw, err := rawAllow(sid, windows.INHERITED_ACE, fileAllAccess)
		if err != nil {
			t.Fatal(err)
		}
		return ace{typ: accessAllowed, flags: windows.INHERITED_ACE, mask: fileAllAccess, sid: sid, raw: raw}
	}
	const creator, granted = "S-1-5-21-1-2-3-1001", "S-1-5-21-1-2-3-1002"
	parent := func() ([]ace, bool) {
		return []ace{
			{typ: accessAllowed, sid: "S-1-3-0", flags: aceInheritFlags, mask: fileAllAccess},
			{typ: accessAllowed, sid: granted, flags: aceInheritFlags, mask: fileAllAccess},
		}, true
	}
	acl, err := cleanACL(security{aces: []ace{inherited(creator), inherited(granted)}}, Private, false, identity{user: sidSystem}, parent)
	if err != nil {
		t.Fatal(err)
	}
	_, aces := parseACL(acl)
	var got []string
	for _, a := range aces {
		if a.inherited() {
			t.Fatal("entries must become explicit")
		}
		got = append(got, a.sid)
	}
	if want := []string{sidSystem, sidAdministrators, granted}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReplaceOwner(t *testing.T) {
	sid, _ := windows.StringToSid(sidAdministrators)
	id := identity{user: "S-1-5-21-1-2-3-1001", elevated: true, owner: sid}
	for owner, want := range map[string]bool{
		sidSystem: false, sidAdministrators: false, sidTrustedInstaller: false,
		id.user: false, "": false, "S-1-5-21-1-2-3-1002": true,
	} {
		if got := id.replaceOwner(owner); got != want {
			t.Errorf("%q: %v", owner, got)
		}
	}
	id.elevated = false
	if id.replaceOwner("S-1-5-21-1-2-3-1002") {
		t.Error("only an elevated process can take ownership")
	}
}

// The token is written in place, the users database with temp + rename.
func TestRewrittenSecretsStayRestricted(t *testing.T) {
	dir := broadDir(t)
	token := filepath.Join(dir, ".ncc-api-token")
	users := filepath.Join(dir, ".ncc-api-users.json")
	for i := range 2 {
		if err := WriteFile(token, []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		assertPrivate(t, token)
		tmp := write(t, users+".tmp")
		if err := Rename(tmp, users); err != nil {
			t.Fatal(err)
		}
		assertPrivate(t, users)
		if i == 0 { // an administrator lets one more account read it
			if out, err := exec.Command("icacls", users, "/grant", "*S-1-5-20:R").CombinedOutput(); err != nil {
				t.Fatalf("icacls: %v %s", err, out)
			}
			continue
		}
		if mask(t, users, "S-1-5-20") == 0 {
			t.Fatal("the rewritten file lost the access an administrator added")
		}
	}
	if b, err := os.ReadFile(token); err != nil || string(b) != "secret" {
		t.Fatal(string(b), err)
	}
	open := filepath.Join(dir, "report.html")
	if err := WriteFile(open, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if len(read(t, open).excess(Private)) == 0 {
		t.Fatal("only 0600 files are restricted")
	}
}

func TestMissingAndLinkedPaths(t *testing.T) {
	dir := broadDir(t)
	if changed, err := Restrict(filepath.Join(dir, "no", "such"), Private); changed || err != nil {
		t.Fatalf("missing: %v %v", changed, err)
	}
	if changed, errs := RestrictTree(filepath.Join(dir, "none"), Private); len(changed) > 0 || len(errs) > 0 {
		t.Fatalf("missing tree: %v %v", changed, errs)
	}
	path := write(t, filepath.Join(dir, "f"))
	if err := os.Link(path, path+"2"); err != nil {
		t.Skip(err)
	}
	if _, err := Restrict(path, Private); !errors.Is(err, ErrLink) {
		t.Fatalf("hard link: %v", err)
	}
}
