//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// LIN-02 (U5): the install is laid out for the service user, and a rollback
// gives it back to root exactly as it was. Needs root (chown).
func TestServiceOwnershipApplyAndRestore(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root")
	}
	const uid, gid = 4242, 4243
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]os.FileMode{
		"bin/ncc-orchestrator":      0o775,
		"frontend-dist/index.htm":   0o664,
		"helper/ncc-upgrade-helper": 0o755,
		"config.yaml":               0o644,
		".ncc-api-token":            0o600,
		trustedStackFile:            0o644,
		"logs/v2-api.log":           0o600,
		"run/v2-supervisor.pid":     0o644,
		"run/upgrade/state.json":    0o600,
		"outputfiles/runs/x.json":   0o644,
	}
	for rel, mode := range files {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(rel), mode); err != nil {
			t.Fatal(err)
		}
		_ = os.Chmod(p, mode)
	}
	// A link the service could have planted must not be followed.
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "logs", "link")); err != nil {
		t.Fatal(err)
	}

	before, err := captureOwnership(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ownershipCurrent(dir, uid, gid) {
		t.Fatal("a root install is not in the service layout")
	}
	if err := applyServiceOwnership(dir, uid, gid); err != nil {
		t.Fatal(err)
	}
	if !ownershipCurrent(dir, uid, gid) {
		t.Fatal("not in the service layout after applying it")
	}
	owner := func(rel string) (int, int, os.FileMode) {
		u, g, m, _, err := statOwner(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		return u, g, m
	}
	for rel, want := range map[string][2]int{
		".":                       {0, gid},
		"run":                     {0, gid},
		"run/ops":                 {0, gid},
		"run/ops/req":             {0, gid},
		"run/upgrade/state.json":  {0, 0},
		"bin/ncc-orchestrator":    {0, 0},
		"helper":                  {0, 0},
		trustedStackFile:          {0, 0},
		"config.yaml":             {uid, gid},
		".ncc-api-token":          {uid, gid},
		"logs/v2-api.log":         {uid, gid},
		"logs/link":               {uid, gid},
		"run/v2-supervisor.pid":   {uid, gid},
		"outputfiles/runs/x.json": {uid, gid},
	} {
		if u, g, _ := owner(rel); u != want[0] || g != want[1] {
			t.Errorf("%s is %d:%d, want %d:%d", rel, u, g, want[0], want[1])
		}
	}
	if _, _, m := owner("."); m&os.ModeSticky == 0 {
		t.Error("the install is not sticky")
	}
	if _, _, m := owner("bin/ncc-orchestrator"); m.Perm()&0o022 != 0 {
		t.Errorf("the program is writable by others: %v", m)
	}
	if _, _, m := owner(".ncc-api-token"); m.Perm() != 0o600 {
		t.Errorf("the token is %v", m.Perm())
	}
	if _, _, m := owner("run/ops/req"); m&os.ModeSticky == 0 || m.Perm() != 0o770 {
		t.Errorf("the request folder is %v", m)
	}
	if u, _, _, _, _ := statOwner(outside); u != 0 {
		t.Error("the link was followed")
	}
	if !writableBy(filepath.Join(dir, "logs"), uid, gid) || writableBy(filepath.Join(dir, "bin"), uid, gid) {
		t.Error("writableBy")
	}
	if u, g, ok := serviceIDsOfInstall(dir); ok {
		t.Logf("service ids %d:%d (a group of this id exists)", u, g)
	}

	// Something the service wrote since, then the rollback.
	if err := os.WriteFile(filepath.Join(dir, "logs", "new.log"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Lchown(filepath.Join(dir, "logs", "new.log"), uid, gid)
	rec, err := decodeOwnershipRecord(before.encode())
	if err != nil {
		t.Fatal(err)
	}
	if err := restoreOwnership(dir, rec); err != nil {
		t.Fatal(err)
	}
	for rel := range files {
		if u, g, _ := owner(rel); u != 0 || g != 0 {
			t.Errorf("%s is %d:%d after the rollback", rel, u, g)
		}
	}
	if u, g, _ := owner("logs/new.log"); u != 0 || g != 0 {
		t.Errorf("a new file is %d:%d after the rollback", u, g)
	}
	if _, _, m := owner("."); m&os.ModeSticky != 0 || m.Perm() != 0o755 {
		t.Errorf("the install is %v after the rollback", m)
	}
	// The write bits taken from the programs' group and others stay off.
}

func TestApplyServiceOwnershipRefusesRoot(t *testing.T) {
	if err := applyServiceOwnership(t.TempDir(), 0, 0); err == nil {
		t.Fatal("root accepted as the service user")
	}
}
