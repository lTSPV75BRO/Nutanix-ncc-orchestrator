//go:build !windows

package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// statOwner returns the owner, group and permission bits (with sticky) of a
// path, without following a symbolic link.
func statOwner(p string) (uid, gid int, mode os.FileMode, isDir bool, err error) {
	st, err := os.Lstat(p)
	if err != nil {
		return 0, 0, 0, false, err
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, false, errors.New("no owner information")
	}
	return int(sys.Uid), int(sys.Gid), st.Mode() & (os.ModePerm | os.ModeSticky | os.ModeSetgid | os.ModeSetuid), st.IsDir(), nil
}

// lchownTree gives everything below root (root included) to uid:gid. Symbolic
// links are changed themselves and never followed. stripWrite also takes the
// write bits of group and others away.
func lchownTree(root string, uid, gid int, stripWrite bool) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if err := os.Lchown(p, uid, gid); err != nil {
			return err
		}
		if stripWrite && d.Type()&fs.ModeSymlink == 0 {
			if info, err := d.Info(); err == nil && info.Mode().Perm()&0o022 != 0 {
				if err := os.Chmod(p, info.Mode().Perm()&^0o022|info.Mode()&(os.ModeSticky|os.ModeSetgid)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// setDir makes p a folder with this owner and mode (no symbolic link).
func setDir(p string, uid, gid int, mode os.FileMode) error {
	if err := os.MkdirAll(p, mode.Perm()); err != nil {
		return err
	}
	st, err := os.Lstat(p)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a folder", p)
	}
	if err := os.Lchown(p, uid, gid); err != nil {
		return err
	}
	return os.Chmod(p, mode)
}

// isRootTree reports a top-level entry that stays root's.
func isRootTree(name string) bool {
	for _, t := range serviceRootTrees {
		if name == t {
			return true
		}
	}
	return false
}

// applyServiceOwnership puts the install into the layout for the service user
// uid:gid (see serviceowner.go). Run as root, with the service stopped.
func applyServiceOwnership(installDir string, uid, gid int) error {
	if uid == 0 {
		return errors.New("the service user must not be root")
	}
	st, err := os.Lstat(installDir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a folder", installDir)
	}
	// The install first: from here on the service cannot add anything to it
	// that root has to trust.
	if err := setDir(installDir, 0, gid, st.Mode().Perm()&0o005|0o770|os.ModeSticky); err != nil {
		return err
	}
	runDir := filepath.Join(installDir, "run")
	if err := setDir(runDir, 0, gid, 0o775|os.ModeSticky); err != nil {
		return err
	}
	entries, err := os.ReadDir(installDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name, p := e.Name(), filepath.Join(installDir, e.Name())
		switch {
		case name == "run":
		case isRootTree(name):
			if err := lchownTree(p, 0, 0, true); err != nil {
				return err
			}
		case name == trustedStackFile && e.Type().IsRegular():
			// The ledger of trusted program files is root's: a restore by
			// root trusts it (the sticky install keeps the service from
			// replacing it).
			if err := os.Lchown(p, 0, 0); err != nil {
				return err
			}
			if err := os.Chmod(p, 0o600); err != nil {
				return err
			}
		default:
			if err := lchownTree(p, uid, gid, false); err != nil {
				return err
			}
		}
	}
	runEntries, err := os.ReadDir(runDir)
	if err != nil {
		return err
	}
	for _, e := range runEntries {
		p := filepath.Join(runDir, e.Name())
		switch e.Name() {
		case "upgrade":
			if err := lchownTree(p, 0, 0, true); err != nil {
				return err
			}
		case opsDirName:
		default:
			if err := lchownTree(p, uid, gid, false); err != nil {
				return err
			}
		}
	}
	return prepareOpsDirs(installDir, gid)
}

// prepareOpsDirs creates the request folders of the service helper.
func prepareOpsDirs(installDir string, gid int) error {
	ops := opsDir(installDir)
	if err := setDir(ops, 0, gid, 0o750); err != nil {
		return err
	}
	if err := setDir(opsRequestDir(installDir), 0, gid, 0o770|os.ModeSticky); err != nil {
		return err
	}
	return setDir(opsResultDir(installDir), 0, gid, 0o750)
}

// ownershipCurrent reports whether the install is in the layout for uid:gid,
// as far as a quick look tells (the top-level entries).
func ownershipCurrent(installDir string, uid, gid int) bool {
	check := func(p string, wantUID, wantGID int, sticky bool) bool {
		u, g, m, _, err := statOwner(p)
		return err == nil && u == wantUID && g == wantGID && (!sticky || m&os.ModeSticky != 0)
	}
	if !check(installDir, 0, gid, true) || !check(filepath.Join(installDir, "run"), 0, gid, true) || !check(opsRequestDir(installDir), 0, gid, true) {
		return false
	}
	entries, err := os.ReadDir(installDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		p := filepath.Join(installDir, e.Name())
		switch {
		case e.Name() == "run":
		case isRootTree(e.Name()):
			u, _, m, _, err := statOwner(p)
			if err != nil || u != 0 || m.Perm()&0o022 != 0 {
				return false
			}
		case e.Name() == trustedStackFile && e.Type().IsRegular():
			if !check(p, 0, 0, false) {
				return false
			}
		default:
			if !check(p, uid, gid, false) {
				return false
			}
		}
	}
	return true
}

// captureOwnership records the owners of the install, for a rollback.
func captureOwnership(installDir string) (ownershipRecord, error) {
	var rec ownershipRecord
	add := func(rel string, tree bool) error {
		p := filepath.Join(installDir, filepath.FromSlash(rel))
		uid, gid, mode, isDir, err := statOwner(p)
		if err != nil {
			return err
		}
		rec.Entries = append(rec.Entries, ownEntry{Path: rel, UID: uid, GID: gid, Mode: uint32(mode), Tree: tree && isDir})
		if !tree || !isDir {
			return nil
		}
		return filepath.WalkDir(p, func(q string, d fs.DirEntry, err error) error {
			if err != nil || q == p {
				return err
			}
			u, g, m, _, err := statOwner(q)
			if err != nil {
				return nil // gone in the meantime
			}
			if u != uid || g != gid {
				r, _ := filepath.Rel(installDir, q)
				rec.Exceptions = append(rec.Exceptions, ownEntry{Path: filepath.ToSlash(r), UID: u, GID: g, Mode: uint32(m)})
				if len(rec.Exceptions) > maxOwnershipExceptions {
					return fmt.Errorf("more than %d files of %s have owners of their own", maxOwnershipExceptions, installDir)
				}
			}
			return nil
		})
	}
	if err := add(".", false); err != nil {
		return rec, err
	}
	rec.DefaultUID, rec.DefaultGID = rec.Entries[0].UID, rec.Entries[0].GID
	entries, err := os.ReadDir(installDir)
	if err != nil {
		return rec, err
	}
	counts := map[[2]int]int{}
	for _, e := range entries {
		if e.Name() == "run" && e.IsDir() {
			if err := add("run", false); err != nil {
				return rec, err
			}
			runEntries, err := os.ReadDir(filepath.Join(installDir, "run"))
			if err != nil {
				return rec, err
			}
			for _, r := range runEntries {
				if r.Name() == "upgrade" {
					continue // the update helper's own folder stays root's
				}
				if err := add("run/"+r.Name(), true); err != nil {
					return rec, err
				}
			}
			continue
		}
		if err := add(e.Name(), true); err != nil {
			return rec, err
		}
		if !isRootTree(e.Name()) {
			last := rec.Entries[len(rec.Entries)-1]
			counts[[2]int{last.UID, last.GID}]++
		}
	}
	best := 0
	for k, n := range counts {
		if n > best {
			best, rec.DefaultUID, rec.DefaultGID = n, k[0], k[1]
		}
	}
	return rec, nil
}

// restoreOwnership puts the owners of a record back. What was added since gets
// the record's default owner.
func restoreOwnership(installDir string, rec ownershipRecord) error {
	known := map[string]bool{}
	var errs []error
	for _, e := range sortedEntries(rec.Entries) {
		known[e.Path] = true
		p := filepath.Join(installDir, filepath.FromSlash(e.Path))
		if _, err := os.Lstat(p); os.IsNotExist(err) {
			continue
		}
		if e.Tree {
			errs = append(errs, lchownTree(p, e.UID, e.GID, false))
		} else {
			errs = append(errs, os.Lchown(p, e.UID, e.GID))
		}
		if st, err := os.Lstat(p); err == nil && st.Mode()&os.ModeSymlink == 0 {
			errs = append(errs, os.Chmod(p, os.FileMode(e.Mode)))
		}
	}
	for _, e := range rec.Exceptions {
		p := filepath.Join(installDir, filepath.FromSlash(e.Path))
		if st, err := os.Lstat(p); err == nil {
			errs = append(errs, os.Lchown(p, e.UID, e.GID))
			if st.Mode()&os.ModeSymlink == 0 {
				errs = append(errs, os.Chmod(p, os.FileMode(e.Mode)))
			}
		}
	}
	for _, dir := range []string{".", "run"} {
		entries, err := os.ReadDir(filepath.Join(installDir, dir))
		if err != nil {
			continue
		}
		for _, x := range entries {
			rel := x.Name()
			if dir != "." {
				rel = dir + "/" + rel
			}
			if known[rel] || rel == "run" || rel == "run/upgrade" {
				continue
			}
			uid, gid := rec.DefaultUID, rec.DefaultGID
			if isRootTree(rel) || rel == "run/"+opsDirName {
				uid, gid = 0, 0
			}
			errs = append(errs, lchownTree(filepath.Join(installDir, filepath.FromSlash(rel)), uid, gid, false))
		}
	}
	return errors.Join(errs...)
}

// writableBy reports whether uid:gid can write the folder p.
func writableBy(p string, uid, gid int) bool {
	u, g, m, isDir, err := statOwner(p)
	if err != nil || !isDir {
		return false
	}
	switch {
	case u == uid:
		return m&0o200 != 0
	case g == gid:
		return m&0o020 != 0
	}
	return m&0o002 != 0
}

// openNoFollow opens a file for reading, refusing a symbolic link.
func openNoFollow(p string) (*os.File, error) {
	return os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

// fileOwnerUID is the owner of an open file (-1 when unknown).
func fileOwnerUID(fi os.FileInfo) int {
	if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int(sys.Uid)
	}
	return -1
}

// serviceIDsOfInstall returns the service user of an install in the
// unprivileged layout (root:<group> and sticky), found by its group.
func serviceIDsOfInstall(installDir string) (uid, gid int, ok bool) {
	u, g, m, isDir, err := statOwner(installDir)
	if err != nil || !isDir || u != 0 || g == 0 || m&os.ModeSticky == 0 {
		return 0, 0, false
	}
	grp, err := user.LookupGroupId(strconv.Itoa(g))
	if err != nil {
		return 0, 0, false
	}
	uid, gid, ok = lookupServiceIDs(grp.Name)
	return uid, gid, ok && gid == g && uid != 0
}

// unprivilegedInstall reports that this process is not root and the install
// is laid out for a service user (so root-only files are not ours to write).
func unprivilegedInstall(installDir string) bool {
	if os.Geteuid() == 0 {
		return false
	}
	_, _, ok := serviceIDsOfInstall(installDir)
	return ok
}

// giveToService hands a file root just wrote in the install to the service
// user, so the service can replace it later (run/ is sticky).
func giveToService(installDir, path string) {
	if os.Geteuid() != 0 {
		return
	}
	if uid, gid, ok := serviceIDsOfInstall(installDir); ok {
		_ = os.Lchown(path, uid, gid)
	}
}

// serviceUserRunner runs programs as the service user of an unprivileged
// install (for root: doctor fixes write into the service user's folders),
// else as this process.
func serviceUserRunner(installDir string) func(time.Duration, string, ...string) (string, error) {
	uid, gid, ok := serviceIDsOfInstall(installDir)
	if os.Geteuid() != 0 || !ok {
		return runUpgradeCommand
	}
	return func(timeout time.Duration, name string, args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, name, args...) // #nosec G204 -- our own program, argv only
		cmd.Dir = installDir
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid), Groups: []uint32{}}}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
}

// openLogNoFollow opens a log for appending; never through a symbolic link,
// and only a regular file (root writes logs in the service user's folder).
func openLogNoFollow(p string) (*os.File, error) {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o640)
	if err != nil {
		return nil, err
	}
	if st, err := f.Stat(); err != nil || !st.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.New("not a regular file")
	}
	return f, nil
}
