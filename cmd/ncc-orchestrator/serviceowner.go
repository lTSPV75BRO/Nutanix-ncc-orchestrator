package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Who owns what in an install whose service runs as an unprivileged user
// (LIN-02). The install folder and run/ are root's, group-writable for the
// service and sticky, so the service can add and replace its own files but can
// neither rename nor remove what root owns in them:
//
//	<install>/                root:ncc 1775 (others' bits kept)
//	  bin/ frontend-dist/ helper/   root:root, not writable by group or others
//	  run/                    root:ncc 1775
//	    upgrade/              root:root 0700 (the update helper's)
//	    ops/                  root:ncc 0750; req/ 1770 (requests), res/ 0750
//	  everything else         ncc:ncc, modes kept (secrets stay 0600)
//
// The layout before a migration is saved with the service definitions, so a
// rollback puts the owners back too.

const (
	ownerDefPrefix = "owner:"
	opsDirName     = "ops"
)

// ownEntry is the owner and mode of one path of the install (relative, slash).
type ownEntry struct {
	Path string `json:"path"`
	UID  int    `json:"uid"`
	GID  int    `json:"gid"`
	Mode uint32 `json:"mode"`
	// Tree: everything below has this owner too, except the exceptions.
	Tree bool `json:"tree,omitempty"`
}

// ownershipRecord is the layout of an install as it was.
type ownershipRecord struct {
	Entries    []ownEntry `json:"entries"`
	Exceptions []ownEntry `json:"exceptions,omitempty"`
	// Default owner of what was created after the record was taken.
	DefaultUID int `json:"default_uid"`
	DefaultGID int `json:"default_gid"`
}

// maxOwnershipExceptions bounds the record: an install whose owners are that
// mixed is not migrated (the migration says so).
const maxOwnershipExceptions = 100000

func (r ownershipRecord) encode() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func decodeOwnershipRecord(s string) (ownershipRecord, error) {
	var r ownershipRecord
	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, err
	}
	for _, e := range append(append([]ownEntry{}, r.Entries...), r.Exceptions...) {
		if !ownRelPathOK(e.Path) {
			return r, fmt.Errorf("invalid path %q in the saved owners", e.Path)
		}
	}
	return r, nil
}

// ownRelPathOK: a clean relative path inside the install ("." is the install).
func ownRelPathOK(p string) bool {
	if p == "." {
		return true
	}
	return p != "" && !strings.HasPrefix(p, "/") && filepath.ToSlash(filepath.Clean(p)) == p && p != ".." && !strings.HasPrefix(p, "../")
}

// lookupServiceIDs returns the user and primary group ids of name.
func lookupServiceIDs(name string) (uid, gid int, ok bool) {
	u, err := user.Lookup(name)
	if err != nil {
		return 0, 0, false
	}
	uid, err1 := strconv.Atoi(u.Uid)
	gid, err2 := strconv.Atoi(u.Gid)
	return uid, gid, err1 == nil && err2 == nil
}

// lookupPrimaryGroup is the name of user's primary group ("" when unknown).
func lookupPrimaryGroup(name string) string {
	u, err := user.Lookup(name)
	if err != nil {
		return ""
	}
	g, err := user.LookupGroupId(u.Gid)
	if err != nil {
		return ""
	}
	return g.Name
}

// nologinShell is the shell of the service user.
func nologinShell() string {
	for _, p := range []string{"/usr/sbin/nologin", "/sbin/nologin", "/bin/false"} {
		if fileExists(p) {
			return p
		}
	}
	return "/bin/false"
}

// runUserCommand creates users; a variable for tests.
var runUserCommand = func(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput() // #nosec G204 -- fixed programs; the name matched serviceUserPattern
	return string(out), err
}

// ensureServiceUser creates the system user (and its group) when it is missing.
func ensureServiceUser(name string) (created bool, err error) {
	if rootService(name) {
		return false, nil
	}
	if !serviceUserPattern.MatchString(name) {
		return false, fmt.Errorf("invalid service user %q", name)
	}
	if _, _, ok := lookupServiceIDs(name); ok {
		return false, nil
	}
	args := []string{"--system", "--no-create-home", "--home-dir", "/nonexistent", "--shell", nologinShell(), "--user-group", "--comment", "NCC Orchestrator service", name}
	if out, err := runUserCommand("useradd", args...); err != nil {
		// A group of that name may exist already (a user removed by hand).
		if strings.Contains(out, "group") && strings.Contains(out, "exists") {
			args = []string{"--system", "--no-create-home", "--home-dir", "/nonexistent", "--shell", nologinShell(), "--gid", name, "--comment", "NCC Orchestrator service", name}
			out, err = runUserCommand("useradd", args...)
		}
		if err != nil {
			return false, fmt.Errorf("create the system user %s: %v (%s)", name, err, strings.TrimSpace(out))
		}
	}
	if _, _, ok := lookupServiceIDs(name); !ok {
		return false, fmt.Errorf("the system user %s was created but cannot be found", name)
	}
	return true, nil
}

// ownershipPlanDetail describes what the migration does with the owners.
func ownershipPlanDetail(name string) string {
	return "programs stay root's; the service user " + name + " owns configuration, state, outputs and logs"
}

// sortedEntries orders a record's entries so parents come before children.
func sortedEntries(es []ownEntry) []ownEntry {
	depth := func(p string) int {
		if p == "." {
			return -1
		}
		return strings.Count(p, "/")
	}
	out := append([]ownEntry{}, es...)
	sort.SliceStable(out, func(i, j int) bool { return depth(out[i].Path) < depth(out[j].Path) })
	return out
}

// outsideFoldersNotWritable lists configured folders outside the install that
// the service user cannot write. They are not given to it: a folder such as
// /var/log belongs to others too.
func outsideFoldersNotWritable(installDir string, uid, gid int) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range configuredPaths(installDir) {
		p = filepath.Clean(p)
		if !existingDir(p) {
			p = filepath.Dir(p)
		}
		if seen[p] || pathWithin(p, installDir) || !existingDir(p) || filepath.Dir(p) == p || filepath.Dir(p) == "/" {
			continue
		}
		seen[p] = true
		if withinAny(filepath.ToSlash(p), systemdStrictDenied) {
			continue
		}
		if !writableBy(p, uid, gid) {
			out = append(out, p)
		}
	}
	return out
}
