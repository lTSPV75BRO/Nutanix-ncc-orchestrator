// Package winacl keeps the product's secret files and program folders away
// from ordinary local users on Windows, where Go's 0600/0700 modes have no
// effect. On other platforms every function is a no-op (or the plain os call).
//
// A path is changed only when a broad group (Everyone, Authenticated Users,
// Users, Guests, INTERACTIVE, ...) has access it should not have. The new DACL
// is protected from inheritance and grants full control to SYSTEM,
// Administrators and the account running the process. Access given to specific
// users, service accounts and other groups is kept. All accounts are handled
// as SIDs, so the Windows display language does not matter.
package winacl

import "errors"

// Mode says what broad groups may keep on a path.
type Mode int

const (
	// Private removes all access for broad groups.
	Private Mode = iota
	// NoBroadWrite lets broad groups read and execute, but not change, create,
	// delete or take over anything.
	NoBroadWrite
)

var (
	// ErrUnsupported means the volume keeps no ACLs (FAT32, exFAT, some shares).
	ErrUnsupported = errors.New("the file system does not support Windows permissions")
	// ErrLink means the path is a symbolic link, junction or hard link. Its
	// permissions are not changed, because the target may be someone else's.
	ErrLink = errors.New("the path is a link, so its permissions were not changed")
)

// PathError is a failure on one path inside a tree.
type PathError struct {
	Path string
	Err  error
}

func (e *PathError) Error() string { return e.Path + ": " + e.Err.Error() }
func (e *PathError) Unwrap() error { return e.Err }
