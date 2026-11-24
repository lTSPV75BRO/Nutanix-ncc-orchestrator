package winacl

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Supported is true on Windows.
const Supported = true

const (
	sidSystem           = "S-1-5-18"
	sidAdministrators   = "S-1-5-32-544"
	sidTrustedInstaller = "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"

	fileAllAccess = 0x1F01FF

	// Every right that changes, creates, deletes or takes over something.
	writeRights = windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA | windows.FILE_WRITE_EA |
		windows.FILE_WRITE_ATTRIBUTES | 0x40 /* FILE_DELETE_CHILD */ | windows.DELETE |
		windows.WRITE_DAC | windows.WRITE_OWNER | windows.GENERIC_WRITE | windows.GENERIC_ALL

	aceInheritFlags = windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE

	accessAllowed               = 0
	accessDenied                = 1
	accessAllowedObject         = 5
	accessDeniedObject          = 6
	accessAllowedCallback       = 9
	accessDeniedCallback        = 10
	accessAllowedCallbackObject = 11
	accessDeniedCallbackObject  = 12
)

// broadSIDs are groups that stand for "any user", not a chosen account.
var broadSIDs = map[string]string{
	"S-1-1-0":      "Everyone",
	"S-1-2-0":      "LOCAL",
	"S-1-2-1":      "CONSOLE LOGON",
	"S-1-5-2":      "NETWORK",
	"S-1-5-3":      "BATCH",
	"S-1-5-4":      "INTERACTIVE",
	"S-1-5-6":      "SERVICE",
	"S-1-5-7":      "ANONYMOUS LOGON",
	"S-1-5-11":     "Authenticated Users",
	"S-1-5-13":     "TERMINAL SERVER USER",
	"S-1-5-14":     "REMOTE INTERACTIVE LOGON",
	"S-1-5-15":     "This Organization",
	"S-1-5-113":    "Local account",
	"S-1-5-32-545": "Users",
	"S-1-5-32-546": "Guests",
	"S-1-5-32-547": "Power Users",
	"S-1-5-32-554": "Pre-Windows 2000 Compatible Access",
	"S-1-5-80-0":   "ALL SERVICES",
	"S-1-15-2-1":   "ALL APPLICATION PACKAGES",
	"S-1-15-2-2":   "ALL RESTRICTED APPLICATION PACKAGES",
}

// broadName names a broad principal, or returns "" for a specific account.
func broadName(sid string) string {
	if n, ok := broadSIDs[sid]; ok {
		return n
	}
	if strings.HasPrefix(sid, "S-1-5-21-") {
		switch sid[strings.LastIndexByte(sid, '-')+1:] {
		case "501":
			return "Guest"
		case "513": // also the local "None" group every local user is in
			return "Domain Users"
		case "514":
			return "Domain Guests"
		case "515":
			return "Domain Computers"
		}
	}
	return ""
}

// creatorSID reports CREATOR OWNER / CREATOR GROUP: placeholders that give
// whoever creates a file full access to it.
func creatorSID(sid string) bool { return sid == "S-1-3-0" || sid == "S-1-3-1" }

type ace struct {
	typ, flags byte
	mask       uint32
	sid        string // "" when the ACE type is unknown
	raw        []byte
}

func (a ace) allow() bool {
	switch a.typ {
	case accessAllowed, accessAllowedObject, accessAllowedCallback, accessAllowedCallbackObject:
		return true
	}
	return false
}

func (a ace) deny() bool {
	switch a.typ {
	case accessDenied, accessDeniedObject, accessDeniedCallback, accessDeniedCallbackObject:
		return true
	}
	return false
}

func (a ace) inherited() bool { return a.flags&windows.INHERITED_ACE != 0 }

// excess returns the broad principal that has more than mode allows, or "".
func (a ace) excess(mode Mode) string {
	if !a.allow() || a.mask == 0 {
		return ""
	}
	name := broadName(a.sid)
	if name == "" || (mode == NoBroadWrite && a.mask&writeRights == 0) {
		return ""
	}
	return name
}

// security is a parsed owner and DACL.
type security struct {
	owner     string
	aces      []ace
	open      bool // no DACL at all: everyone has full access
	protected bool
	revision  byte
}

func parseACL(acl *windows.ACL) (byte, []ace) {
	p := unsafe.Pointer(acl)
	hdr := unsafe.Slice((*byte)(p), 8)
	size := int(binary.LittleEndian.Uint16(hdr[2:]))
	count := int(binary.LittleEndian.Uint16(hdr[4:]))
	if size < 8 {
		return hdr[0], nil
	}
	buf := unsafe.Slice((*byte)(p), size)
	var out []ace
	for off := 8; count > 0 && off+8 <= size; count-- {
		n := int(binary.LittleEndian.Uint16(buf[off+2:]))
		if n < 8 || off+n > size {
			break
		}
		raw := append([]byte(nil), buf[off:off+n]...)
		off += n
		a := ace{typ: raw[0], flags: raw[1], raw: raw, mask: binary.LittleEndian.Uint32(raw[4:])}
		sidAt := 0
		switch a.typ {
		case accessAllowed, accessDenied, accessAllowedCallback, accessDeniedCallback:
			sidAt = 8
		case accessAllowedObject, accessDeniedObject, accessAllowedCallbackObject, accessDeniedCallbackObject:
			if n >= 12 {
				sidAt = 12
				present := binary.LittleEndian.Uint32(raw[8:])
				if present&1 != 0 {
					sidAt += 16
				}
				if present&2 != 0 {
					sidAt += 16
				}
			}
		}
		// A SID is 8 bytes plus 4 per sub-authority.
		if sidAt > 0 && sidAt+8 <= n && sidAt+8+4*int(raw[sidAt+1]) <= n {
			a.sid = (*windows.SID)(unsafe.Pointer(&raw[sidAt])).String()
		}
		out = append(out, a)
	}
	return hdr[0], out
}

func parseSD(sd *windows.SECURITY_DESCRIPTOR) (security, error) {
	var s security
	if o, _, err := sd.Owner(); err == nil && o != nil {
		s.owner = o.String()
	}
	if c, _, err := sd.Control(); err == nil {
		s.protected = c&windows.SE_DACL_PROTECTED != 0
	}
	acl, _, err := sd.DACL()
	if err != nil && !errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
		return s, err
	}
	if acl == nil {
		s.open = true
		return s, nil
	}
	s.revision, s.aces = parseACL(acl)
	runtime.KeepAlive(sd)
	return s, nil
}

// excess lists the broad principals that have more access than mode allows.
func (s security) excess(mode Mode) []string {
	if s.open {
		return []string{"Everyone"}
	}
	var out []string
	seen := map[string]bool{}
	for _, a := range s.aces {
		if n := a.excess(mode); n != "" && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

const infoFlags = windows.OWNER_SECURITY_INFORMATION | windows.DACL_SECURITY_INFORMATION

// openPath opens a file or folder itself, never what a link points to.
func openPath(path string, access uint32) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	return windows.CreateFile(p, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
}

type fileKind struct {
	dir  bool
	link bool
}

// inspect reports the kind of an open file and whether its volume keeps ACLs.
func inspect(h windows.Handle) (fileKind, error) {
	var fi windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &fi); err != nil {
		return fileKind{}, err
	}
	k := fileKind{dir: fi.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0}
	k.link = !k.dir && fi.NumberOfLinks > 1
	if fi.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		var tag struct{ attributes, reparseTag uint32 }
		err := windows.GetFileInformationByHandleEx(h, windows.FileAttributeTagInfo,
			(*byte)(unsafe.Pointer(&tag)), uint32(unsafe.Sizeof(tag)))
		// Name surrogates are symlinks and junctions. Other reparse points
		// (deduplicated or cloud-synced files) are ordinary files here.
		if err != nil || tag.reparseTag&0x20000000 != 0 {
			k.link = true
		}
	}
	var volFlags uint32
	if err := windows.GetVolumeInformationByHandle(h, nil, 0, nil, nil, &volFlags, nil, 0); err == nil &&
		volFlags&windows.FILE_PERSISTENT_ACLS == 0 {
		return k, ErrUnsupported
	}
	return k, nil
}

func readSecurity(h windows.Handle) (security, error) {
	sd, err := windows.GetSecurityInfo(h, windows.SE_FILE_OBJECT, infoFlags)
	if err != nil {
		return security{}, unsupported(err)
	}
	return parseSD(sd)
}

func unsupported(err error) error {
	if errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_INVALID_FUNCTION) ||
		errors.Is(err, windows.ERROR_CALL_NOT_IMPLEMENTED) {
		return ErrUnsupported
	}
	return err
}

// identity is the account this process runs as.
type identity struct {
	user     string
	elevated bool
	owner    *windows.SID // owner the token may assign
}

func currentIdentity() (identity, error) {
	tok := windows.GetCurrentProcessToken()
	tu, err := tok.GetTokenUser()
	if err != nil {
		return identity{}, err
	}
	id := identity{user: tu.User.Sid.String(), elevated: tok.IsElevated()}
	var n uint32
	_ = windows.GetTokenInformation(tok, windows.TokenOwner, nil, 0, &n)
	if n > 0 {
		buf := make([]uint64, (n+7)/8) // a pointer to a SID stored behind it
		if windows.GetTokenInformation(tok, windows.TokenOwner, (*byte)(unsafe.Pointer(&buf[0])), n, &n) == nil {
			if sid := *(**windows.SID)(unsafe.Pointer(&buf[0])); sid != nil {
				id.owner, _ = sid.Copy()
			}
			runtime.KeepAlive(buf)
		}
	}
	return id, nil
}

// required are the accounts that always get full control.
func (id identity) required() []string {
	out := []string{sidSystem, sidAdministrators}
	if id.user != sidSystem && id.user != sidAdministrators {
		out = append(out, id.user)
	}
	return out
}

// replaceOwner reports whether the owner should be taken over: an owner can
// always rewrite the DACL, so a file another user created first must not stay
// theirs once the stack uses it. Only possible when running elevated.
func (id identity) replaceOwner(owner string) bool {
	if !id.elevated || id.owner == nil || owner == "" {
		return false
	}
	switch owner {
	case sidSystem, sidAdministrators, sidTrustedInstaller, id.user:
		return false
	}
	return true
}

func rawAllow(sid string, flags byte, mask uint32) ([]byte, error) {
	s, err := windows.StringToSid(sid)
	if err != nil {
		return nil, err
	}
	body := unsafe.Slice((*byte)(unsafe.Pointer(s)), s.Len())
	raw := make([]byte, 8, 8+len(body))
	raw[0], raw[1] = accessAllowed, flags
	binary.LittleEndian.PutUint16(raw[2:], uint16(8+len(body)))
	binary.LittleEndian.PutUint32(raw[4:], mask)
	raw = append(raw, body...)
	runtime.KeepAlive(s)
	return raw, nil
}

// buildACL lays out ACE bytes as an ACL. The backing array is []uint32 so the
// structure is aligned.
func buildACL(revision byte, raws [][]byte) *windows.ACL {
	size := 8
	for _, r := range raws {
		size += len(r)
	}
	words := make([]uint32, (size+3)/4)
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), size)
	buf[0] = max(revision, 2)
	binary.LittleEndian.PutUint16(buf[2:], uint16(size))
	binary.LittleEndian.PutUint16(buf[4:], uint16(len(raws)))
	off := 8
	for _, r := range raws {
		off += copy(buf[off:], r)
	}
	return (*windows.ACL)(unsafe.Pointer(&words[0]))
}

// cleanACL is the protected DACL for s: deny entries first, then full control
// for the required accounts, then what specific accounts already had.
func cleanACL(s security, mode Mode, dir bool, id identity, parent func() ([]ace, bool)) (*windows.ACL, error) {
	var denies, allows [][]byte
	var flags byte
	if dir {
		flags = aceInheritFlags
	}
	need := map[string]bool{}
	for _, sid := range id.required() {
		raw, err := rawAllow(sid, flags, fileAllAccess)
		if err != nil {
			return nil, err
		}
		need[sid] = true
		allows = append(allows, raw)
	}
	for _, a := range s.aces {
		raw := append([]byte(nil), a.raw...)
		raw[1] &^= windows.INHERITED_ACE
		switch {
		case a.deny():
			denies = append(denies, raw)
			continue
		case !a.allow() || creatorSID(a.sid) || need[a.sid]:
			continue
		case broadName(a.sid) != "":
			if mode == Private {
				continue
			}
			m := a.mask
			if m&windows.GENERIC_ALL != 0 {
				m |= windows.GENERIC_READ | windows.GENERIC_EXECUTE
			}
			if m &^= writeRights; m == 0 {
				continue
			}
			binary.LittleEndian.PutUint32(raw[4:], m)
		case a.inherited() && fromCreatorOwner(a, dir, parent):
			continue
		}
		allows = append(allows, raw)
	}
	return buildACL(s.revision, append(denies, allows...)), nil
}

// fromCreatorOwner reports whether an inherited entry for a specific account
// is there only because that account created the file (the parent grants
// CREATOR OWNER, not the account). Such an entry was not a deliberate grant.
func fromCreatorOwner(a ace, dir bool, parent func() ([]ace, bool)) bool {
	aces, ok := parent()
	if !ok {
		return false
	}
	var want byte = windows.OBJECT_INHERIT_ACE
	if dir {
		want = aceInheritFlags
	}
	for _, p := range aces {
		if p.allow() && p.sid == a.sid && p.flags&want != 0 {
			return false
		}
	}
	return true
}

func parentACEs(path string) func() ([]ace, bool) {
	return func() ([]ace, bool) {
		h, err := openPath(filepath.Dir(path), windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES)
		if err != nil {
			return nil, false
		}
		defer func() { _ = windows.CloseHandle(h) }()
		s, err := readSecurity(h)
		return s.aces, err == nil
	}
}

// Check lists the broad principals that have more access to path than mode
// allows. A missing path has none.
func Check(path string, mode Mode) ([]string, error) {
	h, err := openPath(path, windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = windows.CloseHandle(h) }()
	k, err := inspect(h)
	if err != nil {
		return nil, err
	}
	if k.link {
		return nil, ErrLink
	}
	s, err := readSecurity(h)
	if err != nil {
		return nil, err
	}
	return s.excess(mode), nil
}

// Restrict removes broad access from path as mode says, and reports whether
// anything changed. A missing path is not an error.
func Restrict(path string, mode Mode) (bool, error) {
	return restrict(path, mode, false)
}

// restrict also takes the file over from an untrusted owner when the ACL needs
// work or takeOwner is set (a child of a folder whose ACL needed work).
func restrict(path string, mode Mode, takeOwner bool) (bool, error) {
	// Nothing is opened for writing unless there is something to do.
	broad, err := Check(path, mode)
	if err != nil || (len(broad) == 0 && !takeOwner) {
		return false, err
	}
	id, err := currentIdentity()
	if err != nil {
		return false, err
	}
	access := uint32(windows.READ_CONTROL | windows.WRITE_DAC | windows.FILE_READ_ATTRIBUTES)
	withOwner := id.elevated
	var h windows.Handle
	if withOwner {
		if h, err = openPath(path, access|windows.WRITE_OWNER); err != nil {
			withOwner = false
		}
	}
	if !withOwner {
		if len(broad) == 0 {
			return false, nil
		}
		if h, err = openPath(path, access); err != nil {
			if errors.Is(err, fs.ErrNotExist) { // a temp file that is gone again
				return false, nil
			}
			return false, err
		}
	}
	defer func() { _ = windows.CloseHandle(h) }()
	// The path may have changed since the check; decide on the open file.
	k, err := inspect(h)
	if err != nil {
		return false, err
	}
	if k.link {
		return false, ErrLink
	}
	s, err := readSecurity(h)
	if err != nil {
		return false, err
	}
	var info windows.SECURITY_INFORMATION
	var owner *windows.SID
	var acl *windows.ACL
	fix := len(s.excess(mode)) > 0
	if fix {
		info = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
		if acl, err = cleanACL(s, mode, k.dir, id, parentACEs(path)); err != nil {
			return false, err
		}
	}
	if withOwner && (fix || takeOwner) && id.replaceOwner(s.owner) {
		info |= windows.OWNER_SECURITY_INFORMATION
		owner = id.owner
	}
	if info == 0 {
		return false, nil
	}
	if err := windows.SetSecurityInfo(h, windows.SE_FILE_OBJECT, info, owner, nil, acl, nil); err != nil {
		return false, unsupported(err)
	}
	return true, nil
}

// walk calls fn for root and everything below it. Links below root are skipped
// and never followed.
func walk(root string, fn func(path string) error) []error {
	if err := fn(root); err != nil {
		return []error{&PathError{root, err}}
	}
	var errs []error
	_ = filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err == nil && path != root {
			err = fn(path)
		}
		if err != nil && !errors.Is(err, ErrLink) && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, &PathError{path, err})
		}
		return nil
	})
	return errs
}

// RestrictTree restricts a folder and everything in it. The folder's new ACL is
// inherited, so files added later are covered too. It returns the changed paths.
func RestrictTree(root string, mode Mode) ([]string, []error) {
	var changed []string
	rootChanged := false
	errs := walk(root, func(path string) error {
		did, err := restrict(path, mode, rootChanged)
		if did {
			changed = append(changed, path)
			rootChanged = rootChanged || path == root
		}
		return err
	})
	return changed, errs
}

// CheckTree lists the paths in a tree that broad groups can access too freely.
func CheckTree(root string, mode Mode) ([]string, []error) {
	var broad []string
	errs := walk(root, func(path string) error {
		names, err := Check(path, mode)
		if len(names) > 0 {
			broad = append(broad, path)
		}
		return err
	})
	return broad, errs
}

// WriteFile is os.WriteFile that also restricts a 0600 file before its content
// is written. Permission errors are ignored here: a start reports them.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if perm&0o077 == 0 {
		_, _ = Restrict(path, Private)
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Rename is os.Rename for replacing a secret file. A renamed file keeps its own
// ACL, so dst would otherwise end up with whatever the folder hands out:
// tmp first gets the access dst had, without broad groups.
func Rename(tmp, dst string) error {
	_ = copyAccess(tmp, dst)
	_, _ = Restrict(tmp, Private)
	return os.Rename(tmp, dst)
}

// copyAccess gives tmp the entries set directly on dst.
func copyAccess(tmp, dst string) error {
	h, err := openPath(dst, windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(h) }()
	k, err := inspect(h)
	if err != nil {
		return err
	}
	if k.dir || k.link {
		return ErrLink
	}
	s, err := readSecurity(h)
	if err != nil || s.open {
		return err
	}
	var own []ace
	for _, a := range s.aces {
		if !a.inherited() && (s.protected || a.allow()) {
			own = append(own, a)
		}
	}
	if len(own) == 0 {
		return nil
	}
	id, err := currentIdentity()
	if err != nil {
		return err
	}
	info := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.UNPROTECTED_DACL_SECURITY_INFORMATION)
	if s.protected {
		info = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
	}
	s.aces = own
	acl, err := cleanACL(s, Private, false, id, func() ([]ace, bool) { return nil, false })
	if err != nil {
		return err
	}
	t, err := openPath(tmp, windows.READ_CONTROL|windows.WRITE_DAC|windows.FILE_READ_ATTRIBUTES)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(t) }()
	if tk, err := inspect(t); err != nil || tk.dir || tk.link {
		return errors.Join(err, ErrLink)
	}
	return windows.SetSecurityInfo(t, windows.SE_FILE_OBJECT, info, nil, nil, acl, nil)
}
