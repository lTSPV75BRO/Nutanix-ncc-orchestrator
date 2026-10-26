// Package procutil answers "is this PID a live process?" portably. The
// POSIX idiom (os.FindProcess + Signal(0)) does not work on Windows, where
// Process.Signal only supports Kill and returns an error for every other
// signal, so a signal-0 probe reports every live process as dead there.
package procutil

import "errors"

// Exists reports whether a process with the given pid is currently running.
// A process that exists but belongs to another user (EPERM on POSIX,
// ERROR_ACCESS_DENIED on Windows) counts as existing. Non-positive pids never
// exist.
func Exists(pid int) bool {
	if pid <= 0 {
		return false
	}
	return exists(pid)
}

// ImagePath returns the full path of the executable running as pid. It is
// implemented on Windows only (QueryFullProcessImageName), where there is no
// permission-safe way to read another process's command line; elsewhere it
// returns errors.ErrUnsupported.
func ImagePath(pid int) (string, error) {
	if pid <= 0 {
		return "", errors.New("invalid pid")
	}
	return imagePath(pid)
}
