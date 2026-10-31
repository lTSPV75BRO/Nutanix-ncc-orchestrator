//go:build !windows

package main

import "syscall"

// diskFreeBytes returns the number of bytes available to an unprivileged
// process on the filesystem backing path. ok is false when the figure could
// not be determined (so callers report "unknown" rather than a false alarm).
func diskFreeBytes(path string) (free uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, false
	}
	// Bavail is blocks available to non-root; Bsize is the block size.
	return uint64(st.Bavail) * uint64(st.Bsize), true
}
