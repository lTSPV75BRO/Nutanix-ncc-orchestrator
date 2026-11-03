//go:build !windows

package hostinfo

import "syscall"

func fileLimit() (soft, hard uint64, ok bool) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, 0, false
	}
	return uint64(rl.Cur), uint64(rl.Max), true
}

func inodes(path string) (free, total uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	return uint64(st.Ffree), uint64(st.Files), true
}
