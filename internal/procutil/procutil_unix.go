//go:build !windows

package procutil

import (
	"errors"
	"os"
	"syscall"
)

func exists(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EPERM)
}

// POSIX callers read the full argv (/proc or ps) instead.
func imagePath(int) (string, error) { return "", errors.ErrUnsupported }
