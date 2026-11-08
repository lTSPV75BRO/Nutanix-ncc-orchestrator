//go:build windows

package notify

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// lockStatusFile takes an exclusive LockFileEx lock on path, retrying until
// timeout, so read-modify-write cycles on shared state files are serialized
// across processes on Windows hosts.
func lockStatusFile(path string, timeout time.Duration) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open notification status lock: %w", err)
	}
	h := windows.Handle(file.Fd())
	deadline := time.Now().Add(timeout)
	for {
		ol := new(windows.Overlapped)
		err = windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, ol)
		if err == nil {
			break
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) && !errors.Is(err, windows.ERROR_IO_PENDING) {
			_ = file.Close()
			return nil, fmt.Errorf("lock notification status file: %w", err)
		}
		if time.Now().After(deadline) {
			_ = file.Close()
			return nil, errStatusLockTimeout
		}
		time.Sleep(15 * time.Millisecond)
	}
	return func() {
		ol := new(windows.Overlapped)
		_ = windows.UnlockFileEx(h, 0, 1, 0, ol)
		_ = file.Close()
	}, nil
}
