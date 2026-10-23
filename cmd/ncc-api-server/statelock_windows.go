//go:build windows

package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// lockStateFile takes an exclusive LockFileEx lock on path, retrying until
// timeout, so read-modify-write cycles on shared state files are serialized
// across processes on Windows hosts.
func lockStateFile(path string, timeout time.Duration) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
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
			return nil, fmt.Errorf("lock state file: %w", err)
		}
		if time.Now().After(deadline) {
			_ = file.Close()
			return nil, errStateLockTimeout
		}
		time.Sleep(15 * time.Millisecond)
	}
	return func() {
		ol := new(windows.Overlapped)
		_ = windows.UnlockFileEx(h, 0, 1, 0, ol)
		_ = file.Close()
	}, nil
}
