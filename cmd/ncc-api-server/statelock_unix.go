//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// lockStateFile takes a blocking (bounded by timeout) exclusive advisory lock
// on path so read-modify-write cycles on shared state files are serialized
// across processes — including multiple API replicas sharing a ReadWriteMany
// PVC in Kubernetes. The in-process mutexes alone only protect one replica.
func lockStateFile(path string, timeout time.Duration) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
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
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, nil
}
