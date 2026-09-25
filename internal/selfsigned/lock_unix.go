//go:build unix

package selfsigned

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func withDirLock(dir string, fn func() error) error {
	path := filepath.Join(dir, lockName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open TLS lock: %w", err)
	}
	defer file.Close()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("TLS lock: %w", err)
	}
	defer func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN) }()
	return fn()
}
