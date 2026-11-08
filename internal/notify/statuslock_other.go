//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package notify

import (
	"os"
	"time"
)

// lockStatusFile is a best-effort fallback for platforms without flock or
// LockFileEx: it relies on O_EXCL creation of a sibling ".excl" marker.
func lockStatusFile(path string, timeout time.Duration) (func(), error) {
	marker := path + ".excl"
	deadline := time.Now().Add(timeout)
	for {
		f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_ = f.Close()
			return func() { _ = os.Remove(marker) }, nil
		}
		if time.Now().After(deadline) {
			return nil, errStatusLockTimeout
		}
		time.Sleep(15 * time.Millisecond)
	}
}
