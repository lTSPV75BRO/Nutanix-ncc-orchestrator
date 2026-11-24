//go:build !windows

package winacl

import "os"

// Supported is false where POSIX modes already do this job.
const Supported = false

// Check is a no-op on this platform.
func Check(string, Mode) ([]string, error) { return nil, nil }

// CheckTree is a no-op on this platform.
func CheckTree(string, Mode) ([]string, []error) { return nil, nil }

// Restrict is a no-op on this platform.
func Restrict(string, Mode) (bool, error) { return false, nil }

// RestrictTree is a no-op on this platform.
func RestrictTree(string, Mode) ([]string, []error) { return nil, nil }

// WriteFile is os.WriteFile on this platform.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// Rename is os.Rename on this platform.
func Rename(tmp, dst string) error { return os.Rename(tmp, dst) }
