package types

import (
	"os"
)

// OSFS implements the FS interface using the operating system's file system
type OSFS struct{}

// MkdirAll creates a directory and all necessary parent directories
func (OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// WriteFile writes data to a file
func (OSFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// ReadFile reads the contents of a file
func (OSFS) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// ReadDir reads the contents of a directory
func (OSFS) ReadDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}

// Create creates a new file
func (OSFS) Create(path string) (*os.File, error) {
	return os.Create(path)
}
