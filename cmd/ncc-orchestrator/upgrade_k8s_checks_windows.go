//go:build windows

package main

import "errors"

// freeBytes: the checks run in Linux containers; Windows has no use for them.
func freeBytes(string) (uint64, error) {
	return 0, errors.New("free space is only read on Linux and macOS")
}
