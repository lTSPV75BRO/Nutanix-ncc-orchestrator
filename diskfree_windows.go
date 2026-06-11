//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

// diskFreeBytes returns the bytes available to the caller on the volume backing
// path, via the Win32 GetDiskFreeSpaceExW API. ok is false when the figure
// could not be determined.
func diskFreeBytes(path string) (free uint64, ok bool) {
	kernel32, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		return 0, false
	}
	proc, err := kernel32.FindProc("GetDiskFreeSpaceExW")
	if err != nil {
		return 0, false
	}
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, false
	}
	var freeBytesAvailable uint64
	r1, _, _ := proc.Call(
		uintptr(unsafe.Pointer(p)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)
	if r1 == 0 {
		return 0, false
	}
	return freeBytesAvailable, true
}
