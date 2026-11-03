package hostinfo

import (
	"errors"
	"runtime"

	"golang.org/x/sys/unix"
)

const quarantineAttr = "com.apple.quarantine"

// nativeArch detects Rosetta 2: an amd64 build on Apple silicon runs
// translated, noticeably slower than the arm64 build.
func nativeArch() (string, bool, bool) {
	translated, err := unix.SysctlUint32("sysctl.proc_translated")
	if err == nil && translated == 1 {
		return "arm64", true, true
	}
	return runtime.GOARCH, false, true
}

func systemMemory() Memory {
	var m Memory
	if total, err := unix.SysctlUint64("hw.memsize"); err == nil {
		m.Total = total
	}
	return m
}

func quarantined(path string) bool {
	_, err := unix.Getxattr(path, quarantineAttr, nil)
	return err == nil
}

func clearQuarantine(path string) error {
	err := unix.Removexattr(path, quarantineAttr)
	if errors.Is(err, unix.ENOATTR) {
		return nil
	}
	return err
}
