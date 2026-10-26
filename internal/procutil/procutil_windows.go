//go:build windows

package procutil

import (
	"errors"

	"golang.org/x/sys/windows"
)

func exists(pid int) bool {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		// Protected/system processes refuse the handle but do exist; any other
		// error (ERROR_INVALID_PARAMETER) means there is no such process.
		return errors.Is(err, windows.ERROR_ACCESS_DENIED)
	}
	defer func() { _ = windows.CloseHandle(h) }()
	// A handle to an exited process stays valid while anyone holds it, so the
	// open alone is not proof of life. A zero-timeout wait on the process
	// object times out only while the process is still running; unlike
	// GetExitCodeProcess it cannot confuse a real exit code of 259 with
	// STILL_ACTIVE.
	ev, err := windows.WaitForSingleObject(h, 0)
	if err != nil {
		return true
	}
	return ev == uint32(windows.WAIT_TIMEOUT)
}

func imagePath(pid int) (string, error) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer func() { _ = windows.CloseHandle(h) }()
	buf := make([]uint16, windows.MAX_LONG_PATH)
	n := uint32(len(buf))
	if err := windows.QueryFullProcessImageName(h, 0, &buf[0], &n); err != nil {
		return "", err
	}
	return windows.UTF16ToString(buf[:n]), nil
}
