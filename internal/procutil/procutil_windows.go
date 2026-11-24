//go:build windows

package procutil

import (
	"errors"
	"fmt"
	"os"

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

func terminate(pid int) error {
	h, err := openToTerminate(pid)
	if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		// Windows does not let even an Administrator end a process of SYSTEM,
		// unless the Administrator uses the privilege to debug any process.
		restore, privErr := enableDebugPrivilege()
		if privErr != nil {
			return fmt.Errorf("%w (the process belongs to another account; run as Administrator)", err)
		}
		defer restore()
		h, err = openToTerminate(pid)
	}
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return os.ErrProcessDone
	}
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(h) }()
	if err := windows.TerminateProcess(h, 1); err != nil {
		if !exists(pid) {
			return os.ErrProcessDone
		}
		return err
	}
	return nil
}

func openToTerminate(pid int) (windows.Handle, error) {
	return windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
}

// enableDebugPrivilege turns on SeDebugPrivilege, which an elevated
// Administrator holds switched off. The returned function puts it back.
func enableDebugPrivilege() (func(), error) {
	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &token); err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString("SeDebugPrivilege")
	if err != nil {
		_ = token.Close()
		return nil, err
	}
	state := windows.Tokenprivileges{PrivilegeCount: 1}
	state.Privileges[0].Attributes = windows.SE_PRIVILEGE_ENABLED
	if err := windows.LookupPrivilegeValue(nil, name, &state.Privileges[0].Luid); err != nil {
		_ = token.Close()
		return nil, err
	}
	if err := windows.AdjustTokenPrivileges(token, false, &state, 0, nil, nil); err != nil {
		_ = token.Close()
		return nil, err
	}
	// The call also succeeds when the token does not hold the privilege.
	if !token.IsElevated() {
		_ = token.Close()
		return nil, windows.ERROR_PRIVILEGE_NOT_HELD
	}
	return func() {
		state.Privileges[0].Attributes = 0
		_ = windows.AdjustTokenPrivileges(token, false, &state, 0, nil, nil)
		_ = token.Close()
	}, nil
}
