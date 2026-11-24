//go:build windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

// startDetachedProcess starts a process that outlives this one, also when this
// one runs in a job object that ends its members (Task Scheduler, a console).
func startDetachedProcess(build func() *exec.Cmd) (*exec.Cmd, error) {
	const flags = windows.CREATE_NEW_PROCESS_GROUP | windows.DETACHED_PROCESS
	var err error
	for _, extra := range []uint32{windows.CREATE_BREAKAWAY_FROM_JOB, 0} {
		cmd := build()
		cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: flags | extra, HideWindow: true}
		if err = cmd.Start(); err == nil {
			return cmd, nil
		}
	}
	return nil, err
}

// windowsElevated: may this process change tasks that run as SYSTEM?
func windowsElevated() error {
	if windows.GetCurrentProcessToken().IsElevated() {
		return nil
	}
	return errors.New("changing the scheduled task needs Administrator")
}

// checkFolderOwner: on Windows the folder's access list is what protects it.
func checkFolderOwner(string, os.FileInfo) error { return nil }
