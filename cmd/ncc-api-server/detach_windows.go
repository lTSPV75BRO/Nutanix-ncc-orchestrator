//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// detachProcess starts the child in a new process group on Windows so a
// console/stop signal sent to this api-server doesn't propagate to the
// restarter that brings the stack back up.
func detachProcess(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}
