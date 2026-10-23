//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// detachProcess puts the child in its own process group so a stop signal
// directed at this api-server's group (during a restore-triggered restart)
// can't terminate the restarter before it brings the stack back up.
func detachProcess(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// startDetached starts build() in its own process group.
func startDetached(build func() *exec.Cmd) (*exec.Cmd, error) {
	cmd := build()
	detachProcess(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}
