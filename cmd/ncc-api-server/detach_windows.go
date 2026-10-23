//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

const (
	winDetachedProcess        = 0x00000008 // DETACHED_PROCESS: no inherited console
	winCreateBreakawayFromJob = 0x01000000 // CREATE_BREAKAWAY_FROM_JOB
)

// detachProcess starts the child in a new process group without the parent's
// console, so a console/stop signal or console close for this api-server
// doesn't take the restarter down with it.
func detachProcess(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP | winDetachedProcess
	cmd.SysProcAttr.HideWindow = true
}

// startDetached starts a restarter that must outlive this process. When the
// api-server runs inside a Windows job object that kills its members on close
// (service wrappers, Task Scheduler), a plain child is terminated together
// with the api-server it is supposed to restart. Try to break away from the
// job first; if the job forbids breakaway, fall back to a normal detached
// start (build must return a fresh *exec.Cmd each call).
func startDetached(build func() *exec.Cmd) (*exec.Cmd, error) {
	cmd := build()
	detachProcess(cmd)
	cmd.SysProcAttr.CreationFlags |= winCreateBreakawayFromJob
	if err := cmd.Start(); err == nil {
		return cmd, nil
	}
	cmd = build()
	detachProcess(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}
