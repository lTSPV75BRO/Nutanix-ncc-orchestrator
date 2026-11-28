//go:build !windows

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// startDetachedProcess starts a process that outlives this one.
func startDetachedProcess(build func() *exec.Cmd) (*exec.Cmd, error) {
	cmd := build()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func windowsElevated() error { return errors.New("not Windows") }

// checkFolderOwner refuses a helper folder that another user prepared.
func checkFolderOwner(path string, st os.FileInfo) error {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok && int(sys.Uid) != os.Geteuid() {
		return fmt.Errorf("%s belongs to another user", path)
	}
	return nil
}
