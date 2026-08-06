package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// processIdentityMatches checks whether a live PID is running one of the
// expected commands. Linux exposes the complete argv through /proc. Other
// supported platforms do not provide a portable, permission-safe equivalent,
// so callers must treat an unknown identity conservatively.
func processIdentityMatches(pid int, expected ...string) (known bool, matches bool) {
	if pid <= 0 || !processIsAlive(pid) {
		return true, false
	}
	if runtime.GOOS == "windows" {
		return false, true
	}
	var raw string
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile(filepath.Join("/proc", fmt.Sprintf("%d", pid), "cmdline"))
		if err != nil {
			return false, true
		}
		raw = strings.ReplaceAll(string(data), "\x00", " ")
	} else {
		out, err := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "command=").Output()
		if err != nil {
			return false, true
		}
		raw = string(out)
	}
	if strings.TrimSpace(raw) == "" {
		return false, true
	}
	args := strings.Fields(raw)
	for _, want := range expected {
		want = strings.TrimSpace(want)
		if want == "" {
			continue
		}
		wantBase := filepath.Base(want)
		for _, arg := range args {
			if arg == want || filepath.Base(arg) == wantBase {
				return true, true
			}
		}
	}
	return true, false
}

func processIsExpected(pid int, expected ...string) bool {
	known, matches := processIdentityMatches(pid, expected...)
	return processIsAlive(pid) && (!known || matches)
}
