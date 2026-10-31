package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"goncc/internal/procutil"
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
		return windowsImageIdentity(pid, expected...)
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

// windowsImageIdentity matches pid's executable name against the expected
// tokens. Windows has no permission-safe way to read another process's argv,
// but the image name is enough to tell a stack binary from an unrelated
// process that reused the pid (Windows recycles pids quickly, so after a
// reboot a stale run/*.pid usually points at something else). Identities
// that name a v2 helper loop (v2-api-supervisor, v2-ui-supervisor) run under
// a shell whose image says nothing, so they stay unknown, as before.
func windowsImageIdentity(pid int, expected ...string) (known bool, matches bool) {
	stackBinary := false
	for _, want := range expected {
		w := strings.ToLower(strings.TrimSuffix(filepath.Base(strings.TrimSpace(want)), ".exe"))
		if strings.HasSuffix(w, "-supervisor") {
			return false, true
		}
		if w == "ncc-orchestrator" || w == "ncc-api-server" || w == "ncc-ui-server" {
			stackBinary = true
		}
	}
	if !stackBinary {
		return false, true
	}
	image, err := procutil.ImagePath(pid)
	if err != nil || strings.TrimSpace(image) == "" {
		return false, true
	}
	got := strings.ToLower(strings.TrimSuffix(filepath.Base(image), filepath.Ext(image)))
	for _, want := range expected {
		w := strings.ToLower(strings.TrimSuffix(filepath.Base(strings.TrimSpace(want)), ".exe"))
		if w != "" && w == got {
			return true, true
		}
	}
	return true, false
}

func processIsExpected(pid int, expected ...string) bool {
	known, matches := processIdentityMatches(pid, expected...)
	return processIsAlive(pid) && (!known || matches)
}
