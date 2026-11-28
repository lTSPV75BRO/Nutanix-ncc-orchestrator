package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDetachedSystemdRestartScriptIsValidShell(t *testing.T) {
	for _, inService := range []bool{true, false} {
		script := detachedSystemdRestartScript("/root/it's a test", inService)
		if strings.Contains(script, "then;") {
			t.Fatalf("joining if/then with '; ' produced invalid shell:\n%s", script)
		}
		if !strings.Contains(script, "restart ncc-orchestrator.service") {
			t.Fatalf("missing systemd restart:\n%s", script)
		}
		out, err := exec.Command("sh", "-n", "-c", script).CombinedOutput()
		if err != nil {
			t.Fatalf("sh -n rejected restart script: %v\n%s\n%s", err, script, out)
		}
	}
}

// runRestartScript runs the script against stub commands and returns the calls
// they saw. With inCgroup, stopping the unit kills the script, as systemd does
// to every process of the service.
func runRestartScript(t *testing.T, installDir, script string, inCgroup bool) []string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell and PATH")
	}
	stubs := t.TempDir()
	calls := filepath.Join(stubs, "calls")
	kill := ""
	if inCgroup {
		kill = `case "$1" in stop|restart) kill -TERM $PPID; sleep 1;; esac`
	}
	write := func(p, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(stubs, "systemctl"), `echo "systemctl $*" >> '`+calls+"'\n"+kill)
	write(filepath.Join(stubs, "chcon"), `echo "chcon" >> '`+calls+`'; exit 1`)
	write(filepath.Join(installDir, "bin", "ncc-orchestrator"), `echo "orchestrator $1" >> '`+calls+`'`)
	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+stubs+":"+os.Getenv("PATH"))
	_ = cmd.Run() // killed in the cgroup case on the old script
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// REL-07: the script runs in the service's cgroup, so stopping the service
// killed it before it could start the service again.
func TestRestartScriptInServiceQueuesOneRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ncc install")
	calls := runRestartScript(t, dir, detachedSystemdRestartScript(dir, true), true)
	want := "chcon|systemctl --no-block restart ncc-orchestrator.service"
	if got := strings.Join(calls, "|"); got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
}

// A stack started by hand keeps the ports, so it is stopped before the unit
// starts.
func TestRestartScriptOutsideServiceStopsLeftovers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ncc install")
	calls := runRestartScript(t, dir, detachedSystemdRestartScript(dir, false), false)
	want := "chcon|systemctl stop ncc-orchestrator.service|orchestrator v2-stop|systemctl restart ncc-orchestrator.service"
	if got := strings.Join(calls, "|"); got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
}

func TestCgroupInStackService(t *testing.T) {
	for cgroup, want := range map[string]bool{
		"0::/system.slice/ncc-orchestrator.service\n":                                        true,
		"0::/system.slice/ncc-orchestrator.service/child\n":                                  true,
		"12:pids:/system.slice\n1:name=systemd:/system.slice/ncc-orchestrator.service\n0::/": true,
		"0::/system.slice/ncc-orchestrator-apply-update.service\n":                           false,
		"0::/system.slice/my-ncc-orchestrator.service\n":                                     false,
		"0::/user.slice/user-0.slice/session-3.scope\n":                                      false,
		"": false,
	} {
		if got := cgroupInStackService(cgroup); got != want {
			t.Errorf("cgroupInStackService(%q) = %v, want %v", cgroup, got, want)
		}
	}
}
