package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestDetachedSystemdRestartScriptIsValidShell(t *testing.T) {
	script := detachedSystemdRestartScript("/root/test")
	if strings.Contains(script, "then;") {
		t.Fatalf("joining if/then with '; ' produced invalid shell:\n%s", script)
	}
	if !strings.Contains(script, "systemctl restart ncc-orchestrator.service") {
		t.Fatalf("missing systemd restart:\n%s", script)
	}
	out, err := exec.Command("sh", "-n", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("sh -n rejected restart script: %v\n%s\n%s", err, script, out)
	}
}
