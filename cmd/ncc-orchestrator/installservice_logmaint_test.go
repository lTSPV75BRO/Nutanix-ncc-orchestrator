package main

import (
	"strings"
	"testing"
)

// Regression (environment test ENV-08): a host installed with
// v2-install-service had no log maintenance timer; only the cloud-init path
// wrote one.
func TestLogMaintenanceUnits(t *testing.T) {
	o := installServiceOptions{InstallDir: "/opt/ncc v2", OrchestratorBin: "/opt/ncc v2/bin/ncc-orchestrator"}

	svc := systemdLogMaintenanceServiceUnit(o)
	for _, want := range []string{
		"Type=oneshot",
		"ExecStart=/bin/sh -c ",
		"doctor --install-dir ",
		"--fix --only-checks log-sizes --json",
		"/opt/ncc v2/logs/doctor.log",
		"|| true",
	} {
		if !strings.Contains(svc, want) {
			t.Errorf("service unit lacks %q:\n%s", want, svc)
		}
	}
	// Paths with spaces must be shell-quoted inside the sh -c script.
	if !strings.Contains(svc, "'/opt/ncc v2/bin/ncc-orchestrator'") && !strings.Contains(svc, `'"'"'/opt/ncc v2/bin/ncc-orchestrator'"'"'`) {
		t.Errorf("binary path is not quoted:\n%s", svc)
	}

	timer := systemdLogMaintenanceTimerUnit()
	for _, want := range []string{"OnBootSec=5min", "OnUnitActiveSec=15min", "Persistent=true", "WantedBy=timers.target"} {
		if !strings.Contains(timer, want) {
			t.Errorf("timer unit lacks %q:\n%s", want, timer)
		}
	}
}
