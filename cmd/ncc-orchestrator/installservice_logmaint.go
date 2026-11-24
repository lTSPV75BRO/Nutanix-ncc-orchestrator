package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

// Log maintenance timer for hosts set up with v2-install-service.
//
// deploy/cloud-init/ncc-v2.yaml used to write a timer that runs
// `doctor --fix --only-checks log-sizes` every 15 minutes, but a host installed
// with `v2-install-service` had no such timer, so its logs were only rotated
// when someone ran doctor by hand. Since 2.4.0 cloud-init leaves it to this.

const logMaintenanceUnitName = "ncc-log-maintenance"

func logMaintenanceServicePath() string {
	return filepath.Join(systemdUnitDir, logMaintenanceUnitName+".service")
}

func logMaintenanceTimerPath() string {
	return filepath.Join(systemdUnitDir, logMaintenanceUnitName+".timer")
}

func systemdLogMaintenanceServiceUnit(o installServiceOptions) string {
	script := fmt.Sprintf("%s doctor --install-dir %s --fix --only-checks log-sizes --json >> %s 2>&1 || true",
		systemdQuote(o.OrchestratorBin), systemdQuote(o.InstallDir), systemdQuote(path.Join(filepath.ToSlash(o.InstallDir), "logs", "doctor.log")))
	lines := []string{
		"[Unit]",
		"Description=NCC log size check and rotation",
		"",
		"[Service]",
		"Type=oneshot",
		"ExecStart=/bin/sh -c " + systemdQuote(script),
	}
	lines = append(lines, systemdHardening()...)
	return strings.Join(append(lines, ""), "\n")
}

func systemdLogMaintenanceTimerUnit() string {
	return strings.Join([]string{
		"[Unit]",
		"Description=Check NCC log sizes every 15 minutes",
		"",
		"[Timer]",
		"OnBootSec=5min",
		"OnUnitActiveSec=15min",
		"Persistent=true",
		"",
		"[Install]",
		"WantedBy=timers.target",
		"",
	}, "\n")
}

func installLogMaintenanceTimer(o installServiceOptions) error {
	if err := os.WriteFile(logMaintenanceServicePath(), []byte(systemdLogMaintenanceServiceUnit(o)), 0o644); err != nil {
		return fmt.Errorf("write log maintenance service: %w", err)
	}
	if err := os.WriteFile(logMaintenanceTimerPath(), []byte(systemdLogMaintenanceTimerUnit()), 0o644); err != nil {
		return fmt.Errorf("write log maintenance timer: %w", err)
	}
	if out, err := exec.Command("systemctl", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	name := logMaintenanceUnitName + ".timer"
	if out, err := exec.Command("systemctl", "enable", "--now", name).CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl enable --now %s: %v (%s)", name, err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed log maintenance timer %s (log size check every 15 minutes)\n", name)
	return nil
}

func uninstallLogMaintenanceTimer() {
	if systemctlAvailable() {
		_, _ = exec.Command("systemctl", "disable", "--now", logMaintenanceUnitName+".timer").CombinedOutput()
		_, _ = exec.Command("systemctl", "stop", logMaintenanceUnitName+".service").CombinedOutput()
	}
	for _, p := range []string{logMaintenanceTimerPath(), logMaintenanceServicePath()} {
		_ = os.Remove(p)
	}
}
