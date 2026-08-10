package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// This file installs the v2 stack *supervisor* (`ncc-orchestrator v2-supervise`)
// as a boot-persistent OS service so the whole stack survives an OS restart:
//
//   - Linux  : a Type=simple systemd service (Restart=always). systemd keeps
//              the supervisor alive across reboots and restarts it if it dies;
//              the supervisor keeps the api/ui children alive.
//   - Windows: a Task Scheduler task triggered "at system startup" (ONSTART)
//              running the supervisor as SYSTEM (there is no systemd, and this
//              is the lightest cross-version equivalent without bundling NSSM).
//   - macOS  : a launchd LaunchDaemon (RunAtLoad + KeepAlive).
//
// This replaces the hand-rolled systemd unit and unifies boot persistence with
// the native supervisor across platforms.

type installServiceOptions struct {
	InstallDir      string
	ServiceName     string
	OrchestratorBin string
	Now             bool // start/enable immediately (Linux: enable --now; macOS: load -w)
	PrintOnly       bool
}

func (o *installServiceOptions) resolve() error {
	o.InstallDir = strings.TrimSpace(o.InstallDir)
	if o.InstallDir == "" {
		o.InstallDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(o.InstallDir); err == nil {
		o.InstallDir = abs
	}
	o.ServiceName = strings.TrimSpace(o.ServiceName)
	if o.ServiceName == "" {
		o.ServiceName = "ncc-orchestrator"
	}
	o.OrchestratorBin = strings.TrimSpace(o.OrchestratorBin)
	if o.OrchestratorBin == "" {
		if self, err := os.Executable(); err == nil {
			o.OrchestratorBin = self
		} else {
			o.OrchestratorBin = binaryPathInInstallDir(o.InstallDir, "ncc-orchestrator")
		}
	}
	if abs, err := filepath.Abs(o.OrchestratorBin); err == nil {
		o.OrchestratorBin = abs
	}
	return nil
}

func systemdSupervisorUnit(o installServiceOptions) string {
	return strings.Join([]string{
		"[Unit]",
		"Description=NCC Orchestrator v2 stack supervisor (API + UI)",
		"Documentation=https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
		"After=network-online.target",
		"Wants=network-online.target",
		"",
		"[Service]",
		"# A single long-lived native supervisor owns the api + ui children and",
		"# keeps them alive (liveness + HTTP --health-check probes, exponential",
		"# backoff, cooldown-and-resume). Running it Type=simple gives reboot",
		"# persistence; on start it replays saved settings from .ncc-v2-start.json.",
		"Type=simple",
		"WorkingDirectory=" + o.InstallDir,
		fmt.Sprintf("ExecStart=%s v2-supervise --install-dir %s", o.OrchestratorBin, o.InstallDir),
		"Restart=always",
		"RestartSec=3",
		"TimeoutStopSec=30",
		"KillMode=control-group",
		"",
		"[Install]",
		"WantedBy=multi-user.target",
		"",
	}, "\n")
}

func runV2InstallService(o installServiceOptions) error {
	if err := o.resolve(); err != nil {
		return err
	}
	switch runtime.GOOS {
	case "linux":
		return installSupervisorSystemd(o)
	case "windows":
		return installSupervisorWindows(o)
	case "darwin":
		return installSupervisorLaunchd(o)
	default:
		return fmt.Errorf("v2-install-service is not supported on %s; run `ncc-orchestrator v2-supervise --install-dir %s` under your platform's service manager", runtime.GOOS, o.InstallDir)
	}
}

func runV2UninstallService(o installServiceOptions) error {
	if err := o.resolve(); err != nil {
		return err
	}
	switch runtime.GOOS {
	case "linux":
		return uninstallSupervisorSystemd(o)
	case "windows":
		return uninstallSupervisorWindows(o)
	case "darwin":
		return uninstallSupervisorLaunchd(o)
	default:
		return fmt.Errorf("v2-uninstall-service is not supported on %s", runtime.GOOS)
	}
}

// ---- Linux / systemd ----

func installSupervisorSystemd(o installServiceOptions) error {
	unit := systemdSupervisorUnit(o)
	unitPath := filepath.Join(systemdUnitDir, o.ServiceName+".service")
	if o.PrintOnly {
		fmt.Printf("Would write %s:\n\n%s\n", unitPath, unit)
		fmt.Printf("Then run:\n  systemctl daemon-reload\n  systemctl enable%s %s.service\n",
			ifThen(o.Now, " --now", ""), o.ServiceName)
		return nil
	}
	if !systemctlAvailable() {
		return fmt.Errorf("systemd is not available (systemctl not found or systemd is not the init system)")
	}
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", unitPath, err)
	}
	// SELinux: a binary under a home dir (e.g. /root) is typically labeled
	// admin_home_t by policy default. The init_t domain systemd execs units
	// in is denied from executing admin_home_t content — and on a targeted
	// policy that specific denial is usually "dontaudited", so it silently
	// fails as EACCES (systemd reports "status=203/EXEC") without ever
	// appearing in `ausearch -m avc`. `restorecon` alone does NOT fix this:
	// it only resets a file to the policy's *default* context for its path,
	// which for anything under a home directory is admin_home_t, not bin_t.
	// Force bin_t directly with `chcon`, and best-effort persist the rule
	// with `semanage fcontext` so a future relabel/restorecon (e.g. from a
	// security scan or `fixfiles`) doesn't silently revert it. Relabel the
	// whole bin/ dir, not just the orchestrator binary: the supervisor also
	// execs ncc-api-server/ncc-ui-server as children from the same dir.
	binDir := filepath.Dir(o.OrchestratorBin)
	if _, err := exec.LookPath("chcon"); err == nil {
		_, _ = exec.Command("chcon", "-R", "-t", "bin_t", binDir).CombinedOutput()
	}
	if _, err := exec.LookPath("semanage"); err == nil {
		pattern := binDir + "(/.*)?"
		if out, err := exec.Command("semanage", "fcontext", "-a", "-t", "bin_t", pattern).CombinedOutput(); err != nil {
			_, _ = exec.Command("semanage", "fcontext", "-m", "-t", "bin_t", pattern).CombinedOutput()
			_ = out
		}
	}
	if _, err := exec.LookPath("restorecon"); err == nil {
		_, _ = exec.Command("restorecon", "-R", "-v", binDir).CombinedOutput()
	}
	if out, err := exec.Command("systemctl", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	enableArgs := []string{"enable"}
	if o.Now {
		enableArgs = append(enableArgs, "--now")
	}
	enableArgs = append(enableArgs, o.ServiceName+".service")
	if out, err := exec.Command("systemctl", enableArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl %s: %v (%s)", strings.Join(enableArgs, " "), err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed systemd service %s.service (ExecStart=%s v2-supervise --install-dir %s)\n", o.ServiceName, o.OrchestratorBin, o.InstallDir)
	fmt.Printf("Enabled at boot%s. Manage with: systemctl status|restart|stop %s\n", ifThen(o.Now, " and started now", ""), o.ServiceName)
	return nil
}

func uninstallSupervisorSystemd(o installServiceOptions) error {
	unitPath := filepath.Join(systemdUnitDir, o.ServiceName+".service")
	if o.PrintOnly {
		fmt.Printf("Would run:\n  systemctl disable --now %s.service\n  rm -f %s\n  systemctl daemon-reload\n", o.ServiceName, unitPath)
		return nil
	}
	if systemctlAvailable() {
		_, _ = exec.Command("systemctl", "disable", "--now", o.ServiceName+".service").CombinedOutput()
	}
	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", unitPath, err)
	}
	if systemctlAvailable() {
		_, _ = exec.Command("systemctl", "daemon-reload").CombinedOutput()
	}
	fmt.Printf("Removed systemd service %s.service\n", o.ServiceName)
	return nil
}

// ---- Windows / Task Scheduler (ONSTART) ----

func windowsSupervisorTaskCmd(o installServiceOptions) string {
	return fmt.Sprintf("\"%s\" v2-supervise --install-dir \"%s\"", o.OrchestratorBin, o.InstallDir)
}

func installSupervisorWindows(o installServiceOptions) error {
	tr := windowsSupervisorTaskCmd(o)
	args := []string{
		"/Create",
		"/TN", o.ServiceName,
		"/TR", tr,
		"/SC", "ONSTART",
		"/RU", "SYSTEM",
		"/RL", "HIGHEST",
		"/F",
	}
	if o.PrintOnly {
		fmt.Printf("Would run:\n  schtasks %s\n", strings.Join(args, " "))
		return nil
	}
	if out, err := exec.Command("schtasks", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("create scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Created Windows Scheduled Task %q (trigger: at system startup, run as SYSTEM)\n", o.ServiceName)
	fmt.Printf("Command: %s\n", tr)
	if o.Now {
		if out, err := exec.Command("schtasks", "/Run", "/TN", o.ServiceName).CombinedOutput(); err != nil {
			return fmt.Errorf("start task now: %v (%s)", err, strings.TrimSpace(string(out)))
		}
		fmt.Printf("Started task %q now.\n", o.ServiceName)
	}
	return nil
}

func uninstallSupervisorWindows(o installServiceOptions) error {
	if o.PrintOnly {
		fmt.Printf("Would run:\n  schtasks /End /TN %q\n  schtasks /Delete /TN %q /F\n", o.ServiceName, o.ServiceName)
		return nil
	}
	_, _ = exec.Command("schtasks", "/End", "/TN", o.ServiceName).CombinedOutput()
	if out, err := exec.Command("schtasks", "/Delete", "/TN", o.ServiceName, "/F").CombinedOutput(); err != nil {
		return fmt.Errorf("delete scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed Windows Scheduled Task %q\n", o.ServiceName)
	return nil
}

// ---- macOS / launchd ----

func launchdLabel(serviceName string) string {
	return "io.github.ncc-orchestrator." + sanitizeSystemdName(serviceName)
}

func launchdPlistPath(serviceName string) string {
	return filepath.Join("/Library/LaunchDaemons", launchdLabel(serviceName)+".plist")
}

func launchdSupervisorPlist(o installServiceOptions) string {
	logDir := filepath.Join(o.InstallDir, "logs")
	return strings.Join([]string{
		`<?xml version="1.0" encoding="UTF-8"?>`,
		`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">`,
		`<plist version="1.0">`,
		`<dict>`,
		`  <key>Label</key>`,
		`  <string>` + launchdLabel(o.ServiceName) + `</string>`,
		`  <key>ProgramArguments</key>`,
		`  <array>`,
		`    <string>` + o.OrchestratorBin + `</string>`,
		`    <string>v2-supervise</string>`,
		`    <string>--install-dir</string>`,
		`    <string>` + o.InstallDir + `</string>`,
		`  </array>`,
		`  <key>WorkingDirectory</key>`,
		`  <string>` + o.InstallDir + `</string>`,
		`  <key>RunAtLoad</key>`,
		`  <true/>`,
		`  <key>KeepAlive</key>`,
		`  <true/>`,
		`  <key>StandardOutPath</key>`,
		`  <string>` + filepath.Join(logDir, "v2-supervisor.out.log") + `</string>`,
		`  <key>StandardErrorPath</key>`,
		`  <string>` + filepath.Join(logDir, "v2-supervisor.err.log") + `</string>`,
		`</dict>`,
		`</plist>`,
		``,
	}, "\n")
}

func installSupervisorLaunchd(o installServiceOptions) error {
	plist := launchdSupervisorPlist(o)
	plistPath := launchdPlistPath(o.ServiceName)
	if o.PrintOnly {
		fmt.Printf("Would write %s:\n\n%s\n", plistPath, plist)
		fmt.Printf("Then run:\n  launchctl load -w %s\n", plistPath)
		return nil
	}
	if err := os.MkdirAll(filepath.Join(o.InstallDir, "logs"), 0o755); err != nil {
		return fmt.Errorf("prepare log dir: %w", err)
	}
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil {
		return fmt.Errorf("write %s: %w (try sudo)", plistPath, err)
	}
	if out, err := exec.Command("launchctl", "load", "-w", plistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl load: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed launchd daemon %s (%s)\n", launchdLabel(o.ServiceName), plistPath)
	return nil
}

func uninstallSupervisorLaunchd(o installServiceOptions) error {
	plistPath := launchdPlistPath(o.ServiceName)
	if o.PrintOnly {
		fmt.Printf("Would run:\n  launchctl unload -w %s\n  rm -f %s\n", plistPath, plistPath)
		return nil
	}
	_, _ = exec.Command("launchctl", "unload", "-w", plistPath).CombinedOutput()
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", plistPath, err)
	}
	fmt.Printf("Removed launchd daemon %s\n", launchdLabel(o.ServiceName))
	return nil
}

func ifThen(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}
