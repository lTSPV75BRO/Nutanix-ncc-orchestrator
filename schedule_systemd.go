package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// This file adds a systemd-timer scheduler backend alongside the existing cron
// (Linux/macOS) and schtasks (Windows) backends. A systemd timer is a better
// fit than cron on a systemd host: the run executes with an explicit
// WorkingDirectory and environment (no cron cwd/PATH surprises), output is
// captured per-run, overlap is prevented for free (systemd will not start a
// second activation of a Type=oneshot service while the previous run is still
// active), and Persistent=true replays a run missed while the box was off.
//
// A schedule is two unit files written under /etc/systemd/system:
//
//	ncc-sched-<task>.service  (Type=oneshot; runs the scan via a small wrapper
//	                           script so logging/redirection works on every
//	                           systemd version, incl. pre-240 without append:)
//	ncc-sched-<task>.timer    (OnCalendar=<derived>; Persistent=true)
//
// Both carry the same `# ncc-orchestrator:<task>` marker comment that the cron
// backend uses, so detection (api-server) and removal are marker-scoped and
// uniform across backends.

const systemdUnitDir = "/etc/systemd/system"

// systemctlAvailable reports whether a usable systemctl + running systemd is
// present. We require both the binary and a live system manager (so this is
// false inside containers/CI without systemd as PID 1).
func systemctlAvailable() bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	// `systemctl is-system-running` returns non-zero for degraded/offline, but
	// any parseable answer other than a hard failure means systemd is the
	// manager. Treat an exec error (no system bus) as unavailable.
	out, err := exec.Command("systemctl", "is-system-running").CombinedOutput()
	if err == nil {
		return true
	}
	state := strings.TrimSpace(string(out))
	// "degraded", "starting", "maintenance", "stopping" are still systemd.
	switch state {
	case "degraded", "starting", "maintenance", "stopping", "running":
		return true
	}
	return false
}

// sanitizeSystemdName maps a task name to the safe subset for a systemd unit
// name. Task names are already validated to [A-Za-z0-9._:-]; ':' is replaced
// with '-' since it is special in some systemd contexts.
func sanitizeSystemdName(taskName string) string {
	name := strings.TrimSpace(taskName)
	if name == "" {
		name = "default"
	}
	var b strings.Builder
	for _, ch := range name {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9',
			ch == '_' || ch == '.' || ch == '-':
			b.WriteRune(ch)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// systemdUnitBase returns the unit base name (without .service/.timer suffix).
func systemdUnitBase(taskName string) string {
	return "ncc-sched-" + sanitizeSystemdName(taskName)
}

// cronFieldToCalendar converts one cron field to its systemd OnCalendar
// equivalent. start is the field's minimum (0 for minute/hour, 1 for
// day-of-month/month) so a `*/n` step starts where cron would.
func cronFieldToCalendar(field string, start int) string {
	f := strings.TrimSpace(field)
	if f == "" || f == "*" {
		return "*"
	}
	if strings.HasPrefix(f, "*/") {
		return fmt.Sprintf("%d/%s", start, strings.TrimPrefix(f, "*/"))
	}
	// Numbers and comma lists (e.g. "0,30") pass through unchanged; systemd
	// understands the same syntax.
	return f
}

// cronDowNames maps cron's 3-letter day-of-week names to 0..6 (Sun=0..Sat=6).
var cronDowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// systemdDowName renders 0..6 (Sun=0..Sat=6) as the systemd weekday name.
var systemdDowName = [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}

// parseCronDowValue maps a single cron day-of-week value (number 0-7 or a
// 3-letter name like "mon"/"SUN") to 0..6 (Sun=0..Sat=6). cron's 7 and 0 both
// mean Sunday.
func parseCronDowValue(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty day-of-week value")
	}
	if n, err := strconv.Atoi(s); err == nil {
		if n < 0 || n > 7 {
			return 0, fmt.Errorf("day-of-week number out of range (0-7): %d", n)
		}
		return n % 7, nil // 7 -> 0 (Sunday)
	}
	key := strings.ToLower(s)
	if len(key) > 3 {
		key = key[:3]
	}
	if v, ok := cronDowNames[key]; ok {
		return v, nil
	}
	return 0, fmt.Errorf("invalid day-of-week value: %q", s)
}

// cronDowToSystemd converts a cron day-of-week field to a systemd OnCalendar
// weekday prefix (e.g. cron "1-5" -> "Mon,Tue,Wed,Thu,Fri"). It returns
// restricted=false for "*" (every day — no weekday prefix). Numbers, names,
// comma lists, ranges (including wrap-around like "5-0" = Fri..Sun) and steps
// are all supported by enumerating the matching weekday set, which sidesteps
// the cron (Sun=0..Sat=6, 7=Sun) vs systemd (Mon..Sun) ordering mismatch that
// makes a naive textual translation run on the wrong days.
func cronDowToSystemd(field string) (spec string, restricted bool, err error) {
	f := strings.TrimSpace(field)
	if f == "" || f == "*" {
		return "", false, nil
	}
	var present [7]bool
	for _, tok := range strings.Split(f, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			return "", false, fmt.Errorf("empty day-of-week token in %q", field)
		}
		step := 1
		base := tok
		if slash := strings.IndexByte(tok, '/'); slash >= 0 {
			base = strings.TrimSpace(tok[:slash])
			n, serr := strconv.Atoi(strings.TrimSpace(tok[slash+1:]))
			if serr != nil || n <= 0 {
				return "", false, fmt.Errorf("invalid day-of-week step in %q", tok)
			}
			step = n
			if base == "" || base == "*" {
				base = "0-6"
			}
		}
		var start, end int
		if dash := strings.IndexByte(base, '-'); dash >= 0 {
			a, aerr := parseCronDowValue(base[:dash])
			if aerr != nil {
				return "", false, aerr
			}
			b, berr := parseCronDowValue(base[dash+1:])
			if berr != nil {
				return "", false, berr
			}
			start, end = a, b
		} else {
			v, verr := parseCronDowValue(base)
			if verr != nil {
				return "", false, verr
			}
			start, end = v, v
		}
		// Enumerate start..end inclusive, wrapping through Sat->Sun so a range
		// like 5-0 (Fri..Sun) still resolves, applying the step.
		span := (end - start + 7) % 7
		for i := 0; i <= span; i += step {
			present[(start+i)%7] = true
		}
	}
	allDays := true
	for _, p := range present {
		if !p {
			allDays = false
			break
		}
	}
	if allDays {
		return "", false, nil // every day -> no weekday restriction
	}
	order := []int{1, 2, 3, 4, 5, 6, 0} // systemd lists weekdays Mon..Sun
	var names []string
	for _, idx := range order {
		if present[idx] {
			names = append(names, systemdDowName[idx])
		}
	}
	return strings.Join(names, ","), true, nil
}

// cronToOnCalendar translates a 5-field cron expression to a systemd
// OnCalendar specification, including day-of-week fields (mapped by
// cronDowToSystemd, which enumerates weekdays to avoid the cron/systemd
// numbering mismatch).
func cronToOnCalendar(spec string) (string, error) {
	fields := strings.Fields(strings.TrimSpace(spec))
	if len(fields) != 5 {
		return "", fmt.Errorf("cron expression must have 5 fields, got %d", len(fields))
	}
	minute := cronFieldToCalendar(fields[0], 0)
	hour := cronFieldToCalendar(fields[1], 0)
	dom := cronFieldToCalendar(fields[2], 1)
	mon := cronFieldToCalendar(fields[3], 1)
	dowSpec, dowRestricted, err := cronDowToSystemd(fields[4])
	if err != nil {
		return "", fmt.Errorf("day-of-week: %w", err)
	}
	// cron treats a restricted day-of-month AND a restricted day-of-week as an
	// OR (the run fires if either matches); systemd ANDs the two. Refuse the
	// ambiguous combination rather than silently change the schedule's meaning.
	domRestricted := strings.TrimSpace(fields[2]) != "*"
	if dowRestricted && domRestricted {
		return "", errors.New("cannot combine a day-of-month and a day-of-week restriction for a systemd timer (cron ORs them, systemd ANDs them); restrict only one")
	}
	cal := fmt.Sprintf("*-%s-%s %s:%s:00", mon, dom, hour, minute)
	if dowRestricted {
		// OnCalendar form with a weekday prefix: "Mon,Wed,Fri *-*-* HH:MM:SS".
		cal = dowSpec + " " + cal
	}
	return cal, nil
}

// onCalendarFromSchedule derives the OnCalendar spec from an explicit cron
// expression (preferred) or a periodic interval.
func onCalendarFromSchedule(cronSpec string, every time.Duration) (string, error) {
	if strings.TrimSpace(cronSpec) != "" {
		return cronToOnCalendar(cronSpec)
	}
	// Reuse the cron derivation so systemd and cron stay in lockstep for the
	// same --every value, then translate.
	derived, err := cronExprFromInterval(every)
	if err != nil {
		return "", err
	}
	return cronToOnCalendar(derived)
}

// buildSystemdScheduleUnits returns the .service and .timer unit file contents.
func buildSystemdScheduleUnits(taskName, onCalendar, scriptPath, workDir, marker string) (service, timer string) {
	service = strings.Join([]string{
		"[Unit]",
		"Description=NCC Orchestrator scheduled run (" + taskName + ")",
		"Documentation=https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
		"# " + marker,
		"",
		"[Service]",
		"Type=oneshot",
		"WorkingDirectory=" + workDir,
		"ExecStart=/bin/sh " + scriptPath,
		"",
	}, "\n")
	timer = strings.Join([]string{
		"[Unit]",
		"Description=NCC Orchestrator scheduled run timer (" + taskName + ")",
		"# " + marker,
		"",
		"[Timer]",
		"OnCalendar=" + onCalendar,
		// Run a missed activation (box was off) as soon as possible.
		"Persistent=true",
		"Unit=" + systemdUnitBase(taskName) + ".service",
		"",
		"[Install]",
		"WantedBy=timers.target",
		"",
	}, "\n")
	return service, timer
}

// scheduleRunnerScript writes (and returns the path of) the small wrapper
// script the oneshot service executes. Using a script keeps stdout/stderr
// redirection to the scheduler log working on every systemd version and avoids
// fragile ExecStart quoting of the (already POSIX-quoted) command.
func scheduleRunnerScriptPath(taskName, logPath string) string {
	return filepath.Join(filepath.Dir(logPath), systemdUnitBase(taskName)+".sh")
}

func writeScheduleRunnerScript(taskName, runCmd, logPath string) (string, error) {
	scriptPath := scheduleRunnerScriptPath(taskName, logPath)
	if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
		return "", fmt.Errorf("prepare scheduler script dir: %w", err)
	}
	content := strings.Join([]string{
		"#!/bin/sh",
		"# Generated by ncc-orchestrator create-schedule (systemd backend). Marker: " + scheduleMarker(taskName),
		"exec " + runCmd + " >> " + shellQuotePOSIX(logPath) + " 2>&1",
		"",
	}, "\n")
	if err := os.WriteFile(scriptPath, []byte(content), 0o755); err != nil {
		return "", fmt.Errorf("write scheduler script: %w", err)
	}
	return scriptPath, nil
}

// installSystemdSchedule writes the unit files + runner script, reloads
// systemd, and enables+starts the timer.
func installSystemdSchedule(taskName, cronSpec string, every time.Duration, runCmd, logPath, workDir string) error {
	if !systemctlAvailable() {
		return errors.New("systemd is not available on this host (systemctl not found or systemd is not the init system); use --type cron instead")
	}
	onCalendar, err := onCalendarFromSchedule(cronSpec, every)
	if err != nil {
		return fmt.Errorf("derive OnCalendar: %w", err)
	}
	if strings.TrimSpace(workDir) == "" {
		workDir = filepath.Dir(logPath)
	}
	scriptPath, err := writeScheduleRunnerScript(taskName, runCmd, logPath)
	if err != nil {
		return err
	}
	marker := scheduleMarker(taskName)
	service, timer := buildSystemdScheduleUnits(taskName, onCalendar, scriptPath, workDir, marker)
	base := systemdUnitBase(taskName)
	servicePath := filepath.Join(systemdUnitDir, base+".service")
	timerPath := filepath.Join(systemdUnitDir, base+".timer")
	if err := os.WriteFile(servicePath, []byte(service), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", servicePath, err)
	}
	if err := os.WriteFile(timerPath, []byte(timer), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", timerPath, err)
	}
	if out, err := exec.Command("systemctl", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	if out, err := exec.Command("systemctl", "enable", "--now", base+".timer").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl enable --now %s.timer: %v (%s)", base, err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed systemd timer %s.timer with marker %q\n", base, marker)
	fmt.Printf("OnCalendar=%s\n", onCalendar)
	fmt.Printf("Service: %s\nTimer:   %s\nRunner:  %s\n", servicePath, timerPath, scriptPath)
	return nil
}

// removeSystemdSchedule disables+stops the timer/service and deletes the unit
// files and runner script. Returns removed=false (no error) when nothing was
// present, so callers (incl. the auto backend) can treat it as best-effort.
func removeSystemdSchedule(taskName, logPath string) (bool, error) {
	base := systemdUnitBase(taskName)
	servicePath := filepath.Join(systemdUnitDir, base+".service")
	timerPath := filepath.Join(systemdUnitDir, base+".timer")
	_, serviceErr := os.Stat(servicePath)
	_, timerErr := os.Stat(timerPath)
	if os.IsNotExist(serviceErr) && os.IsNotExist(timerErr) {
		return false, nil
	}
	if systemctlAvailable() {
		// Best-effort: ignore "not loaded" errors so a partial state still cleans up.
		_, _ = exec.Command("systemctl", "disable", "--now", base+".timer").CombinedOutput()
		_, _ = exec.Command("systemctl", "stop", base+".service").CombinedOutput()
	}
	var firstErr error
	for _, p := range []string{timerPath, servicePath} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	if scriptPath := scheduleRunnerScriptPath(taskName, logPath); strings.TrimSpace(logPath) != "" {
		_ = os.Remove(scriptPath)
	}
	if systemctlAvailable() {
		_, _ = exec.Command("systemctl", "daemon-reload").CombinedOutput()
	}
	if firstErr != nil {
		return true, fmt.Errorf("remove systemd unit files: %w", firstErr)
	}
	fmt.Printf("Removed systemd timer %s.timer for marker %q\n", base, scheduleMarker(taskName))
	return true, nil
}

// listSystemdSchedules prints any installed systemd timer for the task,
// including the marker line so marker-based detection (api-server) works. It
// is silent-friendly: prints a stable "No systemd timer" line when absent.
func listSystemdSchedules(taskName string) error {
	marker := scheduleMarker(taskName)
	base := systemdUnitBase(taskName)
	timerPath := filepath.Join(systemdUnitDir, base+".timer")
	data, err := os.ReadFile(timerPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("No systemd timer found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read %s: %w", timerPath, err)
	}
	onCalendar := ""
	for _, ln := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(ln), "OnCalendar=") {
			onCalendar = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(ln), "OnCalendar="))
		}
	}
	// Emit a line carrying the marker so detectInstalledSchedule matches.
	fmt.Printf("# %s systemd timer %s.timer (OnCalendar=%s)\n", marker, base, onCalendar)
	if systemctlAvailable() {
		if out, err := exec.Command("systemctl", "is-enabled", base+".timer").CombinedOutput(); err == nil {
			fmt.Printf("timer enabled: %s", string(out))
		}
		if out, err := exec.Command("systemctl", "list-timers", "--all", base+".timer").CombinedOutput(); err == nil {
			fmt.Print(string(out))
		}
	}
	return nil
}

// schedulerHasCronEntry reports whether the crontab currently holds an entry
// for the task's marker (used by the coexistence guard).
func schedulerHasCronEntry(taskName string) bool {
	marker := scheduleMarker(taskName)
	out, err := exec.Command("crontab", "-l").CombinedOutput()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), marker)
}

// schedulerHasSystemdTimer reports whether a systemd timer unit file exists for
// the task (used by the coexistence guard).
func schedulerHasSystemdTimer(taskName string) bool {
	timerPath := filepath.Join(systemdUnitDir, systemdUnitBase(taskName)+".timer")
	_, err := os.Stat(timerPath)
	return err == nil
}
