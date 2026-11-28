package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"goncc/internal/plural"
	"goncc/internal/runtimecaps"

	"github.com/spf13/cobra"
)

func normalizeScheduleType(s string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "", "auto":
		if runtime.GOOS == "windows" {
			return "windows", nil
		}
		return "cron", nil
	case "cron":
		return "cron", nil
	case "systemd", "systemd-timer", "timer":
		return "systemd", nil
	case "windows", "windows-task":
		return "windows", nil
	default:
		return "", fmt.Errorf("type must be auto, cron, systemd, or windows (got %q)", s)
	}
}

func cronExprFromInterval(every time.Duration) (string, error) {
	if every <= 0 {
		return "", errors.New("every must be > 0")
	}
	if every%time.Minute != 0 {
		return "", errors.New("every must be a whole number of minutes for cron")
	}
	minutes := int(every / time.Minute)
	switch {
	case minutes < 60:
		return fmt.Sprintf("*/%d * * * *", minutes), nil
	case minutes%60 == 0 && minutes < 24*60:
		return fmt.Sprintf("0 */%d * * *", minutes/60), nil
	case minutes%(24*60) == 0:
		days := minutes / (24 * 60)
		return fmt.Sprintf("0 0 */%d * *", days), nil
	default:
		return "", errors.New("every must be an even minute/hour/day interval for cron (or provide --cron)")
	}
}

func shellQuotePOSIX(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func validateScheduleTaskName(taskName string) error {
	name := strings.TrimSpace(taskName)
	if name == "" {
		return errors.New("task-name must not be empty")
	}
	if len(name) > 128 {
		return errors.New("task-name too long")
	}
	for _, ch := range name {
		if !(ch == '.' || ch == '_' || ch == '-' || ch == ':' ||
			(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
			return errors.New("task-name contains invalid characters")
		}
	}
	return nil
}

func sanitizeScheduleCommand(runCmd string) (string, error) {
	v := strings.TrimSpace(runCmd)
	if v == "" {
		return "", errors.New("command cannot be empty")
	}
	if strings.ContainsAny(v, "&;|`$><\n\r\t") {
		return "", errors.New("command contains unsafe shell metacharacters")
	}
	return v, nil
}

func parseCommandLineStrict(raw string) (string, []string, error) {
	clean, err := sanitizeScheduleCommand(raw)
	if err != nil {
		return "", nil, err
	}
	parts := []string{}
	var current strings.Builder
	var quote rune
	flush := func() {
		if current.Len() == 0 {
			return
		}
		parts = append(parts, current.String())
		current.Reset()
	}
	for _, ch := range clean {
		if quote != 0 {
			if ch == quote {
				quote = 0
				continue
			}
			current.WriteRune(ch)
			continue
		}
		if ch == '"' || ch == '\'' {
			quote = ch
			continue
		}
		if ch == ' ' {
			flush()
			continue
		}
		current.WriteRune(ch)
	}
	if quote != 0 {
		return "", nil, errors.New("unterminated quotes in command")
	}
	flush()
	if len(parts) == 0 {
		return "", nil, errors.New("command cannot be empty")
	}
	name := strings.TrimSpace(parts[0])
	if name == "" {
		return "", nil, errors.New("command executable cannot be empty")
	}
	args := make([]string, 0, len(parts)-1)
	for _, p := range parts[1:] {
		args = append(args, strings.TrimSpace(p))
	}
	return name, args, nil
}

func scheduleMarker(taskName string) string {
	name := strings.TrimSpace(taskName)
	if name == "" {
		name = "default"
	}
	return "ncc-orchestrator:" + name
}

// scheduleRunHistoryFlags are appended to generated schedule commands so a
// scheduled run records a per-run snapshot under <output-dir-filtered>/runs.
// Without this, scheduled scans only overwrite the single in-place
// run-summary.json and never accumulate, so the dashboard "recent runs" history
// omits them entirely (only API/UI-triggered runs, which the api-server archives
// itself, would appear). The flag is passed explicitly rather than relying on
// `run-history: true` in config because it is the authoritative source for the
// run and is honored regardless of config-key precedence. Retention is bounded
// so a frequent schedule can't grow the history dir without limit.
const scheduleRunHistoryFlags = "--run-history --retain-last 500 --retain-days 30"

func defaultScheduleCommand(configPath string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	exe = filepath.Clean(exe)
	if runtime.GOOS == "windows" {
		if strings.TrimSpace(configPath) == "" {
			return fmt.Sprintf("\"%s\" %s", exe, scheduleRunHistoryFlags), nil
		}
		return fmt.Sprintf("\"%s\" --config \"%s\" %s", exe, configPath, scheduleRunHistoryFlags), nil
	}
	if strings.TrimSpace(configPath) == "" {
		return fmt.Sprintf("%s %s", shellQuotePOSIX(exe), scheduleRunHistoryFlags), nil
	}
	return fmt.Sprintf("%s --config %s %s", shellQuotePOSIX(exe), shellQuotePOSIX(configPath), scheduleRunHistoryFlags), nil
}

func upsertScheduleLine(content, marker, line string) string {
	lines := splitLines(content)
	out := make([]string, 0, len(lines)+1)
	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" {
			continue
		}
		if strings.Contains(ln, marker) {
			continue
		}
		out = append(out, ln)
	}
	out = append(out, line)
	return strings.Join(out, "\n") + "\n"
}

func removeScheduleLine(content, marker string) (string, bool) {
	lines := splitLines(content)
	out := make([]string, 0, len(lines))
	removed := false
	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" {
			continue
		}
		if strings.Contains(ln, marker) {
			removed = true
			continue
		}
		out = append(out, ln)
	}
	if len(out) == 0 {
		return "", removed
	}
	return strings.Join(out, "\n") + "\n", removed
}

func listCronSchedules(taskName string) error {
	marker := scheduleMarker(taskName)
	out, err := exec.Command("crontab", "-l").CombinedOutput()
	if err != nil {
		low := strings.ToLower(string(out))
		if strings.Contains(low, "no crontab") {
			fmt.Printf("No cron entries found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	lines := splitLines(string(out))
	found := false
	for _, ln := range lines {
		if strings.Contains(ln, marker) {
			fmt.Println(ln)
			found = true
		}
	}
	if !found {
		fmt.Printf("No cron entries found for marker %q\n", marker)
	}
	return nil
}

func removeCronSchedule(taskName string) error {
	marker := scheduleMarker(taskName)
	existingBytes, err := exec.Command("crontab", "-l").CombinedOutput()
	existing := string(existingBytes)
	if err != nil {
		low := strings.ToLower(existing)
		if strings.Contains(low, "no crontab") {
			fmt.Printf("No cron entries found for marker %q\n", marker)
			return nil
		}
		return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(existing))
	}
	updated, removed := removeScheduleLine(existing, marker)
	if !removed {
		fmt.Printf("No cron entries found for marker %q\n", marker)
		return nil
	}
	c := exec.Command("crontab", "-")
	c.Stdin = strings.NewReader(updated)
	if out, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("write updated crontab: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed cron entry for marker %q\n", marker)
	return nil
}

func listWindowsSchedule(taskName string) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be listed on Windows")
	}
	marker := scheduleMarker(taskName)
	taskName, found := findWindowsSchedule(taskName)
	if !found {
		fmt.Printf("No scheduled tasks found for marker %q\n", marker)
		return nil
	}
	out, err := exec.Command("schtasks", "/Query", "/TN", taskName, "/FO", "LIST", "/V").CombinedOutput() // #nosec G702 -- no shell; the task name is the value of /TN
	if err != nil {
		return fmt.Errorf("query scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Print(string(out))
	// The API server looks for this line.
	fmt.Printf("# %s Windows task %s\n", marker, taskName)
	return nil
}

func removeWindowsSchedule(taskName string) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be removed on Windows")
	}
	taskName, found := findWindowsSchedule(taskName)
	if !found {
		fmt.Printf("No Windows Scheduled Task %q found\n", taskName)
		return nil
	}
	out, err := exec.Command("schtasks", "/Delete", "/TN", taskName, "/F").CombinedOutput() // #nosec G702 -- no shell; the task name is the value of /TN
	if err != nil {
		return fmt.Errorf("delete scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Removed Windows Scheduled Task %q\n%s\n", taskName, strings.TrimSpace(string(out)))
	return nil
}

func runScheduleCommandNow(runCmd string) error {
	name, args, err := parseCommandLineStrict(runCmd)
	if err != nil {
		return err
	}
	c := exec.Command(name, args...) // #nosec G702 -- runs the schedule command the operator just asked for, parsed without a shell
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin
	return c.Run()
}

func buildCronScheduleLine(cronSpec, runCmd, logPath, marker string, withLock bool) string {
	if withLock {
		lockFile := filepath.Join(filepath.Dir(logPath), ".ncc-scheduler.lock")
		return fmt.Sprintf("%s flock -n %s -c %s >> %s 2>&1 # %s",
			cronSpec,
			shellQuotePOSIX(lockFile),
			shellQuotePOSIX(runCmd),
			shellQuotePOSIX(logPath),
			marker,
		)
	}
	return fmt.Sprintf("%s %s >> %s 2>&1 # %s", cronSpec, runCmd, shellQuotePOSIX(logPath), marker)
}

func installCronSchedule(taskName, cronSpec, runCmd, logPath string, withLock bool) error {
	marker := scheduleMarker(taskName)
	line := buildCronScheduleLine(cronSpec, runCmd, logPath, marker, withLock)

	existingBytes, err := exec.Command("crontab", "-l").CombinedOutput()
	existing := string(existingBytes)
	if err != nil {
		// "no crontab for <user>" should be treated as empty content.
		low := strings.ToLower(existing)
		if !strings.Contains(low, "no crontab") {
			return fmt.Errorf("read current crontab: %v (%s)", err, strings.TrimSpace(existing))
		}
		existing = ""
	}

	updated := upsertScheduleLine(existing, marker, line)
	c := exec.Command("crontab", "-")
	c.Stdin = strings.NewReader(updated)
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("install crontab entry: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Installed cron schedule with marker %q\n", marker)
	fmt.Printf("%s\n", line)
	return nil
}

func installWindowsSchedule(taskName, runCmd string, every time.Duration) error {
	if runtime.GOOS != "windows" {
		return errors.New("windows schedule can only be created on Windows")
	}
	if every <= 0 {
		return errors.New("every must be > 0")
	}
	if every%time.Minute != 0 {
		return errors.New("every must be a whole number of minutes")
	}
	minutes := int(every / time.Minute)
	if minutes < 1 {
		return errors.New("every must be at least 1 minute")
	}

	legacy, hadLegacy := findWindowsSchedule(taskName)
	taskName = windowsScheduleTaskName(taskName)
	args := []string{
		"/Create",
		"/TN", taskName,
		"/TR", runCmd,
		"/SC", "MINUTE",
		"/MO", strconv.Itoa(minutes),
		"/F",
	}
	out, err := exec.Command("schtasks", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("create scheduled task: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Created/updated Windows Scheduled Task %q (every %s)\n", taskName, plural.Count(minutes, "minute"))
	if hadLegacy && legacy != taskName {
		// Earlier releases used the plain name: two tasks would run every check twice.
		if out, err := exec.Command("schtasks", "/Delete", "/TN", legacy, "/F").CombinedOutput(); err != nil { // #nosec G702 -- no shell; the task name is the value of /TN
			return fmt.Errorf("remove the previous schedule task %q: %v (%s)", legacy, err, strings.TrimSpace(string(out)))
		}
	}
	return nil
}

// windowsScheduleTaskName is the Task Scheduler name of a schedule. The
// default schedule name is also the supervisor's task name, and creating one
// with /F would replace the other (WIN-06), so it gets its own.
func windowsScheduleTaskName(taskName string) string {
	if strings.EqualFold(strings.TrimSpace(taskName), supervisorServiceName) {
		return supervisorServiceName + "-schedule"
	}
	return taskName
}

// queryWindowsTaskXML reads a task's definition; ok is false when there is
// none (or it cannot be read). A variable so tests never see the host's tasks.
var queryWindowsTaskXML = func(name string) (string, bool) {
	out, err := runPlatformCommand(15*time.Second, "schtasks", "/Query", "/TN", name, "/XML")
	if err != nil {
		return "", false
	}
	return strings.ReplaceAll(out, "\x00", ""), true
}

// windowsScanTask reports whether a task definition is a scan schedule made by
// this program, as opposed to the supervisor or somebody else's task.
func windowsScanTask(taskXML string) bool {
	run := strings.ToLower(xmlElement(taskXML, "Command") + " " + xmlElement(taskXML, "Arguments"))
	return strings.Contains(run, "ncc-orchestrator") && !strings.Contains(run, "v2-supervise")
}

// findWindowsSchedule returns the task that holds a schedule: its own name, or
// the plain name earlier releases used. The supervisor's task never matches.
func findWindowsSchedule(taskName string) (name string, found bool) {
	name = windowsScheduleTaskName(taskName)
	if name == taskName {
		_, found = queryWindowsTaskXML(name)
		return name, found
	}
	if _, ok := queryWindowsTaskXML(name); ok {
		return name, true
	}
	if x, ok := queryWindowsTaskXML(taskName); ok && windowsScanTask(x) {
		return taskName, true
	}
	return name, false
}

func runCreateSchedule(cmd *cobra.Command, args []string) error {
	if err := runtimecaps.Detect().RejectHostOperation("host schedule changes"); err != nil {
		return exitConfig(err)
	}
	scheduleTypeRaw, _ := cmd.Flags().GetString("type")
	scheduleType, err := normalizeScheduleType(scheduleTypeRaw)
	if err != nil {
		return exitConfig(err)
	}
	rawLower := strings.ToLower(strings.TrimSpace(scheduleTypeRaw))
	isAuto := rawLower == "" || rawLower == "auto"
	if isAuto && scheduleType == "cron" {
		// An unprivileged service cannot use crontab (setuid or setgid, which
		// NoNewPrivileges blocks); its schedules are systemd timers that the
		// service helper installs.
		if _, ok := opsAvailable(stackInstallDir()); ok {
			scheduleType = "systemd"
		}
	}

	taskName, _ := cmd.Flags().GetString("task-name")
	if err := validateScheduleTaskName(taskName); err != nil {
		return exitConfig(err)
	}
	logPath, _ := cmd.Flags().GetString("log-path")
	if strings.TrimSpace(logPath) == "" {
		logPath = "logs/ncc-scheduler.log"
	}
	logPath = strings.TrimSpace(logPath)
	if !filepath.IsAbs(logPath) {
		if absLogPath, absErr := filepath.Abs(logPath); absErr == nil {
			logPath = absLogPath
		}
	}

	configPath, _ := cmd.Flags().GetString("config")
	configPath = strings.TrimSpace(configPath)
	if configPath != "" && !filepath.IsAbs(configPath) {
		if absConfigPath, absErr := filepath.Abs(configPath); absErr == nil {
			configPath = absConfigPath
		}
	}
	if err := validateSchedulePaths(logPath, configPath); err != nil {
		return exitConfig(err)
	}
	if spec, _ := cmd.Flags().GetString("cron"); strings.TrimSpace(spec) != "" {
		if err := validateCronSpec(spec); err != nil {
			return exitConfig(err)
		}
	}
	runCmd, _ := cmd.Flags().GetString("command")
	if strings.TrimSpace(runCmd) == "" {
		runCmd, err = defaultScheduleCommand(configPath)
		if err != nil {
			return fmt.Errorf("build schedule command: %w", err)
		}
	}
	// systemd timers run with an explicit WorkingDirectory; anchor it to the
	// config file's directory so a relative output-dir resolves the same way an
	// interactive run from there would (the cron cwd footgun).
	systemdWorkDir := ""
	if configPath != "" {
		systemdWorkDir = filepath.Dir(configPath)
	}
	runCmd, err = sanitizeScheduleCommand(runCmd)
	if err != nil {
		return exitConfig(err)
	}

	printOnly, _ := cmd.Flags().GetBool("print-only")
	withLock, _ := cmd.Flags().GetBool("with-lock")
	every, _ := cmd.Flags().GetDuration("every")
	action, _ := cmd.Flags().GetString("action")
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = "create"
	}

	switch scheduleType {
	case "cron":
		switch action {
		case "list":
			if err := listCronSchedules(taskName); err != nil {
				return err
			}
			// With --type auto on a systemd host, also surface (and below,
			// also remove) a systemd timer so detection/cleanup is uniform
			// regardless of which backend installed the schedule.
			if isAuto && systemctlAvailable() {
				_ = listSystemdSchedules(taskName)
			}
			return nil
		case "remove":
			if printOnly {
				fmt.Printf("Cron remove preview: marker=%q\n", scheduleMarker(taskName))
				return nil
			}
			if err := removeCronSchedule(taskName); err != nil {
				return err
			}
			if isAuto && systemctlAvailable() {
				if _, rmErr := removeSystemdSchedule(taskName, logPath); rmErr != nil {
					fmt.Fprintf(os.Stderr, "warning: remove systemd timer: %v\n", rmErr)
				}
			}
			return nil
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}

		cronSpec, _ := cmd.Flags().GetString("cron")
		cronSpec = strings.TrimSpace(cronSpec)
		if cronSpec == "" {
			cronSpec, err = cronExprFromInterval(every)
			if err != nil {
				return exitConfig(fmt.Errorf("derive cron from --every: %w", err))
			}
		}
		line := buildCronScheduleLine(cronSpec, runCmd, logPath, scheduleMarker(taskName), withLock)
		if printOnly {
			fmt.Printf("Cron entry preview:\n%s\n", line)
			return nil
		}
		// Coexistence guard: a systemd timer for the same task would double-run
		// the scan. Remove it when switching to cron.
		if schedulerHasSystemdTimer(taskName) {
			fmt.Fprintf(os.Stderr, "note: removing existing systemd timer for %q to avoid duplicate runs\n", scheduleMarker(taskName))
			if _, rmErr := removeSystemdSchedule(taskName, logPath); rmErr != nil {
				fmt.Fprintf(os.Stderr, "warning: could not remove conflicting systemd timer: %v\n", rmErr)
			}
		}
		return installCronSchedule(taskName, cronSpec, runCmd, logPath, withLock)
	case "systemd":
		switch action {
		case "list":
			return listSystemdSchedules(taskName)
		case "remove":
			if printOnly {
				fmt.Printf("systemd remove preview: timer=%s.timer marker=%q\n", systemdUnitBase(taskName), scheduleMarker(taskName))
				return nil
			}
			removed, rmErr := removeSystemdSchedule(taskName, logPath)
			if rmErr != nil {
				return rmErr
			}
			if !removed {
				fmt.Printf("No systemd timer found for marker %q\n", scheduleMarker(taskName))
			}
			return nil
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}

		// Defense-in-depth: a systemd timer runs the scan from an explicit
		// WorkingDirectory that is NOT the operator's interactive cwd, so a
		// config-less run cannot reliably locate config.yaml and fails with
		// "at least one cluster must be provided". Require an explicit --config
		// (anchored absolute above) rather than installing a timer that is
		// guaranteed to fail on its first activation.
		if strings.TrimSpace(configPath) == "" {
			return exitConfig(errors.New("systemd timers require an explicit --config (preferably an absolute path): without it the timer runs from systemd's working directory and cannot locate config.yaml"))
		}

		cronSpec, _ := cmd.Flags().GetString("cron")
		cronSpec = strings.TrimSpace(cronSpec)
		onCalendar, ocErr := onCalendarFromSchedule(cronSpec, every)
		if ocErr != nil {
			return exitConfig(fmt.Errorf("derive OnCalendar: %w", ocErr))
		}
		if printOnly {
			marker := scheduleMarker(taskName)
			workDir := systemdWorkDir
			if workDir == "" {
				workDir = filepath.Dir(logPath)
			}
			svc, tmr := buildSystemdScheduleUnitsFor(taskName, onCalendar, scheduleRunnerScriptPath(taskName, logPath), workDir, marker, stackServiceUser(stackInstallDir()))
			fmt.Printf("systemd unit preview (%s.service):\n%s\nsystemd unit preview (%s.timer):\n%s\n",
				systemdUnitBase(taskName), svc, systemdUnitBase(taskName), tmr)
			return nil
		}
		// Coexistence guard: drop any cron entry for the same task so the scan
		// is not scheduled twice.
		if schedulerHasCronEntry(taskName) {
			fmt.Fprintf(os.Stderr, "note: removing existing cron entry for %q to avoid duplicate runs\n", scheduleMarker(taskName))
			if rmErr := removeCronSchedule(taskName); rmErr != nil {
				fmt.Fprintf(os.Stderr, "warning: could not remove conflicting cron entry: %v\n", rmErr)
			}
		}
		return installSystemdSchedule(taskName, cronSpec, every, runCmd, logPath, systemdWorkDir)
	case "windows":
		switch action {
		case "list":
			return listWindowsSchedule(taskName)
		case "remove":
			if printOnly {
				name, _ := findWindowsSchedule(taskName)
				fmt.Printf("Windows remove preview: schtasks /Delete /TN %q /F\n", name)
				return nil
			}
			return removeWindowsSchedule(taskName)
		case "run-now":
			fmt.Printf("Running now: %s\n", runCmd)
			return runScheduleCommandNow(runCmd)
		case "create":
		default:
			return exitConfig(fmt.Errorf("action must be create, list, remove, or run-now (got %q)", action))
		}
		// schtasks repeats on a fixed interval; a cron expression used to be
		// ignored silently, installing the --every interval instead.
		if c, _ := cmd.Flags().GetString("cron"); strings.TrimSpace(c) != "" {
			return exitConfig(errors.New("cron expressions are not supported by Windows Scheduled Tasks, which run on an interval; clear --cron and set --every"))
		}
		if printOnly {
			if every <= 0 {
				return exitConfig(errors.New("every must be > 0"))
			}
			fmt.Printf("Windows task preview:\nschtasks /Create /TN %q /TR %q /SC MINUTE /MO %d /F\n",
				windowsScheduleTaskName(taskName), runCmd, int(every/time.Minute))
			return nil
		}
		return installWindowsSchedule(taskName, runCmd, every)
	default:
		return exitConfig(fmt.Errorf("unsupported type %q", scheduleType))
	}
}
