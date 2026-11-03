package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"goncc/internal/hostinfo"
	"goncc/internal/runtimetune"
)

// Supervisor and platform checks: the supervisor's own health, whether the
// boot service is defined with the restart and limit settings this release
// expects, and OS / CPU-architecture specific problems (emulation, open-file
// limits, clock sync, inodes, memory, download quarantine, container limits).
// Same rules as selfheal.go: read-only unless hc.Fix, and only fixes that are
// safe unattended.

const supervisorServiceName = "ncc-orchestrator"

// slowProbe is a health probe slow enough to mean the server is struggling.
const slowProbe = 2 * time.Second

// checkSupervisorHealth reads the state the supervisor publishes. A child
// stuck in cooldown is down until the cooldown ends; the fix asks the
// supervisor to start it now (the request bypasses the restart budget).
func checkSupervisorHealth(hc *healContext) healResult {
	res := healResult{ID: "supervisor-health", Title: "Supervisor health", Category: "process"}
	supervisorPID, _, _ := detectRuntimePIDs(hc.InstallDir)
	st, err := readSupervisorState(hc.InstallDir)
	if err != nil {
		res.Status = healOK
		if supervisorPID > 0 {
			res.Message = fmt.Sprintf("supervisor pid %d is running but publishes no state (started by an older release)", supervisorPID)
			res.Hint = "Restart the ncc-orchestrator service to run the current supervisor, which reports restarts and exit reasons here."
		} else {
			res.Message = "no supervisor running (the stack is not service-managed here)"
		}
		return res
	}
	if st.PID <= 0 || !processIsAlive(st.PID) {
		res.Status = healWarn
		res.Message = fmt.Sprintf("state file left by supervisor pid %d, which is no longer running", st.PID)
		if !hc.Fix {
			res.Hint = "Heal removes the stale state file."
			return res
		}
		if err := os.Remove(supervisorStatePath(hc.InstallDir)); err != nil {
			res.Hint = "Could not remove it: " + err.Error()
			return res
		}
		res.Status = healOK
		res.Fixed = true
		res.FixMsg = "removed the stale supervisor state file"
		res.Message = "no supervisor running"
		return res
	}

	names := make([]string, 0, len(st.Children))
	for n := range st.Children {
		names = append(names, n)
	}
	sort.Strings(names)
	var summary, problems, cooling []string
	worst := healOK
	raise := func(s healStatus) {
		if s == healFail || worst == healOK {
			worst = s
		}
	}
	for _, n := range names {
		c := st.Children[n]
		switch c.State {
		case "running":
			up := ""
			if t, err := time.Parse(time.RFC3339, c.StartedAt); err == nil {
				up = " " + shortDuration(time.Since(t).Truncate(time.Minute))
			}
			summary = append(summary, fmt.Sprintf("%s up%s", n, up))
		default:
			summary = append(summary, n+" "+c.State)
		}
		last := ""
		if c.LastExitReason != "" {
			last = fmt.Sprintf("; last exit %s", c.LastExitReason)
			if c.LastExitDetail != "" {
				last += " (" + c.LastExitDetail + ")"
			}
			if line := exitSummaryLine(c.LastExitLogTail); line != "" {
				last += ": " + line
			}
		}
		switch {
		case c.State == "cooldown":
			raise(healFail)
			cooling = append(cooling, n)
			problems = append(problems, fmt.Sprintf("%s is down after %d restarts in a row and waits until %s%s", n, c.RestartsInWindow, c.NextStartAt, last))
		case c.State == "backoff":
			raise(healWarn)
			problems = append(problems, fmt.Sprintf("%s is restarting (attempt %d)%s", n, c.RestartsInWindow, last))
		case c.RestartsInWindow >= 2:
			raise(healWarn)
			problems = append(problems, fmt.Sprintf("%s restarted %d times recently%s", n, c.RestartsInWindow, last))
		}
		if c.LastProbeOK != nil && *c.LastProbeOK && time.Duration(c.LastProbeMS)*time.Millisecond >= slowProbe {
			raise(healWarn)
			problems = append(problems, fmt.Sprintf("%s answers its health probe slowly (%d ms)", n, c.LastProbeMS))
		}
	}
	res.Status = worst
	res.Message = fmt.Sprintf("supervisor pid %d: %s", st.PID, strings.Join(summary, ", "))
	if len(problems) > 0 {
		res.Message += ". " + strings.Join(problems, "; ")
		res.Hint = "Status → Supervisor shows each exit with the end of the server's log."
	}
	if len(cooling) == 0 || !hc.Fix {
		if len(cooling) > 0 {
			res.Hint = "Heal asks the supervisor to start " + strings.Join(cooling, " and ") + " now instead of waiting. " + res.Hint
		}
		return res
	}
	var asked []string
	for _, n := range cooling {
		if os.WriteFile(supervisorRestartRequestPath(hc.InstallDir, n), []byte(nowRFC3339()+"\n"), 0o644) == nil {
			asked = append(asked, n)
		}
	}
	if len(asked) == 0 {
		return res
	}
	// The supervisor looks for requests every couple of seconds; wait for it
	// so the re-check that follows sees the restarted child.
	for deadline := time.Now().Add(supervisorRequestWait); time.Now().Before(deadline); {
		pending := false
		for _, n := range asked {
			if fileExists(supervisorRestartRequestPath(hc.InstallDir, n)) {
				pending = true
			}
		}
		if !pending {
			time.Sleep(time.Second)
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	res.Fixed = true
	res.FixMsg = "asked the supervisor to start " + strings.Join(asked, " and ") + " now"
	return res
}

// supervisorRequestWait bounds how long the fix waits for the supervisor.
var supervisorRequestWait = 8 * time.Second

// exitSummaryLine picks the log line that best explains an exit: the last
// panic or fatal error, else the last error, else the last line.
func exitSummaryLine(tail []string) string {
	for _, words := range [][]string{{"panic:", "fatal error:", "level=fatal", "FATAL"}, {"error", "Error", "ERROR"}} {
		for i := len(tail) - 1; i >= 0; i-- {
			for _, w := range words {
				if strings.Contains(tail[i], w) {
					return strings.TrimSpace(tail[i])
				}
			}
		}
	}
	if len(tail) == 0 {
		return ""
	}
	return strings.TrimSpace(tail[len(tail)-1])
}

var installDirArg = regexp.MustCompile(`--install-dir\s+(?:"([^"]+)"|(\S+))`)

// installDirFromCommand pulls --install-dir out of a service command line.
func installDirFromCommand(cmd string) string {
	m := installDirArg.FindStringSubmatch(cmd)
	if m == nil {
		return ""
	}
	if m[1] != "" {
		return m[1]
	}
	return m[2]
}

func sameDir(a, b string) bool {
	ca, cb := filepath.Clean(a), filepath.Clean(b)
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		return strings.EqualFold(ca, cb)
	}
	return ca == cb
}

// checkServiceConfig compares the installed boot service with what this
// release would install: no start-rate limit, unlimited restarts, raised
// file limits, and (Windows) no 72-hour run limit. Services that belong to
// another install directory are left alone.
func checkServiceConfig(hc *healContext) healResult {
	res := healResult{ID: "service-config", Title: "Boot service definition", Category: "platform"}
	switch runtime.GOOS {
	case "linux":
		return checkSystemdUnit(hc, res, filepath.Join(systemdUnitDir, supervisorServiceName+".service"))
	case "windows":
		return checkWindowsTask(hc, res)
	case "darwin":
		return checkLaunchdPlist(hc, res, launchdPlistPath(supervisorServiceName))
	}
	res.Status = healOK
	res.Message = "no boot service support on " + runtime.GOOS
	return res
}

// systemdUnitGaps lists what a supervisor unit lacks; directive -> section.
func systemdUnitGaps(unit string) []string {
	var gaps []string
	if !regexp.MustCompile(`(?m)^StartLimitIntervalSec=0\s*$`).MatchString(unit) {
		gaps = append(gaps, "StartLimitIntervalSec=0")
	}
	if !regexp.MustCompile(`(?m)^Restart=always\s*$`).MatchString(unit) {
		gaps = append(gaps, "Restart=always")
	}
	if !regexp.MustCompile(`(?m)^LimitNOFILE=`).MatchString(unit) {
		gaps = append(gaps, "LimitNOFILE=65536")
	}
	return gaps
}

// patchSystemdUnit adds the missing directives, keeping everything else
// (including local edits) as it is.
func patchSystemdUnit(unit string, gaps []string) string {
	lines := strings.Split(strings.ReplaceAll(unit, "\r\n", "\n"), "\n")
	insertAfter := func(section, line string) {
		for i, l := range lines {
			if strings.TrimSpace(l) == section {
				lines = append(lines[:i+1], append([]string{line}, lines[i+1:]...)...)
				return
			}
		}
		lines = append(lines, section, line)
	}
	for _, g := range gaps {
		switch g {
		case "StartLimitIntervalSec=0":
			insertAfter("[Unit]", g)
		case "Restart=always":
			replaced := false
			for i, l := range lines {
				if strings.HasPrefix(strings.TrimSpace(l), "Restart=") {
					lines[i] = g
					replaced = true
				}
			}
			if !replaced {
				insertAfter("[Service]", g)
			}
		default:
			insertAfter("[Service]", g)
		}
	}
	return strings.Join(lines, "\n")
}

func checkSystemdUnit(hc *healContext, res healResult, unitPath string) healResult {
	raw, err := os.ReadFile(unitPath)
	if err != nil {
		res.Status = healOK
		res.Message = "not installed as a systemd service"
		return res
	}
	unit := string(raw)
	execLine := ""
	for _, l := range strings.Split(unit, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "ExecStart=") {
			execLine = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(l), "ExecStart="))
		}
	}
	if !strings.Contains(execLine, "v2-supervise") {
		res.Status = healOK
		res.Message = unitPath + " does not run the native supervisor; nothing to compare"
		return res
	}
	if dir := installDirFromCommand(execLine); dir != "" && !sameDir(dir, hc.InstallDir) {
		res.Status = healOK
		res.Message = "the systemd service belongs to another install (" + dir + ")"
		return res
	}
	if bin := strings.Fields(execLine)[0]; !fileExists(bin) {
		res.Status = healFail
		res.Message = "the service starts " + bin + ", which does not exist: the stack will not come back after a reboot"
		res.Hint = "Run: ncc-orchestrator v2-install-service --now (from the current install) to point the service at the right binary."
		return res
	}
	gaps := systemdUnitGaps(unit)
	if len(gaps) == 0 {
		res.Status = healOK
		res.Message = unitPath + " has unlimited restarts and raised file limits"
		return res
	}
	res.Status = healWarn
	res.Message = unitPath + " lacks " + strings.Join(gaps, ", ")
	if !hc.Fix {
		res.Hint = "Without them systemd stops restarting after 5 quick failures, and many parallel cluster connections can run out of file descriptors. Heal adds them (other lines are kept)."
		return res
	}
	if err := os.WriteFile(unitPath, []byte(patchSystemdUnit(unit, gaps)), 0o644); err != nil {
		res.Hint = "Could not update the unit: " + err.Error()
		return res
	}
	if out, err := systemdDaemonReload(); err != nil {
		res.Hint = "Unit updated, but systemctl daemon-reload failed: " + strings.TrimSpace(out)
	}
	res.Status = healOK
	res.Fixed = true
	res.FixMsg = "added " + strings.Join(gaps, ", ") + " to " + unitPath + " (file limits apply from the next service restart)"
	res.Message = unitPath + " has unlimited restarts and raised file limits"
	return res
}

// windowsTaskGaps lists what a Task Scheduler definition lacks.
func windowsTaskGaps(taskXML string) []string {
	var gaps []string
	if !strings.Contains(taskXML, "<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>") {
		gaps = append(gaps, "the task is stopped after 72 hours")
	}
	if !strings.Contains(taskXML, "<RestartOnFailure>") {
		gaps = append(gaps, "Windows does not restart it if it fails")
	}
	if strings.Contains(taskXML, "<StopIfGoingOnBatteries>true</StopIfGoingOnBatteries>") ||
		strings.Contains(taskXML, "<DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>") {
		gaps = append(gaps, "it stops or will not start on battery power")
	}
	return gaps
}

var xmlUnescaper = strings.NewReplacer("&quot;", `"`, "&apos;", "'", "&lt;", "<", "&gt;", ">", "&amp;", "&")

func xmlElement(doc, name string) string {
	m := regexp.MustCompile(`<` + name + `>([^<]*)</` + name + `>`).FindStringSubmatch(doc)
	if m == nil {
		return ""
	}
	return strings.TrimSpace(xmlUnescaper.Replace(m[1]))
}

func checkWindowsTask(hc *healContext, res healResult) healResult {
	out, err := runPlatformCommand(15*time.Second, "schtasks", "/Query", "/TN", supervisorServiceName, "/XML")
	if err != nil {
		res.Status = healOK
		res.Message = "not installed as a Windows scheduled task"
		return res
	}
	taskXML := strings.ReplaceAll(out, "\x00", "")
	command, args := xmlElement(taskXML, "Command"), xmlElement(taskXML, "Arguments")
	if !strings.Contains(args, "v2-supervise") {
		res.Status = healOK
		res.Message = "the scheduled task does not run the native supervisor; nothing to compare"
		return res
	}
	dir := installDirFromCommand(args)
	if dir != "" && !sameDir(dir, hc.InstallDir) {
		res.Status = healOK
		res.Message = "the scheduled task belongs to another install (" + dir + ")"
		return res
	}
	if command != "" && !fileExists(strings.Trim(command, `"`)) {
		res.Status = healFail
		res.Message = "the task starts " + command + ", which does not exist: the stack will not come back after a reboot"
		res.Hint = "Run: ncc-orchestrator v2-install-service (as Administrator, from the current install)."
		return res
	}
	gaps := windowsTaskGaps(taskXML)
	if len(gaps) == 0 {
		res.Status = healOK
		res.Message = "scheduled task " + supervisorServiceName + " has no time limit and restarts on failure"
		return res
	}
	res.Status = healWarn
	res.Message = "scheduled task " + supervisorServiceName + ": " + strings.Join(gaps, "; ")
	if !hc.Fix {
		res.Hint = "Heal re-registers the task from this release's definition (needs Administrator; the running supervisor keeps running)."
		return res
	}
	if dir == "" {
		dir = hc.InstallDir
	}
	o := installServiceOptions{InstallDir: dir, ServiceName: supervisorServiceName, OrchestratorBin: strings.Trim(command, `"`)}
	if err := registerWindowsSupervisorTask(o); err != nil {
		res.Hint = "Could not re-register the task (run doctor --fix as Administrator): " + err.Error()
		return res
	}
	res.Status = healOK
	res.Fixed = true
	res.FixMsg = "re-registered scheduled task " + supervisorServiceName + " with no time limit and restart on failure"
	res.Message = "scheduled task " + supervisorServiceName + " has no time limit and restarts on failure"
	return res
}

func checkLaunchdPlist(hc *healContext, res healResult, plistPath string) healResult {
	raw, err := os.ReadFile(plistPath)
	if err != nil {
		res.Status = healOK
		res.Message = "not installed as a launchd daemon"
		return res
	}
	plist := string(raw)
	if !strings.Contains(plist, "v2-supervise") {
		res.Status = healOK
		res.Message = plistPath + " does not run the native supervisor; nothing to compare"
		return res
	}
	if !strings.Contains(plist, hc.InstallDir) {
		res.Status = healOK
		res.Message = "the launchd daemon belongs to another install"
		return res
	}
	var gaps []string
	if !strings.Contains(plist, "<key>KeepAlive</key>") {
		gaps = append(gaps, "KeepAlive (restart when it exits)")
	}
	if !strings.Contains(plist, "<key>NumberOfFiles</key>") {
		gaps = append(gaps, "a raised open-file limit (launchd's default is 256)")
	}
	if len(gaps) == 0 {
		res.Status = healOK
		res.Message = plistPath + " keeps the supervisor alive with raised file limits"
		return res
	}
	res.Status = healWarn
	res.Message = plistPath + " lacks " + strings.Join(gaps, " and ")
	res.Hint = "Re-run: sudo ncc-orchestrator v2-install-service --now. Doctor does not reload launchd daemons itself because that restarts the stack."
	return res
}

// checkPlatformArch spots a binary running under CPU emulation (Rosetta 2,
// Windows x64-on-ARM, WOW64): it works, but slower and with more memory.
func checkPlatformArch(hc *healContext) healResult {
	res := healResult{ID: "platform-arch", Title: "Native CPU architecture", Category: "performance"}
	a := hostinfo.CPUArch()
	if !a.Known {
		res.Status = healOK
		res.Message = fmt.Sprintf("%s/%s build; native architecture not detectable here", runtime.GOOS, a.Binary)
		return res
	}
	if !a.Emulated {
		res.Status = healOK
		res.Message = fmt.Sprintf("native %s/%s build", runtime.GOOS, a.Binary)
		return res
	}
	res.Status = healWarn
	res.Message = fmt.Sprintf("the %s build runs under emulation on this %s machine", a.Binary, a.Native)
	res.Hint = fmt.Sprintf("Install the %s/%s release package: runs, reports and the web UI get noticeably faster and use less memory.", runtime.GOOS, a.Native)
	return res
}

// minFileLimit is the open-file limit below which parallel cluster runs, the
// API's connections and log files can run out of descriptors.
const minFileLimit = 8192

// checkOSLimits checks the open-file limit (Unix). Go raises the soft limit
// to the hard one on start, so the hard limit is what counts.
func checkOSLimits(hc *healContext) healResult {
	res := healResult{ID: "os-limits", Title: "Open-file limit", Category: "performance"}
	soft, hard, ok := hostinfo.FileLimit()
	if !ok {
		res.Status = healOK
		res.Message = "no per-process open-file limit on " + runtime.GOOS
		return res
	}
	res.Message = fmt.Sprintf("open files: %d (hard limit %d)", soft, hard)
	if hard >= minFileLimit {
		res.Status = healOK
		return res
	}
	res.Status = healWarn
	switch runtime.GOOS {
	case "linux":
		res.Hint = "Raise it for the service: LimitNOFILE=65536 in the unit (Boot service definition fixes that), or nofile in /etc/security/limits.conf for manual starts."
	case "darwin":
		res.Hint = "Raise it for the daemon (NumberOfFiles in the launchd plist; re-run v2-install-service) or with launchctl limit maxfiles."
	default:
		res.Hint = fmt.Sprintf("Raise the open-file limit to at least %d.", minFileLimit)
	}
	return res
}

// checkTimeSync warns when the clock is not synchronised: TLS certificate
// checks, schedules, session expiry and report timestamps all depend on it.
func checkTimeSync(hc *healContext) healResult {
	res := healResult{ID: "time-sync", Title: "Clock synchronisation", Category: "platform"}
	res.Status = healOK
	switch runtime.GOOS {
	case "linux":
		if _, err := exec.LookPath("timedatectl"); err != nil {
			res.Message = "timedatectl not available (container or non-systemd host); not checked"
			return res
		}
		out, err := runPlatformCommand(5*time.Second, "timedatectl", "show", "-p", "NTPSynchronized", "--value")
		switch v := strings.TrimSpace(out); {
		case err != nil || v == "":
			res.Message = "sync state not available; not checked"
		case v == "yes":
			res.Message = "clock synchronised (NTP)"
		default:
			res.Status = healWarn
			res.Message = "the system clock is not synchronised"
			res.Hint = "Enable time sync: timedatectl set-ntp true (systemd-timesyncd or chrony)."
		}
	case "windows":
		out, err := runPlatformCommand(5*time.Second, "w32tm", "/query", "/status")
		if err != nil {
			res.Message = "Windows Time status not readable (needs Administrator); not checked"
			return res
		}
		for _, l := range strings.Split(out, "\n") {
			l = strings.TrimSpace(l)
			if strings.HasPrefix(l, "Source:") {
				src := strings.TrimSpace(strings.TrimPrefix(l, "Source:"))
				if strings.Contains(src, "Local CMOS Clock") || strings.Contains(src, "Free-running") {
					res.Status = healWarn
					res.Message = "the clock is not synchronised (source: " + src + ")"
					res.Hint = "Start the Windows Time service and resync: w32tm /resync (as Administrator), or join it to a domain time source."
					return res
				}
				res.Message = "clock synchronised from " + src
				return res
			}
		}
		res.Message = "time source not reported; not checked"
	default:
		res.Message = "not checked on " + runtime.GOOS + " (the OS keeps time in sync by default)"
	}
	return res
}

// checkInodes warns when the filesystem runs out of inodes: writes then fail
// with "no space left" although df shows free space.
func checkInodes(hc *healContext) healResult {
	res := healResult{ID: "inodes", Title: "Free inodes", Category: "storage"}
	free, total, ok := hostinfo.Inodes(hc.InstallDir)
	if !ok {
		res.Status = healOK
		res.Message = "filesystem has no fixed inode count; not applicable"
		return res
	}
	pct := float64(free) * 100 / float64(total)
	res.Message = fmt.Sprintf("%.0f%% of inodes free (%d of %d)", pct, free, total)
	switch {
	case pct < 2:
		res.Status = healFail
	case pct < 10:
		res.Status = healWarn
	default:
		res.Status = healOK
		return res
	}
	res.Hint = "Many small files use up inodes: prune old run history and reports (Report and history growth), and leftover temporary files."
	return res
}

// checkMemoryPressure warns when the machine or the container is close to
// its memory limit, where runs slow down or the OOM killer stops servers.
func checkMemoryPressure(hc *healContext) healResult {
	res := healResult{ID: "memory-pressure", Title: "Memory headroom", Category: "performance"}
	res.Status = healOK
	var parts []string
	m := hostinfo.SystemMemory()
	if m.Total > 0 && m.Available > 0 {
		pct := float64(m.Available) * 100 / float64(m.Total)
		parts = append(parts, fmt.Sprintf("%s of %s available (%.0f%%)", humanBytes(m.Available), humanBytes(m.Total), pct))
		if pct < 10 {
			res.Status = healWarn
			res.Hint = "Close other workloads, add memory, or lower runner concurrency (Run performance settings)."
		}
	}
	if l := runtimetune.ReadLimits(); l.MemoryLimit > 0 {
		pct := float64(l.MemoryUsage) * 100 / float64(l.MemoryLimit)
		parts = append(parts, fmt.Sprintf("container uses %s of its %s limit (%.0f%%)", humanBytes(l.MemoryUsage), humanBytes(l.MemoryLimit), pct))
		if pct >= 90 {
			res.Status = healWarn
			res.Hint = "Raise the container memory limit or lower runner concurrency."
		}
	}
	if len(parts) == 0 {
		res.Message = "memory figures not available on " + runtime.GOOS
		return res
	}
	res.Message = strings.Join(parts, "; ")
	return res
}

// checkBinaryQuarantine finds the downloaded-file mark on the stack's
// binaries (macOS quarantine attribute, Windows Zone.Identifier), which makes
// Gatekeeper or SmartScreen block or prompt when the service starts them.
func checkBinaryQuarantine(hc *healContext) healResult {
	res := healResult{ID: "binary-quarantine", Title: "Download quarantine on binaries", Category: "platform"}
	if runtime.GOOS != "darwin" && runtime.GOOS != "windows" {
		res.Status = healOK
		res.Message = "not applicable on " + runtime.GOOS
		return res
	}
	seen := map[string]bool{}
	var bins []string
	add := func(p string) {
		if p == "" || seen[p] || !fileExists(p) {
			return
		}
		seen[p] = true
		bins = append(bins, p)
	}
	for _, b := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server", "ncc-mcp-server"} {
		add(binaryPathInInstallDir(hc.InstallDir, b))
	}
	if self, err := os.Executable(); err == nil {
		add(self)
	}
	var marked []string
	for _, b := range bins {
		if hostinfo.Quarantined(b) {
			marked = append(marked, b)
		}
	}
	if len(marked) == 0 {
		res.Status = healOK
		res.Message = binariesChecked(len(bins))
		return res
	}
	res.Status = healWarn
	res.Message = "downloaded-file mark on: " + strings.Join(marked, ", ")
	if !hc.Fix {
		if runtime.GOOS == "darwin" {
			res.Hint = "Gatekeeper can block these when launchd starts them. Heal removes the com.apple.quarantine attribute."
		} else {
			res.Hint = "SmartScreen can prompt or block these. Heal removes the Zone.Identifier mark (same as Properties → Unblock)."
		}
		return res
	}
	var cleared, failed []string
	for _, b := range marked {
		if err := hostinfo.ClearQuarantine(b); err != nil {
			failed = append(failed, fmt.Sprintf("%s (%v)", filepath.Base(b), err))
		} else {
			cleared = append(cleared, filepath.Base(b))
		}
	}
	if len(cleared) > 0 {
		res.Fixed = true
		res.FixMsg = "removed the download mark from " + strings.Join(cleared, ", ")
	}
	if len(failed) > 0 {
		res.Hint = "Could not clear: " + strings.Join(failed, ", ")
		return res
	}
	res.Status = healOK
	res.Message = binariesChecked(len(bins))
	return res
}

// checkCgroupLimits reports container limits and OOM kills, and confirms the
// runtime adapted to them (memory limit set, GOMAXPROCS from the CPU quota).
func checkCgroupLimits(hc *healContext) healResult {
	res := healResult{ID: "cgroup-limits", Title: "Container resource limits", Category: "performance"}
	res.Status = healOK
	l := runtimetune.ReadLimits()
	if l.Version == 0 || (l.MemoryLimit == 0 && l.CPULimit == 0 && l.OOMKills == 0) {
		res.Message = "no container memory or CPU limits"
		return res
	}
	var parts []string
	if l.MemoryLimit > 0 {
		parts = append(parts, "memory "+humanBytes(l.MemoryLimit))
	}
	if l.CPULimit > 0 {
		parts = append(parts, fmt.Sprintf("%.2g CPUs", l.CPULimit))
	}
	res.Message = fmt.Sprintf("cgroup v%d limits: %s; servers size their memory limit and GOMAXPROCS to them", l.Version, strings.Join(parts, ", "))
	var hints []string
	if l.MemoryLimit > 0 && l.MemoryLimit < 512<<20 {
		res.Status = healWarn
		hints = append(hints, "512 MiB or more is recommended for the API, UI and a run together")
	}
	if l.CPULimit > 0 && l.CPULimit < 1 {
		res.Status = healWarn
		hints = append(hints, "less than one CPU makes runs and the web UI slow; give it at least 1")
	}
	if l.OOMKills > 0 {
		res.Status = healWarn
		res.Message += fmt.Sprintf("; the kernel killed processes for memory %d time(s)", l.OOMKills)
		hints = append(hints, "raise the memory limit or lower runner concurrency")
	}
	if len(hints) > 0 {
		res.Hint = strings.ToUpper(hints[0][:1]) + strings.Join(hints, "; ")[1:] + "."
	}
	return res
}

// systemdDaemonReload is a variable so tests do not reload the host's systemd.
var systemdDaemonReload = func() (string, error) {
	return runPlatformCommand(15*time.Second, "systemctl", "daemon-reload")
}

func binariesChecked(n int) string {
	if n == 1 {
		return "1 binary checked, no download mark"
	}
	return fmt.Sprintf("%d binaries checked, none carries a download mark", n)
}

// runPlatformCommand runs a short OS tool with a deadline.
func runPlatformCommand(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), err
}
