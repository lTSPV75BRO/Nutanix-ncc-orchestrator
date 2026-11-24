package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// The update helper's service managers on Windows and macOS. Both files build
// on every platform, so their definitions are tested everywhere.

// ---- Windows Task Scheduler ----

type windowsService struct {
	o       installServiceOptions
	taskXML string
}

func findWindowsService(installDir string) stackService {
	x, ok := queryWindowsTaskXML(supervisorServiceName)
	if !ok {
		return nil
	}
	return windowsServiceFromTask(installDir, x)
}

func windowsServiceFromTask(installDir, taskXML string) stackService {
	args := xmlElement(taskXML, "Arguments")
	dir := installDirFromCommand(args)
	if !strings.HasPrefix(args, "v2-supervise") || !sameDir(dir, installDir) {
		return nil
	}
	bin := strings.Trim(xmlElement(taskXML, "Command"), `"`)
	return &windowsService{taskXML: taskXML, o: installServiceOptions{InstallDir: dir, ServiceName: supervisorServiceName, OrchestratorBin: bin}}
}

func (s *windowsService) kind() string            { return kindWindows }
func (s *windowsService) title() string           { return "scheduled task " + s.o.ServiceName }
func (s *windowsService) orchestratorBin() string { return s.o.OrchestratorBin }
func (s *windowsService) helperName() string      { return helperServiceName(s.o.ServiceName) }

// helperXML: started on demand and at boot (it exits at once when there is
// nothing to do). While there is work it is armed: Task Scheduler starts it
// again within a minute when it was killed, which RestartOnFailure does not do.
func (s *windowsService) helperXML(bin string, armed bool) string {
	return windowsTaskXML(windowsTaskSpec{
		Description: "NCC Orchestrator update helper (does nothing unless an upgrade is in progress)",
		Command:     bin,
		Arguments:   `upgrade-helper --install-dir "` + s.o.InstallDir + `"`,
		WorkingDir:  s.o.InstallDir,
		Retries:     5,
		EveryMinute: armed,
	})
}

func (s *windowsService) helperCurrent(bin string) bool {
	x, ok := queryWindowsTaskXML(s.helperName())
	if !ok {
		return false
	}
	// Task Scheduler rewrites the document; compare what matters.
	want := s.helperXML(bin, false)
	for _, el := range []string{"Command", "Arguments", "WorkingDirectory"} {
		if xmlElement(x, el) != xmlElement(want, el) {
			return false
		}
	}
	// It leaves out defaults: a plain boot trigger is stored as <BootTrigger />.
	return strings.Contains(x, "<BootTrigger") && strings.Contains(x, "<RestartOnFailure>")
}

func (s *windowsService) installHelper(bin string) error {
	return registerWindowsTaskXML(s.helperName(), s.helperXML(bin, false))
}

func (s *windowsService) removeHelper() {
	_, _ = svcCommand("schtasks", "/End", "/TN", s.helperName())
	_, _ = svcCommand("schtasks", "/Delete", "/TN", s.helperName(), "/F")
}

func (s *windowsService) wakeHelper() error {
	if err := registerWindowsTaskXML(s.helperName(), s.helperXML(helperBinaryPath(s.o.InstallDir), true)); err != nil {
		return err
	}
	_, err := svcCommand("schtasks", "/Run", "/TN", s.helperName())
	return err
}

// disarmHelper ends the starts every minute. The helper calls it when it has
// nothing left to do; its own run goes on.
func (s *windowsService) disarmHelper() {
	if x, ok := queryWindowsTaskXML(s.helperName()); ok && strings.Contains(x, "<Repetition>") {
		_ = registerWindowsTaskXML(s.helperName(), s.helperXML(helperBinaryPath(s.o.InstallDir), false))
	}
}

func (s *windowsService) probe() error {
	if _, ok := queryWindowsTaskXML(s.o.ServiceName); !ok {
		return fmt.Errorf("cannot read the scheduled task %s", s.o.ServiceName)
	}
	// Registering the helper's own task again proves the right to register.
	x, ok := queryWindowsTaskXML(s.helperName())
	if !ok {
		return fmt.Errorf("cannot read the scheduled task %s", s.helperName())
	}
	return registerWindowsTaskXML(s.helperName(), x)
}

func (s *windowsService) privileged() error { return windowsElevated() }
func (s *windowsService) running() bool {
	return supervisorRuns(s.o.InstallDir, s.o.OrchestratorBin)
}

func (s *windowsService) stop() error {
	// /End first: a task that is ended is not restarted by Task Scheduler.
	_, endErr := svcCommand("schtasks", "/End", "/TN", s.o.ServiceName)
	// The servers (or a supervisor that did not obey) may outlive it.
	_ = runV2Stop(v2StopOptions{InstallDir: s.o.InstallDir, StopTimeout: 15 * time.Second})
	if !waitUntil(10*time.Second, func() bool { return !s.running() }) {
		return errors.Join(fmt.Errorf("%s is still running", s.title()), endErr)
	}
	return nil
}

func (s *windowsService) start() error {
	// Right after /End, Task Scheduler can still think the task runs, and then
	// ignores /Run without an error.
	var err error
	for i := 0; i < 6; i++ {
		if _, err = svcCommand("schtasks", "/Run", "/TN", s.o.ServiceName); err == nil &&
			waitUntil(10*time.Second, s.running) {
			return nil
		}
	}
	if err == nil {
		err = fmt.Errorf("%s did not start", s.title())
	}
	return err
}

// planWindowsTask compares the installed supervisor task with ours.
func planWindowsTask(o installServiceOptions, installed string) defChange {
	c := defChange{ID: taskDefPrefix + o.ServiceName, Title: "Scheduled task " + o.ServiceName, body: windowsSupervisorTaskXML(o)}
	user := strings.ToUpper(xmlElement(installed, "UserId"))
	gaps := windowsTaskGaps(installed)
	switch {
	case user != "S-1-5-18" && user != "SYSTEM" && !strings.HasSuffix(user, `\SYSTEM`):
		c.Action = defKeep
		c.Detail = "it runs as another user on this host"
	case strings.Count(installed, "<Exec>") != 1 || xmlElement(installed, "Arguments") != xmlElement(c.body, "Arguments"):
		c.Action = defKeep
		c.Detail = "its command was changed on this host"
	case len(gaps) == 0:
		c.Action = defUnchanged
	default:
		c.Action = defReplace
		c.Detail = strings.Join(gaps, "; ")
	}
	return c
}

func (s *windowsService) plan() []defChange {
	x, ok := queryWindowsTaskXML(s.o.ServiceName)
	if !ok {
		x = s.taskXML
	}
	main := planWindowsTask(s.o, x)
	out := []defChange{main}
	if main.Action == defKeep {
		return out // not started as we would: nothing is added to it
	}
	w := defChange{ID: taskDefPrefix + watchdogTaskName(s.o.ServiceName), Title: "Watchdog task", Action: defUnchanged, body: windowsWatchdogTaskXML(s.o)}
	switch x, ok := queryWindowsTaskXML(watchdogTaskName(s.o.ServiceName)); {
	case !ok:
		w.Action, w.Detail = defCreate, "starts the supervisor again when it has died"
	case !watchdogTaskCurrent(x, s.o):
		w.Action = defReplace
	}
	return append(out, w)
}

func (s *windowsService) migrate() ([]defChange, error) {
	changes := s.plan()
	for _, c := range changes {
		if c.pending() {
			if err := registerWindowsTaskXML(strings.TrimPrefix(c.ID, taskDefPrefix), c.body); err != nil {
				return changes, err
			}
		}
	}
	return changes, nil
}

func (s *windowsService) save() ([]savedDefinition, error) {
	x, ok := queryWindowsTaskXML(s.o.ServiceName)
	if !ok {
		return nil, fmt.Errorf("cannot read the scheduled task %s", s.o.ServiceName)
	}
	w, ok := queryWindowsTaskXML(watchdogTaskName(s.o.ServiceName))
	return []savedDefinition{
		{ID: taskDefPrefix + s.o.ServiceName, Body: x},
		{ID: taskDefPrefix + watchdogTaskName(s.o.ServiceName), Body: w, Missing: !ok},
	}, nil
}

func (s *windowsService) restore(defs []savedDefinition) error {
	for _, d := range defs {
		name := strings.TrimPrefix(d.ID, taskDefPrefix)
		watchdog := name == watchdogTaskName(s.o.ServiceName)
		if !strings.HasPrefix(d.ID, taskDefPrefix) || (name != s.o.ServiceName && !watchdog) || (d.Missing && !watchdog) {
			return fmt.Errorf("not the task of this service: %s", d.ID)
		}
		x, ok := queryWindowsTaskXML(name)
		switch {
		case d.Missing && ok:
			removeWatchdogTask(s.o.ServiceName)
		case d.Missing || (ok && x == d.Body):
		default:
			if err := registerWindowsTaskXML(name, d.Body); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *windowsService) holdRestarts() func() { return func() {} }

// ---- macOS launchd ----

type launchdService struct {
	o    installServiceOptions
	dir  string
	path string
}

var (
	plistString = regexp.MustCompile(`(?s)<string>(.*?)</string>`)
	plistKey    = regexp.MustCompile(`<key>(.*?)</key>`)
)

// plistProgram returns the ProgramArguments of a plist.
func plistProgram(plist string) []string {
	_, rest, ok := strings.Cut(plist, "<key>ProgramArguments</key>")
	if !ok {
		return nil
	}
	rest, _, ok = strings.Cut(rest, "</array>")
	if !ok {
		return nil
	}
	var out []string
	for _, m := range plistString.FindAllStringSubmatch(rest, -1) {
		out = append(out, strings.TrimSpace(xmlUnescaper.Replace(m[1])))
	}
	return out
}

func findLaunchdService(installDir, dir string) stackService {
	paths, _ := filepath.Glob(filepath.Join(dir, launchdLabelPrefix+"*.plist"))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		args := plistProgram(string(b))
		if len(args) < 4 || args[1] != "v2-supervise" || args[2] != "--install-dir" || !sameDir(args[3], installDir) {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), launchdLabelPrefix), ".plist")
		return &launchdService{path: p, dir: dir, o: installServiceOptions{InstallDir: args[3], ServiceName: name, OrchestratorBin: args[0]}}
	}
	return nil
}

func (s *launchdService) kind() string            { return kindLaunchd }
func (s *launchdService) title() string           { return launchdLabel(s.o.ServiceName) }
func (s *launchdService) orchestratorBin() string { return s.o.OrchestratorBin }
func (s *launchdService) helperLabel() string {
	return launchdLabel(helperServiceName(s.o.ServiceName))
}
func (s *launchdService) helperPlistPath() string {
	return filepath.Join(s.dir, s.helperLabel()+".plist")
}

// helperPlist: launchd runs the helper only while the wake file exists.
func (s *launchdService) helperPlist(bin string) string {
	esc := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace
	return strings.Join([]string{
		`<?xml version="1.0" encoding="UTF-8"?>`,
		`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">`,
		`<plist version="1.0">`,
		`<dict>`,
		`  <key>Label</key>`,
		`  <string>` + s.helperLabel() + `</string>`,
		`  <key>ProgramArguments</key>`,
		`  <array>`,
		`    <string>` + esc(bin) + `</string>`,
		`    <string>upgrade-helper</string>`,
		`    <string>--install-dir</string>`,
		`    <string>` + esc(s.o.InstallDir) + `</string>`,
		`  </array>`,
		`  <key>KeepAlive</key>`,
		`  <dict><key>PathState</key><dict><key>` + esc(helperWakePath(s.o.InstallDir)) + `</key><true/></dict></dict>`,
		`  <key>ThrottleInterval</key>`,
		`  <integer>10</integer>`,
		`  <key>StandardErrorPath</key>`,
		`  <string>` + esc(helperLogPath(s.o.InstallDir)) + `</string>`,
		`</dict>`,
		`</plist>`,
		``,
	}, "\n")
}

func (s *launchdService) helperCurrent(bin string) bool {
	b, err := os.ReadFile(s.helperPlistPath())
	return err == nil && string(b) == s.helperPlist(bin)
}

// launchctlLoad (re)loads a daemon.
func launchctlLoad(label, path string) error {
	_, _ = svcCommand("launchctl", "enable", "system/"+label)
	_, _ = svcCommand("launchctl", "bootout", "system/"+label)
	// bootout returns before the old job is gone.
	var err error
	for i := 0; i < 20; i++ {
		if _, err = svcCommand("launchctl", "bootstrap", "system", path); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

func (s *launchdService) installHelper(bin string) error {
	if err := writeDefinitionFile(s.helperPlistPath(), s.helperPlist(bin)); err != nil {
		return err
	}
	return launchctlLoad(s.helperLabel(), s.helperPlistPath())
}

func (s *launchdService) removeHelper() {
	_, _ = svcCommand("launchctl", "bootout", "system/"+s.helperLabel())
	_ = os.Remove(s.helperPlistPath())
}

func (s *launchdService) wakeHelper() error {
	_, err := svcCommand("launchctl", "kickstart", "system/"+s.helperLabel())
	return err
}

func (s *launchdService) probe() error {
	if err := dirWritable(s.dir); err != nil {
		return fmt.Errorf("cannot write service definitions: %w", err)
	}
	_, err := svcCommand("launchctl", "print", "system/"+s.title())
	return err
}

func (s *launchdService) privileged() error {
	if os.Geteuid() != 0 {
		return errors.New("changing the service needs root (sudo)")
	}
	return nil
}

func (s *launchdService) running() bool {
	return supervisorRuns(s.o.InstallDir, s.o.OrchestratorBin)
}

func (s *launchdService) stop() error {
	_, err := svcCommand("launchctl", "bootout", "system/"+s.title())
	if !waitUntil(40*time.Second, func() bool { return !s.running() }) {
		return errors.Join(fmt.Errorf("%s is still running", s.title()), err)
	}
	return nil
}

func (s *launchdService) start() error { return launchctlLoad(s.title(), s.path) }

func (s *launchdService) plan() []defChange {
	c := defChange{ID: fileDefPrefix + s.path, Title: "Daemon " + s.title(), body: launchdSupervisorPlist(s.o)}
	b, _ := os.ReadFile(s.path)
	have := string(b)
	known := map[string]bool{}
	for _, m := range plistKey.FindAllStringSubmatch(c.body, -1) {
		known[m[1]] = true
	}
	var extra []string
	for _, m := range plistKey.FindAllStringSubmatch(have, -1) {
		if !known[m[1]] {
			extra = append(extra, m[1])
		}
	}
	switch {
	case have == c.body:
		c.Action = defUnchanged
	case len(extra) > 0:
		c.Action = defKeep
		c.Detail = "changed on this host: " + strings.Join(extra, ", ")
	default:
		c.Action = defReplace
	}
	return []defChange{c}
}

func (s *launchdService) migrate() ([]defChange, error) {
	changes := s.plan()
	for _, c := range changes {
		if c.pending() {
			if err := writeDefinitionFile(s.path, c.body); err != nil {
				return changes, err
			}
		}
	}
	return changes, nil
}

func (s *launchdService) save() ([]savedDefinition, error) {
	d, err := saveFileDefinition(s.path)
	return []savedDefinition{d}, err
}

func (s *launchdService) restore(defs []savedDefinition) error {
	for _, d := range defs {
		if d.ID != fileDefPrefix+s.path || d.Missing {
			return fmt.Errorf("not the plist of this service: %s", d.ID)
		}
		if err := restoreFileDefinition(d); err != nil {
			return err
		}
	}
	return nil
}

func (s *launchdService) holdRestarts() func() { return func() {} }
