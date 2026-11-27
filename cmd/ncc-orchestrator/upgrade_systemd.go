package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// systemdService is a stack that runs as a systemd service.
type systemdService struct {
	o       installServiceOptions
	unitDir string
}

// unitExecStart returns a unit's ExecStart command line ("" when it has none).
func unitExecStart(unit string) string {
	for _, l := range unitLines(unit) {
		if l.section == "[Service]" && l.key() == "ExecStart" {
			_, v, _ := strings.Cut(l.text, "=")
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func findSystemdService(installDir, unitDir string) stackService {
	names := []string{filepath.Join(unitDir, supervisorServiceName+".service")}
	if more, err := filepath.Glob(filepath.Join(unitDir, "*.service")); err == nil {
		names = append(names, more...)
	}
	for _, p := range names {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		cmd := unitExecStart(string(b))
		if !strings.Contains(cmd, " v2-supervise") || !sameDir(installDirFromCommand(cmd), installDir) {
			continue
		}
		o := installServiceOptions{
			// As the unit spells it, so that its lines compare equal.
			InstallDir:      installDirFromCommand(cmd),
			ServiceName:     strings.TrimSuffix(filepath.Base(p), ".service"),
			OrchestratorBin: strings.Trim(strings.Fields(cmd)[0], `"'`),
		}
		// The user it is to run as: what the operator chose, else what the
		// unit says, else ncc (LIN-02).
		o.ServiceUser, o.userNote = desiredServiceUser(o.InstallDir, string(b))
		return &systemdService{unitDir: unitDir, o: o}
	}
	return nil
}

func (s *systemdService) kind() string            { return kindSystemd }
func (s *systemdService) title() string           { return s.o.ServiceName + ".service" }
func (s *systemdService) orchestratorBin() string { return s.o.OrchestratorBin }
func (s *systemdService) unit(name string) string { return filepath.Join(s.unitDir, name) }
func (s *systemdService) helperName() string      { return helperServiceName(s.o.ServiceName) }

// helperServiceName is the name of the update helper's own service or task.
func helperServiceName(service string) string {
	if strings.TrimSpace(service) == "" {
		service = supervisorServiceName
	}
	return service + "-upgrade"
}

// systemdArg quotes one argument or path for a unit file.
func systemdArg(v string) string {
	v = strings.ReplaceAll(v, "%", "%%")
	if !strings.ContainsAny(v, " \t\"'\\;$") {
		return v
	}
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "$", "$$").Replace(v) + `"`
}

// helperUnit is the helper's service. It gets the process hardening only: it
// writes units and calls systemctl.
func (s *systemdService) helperUnit(bin string) string {
	lines := []string{
		"[Unit]",
		"Description=NCC Orchestrator update helper (runs only during an upgrade)",
		"Documentation=https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
		"# An upgrade that keeps failing stops being retried.",
		"StartLimitIntervalSec=900",
		"StartLimitBurst=5",
		"",
		"[Service]",
		"Type=oneshot",
		fmt.Sprintf("ExecStart=%s upgrade-helper --install-dir %s", systemdArg(bin), systemdArg(s.o.InstallDir)),
		"TimeoutStartSec=infinity",
		"TimeoutStopSec=60",
		"OOMScoreAdjust=-500",
	}
	lines = append(lines, systemdProcessHardening()...)
	return strings.Join(append(lines, ""), "\n")
}

// helperPathUnit starts the helper while the wake file exists, also at boot.
func (s *systemdService) helperPathUnit() string {
	return strings.Join([]string{
		"[Unit]",
		"Description=Start the NCC Orchestrator update helper when an upgrade is requested",
		"Documentation=https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
		"",
		"[Path]",
		"PathExists=" + strings.ReplaceAll(filepath.ToSlash(helperWakePath(s.o.InstallDir)), "%", "%%"),
		"Unit=" + s.helperName() + ".service",
		"",
		"[Install]",
		"WantedBy=multi-user.target",
		"",
	}, "\n")
}

func (s *systemdService) helperUnits(bin string) map[string]string {
	return map[string]string{
		s.unit(s.helperName() + ".service"): s.helperUnit(bin),
		s.unit(s.helperName() + ".path"):    s.helperPathUnit(),
	}
}

func (s *systemdService) helperCurrent(bin string) bool {
	for path, body := range s.helperUnits(bin) {
		if b, err := os.ReadFile(path); err != nil || string(b) != body {
			return false
		}
	}
	return true
}

func (s *systemdService) installHelper(bin string) error {
	relabelPrograms(filepath.Dir(bin))
	for path, body := range s.helperUnits(bin) {
		if err := writeDefinitionFile(path, body); err != nil {
			return err
		}
	}
	if _, err := svcCommand("systemctl", "daemon-reload"); err != nil {
		return err
	}
	_, _ = svcCommand("systemctl", "reset-failed", s.helperName()+".service", s.helperName()+".path")
	_, err := svcCommand("systemctl", "enable", "--now", s.helperName()+".path")
	return err
}

func (s *systemdService) removeHelper() {
	_, _ = svcCommand("systemctl", "disable", "--now", s.helperName()+".path")
	_, _ = svcCommand("systemctl", "stop", s.helperName()+".service")
	for _, n := range []string{".path", ".service"} {
		_ = os.Remove(s.unit(s.helperName() + n))
	}
	_, _ = svcCommand("systemctl", "daemon-reload")
}

func (s *systemdService) wakeHelper() error {
	_, _ = svcCommand("systemctl", "reset-failed", s.helperName()+".service", s.helperName()+".path")
	_, _ = svcCommand("systemctl", "start", s.helperName()+".path")
	_, err := svcCommand("systemctl", "start", "--no-block", s.helperName()+".service")
	return err
}

func (s *systemdService) probe() error {
	if err := dirWritable(s.unitDir); err != nil {
		return fmt.Errorf("cannot write service definitions: %w", err)
	}
	out, err := svcCommand("systemctl", "show", "-p", "LoadState", s.title())
	if err != nil {
		return err
	}
	if !strings.Contains(out, "LoadState=loaded") {
		return fmt.Errorf("systemd does not know %s (%s)", s.title(), strings.TrimSpace(out))
	}
	return nil
}

func (s *systemdService) privileged() error {
	if os.Geteuid() != 0 {
		return errors.New("changing the service needs root")
	}
	if !systemctlAvailable() {
		return errors.New("systemd is not running here")
	}
	return nil
}

func (s *systemdService) running() bool {
	out, err := svcCommand("systemctl", "is-active", s.title())
	return err == nil && strings.TrimSpace(out) == "active"
}

func (s *systemdService) stop() error {
	_, err := svcCommand("systemctl", "stop", s.title())
	return err
}

func (s *systemdService) start() error {
	relabelPrograms(filepath.Dir(s.o.OrchestratorBin), filepath.Join(s.o.InstallDir, "bin"))
	_, _ = svcCommand("systemctl", "daemon-reload")
	_, _ = svcCommand("systemctl", "reset-failed", s.title())
	_, err := svcCommand("systemctl", "restart", s.title())
	return err
}

// relabelPrograms makes replaced programs runnable under SELinux. Only folders
// named bin or helper: never a home directory as a whole.
func relabelPrograms(dirs ...string) {
	if runtime.GOOS != "linux" {
		return
	}
	if _, err := exec.LookPath("chcon"); err != nil {
		return
	}
	for _, d := range dirs {
		if base := filepath.Base(d); (base == "bin" || base == "helper") && existingDir(d) {
			_, _ = svcCommand("chcon", "-R", "-t", "bin_t", d)
		}
	}
}

// scheduleUnit rebuilds a schedule's service from the installed one, for the
// service user.
func scheduleUnit(installed, user string) (service, workDir string, ok bool) {
	const descPrefix = "Description=NCC Orchestrator scheduled run ("
	var task, dir, script, marker string
	for _, raw := range strings.Split(installed, "\n") {
		l := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(l, "# ncc-orchestrator:"):
			marker = strings.TrimPrefix(l, "# ")
		case strings.HasPrefix(l, descPrefix):
			task = strings.TrimSuffix(strings.TrimPrefix(l, descPrefix), ")")
		case strings.HasPrefix(l, "WorkingDirectory="):
			dir = strings.TrimPrefix(l, "WorkingDirectory=")
		case strings.HasPrefix(l, "ExecStart=/bin/sh "):
			script = strings.TrimPrefix(l, "ExecStart=/bin/sh ")
		}
	}
	if task == "" || dir == "" || script == "" || marker == "" {
		return "", "", false
	}
	service, _ = buildSystemdScheduleUnitsFor(task, "", script, dir, marker, user)
	return service, dir, true
}

// savedScheduleTask is the name of the saved schedule ("" when there is none).
func savedScheduleTask(installDir string) string {
	b, err := os.ReadFile(filepath.Join(installDir, ".ncc-api-schedule.json"))
	if err != nil {
		return ""
	}
	var st map[string]interface{}
	if json.Unmarshal(b, &st) != nil {
		return ""
	}
	if t := scheduleStateString(st, "task_name"); t != "" {
		return t
	}
	return supervisorServiceName
}

// unprivileged: the stack is to run as a user of its own.
func (s *systemdService) unprivileged() bool { return !rootService(s.o.ServiceUser) }

// plan looks at the service itself and at what belongs to it: the update
// watcher, the log timer, the saved schedule, and for an unprivileged service
// the service helper and the owners of the install.
func (s *systemdService) plan(replace bool) []defChange {
	read := func(path string) string {
		b, _ := os.ReadFile(path)
		return string(b)
	}
	dir := s.o.InstallDir
	main := s.unit(s.title())
	out := []defChange{planUnit("Service "+s.title(), main, read(main), systemdSupervisorUnit(s.o), dir)}
	// Installs from before 2.4.0 have no log timer, and no sandboxing either.
	// Later it is not put back: an administrator may have removed it.
	beforeLogTimer := !strings.Contains(read(main), "NoNewPrivileges=") &&
		!fileExists(s.unit(logMaintenanceUnitName+".service")) && !fileExists(s.unit(logMaintenanceUnitName+".timer"))
	watcher := applyUpdateWatcherName(s.o.ServiceName)
	ops := opsServiceName(s.o.ServiceName)
	for _, u := range []struct {
		name, title, body string
		add               bool
	}{
		{watcher + ".path", "Update watcher", systemdApplyUpdatePathUnit(s.o), false},
		{watcher + ".service", "Update watcher service", systemdApplyUpdateOneshotUnit(s.o), false},
		{logMaintenanceUnitName + ".service", "Log maintenance", systemdLogMaintenanceServiceUnit(s.o), beforeLogTimer},
		{logMaintenanceUnitName + ".timer", "Log maintenance timer", systemdLogMaintenanceTimerUnit(), beforeLogTimer},
		{ops + ".service", "Service helper", s.opsUnit(), s.unprivileged()},
		{ops + ".path", "Service helper trigger", s.opsPathUnit(), s.unprivileged()},
	} {
		if have := read(s.unit(u.name)); have != "" || u.add {
			out = append(out, planUnit(u.title, s.unit(u.name), have, u.body, dir))
		}
	}
	if task := savedScheduleTask(dir); task != "" {
		path := s.unit(systemdUnitBase(task) + ".service")
		if have := read(path); have != "" {
			if want, workDir, ok := scheduleUnit(have, s.o.ServiceUser); ok {
				out = append(out, planUnit("Schedule "+task, path, have, want, dir, workDir))
			}
		}
	}
	if s.unprivileged() {
		c := defChange{ID: ownerDefPrefix + dir, Title: "File owners", Action: defReplace, Detail: ownershipPlanDetail(s.o.ServiceUser)}
		if uid, gid, ok := lookupServiceIDs(s.o.ServiceUser); ok && ownershipCurrent(dir, uid, gid) {
			c.Action, c.Detail = defUnchanged, ""
		} else if !ok {
			c.Detail += "; the system user " + s.o.ServiceUser + " is created"
		}
		out = append(out, c)
		if c, ok := s.cronScheduleToMove(); ok {
			out = append(out, c)
		}
	}
	if replace {
		out = replaceEdited(out)
	}
	return out
}

// runActive names the saved schedule's run when its service is running now.
func (s *systemdService) runActive() string {
	task := savedScheduleTask(s.o.InstallDir)
	if task == "" {
		return ""
	}
	unit := systemdUnitBase(task) + ".service"
	if !fileExists(s.unit(unit)) {
		return ""
	}
	if out, err := svcCommand("systemctl", "is-active", unit); err == nil && strings.TrimSpace(out) == "active" {
		return "the scheduled run " + task
	}
	return ""
}

func (s *systemdService) migrate(replace bool) ([]defChange, error) {
	if s.unprivileged() {
		// The user first: the units name its group.
		if _, err := ensureServiceUser(s.o.ServiceUser); err != nil {
			return nil, err
		}
	}
	changes := s.plan(replace)
	for _, c := range changes {
		if !strings.HasPrefix(c.ID, fileDefPrefix) {
			continue
		}
		if err := applyUnit(c); err != nil {
			return changes, err
		}
	}
	if s.unprivileged() {
		// Always: what root wrote into the install since (a restored
		// configuration, say) goes to the service user too.
		uid, gid, _ := lookupServiceIDs(s.o.ServiceUser)
		if err := applyServiceOwnership(s.o.InstallDir, uid, gid); err != nil {
			return changes, fmt.Errorf("give the install to %s: %w", s.o.ServiceUser, err)
		}
	}
	// Refuse a unit systemd cannot load before the stack depends on it.
	if _, err := svcCommand("systemctl", "daemon-reload"); err != nil {
		return changes, err
	}
	out, err := svcCommand("systemctl", "show", "-p", "LoadState", "-p", "LoadError", s.title())
	if err != nil {
		return changes, err
	}
	if !strings.Contains(out, "LoadState=loaded") {
		return changes, fmt.Errorf("systemd does not accept the new %s: %s", s.title(), strings.Join(strings.Fields(out), " "))
	}
	for _, c := range changes {
		name := strings.TrimPrefix(c.ID, fileDefPrefix)
		if c.Action == defCreate && strings.HasPrefix(c.ID, fileDefPrefix) && (strings.HasSuffix(name, ".timer") || strings.HasSuffix(name, ".path")) {
			if _, err := svcCommand("systemctl", "enable", "--now", filepath.Base(name)); err != nil {
				return changes, err
			}
		}
	}
	for _, c := range changes {
		if task, ok := strings.CutPrefix(c.ID, cronDefPrefix); ok {
			if err := s.moveCronSchedule(task); err != nil {
				return changes, err
			}
		}
	}
	return changes, nil
}

func (s *systemdService) save() ([]savedDefinition, error) {
	var out []savedDefinition
	for _, c := range s.plan(false) {
		if strings.HasPrefix(c.ID, ownerDefPrefix) {
			rec, err := captureOwnership(s.o.InstallDir)
			if err != nil {
				return nil, fmt.Errorf("record the owners of %s: %w", s.o.InstallDir, err)
			}
			out = append(out, savedDefinition{ID: c.ID, Body: rec.encode()})
			continue
		}
		if task, ok := strings.CutPrefix(c.ID, cronDefPrefix); ok {
			d, err := saveCron(task)
			if err != nil {
				return nil, err
			}
			out = append(out, d)
			continue
		}
		path := strings.TrimPrefix(c.ID, fileDefPrefix)
		for _, p := range []string{path, unitDropInPath(path)} {
			d, err := saveFileDefinition(p)
			if err != nil {
				return nil, err
			}
			out = append(out, d)
		}
	}
	return out, nil
}

func (s *systemdService) restore(defs []savedDefinition) error {
	var errs []error
	var owners []ownershipRecord
	var crons []savedDefinition
	for _, d := range defs {
		if dir, ok := strings.CutPrefix(d.ID, ownerDefPrefix); ok {
			if !sameDir(dir, s.o.InstallDir) {
				return fmt.Errorf("not this install: %s", d.ID)
			}
			rec, err := decodeOwnershipRecord(d.Body)
			if err != nil {
				return fmt.Errorf("the saved owners cannot be read: %w", err)
			}
			owners = append(owners, rec)
			continue
		}
		if strings.HasPrefix(d.ID, cronDefPrefix) {
			crons = append(crons, d)
			continue
		}
		path, ok := strings.CutPrefix(d.ID, fileDefPrefix)
		if !ok || filepath.Clean(path) != path || !pathWithin(path, s.unitDir) {
			return fmt.Errorf("not a service definition: %s", d.ID)
		}
		if d.Missing && (strings.HasSuffix(path, ".timer") || strings.HasSuffix(path, ".path")) && fileExists(path) {
			_, _ = svcCommand("systemctl", "disable", "--now", filepath.Base(path))
		}
		errs = append(errs, restoreFileDefinition(d))
	}
	for _, d := range crons {
		errs = append(errs, s.restoreCron(d))
	}
	// Last: the programs and the configuration are back already.
	for _, rec := range owners {
		errs = append(errs, restoreOwnership(s.o.InstallDir, rec))
	}
	_, err := svcCommand("systemctl", "daemon-reload")
	return errors.Join(append(errs, err)...)
}

func (s *systemdService) holdRestarts() func() {
	unit := applyUpdateWatcherName(s.o.ServiceName) + ".path"
	if !fileExists(s.unit(unit)) {
		return func() {}
	}
	_, _ = svcCommand("systemctl", "stop", unit)
	return func() { _, _ = svcCommand("systemctl", "start", unit) }
}
