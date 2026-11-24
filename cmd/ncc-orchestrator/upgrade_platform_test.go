package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// unitOf builds the unit that a release of the given age installed.
func unitOf(release string, o installServiceOptions) string {
	drop := map[string][]string{
		"2.3.0": append(append([]string{}, systemdProcessHardening()...), systemdKernelHardening()...),
	}
	drop["2.2.1"] = append([]string{"StartLimitIntervalSec=0", "LimitNOFILE=65536", "OOMScoreAdjust=-500"}, drop["2.3.0"]...)
	var out []string
	for _, l := range strings.Split(systemdSupervisorUnit(o), "\n") {
		keep := !strings.HasPrefix(l, "#")
		for _, d := range drop[release] {
			keep = keep && l != d
		}
		if keep {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestPlanUnit(t *testing.T) {
	o := installServiceOptions{InstallDir: "/opt/ncc", ServiceName: "ncc-orchestrator", OrchestratorBin: "/opt/ncc/bin/ncc-orchestrator"}
	desired := systemdSupervisorUnit(o)
	v221 := unitOf("2.2.1", o)
	after := func(unit, line, add string) string { return strings.Replace(unit, line, line+"\n"+add, 1) }
	for _, tc := range []struct{ name, installed, action, detail, dropIn string }{
		{"same", desired, defUnchanged, "", ""},
		{"windows line ends", strings.ReplaceAll(desired, "\n", "\r\n"), defUnchanged, "", ""},
		{"2.3.0", unitOf("2.3.0", o), defReplace, "", ""},
		{"2.2.1", v221, defReplace, "", ""},
		{"missing", "", defCreate, "", ""},
		{"own environment", after(v221, "[Service]", "Environment=HTTPS_PROXY=http://proxy:3128"), defMerge, "Environment", "[Service]\nEnvironment=HTTPS_PROXY=http://proxy:3128\n"},
		{"own value", strings.Replace(v221, "RestartSec=3", "RestartSec=30", 1), defMerge, "RestartSec", "[Service]\nRestartSec=30\n"},
		{"own dependency", after(v221, "[Unit]", "RequiresMountsFor=/data"), defMerge, "RequiresMountsFor", "[Unit]\nRequiresMountsFor=/data\n"},
		{"other user", after(v221, "[Service]", "User=ncc"), defKeep, "User", ""},
		{"other command", strings.Replace(v221, "v2-supervise", "v2-supervise --x", 1), defKeep, "ExecStart", ""},
		{"other target", strings.Replace(v221, "WantedBy=multi-user.target", "WantedBy=graphical.target", 1), defKeep, "WantedBy", ""},
		{"continued line", after(v221, "[Service]", "Environment=A=1 \\\n  B=2"), defMerge, "Environment", "Environment=A=1 B=2\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := planUnit("Service", filepath.Join(t.TempDir(), "ncc.service"), tc.installed, desired)
			if c.Action != tc.action || !strings.Contains(c.Detail, tc.detail) || !strings.Contains(c.dropIn, tc.dropIn) {
				t.Errorf("%s, detail %q, drop-in %q", c.Action, c.Detail, c.dropIn)
			}
			if err := applyUnit(c); err != nil {
				t.Fatal(err)
			}
			path := strings.TrimPrefix(c.ID, fileDefPrefix)
			b, _ := os.ReadFile(path)
			if c.pending() != (string(b) == desired) {
				t.Errorf("after %s the unit is\n%s", c.Action, b)
			}
			if fileExists(unitDropInPath(path)) != (tc.action == defMerge) {
				t.Error("drop-in")
			}
			if c.pending() {
				if again := planUnit("Service", path, string(b), desired); again.Action != defUnchanged {
					t.Errorf("a second run would %s", again.Action)
				}
			}
		})
	}
}

func TestDefinitionSaveAndRestore(t *testing.T) {
	dir := t.TempDir()
	unit := filepath.Join(dir, "ncc.service")
	writeTestFile(t, unit, "old")
	a, err1 := saveFileDefinition(unit)
	b, err2 := saveFileDefinition(unitDropInPath(unit))
	if err1 != nil || err2 != nil || !b.Missing {
		t.Fatal(err1, err2, b)
	}
	writeTestFile(t, unit, "new")
	writeTestFile(t, unitDropInPath(unit), "new")
	for _, d := range []savedDefinition{a, b, a} {
		if err := restoreFileDefinition(d); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := os.ReadFile(unit); string(got) != "old" {
		t.Errorf("unit = %q", got)
	}
	if existingDir(unit + ".d") {
		t.Error("the drop-in folder stays")
	}
}

// fakeSvcCommands replaces the service manager's commands.
func fakeSvcCommands(t *testing.T, answer func(cmd string) (string, error)) *[]string {
	t.Helper()
	var calls []string
	old := svcCommand
	svcCommand = func(name string, args ...string) (string, error) {
		cmd := name + " " + strings.Join(args, " ")
		calls = append(calls, cmd)
		if answer != nil {
			return answer(cmd)
		}
		return "", nil
	}
	t.Cleanup(func() { svcCommand = old })
	return &calls
}

func TestSystemdService(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX paths")
	}
	dir, units := t.TempDir(), t.TempDir()
	o := installServiceOptions{InstallDir: dir, ServiceName: "my-ncc", OrchestratorBin: filepath.Join(dir, "bin", "ncc-orchestrator")}
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	if findSystemdService(dir, units) != nil {
		t.Fatal("found a service that is not there")
	}
	other := o
	other.InstallDir = "/somewhere/else"
	writeTestFile(t, filepath.Join(units, "ncc-orchestrator.service"), systemdSupervisorUnit(other))
	writeTestFile(t, filepath.Join(units, "my-ncc.service"), unitOf("2.2.1", o))
	writeTestFile(t, filepath.Join(units, "my-ncc-apply-update.path"), systemdApplyUpdatePathUnit(o))
	writeTestFile(t, filepath.Join(units, "my-ncc-apply-update.service"), strings.ReplaceAll(systemdApplyUpdateOneshotUnit(o), "NoNewPrivileges=yes\n", ""))
	writeTestFile(t, filepath.Join(units, logMaintenanceUnitName+".service"), strings.Replace(systemdLogMaintenanceServiceUnit(o), "ExecStart=", "ExecStart=/usr/bin/nice ", 1))
	sched, _ := buildSystemdScheduleUnits("nightly", "*-*-* 02:00:00", "/opt/ncc/logs/run.sh", "/opt/ncc", "ncc-orchestrator:nightly")
	writeTestFile(t, filepath.Join(units, "ncc-sched-nightly.service"), strings.ReplaceAll(sched, "LockPersonality=yes\n", ""))
	writeTestFile(t, filepath.Join(dir, ".ncc-api-schedule.json"), `{"task_name":"nightly"}`)

	svc, _ := findSystemdService(dir, units).(*systemdService)
	if svc == nil || svc.o.ServiceName != "my-ncc" || svc.orchestratorBin() != o.OrchestratorBin {
		t.Fatalf("found %+v", svc)
	}
	calls := fakeSvcCommands(t, func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "systemctl show") {
			return "LoadState=loaded\nLoadError=\n", nil
		}
		return "", nil
	})
	saved, err := svc.save()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	changes, err := svc.migrate()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		got[filepath.Base(c.ID)] = c.Action
	}
	want := map[string]string{"my-ncc.service": defReplace, "my-ncc-apply-update.path": defUnchanged, "my-ncc-apply-update.service": defReplace,
		logMaintenanceUnitName + ".service": defKeep, "ncc-sched-nightly.service": defReplace}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %s, want %s", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("changes %v", got)
	}
	if b, _ := os.ReadFile(filepath.Join(units, "ncc-sched-nightly.service")); string(b) != sched {
		t.Errorf("schedule:\n%s", b)
	}
	if err := svc.restore(saved); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(units, "my-ncc.service")); string(b) != unitOf("2.2.1", o) {
		t.Error("the old unit was not put back")
	}
	for _, id := range []string{"file:/etc/passwd", "file:" + units + "/../x", "task:x"} {
		if svc.restore([]savedDefinition{{ID: id, Body: "x"}}) == nil {
			t.Errorf("restored %s", id)
		}
	}

	bin := helperBinaryPath(dir)
	writeTestFile(t, bin, "x")
	if svc.helperCurrent(bin) {
		t.Error("the helper is not installed yet")
	}
	if err := svc.installHelper(bin); err != nil || !svc.helperCurrent(bin) {
		t.Fatal(err)
	}
	unit, _ := os.ReadFile(filepath.Join(units, "my-ncc-upgrade.service"))
	path, _ := os.ReadFile(filepath.Join(units, "my-ncc-upgrade.path"))
	for _, s := range []string{"ExecStart=" + bin + " upgrade-helper --install-dir " + dir, "Type=oneshot", "NoNewPrivileges=yes", "StartLimitBurst=5"} {
		if !strings.Contains(string(unit), s) {
			t.Errorf("no %q in\n%s", s, unit)
		}
	}
	if !strings.Contains(string(path), "PathExists="+helperWakePath(dir)+"\nUnit=my-ncc-upgrade.service") {
		t.Errorf("path unit:\n%s", path)
	}
	if !strings.Contains(strings.Join(*calls, "\n"), "systemctl enable --now my-ncc-upgrade.path") {
		t.Errorf("calls %v", *calls)
	}
	svc.removeHelper()
	if fileExists(filepath.Join(units, "my-ncc-upgrade.path")) || fileExists(filepath.Join(units, "my-ncc-upgrade.service")) {
		t.Error("the helper's units stay")
	}

	// An install from before the log timer gets it, and a rollback removes it.
	timer := filepath.Join(units, logMaintenanceUnitName+".timer")
	_ = os.Remove(filepath.Join(units, logMaintenanceUnitName+".service"))
	calls = fakeSvcCommands(t, func(string) (string, error) { return "LoadState=loaded\n", nil })
	if saved, err = svc.save(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.migrate(); err != nil || !fileExists(timer) || !strings.Contains(strings.Join(*calls, "\n"), "enable --now "+logMaintenanceUnitName+".timer") {
		t.Fatalf("no log timer (%v), calls %v", err, *calls)
	}
	if err := svc.restore(saved); err != nil || fileExists(timer) || !strings.Contains(strings.Join(*calls, "\n"), "disable --now "+logMaintenanceUnitName+".timer") {
		t.Fatalf("the log timer stays (%v)", err)
	}

	// A unit that systemd refuses is an error, before anything starts.
	fakeSvcCommands(t, func(cmd string) (string, error) {
		return "LoadState=bad-setting\n", nil
	})
	if _, err := svc.migrate(); err == nil {
		t.Error("a unit that cannot be loaded was accepted")
	}
}

func TestSystemdArg(t *testing.T) {
	for in, want := range map[string]string{"/opt/ncc": "/opt/ncc", "/opt/my ncc": `"/opt/my ncc"`, "/a%b": "/a%%b", `/a"$b c`: `"/a\"$$b c"`} {
		if got := systemdArg(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}

func TestWindowsServicePlan(t *testing.T) {
	o := installServiceOptions{InstallDir: `C:\NCC Data`, ServiceName: supervisorServiceName, OrchestratorBin: `C:\NCC Data\bin\ncc-orchestrator.exe`}
	ours := windowsSupervisorTaskXML(o)
	legacy := strings.Replace(ours, "    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>\r\n", "", 1)
	legacy = strings.Replace(legacy, "PT0S", "PT72H", 1)
	for name, tc := range map[string]struct{ xml, action string }{
		"same":           {ours, defUnchanged},
		"2.2.1":          {legacy, defReplace},
		"other user":     {strings.Replace(legacy, "S-1-5-18", `HOST\ncc`, 1), defKeep},
		"other command":  {strings.Replace(legacy, "v2-supervise", "v2-supervise --x", 1), defKeep},
		"second command": {strings.Replace(legacy, "</Exec>", "</Exec><Exec><Command>x</Command></Exec>", 1), defKeep},
	} {
		if c := planWindowsTask(o, tc.xml); c.Action != tc.action {
			t.Errorf("%s: %s (%s), want %s", name, c.Action, c.Detail, tc.action)
		}
	}
	if runtime.GOOS != "windows" {
		return
	}
	svc, _ := windowsServiceFromTask(`c:\ncc data`, ours).(*windowsService)
	if svc == nil || svc.orchestratorBin() != o.OrchestratorBin {
		t.Fatalf("found %+v", svc)
	}
	if windowsServiceFromTask(`C:\Other`, ours) != nil {
		t.Error("found the service of another folder")
	}
	if armed := svc.helperXML(`C:\x.exe`, true); !strings.Contains(armed, "<BootTrigger>") || strings.Count(armed, "<Repetition>") != 1 {
		t.Errorf("not started again while there is work:\n%s", armed)
	}
	x := svc.helperXML(`C:\NCC Data\helper\ncc-upgrade-helper.exe`, false)
	if strings.Contains(x, "<Repetition>") {
		t.Error("the dormant helper is started every minute")
	}
	for _, s := range []string{"<UserId>S-1-5-18</UserId>", "<BootTrigger>", "<Count>5</Count>", `<Arguments>upgrade-helper --install-dir "C:\NCC Data"</Arguments>`} {
		if !strings.Contains(x, s) {
			t.Errorf("no %s in\n%s", s, x)
		}
	}
}

func TestLaunchdService(t *testing.T) {
	daemons := t.TempDir()
	dir := t.TempDir()
	o := installServiceOptions{InstallDir: dir, ServiceName: "ncc-orchestrator", OrchestratorBin: filepath.Join(dir, "bin", "ncc-orchestrator")}
	plist := launchdSupervisorPlist(o)
	path := filepath.Join(daemons, launchdLabel(o.ServiceName)+".plist")
	writeTestFile(t, path, strings.Replace(plist, "  <key>ThrottleInterval</key>\n  <integer>10</integer>\n", "", 1))
	svc, _ := findLaunchdService(dir, daemons).(*launchdService)
	if svc == nil || svc.o.ServiceName != "ncc-orchestrator" {
		t.Fatalf("found %+v", svc)
	}
	calls := fakeSvcCommands(t, nil)
	saved, err := svc.save()
	if err != nil {
		t.Fatal(err)
	}
	if changes, err := svc.migrate(); err != nil || changes[0].Action != defReplace {
		t.Fatal(changes, err)
	}
	if b, _ := os.ReadFile(path); string(b) != plist {
		t.Error("not migrated")
	}
	if svc.plan()[0].Action != defUnchanged {
		t.Error("not up to date after the migration")
	}
	if err := svc.restore(saved); err != nil || svc.plan()[0].Action != defReplace {
		t.Error("not restored", err)
	}
	writeTestFile(t, path, strings.Replace(plist, "<key>Label</key>", "<key>UserName</key><string>ncc</string><key>Label</key>", 1))
	if c := svc.plan()[0]; c.Action != defKeep || !strings.Contains(c.Detail, "UserName") {
		t.Errorf("%+v", c)
	}
	if svc.restore([]savedDefinition{{ID: "file:/Library/LaunchDaemons/other.plist"}}) == nil {
		t.Error("restored another daemon")
	}
	bin := helperBinaryPath(dir)
	if err := svc.installHelper(bin); err != nil || !svc.helperCurrent(bin) {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(svc.helperPlistPath())
	if !strings.Contains(string(b), "<key>PathState</key><dict><key>"+helperWakePath(dir)) {
		t.Errorf("%s", b)
	}
	// The helper's own plist is not mistaken for the stack's.
	_ = os.Remove(path)
	if findLaunchdService(dir, daemons) != nil {
		t.Error("the helper was taken for the stack")
	}
	if !strings.Contains(strings.Join(*calls, "\n"), "launchctl bootstrap system "+svc.helperPlistPath()) {
		t.Errorf("calls %v", *calls)
	}
}

func TestInstallHelperProgramRefusesAWrongVersion(t *testing.T) {
	h := newTestHost(t)
	if _, err := installHelperProgram(h.dir, h.svc, []byte("version 2.3.9 x"), "2.4.0", fakeRun); err == nil {
		t.Fatal("no error")
	}
	if fileExists(helperBinaryPath(h.dir)) || h.svc.helperDef != "" {
		t.Error("it was installed all the same")
	}
	if entries, _ := os.ReadDir(filepath.Dir(helperBinaryPath(h.dir))); len(entries) != 0 {
		t.Errorf("left behind: %v", entries)
	}
}
