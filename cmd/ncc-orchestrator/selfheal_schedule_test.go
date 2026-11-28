package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The API leaves out empty fields, so a schedule saved without a task name has
// no task_name key. "%v" of the missing value is "<nil>".
func TestSchedulerIntegrityWithoutTaskName(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".ncc-api-schedule.json"), []byte(`{"type":"systemd","action":"create","every":"4h"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	res := checkSchedulerIntegrity(&healContext{InstallDir: dir})
	if strings.Contains(res.Message, "<nil>") || !strings.Contains(res.Message, "ncc-sched-ncc-orchestrator.sh") {
		t.Fatalf("message = %q", res.Message)
	}

	// A missing type or log path must not become "<nil>" either.
	fakeScheduleParts(t, []string{"part"}, nil)
	if err := os.WriteFile(filepath.Join(dir, ".ncc-api-schedule.json"), []byte(`{"task_name":null}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if res := checkSchedulerIntegrity(&healContext{InstallDir: dir}); strings.Contains(res.Message, "<nil>") {
		t.Fatalf("message = %q", res.Message)
	}
}

func TestScheduleRepairArgs(t *testing.T) {
	for name, tc := range map[string]struct {
		state    string
		want     []string
		dontWant []string
	}{
		"all missing": {`{}`, []string{"--every 4h", "--with-lock=false"}, []string{"--cron"}},
		"null values": {`{"every":null,"config":null,"with_lock":null,"cron":null}`, []string{"--every 4h", "--with-lock=false"}, []string{"--cron"}},
		"lock on":     {`{"every":"30m","with_lock":true}`, []string{"--every 30m", "--with-lock=true"}, nil},
		"cron":        {`{"cron":"15 */4 * * *"}`, []string{"--cron 15 */4 * * *"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			var st map[string]interface{}
			if err := json.Unmarshal([]byte(tc.state), &st); err != nil {
				t.Fatal(err)
			}
			args := scheduleRepairArgs("/opt/ncc", st, "systemd", "ncc-orchestrator", "")
			got := strings.Join(args, " ")
			if strings.Contains(got, "<nil>") || !strings.Contains(got, "config.yaml create-schedule --type systemd") {
				t.Fatalf("args = %s", got)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("args lack %q: %s", w, got)
				}
			}
			for _, w := range tc.dontWant {
				if strings.Contains(got, w) {
					t.Errorf("args contain %q: %s", w, got)
				}
			}
		})
	}
}

// A schedule that was saved but never applied must not be installed.
func TestSchedulerRepairSkipsUnappliedSchedule(t *testing.T) {
	if fixed, _ := repairSchedulerArtifactsFromState(t.TempDir(), map[string]interface{}{"action": "create", "print_only": true}, "systemd", "x", ""); fixed {
		t.Fatal("repaired a schedule that was never applied")
	}
}

// fakeScheduleParts keeps the check away from the host's own schedules.
func fakeScheduleParts(t *testing.T, missing []string, err error) *string {
	t.Helper()
	asked := new(string)
	real := missingScheduleParts
	missingScheduleParts = func(typ, task, logPath string) ([]string, error) {
		*asked = typ + " " + task
		return missing, err
	}
	t.Cleanup(func() { missingScheduleParts = real })
	return asked
}

// INS-01: "auto" is a valid type, and a removed or preview-only schedule has
// nothing to check.
func TestSchedulerIntegrityStates(t *testing.T) {
	auto := "cron"
	if runtime.GOOS == "windows" {
		auto = "windows"
	}
	for name, tc := range map[string]struct {
		state   string
		missing []string
		err     error
		status  healStatus
		asked   string
		message string
	}{
		"auto installed":   {`{"type":"auto","action":"create"}`, nil, nil, healOK, auto + " ncc-orchestrator", "installed"},
		"empty type":       {`{"action":"create"}`, nil, nil, healOK, auto + " ncc-orchestrator", "installed"},
		"cron missing":     {`{"type":"cron","task_name":"nightly"}`, []string{"crontab entry"}, nil, healWarn, "cron nightly", "missing: crontab entry"},
		"cannot look":      {`{"type":"cron"}`, nil, errors.New("no permission"), healWarn, "cron ncc-orchestrator", "no permission"},
		"removed":          {`{"type":"cron","action":"remove"}`, []string{"x"}, nil, healOK, "", "removed"},
		"preview only":     {`{"type":"cron","action":"create","print_only":true}`, []string{"x"}, nil, healOK, "", "never installed"},
		"unknown type":     {`{"type":"at"}`, nil, nil, healWarn, "", "unknown scheduler type in persisted state: at"},
		"explicit windows": {`{"type":"windows"}`, nil, nil, healOK, "windows ncc-orchestrator", "installed"},
	} {
		t.Run(name, func(t *testing.T) {
			asked := fakeScheduleParts(t, tc.missing, tc.err)
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, ".ncc-api-schedule.json"), []byte(tc.state), 0o600); err != nil {
				t.Fatal(err)
			}
			res := checkSchedulerIntegrity(&healContext{InstallDir: dir})
			if res.Status != tc.status || *asked != tc.asked || !strings.Contains(res.Message, tc.message) {
				t.Fatalf("status %q asked %q message %q", res.Status, *asked, res.Message)
			}
		})
	}
}

// WIN-06: the default schedule name is the supervisor task's name; a schedule
// must never replace, list or remove that task.
func TestWindowsScheduleNeverTheSupervisorTask(t *testing.T) {
	supervisor := windowsSupervisorTaskXML(installServiceOptions{InstallDir: `C:\NCC`, ServiceName: supervisorServiceName, OrchestratorBin: `C:\NCC\bin\ncc-orchestrator.exe`})
	scan := `<Task><Actions><Exec><Command>C:\NCC\bin\ncc-orchestrator.exe</Command><Arguments>--config C:\NCC\config.yaml</Arguments></Exec></Actions></Task>`
	other := `<Task><Actions><Exec><Command>C:\Tools\backup.exe</Command></Exec></Actions></Task>`
	for name, tc := range map[string]struct {
		tasks map[string]string
		ask   string
		want  string
		found bool
	}{
		"only the supervisor":     {map[string]string{"ncc-orchestrator": supervisor}, "ncc-orchestrator", "ncc-orchestrator-schedule", false},
		"both":                    {map[string]string{"ncc-orchestrator": supervisor, "ncc-orchestrator-schedule": scan}, "ncc-orchestrator", "ncc-orchestrator-schedule", true},
		"earlier release's name":  {map[string]string{"ncc-orchestrator": scan}, "ncc-orchestrator", "ncc-orchestrator", true},
		"somebody else's task":    {map[string]string{"ncc-orchestrator": other}, "ncc-orchestrator", "ncc-orchestrator-schedule", false},
		"a name of its own":       {map[string]string{"nightly": scan}, "nightly", "nightly", true},
		"a name of its own, none": {nil, "nightly", "nightly", false},
	} {
		t.Run(name, func(t *testing.T) {
			real := queryWindowsTaskXML
			queryWindowsTaskXML = func(n string) (string, bool) { x, ok := tc.tasks[n]; return x, ok }
			t.Cleanup(func() { queryWindowsTaskXML = real })
			got, found := findWindowsSchedule(tc.ask)
			if got != tc.want || found != tc.found {
				t.Fatalf("got %q %v, want %q %v", got, found, tc.want, tc.found)
			}
		})
	}
}

// INS-01: a stopping supervisor leaves the pid file of a server started by
// hand, which v2-stop needs to find it.
func TestSuperviseKeepsForeignPIDFile(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pidPath := filepath.Join(t.TempDir(), "v2-api.pid")
	write := func(pid int) {
		t.Helper()
		if err := os.WriteFile(pidPath, []byte(fmt.Sprintf("%d\n", pid)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// This test process stands in for the server started by hand.
	write(os.Getpid())
	c := &superviseChild{bin: self, pidPath: pidPath, lastPID: 4}
	c.removeOwnPIDFile()
	if _, err := os.Stat(pidPath); err != nil {
		t.Fatalf("the other server's pid file was removed: %v", err)
	}
	c.lastPID = os.Getpid()
	c.removeOwnPIDFile()
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("own pid file kept (%v)", err)
	}
	// A stale file (process gone) goes too.
	write(1 << 29)
	c.removeOwnPIDFile()
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("stale pid file kept (%v)", err)
	}
}
