package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// What schtasks /Query /FO CSV /V /NH answers for a task with two triggers.
func taskCSV(result string) string {
	line := `"HOST","\ncc-orchestrator","N/A","Bereit","Interaktiv","24.11.2026 20:09:05","` + result + `","N/A","C:\NCC Data\bin\ncc-orchestrator.exe v2-supervise --install-dir "C:\NCC Data"","N/A"`
	return "\r\n" + line + "\r\n" + line + "\r\n"
}

func TestWindowsTaskLastResult(t *testing.T) {
	for in, want := range map[string]int64{"-1": -1, "267014": taskEnded, "1": 1, "-1073741510": -1073741510} {
		if got, err := windowsTaskLastResult(taskCSV(in)); err != nil || got != want {
			t.Errorf("%s: %d, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "ERROR: no such task", taskCSV("N/A")} {
		if _, err := windowsTaskLastResult(in); err == nil {
			t.Errorf("a result was read from %q", in)
		}
	}
}

// exitedPID is the process ID of a process that has ended.
func exitedPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

func TestWatchdog(t *testing.T) {
	dead := exitedPID(t)
	for name, tc := range map[string]struct {
		pid     int // in the pid file; 0 = no file
		result  string
		during  func(dir string) // while it waits for its second look
		upgrade bool
		want    bool
	}{
		"died":               {pid: dead, result: "-1", want: true},
		"crashed":            {pid: dead, result: "2", want: true},
		"ended in Windows":   {pid: dead, result: strconv.Itoa(taskEnded)},
		"being started":      {pid: dead, result: "267009"},
		"disabled":           {pid: dead, result: "267010"},
		"stopped":            {result: "1"},
		"upgrade":            {pid: dead, result: "-1", upgrade: true},
		"stopped in between": {pid: dead, result: "1", during: func(dir string) { _ = os.Remove(filepath.Join(dir, "run", supervisorPIDFile)) }},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			for _, d := range []string{"run", "logs", filepath.Join("run", "upgrade")} {
				if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			pidPath := filepath.Join(dir, "run", supervisorPIDFile)
			if tc.pid != 0 {
				writeTestFile(t, pidPath, fmt.Sprintln(tc.pid))
			}
			if tc.upgrade {
				writeTestFile(t, helperJournalPath(dir), "{}")
			}
			calls := fakeSvcCommands(t, func(cmd string) (string, error) { return taskCSV(tc.result), nil })
			err := runWatchdog(dir, "my-ncc", func(time.Duration) {
				if tc.during != nil {
					tc.during(dir)
				}
			}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			all := strings.Join(*calls, "\n")
			if got := strings.Contains(all, "schtasks /Run /TN my-ncc"); got != tc.want {
				t.Errorf("started again: %v, want %v (%s)", got, tc.want, all)
			}
			if tc.want && !fileExists(pidPath) {
				t.Error("the pid file is gone: a start that fails would not be tried again")
			}
		})
	}

	// A start that fails is tried again the next minute.
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "run"), 0o755)
	writeTestFile(t, filepath.Join(dir, "run", supervisorPIDFile), fmt.Sprintln(dead))
	fakeSvcCommands(t, func(cmd string) (string, error) {
		if strings.Contains(cmd, "/Run") {
			return "", fmt.Errorf("no")
		}
		return taskCSV("-1"), nil
	})
	if runWatchdog(dir, "my-ncc", func(time.Duration) {}, io.Discard) == nil || !supervisorDied(dir) {
		t.Error("a failed start was forgotten")
	}
}

// v2-stop after the supervisor died must not leave the pid file, or the
// watchdog would undo the stop.
func TestStopAfterSupervisorDied(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "run"), 0o755)
	writeTestFile(t, filepath.Join(dir, "run", supervisorPIDFile), fmt.Sprintln(exitedPID(t)))
	_ = runV2Stop(v2StopOptions{InstallDir: dir})
	if fileExists(filepath.Join(dir, "run", supervisorPIDFile)) {
		t.Error("the pid file is still there")
	}
}

func TestWatchdogTaskXML(t *testing.T) {
	o := installServiceOptions{InstallDir: `C:\NCC Data`, ServiceName: "ncc-orchestrator", OrchestratorBin: `C:\NCC Data\bin\ncc-orchestrator.exe`}
	x := windowsWatchdogTaskXML(o)
	for _, s := range []string{"<Interval>PT1M</Interval><StopAtDurationEnd>", "<ExecutionTimeLimit>PT5M</ExecutionTimeLimit>", "<UserId>S-1-5-18</UserId>",
		`<Arguments>v2-watchdog --install-dir "C:\NCC Data" --service-name "ncc-orchestrator"</Arguments>`} {
		if !strings.Contains(x, s) {
			t.Errorf("no %s in\n%s", s, x)
		}
	}
	if strings.Contains(x, "BootTrigger") || strings.Contains(windowsSupervisorTaskXML(o), "<Repetition>") {
		t.Error("the wrong trigger")
	}
	if !watchdogTaskCurrent(x, o) || watchdogTaskCurrent(windowsSupervisorTaskXML(o), o) {
		t.Error("the watchdog's task is not recognised")
	}
}

func TestSaveListenAddresses(t *testing.T) {
	dir := t.TempDir()
	if err := saveListenAddresses(dir, "", ""); err != nil || fileExists(filepath.Join(dir, v2StartStateFile)) {
		t.Fatal("nothing to save", err)
	}
	for _, bad := range []string{"8081", "host", ":0", ":70000", ":http"} {
		if saveListenAddresses(dir, bad, "") == nil {
			t.Errorf("%s was accepted", bad)
		}
	}
	if err := saveListenAddresses(dir, ":9081", ""); err != nil {
		t.Fatal(err)
	}
	st, ok := loadV2StartState(dir)
	if !ok || st.APIListen != ":9081" || st.UIListen != "" || st.APIRateLimitPerMinute != defaultAPIRateLimitPerMinute {
		t.Fatalf("%+v", st)
	}
	st.APIAuthMode = "session"
	b, _ := json.Marshal(st)
	writeTestFile(t, filepath.Join(dir, v2StartStateFile), string(b))
	if err := saveListenAddresses(dir, "", "127.0.0.1:9080"); err != nil {
		t.Fatal(err)
	}
	if st, _ = loadV2StartState(dir); st.APIListen != ":9081" || st.UIListen != "127.0.0.1:9080" || st.APIAuthMode != "session" {
		t.Fatalf("%+v", st)
	}
}
