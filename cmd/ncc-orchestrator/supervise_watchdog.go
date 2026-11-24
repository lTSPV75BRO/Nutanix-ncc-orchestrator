package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// Windows has nothing like systemd's Restart=always: Task Scheduler's
// RestartOnFailure applies only when a program cannot be started, so a
// supervisor that died stayed down until the next boot (WIN-07). A second task
// looks every minute and starts the supervisor's task again, but not after a
// deliberate stop: v2-stop removes the pid file, and a task that was ended or
// disabled says so in its last result.

func watchdogTaskName(service string) string { return service + "-watchdog" }

func windowsWatchdogTaskXML(o installServiceOptions) string {
	return windowsTaskXML(windowsTaskSpec{
		Description: "Starts the NCC Orchestrator supervisor again when it has died",
		Command:     o.OrchestratorBin,
		Arguments:   `v2-watchdog --install-dir "` + o.InstallDir + `" --service-name "` + o.ServiceName + `"`,
		WorkingDir:  o.InstallDir,
		Retries:     3,
		EveryMinute: true,
		NotAtBoot:   true, // the supervisor's own task starts it then
		TimeLimit:   "PT5M",
	})
}

// watchdogTaskCurrent: Task Scheduler rewrites the document, so only what
// matters is compared. A task that somebody disabled stays disabled.
func watchdogTaskCurrent(installed string, o installServiceOptions) bool {
	want := windowsWatchdogTaskXML(o)
	for _, el := range []string{"Command", "Arguments", "WorkingDirectory"} {
		if xmlElement(installed, el) != xmlElement(want, el) {
			return false
		}
	}
	return strings.Contains(installed, "<Repetition>")
}

func installWatchdogTask(o installServiceOptions) error {
	if x, ok := queryWindowsTaskXML(watchdogTaskName(o.ServiceName)); ok && watchdogTaskCurrent(x, o) {
		return nil
	}
	return registerWindowsTaskXML(watchdogTaskName(o.ServiceName), windowsWatchdogTaskXML(o))
}

func removeWatchdogTask(service string) {
	_, _ = svcCommand("schtasks", "/Delete", "/TN", watchdogTaskName(service), "/F")
}

// taskEnded is the result of a task that was ended by somebody. The scheduler's
// other results of its own (running, disabled, never run, ...) are close to it;
// anything else is the exit code of the program.
const (
	taskEnded           = 0x41306
	taskResultFirst     = 0x41300
	taskResultLast      = 0x4130F
	watchdogSecondLook  = 5 * time.Second
	supervisorPIDFile   = "v2-supervisor.pid"
	windowsTaskCSVField = 6 // "Last Result"; the column order is the same in every language
)

// windowsTaskLastResult reads the last result from `schtasks /Query /FO CSV /V /NH`.
func windowsTaskLastResult(out string) (int64, error) {
	r := csv.NewReader(strings.NewReader(strings.ReplaceAll(out, "\x00", "")))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	for {
		rec, err := r.Read()
		if err != nil {
			return 0, errors.New("no last result in the answer of schtasks")
		}
		if len(rec) > windowsTaskCSVField {
			if v, err := strconv.ParseInt(strings.TrimSpace(rec[windowsTaskCSVField]), 10, 64); err == nil {
				return v, nil
			}
		}
	}
}

// supervisorDied reports whether a supervisor started here and is gone without
// having stopped.
func supervisorDied(installDir string) bool {
	pid, err := readPIDFromFile(filepath.Join(installDir, "run", supervisorPIDFile))
	if err != nil || processIsExpected(pid, "ncc-orchestrator", "v2-supervise") {
		return false
	}
	// The helper starts and stops the supervisor during an upgrade.
	_, busy := jobRunning(installDir)
	return !busy && !helperUnfinished(installDir)
}

// runWatchdog starts the supervisor's task again when the supervisor has died.
func runWatchdog(installDir, service string, pause func(time.Duration), out io.Writer) error {
	if !supervisorDied(installDir) {
		return nil
	}
	// v2-stop removes the pid file just after it ends the process.
	pause(watchdogSecondLook)
	if !supervisorDied(installDir) {
		return nil
	}
	answer, err := svcCommand("schtasks", "/Query", "/TN", service, "/FO", "CSV", "/V", "/NH")
	if err != nil {
		return err
	}
	result, err := windowsTaskLastResult(answer)
	if err != nil {
		return err
	}
	if result >= taskResultFirst && result <= taskResultLast {
		return nil
	}
	line := fmt.Sprintf("watchdog: the supervisor has died (result %d); starting it again", result)
	fmt.Fprintln(out, line)
	appendSupervisorLog(installDir, line)

	// Its servers live on and hold their ports. Stopping them removes the pid
	// files too; the supervisor's stays until a new one runs.
	pidPath := filepath.Join(installDir, "run", supervisorPIDFile)
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		return err
	}
	_ = runV2Stop(v2StopOptions{InstallDir: installDir, StopTimeout: 15 * time.Second})
	if err := os.WriteFile(pidPath, raw, 0o644); err != nil { // #nosec G306 -- a process ID, as the supervisor writes it
		return err
	}
	_, err = svcCommand("schtasks", "/Run", "/TN", service)
	return err
}

func appendSupervisorLog(installDir, line string) {
	f, err := os.OpenFile(filepath.Join(installDir, "logs", "v2-supervisor.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644) // #nosec G302 -- the supervisor's log
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = fmt.Fprintf(f, "%s %s\n", time.Now().UTC().Format("2006-01-02T15:04:05Z"), line)
}

func newWatchdogCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "v2-watchdog", Hidden: true, SilenceUsage: true, Args: cobra.NoArgs,
		Short: "Start the supervisor's scheduled task again when the supervisor has died (Windows)",
		RunE: func(cmd *cobra.Command, args []string) error {
			get := func(n string) string { v, _ := cmd.Flags().GetString(n); return strings.TrimSpace(v) }
			if get("install-dir") == "" {
				return errors.New("--install-dir is required")
			}
			return runWatchdog(get("install-dir"), get("service-name"), time.Sleep, cmd.OutOrStdout())
		},
	}
	cmd.Flags().String("install-dir", "", "Install folder")
	cmd.Flags().String("service-name", supervisorServiceName, "The supervisor's task")
	return cmd
}
