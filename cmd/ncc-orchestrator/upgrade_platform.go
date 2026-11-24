package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// stackService is what the update helper needs from a service manager. The
// stack is the supervisor service; the helper has a definition of its own,
// which runs it only while run/upgrade/wake exists.
type stackService interface {
	kind() string
	title() string
	// orchestratorBin is the program the stack's definition starts.
	orchestratorBin() string

	// helperCurrent reports whether the helper's own definitions are installed
	// as installHelper would write them.
	helperCurrent(bin string) bool
	installHelper(bin string) error
	removeHelper()
	// wakeHelper starts the helper (the wake file is already there).
	wakeHelper() error
	// probe is run by the helper: can it control the stack and its definitions?
	probe() error

	// privileged: may this process change the definitions?
	privileged() error
	running() bool
	start() error
	stop() error

	// plan lists the definitions and what migrate would do with them.
	plan() []defChange
	migrate() ([]defChange, error)
	save() ([]savedDefinition, error)
	restore([]savedDefinition) error

	// holdRestarts stops anything that restarts the stack on its own when
	// program files change.
	holdRestarts() (release func())
}

const (
	kindSystemd = "systemd"
	kindWindows = "windows-task"
	kindLaunchd = "launchd"
	kindManual  = "manual"
)

// helperChildEnv marks a v2-start that the helper runs, so it does not try to
// finish the helper's own job.
const helperChildEnv = "NCC_UPGRADE_HELPER_CHILD"

// resumeInterruptedUpgrade: with no service manager, nothing starts the helper
// again after a reboot in the middle of an upgrade, except the next v2-start.
func resumeInterruptedUpgrade(installDir string) bool {
	if os.Getenv(helperChildEnv) != "" {
		return false
	}
	var j helperJournal
	if readHelperJSON(helperJournalPath(installDir), &j) != nil || j.validate() != nil || j.Kind != kindManual || !fileExists(helperBinaryPath(installDir)) {
		return false
	}
	return newManualService(installDir).wakeHelper() == nil
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// svcCommand runs a service manager command. A variable for tests.
var svcCommand = func(name string, args ...string) (string, error) {
	out, err := runPlatformCommand(2*time.Minute, name, args...)
	if err != nil {
		line, _, _ := strings.Cut(strings.TrimSpace(out), "\n")
		if line = strings.TrimSpace(line); line == "" {
			line = err.Error()
		}
		return out, fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), line)
	}
	return out, nil
}

// findStackService returns the service that runs the stack in installDir, or
// nil when no service manager does. A variable for tests.
var findStackService = func(installDir string) stackService {
	switch runtime.GOOS {
	case "linux":
		return findSystemdService(installDir, systemdUnitDir)
	case "windows":
		return findWindowsService(installDir)
	case "darwin":
		return findLaunchdService(installDir, "/Library/LaunchDaemons")
	}
	return nil
}

// stackServiceFor returns the service of the given kind, for a resumed job.
func stackServiceFor(kind, installDir string) (stackService, error) {
	if kind == kindManual {
		return newManualService(installDir), nil
	}
	svc := findStackService(installDir)
	if svc == nil || svc.kind() != kind {
		return nil, fmt.Errorf("the %s service of %s was not found", kind, installDir)
	}
	return svc, nil
}

// supervisorRuns reports whether the supervisor of installDir is alive.
func supervisorRuns(installDir, bin string) bool {
	st, err := readSupervisorState(installDir)
	if err != nil || st.PID == 0 {
		return false
	}
	return processIsExpected(st.PID, filepath.Base(bin), "v2-supervise")
}

// waitUntil polls cond for at most d.
func waitUntil(d time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(d); ; time.Sleep(250 * time.Millisecond) {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// ---- no service manager: v2-start / v2-stop ----

type manualService struct{ installDir string }

func newManualService(installDir string) *manualService { return &manualService{installDir} }

func (s *manualService) kind() string  { return kindManual }
func (s *manualService) title() string { return "the stack started with v2-start" }
func (s *manualService) orchestratorBin() string {
	if p := existingBinaryInInstallDir(s.installDir, "ncc-orchestrator"); p != "" {
		return p
	}
	return binaryPathInInstallDir(s.installDir, "ncc-orchestrator")
}

func (s *manualService) helperCurrent(string) bool  { return true }
func (s *manualService) installHelper(string) error { return nil }
func (s *manualService) removeHelper()              {}

// wakeHelper starts the helper as a process of its own.
func (s *manualService) wakeHelper() error {
	if err := os.MkdirAll(filepath.Join(s.installDir, "logs"), 0o755); err != nil { // #nosec G301 -- same mode as the stack's own log folder
		return err
	}
	logFile, err := os.OpenFile(helperLogPath(s.installDir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd, err := startDetachedProcess(func() *exec.Cmd {
		c := exec.Command(helperBinaryPath(s.installDir), "upgrade-helper", "--install-dir", s.installDir) // #nosec G204 -- our own program in the install folder
		c.Dir = s.installDir
		c.Stdout, c.Stderr = logFile, logFile
		return c
	})
	if err != nil {
		return err
	}
	return cmd.Process.Release()
}

func (s *manualService) probe() error      { return nil }
func (s *manualService) privileged() error { return nil }

func (s *manualService) running() bool {
	for _, f := range []string{"v2-api.pid", "v2-ui.pid"} {
		pid, err := readPIDFromFile(filepath.Join(s.installDir, "run", f))
		if err == nil && processIsExpected(pid, "ncc-api-server", "ncc-ui-server") {
			return true
		}
	}
	return false
}

func (s *manualService) stop() error {
	if err := runV2Stop(v2StopOptions{InstallDir: s.installDir, StopTimeout: 15 * time.Second}); err != nil && s.running() {
		return err
	}
	if !waitUntil(10*time.Second, func() bool { return !s.running() }) {
		return errors.New("the servers are still running")
	}
	return nil
}

func (s *manualService) start() error {
	args, ok := v2StartArgsFromState(s.installDir)
	if !ok {
		args = []string{"v2-start", "--install-dir", s.installDir, "--detach"}
	}
	cmd := exec.Command(s.orchestratorBin(), args...) // #nosec G204 -- our own program in the install folder
	cmd.Dir = s.installDir
	cmd.Env = append(os.Environ(), helperChildEnv+"=1")
	// Not CombinedOutput: the servers it starts would keep the pipe open.
	logFile, err := os.CreateTemp("", "ncc-v2-start-*.log")
	if err != nil {
		return err
	}
	defer os.Remove(logFile.Name())
	defer logFile.Close()
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Run(); err != nil {
		out, _ := os.ReadFile(logFile.Name())
		return fmt.Errorf("v2-start: %s", upgradeErrorLine(string(out), err))
	}
	return nil
}

func (s *manualService) plan() []defChange                { return nil }
func (s *manualService) migrate() ([]defChange, error)    { return nil, nil }
func (s *manualService) save() ([]savedDefinition, error) { return nil, nil }
func (s *manualService) restore([]savedDefinition) error  { return nil }
func (s *manualService) holdRestarts() func()             { return func() {} }
