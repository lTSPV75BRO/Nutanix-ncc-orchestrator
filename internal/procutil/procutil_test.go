package procutil

import (
	"os"
	"os/exec"
	"runtime"
	"testing"
)

func TestExistsSelf(t *testing.T) {
	if !Exists(os.Getpid()) {
		t.Fatal("the current process must be reported as existing")
	}
}

func TestExistsRejectsNonPositive(t *testing.T) {
	for _, pid := range []int{0, -1} {
		if Exists(pid) {
			t.Fatalf("Exists(%d) = true, want false", pid)
		}
	}
}

// TestExistsExitedChild starts a short-lived child, waits for it to exit, and
// checks the pid is then reported dead. This is the case the signal-0 probe
// got wrong on Windows in the opposite direction (every live pid was "dead"),
// so both directions are covered: alive while running, dead after exit.
func TestExistsExitedChild(t *testing.T) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "exit", "0")
	} else {
		cmd = exec.Command("sh", "-c", "exit 0")
	}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start helper process: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper process: %v", err)
	}
	if Exists(pid) {
		t.Fatalf("Exists(%d) = true for a process that has exited", pid)
	}
}

func TestExistsRunningChild(t *testing.T) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("powershell", "-NoProfile", "-Command", "Start-Sleep -Seconds 30")
	} else {
		cmd = exec.Command("sleep", "30")
	}
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start helper process: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	if !Exists(cmd.Process.Pid) {
		t.Fatalf("Exists(%d) = false for a running child process", cmd.Process.Pid)
	}
}
