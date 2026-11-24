package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// extraArgsHaveFlag reports whether the caller-supplied extra args already
// include the given flag (matching `--flag` or `--flag=value`), so the
// api-server doesn't double-specify output directories it would otherwise pin.
func extraArgsHaveFlag(args []string, flag string) bool {
	want := "--" + flag
	for _, a := range args {
		a = strings.TrimSpace(a)
		if a == want || strings.HasPrefix(a, want+"=") {
			return true
		}
	}
	return false
}

func (s *apiServer) runOrchestrator(args []string, timeout time.Duration) (string, error) {
	return s.runOrchestratorEnv(args, timeout, nil)
}

// runOrchestratorEnv is runOrchestrator with extra environment variables
// appended to the (master-key-scrubbed) child environment. Used to pass a
// backup passphrase (NCC_BACKUP_PASSPHRASE) to v2-backup/v2-restore via the
// environment rather than argv, so it never lands in a process listing or the
// audit log.
func (s *apiServer) runOrchestratorEnv(args []string, timeout time.Duration, extraEnv []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	if len(extraEnv) > 0 {
		cmd.Env = append(cmd.Env, extraEnv...)
	}
	start := time.Now()
	serverLog.Debug("orchestrator command starting", "args", strings.Join(redactedArgs(args), " "), "timeout", timeout.String())
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		err = fmt.Errorf("command timed out after %s", timeout)
	}
	logSubprocess("orchestrator command", redactedArgs(args), time.Since(start), string(out), err)
	return string(out), err
}

func (s *apiServer) orchestratorBaseCommand() []string {
	binPath, _ := filepath.Abs(s.absPath(s.orchestratorBin))
	if st, err := os.Stat(binPath); err == nil && !st.IsDir() {
		return []string{binPath}
	}
	// Dev-friendly fallback: run source directly when binary is not built yet.
	pkgPath, _ := filepath.Abs(filepath.Join(s.absPath(s.repoRoot), "cmd", "ncc-orchestrator"))
	return []string{"go", "run", pkgPath}
}

func (s *apiServer) makeOrchestratorCommand(ctx context.Context, args ...string) *exec.Cmd {
	base := s.orchestratorBaseCommand()
	full := append(append([]string{}, base...), args...)
	if len(full) == 0 {
		return exec.Command("true")
	}
	name := full[0]
	rest := full[1:]
	var cmd *exec.Cmd
	// The program is the configured orchestrator binary (or `go run` in a
	// source checkout); arguments are passed as argv, never through a shell.
	if ctx == nil {
		cmd = exec.Command(name, rest...) // #nosec G702 -- configured binary, argv only
	} else {
		cmd = exec.CommandContext(ctx, name, rest...) // #nosec G702 -- configured binary, argv only
	}
	// Never hand the user-store master key to an orchestrator child; it copies
	// the (already-sealed) user DB as opaque bytes and has no need for the key.
	cmd.Env = childEnv()
	return cmd
}

func tailString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}

func (s *apiServer) currentLiveOutput() string {
	if s.liveOut == nil {
		return ""
	}
	return strings.TrimSpace(s.liveOut.String())
}

func redactedArgs(args []string) []string {
	if len(args) == 0 {
		return args
	}
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == "--password" {
			out[i+1] = "***"
		}
	}
	return out
}
