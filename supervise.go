package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// This file implements the native, in-process foreground supervisor used by
// `ncc-orchestrator v2-supervise` (and `v2-start --supervise`).
//
// It unifies the two previous mechanisms that kept the v2 stack alive:
//
//  1. the POSIX-sh self-heal loops spawned by `v2-start --detach --self-heal`
//     (runtime crash/hang recovery), and
//  2. the systemd "oneshot" wrapper that replayed `v2-restart` on boot
//     (OS-reboot persistence).
//
// Instead, a single long-lived process owns the API and UI children directly:
// run it as a `Type=simple` systemd service (Restart=always) and the OS keeps
// the supervisor alive across reboots, while the supervisor keeps the children
// alive across crashes and hangs. The supervision policy (exponential backoff
// capped, restart budget per rolling window, then cooldown-and-resume, plus
// HTTP health probes to catch hung-but-alive processes) mirrors
// selfHealSupervisorScript so behavior is identical to the sh supervisors.

// superviseChild describes one managed process (api or ui).
type superviseChild struct {
	name       string   // "api" / "ui"; drives the log prefix
	bin        string   // absolute path to the server binary
	args       []string // argv passed on every (re)start
	listen     string   // listen address; used for an early bind-conflict check
	pidPath    string   // pid file updated with the live child PID
	logPath    string   // child stdout/stderr is appended here
	healthArgs []string // argv for a `--health-check` self-probe (nil = liveness-only)
	waitToken  string   // optional: gate the first start until this file exists
}

// superviseConfig is the policy + child set for runV2Supervise.
type superviseConfig struct {
	installDir         string
	children           []*superviseChild
	maxRestarts        int           // restarts allowed within window before a cooldown
	window             time.Duration // rolling restart-budget window (and cooldown length)
	probeInterval      time.Duration // how often to health-probe a live child
	unhealthyThreshold int           // consecutive failed probes that force a restart
	initialBackoff     time.Duration // first restart delay; doubles up to backoffCap
	backoffCap         time.Duration // ceiling for exponential restart backoff
	stopGrace          time.Duration // SIGTERM->SIGKILL grace per child on shutdown
}

func (c *superviseConfig) applyDefaults() {
	if c.maxRestarts <= 0 {
		c.maxRestarts = 3
	}
	if c.window <= 0 {
		c.window = 10 * time.Minute
	}
	if c.probeInterval <= 0 {
		c.probeInterval = 10 * time.Second
	}
	if c.unhealthyThreshold <= 0 {
		c.unhealthyThreshold = 3
	}
	if c.initialBackoff <= 0 {
		c.initialBackoff = time.Second
	}
	if c.backoffCap <= 0 {
		c.backoffCap = 30 * time.Second
	}
	if c.stopGrace <= 0 {
		// Kept below v2-stop's default 5s timeout so a manual `v2-stop` that
		// targets the supervisor sees it exit cleanly before force-killing it.
		c.stopGrace = 3 * time.Second
	}
}

// runV2Supervise runs the foreground supervisor until it receives SIGTERM/SIGINT
// (or, under systemd, until the unit is stopped). It never returns on its own
// while children keep restarting; it returns nil after a clean shutdown.
func runV2Supervise(cfg superviseConfig) error {
	cfg.applyDefaults()
	if len(cfg.children) == 0 {
		return fmt.Errorf("supervisor: no child processes configured")
	}

	runDir := filepath.Join(cfg.installDir, "run")
	logDir := filepath.Join(cfg.installDir, "logs")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("supervisor: prepare run dir: %w", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return fmt.Errorf("supervisor: prepare log dir: %w", err)
	}
	unlock, err := acquireSupervisorLock(filepath.Join(runDir, "v2-supervisor.lock"))
	if err != nil {
		return fmt.Errorf("supervisor: another instance is already running: %w", err)
	}
	defer unlock()

	supLogPath := filepath.Join(logDir, "v2-supervisor.log")
	supLogFile, err := os.OpenFile(supLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("supervisor: open log: %w", err)
	}
	defer supLogFile.Close()
	// Tee supervisor events to both the rotating log file and stderr so they
	// are captured by `journalctl -u ncc-orchestrator` under systemd.
	logf := func(format string, a ...any) {
		line := fmt.Sprintf("%s supervisor %s\n",
			time.Now().UTC().Format("2006-01-02T15:04:05Z"),
			fmt.Sprintf(format, a...))
		_, _ = supLogFile.WriteString(line)
		_, _ = os.Stderr.WriteString(line)
	}

	supPIDPath := filepath.Join(runDir, "v2-supervisor.pid")
	// Single-instance guard: refuse to start if another supervisor is already
	// live. A second supervisor can never bind the shared ports anyway (its
	// children crash-loop on "address already in use"), and on exit it would
	// delete the shared run/v2-*.pid files out from under the running stack,
	// blinding v2-status/v2-stop. Aborting early keeps the live stack's state
	// intact. A stale pid file (process gone) is ignored and overwritten.
	if existing, perr := readPIDFromFile(supPIDPath); perr == nil && existing != os.Getpid() && processIsAlive(existing) {
		known, matches := processIdentityMatches(existing, filepath.Base(os.Args[0]), "v2-supervise")
		if !known || matches {
			return fmt.Errorf("supervisor: another ncc-orchestrator supervisor is already running (pid %d); stop it first (`systemctl stop ncc-orchestrator` or `ncc-orchestrator v2-stop`) before starting another", existing)
		}
		// A live PID with a different command is a reused PID, not a running
		// supervisor. It is safe to replace only this stale metadata file.
		_ = os.Remove(supPIDPath)
	}
	if err := os.WriteFile(supPIDPath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("supervisor: write pid file: %w", err)
	}
	defer os.Remove(supPIDPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		logf("received %s; stopping %d managed process(es)", sig, len(cfg.children))
		cancel()
	}()

	logf("starting (install-dir=%s, max-restarts=%d/%s, probe-every=%s after %d failures, cooldown-and-resume)",
		cfg.installDir, cfg.maxRestarts, cfg.window, cfg.probeInterval, cfg.unhealthyThreshold)

	superviseRun(ctx, &cfg, logf)
	logf("stopped")
	return nil
}

// superviseRun runs every child's supervision loop until ctx is cancelled, then
// waits for them all to stop and clears their pid files. Split out from
// runV2Supervise (which owns signal handling, the supervisor pid file, and
// logging) so the core supervision behavior is unit-testable.
func superviseRun(ctx context.Context, cfg *superviseConfig, logf func(string, ...any)) {
	var wg sync.WaitGroup
	for _, child := range cfg.children {
		wg.Add(1)
		go superviseChildLoop(ctx, &wg, cfg, child, logf)
	}
	wg.Wait()
	// Best-effort: drop the children's pid files now that they are stopped so a
	// later `v2-status` does not report stale PIDs.
	for _, child := range cfg.children {
		_ = os.Remove(child.pidPath)
	}
}

// childStopReason explains why a single supervised run of a child ended.
type childStopReason int

const (
	childExited    childStopReason = iota // process exited on its own (crash or clean)
	childUnhealthy                        // health probe tripped; we killed it
	childShutdown                         // ctx cancelled (supervisor is stopping)
)

func superviseChildLoop(ctx context.Context, wg *sync.WaitGroup, cfg *superviseConfig, c *superviseChild, logf func(string, ...any)) {
	defer wg.Done()

	// Gate the first start (UI) until the API has written the shared token
	// file, mirroring runV2Start's start ordering.
	if c.waitToken != "" {
		for ctx.Err() == nil {
			if _, err := os.Stat(c.waitToken); err == nil {
				break
			}
			if !sleepCtx(ctx, 250*time.Millisecond) {
				return
			}
		}
	}

	restarts := 0
	windowStart := time.Now()
	backoff := cfg.initialBackoff

	for ctx.Err() == nil {
		reason := runChildOnce(ctx, cfg, c, logf)
		if reason == childShutdown {
			return
		}

		now := time.Now()
		if now.Sub(windowStart) > cfg.window {
			windowStart = now
			restarts = 0
			backoff = cfg.initialBackoff
		}
		restarts++
		if restarts > cfg.maxRestarts {
			logf("%s exhausted restarts (%d within %s); cooling down %s before resuming",
				c.name, cfg.maxRestarts, cfg.window, cfg.window)
			if !sleepCtx(ctx, cfg.window) {
				return
			}
			windowStart = time.Now()
			restarts = 0
			backoff = cfg.initialBackoff
			continue
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > cfg.backoffCap {
			backoff = cfg.backoffCap
		}
	}
}

// runChildOnce starts the child, supervises it until it exits / is found
// unhealthy / the supervisor is shutting down, and returns the reason.
func runChildOnce(ctx context.Context, cfg *superviseConfig, c *superviseChild, logf func(string, ...any)) childStopReason {
	logFile, err := os.OpenFile(c.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		logf("%s: open log %s failed: %v", c.name, c.logPath, err)
		// Treat as a failed start; caller applies backoff.
		_ = sleepCtx(ctx, time.Second)
		if ctx.Err() != nil {
			return childShutdown
		}
		return childExited
	}
	defer logFile.Close()

	// Fail clearly before spawning a child when another process already owns
	// its port. Without this check the child only reports a generic exit 1 and
	// the supervisor appears to be endlessly restarting a healthy process.
	if strings.TrimSpace(c.listen) != "" {
		if err := canBindListenAddress(c.listen); err != nil {
			logf("%s: listen address %s unavailable: %v; refusing to spawn duplicate listener", c.name, c.listen, err)
			return childExited
		}
	}

	cmd := exec.Command(c.bin, c.args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logf("%s: start failed: %v", c.name, err)
		if ctx.Err() != nil {
			return childShutdown
		}
		return childExited
	}
	pid := cmd.Process.Pid
	_ = os.WriteFile(c.pidPath, []byte(fmt.Sprintf("%d\n", pid)), 0o644)
	logf("%s started pid=%d", c.name, pid)

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	// Liveness-only child: just wait for exit or shutdown.
	if len(c.healthArgs) == 0 {
		select {
		case <-ctx.Done():
			stopChild(cmd, waitCh, cfg.stopGrace)
			return childShutdown
		case err := <-waitCh:
			logf("%s exited pid=%d (%s)", c.name, pid, exitDesc(err))
			return childExited
		}
	}

	// Health-probed child: poll the built-in --health-check while alive so a
	// hung-but-alive process (deadlock, stuck handler) is restarted too.
	ticker := time.NewTicker(cfg.probeInterval)
	defer ticker.Stop()
	unhealthy := 0
	for {
		select {
		case <-ctx.Done():
			stopChild(cmd, waitCh, cfg.stopGrace)
			return childShutdown
		case err := <-waitCh:
			logf("%s exited pid=%d (%s)", c.name, pid, exitDesc(err))
			return childExited
		case <-ticker.C:
			if runHealthProbe(ctx, c.healthArgs, cfg.probeInterval) {
				unhealthy = 0
				continue
			}
			unhealthy++
			logf("%s health probe failed (%d/%d) pid=%d", c.name, unhealthy, cfg.unhealthyThreshold, pid)
			if unhealthy >= cfg.unhealthyThreshold {
				logf("%s unhealthy threshold reached; restarting pid=%d", c.name, pid)
				stopChild(cmd, waitCh, cfg.stopGrace)
				return childUnhealthy
			}
		}
	}
}

// stopChild sends SIGTERM, waits up to grace for a clean exit, then SIGKILLs.
func stopChild(cmd *exec.Cmd, waitCh <-chan error, grace time.Duration) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	signalProcessStop(cmd)
	select {
	case <-waitCh:
		return
	case <-time.After(grace):
		_ = cmd.Process.Kill()
		<-waitCh
	}
}

// runHealthProbe runs the child's `--health-check` self-probe argv and reports
// whether it exited 0 (healthy). The probe is bounded so a hung probe cannot
// stall the supervision loop.
func runHealthProbe(ctx context.Context, args []string, timeout time.Duration) bool {
	if len(args) == 0 {
		return true
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, args[0], args[1:]...)
	return cmd.Run() == nil
}

// sleepCtx sleeps for d, returning false if the context is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func exitDesc(err error) string {
	if err == nil {
		return "clean exit"
	}
	return err.Error()
}

// buildAPIHealthProbeArgs returns the argv (no shell) for the api-server's
// built-in `--health-check` self-probe. Mirrors buildAPIHealthProbeCmd, which
// the sh supervisor uses. Returns nil if apiBin is empty.
func buildAPIHealthProbeArgs(apiBin, listen, tokenFile, repoRoot string) []string {
	if strings.TrimSpace(apiBin) == "" {
		return nil
	}
	args := []string{"--health-check"}
	if strings.TrimSpace(listen) != "" {
		args = append(args, "--listen", listen)
	}
	if strings.TrimSpace(tokenFile) != "" {
		args = append(args, "--token-file-path", tokenFile)
	}
	if strings.TrimSpace(repoRoot) != "" {
		args = append(args, "--repo-root", repoRoot)
	}
	return append([]string{apiBin}, args...)
}

// buildUIHealthProbeArgs returns the argv (no shell) for the ui-server's
// built-in `--health-check` self-probe. Mirrors buildUIHealthProbeCmd. The
// placeholder cert/key flags only switch the probe to the https scheme (it
// skips verification), matching the sh supervisor. Returns nil if uiBin empty.
func buildUIHealthProbeArgs(uiBin, listen string, tls bool) []string {
	if strings.TrimSpace(uiBin) == "" {
		return nil
	}
	args := []string{"--health-check"}
	if strings.TrimSpace(listen) != "" {
		args = append(args, "--listen", listen)
	}
	if tls {
		args = append(args, "--tls-cert-file", "tls.crt", "--tls-key-file", "tls.key")
	}
	return append([]string{uiBin}, args...)
}
