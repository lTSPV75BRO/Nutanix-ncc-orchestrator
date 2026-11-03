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

	"goncc/internal/runtimecaps"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
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
	// startGrace: probes do not count against a child this soon after it
	// starts, so a slow start (migrations, a cold disk) is not killed.
	startGrace time.Duration
	// stableAfter: a child that ran this long before exiting starts again with
	// a fresh budget and backoff; one crash after days of uptime is not a loop.
	stableAfter time.Duration
	// requestPoll: how often to look for a restart request file.
	requestPoll time.Duration
	state       *superviseStateWriter
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
	if c.startGrace <= 0 {
		c.startGrace = 30 * time.Second
	}
	if c.stableAfter <= 0 {
		c.stableAfter = 2 * time.Minute
	}
	if c.requestPoll <= 0 {
		c.requestPoll = 2 * time.Second
	}
}

// runV2Supervise runs the foreground supervisor until it receives SIGTERM/SIGINT
// (or, under systemd, until the unit is stopped). It never returns on its own
// while children keep restarting; it returns nil after a clean shutdown.
func runV2Supervise(cfg superviseConfig) error {
	if err := runtimecaps.Detect().RejectHostOperation("native stack supervision"); err != nil {
		return err
	}
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

	cfg.state = newSuperviseStateWriter(supervisorStatePath(cfg.installDir), &cfg)
	defer cfg.state.remove()
	for _, c := range cfg.children {
		// A request left over from before this start is stale.
		_ = os.Remove(supervisorRestartRequestPath(cfg.installDir, c.name))
	}

	logf("starting (install-dir=%s, max-restarts=%d/%s, probe-every=%s after %d failures, start-grace=%s, stable-after=%s, cooldown-and-resume)",
		cfg.installDir, cfg.maxRestarts, cfg.window, cfg.probeInterval, cfg.unhealthyThreshold, cfg.startGrace, cfg.stableAfter)

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
	if cfg.state != nil {
		// Upgrades handed over by the API server (supervise_upgrade.go).
		wg.Add(1)
		go func() {
			defer wg.Done()
			newUpgrader(cfg, logf).loop(ctx)
		}()
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
	childRequested                        // an admin asked for a restart
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
		started := time.Now()
		reason := runChildOnce(ctx, cfg, c, logf)
		if reason == childShutdown {
			cfg.state.update(c.name, func(s *superviseChildState) { s.State = "stopped"; s.PID = 0 })
			return
		}
		if reason == childRequested {
			// Requested restarts are not failures: no budget, no backoff.
			cfg.state.update(c.name, func(s *superviseChildState) { s.RestartsTotal++ })
			if !sleepCtx(ctx, 500*time.Millisecond) {
				return
			}
			continue
		}

		now := time.Now()
		if now.Sub(windowStart) > cfg.window || now.Sub(started) >= cfg.stableAfter {
			// Budget window over, or the child had been stable: start fresh.
			windowStart = now
			restarts = 0
			backoff = cfg.initialBackoff
		}
		restarts++
		inWindow := restarts
		cfg.state.update(c.name, func(s *superviseChildState) {
			s.RestartsTotal++
			s.RestartsInWindow = inWindow
		})
		if restarts > cfg.maxRestarts {
			logf("%s exhausted restarts (%d within %s); cooling down %s before resuming",
				c.name, cfg.maxRestarts, cfg.window, cfg.window)
			cfg.state.event(c.name, "cooldown", fmt.Sprintf("%d restarts within %s", cfg.maxRestarts, cfg.window))
			next := time.Now().Add(cfg.window).UTC().Format(time.RFC3339)
			cfg.state.update(c.name, func(s *superviseChildState) { s.State = "cooldown"; s.PID = 0; s.NextStartAt = next })
			if !waitOrRestartRequest(ctx, cfg, c, cfg.window) {
				return
			}
			cfg.state.update(c.name, func(s *superviseChildState) { s.RestartsInWindow = 0 })
			windowStart = time.Now()
			restarts = 0
			backoff = cfg.initialBackoff
			continue
		}
		next := time.Now().Add(backoff).UTC().Format(time.RFC3339)
		cfg.state.update(c.name, func(s *superviseChildState) { s.State = "backoff"; s.PID = 0; s.NextStartAt = next })
		if !waitOrRestartRequest(ctx, cfg, c, backoff) {
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
	logFile := &lumberjack.Logger{
		Filename:   c.logPath,
		MaxSize:    50,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   true,
	}
	defer logFile.Close()

	// Fail clearly before spawning a child when another process already owns
	// its port. Without this check the child only reports a generic exit 1 and
	// the supervisor appears to be endlessly restarting a healthy process.
	if strings.TrimSpace(c.listen) != "" {
		if err := canBindListenAddress(c.listen); err != nil {
			logf("%s: listen address %s unavailable: %v; refusing to spawn duplicate listener", c.name, c.listen, err)
			recordChildExit(cfg, c, "port-in-use", fmt.Sprintf("%s is in use by another process: %v", c.listen, err), false)
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
		recordChildExit(cfg, c, "start-failed", err.Error(), false)
		return childExited
	}
	pid := cmd.Process.Pid
	startedAt := time.Now()
	_ = os.WriteFile(c.pidPath, []byte(fmt.Sprintf("%d\n", pid)), 0o644)
	logf("%s started pid=%d", c.name, pid)
	cfg.state.update(c.name, func(s *superviseChildState) {
		s.State, s.PID, s.StartedAt, s.NextStartAt, s.ProbeFailures = "running", pid, startedAt.UTC().Format(time.RFC3339), "", 0
	})
	cfg.state.event(c.name, "started", fmt.Sprintf("pid %d", pid))

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	// Probes run only for health-probed children; restart requests are
	// checked for every child.
	var probeC <-chan time.Time
	if len(c.healthArgs) > 0 {
		ticker := time.NewTicker(cfg.probeInterval)
		defer ticker.Stop()
		probeC = ticker.C
	}
	reqTicker := time.NewTicker(cfg.requestPoll)
	defer reqTicker.Stop()
	requestPath := supervisorRestartRequestPath(cfg.installDir, c.name)
	unhealthy := 0
	for {
		select {
		case <-ctx.Done():
			_ = stopChild(cmd, waitCh, cfg.stopGrace)
			return childShutdown
		case err := <-waitCh:
			logf("%s exited pid=%d (%s)", c.name, pid, exitDesc(err))
			recordChildExit(cfg, c, "crashed", exitDesc(err), true)
			return childExited
		case <-reqTicker.C:
			if _, err := os.Stat(requestPath); err != nil {
				continue
			}
			_ = os.Remove(requestPath)
			logf("%s restart requested; stopping pid=%d gracefully", c.name, pid)
			_ = stopChild(cmd, waitCh, cfg.stopGrace)
			recordChildExit(cfg, c, "requested", "restart requested from Status → Supervisor", false)
			return childRequested
		case <-probeC:
			// Hung-but-alive process (deadlock, stuck handler): restart it too.
			probeStart := time.Now()
			ok := runHealthProbe(ctx, c.healthArgs, cfg.probeInterval)
			took := time.Since(probeStart).Milliseconds()
			if !ok && time.Since(startedAt) < cfg.startGrace {
				// Still starting up; a failed probe does not count yet.
				continue
			}
			if ok {
				unhealthy = 0
			} else {
				unhealthy++
			}
			failures := unhealthy
			cfg.state.update(c.name, func(s *superviseChildState) {
				s.LastProbeAt, s.LastProbeMS, s.LastProbeOK, s.ProbeFailures = nowRFC3339(), took, &ok, failures
			})
			if ok {
				continue
			}
			logf("%s health probe failed (%d/%d) pid=%d", c.name, unhealthy, cfg.unhealthyThreshold, pid)
			if unhealthy >= cfg.unhealthyThreshold {
				logf("%s unhealthy threshold reached; restarting pid=%d", c.name, pid)
				_ = stopChild(cmd, waitCh, cfg.stopGrace)
				recordChildExit(cfg, c, "unhealthy", fmt.Sprintf("%d health probes in a row failed", unhealthy), true)
				return childUnhealthy
			}
		}
	}
}

// recordChildExit publishes why a child stopped; withLog adds the tail of
// its log, which usually holds the panic or fatal error.
func recordChildExit(cfg *superviseConfig, c *superviseChild, reason, detail string, withLog bool) {
	var tail []string
	if withLog {
		tail = tailLines(c.logPath, exitLogTailLines)
	}
	cfg.state.update(c.name, func(s *superviseChildState) {
		s.State, s.PID = "stopped", 0
		s.LastExitAt, s.LastExitReason, s.LastExitDetail, s.LastExitLogTail = nowRFC3339(), reason, detail, tail
	})
	cfg.state.event(c.name, reason, detail)
}

// stopChild sends SIGTERM, waits up to grace for a clean exit, then SIGKILLs.
// It returns the child's exit error (nil for a clean exit).
func stopChild(cmd *exec.Cmd, waitCh <-chan error, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	signalProcessStop(cmd)
	select {
	case err := <-waitCh:
		return err
	case <-time.After(grace):
		_ = cmd.Process.Kill()
		return <-waitCh
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
// waitOrRestartRequest waits like sleepCtx but ends early when a restart of
// the child is requested, so an operator (or doctor) can end a cooldown
// without restarting the whole service.
func waitOrRestartRequest(ctx context.Context, cfg *superviseConfig, c *superviseChild, d time.Duration) bool {
	requestPath := supervisorRestartRequestPath(cfg.installDir, c.name)
	deadline := time.Now().Add(d)
	for {
		if _, err := os.Stat(requestPath); err == nil {
			_ = os.Remove(requestPath)
			cfg.state.event(c.name, "restart-requested", "starting now instead of waiting")
			return ctx.Err() == nil
		}
		left := time.Until(deadline)
		if left <= 0 {
			return ctx.Err() == nil
		}
		if left > cfg.requestPoll {
			left = cfg.requestPoll
		}
		if !sleepCtx(ctx, left) {
			return false
		}
	}
}

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
// built-in `--health-check` self-probe. Mirrors buildUIHealthProbeCmd. When the
// UI serves TLS, the probe gets the same cert/key paths the UI was started
// with: it switches to https and verifies the server against that exact
// certificate, so a placeholder path would fail every probe and make the
// supervisor restart a healthy UI. Returns nil if uiBin is empty.
func buildUIHealthProbeArgs(uiBin, listen, tlsCertFile, tlsKeyFile string) []string {
	if strings.TrimSpace(uiBin) == "" {
		return nil
	}
	args := []string{"--health-check"}
	if strings.TrimSpace(listen) != "" {
		args = append(args, "--listen", listen)
	}
	if strings.TrimSpace(tlsCertFile) != "" && strings.TrimSpace(tlsKeyFile) != "" {
		args = append(args, "--tls-cert-file", tlsCertFile, "--tls-key-file", tlsKeyFile)
	}
	return append([]string{uiBin}, args...)
}
