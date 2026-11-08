package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// selfHealReport is the parsed output of the orchestrator's `doctor --json`.
// The api-server caches the most recent one so the diagnostics endpoint and
// metrics can report it without re-running the (potentially slow) checks on
// every scrape.
type selfHealReport struct {
	GeneratedAt string                   `json:"generated_at"`
	InstallDir  string                   `json:"install_dir"`
	ConfigPath  string                   `json:"config_path"`
	FixApplied  bool                     `json:"fix_applied"`
	Summary     map[string]int           `json:"summary"`
	Results     []map[string]interface{} `json:"results"`
	// RanAt is when the api-server captured this report (server clock).
	RanAt time.Time `json:"ran_at"`
}

type selfHealRunOptions struct {
	Fix          bool
	CheckIDs     []string
	NoDisruptive bool
}

// startSelfHealLoop launches the periodic self-heal goroutine when an interval
// is configured. The first cycle runs shortly after startup; subsequent cycles
// run every selfHealInterval. The loop exits when ctx is cancelled.
func (s *apiServer) startSelfHealLoop(ctx context.Context) {
	if s.selfHealInterval <= 0 {
		return
	}
	serverLog.Info("self-heal loop enabled", "every", s.selfHealInterval.String(), "auto_fix", s.selfHealAutoFix)
	go func() {
		first := time.NewTimer(30 * time.Second)
		defer first.Stop()
		select {
		case <-ctx.Done():
			return
		case <-first.C:
		}
		if _, err := s.runSelfHealOnce(ctx, s.selfHealAutoFix); err != nil {
			serverLog.Error("self-heal check failed to run", "error", err.Error())
		}
		ticker := time.NewTicker(s.selfHealInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := s.runSelfHealOnce(ctx, s.selfHealAutoFix); err != nil {
					serverLog.Error("self-heal check failed to run", "error", err.Error())
				}
			}
		}
	}()
}

// runSelfHealOnce executes the orchestrator's doctor self-heal checks (as a
// subprocess, in JSON mode) once, caches the result, and emits audit/metrics.
// It is also called on demand by the diagnostics endpoint. doctor --json exits
// non-zero when a check fails, which is expected — the JSON is still valid and
// is parsed regardless of exit code.
func (s *apiServer) runSelfHealOnce(ctx context.Context, fix bool) (*selfHealReport, error) {
	return s.runSelfHealOnceWithOptions(ctx, selfHealRunOptions{Fix: fix, CheckIDs: s.doctorCheckIDs(nil)})
}

func (s *apiServer) runSelfHealOnceWithOptions(ctx context.Context, opts selfHealRunOptions) (*selfHealReport, error) {
	// Wait briefly for a cycle already in flight (the background loop, or a
	// second admin) instead of failing straight away.
	if !s.lockSelfHealRun(ctx, 45*time.Second) {
		return nil, fmt.Errorf("self-heal is already running; try again shortly")
	}
	defer s.selfHealRunMu.Unlock()

	bin := strings.TrimSpace(s.absPath(s.orchestratorBin))
	if bin == "" {
		return nil, fmt.Errorf("orchestrator binary path not configured")
	}
	args := []string{"doctor", "--json"}
	// Anchor doctor checks to the same install root the API server is managing.
	// Without this, doctor may resolve paths relative to a default stack root,
	// which can point at a different run/backup directory and misclassify
	// supervisor/runtime alignment.
	if cfg := strings.TrimSpace(s.configPath); cfg != "" {
		if absCfg := strings.TrimSpace(s.absPath(cfg)); absCfg != "" {
			installDir := s.maintenanceInstallDir()
			if strings.TrimSpace(installDir) == "" {
				installDir = filepath.Dir(absCfg)
			}
			args = append(args, "--install-dir", installDir)
		}
	}
	if opts.Fix {
		args = append(args, "--fix")
	}
	if len(opts.CheckIDs) > 0 {
		args = append(args, "--only-checks", strings.Join(opts.CheckIDs, ","))
	}
	if opts.NoDisruptive {
		args = append(args, "--no-disruptive")
	}
	if cfg := strings.TrimSpace(s.configPath); cfg != "" {
		args = append(args, "--config", s.absPath(cfg))
	}
	cctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	cmd.Env = childEnv()
	var stderr strings.Builder
	cmd.Stderr = &stderr
	started := time.Now()
	out, runErr := cmd.Output()

	var rep selfHealReport
	if jerr := json.Unmarshal(out, &rep); jerr != nil {
		logSubprocess("self-heal doctor", args, time.Since(started), stderr.String()+string(out), errors.Join(runErr, jerr))
		if runErr != nil {
			return nil, fmt.Errorf("self-heal doctor failed: %v", runErr)
		}
		return nil, fmt.Errorf("parse doctor output: %w", jerr)
	}
	serverLog.Info("self-heal checks ran", "fix", opts.Fix, "ok", rep.Summary["ok"], "warn", rep.Summary["warn"], "fail", rep.Summary["fail"], "took_ms", time.Since(started).Milliseconds())
	for _, r := range rep.Results {
		status, _ := r["status"].(string)
		if status != "fail" && status != "warn" {
			if f, _ := r["fixed"].(bool); !f {
				continue
			}
		}
		id, _ := r["id"].(string)
		title, _ := r["title"].(string)
		msg, _ := r["message"].(string)
		fixed, _ := r["fixed"].(bool)
		lvl := slog.LevelWarn
		if status == "fail" && !fixed {
			lvl = slog.LevelError
		} else if fixed {
			lvl = slog.LevelInfo
		}
		serverLog.Log(ctx, lvl, "self-heal check", "check", id, "title", title, "status", status, "fixed", fixed, "detail", msg)
	}
	rep.RanAt = time.Now().UTC()

	s.selfHealMu.Lock()
	s.lastSelfHeal = &rep
	s.selfHealMu.Unlock()
	s.selfHealRunsTotal.Add(1)

	fixes := 0
	for _, r := range rep.Results {
		if f, _ := r["fixed"].(bool); f {
			fixes++
		}
	}
	if fixes > 0 {
		s.selfHealFixesTotal.Add(int64(fixes))
		s.auditEvent("selfheal.autofix", true, map[string]interface{}{"fixes": fixes})
	}
	// Notify only on a transition into a failing state (prev had no failures),
	// so a persistently-failing check doesn't alert on every cycle.
	prev := s.prevSelfHealFail.Swap(int64(rep.Summary["fail"]))
	if rep.Summary["fail"] > 0 && prev == 0 {
		failed := make([]string, 0, rep.Summary["fail"])
		for _, r := range rep.Results {
			if status, _ := r["status"].(string); status == "fail" {
				if title, _ := r["title"].(string); strings.TrimSpace(title) != "" {
					failed = append(failed, title)
					continue
				}
				if id, _ := r["id"].(string); strings.TrimSpace(id) != "" {
					failed = append(failed, id)
				}
			}
		}
		s.notifyOperationalFailure("selfheal_failure", fmt.Sprintf("NCC self-heal found %d failing check(s)", rep.Summary["fail"]), map[string]interface{}{
			"fail":   rep.Summary["fail"],
			"warn":   rep.Summary["warn"],
			"ok":     rep.Summary["ok"],
			"checks": failed,
		})
	}
	return &rep, nil
}

// lockSelfHealRun takes selfHealRunMu, waiting up to wait (or until ctx ends).
func (s *apiServer) lockSelfHealRun(ctx context.Context, wait time.Duration) bool {
	if s.selfHealRunMu.TryLock() {
		return true
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-tick.C:
			if s.selfHealRunMu.TryLock() {
				return true
			}
		}
	}
}

// cachedSelfHeal returns the most recent self-heal report, if any.
func (s *apiServer) cachedSelfHeal() *selfHealReport {
	s.selfHealMu.RLock()
	defer s.selfHealMu.RUnlock()
	return s.lastSelfHeal
}
