package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
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

// startSelfHealLoop launches the periodic self-heal goroutine when an interval
// is configured. The first cycle runs shortly after startup; subsequent cycles
// run every selfHealInterval. The loop exits when ctx is cancelled.
func (s *apiServer) startSelfHealLoop(ctx context.Context) {
	if s.selfHealInterval <= 0 {
		return
	}
	log.Printf("self-heal loop enabled: every %s (auto-fix=%t)", s.selfHealInterval, s.selfHealAutoFix)
	go func() {
		first := time.NewTimer(30 * time.Second)
		defer first.Stop()
		select {
		case <-ctx.Done():
			return
		case <-first.C:
		}
		s.runSelfHealOnce(ctx, s.selfHealAutoFix)
		ticker := time.NewTicker(s.selfHealInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runSelfHealOnce(ctx, s.selfHealAutoFix)
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
	bin := strings.TrimSpace(s.absPath(s.orchestratorBin))
	if bin == "" {
		return nil, fmt.Errorf("orchestrator binary path not configured")
	}
	args := []string{"doctor", "--json"}
	if fix {
		args = append(args, "--fix")
	}
	if cfg := strings.TrimSpace(s.configPath); cfg != "" {
		args = append(args, "--config", s.absPath(cfg))
	}
	cctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	out, runErr := cmd.Output()

	var rep selfHealReport
	if jerr := json.Unmarshal(out, &rep); jerr != nil {
		if runErr != nil {
			return nil, fmt.Errorf("self-heal doctor failed: %v", runErr)
		}
		return nil, fmt.Errorf("parse doctor output: %w", jerr)
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
	if rep.Summary["fail"] > 0 {
		log.Printf("self-heal: %d failing check(s) (warn=%d, ok=%d)", rep.Summary["fail"], rep.Summary["warn"], rep.Summary["ok"])
	}
	return &rep, nil
}

// cachedSelfHeal returns the most recent self-heal report, if any.
func (s *apiServer) cachedSelfHeal() *selfHealReport {
	s.selfHealMu.RLock()
	defer s.selfHealMu.RUnlock()
	return s.lastSelfHeal
}
