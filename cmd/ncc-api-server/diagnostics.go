package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// unifiedCheck is one row in the System Health view. It merges the
// orchestrator's doctor self-heal checks (source "orchestrator") with the
// api-server's live external-auth probes (source "api").
type unifiedCheck struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Category   string `json:"category"`
	Status     string `json:"status"` // ok | warn | fail
	Message    string `json:"message"`
	Hint       string `json:"hint,omitempty"`
	Fixed      bool   `json:"fixed,omitempty"`
	FixMsg     string `json:"fix_message,omitempty"`
	Source     string `json:"source"`
	Disruptive bool   `json:"disruptive,omitempty"`
}

// handleHealthDiagnostics powers the Settings → System Health view (admin-only).
//
//	GET  → run a read-only self-heal scan (orchestrator doctor + auth probes)
//	POST → run the orchestrator doctor with --fix to apply safe remediations,
//	       then re-probe auth, and return the post-fix state.
func (s *apiServer) handleHealthDiagnostics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.writeDiagnostics(w, r, diagnosticsRequest{})
	case http.MethodPost:
		var req diagnosticsRequest
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&req)
		req.Fix = true
		s.writeDiagnostics(w, r, req)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleHealthSupportBundle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	installDir := filepath.Dir(s.absPath(s.configPath))
	outPath := filepath.Join(installDir, "logs", fmt.Sprintf("ncc-support-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z")))
	out, err := s.runOrchestrator([]string{"doctor", "--install-dir", installDir, "--output-file", outPath}, 2*time.Minute)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: "support bundle generation failed: " + strings.TrimSpace(out)})
		return
	}
	s.audit(r, "health.diagnostics.bundle", true, map[string]interface{}{"path": outPath})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "support bundle generated", Data: map[string]interface{}{"path": outPath}})
}

type diagnosticsRequest struct {
	Fix            bool     `json:"fix,omitempty"`
	CheckIDs       []string `json:"check_ids,omitempty"`
	VerifyAfterFix bool     `json:"verify_after_fix,omitempty"`
	NoDisruptive   bool     `json:"no_disruptive,omitempty"`
	// AllowDisruptive opts into restart-capable remediations. This is still
	// force-disabled while runs are active.
	AllowDisruptive bool `json:"allow_disruptive,omitempty"`
}

func (s *apiServer) hasInFlightRuns() bool {
	if len(s.activeRunsSnapshot()) > 0 {
		return true
	}
	return s.queuedCount() > 0
}

func (s *apiServer) writeDiagnostics(w http.ResponseWriter, r *http.Request, req diagnosticsRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), 100*time.Second)
	defer cancel()

	checks := make([]unifiedCheck, 0, 16)
	summary := map[string]int{"ok": 0, "warn": 0, "fail": 0}
	tally := func(status string) {
		if _, ok := summary[status]; !ok {
			status = "warn"
		}
		summary[status]++
	}

	// Orchestrator-side self-heal (config, storage, encryption perms, backups,
	// runs, TLS, process, logs) via the doctor subprocess.
	// API-triggered heals default to non-disruptive mode so a UI/operator
	// "Heal now" action cannot restart/stop the running stack from inside the
	// request path. CLI doctor remains the path for disruptive remediations.
	activeRunGuard := req.Fix && s.hasInFlightRuns()
	noDisruptive := req.NoDisruptive
	if req.Fix {
		// Keep API heals non-disruptive by default; require explicit opt-in.
		noDisruptive = !req.AllowDisruptive
	}
	if activeRunGuard {
		noDisruptive = true
	}
	rep, derr := s.runSelfHealOnceWithOptions(ctx, selfHealRunOptions{
		Fix:          req.Fix,
		CheckIDs:     req.CheckIDs,
		NoDisruptive: noDisruptive,
	})
	orchestratorErr := ""
	fixedIDs := []string{}
	fixedTitles := []string{}
	if derr != nil {
		orchestratorErr = derr.Error()
	} else if rep != nil {
		for _, raw := range rep.Results {
			c := unifiedCheck{Source: "orchestrator"}
			c.ID, _ = raw["id"].(string)
			c.Title, _ = raw["title"].(string)
			c.Category, _ = raw["category"].(string)
			c.Status, _ = raw["status"].(string)
			c.Message, _ = raw["message"].(string)
			c.Hint, _ = raw["hint"].(string)
			c.Fixed, _ = raw["fixed"].(bool)
			c.FixMsg, _ = raw["fix_message"].(string)
			if d, ok := raw["disruptive"].(bool); ok {
				c.Disruptive = d
			}
			checks = append(checks, c)
			tally(c.Status)
			if c.Fixed {
				fixedIDs = append(fixedIDs, c.ID)
				fixedTitles = append(fixedTitles, c.Title)
			}
		}
	}

	// Api-server-side live auth probes (LDAP/AD bind, SAML SP cert, clock skew).
	for _, d := range s.authDiagnostics() {
		checks = append(checks, unifiedCheck{
			ID: d.ID, Title: d.Title, Category: d.Category,
			Status: string(d.Status), Message: d.Message, Hint: d.Hint,
			Source: "api",
		})
		tally(string(d.Status))
	}

	// Stable display order: fail first, then warn, then ok; ties by category/id.
	rank := map[string]int{"fail": 0, "warn": 1, "ok": 2}
	sort.SliceStable(checks, func(i, j int) bool {
		ri, rj := rank[checks[i].Status], rank[checks[j].Status]
		if ri != rj {
			return ri < rj
		}
		if checks[i].Category != checks[j].Category {
			return checks[i].Category < checks[j].Category
		}
		return checks[i].ID < checks[j].ID
	})

	worst := "ok"
	if summary["warn"] > 0 {
		worst = "warn"
	}
	if summary["fail"] > 0 {
		worst = "fail"
	}
	actionableCount := 0
	autoFixableCount := 0
	manualActionCount := 0
	disruptiveSkippedCount := 0
	for _, c := range checks {
		if c.Status == "ok" {
			continue
		}
		actionableCount++
		if c.Source == "orchestrator" {
			autoFixableCount++
			if c.Disruptive && noDisruptive {
				disruptiveSkippedCount++
			}
			continue
		}
		manualActionCount++
	}

	verificationRuns := 0
	verifiedStable := false
	if req.Fix && req.VerifyAfterFix {
		// Re-scan briefly to ensure post-fix state remains stable.
		stablePasses := 0
		for i := 0; i < 3; i++ {
			verificationRuns++
			timer := time.NewTimer(2 * time.Second)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
			}
			if ctx.Err() != nil {
				break
			}
			rep2, err2 := s.runSelfHealOnceWithOptions(ctx, selfHealRunOptions{
				Fix:          false,
				CheckIDs:     req.CheckIDs,
				NoDisruptive: noDisruptive,
			})
			if err2 == nil && rep2 != nil && rep2.Summary["fail"] == 0 {
				stablePasses++
			}
		}
		verifiedStable = stablePasses >= 2
	}

	if req.Fix {
		s.audit(r, "health.diagnostics.heal", true, map[string]interface{}{
			"ok": summary["ok"], "warn": summary["warn"], "fail": summary["fail"], "check_ids": req.CheckIDs, "fixed_ids": fixedIDs,
		})
	}

	data := map[string]interface{}{
		"generated_at":  time.Now().UTC().Format(time.RFC3339),
		"fix_applied":   req.Fix,
		"overall":       worst,
		"summary":       summary,
		"checks":        checks,
		"auto_fix_loop": s.selfHealInterval > 0,
		"fix_history": map[string]interface{}{
			"fixed_ids":    fixedIDs,
			"fixed_titles": fixedTitles,
			"count":        len(fixedIDs),
		},
		"guardrails": map[string]interface{}{
			"no_disruptive":              noDisruptive,
			"active_run_guard":           activeRunGuard,
			"allow_disruptive_requested": req.Fix && req.AllowDisruptive,
			"allow_disruptive_applied":   req.Fix && req.AllowDisruptive && !noDisruptive,
		},
		"verification_runs": verificationRuns,
		"verified_stable":   verifiedStable,
		"actionable": map[string]interface{}{
			"count":              actionableCount,
			"auto_fixable":       autoFixableCount,
			"manual_action":      manualActionCount,
			"disruptive_skipped": disruptiveSkippedCount,
		},
	}
	if orchestratorErr != "" {
		data["orchestrator_error"] = orchestratorErr
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}
