package main

import (
	"context"
	"net/http"
	"sort"
	"time"
)

// unifiedCheck is one row in the System Health view. It merges the
// orchestrator's doctor self-heal checks (source "orchestrator") with the
// api-server's live external-auth probes (source "api").
type unifiedCheck struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Category string `json:"category"`
	Status   string `json:"status"` // ok | warn | fail
	Message  string `json:"message"`
	Hint     string `json:"hint,omitempty"`
	Fixed    bool   `json:"fixed,omitempty"`
	FixMsg   string `json:"fix_message,omitempty"`
	Source   string `json:"source"`
}

// handleHealthDiagnostics powers the Settings → System Health view (admin-only).
//
//	GET  → run a read-only self-heal scan (orchestrator doctor + auth probes)
//	POST → run the orchestrator doctor with --fix to apply safe remediations,
//	       then re-probe auth, and return the post-fix state.
func (s *apiServer) handleHealthDiagnostics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.writeDiagnostics(w, r, false)
	case http.MethodPost:
		s.writeDiagnostics(w, r, true)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) writeDiagnostics(w http.ResponseWriter, r *http.Request, fix bool) {
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
	rep, derr := s.runSelfHealOnce(ctx, fix)
	orchestratorErr := ""
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
			checks = append(checks, c)
			tally(c.Status)
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

	if fix {
		s.audit(r, "health.diagnostics.heal", true, map[string]interface{}{
			"ok": summary["ok"], "warn": summary["warn"], "fail": summary["fail"],
		})
	}

	data := map[string]interface{}{
		"generated_at":  time.Now().UTC().Format(time.RFC3339),
		"fix_applied":   fix,
		"overall":       worst,
		"summary":       summary,
		"checks":        checks,
		"auto_fix_loop": s.selfHealInterval > 0,
	}
	if orchestratorErr != "" {
		data["orchestrator_error"] = orchestratorErr
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}
