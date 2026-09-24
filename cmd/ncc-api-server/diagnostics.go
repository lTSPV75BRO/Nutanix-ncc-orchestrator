package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
	installDir := s.maintenanceInstallDir()
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
	doctorIDs := s.doctorCheckIDs(req.CheckIDs)
	var rep *selfHealReport
	var derr error
	runDoctor := !s.capabilities.Kubernetes || len(req.CheckIDs) == 0 || len(doctorIDs) > 0
	if runDoctor {
		rep, derr = s.runSelfHealOnceWithOptions(ctx, selfHealRunOptions{
			Fix:          req.Fix,
			CheckIDs:     doctorIDs,
			NoDisruptive: noDisruptive,
		})
	}
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
	for _, d := range s.k8sRuntimeDiagnostics() {
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
	if req.Fix && req.VerifyAfterFix && (!s.capabilities.Kubernetes || len(doctorIDs) > 0) {
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
				CheckIDs:     doctorIDs,
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

// k8sDoctorCheckIDs are PVC-safe orchestrator doctor checks. Host supervisor,
// PID, SELinux, and in-process TLS-file checks do not apply when Kubernetes
// controllers own process lifecycle and Ingress terminates TLS.
var k8sDoctorCheckIDs = []string{
	"config-schema",
	"config-valid",
	"config-output-routing",
	"output-dirs-writable",
	"disk-space",
	"secrets-perms",
	"backup-staleness",
	"backup-restorable",
	"recent-run-health",
	"run-output-freshness",
	"log-sizes",
}

func (s *apiServer) doctorCheckIDs(requested []string) []string {
	if s == nil || !s.capabilities.Kubernetes {
		return requested
	}
	allow := make(map[string]bool, len(k8sDoctorCheckIDs))
	for _, id := range k8sDoctorCheckIDs {
		allow[id] = true
	}
	if len(requested) == 0 {
		out := make([]string, len(k8sDoctorCheckIDs))
		copy(out, k8sDoctorCheckIDs)
		return out
	}
	out := make([]string, 0, len(requested))
	for _, id := range requested {
		if allow[strings.TrimSpace(id)] {
			out = append(out, id)
		}
	}
	return out
}

func (s *apiServer) k8sRuntimeDiagnostics() []diagResult {
	if s == nil || !s.capabilities.Kubernetes {
		return nil
	}
	out := []diagResult{
		{
			ID:       "k8s-runtime",
			Title:    "Kubernetes runtime",
			Category: "runtime",
			Status:   diagOK,
			Message:  "API is running in Kubernetes mode. Deployments and the runner CronJob own process lifecycle.",
		},
	}

	jwt := diagResult{ID: "k8s-jwt-secret", Title: "Shared JWT signing secret", Category: "runtime"}
	if s.jwtSecretEphemeral || len(s.jwtSecret) == 0 {
		jwt.Status = diagFail
		jwt.Message = "NCC_JWT_SECRET is missing; session JWTs cannot be shared across API replicas."
		jwt.Hint = "Set the jwt-secret key on ncc-v2-secrets (or Helm secretName) to the same value on every replica."
	} else {
		jwt.Status = diagOK
		jwt.Message = "NCC_JWT_SECRET is configured so API replicas can validate the same session JWTs."
	}
	out = append(out, jwt)

	users := diagResult{ID: "k8s-users-store", Title: "Shared user store", Category: "runtime"}
	if s.users == nil || !s.users.writable() {
		users.Status = diagWarn
		users.Message = "No writable user store is configured."
		users.Hint = "Set --users-db-secret so accounts live in a Kubernetes Secret visible to every replica."
	} else {
		users.Status = diagOK
		users.Message = "User database is persisted at " + s.users.location() + "."
	}
	out = append(out, users)

	cookie := diagResult{ID: "k8s-cookie-secure", Title: "Secure session cookies", Category: "tls"}
	if s.cookieSecure() {
		cookie.Status = diagOK
		cookie.Message = "Session cookies are marked Secure for Ingress-terminated HTTPS."
	} else {
		cookie.Status = diagWarn
		cookie.Message = "Session cookies are not marked Secure."
		cookie.Hint = "Pass --cookie-secure (or unset --cookie-insecure) so browsers keep auth_token on https origins."
	}
	out = append(out, cookie)

	tls := diagResult{ID: "k8s-ingress-tls", Title: "Ingress TLS termination", Category: "tls"}
	tls.Status = diagOK
	tls.Message = "HTTPS is terminated at the Ingress (secret " + k8sIngressTLSSecret() + "). Manage certificates on that secret or with cert-manager."
	out = append(out, tls)

	pvc := diagResult{ID: "k8s-pvc-writable", Title: "Shared PVC writable", Category: "storage"}
	root := strings.TrimSpace(s.absPath(s.repoRoot))
	if root == "" {
		root = "/data"
	}
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		pvc.Status = diagFail
		pvc.Message = "Shared data root " + root + " is not available."
		pvc.Hint = "Confirm the ncc-v2-data PVC is bound and mounted at /data."
	} else if f, err := os.CreateTemp(root, ".ncc-health-*"); err != nil {
		pvc.Status = diagFail
		pvc.Message = "Shared data root " + root + " is not writable: " + err.Error()
	} else {
		name := f.Name()
		_ = f.Close()
		_ = os.Remove(name)
		pvc.Status = diagOK
		pvc.Message = "Shared volume at " + root + " is writable."
	}
	out = append(out, pvc)
	return out
}

func k8sIngressTLSSecret() string {
	if v := strings.TrimSpace(os.Getenv("NCC_INGRESS_TLS_SECRET")); v != "" {
		return v
	}
	return "ncc-v2-ui-tls"
}
