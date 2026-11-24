package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"goncc/internal/plural"
)

// handleRuns returns a merged, enriched feed of historical orchestrator runs.
//
// Query params:
//   - limit (default 200, max 1000) — cap on entries returned (newest first).
//   - source=history|summary|trigger — keep only one source kind.
//   - since=RFC3339|2006-01-02 — drop entries older than this instant.
//
// Sources merged (deduped by minute-precision timestamp; "history" > "summary"
// > "trigger" when keeping a single representative for the same run):
//   - outputDir/runs/<id>/run-summary.json  (archived, full metrics)
//   - outputDir/run-summary.json            (the latest run on disk)
//   - audit-log runs.trigger events         (trigger-only, no artifacts)
func (s *apiServer) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}

	q := r.URL.Query()
	limit := 200
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid limit: must be a positive integer"})
			return
		}
		limit = n
	}
	if limit > 1000 {
		limit = 1000
	}
	sourceFilter := strings.TrimSpace(q.Get("source"))
	if sourceFilter != "" && sourceFilter != "history" && sourceFilter != "summary" && sourceFilter != "trigger" {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   "invalid source: must be one of history, summary, trigger",
		})
		return
	}
	var sinceTS time.Time
	if raw := strings.TrimSpace(q.Get("since")); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			sinceTS = t
		} else if t, err := time.Parse("2006-01-02", raw); err == nil {
			sinceTS = t
		} else {
			writeJSON(w, http.StatusBadRequest, envelope{
				Success: false,
				Error:   "invalid since: expected RFC3339 timestamp or YYYY-MM-DD date",
			})
			return
		}
	}

	outDir := s.absPath(s.outputDir)
	collected := []runInfo{}

	// 1) Archived history directories.
	runsDir := filepath.Join(outDir, "runs")
	if entries, err := os.ReadDir(runsDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			info, infoErr := e.Info()
			if infoErr != nil {
				continue
			}
			runPath := filepath.Join(runsDir, e.Name())
			ri := runInfo{
				ID:      e.Name(),
				Path:    "runs/" + e.Name(),
				ModTime: info.ModTime().UTC().Format(time.RFC3339),
				Source:  "history",
			}
			if _, err := os.Stat(filepath.Join(runPath, "index.html")); err == nil {
				ri.HasIndex = true
			}
			enrichRunInfoFromSummary(&ri, filepath.Join(runPath, "run-summary.json"))
			ri.Environment = environmentForRunDir(runPath)
			collected = append(collected, ri)
		}
	}

	// 2) Latest top-level run-summary.json (the in-place "current" run).
	latestSummary := filepath.Join(outDir, "run-summary.json")
	if info, err := os.Stat(latestSummary); err == nil {
		latestID := info.ModTime().UTC().Format("20060102T150405Z")
		latest := runInfo{
			ID:      latestID,
			Path:    ".",
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
			Source:  "summary",
		}
		if _, err := os.Stat(filepath.Join(outDir, "index.html")); err == nil {
			latest.HasIndex = true
		}
		enrichRunInfoFromSummary(&latest, latestSummary)
		collected = append(collected, latest)
	}

	// 3) Audit-log `runs.trigger` events for triggers that may not have
	//    produced any artifacts (run still in flight, ephemeral storage, etc).
	if auditEntries, err := s.auditEntries(500, auditFilter{actionPrefix: "runs.trigger"}); err == nil {
		for _, e := range auditEntries {
			act, _ := e["action"].(string)
			if act != "runs.trigger" {
				continue
			}
			ts, _ := e["ts"].(string)
			if strings.TrimSpace(ts) == "" {
				continue
			}
			ri := runInfo{
				ID:      "trigger-" + ts,
				ModTime: ts,
				Source:  "trigger",
			}
			if ts != "" {
				ri.Timestamp = ts
			}
			if ok, present := e["success"].(bool); present {
				ri.Success = &ok
			}
			ri.StatusSource = "audit"
			ri.Status = "triggered"
			if ri.Success != nil && !*ri.Success {
				ri.Status = "trigger_failed"
			}
			if v, _ := e["client"].(string); v != "" {
				ri.Client = v
			}
			if v, _ := e["user_agent"].(string); v != "" {
				ri.UserAgent = v
			}
			if v, _ := e["auth_mode"].(string); v != "" {
				ri.AuthMode = v
			}
			collected = append(collected, ri)
		}
	}

	// Dedupe by minute-precision timestamp; prefer richer sources.
	priority := map[string]int{"history": 3, "summary": 2, "trigger": 1}
	type dedupedEntry struct {
		ri  runInfo
		pri int
	}
	bucket := map[string]dedupedEntry{}
	bucketKey := func(ri runInfo) string {
		raw := ri.Timestamp
		if raw == "" {
			raw = ri.ModTime
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t.UTC().Format("2006-01-02T15:04")
		}
		return raw
	}
	for _, ri := range collected {
		k := bucketKey(ri)
		if k == "" {
			continue
		}
		p := priority[ri.Source]
		if existing, ok := bucket[k]; !ok || p > existing.pri {
			bucket[k] = dedupedEntry{ri: ri, pri: p}
		}
	}

	out := make([]runInfo, 0, len(bucket))
	for _, v := range bucket {
		if sourceFilter != "" && v.ri.Source != sourceFilter {
			continue
		}
		if !sinceTS.IsZero() {
			ref := v.ri.Timestamp
			if ref == "" {
				ref = v.ri.ModTime
			}
			if t, err := time.Parse(time.RFC3339, ref); err == nil && t.Before(sinceTS) {
				continue
			}
		}
		out = append(out, v.ri)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModTime > out[j].ModTime })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	// Cluster-group scoping: a run row aggregates every cluster in the run, so
	// for non-admins blank the cluster-level counts/metrics that would leak
	// global state. The run still appears (id/time/duration/outcome) and the
	// per-cluster detail comes from the filtered /report/data endpoint.
	if p, ok := principalFromContext(r.Context()); ok && !s.allowedClusters(p).unrestricted {
		for i := range out {
			redactRunInfoCounts(&out[i])
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
}

// redactRunInfoCounts zeroes the aggregate, all-cluster metric fields of a run
// row so a group-restricted caller cannot infer global cluster state from the
// runs feed. Identity/outcome fields are preserved for the listing.
func redactRunInfoCounts(ri *runInfo) {
	ri.ClustersOK = 0
	ri.ClustersFailed = 0
	ri.TotalChecks = 0
	ri.AvgHealthScore = 0
	ri.MinHealthScore = 0
	ri.FailTotal = 0
	ri.WarnTotal = 0
	ri.ErrTotal = 0
	ri.InfoTotal = 0
}

// handleRunsRouter dispatches /api/v1/runs/<id> requests that are not
// already claimed by more-specific routes ("summary", "active", "preflight",
// "trigger"). It supports GET (return archived run metadata + summary) and
// returns a clear 404 for unknown IDs. Path traversal is rejected.
func (s *apiServer) handleRunsRouter(w http.ResponseWriter, r *http.Request) {
	tail := strings.TrimPrefix(r.URL.Path, "/api/v1/runs/")
	if tail == "" {
		s.handleRuns(w, r)
		return
	}
	// Reserved sub-routes are claimed by their explicit registrations above;
	// anything else is treated as an archived run ID lookup.
	// Backslash too: it is a path separator on Windows. Colons stay allowed:
	// trigger row ids carry a timestamp.
	if strings.ContainsAny(tail, `/\`) || strings.Contains(tail, "..") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid run id"})
		return
	}
	s.handleRunByID(w, r, tail)
}

// handleRunByID returns metadata + summary for a single archived run under
// outputDir/runs/<id>/. Only GET is supported.
func (s *apiServer) handleRunByID(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	runPath := filepath.Join(s.absPath(s.outputDir), "runs", id)
	info, err := os.Stat(runPath)
	if err != nil || !info.IsDir() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: fmt.Sprintf("run %q not found", id)})
		return
	}
	ri := runInfo{
		ID:      id,
		Path:    "runs/" + id,
		ModTime: info.ModTime().UTC().Format(time.RFC3339),
		Source:  "history",
	}
	if _, err := os.Stat(filepath.Join(runPath, "index.html")); err == nil {
		ri.HasIndex = true
	}
	enrichRunInfoFromSummary(&ri, filepath.Join(runPath, "run-summary.json"))
	ri.Environment = environmentForRunDir(runPath)

	// Embed the artifacts we can find — keeps the UI from making N follow-up
	// requests for a single drill-down.
	artifacts := map[string]interface{}{}
	// checks-snapshot.json lists each check without its detail text (that
	// lives once, in the run's aggregated rows); join it back so the run's
	// Checks tab can show what each finding says.
	if snap := readJSONArtifactCached(filepath.Join(runPath, "checks-snapshot.json"), nil); snap != nil {
		artifacts["checks-snapshot.json"] = withCheckDetails(snap, readAggRowsCached(runPath))
	}
	for _, name := range []string{"run-summary.json", "ncc-run-record.json", "regression-summary.json", "run-meta.json"} {
		if b, rerr := os.ReadFile(filepath.Join(runPath, name)); rerr == nil {
			var v interface{}
			if json.Unmarshal(b, &v) == nil {
				if m, ok := v.(map[string]interface{}); ok {
					scrubSummaryPaths(m)
				}
				artifacts[name] = v
			} else {
				artifacts[name] = string(b)
			}
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"run":       ri,
		"artifacts": artifacts,
	}})
}

// enrichRunInfoFromSummary fills the metric fields on ri by parsing a
// run-summary.json file and derives an explicit run Status.
//
// Root cause of the "—" shown for successful runs: the runner serialized
// exit_code with `omitempty`, so a successful run (exit code 0) wrote no
// exit_code at all and Success stayed nil. The runner now always writes
// exit_code and status; for summaries written by older runners, the presence
// of index_html (written by the same completion step that computes the exit
// code) without exit_code means the runner completed with exit code 0. A
// summary that has neither is reported as "unknown" — missing data is never
// treated as success.
func enrichRunInfoFromSummary(ri *runInfo, summaryPath string) {
	ri.Status = "unknown"
	ri.StatusSource = "none"
	b, err := os.ReadFile(summaryPath)
	if err != nil {
		return
	}
	var s trendRunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return
	}
	var present map[string]json.RawMessage
	_ = json.Unmarshal(b, &present)
	if strings.TrimSpace(s.Timestamp) != "" {
		ri.Timestamp = s.Timestamp
		if ri.ModTime == "" {
			ri.ModTime = s.Timestamp
		}
	}
	ri.DurationS = s.DurationS
	ri.RunSource = strings.TrimSpace(s.Source)
	ri.ClustersOK = s.ClustersOK
	ri.ClustersFailed = s.ClustersFailed
	ri.TotalChecks = s.TotalChecks
	ri.AvgHealthScore = s.AvgHealthScore
	ri.MinHealthScore = s.MinHealthScore
	for _, c := range s.Clusters {
		ri.FailTotal += c.FailCount
		ri.WarnTotal += c.WarnCount
		ri.ErrTotal += c.ErrCount
		ri.InfoTotal += c.InfoCount
	}
	code, source := runExitCodeFromSummary(s, present)
	if source == "" {
		return
	}
	ri.ExitCode = &code
	success := code == 0
	ri.Success = &success
	ri.StatusSource = source
	ri.Status = runStatusFromExitCode(code, s.ClustersOK)
}

// runExitCodeFromSummary returns the run's exit code and how it was obtained,
// or source "" when it cannot be determined.
func runExitCodeFromSummary(s trendRunSummary, present map[string]json.RawMessage) (int, string) {
	if s.ExitCode != nil {
		return *s.ExitCode, "exit_code"
	}
	if raw, ok := present["status"]; ok {
		var st string
		if json.Unmarshal(raw, &st) == nil {
			switch strings.ToLower(strings.TrimSpace(st)) {
			case "success":
				return 0, "exit_code"
			}
		}
	}
	if _, ok := present["index_html"]; ok {
		return 0, "legacy_summary"
	}
	return 0, ""
}

// runStatusFromExitCode maps the runner's exit codes (0 success, 3 partial:
// some clusters failed, anything else failed) onto API status values.
func runStatusFromExitCode(code, clustersOK int) string {
	switch {
	case code == 0:
		return "success"
	case code == 3 && clustersOK > 0:
		return "partial"
	default:
		return "failed"
	}
}

func (s *apiServer) handleRunSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), "run-summary.json")
	b, err := os.ReadFile(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	var raw interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	// Apply the same on-read error-classification healing as
	// /api/v1/report/data so /runs/summary stays consistent.
	if m, ok := raw.(map[string]interface{}); ok {
		reclassifyRunSummaryInPlace(m)
		scrubSummaryPaths(m)
		raw = m
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: raw})
}

func (s *apiServer) handleRunActive(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// fallthrough to snapshot code below
	case http.MethodDelete:
		s.cancelActiveRun(w, r)
		return
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	// Concurrent run engine: report the full set of queued/running runs as well
	// as the legacy single-run summary (mirroring the most-recent running run)
	// so older clients keep working.
	runs := s.activeRunsSnapshot()
	queued := s.queuedCount()
	s.mu.Lock()
	defer s.mu.Unlock()
	var elapsedSeconds int
	var elapsedHuman string
	var overdue bool
	var deadlineISO string
	if s.active && !s.started.IsZero() {
		elapsed := time.Since(s.started).Round(time.Second)
		elapsedSeconds = int(elapsed.Seconds())
		elapsedHuman = elapsed.String()
		if s.runTimeout > 0 {
			deadlineISO = s.started.Add(s.runTimeout).UTC().Format(time.RFC3339)
			overdue = time.Now().After(s.started.Add(s.runTimeout))
		}
	}
	runningCount := 0
	managedPIDs := map[int]bool{}
	for _, rec := range s.runs {
		if rec.status == "running" {
			runningCount++
		}
		if rec.pid > 0 {
			managedPIDs[rec.pid] = true
		}
	}
	// Surface runs this api-server didn't launch (scheduled systemd-timer/cron
	// runs, or a manual CLI run on the host) so they appear in Active Runs too.
	if ext := s.externalActiveRuns(managedPIDs); len(ext) > 0 {
		runs = append(runs, ext...)
		runningCount += len(ext)
	}
	data := map[string]interface{}{
		"active":            s.active,
		"started_at":        s.started.UTC().Format(time.RFC3339),
		"elapsed_seconds":   elapsedSeconds,
		"elapsed_human":     elapsedHuman,
		"expected_deadline": deadlineISO,
		"overdue":           overdue,
		"pid":               s.lastPID,
		"last_error":        s.lastErr,
		"last_output":       s.lastOut,
		"live_output":       s.currentLiveOutput(),
		"runner_log":        s.pathForCaller(r, s.runnerLogPath),
		"output_dir":        s.pathForCaller(r, s.outputDir),
		"config_path":       s.pathForCaller(r, defaultIfEmpty(s.lastCfg, s.configPath)),
		// Concurrent-run fields.
		"runs":                 runs,
		"running_count":        runningCount,
		"queued_count":         queued,
		"max_concurrent":       s.maxConcurrentRuns,
		"avg_run_duration_sec": s.avgRunDurationSeconds(),
	}
	if s.debugExpose {
		data["command"] = s.lastCmd
		data["work_dir"] = s.lastCwd
		data["env"] = s.lastEnv
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// cancelActiveRun cancels in-flight orchestrator run(s). With ?id=<run-id> it
// cancels a single run (running: signals the process to exit; queued: removes it
// from the queue and frees its reserved clusters). Without an id, it cancels
// every queued and running run. The goroutine running cmd.Wait() observes the
// context cancellation, reaps the child, and routes the result through
// finishRun() exactly as it would for a normal exit.
//
// This is the user-facing escape hatch for runs that are stuck on a slow
// cluster, broken DNS, etc., without forcing the operator to restart the API
// server.
func (s *apiServer) cancelActiveRun(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id != "" {
		rec, ok := s.cancelRunByID(id)
		if !ok {
			writeJSON(w, http.StatusConflict, envelope{
				Success: false,
				Error:   fmt.Sprintf("no queued or running run with id %q", id),
				Data:    map[string]interface{}{"run_id": id},
			})
			return
		}
		s.audit(r, "runs.cancel", true, map[string]interface{}{"run_id": id, "pid": rec.pid})
		writeJSON(w, http.StatusAccepted, envelope{
			Success: true,
			Message: "cancellation signalled",
			Data: map[string]interface{}{
				"run_id":        rec.id,
				"pid":           rec.pid,
				"poll_endpoint": "/api/v1/runs/active",
			},
		})
		return
	}

	n := s.cancelAllRuns()
	if n == 0 {
		writeJSON(w, http.StatusConflict, envelope{
			Success: false,
			Error:   "no run is currently active",
			Data:    map[string]interface{}{"active": false},
		})
		return
	}
	s.audit(r, "runs.cancel", true, map[string]interface{}{"cancelled": n})
	writeJSON(w, http.StatusAccepted, envelope{
		Success: true,
		Message: "cancellation signalled for " + plural.Count(n, "run"),
		Data: map[string]interface{}{
			"cancelled":     n,
			"poll_endpoint": "/api/v1/runs/active",
		},
	})
}

// preflightTimeout bounds the preflight-check child. The UI client waits a
// little longer (see runPreflight in frontend/src/api/client.ts).
const preflightTimeout = 120 * time.Second

func (s *apiServer) handleRunPreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	var req runPreflightRequest
	if r.ContentLength > 0 {
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
	}
	choice, err := s.resolveRunConfig(r, req.Environment, req.ConfigPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	resolvedCfgPath := choice.path
	extendDeadlines(w, preflightTimeout)
	out, err := s.runOrchestrator([]string{"preflight-check", "--config", resolvedCfgPath, "--format", "json"}, preflightTimeout)
	if err != nil {
		s.audit(r, "runs.preflight", false, map[string]interface{}{
			"config_path": resolvedCfgPath,
			"reason":      "orchestrator_failed",
			"error":       err.Error(),
		})
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   fmt.Sprintf("preflight-check failed: %v", err),
			Data:    map[string]string{"output": tailString(strings.TrimSpace(out), 4000)},
		})
		return
	}
	var payload map[string]interface{}
	if uerr := json.Unmarshal([]byte(strings.TrimSpace(out)), &payload); uerr != nil {
		s.audit(r, "runs.preflight", false, map[string]interface{}{
			"config_path": resolvedCfgPath,
			"reason":      "parse_failed",
			"error":       uerr.Error(),
		})
		writeJSON(w, http.StatusInternalServerError, envelope{
			Success: false,
			Error:   fmt.Sprintf("parse preflight-check output failed: %v", uerr),
			Data:    map[string]string{"output": tailString(strings.TrimSpace(out), 4000)},
		})
		return
	}
	// Surface ok/failed counts in the audit entry so admins can spot
	// preflight-failure trends without re-running.
	successFields := map[string]interface{}{"config_path": resolvedCfgPath}
	if v, present := payload["ok"]; present {
		successFields["ok"] = v
	}
	if v, present := payload["failed"]; present {
		successFields["failed"] = v
	}
	if choice.environment != "" {
		successFields["environment"] = choice.environment
		payload["environment"] = choice.environment
		// Show the environment file, not the generated merge.
		if src, err := s.environmentFile(choice.environment); err == nil {
			payload["config_path"] = s.displayRelPath(src)
		}
	}
	s.audit(r, "runs.preflight", true, successFields)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: payload})
}

func (s *apiServer) handleRunTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	// Kubernetes: a runner image on another major.minor release could write
	// data this API server cannot read, so refuse until the rollout matches.
	if err := s.runnerVersionSkewError(); err != nil {
		writeJSON(w, http.StatusConflict, envelope{Success: false, Error: err.Error(), ErrorCode: errorCodeVersionSkew})
		return
	}
	var req runTriggerRequest
	if err := decodeJSON(r.Body, &req); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	choice, err := s.resolveRunConfig(r, req.Environment, req.ConfigPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	resolvedCfgPath := choice.path
	cfgPath := resolvedCfgPath
	if len(req.Password) > 256 {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "password too long"})
		return
	}
	cleanExtraArgs, err := sanitizeExtraArgs(req.ExtraArgs)
	if err == nil {
		cleanExtraArgs, err = s.confineExtraArgPaths(cleanExtraArgs)
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	// Cluster-group scoping: confine the run to the caller's allowed clusters
	// (admins/static tokens are unrestricted) and to any requested group/subset.
	p, _ := principalFromContext(r.Context())
	access := s.allowedClusters(p)
	scopedClusters, scopeErr := s.resolveRunClusterScope(req, access, cleanExtraArgs)
	if scopeErr == nil {
		scopedClusters, scopeErr = confineToEnvironment(scopedClusters, choice, access.unrestricted)
	}
	if scopeErr != nil {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: scopeErr.Error()})
		return
	}
	if st, err := os.Stat(resolvedCfgPath); err != nil || st.IsDir() {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false,
			Error:   fmt.Sprintf("config file not found: %s", cfgPath),
			Data: map[string]interface{}{
				"config_path": cfgPath,
				"resolved":    resolvedCfgPath,
			},
		})
		return
	}

	// Hand off to the concurrent run engine. It de-duplicates clusters that are
	// already being refreshed by another active run (so two cluster groups can
	// run at once without re-running shared clusters), and queues the request
	// when all concurrency slots are busy.
	res := s.submitRun(runStartParams{
		cfgPath:      resolvedCfgPath,
		environment:  choice.environment,
		password:     req.Password,
		extraArgs:    cleanExtraArgs,
		group:        strings.TrimSpace(req.Group),
		requested:    scopedClusters,
		unrestricted: access.unrestricted,
		auditSubject: p.subject,
		auditRole:    p.role.String(),
	})

	baseFields := map[string]interface{}{"config_path": resolvedCfgPath, "extra_args_count": len(cleanExtraArgs)}
	if choice.environment != "" {
		baseFields["environment"] = choice.environment
	}
	if strings.TrimSpace(req.Group) != "" {
		baseFields["group"] = strings.TrimSpace(req.Group)
	}

	switch {
	case res.noop:
		baseFields["reason"] = "already_running"
		baseFields["skipped"] = len(res.skipped)
		s.audit(r, "runs.trigger", true, baseFields)
		writeJSON(w, http.StatusOK, envelope{
			Success: true,
			Message: "all requested clusters are already being refreshed by an in-progress run",
			Data: map[string]interface{}{
				"started":          false,
				"queued":           false,
				"skipped_clusters": res.skipped,
				"skipped_owner":    res.skippedOwner,
			},
		})
		return
	case res.queued:
		baseFields["queued"] = true
		baseFields["queue_position"] = res.queuePos
		if len(res.skipped) > 0 {
			baseFields["skipped"] = len(res.skipped)
		}
		s.audit(r, "runs.trigger", true, baseFields)
		writeJSON(w, http.StatusAccepted, envelope{
			Success: true,
			Message: fmt.Sprintf("run queued (position %d) — will start automatically when a slot frees", res.queuePos),
			Data: map[string]interface{}{
				"run_id":           res.rec.id,
				"started":          false,
				"queued":           true,
				"queue_position":   res.queuePos,
				"config_path":      resolvedCfgPath,
				"skipped_clusters": res.skipped,
				"skipped_owner":    res.rec.skippedOwner,
			},
		})
		return
	default: // started immediately
		if len(res.rec.clusters) > 0 {
			baseFields["scoped_clusters"] = len(res.rec.clusters)
		}
		if len(res.skipped) > 0 {
			baseFields["skipped"] = len(res.skipped)
		}
		baseFields["run_id"] = res.rec.id
		s.audit(r, "runs.trigger", true, baseFields)
		writeJSON(w, http.StatusAccepted, envelope{Success: true, Message: "run triggered", Data: map[string]interface{}{
			"run_id":           res.rec.id,
			"started":          true,
			"queued":           false,
			"started_at":       res.rec.startedAt.Format(time.RFC3339),
			"config_path":      resolvedCfgPath,
			"used_password":    strings.TrimSpace(req.Password) != "",
			"clusters":         res.rec.clusters,
			"skipped_clusters": res.skipped,
			"skipped_owner":    res.rec.skippedOwner,
			"running_count":    res.runningCount,
		}})
		return
	}
}

func (s *apiServer) handleRunnerLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	logPath := s.absPath(s.runnerLogPath)
	content, err := readTailFile(logPath, 64000)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
				"path":    logPath,
				"content": "",
				"exists":  false,
			}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"path":    logPath,
		"content": strings.TrimSpace(content),
		"exists":  true,
	}})
}
