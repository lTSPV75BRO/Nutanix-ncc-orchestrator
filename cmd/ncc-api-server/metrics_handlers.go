package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"goncc/internal/auth"
	"goncc/internal/promtext"
)

func (s *apiServer) handleRateLimitMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.rateLimiter == nil {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"enabled": false,
			"config": map[string]interface{}{
				"rate_limit_per_minute": s.rateLimitPerMinute,
				"window_seconds":        60,
			},
		}})
		return
	}
	st := s.rateLimiter.stats(time.Now().UTC())
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"enabled": true,
		"config": map[string]interface{}{
			"rate_limit_per_minute": s.rateLimitPerMinute,
			"window_seconds":        st.WindowSeconds,
		},
		"metrics": st,
	}})
}

// handlePrometheusMetrics emits Prometheus exposition format on
// GET /metrics. Intentionally hand-rolled (no client_golang dep) so
// the api-server stays at its current dependency footprint and the
// scrape endpoint never needs reachability to a Prometheus library
// for build-time updates.
//
// Metric naming follows the Prometheus best practices: snake_case,
// `_total` suffix for monotonic counters, `_seconds` for time, and a
// `ncc_` namespace prefix to avoid collisions when scraped alongside
// other targets.
//
// HELP / TYPE comments are emitted so the endpoint passes
// `promtool check metrics`. The response is text/plain;
// version=0.0.4; charset=utf-8 — the canonical content type
// Prometheus advertises in its scrape Accept header.
func (s *apiServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	now := time.Now().UTC()
	uptime := now.Sub(s.startedAt).Seconds()

	s.mu.Lock()
	active := 0
	if s.active {
		active = 1
	}
	startedTS := float64(0)
	if !s.started.IsZero() {
		startedTS = float64(s.started.Unix())
	}
	runningNow := s.runningCountLocked()
	queuedNow := len(s.runQueue)
	maxConc := s.maxConcurrentRuns
	s.mu.Unlock()

	fmt.Fprintf(w, "# HELP ncc_build_info Build metadata for this api-server (always 1).\n")
	fmt.Fprintf(w, "# TYPE ncc_build_info gauge\n")
	fmt.Fprintf(w, "ncc_build_info{version=%q,stream=%q,go_version=%q,os=%q,arch=%q} 1\n",
		Version, Stream, GoVersion, runtime.GOOS, runtime.GOARCH)

	fmt.Fprintf(w, "# HELP ncc_process_start_time_seconds Unix epoch start time of the api-server process.\n")
	fmt.Fprintf(w, "# TYPE ncc_process_start_time_seconds gauge\n")
	fmt.Fprintf(w, "ncc_process_start_time_seconds %d\n", s.startedAt.Unix())

	fmt.Fprintf(w, "# HELP ncc_process_uptime_seconds Seconds since the api-server started.\n")
	fmt.Fprintf(w, "# TYPE ncc_process_uptime_seconds gauge\n")
	fmt.Fprintf(w, "ncc_process_uptime_seconds %.3f\n", uptime)

	fmt.Fprintf(w, "# HELP ncc_run_active 1 when an NCC run is currently executing, else 0.\n")
	fmt.Fprintf(w, "# TYPE ncc_run_active gauge\n")
	fmt.Fprintf(w, "ncc_run_active %d\n", active)

	fmt.Fprintf(w, "# HELP ncc_run_started_seconds Unix epoch start time of the currently-running NCC run (0 when idle).\n")
	fmt.Fprintf(w, "# TYPE ncc_run_started_seconds gauge\n")
	fmt.Fprintf(w, "ncc_run_started_seconds %.0f\n", startedTS)

	fmt.Fprintf(w, "# HELP ncc_runs_triggered_total Cumulative count of NCC runs triggered via /api/v1/runs/trigger since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_triggered_total counter\n")
	fmt.Fprintf(w, "ncc_runs_triggered_total %d\n", s.runsTriggeredTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_runs_completed_total Cumulative count of NCC runs that completed (any exit code) since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_completed_total counter\n")
	fmt.Fprintf(w, "ncc_runs_completed_total %d\n", s.runsCompletedTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_runs_failed_total Cumulative count of NCC runs that exited with a non-zero status since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_failed_total counter\n")
	fmt.Fprintf(w, "ncc_runs_failed_total %d\n", s.runsFailedTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_api_background_panics_total Panics recovered in background work (schedulers, cache refresh, discovery). Any value above 0 is a bug worth reporting; the server log has the stack.\n")
	fmt.Fprintf(w, "# TYPE ncc_api_background_panics_total counter\n")
	fmt.Fprintf(w, "ncc_api_background_panics_total %d\n", backgroundPanicTotal.Load())
	fmt.Fprintf(w, "# HELP ncc_run_auto_retries_total Cumulative count of self-heal auto-retries triggered after a recoverable run failure (rate-limit/timeout/network).\n")
	fmt.Fprintf(w, "# TYPE ncc_run_auto_retries_total counter\n")
	fmt.Fprintf(w, "ncc_run_auto_retries_total %d\n", s.runAutoRetriesTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_selfheal_runs_total Cumulative count of periodic self-heal (doctor) cycles executed.\n")
	fmt.Fprintf(w, "# TYPE ncc_selfheal_runs_total counter\n")
	fmt.Fprintf(w, "ncc_selfheal_runs_total %d\n", s.selfHealRunsTotal.Load())
	fmt.Fprintf(w, "# HELP ncc_selfheal_fixes_total Cumulative count of remediations applied by periodic self-heal cycles.\n")
	fmt.Fprintf(w, "# TYPE ncc_selfheal_fixes_total counter\n")
	fmt.Fprintf(w, "ncc_selfheal_fixes_total %d\n", s.selfHealFixesTotal.Load())
	if rep := s.cachedSelfHeal(); rep != nil {
		fmt.Fprintf(w, "# HELP ncc_selfheal_checks Most recent self-heal check counts by status (ok/warn/fail).\n")
		fmt.Fprintf(w, "# TYPE ncc_selfheal_checks gauge\n")
		for _, status := range []string{"ok", "warn", "fail"} {
			fmt.Fprintf(w, "ncc_selfheal_checks{status=%q} %d\n", status, rep.Summary[status])
		}
	}

	fmt.Fprintf(w, "# HELP ncc_runs_running Number of NCC runs currently executing (concurrent engine).\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_running gauge\n")
	fmt.Fprintf(w, "ncc_runs_running %d\n", runningNow)

	fmt.Fprintf(w, "# HELP ncc_runs_queued Number of NCC runs waiting for a free concurrency slot.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_queued gauge\n")
	fmt.Fprintf(w, "ncc_runs_queued %d\n", queuedNow)

	fmt.Fprintf(w, "# HELP ncc_runs_max_concurrent Configured maximum number of concurrent NCC runs.\n")
	fmt.Fprintf(w, "# TYPE ncc_runs_max_concurrent gauge\n")
	fmt.Fprintf(w, "ncc_runs_max_concurrent %d\n", maxConc)

	// Run-duration summary. Emitting _sum and _count (no buckets) keeps the
	// hand-rolled exposition simple while still letting Prometheus compute the
	// average run time as rate(_sum)/rate(_count).
	durCount := s.runDurationCount.Load()
	durSumSeconds := float64(s.runDurationMillisSum.Load()) / 1000.0
	fmt.Fprintf(w, "# HELP ncc_run_duration_seconds Wall-clock duration of completed NCC runs (summary; sum+count only).\n")
	fmt.Fprintf(w, "# TYPE ncc_run_duration_seconds summary\n")
	fmt.Fprintf(w, "ncc_run_duration_seconds_sum %.3f\n", durSumSeconds)
	fmt.Fprintf(w, "ncc_run_duration_seconds_count %d\n", durCount)

	fmt.Fprintf(w, "# HELP ncc_run_last_duration_seconds Wall-clock duration of the most recently completed NCC run.\n")
	fmt.Fprintf(w, "# TYPE ncc_run_last_duration_seconds gauge\n")
	fmt.Fprintf(w, "ncc_run_last_duration_seconds %.3f\n", float64(s.lastRunDurationMs.Load())/1000.0)

	fmt.Fprintf(w, "# HELP ncc_auth_logins_total Cumulative successful interactive logins (local + LDAP) since process start.\n")
	fmt.Fprintf(w, "# TYPE ncc_auth_logins_total counter\n")
	fmt.Fprintf(w, "ncc_auth_logins_total %d\n", s.loginSuccessTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_auth_login_failures_total Cumulative failed interactive login attempts (bad credentials, locked, or directory unavailable).\n")
	fmt.Fprintf(w, "# TYPE ncc_auth_login_failures_total counter\n")
	fmt.Fprintf(w, "ncc_auth_login_failures_total %d\n", s.loginFailureTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_auth_lockouts_total Cumulative account lockouts triggered by the brute-force guard.\n")
	fmt.Fprintf(w, "# TYPE ncc_auth_lockouts_total counter\n")
	fmt.Fprintf(w, "ncc_auth_lockouts_total %d\n", s.lockoutTotal.Load())

	auth.WritePrometheus(w)

	fmt.Fprintf(w, "# HELP ncc_update_applied_total Cumulative in-app software updates successfully installed.\n")
	fmt.Fprintf(w, "# TYPE ncc_update_applied_total counter\n")
	fmt.Fprintf(w, "ncc_update_applied_total %d\n", s.updateAppliedTotal.Load())

	fmt.Fprintf(w, "# HELP ncc_update_failed_total Cumulative in-app software updates that failed (backup or install error).\n")
	fmt.Fprintf(w, "# TYPE ncc_update_failed_total counter\n")
	fmt.Fprintf(w, "ncc_update_failed_total %d\n", s.updateFailedTotal.Load())

	if s.auditForwarder != nil {
		fmt.Fprintf(w, "# HELP ncc_audit_forward_dropped_total Audit events dropped (buffer full or sink error) instead of forwarded to the SIEM/syslog sink.\n")
		fmt.Fprintf(w, "# TYPE ncc_audit_forward_dropped_total counter\n")
		fmt.Fprintf(w, "ncc_audit_forward_dropped_total %d\n", s.auditForwarder.dropped.Load())
	}

	// Update-availability gauge from the cached `update --check` result (set
	// when the UI last polled). Omitted entirely until a check has run so a
	// stale 0 is never mistaken for "up to date".
	if avail, _, at := s.updateCheckSnapshot(); !at.IsZero() {
		availVal := 0
		if avail {
			availVal = 1
		}
		fmt.Fprintf(w, "# HELP ncc_update_available 1 when the last update check found a newer release in-track, else 0.\n")
		fmt.Fprintf(w, "# TYPE ncc_update_available gauge\n")
		fmt.Fprintf(w, "ncc_update_available %d\n", availVal)
		fmt.Fprintf(w, "# HELP ncc_update_check_timestamp_seconds Unix epoch of the most recent successful update check.\n")
		fmt.Fprintf(w, "# TYPE ncc_update_check_timestamp_seconds gauge\n")
		fmt.Fprintf(w, "ncc_update_check_timestamp_seconds %d\n", at.Unix())
	}

	// Server-side backup inventory (best-effort; a missing/unreadable backups
	// directory simply yields zeros). Lets Prometheus alert on "no recent
	// snapshot" and track encryption coverage.
	if entries, err := s.listBackupEntries(); err == nil {
		byKind := map[string]int{"manual": 0, "pre-update": 0, "other": 0}
		encrypted := 0
		var newest time.Time
		lastTS := int64(0)
		for _, e := range entries {
			byKind[e.Kind]++
			if e.Encrypted {
				encrypted++
			}
			if t, perr := time.Parse(time.RFC3339, e.ModTime); perr == nil && t.After(newest) {
				newest = t
			}
		}
		fmt.Fprintf(w, "# HELP ncc_backups Number of server-side backup snapshots present, by kind.\n")
		fmt.Fprintf(w, "# TYPE ncc_backups gauge\n")
		for _, k := range []string{"manual", "pre-update", "other"} {
			fmt.Fprintf(w, "ncc_backups{kind=%q} %d\n", k, byKind[k])
		}
		fmt.Fprintf(w, "# HELP ncc_backups_encrypted Number of server-side backup snapshots sealed at rest (AES-256-GCM).\n")
		fmt.Fprintf(w, "# TYPE ncc_backups_encrypted gauge\n")
		fmt.Fprintf(w, "ncc_backups_encrypted %d\n", encrypted)
		if !newest.IsZero() {
			lastTS = newest.Unix()
		}
		fmt.Fprintf(w, "# HELP ncc_backup_last_timestamp_seconds Unix epoch modification time of the most recent backup snapshot (0 when none).\n")
		fmt.Fprintf(w, "# TYPE ncc_backup_last_timestamp_seconds gauge\n")
		fmt.Fprintf(w, "ncc_backup_last_timestamp_seconds %d\n", lastTS)
	}

	fmt.Fprintf(w, "# HELP ncc_go_goroutines Number of goroutines in the running api-server process.\n")
	fmt.Fprintf(w, "# TYPE ncc_go_goroutines gauge\n")
	fmt.Fprintf(w, "ncc_go_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP ncc_go_memstats_alloc_bytes Currently allocated heap bytes (runtime.MemStats.Alloc).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(w, "ncc_go_memstats_alloc_bytes %d\n", ms.Alloc)

	fmt.Fprintf(w, "# HELP ncc_go_memstats_sys_bytes Total bytes obtained from the OS (runtime.MemStats.Sys).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(w, "ncc_go_memstats_sys_bytes %d\n", ms.Sys)

	fmt.Fprintf(w, "# HELP ncc_go_memstats_heap_inuse_bytes Heap in-use bytes (runtime.MemStats.HeapInuse).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(w, "ncc_go_memstats_heap_inuse_bytes %d\n", ms.HeapInuse)

	fmt.Fprintf(w, "# HELP ncc_go_gc_total Total GC cycles since process start (runtime.MemStats.NumGC).\n")
	fmt.Fprintf(w, "# TYPE ncc_go_gc_total counter\n")
	fmt.Fprintf(w, "ncc_go_gc_total %d\n", ms.NumGC)

	if s.rateLimiter != nil {
		st := s.rateLimiter.stats(now)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_window_seconds Rate-limiter window length in seconds.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_window_seconds gauge\n")
		fmt.Fprintf(w, "ncc_ratelimit_window_seconds %d\n", st.WindowSeconds)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_active_buckets Number of distinct (client, route) buckets tracked in the current window.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_active_buckets gauge\n")
		fmt.Fprintf(w, "ncc_ratelimit_active_buckets %d\n", st.ActiveBuckets)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_allowed_total Cumulative requests allowed by the rate limiter.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_allowed_total counter\n")
		fmt.Fprintf(w, "ncc_ratelimit_allowed_total %d\n", st.AllowedTotal)
		fmt.Fprintf(w, "# HELP ncc_ratelimit_blocked_total Cumulative requests blocked by the rate limiter.\n")
		fmt.Fprintf(w, "# TYPE ncc_ratelimit_blocked_total counter\n")
		fmt.Fprintf(w, "ncc_ratelimit_blocked_total %d\n", st.BlockedTotal)
	}

	// NCC run metrics derived from the latest run-summary.json. Serving these
	// over /metrics lets Prometheus scrape per-cluster severity/health
	// directly from the api-server instead of relying on a node_exporter
	// textfile collector reading <cluster>.prom files. Best-effort: a missing
	// or unreadable summary simply omits this block.
	if v, ok := s.latestRunSummaryView(); ok {
		fmt.Fprint(w, promtext.RenderRunSummaryMetrics(v))
	}

	// NCC alert lifecycle gauges (persistent store; see alertlifecycle.go).
	s.writeAlertLifecycleMetrics(w)

	// Report data cache state (see reportcachestate.go).
	writeReportCacheMetrics(w, focusedReportCacheStatus(), now)

	// Per-cluster data age and overdue schedules (see freshness_report.go).
	s.writeFreshnessMetrics(w)

	// Per-route request counts and latency (see requestmeta.go).
	s.httpMetrics.write(w)
}

// latestRunSummaryView loads the most recent run-summary.json into the view
// used to render Prometheus run metrics. It returns ok=false when no readable
// summary exists.
func (s *apiServer) latestRunSummaryView() (promtext.RunSummaryView, bool) {
	path := filepath.Join(s.absPath(s.outputDir), "run-summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return promtext.RunSummaryView{}, false
	}
	var v promtext.RunSummaryView
	if err := json.Unmarshal(data, &v); err != nil {
		return promtext.RunSummaryView{}, false
	}
	return v, true
}
