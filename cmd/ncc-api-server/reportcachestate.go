package main

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Report-cache observability.
//
// The report endpoints serve parsed artifacts from reportArtifacts; the NCC
// log summaries are refreshed in the background while the previous value is
// served (derivedAsync). This file turns the cache entries behind the current
// report into one state that GET /api/v1/report/data returns as `cache` and
// /metrics exports as ncc_report_cache_state / ncc_report_cache_age_seconds:
//
//	cold        nothing for the current report has been built yet
//	warm        everything served matches the report files on disk
//	refreshing  a newer report is being built; the previous one is served
//	stale       the report files changed and no refresh has started yet
//	failed      the last refresh failed; the previous report is served

const (
	reportCacheCold       = "cold"
	reportCacheWarm       = "warm"
	reportCacheRefreshing = "refreshing"
	reportCacheStale      = "stale"
	reportCacheFailed     = "failed"
)

// reportCacheStates lists every state in a fixed order (metrics output).
var reportCacheStates = []string{reportCacheCold, reportCacheWarm, reportCacheRefreshing, reportCacheStale, reportCacheFailed}

// entryState is the observable part of an artifactEntry. Guarded by the
// owning artifactCache's mu.
type entryState struct {
	loaded         bool
	stamp          fileStamp // sources' signature when the value was built
	sources        []string
	builtAt        time.Time
	building       bool // a synchronous build is in progress
	refreshStarted time.Time
	refreshSig     fileStamp // sources' signature when the refresh started
	lastErr        string
	failedAt       time.Time
}

func (c *artifactCache) noteBuildStart(e *artifactEntry, sources []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.st.building = true
	e.st.sources = sources
}

// noteBuildEnd records the end of a build. built is false when nothing new was
// stored (read error, panic).
func (c *artifactCache) noteBuildEnd(e *artifactEntry, built bool, stamp fileStamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.st.building = false
	if !built {
		return
	}
	e.st.loaded = true
	e.st.stamp = stamp
	e.st.builtAt = time.Now()
	e.st.refreshStarted = time.Time{}
	e.st.lastErr = ""
	e.st.failedAt = time.Time{}
}

func (c *artifactCache) noteRefreshStart(e *artifactEntry, sources []string, sig fileStamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.st.sources = sources
	e.st.refreshStarted = time.Now()
	e.st.refreshSig = sig
}

func (c *artifactCache) noteRefreshFailed(e *artifactEntry, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.TrimSpace(reason) == "" {
		reason = "the refresh stopped unexpectedly"
	}
	e.st.refreshStarted = time.Time{}
	e.st.lastErr = reason
	e.st.failedAt = time.Now()
}

// holdRetry reports whether a refresh of e failed less than retryAfter ago.
func (c *artifactCache) holdRetry(e *artifactEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.retryAfter > 0 && e.st.lastErr != "" && time.Since(e.st.failedAt) < c.retryAfter
}

// reportCacheStatus is the `cache` object of GET /api/v1/report/data.
type reportCacheStatus struct {
	State string `json:"state"`
	// BuiltAt is when the oldest part of the served report was built.
	BuiltAt          string `json:"built_at,omitempty"`
	RefreshStartedAt string `json:"refresh_started_at,omitempty"`
	LastError        string `json:"last_error,omitempty"`
	FailedAt         string `json:"failed_at,omitempty"`
	SourceRunID      string `json:"source_run_id,omitempty"`
	SourceRunTime    string `json:"source_run_time,omitempty"`

	builtAt time.Time
}

// status combines the entries under keys into one report-cache state. Keys
// without an entry (missing optional files, features turned off) are skipped.
// It never waits for a build in progress.
func (c *artifactCache) status(keys []string) reportCacheStatus {
	c.mu.Lock()
	snaps := make([]entryState, 0, len(keys))
	for _, k := range keys {
		if e, ok := c.entries[k]; ok {
			snaps = append(snaps, e.st)
		}
	}
	c.mu.Unlock()

	var (
		loaded, refreshing, failed, stale bool
		builtAt, refreshStarted, failedAt time.Time
		lastErr                           string
	)
	for _, st := range snaps {
		if !st.loaded {
			continue
		}
		loaded = true
		if builtAt.IsZero() || st.builtAt.Before(builtAt) {
			builtAt = st.builtAt
		}
		// A periodic re-check of unchanged sources (ttl expiry) serves the
		// current report, so it does not count as refreshing.
		if st.building || (!st.refreshStarted.IsZero() && st.refreshSig != st.stamp) {
			refreshing = true
			if !st.refreshStarted.IsZero() && (refreshStarted.IsZero() || st.refreshStarted.Before(refreshStarted)) {
				refreshStarted = st.refreshStarted
			}
		}
		if st.lastErr != "" {
			failed = true
			if st.failedAt.After(failedAt) {
				failedAt, lastErr = st.failedAt, st.lastErr
			}
		}
		if sourceSig(st.sources) != st.stamp {
			stale = true
		}
	}
	out := reportCacheStatus{builtAt: builtAt}
	switch {
	case !loaded:
		out.State = reportCacheCold
	case refreshing:
		out.State = reportCacheRefreshing
	case failed:
		out.State = reportCacheFailed
	case stale:
		out.State = reportCacheStale
	default:
		out.State = reportCacheWarm
	}
	if !builtAt.IsZero() {
		out.BuiltAt = builtAt.UTC().Format(time.RFC3339)
	}
	if !refreshStarted.IsZero() {
		out.RefreshStartedAt = refreshStarted.UTC().Format(time.RFC3339)
	}
	if failed {
		out.LastError = lastErr
		out.FailedAt = failedAt.UTC().Format(time.RFC3339)
	}
	return out
}

// reportCacheKeys are the cache entries that make up the report served from
// outDir: the run summary, the aggregated rows and (when the NCC log index is
// on) the log summaries refreshed in the background.
func reportCacheKeys(outDir, logDir string, logs bool) []string {
	summary := filepath.Join(outDir, "run-summary.json")
	keys := []string{
		"runsummary:" + summary,
		"inline:AGG:" + filepath.Join(outDir, "index.html"),
		"json:" + filepath.Join(outDir, "aggregated-data.json"),
	}
	if logs {
		for _, kind := range []string{"list", "counts", "clusters"} {
			keys = append(keys, "logs:"+kind+":"+logDir)
		}
	}
	return keys
}

// The keys of the report last served or warmed, for /metrics (which must not
// pick the report directory itself: that parses the report on a cold cache).
var reportCacheFocus struct {
	sync.Mutex
	keys []string
}

func (s *apiServer) focusReportCache(outDir string, flags featureFlags) []string {
	keys := reportCacheKeys(outDir, s.absPath(s.logDir), flags.NCCLogIndex)
	reportCacheFocus.Lock()
	reportCacheFocus.keys = keys
	reportCacheFocus.Unlock()
	return keys
}

func focusedReportCacheStatus() reportCacheStatus {
	reportCacheFocus.Lock()
	keys := reportCacheFocus.keys
	reportCacheFocus.Unlock()
	if len(keys) == 0 {
		return reportCacheStatus{State: reportCacheCold}
	}
	return reportArtifacts.status(keys)
}

// reportCacheStatusFor is the `cache` object for a /report/data response.
func (s *apiServer) reportCacheStatusFor(outDir string, flags featureFlags) reportCacheStatus {
	st := reportArtifacts.status(s.focusReportCache(outDir, flags))
	if sum, ok := readRunSummaryCached(outDir).(map[string]interface{}); ok {
		st.SourceRunID = strings.TrimSpace(fmt.Sprint(firstNonNil(sum["run_id"])))
		st.SourceRunTime = strings.TrimSpace(fmt.Sprint(firstNonNil(sum["timestamp"])))
	}
	return st
}

func firstNonNil(v interface{}) interface{} {
	if v == nil {
		return ""
	}
	return v
}

// writeReportCacheMetrics emits the report-cache gauges on /metrics.
func writeReportCacheMetrics(w io.Writer, st reportCacheStatus, now time.Time) {
	fmt.Fprintf(w, "# HELP ncc_report_cache_state Report cache state: 1 for the current state (cold, warm, refreshing, stale, failed), 0 for the others.\n")
	fmt.Fprintf(w, "# TYPE ncc_report_cache_state gauge\n")
	for _, state := range reportCacheStates {
		v := 0
		if state == st.State {
			v = 1
		}
		fmt.Fprintf(w, "ncc_report_cache_state{state=%q} %d\n", state, v)
	}
	if !st.builtAt.IsZero() {
		age := now.Sub(st.builtAt).Seconds()
		if age < 0 {
			age = 0
		}
		fmt.Fprintf(w, "# HELP ncc_report_cache_age_seconds Seconds since the oldest part of the served report was built.\n")
		fmt.Fprintf(w, "# TYPE ncc_report_cache_age_seconds gauge\n")
		fmt.Fprintf(w, "ncc_report_cache_age_seconds %.3f\n", age)
	}
}
