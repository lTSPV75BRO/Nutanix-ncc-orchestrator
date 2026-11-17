package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func waitForState(t *testing.T, c *artifactCache, keys []string, want string) reportCacheStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := c.status(keys)
		if st.State == want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("state = %q, want %q (%+v)", st.State, want, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// cold → warm → stale → refreshing → warm → failed (held) → warm.
func TestReportCacheStateTransitions(t *testing.T) {
	captureServerLog(t, slog.LevelDebug)
	dir := t.TempDir()
	src := filepath.Join(dir, "run-summary.json")
	writeTestFile(t, src, `{"v":1}`)
	sources := []string{src}
	keys := []string{"k", "missing"}

	c := newArtifactCache(1 << 20)
	c.retryAfter = time.Hour
	if st := c.status(keys); st.State != reportCacheCold || st.BuiltAt != "" {
		t.Fatalf("empty cache: %+v", st)
	}

	// First load is synchronous.
	if got := c.derivedAsync("k", sources, time.Hour, 1, func() interface{} { return "v1" }); got != "v1" {
		t.Fatalf("first load: %v", got)
	}
	st := c.status(keys)
	if st.State != reportCacheWarm || st.BuiltAt == "" || st.LastError != "" {
		t.Fatalf("after first load: %+v", st)
	}

	// A new report on disk: stale until a request starts the refresh.
	writeTestFile(t, src, `{"v":22}`)
	if st := c.status(keys); st.State != reportCacheStale {
		t.Fatalf("after the source changed: %+v", st)
	}

	release := make(chan struct{})
	if got := c.derivedAsync("k", sources, time.Hour, 1, func() interface{} { <-release; return "v2" }); got != "v1" {
		t.Fatalf("previous value expected while refreshing: %v", got)
	}
	st = c.status(keys)
	if st.State != reportCacheRefreshing || st.RefreshStartedAt == "" {
		t.Fatalf("during refresh: %+v", st)
	}
	close(release)
	st = waitForState(t, c, keys, reportCacheWarm)
	if st.RefreshStartedAt != "" {
		t.Fatalf("refresh start kept after it finished: %+v", st)
	}

	// A periodic re-check of unchanged files (ttl expired) is not a new report.
	hold := make(chan struct{})
	c.derivedAsync("ttl", sources, time.Nanosecond, 1, func() interface{} { return "t1" })
	time.Sleep(2 * time.Millisecond)
	c.derivedAsync("ttl", sources, time.Nanosecond, 1, func() interface{} { <-hold; return "t2" })
	if st := c.status([]string{"ttl"}); st.State != reportCacheWarm {
		close(hold)
		t.Fatalf("ttl re-check reported as %+v", st)
	}
	close(hold)

	// A refresh that fails: the old value is served and the error reported.
	writeTestFile(t, src, `{"v":333}`)
	if got := c.derivedAsync("k", sources, time.Hour, 1, func() interface{} { panic("log directory unreadable") }); got != "v2" {
		t.Fatalf("previous value expected: %v", got)
	}
	st = waitForState(t, c, keys, reportCacheFailed)
	if st.LastError != "log directory unreadable" || st.FailedAt == "" {
		t.Fatalf("failure not reported: %+v", st)
	}
	// Within retryAfter a request does not start another refresh.
	var called atomic.Bool
	if got := c.derivedAsync("k", sources, time.Hour, 1, func() interface{} { called.Store(true); return "v3" }); got != "v2" {
		t.Fatalf("previous value expected while holding: %v", got)
	}
	time.Sleep(20 * time.Millisecond)
	if called.Load() || c.status(keys).State != reportCacheFailed {
		t.Fatalf("refresh retried too soon (called=%v, %+v)", called.Load(), c.status(keys))
	}

	// Once the hold is over the next request retries, and success clears it.
	c.retryAfter = 0
	c.derivedAsync("k", sources, time.Hour, 1, func() interface{} { return "v3" })
	st = waitForState(t, c, keys, reportCacheWarm)
	if st.LastError != "" || st.FailedAt != "" {
		t.Fatalf("failure kept after a good refresh: %+v", st)
	}
}

// Synchronously parsed files count as stale when the file changes underneath.
func TestReportCacheStateFileEntries(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "run-summary.json")
	writeTestFile(t, p, `{"a":1}`)
	c := newArtifactCache(1 << 20)
	if _, ok := c.file("f", p, parseJSONValue); !ok {
		t.Fatal("parse failed")
	}
	if st := c.status([]string{"f"}); st.State != reportCacheWarm {
		t.Fatalf("after load: %+v", st)
	}
	writeTestFile(t, p, `{"a":12345}`)
	if st := c.status([]string{"f"}); st.State != reportCacheStale {
		t.Fatalf("after rewrite: %+v", st)
	}
	c.file("f", p, parseJSONValue)
	if st := c.status([]string{"f"}); st.State != reportCacheWarm {
		t.Fatalf("after reload: %+v", st)
	}
}

func TestReportCacheMetrics(t *testing.T) {
	now := time.Now()
	var b bytes.Buffer
	writeReportCacheMetrics(&b, reportCacheStatus{State: reportCacheRefreshing, builtAt: now.Add(-90 * time.Second)}, now)
	out := b.String()
	for _, want := range []string{
		"# TYPE ncc_report_cache_state gauge",
		`ncc_report_cache_state{state="cold"} 0`,
		`ncc_report_cache_state{state="warm"} 0`,
		`ncc_report_cache_state{state="refreshing"} 1`,
		`ncc_report_cache_state{state="stale"} 0`,
		`ncc_report_cache_state{state="failed"} 0`,
		"# TYPE ncc_report_cache_age_seconds gauge",
		"ncc_report_cache_age_seconds 90.000",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}

	b.Reset()
	writeReportCacheMetrics(&b, reportCacheStatus{State: reportCacheCold}, now)
	if !strings.Contains(b.String(), `ncc_report_cache_state{state="cold"} 1`) || strings.Contains(b.String(), "ncc_report_cache_age_seconds") {
		t.Fatalf("cold output:\n%s", b.String())
	}
}

// /report/data carries the cache object and /metrics the matching gauge.
func TestReportDataIncludesCacheState(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "outputfiles")
	writeTestFile(t, filepath.Join(out, "index.html"), "const AGG = [{\"cluster\":\"c1\",\"check\":\"x\",\"severity\":\"WARN\"}]\n")
	writeTestFile(t, filepath.Join(out, "run-summary.json"), `{"run_id":"run-42","timestamp":"2026-11-01T00:00:00Z","clusters":[]}`)

	logs := filepath.Join(dir, "ncc-logs")
	writeTestFile(t, filepath.Join(logs, "c1.log"), "WARN: something\n")
	// A log directory of its own: the alert store written under repoRoot
	// during the metrics call must not count as a new report.
	s := &apiServer{repoRoot: dir, outputDir: out, logDir: logs}
	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/report/data", nil), principal{subject: "a", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleReportData(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Data struct {
			Cache   map[string]interface{}   `json:"cache"`
			AggRows []map[string]interface{} `json:"agg_rows"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Data.AggRows) != 1 {
		t.Fatalf("existing fields changed: %v", env.Data.AggRows)
	}
	c := env.Data.Cache
	if c["state"] != reportCacheWarm || c["built_at"] == nil || c["source_run_id"] != "run-42" || c["source_run_time"] != "2026-11-01T00:00:00Z" {
		t.Fatalf("cache = %v", c)
	}

	mrec := httptest.NewRecorder()
	s.handlePrometheusMetrics(mrec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := mrec.Body.String(); !strings.Contains(body, `ncc_report_cache_state{state="warm"} 1`) || !strings.Contains(body, "ncc_report_cache_age_seconds ") {
		t.Fatalf("metrics missing report cache gauges:\n%s", body)
	}
}
