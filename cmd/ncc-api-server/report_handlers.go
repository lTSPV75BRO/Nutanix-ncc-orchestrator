package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"goncc/internal/ai"
)

func (s *apiServer) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.artifactsRestricted(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "raw report artifacts include all clusters and are limited to administrators; use the dashboard for your cluster groups"})
		return
	}
	dir := s.absPath(s.outputDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: err.Error()})
		return
	}
	out := make([]artifactInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if isInternalArtifactName(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, artifactInfo{
			Name:    e.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: out})
}

// artifactInlineMaxBytes caps the size of artifact bodies returned inline as
// JSON. Anything bigger is served as a truncated tail with a `truncated: true`
// flag so the UI can prompt the user to download instead. Direct downloads
// (?download=1) stream the full file via http.ServeFile and aren't subject to
// this cap.
const artifactInlineMaxBytes = 5 * 1024 * 1024

func (s *apiServer) handleArtifactByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if s.artifactsRestricted(r) {
		writeJSON(w, http.StatusForbidden, envelope{Success: false, Error: "raw report artifacts include all clusters and are limited to administrators; use the dashboard for your cluster groups"})
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, "..") {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid artifact name"})
		return
	}
	if isInternalArtifactName(name) {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "artifact not found"})
		return
	}
	path := filepath.Join(s.absPath(s.outputDir), name)
	info, statErr := os.Stat(path)
	if statErr != nil || info.IsDir() {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "artifact not found"})
		return
	}
	if r.URL.Query().Get("download") == "1" {
		ct := mime.TypeByExtension(filepath.Ext(name))
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
		// http.ServeFile streams the response and supports Range requests,
		// avoiding an arbitrary-size buffer for large artifacts (NCC logs can
		// easily be tens of megabytes).
		http.ServeFile(w, r, path)
		return
	}
	// Inline JSON view: read up to the cap. Truncate large files so a
	// careless click on a huge log file can't OOM the server.
	f, openErr := os.Open(path)
	if openErr != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: openErr.Error()})
		return
	}
	defer f.Close()
	b, readErr := io.ReadAll(io.LimitReader(f, artifactInlineMaxBytes+1))
	if readErr != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: readErr.Error()})
		return
	}
	truncated := false
	if int64(len(b)) > artifactInlineMaxBytes {
		b = b[:artifactInlineMaxBytes]
		truncated = true
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"name":      name,
		"size":      info.Size(),
		"mod_time":  info.ModTime().UTC().Format(time.RFC3339),
		"content":   string(b),
		"truncated": truncated,
		"max_bytes": int64(artifactInlineMaxBytes),
	}})
}

func (s *apiServer) handleReportData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	pg, err := parseReportDataPagination(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	// Cluster-group scoping: non-admins who belong to one or more cluster groups
	// only see data for clusters in those groups; ungrouped non-admins (and
	// admins/static tokens) are unrestricted. The helper is a no-op when
	// unrestricted, so the filter calls below are cheap for those callers.
	p, _ := principalFromContext(r.Context())
	access := s.allowedClusters(p)
	outDir := s.selectBestReportOutDir()
	// Report files are parsed once per run and shared between requests (see
	// artifactcache.go); deepFilterClusters copies whatever it filters.
	//
	// run-summary.json is healed at load: `error_class` / `failure_classes`
	// from older orchestrator builds that misclassified DNS-driven
	// circuit-breaker errors as `rate_limit` are re-classified, and absolute
	// paths are scrubbed.
	runSummary := deepFilterClusters(readRunSummaryCached(outDir), access)
	flags := s.loadFeatureFlags()
	allAggRows := readAggRowsCached(outDir)
	// With the aggregated rows present, the UI reads the checks snapshot only
	// for cluster names, so its per-check lists (most of its size) are left out.
	var rawChecks interface{}
	if len(allAggRows) > 0 {
		rawChecks = slimChecksSnapshotCached(outDir)
	} else {
		rawChecks = readJSONArtifactCached(filepath.Join(outDir, "checks-snapshot.json"), []interface{}{})
	}
	checksSnapshot := deepFilterClusters(rawChecks, access)
	filters, filterErr := parseReportFindingFilters(r)
	if filterErr != nil {
		writeJSON(w, http.StatusBadRequest, envelope{
			Success: false, Error: filterErr.Error(), ErrorCode: ai.CodeInvalidArgument,
		})
		return
	}
	checksSnapshot = filterReportRows(checksSnapshot, filters)
	var drilldownDiff interface{} = map[string]interface{}{}
	var regressionSummary interface{} = map[string]interface{}{}
	if flags.RunComparison {
		drilldownDiff = deepFilterClusters(readJSONArtifactCached(filepath.Join(outDir, "drilldown-diff.json"), map[string]interface{}{}), access)
		regressionSummary = deepFilterClusters(readJSONArtifactCached(filepath.Join(outDir, "regression-summary.json"), map[string]interface{}{}), access)
	}
	var flakyChecks interface{} = map[string]interface{}{}
	if flags.FlakyChecks {
		flakyChecks = deepFilterClusters(readJSONArtifactCached(filepath.Join(outDir, "flaky-checks.json"), map[string]interface{}{}), access)
	}
	var sloDashboard interface{} = map[string]interface{}{}
	if flags.SLO {
		sloDashboard = deepFilterClusters(readJSONArtifactCached(filepath.Join(outDir, "slo-dashboard.json"), map[string]interface{}{}), access)
	}
	policyViolations := []string{}
	if b, err := os.ReadFile(filepath.Join(outDir, "policy-gates.txt")); err == nil {
		for _, ln := range strings.Split(string(b), "\n") {
			ln = strings.TrimSpace(ln)
			if ln == "" {
				continue
			}
			policyViolations = append(policyViolations, ln)
		}
	}
	pagination := map[string]interface{}{}
	// Large runs keep the rows in aggregated-data.json rather than index.html;
	// reading only index.html showed no findings above 20,000 rows.
	aggRows := deepFilterClusters(allAggRows, access)
	aggRows = filterReportRows(aggRows, filters)
	if rows, ok := checksSnapshot.([]interface{}); ok && pg.enabled() {
		pageRows, meta := paginateAnySlice(rows, pg)
		checksSnapshot = pageRows
		pagination["checks_snapshot"] = meta
	}
	if rows, ok := aggRows.([]interface{}); ok && pg.enabled() {
		pageRows, meta := paginateAnySlice(rows, pg)
		aggRows = pageRows
		pagination["agg_rows"] = meta
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"run_summary":         runSummary,
		"checks_snapshot":     checksSnapshot,
		"drilldown_diff":      drilldownDiff,
		"flaky_checks":        flakyChecks,
		"regression_summary":  regressionSummary,
		"slo_dashboard":       sloDashboard,
		"policy_violations":   policyViolations,
		"agg_rows":            aggRows,
		"diff_flags":          readInlineJSONVarCached(filepath.Join(outDir, "index.html"), "DIFF_FLAGS", map[string]interface{}{}),
		"flaky_keys":          readInlineJSONVarCached(filepath.Join(outDir, "index.html"), "FLAKY_KEYS", map[string]interface{}{}),
		"cluster_links":       deepFilterClusters(readInlineJSONVarCached(filepath.Join(outDir, "index.html"), "CLUSTER_LINKS", []interface{}{}), access),
		"artifact_links":      readInlineJSONVarCached(filepath.Join(outDir, "index.html"), "ARTIFACT_LINKS", map[string]interface{}{}),
		"report_meta":         loadReportMeta(outDir),
		"ncc_logs":            nccLogsForReport(s, flags, outDir),
		"ncc_summary_counts":  nccSummaryForReport(s, flags, outDir),
		"ncc_cluster_summary": nccClusterSummaryForReport(s, flags, access, outDir),
		"trends":              trendsForReport(flags, outDir),
		"report_source_dir":   s.pathForCaller(r, outDir),
		"pagination":          pagination,
		// Built last, after the reads above started any background refresh.
		"cache": s.reportCacheStatusFor(outDir, flags),
	}})
}

type reportDataPagination struct {
	Limit        int
	Offset       int
	CursorOffset int
	HasCursor    bool
}

type reportFindingFilters struct {
	Cluster  string
	Check    string
	Severity string
}

func parseReportFindingFilters(r *http.Request) (reportFindingFilters, error) {
	q := r.URL.Query()
	out := reportFindingFilters{
		Cluster:  strings.TrimSpace(q.Get("cluster")),
		Check:    strings.TrimSpace(q.Get("check")),
		Severity: strings.ToUpper(strings.TrimSpace(q.Get("severity"))),
	}
	if out.Severity != "" {
		switch out.Severity {
		case "FAIL", "WARN", "ERR", "INFO":
		default:
			return out, errors.New("severity must be one of FAIL, WARN, ERR, INFO")
		}
	}
	return out, nil
}

func filterReportRows(value interface{}, filters reportFindingFilters) interface{} {
	if filters.Cluster == "" && filters.Check == "" && filters.Severity == "" {
		return value
	}
	rows, ok := value.([]interface{})
	if !ok {
		return value
	}
	out := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		m, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		cluster := strings.ToLower(fmt.Sprint(firstMapValue(m, "cluster", "address", "cluster_name")))
		check := strings.ToLower(fmt.Sprint(firstMapValue(m, "check", "check_name", "name")))
		severity := strings.ToUpper(fmt.Sprint(firstMapValue(m, "severity", "status")))
		if filters.Cluster != "" && !strings.Contains(cluster, strings.ToLower(filters.Cluster)) {
			continue
		}
		if filters.Check != "" && !strings.Contains(check, strings.ToLower(filters.Check)) {
			continue
		}
		if filters.Severity != "" && severity != filters.Severity {
			continue
		}
		out = append(out, row)
	}
	return out
}

func firstMapValue(m map[string]interface{}, keys ...string) interface{} {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			return value
		}
	}
	return ""
}

func (p reportDataPagination) enabled() bool {
	return p.Limit > 0 || p.Offset > 0 || p.HasCursor
}

func parseReportDataPagination(r *http.Request) (reportDataPagination, error) {
	const maxLimit = 5000
	out := reportDataPagination{}
	limitRaw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if limitRaw != "" {
		v, err := strconv.Atoi(limitRaw)
		if err != nil || v < 0 {
			return out, errors.New("limit must be a non-negative integer")
		}
		if v > maxLimit {
			return out, fmt.Errorf("limit must be <= %d", maxLimit)
		}
		out.Limit = v
	}
	offsetRaw := strings.TrimSpace(r.URL.Query().Get("offset"))
	if offsetRaw != "" {
		v, err := strconv.Atoi(offsetRaw)
		if err != nil || v < 0 {
			return out, errors.New("offset must be a non-negative integer")
		}
		out.Offset = v
	}
	cursorRaw := strings.TrimSpace(r.URL.Query().Get("cursor"))
	if cursorRaw != "" {
		if out.Offset > 0 {
			return out, errors.New("cursor and offset cannot be used together")
		}
		v, err := strconv.Atoi(cursorRaw)
		if err != nil || v < 0 {
			return out, errors.New("cursor must be a non-negative opaque offset")
		}
		out.CursorOffset = v
		out.HasCursor = true
	}
	return out, nil
}

func paginateAnySlice(items []interface{}, p reportDataPagination) ([]interface{}, map[string]interface{}) {
	total := len(items)
	start := p.Offset
	if p.HasCursor {
		start = p.CursorOffset
	}
	if start > total {
		start = total
	}
	end := total
	if p.Limit > 0 && start+p.Limit < end {
		end = start + p.Limit
	}
	out := items[start:end]
	meta := map[string]interface{}{
		"total":    total,
		"offset":   p.Offset,
		"limit":    p.Limit,
		"count":    len(out),
		"has_more": end < total,
	}
	if p.HasCursor {
		meta["cursor"] = strconv.Itoa(p.CursorOffset)
	}
	if end < total {
		meta["next_cursor"] = strconv.Itoa(end)
	}
	return out, meta
}

type trendClusterSummary struct {
	FailCount int `json:"fail_count"`
	WarnCount int `json:"warn_count"`
	ErrCount  int `json:"err_count"`
	InfoCount int `json:"info_count"`
}

type trendRunSummary struct {
	Timestamp      string                `json:"timestamp"`
	DurationS      float64               `json:"duration_s"`
	ClustersOK     int                   `json:"clusters_ok"`
	ClustersFailed int                   `json:"clusters_failed"`
	TotalChecks    int                   `json:"total_checks"`
	AvgHealthScore int                   `json:"avg_health_score"`
	MinHealthScore int                   `json:"min_health_score"`
	ExitCode       *int                  `json:"exit_code,omitempty"`
	Source         string                `json:"source,omitempty"`
	Clusters       []trendClusterSummary `json:"clusters"`
}

type trendPoint struct {
	Timestamp      string  `json:"timestamp"`
	DurationS      float64 `json:"duration_s"`
	ClustersOK     int     `json:"clusters_ok"`
	ClustersFailed int     `json:"clusters_failed"`
	TotalChecks    int     `json:"total_checks"`
	AvgHealthScore int     `json:"avg_health_score"`
	MinHealthScore int     `json:"min_health_score"`
	FailTotal      int     `json:"fail_total"`
	WarnTotal      int     `json:"warn_total"`
	ErrTotal       int     `json:"err_total"`
	InfoTotal      int     `json:"info_total"`
}

func parseTrendLimit(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 30
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 30
	}
	if n > 365 {
		return 365
	}
	return n
}

func readTrendRunSummary(path string) (trendPoint, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return trendPoint{}, false
	}
	var s trendRunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return trendPoint{}, false
	}
	failTotal, warnTotal, errTotal, infoTotal := 0, 0, 0, 0
	for _, c := range s.Clusters {
		failTotal += c.FailCount
		warnTotal += c.WarnCount
		errTotal += c.ErrCount
		infoTotal += c.InfoCount
	}
	return trendPoint{
		Timestamp:      s.Timestamp,
		DurationS:      s.DurationS,
		ClustersOK:     s.ClustersOK,
		ClustersFailed: s.ClustersFailed,
		TotalChecks:    s.TotalChecks,
		AvgHealthScore: s.AvgHealthScore,
		MinHealthScore: s.MinHealthScore,
		FailTotal:      failTotal,
		WarnTotal:      warnTotal,
		ErrTotal:       errTotal,
		InfoTotal:      infoTotal,
	}, true
}

func collectTrendPoints(outDir string, limit int) []trendPoint {
	points := make([]trendPoint, 0)
	seenTs := map[string]bool{}
	addPoint := func(p trendPoint) {
		if strings.TrimSpace(p.Timestamp) == "" {
			return
		}
		if seenTs[p.Timestamp] {
			return
		}
		seenTs[p.Timestamp] = true
		points = append(points, p)
	}
	if p, ok := readTrendRunSummary(filepath.Join(outDir, "run-summary.json")); ok {
		addPoint(p)
	}
	runsDir := filepath.Join(outDir, "runs")
	entries, err := os.ReadDir(runsDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if p, ok := readTrendRunSummary(filepath.Join(runsDir, e.Name(), "run-summary.json")); ok {
				addPoint(p)
			}
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	if limit > 0 && len(points) > limit {
		points = points[len(points)-limit:]
	}
	return points
}

func (s *apiServer) handleReportTrends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if !s.loadFeatureFlags().Insights {
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"points":   []interface{}{},
			"count":    0,
			"disabled": true,
		}})
		return
	}
	outDir := s.selectBestReportOutDir()
	limit := parseTrendLimit(r.URL.Query().Get("limit"))
	points := collectTrendPoints(outDir, limit)
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"points":            points,
		"count":             len(points),
		"limit":             limit,
		"report_source_dir": outDir,
	}})
}
