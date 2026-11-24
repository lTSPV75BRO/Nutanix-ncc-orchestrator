package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"
)

type JSONOutput struct {
	GeneratedAt string      `json:"generated_at"`
	Checks      []JSONCheck `json:"checks"`
	Summary     JSONSummary `json:"summary"`
}

type JSONCheck struct {
	Severity  string `json:"severity"`
	CheckName string `json:"check_name"`
	Detail    string `json:"detail"`
}

type JSONSummary struct {
	Total int            `json:"total"`
	Count map[string]int `json:"count"`
}

// RunSummaryJSON is the machine-readable run result written to run-summary.json.
type RunSummaryJSON struct {
	SchemaVersion  string              `json:"schema_version"`
	RunID          string              `json:"run_id,omitempty"`
	Timestamp      string              `json:"timestamp"`
	DurationS      float64             `json:"duration_s"`
	ClustersOK     int                 `json:"clusters_ok"`
	ClustersFailed int                 `json:"clusters_failed"`
	FailedClusters []string            `json:"failed_clusters,omitempty"`
	Clusters       []RunClusterSummary `json:"clusters,omitempty"`
	// ExitCode is always written (no omitempty): with omitempty a successful
	// run (exit code 0) omitted the field and the UI could not tell success
	// from "unknown".
	ExitCode int `json:"exit_code"`
	// Status is the explicit outcome: "success", "partial" (some clusters
	// failed, exit 3) or "failed".
	Status         string         `json:"status,omitempty"`
	IndexHTML      string         `json:"index_html"`
	TotalChecks    int            `json:"total_checks,omitempty"`
	AvgHealthScore int            `json:"avg_health_score,omitempty"`
	MinHealthScore int            `json:"min_health_score,omitempty"`
	FailureClasses map[string]int `json:"failure_classes,omitempty"`
	// Source records how the run was launched ("scheduled", "manual", ""),
	// taken from NCC_RUN_SOURCE, so the dashboard can distinguish a systemd-
	// timer/cron scheduled run from an interactive/API-triggered one.
	Source string `json:"source,omitempty"`
}

// runSummaryStatus maps the run exit code onto the run-summary status field.
func runSummaryStatus(exitCode, clustersOK int) string {
	switch {
	case exitCode == 0:
		return "success"
	case exitCode == 3 && clustersOK > 0:
		return "partial"
	default:
		return "failed"
	}
}

// RunClusterSummary is per-cluster stats for automation (run-summary.json).
type RunClusterSummary struct {
	Address     string `json:"address"`
	OK          bool   `json:"ok"`
	Error       string `json:"error,omitempty"`
	ErrorClass  string `json:"error_class,omitempty"`
	FailCount   int    `json:"fail_count,omitempty"`
	WarnCount   int    `json:"warn_count,omitempty"`
	ErrCount    int    `json:"err_count,omitempty"`
	InfoCount   int    `json:"info_count,omitempty"`
	ChecksTotal int    `json:"checks_total,omitempty"`
	HealthScore int    `json:"health_score,omitempty"`
}

type CheckSnapshotEntry struct {
	FindingID string `json:"finding_id,omitempty"`
	CheckName string `json:"check_name"`
	Severity  string `json:"severity"`
	Detail    string `json:"detail,omitempty"`
}

func stableFindingID(cluster, check string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(cluster)) + "\x00" + strings.ToLower(strings.TrimSpace(check))))
	return "finding-" + hex.EncodeToString(sum[:8])
}

type ClusterChecksSnapshot struct {
	Address     string               `json:"address"`
	Checks      []CheckSnapshotEntry `json:"checks,omitempty"`
	FailCount   int                  `json:"fail_count,omitempty"`
	WarnCount   int                  `json:"warn_count,omitempty"`
	ErrCount    int                  `json:"err_count,omitempty"`
	InfoCount   int                  `json:"info_count,omitempty"`
	ChecksTotal int                  `json:"checks_total,omitempty"`
	HealthScore int                  `json:"health_score,omitempty"`
}

type ChecksSnapshotJSON struct {
	Timestamp string                  `json:"timestamp"`
	Clusters  []ClusterChecksSnapshot `json:"clusters,omitempty"`
}

type SeverityChange struct {
	CheckName string `json:"check_name"`
	From      string `json:"from"`
	To        string `json:"to"`
}

type ClusterDiffSummary struct {
	Address          string           `json:"address"`
	NewFailures      []string         `json:"new_failures,omitempty"`
	ResolvedFailures []string         `json:"resolved_failures,omitempty"`
	NewChecks        []string         `json:"new_checks,omitempty"`
	RemovedChecks    []string         `json:"removed_checks,omitempty"`
	SeverityChanges  []SeverityChange `json:"severity_changes,omitempty"`
}

type DrillDownDiffJSON struct {
	Timestamp         string               `json:"timestamp"`
	PreviousTimestamp string               `json:"previous_timestamp,omitempty"`
	NewFailCount      int                  `json:"new_fail_count"`
	ResolvedFailCount int                  `json:"resolved_fail_count"`
	Clusters          []ClusterDiffSummary `json:"clusters,omitempty"`
}

type FlakyCheckSummary struct {
	Cluster      string   `json:"cluster"`
	CheckName    string   `json:"check_name"`
	Transitions  int      `json:"transitions"`
	Observations int      `json:"observations"`
	States       []string `json:"states,omitempty"`
	Current      string   `json:"current"`
}

type FlakyChecksJSON struct {
	Timestamp        string              `json:"timestamp"`
	LookbackRuns     int                 `json:"lookback_runs"`
	MinTransitions   int                 `json:"min_transitions"`
	TotalFlakyChecks int                 `json:"total_flaky_checks"`
	Checks           []FlakyCheckSummary `json:"checks,omitempty"`
}

type SLOClusterExport struct {
	Address         string  `json:"address"`
	ChecksTotal     int     `json:"checks_total"`
	FailCount       int     `json:"fail_count"`
	WarnCount       int     `json:"warn_count"`
	ErrCount        int     `json:"err_count"`
	InfoCount       int     `json:"info_count"`
	FailRatePercent float64 `json:"fail_rate_percent"`
	HealthScore     int     `json:"health_score"`
	Status          string  `json:"status"`
}

type SLODashboardJSON struct {
	Timestamp string             `json:"timestamp"`
	DurationS float64            `json:"duration_s"`
	Clusters  []SLOClusterExport `json:"clusters,omitempty"`
}

func writeRunSummaryJSON(fs FS, outDir string, summary RunSummaryJSON) error {
	path := filepath.Join(outDir, "run-summary.json")
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// NCCRunRecord is a versioned machine-readable bundle (ncc-run-record.json) for automation pipelines.
type NCCRunRecord struct {
	SchemaVersion       string         `json:"schema_version"`
	OrchestratorVersion string         `json:"orchestrator_version"`
	GitRevision         string         `json:"git_revision,omitempty"`
	Hostname            string         `json:"hostname,omitempty"`
	SchedulerSource     string         `json:"scheduler_source,omitempty"`
	Stream              string         `json:"stream,omitempty"`
	Run                 RunSummaryJSON `json:"run"`
}

func writeNCCRunRecordJSON(fs FS, outDir string, summary RunSummaryJSON) error {
	path := filepath.Join(outDir, "ncc-run-record.json")
	hostname, _ := os.Hostname()
	schedulerSource := strings.TrimSpace(os.Getenv("NCC_SCHEDULER_SOURCE"))
	if schedulerSource == "" {
		schedulerSource = "manual"
	}
	gitRevision := ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				gitRevision = s.Value
				break
			}
		}
	}
	rec := NCCRunRecord{
		SchemaVersion:       "1.0",
		OrchestratorVersion: Version,
		GitRevision:         gitRevision,
		Hostname:            hostname,
		SchedulerSource:     schedulerSource,
		Stream:              Stream,
		Run:                 summary,
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

// RegressionSummary captures change in FAIL counts vs the previous run-summary.json.
type RegressionSummary struct {
	Timestamp         string         `json:"timestamp"`
	PreviousTimestamp string         `json:"previous_timestamp,omitempty"`
	Current           RunSummaryJSON `json:"current"`
	PreviousFailTotal int            `json:"previous_fail_total"`
	CurrentFailTotal  int            `json:"current_fail_total"`
	DeltaFailTotal    int            `json:"delta_fail_total"`
	HasRegression     bool           `json:"has_regression"`
	IncreasedClusters []string       `json:"increased_clusters,omitempty"`
	DecreasedClusters []string       `json:"decreased_clusters,omitempty"`
	UnchangedClusters []string       `json:"unchanged_clusters,omitempty"`
}

func failCountByCluster(summary RunSummaryJSON) map[string]int {
	out := make(map[string]int, len(summary.Clusters))
	for _, c := range summary.Clusters {
		out[strings.TrimSpace(c.Address)] = c.FailCount
	}
	return out
}

func loadRunSummaryJSON(path string) (RunSummaryJSON, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RunSummaryJSON{}, false, nil
		}
		return RunSummaryJSON{}, false, err
	}
	var s RunSummaryJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return RunSummaryJSON{}, false, err
	}
	return s, true, nil
}

func computeRegressionSummary(previous RunSummaryJSON, hasPrevious bool, current RunSummaryJSON) RegressionSummary {
	reg := RegressionSummary{
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		Current:           current,
		CurrentFailTotal:  0,
		PreviousFailTotal: 0,
	}
	if hasPrevious {
		reg.PreviousTimestamp = previous.Timestamp
	}

	prevMap := failCountByCluster(previous)
	currMap := failCountByCluster(current)
	seen := make(map[string]bool, len(prevMap)+len(currMap))

	for k := range prevMap {
		seen[k] = true
	}
	for k := range currMap {
		seen[k] = true
	}

	for cluster := range seen {
		prev := prevMap[cluster]
		curr := currMap[cluster]
		reg.PreviousFailTotal += prev
		reg.CurrentFailTotal += curr
		switch {
		case curr > prev:
			reg.IncreasedClusters = append(reg.IncreasedClusters, cluster)
		case curr < prev:
			reg.DecreasedClusters = append(reg.DecreasedClusters, cluster)
		default:
			reg.UnchangedClusters = append(reg.UnchangedClusters, cluster)
		}
	}
	sort.Strings(reg.IncreasedClusters)
	sort.Strings(reg.DecreasedClusters)
	sort.Strings(reg.UnchangedClusters)
	reg.DeltaFailTotal = reg.CurrentFailTotal - reg.PreviousFailTotal
	reg.HasRegression = len(reg.IncreasedClusters) > 0
	return reg
}

func writeRegressionSummaryJSON(fs FS, outDir string, reg RegressionSummary) error {
	path := filepath.Join(outDir, "regression-summary.json")
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeChecksSnapshotJSON(fs FS, outDir string, snap ChecksSnapshotJSON) error {
	path := filepath.Join(outDir, "checks-snapshot.json")
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func loadChecksSnapshotJSON(path string) (ChecksSnapshotJSON, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ChecksSnapshotJSON{}, false, nil
		}
		return ChecksSnapshotJSON{}, false, err
	}
	var s ChecksSnapshotJSON
	if err := json.Unmarshal(b, &s); err != nil {
		return ChecksSnapshotJSON{}, false, err
	}
	return s, true, nil
}

func writeDrillDownDiffJSON(fs FS, outDir string, diff DrillDownDiffJSON) error {
	path := filepath.Join(outDir, "drilldown-diff.json")
	data, err := json.MarshalIndent(diff, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeFlakyChecksJSON(fs FS, outDir string, flaky FlakyChecksJSON) error {
	path := filepath.Join(outDir, "flaky-checks.json")
	data, err := json.MarshalIndent(flaky, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

func writeSLODashboardJSON(fs FS, outDir string, slo SLODashboardJSON) error {
	path := filepath.Join(outDir, "slo-dashboard.json")
	data, err := json.MarshalIndent(slo, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}

type ExcludedAlertsClusterAudit struct {
	Cluster       string          `json:"cluster"`
	ExcludedCount int             `json:"excluded_count"`
	Alerts        []ExcludedAlert `json:"alerts,omitempty"`
}

type ExcludedAlertsAudit struct {
	SchemaVersion      string                       `json:"schema_version"`
	Timestamp          string                       `json:"timestamp"`
	MatchMode          string                       `json:"match_mode"`
	ExcludeAlertTitles []string                     `json:"exclude_alert_titles"`
	TotalExcluded      int                          `json:"total_excluded"`
	Clusters           []ExcludedAlertsClusterAudit `json:"clusters"`
}

func writeExcludedAlertsAuditJSON(fs FS, outDir string, matchMode string, titles []string, perCluster map[string][]ExcludedAlert) error {
	path := filepath.Join(outDir, "excluded-alerts.json")
	clusters := make([]ExcludedAlertsClusterAudit, 0, len(perCluster))
	total := 0
	for cluster, alerts := range perCluster {
		cp := make([]ExcludedAlert, len(alerts))
		copy(cp, alerts)
		clusters = append(clusters, ExcludedAlertsClusterAudit{
			Cluster:       cluster,
			ExcludedCount: len(cp),
			Alerts:        cp,
		})
		total += len(cp)
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Cluster < clusters[j].Cluster })
	payload := ExcludedAlertsAudit{
		SchemaVersion:      "1.0",
		Timestamp:          time.Now().UTC().Format(time.RFC3339),
		MatchMode:          matchMode,
		ExcludeAlertTitles: append([]string{}, titles...),
		TotalExcluded:      total,
		Clusters:           clusters,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return fs.WriteFile(path, data, 0644)
}
