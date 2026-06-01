// Package promtext renders the Prometheus textfile-collector outputs produced
// by an NCC run: the per-cluster check metrics (<cluster>.prom) and the
// run-level notification delivery metrics (notifications.prom). It depends
// only on goncc/internal/model so it can be reused without pulling in package
// main.
package promtext

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/model"
)

// SanitizeLabel ensures Prometheus label values are safe-ish (no newlines,
// quotes/backslashes escaped).
func SanitizeLabel(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

// WritePrometheusFile writes the per-cluster check metrics textfile
// (<cluster>.prom) under promDir.
func WritePrometheusFile(fs model.FS, promDir, cluster string, blocks []model.ParsedBlock) error {
	if err := fs.MkdirAll(promDir, 0755); err != nil {
		return err
	}
	filename := filepath.Join(promDir, fmt.Sprintf("%s.prom", cluster))

	var b strings.Builder

	// Metric headers.
	b.WriteString(`# HELP nutanix_ncc_check_result Result of an NCC check (1 = present)` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_result gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_summary_total Number of NCC checks per severity` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_summary_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_total Total NCC checks for this cluster` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_problem_total Total non-INFO checks (FAIL+WARN+ERR)` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_problem_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_problem_ratio Ratio of non-INFO checks to total checks` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_problem_ratio gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_run_has_failures 1 when at least one FAIL exists in this run` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_run_has_failures gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_run_has_warnings 1 when at least one WARN exists in this run` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_run_has_warnings gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_run_has_errors 1 when at least one ERR exists in this run` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_run_has_errors gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_run_has_problems 1 when at least one non-INFO check exists` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_run_has_problems gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_run_health_score Cluster run health score (0-100)` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_run_health_score gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_unique_total Number of unique check names in the run` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_unique_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_duplicate_total Number of duplicate check rows (total - unique)` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_duplicate_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_detail_bytes_total Total UTF-8 bytes across check details` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_detail_bytes_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_detail_bytes_avg Average UTF-8 detail bytes per check row` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_detail_bytes_avg gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_check_severity_ratio Ratio of checks by severity` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_check_severity_ratio gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_last_run_timestamp_seconds Unix timestamp when metrics were generated` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_last_run_timestamp_seconds gauge` + "\n")

	// Per-check result metrics.
	counts := map[string]int{
		"FAIL": 0,
		"WARN": 0,
		"ERR":  0,
		"INFO": 0,
		"PASS": 0, // in case parser ever maps PASS
	}
	uniqueChecks := map[string]bool{}
	detailBytesTotal := 0

	for _, pb := range blocks {
		sev := pb.Severity
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; !ok {
			counts[sev] = 0
		}
		counts[sev]++
		if name := strings.TrimSpace(pb.CheckName); name != "" {
			uniqueChecks[name] = true
		}
		detailBytesTotal += len(pb.DetailRaw)

		// one sample per check
		b.WriteString(fmt.Sprintf(
			`nutanix_ncc_check_result{cluster="%s",check="%s",severity="%s"} 1`+"\n",
			SanitizeLabel(cluster),
			SanitizeLabel(pb.CheckName),
			SanitizeLabel(sev),
		))
	}

	// Summary per severity.
	for _, sev := range []string{"FAIL", "WARN", "ERR", "INFO", "PASS"} {
		c := counts[sev]
		b.WriteString(fmt.Sprintf(
			`nutanix_ncc_check_summary_total{cluster="%s",severity="%s"} %d`+"\n",
			SanitizeLabel(cluster),
			SanitizeLabel(sev),
			c,
		))
	}

	// Total checks.
	totalChecks := len(blocks)
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_total{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		totalChecks,
	))
	problemTotal := counts["FAIL"] + counts["WARN"] + counts["ERR"]
	problemRatio := 0.0
	if totalChecks > 0 {
		problemRatio = float64(problemTotal) / float64(totalChecks)
	}
	hasFailures := 0
	if counts["FAIL"] > 0 {
		hasFailures = 1
	}
	hasWarnings := 0
	if counts["WARN"] > 0 {
		hasWarnings = 1
	}
	hasErrors := 0
	if counts["ERR"] > 0 {
		hasErrors = 1
	}
	hasProblems := 0
	if problemTotal > 0 {
		hasProblems = 1
	}
	uniqueTotal := len(uniqueChecks)
	duplicateTotal := totalChecks - uniqueTotal
	detailBytesAvg := 0.0
	if totalChecks > 0 {
		detailBytesAvg = float64(detailBytesTotal) / float64(totalChecks)
	}
	healthScore := model.ClusterHealthScore(counts["FAIL"], counts["WARN"], counts["ERR"], totalChecks)
	nowUnix := time.Now().Unix()
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_problem_total{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		problemTotal,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_problem_ratio{cluster="%s"} %.6f`+"\n",
		SanitizeLabel(cluster),
		problemRatio,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_run_has_failures{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		hasFailures,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_run_has_warnings{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		hasWarnings,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_run_has_errors{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		hasErrors,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_run_has_problems{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		hasProblems,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_run_health_score{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		healthScore,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_unique_total{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		uniqueTotal,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_duplicate_total{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		duplicateTotal,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_detail_bytes_total{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		detailBytesTotal,
	))
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_check_detail_bytes_avg{cluster="%s"} %.6f`+"\n",
		SanitizeLabel(cluster),
		detailBytesAvg,
	))
	for _, sev := range []string{"FAIL", "WARN", "ERR", "INFO", "PASS"} {
		ratio := 0.0
		if totalChecks > 0 {
			ratio = float64(counts[sev]) / float64(totalChecks)
		}
		b.WriteString(fmt.Sprintf(
			`nutanix_ncc_check_severity_ratio{cluster="%s",severity="%s"} %.6f`+"\n",
			SanitizeLabel(cluster),
			SanitizeLabel(sev),
			ratio,
		))
	}
	b.WriteString(fmt.Sprintf(
		`nutanix_ncc_last_run_timestamp_seconds{cluster="%s"} %d`+"\n",
		SanitizeLabel(cluster),
		nowUnix,
	))

	return fs.WriteFile(filename, []byte(b.String()), 0644)
}

// WriteNotificationMetricsFile writes the run-level notifications.prom textfile
// with per-channel attempt/failure counters. A line is always emitted for
// every channel in channels (0 when unused) so alerting rules don't break on a
// missing series.
func WriteNotificationMetricsFile(fs model.FS, promDir string, channels []string, attempts, failures map[string]int) error {
	if err := fs.MkdirAll(promDir, 0755); err != nil {
		return err
	}

	var b strings.Builder
	b.WriteString(`# HELP nutanix_ncc_notification_attempts_total Notification deliveries attempted this run, per channel` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_notification_attempts_total gauge` + "\n")
	b.WriteString(`# HELP nutanix_ncc_notification_failures_total Notification deliveries that failed after retries this run, per channel` + "\n")
	b.WriteString(`# TYPE nutanix_ncc_notification_failures_total gauge` + "\n")
	for _, ch := range channels {
		b.WriteString(fmt.Sprintf(`nutanix_ncc_notification_attempts_total{channel="%s"} %d`+"\n", SanitizeLabel(ch), attempts[ch]))
		b.WriteString(fmt.Sprintf(`nutanix_ncc_notification_failures_total{channel="%s"} %d`+"\n", SanitizeLabel(ch), failures[ch]))
	}

	filename := filepath.Join(promDir, "notifications.prom")
	return fs.WriteFile(filename, []byte(b.String()), 0644)
}
