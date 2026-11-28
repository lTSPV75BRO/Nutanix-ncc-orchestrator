package main

import (
	"fmt"
	"html/template"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ==================== Aggregation ====================

// generateTestAgg produces a test index.html with n clusters and ~10–15 rows per cluster for scalability testing.
func generateTestAgg(n int, outDir string) error {
	severities := []string{"FAIL", "WARN", "ERR", "INFO"}
	checks := []string{
		"AHV host time sync", "CVM memory", "Disk health", "Network connectivity",
		"Storage pool", "Prism connectivity", "NCC version", "Cluster health",
		"Data resilience", "VM placement", "Controller VM", "License validity",
	}
	agg := make([]AggBlock, 0, n*12)
	clusterFiles := make([]struct{ Cluster, HTML, CSV string }, 0, n)
	snapshotClusters := make([]ClusterChecksSnapshot, 0, n)
	runClusters := make([]RunClusterSummary, 0, n)
	totalChecks := 0
	for i := 1; i <= n; i++ {
		cluster := fmt.Sprintf("10.0.%d.%d", (i-1)/255, (i-1)%255+1)
		clusterName := fmt.Sprintf("cluster-%03d", i)
		clusterVersion := "6.5.2"
		nccVersion := "4.2.0"
		clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
			Cluster: cluster,
			HTML:    cluster + ".html",
			CSV:     cluster + ".csv",
		})
		numChecks := 8 + (i % 8)
		clusterChecks := make([]CheckSnapshotEntry, 0, numChecks)
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		for j := 0; j < numChecks; j++ {
			sev := severities[(i+j)%len(severities)]
			check := checks[j%len(checks)]
			agg = append(agg, AggBlock{
				Cluster:        cluster,
				Severity:       sev,
				Check:          check,
				Detail:         fmt.Sprintf("Test detail for %s on %s. See https://portal.nutanix.com/kb/%d for more.", check, clusterName, 1000+i+j),
				ClusterName:    clusterName,
				ClusterVersion: clusterVersion,
				NCCVersion:     nccVersion,
			})
			clusterChecks = append(clusterChecks, CheckSnapshotEntry{
				FindingID: stableFindingID(cluster, check),
				CheckName: check,
				Severity:  sev,
			})
			counts[sev]++
		}
		sort.Slice(clusterChecks, func(a, b int) bool {
			if clusterChecks[a].CheckName == clusterChecks[b].CheckName {
				return clusterChecks[a].Severity < clusterChecks[b].Severity
			}
			return clusterChecks[a].CheckName < clusterChecks[b].CheckName
		})
		total := len(clusterChecks)
		health := clusterHealthScore(counts["FAIL"], counts["WARN"], counts["ERR"], total)
		snapshotClusters = append(snapshotClusters, ClusterChecksSnapshot{
			Address:     cluster,
			Checks:      clusterChecks,
			FailCount:   counts["FAIL"],
			WarnCount:   counts["WARN"],
			ErrCount:    counts["ERR"],
			InfoCount:   counts["INFO"],
			ChecksTotal: total,
			HealthScore: health,
		})
		runClusters = append(runClusters, RunClusterSummary{
			Address:     cluster,
			OK:          true,
			FailCount:   counts["FAIL"],
			WarnCount:   counts["WARN"],
			ErrCount:    counts["ERR"],
			InfoCount:   counts["INFO"],
			ChecksTotal: total,
			HealthScore: health,
		})
		totalChecks += total
	}
	fs := OSFS{}
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return err
	}
	sort.Slice(snapshotClusters, func(i, j int) bool { return snapshotClusters[i].Address < snapshotClusters[j].Address })
	sort.Slice(runClusters, func(i, j int) bool { return runClusters[i].Address < runClusters[j].Address })

	now := time.Now().UTC()
	currTS := now.Format(time.RFC3339)
	prevTS := now.Add(-4 * time.Hour).Format(time.RFC3339)

	avgHealth := 0
	minHealth := 100
	for _, c := range runClusters {
		avgHealth += c.HealthScore
		if c.HealthScore < minHealth {
			minHealth = c.HealthScore
		}
	}
	if len(runClusters) > 0 {
		avgHealth /= len(runClusters)
	} else {
		minHealth = 0
	}

	runSummary := RunSummaryJSON{
		Timestamp:      currTS,
		DurationS:      137.7,
		ClustersOK:     len(runClusters),
		ClustersFailed: 0,
		Clusters:       runClusters,
		IndexHTML:      filepath.ToSlash(filepath.Join(filepath.Base(outDir), "index.html")),
		TotalChecks:    totalChecks,
		AvgHealthScore: avgHealth,
		MinHealthScore: minHealth,
	}
	currentSnapshot := ChecksSnapshotJSON{
		Timestamp: currTS,
		Clusters:  snapshotClusters,
	}

	// Build a previous snapshot with small deterministic differences so
	// drill-down/regression/flaky artifacts are populated for UI simulation.
	previousSnapshot := ChecksSnapshotJSON{
		Timestamp: prevTS,
		Clusters:  make([]ClusterChecksSnapshot, 0, len(snapshotClusters)),
	}
	for idx, c := range snapshotClusters {
		cp := c
		cp.Checks = append([]CheckSnapshotEntry(nil), c.Checks...)
		if len(cp.Checks) > 0 {
			switch cp.Checks[0].Severity {
			case "FAIL":
				cp.Checks[0].Severity = "WARN"
			case "WARN":
				cp.Checks[0].Severity = "INFO"
			case "ERR":
				cp.Checks[0].Severity = "WARN"
			default:
				cp.Checks[0].Severity = "WARN"
			}
		}
		if idx%3 == 0 && len(cp.Checks) > 1 {
			cp.Checks = cp.Checks[:len(cp.Checks)-1]
		}
		if idx%4 == 0 {
			cp.Checks = append(cp.Checks, CheckSnapshotEntry{
				FindingID: stableFindingID(cp.Address, "Legacy replication guard"),
				CheckName: "Legacy replication guard",
				Severity:  "FAIL",
			})
		}
		counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
		for _, ch := range cp.Checks {
			sev := strings.ToUpper(strings.TrimSpace(ch.Severity))
			if _, ok := counts[sev]; !ok {
				sev = "INFO"
			}
			counts[sev]++
		}
		cp.FailCount = counts["FAIL"]
		cp.WarnCount = counts["WARN"]
		cp.ErrCount = counts["ERR"]
		cp.InfoCount = counts["INFO"]
		cp.ChecksTotal = len(cp.Checks)
		cp.HealthScore = clusterHealthScore(cp.FailCount, cp.WarnCount, cp.ErrCount, cp.ChecksTotal)
		previousSnapshot.Clusters = append(previousSnapshot.Clusters, cp)
	}

	prevRunClusters := make([]RunClusterSummary, 0, len(previousSnapshot.Clusters))
	prevTotalChecks := 0
	prevAvgHealth := 0
	prevMinHealth := 100
	for _, c := range previousSnapshot.Clusters {
		prevRunClusters = append(prevRunClusters, RunClusterSummary{
			Address:     c.Address,
			OK:          true,
			FailCount:   c.FailCount,
			WarnCount:   c.WarnCount,
			ErrCount:    c.ErrCount,
			InfoCount:   c.InfoCount,
			ChecksTotal: c.ChecksTotal,
			HealthScore: c.HealthScore,
		})
		prevTotalChecks += c.ChecksTotal
		prevAvgHealth += c.HealthScore
		if c.HealthScore < prevMinHealth {
			prevMinHealth = c.HealthScore
		}
	}
	if len(prevRunClusters) > 0 {
		prevAvgHealth /= len(prevRunClusters)
	} else {
		prevMinHealth = 0
	}
	previousSummary := RunSummaryJSON{
		Timestamp:      prevTS,
		DurationS:      129.4,
		ClustersOK:     len(prevRunClusters),
		ClustersFailed: 0,
		Clusters:       prevRunClusters,
		IndexHTML:      filepath.ToSlash(filepath.Join(filepath.Base(outDir), "index.html")),
		TotalChecks:    prevTotalChecks,
		AvgHealthScore: prevAvgHealth,
		MinHealthScore: prevMinHealth,
	}

	regression := computeRegressionSummary(previousSummary, true, runSummary)
	drillDown := computeDrillDownDiff(previousSnapshot, true, currentSnapshot)

	// Build synthetic history to ensure flaky-checks.json has representative data.
	olderA := previousSnapshot
	olderA.Timestamp = now.Add(-8 * time.Hour).Format(time.RFC3339)
	olderB := previousSnapshot
	olderB.Timestamp = now.Add(-6 * time.Hour).Format(time.RFC3339)
	for i := range olderA.Clusters {
		if len(olderA.Clusters[i].Checks) > 0 {
			olderA.Clusters[i].Checks[0].Severity = "WARN"
		}
		if len(olderB.Clusters[i].Checks) > 0 {
			olderB.Clusters[i].Checks[0].Severity = "FAIL"
		}
	}
	flaky := computeFlakyChecks([]ChecksSnapshotJSON{olderA, olderB, currentSnapshot}, 2)
	slo := buildSLODashboard(runSummary)

	if err := writeRunSummaryJSON(fs, outDir, runSummary); err != nil {
		return err
	}
	if err := writeChecksSnapshotJSON(fs, outDir, currentSnapshot); err != nil {
		return err
	}
	if err := writeDrillDownDiffJSON(fs, outDir, drillDown); err != nil {
		return err
	}
	if err := writeFlakyChecksJSON(fs, outDir, flaky); err != nil {
		return err
	}
	if err := writeRegressionSummaryJSON(fs, outDir, regression); err != nil {
		return err
	}
	if err := writeSLODashboardJSON(fs, outDir, slo); err != nil {
		return err
	}
	if err := writeAggregatedHTMLSingle(fs, outDir, agg, clusterFiles, Config{}); err != nil {
		return err
	}
	return nil
}

type AggBlock struct {
	Cluster        string
	Severity       string
	Check          string
	Detail         string
	ClusterName    string
	ClusterVersion string
	NCCVersion     string
}

// writeAllClustersFailedHTML writes a minimal index.html when every cluster failed, so the report page exists.
func writeAllClustersFailedHTML(fs FS, outDir string, failedClusters []string) error {
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, "index.html")
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>NCC Aggregated Report</title></head>
<body style="font-family: system-ui; max-width: 800px; margin: 2rem auto; padding: 1rem; background: #0f172a; color: #e5e7eb;">
<h1>NCC Aggregated Report</h1>
<p style="color: #f59e0b;">All clusters failed. No data was collected.</p>
<p>Failed clusters:</p>
<ul>{{range .Failed}}
<li><code>{{.}}</code></li>{{end}}
</ul>
<p style="color: #9ca3af; font-size: 14px;">Generated at {{.GeneratedAt}}</p>
</body>
</html>`
	t := template.Must(template.New("allfailed").Parse(tmpl))
	type data struct {
		Failed      []string
		GeneratedAt string
	}
	d := data{Failed: failedClusters, GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	return t.Execute(f, d)
}
