package main

import (
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"goncc/internal/model"

	"github.com/rs/zerolog/log"
)

// clusterHealthScore is an alias for model.ClusterHealthScore (moved to the
// leaf model package); existing call sites in this package are unchanged.
var clusterHealthScore = model.ClusterHealthScore

// clusterRunFailedCheckName is the synthetic check title used to surface a
// cluster's connection/run failure as a real, visible alert row instead of
// leaving the cluster's entry with no checks at all. Without this, a cluster
// that fails to run simply vanishes from the Alerts table (and from any
// downstream consumer that iterates a cluster's checks) with no indication
// anything went wrong, and previously-known-good rows for that cluster can be
// silently dropped when concurrent/scoped runs merge into the canonical
// report — a state that only self-heals once a new run succeeds.
//
// Severity is UNKNOWN, not FAIL: the orchestrator couldn't even reach the
// cluster to run any NCC checks, so it has no finding to report — FAIL would
// misrepresent "we don't know this cluster's health" as "NCC found a real
// failing check". There's no NCC KB for "the run itself didn't happen", so
// the KB column stays empty; Detail instead carries an actionable remediation
// hint (see runFailedRemediation) so the row is still useful without one.
const clusterRunFailedCheckName = "NCC run failed"

func buildClusterChecksSnapshotFromResult(r ClusterResult) ClusterChecksSnapshot {
	cluster := ClusterChecksSnapshot{Address: r.Cluster}
	if r.Err != nil {
		cluster.Checks = []CheckSnapshotEntry{{
			FindingID: stableFindingID(r.Cluster, clusterRunFailedCheckName),
			CheckName: clusterRunFailedCheckName,
			Severity:  "UNKNOWN",
			Detail:    r.Err.Error() + " — " + runFailedRemediation(r.Err, r.ErrorClass),
		}}
		cluster.ChecksTotal = 1
		cluster.HealthScore = 0
		return cluster
	}
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	cluster.Checks = make([]CheckSnapshotEntry, 0, len(r.Blocks))
	for _, b := range r.Blocks {
		sev := strings.ToUpper(strings.TrimSpace(b.Severity))
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; !ok {
			sev = "INFO"
		}
		counts[sev]++
		cluster.Checks = append(cluster.Checks, CheckSnapshotEntry{
			FindingID: stableFindingID(r.Cluster, strings.TrimSpace(b.CheckName)),
			CheckName: strings.TrimSpace(b.CheckName),
			Severity:  sev,
		})
	}
	sort.Slice(cluster.Checks, func(i, j int) bool {
		if cluster.Checks[i].CheckName == cluster.Checks[j].CheckName {
			return cluster.Checks[i].Severity < cluster.Checks[j].Severity
		}
		return cluster.Checks[i].CheckName < cluster.Checks[j].CheckName
	})
	cluster.FailCount = counts["FAIL"]
	cluster.WarnCount = counts["WARN"]
	cluster.ErrCount = counts["ERR"]
	cluster.InfoCount = counts["INFO"]
	cluster.ChecksTotal = len(r.Blocks)
	cluster.HealthScore = clusterHealthScore(cluster.FailCount, cluster.WarnCount, cluster.ErrCount, cluster.ChecksTotal)
	return cluster
}

func checksMapForCluster(c ClusterChecksSnapshot) map[string]string {
	out := make(map[string]string, len(c.Checks))
	for _, ch := range c.Checks {
		name := strings.TrimSpace(ch.CheckName)
		if name == "" {
			continue
		}
		out[name] = strings.ToUpper(strings.TrimSpace(ch.Severity))
	}
	return out
}

func computeDrillDownDiff(previous ChecksSnapshotJSON, hasPrevious bool, current ChecksSnapshotJSON) DrillDownDiffJSON {
	diff := DrillDownDiffJSON{
		Timestamp: current.Timestamp,
	}
	if !hasPrevious {
		return diff
	}
	if hasPrevious {
		diff.PreviousTimestamp = previous.Timestamp
	}
	prevClusters := map[string]ClusterChecksSnapshot{}
	currClusters := map[string]ClusterChecksSnapshot{}
	for _, c := range previous.Clusters {
		prevClusters[c.Address] = c
	}
	for _, c := range current.Clusters {
		currClusters[c.Address] = c
	}
	allClusters := map[string]bool{}
	for k := range prevClusters {
		allClusters[k] = true
	}
	for k := range currClusters {
		allClusters[k] = true
	}
	for addr := range allClusters {
		prevMap := checksMapForCluster(prevClusters[addr])
		currMap := checksMapForCluster(currClusters[addr])
		cd := ClusterDiffSummary{Address: addr}
		for name, currSev := range currMap {
			prevSev, hadPrev := prevMap[name]
			if !hadPrev {
				cd.NewChecks = append(cd.NewChecks, name)
				if currSev == "FAIL" {
					cd.NewFailures = append(cd.NewFailures, name)
					diff.NewFailCount++
				}
				continue
			}
			if currSev != prevSev {
				cd.SeverityChanges = append(cd.SeverityChanges, SeverityChange{
					CheckName: name, From: prevSev, To: currSev,
				})
			}
			if prevSev != "FAIL" && currSev == "FAIL" {
				cd.NewFailures = append(cd.NewFailures, name)
				diff.NewFailCount++
			}
			if prevSev == "FAIL" && currSev != "FAIL" {
				cd.ResolvedFailures = append(cd.ResolvedFailures, name)
				diff.ResolvedFailCount++
			}
		}
		for name := range prevMap {
			if _, ok := currMap[name]; !ok {
				cd.RemovedChecks = append(cd.RemovedChecks, name)
				if prevMap[name] == "FAIL" {
					cd.ResolvedFailures = append(cd.ResolvedFailures, name)
					diff.ResolvedFailCount++
				}
			}
		}
		sort.Strings(cd.NewFailures)
		sort.Strings(cd.ResolvedFailures)
		sort.Strings(cd.NewChecks)
		sort.Strings(cd.RemovedChecks)
		sort.Slice(cd.SeverityChanges, func(i, j int) bool { return cd.SeverityChanges[i].CheckName < cd.SeverityChanges[j].CheckName })
		if len(cd.NewFailures) > 0 || len(cd.ResolvedFailures) > 0 || len(cd.NewChecks) > 0 || len(cd.RemovedChecks) > 0 || len(cd.SeverityChanges) > 0 {
			diff.Clusters = append(diff.Clusters, cd)
		}
	}
	sort.Slice(diff.Clusters, func(i, j int) bool { return diff.Clusters[i].Address < diff.Clusters[j].Address })
	return diff
}

func computeFlakyChecks(snapshots []ChecksSnapshotJSON, minTransitions int) FlakyChecksJSON {
	report := FlakyChecksJSON{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		LookbackRuns:   len(snapshots),
		MinTransitions: minTransitions,
		Checks:         []FlakyCheckSummary{},
	}
	if len(snapshots) < 2 {
		return report
	}
	type key struct{ cluster, check string }
	series := map[key][]string{}
	for _, snap := range snapshots {
		for _, c := range snap.Clusters {
			for _, ch := range c.Checks {
				k := key{cluster: c.Address, check: strings.TrimSpace(ch.CheckName)}
				if k.check == "" {
					continue
				}
				series[k] = append(series[k], strings.ToUpper(strings.TrimSpace(ch.Severity)))
			}
		}
	}
	for k, states := range series {
		if len(states) < 2 {
			continue
		}
		transitions := 0
		uniq := map[string]bool{}
		prev := states[0]
		uniq[prev] = true
		for i := 1; i < len(states); i++ {
			cur := states[i]
			uniq[cur] = true
			if cur != prev {
				transitions++
			}
			prev = cur
		}
		if transitions >= minTransitions && len(uniq) > 1 {
			report.Checks = append(report.Checks, FlakyCheckSummary{
				Cluster:      k.cluster,
				CheckName:    k.check,
				Transitions:  transitions,
				Observations: len(states),
				States:       states,
				Current:      states[len(states)-1],
			})
		}
	}
	sort.Slice(report.Checks, func(i, j int) bool {
		if report.Checks[i].Transitions == report.Checks[j].Transitions {
			if report.Checks[i].Cluster == report.Checks[j].Cluster {
				return report.Checks[i].CheckName < report.Checks[j].CheckName
			}
			return report.Checks[i].Cluster < report.Checks[j].Cluster
		}
		return report.Checks[i].Transitions > report.Checks[j].Transitions
	})
	report.TotalFlakyChecks = len(report.Checks)
	return report
}

func loadRecentCheckSnapshots(historyDir string, maxRuns int) ([]ChecksSnapshotJSON, error) {
	if maxRuns <= 0 {
		return nil, nil
	}
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return nil, err
	}
	if len(dirs) > maxRuns {
		dirs = dirs[:maxRuns]
	}
	outNewest := make([]ChecksSnapshotJSON, 0, len(dirs))
	for _, d := range dirs {
		path := filepath.Join(historyDir, d, "checks-snapshot.json")
		s, ok, err := loadChecksSnapshotJSON(path)
		if err != nil {
			log.Warn().Err(err).Str("path", path).Msg("failed to load checks snapshot from history (ignored)")
			continue
		}
		if ok {
			outNewest = append(outNewest, s)
		}
	}
	// Return oldest -> newest
	for i, j := 0, len(outNewest)-1; i < j; i, j = i+1, j-1 {
		outNewest[i], outNewest[j] = outNewest[j], outNewest[i]
	}
	return outNewest, nil
}

func buildSLODashboard(run RunSummaryJSON) SLODashboardJSON {
	out := SLODashboardJSON{
		Timestamp: run.Timestamp,
		DurationS: run.DurationS,
		Clusters:  make([]SLOClusterExport, 0, len(run.Clusters)),
	}
	for _, c := range run.Clusters {
		failRate := 0.0
		if c.ChecksTotal > 0 {
			failRate = float64(c.FailCount) * 100.0 / float64(c.ChecksTotal)
		}
		status := "ok"
		if !c.OK {
			status = "error"
		} else if c.FailCount > 0 {
			status = "degraded"
		} else if c.WarnCount > 0 {
			status = "warn"
		}
		out.Clusters = append(out.Clusters, SLOClusterExport{
			Address:         c.Address,
			ChecksTotal:     c.ChecksTotal,
			FailCount:       c.FailCount,
			WarnCount:       c.WarnCount,
			ErrCount:        c.ErrCount,
			InfoCount:       c.InfoCount,
			FailRatePercent: math.Round(failRate*100) / 100,
			HealthScore:     c.HealthScore,
			Status:          status,
		})
	}
	sort.Slice(out.Clusters, func(i, j int) bool { return out.Clusters[i].Address < out.Clusters[j].Address })
	return out
}
