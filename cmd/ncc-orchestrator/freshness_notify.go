package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"goncc/internal/plural"
)

const (
	// unreachedHistoryMaxRuns bounds how far back the run history is read
	// for the last time a cluster returned data.
	unreachedHistoryMaxRuns = 200
	// unreachedNoticeMax caps the lines in one notification.
	unreachedNoticeMax = 10
)

// lastSuccessByCluster reads the run history (newest first) and returns, for
// each of the wanted clusters (lower-cased address), the newest run that
// returned data for it.
func lastSuccessByCluster(historyDir string, wanted []string) map[string]time.Time {
	out := map[string]time.Time{}
	if strings.TrimSpace(historyDir) == "" || len(wanted) == 0 {
		return out
	}
	want := map[string]bool{}
	for _, c := range wanted {
		if k := strings.ToLower(strings.TrimSpace(c)); k != "" {
			want[k] = true
		}
	}
	dirs, err := listHistoryRunDirs(historyDir)
	if err != nil {
		return out
	}
	if len(dirs) > unreachedHistoryMaxRuns {
		dirs = dirs[:unreachedHistoryMaxRuns]
	}
	for _, d := range dirs {
		if len(out) == len(want) {
			break
		}
		sum, ok, err := loadRunSummaryJSON(filepath.Join(historyDir, d, "run-summary.json"))
		if err != nil || !ok {
			continue
		}
		at, err := time.Parse(time.RFC3339, strings.TrimSpace(sum.Timestamp))
		if err != nil {
			continue
		}
		for _, c := range sum.Clusters {
			k := strings.ToLower(strings.TrimSpace(c.Address))
			if !c.OK || !want[k] {
				continue
			}
			if _, seen := out[k]; !seen {
				out[k] = at
			}
		}
	}
	return out
}

// dataAgeText is "5 minutes", "3 hours" or "2 days".
func dataAgeText(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "less than a minute"
	case d < time.Hour:
		return plural.Count(int(d/time.Minute), "minute")
	case d < 24*time.Hour:
		return plural.Count(int(d/time.Hour), "hour")
	default:
		return plural.Count(int(d/(24*time.Hour)), "day")
	}
}

// unreachedClusterLines is one plain-text line per cluster this run could
// not reach, saying how old its data is. Empty when every cluster was reached.
func unreachedClusterLines(historyDir string, failed []string, now time.Time) []string {
	if len(failed) == 0 {
		return nil
	}
	last := lastSuccessByCluster(historyDir, failed)
	var lines []string
	for i, c := range failed {
		if i == unreachedNoticeMax {
			more := len(failed) - unreachedNoticeMax
			lines = append(lines, fmt.Sprintf("… and %d more %s not reached.", more, plural.Word(more, "cluster")))
			break
		}
		age := "no run has returned data for it yet"
		if t, ok := last[strings.ToLower(strings.TrimSpace(c))]; ok {
			age = "its data is from " + dataAgeText(now.Sub(t)) + " ago"
		}
		lines = append(lines, fmt.Sprintf("%s was not reached; %s.", strings.TrimSpace(c), age))
	}
	return lines
}
