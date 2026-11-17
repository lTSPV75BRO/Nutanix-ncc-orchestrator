package main

// Data freshness outside the dashboard: Prometheus gauges on /metrics and
// short plain-text lines for the run and digest notifications. Both read
// the same snapshot as GET /api/v1/freshness (freshness.go).

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"goncc/internal/plural"
)

// freshnessMetricsTTL bounds how often a scrape recomputes the snapshot.
// The run history is cached separately; this also spares the schedule
// lookups (a Kubernetes API call on Kubernetes) on every scrape.
const freshnessMetricsTTL = time.Minute

// freshnessNoticeMax caps the not-reached lines in one notification.
const freshnessNoticeMax = 10

var freshnessMetricsCache struct {
	mu   sync.Mutex
	at   time.Time
	data freshnessData
}

// writeFreshnessMetrics emits the data-age gauges on /metrics. Like the
// other per-cluster metrics, /metrics is an admin/monitoring endpoint and is
// not scoped to cluster groups.
func (s *apiServer) writeFreshnessMetrics(w io.Writer) {
	now := time.Now().UTC()
	freshnessMetricsCache.mu.Lock()
	if freshnessMetricsCache.at.IsZero() || now.Sub(freshnessMetricsCache.at) >= freshnessMetricsTTL {
		freshnessMetricsCache.data = s.freshnessSnapshot(now)
		freshnessMetricsCache.at = now
	}
	data := freshnessMetricsCache.data
	freshnessMetricsCache.mu.Unlock()
	fmt.Fprint(w, renderFreshnessMetrics(data))
}

// promLabelValue escapes a label value for the text exposition format.
func promLabelValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

// renderFreshnessMetrics renders ncc_cluster_last_success_timestamp_seconds
// and ncc_schedule_overdue.
func renderFreshnessMetrics(data freshnessData) string {
	var b strings.Builder
	b.WriteString("# HELP ncc_cluster_last_success_timestamp_seconds Unix time of the newest run that returned data for the cluster (0 when none in the run history).\n")
	b.WriteString("# TYPE ncc_cluster_last_success_timestamp_seconds gauge\n")
	for _, c := range data.Clusters {
		ts := int64(0)
		if t := parseRFC3339(c.LastSuccessAt); !t.IsZero() {
			ts = t.Unix()
		}
		fmt.Fprintf(&b, "ncc_cluster_last_success_timestamp_seconds{cluster=\"%s\"} %d\n", promLabelValue(c.Address), ts)
	}
	b.WriteString("# HELP ncc_schedule_overdue 1 when an active schedule has gone two intervals without a run, else 0.\n")
	b.WriteString("# TYPE ncc_schedule_overdue gauge\n")
	for _, sch := range data.Schedules {
		v := 0
		if sch.Overdue {
			v = 1
		}
		fmt.Fprintf(&b, "ncc_schedule_overdue{schedule=\"%s\",environment=\"%s\"} %d\n",
			promLabelValue(freshnessScheduleName(sch)), promLabelValue(sch.Environment), v)
	}
	return b.String()
}

// freshnessScheduleName is "main" for the config.yaml schedule and the
// schedule ID (or environment name) for an environment schedule.
func freshnessScheduleName(sch freshnessSchedule) string {
	if sch.Kind != "environment" {
		return "main"
	}
	if strings.TrimSpace(sch.ID) != "" {
		return sch.ID
	}
	return sch.Environment
}

// ageText is "5 minutes", "3 hours" or "2 days".
func ageText(d time.Duration) string {
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

var reEveryLabel = regexp.MustCompile(`^(\d+)([mhd])$`)

// everyText is "4h" → "4 hours", "1d" → "day", as the dashboard words it.
func everyText(every string) string {
	m := reEveryLabel.FindStringSubmatch(strings.TrimSpace(every))
	if m == nil {
		return every
	}
	n, _ := strconv.Atoi(m[1])
	unit := map[string]string{"m": "minute", "h": "hour", "d": "day"}[m[2]]
	if n == 1 {
		return unit
	}
	return plural.Count(n, unit)
}

// freshnessNoticeLines are the plain-text lines a notification adds about
// data age: one per cluster whose newest run could not reach it, and one per
// overdue schedule. Empty when everything is current.
func freshnessNoticeLines(data freshnessData, now time.Time) []string {
	var lines []string
	var down []freshnessCluster
	for _, c := range data.Clusters {
		if !c.OK {
			down = append(down, c)
		}
	}
	for i, c := range down {
		if i == freshnessNoticeMax {
			more := len(down) - freshnessNoticeMax
			lines = append(lines, fmt.Sprintf("… and %d more %s not reached.", more, plural.Word(more, "cluster")))
			break
		}
		age := "no run has returned data for it yet"
		if t := parseRFC3339(c.LastSuccessAt); !t.IsZero() {
			age = "its data is from " + ageText(now.Sub(t)) + " ago"
		}
		lines = append(lines, fmt.Sprintf("%s was not reached; %s.", c.Address, age))
	}
	for _, sch := range data.Schedules {
		if !sch.Overdue {
			continue
		}
		who := "The schedule"
		if sch.Kind == "environment" {
			who = fmt.Sprintf("The %s schedule", sch.Environment)
		}
		last := "no run has finished since it was set up"
		if t := parseRFC3339(sch.LastRunAt); !t.IsZero() {
			last = "the last run was " + ageText(now.Sub(t)) + " ago"
		}
		lines = append(lines, fmt.Sprintf("%s runs every %s, but %s.", who, everyText(sch.Every), last))
	}
	return lines
}

// freshnessNotice is freshnessNoticeLines for the current run history.
func (s *apiServer) freshnessNotice() []string {
	now := time.Now().UTC()
	return freshnessNoticeLines(s.freshnessSnapshot(now), now)
}
