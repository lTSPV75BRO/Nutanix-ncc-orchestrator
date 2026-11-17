package main

// Data freshness: how old each cluster's data is, and whether a schedule has
// stopped producing runs.
//
// The dashboard merges results across runs, so a cluster whose last run
// failed keeps showing the findings from its last good run, and a schedule
// that silently stopped leaves yesterday's data looking current. This
// endpoint reads the run history (runs/<id>/run-summary.json plus the latest
// top-level summary) and reports, per cluster, when it was last checked, when
// it last returned data, and how many runs in a row failed; and, per active
// schedule, whether two intervals have passed without a run.

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// freshnessMaxRuns bounds how far back the history scan goes.
	freshnessMaxRuns = 200
	// freshnessClusterMaxAge drops clusters whose last check is older than
	// this, so clusters removed from the config fall off the list.
	freshnessClusterMaxAge = 30 * 24 * time.Hour
	// freshnessMissedIntervals is how many intervals a schedule may miss
	// before it is reported as overdue.
	freshnessMissedIntervals = 2
	// freshnessGrace covers run start jitter and long runs.
	freshnessGrace = 10 * time.Minute
)

type freshnessCluster struct {
	Address string `json:"address"`
	// LastCheckedAt is the newest run that tried this cluster.
	LastCheckedAt string `json:"last_checked_at"`
	// OK is that run's result for the cluster.
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	ErrorClass string `json:"error_class,omitempty"`
	// Environment is the environment of the newest run ("" = main config).
	Environment string `json:"environment,omitempty"`
	// LastSuccessAt is the newest run that returned data for the cluster;
	// empty when none in the scanned history did.
	LastSuccessAt string `json:"last_success_at,omitempty"`
	// FailedRuns counts the newest consecutive runs that failed for it.
	FailedRuns int `json:"failed_runs,omitempty"`
	// Counts from the newest successful run.
	FailCount   int  `json:"fail_count"`
	WarnCount   int  `json:"warn_count"`
	ErrCount    int  `json:"err_count"`
	HealthScore *int `json:"health_score,omitempty"`
}

type freshnessSchedule struct {
	// Kind is "main" (the OS or Kubernetes schedule of config.yaml) or
	// "environment".
	Kind        string `json:"kind"`
	ID          string `json:"id,omitempty"`
	Environment string `json:"environment,omitempty"`
	Every       string `json:"every"`
	IntervalS   int64  `json:"interval_s"`
	// LastRunAt is the newest run in the schedule's scope (main config or
	// that environment), from any source.
	LastRunAt string `json:"last_run_at,omitempty"`
	// Since is the reference time: LastRunAt, or when the schedule was
	// saved when no run is newer.
	Since string `json:"since,omitempty"`
	// Overdue is true once freshnessMissedIntervals intervals have passed
	// since Since.
	Overdue       bool  `json:"overdue"`
	MissedRuns    int   `json:"missed_runs,omitempty"`
	OverdueBySecs int64 `json:"overdue_by_s,omitempty"`
}

type freshnessData struct {
	GeneratedAt string              `json:"generated_at"`
	Clusters    []freshnessCluster  `json:"clusters"`
	Schedules   []freshnessSchedule `json:"schedules"`
	// ScheduleOverdue is true when any schedule is overdue.
	ScheduleOverdue bool `json:"schedule_overdue"`
}

// freshnessRun is the part of a run summary this file needs.
type freshnessRun struct {
	at          time.Time
	environment string
	clusters    []freshnessRunCluster
}

type freshnessRunCluster struct {
	Address     string `json:"address"`
	OK          bool   `json:"ok"`
	Error       string `json:"error"`
	ErrorClass  string `json:"error_class"`
	FailCount   int    `json:"fail_count"`
	WarnCount   int    `json:"warn_count"`
	ErrCount    int    `json:"err_count"`
	HealthScore *int   `json:"health_score"`
}

func readFreshnessRun(summaryPath, environment string) (freshnessRun, bool) {
	b, err := os.ReadFile(summaryPath)
	if err != nil {
		return freshnessRun{}, false
	}
	var raw struct {
		Timestamp string                `json:"timestamp"`
		Clusters  []freshnessRunCluster `json:"clusters"`
	}
	if json.Unmarshal(b, &raw) != nil {
		return freshnessRun{}, false
	}
	at, err := time.Parse(time.RFC3339, strings.TrimSpace(raw.Timestamp))
	if err != nil {
		return freshnessRun{}, false
	}
	return freshnessRun{at: at.UTC(), environment: environment, clusters: raw.Clusters}, true
}

// freshnessHistory returns the newest runs first, at most freshnessMaxRuns,
// deduplicated by timestamp (the top-level summary is usually archived too).
func freshnessHistory(outDir string) []freshnessRun {
	type dirRef struct {
		path string
		mod  time.Time
	}
	var dirs []dirRef
	runsDir := filepath.Join(outDir, "runs")
	if entries, err := os.ReadDir(runsDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			dirs = append(dirs, dirRef{path: filepath.Join(runsDir, e.Name()), mod: info.ModTime()})
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].mod.After(dirs[j].mod) })
	if len(dirs) > freshnessMaxRuns {
		dirs = dirs[:freshnessMaxRuns]
	}
	seen := map[int64]bool{}
	var runs []freshnessRun
	add := func(run freshnessRun) {
		k := run.at.Unix()
		if seen[k] {
			return
		}
		seen[k] = true
		runs = append(runs, run)
	}
	for _, d := range dirs {
		if run, ok := readFreshnessRun(filepath.Join(d.path, "run-summary.json"), environmentForRunDir(d.path)); ok {
			add(run)
		}
	}
	// The in-place latest run. Its environment is recorded only in the
	// archived copy, which the loop above has already read when it exists.
	if run, ok := readFreshnessRun(filepath.Join(outDir, "run-summary.json"), ""); ok {
		add(run)
	}
	sort.SliceStable(runs, func(i, j int) bool { return runs[i].at.After(runs[j].at) })
	return runs
}

// clusterFreshness folds the history (newest first) into one entry per
// cluster, dropping clusters not checked within freshnessClusterMaxAge.
func clusterFreshness(runs []freshnessRun, now time.Time) []freshnessCluster {
	type state struct {
		c    freshnessCluster
		done bool // a successful run has been found
	}
	byKey := map[string]*state{}
	var order []string
	for _, run := range runs {
		at := run.at.Format(time.RFC3339)
		for _, rc := range run.clusters {
			key := normClusterName(rc.Address)
			if key == "" {
				continue
			}
			st, seen := byKey[key]
			if !seen {
				if now.Sub(run.at) > freshnessClusterMaxAge {
					continue
				}
				st = &state{c: freshnessCluster{
					Address:       strings.TrimSpace(rc.Address),
					LastCheckedAt: at,
					OK:            rc.OK,
					Error:         rc.Error,
					ErrorClass:    rc.ErrorClass,
					Environment:   run.environment,
				}}
				byKey[key] = st
				order = append(order, key)
			}
			if st.done {
				continue
			}
			if rc.OK {
				st.done = true
				st.c.LastSuccessAt = at
				st.c.FailCount, st.c.WarnCount, st.c.ErrCount = rc.FailCount, rc.WarnCount, rc.ErrCount
				st.c.HealthScore = rc.HealthScore
				continue
			}
			st.c.FailedRuns++
		}
	}
	out := make([]freshnessCluster, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k].c)
	}
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Address) < strings.ToLower(out[j].Address) })
	return out
}

// scheduleFreshness reports whether an active schedule with the given
// interval has gone freshnessMissedIntervals intervals without a run.
func scheduleFreshness(sch freshnessSchedule, interval time.Duration, lastRun, savedAt, now time.Time) freshnessSchedule {
	sch.IntervalS = int64(interval / time.Second)
	since := savedAt
	if lastRun.After(since) {
		since = lastRun
	}
	if !lastRun.IsZero() {
		sch.LastRunAt = lastRun.UTC().Format(time.RFC3339)
	}
	if since.IsZero() || interval <= 0 {
		return sch
	}
	sch.Since = since.UTC().Format(time.RFC3339)
	limit := time.Duration(freshnessMissedIntervals)*interval + freshnessGrace
	if gap := now.Sub(since); gap > limit {
		sch.Overdue = true
		sch.MissedRuns = int(gap / interval)
		sch.OverdueBySecs = int64((gap - interval) / time.Second)
	}
	return sch
}

// newestRunFor is the newest run whose environment matches.
func newestRunFor(runs []freshnessRun, environment string) time.Time {
	for _, run := range runs {
		if strings.EqualFold(run.environment, environment) {
			return run.at
		}
	}
	return time.Time{}
}

func parseRFC3339(v string) time.Time {
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(v))
	if err != nil {
		return time.Time{}
	}
	return t
}

// scheduleInterval is the interval of an "every" value or, failing that, of
// a cron expression that is a plain interval. Zero for custom cron.
func scheduleInterval(every, cron string) (string, time.Duration) {
	every = strings.TrimSpace(every)
	if every == "" && strings.TrimSpace(cron) != "" {
		every = kubernetesEvery(cron)
	}
	if every == "" {
		return "", 0
	}
	d, err := parseEveryDuration(every)
	if err != nil {
		return every, 0
	}
	return every, d
}

func (s *apiServer) freshnessSchedules(runs []freshnessRun, now time.Time) []freshnessSchedule {
	out := []freshnessSchedule{}
	// The main schedule (config.yaml).
	active, every, cron, savedAt := false, "", "", time.Time{}
	if s.capabilities.Kubernetes {
		if ks, err := s.readKubernetesSchedule(); err == nil {
			active = !ks.Suspended && strings.TrimSpace(ks.State.Cron) != ""
			every, cron = ks.State.Every, ks.State.Cron
			savedAt = parseRFC3339(ks.State.UpdatedAt)
		}
	} else if st, err := s.loadSchedule(); err == nil {
		action := strings.ToLower(strings.TrimSpace(st.Action))
		active = strings.TrimSpace(st.UpdatedAt) != "" && !st.PrintOnly && action != "remove" && action != "delete"
		every, cron = st.Every, st.Cron
		savedAt = parseRFC3339(st.UpdatedAt)
	}
	if active {
		if label, interval := scheduleInterval(every, cron); interval > 0 {
			out = append(out, scheduleFreshness(freshnessSchedule{Kind: "main", Every: label}, interval, newestRunFor(runs, ""), savedAt, now))
		}
	}
	// Environment schedules.
	if list, err := s.loadEnvironmentSchedules(); err == nil {
		for _, e := range list {
			if !e.Enabled {
				continue
			}
			label, interval := scheduleInterval(e.Every, "")
			if interval <= 0 {
				continue
			}
			sch := freshnessSchedule{Kind: "environment", ID: e.ID, Environment: e.Environment, Every: label}
			out = append(out, scheduleFreshness(sch, interval, newestRunFor(runs, e.Environment), parseRFC3339(e.CreatedAt), now))
		}
	}
	return out
}

// handleFreshness serves GET /api/v1/freshness (viewer+). Clusters are
// filtered to the caller's cluster groups.
func (s *apiServer) handleFreshness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	data := s.freshnessSnapshot(time.Now().UTC())
	clusters := data.Clusters
	if p, ok := principalFromContext(r.Context()); ok {
		if access := s.allowedClusters(p); !access.unrestricted {
			kept := clusters[:0]
			for _, c := range clusters {
				if access.permits(c.Address) {
					kept = append(kept, c)
				}
			}
			clusters = kept
		}
	}
	data.Clusters = clusters
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: data})
}

// freshnessRuns is the run history, cached until the runs directory or the
// latest summary changes (at most a minute).
func (s *apiServer) freshnessRuns() []freshnessRun {
	outDir := s.absPath(s.outputDir)
	sources := []string{filepath.Join(outDir, "runs"), filepath.Join(outDir, "run-summary.json")}
	runs, _ := reportArtifacts.derived("freshness:"+outDir, sources, time.Minute, 1<<20, func() interface{} {
		return freshnessHistory(outDir)
	}).([]freshnessRun)
	return runs
}

// freshnessSnapshot is every cluster's data age and every schedule's state,
// not filtered by cluster group. Callers that serve users filter it.
func (s *apiServer) freshnessSnapshot(now time.Time) freshnessData {
	runs := s.freshnessRuns()
	schedules := s.freshnessSchedules(runs, now)
	overdue := false
	for _, sch := range schedules {
		overdue = overdue || sch.Overdue
	}
	return freshnessData{
		GeneratedAt:     now.Format(time.RFC3339),
		Clusters:        clusterFreshness(runs, now),
		Schedules:       schedules,
		ScheduleOverdue: overdue,
	}
}
