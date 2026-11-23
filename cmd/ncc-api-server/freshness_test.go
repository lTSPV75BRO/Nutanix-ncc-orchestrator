package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fRun(at time.Time, env string, clusters ...freshnessRunCluster) freshnessRun {
	return freshnessRun{at: at, environment: env, clusters: clusters}
}

func TestClusterFreshness(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	score := 80
	runs := []freshnessRun{ // newest first
		fRun(now.Add(-1*time.Hour), "",
			freshnessRunCluster{Address: "10.0.0.1", OK: false, Error: "timeout", ErrorClass: "timeout"},
			freshnessRunCluster{Address: "10.0.0.2", OK: true, WarnCount: 1}),
		fRun(now.Add(-2*time.Hour), "", freshnessRunCluster{Address: "10.0.0.1", OK: false}),
		fRun(now.Add(-3*time.Hour), "prod", freshnessRunCluster{Address: "10.0.0.1", OK: true, FailCount: 2, HealthScore: &score}),
		fRun(now.Add(-4*time.Hour), "", freshnessRunCluster{Address: "10.0.0.1", OK: false}),
		fRun(now.Add(-40*24*time.Hour), "", freshnessRunCluster{Address: "old-cluster", OK: true}),
	}
	got := clusterFreshness(runs, now)
	if len(got) != 2 {
		t.Fatalf("want 2 clusters (old one dropped), got %+v", got)
	}
	a := got[0]
	if a.Address != "10.0.0.1" || a.OK || a.ErrorClass != "timeout" || a.FailedRuns != 2 {
		t.Fatalf("failing cluster: %+v", a)
	}
	if a.LastCheckedAt != now.Add(-1*time.Hour).Format(time.RFC3339) || a.LastSuccessAt != now.Add(-3*time.Hour).Format(time.RFC3339) {
		t.Fatalf("times: %+v", a)
	}
	if a.FailCount != 2 || a.HealthScore == nil || *a.HealthScore != 80 {
		t.Fatalf("counts should come from the last successful run: %+v", a)
	}
	b := got[1]
	if !b.OK || b.FailedRuns != 0 || b.LastSuccessAt != b.LastCheckedAt || b.WarnCount != 1 {
		t.Fatalf("healthy cluster: %+v", b)
	}
}

func TestClusterFreshnessNeverSucceeded(t *testing.T) {
	now := time.Now().UTC()
	got := clusterFreshness([]freshnessRun{
		fRun(now.Add(-time.Hour), "", freshnessRunCluster{Address: "pe1", OK: false}),
		fRun(now.Add(-2*time.Hour), "", freshnessRunCluster{Address: "PE1", OK: false}),
	}, now)
	if len(got) != 1 || got[0].LastSuccessAt != "" || got[0].FailedRuns != 2 {
		t.Fatalf("names match case-insensitively and no success is reported: %+v", got)
	}
}

func TestScheduleFreshness(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	saved := now.Add(-30 * 24 * time.Hour)
	cases := []struct {
		name    string
		lastRun time.Time
		saved   time.Time
		overdue bool
		missed  int
	}{
		{"ran on time", now.Add(-50 * time.Minute), saved, false, 0},
		{"one interval missed", now.Add(-110 * time.Minute), saved, false, 0},
		{"within grace", now.Add(-125 * time.Minute), saved, false, 0},
		{"two intervals missed", now.Add(-3 * time.Hour), saved, true, 3},
		{"never ran, saved long ago", time.Time{}, saved, true, 720},
		{"never ran, saved just now", time.Time{}, now.Add(-time.Minute), false, 0},
		{"saved after the last run", now.Add(-5 * time.Hour), now.Add(-30 * time.Minute), false, 0},
	}
	for _, c := range cases {
		got := scheduleFreshness(freshnessSchedule{Kind: "main", Every: "1h"}, time.Hour, c.lastRun, c.saved, now)
		if got.Overdue != c.overdue || got.MissedRuns != c.missed {
			t.Errorf("%s: overdue=%v missed=%d, want %v %d", c.name, got.Overdue, got.MissedRuns, c.overdue, c.missed)
		}
		if got.IntervalS != 3600 {
			t.Errorf("%s: interval_s=%d", c.name, got.IntervalS)
		}
	}
}

func TestScheduleInterval(t *testing.T) {
	for _, c := range []struct {
		every, cron, label string
		d                  time.Duration
	}{
		{"4h", "", "4h", 4 * time.Hour},
		{"", "0 */6 * * *", "6h", 6 * time.Hour},
		{"", "15 3 * * 1-5", "", 0},
		{"", "", "", 0},
	} {
		label, d := scheduleInterval(c.every, c.cron)
		if label != c.label || d != c.d {
			t.Errorf("scheduleInterval(%q,%q) = %q %v, want %q %v", c.every, c.cron, label, d, c.label, c.d)
		}
	}
}

func writeFreshnessRun(t *testing.T, dir string, at time.Time, env string, clusters []map[string]interface{}) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]interface{}{"timestamp": at.Format(time.RFC3339), "clusters": clusters})
	if err := os.WriteFile(filepath.Join(dir, "run-summary.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if env != "" {
		m, _ := json.Marshal(map[string]string{"environment": env})
		if err := os.WriteFile(filepath.Join(dir, "run-meta.json"), m, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHandleFreshness(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "outputfiles")
	now := time.Now().UTC().Truncate(time.Second)
	writeFreshnessRun(t, filepath.Join(out, "runs", "r1"), now.Add(-5*time.Hour), "", []map[string]interface{}{
		{"address": "pe-a", "ok": true, "fail_count": 3},
		{"address": "pe-b", "ok": true},
	})
	writeFreshnessRun(t, filepath.Join(out, "runs", "r2"), now.Add(-4*time.Hour), "", []map[string]interface{}{
		{"address": "pe-a", "ok": false, "error": "401", "error_class": "auth"},
		{"address": "pe-b", "ok": true},
	})
	writeFreshnessRun(t, filepath.Join(out, "runs", "r3"), now.Add(-30*time.Minute), "dev", []map[string]interface{}{
		{"address": "pe-dev", "ok": true},
	})
	// The latest in-place summary repeats r2.
	writeFreshnessRun(t, out, now.Add(-4*time.Hour), "", nil)

	sched, _ := json.Marshal(scheduleState{Type: "cron", Action: "create", Every: "1h", UpdatedAt: now.Add(-48 * time.Hour).Format(time.RFC3339)})
	if err := os.WriteFile(filepath.Join(root, ".ncc-api-schedule.json"), sched, 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{repoRoot: root, outputDir: "outputfiles", configPath: "config.yaml", scheduleStatePath: ".ncc-api-schedule.json"}
	if err := s.saveEnvironmentSchedules([]environmentSchedule{
		{ID: "e1", Environment: "dev", Enabled: true, Every: "1h", CreatedAt: now.Add(-48 * time.Hour).Format(time.RFC3339)},
		{ID: "e2", Environment: "dev", Enabled: false, Every: "1h"},
	}); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.handleFreshness(rr, httptest.NewRequest(http.MethodGet, "/api/v1/freshness", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body)
	}
	var env struct {
		Data freshnessData `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	d := env.Data
	if len(d.Clusters) != 3 {
		t.Fatalf("clusters: %+v", d.Clusters)
	}
	a := d.Clusters[0]
	if a.Address != "pe-a" || a.OK || a.ErrorClass != "auth" || a.FailedRuns != 1 || a.FailCount != 3 ||
		a.LastSuccessAt != now.Add(-5*time.Hour).Format(time.RFC3339) {
		t.Fatalf("pe-a: %+v", a)
	}
	if d.Clusters[2].Address != "pe-dev" || d.Clusters[2].Environment != "dev" {
		t.Fatalf("pe-dev: %+v", d.Clusters[2])
	}
	if len(d.Schedules) != 2 {
		t.Fatalf("want the main schedule and one enabled environment schedule: %+v", d.Schedules)
	}
	main, dev := d.Schedules[0], d.Schedules[1]
	if main.Kind != "main" || !main.Overdue || main.LastRunAt != now.Add(-4*time.Hour).Format(time.RFC3339) {
		t.Fatalf("main schedule: last main-config run was 4h ago with a 1h interval: %+v", main)
	}
	if dev.Kind != "environment" || dev.Environment != "dev" || dev.Overdue {
		t.Fatalf("dev schedule ran 30m ago: %+v", dev)
	}
	if !d.ScheduleOverdue {
		t.Fatal("schedule_overdue should be set")
	}
}

func TestHandleFreshnessInactiveSchedule(t *testing.T) {
	root := t.TempDir()
	for _, st := range []scheduleState{
		{Action: "remove", Every: "1h", UpdatedAt: "2026-01-01T00:00:00Z"},
		{Action: "create", Every: "1h", UpdatedAt: "2026-01-01T00:00:00Z", PrintOnly: true},
		{Action: "create", Every: "1h"}, // never saved
	} {
		b, _ := json.Marshal(st)
		if err := os.WriteFile(filepath.Join(root, "sched.json"), b, 0o600); err != nil {
			t.Fatal(err)
		}
		s := &apiServer{repoRoot: root, outputDir: "out", configPath: "config.yaml", scheduleStatePath: "sched.json"}
		rr := httptest.NewRecorder()
		s.handleFreshness(rr, httptest.NewRequest(http.MethodGet, "/api/v1/freshness", nil))
		var env struct {
			Data freshnessData `json:"data"`
		}
		_ = json.Unmarshal(rr.Body.Bytes(), &env)
		if len(env.Data.Schedules) != 0 || env.Data.ScheduleOverdue {
			t.Fatalf("%+v should not count as an active schedule: %+v", st, env.Data)
		}
		if env.Data.Clusters == nil || env.Data.Schedules == nil {
			t.Fatal("empty lists must encode as [] not null")
		}
	}
}

// A caller limited to cluster groups sees only the environment schedules that
// cover one of their clusters, and the overdue flag follows (FRESH-01).
func TestHandleFreshnessSchedulesFollowClusterGroups(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "outputfiles")
	now := time.Now().UTC().Truncate(time.Second)
	writeFreshnessRun(t, filepath.Join(out, "runs", "r1"), now.Add(-30*time.Minute), "dev", []map[string]interface{}{{"address": "pe-dev", "ok": true}})
	writeFreshnessRun(t, filepath.Join(out, "runs", "r2"), now.Add(-9*time.Hour), "prod", []map[string]interface{}{{"address": "pe-prod", "ok": true}})
	envDir := filepath.Join(root, filepath.FromSlash(environmentsRelDir))
	if err := os.MkdirAll(envDir, 0o755); err != nil {
		t.Fatal(err)
	}
	lab := "runner:\n  targets:\n    mode: clusters\n    clusters: [pe-dev]\n"
	if err := os.WriteFile(filepath.Join(envDir, "lab.yaml"), []byte(lab), 0o600); err != nil {
		t.Fatal(err)
	}

	s := newClusterGroupTestServer(t, []clusterGroup{{Name: "Dev", Clusters: []string{"pe-dev"}, LocalUsers: []string{"alice"}}})
	s.repoRoot, s.outputDir, s.configPath, s.scheduleStatePath = root, "outputfiles", "config.yaml", ".ncc-api-schedule.json"
	saved := now.Add(-48 * time.Hour).Format(time.RFC3339)
	if err := s.saveEnvironmentSchedules([]environmentSchedule{
		{ID: "e1", Environment: "dev", Enabled: true, Every: "1h", CreatedAt: saved},
		{ID: "e2", Environment: "prod", Enabled: true, Every: "1h", CreatedAt: saved},
		// Never ran, but its cluster list contains alice's cluster.
		{ID: "e3", Environment: "lab", Enabled: true, Every: "96h", CreatedAt: saved},
	}); err != nil {
		t.Fatal(err)
	}

	get := func(p principal) freshnessData {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/freshness", nil)
		rr := httptest.NewRecorder()
		s.handleFreshness(rr, withPrincipal(req, p))
		var env struct {
			Data freshnessData `json:"data"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
			t.Fatal(err)
		}
		return env.Data
	}

	admin := get(principal{role: RoleAdmin, subject: "root"})
	if len(admin.Schedules) != 3 || !admin.ScheduleOverdue {
		t.Fatalf("admin: %+v", admin)
	}
	alice := get(principal{role: RoleViewer, subject: "alice"})
	names := []string{}
	for _, sch := range alice.Schedules {
		names = append(names, sch.Environment)
	}
	if strings.Join(names, ",") != "dev,lab" || alice.ScheduleOverdue {
		t.Fatalf("alice sees %v (overdue %v), want dev,lab and not overdue", names, alice.ScheduleOverdue)
	}
}
