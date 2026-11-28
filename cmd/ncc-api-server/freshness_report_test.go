package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRenderFreshnessMetrics(t *testing.T) {
	ok := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	out := renderFreshnessMetrics(freshnessData{
		Clusters: []freshnessCluster{
			{Address: "10.0.0.1", OK: true, LastSuccessAt: ok.Format(time.RFC3339)},
			{Address: `pe"2`, OK: false},
		},
		Schedules: []freshnessSchedule{
			{Kind: "main", Every: "4h", Overdue: true},
			{Kind: "environment", ID: "s1", Environment: "prod", Every: "1d"},
		},
	})
	for _, want := range []string{
		"# TYPE ncc_cluster_last_success_timestamp_seconds gauge\n",
		`ncc_cluster_last_success_timestamp_seconds{cluster="10.0.0.1"} ` + strconv.FormatInt(ok.Unix(), 10) + "\n",
		`ncc_cluster_last_success_timestamp_seconds{cluster="pe\"2"} 0` + "\n",
		"# TYPE ncc_schedule_overdue gauge\n",
		`ncc_schedule_overdue{schedule="main",environment=""} 1` + "\n",
		`ncc_schedule_overdue{schedule="s1",environment="prod"} 0` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRenderFreshnessMetricsEmpty(t *testing.T) {
	out := renderFreshnessMetrics(freshnessData{})
	if strings.Contains(out, "{") {
		t.Fatalf("no series expected without data:\n%s", out)
	}
}

func TestFreshnessNoticeLines(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	data := freshnessData{
		Clusters: []freshnessCluster{
			{Address: "10.0.0.1", OK: true, LastSuccessAt: now.Add(-time.Hour).Format(time.RFC3339)},
			{Address: "10.0.0.2", OK: false, LastSuccessAt: now.Add(-74 * time.Hour).Format(time.RFC3339)},
			{Address: "10.0.0.3", OK: false},
		},
		Schedules: []freshnessSchedule{
			{Kind: "main", Every: "4h", Overdue: true, LastRunAt: now.Add(-26 * time.Hour).Format(time.RFC3339)},
			{Kind: "environment", Environment: "prod", Every: "1d", Overdue: true},
			{Kind: "environment", Environment: "lab", Every: "1h"},
		},
	}
	got := freshnessNoticeLines(data, now)
	want := []string{
		"10.0.0.2 was not reached; its data is from 3 days ago.",
		"10.0.0.3 was not reached; no run has returned data for it yet.",
		"The schedule runs every 4 hours, but the last run was 1 day ago.",
		"The prod schedule runs every day, but no run has finished since it was set up.",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("lines:\n got %q\nwant %q", got, want)
	}

	// Nothing to say when every cluster was reached and no schedule is late.
	if lines := freshnessNoticeLines(freshnessData{Clusters: data.Clusters[:1], Schedules: data.Schedules[2:]}, now); len(lines) != 0 {
		t.Fatalf("want no lines, got %q", lines)
	}
}

func TestFreshnessNoticeLinesCapped(t *testing.T) {
	now := time.Now().UTC()
	var data freshnessData
	for i := 0; i < freshnessNoticeMax+3; i++ {
		data.Clusters = append(data.Clusters, freshnessCluster{Address: "pe" + string(rune('a'+i)), OK: false})
	}
	got := freshnessNoticeLines(data, now)
	if len(got) != freshnessNoticeMax+1 || got[len(got)-1] != "… and 3 more clusters not reached." {
		t.Fatalf("lines: %q", got)
	}
}

func TestNotificationTextFreshness(t *testing.T) {
	text := notificationText("run_success", map[string]interface{}{
		"freshness": []string{"10.0.0.2 was not reached; its data is from 3 days ago."},
	})
	if !strings.Contains(text, "Data age:\n  - 10.0.0.2 was not reached; its data is from 3 days ago.") {
		t.Fatalf("text:\n%s", text)
	}
	if strings.Contains(notificationText("run_success", map[string]interface{}{}), "Data age") {
		t.Fatal("no data-age section without lines")
	}
}

func TestBuildDigestDataAge(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "runs", "20261115-100000")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-50 * time.Hour).Format(time.RFC3339)
	recent := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	writeFile := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(filepath.Join(runDir, "run-summary.json"), `{"timestamp":"`+old+`","clusters":[{"address":"pe1","ok":true}]}`)
	writeFile(filepath.Join(dir, "run-summary.json"), `{"timestamp":"`+recent+`","clusters_ok":0,"clusters_failed":1,"clusters":[{"address":"pe1","ok":false}]}`)

	s := &apiServer{outputDir: dir, repoRoot: dir}
	_, body := s.buildDigest()
	if !strings.Contains(body, "Data age:\n  - pe1 was not reached; its data is from 2 days ago.") {
		t.Fatalf("digest body:\n%s", body)
	}
}

func TestMetricsFreshnessGauges(t *testing.T) {
	dir := t.TempDir()
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if err := os.WriteFile(filepath.Join(dir, "run-summary.json"),
		[]byte(`{"timestamp":"`+at.Format(time.RFC3339)+`","clusters":[{"address":"pe1","ok":true}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	freshnessMetricsCache.mu.Lock()
	freshnessMetricsCache.at = time.Time{}
	freshnessMetricsCache.mu.Unlock()
	t.Cleanup(func() {
		freshnessMetricsCache.mu.Lock()
		freshnessMetricsCache.at = time.Time{}
		freshnessMetricsCache.mu.Unlock()
	})

	s := &apiServer{outputDir: dir, repoRoot: dir}
	rec := httptest.NewRecorder()
	s.writeFreshnessMetrics(rec)
	if !strings.Contains(rec.Body.String(), `ncc_cluster_last_success_timestamp_seconds{cluster="pe1"} `+strconv.FormatInt(at.Unix(), 10)+"\n") {
		t.Fatalf("metrics:\n%s", rec.Body.String())
	}
}
