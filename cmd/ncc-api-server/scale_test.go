package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Regression tests for fleet-scale behaviour (thousands of clusters).

func writeTestFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Large runs move the rows out of index.html; /report/data must still return
// them (it returned none above 20,000 rows before).
func TestReportDataReadsAggregatedSidecar(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "outputfiles")
	writeTestFile(t, filepath.Join(out, "index.html"), "const AGG = []\nconst AGG_DATA_URL = \"aggregated-data.json\"\n")
	writeTestFile(t, filepath.Join(out, "aggregated-data.json"),
		`[{"cluster":"10.0.0.1","check":"disk_usage_check","severity":"FAIL"},{"cluster":"10.0.0.2","check":"ntp_check","severity":"WARN"}]`)
	writeTestFile(t, filepath.Join(out, "run-summary.json"), `{"timestamp":"2026-10-31T00:00:00Z","clusters":[]}`)
	writeTestFile(t, filepath.Join(out, "checks-snapshot.json"),
		`{"timestamp":"x","clusters":[{"address":"10.0.0.1","checks":[{"check":"a"},{"check":"b"}]}]}`)

	s := &apiServer{repoRoot: dir, outputDir: out}
	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/report/data?limit=1", nil), principal{subject: "a", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleReportData(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Data struct {
			AggRows        []map[string]interface{} `json:"agg_rows"`
			ChecksSnapshot map[string]interface{}   `json:"checks_snapshot"`
			Pagination     map[string]map[string]interface{}
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Data.AggRows) != 1 || env.Data.Pagination["agg_rows"]["total"] != float64(2) {
		t.Fatalf("rows=%v pagination=%v", env.Data.AggRows, env.Data.Pagination)
	}
	// With rows present the snapshot keeps cluster identities but not the
	// per-check lists (most of its size at scale).
	clusters, _ := env.Data.ChecksSnapshot["clusters"].([]interface{})
	if len(clusters) != 1 {
		t.Fatalf("snapshot clusters = %v", env.Data.ChecksSnapshot)
	}
	if c, _ := clusters[0].(map[string]interface{}); c["checks"] != nil || c["address"] != "10.0.0.1" {
		t.Fatalf("snapshot cluster = %v", c)
	}
}

func TestArtifactCacheSeesRewrittenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run-summary.json")
	writeTestFile(t, path, `{"n":1}`)
	if v, _ := readJSONArtifactCached(path, nil).(map[string]interface{}); v["n"] != float64(1) {
		t.Fatalf("first read = %v", v)
	}
	// A new run rewrites the file; the next request must see it.
	writeTestFile(t, path, `{"n":22}`)
	later := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(path, later, later)
	if v, _ := readJSONArtifactCached(path, nil).(map[string]interface{}); v["n"] != float64(22) {
		t.Fatalf("after rewrite = %v", v)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if v := readJSONArtifactCached(path, "gone"); v != "gone" {
		t.Fatalf("after delete = %v", v)
	}
}

// Cached artifacts are shared between requests, so cluster-group filtering
// must copy rather than edit them.
func TestDeepFilterClustersLeavesInputUntouched(t *testing.T) {
	in := map[string]interface{}{
		"clusters": []interface{}{
			map[string]interface{}{"cluster": "prod-a"},
			map[string]interface{}{"cluster": "prod-b"},
		},
	}
	access := clusterAccess{allowed: map[string]string{normClusterName("prod-a"): "prod-a"}}
	got := deepFilterClusters(in, access).(map[string]interface{})
	if n := len(got["clusters"].([]interface{})); n != 1 {
		t.Fatalf("filtered = %v", got)
	}
	if n := len(in["clusters"].([]interface{})); n != 2 {
		t.Fatalf("input was modified: %v", in)
	}
}

func TestFindAlertIndexAcrossAppendAndPrune(t *testing.T) {
	doc := &alertLifecycleDoc{}
	for i := 0; i < 50; i++ {
		doc.Alerts = append(doc.Alerts, nccAlert{ID: fmt.Sprintf("id-%d", i)})
	}
	if got := findAlertIndex(doc, "id-42"); got != 42 {
		t.Fatalf("id-42 at %d", got)
	}
	st := newAlertLifecycleStore(filepath.Join(t.TempDir(), "lc.json"), "")
	a, err := st.newAlert(doc, "c1", "", "check", "FAIL")
	if err != nil {
		t.Fatal(err)
	}
	if got := findAlertIndex(doc, a.ID); got != 50 {
		t.Fatalf("new alert at %d", got)
	}
	// Prune-style rewrite: positions move.
	doc.Alerts = append([]nccAlert(nil), doc.Alerts[10:]...)
	doc.idIndex = nil
	if got := findAlertIndex(doc, "id-42"); got != 32 {
		t.Fatalf("after prune id-42 at %d", got)
	}
	if got := findAlertIndex(doc, "id-3"); got != -1 {
		t.Fatalf("pruned id-3 found at %d", got)
	}
}

// The store keeps its parsed document in memory but must notice another
// replica (or process) writing the shared file.
func TestAlertStoreCacheFollowsExternalWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lc.json")
	st := newAlertLifecycleStore(path, "")
	snap := nccRunSnapshot{RunAt: "2026-10-31T00:00:00Z", Findings: []nccFinding{{Cluster: "c1", Check: "x", Severity: "FAIL"}}}
	if _, err := st.ingestRun(snap); err != nil {
		t.Fatal(err)
	}
	first, err := st.read()
	if err != nil || len(first.Alerts) != 1 {
		t.Fatalf("read = %v, %v", first, err)
	}
	if again, _ := st.read(); again != first {
		t.Fatal("unchanged file should be served from memory")
	}

	other := newAlertLifecycleStore(path, "")
	snap2 := nccRunSnapshot{RunAt: "2026-10-31T01:00:00Z", Findings: []nccFinding{
		{Cluster: "c1", Check: "x", Severity: "FAIL"},
		{Cluster: "c2", Check: "y", Severity: "WARN"},
	}, Covered: map[string]struct{}{}}
	if _, err := other.ingestRun(snap2); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(path, later, later)
	after, err := st.read()
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{}
	for _, a := range after.Alerts {
		keys = append(keys, a.Cluster)
	}
	if !strings.Contains(strings.Join(keys, ","), "c2") {
		t.Fatalf("external write not seen: %v", keys)
	}
	if !st.hasIngested("2026-10-31T01:00:00Z") || st.hasIngested("2026-10-31T02:00:00Z") {
		t.Fatal("hasIngested disagrees with the store")
	}
}
