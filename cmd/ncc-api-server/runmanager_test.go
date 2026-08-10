package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestComputeRunRemainderOverlap(t *testing.T) {
	// group1 already owns A,B,C (run id "r1"). group2 wants C,D,E.
	owners := map[string]string{
		"a": "r1", "b": "r1", "c": "r1",
	}
	remainder, skipped, skippedOwner := computeRunRemainder([]string{"C", "D", "E"}, owners, "r2")

	sort.Strings(remainder)
	if !reflect.DeepEqual(remainder, []string{"D", "E"}) {
		t.Fatalf("remainder = %v, want [D E]", remainder)
	}
	if len(skipped) != 1 || skipped[0] != "C" {
		t.Fatalf("skipped = %v, want [C]", skipped)
	}
	if skippedOwner["C"] != "r1" {
		t.Fatalf("skippedOwner[C] = %q, want r1", skippedOwner["C"])
	}
}

func TestComputeRunRemainderFullOverlap(t *testing.T) {
	owners := map[string]string{"a": "r1", "b": "r1"}
	remainder, skipped, _ := computeRunRemainder([]string{"A", "B"}, owners, "r2")
	if len(remainder) != 0 {
		t.Fatalf("remainder = %v, want empty (full overlap)", remainder)
	}
	if len(skipped) != 2 {
		t.Fatalf("skipped = %v, want 2 entries", skipped)
	}
}

func TestComputeRunRemainderNoOverlapAndSelfOwned(t *testing.T) {
	// Clusters owned by self (a requeued run) are not skipped.
	owners := map[string]string{"a": "me", "x": "other"}
	remainder, skipped, _ := computeRunRemainder([]string{"A", "B"}, owners, "me")
	sort.Strings(remainder)
	if !reflect.DeepEqual(remainder, []string{"A", "B"}) {
		t.Fatalf("remainder = %v, want [A B]", remainder)
	}
	if len(skipped) != 0 {
		t.Fatalf("skipped = %v, want none", skipped)
	}
}

func TestMergeRunSummaryRecomputesAggregates(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "canonical")
	perRun := filepath.Join(dir, "run")
	mustMkdir(t, canonical)
	mustMkdir(t, perRun)

	// Canonical baseline: clusters A (ok) and B (failed).
	writeJSONFile(t, filepath.Join(canonical, "run-summary.json"), map[string]interface{}{
		"timestamp":       "2026-01-01T00:00:00Z",
		"clusters_ok":     1,
		"clusters_failed": 1,
		"clusters": []interface{}{
			map[string]interface{}{"address": "A", "ok": true, "checks_total": 10, "health_score": 90},
			map[string]interface{}{"address": "B", "ok": false, "checks_total": 5, "health_score": 40},
		},
	})
	// Per-run refreshed B (now ok) and added C.
	writeJSONFile(t, filepath.Join(perRun, "run-summary.json"), map[string]interface{}{
		"timestamp": "2026-02-02T00:00:00Z",
		"clusters": []interface{}{
			map[string]interface{}{"address": "B", "ok": true, "checks_total": 6, "health_score": 80},
			map[string]interface{}{"address": "C", "ok": true, "checks_total": 4, "health_score": 70},
		},
	})

	mergeRunSummary(canonical, perRun, map[string]bool{"b": true, "c": true})

	var got map[string]interface{}
	readJSONFile(t, filepath.Join(canonical, "run-summary.json"), &got)

	if int(got["clusters_ok"].(float64)) != 3 {
		t.Fatalf("clusters_ok = %v, want 3", got["clusters_ok"])
	}
	if int(got["clusters_failed"].(float64)) != 0 {
		t.Fatalf("clusters_failed = %v, want 0", got["clusters_failed"])
	}
	if int(got["total_checks"].(float64)) != 20 {
		t.Fatalf("total_checks = %v, want 20 (10+6+4)", got["total_checks"])
	}
	if got["timestamp"].(string) != "2026-02-02T00:00:00Z" {
		t.Fatalf("timestamp = %v, want newest", got["timestamp"])
	}
	clusters := got["clusters"].([]interface{})
	if len(clusters) != 3 {
		t.Fatalf("clusters = %d entries, want 3 (A kept, B replaced, C added)", len(clusters))
	}
}

func TestMergeClusterArrayArtifact(t *testing.T) {
	dir := t.TempDir()
	canonicalPath := filepath.Join(dir, "checks-snapshot.json")
	perRunPath := filepath.Join(dir, "run-checks-snapshot.json")

	writeJSONFile(t, canonicalPath, map[string]interface{}{
		"timestamp": "2026-01-01T00:00:00Z",
		"clusters": []interface{}{
			map[string]interface{}{"address": "A", "fail_count": 1},
			map[string]interface{}{"address": "B", "fail_count": 2},
		},
	})
	writeJSONFile(t, perRunPath, map[string]interface{}{
		"timestamp": "2026-03-03T00:00:00Z",
		"clusters": []interface{}{
			map[string]interface{}{"address": "B", "fail_count": 0},
		},
	})

	mergeClusterArrayArtifact(canonicalPath, perRunPath, map[string]bool{"b": true})

	var got map[string]interface{}
	readJSONFile(t, canonicalPath, &got)
	clusters := got["clusters"].([]interface{})
	if len(clusters) != 2 {
		t.Fatalf("clusters = %d, want 2 (A kept + refreshed B)", len(clusters))
	}
	// B must reflect the refreshed value (fail_count 0).
	for _, it := range clusters {
		m := it.(map[string]interface{})
		if m["address"] == "B" && int(m["fail_count"].(float64)) != 0 {
			t.Fatalf("B fail_count = %v, want 0 (refreshed)", m["fail_count"])
		}
	}
	if got["timestamp"].(string) != "2026-03-03T00:00:00Z" {
		t.Fatalf("timestamp not advanced: %v", got["timestamp"])
	}
}

func TestReplaceInlineJSONVarRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.html")
	html := `<html><script>var AGG = [{"cluster":"A"}]; var OTHER = 1;</script></html>`
	if err := os.WriteFile(path, []byte(html), 0o644); err != nil {
		t.Fatal(err)
	}
	newVal := []interface{}{map[string]interface{}{"cluster": "B"}}
	if err := replaceInlineJSONVar(path, "AGG", newVal); err != nil {
		t.Fatalf("replaceInlineJSONVar: %v", err)
	}
	got := readInlineJSONVar(path, "AGG", nil)
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("AGG = %v, want one entry", got)
	}
	if arr[0].(map[string]interface{})["cluster"] != "B" {
		t.Fatalf("AGG[0].cluster = %v, want B", arr[0])
	}
	// The untouched OTHER var must survive.
	if v := readInlineJSONVar(path, "OTHER", nil); v != float64(1) {
		t.Fatalf("OTHER = %v, want 1 (preserved)", v)
	}
}

// TestReadInlineJSONVarSemicolonInValue guards against a regression where a
// literal ';' embedded in a check title/detail (very common in real NCC
// output, e.g. "Description: X; Recommendation: Y") truncated the naive
// `(.*?);` regex previously used to extract embedded JSON, silently losing
// all report rows.
func TestReadInlineJSONVarSemicolonInValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.html")
	html := `<html><script>const AGG = [{"cluster":"10.0.0.1","severity":"WARN","check":"c1","detail":"Description: X; Recommendation: Y"},{"cluster":"10.0.0.2","severity":"FAIL","check":"c2","detail":"no semicolon here"}]; let AGG_ROWS = AGG;</script></html>`
	if err := os.WriteFile(path, []byte(html), 0o644); err != nil {
		t.Fatal(err)
	}
	got := readInlineJSONVar(path, "AGG", []interface{}{})
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("AGG = %#v, want 2 entries (semicolon in detail should not truncate the array)", got)
	}
	first := arr[0].(map[string]interface{})
	if first["check"] != "c1" || first["cluster"] != "10.0.0.1" {
		t.Fatalf("AGG[0] = %v, want cluster/check preserved intact", first)
	}
}

// TestReplaceInlineJSONVarSemicolonInOldValue guards against a regression
// where a semicolon in the EXISTING (old) value being overwritten caused
// replaceInlineJSONVar's old `(.*?);` locator regex to only replace up to
// that semicolon, splicing the new JSON together with a dangling fragment of
// the old JSON and permanently corrupting the file until a full rewrite.
func TestReplaceInlineJSONVarSemicolonInOldValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.html")
	html := `<html><script>const AGG = [{"cluster":"A","detail":"step 1; step 2; step 3"}]; let AGG_ROWS = AGG;</script></html>`
	if err := os.WriteFile(path, []byte(html), 0o644); err != nil {
		t.Fatal(err)
	}
	newVal := []interface{}{map[string]interface{}{"cluster": "B", "detail": "clean"}}
	if err := replaceInlineJSONVar(path, "AGG", newVal); err != nil {
		t.Fatalf("replaceInlineJSONVar: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The file must remain valid: reading AGG back must yield exactly the new
	// value, with no leftover fragment of the old JSON dangling after it.
	got := readInlineJSONVar(path, "AGG", nil)
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 1 || arr[0].(map[string]interface{})["cluster"] != "B" {
		t.Fatalf("AGG after replace = %#v, want single entry with cluster B", got)
	}
	if strings.Contains(string(b), "step 1") {
		t.Fatalf("old value leaked into file after replace: %s", b)
	}
}

func TestMergeIndexHTMLPerCluster(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "canonical")
	perRun := filepath.Join(dir, "run")
	mustMkdir(t, canonical)
	mustMkdir(t, perRun)

	canonicalHTML := `<script>const AGG = [{"cluster":"A","fail":1},{"cluster":"B","fail":1}]; const CLUSTER_LINKS = [];</script>`
	perRunHTML := `<script>const AGG = [{"cluster":"B","fail":0}]; const CLUSTER_LINKS = [];</script>`
	if err := os.WriteFile(filepath.Join(canonical, "index.html"), []byte(canonicalHTML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(perRun, "index.html"), []byte(perRunHTML), 0o644); err != nil {
		t.Fatal(err)
	}

	mergeIndexHTML(canonical, perRun, map[string]bool{"b": true})

	agg := readInlineJSONVar(filepath.Join(canonical, "index.html"), "AGG", nil).([]interface{})
	if len(agg) != 2 {
		t.Fatalf("AGG = %d entries, want 2 (A kept, B refreshed)", len(agg))
	}
	for _, it := range agg {
		m := it.(map[string]interface{})
		if m["cluster"] == "B" && int(m["fail"].(float64)) != 0 {
			t.Fatalf("B fail = %v, want 0 (refreshed)", m["fail"])
		}
	}
}

// TestMergeIndexHTMLEmptyPerRunFileDoesNotWipeCanonical guards against a
// regression where a run that aborted before generating its real report
// (e.g. cluster discovery failed right after the output-permission probe
// created an empty index.html stub in the per-run dir) caused mergeIndexHTML
// to treat the 0-byte file as "this run legitimately produced zero rows" and
// erase every owned cluster's entries from the canonical aggregated report.
func TestMergeIndexHTMLEmptyPerRunFileDoesNotWipeCanonical(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "canonical")
	perRun := filepath.Join(dir, "run")
	mustMkdir(t, canonical)
	mustMkdir(t, perRun)

	canonicalHTML := `<script>const AGG = [{"cluster":"A","fail":1},{"cluster":"B","fail":1}]; const CLUSTER_LINKS = [];</script>`
	if err := os.WriteFile(filepath.Join(canonical, "index.html"), []byte(canonicalHTML), 0o644); err != nil {
		t.Fatal(err)
	}
	// Simulate the aborted-run stub: a 0-byte index.html left behind in the
	// per-run dir by the write-permission probe.
	if err := os.WriteFile(filepath.Join(perRun, "index.html"), []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	// The run "owns" every cluster (as happens for an unscoped/all-clusters
	// run whose ownedClusterSet is derived from the seeded run-summary.json).
	mergeIndexHTML(canonical, perRun, map[string]bool{"a": true, "b": true})

	agg := readInlineJSONVar(filepath.Join(canonical, "index.html"), "AGG", nil).([]interface{})
	if len(agg) != 2 {
		t.Fatalf("AGG = %d entries, want 2 (canonical preserved, empty per-run stub ignored)", len(agg))
	}
}

// TestMergeClusterArrayArtifactEmptyPerRunFileDoesNotWipeCanonical is the
// checks-snapshot.json / slo-dashboard.json / etc. analogue of
// TestMergeIndexHTMLEmptyPerRunFileDoesNotWipeCanonical.
func TestMergeClusterArrayArtifactEmptyPerRunFileDoesNotWipeCanonical(t *testing.T) {
	dir := t.TempDir()
	canonicalPath := filepath.Join(dir, "checks-snapshot.json")
	perRunPath := filepath.Join(dir, "run-checks-snapshot.json")

	writeJSONFile(t, canonicalPath, map[string]interface{}{
		"clusters": []interface{}{
			map[string]interface{}{"address": "A", "fail_count": 1},
		},
	})
	if err := os.WriteFile(perRunPath, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	mergeClusterArrayArtifact(canonicalPath, perRunPath, map[string]bool{"a": true})

	var got map[string]interface{}
	readJSONFile(t, canonicalPath, &got)
	clusters := got["clusters"].([]interface{})
	if len(clusters) != 1 {
		t.Fatalf("clusters = %d, want 1 (canonical preserved, empty per-run file ignored)", len(clusters))
	}
}

func TestNewestTimestamp(t *testing.T) {
	if got := newestTimestamp("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"); got != "2026-02-01T00:00:00Z" {
		t.Fatalf("newestTimestamp = %q, want the later one", got)
	}
	if got := newestTimestamp("", "2026-02-01T00:00:00Z"); got != "2026-02-01T00:00:00Z" {
		t.Fatalf("newestTimestamp with empty a = %q", got)
	}
}

func TestRemoveString(t *testing.T) {
	got := removeString([]string{"a", "b", "c"}, "b")
	if !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("removeString = %v, want [a c]", got)
	}
}

// --- test helpers ---

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func writeJSONFile(t *testing.T, path string, v interface{}) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readJSONFile(t *testing.T, path string, out interface{}) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
}
