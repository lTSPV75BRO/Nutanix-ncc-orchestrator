package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Regression for "successful runs show — instead of a status": the runner
// wrote exit_code with omitempty, so exit code 0 was absent from
// run-summary.json and the API returned no success/exit_code at all.
func TestRunStatusFromSummary(t *testing.T) {
	cases := []struct {
		name       string
		summary    string
		wantStatus string
		wantSource string
		wantOK     *bool
	}{
		{"legacy success (exit_code omitted)", `{"timestamp":"2026-10-20T10:00:00Z","clusters_ok":2,"index_html":"/data/outputfiles/index.html"}`, "success", "legacy_summary", boolPtr(true)},
		{"explicit success", `{"timestamp":"2026-10-20T10:00:00Z","exit_code":0,"status":"success","index_html":"x"}`, "success", "exit_code", boolPtr(true)},
		{"partial", `{"timestamp":"2026-10-20T10:00:00Z","exit_code":3,"clusters_ok":1,"clusters_failed":1,"index_html":"x"}`, "partial", "exit_code", boolPtr(false)},
		{"failed", `{"timestamp":"2026-10-20T10:00:00Z","exit_code":1,"clusters_failed":2,"index_html":"x"}`, "failed", "exit_code", boolPtr(false)},
		{"partially populated (no completion marker)", `{"timestamp":"2026-10-20T10:00:00Z"}`, "unknown", "none", nil},
		{"corrupt", `{not json`, "unknown", "none", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "run-summary.json")
			if err := os.WriteFile(p, []byte(tc.summary), 0o644); err != nil {
				t.Fatal(err)
			}
			var ri runInfo
			enrichRunInfoFromSummary(&ri, p)
			if ri.Status != tc.wantStatus || ri.StatusSource != tc.wantSource {
				t.Fatalf("status=%q source=%q, want %q/%q", ri.Status, ri.StatusSource, tc.wantStatus, tc.wantSource)
			}
			if (tc.wantOK == nil) != (ri.Success == nil) || (tc.wantOK != nil && *tc.wantOK != *ri.Success) {
				t.Fatalf("success=%v want %v", ri.Success, tc.wantOK)
			}
		})
	}
	var missing runInfo
	enrichRunInfoFromSummary(&missing, filepath.Join(t.TempDir(), "absent.json"))
	if missing.Status != "unknown" || missing.Success != nil {
		t.Fatalf("missing summary must be unknown, got %+v", missing)
	}
}

func boolPtr(b bool) *bool { return &b }

func TestRunsAPIReturnsExplicitStatusAndNoAbsolutePaths(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "outputfiles")
	hist := filepath.Join(out, "runs", "20261020T100000Z")
	if err := os.MkdirAll(hist, 0o755); err != nil {
		t.Fatal(err)
	}
	legacy := `{"timestamp":"2026-10-20T10:00:00Z","clusters_ok":1,"index_html":"` + filepath.ToSlash(filepath.Join(out, "index.html")) + `"}`
	if err := os.WriteFile(filepath.Join(hist, "run-summary.json"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{repoRoot: dir, outputDir: out}
	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil), principal{subject: "v", role: RoleViewer})
	rec := httptest.NewRecorder()
	s.handleRuns(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data []runInfo `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || len(resp.Data) != 1 {
		t.Fatalf("decode: %v %s", err, rec.Body.String())
	}
	if resp.Data[0].Status != "success" {
		t.Fatalf("legacy successful run must report success, got %+v", resp.Data[0])
	}
	if strings.Contains(rec.Body.String(), dir) {
		t.Fatalf("runs response leaks absolute paths: %s", rec.Body.String())
	}
}
