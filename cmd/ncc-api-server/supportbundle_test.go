package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// GET /api/v1/health/diagnostics/support-bundle?name= serves only bundles the
// POST wrote, from logs/, and nothing else in that directory.
func TestSupportBundleDownload(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	name := "ncc-support-20261108T101500Z.tar.gz"
	if err := os.WriteFile(filepath.Join(dir, "logs", name), []byte("bundle-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "logs", "ncc-api.log"), []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{configPath: filepath.Join(dir, "config.yaml")}

	get := func(q string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleHealthSupportBundle(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/diagnostics/support-bundle?name="+q, nil))
		return rec
	}
	rec := get(name)
	if rec.Code != http.StatusOK || rec.Body.String() != "bundle-bytes" {
		t.Fatalf("download: %d %q", rec.Code, rec.Body.String())
	}
	if cd := rec.Header().Get("Content-Disposition"); cd != `attachment; filename="`+name+`"` {
		t.Errorf("Content-Disposition %q", cd)
	}
	for _, bad := range []string{"ncc-api.log", "../config.yaml", "ncc-support-x.tar.gz", "..%2Fncc-support-20261108T101500Z.tar.gz", ""} {
		if rec := get(bad); rec.Code != http.StatusBadRequest {
			t.Errorf("name %q: status %d, want 400", bad, rec.Code)
		}
	}
	if rec := get("ncc-support-20261108T101501Z.tar.gz"); rec.Code != http.StatusNotFound {
		t.Errorf("missing bundle: status %d, want 404", rec.Code)
	}
}
