package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetentionPreviewRouteIsAdminOnly(t *testing.T) {
	for _, read := range []bool{true, false} {
		if got := routeMinRoleFor("/api/v1/settings/retention/preview", read); got != RoleAdmin {
			t.Errorf("read=%v: %v, want admin", read, got)
		}
	}
	found := false
	for _, rt := range apiRouteCatalog() {
		if rt.Path == "/api/v1/settings/retention/preview" {
			found = true
		}
	}
	if !found {
		t.Fatal("retention preview missing from the route catalog")
	}
}

func TestRetentionPreviewArgsUseCanonicalFolders(t *testing.T) {
	root := t.TempDir()
	// Canonical: links and Windows short names (RUNNER~1) resolved.
	if r, err := filepath.EvalSymlinks(root); err == nil {
		root = r
	}
	s := &apiServer{repoRoot: root, configPath: "config.yaml", outputDir: "outputfiles", logDir: "nccfiles"}
	got := strings.Join(s.retentionPreviewArgs(), " ")
	for _, want := range []string{
		"retention --dry-run --json",
		"--config " + filepath.Join(root, "config.yaml"),
		"--output-dir-filtered " + filepath.Join(root, "outputfiles"),
		"--output-dir-logs " + filepath.Join(root, "nccfiles"),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("args %q missing %q", got, want)
		}
	}
}

func TestRetentionPreviewRejectsWrites(t *testing.T) {
	s := &apiServer{repoRoot: t.TempDir(), configPath: "config.yaml"}
	rec := httptest.NewRecorder()
	s.handleRetentionPreview(rec, httptest.NewRequest(http.MethodPost, "/api/v1/settings/retention/preview", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d, want 405", rec.Code)
	}
}
