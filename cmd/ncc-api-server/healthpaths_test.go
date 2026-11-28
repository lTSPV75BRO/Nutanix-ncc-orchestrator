package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestHealthPathsAreCachedForTheTTL(t *testing.T) {
	root := t.TempDir()
	s := &apiServer{repoRoot: root, configPath: "config.yaml", outputDir: "outputfiles", logDir: "logs", tokenFilePath: ".ncc-api-token", orchestratorBin: "bin/ncc-orchestrator"}
	t0 := time.Now()
	first := s.healthPaths(t0)
	if first.configRel != "config.yaml" || first.outputRel != "outputfiles" {
		t.Fatalf("relative paths wrong: %+v", first)
	}
	if !filepath.IsAbs(first.configAbs) || !filepath.IsAbs(first.tokenAbs) {
		t.Fatalf("absolute paths wrong: %+v", first)
	}

	// Within the TTL a changed setting is not re-resolved.
	s.configPath = "other.yaml"
	if got := s.healthPaths(t0.Add(healthPathsTTL - time.Second)); got != first {
		t.Fatalf("expected the cached set inside the TTL, got %+v", got)
	}
	// After the TTL it is recomputed.
	if got := s.healthPaths(t0.Add(healthPathsTTL + time.Second)); got.configRel != "other.yaml" {
		t.Fatalf("expected a refresh after the TTL, got %+v", got)
	}
}

// The cache holds every field, so the role check in healthPayload must still
// decide what a caller sees: a viewer never receives the absolute paths.
func TestHealthPayloadStillHidesAbsolutePathsFromViewers(t *testing.T) {
	root := t.TempDir()
	s := &apiServer{authMode: "token", repoRoot: root, configPath: "config.yaml", outputDir: "outputfiles", logDir: "logs", tokenFilePath: ".ncc-api-token", orchestratorBin: "bin/ncc-orchestrator"}
	admin := s.healthPayload(&principal{role: RoleAdmin, subject: "a"})
	if _, ok := admin["token_file"]; !ok {
		t.Fatal("admin should see token_file")
	}
	viewer := s.healthPayload(&principal{role: RoleViewer, subject: "v"})
	for _, k := range []string{"token_file", "log_dir", "orchestrator_bin"} {
		if _, ok := viewer[k]; ok {
			t.Fatalf("viewer must not see %s", k)
		}
	}
	if viewer["config_path"] != "config.yaml" {
		t.Fatalf("viewer config_path = %v", viewer["config_path"])
	}
	if anon := s.healthPayload(nil); anon["detail_level"] != "public" {
		t.Fatalf("anonymous detail_level = %v", anon["detail_level"])
	}
}
