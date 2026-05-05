package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeRunSummaryForTrend(t *testing.T, path string, payload map[string]interface{}) {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal run summary: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir run summary dir: %v", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write run summary: %v", err)
	}
}

func TestCollectTrendPoints(t *testing.T) {
	outDir := t.TempDir()
	writeRunSummaryForTrend(t, filepath.Join(outDir, "runs", "20260101T000000Z", "run-summary.json"), map[string]interface{}{
		"timestamp":       "2026-01-01T00:00:00Z",
		"duration_s":      10.5,
		"clusters_ok":     1,
		"clusters_failed": 0,
		"total_checks":    5,
		"clusters": []map[string]interface{}{
			{"fail_count": 1, "warn_count": 2, "err_count": 0, "info_count": 2},
		},
	})
	writeRunSummaryForTrend(t, filepath.Join(outDir, "run-summary.json"), map[string]interface{}{
		"timestamp":       "2026-01-02T00:00:00Z",
		"duration_s":      12.0,
		"clusters_ok":     1,
		"clusters_failed": 1,
		"total_checks":    8,
		"clusters": []map[string]interface{}{
			{"fail_count": 2, "warn_count": 1, "err_count": 1, "info_count": 4},
		},
	})
	points := collectTrendPoints(outDir, 30)
	if len(points) != 2 {
		t.Fatalf("expected 2 trend points, got %d", len(points))
	}
	if points[1].FailTotal != 2 || points[1].WarnTotal != 1 || points[1].ErrTotal != 1 || points[1].InfoTotal != 4 {
		t.Fatalf("unexpected totals in latest trend point: %+v", points[1])
	}
}

func TestParseTrendLimit(t *testing.T) {
	if got := parseTrendLimit(""); got != 30 {
		t.Fatalf("expected default limit 30, got %d", got)
	}
	if got := parseTrendLimit("500"); got != 365 {
		t.Fatalf("expected capped limit 365, got %d", got)
	}
	if got := parseTrendLimit("7"); got != 7 {
		t.Fatalf("expected limit 7, got %d", got)
	}
}
