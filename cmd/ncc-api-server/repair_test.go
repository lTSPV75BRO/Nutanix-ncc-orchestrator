package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRepairRoutesAreAdminOnly(t *testing.T) {
	for _, p := range []string{"/api/v1/health/history", "/api/v1/health/autoheal", "/api/v1/health/migrations/config"} {
		for _, read := range []bool{true, false} {
			if got := routeMinRoleFor(p, read); got != RoleAdmin {
				t.Errorf("%s (read=%v) = %v, want admin", p, read, got)
			}
		}
	}
}

func TestHealHistoryNewestFirstWithRecurrence(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, configPath: filepath.Join(dir, "config.yaml")}
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o750); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	lines := ""
	for i := 0; i < 4; i++ {
		at := now.Add(time.Duration(i-4) * time.Hour).Format(time.RFC3339)
		lines += `{"at":"` + at + `","id":"secrets-perms","title":"Secret file permissions","fix_message":"chmod","status":"ok","verified":true}` + "\n"
	}
	lines += "not json\n"
	lines += `{"at":"` + now.Add(-10*24*time.Hour).Format(time.RFC3339) + `","id":"log-sizes","title":"Log file sizes","fix_message":"rotated","status":"ok","verified":false}` + "\n"
	if err := os.WriteFile(s.healHistoryPath(), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.handleHealHistory(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/history?limit=2", nil))
	var env struct {
		Data struct {
			Entries []healHistoryEntry `json:"entries"`
			Total   int                `json:"total"`
			ByCheck []struct {
				ID        string `json:"id"`
				Fixes24h  int    `json:"fixes_24h"`
				Fixes7d   int    `json:"fixes_7d"`
				Recurring bool   `json:"recurring"`
			} `json:"by_check"`
			Unverified int `json:"unverified_7d"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	d := env.Data
	if d.Total != 5 || len(d.Entries) != 2 || d.Entries[0].ID != "log-sizes" {
		t.Fatalf("entries = %+v total=%d", d.Entries, d.Total)
	}
	if len(d.ByCheck) != 2 || d.ByCheck[0].ID != "secrets-perms" || d.ByCheck[0].Fixes24h != 4 || !d.ByCheck[0].Recurring {
		t.Fatalf("by_check = %+v", d.ByCheck)
	}
	// The unverified log-sizes fix is older than a week.
	if d.ByCheck[1].Fixes7d != 0 || d.Unverified != 0 {
		t.Fatalf("by_check[1] = %+v unverified=%d", d.ByCheck[1], d.Unverified)
	}
}

func TestUnresolvedFixes(t *testing.T) {
	rep := &selfHealReport{Results: []map[string]interface{}{
		{"id": "a", "status": "ok"},
		{"id": "b", "status": "warn"},
	}}
	got := unresolvedFixes(rep, []string{"a", "b", "c"})
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("unresolved = %v", got)
	}
}

func TestAutoHealReportsStates(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfg, []byte("schema-version: 1\nrunner:\n  automation:\n    enabled: true\n    level: full-auto\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &apiServer{repoRoot: dir, configPath: cfg, outputDir: dir, selfHealInterval: time.Hour, runAutoRetryDisabled: true}
	rec := httptest.NewRecorder()
	s.handleAutoHeal(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/autoheal", nil))
	var env struct {
		Data struct {
			Healers []autoHealer `json:"healers"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	state := map[string]autoHealer{}
	for _, h := range env.Data.Healers {
		state[h.ID] = h
	}
	if state["selfheal-loop"].State != "partial" || state["run-auto-retry"].State != "off" {
		t.Fatalf("healers = %+v", env.Data.Healers)
	}
	if r := state["runner-automation"]; r.State != "on" || r.Detail != "Level: full-auto" {
		t.Fatalf("runner automation = %+v", r)
	}
}
