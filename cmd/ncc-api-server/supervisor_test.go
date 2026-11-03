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

func TestSupervisorRoutesAreAdminOnly(t *testing.T) {
	for _, p := range []string{"/api/v1/health/supervisor", "/api/v1/health/supervisor/restart", "/api/v1/health/platform"} {
		for _, read := range []bool{true, false} {
			if got := routeMinRoleFor(p, read); got != RoleAdmin {
				t.Errorf("%s (read=%v) = %v, want admin", p, read, got)
			}
		}
	}
}

func supervisorTestServer(t *testing.T) *apiServer {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0o750); err != nil {
		t.Fatal(err)
	}
	return &apiServer{repoRoot: dir, configPath: filepath.Join(dir, "config.yaml")}
}

func decodeData(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var env struct {
		Success bool                   `json:"success"`
		Data    map[string]interface{} `json:"data"`
		Error   string                 `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("%v: %s", err, rec.Body.String())
	}
	if env.Data == nil {
		env.Data = map[string]interface{}{}
	}
	env.Data["_error"] = env.Error
	return env.Data
}

func TestSupervisorStateAndRestart(t *testing.T) {
	s := supervisorTestServer(t)
	rec := httptest.NewRecorder()
	s.handleSupervisor(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/supervisor", nil))
	if d := decodeData(t, rec); d["mode"] != "manual" || d["running"] != false {
		t.Fatalf("no supervisor: %v", d)
	}

	restart := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleSupervisorRestart(rec, httptest.NewRequest(http.MethodPost, "/api/v1/health/supervisor/restart", strings.NewReader(body)))
		return rec
	}
	if rec := restart(`{"child":"api"}`); rec.Code != http.StatusConflict {
		t.Fatalf("restart without supervisor = %d", rec.Code)
	}

	state := `{"pid":` + itoa(os.Getpid()) + `,"children":{"api":{"name":"api","state":"running"},"ui":{"name":"ui","state":"cooldown"}},"events":[]}`
	if err := os.WriteFile(s.supervisorStatePath(), []byte(state), 0o644); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	s.handleSupervisor(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/supervisor", nil))
	if d := decodeData(t, rec); d["mode"] != "supervised" || d["running"] != true || d["state"] == nil {
		t.Fatalf("supervised: %v", d)
	}

	if rec := restart(`{"child":"mcp"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown child = %d", rec.Code)
	}
	if rec := restart(`{"child":"ui"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("restart ui = %d %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(s.supervisorRestartRequestPath("ui")); err != nil {
		t.Fatalf("request file not written: %v", err)
	}
}

func TestPlatformAndRuntimeDiagnostic(t *testing.T) {
	s := supervisorTestServer(t)
	rec := httptest.NewRecorder()
	s.handlePlatform(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/platform", nil))
	d := decodeData(t, rec)
	host, _ := d["host"].(map[string]interface{})
	proc, _ := d["api_process"].(map[string]interface{})
	if host["os"] == nil || host["arch"] == nil || proc["goroutines"] == nil {
		t.Fatalf("platform: %v", d)
	}
	if r := s.apiRuntimeDiagnostic(); r.ID != "api-runtime" || r.Status != diagOK || !strings.Contains(r.Message, "goroutines") {
		t.Fatalf("api-runtime: %+v", r)
	}
}

func TestSupervisedUpgradeHandover(t *testing.T) {
	s := supervisorTestServer(t)
	if s.supervisorSupports("upgrade") {
		t.Fatal("no supervisor yet")
	}
	state := `{"pid":` + itoa(os.Getpid()) + `,"features":["restart","upgrade"],"children":{}}`
	if err := os.WriteFile(s.supervisorStatePath(), []byte(state), 0o644); err != nil {
		t.Fatal(err)
	}
	if !s.supervisorSupports("upgrade") || s.supervisorSupports("teleport") {
		t.Fatal("feature detection")
	}

	if err := s.requestSupervisedUpgrade(upgradeRequest{ID: "x", TargetVersion: "2.4.0", BackupPath: "/b/pre.tar.gz"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(s.maintenanceInstallDir(), "run", "v2-supervisor-upgrade.request"))
	if err != nil || !strings.Contains(string(b), `"target_version":"2.4.0"`) {
		t.Fatalf("request = %s %v", b, err)
	}

	job := `{"id":"x","phase":"verifying","message":"checking","steps":[]}`
	if err := os.WriteFile(s.upgradeStatePath(), []byte(job), 0o644); err != nil {
		t.Fatal(err)
	}
	if j := s.readUpgradeJob(); j == nil || !upgradeJobActive(j) || j.Stale {
		t.Fatalf("active job = %+v", j)
	}
	rec := httptest.NewRecorder()
	s.handleSupervisor(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health/supervisor", nil))
	if d := decodeData(t, rec); d["upgrade"] == nil {
		t.Fatalf("supervisor view lacks the upgrade: %v", d)
	}

	// Without a supervisor the job cannot progress: stale, not active.
	if err := os.Remove(s.supervisorStatePath()); err != nil {
		t.Fatal(err)
	}
	if j := s.readUpgradeJob(); j == nil || !j.Stale || upgradeJobActive(j) {
		t.Fatalf("stale job = %+v", j)
	}
}
