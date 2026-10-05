package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFeatureFlagsPartialUpdateKeepsDefaults(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir}
	got := s.loadFeatureFlags()
	if !got.Insights || !got.PCAlerts || !got.NCCLogIndex {
		t.Fatalf("defaults = %#v", got)
	}

	body := []byte(`{"insights":false,"pc_alerts":false}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/features", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withPrincipal(req, principal{subject: "alice", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleFeatureFlags(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	saved := s.loadFeatureFlags()
	if saved.Insights || saved.PCAlerts || !saved.RunComparison || !saved.SLO || !saved.PCDiscovery {
		t.Fatalf("saved = %#v", saved)
	}

	get := httptest.NewRequest(http.MethodGet, "/api/v1/features", nil)
	rec = httptest.NewRecorder()
	s.handleFeatureFlags(rec, get)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"insights":false`) {
		t.Fatalf("get %d %s", rec.Code, rec.Body.String())
	}
}

func TestDisabledPCAlertsDoesNotReadConfig(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir}
	flags := defaultFeatureFlags()
	flags.PCAlerts = false
	if err := s.saveFeatureFlags(flags); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts", nil)
	rec := httptest.NewRecorder()
	s.handleAlerts(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var env envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	data, _ := env.Data.(map[string]interface{})
	if data["disabled"] != true {
		t.Fatalf("data = %#v", env.Data)
	}
}

func TestDisabledInsightsSkipsTrendScan(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, outputDir: dir}
	flags := defaultFeatureFlags()
	flags.Insights = false
	if err := s.saveFeatureFlags(flags); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/report/trends", nil)
	rec := httptest.NewRecorder()
	s.handleReportTrends(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"disabled":true`) {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
}
