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

func TestNewFeatureFlagsDefaultOnAndRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir}
	got := s.loadFeatureFlags()
	if !got.AlertHandling || !got.SelfHeal || !got.UpdateChecks || !got.IntroTitle {
		t.Fatalf("defaults = %#v", got)
	}

	body := []byte(`{"alert_handling":false,"self_heal":false,"update_checks":false,"intro_title":false}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/features", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withPrincipal(req, principal{subject: "alice", role: RoleAdmin})
	rec := httptest.NewRecorder()
	s.handleFeatureFlags(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	saved := s.loadFeatureFlags()
	if saved.AlertHandling || saved.SelfHeal || saved.UpdateChecks || saved.IntroTitle || !saved.Insights {
		t.Fatalf("saved = %#v", saved)
	}

	bad := httptest.NewRequest(http.MethodPut, "/api/v1/features", strings.NewReader(`{"nope":true}`))
	bad.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	s.handleFeatureFlags(rec, withPrincipal(bad, principal{subject: "alice", role: RoleAdmin}))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "intro_title") {
		t.Fatalf("unknown key: %d %s", rec.Code, rec.Body.String())
	}
}

func disableFeatures(t *testing.T, s *apiServer, change func(*featureFlags)) {
	t.Helper()
	flags := defaultFeatureFlags()
	change(&flags)
	if err := s.saveFeatureFlags(flags); err != nil {
		t.Fatal(err)
	}
}

func expectFeatureDisabled(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "NCC_API_FEATURE_DISABLED") {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
}

func TestDisabledAlertHandlingRefusesMarks(t *testing.T) {
	s := &apiServer{repoRoot: t.TempDir()}
	disableFeatures(t, s, func(f *featureFlags) { f.AlertHandling = false })

	body := `{"action":"resolve","items":[{"cluster":"c1","check":"disk_usage_check"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/ncc-dispositions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleNCCDispositions(rec, withPrincipal(req, principal{subject: "op", role: RoleOperator}))
	expectFeatureDisabled(t, rec)

	id := "0b9f7c1e-2c3d-4e5f-8a6b-7c8d9e0f1a2b"
	req = httptest.NewRequest(http.MethodPost, "/api/v1/alerts/ncc/"+id+"/acknowledge", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	s.handleNCCAlerts(rec, withPrincipal(req, principal{subject: "op", role: RoleOperator}))
	expectFeatureDisabled(t, rec)
}

func TestDisabledSelfHealRefusesFix(t *testing.T) {
	s := &apiServer{repoRoot: t.TempDir()}
	disableFeatures(t, s, func(f *featureFlags) { f.SelfHeal = false })
	req := httptest.NewRequest(http.MethodPost, "/api/v1/health/diagnostics", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	s.handleHealthDiagnostics(rec, withPrincipal(req, principal{subject: "admin", role: RoleAdmin}))
	expectFeatureDisabled(t, rec)
}

func TestDisabledUpdateChecksSkipsLookupAndApply(t *testing.T) {
	s := &apiServer{repoRoot: t.TempDir()}
	disableFeatures(t, s, func(f *featureFlags) { f.UpdateChecks = false })

	rec := httptest.NewRecorder()
	s.handleUpdateCheck(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/update", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"disabled":true`) {
		t.Fatalf("check: %d %s", rec.Code, rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/update/apply", strings.NewReader(`{}`))
	rec = httptest.NewRecorder()
	s.handleUpdateApply(rec, withPrincipal(req, principal{subject: "admin", role: RoleAdmin}))
	expectFeatureDisabled(t, rec)
}

func TestHealthTellsSignInPageAboutIntroTitle(t *testing.T) {
	s := &apiServer{repoRoot: t.TempDir(), authMode: "token"}
	read := func() interface{} {
		rec := httptest.NewRecorder()
		s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
		var body struct {
			Data map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Data["intro_title"]
	}
	if read() != true {
		t.Fatal("intro_title should default to true")
	}
	disableFeatures(t, s, func(f *featureFlags) { f.IntroTitle = false })
	if read() != false {
		t.Fatal("intro_title should follow the saved switch")
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
