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

func newOnboardingServer(t *testing.T, config string) *apiServer {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "config.yaml"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "outputfiles"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &apiServer{
		repoRoot:                root,
		configPath:              "config.yaml",
		outputDir:               "outputfiles",
		scheduleStatePath:       ".ncc-api-schedule.json",
		backupScheduleStatePath: ".ncc-api-backup-schedule.json",
		notificationStatePath:   ".ncc-api-notifications.json",
	}
}

func stepByID(t *testing.T, st onboardingStatus, id string) onboardingStep {
	t.Helper()
	for _, s := range st.Steps {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("step %q missing", id)
	return onboardingStep{}
}

const sampleCanonicalConfig = `runner:
  targets:
    mode: clusters
    clusters: ["REPLACE_WITH_CLUSTER_IP"]
  credentials:
    username: admin
`

func TestOnboardingFreshInstallIsIncomplete(t *testing.T) {
	s := newOnboardingServer(t, sampleCanonicalConfig)
	st := s.onboardingStatus()
	if st.Completed || st.RequiredDone != 0 {
		t.Fatalf("fresh install should have nothing done: %+v", st)
	}
	if c := stepByID(t, st, "clusters"); c.Done || !strings.Contains(c.Detail, "sample value") {
		t.Fatalf("placeholder cluster must not count as configured: %+v", c)
	}
	if st.RequiredTotal != 4 {
		t.Fatalf("required steps = %d, want 4 (clusters, first run, schedule, backups)", st.RequiredTotal)
	}
}

func TestOnboardingReflectsRealState(t *testing.T) {
	s := newOnboardingServer(t, strings.Replace(sampleCanonicalConfig, "REPLACE_WITH_CLUSTER_IP", "10.1.2.3", 1))
	write := func(rel string, v interface{}) {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(filepath.Join(s.repoRoot, rel), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// A run that reached no cluster must not tick "first run".
	write("outputfiles/run-summary.json", map[string]interface{}{"clusters_ok": 0, "clusters_failed": 1})
	st := s.onboardingStatus()
	if !stepByID(t, st, "clusters").Done {
		t.Fatal("a real cluster address should complete the clusters step")
	}
	if r := stepByID(t, st, "first_run"); r.Done || !strings.Contains(r.Detail, "credentials") {
		t.Fatalf("a run with no successful cluster must not count: %+v", r)
	}

	write("outputfiles/run-summary.json", map[string]interface{}{"clusters_ok": 2, "clusters_failed": 0})
	write(".ncc-api-schedule.json", scheduleState{Type: "auto", Action: "create", Every: "4h"})
	write(".ncc-api-backup-schedule.json", backupScheduleState{Enabled: true, Every: "24h", Encrypt: true})
	st = s.onboardingStatus()
	if !st.Completed || st.RequiredDone != st.RequiredTotal {
		t.Fatalf("all required steps should be done: %+v", st)
	}
	if d := stepByID(t, st, "schedule").Detail; !strings.Contains(d, "every 4h") {
		t.Fatalf("schedule detail = %q", d)
	}
}

func TestOnboardingClustersNeedUsername(t *testing.T) {
	s := newOnboardingServer(t, "clusters: \"10.1.2.3\"\n")
	if c := stepByID(t, s.onboardingStatus(), "clusters"); c.Done {
		t.Fatalf("clusters without a username must not be complete: %+v", c)
	}
}

func TestOnboardingDismissPersistsAndRoles(t *testing.T) {
	s := newOnboardingServer(t, sampleCanonicalConfig)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/onboarding", strings.NewReader(`{"dismissed":true}`))
	req.Header.Set("Content-Type", "application/json")
	req = withPrincipal(req, principal{role: RoleAdmin, subject: "alice"})
	rr := httptest.NewRecorder()
	s.handleOnboarding(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT: %d %s", rr.Code, rr.Body.String())
	}
	st := s.onboardingStatus()
	if !st.Dismissed || st.DismissedBy != "alice" || st.DismissedAt == "" {
		t.Fatalf("dismissal not persisted: %+v", st)
	}

	if got := routeMinRoleFor("/api/v1/onboarding", true); got != RoleOperator {
		t.Fatalf("GET min role = %v, want operator", got)
	}
	if got := routeMinRoleFor("/api/v1/onboarding", false); got != RoleAdmin {
		t.Fatalf("PUT min role = %v, want admin", got)
	}
}
