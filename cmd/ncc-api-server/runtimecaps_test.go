package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"goncc/internal/runtimecaps"
)

func TestHandleHealthIncludesRuntimeCapabilities(t *testing.T) {
	s := &apiServer{authMode: "token", authToken: "health-test-admin-token", capabilities: runtimecaps.Capabilities{
		Mode: "kubernetes", Kubernetes: true,
		ControllerManagedRestart:  true,
		ControllerManagedSchedule: true,
		ImmutableUpdates:          true, PVCStorage: true,
	}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set("Authorization", "Bearer health-test-admin-token")
	s.handleHealth(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	runtimeData, ok := body.Data["runtime"].(map[string]any)
	if !ok || runtimeData["kubernetes"] != true {
		t.Fatalf("runtime capabilities missing: %#v", body.Data["runtime"])
	}
}

// /api/v1/health is public (liveness probes, login page). Anonymous callers
// must not learn filesystem layout, token locations, or storage protection
// details; those are admin-only.
func TestHealthDoesNotDiscloseInternalsToAnonymousCallers(t *testing.T) {
	s := &apiServer{
		authMode: "token", authToken: "health-test-admin-token",
		repoRoot: "/data", configPath: "/data/config/config.yaml", outputDir: "/data/outputfiles",
		logDir: "/data/nccfiles", tokenFilePath: "/data/auth/.ncc-api-token", orchestratorBin: "/tools/ncc-orchestrator",
		capabilities: runtimecaps.Capabilities{Mode: "kubernetes", Kubernetes: true},
	}
	get := func(token string) string {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		s.handleHealth(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		return rec.Body.String()
	}
	anon := get("")
	for _, leak := range []string{"/data/config/config.yaml", "/data/nccfiles", "/data/auth/.ncc-api-token", "/tools/ncc-orchestrator", "users_store_encrypted", "token_source", "go_version", "token_file"} {
		if strings.Contains(anon, leak) {
			t.Fatalf("anonymous health leaks %q: %s", leak, anon)
		}
	}
	if !strings.Contains(anon, `"status":"ok"`) || !strings.Contains(anon, `"login_enabled"`) {
		t.Fatalf("anonymous health lost liveness/login fields: %s", anon)
	}
	// A wrong token is treated as anonymous, not as an error.
	if bad := get("wrong-token"); strings.Contains(bad, "/data/auth") {
		t.Fatalf("invalid token got admin detail: %s", bad)
	}
	admin := get("health-test-admin-token")
	if !strings.Contains(admin, "/data/auth/.ncc-api-token") || !strings.Contains(admin, "users_store_encrypted") {
		t.Fatalf("admin health should keep operational detail: %s", admin)
	}
}

func TestUserStoreEncryptionPolicy(t *testing.T) {
	s := &apiServer{requireEncryptedUserStore: true}
	if err := s.checkUserStoreEncryptionPolicy(true); err == nil {
		t.Fatal("required encryption without a master key must refuse to start")
	}
	s.userStoreEncrypted = true
	if err := s.checkUserStoreEncryptionPolicy(true); err != nil {
		t.Fatalf("encrypted store should pass: %v", err)
	}
	s = &apiServer{}
	if err := s.checkUserStoreEncryptionPolicy(true); err != nil {
		t.Fatalf("policy off must not block: %v", err)
	}
	s.users = newUserDB("")
	s.usersDBSecret = "ncc-v2-users"
	if got := s.userStoreProtection(); got != "kubernetes_secret_etcd_encryption_unverified" {
		t.Fatalf("protection = %q (must not claim unverifiable encryption)", got)
	}
}
