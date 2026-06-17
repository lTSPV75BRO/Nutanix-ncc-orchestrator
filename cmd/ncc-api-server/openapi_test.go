package main

import "testing"

func TestBuildOpenAPISpecIncludesCorePaths(t *testing.T) {
	s := &apiServer{}
	spec := s.buildOpenAPISpec()
	pathsAny, ok := spec["paths"]
	if !ok {
		t.Fatal("expected paths in OpenAPI spec")
	}
	paths, ok := pathsAny.(map[string]interface{})
	if !ok {
		t.Fatal("expected paths to be a map")
	}
	required := []string{
		"/api/v1/health",
		"/api/v1/metrics/rate-limit",
		"/api/v1/runs/trigger",
		"/api/v1/report/data",
		"/api/v1/report/trends",
		"/api/v1/openapi.json",
		"/api/v1/auth/login",
		"/api/v1/auth/logout",
		"/api/v1/auth/me",
		"/api/v1/auth/change-password",
		"/api/v1/auth/refresh",
		"/api/v1/auth/forgot-password",
		"/api/v1/settings/users",
		"/api/v1/settings/users/{name}",
		"/api/v1/settings/sso",
		"/api/v1/settings/ldap",
		"/api/v1/settings/cluster-groups",
		"/api/v1/settings/clusters",
		"/api/v1/settings/session",
		"/api/v1/settings/password-resets",
		"/api/v1/settings/password-resets/{name}",
		"/api/v1/settings/backup",
		"/api/v1/settings/restore",
		"/api/v1/settings/backups",
		"/api/v1/settings/backups/restore",
		"/api/v1/settings/backups/verify",
		"/api/v1/settings/backups/delete",
		"/api/v1/settings/backups/download",
		"/api/v1/health/diagnostics",
	}
	for _, p := range required {
		if _, exists := paths[p]; !exists {
			t.Fatalf("expected path %s in OpenAPI spec", p)
		}
	}
}
