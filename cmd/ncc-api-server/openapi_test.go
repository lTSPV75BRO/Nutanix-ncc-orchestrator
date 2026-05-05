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
		"/api/v1/runs/trigger",
		"/api/v1/report/data",
		"/api/v1/report/trends",
		"/api/v1/openapi.json",
	}
	for _, p := range required {
		if _, exists := paths[p]; !exists {
			t.Fatalf("expected path %s in OpenAPI spec", p)
		}
	}
}
