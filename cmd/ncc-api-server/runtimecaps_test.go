package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"goncc/internal/runtimecaps"
)

func TestHandleHealthIncludesRuntimeCapabilities(t *testing.T) {
	s := &apiServer{capabilities: runtimecaps.Capabilities{
		Mode: "kubernetes", Kubernetes: true,
		ControllerManagedRestart:  true,
		ControllerManagedSchedule: true,
		ImmutableUpdates:          true, PVCStorage: true,
	}}
	rec := httptest.NewRecorder()
	s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
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
