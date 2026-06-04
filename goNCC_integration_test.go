package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestIntegration_DiscoverClustersV4_MockPC exercises the full v4 Prism Central
// discovery HTTP round-trip (request building, pagination, JSON parsing)
// against a mock Prism Central.
func TestIntegration_DiscoverClustersV4_MockPC(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/config/clusters") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if u, _, ok := r.BasicAuth(); !ok || u != "admin" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Only the first page carries data; later pages return empty so the
		// caller's pagination loop terminates.
		if r.URL.Query().Get("$page") != "0" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []interface{}{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"metadata": map[string]interface{}{"totalAvailableResults": 2},
			"data": []interface{}{
				map[string]interface{}{
					"extId":   "ext-aaa",
					"name":    "cluster-a",
					"network": map[string]interface{}{"externalAddress": map[string]interface{}{"ipv4": map[string]interface{}{"value": "10.0.0.1"}}},
				},
				map[string]interface{}{
					"extId":   "ext-bbb",
					"name":    "cluster-b",
					"network": map[string]interface{}{"externalAddress": map[string]interface{}{"ipv4": map[string]interface{}{"value": "10.0.0.2"}}},
				},
			},
		})
	}))
	defer srv.Close()

	rows, err := fetchDiscoverClusterRowsV4(srv.URL, "admin", "secret", true, "v4.2")
	if err != nil {
		t.Fatalf("fetchDiscoverClusterRowsV4: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(rows), rows)
	}
	if rows[0].Name != "cluster-a" || rows[0].Address != "10.0.0.1" || rows[0].API != "v4" || rows[0].ExtID != "ext-aaa" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
}

// TestIntegration_DiscoverClustersV3_MockPC exercises the legacy v3
// clusters/list discovery round-trip against a mock Prism Central.
func TestIntegration_DiscoverClustersV3_MockPC(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/api/nutanix/v3/clusters/list") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"entities": []interface{}{
				map[string]interface{}{
					"metadata": map[string]interface{}{"name": "legacy-1", "uuid": "uuid-1"},
					"spec":     map[string]interface{}{"resources": map[string]interface{}{"network": map[string]interface{}{"external_ip": "10.1.0.9"}}},
				},
			},
		})
	}))
	defer srv.Close()

	rows, err := fetchDiscoverClusterRowsV3(srv.URL, "admin", "secret", true)
	if err != nil {
		t.Fatalf("fetchDiscoverClusterRowsV3: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %+v", len(rows), rows)
	}
	if rows[0].Name != "legacy-1" || rows[0].Address != "10.1.0.9" || rows[0].API != "v3" || rows[0].ExtID != "uuid-1" {
		t.Fatalf("unexpected row: %+v", rows[0])
	}
}

// TestIntegration_TaskPoll_MockPrism exercises the task-poll HTTP round-trip
// (NCCClient.getTaskV2Legacy via doWithRetry) against a mock Prism that reports
// the task as succeeded.
func TestIntegration_TaskPoll_MockPrism(t *testing.T) {
	const taskID = "task-123"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/v2.0/tasks/"+taskID) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"percentage_complete": 100,
			"progress_status":     "Succeeded",
		})
	}))
	defer srv.Close()

	c := &NCCClient{
		baseURL:    srv.URL,
		cluster:    "mock",
		user:       "admin",
		pass:       "secret",
		http:       srv.Client(),
		apiVersion: "v1",
		cfg: Config{
			RetryMaxAttempts: 2,
			RetryBaseDelay:   time.Millisecond,
			RetryMaxDelay:    5 * time.Millisecond,
			RequestTimeout:   5 * time.Second,
			Timeout:          10 * time.Second,
		},
	}

	st, _, err := c.getTaskV2Legacy(context.Background(), taskID)
	if err != nil {
		t.Fatalf("getTaskV2Legacy: %v", err)
	}
	if st.PercentageComplete != 100 || st.ProgressStatus != "Succeeded" {
		t.Fatalf("unexpected task status: %+v", st)
	}
}
