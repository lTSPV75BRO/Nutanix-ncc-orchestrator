package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPCAlertsURL(t *testing.T) {
	got, err := pcAlertsURL("pc.example.com:9440", "v4.3")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://pc.example.com:9440/api/monitoring/v4.3/serviceability/alerts" {
		t.Fatalf("unexpected URL: %s", got)
	}
}

func TestPCAlertsURLDefaultsPort(t *testing.T) {
	got, err := pcAlertsURL("10.21.10.208", "v4.2")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://10.21.10.208:9440/api/monitoring/v4.2/serviceability/alerts" {
		t.Fatalf("unexpected URL: %s", got)
	}
}

func TestNormalizePCAlert(t *testing.T) {
	row := normalizePCAlert(map[string]interface{}{
		"clusterName":    "lab-a",
		"entityName":     "node-1",
		"entityType":     "HOST",
		"title":          "Storage warning",
		"severity":       "WARNING",
		"message":        "Capacity is high",
		"status":         "OPEN",
		"impactType":     "PERFORMANCE",
		"creationTime":   "2026-09-01T10:00:00Z",
		"isAcknowledged": false,
	})
	if row["source"] != "PC" || row["severity"] != "WARN" || row["cluster"] != "lab-a" ||
		row["entity_name"] != "node-1" || row["entity_type"] != "HOST" || row["impact_type"] != "PERFORMANCE" {
		t.Fatalf("unexpected normalized row: %#v", row)
	}
}

func TestFetchPCAlertsPagination(t *testing.T) {
	var requests int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("$limit") != "100" ||
			r.URL.Query().Get("$orderby") != "lastUpdatedTime desc" ||
			r.URL.Query().Get("$filter") != "isResolved eq false" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("$page") == "0" {
			w.Write([]byte(`{"metadata":{"totalAvailableResults":2},"data":[{"clusterName":"lab-a","title":"a","severity":"CRITICAL"}]}`))
			return
		}
		w.Write([]byte(`{"metadata":{"totalAvailableResults":2},"data":[{"clusterName":"lab-b","title":"b","severity":"INFO"}]}`))
	}))
	defer server.Close()

	client := server.Client()
	rows, err := fetchPCAlerts(client, server.URL, "v4.2", "admin", "secret", "No")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["severity"] != "FAIL" || rows[1]["severity"] != "INFO" || requests != 2 {
		t.Fatalf("unexpected rows/requests: %#v/%d", rows, requests)
	}
}

func TestPCAlertTargets(t *testing.T) {
	got := pcAlertTargets(map[string]interface{}{"pcs": "pc-a, pc-b"})
	if strings.Join(got, ",") != "pc-a,pc-b" {
		t.Fatalf("unexpected targets: %#v", got)
	}
	if d := rawDuration(map[string]interface{}{"request-timeout": "2s"}, "request-timeout", time.Second); d != 2*time.Second {
		t.Fatalf("unexpected duration: %s", d)
	}
}
