package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRequestIDHonouredGeneratedAndReturnedOnErrors(t *testing.T) {
	s := &apiServer{}
	h := s.withRequestMeta(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "nope"})
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set(requestIDHeader, "client-req-123")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if got := rr.Header().Get(requestIDHeader); got != "client-req-123" {
		t.Fatalf("well-formed inbound ID should be kept, got %q", got)
	}
	var env envelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.RequestID != "client-req-123" {
		t.Fatalf("error envelope request_id = %q", env.RequestID)
	}

	// A malformed (here: injection-shaped) ID is replaced, not echoed.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set(requestIDHeader, "bad id\r\nX-Evil: 1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if got := rr.Header().Get(requestIDHeader); got == "" || strings.ContainsAny(got, " \r\n") {
		t.Fatalf("malformed inbound ID should be replaced, got %q", got)
	}
}

func TestAuditEntriesCarryRequestID(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{repoRoot: dir, auditLogPath: "audit.log"}
	h := s.withRequestMeta(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.audit(r, "test.action", true, nil)
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil)
	req.Header.Set(requestIDHeader, "trace-abcdef12")
	h.ServeHTTP(httptest.NewRecorder(), req)
	b, err := os.ReadFile(filepath.Join(dir, "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"request_id":"trace-abcdef12"`)) {
		t.Fatalf("audit line lacks request_id: %s", b)
	}
}

func TestRouteLabelBoundsCardinality(t *testing.T) {
	cases := map[string]string{
		"/api/v1/runs":                           "/api/v1/runs",
		"/api/v1/runs/20261026T081019Z-001":      "/api/v1/runs/{id}",
		"/api/v1/alerts/ncc/abc-123/acknowledge": "/api/v1/alerts/ncc/{id}/acknowledge",
		"/api/v1/settings/users/alice":           "/api/v1/settings/users/{id}",
		"/totally/unknown/path":                  "other",
	}
	for in, want := range cases {
		if got := routeLabel(in); got != want {
			t.Errorf("routeLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHTTPMetricsExposition(t *testing.T) {
	var m httpMetrics
	m.observe("/api/v1/runs", "GET", 200, 30*time.Millisecond)
	m.observe("/api/v1/runs", "GET", 500, 2*time.Second)
	var buf bytes.Buffer
	m.write(&buf)
	out := buf.String()
	for _, want := range []string{
		`ncc_http_requests_total{route="/api/v1/runs",method="GET",code="200"} 1`,
		`ncc_http_requests_total{route="/api/v1/runs",method="GET",code="500"} 1`,
		`ncc_http_request_duration_seconds_bucket{route="/api/v1/runs",le="0.05"} 1`,
		`ncc_http_request_duration_seconds_bucket{route="/api/v1/runs",le="+Inf"} 2`,
		`ncc_http_request_duration_seconds_count{route="/api/v1/runs"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}
