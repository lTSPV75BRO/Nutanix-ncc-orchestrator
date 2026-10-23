package main

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestAPICompressionNegotiation(t *testing.T) {
	big := `{"success":true,"data":"` + strings.Repeat("ncc-check-row ", 2000) + `"}`
	s := &apiServer{}
	h := s.withCompression(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/small":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		case "/archive":
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = io.WriteString(w, big)
		case "/pre-encoded":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "br")
			_, _ = io.WriteString(w, big)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, big)
		}
	}))
	get := func(path, ae string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if ae != "" {
			req.Header.Set("Accept-Encoding", ae)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	rec := get("/big", "gzip, deflate, br")
	if rec.Header().Get("Content-Encoding") != "gzip" || rec.Code != http.StatusCreated {
		t.Fatalf("large JSON should be gzipped with the original status: %v %d", rec.Header(), rec.Code)
	}
	compressedLen := rec.Body.Len()
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	plain, _ := io.ReadAll(zr)
	if string(plain) != big || compressedLen >= len(big) {
		t.Fatal("round trip mismatch or no reduction")
	}
	if !strings.Contains(strings.Join(rec.Header().Values("Vary"), ","), "Accept-Encoding") {
		t.Fatal("missing Vary: Accept-Encoding")
	}
	if rec := get("/big", ""); rec.Header().Get("Content-Encoding") != "" || rec.Body.String() != big {
		t.Fatal("no Accept-Encoding must yield identity")
	}
	if rec := get("/big", "gzip;q=0, identity"); rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("gzip;q=0 must be honored")
	}
	if rec := get("/small", "gzip"); rec.Header().Get("Content-Encoding") != "" || rec.Body.String() != `{"ok":true}` {
		t.Fatal("tiny bodies should not be compressed")
	}
	if rec := get("/archive", "gzip"); rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("already-compressed archives must not be gzipped again")
	}
	if rec := get("/pre-encoded", "gzip"); rec.Header().Get("Content-Encoding") != "br" {
		t.Fatal("handler-set Content-Encoding must be preserved")
	}
}

// Measures the effect on a realistic report payload (reported in the final summary).
func TestAPICompressionPayloadReduction(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"success":true,"data":{"agg_rows":[`)
	for i := 0; i < 3000; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"cluster":"10.10.` + itoaTest(i%40) + `.21","clusterName":"prod-cluster-` + itoaTest(i%40) + `","severity":"WARN","check":"Detailed information for disk_usage_check:","detail":"Disk usage on CVM ` + itoaTest(i) + ` is above the threshold of 75%"}`)
	}
	sb.WriteString(`]}}`)
	body := sb.String()
	h := (&apiServer{}).withCompression(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/report/data", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	saved := 100 - float64(rec.Body.Len())*100/float64(len(body))
	t.Logf("report/data sample: %d bytes -> %d bytes gzip (%.1f%% smaller)", len(body), rec.Body.Len(), saved)
	if saved < 70 {
		t.Fatalf("expected substantial reduction, got %.1f%%", saved)
	}
}

func itoaTest(i int) string { return strconv.Itoa(i) }

func TestAuditRotationKeepsGenerations(t *testing.T) {
	dir := t.TempDir()
	s := &apiServer{auditLogPath: dir + "/audit.log", auditLogMaxBytes: 64, auditLogKeep: 3}
	for i := 0; i < 40; i++ {
		s.appendAuditLine([]byte(`{"action":"x","n":` + strconv.Itoa(i) + `,"pad":"................................"}`))
	}
	for _, n := range []string{"audit.log", "audit.log.1", "audit.log.2", "audit.log.3"} {
		if _, err := os.Stat(dir + "/" + n); err != nil {
			t.Fatalf("%s should exist: %v", n, err)
		}
	}
	if _, err := os.Stat(dir + "/audit.log.4"); err == nil {
		t.Fatal("generations beyond --audit-log-keep must be removed")
	}
}
