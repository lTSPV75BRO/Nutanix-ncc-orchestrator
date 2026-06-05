package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeAuditFixture(t *testing.T) *apiServer {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	lines := []string{
		`{"ts":"2026-06-01T10:00:00Z","action":"auth.login","success":true,"user":"alice","role":"admin","client":"10.0.0.1","method":"POST","path":"/api/v1/auth/login"}`,
		`{"ts":"2026-06-02T11:00:00Z","action":"auth.login","success":false,"user":"bob","role":"viewer","client":"10.0.0.2","method":"POST","path":"/api/v1/auth/login"}`,
		`{"ts":"2026-06-03T12:00:00Z","action":"settings.config","success":true,"user":"alice","role":"admin","client":"10.0.0.1","method":"PUT","path":"/api/v1/settings/config","config_path":"x.yaml"}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &apiServer{auditLogPath: path}
}

func TestAuditEntriesFiltering(t *testing.T) {
	s := writeAuditFixture(t)

	all, err := s.auditEntries(100, auditFilter{})
	if err != nil || len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d (err=%v)", len(all), err)
	}
	// User filter (case-insensitive).
	byUser, _ := s.auditEntries(100, auditFilter{user: "ALICE"})
	if len(byUser) != 2 {
		t.Fatalf("expected 2 alice entries, got %d", len(byUser))
	}
	// Failures only.
	fails, _ := s.auditEntries(100, auditFilter{onlyFailures: true})
	if len(fails) != 1 {
		t.Fatalf("expected 1 failure, got %d", len(fails))
	}
	// Action prefix.
	authOnly, _ := s.auditEntries(100, auditFilter{actionPrefix: "auth."})
	if len(authOnly) != 2 {
		t.Fatalf("expected 2 auth entries, got %d", len(authOnly))
	}
	// Date range (since/until as dates).
	since, _ := parseAuditTime("2026-06-02", false)
	until, _ := parseAuditTime("2026-06-02", true)
	day2, _ := s.auditEntries(100, auditFilter{since: since, until: until})
	if len(day2) != 1 {
		t.Fatalf("expected 1 entry on 2026-06-02, got %d", len(day2))
	}
}

func TestHandleAuditCSVExport(t *testing.T) {
	s := writeAuditFixture(t)
	rr := httptest.NewRecorder()
	s.handleAudit(rr, httptest.NewRequest(http.MethodGet, "/api/v1/audit?format=csv&user=alice", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("csv export status %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "text/csv") {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := rr.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment") {
		t.Fatalf("missing attachment disposition: %q", cd)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "ts,user,role,action") {
		t.Fatalf("CSV header missing: %s", body)
	}
	// Only alice rows (2) + header.
	rows := strings.Count(strings.TrimSpace(body), "\n")
	if rows != 2 { // 2 newlines between 3 lines (header + 2 data rows)
		t.Fatalf("expected header + 2 alice rows, got %d newlines:\n%s", rows, body)
	}
	// The extra config_path key folds into the details JSON column.
	if !strings.Contains(body, "config_path") {
		t.Fatalf("details column should include extra keys: %s", body)
	}
}
