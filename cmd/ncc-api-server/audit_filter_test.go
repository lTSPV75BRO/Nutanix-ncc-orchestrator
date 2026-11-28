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
		`{"ts":"2026-06-04T13:00:00Z","action":"mcp.call","success":true,"channel":"mcp","mcp_tool":"run_ncc","user":"svc-mcp","method":"POST","path":"/api/v1/runs/trigger"}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &apiServer{auditLogPath: path}
}

func TestAuditEntriesFiltering(t *testing.T) {
	s := writeAuditFixture(t)

	all, err := s.auditEntries(100, auditFilter{})
	if err != nil || len(all) != 4 {
		t.Fatalf("expected 4 entries, got %d (err=%v)", len(all), err)
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
	mcpOnly, _ := s.auditEntries(100, auditFilter{channel: "mcp"})
	if len(mcpOnly) != 1 {
		t.Fatalf("expected 1 mcp channel entry, got %d", len(mcpOnly))
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
	if !strings.Contains(body, "ts,user,role,auth,pat_id,pat_name,client_kind") {
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

// The audit page hides the automatic request and MCP call records by default
// (?exclude=http.request,mcp.call) so the log shows what people changed, and
// asks for them only when the operator selects them.
func TestAuditExcludeDropsAutomaticRecords(t *testing.T) {
	s := writeAuditFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/audit?limit=50&exclude=mcp.call,http.request", nil)
	rec := httptest.NewRecorder()
	s.handleAudit(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, `"mcp.call"`) {
		t.Fatalf("mcp.call must be excluded:\n%s", body)
	}
	if !strings.Contains(body, `"settings.config"`) || !strings.Contains(body, `"auth.login"`) {
		t.Fatalf("human actions must remain:\n%s", body)
	}
	// Selecting the action explicitly still works.
	rec = httptest.NewRecorder()
	s.handleAudit(rec, httptest.NewRequest(http.MethodGet, "/api/v1/audit?action=mcp.call", nil))
	if !strings.Contains(rec.Body.String(), `"mcp.call"`) {
		t.Fatalf("explicit action filter must show mcp.call:\n%s", rec.Body.String())
	}
}
