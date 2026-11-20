package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"goncc/internal/mcpclient"
)

func TestRejectInvalidMCPClientOldBinary(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	req.Header.Set("User-Agent", "ncc-mcp-server/2.3.0")
	if !rejectInvalidMCPClient(rr, req) {
		t.Fatal("2.3.0 MCP must be refused")
	}
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d", rr.Code)
	}
	var env envelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.ErrorCode != mcpclient.ErrorCode {
		t.Fatalf("envelope %+v", env)
	}
}

func TestRejectInvalidMCPClientVersionStampWithoutProof(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	req.Header.Set(mcpclient.VersionHeader, "2.4.0")
	req.Header.Set("Authorization", "Bearer ncc_pat_testtoken12")
	rr := httptest.NewRecorder()
	if !rejectInvalidMCPClient(rr, req) {
		t.Fatal("claiming 2.4.0 without HMAC proof must be refused")
	}
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d", rr.Code)
	}
}

func TestRejectInvalidMCPClientForgedProof(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	req.Header.Set(mcpclient.VersionHeader, "2.4.0")
	req.Header.Set(mcpclient.ProofHeader, "v1.1.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	req.Header.Set("Authorization", "Bearer ncc_pat_testtoken12")
	rr := httptest.NewRecorder()
	if !rejectInvalidMCPClient(rr, req) {
		t.Fatal("forged proof must be refused")
	}
	var env envelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.ErrorCode != mcpclient.ProofErrorCode {
		t.Fatalf("want proof error, got %+v", env)
	}
}

func TestRejectInvalidMCPClientAcceptsSignedProof(t *testing.T) {
	token := "ncc_pat_testtoken12"
	now := time.Now().UTC()
	proof, err := mcpclient.SignProof(token, "2.4.0", now)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	req.Header.Set(mcpclient.VersionHeader, "2.4.0")
	req.Header.Set(mcpclient.ProofHeader, proof)
	req.Header.Set("Authorization", "Bearer "+token)
	if rejectInvalidMCPClient(httptest.NewRecorder(), req) {
		t.Fatal("valid 2.4.0 proof must pass this gate")
	}
}

func TestRejectInvalidMCPClientToolHeaderWithoutProof(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	req.Header.Set(mcpclient.ToolHeader, "get_findings")
	req.Header.Set("Authorization", "Bearer ncc_pat_testtoken12")
	rr := httptest.NewRecorder()
	if !rejectInvalidMCPClient(rr, req) {
		t.Fatal("tool header without proof must be treated as MCP and refused")
	}
}

func TestRejectInvalidMCPClientIgnoresBrowsersAndThirdPartyUA(t *testing.T) {
	browser := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	if rejectInvalidMCPClient(httptest.NewRecorder(), browser) {
		t.Fatal("browser must not be gated as MCP")
	}
	curl := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/ncc", nil)
	curl.Header.Set("User-Agent", "curl/8.4.0")
	curl.Header.Set("Authorization", "Bearer ncc_pat_testtoken12")
	if rejectInvalidMCPClient(httptest.NewRecorder(), curl) {
		t.Fatal("non-MCP automation must not be gated as MCP")
	}
}

func TestHandleMeRejectsOldMCP(t *testing.T) {
	s := newLoginServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	req.Header.Set("User-Agent", "ncc-mcp-server/2.3.0")
	req.Header.Set("X-API-Token", "admintoken")
	rr := httptest.NewRecorder()
	s.handleMe(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
}

func TestMCPCallAuditLine(t *testing.T) {
	s := newLoginServer(t)
	dir := t.TempDir()
	s.auditLogPath = filepath.Join(dir, "audit.jsonl")
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})
	h := s.withRequestMeta(inner)
	token := "ncc_pat_testtoken12"
	proof, err := mcpclient.SignProof(token, "2.4.0", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set("User-Agent", "ncc-mcp-server/2.4.0")
	req.Header.Set(mcpclient.ToolHeader, "get_run_summary")
	req.Header.Set(mcpclient.VersionHeader, "2.4.0")
	req.Header.Set(mcpclient.ProofHeader, proof)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	entries, err := s.auditEntries(10, auditFilter{actionPrefix: "mcp.call"})
	if err != nil || len(entries) != 1 {
		t.Fatalf("want 1 mcp.call, got %d err=%v", len(entries), err)
	}
	if entries[0]["channel"] != "mcp" || entries[0]["mcp_tool"] != "get_run_summary" {
		t.Fatalf("entry %+v", entries[0])
	}
	meReq := httptest.NewRequest(http.MethodGet, mcpclient.AuthMePath, nil)
	meReq.Header.Set(mcpclient.ToolHeader, "auth")
	meReq.Header.Set(mcpclient.VersionHeader, "2.4.0")
	meReq.Header.Set(mcpclient.ProofHeader, proof)
	h.ServeHTTP(httptest.NewRecorder(), meReq)
	entries, err = s.auditEntries(10, auditFilter{actionPrefix: "mcp.call"})
	if err != nil || len(entries) != 1 {
		t.Fatalf("auth/me must not add mcp.call, got %d err=%v", len(entries), err)
	}
}

func TestMCPCallAuditPATIdentityAndFailure(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "alice", RoleAdmin)
	secret := patPrefix + "secret-alice-admin-mcp12"
	id := "pat-alice-mcp"
	if err := s.users.addToken(personalToken{
		ID: id, Name: "cursor-mcp", Owner: "alice", OwnerLocal: true,
		Role: "admin", Hash: patHash(secret),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatal(err)
	}
	s.auditLogPath = filepath.Join(t.TempDir(), "audit.jsonl")
	h := s.withRequestMeta(withErrorCapture(s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, envelope{Success: true})
	}))))

	proof, err := mcpclient.SignProof(secret, "2.4.0", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	okReq.Header.Set(mcpclient.ToolHeader, "get_run_summary")
	okReq.Header.Set(mcpclient.VersionHeader, "2.4.0")
	okReq.Header.Set(mcpclient.ProofHeader, proof)
	okReq.Header.Set("Authorization", "Bearer "+secret)
	okReq.Header.Set("User-Agent", "ncc-mcp-server/2.4.0")
	h.ServeHTTP(httptest.NewRecorder(), okReq)
	entries, err := s.auditEntries(10, auditFilter{actionPrefix: "mcp.call"})
	if err != nil || len(entries) != 1 {
		t.Fatalf("want 1 success mcp.call, got %d err=%v", len(entries), err)
	}
	e := entries[0]
	if e["success"] != true || e["user"] != "alice" || e["pat_id"] != id || e["pat_name"] != "cursor-mcp" || e["auth"] != "personal-access-token" {
		t.Fatalf("success entry %+v", e)
	}

	bad := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	bad.Header.Set(mcpclient.ToolHeader, "get_run_summary")
	bad.Header.Set(mcpclient.VersionHeader, "2.4.0")
	bad.Header.Set("Authorization", "Bearer "+secret)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, bad)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d %s", rr.Code, rr.Body.String())
	}
	entries, err = s.auditEntries(10, auditFilter{actionPrefix: "mcp.call", onlyFailures: true})
	if err != nil || len(entries) != 1 {
		t.Fatalf("want 1 failed mcp.call, got %d err=%v", len(entries), err)
	}
	f := entries[0]
	if f["pat_id"] != id || f["pat_name"] != "cursor-mcp" || f["user"] != "alice" {
		t.Fatalf("failed PAT identity %+v", f)
	}
	errText, _ := f["error"].(string)
	rem, _ := f["remediation"].(string)
	if errText == "" || rem == "" {
		t.Fatalf("failed entry missing error/remediation %+v", f)
	}
}

func TestHandleMeRejectsStampedVersion(t *testing.T) {
	s := newLoginServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	req.Header.Set("User-Agent", "ncc-mcp-server/2.4.0")
	req.Header.Set(mcpclient.VersionHeader, "2.4.0")
	req.Header.Set("X-API-Token", "admintoken")
	rr := httptest.NewRecorder()
	s.handleMe(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("stamped 2.4.0 without proof: status %d body %s", rr.Code, rr.Body.String())
	}
}
