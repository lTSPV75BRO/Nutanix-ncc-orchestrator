package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"goncc/internal/uiversion"
)

func TestRejectCompromisedUIVersion221(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "alice", RoleAdmin)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	req.Header.Set(uiversion.VersionHeader, "2.2.1")
	rr := httptest.NewRecorder()
	if !s.rejectCompromisedUIClient(rr, req) {
		t.Fatal("2.2.1 UI must be refused")
	}
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status %d", rr.Code)
	}
	var env envelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.ErrorCode != uiversion.ErrorCode {
		t.Fatalf("envelope %+v", env)
	}
}

func TestRejectUnversionedUIByDefault(t *testing.T) {
	s := newTokenTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	rr := httptest.NewRecorder()
	if !s.rejectCompromisedUIClient(rr, req) {
		t.Fatal("unversioned ncc-ui must be refused by default")
	}
}

func TestAllowCurrentUIVersion(t *testing.T) {
	s := newTokenTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	req.Header.Set(uiversion.VersionHeader, "2.4.0")
	if s.rejectCompromisedUIClient(httptest.NewRecorder(), req) {
		t.Fatal("2.4.0 UI must pass")
	}
}

func TestExtraBlockedUIVersion(t *testing.T) {
	s := newTokenTestServer(t)
	if err := s.users.setUIVersionPolicy(uiVersionPolicy{
		BlockUnversioned: true,
		Extra:            []uiversion.Entry{{Version: "2.0.0", Component: "ui", Reason: "test"}},
	}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	req.Header.Set(uiversion.VersionHeader, "2.0.0")
	if !s.rejectCompromisedUIClient(httptest.NewRecorder(), req) {
		t.Fatal("extra blocked version must be refused")
	}
}

func TestAuditTagsUIVersion(t *testing.T) {
	s := newTokenTestServer(t)
	s.auditLogPath = filepath.Join(t.TempDir(), "audit.jsonl")
	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings/config", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	req.Header.Set(uiversion.VersionHeader, "2.4.0")
	req.Header.Set(uiversion.ServerVersionHeader, "2.4.0")
	s.audit(req, "settings.config.update", true, nil)
	entries, err := s.auditEntries(5, auditFilter{})
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries %d err=%v", len(entries), err)
	}
	e := entries[0]
	if e["client_kind"] != "ui" || e["ui_version"] != "2.4.0" || e["ui_server_version"] != "2.4.0" || e["channel"] != "ui" {
		t.Fatalf("entry %+v", e)
	}
}

func TestAutoAuditMutatingWhenHandlerSilent(t *testing.T) {
	s := newTokenTestServer(t)
	s.auditLogPath = filepath.Join(t.TempDir(), "audit.jsonl")
	h := s.withRequestMeta(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/trigger", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	entries, err := s.auditEntries(5, auditFilter{actionPrefix: "http.request"})
	if err != nil || len(entries) != 1 {
		t.Fatalf("want auto http.request, got %d err=%v", len(entries), err)
	}
}

func TestHandleUIVersionPolicyRoundTrip(t *testing.T) {
	s := newTokenTestServer(t)
	body := strings.NewReader(`{"block_unversioned":false,"extra":[{"version":"1.9.0","component":"ui","reason":"lab"}]}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings/ui-versions", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.handleUIVersionPolicy(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put %d %s", rr.Code, rr.Body.String())
	}
	if s.users.blockUnversionedUI() {
		t.Fatal("block_unversioned should be false")
	}
	get := httptest.NewRecorder()
	s.handleUIVersionPolicy(get, httptest.NewRequest(http.MethodGet, "/api/v1/settings/ui-versions", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), "1.9.0") {
		t.Fatalf("get %s", get.Body.String())
	}
}
