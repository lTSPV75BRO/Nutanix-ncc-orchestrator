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

// Every release the scan found a vulnerability in is on the shipped list, with
// the advisory ids the API returns so the dashboard can show them.
func TestShippedBlockedVersionsCarryAdvisories(t *testing.T) {
	list := uiversion.Builtin()
	want := []string{"2.0.0", "2.0.1", "2.0.2", "2.1.0", "2.1.1", "2.2.0", "2.2.1"}
	for _, v := range want {
		e, ok := uiversion.Match(v, "ui", list)
		if !ok {
			t.Errorf("%s should be blocked", v)
			continue
		}
		if len(e.Advisories) == 0 || e.FixedIn != uiversion.SupportedFrom || e.Severity == "" {
			t.Errorf("%s entry incomplete: %+v", v, e)
		}
	}
	for _, v := range []string{"2.3.0", "2.4.0", "2.5.0"} {
		if _, ok := uiversion.Match(v, "ui", list); ok {
			t.Errorf("%s must not be on the shipped list", v)
		}
	}
}

func TestBlockedResponseNamesTheAdvisories(t *testing.T) {
	s := newTokenTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	req.Header.Set(uiversion.VersionHeader, "2.1.0")
	rr := httptest.NewRecorder()
	if !s.rejectCompromisedUIClient(rr, req) {
		t.Fatal("2.1.0 must be refused")
	}
	var env struct {
		ErrorCode string                 `json:"error_code"`
		Data      map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	adv, _ := env.Data["advisory"].(string)
	if env.ErrorCode != uiversion.ErrorCode || !strings.Contains(adv, "GO-2026-6603") || env.Data["fixed_in"] != uiversion.SupportedFrom {
		t.Fatalf("response data: %+v", env)
	}
}

// A dashboard that cannot declare a version is older than 2.4.0; the answer
// must say that, and must not tell the operator that 2.3.0 is a fix.
func TestUnversionedRefusalPointsAt240(t *testing.T) {
	s := newTokenTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set("X-Requested-With", "ncc-ui")
	rr := httptest.NewRecorder()
	if !s.rejectCompromisedUIClient(rr, req) {
		t.Fatal("unversioned dashboard must be refused by default")
	}
	body := rr.Body.String()
	if !strings.Contains(body, "2.4.0") || strings.Contains(body, "2.3.0 or later") {
		t.Fatalf("remediation text: %s", body)
	}
}
