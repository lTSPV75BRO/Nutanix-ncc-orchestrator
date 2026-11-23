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

func TestSSOAuthPathsSkipUIVersionGate(t *testing.T) {
	s := newTokenTestServer(t)
	for _, path := range []string{oidcLoginPath, oidcCallbackPath, "/saml/login", "/saml/acs"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		// These are the headers on an identity redirect routed through
		// ncc-ui-server: it stamps its own version, but an IdP callback has
		// no SPA request header from which to obtain the frontend version.
		req.Header.Set("X-Requested-With", "ncc-ui")
		req.Header.Set(uiversion.ServerVersionHeader, "2.4.0")
		if s.rejectCompromisedUIClient(httptest.NewRecorder(), req) {
			t.Errorf("%s must skip the UI version gate", path)
		}
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

func TestBlockedUIWritesUiBlockedAuditThroughAuth(t *testing.T) {
	s := newTokenTestServer(t)
	addLocalAccount(t, s.users, "alice", RoleAdmin)
	s.auditLogPath = filepath.Join(t.TempDir(), "audit.jsonl")
	h := s.withRequestMeta(withErrorCapture(s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, envelope{Success: true})
	}))))

	blocked := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	blocked.Header.Set("X-Requested-With", "ncc-ui")
	blocked.Header.Set(uiversion.VersionHeader, "2.2.1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, blocked)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("2.2.1: want 403, got %d %s", rr.Code, rr.Body.String())
	}
	var env envelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.ErrorCode != uiversion.ErrorCode || env.Remediation == "" {
		t.Fatalf("block envelope %+v", env)
	}
	entries, err := s.auditEntries(10, auditFilter{actionPrefix: "ui.blocked"})
	if err != nil || len(entries) != 1 {
		t.Fatalf("want 1 ui.blocked, got %d err=%v", len(entries), err)
	}
	e := entries[0]
	if e["success"] != false || e["ui_version"] != "2.2.1" || e["ui_compromised"] != true || e["client_kind"] != "ui" {
		t.Fatalf("ui.blocked entry %+v", e)
	}
	if _, ok := e["error"].(string); !ok {
		t.Fatalf("missing error on %+v", e)
	}
	if _, ok := e["remediation"].(string); !ok {
		t.Fatalf("missing remediation on %+v", e)
	}

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	okReq.Header.Set("X-Requested-With", "ncc-ui")
	okReq.Header.Set(uiversion.VersionHeader, "2.4.0")
	okRR := httptest.NewRecorder()
	h.ServeHTTP(okRR, okReq)
	if okRR.Code == http.StatusForbidden {
		var blockedEnv envelope
		_ = json.Unmarshal(okRR.Body.Bytes(), &blockedEnv)
		if blockedEnv.ErrorCode == uiversion.ErrorCode {
			t.Fatalf("2.4.0 UI must not be version-blocked: %s", okRR.Body.String())
		}
	}

	health := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	health.Header.Set("X-Requested-With", "ncc-ui")
	health.Header.Set(uiversion.VersionHeader, "2.2.1")
	healthRR := httptest.NewRecorder()
	h.ServeHTTP(healthRR, health)
	if healthRR.Code == http.StatusForbidden {
		t.Fatalf("health must skip UI version gate, got %s", healthRR.Body.String())
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

// ncc-ui-server stamps its version on everything it proxies. A script, a custom
// client or a plain link click through that port is not "the dashboard" and must
// not be refused as an unversioned one.
func TestProxiedNonDashboardRequestsAreNotRefusedAsUnversioned(t *testing.T) {
	s := newTokenTestServer(t)
	cases := map[string]func(*http.Request){
		"script with a token":          func(r *http.Request) { r.Header.Set("Authorization", "Bearer x") },
		"custom client":                func(r *http.Request) { r.Header.Set(uiversion.ClientHeader, "acme-console") },
		"custom client, blocked label": func(r *http.Request) { r.Header.Set(uiversion.ClientHeader, "acme"); r.Header.Set(uiversion.VersionHeader, "2.2.1") },
		"download link (cookie only)":  func(r *http.Request) { r.Header.Set("Cookie", "ncc_session=x") },
	}
	for name, mod := range cases {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/artifacts/index.html?download=1", nil)
		req.Header.Set(uiversion.ServerVersionHeader, "2.4.0")
		mod(req)
		if s.rejectCompromisedUIClient(httptest.NewRecorder(), req) {
			t.Errorf("%s: refused as a dashboard, but it is not one", name)
		}
	}
}

func TestProxiedDashboardWithoutVersionIsStillRefused(t *testing.T) {
	s := newTokenTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set(uiversion.ServerVersionHeader, "2.4.0")
	req.Header.Set("X-Requested-With", "ncc-ui") // claims to be the dashboard, declares no version
	if !s.rejectCompromisedUIClient(httptest.NewRecorder(), req) {
		t.Fatal("a pre-2.4.0 dashboard (marker, no version) must still be refused")
	}
}

func TestBlockedUIServerVersionStillRefusesEverythingThroughIt(t *testing.T) {
	s := newTokenTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/summary", nil)
	req.Header.Set(uiversion.ServerVersionHeader, "2.2.1")
	if !s.rejectCompromisedUIClient(httptest.NewRecorder(), req) {
		t.Fatal("a blocked ui-server version must be refused whoever is behind it")
	}
}
