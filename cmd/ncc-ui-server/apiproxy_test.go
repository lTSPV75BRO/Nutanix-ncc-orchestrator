package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// seenRequest is what the fake API server observed on one proxied request.
type seenRequest struct {
	Host   string
	Path   string
	Header http.Header
}

// newProxyFixture starts a fake API server answering status (recording the
// request it saw) and a front server running newAPIProxy against it. Fields
// left unset in cfg get token-injection defaults.
func newProxyFixture(t *testing.T, status int, cfg apiProxyConfig) (front *httptest.Server, seen chan seenRequest) {
	t.Helper()
	seen = make(chan seenRequest, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- seenRequest{Host: r.Host, Path: r.URL.Path, Header: r.Header.Clone()}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"success":false,"error":"unauthorized"}`)
	}))
	t.Cleanup(backend.Close)
	u, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Backend = u
	if cfg.ForwardSessions == nil {
		cfg.ForwardSessions = func() bool { return false }
	}
	if cfg.AuthMode == "" {
		cfg.AuthMode = "token"
	}
	if cfg.MintSession == nil {
		cfg.MintSession = func() string { return "minted-session" }
	}
	if cfg.BackendToken == nil {
		cfg.BackendToken = func() string { return "ui-admin-token" }
	}
	if cfg.APICSP == "" {
		cfg.APICSP = "default-src 'none'"
	}
	front = httptest.NewServer(newAPIProxy(cfg))
	t.Cleanup(front.Close)
	return front, seen
}

func proxyGet(t *testing.T, front *httptest.Server, path string, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, front.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	req.Host = "ncc.example.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestAPIProxyPreservesHostAndForwardingHeaders(t *testing.T) {
	front, seen := newProxyFixture(t, http.StatusOK, apiProxyConfig{})
	proxyGet(t, front, "/api/v1/runs", map[string]string{
		"X-Forwarded-For":   "203.0.113.7, 10.0.0.2",
		"X-Forwarded-Host":  "evil.example",
		"X-Forwarded-Proto": "gopher",
	})
	got := <-seen
	if got.Host != "ncc.example.com" {
		t.Errorf("Host = %q, want inbound ncc.example.com", got.Host)
	}
	if got.Path != "/api/v1/runs" {
		t.Errorf("Path = %q", got.Path)
	}
	if xff := got.Header.Get("X-Forwarded-For"); xff != "203.0.113.7, 10.0.0.2, 127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want inbound chain plus client IP", xff)
	}
	if v := got.Header.Get("X-Forwarded-Host"); v != "" {
		t.Errorf("X-Forwarded-Host = %q, want removed", v)
	}
	if v := got.Header.Get("X-Forwarded-Proto"); v != "http" {
		t.Errorf("X-Forwarded-Proto = %q, want backend scheme http", v)
	}
}

func TestAPIProxySetsClientIPWithoutInboundChain(t *testing.T) {
	front, seen := newProxyFixture(t, http.StatusOK, apiProxyConfig{})
	proxyGet(t, front, "/api/v1/runs", nil)
	if xff := (<-seen).Header.Get("X-Forwarded-For"); xff != "127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want 127.0.0.1", xff)
	}
}

func TestAPIProxyReplacesClientCredentials(t *testing.T) {
	cases := []struct {
		name      string
		cfg       apiProxyConfig
		wantToken string
		wantAuth  string
	}{
		{"token mode", apiProxyConfig{}, "ui-admin-token", ""},
		{"session mode", apiProxyConfig{AuthMode: "session"}, "", "Bearer minted-session"},
		{"forward sessions injects nothing", apiProxyConfig{ForwardSessions: func() bool { return true }}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			front, seen := newProxyFixture(t, http.StatusOK, tc.cfg)
			proxyGet(t, front, "/api/v1/runs", map[string]string{
				"Authorization": "Bearer client-supplied",
				"X-API-Token":   "client-supplied",
				"Cookie":        "ncc_session=abc",
			})
			got := <-seen
			if v := got.Header.Get("X-API-Token"); v != tc.wantToken {
				t.Errorf("X-API-Token = %q, want %q", v, tc.wantToken)
			}
			if v := got.Header.Get("Authorization"); v != tc.wantAuth {
				t.Errorf("Authorization = %q, want %q", v, tc.wantAuth)
			}
			if v := got.Header.Get("Cookie"); v != "ncc_session=abc" {
				t.Errorf("Cookie = %q, want forwarded", v)
			}
		})
	}
}

// A client naming the credential headers as hop-by-hop must not be able to
// strip what the proxy injects (Director ran before hop-by-hop removal;
// Rewrite runs after it).
func TestAPIProxyConnectionHeaderCannotStripInjectedCredentials(t *testing.T) {
	t.Run("X-API-Token", func(t *testing.T) {
		front, seen := newProxyFixture(t, http.StatusOK, apiProxyConfig{})
		proxyGet(t, front, "/api/v1/runs", map[string]string{"Connection": "X-API-Token"})
		if v := (<-seen).Header.Get("X-API-Token"); v != "ui-admin-token" {
			t.Errorf("X-API-Token = %q, want injected token", v)
		}
	})
	t.Run("Authorization", func(t *testing.T) {
		front, seen := newProxyFixture(t, http.StatusOK, apiProxyConfig{AuthMode: "session"})
		proxyGet(t, front, "/api/v1/runs", map[string]string{"Connection": "Authorization"})
		if v := (<-seen).Header.Get("Authorization"); v != "Bearer minted-session" {
			t.Errorf("Authorization = %q, want minted bearer", v)
		}
	})
}

func TestAPIProxyRewrites401IntoTokenMismatch(t *testing.T) {
	front, _ := newProxyFixture(t, http.StatusUnauthorized, apiProxyConfig{AbsTokenFile: "/etc/ncc/.ncc-api-token"})
	resp := proxyGet(t, front, "/api/v1/runs", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body struct {
		ErrorCode string `json:"error_code"`
		Data      struct {
			TokenFile string `json:"token_file"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.ErrorCode != "NCC_UI_TOKEN_MISMATCH" {
		t.Errorf("error_code = %q, want NCC_UI_TOKEN_MISMATCH", body.ErrorCode)
	}
	if body.Data.TokenFile != ".ncc-api-token" {
		t.Errorf("token_file = %q", body.Data.TokenFile)
	}
	if v := resp.Header.Get("Content-Security-Policy"); v != "default-src 'none'" {
		t.Errorf("Content-Security-Policy = %q", v)
	}
}

func TestAPIProxyPassesUser401Through(t *testing.T) {
	t.Run("auth path", func(t *testing.T) {
		front, _ := newProxyFixture(t, http.StatusUnauthorized, apiProxyConfig{})
		b, _ := io.ReadAll(proxyGet(t, front, "/api/v1/auth/login", nil).Body)
		if string(b) != `{"success":false,"error":"unauthorized"}` {
			t.Errorf("body = %s, want backend body untouched", b)
		}
	})
	t.Run("forward sessions", func(t *testing.T) {
		front, _ := newProxyFixture(t, http.StatusUnauthorized, apiProxyConfig{ForwardSessions: func() bool { return true }})
		b, _ := io.ReadAll(proxyGet(t, front, "/api/v1/runs", nil).Body)
		if string(b) != `{"success":false,"error":"unauthorized"}` {
			t.Errorf("body = %s, want backend body untouched", b)
		}
	})
}

func TestAPIProxyBackendDownReturns502(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:1")
	front := httptest.NewServer(newAPIProxy(apiProxyConfig{
		Backend:         u,
		ForwardSessions: func() bool { return false },
		BackendToken:    func() string { return "t" },
		APICSP:          "default-src 'none'",
	}))
	defer front.Close()
	resp := proxyGet(t, front, "/api/v1/runs", nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}
