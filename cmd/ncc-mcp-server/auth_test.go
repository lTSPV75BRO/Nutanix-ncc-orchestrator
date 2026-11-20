package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
	"goncc/internal/mcpclient"
)

func fakeAuthAPI(t *testing.T, authenticated bool, extra http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if extra != nil {
			extra(w, r)
		}
		if r.URL.Path != mcpclient.AuthMePath {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"authenticated": authenticated},
		})
	}))
	t.Cleanup(srv.Close)
	t.Setenv(mcpAuthAPIURLEnv, srv.URL)
	return srv
}

func TestMCPAuthReadyRequiresToken(t *testing.T) {
	t.Setenv(mcpAuthTokenEnv, "")
	if err := mcpAuthReady(); err == nil || !strings.Contains(err.Error(), mcpAuthTokenEnv) {
		t.Fatalf("want missing-token error, got %v", err)
	}
	t.Setenv(mcpAuthTokenEnv, "tooshort")
	if err := mcpAuthReady(); err == nil {
		t.Fatal("short token must be rejected")
	}
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	if err := mcpAuthReady(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizeMCPRequestStdioUsesEnvToken(t *testing.T) {
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	fakeAuthAPI(t, true, nil)
	req := &mcp.CallToolRequest{}
	if err := authorizeMCPRequest(req); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizeMCPRequestHTTPRequiresMatchingHeader(t *testing.T) {
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	fakeAuthAPI(t, true, nil)
	missing := &mcp.CallToolRequest{Extra: &mcp.RequestExtra{Header: http.Header{}}}
	if err := authorizeMCPRequest(missing); err == nil || err.Error() != "authentication required" {
		t.Fatalf("want authentication required, got %v", err)
	}
	wrong := &mcp.CallToolRequest{Extra: &mcp.RequestExtra{Header: http.Header{
		"Authorization": []string{"Bearer ncc_pat_other_token"},
	}}}
	if err := authorizeMCPRequest(wrong); err == nil {
		t.Fatal("mismatched bearer token must be rejected")
	}
	ok := &mcp.CallToolRequest{Extra: &mcp.RequestExtra{Header: http.Header{
		"Authorization": []string{"Bearer ncc_pat_testtoken12"},
	}}}
	if err := authorizeMCPRequest(ok); err != nil {
		t.Fatal(err)
	}
	okHeader := &mcp.CallToolRequest{Extra: &mcp.RequestExtra{Header: http.Header{}}}
	okHeader.Extra.Header.Set("X-API-Token", "ncc_pat_testtoken12")
	if err := authorizeMCPRequest(okHeader); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizeMCPRequestVerifiesAgainstAPI(t *testing.T) {
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	fakeAuthAPI(t, true, nil)
	if err := authorizeMCPRequest(&mcp.CallToolRequest{}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_wrong_token1")
	fakeAuthAPI(t, false, nil)
	if err := authorizeMCPRequest(&mcp.CallToolRequest{}); err == nil {
		t.Fatal("API must reject an unauthenticated token")
	}
}

func TestParseMCPAPIURLRejectsPlainHTTPOffLoopback(t *testing.T) {
	if _, err := mcpclient.ParseAPIURL("http://example.invalid", mcpAuthAPIURLEnv); err == nil {
		t.Fatal("non-loopback http must be rejected")
	}
	if _, err := mcpclient.ParseAPIURL("https://ncc.example.com", mcpAuthAPIURLEnv); err != nil {
		t.Fatal(err)
	}
	if _, err := mcpclient.ParseAPIURL("http://127.0.0.1:8081", mcpAuthAPIURLEnv); err != nil {
		t.Fatal(err)
	}
}

func TestRequireMCPAuthBlocksToolCalls(t *testing.T) {
	t.Setenv(mcpAuthTokenEnv, "")
	called := false
	h := requireMCPAuth(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		called = true
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ran"}}}, nil
	})
	res, err := h(context.Background(), "tools/call", &mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := res.(*mcp.CallToolResult)
	if !ok || got == nil || !got.IsError {
		t.Fatalf("want tool error, got %#v", res)
	}
	if called {
		t.Fatal("handler must not run without auth")
	}
	text := got.Content[0].(*mcp.TextContent).Text
	var envelope ai.Error
	if json.Unmarshal([]byte(text), &envelope) != nil || envelope.Code != ai.CodePermissionDenied {
		t.Fatalf("want PERMISSION_DENIED envelope, got %q", text)
	}
	res, err = h(context.Background(), "initialize", &mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("initialize must not require a token")
	}
	_ = res
}

func TestLookupMCPIdentitySurfacesAPIUpgradeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(mcpclient.VersionHeader) != productVersion {
			t.Errorf("missing MCP version header")
		}
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success":     false,
			"error":       mcpclient.UpgradeMessage(),
			"error_code":  mcpclient.ErrorCode,
			"remediation": mcpclient.Remediation(),
		})
	}))
	t.Cleanup(srv.Close)
	err := lookupMCPIdentity(srv.URL, "ncc_pat_testtoken12")
	if err == nil || !strings.Contains(err.Error(), "upgrade ncc-mcp-server") {
		t.Fatalf("got %v", err)
	}
}

func TestLookupMCPIdentityRejectsUnauthenticatedMe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"authenticated": false},
		})
	}))
	t.Cleanup(srv.Close)
	err := lookupMCPIdentity(srv.URL, "ncc_pat_testtoken12")
	if err == nil || err.Error() != "authentication required" {
		t.Fatalf("got %v", err)
	}
}

func TestCallAPISendsProofAndToolHeader(t *testing.T) {
	var proof, tool string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proof = r.Header.Get(mcpclient.ProofHeader)
		tool = r.Header.Get(mcpclient.ToolHeader)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"ok": true}})
	}))
	t.Cleanup(srv.Close)
	t.Setenv(mcpAuthAPIURLEnv, srv.URL)
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	orig := apiTransport
	t.Cleanup(func() { apiTransport = orig })
	apiTransport = srv.Client().Transport
	_, _, err := callAPI(context.Background(), "get_run_summary", http.MethodGet, "/api/v1/runs/summary", nil, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tool != "get_run_summary" {
		t.Fatalf("tool header %q", tool)
	}
	if err := mcpclient.VerifyProof("ncc_pat_testtoken12", productVersion, proof, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}
