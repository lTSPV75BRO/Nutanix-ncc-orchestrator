package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/mcpclient"
)

func TestRedTeamRunNCCRequiresConfirm(t *testing.T) {
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "")
	res, _, err := runNCC(context.Background(), &mcp.CallToolRequest{}, RunNCCInput{ConfigPath: "config.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !res.IsError {
		t.Fatal("run_ncc must not trigger without mutations+APPLY")
	}
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "true")
	res, _, err = runNCC(context.Background(), &mcp.CallToolRequest{}, RunNCCInput{ConfigPath: "config.yaml", Confirm: "apply"})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !res.IsError {
		t.Fatal("confirm must be exact APPLY")
	}
}

func TestRedTeamRunNCCRefusesInsecureSkip(t *testing.T) {
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "true")
	res, _, err := runNCC(context.Background(), &mcp.CallToolRequest{}, RunNCCInput{Confirm: "APPLY", InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !res.IsError {
		t.Fatal("insecure_skip_verify must be refused")
	}
}

func TestRedTeamReplayRequiresConfirm(t *testing.T) {
	res, _, err := replayReports(context.Background(), &mcp.CallToolRequest{}, ReplayReportsInput{ConfigPath: "config.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !res.IsError {
		t.Fatal("replay_reports must require APPLY")
	}
}

func TestRedTeamGetReportTraversal(t *testing.T) {
	for _, file := range []string{"../etc/passwd", "..\\windows", "foo/bar", "foo\\bar", "index.html\x00.log", "a..b/c"} {
		res, _, err := getReport(context.Background(), &mcp.CallToolRequest{}, GetReportInput{File: file})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil || !res.IsError {
			t.Fatalf("wanted refuse for %q", file)
		}
	}
}

func TestRedTeamCompareRunsRejectsTraversalIDs(t *testing.T) {
	res, _, err := compareRuns(context.Background(), &mcp.CallToolRequest{}, CompareRunsInput{CurrentRun: "../x", BaselineRun: "20261120T080424Z"})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !res.IsError {
		t.Fatal("traversal run id must be refused")
	}
}

func TestRedTeamDiscoverRefusesHTTPAndUserinfo(t *testing.T) {
	for _, pc := range []string{"http://10.0.0.1:9440", "https://user:pass@pc.example:9440", "file:///etc/passwd", "https://pc.example#x"} {
		res, _, err := discoverClusters(context.Background(), &mcp.CallToolRequest{}, DiscoverClustersInput{PrismCentralURL: pc})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil || !res.IsError {
			t.Fatalf("wanted refuse for %q", pc)
		}
	}
}

func TestRedTeamJoinAPIURLStaysOnHost(t *testing.T) {
	base, err := url.Parse("http://127.0.0.1:8081")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinAPIURL(base, "/api/v1/runs/summary"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"https://evil.example/api/v1/x", "/etc/passwd", "/api/v1/../secret", "/api/v1/x://evil", "api/v1/runs"} {
		if _, err := joinAPIURL(base, p); err == nil {
			t.Fatalf("wanted reject %q", p)
		}
	}
}

func TestRedTeamCallAPIRejectsOpenRedirectPath(t *testing.T) {
	var saw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw = r.URL.String()
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{}})
	}))
	t.Cleanup(srv.Close)
	t.Setenv(mcpAuthAPIURLEnv, srv.URL)
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	orig := apiTransport
	t.Cleanup(func() { apiTransport = orig })
	apiTransport = srv.Client().Transport
	_, _, err := callAPI(context.Background(), "get_report", http.MethodGet, "https://evil.example/steal", nil, nil, 0)
	if err == nil {
		t.Fatal("absolute URL path must not be fetched")
	}
	if saw != "" {
		t.Fatalf("server was hit: %s", saw)
	}
}

func TestRedTeamParseAPIURLRejectsUserinfoAndQuery(t *testing.T) {
	if _, err := mcpclient.ParseAPIURL("http://user:pass@127.0.0.1:8081", "NCC_MCP_API_URL"); err == nil {
		t.Fatal("userinfo must be refused")
	}
	if _, err := mcpclient.ParseAPIURL("http://127.0.0.1:8081/?next=https://evil", "NCC_MCP_API_URL"); err == nil {
		t.Fatal("query must be refused")
	}
	if _, err := mcpclient.ParseAPIURL("http://127.0.0.1:8081/#@evil", "NCC_MCP_API_URL"); err == nil {
		t.Fatal("fragment must be refused")
	}
}

func TestRedTeamConfirmHomoglyphAndPadding(t *testing.T) {
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "true")
	for _, c := range []string{"APPLY\n", "APPLY\x00", "ＡＰＰＬＹ", " Apply "} {
		res, _, err := applyUpgrade(context.Background(), &mcp.CallToolRequest{}, ApplyUpgradeInput{Confirm: c})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil || !res.IsError {
			t.Fatalf("confirm %q must not pass", c)
		}
		// The refusal must say what is needed, so the model is told to ask the user.
		text := strings.ToLower(res.Content[0].(*mcp.TextContent).Text)
		if !strings.Contains(text, "apply") && !strings.Contains(text, "confirm") {
			t.Fatalf("refusal for %q does not mention confirm/APPLY: %s", c, text)
		}
	}
}
