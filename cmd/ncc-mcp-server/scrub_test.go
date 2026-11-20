package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func stubAPI(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	t.Setenv(mcpAuthAPIURLEnv, srv.URL)
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12345")
	orig := apiTransport
	t.Cleanup(func() { apiTransport = orig })
	apiTransport = srv.Client().Transport
}

func toolText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatal("empty tool result")
	}
	return res.Content[0].(*mcp.TextContent).Text
}

// Regression: get_sample_config returned the configuration endpoint's data
// as-is, which carries the raw file in "content" next to "content_redacted".
// The model must only ever see the redacted copy.
func TestGetSampleConfigNeverReturnsTheRawFile(t *testing.T) {
	const rawSecret = "Sup3r-Secret-Value"
	stubAPI(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{
			"path":             "/data/config/config.yaml",
			"content":          "password: " + rawSecret + "\nslack: https://hooks.slack.com/services/T0/B0/XYZ\n",
			"content_redacted": "password: \"***\"\nslack: https://hooks.slack.com/services/***\n",
		}})
	})
	res, _, err := getSampleConfig(context.Background(), &mcp.CallToolRequest{}, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	out := toolText(t, res)
	for _, leaked := range []string{rawSecret, "XYZ"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("%q reached the model:\n%s", leaked, out)
		}
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("expected the redacted copy:\n%s", out)
	}
}

func TestScrubForModelMasksCredentialKeysAndKeepsReferences(t *testing.T) {
	in := map[string]any{
		"status":         "ok",
		"password":       "hunter2",
		"api_token":      "abc",
		"token_source":   "generated",
		"webhook_secret": "s3",
		"smtp_password":  "secret://SMTP_PASSWORD",
		"nested": map[string]any{
			"authorization": "Bearer xyz",
			"url":           "smtps://user:pa55@mail.example.com",
			"count":         float64(3),
		},
		"list": []any{map[string]any{"bind_pw": "x"}},
	}
	out := scrubForModel(in).(map[string]any)
	if out["password"] != scrubMask || out["api_token"] != scrubMask || out["webhook_secret"] != scrubMask {
		t.Fatalf("credentials not masked: %#v", out)
	}
	if out["token_source"] != "generated" || out["smtp_password"] != "secret://SMTP_PASSWORD" || out["status"] != "ok" {
		t.Fatalf("benign values were changed: %#v", out)
	}
	n := out["nested"].(map[string]any)
	if n["authorization"] != scrubMask || strings.Contains(n["url"].(string), "pa55") || n["count"] != float64(3) {
		t.Fatalf("nested values wrong: %#v", n)
	}
	if out["list"].([]any)[0].(map[string]any)["bind_pw"] != scrubMask {
		t.Fatalf("list values not masked: %#v", out["list"])
	}
}

// Regression: backup_v2_stack called GET /settings/backup (a download of the
// whole archive), discarded it and answered {"ok":true} while nothing was
// saved. It now asks the server to create its own snapshot.
func TestBackupToolCreatesAServerSideSnapshot(t *testing.T) {
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "true")
	var method, path string
	var body map[string]any
	stubAPI(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "message": "snapshot created", "data": map[string]any{"name": "manual-20261120T101500Z.tar.gz.enc"}})
	})
	res, _, err := backupV2Stack(context.Background(), &mcp.CallToolRequest{}, BackupV2StackInput{Confirm: "APPLY"})
	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/api/v1/settings/backups" {
		t.Fatalf("called %s %s", method, path)
	}
	if _, has := body["passphrase"]; has {
		t.Fatal("no passphrase may be sent over MCP")
	}
	if !strings.Contains(toolText(t, res), "manual-20261120T101500Z.tar.gz.enc") {
		t.Fatalf("the snapshot name should be returned: %s", toolText(t, res))
	}
}

func TestRestoreToolAcceptsOnlyBackupNames(t *testing.T) {
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "true")
	for _, bad := range []string{"../../etc/passwd", "/etc/passwd", `C:\x.tar.gz`, "a/b.tar.gz", "backup.zip", "x.tar.gz.enc.exe", "..tar.gz", ""} {
		res, _, err := restoreV2Backup(context.Background(), &mcp.CallToolRequest{}, RestoreV2BackupInput{Confirm: "APPLY", InputFile: bad})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil || !res.IsError {
			t.Fatalf("%q must be refused", bad)
		}
	}
	for _, good := range []string{"manual-20261120T101500Z.tar.gz.enc", "pre-update-1.tar.gz"} {
		if _, err := safeBackupName(good); err != nil {
			t.Fatalf("%q should be accepted: %v", good, err)
		}
	}
}
