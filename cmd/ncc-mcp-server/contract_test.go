package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

func TestNormalizeMCPOutputParsesJSON(t *testing.T) {
	result := &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: `{"clusters_ok":2,"clusters_failed":1}`}},
	}
	got := normalizeMCPOutput(result, nil, nil)
	if got.SchemaVersion != ai.SchemaVersion || got.Status != "success" {
		t.Fatalf("unexpected output: %+v", got)
	}
	data, ok := got.Data.(map[string]any)
	if !ok || data["clusters_ok"] != float64(2) {
		t.Fatalf("expected parsed JSON data, got %#v", got.Data)
	}
}

func TestNormalizeMCPOutputIncludesStableError(t *testing.T) {
	result := &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: "permission denied"}},
		IsError: true,
	}
	errEnvelope := ai.NewError(ai.CodePermissionDenied, "authorization", "permission denied")
	got := normalizeMCPOutput(result, errEnvelope, nil)
	if got.Status != "error" || got.Error == nil || got.Error.Code != ai.CodePermissionDenied {
		t.Fatalf("unexpected error output: %+v", got)
	}
}

func TestAddAIToolAdvertisesOutputSchema(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "3.0.0"}, nil)
	addAITool(server, &mcp.Tool{Name: "contract_test", Description: "contract test"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		return nil, map[string]any{"ok": true}, nil
	})
	// Registration would panic if the adapter did not provide an object input
	// schema and output schema. The assertion covers the shared schema itself.
	if mcpV3OutputSchema["type"] != "object" {
		t.Fatalf("unexpected output schema: %#v", mcpV3OutputSchema)
	}
}

func TestMCPProfilesRestrictCapabilities(t *testing.T) {
	t.Setenv("NCC_MCP_PROFILE", "readonly")
	if mcpToolAllowed("run_ncc") {
		t.Fatal("readonly profile must not advertise run_ncc")
	}
	if !mcpToolAllowed("get_run_summary") {
		t.Fatal("readonly profile must advertise read-only tools")
	}
	t.Setenv("NCC_MCP_PROFILE", "admin")
	if !mcpToolAllowed("apply_upgrade") {
		t.Fatal("admin profile must advertise admin tools")
	}
}

func TestLLMScenarioFixturesAreValid(t *testing.T) {
	data, err := os.ReadFile("testdata/llm_scenarios.json")
	if err != nil {
		t.Fatalf("read scenarios: %v", err)
	}
	var scenarios []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &scenarios); err != nil {
		t.Fatalf("parse scenarios: %v", err)
	}
	if len(scenarios) < 3 {
		t.Fatalf("expected representative evaluation scenarios, got %d", len(scenarios))
	}
}
