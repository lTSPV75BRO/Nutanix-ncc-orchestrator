package main

import (
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

func mcpTextError(message string) (*mcp.CallToolResult, any, error) {
	return mcpContractError(ai.CodePermissionDenied, "authorization", message, false)
}

func requireMCPConfirmation(confirm string) (*mcp.CallToolResult, any, error) {
	if !mcpMutationsAllowed() {
		return mcpTextError("mutating MCP tools are disabled; set NCC_MCP_ALLOW_MUTATIONS=true in the MCP server environment")
	}
	if confirm != "APPLY" {
		return mcpTextError("mutating MCP tools require confirm=\"APPLY\"")
	}
	return nil, nil, nil
}

func mcpMutationsAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("NCC_MCP_ALLOW_MUTATIONS"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
