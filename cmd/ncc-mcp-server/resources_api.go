package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

func handleRunSummaryResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	env, raw, err := callAPI(ctx, "resource_run_summary", http.MethodGet, "/api/v1/runs/summary", nil, nil, 30*time.Second)
	if err != nil {
		return nil, err
	}
	text := string(env.Data)
	if text == "" {
		text = string(raw)
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{URI: req.Params.URI, MIMEType: "application/json", Text: text},
		},
	}, nil
}

func handleReportResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	env, raw, err := callAPI(ctx, "resource_report", http.MethodGet, "/api/v1/artifacts/index.html", nil, nil, 30*time.Second)
	if err != nil {
		return nil, err
	}
	text := resourceText(env, raw)
	if len(text) > 150000 {
		text = text[:150000] + "\n\n... [truncated]"
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{URI: req.Params.URI, MIMEType: "text/html", Text: text},
		},
	}, nil
}

func handleConfigSchemaResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	schema := map[string]any{
		"$schema":        "https://json-schema.org/draft/2020-12/schema",
		"$id":            "ncc://config-schema",
		"title":          "NCC Orchestrator configuration",
		"schema_version": ai.SchemaVersion,
		"type":           "object",
		"description":    "Use secret://NAME for credentials. MCP does not accept passwords in tool arguments.",
	}
	data, _ := json.MarshalIndent(schema, "", "  ")
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{URI: req.Params.URI, MIMEType: "application/schema+json", Text: string(data)},
		},
	}, nil
}

func handleArchivedRunResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	u, err := url.Parse(req.Params.URI)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	// ncc://runs/{id}/summary or ncc://runs/{id}/report/{file}
	if len(parts) < 2 {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	runID, idErr := safeRunID(parts[0])
	if idErr != nil {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	env, raw, err := callAPI(ctx, "resource_run", http.MethodGet, "/api/v1/runs/"+url.PathEscape(runID), nil, nil, 30*time.Second)
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{URI: req.Params.URI, MIMEType: "application/json", Text: resourceText(env, raw)},
		},
	}, nil
}

func resourceText(env apiEnvelope, raw []byte) string {
	if len(env.Data) > 0 {
		return string(env.Data)
	}
	return string(raw)
}
