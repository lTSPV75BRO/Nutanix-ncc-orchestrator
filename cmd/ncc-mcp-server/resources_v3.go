package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

func addAIResources(server *mcp.Server) {
	server.AddResource(&mcp.Resource{
		URI:         "ncc://config-schema",
		Name:        "ncc-config-schema",
		Description: "Versioned configuration contract for creating and validating NCC configurations. Secrets are represented by secret:// references.",
		MIMEType:    "application/schema+json",
	}, handleConfigSchemaResource)
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "ncc://runs/{run_id}/summary",
		Name:        "ncc-archived-run-summary",
		Description: "Machine-readable summary for one archived run. Use the run_id returned by run history or a run job.",
		MIMEType:    "application/json",
	}, handleArchivedRunResource)
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "ncc://runs/{run_id}/report/{file}",
		Name:        "ncc-archived-run-report",
		Description: "A bounded report artifact for one archived run. The file is confined to that run's artifact directory.",
		MIMEType:    "text/plain",
	}, handleArchivedRunResource)
}

func handleConfigSchemaResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	schema := map[string]any{
		"$schema":        "https://json-schema.org/draft/2020-12/schema",
		"$id":            "ncc://config-schema",
		"title":          "NCC Orchestrator configuration",
		"schema_version": ai.SchemaVersion,
		"type":           "object",
		"description":    "Use secret://NAME for credentials and notification secrets; do not embed plaintext secrets.",
		"properties": map[string]any{
			"schema-version": map[string]any{"type": "integer", "const": 1},
			"runner":         map[string]any{"type": "object"},
			"clusters":       map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
			"secrets":        map[string]any{"type": "object"},
			"notifications":  map[string]any{"type": "object"},
		},
	}
	data, _ := json.MarshalIndent(schema, "", "  ")
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
		URI: req.Params.URI, MIMEType: "application/schema+json", Text: string(data),
	}}}, nil
}

func handleArchivedRunResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	u, err := url.Parse(req.Params.URI)
	if err != nil {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if u.Host != "runs" || len(parts) < 2 || parts[0] == "" {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	// The run ID must name a directory inside runs/: ".." (or, on Windows, a
	// backslash path) would otherwise move runDir, and the report path check
	// below only confines files to runDir.
	runID := parts[0]
	runDir, err := safeJoin(filepath.Join(defaultOutputDir, "runs"), runID)
	if err != nil || runID == "." || strings.ContainsAny(runID, `\/`) {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	var file string
	switch {
	case len(parts) == 2 && parts[1] == "summary":
		file = "run-summary.json"
	case len(parts) >= 3 && parts[1] == "report":
		file = strings.Join(parts[2:], "/")
	default:
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	path, err := safeJoin(runDir, file)
	if err != nil {
		return nil, mcp.ResourceNotFoundError(req.Params.URI)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}
		return nil, err
	}
	const maxResourceBytes = 256 * 1024
	text := string(data)
	truncated := len(data) > maxResourceBytes
	if truncated {
		text = string(data[:maxResourceBytes]) + "\n\n... [truncated]"
	}
	mime := "application/json"
	if !strings.HasSuffix(file, ".json") {
		mime = "text/plain"
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
		URI: req.Params.URI, MIMEType: mime, Text: text,
	}}}, nil
}
