package main

import (
	"github.com/modelcontextprotocol/go-sdk/mcp"
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
		Description: "A bounded report artifact for one archived run, served through the authenticated API.",
		MIMEType:    "text/plain",
	}, handleArchivedRunResource)
}
