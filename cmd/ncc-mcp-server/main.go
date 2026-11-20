// ncc-mcp-server exposes the Nutanix NCC Orchestrator as an MCP server so AI assistants
// (Cursor, Claude, etc.) can run NCC checks, discover clusters, and read run summaries.
package main

import (
	"context"
	"log"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const serverName = "ncc-orchestrator"

// serverVersion is the advertised MCP server version. It is overridable at build
// time via -ldflags "-X main.serverVersion=<v>" (the release build stamps the
// orchestrator version here) and otherwise defaults to the current release.
var serverVersion = "3.0.0"

// productVersion is the orchestrator release this MCP binary ships with.
// Release builds stamp it via -ldflags "-X main.productVersion=<v>".
var productVersion = "2.4.0"

func boolPtr(b bool) *bool { return &b }

func main() {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    serverName,
		Version: serverVersion,
	}, nil)

	// Tool: run_ncc — run NCC checks on clusters and generate reports
	addAITool(server, &mcp.Tool{
		Name:        "run_ncc",
		Title:       "Run NCC checks",
		Description: "Trigger NCC checks through the authenticated API. Requires confirm=APPLY and NCC_MCP_ALLOW_MUTATIONS. Do not send usernames or passwords.",
		Annotations: &mcp.ToolAnnotations{
			Title:           "Run NCC checks",
			ReadOnlyHint:    false,
			DestructiveHint: boolPtr(false),
			OpenWorldHint:   boolPtr(true), // contacts Prism / clusters
		},
	}, runNCC)

	// Tool: discover_clusters — list clusters from Prism Central
	addAITool(server, &mcp.Tool{
		Name:        "discover_clusters",
		Title:       "Discover clusters from Prism Central",
		Description: "List cluster IPs/hostnames from Prism Central through the API. Requires prism_central_url; credentials stay on the API server.",
		Annotations: &mcp.ToolAnnotations{
			Title:         "Discover clusters from Prism Central",
			ReadOnlyHint:  true,
			OpenWorldHint: boolPtr(true),
		},
	}, discoverClusters)

	// Tool: validate_config — validate a config file without contacting clusters
	addAITool(server, &mcp.Tool{
		Name:        "validate_config",
		Title:       "Validate config file",
		Description: "Validate an ncc-orchestrator configuration file (and its secret:// references) without contacting any cluster. Returns validation errors/warnings. Use this before run_ncc to catch misconfiguration early.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Validate config file",
			ReadOnlyHint:   true,
			IdempotentHint: true,
			OpenWorldHint:  boolPtr(false),
		},
	}, validateConfig)

	// Tool: preflight_check — combined config/secrets/path preflight (JSON)
	addAITool(server, &mcp.Tool{
		Name:        "preflight_check",
		Title:       "Preflight check",
		Description: "Run the combined config/secrets/path preflight checks and return the structured JSON report (per-check id/status/title/message with remediation guidance). Broader than validate_config; run it before scheduling production runs.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Preflight check",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, preflightCheck)

	// Tool: get_run_summary — read last run summary from output directory
	addAITool(server, &mcp.Tool{
		Name:        "get_run_summary",
		Title:       "Read run summary",
		Description: "Read run-summary.json from a previous NCC run (output directory). Includes timestamp, duration, clusters_ok/failed, per-cluster clusters[] (severity counts, errors), exit_code (0/1/3), index_html path.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Read run summary",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, getRunSummary)

	// Tool: replay_reports — regenerate reports from existing logs without calling NCC API
	addAITool(server, &mcp.Tool{
		Name:        "replay_reports",
		Title:       "Replay reports from logs",
		Description: "Replay from existing NCC log files: regenerate HTML/CSV reports and optional notifications without calling the NCC API. Uses config for output paths and options.",
		Annotations: &mcp.ToolAnnotations{
			Title:           "Replay reports from logs",
			ReadOnlyHint:    false,
			DestructiveHint: boolPtr(false),
			OpenWorldHint:   boolPtr(true), // may send notifications
		},
	}, replayReports)

	// Tool: list_run_artifacts — list files in an NCC run output directory
	addAITool(server, &mcp.Tool{
		Name:        "list_run_artifacts",
		Title:       "List run artifacts",
		Description: "List files in an NCC run output directory (run-summary.json, ncc-run-record.json, regression-summary.json, index.html, per-cluster .log/.html/.csv/.sarif). Use to discover what reports exist from a previous run.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "List run artifacts",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, listRunArtifacts)

	// Tool: list_run_history — summarize archived runs
	addAITool(server, &mcp.Tool{
		Name:        "list_run_history",
		Title:       "List run history",
		Description: "List archived NCC runs from the run-history directory with timestamps, status, cluster counts, and report paths.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "List run history",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, listRunHistory)

	// Tool: get_sample_config — read the shipped example configuration
	addAITool(server, &mcp.Tool{
		Name:        "get_sample_config",
		Title:       "Get sample config",
		Description: "Return the shipped example_config.yaml so an AI assistant can help create or explain a configuration.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Get sample config",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, getSampleConfig)

	// Tool: get_metrics — read Prometheus textfile metrics
	addAITool(server, &mcp.Tool{
		Name:        "get_metrics",
		Title:       "Read Prometheus metrics",
		Description: "Read the latest Prometheus textfile metrics generated by NCC from the configured metrics directory.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Read Prometheus metrics",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, getMetrics)

	// Tool: troubleshoot_v2_stack — read-only v2 diagnostics
	addAITool(server, &mcp.Tool{
		Name:        "troubleshoot_v2_stack",
		Title:       "Troubleshoot v2 stack",
		Description: "Run the read-only v2 doctor diagnostics for binaries, paths, services, health probes, environment names, and recent log tails. Does not write a support bundle or apply fixes.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Troubleshoot v2 stack",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, troubleshootV2Stack)

	// Tool: check_upgrade — check release availability without changing files
	addAITool(server, &mcp.Tool{
		Name:        "check_upgrade",
		Title:       "Check for upgrade",
		Description: "Check for a newer NCC Orchestrator release and return the machine-readable result without downloading or replacing anything.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Check for upgrade",
			ReadOnlyHint:   true,
			IdempotentHint: true,
			OpenWorldHint:  boolPtr(true),
		},
	}, checkUpgrade)

	// Tool: preview_install_service — preview OS service installation
	addAITool(server, &mcp.Tool{
		Name:        "preview_install_service",
		Title:       "Preview service installation",
		Description: "Render the v2 boot-persistent service/task for Linux systemd, Windows Task Scheduler, or macOS launchd without applying it.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Preview service installation",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, previewInstallService)

	// Mutating lifecycle tools require an explicit confirmation token. This
	// prevents an assistant from applying a consequential change implicitly.
	addAITool(server, &mcp.Tool{
		Name:        "apply_upgrade",
		Title:       "Apply orchestrator upgrade",
		Description: "Download and install an orchestrator upgrade. Requires confirm='APPLY' and never skips checksum verification unless explicitly requested.",
		Annotations: &mcp.ToolAnnotations{Title: "Apply orchestrator upgrade", DestructiveHint: boolPtr(true)},
	}, applyUpgrade)

	addAITool(server, &mcp.Tool{
		Name:        "bootstrap_v2_stack",
		Title:       "Bootstrap v2 stack",
		Description: "Download and prepare the v2 API/UI stack and startup scripts. Requires confirm='APPLY'.",
		Annotations: &mcp.ToolAnnotations{Title: "Bootstrap v2 stack", DestructiveHint: boolPtr(true)},
	}, bootstrapV2Stack)

	addAITool(server, &mcp.Tool{
		Name:        "install_service",
		Title:       "Install v2 service",
		Description: "Install and optionally start the boot-persistent v2 service. Requires confirm='APPLY'. Use preview_install_service first.",
		Annotations: &mcp.ToolAnnotations{Title: "Install v2 service", DestructiveHint: boolPtr(true)},
	}, installService)

	addAITool(server, &mcp.Tool{
		Name:        "uninstall_service",
		Title:       "Uninstall v2 service",
		Description: "Remove the boot-persistent v2 service. Requires confirm='APPLY'.",
		Annotations: &mcp.ToolAnnotations{Title: "Uninstall v2 service", DestructiveHint: boolPtr(true)},
	}, uninstallService)

	addAITool(server, &mcp.Tool{
		Name:        "start_v2_stack",
		Title:       "Start v2 stack",
		Description: "Start the v2 API/UI stack in detached mode. Requires confirm='APPLY'.",
		Annotations: &mcp.ToolAnnotations{Title: "Start v2 stack", DestructiveHint: boolPtr(true)},
	}, startV2Stack)

	addAITool(server, &mcp.Tool{
		Name:        "stop_v2_stack",
		Title:       "Stop v2 stack",
		Description: "Stop the detached v2 API/UI stack. Requires confirm='APPLY'.",
		Annotations: &mcp.ToolAnnotations{Title: "Stop v2 stack", DestructiveHint: boolPtr(true)},
	}, stopV2Stack)

	addAITool(server, &mcp.Tool{
		Name:        "restart_v2_stack",
		Title:       "Restart v2 stack",
		Description: "Restart the v2 API/UI stack. Requires confirm='APPLY'.",
		Annotations: &mcp.ToolAnnotations{Title: "Restart v2 stack", DestructiveHint: boolPtr(true)},
	}, restartV2Stack)

	addAITool(server, &mcp.Tool{
		Name:        "repair_v2_stack",
		Title:       "Repair v2 stack",
		Description: "Run doctor self-heal remediations without creating a support bundle. Requires confirm='APPLY'; disruptive checks remain disabled.",
		Annotations: &mcp.ToolAnnotations{Title: "Repair v2 stack", DestructiveHint: boolPtr(true)},
	}, repairV2Stack)

	addAITool(server, &mcp.Tool{
		Name:        "restore_v2_backup",
		Title:       "Restore v2 backup",
		Description: "Restore a previously created v2 backup archive. Requires confirm='APPLY'; archive validation is performed by the orchestrator.",
		Annotations: &mcp.ToolAnnotations{Title: "Restore v2 backup", DestructiveHint: boolPtr(true)},
	}, restoreV2Backup)

	addAITool(server, &mcp.Tool{
		Name:        "backup_v2_stack",
		Title:       "Back up v2 stack",
		Description: "Create a v2 configuration/state backup archive. Requires confirm='APPLY'; backups contain secrets and should be stored securely.",
		Annotations: &mcp.ToolAnnotations{Title: "Back up v2 stack", DestructiveHint: boolPtr(false)},
	}, backupV2Stack)

	addAITool(server, &mcp.Tool{
		Name:        "orchestrator_action",
		Title:       "Run supported orchestrator action",
		Description: "Run an allowlisted read-only operator action through the API (status, diagnostics, config, preflight). Host-only actions such as uninstall and password recovery are refused.",
		Annotations: &mcp.ToolAnnotations{Title: "Run supported orchestrator action", DestructiveHint: boolPtr(true)},
	}, orchestratorAction)

	// Tool: get_report — read aggregated or per-cluster report content (HTML/text)
	addAITool(server, &mcp.Tool{
		Name:        "get_report",
		Title:       "Read report file",
		Description: "Read the aggregated index.html or a specific cluster report file from an output directory. For *.log files, KB references (e.g. KB 5582) are expanded to markdown links to portal.nutanix.com. Returns report content for the AI to summarize or analyze.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Read report file",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, getReport)

	addAITool(server, &mcp.Tool{
		Name:        "get_findings",
		Title:       "Get structured findings",
		Description: "Read structured NCC findings from checks-snapshot.json. Filter by run, cluster, check, or severity; use this instead of parsing large HTML reports.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Get structured findings",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, getFindings)

	addAITool(server, &mcp.Tool{
		Name:        "compare_runs",
		Title:       "Compare NCC runs",
		Description: "Compare two run summaries and return failure, warning, health, and status deltas. Use this to distinguish regressions from persistent findings.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Compare NCC runs",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, compareRuns)

	// Tool: create_schedule — create/update scheduler entry
	addAITool(server, &mcp.Tool{
		Name:        "create_schedule",
		Title:       "Create/update schedule",
		Description: "Create or update a periodic schedule through PUT /api/v1/schedule. Custom command lines are never accepted. Mutating calls require confirm=APPLY.",
		Annotations: &mcp.ToolAnnotations{
			Title:           "Create/update schedule",
			ReadOnlyHint:    false,
			DestructiveHint: boolPtr(false),
			IdempotentHint:  true, // create/update keyed by task name
		},
	}, createSchedule)
	// Tool: list_schedules — list scheduler entries for task marker/name
	addAITool(server, &mcp.Tool{
		Name:        "list_schedules",
		Title:       "List schedules",
		Description: "List existing schedule entries for ncc-orchestrator task marker/name.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "List schedules",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, listSchedules)
	// Tool: delete_schedule — remove scheduler entry/task
	addAITool(server, &mcp.Tool{
		Name:        "delete_schedule",
		Title:       "Delete schedule",
		Description: "Remove existing ncc-orchestrator schedule entry/task by task name.",
		Annotations: &mcp.ToolAnnotations{
			Title:           "Delete schedule",
			ReadOnlyHint:    false,
			DestructiveHint: boolPtr(true),
			IdempotentHint:  true, // deleting an absent task is a no-op
		},
	}, deleteSchedule)

	// Resources: latest run-summary and report (from default output dir relative to cwd)
	server.AddResource(&mcp.Resource{
		URI:         "ncc://run-summary",
		Name:        "ncc-run-summary",
		Description: "Latest run-summary.json from the default output directory (outputfiles).",
		MIMEType:    "application/json",
	}, handleRunSummaryResource)
	server.AddResource(&mcp.Resource{
		URI:         "ncc://report",
		Name:        "ncc-report",
		Description: "Latest aggregated NCC report (index.html) from the default output directory (outputfiles).",
		MIMEType:    "text/html",
	}, handleReportResource)
	addAIResources(server)

	// Log to stderr only (stdout is used for MCP JSON-RPC)
	log.SetOutput(os.Stderr)
	if err := mcpAuthReady(); err != nil {
		log.Fatalf("MCP server authentication: %v", err)
	}
	server.AddReceivingMiddleware(requireMCPAuth)
	server.AddReceivingMiddleware(logMCPCalls)
	log.Printf("level=INFO msg=\"ncc-mcp-server starting\" transport=stdio auth=required")
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("MCP server error: %v", err)
	}
}
