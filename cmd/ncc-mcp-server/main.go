// ncc-mcp-server exposes the Nutanix NCC Orchestrator as an MCP server so AI assistants
// (Cursor, Claude, etc.) can run NCC checks, discover clusters, and read run summaries.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
	"goncc/internal/kblinks"
)

const serverName = "ncc-orchestrator"

// serverVersion is the advertised MCP server version. It is overridable at build
// time via -ldflags "-X main.serverVersion=<v>" (the release build stamps the
// orchestrator version here) and otherwise defaults to the current release.
var serverVersion = "3.0.0"

// orchestratorBinaryName is the platform-specific orchestrator executable name.
func orchestratorBinaryName() string {
	if runtime.GOOS == "windows" {
		return "ncc-orchestrator.exe"
	}
	return "ncc-orchestrator"
}

// orchestratorBin returns the path to the ncc-orchestrator binary (env, same-dir, or PATH).
func orchestratorBin() string {
	if b := os.Getenv("NCC_ORCHESTRATOR_BIN"); b != "" {
		return b
	}
	name := orchestratorBinaryName()
	// When MCP server and orchestrator live in the same directory (e.g. project root), use that.
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), name)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return name
}

// boolPtr returns a pointer to b, for the optional *bool hint fields in
// mcp.ToolAnnotations.
func boolPtr(b bool) *bool { return &b }

// runOrchestrator invokes the ncc-orchestrator binary with args and returns its
// combined stdout/stderr plus any execution error.
func runOrchestrator(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, orchestratorBin(), args...)
	cmd.Stdin = nil
	cmd.Env = sanitizedEnvironment()
	var output limitedOutput
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if output.truncated {
		output.buffer.WriteString("\n... [MCP output truncated]\n")
	}
	return output.buffer.String(), err
}

const maxMCPCommandOutput = 1_000_000

type limitedOutput struct {
	buffer    bytes.Buffer
	truncated bool
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	remaining := maxMCPCommandOutput - w.buffer.Len()
	if remaining <= 0 {
		w.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = w.buffer.Write(p[:remaining])
		w.truncated = true
		return len(p), nil
	}
	return w.buffer.Write(p)
}

func sanitizedEnvironment() []string {
	env := os.Environ()
	// Do not allow an MCP request to alter the subprocess command's execution
	// policy through inherited Go/runtime or loader variables.
	filtered := env[:0]
	for _, entry := range env {
		name := entry
		if i := strings.IndexByte(entry, '='); i >= 0 {
			name = entry[:i]
		}
		switch name {
		case "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH", "GODEBUG", "GOTRACEBACK":
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

// safeJoin joins a caller-supplied relative file name onto dir, guaranteeing the
// result stays inside dir. It rejects absolute paths and "../" traversal so a
// report-reading tool cannot be used to read arbitrary files on the host.
func safeJoin(dir, file string) (string, error) {
	base, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	joined := filepath.Join(base, file)
	rel, err := filepath.Rel(base, joined)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes output directory %q", file, dir)
	}
	return joined, nil
}

func main() {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    serverName,
		Version: serverVersion,
	}, nil)

	// Tool: run_ncc — run NCC checks on clusters and generate reports
	addAITool(server, &mcp.Tool{
		Name:        "run_ncc",
		Title:       "Run NCC checks",
		Description: "Run Nutanix NCC (Cluster Check) across one or more clusters. Uses config file and/or cluster list and credentials. Returns run summary (duration, clusters ok/failed, index path).",
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
		Description: "List cluster IPs/hostnames from Prism Central (default v4 API, optional v3). Requires prism_central_url and credentials.",
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
		Description: "Run an allowlisted NCC operator action not covered by a specialized MCP tool, including status, verification, validation, quickstart, update-watcher installation, uninstall, and password recovery. Mutating actions require confirm='APPLY'.",
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
		Description: "Create or update a periodic schedule for ncc-orchestrator. Supports cron (Linux/macOS) and Windows Scheduled Task.",
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
	server.AddReceivingMiddleware(logMCPCalls)
	log.Printf("level=INFO msg=\"ncc-mcp-server starting\" transport=stdio")
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("MCP server error: %v", err)
	}
}

// --- run_ncc ---

type RunNCCInput struct {
	ConfigPath         string `json:"config_path,omitempty" jsonschema:"Path to YAML/JSON config file (clusters, credentials, options)."`
	Clusters           string `json:"clusters,omitempty" jsonschema:"Comma-separated cluster IPs or FQDNs (overrides config if set)."`
	Username           string `json:"username,omitempty" jsonschema:"Prism username (default admin)."`
	Password           string `json:"password,omitempty" jsonschema:"Prism password (prefer env NCC_PASSWORD for security)."`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty" jsonschema:"If true, skip TLS certificate verification (lab/self-signed only)."`
	DryRun             bool   `json:"dry_run,omitempty" jsonschema:"If true, only validate config and do not run checks."`
}

// withPasswordEnv passes a Prism password to the orchestrator through
// NCC_PASSWORD (read via viper's NCC_ env binding). It used to be passed as
// --password as well, which exposes it to every local user through the
// process list (ps, /proc/<pid>/cmdline, Task Manager).
func withPasswordEnv(env []string, password string) []string {
	if password == "" {
		return env
	}
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, "NCC_PASSWORD=") {
			out = append(out, kv)
		}
	}
	return append(out, "NCC_PASSWORD="+password)
}

func runNCC(ctx context.Context, req *mcp.CallToolRequest, input RunNCCInput) (*mcp.CallToolResult, any, error) {
	bin := orchestratorBin()
	args := []string{}
	if input.ConfigPath != "" {
		args = append(args, "--config", input.ConfigPath)
	}
	if input.Clusters != "" {
		args = append(args, "--clusters", strings.TrimSpace(input.Clusters))
	}
	if input.Username != "" {
		args = append(args, "--username", input.Username)
	}
	if input.InsecureSkipVerify {
		args = append(args, "--insecure-skip-verify")
	}
	if input.DryRun {
		args = append(args, "--dry-run")
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = nil
	cmd.Env = withPasswordEnv(sanitizedEnvironment(), input.Password)
	out, err := cmd.CombinedOutput()
	text := string(out)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: " + err.Error() + "\nOutput:\n" + text}},
			IsError: true,
		}, nil, nil
	}
	// Try to append run summary if the default output dir exists
	summaryPath := filepath.Join("outputfiles", "run-summary.json")
	if input.ConfigPath != "" {
		dir := filepath.Dir(input.ConfigPath)
		summaryPath = filepath.Join(dir, "outputfiles", "run-summary.json")
	}
	if b, err := os.ReadFile(summaryPath); err == nil {
		text += "\n\n--- run-summary.json ---\n" + string(b)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}, nil, nil
}

// --- discover_clusters ---

type DiscoverClustersInput struct {
	ConfigPath          string `json:"config_path,omitempty" jsonschema:"Path to config file (may contain prism-central-url, username, password)."`
	PrismCentralURL     string `json:"prism_central_url" jsonschema:"Prism Central URL (e.g. https://10.0.0.1:9440)."`
	Username            string `json:"username,omitempty" jsonschema:"Prism username (default admin)."`
	Password            string `json:"password,omitempty" jsonschema:"Prism password (or set NCC_PASSWORD)."`
	InsecureSkipVerify  bool   `json:"insecure_skip_verify,omitempty" jsonschema:"Skip TLS verification (lab only)."`
	DiscoverAPIVersion  string `json:"discover_api_version,omitempty" jsonschema:"Cluster list API: v4 (default) or v3 (legacy POST)."`
	NutanixV4APIVersion string `json:"nutanix_v4_api_version,omitempty" jsonschema:"Nutanix v4 path revision for clustermgmt/monitoring (default v4.2; e.g. v4.0.a1)."`
	OutputPath          string `json:"output_path,omitempty" jsonschema:"Optional file path to write cluster list (one per line)."`
}

func discoverClusters(ctx context.Context, req *mcp.CallToolRequest, input DiscoverClustersInput) (*mcp.CallToolResult, any, error) {
	if input.PrismCentralURL == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "prism_central_url is required"}},
			IsError: true,
		}, nil, nil
	}

	bin := orchestratorBin()
	args := []string{}
	if input.ConfigPath != "" {
		args = append(args, "--config", input.ConfigPath)
	}
	args = append(args, "discover-clusters", "--prism-central-url", strings.TrimSpace(input.PrismCentralURL))
	if input.Username != "" {
		args = append(args, "--username", input.Username)
	}
	if input.InsecureSkipVerify {
		args = append(args, "--insecure-skip-verify")
	}
	if strings.TrimSpace(input.DiscoverAPIVersion) != "" {
		args = append(args, "--discover-api-version", strings.TrimSpace(input.DiscoverAPIVersion))
	}
	if strings.TrimSpace(input.NutanixV4APIVersion) != "" {
		args = append(args, "--nutanix-v4-api-version", strings.TrimSpace(input.NutanixV4APIVersion))
	}
	if input.OutputPath != "" {
		args = append(args, "--output", input.OutputPath)
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = withPasswordEnv(sanitizedEnvironment(), input.Password)
	out, err := cmd.CombinedOutput()
	text := string(out)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: " + err.Error() + "\nOutput:\n" + text}},
			IsError: true,
		}, nil, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: "Discovered clusters:\n" + text}},
	}, nil, nil
}

// --- validate_config ---

type ValidateConfigInput struct {
	ConfigPath     string `json:"config_path" jsonschema:"Path to the YAML/JSON config file to validate."`
	IncludeSecrets bool   `json:"include_secrets,omitempty" jsonschema:"Also validate secret:// references and secret-source accessibility (validate-secrets)."`
}

func validateConfig(ctx context.Context, req *mcp.CallToolRequest, input ValidateConfigInput) (*mcp.CallToolResult, any, error) {
	if strings.TrimSpace(input.ConfigPath) == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "config_path is required"}},
			IsError: true,
		}, nil, nil
	}
	sub := "validate-config"
	if input.IncludeSecrets {
		sub = "validate-secrets"
	}
	out, err := runOrchestrator(ctx, "--config", strings.TrimSpace(input.ConfigPath), sub)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Validation failed:\n" + out}},
			IsError: true,
		}, nil, nil
	}
	if strings.TrimSpace(out) == "" {
		out = "Configuration is valid."
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: out}},
	}, nil, nil
}

// --- preflight_check ---

type PreflightCheckInput struct {
	ConfigPath string `json:"config_path" jsonschema:"Path to the config file to preflight."`
}

func preflightCheck(ctx context.Context, req *mcp.CallToolRequest, input PreflightCheckInput) (*mcp.CallToolResult, any, error) {
	if strings.TrimSpace(input.ConfigPath) == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "config_path is required"}},
			IsError: true,
		}, nil, nil
	}
	out, err := runOrchestrator(ctx, "--config", strings.TrimSpace(input.ConfigPath), "preflight-check", "--format", "json")
	// preflight-check returns a non-zero exit when checks fail, but still emits a
	// useful JSON report; surface the report either way and only flag IsError when
	// there was no parseable output.
	text := out
	if strings.TrimSpace(text) == "" && err != nil {
		text = "Error: " + err.Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: text}},
			IsError: true,
		}, nil, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
		IsError: err != nil,
	}, nil, nil
}

// --- get_run_summary ---

type GetRunSummaryInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Directory containing run-summary.json (default: outputfiles)."`
}

func getRunSummary(ctx context.Context, req *mcp.CallToolRequest, input GetRunSummaryInput) (*mcp.CallToolResult, any, error) {
	dir := input.OutputDir
	if dir == "" {
		dir = "outputfiles"
	}
	path := filepath.Join(dir, "run-summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Could not read %s: %v", path, err)}},
			IsError: true,
		}, nil, nil
	}
	var summary map[string]interface{}
	if err := json.Unmarshal(data, &summary); err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil, nil
	}
	indent, _ := json.MarshalIndent(summary, "", "  ")
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(indent)}},
	}, nil, nil
}

// --- replay_reports ---

type ReplayReportsInput struct {
	ConfigPath string `json:"config_path" jsonschema:"Path to YAML/JSON config (output dirs, notification settings)."`
}

func replayReports(ctx context.Context, req *mcp.CallToolRequest, input ReplayReportsInput) (*mcp.CallToolResult, any, error) {
	if input.ConfigPath == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "config_path is required for replay"}},
			IsError: true,
		}, nil, nil
	}

	bin := orchestratorBin()
	args := []string{"--config", input.ConfigPath, "--replay"}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = sanitizedEnvironment()
	out, err := cmd.CombinedOutput()
	text := string(out)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: " + err.Error() + "\nOutput:\n" + text}},
			IsError: true,
		}, nil, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}, nil, nil
}

// --- list_run_artifacts ---

type ListRunArtifactsInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Directory containing run artifacts (default: outputfiles)."`
}

func listRunArtifacts(ctx context.Context, req *mcp.CallToolRequest, input ListRunArtifactsInput) (*mcp.CallToolResult, any, error) {
	dir := input.OutputDir
	if dir == "" {
		dir = "outputfiles"
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Directory %q does not exist (no run artifacts yet).", dir)}},
			}, nil, nil
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Failed to read %s: %v", dir, err)}},
			IsError: true,
		}, nil, nil
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := "Run artifacts in " + dir + ":\n"
	for _, n := range names {
		out += "  " + n + "\n"
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: out}},
	}, nil, nil
}

// --- list_run_history ---

type ListRunHistoryInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Parent output directory containing runs/ (default: outputfiles)."`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum number of runs to return, newest first (default 20, maximum 100)."`
}

func listRunHistory(ctx context.Context, req *mcp.CallToolRequest, input ListRunHistoryInput) (*mcp.CallToolResult, any, error) {
	dir := input.OutputDir
	if dir == "" {
		dir = defaultOutputDir
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	historyDir, err := safeJoin(dir, "runs")
	if err != nil {
		return mcpTextError(err.Error())
	}
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "No archived run history found."}}}, nil, nil
		}
		return mcpTextError(fmt.Sprintf("failed to read %s: %v", historyDir, err))
	}
	type historyEntry struct {
		ID             string  `json:"id"`
		Timestamp      string  `json:"timestamp,omitempty"`
		DurationS      float64 `json:"duration_s,omitempty"`
		ClustersOK     int     `json:"clusters_ok,omitempty"`
		ClustersFailed int     `json:"clusters_failed,omitempty"`
		ExitCode       int     `json:"exit_code,omitempty"`
		Path           string  `json:"path"`
	}
	items := make([]historyEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		summaryPath, joinErr := safeJoin(historyDir, filepath.Join(entry.Name(), "run-summary.json"))
		if joinErr != nil {
			continue
		}
		data, readErr := os.ReadFile(summaryPath)
		if readErr != nil {
			continue
		}
		var summary struct {
			Timestamp      string  `json:"timestamp"`
			DurationS      float64 `json:"duration_s"`
			ClustersOK     int     `json:"clusters_ok"`
			ClustersFailed int     `json:"clusters_failed"`
			ExitCode       int     `json:"exit_code"`
		}
		if json.Unmarshal(data, &summary) != nil {
			continue
		}
		items = append(items, historyEntry{
			ID: entry.Name(), Timestamp: summary.Timestamp, DurationS: summary.DurationS,
			ClustersOK: summary.ClustersOK, ClustersFailed: summary.ClustersFailed,
			ExitCode: summary.ExitCode, Path: filepath.Join("runs", entry.Name()),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Timestamp == items[j].Timestamp {
			return items[i].ID > items[j].ID
		}
		return items[i].Timestamp > items[j].Timestamp
	})
	if len(items) > limit {
		items = items[:limit]
	}
	body, _ := json.MarshalIndent(items, "", "  ")
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}, nil, nil
}

// --- get_sample_config ---

func getSampleConfig(ctx context.Context, req *mcp.CallToolRequest, input struct{}) (*mcp.CallToolResult, any, error) {
	candidates := []string{"examples/example_config.yaml"}
	if exe, err := os.Executable(); err == nil {
		root := filepath.Dir(exe)
		candidates = append(candidates, filepath.Join(root, "examples", "example_config.yaml"))
		candidates = append(candidates, filepath.Join(root, "..", "examples", "example_config.yaml"))
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err == nil {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}}, nil, nil
		}
	}
	return mcpTextError("examples/example_config.yaml was not found")
}

// --- get_metrics ---

type GetMetricsInput struct {
	PromDir  string `json:"prom_dir,omitempty" jsonschema:"Prometheus textfile directory (default: promfiles)."`
	MaxBytes int    `json:"max_bytes,omitempty" jsonschema:"Maximum response size (default 200000, maximum 1000000)."`
}

func getMetrics(ctx context.Context, req *mcp.CallToolRequest, input GetMetricsInput) (*mcp.CallToolResult, any, error) {
	dir := input.PromDir
	if dir == "" {
		dir = "promfiles"
	}
	maxBytes := input.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 200000
	}
	if maxBytes > 1000000 {
		maxBytes = 1000000
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return mcpTextError(fmt.Sprintf("failed to read metrics directory %s: %v", dir, err))
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".prom") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		path, joinErr := safeJoin(dir, name)
		if joinErr != nil {
			continue
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		if b.Len()+len(data)+1 > maxBytes {
			b.WriteString("\n... [truncated for length]\n")
			break
		}
		b.Write(data)
		if !strings.HasSuffix(string(data), "\n") {
			b.WriteByte('\n')
		}
	}
	if b.Len() == 0 {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "No Prometheus metrics found."}}}, nil, nil
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: b.String()}}}, nil, nil
}

func mcpTextError(message string) (*mcp.CallToolResult, any, error) {
	errEnvelope := ai.NewError(ai.CodeInvalidArgument, "validation", message)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
		IsError: true,
	}, errEnvelope, nil
}

// --- get_report ---

type GetReportInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Output directory (default: outputfiles)."`
	File      string `json:"file,omitempty" jsonschema:"File to read: 'index' for index.html, or a filename e.g. 10.0.0.1.log or 10.0.0.1.html."`
}

func getReport(ctx context.Context, req *mcp.CallToolRequest, input GetReportInput) (*mcp.CallToolResult, any, error) {
	dir := input.OutputDir
	if dir == "" {
		dir = "outputfiles"
	}
	file := input.File
	if file == "" || strings.ToLower(file) == "index" {
		file = "index.html"
	}
	path, err := safeJoin(dir, file)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
			IsError: true,
		}, nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("File %s not found.", path)}},
				IsError: true,
			}, nil, nil
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Failed to read %s: %v", path, err)}},
			IsError: true,
		}, nil, nil
	}
	text := string(data)
	// Truncate very large HTML for context window; still useful for summaries
	const maxLen = 150000
	if len(text) > maxLen {
		text = text[:maxLen] + "\n\n... [truncated for length]"
	}
	// Markdown KB links for Cursor/IDE on filtered NCC text logs
	if strings.HasSuffix(strings.ToLower(file), ".log") {
		text = kblinks.AnnotateMarkdown(text)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}, nil, nil
}

type GetFindingsInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Output directory (default: outputfiles)."`
	RunID     string `json:"run_id,omitempty" jsonschema:"Archived run ID under outputfiles/runs. Empty reads the latest canonical run."`
	Cluster   string `json:"cluster,omitempty" jsonschema:"Case-insensitive cluster address or name filter."`
	Check     string `json:"check,omitempty" jsonschema:"Case-insensitive check-name substring filter."`
	Severity  string `json:"severity,omitempty" jsonschema:"Severity filter: FAIL, WARN, ERR, or INFO."`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum findings to return (default 500, maximum 5000)."`
}

func getFindings(ctx context.Context, req *mcp.CallToolRequest, input GetFindingsInput) (*mcp.CallToolResult, any, error) {
	dir := strings.TrimSpace(input.OutputDir)
	if dir == "" {
		dir = defaultOutputDir
	}
	if input.RunID != "" {
		var err error
		dir, err = safeJoin(filepath.Join(dir, "runs"), input.RunID)
		if err != nil {
			return mcpInvalidArgument("run_id", err.Error())
		}
	}
	limit := input.Limit
	if limit == 0 {
		limit = 500
	}
	if limit < 1 || limit > 5000 {
		return mcpInvalidArgument("limit", "limit must be between 1 and 5000")
	}
	severity := strings.ToUpper(strings.TrimSpace(input.Severity))
	if severity != "" && severity != "FAIL" && severity != "WARN" && severity != "ERR" && severity != "INFO" {
		return mcpInvalidArgument("severity", "severity must be one of FAIL, WARN, ERR, INFO")
	}
	data, err := os.ReadFile(filepath.Join(dir, "checks-snapshot.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return mcpContractError(ai.CodeNotFound, "report", "checks-snapshot.json was not found", false)
		}
		return mcpContractError(ai.CodeInternal, "report", err.Error(), false)
	}
	var snapshot struct {
		Timestamp string `json:"timestamp"`
		Clusters  []struct {
			Address string `json:"address"`
			Checks  []struct {
				FindingID string `json:"finding_id"`
				CheckName string `json:"check_name"`
				Severity  string `json:"severity"`
				Detail    string `json:"detail"`
			} `json:"checks"`
		} `json:"clusters"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return mcpContractError(ai.CodeInternal, "report", "invalid checks-snapshot.json: "+err.Error(), false)
	}
	findings := make([]ai.Finding, 0)
	for _, cluster := range snapshot.Clusters {
		for _, check := range cluster.Checks {
			if input.Cluster != "" && !strings.Contains(strings.ToLower(cluster.Address), strings.ToLower(input.Cluster)) {
				continue
			}
			if input.Check != "" && !strings.Contains(strings.ToLower(check.CheckName), strings.ToLower(input.Check)) {
				continue
			}
			if severity != "" && strings.ToUpper(check.Severity) != severity {
				continue
			}
			id := check.FindingID
			if id == "" {
				id = "finding:" + strings.ToLower(cluster.Address) + ":" + strings.ToLower(check.CheckName)
			}
			findings = append(findings, ai.Finding{
				ID: id, RunID: input.RunID, Cluster: cluster.Address,
				Check: check.CheckName, Severity: strings.ToUpper(check.Severity),
				Detail: check.Detail, ReportRef: filepath.Join(dir, cluster.Address+".log"),
			})
			if len(findings) >= limit {
				break
			}
		}
		if len(findings) >= limit {
			break
		}
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: mustJSON(map[string]any{
			"timestamp": snapshot.Timestamp, "findings": findings, "count": len(findings),
		})}},
	}, nil, nil
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type CompareRunsInput struct {
	OutputDir   string `json:"output_dir,omitempty" jsonschema:"Output directory (default: outputfiles)."`
	CurrentRun  string `json:"current_run_id" jsonschema:"Current archived run ID under outputfiles/runs."`
	BaselineRun string `json:"baseline_run_id" jsonschema:"Baseline archived run ID under outputfiles/runs."`
}

func compareRuns(ctx context.Context, req *mcp.CallToolRequest, input CompareRunsInput) (*mcp.CallToolResult, any, error) {
	dir := strings.TrimSpace(input.OutputDir)
	if dir == "" {
		dir = defaultOutputDir
	}
	if strings.TrimSpace(input.CurrentRun) == "" || strings.TrimSpace(input.BaselineRun) == "" {
		return mcpInvalidArgument("current_run_id", "current_run_id and baseline_run_id are required")
	}
	current, err := readArchivedSummary(dir, input.CurrentRun)
	if err != nil {
		return mcpContractError(ai.CodeNotFound, "report", err.Error(), false)
	}
	baseline, err := readArchivedSummary(dir, input.BaselineRun)
	if err != nil {
		return mcpContractError(ai.CodeNotFound, "report", err.Error(), false)
	}
	metric := func(m map[string]any, key string) float64 {
		switch v := m[key].(type) {
		case float64:
			return v
		case int:
			return float64(v)
		default:
			return 0
		}
	}
	deltas := map[string]any{}
	for _, key := range []string{"clusters_failed", "total_checks", "avg_health_score", "min_health_score"} {
		deltas[key] = metric(current, key) - metric(baseline, key)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: mustJSON(map[string]any{
			"current_run_id": input.CurrentRun, "baseline_run_id": input.BaselineRun,
			"current": current, "baseline": baseline, "deltas": deltas,
		})}},
	}, nil, nil
}

func readArchivedSummary(outputDir, runID string) (map[string]any, error) {
	dir, err := safeJoin(filepath.Join(outputDir, "runs"), runID)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, "run-summary.json"))
	if err != nil {
		return nil, fmt.Errorf("run %q summary not found", runID)
	}
	var summary map[string]any
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, fmt.Errorf("run %q summary is invalid: %v", runID, err)
	}
	return summary, nil
}

// --- v2 troubleshooting and lifecycle tools ---

type TroubleshootV2StackInput struct {
	InstallDir   string `json:"install_dir,omitempty" jsonschema:"v2 installation directory (default: auto-detect or .ncc-v2)."`
	ConfigPath   string `json:"config_path,omitempty" jsonschema:"Optional config file used by self-heal checks."`
	APIListen    string `json:"api_listen,omitempty" jsonschema:"API listen address used for the health probe (default: :8081)."`
	UIListen     string `json:"ui_listen,omitempty" jsonschema:"UI listen address shown in diagnostics (default: :8080)."`
	OnlyChecks   string `json:"only_checks,omitempty" jsonschema:"Optional comma-separated self-heal check IDs to run."`
	NoDisruptive bool   `json:"no_disruptive,omitempty" jsonschema:"Skip disruptive checks; always true unless explicitly disabled."`
}

func troubleshootV2Stack(ctx context.Context, req *mcp.CallToolRequest, input TroubleshootV2StackInput) (*mcp.CallToolResult, any, error) {
	args := []string{"doctor", "--no-bundle", "--json", "--no-disruptive"}
	if strings.TrimSpace(input.InstallDir) != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	if strings.TrimSpace(input.ConfigPath) != "" {
		args = append(args, "--config", strings.TrimSpace(input.ConfigPath))
	}
	if strings.TrimSpace(input.APIListen) != "" {
		args = append(args, "--api-listen", strings.TrimSpace(input.APIListen))
	}
	if strings.TrimSpace(input.UIListen) != "" {
		args = append(args, "--ui-listen", strings.TrimSpace(input.UIListen))
	}
	if strings.TrimSpace(input.OnlyChecks) != "" {
		args = append(args, "--only-checks", strings.TrimSpace(input.OnlyChecks))
	}
	return runMCPOrchestrator(ctx, args...)
}

type CheckUpgradeInput struct {
	Repo          string `json:"repo,omitempty" jsonschema:"GitHub owner/repo or URL (default: project repository)."`
	BinaryURL     string `json:"binary_url,omitempty" jsonschema:"Optional release binary URL to inspect."`
	TargetVersion string `json:"target_version,omitempty" jsonschema:"Optional target version hint."`
	AllowMajor    bool   `json:"allow_major_upgrade,omitempty" jsonschema:"Allow checking across major release tracks."`
}

func checkUpgrade(ctx context.Context, req *mcp.CallToolRequest, input CheckUpgradeInput) (*mcp.CallToolResult, any, error) {
	args := []string{"update", "--check", "--json"}
	if strings.TrimSpace(input.Repo) != "" {
		args = append(args, "--repo", strings.TrimSpace(input.Repo))
	}
	if strings.TrimSpace(input.BinaryURL) != "" {
		args = append(args, "--binary-url", strings.TrimSpace(input.BinaryURL))
	}
	if strings.TrimSpace(input.TargetVersion) != "" {
		args = append(args, "--target-version", strings.TrimSpace(input.TargetVersion))
	}
	if input.AllowMajor {
		args = append(args, "--allow-major-upgrade")
	}
	return runMCPOrchestrator(ctx, args...)
}

type PreviewInstallServiceInput struct {
	InstallDir      string `json:"install_dir,omitempty" jsonschema:"v2 installation directory (default: auto-detect or .ncc-v2)."`
	ServiceName     string `json:"service_name,omitempty" jsonschema:"Service/task name (default: ncc-orchestrator)."`
	OrchestratorBin string `json:"orchestrator_bin,omitempty" jsonschema:"Path to the orchestrator binary used by the service."`
}

func previewInstallService(ctx context.Context, req *mcp.CallToolRequest, input PreviewInstallServiceInput) (*mcp.CallToolResult, any, error) {
	args := []string{"v2-install-service", "--print-only=true", "--now=false"}
	if strings.TrimSpace(input.InstallDir) != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	if strings.TrimSpace(input.ServiceName) != "" {
		args = append(args, "--service-name", strings.TrimSpace(input.ServiceName))
	}
	if strings.TrimSpace(input.OrchestratorBin) != "" {
		args = append(args, "--orchestrator-bin", strings.TrimSpace(input.OrchestratorBin))
	}
	return runMCPOrchestrator(ctx, args...)
}

func runMCPOrchestrator(ctx context.Context, args ...string) (*mcp.CallToolResult, any, error) {
	out, err := runOrchestrator(ctx, args...)
	if err != nil {
		text := strings.TrimSpace(out)
		if text == "" {
			text = err.Error()
		} else {
			text = "Error: " + err.Error() + "\n" + text
		}
		errEnvelope := contractErrorFromProcess(err, out)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: text}},
			IsError: true,
		}, errEnvelope, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: out}},
	}, nil, nil
}

func requireMCPConfirmation(confirm string) (*mcp.CallToolResult, any, error) {
	if !mcpMutationsAllowed() {
		return mcpTextError("mutating MCP tools are disabled; set NCC_MCP_ALLOW_MUTATIONS=true in the MCP server environment")
	}
	if strings.TrimSpace(confirm) != "APPLY" {
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

type ApplyUpgradeInput struct {
	Confirm            string `json:"confirm" jsonschema:"Required exact value APPLY."`
	Repo               string `json:"repo,omitempty"`
	BinaryURL          string `json:"binary_url,omitempty"`
	BinarySHA256       string `json:"binary_sha256,omitempty"`
	TargetVersion      string `json:"target_version,omitempty"`
	AllowMajor         bool   `json:"allow_major_upgrade,omitempty"`
	SkipChecksumVerify bool   `json:"skip_checksum_verify,omitempty" jsonschema:"Unsafe; avoid unless operating from a trusted air-gapped mirror."`
}

func applyUpgrade(ctx context.Context, req *mcp.CallToolRequest, input ApplyUpgradeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"update", "--json"}
	if input.Repo != "" {
		args = append(args, "--repo", strings.TrimSpace(input.Repo))
	}
	if input.BinaryURL != "" {
		args = append(args, "--binary-url", strings.TrimSpace(input.BinaryURL))
	}
	if input.BinarySHA256 != "" {
		args = append(args, "--binary-sha256", strings.TrimSpace(input.BinarySHA256))
	}
	if input.TargetVersion != "" {
		args = append(args, "--target-version", strings.TrimSpace(input.TargetVersion))
	}
	if input.AllowMajor {
		args = append(args, "--allow-major-upgrade")
	}
	if input.SkipChecksumVerify {
		args = append(args, "--skip-checksum-verify")
	}
	return runMCPOrchestrator(ctx, args...)
}

type BootstrapV2StackInput struct {
	Confirm            string `json:"confirm" jsonschema:"Required exact value APPLY."`
	Repo               string `json:"repo,omitempty"`
	Version            string `json:"version,omitempty"`
	InstallDir         string `json:"install_dir,omitempty"`
	ConfigPath         string `json:"config_path,omitempty"`
	OutputDir          string `json:"output_dir,omitempty"`
	LogDir             string `json:"log_dir,omitempty"`
	OrchestratorBin    string `json:"orchestrator_bin,omitempty"`
	APIListen          string `json:"api_listen,omitempty"`
	UIListen           string `json:"ui_listen,omitempty"`
	TokenFile          string `json:"token_file,omitempty"`
	SkipChecksumVerify bool   `json:"skip_checksum_verify,omitempty"`
}

func bootstrapV2Stack(ctx context.Context, req *mcp.CallToolRequest, input BootstrapV2StackInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-bootstrap"}
	appendFlag := func(flag, value string) {
		if strings.TrimSpace(value) != "" {
			args = append(args, flag, strings.TrimSpace(value))
		}
	}
	appendFlag("--repo", input.Repo)
	appendFlag("--version", input.Version)
	appendFlag("--install-dir", input.InstallDir)
	appendFlag("--config-path", input.ConfigPath)
	appendFlag("--output-dir", input.OutputDir)
	appendFlag("--log-dir", input.LogDir)
	appendFlag("--orchestrator-bin", input.OrchestratorBin)
	appendFlag("--api-listen", input.APIListen)
	appendFlag("--ui-listen", input.UIListen)
	appendFlag("--token-file", input.TokenFile)
	if input.SkipChecksumVerify {
		args = append(args, "--skip-checksum-verify")
	}
	return runMCPOrchestrator(ctx, args...)
}

type ServiceChangeInput struct {
	Confirm         string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir      string `json:"install_dir,omitempty"`
	ServiceName     string `json:"service_name,omitempty"`
	OrchestratorBin string `json:"orchestrator_bin,omitempty"`
	Now             *bool  `json:"now,omitempty" jsonschema:"Whether to enable/start immediately for install_service (default true)."`
}

func installService(ctx context.Context, req *mcp.CallToolRequest, input ServiceChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-install-service", "--print-only=false"}
	appendServiceFlags := func() {
		if input.InstallDir != "" {
			args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
		}
		if input.ServiceName != "" {
			args = append(args, "--service-name", strings.TrimSpace(input.ServiceName))
		}
		if input.OrchestratorBin != "" {
			args = append(args, "--orchestrator-bin", strings.TrimSpace(input.OrchestratorBin))
		}
	}
	appendServiceFlags()
	if input.Now != nil {
		args = append(args, "--now="+strconv.FormatBool(*input.Now))
	}
	return runMCPOrchestrator(ctx, args...)
}

func uninstallService(ctx context.Context, req *mcp.CallToolRequest, input ServiceChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-uninstall-service", "--print-only=false"}
	if input.InstallDir != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	if input.ServiceName != "" {
		args = append(args, "--service-name", strings.TrimSpace(input.ServiceName))
	}
	return runMCPOrchestrator(ctx, args...)
}

type StackChangeInput struct {
	Confirm    string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir string `json:"install_dir,omitempty"`
	Force      bool   `json:"force,omitempty" jsonschema:"For stop only, use a hard kill when graceful stop times out."`
}

func startV2Stack(ctx context.Context, req *mcp.CallToolRequest, input StackChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-start", "--detach=true"}
	if input.InstallDir != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	return runMCPOrchestrator(ctx, args...)
}

func stopV2Stack(ctx context.Context, req *mcp.CallToolRequest, input StackChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-stop"}
	if input.InstallDir != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	if input.Force {
		args = append(args, "--force")
	}
	return runMCPOrchestrator(ctx, args...)
}

func restartV2Stack(ctx context.Context, req *mcp.CallToolRequest, input StackChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-restart"}
	if input.InstallDir != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	return runMCPOrchestrator(ctx, args...)
}

type RepairV2StackInput struct {
	Confirm    string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir string `json:"install_dir,omitempty"`
	ConfigPath string `json:"config_path,omitempty"`
	OnlyChecks string `json:"only_checks,omitempty"`
}

func repairV2Stack(ctx context.Context, req *mcp.CallToolRequest, input RepairV2StackInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"doctor", "--fix", "--no-bundle", "--json", "--no-disruptive"}
	if input.InstallDir != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	if input.ConfigPath != "" {
		args = append(args, "--config", strings.TrimSpace(input.ConfigPath))
	}
	if input.OnlyChecks != "" {
		args = append(args, "--only-checks", strings.TrimSpace(input.OnlyChecks))
	}
	return runMCPOrchestrator(ctx, args...)
}

type RestoreV2BackupInput struct {
	Confirm    string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir string `json:"install_dir,omitempty"`
	InputFile  string `json:"input_file"`
	KeyFile    string `json:"key_file,omitempty"`
	Force      bool   `json:"force,omitempty"`
	Restart    bool   `json:"restart,omitempty"`
	NoRestart  bool   `json:"no_restart,omitempty"`
}

func restoreV2Backup(ctx context.Context, req *mcp.CallToolRequest, input RestoreV2BackupInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	if strings.TrimSpace(input.InputFile) == "" {
		return mcpTextError("input_file is required")
	}
	args := []string{"v2-restore", "--input-file", strings.TrimSpace(input.InputFile)}
	if input.InstallDir != "" {
		args = append(args, "--install-dir", strings.TrimSpace(input.InstallDir))
	}
	if input.KeyFile != "" {
		args = append(args, "--key-file", strings.TrimSpace(input.KeyFile))
	}
	if input.Force {
		args = append(args, "--force")
	}
	if input.Restart {
		args = append(args, "--restart")
	}
	if input.NoRestart {
		args = append(args, "--no-restart")
	}
	return runMCPOrchestrator(ctx, args...)
}

type BackupV2StackInput struct {
	Confirm      string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir   string `json:"install_dir,omitempty"`
	OutputFile   string `json:"output_file,omitempty"`
	OutputDir    string `json:"output_dir,omitempty"`
	Retain       int    `json:"retain,omitempty"`
	IncludeStack bool   `json:"include_stack,omitempty"`
	Encrypt      bool   `json:"encrypt,omitempty" jsonschema:"Use only with key_file or an environment-provided encryption key."`
	KeyFile      string `json:"key_file,omitempty"`
}

func backupV2Stack(ctx context.Context, req *mcp.CallToolRequest, input BackupV2StackInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	args := []string{"v2-backup"}
	appendFlag := func(flag, value string) {
		if strings.TrimSpace(value) != "" {
			args = append(args, flag, strings.TrimSpace(value))
		}
	}
	appendFlag("--install-dir", input.InstallDir)
	appendFlag("--output-file", input.OutputFile)
	appendFlag("--output-dir", input.OutputDir)
	appendFlag("--key-file", input.KeyFile)
	if input.Retain > 0 {
		args = append(args, "--retain", strconv.Itoa(input.Retain))
	}
	if input.IncludeStack {
		args = append(args, "--include-stack")
	}
	if input.Encrypt {
		args = append(args, "--encrypt")
	}
	return runMCPOrchestrator(ctx, args...)
}

type OrchestratorActionInput struct {
	Action           string `json:"action" jsonschema:"Allowlisted action: terms, env-info, version, verify, v2-status, v2-check, config-schema, validate-config, validate-secrets, preflight-check, gen-test-agg, install-update-watcher, quickstart, uninstall, or reset-password."`
	Confirm          string `json:"confirm,omitempty" jsonschema:"Required exact value APPLY for mutating actions."`
	ConfigPath       string `json:"config_path,omitempty"`
	InstallDir       string `json:"install_dir,omitempty"`
	OutputDir        string `json:"output_dir,omitempty"`
	OutputPath       string `json:"output_path,omitempty"`
	Repo             string `json:"repo,omitempty"`
	Release          string `json:"release,omitempty"`
	User             string `json:"user,omitempty"`
	UsersDB          string `json:"users_db,omitempty"`
	UsersDBSecret    string `json:"users_db_secret,omitempty"`
	UsersDBNamespace string `json:"users_db_secret_namespace,omitempty"`
	Clusters         int    `json:"clusters,omitempty"`
	Online           bool   `json:"online,omitempty"`
	RequireSignature bool   `json:"require_signature,omitempty"`
	AutoFix          bool   `json:"auto_fix,omitempty"`
	AssumeYes        bool   `json:"assume_yes,omitempty"`
	DryRun           bool   `json:"dry_run,omitempty"`
	Force            bool   `json:"force,omitempty"`
	IncludeV2        bool   `json:"include_v2,omitempty"`
	RemoveSchedule   *bool  `json:"remove_schedule,omitempty"`
	RemoveRuntime    *bool  `json:"remove_v2_runtime,omitempty"`
}

func orchestratorAction(ctx context.Context, req *mcp.CallToolRequest, input OrchestratorActionInput) (*mcp.CallToolResult, any, error) {
	action := strings.ToLower(strings.TrimSpace(input.Action))
	mutating := map[string]bool{
		"install-update-watcher": true, "quickstart": true, "uninstall": true, "reset-password": true, "gen-test-agg": true,
	}
	allowed := map[string]bool{
		"terms": true, "env-info": true, "version": true, "verify": true, "v2-status": true,
		"v2-check": true, "config-schema": true, "validate-config": true, "validate-secrets": true,
		"preflight-check": true, "gen-test-agg": true, "install-update-watcher": true,
		"quickstart": true, "uninstall": true, "reset-password": true,
	}
	if !allowed[action] {
		return mcpTextError("unsupported action; use the documented allowlist")
	}
	if mutating[action] {
		if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
			return result, nil, nil
		}
	}
	args := []string{action}
	appendFlag := func(flag, value string) {
		if strings.TrimSpace(value) != "" {
			args = append(args, flag, strings.TrimSpace(value))
		}
	}
	switch action {
	case "v2-status":
		appendFlag("--install-dir", input.InstallDir)
		args = append(args, "--json")
	case "v2-check":
		appendFlag("--install-dir", input.InstallDir)
		appendFlag("--config-path", input.ConfigPath)
		appendFlag("--output-dir", input.OutputDir)
	case "config-schema":
		appendFlag("--output", input.OutputPath)
	case "validate-config", "validate-secrets", "preflight-check":
		if strings.TrimSpace(input.ConfigPath) == "" {
			return mcpTextError("config_path is required for " + action)
		}
		args = []string{"--config", strings.TrimSpace(input.ConfigPath), action}
		if action == "preflight-check" {
			args = append(args, "--format", "json")
		}
	case "verify":
		if input.Online {
			args = append(args, "--online")
		}
		appendFlag("--release", input.Release)
		appendFlag("--repo", input.Repo)
		if input.RequireSignature {
			args = append(args, "--require-signature")
		}
	case "gen-test-agg":
		if input.Clusters <= 0 {
			return mcpTextError("clusters must be greater than zero for gen-test-agg")
		}
		args = append(args, "--clusters", strconv.Itoa(input.Clusters))
		appendFlag("--output-dir", input.OutputDir)
	case "install-update-watcher":
		args = append(args, "--print-only=false")
		appendFlag("--install-dir", input.InstallDir)
	case "quickstart":
		appendFlag("--install-dir", input.InstallDir)
		appendFlag("--repo", input.Repo)
		if input.AutoFix {
			args = append(args, "--auto-fix=true")
		}
		if input.AssumeYes {
			args = append(args, "--assume-yes=true")
		}
	case "uninstall":
		appendFlag("--install-dir", input.InstallDir)
		args = append(args, "--dry-run="+strconv.FormatBool(input.DryRun))
		if input.Force {
			args = append(args, "--force")
		}
		if input.RemoveSchedule != nil {
			args = append(args, "--remove-schedule="+strconv.FormatBool(*input.RemoveSchedule))
		}
		if input.RemoveRuntime != nil {
			args = append(args, "--remove-v2-runtime="+strconv.FormatBool(*input.RemoveRuntime))
		}
	case "reset-password":
		appendFlag("--install-dir", input.InstallDir)
		appendFlag("--user", input.User)
		appendFlag("--users-db", input.UsersDB)
		appendFlag("--users-db-secret", input.UsersDBSecret)
		appendFlag("--users-db-secret-namespace", input.UsersDBNamespace)
	}
	return runMCPOrchestrator(ctx, args...)
}

// --- schedule tools ---

type CreateScheduleInput struct {
	Type      string `json:"type,omitempty" jsonschema:"Scheduler type: auto, cron, windows"`
	TaskName  string `json:"task_name,omitempty" jsonschema:"Schedule/task name (default ncc-orchestrator)"`
	Config    string `json:"config,omitempty" jsonschema:"Config file path passed to orchestrator --config"`
	Command   string `json:"command,omitempty" jsonschema:"Custom command for scheduler (refused unless the MCP server runs with NCC_MCP_ALLOW_SCHEDULE_COMMAND=true)"`
	Cron      string `json:"cron,omitempty" jsonschema:"Cron expression for cron scheduler"`
	Every     string `json:"every,omitempty" jsonschema:"Interval duration like 30m, 4h"`
	LogPath   string `json:"log_path,omitempty" jsonschema:"Log path for cron redirection"`
	PrintOnly bool   `json:"print_only,omitempty" jsonschema:"Preview only; do not apply changes"`
}

// allowScheduleCommandEnv opts in to create_schedule's free-form "command".
// That field installs an arbitrary command line as a recurring cron job or
// Windows scheduled task, i.e. persistent code execution chosen by the model;
// text an assistant reads (a report, a ticket) could steer it there. The
// orchestrator's own run command needs no override, so it is off by default.
const allowScheduleCommandEnv = "NCC_MCP_ALLOW_SCHEDULE_COMMAND"

func scheduleCommandAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(allowScheduleCommandEnv))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func createSchedule(ctx context.Context, req *mcp.CallToolRequest, input CreateScheduleInput) (*mcp.CallToolResult, any, error) {
	if !input.PrintOnly {
		if result, _, _ := requireMCPConfirmation("APPLY"); result != nil {
			return result, nil, nil
		}
	}
	if strings.TrimSpace(input.Command) != "" && !scheduleCommandAllowed() {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "create_schedule: a custom \"command\" is disabled for MCP clients. Omit it to schedule the standard ncc-orchestrator run, or start the MCP server with " + allowScheduleCommandEnv + "=true to allow it."}},
			IsError: true,
		}, nil, nil
	}
	bin := orchestratorBin()
	args := []string{"create-schedule", "--action", "create"}
	if strings.TrimSpace(input.Type) != "" {
		args = append(args, "--type", strings.TrimSpace(input.Type))
	}
	if strings.TrimSpace(input.TaskName) != "" {
		args = append(args, "--task-name", strings.TrimSpace(input.TaskName))
	}
	if strings.TrimSpace(input.Config) != "" {
		args = append(args, "--config", strings.TrimSpace(input.Config))
	}
	if strings.TrimSpace(input.Command) != "" {
		args = append(args, "--command", input.Command)
	}
	if strings.TrimSpace(input.Cron) != "" {
		args = append(args, "--cron", strings.TrimSpace(input.Cron))
	}
	if strings.TrimSpace(input.Every) != "" {
		args = append(args, "--every", strings.TrimSpace(input.Every))
	}
	if strings.TrimSpace(input.LogPath) != "" {
		args = append(args, "--log-path", strings.TrimSpace(input.LogPath))
	}
	if input.PrintOnly {
		args = append(args, "--print-only=true")
	} else {
		args = append(args, "--print-only=false")
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = sanitizedEnvironment()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: " + err.Error() + "\nOutput:\n" + string(out)}},
			IsError: true,
		}, nil, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(out)}},
	}, nil, nil
}

type ListSchedulesInput struct {
	Type     string `json:"type,omitempty" jsonschema:"Scheduler type: auto, cron, windows"`
	TaskName string `json:"task_name,omitempty" jsonschema:"Schedule/task name (default ncc-orchestrator)"`
}

func listSchedules(ctx context.Context, req *mcp.CallToolRequest, input ListSchedulesInput) (*mcp.CallToolResult, any, error) {
	bin := orchestratorBin()
	args := []string{"create-schedule", "--action", "list"}
	if strings.TrimSpace(input.Type) != "" {
		args = append(args, "--type", strings.TrimSpace(input.Type))
	}
	if strings.TrimSpace(input.TaskName) != "" {
		args = append(args, "--task-name", strings.TrimSpace(input.TaskName))
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = sanitizedEnvironment()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: " + err.Error() + "\nOutput:\n" + string(out)}},
			IsError: true,
		}, nil, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(out)}},
	}, nil, nil
}

type DeleteScheduleInput struct {
	Type      string `json:"type,omitempty" jsonschema:"Scheduler type: auto, cron, windows"`
	TaskName  string `json:"task_name,omitempty" jsonschema:"Schedule/task name (default ncc-orchestrator)"`
	PrintOnly bool   `json:"print_only,omitempty" jsonschema:"Preview removal only"`
}

func deleteSchedule(ctx context.Context, req *mcp.CallToolRequest, input DeleteScheduleInput) (*mcp.CallToolResult, any, error) {
	if !input.PrintOnly {
		if result, _, _ := requireMCPConfirmation("APPLY"); result != nil {
			return result, nil, nil
		}
	}
	bin := orchestratorBin()
	args := []string{"create-schedule", "--action", "remove"}
	if strings.TrimSpace(input.Type) != "" {
		args = append(args, "--type", strings.TrimSpace(input.Type))
	}
	if strings.TrimSpace(input.TaskName) != "" {
		args = append(args, "--task-name", strings.TrimSpace(input.TaskName))
	}
	if input.PrintOnly {
		args = append(args, "--print-only=true")
	} else {
		args = append(args, "--print-only=false")
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = sanitizedEnvironment()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "Error: " + err.Error() + "\nOutput:\n" + string(out)}},
			IsError: true,
		}, nil, nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(out)}},
	}, nil, nil
}

// --- resources: run-summary and report ---

const defaultOutputDir = "outputfiles"

func handleRunSummaryResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	path := filepath.Join(defaultOutputDir, "run-summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{URI: req.Params.URI, MIMEType: "application/json", Text: string(data)},
		},
	}, nil
}

func handleReportResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	path := filepath.Join(defaultOutputDir, "index.html")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}
		return nil, err
	}
	text := string(data)
	const maxLen = 150000
	if len(text) > maxLen {
		text = text[:maxLen] + "\n\n... [truncated]"
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{URI: req.Params.URI, MIMEType: "text/html", Text: text},
		},
	}, nil
}
