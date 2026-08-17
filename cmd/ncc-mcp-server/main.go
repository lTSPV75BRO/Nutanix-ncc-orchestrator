// ncc-mcp-server exposes the Nutanix NCC Orchestrator as an MCP server so AI assistants
// (Cursor, Claude, etc.) can run NCC checks, discover clusters, and read run summaries.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/kblinks"
)

const serverName = "ncc-orchestrator"

// serverVersion is the advertised MCP server version. It is overridable at build
// time via -ldflags "-X main.serverVersion=<v>" (the release build stamps the
// orchestrator version here) and otherwise defaults to the current release.
var serverVersion = "2.1.1"

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
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return string(out), err
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_run_artifacts",
		Title:       "List run artifacts",
		Description: "List files in an NCC run output directory (run-summary.json, ncc-run-record.json, regression-summary.json, index.html, per-cluster .log/.html/.csv/.sarif). Use to discover what reports exist from a previous run.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "List run artifacts",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, listRunArtifacts)

	// Tool: get_report — read aggregated or per-cluster report content (HTML/text)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_report",
		Title:       "Read report file",
		Description: "Read the aggregated index.html or a specific cluster report file from an output directory. For *.log files, KB references (e.g. KB 5582) are expanded to markdown links to portal.nutanix.com. Returns report content for the AI to summarize or analyze.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Read report file",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, getReport)

	// Tool: create_schedule — create/update scheduler entry
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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
	mcp.AddTool(server, &mcp.Tool{
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

	// Log to stderr only (stdout is used for MCP JSON-RPC)
	log.SetOutput(os.Stderr)
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
	if input.Password != "" {
		args = append(args, "--password", input.Password)
	}
	if input.InsecureSkipVerify {
		args = append(args, "--insecure-skip-verify")
	}
	if input.DryRun {
		args = append(args, "--dry-run")
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = nil
	cmd.Env = os.Environ()
	// Pass password via env so the orchestrator gets it without TTY prompt when run non-interactively
	if input.Password != "" {
		cmd.Env = append(cmd.Env, "NCC_PASSWORD="+input.Password)
	}
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
	if input.Password != "" {
		args = append(args, "--password", input.Password)
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
	cmd.Env = os.Environ()
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
	cmd.Env = os.Environ()
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

// --- schedule tools ---

type CreateScheduleInput struct {
	Type      string `json:"type,omitempty" jsonschema:"Scheduler type: auto, cron, windows"`
	TaskName  string `json:"task_name,omitempty" jsonschema:"Schedule/task name (default ncc-orchestrator)"`
	Config    string `json:"config,omitempty" jsonschema:"Config file path passed to orchestrator --config"`
	Command   string `json:"command,omitempty" jsonschema:"Custom command for scheduler"`
	Cron      string `json:"cron,omitempty" jsonschema:"Cron expression for cron scheduler"`
	Every     string `json:"every,omitempty" jsonschema:"Interval duration like 30m, 4h"`
	LogPath   string `json:"log_path,omitempty" jsonschema:"Log path for cron redirection"`
	PrintOnly bool   `json:"print_only,omitempty" jsonschema:"Preview only; do not apply changes"`
}

func createSchedule(ctx context.Context, req *mcp.CallToolRequest, input CreateScheduleInput) (*mcp.CallToolResult, any, error) {
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
	cmd.Env = os.Environ()
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
	cmd.Env = os.Environ()
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
	cmd.Env = os.Environ()
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
