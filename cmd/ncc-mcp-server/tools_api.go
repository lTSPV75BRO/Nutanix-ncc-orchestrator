package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

type RunNCCInput struct {
	Confirm            string `json:"confirm,omitempty" jsonschema:"Required exact value APPLY."`
	ConfigPath         string `json:"config_path,omitempty" jsonschema:"Path to YAML/JSON config file on the API server."`
	Clusters           string `json:"clusters,omitempty" jsonschema:"Comma-separated cluster IPs or FQDNs (optional scope)."`
	Username           string `json:"username,omitempty" jsonschema:"Ignored; credentials live on the API server."`
	Password           string `json:"password,omitempty" jsonschema:"Must be empty. Use secret:// on the API server."`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty" jsonschema:"Must be false. TLS policy is configured on the API server."`
	DryRun             bool   `json:"dry_run,omitempty" jsonschema:"If true, pass --dry-run as an extra run argument."`
}

func runNCC(ctx context.Context, req *mcp.CallToolRequest, input RunNCCInput) (*mcp.CallToolResult, any, error) {
	if res, _, _ := refusePasswordArg(input.Password); res != nil {
		return res, nil, nil
	}
	if strings.TrimSpace(input.Username) != "" {
		return mcpContractError(ai.CodePermissionDenied, "authorization", "MCP tools must not send cluster usernames; use the API server config", false)
	}
	if input.InsecureSkipVerify {
		return mcpContractError(ai.CodePermissionDenied, "authorization", "insecure_skip_verify is not allowed over MCP", false)
	}
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	body := map[string]any{}
	if input.ConfigPath != "" {
		body["config_path"] = strings.TrimSpace(input.ConfigPath)
	}
	if input.Clusters != "" {
		var clusters []string
		for _, c := range strings.Split(input.Clusters, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				clusters = append(clusters, c)
			}
		}
		body["clusters"] = clusters
	}
	var extra []string
	if input.DryRun {
		extra = append(extra, "--dry-run")
	}
	if len(extra) > 0 {
		body["extra_args"] = extra
	}
	env, _, err := callAPI(ctx, "run_ncc", http.MethodPost, "/api/v1/runs/trigger", nil, body, 60*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type DiscoverClustersInput struct {
	ConfigPath          string `json:"config_path,omitempty" jsonschema:"Unused; discovery uses API-server Prism Central settings."`
	PrismCentralURL     string `json:"prism_central_url" jsonschema:"Prism Central URL (e.g. https://10.0.0.1:9440)."`
	Username            string `json:"username,omitempty" jsonschema:"Must be empty; credentials stay on the API server."`
	Password            string `json:"password,omitempty" jsonschema:"Must be empty; credentials stay on the API server."`
	InsecureSkipVerify  bool   `json:"insecure_skip_verify,omitempty" jsonschema:"Ignored; TLS policy is configured on the API server."`
	DiscoverAPIVersion  string `json:"discover_api_version,omitempty"`
	NutanixV4APIVersion string `json:"nutanix_v4_api_version,omitempty"`
	OutputPath          string `json:"output_path,omitempty"`
}

func discoverClusters(ctx context.Context, req *mcp.CallToolRequest, input DiscoverClustersInput) (*mcp.CallToolResult, any, error) {
	if res, _, _ := refusePasswordArg(input.Password); res != nil {
		return res, nil, nil
	}
	if strings.TrimSpace(input.Username) != "" {
		return mcpContractError(ai.CodePermissionDenied, "authorization", "MCP tools must not send Prism credentials; configure them on the API server", false)
	}
	pc := strings.TrimSpace(input.PrismCentralURL)
	if pc == "" {
		return mcpInvalidArgument("prism_central_url", "prism_central_url is required")
	}
	safePC, err := safeHTTPSEndpoint(pc, "prism_central_url")
	if err != nil {
		return mcpInvalidArgument("prism_central_url", err.Error())
	}
	q := url.Values{"pc": {safePC}}
	env, _, err := callAPI(ctx, "discover_clusters", http.MethodGet, "/api/v1/settings/pc-clusters", q, nil, 60*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type ValidateConfigInput struct {
	ConfigPath     string `json:"config_path" jsonschema:"Config path on the API server."`
	IncludeSecrets bool   `json:"include_secrets,omitempty" jsonschema:"Preflight includes secret:// checks when true."`
}

func validateConfig(ctx context.Context, req *mcp.CallToolRequest, input ValidateConfigInput) (*mcp.CallToolResult, any, error) {
	path := strings.TrimSpace(input.ConfigPath)
	if path == "" {
		return mcpInvalidArgument("config_path", "config_path is required")
	}
	env, _, err := callAPI(ctx, "validate_config", http.MethodPost, "/api/v1/runs/preflight", nil, map[string]any{"config_path": path}, 120*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type PreflightCheckInput struct {
	ConfigPath string `json:"config_path" jsonschema:"Config path on the API server."`
}

func preflightCheck(ctx context.Context, req *mcp.CallToolRequest, input PreflightCheckInput) (*mcp.CallToolResult, any, error) {
	path := strings.TrimSpace(input.ConfigPath)
	if path == "" {
		return mcpInvalidArgument("config_path", "config_path is required")
	}
	env, _, err := callAPI(ctx, "preflight_check", http.MethodPost, "/api/v1/runs/preflight", nil, map[string]any{"config_path": path}, 120*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type GetRunSummaryInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Ignored; the API server output directory is used."`
}

func getRunSummary(ctx context.Context, req *mcp.CallToolRequest, input GetRunSummaryInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "get_run_summary", http.MethodGet, "/api/v1/runs/summary", nil, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type ReplayReportsInput struct {
	Confirm    string `json:"confirm,omitempty" jsonschema:"Required exact value APPLY."`
	ConfigPath string `json:"config_path" jsonschema:"Config path on the API server."`
}

func replayReports(ctx context.Context, req *mcp.CallToolRequest, input ReplayReportsInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	if strings.TrimSpace(input.ConfigPath) == "" {
		return mcpInvalidArgument("config_path", "config_path is required")
	}
	body := map[string]any{"config_path": strings.TrimSpace(input.ConfigPath), "extra_args": []string{"--replay"}}
	env, _, err := callAPI(ctx, "replay_reports", http.MethodPost, "/api/v1/runs/trigger", nil, body, 60*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type ListRunArtifactsInput struct {
	OutputDir string `json:"output_dir,omitempty" jsonschema:"Ignored; the API server output directory is used."`
}

func listRunArtifacts(ctx context.Context, req *mcp.CallToolRequest, input ListRunArtifactsInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "list_run_artifacts", http.MethodGet, "/api/v1/artifacts", nil, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type ListRunHistoryInput struct {
	OutputDir string `json:"output_dir,omitempty"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum runs (default 20, maximum 100)."`
}

func listRunHistory(ctx context.Context, req *mcp.CallToolRequest, input ListRunHistoryInput) (*mcp.CallToolResult, any, error) {
	limit := input.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	env, _, err := callAPI(ctx, "list_run_history", http.MethodGet, "/api/v1/runs", q, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

func getSampleConfig(ctx context.Context, req *mcp.CallToolRequest, input struct{}) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "get_sample_config", http.MethodGet, "/api/v1/settings/config", nil, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type GetMetricsInput struct {
	PromDir  string `json:"prom_dir,omitempty"`
	MaxBytes int    `json:"max_bytes,omitempty"`
}

func getMetrics(ctx context.Context, req *mcp.CallToolRequest, input GetMetricsInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "get_metrics", http.MethodGet, "/api/v1/health", nil, nil, 15*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type GetReportInput struct {
	OutputDir string `json:"output_dir,omitempty"`
	File      string `json:"file,omitempty" jsonschema:"Artifact name (index.html or a cluster file)."`
}

func getReport(ctx context.Context, req *mcp.CallToolRequest, input GetReportInput) (*mcp.CallToolResult, any, error) {
	file, err := safeArtifactName(input.File)
	if err != nil {
		return mcpInvalidArgument("file", err.Error())
	}
	env, _, err := callAPI(ctx, "get_report", http.MethodGet, "/api/v1/artifacts/"+url.PathEscape(file), nil, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type GetFindingsInput struct {
	OutputDir string `json:"output_dir,omitempty"`
	RunID     string `json:"run_id,omitempty"`
	Cluster   string `json:"cluster,omitempty"`
	Check     string `json:"check,omitempty"`
	Severity  string `json:"severity,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

func getFindings(ctx context.Context, req *mcp.CallToolRequest, input GetFindingsInput) (*mcp.CallToolResult, any, error) {
	q := url.Values{}
	if input.Cluster != "" {
		q.Set("cluster", input.Cluster)
	}
	if input.Severity != "" {
		q.Set("severity", input.Severity)
	}
	env, _, err := callAPI(ctx, "get_findings", http.MethodGet, "/api/v1/alerts/ncc", q, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type CompareRunsInput struct {
	OutputDir   string `json:"output_dir,omitempty"`
	CurrentRun  string `json:"current_run_id" jsonschema:"Current archived run ID."`
	BaselineRun string `json:"baseline_run_id" jsonschema:"Baseline archived run ID."`
}

func compareRuns(ctx context.Context, req *mcp.CallToolRequest, input CompareRunsInput) (*mcp.CallToolResult, any, error) {
	curID, err := safeRunID(input.CurrentRun)
	if err != nil {
		return mcpInvalidArgument("current_run_id", err.Error())
	}
	baseID, err := safeRunID(input.BaselineRun)
	if err != nil {
		return mcpInvalidArgument("baseline_run_id", err.Error())
	}
	cur, _, err := callAPI(ctx, "compare_runs", http.MethodGet, "/api/v1/runs/"+url.PathEscape(curID), nil, nil, 30*time.Second)
	if err != nil {
		return mcpAPIToolResult(cur, nil, err)
	}
	base, _, err := callAPI(ctx, "compare_runs", http.MethodGet, "/api/v1/runs/"+url.PathEscape(baseID), nil, nil, 30*time.Second)
	if err != nil {
		return mcpAPIToolResult(base, nil, err)
	}
	data := map[string]any{"current_run_id": curID, "baseline_run_id": baseID, "current": jsonData(cur), "baseline": jsonData(base)}
	return mcpAPIToolResult(apiEnvelope{Success: true}, data, nil)
}

func jsonData(env apiEnvelope) any {
	if len(env.Data) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(env.Data, &v) != nil {
		return nil
	}
	return v
}

type TroubleshootV2StackInput struct {
	InstallDir   string `json:"install_dir,omitempty"`
	ConfigPath   string `json:"config_path,omitempty"`
	APIListen    string `json:"api_listen,omitempty"`
	UIListen     string `json:"ui_listen,omitempty"`
	OnlyChecks   string `json:"only_checks,omitempty"`
	NoDisruptive bool   `json:"no_disruptive,omitempty"`
}

func troubleshootV2Stack(ctx context.Context, req *mcp.CallToolRequest, input TroubleshootV2StackInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "troubleshoot_v2_stack", http.MethodGet, "/api/v1/health/diagnostics", nil, nil, 60*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type CheckUpgradeInput struct {
	Repo          string `json:"repo,omitempty"`
	BinaryURL     string `json:"binary_url,omitempty"`
	TargetVersion string `json:"target_version,omitempty"`
	AllowMajor    bool   `json:"allow_major_upgrade,omitempty"`
}

func checkUpgrade(ctx context.Context, req *mcp.CallToolRequest, input CheckUpgradeInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "check_upgrade", http.MethodGet, "/api/v1/settings/update", nil, nil, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type PreviewInstallServiceInput struct {
	InstallDir      string `json:"install_dir,omitempty"`
	ServiceName     string `json:"service_name,omitempty"`
	OrchestratorBin string `json:"orchestrator_bin,omitempty"`
}

func previewInstallService(ctx context.Context, req *mcp.CallToolRequest, input PreviewInstallServiceInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "preview_install_service", http.MethodGet, "/api/v1/health/supervisor", nil, nil, 15*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type ApplyUpgradeInput struct {
	Confirm            string `json:"confirm" jsonschema:"Required exact value APPLY."`
	Repo               string `json:"repo,omitempty"`
	BinaryURL          string `json:"binary_url,omitempty"`
	BinarySHA256       string `json:"binary_sha256,omitempty"`
	TargetVersion      string `json:"target_version,omitempty"`
	AllowMajor         bool   `json:"allow_major_upgrade,omitempty"`
	SkipChecksumVerify bool   `json:"skip_checksum_verify,omitempty"`
}

func applyUpgrade(ctx context.Context, req *mcp.CallToolRequest, input ApplyUpgradeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	if input.SkipChecksumVerify {
		return mcpContractError(ai.CodePermissionDenied, "authorization", "skip_checksum_verify is not allowed over MCP", false)
	}
	env, _, err := callAPI(ctx, "apply_upgrade", http.MethodPost, "/api/v1/settings/update/apply", nil, map[string]any{}, 120*time.Second)
	return mcpAPIToolResult(env, nil, err)
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
	return mcpContractError(ai.CodePermissionDenied, "authorization", "bootstrap_v2_stack is not available over MCP; use the dashboard or an admin host shell", false)
}

type ServiceChangeInput struct {
	Confirm         string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir      string `json:"install_dir,omitempty"`
	ServiceName     string `json:"service_name,omitempty"`
	OrchestratorBin string `json:"orchestrator_bin,omitempty"`
	Now             *bool  `json:"now,omitempty"`
}

func installService(ctx context.Context, req *mcp.CallToolRequest, input ServiceChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	return mcpContractError(ai.CodePermissionDenied, "authorization", "install_service is not available over MCP; use the host installer", false)
}

func uninstallService(ctx context.Context, req *mcp.CallToolRequest, input ServiceChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	return mcpContractError(ai.CodePermissionDenied, "authorization", "uninstall_service is not available over MCP; use the host installer", false)
}

type StackChangeInput struct {
	Confirm    string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir string `json:"install_dir,omitempty"`
	Force      bool   `json:"force,omitempty"`
}

func startV2Stack(ctx context.Context, req *mcp.CallToolRequest, input StackChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	return mcpContractError(ai.CodePermissionDenied, "authorization", "start_v2_stack is not available over MCP; the API server is already the running stack", false)
}

func stopV2Stack(ctx context.Context, req *mcp.CallToolRequest, input StackChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	return mcpContractError(ai.CodePermissionDenied, "authorization", "stop_v2_stack is not available over MCP", false)
}

func restartV2Stack(ctx context.Context, req *mcp.CallToolRequest, input StackChangeInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	env, _, err := callAPI(ctx, "restart_v2_stack", http.MethodPost, "/api/v1/health/supervisor/restart", nil, map[string]any{"child": "api"}, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
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
	env, _, err := callAPI(ctx, "repair_v2_stack", http.MethodPost, "/api/v1/health/autoheal", nil, map[string]any{}, 60*time.Second)
	return mcpAPIToolResult(env, nil, err)
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
		return mcpInvalidArgument("input_file", "input_file is required")
	}
	env, _, err := callAPI(ctx, "restore_v2_backup", http.MethodPost, "/api/v1/settings/restore", nil, map[string]any{"name": strings.TrimSpace(input.InputFile)}, 120*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type BackupV2StackInput struct {
	Confirm      string `json:"confirm" jsonschema:"Required exact value APPLY."`
	InstallDir   string `json:"install_dir,omitempty"`
	OutputFile   string `json:"output_file,omitempty"`
	OutputDir    string `json:"output_dir,omitempty"`
	Retain       int    `json:"retain,omitempty"`
	IncludeStack bool   `json:"include_stack,omitempty"`
	Encrypt      bool   `json:"encrypt,omitempty"`
	KeyFile      string `json:"key_file,omitempty"`
}

func backupV2Stack(ctx context.Context, req *mcp.CallToolRequest, input BackupV2StackInput) (*mcp.CallToolResult, any, error) {
	if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
		return result, nil, nil
	}
	env, _, err := callAPI(ctx, "backup_v2_stack", http.MethodGet, "/api/v1/settings/backup", nil, nil, 120*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type OrchestratorActionInput struct {
	Action           string `json:"action" jsonschema:"Allowlisted action mapped to an API: version, v2-status, v2-check, config-schema, validate-config, validate-secrets, preflight-check."`
	Confirm          string `json:"confirm,omitempty"`
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
	switch action {
	case "version", "env-info", "terms":
		env, _, err := callAPI(ctx, "orchestrator_action", http.MethodGet, "/api/v1/components", nil, nil, 15*time.Second)
		return mcpAPIToolResult(env, nil, err)
	case "v2-status":
		env, _, err := callAPI(ctx, "orchestrator_action", http.MethodGet, "/api/v1/health/supervisor", nil, nil, 15*time.Second)
		return mcpAPIToolResult(env, nil, err)
	case "v2-check", "verify":
		env, _, err := callAPI(ctx, "orchestrator_action", http.MethodGet, "/api/v1/health/diagnostics", nil, nil, 60*time.Second)
		return mcpAPIToolResult(env, nil, err)
	case "config-schema":
		env, _, err := callAPI(ctx, "orchestrator_action", http.MethodGet, "/api/v1/settings/config", nil, nil, 30*time.Second)
		return mcpAPIToolResult(env, nil, err)
	case "validate-config", "validate-secrets", "preflight-check":
		if strings.TrimSpace(input.ConfigPath) == "" {
			return mcpInvalidArgument("config_path", "config_path is required")
		}
		env, _, err := callAPI(ctx, "orchestrator_action", http.MethodPost, "/api/v1/runs/preflight", nil, map[string]any{"config_path": strings.TrimSpace(input.ConfigPath)}, 120*time.Second)
		return mcpAPIToolResult(env, nil, err)
	default:
		return mcpContractError(ai.CodePermissionDenied, "authorization", "action is not exposed over MCP; use the dashboard or a matching API", false)
	}
}

type CreateScheduleInput struct {
	Confirm   string `json:"confirm,omitempty" jsonschema:"Required exact value APPLY unless print_only is true."`
	Type      string `json:"type,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	Config    string `json:"config,omitempty"`
	Command   string `json:"command,omitempty" jsonschema:"Custom commands are never accepted from MCP."`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	LogPath   string `json:"log_path,omitempty"`
	PrintOnly bool   `json:"print_only,omitempty"`
}

func createSchedule(ctx context.Context, req *mcp.CallToolRequest, input CreateScheduleInput) (*mcp.CallToolResult, any, error) {
	if strings.TrimSpace(input.Command) != "" {
		return mcpContractError(ai.CodePermissionDenied, "authorization", "create_schedule does not accept a custom command over MCP", false)
	}
	if !input.PrintOnly {
		if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
			return result, nil, nil
		}
	}
	printOnly := input.PrintOnly
	body := map[string]any{
		"type":       strings.TrimSpace(input.Type),
		"action":     "create",
		"task_name":  strings.TrimSpace(input.TaskName),
		"config":     strings.TrimSpace(input.Config),
		"cron":       strings.TrimSpace(input.Cron),
		"every":      strings.TrimSpace(input.Every),
		"log_path":   strings.TrimSpace(input.LogPath),
		"print_only": printOnly,
		"apply":      !printOnly,
	}
	env, _, err := callAPI(ctx, "create_schedule", http.MethodPut, "/api/v1/schedule", nil, body, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type ListSchedulesInput struct {
	Type     string `json:"type,omitempty"`
	TaskName string `json:"task_name,omitempty"`
}

func listSchedules(ctx context.Context, req *mcp.CallToolRequest, input ListSchedulesInput) (*mcp.CallToolResult, any, error) {
	env, _, err := callAPI(ctx, "list_schedules", http.MethodGet, "/api/v1/schedule", nil, nil, 15*time.Second)
	return mcpAPIToolResult(env, nil, err)
}

type DeleteScheduleInput struct {
	Confirm   string `json:"confirm,omitempty" jsonschema:"Required exact value APPLY unless print_only is true."`
	Type      string `json:"type,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly bool   `json:"print_only,omitempty"`
}

func deleteSchedule(ctx context.Context, req *mcp.CallToolRequest, input DeleteScheduleInput) (*mcp.CallToolResult, any, error) {
	if !input.PrintOnly {
		if result, _, _ := requireMCPConfirmation(input.Confirm); result != nil {
			return result, nil, nil
		}
	}
	printOnly := input.PrintOnly
	body := map[string]any{"action": "remove", "type": strings.TrimSpace(input.Type), "task_name": strings.TrimSpace(input.TaskName), "print_only": printOnly, "apply": !printOnly}
	env, _, err := callAPI(ctx, "delete_schedule", http.MethodPut, "/api/v1/schedule", nil, body, 30*time.Second)
	return mcpAPIToolResult(env, nil, err)
}
