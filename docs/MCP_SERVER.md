# NCC Orchestrator MCP Server

The **NCC MCP server** exposes the Nutanix NCC Orchestrator to AI assistants (Cursor, Claude Desktop, etc.) via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). The AI can validate configs, run preflight checks, run NCC checks, discover clusters from Prism Central, read run summaries, and replay reports.

Current server implementation version: **`3.0.0`** (overridable at build time via `-ldflags "-X main.serverVersion=<v>"`). Version 3 is a breaking MCP contract: every tool advertises an output schema and returns a versioned structured envelope.

Every tool advertises MCP **annotations** (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) and a human-readable title, so MCP hosts can present read-only tools differently from mutating/destructive ones (e.g. ask for confirmation before `delete_schedule`).

## Prerequisites

- **Go 1.27.4+** (same as the main orchestrator; see `go.mod`)
- **ncc-orchestrator** binary on your `PATH`, or set `NCC_ORCHESTRATOR_BIN` to its path

## Build

From the repo root:

```bash
go build -o ncc-mcp-server ./cmd/ncc-mcp-server/
```

Or install into `$GOPATH/bin` (or `$HOME/go/bin`):

```bash
go install ./cmd/ncc-mcp-server/
```

## Tools Exposed to the AI

Hints below: **RO** = read-only, **OW** = open-world (contacts clusters/external services), **D** = destructive.

| Tool | Hints | Description |
| --- | --- | --- |
| **run_ncc** | OW | Run NCC across clusters. Options: `config_path`, `clusters`, `username`, `password`, `insecure_skip_verify` (skip TLS verify for lab/self-signed certs), `dry_run`. Returns CLI output and run summary when available. |
| **discover_clusters** | RO, OW | List cluster IPs from Prism Central (default v4 `clustermgmt` API; optional `discover_api_version`: `v4` or `v3`; optional `nutanix_v4_api_version` such as `v4.2`, `v4.0.a1`). Requires `prism_central_url`; optional `config_path`, `username`, `password`, `insecure_skip_verify`, `output_path`. |
| **validate_config** | RO | Validate a config file without contacting clusters. Requires `config_path`; set `include_secrets: true` to also validate `secret://` references and secret-source accessibility. Run before `run_ncc` to catch misconfiguration early. |
| **preflight_check** | RO | Run the combined config/secrets/path preflight checks and return the structured JSON report (per-check `id`/`status`/`title`/`message` with remediation). Requires `config_path`. Broader than `validate_config`. |
| **get_run_summary** | RO | Read `run-summary.json` from a previous run (per-cluster `clusters[]`, `exit_code`, `health_score` rollups). Optional `output_dir` (default `outputfiles`). |
| **replay_reports** | OW | Regenerate HTML/CSV from existing logs (no NCC API; may send notifications). Requires `config_path`. |
| **list_run_artifacts** | RO | List files in an NCC run output directory (run-summary.json, ncc-run-record.json, regression-summary.json, checks-snapshot.json, drilldown-diff.json, flaky-checks.json, slo-dashboard.json, index.html, per-cluster .log/.html/.csv/.sarif). Optional `output_dir` (default `outputfiles`). |
| **list_run_history** | RO | List archived runs from `outputfiles/runs` with timestamps, duration, cluster success/failure counts, exit codes, and run IDs. Supports `output_dir` and a bounded `limit` (maximum 100). |
| **get_sample_config** | RO | Return the shipped `examples/example_config.yaml` so the AI can explain or help create a configuration. |
| **get_metrics** | RO | Read `.prom` files from `promfiles` (or a supplied `prom_dir`) with a bounded response size. |
| **troubleshoot_v2_stack** | RO | Run read-only `doctor` diagnostics for the v2 stack, including layout, health, service status, environment names, and recent log tails. Uses `--no-bundle --json --no-disruptive`; it never applies fixes. |
| **check_upgrade** | RO, OW | Check for a newer orchestrator release using `update --check --json` without downloading, installing, or modifying files. Optional `repo`, `binary_url`, `target_version`, and `allow_major_upgrade`. |
| **preview_install_service** | RO | Render the v2 service/task installation for the host OS using `v2-install-service --print-only`; it never installs, enables, starts, or changes a service. |
| **get_report** | RO | Read the aggregated index.html or a specific cluster report file. Optional `output_dir` (default `outputfiles`), `file` (`index` or a filename like `10.0.0.1.html`). The `file` is confined to `output_dir` (no `../` traversal). For `*.log` files, `KB NNNN` references are turned into markdown links to `portal.nutanix.com/kb/NNNN` for Cursor/IDE. Large reports are truncated for context. |
| **get_findings** | RO | Return bounded structured findings from `checks-snapshot.json`, filtered by run, cluster, check, and severity. |
| **compare_runs** | RO | Compare two archived run summaries and return failure, warning, health, status, and delta data. |
| **apply_upgrade** | D | Apply an orchestrator upgrade. Requires `confirm: "APPLY"`; checksum verification remains enabled by default. |
| **bootstrap_v2_stack** | D | Download and prepare the v2 API/UI binaries and startup scripts. Requires `confirm: "APPLY"`. |
| **install_service** | D | Install the boot-persistent v2 service and optionally start it. Requires `confirm: "APPLY"`; use `preview_install_service` first. |
| **uninstall_service** | D | Remove the boot-persistent v2 service. Requires `confirm: "APPLY"`. |
| **start_v2_stack** | D | Start the v2 API/UI stack detached. Requires `confirm: "APPLY"`. |
| **stop_v2_stack** | D | Stop the detached v2 stack, optionally forcing termination. Requires `confirm: "APPLY"`. |
| **restart_v2_stack** | D | Restart the v2 stack. Requires `confirm: "APPLY"`. |
| **repair_v2_stack** | D | Apply doctor self-heal remediations without a support bundle. Requires `confirm: "APPLY"` and disables disruptive checks. |
| **restore_v2_backup** | D | Restore a v2 backup archive into an install directory. Requires `confirm: "APPLY"` and an `input_file`. |
| **create_schedule** | — | Create/update scheduler entries via `create-schedule` (`type`, `task_name`, `config`, `command`, `cron`, `every`, `log_path`, `print_only`). A custom `command` installs an arbitrary recurring command line, so it is refused unless the MCP server runs with `NCC_MCP_ALLOW_SCHEDULE_COMMAND=true`. |
| **list_schedules** | RO | List scheduler entries for a task name (`type`, `task_name`). |
| **delete_schedule** | D | Remove scheduler entries for a task name (`type`, `task_name`, `print_only`). |
| **backup_v2_stack** | — | Create a v2 configuration/state backup archive. Requires `confirm: "APPLY"`; backups contain secrets and must be stored securely. |
| **orchestrator_action** | D/RO | Run an allowlisted operator action such as `v2-status`, `v2-check`, `verify`, `config-schema`, validation, `quickstart`, `uninstall`, or password recovery. Mutating actions require `confirm: "APPLY"`; arbitrary subprocess arguments are not accepted. |

## Resources (read-only)

The server exposes two **resources** that clients can list and read via MCP resources/list and resources/read:

| URI | Description |
| --- | --- |
| **ncc://run-summary** | Latest `run-summary.json` from the default output directory (`outputfiles`). MIME: `application/json`. |
| **ncc://report** | Latest aggregated NCC report (`index.html`) from the default output directory. MIME: `text/html`. Large content may be truncated. |

Version 3 also exposes:

| URI | Description |
| --- | --- |
| **ncc://config-schema** | Versioned configuration schema and secret-reference guidance. |
| **ncc://runs/{run_id}/summary** | Archived machine-readable summary for one run. |
| **ncc://runs/{run_id}/report/{file}** | Bounded archived report artifact confined to the run directory. |

## Structured result contract

Successful tool calls return an object with `schema_version: "3.0"`, `status`
(`success`, `partial`, or `error`), optional `summary`, and `data`. Results may
also include `warnings`, `next_actions`, `request_id`, `run_id`, `job_id`, and
`truncated`. Errors use stable `code`, `category`, `message`, `retryable`, and
`remediation` fields. Clients should branch on codes instead of matching
human-readable text.

Common codes include `INVALID_ARGUMENT`, `CONFIG_INVALID`,
`SECRET_UNAVAILABLE`, `CLUSTER_UNREACHABLE`, `PERMISSION_DENIED`,
`NOT_FOUND`, `TIMEOUT`, `CANCELLED`, and `UPSTREAM_UNAVAILABLE`.

Long-running API-triggered runs can be polled through
`GET /api/v1/jobs/{id}`. The response contains `job_id`, `kind`, `state`,
`phase`, progress, timestamps, the originating request ID, and a typed error
when the operation fails. `DELETE /api/v1/jobs/{id}` requests cancellation.

Use these when the AI only needs to read the latest run summary or report without calling a tool (e.g. for context or summarization).

## Run the Server (stdio)

The server uses **stdio** transport by default (stdin/stdout for JSON-RPC). Run it and connect your MCP client to the process:

```bash
./ncc-mcp-server
```

Do not use it interactively; Cursor or another MCP host will start it and talk over stdin/stdout.

### Mutation safety

Mutating tools are disabled by default. To enable them, explicitly set
`NCC_MCP_ALLOW_MUTATIONS=true` in the MCP server environment and include
`confirm: "APPLY"` in each mutating request. Preview and read-only tools do not
need this setting. The server also rejects arbitrary subprocess commands,
sanitizes inherited dynamic-loader/runtime variables, and caps orchestrator
output returned to MCP clients at 1 MiB.

## Configure in Cursor

1. Open **Cursor Settings → MCP** (or edit your MCP config file).
2. Add the NCC MCP server.

**Option A – config file (recommended)**  
Edit the MCP config (e.g. `~/.cursor/mcp.json` or project `.cursor/mcp.json`) and add:

```json
{
  "mcpServers": {
    "ncc-orchestrator": {
      "command": "/absolute/path/to/ncc-mcp-server"
    }
  }
}
```

If the binary is on your PATH:

```json
{
  "mcpServers": {
    "ncc-orchestrator": {
      "command": "ncc-mcp-server"
    }
  }
}
```

**Option B – custom binary path**  
If the orchestrator binary is not on PATH, point the MCP server to it with an env var and use the same env when starting Cursor (or in the MCP config if your client supports env):

```json
{
  "mcpServers": {
    "ncc-orchestrator": {
      "command": "/path/to/ncc-mcp-server",
      "env": {
        "NCC_ORCHESTRATOR_BIN": "/path/to/ncc-orchestrator"
      }
    }
  }
}
```

Restart Cursor (or reload MCP) so it picks up the new server.

## Finding the orchestrator binary

The MCP server looks for the `ncc-orchestrator` binary in this order:

1. **`NCC_ORCHESTRATOR_BIN`** env var (full path)
2. **Same directory as the MCP server executable** — if you run the MCP server from the project root (e.g. `"command": "/path/to/Nutanix-ncc-orchestrator/ncc-mcp-server"`) and keep `ncc-orchestrator` in that same directory, no env var is needed
3. **`ncc-orchestrator`** on `PATH`

So the easiest setup is: build both binaries in the project root, and in Cursor MCP config set `command` to the **full path** to `ncc-mcp-server` (e.g. `/Volumes/Mac/Programs/Nutanix-ncc-orchestrator/ncc-mcp-server`). The server will then find `ncc-orchestrator` in the same folder.

## Environment Variables

| Variable | Description |
| --- | --- |
| `NCC_ORCHESTRATOR_BIN` | Path to the `ncc-orchestrator` binary. If unset, the server looks next to its own executable, then on PATH. |
| `NCC_PASSWORD` | Prism password (used by the orchestrator when you don’t pass `password` in tool args). |
| `NCC_MCP_PROFILE` | Advertised capability profile: `readonly`, `operator` (default), or `admin`. Admin-only tools are not registered outside the `admin` profile. |

Credentials are best supplied via config file or env; avoid putting passwords in tool arguments when possible.

### Important discovery security behavior

- `https://` Prism Central URLs are the default and recommended mode.
- `http://` Prism Central URLs are accepted only when `insecure_skip_verify=true` is explicitly set.

## Security Notes

- The MCP server runs the orchestrator as a subprocess; it inherits your environment (including `NCC_PASSWORD`).
- Use a dedicated config file for MCP-driven runs and restrict who can edit it.
- Prefer `NCC_PASSWORD` (or config file) over passing `password` in tool arguments.

## Troubleshooting

- **“ncc-orchestrator: command not found”**  
  Put the orchestrator binary on your PATH or set `NCC_ORCHESTRATOR_BIN` to its full path in the MCP server’s env.

- **Tools fail with “permission denied” or path errors**  
  The server’s working directory is set by the MCP host (e.g. Cursor). Use absolute paths in `config_path` and `output_dir` when needed.

- **No run summary in run_ncc output**  
  The server looks for `outputfiles/run-summary.json` (relative to the process cwd) or, if `config_path` is set, `<config_dir>/outputfiles/run-summary.json`. Ensure the orchestrator wrote to that path (check `output-dir-filtered` in config).

- **discover_clusters returns HTTP URL policy errors**  
  Use an `https://` Prism Central URL, or set `insecure_skip_verify: true` intentionally for trusted lab-only `http://` endpoints.

## Possible future additions

- **Durable job storage** — Persist job state in a shared store so polling
  remains complete across API restarts and multi-replica deployments.
- **SSE progress stream** — Add a reconnectable stream after polling clients
  are established.
- **Generated check catalog** — Publish check-specific remediation and evidence
  metadata from the orchestrator's check registry.
