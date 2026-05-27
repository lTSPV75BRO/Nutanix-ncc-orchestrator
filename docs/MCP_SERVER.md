# NCC Orchestrator MCP Server

The **NCC MCP server** exposes the Nutanix NCC Orchestrator to AI assistants (Cursor, Claude Desktop, etc.) via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). The AI can run NCC checks, discover clusters from Prism Central, read run summaries, and replay reports.

Current server implementation version: **`2.0.0`**.

## Prerequisites

- **Go 1.26+** (same as the main orchestrator; see `go.mod`)
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

| Tool | Description |
| --- | --- |
| **run_ncc** | Run NCC across clusters. Options: `config_path`, `clusters`, `username`, `password`, `insecure_skip_verify` (skip TLS verify for lab/self-signed certs), `dry_run`. Returns CLI output and run summary when available. |
| **discover_clusters** | List cluster IPs from Prism Central (default v4 `clustermgmt` API; optional `discover_api_version`: `v4` or `v3`; optional `nutanix_v4_api_version` such as `v4.2`, `v4.0.a1`). Requires `prism_central_url`; optional `config_path`, `username`, `password`, `insecure_skip_verify`, `output_path`. |
| **get_run_summary** | Read `run-summary.json` from a previous run (per-cluster `clusters[]`, `exit_code`, `health_score` rollups). Optional `output_dir` (default `outputfiles`). |
| **replay_reports** | Regenerate HTML/CSV from existing logs (no NCC API). Requires `config_path`. |
| **list_run_artifacts** | List files in an NCC run output directory (run-summary.json, ncc-run-record.json, regression-summary.json, checks-snapshot.json, drilldown-diff.json, flaky-checks.json, slo-dashboard.json, index.html, per-cluster .log/.html/.csv/.sarif). Optional `output_dir` (default `outputfiles`). |
| **get_report** | Read the aggregated index.html or a specific cluster report file. Optional `output_dir` (default `outputfiles`), `file` (`index` or a filename like `10.0.0.1.html`). For `*.log` files, `KB NNNN` references are turned into markdown links to `portal.nutanix.com/kb/NNNN` for Cursor/IDE. Large reports are truncated for context. |
| **create_schedule** | Create/update scheduler entries via `create-schedule` (`type`, `task_name`, `config`, `command`, `cron`, `every`, `log_path`, `print_only`). |
| **list_schedules** | List scheduler entries for a task name (`type`, `task_name`). |
| **delete_schedule** | Remove scheduler entries for a task name (`type`, `task_name`, `print_only`). |

## Resources (read-only)

The server exposes two **resources** that clients can list and read via MCP resources/list and resources/read:

| URI | Description |
| --- | --- |
| **ncc://run-summary** | Latest `run-summary.json` from the default output directory (`outputfiles`). MIME: `application/json`. |
| **ncc://report** | Latest aggregated NCC report (`index.html`) from the default output directory. MIME: `text/html`. Large content may be truncated. |

Use these when the AI only needs to read the latest run summary or report without calling a tool (e.g. for context or summarization).

## Run the Server (stdio)

The server uses **stdio** transport by default (stdin/stdout for JSON-RPC). Run it and connect your MCP client to the process:

```bash
./ncc-mcp-server
```

Do not use it interactively; Cursor or another MCP host will start it and talk over stdin/stdout.

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

- **validate_config** — Dedicated tool that only validates config (same as `run_ncc` with `dry_run: true`); can make intent clearer for the AI.
- **get_sample_config** — Return a sample or schema of the orchestrator config so the AI can help users write or edit `config.yaml`.
- **Resource template** — e.g. `ncc://report/{output_dir}` so clients can read run-summary or report for a specific output directory via URI.
- **Prometheus metrics** — Tool or resource to read the latest Prometheus output from `prom-dir` if the orchestrator is configured to write metrics.
