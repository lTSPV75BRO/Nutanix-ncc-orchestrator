# NCC Orchestrator MCP Server

The **NCC MCP server** exposes the Nutanix NCC Orchestrator to AI assistants (Cursor, Claude Desktop, Claude Code, etc.) via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). The AI can validate configs, run preflight checks, run NCC checks, discover clusters from Prism Central, read run summaries, and replay reports.

**Import this into an assistant in five minutes** — jump to [Import in five minutes](#import-in-five-minutes). Agents should follow the shipped skill [`skills/ncc-mcp/SKILL.md`](../skills/ncc-mcp/SKILL.md) (also linked from `.cursor/skills/ncc-mcp`).

How it works: `ncc-mcp-server` is an MCP **stdio** process that is a **strict HTTP client of `ncc-api-server`**. It does not exec `ncc-orchestrator`. Every tool and resource maps to a REST call with:

- `Authorization: Bearer` personal access token (`NCC_MCP_TOKEN`)
- `X-NCC-MCP-Version` and a time-bound HMAC proof (`X-NCC-MCP-Proof`, protocol `ncc-mcp-proof-v1` keyed by the PAT)
- `X-NCC-MCP-Tool` naming the MCP tool (for audit)

Setting the version string to 2.4.0 on a 2.3.x binary is not enough. The 2.4.0 API refuses a missing or forged proof (`NCC_API_MCP_UPGRADE_REQUIRED` / `NCC_API_MCP_PROOF_INVALID`). Third-party HTTP clients that do not identify as `ncc-mcp-server` keep using normal API auth. MCP-originated API requests are audited as `mcp.call` with `channel=mcp`, `mcp_tool`, PAT id/name when a PAT was used, and error/remediation on failures.

This matches MCP practice: stdio transport, JSON-RPC, tool annotations, JSON Schema arguments, no secrets in tool arguments, confirmation for mutations, and host-side RBAC on the API.

Current server implementation version: **`3.0.0`** (overridable at build time via `-ldflags "-X main.serverVersion=<v>"`). Version 3 is a breaking MCP contract: every tool advertises an output schema and returns a versioned structured envelope.

Every tool advertises MCP **annotations** (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) and a human-readable title, so MCP hosts can present read-only tools differently from mutating/destructive ones (e.g. ask for confirmation before `delete_schedule`).

## Import in five minutes

You need a running **ncc-api-server**, the **2.4.0+** `ncc-mcp-server` binary, and a dashboard PAT.

1. **Mint a token** in the dashboard: user menu (top right) → **Personal access tokens → Create token**. Use **viewer** for read-only assistants, **operator** to trigger runs. Copy `ncc_pat_…` once.
2. **Get the binary** (build from this repo; it is not required on the API host):

   ```bash
   go build -o ncc-mcp-server ./cmd/ncc-mcp-server/
   ```

3. **Point at the API.** Default is `http://127.0.0.1:8081`. Off the same machine you must use `https://host:8081`. Cleartext HTTP to a LAN IP is refused. If the API is HTTP-only on another host, tunnel it:

   ```bash
   examples/mcp/forward-api.sh user@api-host
   # then NCC_MCP_API_URL=http://127.0.0.1:18091
   ```

4. **Drop in a host config** (replace the binary path and token):

   | Host | File |
   | --- | --- |
   | Cursor | copy [`examples/mcp/cursor.mcp.json`](../examples/mcp/cursor.mcp.json) into `~/.cursor/mcp.json` or the project `.cursor/mcp.json` |
   | Claude Desktop | merge [`examples/mcp/claude_desktop.json`](../examples/mcp/claude_desktop.json) into Claude’s MCP settings (`claude_desktop_config.json`) |
   | Claude Code / other skill hosts | add the same `command` + `env`, and copy [`skills/ncc-mcp/`](../skills/ncc-mcp/) into the product’s skills directory |

   Start **readonly** (`NCC_MCP_PROFILE`: `readonly`). Do not put the PAT in chat or in tool arguments.

5. **Reload MCP** in the host. Ask: “Call `get_run_summary` and list FAIL/ERR findings.” If that works, import is done.

Agent skill (how the model should use the tools): [`skills/ncc-mcp/SKILL.md`](../skills/ncc-mcp/SKILL.md). Safety and workflows: [`AI_INTEGRATION.md`](AI_INTEGRATION.md).

## Prerequisites

- **Go 1.27.4+** to build from source (same as the main orchestrator; see `go.mod`)
- A running **ncc-api-server** (default `http://127.0.0.1:8081`)
- A dashboard **personal access token** (`ncc_pat_…`) in `NCC_MCP_TOKEN`

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

Hints below: **RO** = read-only, **OW** = open-world (contacts the API / clusters), **D** = destructive.

| Tool | Hints | Description |
| --- | --- | --- |
| **run_ncc** | OW | `POST /api/v1/runs/trigger`. Requires `confirm: "APPLY"` and `NCC_MCP_ALLOW_MUTATIONS`. Options: `config_path`, `clusters`, `dry_run`. Usernames, passwords, and `insecure_skip_verify` are refused. |
| **discover_clusters** | RO, OW | `GET /api/v1/settings/pc-clusters`. Requires `prism_central_url`. Credentials stay on the API server. |
| **validate_config** | RO | `POST /api/v1/runs/preflight` for the given `config_path`. |
| **preflight_check** | RO | Same preflight endpoint; structured report. Requires `config_path`. |
| **get_run_summary** | RO | `GET /api/v1/runs/summary`. |
| **replay_reports** | OW | Triggers a run with `--replay`. Requires `config_path`, `confirm: "APPLY"`, and mutations enabled. |
| **list_run_artifacts** | RO | `GET /api/v1/artifacts`. |
| **list_run_history** | RO | `GET /api/v1/runs` with a bounded `limit` (maximum 100). |
| **get_sample_config** | RO | `GET /api/v1/settings/config`. |
| **get_metrics** | RO | `GET /api/v1/health`. |
| **troubleshoot_v2_stack** | RO | `GET /api/v1/health/diagnostics`. |
| **check_upgrade** | RO, OW | `GET /api/v1/settings/update`. |
| **preview_install_service** | RO | `GET /api/v1/health/supervisor` (status only; does not install a host service). |
| **get_report** | RO | `GET /api/v1/artifacts/{file}` (`index.html` or a basename; no path traversal). |
| **get_findings** | RO | `GET /api/v1/alerts/ncc`, optional cluster/severity filters. |
| **compare_runs** | RO | Fetches two archived runs from `GET /api/v1/runs/{id}` and returns both payloads. |
| **apply_upgrade** | D | `POST /api/v1/settings/update/apply`. Requires `confirm: "APPLY"` and `NCC_MCP_ALLOW_MUTATIONS=true`. Checksum skip is refused. |
| **bootstrap_v2_stack** | D | Refused over MCP (host installer only). Still requires `confirm: "APPLY"` so clients fail closed. |
| **install_service** / **uninstall_service** | D | Refused over MCP (host installer only). |
| **start_v2_stack** / **stop_v2_stack** | D | Refused over MCP (the API is already the running stack). |
| **restart_v2_stack** | D | `POST /api/v1/health/supervisor/restart`. Requires `confirm: "APPLY"`. |
| **repair_v2_stack** | D | `POST /api/v1/health/autoheal`. Requires `confirm: "APPLY"`. |
| **restore_v2_backup** | D | `POST /api/v1/settings/restore`. Requires `confirm: "APPLY"` and `input_file`. |
| **create_schedule** | — | `PUT /api/v1/schedule`. Custom `command` is never accepted. Mutating calls need `confirm: "APPLY"`. |
| **list_schedules** | RO | `GET /api/v1/schedule`. |
| **delete_schedule** | D | `PUT /api/v1/schedule` with `action=remove`. Requires `confirm: "APPLY"` unless `print_only`. |
| **backup_v2_stack** | — | `GET /api/v1/settings/backup`. Requires `confirm: "APPLY"`. |
| **orchestrator_action** | D/RO | Allowlisted read-only API maps: `version`, `v2-status`, `v2-check`, `verify`, `config-schema`, `validate-config`, `validate-secrets`, `preflight-check`. Host-only actions (uninstall, password recovery, custom argv) are refused. |

API RBAC still applies: a viewer PAT cannot trigger runs even if the MCP tool is advertised.

## Resources (read-only)

The server exposes **resources** that clients can list and read via MCP resources/list and resources/read (authenticated):

| URI | Description |
| --- | --- |
| **ncc://run-summary** | Latest run summary from `GET /api/v1/runs/summary`. MIME: `application/json`. |
| **ncc://report** | Aggregated report from `GET /api/v1/artifacts/index.html`. MIME: `text/html`. Large content may be truncated. |

Version 3 also exposes:

| URI | Description |
| --- | --- |
| **ncc://config-schema** | Versioned configuration schema and secret-reference guidance. |
| **ncc://runs/{run_id}/summary** | Archived machine-readable summary for one run. |
| **ncc://runs/{run_id}/report/{file}** | Bounded archived report artifact via the API. |

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

## Authentication

The MCP server refuses to start, and refuses `tools/call` and `resources/read`,
until a personal access token is configured. Mint a PAT in the dashboard
(**user menu → Personal access tokens**) and set it as `NCC_MCP_TOKEN` in the
MCP host environment. Tokens must be at least 16 characters.

When the MCP client talks over HTTP, it must also send the same value as
`Authorization: Bearer …` or `X-API-Token`. Stdio hosts (Cursor, Claude
Desktop) authenticate by placing the token in the server process environment;
the assistant cannot call tools until that is set.

Set `NCC_MCP_API_URL` to the API base (`https://ncc.example.com:8081`). Default
is `http://127.0.0.1:8081`. The server checks the token with
`GET /api/v1/auth/me` (HMAC proof required) and refuses work if the API reports
`authenticated: false`. `http://` is accepted only on loopback.

## Run the Server (stdio)

The server uses **stdio** transport by default (stdin/stdout for JSON-RPC). Run it and connect your MCP client to the process:

```bash
NCC_MCP_TOKEN=ncc_pat_… NCC_MCP_API_URL=http://127.0.0.1:8081 ./ncc-mcp-server
```

Do not use it interactively; Cursor or another MCP host will start it and talk over stdin/stdout.

### Mutation safety

Mutating tools are disabled by default. To enable them, explicitly set
`NCC_MCP_ALLOW_MUTATIONS=true` in the MCP server environment and include
`confirm: "APPLY"` in each mutating request. Preview and read-only tools do not
need this setting. Custom schedule commands and passwords in tool arguments are
never accepted. API responses returned to MCP clients are capped at 1 MiB. The
HTTP client does not honour `HTTP(S)_PROXY`.

## Configure in Cursor

Ready-to-edit JSON: [`examples/mcp/cursor.mcp.json`](../examples/mcp/cursor.mcp.json).

1. Open **Cursor Settings → MCP** (or edit `~/.cursor/mcp.json` / project `.cursor/mcp.json`).
2. Merge the `ncc-orchestrator` server block. `command` must be an **absolute path** to `ncc-mcp-server` unless the binary is on `PATH`.
3. Set `NCC_MCP_TOKEN` and `NCC_MCP_API_URL`. Keep `NCC_MCP_PROFILE` at `readonly` until you need runs or schedules.
4. Reload MCP.

To let Cursor agents follow the NCC workflow automatically, the repo already links [`.cursor/skills/ncc-mcp`](../.cursor/skills/ncc-mcp) → [`skills/ncc-mcp`](../skills/ncc-mcp). Copy that folder into `~/.cursor/skills/ncc-mcp` to use it in other projects.

## Environment Variables

| Variable | Description |
| --- | --- |
| `NCC_MCP_TOKEN` | Required. Personal access token (`ncc_pat_…`) or other bearer secret. The server will not start without it. |
| `NCC_MCP_API_URL` | API base URL (default `http://127.0.0.1:8081`). Each tool/resource call checks `GET /api/v1/auth/me` and then the mapped REST route. Non-loopback `http://` is rejected. |
| `NCC_MCP_ALLOW_MUTATIONS` | Set `true` to allow mutating tools that also send `confirm: "APPLY"`. |
| `NCC_MCP_PROFILE` | Advertised capability profile: `readonly`, `operator` (default), or `admin`. Admin-only tools are not registered outside the `admin` profile. |

Never put passwords in tool arguments. Store Prism credentials with `secret://` on the API server.

## Security Notes

- Configure `NCC_MCP_TOKEN` (a dashboard PAT, not the API process's `NCC_API_TOKEN`) in the MCP host. Without it the server exits on start and tools/resources are refused.
- The MCP process never execs the orchestrator and never puts passwords on a command line.
- The API tags MCP traffic in the JSONL audit log (`action=mcp.call`, `channel=mcp`, `mcp_tool`, `mcp_client_version`). When auth used a PAT, entries also carry `pat_id`, `pat_name`, and the owning `user`. Failures include `error`, `error_code`, and `remediation`. Filter with `GET /api/v1/audit?channel=mcp`.
- Use a dedicated config file and a least-privilege PAT (viewer vs operator vs admin).
- Prefer `secret://` on the API server over any credential in the MCP host environment.

## Troubleshooting

- **“this MCP server is too old for this stack” / `NCC_API_MCP_UPGRADE_REQUIRED`**  
  The host is still running ncc-mcp-server 2.3.x (or it did not send a client version). Replace it with the 2.4.0 binary, set `NCC_MCP_TOKEN`, and reload MCP.

- **`NCC_API_MCP_PROOF_INVALID`**  
  The caller claimed to be MCP without a valid HMAC proof. Use the 2.4.0 `ncc-mcp-server` binary; stamping a version string is not enough.

- **Server exits with `NCC_MCP_TOKEN must be set`**  
  Mint a PAT in the dashboard and set `NCC_MCP_TOKEN` in the MCP host `env`. Reload MCP after saving the config.

- **Tool calls return `PERMISSION_DENIED` / “authentication required”**  
  The HTTP request is missing `Authorization: Bearer` (or `X-API-Token`), the value does not match `NCC_MCP_TOKEN`, the PAT is revoked, or the API URL is wrong.

- **Connection refused / API request failed**  
  Start `ncc-api-server` and set `NCC_MCP_API_URL` to its base URL. Use `https://` off-loopback.

- **Host lifecycle tools fail with “not available over MCP”**  
  Bootstrap, OS service install, start/stop of the stack, and password recovery stay on the host or dashboard. That is intentional.

- **Tools fail with “permission denied”**  
  The PAT role is too low for that API, or `NCC_MCP_ALLOW_MUTATIONS` / `confirm=APPLY` is missing.

## Possible future additions

- **Durable job storage** — Persist job state in a shared store so polling
  remains complete across API restarts and multi-replica deployments.
- **SSE progress stream** — Add a reconnectable stream after polling clients
  are established.
- **Generated check catalog** — Publish check-specific remediation and evidence
  metadata from the orchestrator's check registry.
