# NCC MCP tool reference

Every tool is one REST call to `ncc-api-server` with the user's PAT. The PAT's role
decides whether it works; the "Role" column is the minimum the API enforces. Results
are scrubbed before you see them (credential-like values appear masked).

## Read

| Tool | API | Role | Notes |
| --- | --- | --- | --- |
| `get_run_summary` | `GET /api/v1/runs/summary` | viewer | Latest run |
| `list_run_history` | `GET /api/v1/runs` | viewer | `limit` max 100 |
| `list_run_artifacts` | `GET /api/v1/artifacts` | viewer | Basenames only |
| `get_report` | `GET /api/v1/artifacts/{file}` | viewer | `file` is a basename (`index.html`, `10.0.0.1.log`) |
| `get_findings` | `GET /api/v1/alerts/ncc` | viewer | Optional `cluster`, `check`, `severity` (comma list), `limit` (default 50, max 200), `cursor`; page with `next_cursor`. Oldest change first |
| `compare_runs` | two `GET /api/v1/runs/{id}` | viewer | Needs `current_run_id` and `baseline_run_id` |
| `get_metrics` | `GET /api/v1/health` | viewer | Health summary only, not Prometheus metrics |
| `list_schedules` | `GET /api/v1/schedule` | viewer | |
| `validate_config` / `preflight_check` | `POST /api/v1/runs/preflight` | operator | `config_path` is on the **API host** |
| `get_sample_config` | `GET /api/v1/settings/config` | admin | Current config with credentials masked |
| `discover_clusters` | `GET /api/v1/settings/pc-clusters` | admin | Needs `prism_central_url`; no username or password |
| `troubleshoot_v2_stack` | `GET /api/v1/health/diagnostics` | admin | Self-heal scan |
| `preview_install_service` | `GET /api/v1/health/supervisor` | admin | Status, not an install file |
| `check_upgrade` | `GET /api/v1/settings/update` | admin | Does not install |

## Write (gated)

Need an adequate role, `NCC_MCP_ALLOW_MUTATIONS=true` in the MCP process, and
`confirm: "APPLY"` (unless noted). `confirm` is a safeguard against accidents: only
send it after the user asked for that exact change this turn.

| Tool | API | Role | Notes |
| --- | --- | --- | --- |
| `run_ncc` | `POST /api/v1/runs/trigger` | operator | No password, username or `insecure_skip_verify` |
| `replay_reports` | trigger with `--replay` | operator | Needs `config_path` |
| `create_schedule` | `PUT /api/v1/schedule` | operator | `command` always refused; `print_only` skips APPLY |
| `delete_schedule` | `PUT /api/v1/schedule` (remove) | operator | |
| `apply_upgrade` | `POST /api/v1/settings/update/apply` | admin | No checksum skip |
| `restart_v2_stack` | `POST /api/v1/health/supervisor/restart` | admin | |
| `repair_v2_stack` | `POST /api/v1/health/autoheal` | admin | |
| `backup_v2_stack` | `POST /api/v1/settings/backups` | admin | Creates a server-side snapshot and returns its name. No passphrase over MCP; answers 412 if the server requires a backup key and has none |
| `restore_v2_backup` | `POST /api/v1/settings/restore` | admin | `input_file` must be a backup **name** (`manual-<time>.tar.gz.enc`), never a path |

## Always refused over MCP

`bootstrap_v2_stack`, `install_service`, `uninstall_service`, `start_v2_stack`,
`stop_v2_stack`, password reset, and custom orchestrator arguments.

`orchestrator_action` maps an allowlist onto API reads: `version`, `env-info`, `terms`
(components), `v2-status` (supervisor), `v2-check` and `verify` (diagnostics),
`config-schema` (masked config), and `validate-config`, `validate-secrets`,
`preflight-check` (preflight, needs `config_path`). Anything else is refused.

## Resources

- `ncc://run-summary`
- `ncc://report`
- `ncc://config-schema`
- `ncc://runs/{run_id}/summary`
- `ncc://runs/{run_id}/report/{file}`

## Environment

| Variable | Required | Purpose |
| --- | --- | --- |
| `NCC_MCP_TOKEN` | yes | Dashboard PAT (`ncc_pat_…`), at least 16 characters |
| `NCC_MCP_API_URL` | no | Default `http://127.0.0.1:8081`. `http://` only on loopback |
| `NCC_MCP_PROFILE` | no | `readonly`, `operator` (default) or `admin`: which tools are registered |
| `NCC_MCP_ALLOW_MUTATIONS` | no | Must be `true` for APPLY tools |

## Audit

Every MCP-originated request is recorded by the API as `mcp.call` with `channel=mcp`,
the tool name, the client version, and the PAT's id, name and owner. Failures include
`error` and `remediation`. Read it with `GET /api/v1/audit?channel=mcp` (operator or
higher; the dashboard shows it under System, Audit log). Repeated refusals or one
token used from several addresses show up as `access.suspicious`.
