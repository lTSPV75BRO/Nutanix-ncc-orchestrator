# NCC MCP tool reference

All tools call `ncc-api-server`. RBAC of the PAT still applies.

## Read

| Tool | API | Notes |
| --- | --- | --- |
| `get_run_summary` | `GET /api/v1/runs/summary` | Latest run |
| `list_run_history` | `GET /api/v1/runs` | `limit` max 100 |
| `list_run_artifacts` | `GET /api/v1/artifacts` | Basenames only |
| `get_report` | `GET /api/v1/artifacts/{file}` | `file` is a basename (`index.html` or `10.0.0.1.log`) |
| `get_findings` | `GET /api/v1/alerts/ncc` | Optional `cluster`, `severity` |
| `compare_runs` | two `GET /api/v1/runs/{id}` | Needs `current_run_id` and `baseline_run_id` |
| `get_sample_config` | `GET /api/v1/settings/config` | Live config, not only the example |
| `get_metrics` | `GET /api/v1/health` | Stack health |
| `troubleshoot_v2_stack` | `GET /api/v1/health/diagnostics` | Self-heal scan |
| `check_upgrade` | `GET /api/v1/settings/update` | Does not install |
| `preview_install_service` | `GET /api/v1/health/supervisor` | Status, not an install preview file |
| `list_schedules` | `GET /api/v1/schedule` | |
| `validate_config` / `preflight_check` | `POST /api/v1/runs/preflight` | Needs `config_path` on the **API host** |
| `discover_clusters` | `GET /api/v1/settings/pc-clusters` | Needs `prism_central_url`; no username/password |

## Write (gated)

Need `NCC_MCP_ALLOW_MUTATIONS=true` and `confirm: "APPLY"` unless noted.

| Tool | API | Notes |
| --- | --- | --- |
| `run_ncc` | `POST /api/v1/runs/trigger` | Requires APPLY; no password/username/`insecure_skip_verify` |
| `replay_reports` | trigger with `--replay` | Needs `config_path` |
| `create_schedule` | `PUT /api/v1/schedule` | `command` always refused; `print_only` skips APPLY |
| `delete_schedule` | `PUT /api/v1/schedule` remove | |
| `apply_upgrade` | `POST /api/v1/settings/update/apply` | No checksum skip |
| `restart_v2_stack` | `POST /api/v1/health/supervisor/restart` | |
| `repair_v2_stack` | `POST /api/v1/health/autoheal` | |
| `restore_v2_backup` | `POST /api/v1/settings/restore` | Needs `input_file` |
| `backup_v2_stack` | `GET /api/v1/settings/backup` | |

## Always refused over MCP

`bootstrap_v2_stack`, `install_service`, `uninstall_service`, `start_v2_stack`, `stop_v2_stack`, password reset, custom orchestrator argv.

`orchestrator_action` allowlist: `version`, `env-info`, `terms`, `v2-status`, `v2-check`, `verify`, `config-schema`, `validate-config`, `validate-secrets`, `preflight-check`.

## Resources

- `ncc://run-summary`
- `ncc://report`
- `ncc://config-schema`
- `ncc://runs/{run_id}/summary`
- `ncc://runs/{run_id}/report/{file}`

## Env

| Variable | Required | Purpose |
| --- | --- | --- |
| `NCC_MCP_TOKEN` | yes | Dashboard PAT (`ncc_pat_…`), ≥16 chars |
| `NCC_MCP_API_URL` | no | Default `http://127.0.0.1:8081`. `http://` only on loopback |
| `NCC_MCP_PROFILE` | no | `readonly` / `operator` / `admin` |
| `NCC_MCP_ALLOW_MUTATIONS` | no | Must be `true` for APPLY tools |

Audit on the API: `GET /api/v1/audit?channel=mcp` (`action=mcp.call`).
