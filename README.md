# Nutanix NCC Orchestrator

A CLI tool to run NCC (Nutanix Cluster Check) across multiple clusters in parallel, aggregate results, and generate HTML/CSV reports. Built in Go for efficiency and cross-platform support.

**Contents:** [Build From Scratch](#build-from-scratch) · [Features](#features) · [Installation](#installation) · [Usage](#usage) · [Configuration](#configuration) · [Policy Gates](#policy-gates) · [Kubernetes](#kubernetes-deployment) · [Scripts](#scripts) · [Building](#building-and-contributing)

**Full reference:** [docs/FEATURES_AND_CONFIG_FLAGS.md](docs/FEATURES_AND_CONFIG_FLAGS.md) (all features, config keys, and CLI flags with examples)

---

## Build From Scratch

If you want to build and run the full application from a clean machine (CLI + API + UI + frontend + Kubernetes), use:

- **Canonical guide:** [docs/BUILD_FROM_SCRATCH.md](docs/BUILD_FROM_SCRATCH.md)
- **Engineering handover architecture:** [docs/ARCHITECTURE_AND_HANDOVER.md](docs/ARCHITECTURE_AND_HANDOVER.md)

It includes:

- toolchain prerequisites
- source builds for all binaries
- frontend build/dev flow
- local v2 stack bring-up
- validation/tests
- release packaging basics
- Kubernetes deployment and verification

For developer takeover, architecture and operational ownership boundaries are documented in `docs/ARCHITECTURE_AND_HANDOVER.md`.

---

## Release status

- **Current target release:** `v2.0.0`
- **Release notes:** [RELEASE_NOTES_v2.0.0.md](RELEASE_NOTES_v2.0.0.md)
- **Production readiness checklist:** [docs/PRODUCTION_READINESS_v2.0.0.md](docs/PRODUCTION_READINESS_v2.0.0.md)
- **Checksums + update verification:** [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md)

---

## Author

Prajwal Vernekar (prajwal.vernekar@nutanix.com)

---

## Features
- **Parallel execution** on multiple Nutanix clusters via Prism Gateway API (start checks, poll status, fetch summaries).
- **Configurable** via YAML/JSON config file, environment variables (`NCC_*`), or CLI flags.
- **Outputs**: HTML/CSV/JSON/Markdown/SARIF, aggregated `index.html`, drill-down diff, flaky-check report, and SLO dashboard exports.
- **Reliability**: Retry logic, adaptive parallelism on 429, progress bars, rotated JSON logging, and a preflight write-permission check.
- **Security hardening**: strict path confinement, preflight remediation codes, API rate limiting for sensitive routes, checksum-enforced updater flow, and Kubernetes NetworkPolicy defaults.
- **Replay mode** (`--replay`): Regenerate reports from existing logs without calling the NCC API.
- **Notifications**: Optional email, webhook, and Slack notifications with quiet-hours / maintenance-window suppression.
- **Policy gates**: Fail runs based on automation rules (`policy-gates`) such as `new-fails>0`, `fail-rate>2`, `min-health-score<90`.
- **Secrets support**: `secret://` references resolved from env or file-backed secret map.
- **Prometheus**: Writes `.prom` files for scraping; see [Prometheus.md](Prometheus.md) for monitoring setup.

## v2 Backend + Frontend

`v2.0.0` ships as a split stack:

- **Runner binary**: `ncc-orchestrator` (core NCC execution + artifacts)
- **Backend API**: `cmd/ncc-api-server` (run control, config/schedule APIs, artifact reads)
- **UI proxy/static server**: `cmd/ncc-ui-server` (serves built frontend, proxies `/api/v1/*`)
- **Frontend app**: `frontend/` (React + Vite + TypeScript)

### How it works

1. The frontend calls `/api/v1/*` on the UI server.
2. The UI server proxies those calls to `ncc-api-server` and injects auth (token/session).
3. The API server triggers `ncc-orchestrator` with `--config <file>` (plus allowlisted extra args).
4. `ncc-orchestrator` writes artifacts under `outputfiles/` and raw logs under `nccfiles/`.
5. API endpoints expose run state and artifacts for dashboard/report views.

### API endpoints (major)

- `GET /api/v1/health`
- `GET|PUT /api/v1/settings/config`
- `GET|PUT /api/v1/settings/notifications`
- `POST /api/v1/settings/notifications/test`
- `GET|PUT /api/v1/schedule`
- `GET /api/v1/schedule/health`
- `GET /api/v1/runs`, `GET /api/v1/runs/summary`, `GET /api/v1/runs/active`
- `POST /api/v1/runs/trigger`
- `POST /api/v1/runs/preflight`
- `GET /api/v1/report/data`
- `GET /api/v1/report/trends?limit=30`
- `GET /api/v1/openapi.json`
- `GET /api/v1/artifacts`, `GET /api/v1/artifacts/{name}`
- `GET /api/v1/logs/runner`

### Run backend API server

```bash
go run ./cmd/ncc-api-server \
  --listen :8081 \
  --repo-root . \
  --config-path config.yaml \
  --output-dir outputfiles \
  --log-dir nccfiles \
  --orchestrator-bin ./ncc-orchestrator \
  --rate-limit-per-minute 60 \
  --auth-mode token \
  --cors-origin http://localhost:8080
```

### Backend path requirements (important)

`ncc-api-server` must know where your repository data and runner binary are located. These flags are required for a reliable setup:

- `--repo-root`: base directory used to confine and resolve file paths
- `--orchestrator-bin`: path to the `ncc-orchestrator` executable the API server launches
- `--config-path`: config file used for trigger runs (`--config <path>`)
- `--output-dir`: generated report/artifact directory (for `/api/v1/report/data`, `/api/v1/artifacts`)
- `--log-dir`: raw NCC logs directory
- `--runner-log-path`: orchestrator runtime log file path (for `/api/v1/logs/runner`)

If you run from repository root with local binaries:

```bash
go run ./cmd/ncc-api-server \
  --repo-root . \
  --orchestrator-bin ./ncc-orchestrator \
  --config-path ./config.yaml \
  --output-dir ./outputfiles \
  --log-dir ./nccfiles \
  --runner-log-path ./logs/ncc-runner.log
```

If backend and binary are installed in different locations:

```bash
/opt/ncc/bin/ncc-api-server \
  --listen :8081 \
  --repo-root /srv/ncc \
  --orchestrator-bin /opt/ncc/bin/ncc-orchestrator \
  --config-path /srv/ncc/config/prod.yaml \
  --output-dir /srv/ncc/outputfiles \
  --log-dir /srv/ncc/nccfiles \
  --runner-log-path /srv/ncc/logs/ncc-runner.log
```

Quick verification:

```bash
curl -sS http://localhost:8081/api/v1/health
curl -sS -H "Authorization: Bearer $(cat .ncc-api-token)" http://localhost:8081/api/v1/artifacts
```

### Preflight machine-readable remediation

`ncc-orchestrator preflight-check --format json` now emits `remediation_code` for non-pass checks.  
The same structure is surfaced in `/api/v1/runs/preflight` and consumed by the v2 UI.

Example (truncated):

```json
{
  "id": "validate-secrets",
  "status": "fail",
  "remediation_code": "NCC_PREFLIGHT_VALIDATE_SECRETS",
  "hint": "Set secrets-provider and ensure secret sources are accessible."
}
```

### Run frontend

Dev mode:

```bash
cd frontend
npm install
npm run dev
```

Built mode via UI server:

```bash
cd frontend
npm install
npm run build

go run ../cmd/ncc-ui-server \
  --listen :8080 \
  --dir ./dist \
  --backend-url http://localhost:8081 \
  --api-token-file ../.ncc-api-token \
  --api-auth-mode token
```

### `ncc-orchestrator` binary examples (flags)

Direct run:

```bash
./ncc-orchestrator \
  --config config.yaml \
  --clusters "10.38.66.37,10.38.66.7" \
  --username admin \
  --password "$NCC_PASSWORD" \
  --outputs html,csv,json,markdown,sarif \
  --max-parallel 4 \
  --policy-gates "new-fails>0,fail-rate>2,min-health-score<90"
```

Replay artifacts without calling NCC APIs:

```bash
./ncc-orchestrator --config config.yaml --replay --outputs html,csv,json
```

Config/secret preflight:

```bash
./ncc-orchestrator validate-config --config config.yaml
NCC_SECRETS_PROVIDER=env ./ncc-orchestrator validate-secrets --config config.yaml
```

v2 runtime self-check before startup:

```bash
./ncc-orchestrator v2-check \
  --install-dir .ncc-v2 \
  --config-path /absolute/path/config.yaml \
  --output-dir /absolute/path/outputfiles \
  --log-dir /absolute/path/nccfiles \
  --token-file /absolute/path/.ncc-api-token \
  --api-listen :8081 \
  --ui-listen :8080
```

This validates binary executability, config readability, directory writability, and API/UI port bind availability before `v2-start`.

### Trigger run through backend API (with extra flags)

```bash
curl -sS -X POST "http://localhost:8081/api/v1/runs/trigger" \
  -H "Authorization: Bearer $(cat .ncc-api-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "config_path": "config.yaml",
    "password": "REDACTED_OR_EMPTY",
    "extra_args": ["--output-dir","outputfiles","--prom-dir","promfiles","--no-html"]
  }'
```

`extra_args` are restricted by backend hardening (allowlist + value sanitization), so only specific flags are accepted.

### Trend API example

```bash
curl -sS "http://localhost:8081/api/v1/report/trends?limit=30" \
  -H "Authorization: Bearer $(cat .ncc-api-token)"
```

This returns chronological points built from `run-summary.json` (current + run-history snapshots) with totals for FAIL/WARN/ERR/INFO and health/check aggregates.

### Scheduler health API example

```bash
curl -sS "http://localhost:8081/api/v1/schedule/health" \
  -H "Authorization: Bearer $(cat .ncc-api-token)"
```

This endpoint reports scheduler state and runtime hints (`last_run`, `last_success`, `last_error`) from the scheduler log, and includes lock/log paths used by cron-based schedules.

## Installation

### Prerequisites
- **Go 1.26+** (for building from source; see [go.mod](go.mod)).
- **Nutanix Prism** API access (username, password, cluster IPs).

### From Source
1. Clone the repo: `git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git`
2. Navigate to the directory: `cd Nutanix-ncc-orchestrator`
3. Build (release-style metadata): `go build -ldflags "-w -s -X main.BuildDate=$(date -u '+%Y-%m-%dT%H:%M:%SZ') -X main.Stream=Release -X main.GoVersion=$(go version | cut -d ' ' -f 3)" -o ncc-orchestrator`  
   Official Docker images from CI use `Stream=Release` and set `Version` from the [VERSION](VERSION) file.
4. Run: `./ncc-orchestrator --help` or `./ncc-orchestrator version`
   
  > Add .exe for windows binary.

### Binary Releases
Download pre-built binaries for Linux/Windows/macOS from the [Releases](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases) page.

### Docker image and CI
The [GitHub Action](.github/workflows/docker-publish.yml) builds and pushes the image to Docker Hub on push to `main` (and on release). The **image tag is the same as the code version**:

- **Version source**: the [`VERSION`](VERSION) file (e.g. `2.0.0`). Update this file when you want to release a new image version.
- **Triggers**: push to `main` (when Go code, Dockerfile, or VERSION change) and on GitHub release.
- **Image**: `prajwalnutant/nutanix-ncc-orchestrator:<version>` and `:latest`.
- **Secrets**: In the repo settings, add `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (Docker Hub → Account → Security → New Access Token) so the workflow can push.

## Usage
Basic command:
- `ncc-orchestrator --clusters "10.0.1.1,10.0.2.1" --username admin --password yourpassword`

Full options: Run `ncc-orchestrator --help` for all flags and subcommands. To see current env values: `ncc-orchestrator env-info`. Run `ncc-orchestrator version` to print version, stream, build date, and Go version, then exit. Run `ncc-orchestrator update` to fetch and update binaries (or `ncc-orchestrator update --check` for check-only mode). By default, updates stay on the current major track (`v1.x` -> latest `v1.x`); use `--allow-major-upgrade` to move to `v2.x` after reviewing migration steps in [docs/V2_BACKEND_FRONTEND_MVP.md](docs/V2_BACKEND_FRONTEND_MVP.md). You can also check/use non-GitHub binaries with `--binary-url`; installs from direct URLs require `--binary-sha256` for integrity verification. Set `GITHUB_TOKEN` for higher GitHub API rate limits. Release downloads now require checksum verification before replace. On Windows the new binary is written as `ncc-orchestrator.new.exe`; replace the old exe and run again. Release maintainers: see [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md) for checksum publishing guidance.

For cleanup, use `ncc-orchestrator uninstall` to remove local NCC artifacts/runtime state/schedule entries (including v2 bootstrap binaries/scripts in `--install-dir`). Kubernetes uninstall remains script-only via `scripts/uninstall-v2-clean.sh`.

### Exit codes

| Code | Meaning |
|------|--------|
| **0** | Success — every cluster ran to completion without a runner-level failure |
| **1** | Error — e.g. all clusters failed, or another fatal error |
| **2** | **Configuration** — invalid or missing config (same as `--dry-run` validation failures) |
| **3** | **Partial success** — at least one cluster succeeded and at least one failed (reports written for successful clusters) |

After each run, **`outputfiles/run-summary.json`** includes `exit_code` and per-cluster `clusters` (address, `ok`, severity counts, `error` if failed). **`outputfiles/ncc-run-record.json`** adds `schema_version` and orchestrator metadata around the same run payload. See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for TLS, Prism Central, and API issues.

### Configuration
Config file (YAML/JSON), CLI flags, and **environment variables** (prefix `NCC_`) are supported. Env overrides config file; flags override both.

Create a `config.yaml` with any of the options below. Run with: `ncc-orchestrator --config config.yaml`

| Option | Default | Description |
|--------|---------|-------------|
| `clusters` | — | Comma-separated Prism cluster IPs or FQDNs |
| `clusters-file` | — | Path to cluster file; each line supports `cluster` or `cluster,username[,password]` (overrides `clusters` when set); use with **discover-clusters** to populate |
| `username` | `admin` | Prism Gateway username |
| `password` | — | Prism password (prefer env `NCC_PASSWORD`) |
| `ncc-api-version` | `v4` | **`v4`** (current Nutanix v4 APIs; see README) or **`Legacy`** (Prism Gateway v1 start-checks only). **`v1`** is accepted as an alias for Legacy. |
| `nutanix-v4-api-version` | `v4.2` | Path revision for `/api/clustermgmt/{ver}/` and `/api/monitoring/{ver}/` (e.g. `v4.2`, `v4.1`, `v4.0.a1`); CLI: `--nutanix-v4-api-version` |
| `insecure-skip-verify` | `false` | Skip TLS verify (lab/self-signed only) |
| `timeout` | `15m` | Per-cluster overall timeout |
| `request-timeout` | `20s` | Per HTTP request timeout |
| `poll-interval` | `15s` | Polling interval for NCC task status |
| `poll-jitter` | `2s` | Jitter added to poll interval |
| `max-parallel` | `4` | Max concurrent clusters |
| `outputs` | `html,csv` | Comma-separated: html, csv, json, markdown, sarif |
| `output-dir-logs` | `nccfiles` | Directory for raw NCC summary logs |
| `output-dir-filtered` | `outputfiles` | Directory for filtered HTML/CSV |
| `single-report` | `false` | Also write a single-file report copy (`ncc-report-single.html`) |
| `log-file` | `logs/ncc-runner.log` | Rotated JSON log path |
| `log-level` | — | 0–5 or trace/debug/info/warn/error |
| `log-http` | `false` | Dump HTTP request/response (debug) |
| `retry-max-attempts` | `6` | Max retries per HTTP call |
| `retry-base-delay` | `400ms` | Base backoff delay |
| `retry-max-delay` | `8s` | Max backoff cap |
| `prom-dir` | `promfiles` | Directory for Prometheus .prom files |
| `run-history` | `false` | Save each run snapshot (`index.html`, summaries) into `run-history-dir` |
| `run-history-dir` | `<output-dir-filtered>/runs` | Base directory for timestamped run snapshots |
| `retain-last` | `0` | Keep only last N history snapshots when run-history is enabled (`0` = unlimited) |
| `retain-days` | `0` | Keep history snapshots newer than N days (`0` = unlimited) |
| `notify-on-regression` | `false` | Send notifications only when FAIL count increases vs previous run-summary |
| `adaptive-parallelism` | `true` | Dynamically reduce/increase effective concurrency based on HTTP 429 |
| `policy-gates` | — | Comma-separated policy rules (e.g. `new-fails>0,fail-rate>2,min-health-score<90`) |
| `quiet-hours` | — | Suppress notifications during local window `HH:MM-HH:MM` |
| `maintenance-windows` | — | Suppress notifications during RFC3339 windows `start/end[,start/end...]` |
| `flaky-lookback-runs` | `6` | Number of recent snapshots to inspect for flaky checks |
| `flaky-min-transitions` | `2` | Minimum severity transitions to classify a check as flaky |
| `severity-filter` | — | Comma-separated FAIL,WARN,ERR,INFO; empty = all |
| `dry-run` | `false` | Validate config only, no checks |
| `replay` | `false` | Replay from existing logs (no NCC API) |
| `max-idle-conns` | `100` | HTTP client connection pool: max idle conns total |
| `max-idle-conns-per-host` | `10` | Max idle conns per host |
| `max-conns-per-host` | `0` | Max conns per host (0 = unlimited) |
| `idle-conn-timeout` | `90s` | Idle connection timeout before close |
| `email-enabled` | `false` | Enable email notifications |
| `email-attach-html` | `false` | Attach per-cluster (or digest) HTML report to notification email |
| `notify-digest` | `false` | Send one email/webhook/Slack per run with run overview (and optional index.html attach) instead of per-cluster |
| `smtp-server`, `smtp-port`, `smtp-user`, `smtp-password`, `email-from`, `email-to`, `email-use-tls` | — | SMTP settings |
| `webhook-enabled` | `false` | Enable webhook notifications |
| `webhook-include-html` | `false` | Include per-cluster HTML report as base64 in webhook JSON (brief overview always in payload) |
| `webhook-url`, `webhook-headers` | — | Webhook endpoint and headers |
| `slack-enabled` | `false` | Enable Slack notifications |
| `slack-webhook-url`, `slack-channel` | — | Slack webhook and channel |
| `secrets-provider` | — | Secret source for `secret://` refs: `env` or `file` |
| `secrets-file` | — | YAML/JSON key-value secret map path when `secrets-provider=file` |

### Environment variables (NCC_ prefix)
Any config key can be set via env: **`NCC_`** + key in UPPER_SNAKE (hyphens become underscores). Examples:

- `NCC_CONFIG` — Config file path  
- `NCC_CLUSTERS`, `NCC_CLUSTERS_FILE` — Cluster list or path to file (one per line)  
- `NCC_PRISM_CENTRAL_URL` — Prism Central URL for **discover-clusters**  
- `NCC_DISCOVER_API_VERSION` — `v4` (default) or `v3` for **discover-clusters**  
- `NCC_USERNAME`, `NCC_PASSWORD` — Prism credentials  
- `NCC_NCC_API_VERSION` — Same as `ncc-api-version` (`v4` default, or `Legacy` / `v1`)  
- `NCC_NUTANIX_V4_API_VERSION` — Same as `nutanix-v4-api-version` (default `v4.2`; e.g. `v4.1`, `v4.0.a1`)  
- `NCC_INSECURE_SKIP_VERIFY` — true/false  
- `NCC_TIMEOUT`, `NCC_REQUEST_TIMEOUT`, `NCC_POLL_INTERVAL`, `NCC_POLL_JITTER`  
- `NCC_MAX_PARALLEL`, `NCC_OUTPUTS`  
- `NCC_OUTPUT_DIR_LOGS`, `NCC_OUTPUT_DIR_FILTERED`, `NCC_LOG_FILE`, `NCC_LOG_LEVEL`, `NCC_LOG_HTTP`  
- `NCC_RETRY_MAX_ATTEMPTS`, `NCC_RETRY_BASE_DELAY`, `NCC_RETRY_MAX_DELAY`  
- `NCC_PROM_DIR`, `NCC_SINGLE_REPORT`, `NCC_RUN_HISTORY`, `NCC_RUN_HISTORY_DIR`, `NCC_RETAIN_LAST`, `NCC_RETAIN_DAYS`, `NCC_NOTIFY_ON_REGRESSION`, `NCC_ADAPTIVE_PARALLELISM`, `NCC_POLICY_GATES`, `NCC_QUIET_HOURS`, `NCC_MAINTENANCE_WINDOWS`, `NCC_FLAKY_LOOKBACK_RUNS`, `NCC_FLAKY_MIN_TRANSITIONS`, `NCC_SEVERITY_FILTER`, `NCC_DRY_RUN`, `NCC_REPLAY`  
- `NCC_MAX_IDLE_CONNS`, `NCC_MAX_IDLE_CONNS_PER_HOST`, `NCC_MAX_CONNS_PER_HOST`, `NCC_IDLE_CONN_TIMEOUT`  
- `NCC_EMAIL_ENABLED`, `NCC_EMAIL_ATTACH_HTML`, `NCC_NOTIFY_DIGEST`, `NCC_SMTP_SERVER`, `NCC_SMTP_PORT`, `NCC_SMTP_USER`, `NCC_SMTP_PASSWORD`, `NCC_EMAIL_FROM`, `NCC_EMAIL_TO`, `NCC_EMAIL_USE_TLS`  
- `NCC_WEBHOOK_ENABLED`, `NCC_WEBHOOK_INCLUDE_HTML`, `NCC_WEBHOOK_URL`, `NCC_WEBHOOK_HEADERS`  
- `NCC_SLACK_ENABLED`, `NCC_SLACK_WEBHOOK_URL`, `NCC_SLACK_CHANNEL`, `NCC_SECRETS_PROVIDER`, `NCC_SECRETS_FILE`  

Run `ncc-orchestrator env-info` to print all possible env vars and their current values.

### Run summary and discover-clusters
- After each run, **`outputfiles/run-summary.json`** and **`outputfiles/ncc-run-record.json`** are written (machine-readable run result; the latter includes `schema_version` and orchestrator version).
- Additional automation artifacts are written to `outputfiles/`: **`checks-snapshot.json`**, **`drilldown-diff.json`**, **`flaky-checks.json`**, **`slo-dashboard.json`**, and **`policy-gates.txt`** (when policy violations occur).
- **`ncc-orchestrator discover-clusters`** — Lists clusters from **Prism Central**. **Default:** `GET /api/clustermgmt/{ver}/config/clusters` where `{ver}` is **`nutanix-v4-api-version`** (default **`v4.2`**; set e.g. `v4.0.a1` to match your environment), with `$page` / `$limit` pagination; addresses are taken from `network.externalAddress` (IPv4, then IPv6), then node CVM IP, then `name`. Use **`--discover-api-version v3`** for legacy `POST /api/nutanix/v3/clusters/list`. If v4 returns **404**, the command **falls back to v3** automatically. Requires `--prism-central-url` (or config). `https://` is required by default; `http://` endpoints are only allowed when `--insecure-skip-verify=true`. Use **`--format table`** for columns (NAME, EXT_ID, ADDRESS, API) or **`--format json`** for automation. Example:
  `ncc-orchestrator --config config.yaml discover-clusters --output clusters.txt`
  `ncc-orchestrator discover-clusters --prism-central-url https://pc:9440 --format table`
  then set `clusters-file: clusters.txt` in config for the main run. Env: **`NCC_DISCOVER_API_VERSION`** (`v4` or `v3`).

### Cluster file (`clusters-file`) format

Use `clusters-file` when you manage many clusters or generate cluster lists dynamically.

- Supported line formats:
  - `cluster`
  - `cluster,username`
  - `cluster,username,password`
- Blank lines are ignored
- Lines starting with `#` are treated as comments and ignored
- `clusters-file` takes precedence over `clusters`
- Per-line username/password override global `username`/`password` for that cluster
- Duplicate entries are rejected by validation

Example `clusters.txt`:

```text
# NCC target clusters
10.38.66.37
10.38.66.7,admin
pc-aos01.example.local,admin,secret://pc_aos01_password
```

Config example:

```yaml
clusters-file: "clusters.txt"
```

CLI example:

```bash
ncc-orchestrator --clusters-file clusters.txt --username admin --password "$NCC_PASSWORD"
```

Example with only per-cluster credentials in file:

```bash
ncc-orchestrator --clusters-file clusters.txt
```

Validation tip:

```bash
ncc-orchestrator validate-config --config config.yaml
```

- **`ncc-orchestrator create-schedule`** — Creates periodic execution for NCC:
  - Actions: `--action create|list|remove|run-now` (default `create`)
  - Linux/macOS: cron entry (`--type cron`) using `--cron "15 */4 * * *"` or derived from `--every 4h`
  - Windows: Scheduled Task (`--type windows`) using `--every` interval
  - Preview first with `--print-only` (default `true`), then set `--print-only=false` to apply create/remove.
  - `--action run-now` executes the configured command immediately for validation.
- **`ncc-orchestrator config-schema`** — Prints JSON schema for config keys (use `--output` to write file).
- **`ncc-orchestrator validate-config --config <path>`** — Validates config and exits (automation-friendly).

### Policy Gates

`policy-gates` turns run metrics into enforceable pass/fail rules for CI/CD and release checks.

- **Syntax**: comma-separated expressions in the form `<metric><operator><number>`
  - Example: `new-fails>0,fail-rate>2,min-health-score<90`
- **Operators**: `>`, `>=`, `<`, `<=`, `==`, `!=`
- **Behavior**:
  - Each rule is evaluated at end-of-run.
  - If one or more rules are violated, the run fails and writes `outputfiles/policy-gates.txt`.
  - If no rules are violated, run completion behavior is unchanged.

Supported metrics:

- `new-fails` — count of newly introduced FAIL checks vs previous snapshot
- `resolved-fails` — count of checks that were FAIL before and are now resolved
- `fail-rate` — current FAIL percentage (0..100)
- `clusters-failed` — number of clusters with runner-level failure
- `regressions` — `1` when regression is detected, else `0`
- `flaky-checks` — number of flaky checks detected
- `min-health-score` — lowest cluster health score in the run
- `avg-health-score` — average health score across successful clusters

Examples:

```yaml
policy-gates: "new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>5"
```

```bash
ncc-orchestrator --policy-gates "new-fails>0,regressions>0"
```

Recommended usage:

- Start with one or two gates (`new-fails>0`, `fail-rate>2`) and tighten gradually.
- Keep thresholds aligned with environment (prod vs lab) to avoid noisy blocking.
- Pair with `notify-on-regression` if you want both alert suppression and hard policy enforcement.

### Example webhook payload

When webhook is enabled, the app sends a JSON POST with a structure like:

```json
{
  "Cluster": "10.0.1.1",
  "StartedAt": "2025-02-05T10:00:00Z",
  "FinishedAt": "2025-02-05T10:15:00Z",
  "FailCount": 2,
  "WarnCount": 5,
  "ErrCount": 0,
  "InfoCount": 10,
  "TotalChecks": 17,
  "OutputFiles": ["10.0.1.1.log"],
  "Overview": "NCC run completed for cluster 10.0.1.1. FAIL: 2, WARN: 5, ERR: 0, INFO: 10. Total: 17 checks.",
  "ReportHTMLBase64": "<base64-encoded HTML if webhook-include-html is true>"
}
```

In **digest mode** (`notify-digest: true`), one payload per run is sent; `Cluster` is `"run"` and counts reflect clusters OK/failed and total checks.

### Testing email and webhook

**Webhook (no real NCC run):**

1. Get a request-inspection URL, e.g. [webhook.site](https://webhook.site) — open it and copy your unique URL.
2. Ensure you have at least one filtered log so replay can send a payload (e.g. from a previous run: `outputfiles/<cluster>.log` and optionally `nccfiles/<cluster>.log`).
3. Run in **replay** mode with webhook enabled and your URL:

   ```bash
   ./ncc-orchestrator --config config.yaml --replay \
     --webhook-enabled --webhook-url "https://webhook.site/your-unique-id"
   ```

   Or with a config file that has `webhook-enabled: true` and `webhook-url: "https://..."`.  
   On webhook.site you’ll see the POST body (JSON with `Cluster`, `Overview`, `FailCount`, etc.). Use `--webhook-include-html` to also send the report as base64.

**Email:**

1. Use a test SMTP endpoint so you don’t send to real mailboxes:
   - **[Mailtrap](https://mailtrap.io)** or similar: create an inbox, use their SMTP host/port/user/pass in config.
   - **Local MailHog** (Docker): `docker run -d -p 1025:1025 -p 8025:8025 mailhog/mailhog` then SMTP host `localhost`, port `1025`; open http://localhost:8025 to read caught emails.
2. In your config (or flags): `email-enabled: true`, `smtp-server`, `smtp-port`, `smtp-user`, `smtp-password`, `email-from`, `email-to`.
3. Trigger a notification:
   - **Replay** (no NCC API): `./ncc-orchestrator --config config.yaml --replay` (with email settings in config).
   - **Real run**: run against one cluster; when the run finishes, email is sent (or one digest email if `notify-digest: true`).

**Quick webhook test with replay:**

```bash
# 1) One cluster in config, and existing log at outputfiles/<that-cluster>.log (or nccfiles/<cluster>.log so replay can build filtered)
# 2) Set webhook URL (env or config)
export NCC_WEBHOOK_ENABLED=true
export NCC_WEBHOOK_URL="https://webhook.site/your-unique-id"
./ncc-orchestrator --config config.yaml --replay
```

Check the webhook URL page for the POST. Use `--log-level debug` if you need more detail in logs.

## Migration (v1 -> v2)

If you are moving from v1 workflows to the v2 full stack, use:

- **Migration guide:** [docs/MIGRATION_v1_TO_v2.md](docs/MIGRATION_v1_TO_v2.md)
- **Kubernetes guide:** [k8s/README.md](k8s/README.md)

This includes cutover checklist, verification steps, and rollback guidance.

## Kubernetes deployment

Run the NCC Orchestrator on Kubernetes using **`k8s/`** as the single entrypoint (runner + API + UI + frontend serving).

- **Manifests (full stack)**: [`k8s/`](k8s/) — apply with **`kubectl apply -k k8s/`**.
- **Helm (CronJob only):** [`helm/ncc-orchestrator`](helm/ncc-orchestrator/README.md) — templated image tag and schedule.
- **Full guide**: **[k8s/README.md](k8s/README.md)** — detailed architecture, deployment, runbook, troubleshooting, and rollback.
- **Network controls**: default-deny ingress + scoped allow policies for UI/API are included in `k8s/`.

**Quick start (set config and secret first):**

```bash
kubectl apply -k k8s/
```

Report UI: `http://<LoadBalancer-EXTERNAL-IP>`. To **uninstall** (delete namespace and all resources): see [Scripts](#scripts) below.

## Scripts

Helper scripts (run from repo root; set `KUBECONFIG` if needed):

| Script | Purpose |
|--------|---------|
| **[scripts/uninstall-v2-clean.sh](scripts/uninstall-v2-clean.sh)** | Canonical v2 uninstaller. Deletes v2 k8s resources and namespace, supports `--dry-run`, `--keep-pvc`, `--remove-local-state`, `--wait-timeout`, and optional `--prune-images`. |
| **[scripts/uninstall-ncc-orchestrator.sh](scripts/uninstall-ncc-orchestrator.sh)** | Backward-compatible wrapper that delegates to `uninstall-v2-clean.sh`. Existing automation can keep using it. |
| **[scripts/prune-ncc-images-workers.sh](scripts/prune-ncc-images-workers.sh)** | Removes NCC images from worker nodes via SSH. Supports `--dry-run`, `--node-ips`, `--ssh-user`, `--ssh-key`, and `--image-match`. |

Example:

```bash
export KUBECONFIG=~/kubecon/mycluster.conf
./scripts/uninstall-v2-clean.sh --dry-run   # preview
./scripts/uninstall-v2-clean.sh --force     # uninstall
```

## Building and Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## MCP server (AI assistants)

An **MCP server** is provided so AI tools (e.g. Cursor, Claude Desktop) can run NCC, discover clusters, and read run summaries via the [Model Context Protocol](https://modelcontextprotocol.io/). Build with `go build -o ncc-mcp-server ./cmd/ncc-mcp-server/` and add it in your MCP client config. See **[docs/MCP_SERVER.md](docs/MCP_SERVER.md)** for setup and Cursor configuration.

## See also
- [CHANGELOG.md](CHANGELOG.md) — Version history and release notes.
- [docs/FEATURES_AND_CONFIG_FLAGS.md](docs/FEATURES_AND_CONFIG_FLAGS.md) — Full feature and flag reference with examples.
- [Prometheus.md](Prometheus.md) — Prometheus/Grafana monitoring using NCC Orchestrator `.prom` output.
- [k8s/README.md](k8s/README.md) — Full Kubernetes deployment and troubleshooting.

## v2.0.0 components

`v2.0.0` includes the full stack in this branch:

- CLI orchestrator runtime (`goNCC.go`)
- API server (`cmd/ncc-api-server`)
- UI proxy/server (`cmd/ncc-ui-server`)
- React frontend (`frontend`)

## License
MIT License. See [LICENSE](LICENSE) for details.

## Disclaimer
Use at your own risk. This tool interacts with Nutanix APIs—ensure you have proper permissions.
