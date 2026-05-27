# Nutanix NCC Orchestrator

[![Version](https://img.shields.io/badge/version-2.0.0-blue)](RELEASE_NOTES_v2.0.0.md)
[![Go](https://img.shields.io/badge/go-1.26.3-00ADD8)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/release-GA-success)](docs/PRODUCTION_READINESS_v2.0.0.md)

> A production-ready stack for running Nutanix Cluster Check (NCC) across many clusters in parallel, aggregating results, and serving them through a hardened API and modern web UI.

---

## Table of contents

- [What's in v2.0.0](#whats-in-v200)
- [Quick start](#quick-start)
  - [From a binary release](#from-a-binary-release-recommended)
  - [From the full v2 stack](#from-the-full-v2-stack-cli--api--ui)
  - [From source](#from-source)
- [Configuration](#configuration)
- [The web UI](#the-web-ui)
- [HTTP API](#http-api)
- [Security posture](#security-posture)
- [Kubernetes](#kubernetes)
- [Exit codes and artifacts](#exit-codes-and-artifacts)
- [Documentation map](#documentation-map)
- [Development](#development)

---

## What's in v2.0.0

| Component                       | Path                       | Purpose                                                                 |
| ------------------------------- | -------------------------- | ----------------------------------------------------------------------- |
| **`ncc-orchestrator`** (CLI)    | `goNCC.go`                 | Parallel NCC execution, retry/backoff, report generation, scheduling   |
| **`ncc-api-server`**            | `cmd/ncc-api-server`       | HTTP API: run control, audit, schedule, artifacts; token auth + CSP    |
| **`ncc-ui-server`** + `frontend`| `cmd/ncc-ui-server`, `frontend/` | Static SPA + reverse proxy to API; serves the dashboard          |
| **`ncc-mcp-server`**            | `cmd/ncc-mcp-server`       | Model Context Protocol bridge so AI tools can drive NCC                |

Each binary is independently runnable. Pick the CLI alone for batch automation, or run the full stack for an interactive dashboard.

---

## Quick start

### From a binary release (recommended)

Every release ships a self-contained `ncc-v2-stack-*` archive on the [Releases page](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases). The archive contains all three server binaries, the SPA bundle, and `example_config.yaml`.

```bash
# 1. Download for your platform (linux-amd64 shown here)
curl -LO https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.0.0/ncc-v2-stack-linux-amd64.tar.gz

# 2. Verify checksum (recommended)
curl -LO https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.0.0/checksums.txt
shasum -a 256 -c checksums.txt --ignore-missing | grep ncc-v2-stack-linux-amd64

# 3. Extract
tar -xzf ncc-v2-stack-linux-amd64.tar.gz && cd ncc-v2-stack-linux-amd64 2>/dev/null || true

# 4. Export the Prism password matching the secret:// ref in the template
export NCC_PASSWORD='your-prism-password'

# 5. Edit clusters/username in example_config.yaml, then validate
./bin/ncc-orchestrator-linux-amd64 validate-config --config example_config.yaml

# 6. Run a one-shot scan
./bin/ncc-orchestrator-linux-amd64 --config example_config.yaml run
```

Reports land under `outputfiles/`. Open `outputfiles/index.html` for the aggregated view.

### From the full v2 stack (CLI + API + UI)

After extracting the archive above, run from the archive root (the layout `bin/`, `frontend-dist/`, `example_config.yaml` is auto-discovered):

```bash
# Sanity check binaries, ports, config readability, output writability
./bin/ncc-orchestrator-linux-amd64 v2-check \
  --install-dir . \
  --orchestrator-bin ./bin/ncc-orchestrator-linux-amd64 \
  --config-path ./example_config.yaml \
  --output-dir ./outputfiles \
  --log-dir ./nccfiles \
  --api-listen :8081 --ui-listen :8080

# Start API + UI together (managed lifecycle, foreground)
./bin/ncc-orchestrator-linux-amd64 v2-start \
  --install-dir . \
  --orchestrator-bin ./bin/ncc-orchestrator-linux-amd64 \
  --config-path ./example_config.yaml \
  --output-dir ./outputfiles \
  --log-dir ./nccfiles \
  --api-listen :8081 --ui-listen :8080
```

Add `--detach --self-heal` to background the stack with restart supervision and PID/log files under `<install-dir>/run` and `<install-dir>/logs`.

Open <http://localhost:8080> — the UI picks up the auto-generated `.ncc-api-token` and authenticates against the API server transparently.

### From source

Requires **Go 1.26.3+** and **Node 20+**.

```bash
git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git
cd Nutanix-ncc-orchestrator

# Build all four binaries (orchestrator, api, ui, mcp)
go build ./...

# Build the SPA
(cd frontend && npm ci && npm run build)

# Run a scan
export NCC_PASSWORD='your-prism-password'
./ncc-orchestrator validate-config --config example_config.yaml
./ncc-orchestrator --config example_config.yaml run
```

Reproducible release build (cross-compile + archives + checksums): see [`binaryGO.txt`](binaryGO.txt) and [`docs/BUILD_FROM_SCRATCH.md`](docs/BUILD_FROM_SCRATCH.md).

---

## Configuration

**[`example_config.yaml`](example_config.yaml)** is the canonical, validator-clean template. Every release archive bundles a copy. Three layered sources are honored (later wins):

1. Config file (YAML or JSON, default `config.yaml`)
2. Environment variables with the **`NCC_`** prefix (e.g. `NCC_MAX_PARALLEL=8`)
3. CLI flags

### Secret handling

Plaintext credentials in config files are explicitly rejected by `validate-secrets`. Use one of:

```yaml
# Option A — env-backed (recommended for CLI/CI)
secrets-provider: env
password: "secret://NCC_PASSWORD"     # then: export NCC_PASSWORD=...

# Option B — file-backed (recommended for hardened deployments)
secrets-provider: file
secrets-file: /run/secrets/ncc.yaml
password: "secret://NCC_PASSWORD"
```

Validate before you run:

```bash
./ncc-orchestrator validate-config  --config example_config.yaml
./ncc-orchestrator validate-secrets --config example_config.yaml
```

**Full key-by-key reference (every config option, every env var, every CLI flag) is in [`docs/FEATURES_AND_CONFIG_FLAGS.md`](docs/FEATURES_AND_CONFIG_FLAGS.md).**

### Policy gates

Turn run metrics into pass/fail rules for CI/CD:

```yaml
policy-gates: "new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>5"
```

Supported metrics: `new-fails`, `resolved-fails`, `fail-rate`, `clusters-failed`, `regressions`, `flaky-checks`, `min-health-score`, `avg-health-score`. Operators: `>`, `>=`, `<`, `<=`, `==`, `!=`. Violations are written to `outputfiles/policy-gates.txt` and the run exits non-zero.

---

## The web UI

After `v2-start`, the dashboard at <http://localhost:8080> gives you:

- **Dashboard** — last-run summary, FAIL/WARN/ERR/INFO counts, context-aware alerts table with "Run in progress" / "All clusters clean" / "No alerts yet" states.
- **Runs** — trigger a new run, follow live output, cancel a stuck run (`DELETE /api/v1/runs/active`), browse archived runs with type/status/duration/clusters/issues columns.
- **Insights** — trends, regressions, flaky checks, drill-down diffs.
- **Settings** — config (Form + Monaco YAML editor), schedule, secrets migration, notifications (test send), audit log, API explorer, raw outputs.

Theme-aware (light/dark/IT-Pro), keyboard-friendly, accessible form fields (every input has `id`/`name`/`htmlFor`/`aria-label`), CSP-locked (`script-src 'self'`).

---

## HTTP API

Major endpoints (full surface at `GET /api/v1/meta/routes`, OpenAPI at `GET /api/v1/openapi.json`):

| Path                                | Methods    | Notes                                                              |
| ----------------------------------- | ---------- | ------------------------------------------------------------------ |
| `/api/v1/health`                    | GET        | Version, build date, paths, auth mode, token source                |
| `/api/v1/runs`                      | GET        | List runs (`?source=history\|summary\|trigger`, `?since=RFC3339`) |
| `/api/v1/runs/{id}`                 | GET        | Single archived run + embedded artifacts                          |
| `/api/v1/runs/active`               | GET, DELETE| Active run snapshot; DELETE cancels a stuck run                   |
| `/api/v1/runs/trigger`              | POST       | Start a run (single-flight: 409 on second concurrent trigger)     |
| `/api/v1/runs/preflight`            | POST       | Machine-readable preflight with `remediation_code`                |
| `/api/v1/runs/summary`              | GET        | Latest run summary                                                |
| `/api/v1/settings/config`           | GET, PUT   | Read/write runtime config (redacted on read)                      |
| `/api/v1/settings/notifications`    | GET, PUT   | Notifications state                                               |
| `/api/v1/schedule`                  | GET, PUT   | Cron/every schedule; PUT requires `cron` or `every`               |
| `/api/v1/schedule/health`           | GET        | `last_run`, `last_success`, `last_error`, lock/log paths          |
| `/api/v1/audit`                     | GET        | Audit log (`?limit=N`, `?action=…`, `?failures=true`)             |
| `/api/v1/artifacts`, `/{name}`      | GET        | List/read generated artifacts (size-capped inline, `?download=1`) |
| `/api/v1/report/data`, `/trends`    | GET        | Aggregated report data and trend points                           |
| `/api/v1/logs/runner`               | GET        | Tail of `ncc-runner.log`                                          |
| `/api/v1/metrics/rate-limit`        | GET        | Per-route rate-limiter counters                                   |
| `/api/v1/auth/session`, `/rotate`   | POST       | Issue session token / rotate the API token                        |

All write/mutate routes require `X-API-Token: <token>` (or `Authorization: Bearer …` session token). Errors return a structured envelope with `success: false`, `error`, and `error_code` (e.g. `NCC_API_UNAUTHORIZED`, `NCC_API_BAD_REQUEST`, `NCC_API_NOT_FOUND`, `NCC_API_CONFLICT`).

Trigger a run from the API:

```bash
TOKEN=$(cat .ncc-api-token)
curl -s -X POST http://localhost:8081/api/v1/runs/trigger \
  -H "X-API-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"config":"example_config.yaml","extra_args":["--output-dir","outputfiles"]}'
```

`extra_args` is allowlisted server-side; only safe flags pass through.

---

## Security posture

| Area                  | Default                                                                                                                |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Authentication        | Token-based (`X-API-Token`) using constant-time compare (`crypto/subtle`); session tokens HMAC-signed                  |
| CORS                  | Strict allowlist (default `http://localhost:8080`); wildcard origins rejected at startup                               |
| CSP                   | UI: `script-src 'self'`, no `unsafe-eval`; API: `default-src 'none'`                                                   |
| Transport             | Optional direct HTTPS (`--tls-cert-file`/`--tls-key-file`); optional mTLS (`--tls-client-ca-file`)                     |
| Security headers      | `X-Content-Type-Options`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Permissions-Policy`, HSTS on TLS   |
| Rate limiting         | Per-client token bucket on sensitive auth/mutation routes (`--rate-limit-per-minute`, default 60)                      |
| Path confinement      | All file I/O canonicalized under `--repo-root`; `..` and embedded `/` rejected on `/artifacts/{name}` and `/runs/{id}` |
| Secrets               | `secret://NAME` refs with `env` or `file` provider; plaintext-in-config triggers a startup warning                     |
| Vulnerability scans   | `govulncheck ./...` and `npm audit --omit=dev` both clean as of v2.0.0 (Go 1.26.3, DOMPurify ≥ 3.4.7 enforced)         |

Full release-gate checklist with evidence: [`docs/PRODUCTION_READINESS_v2.0.0.md`](docs/PRODUCTION_READINESS_v2.0.0.md).

---

## Kubernetes

```bash
# Full stack (CronJob runner + API + UI + NetworkPolicies)
kubectl apply -k k8s/
```

Includes default-deny ingress and scoped allow policies for UI/API. See **[`k8s/README.md`](k8s/README.md)** for architecture, runbook, and rollback. Helm chart (CronJob only) at [`helm/ncc-orchestrator`](helm/ncc-orchestrator/README.md).

Uninstall:

```bash
./scripts/uninstall-v2-clean.sh --dry-run   # preview
./scripts/uninstall-v2-clean.sh --force     # apply
```

---

## Exit codes and artifacts

The `ncc-orchestrator` CLI exits with:

| Code | Meaning                                                                                  |
| ---- | ---------------------------------------------------------------------------------------- |
| `0`  | Success — every cluster completed                                                        |
| `1`  | Fatal runner error                                                                       |
| `2`  | Configuration invalid (same as `validate-config` failure)                                |
| `3`  | Partial success — at least one cluster succeeded and at least one failed                |

Artifacts emitted under `outputfiles/`:

- `index.html` — aggregated dashboard report
- `run-summary.json` / `ncc-run-record.json` — machine-readable run result + metadata
- `checks-snapshot.json`, `drilldown-diff.json`, `flaky-checks.json`
- `regression-summary.json`, `slo-dashboard.json`
- `policy-gates.txt` — written only when one or more policy rules are violated
- `<cluster>.html` / `.csv` / `.json` / `.md` / `.sarif` — per-cluster outputs

Raw NCC summaries land under `nccfiles/`. Runner JSON logs under `logs/ncc-runner.log` (rotated).

---

## Documentation map

| Doc                                                                                  | When to read it                                                       |
| ------------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| **[`example_config.yaml`](example_config.yaml)**                                     | Copy → edit → run                                                     |
| [`docs/FEATURES_AND_CONFIG_FLAGS.md`](docs/FEATURES_AND_CONFIG_FLAGS.md)              | Every config key, env var, and CLI flag with examples                 |
| [`docs/BUILD_FROM_SCRATCH.md`](docs/BUILD_FROM_SCRATCH.md)                            | Build the whole stack on a clean machine                              |
| [`docs/ARCHITECTURE_AND_HANDOVER.md`](docs/ARCHITECTURE_AND_HANDOVER.md)              | Engineering handover, component boundaries                            |
| [`docs/MIGRATION_v1_TO_v2.md`](docs/MIGRATION_v1_TO_v2.md)                            | Moving from a v1 deployment                                           |
| [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)                                  | TLS, Prism Central, API issues                                        |
| [`docs/MCP_SERVER.md`](docs/MCP_SERVER.md)                                            | Wire the orchestrator into AI tools via MCP                           |
| [`docs/PRODUCTION_READINESS_v2.0.0.md`](docs/PRODUCTION_READINESS_v2.0.0.md)          | Release gate evidence and checklists                                  |
| [`docs/RELEASE_CHECKSUMS.md`](docs/RELEASE_CHECKSUMS.md)                              | How `--update` verifies downloads                                     |
| [`k8s/README.md`](k8s/README.md)                                                     | Kubernetes deployment guide                                           |
| [`Prometheus.md`](Prometheus.md)                                                     | Prometheus textfile-collector setup                                   |
| [`CHANGELOG.md`](CHANGELOG.md)                                                       | Full version history                                                  |
| [`RELEASE_NOTES_v2.0.0.md`](RELEASE_NOTES_v2.0.0.md)                                  | What changed since v1.1.0                                             |

---

## Development

```bash
git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git
cd Nutanix-ncc-orchestrator

# Backend
go build ./...
go test  -count=1 -race -timeout=180s ./...
go vet   ./...
gofmt -l .

# Frontend
cd frontend
npm ci
npm run build
npx tsc --noEmit
```

Contribution guidelines: [`CONTRIBUTING.md`](CONTRIBUTING.md).

### MCP server (AI assistants)

```bash
go build -o ncc-mcp-server ./cmd/ncc-mcp-server
```

Add the resulting binary in your MCP client (Cursor, Claude Desktop, etc.) — see [`docs/MCP_SERVER.md`](docs/MCP_SERVER.md).

---

## Release status

- **Current GA:** [`v2.0.0`](RELEASE_NOTES_v2.0.0.md)
- **Build provenance:** every binary embeds `Version`, `BuildDate`, `Stream`, `GoVersion`, and the git revision (via `-buildvcs=true`); inspect with `./ncc-orchestrator version`.
- **Checksums:** `dist/checksums.txt` (or the `checksums.txt` attached to the GitHub release) — SHA-256, sorted, includes every binary, every stack archive, `example_config.yaml`, and `RELEASE_NOTES_v2.0.0.md`.
- **Docker:** `prajwalnutant/nutanix-ncc-orchestrator:2.0.0` (and `:latest`).

---

## Author

Prajwal Vernekar — [prajwal.vernekar@nutanix.com](mailto:prajwal.vernekar@nutanix.com)

## License

[MIT](LICENSE). Use at your own risk; this tool interacts with Nutanix APIs — ensure you have proper permissions.
