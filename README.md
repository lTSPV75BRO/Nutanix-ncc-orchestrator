# Nutanix NCC Orchestrator

[![Version](https://img.shields.io/badge/version-2.2.0-blue)](RELEASE_NOTES_v2.2.0.md)
[![Go](https://img.shields.io/badge/go-1.26.4-00ADD8)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/release-development-yellow)](RELEASE_NOTES_v2.2.0.md)

> A production-ready stack for running Nutanix Cluster Check (NCC) across many clusters in parallel, aggregating results, and serving them through a hardened API and modern web UI.

---

## Table of contents

- [What's in this stack](#whats-in-this-stack)
- [Quick start](#quick-start)
  - [From a binary release](#from-a-binary-release-recommended)
  - [From the full v2 stack](#from-the-full-v2-stack-cli--api--ui)
  - [From source](#from-source)
- [Run on macOS / Windows / Linux (trust & verification)](#run-on-macos--windows--linux-trust--verification)
- [Configuration](#configuration)
- [The web UI](#the-web-ui)
- [HTTP API](#http-api)
- [Operability: status, doctor, metrics, completions](#operability-status-doctor-metrics-completions)
- [Running individual components (API only / UI only)](#running-individual-components-api-only--ui-only)
- [Run with Docker Compose](#run-with-docker-compose)
- [Security posture](#security-posture)
- [Kubernetes](#kubernetes)
- [Exit codes and artifacts](#exit-codes-and-artifacts)
- [Documentation map](#documentation-map)
- [Development](#development)

---

## What's in this stack

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
curl -LO https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.1.1/ncc-v2-stack-linux-amd64.tar.gz

# 2. Verify checksum (recommended)
curl -LO https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.1.1/checksums.txt
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

After extracting the archive, the recommended flow (v2.0.2+) is to `cd` into the `bin/` directory and run with no path flags — `v2-check`, `v2-start`, `v2-stop`, and `uninstall` auto-detect the stack root, so config / output / log / token paths default to the archive root automatically:

```bash
# extract once, then:
cd ncc-v2-stack-linux-amd64/bin

# sanity check (auto-detects install-dir from current binary location;
# falls back to <install-dir>/example_config.yaml when config.yaml is absent)
./ncc-orchestrator v2-check --api-listen :8081 --ui-listen :8080

# start API + UI together (managed lifecycle, foreground)
./ncc-orchestrator v2-start --api-listen :8081 --ui-listen :8080

# or background with restart supervision (PID/log files under
# <install-dir>/run and <install-dir>/logs)
./ncc-orchestrator v2-start --detach --self-heal \
  --api-listen :8081 --ui-listen :8080
```

When binding the API to a loopback IP (e.g. `--api-listen 127.0.0.1:8081`), the orchestrator now preserves the IP for connection URLs (so `wait-ready` and the UI backend hit the right address family on macOS, where `localhost` resolves to `::1` first) and additionally adds `http://localhost:port` to the CORS allow-list so browsers can reach the UI under either name.

Older releases (v2.0.0 / v2.0.1) require the explicit form below; it still works in v2.1.1:

```bash
./bin/ncc-orchestrator-linux-amd64 v2-check \
  --install-dir . \
  --orchestrator-bin ./bin/ncc-orchestrator-linux-amd64 \
  --config-path ./example_config.yaml \
  --output-dir ./outputfiles \
  --log-dir ./nccfiles \
  --api-listen :8081 --ui-listen :8080
```

Open <http://localhost:8080> — the UI picks up the auto-generated `.ncc-api-token` and authenticates against the API server transparently.

### From source

Requires **Go 1.26.4+** (matching the `go` directive in [`go.mod`](go.mod)) and **Node 20+**.

```bash
git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git
cd Nutanix-ncc-orchestrator
```

**1. Build the four Go binaries.** `go build ./...` only *compile-checks* the
module — when several `main` packages match, the resulting executables are
discarded. Use explicit `-o` targets to actually produce the binaries:

```bash
go build ./...                                      # fast compile check of everything

go build -o ncc-orchestrator .                      # CLI runner + scheduler + v2 stack manager
go build -o ncc-api-server  ./cmd/ncc-api-server    # REST/JSON API + auth/RBAC backend
go build -o ncc-ui-server   ./cmd/ncc-ui-server     # SPA host + API reverse proxy
go build -o ncc-mcp-server  ./cmd/ncc-mcp-server    # Model Context Protocol server (optional)
```

**2. Build the SPA** (served by `ncc-ui-server`):

```bash
(cd frontend && npm ci && npm run build)
```

**3. (Optional) verify your tree** before running:

```bash
go vet ./...
go test ./...        # add -race for the concurrency-sensitive auth/run suites
```

**4a. Run a one-shot scan** (CLI only, no UI):

```bash
export NCC_PASSWORD='your-prism-password'
./ncc-orchestrator validate-config --config example_config.yaml   # lint config before running
./ncc-orchestrator --config example_config.yaml run
```

**4b. Or launch the full web stack** (API + UI + first-run admin bootstrap;
serves **HTTPS by default** with an auto-generated self-signed cert):

```bash
./ncc-orchestrator v2-start --config-path example_config.yaml \
  --api-listen :8081 --ui-listen :8080
# Open https://localhost:8080 — grab the first-run admin password from the API
# server log (search for "FIRST-RUN ADMIN") and change it on first login.
# Add --ui-insecure-http to serve plain HTTP for a trusted loopback instead.
```

Reproducible release build (cross-compile + archives + checksums): see [`binaryGO.txt`](binaryGO.txt) and [`docs/BUILD_FROM_SCRATCH.md`](docs/BUILD_FROM_SCRATCH.md).

---

## Run on macOS / Windows / Linux (trust & verification)

The release binaries are not (yet) signed with an Apple Developer ID or
Authenticode certificate, so on first launch your OS may flag them as
"untrusted". This is a one-time speed-bump per file — you do **not** need
to disable Gatekeeper / SmartScreen / AV system-wide.

### Step 1 — verify the SHA-256 first

Every release publishes `checksums.txt` (and `release-attestation.json`,
which contains the same hashes plus full provenance metadata). **Always
verify before running an unsigned binary.**

```bash
# macOS / Linux
shasum -a 256 -c <(grep ncc-orchestrator-darwin-arm64 checksums.txt)
# OK
```

```powershell
# Windows
Get-FileHash .\ncc-orchestrator-windows-amd64.exe -Algorithm SHA256
# Compare against checksums.txt
```

If hashes do not match, **stop**. Re-download or report the discrepancy.

### Step 2 — clear the OS warning

| OS      | What you'll see | One-time fix |
| ------- | --------------- | ------------ |
| **macOS** | "cannot be opened because the developer cannot be verified" | `chmod +x ncc-orchestrator-darwin-arm64 && xattr -d com.apple.quarantine ncc-orchestrator-darwin-arm64` (or right-click in Finder → Open → Open) |
| **Windows** | "Windows protected your PC" SmartScreen dialog | Right-click the `.exe` → Properties → tick **Unblock** → Apply. Or: `Unblock-File .\ncc-orchestrator-windows-amd64.exe` in PowerShell. Or click **More info → Run anyway** in the SmartScreen dialog. |
| **Linux** | `Permission denied` | `chmod +x ncc-orchestrator-linux-amd64` |

For an extracted full stack, recurse over the `bin/` directory:

```bash
# macOS
xattr -dr com.apple.quarantine ncc-v2-stack-darwin-arm64/

# Windows
Get-ChildItem -Recurse .\ncc-v2-stack-windows-amd64 | Unblock-File

# Linux
chmod +x ncc-v2-stack-linux-amd64/bin/*
```

### Step 3 — confirm provenance with `verify`

`ncc-orchestrator verify` prints the embedded build metadata plus the
SHA-256 of the running executable. Cross-check against `checksums.txt`
(and the matching tag URL printed at the bottom of the output):

```text
$ ncc-orchestrator verify
version:           2.1.1
git_revision:      914c71d27fb1...
executable_sha256: 23ee3cad876c...
license:           MIT
project_url:       https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator
affiliation:       independent open-source project; not affiliated with or endorsed by Nutanix, Inc.
verify:            compare executable_sha256 against checksums.txt at
                   https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.1.1
```

On Windows, the file Properties dialog also displays the project name
("NCC Orchestrator"), the open-source maintainer copyright, and the
embedded version — written into every `.exe` via VERSIONINFO at build
time so administrators can identify the binary's origin (this project,
MIT licensed) even before any code-signing certificate is involved. The
embedded `LegalTrademarks` field explicitly states the project is not
affiliated with or endorsed by Nutanix, Inc.

For the full deep-dive (notarization caveats, EV vs OV signing, GPG
detached signatures, troubleshooting AV quarantines), see
[`docs/SECURITY_AND_TRUST.md`](docs/SECURITY_AND_TRUST.md).

---

## Configuration

**[`example_config.yaml`](example_config.yaml)** is the canonical, validator-clean template. Every release archive bundles a copy. Configuration precedence is:

1. Built-in defaults
2. Canonical YAML (nested schema version 1; legacy flat keys remain supported)
3. Environment variables with the **`NCC_`** prefix
4. Platform/deployment overlay
5. Explicit CLI flags

### Secret handling

Plaintext credentials in config files are explicitly rejected by `validate-secrets`. Use one of:

```yaml
# Option A — env-backed (recommended for CLI/CI)
secrets:
  provider: env
runner:
  credentials:
    password: "secret://NCC_PASSWORD" # then: export NCC_PASSWORD=...

# Option B — file-backed (recommended for hardened deployments)
secrets:
  provider: file
  file: /run/secrets/ncc.yaml
runner:
  credentials:
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

The v2.2.0 dashboard can combine persisted NCC findings with live Prism
Central serviceability alerts. Configure `pcs` or `prism-central-url` in the
existing config, then use the dashboard's **NCC** and **PC** source selector.
PC alerts are fetched on demand and cached briefly by the API. Unresolved
alerts load first; the complete alert history is fetched in the background
for the resolved/all-status views.

`GET /api/v1/alerts` returns normalized PC alert rows with `source: "PC"`,
severity, cluster, status, timestamps, detail, and KB metadata. It is
viewer-readable and respects cluster-group restrictions. The `resolved`
parameter accepts `No`, `Yes`, or `all` and defaults to `No`; the API translates
this into the Prism Central `isResolved` filter before pagination. Set
`pc-alerts-cache-ttl` to control caching (default `5m`; `0` disables it).
Cluster names are displayed using the NCC cluster mapping while links target
the mapped cluster IP on port `9440`.

Major endpoints (full surface at `GET /api/v1/meta/routes`, OpenAPI at `GET /api/v1/openapi.json`):

| Path                                | Methods    | Notes                                                              |
| ----------------------------------- | ---------- | ------------------------------------------------------------------ |
| `/api/v1/health`                    | GET        | Version, build date, paths, auth mode, token source                |
| `/api/v1/alerts`                   | GET        | PC alerts; `resolved=No`, `resolved=Yes`, or `resolved=all`; optional `refresh=1` |
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
| `/api/v1/auth/login`, `/logout`     | POST       | Username/password login (local or LDAP/AD) / clear session        |
| `/api/v1/auth/me`                   | GET        | Current caller identity, role, and available login methods        |
| `/api/v1/auth/forgot-password`      | POST       | Self-service password-reset request (queued for an admin)         |
| `/saml/metadata`, `/login`, `/acs`  | GET/POST   | SAML SP endpoints (when SAML is configured)                       |

All write/mutate routes require `X-API-Token: <token>` (or `Authorization: Bearer …` session token). Errors return a structured envelope with `success: false`, `error`, and `error_code` (e.g. `NCC_API_UNAUTHORIZED`, `NCC_API_BAD_REQUEST`, `NCC_API_NOT_FOUND`, `NCC_API_CONFLICT`).

**RBAC, login & SSO:** the server enforces three roles — `viewer` (read-only), `operator` (also trigger/cancel runs), and `admin` (everything incl. `/api/v1/settings/*`). A role can be a static token (`NCC_API_TOKEN` = admin, `NCC_API_VIEWER_TOKEN` = viewer), an interactive login, or a self-service **personal access token** (`ncc_pat_…` bearer, inherits the owner's role, expiring or **never**-expiring, revocable; user menu → *Personal access tokens*). Interactive login is on by default with a first-run **admin bootstrap** (random password + forced change); accounts live in a writable store (a `0600` file or a Kubernetes Secret). Login methods: local password accounts (managed in Settings → Access, bcrypt), **SAML SSO** (`--saml-*` or runtime), and **LDAP / Active Directory** (`--ldap-*` or runtime; local-first with AD fallback, AD group→role mapping) — all configurable together. Browser logins use an httpOnly, `SameSite=Strict` session cookie (marked `Secure` whenever the UI is on HTTPS — the default) with double-submit CSRF protection; the UI shows a login screen and hides admin-only/operator-only controls per role. Admins can segregate clusters into **cluster groups** for access control: groups are **opt-in isolation** — an ungrouped viewer/operator sees all clusters, while membership confines a caller to that group's clusters. Lost passwords are recoverable offline (`ncc-orchestrator v2-reset-password`) or via a self-service request queue, and all auth state can be captured with `v2-backup` / restored with `v2-restore`. See [docs/SECURITY_AND_TRUST.md](docs/SECURITY_AND_TRUST.md). With no login configured, the single-token behavior is unchanged.

**HTTPS by default:** `ncc-ui-server` serves HTTPS out of the box — `v2-start` auto-generates a self-signed cert (under `<install-dir>/tls/`) and redirects plain HTTP to HTTPS on the same port. Manage the certificate from **Settings → Access → HTTPS / TLS**: generate/renew a self-signed cert, or install your own PEM cert + key. Opt out with `--ui-insecure-http`.

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

## Operability: status, doctor, metrics, completions

v2.1.1 ships an opinionated set of operator-experience subcommands that
remove the need for ad-hoc `ps | grep` / `lsof` / `curl` invocations
when something looks wrong.

### `ncc-orchestrator v2-status` — what's running, are they healthy

```text
$ ncc-orchestrator v2-status
ncc-orchestrator v2 stack status
--------------------------------
install-dir: /opt/ncc-v2

SERVICE            PID     STATE           LISTEN                 HEALTH       LOG
api-supervisor     1234    alive           -                      n/a          /opt/ncc-v2/logs/v2-api-supervisor.log
ncc-api-server     1235    alive           127.0.0.1:8081         ok (1ms)     /opt/ncc-v2/logs/v2-api.log
ui-supervisor      1236    alive           -                      n/a          /opt/ncc-v2/logs/v2-ui-supervisor.log
ncc-ui-server      1237    alive           0.0.0.0:8080           ok (2ms)     /opt/ncc-v2/logs/v2-ui.log
```

`--json` switches to a JSON object suitable for `jq` and monitoring
scripts. Tolerant of missing PID files (reports `missing-pid` rather
than failing) so it works on any partial state.

### `ncc-orchestrator doctor` — single-command "give me everything"

When you need to file a support ticket, run this. It produces a full
diagnostic report on stdout AND writes a redacted tarball:

```text
$ ncc-orchestrator doctor
========================================
ncc-orchestrator doctor
========================================
generated_at:  2026-05-29T16:30:59Z
install_dir:   /opt/ncc-v2

-- 1. verify (build provenance) --
  (full version + git revision + executable SHA-256)
-- 2. v2-check (install-dir layout) --
  (config readable? binaries executable? ports bindable?)
-- 3. v2-status (running services) --
  (the table above)
-- 4. environment summary --
  go_version: go1.26.3
  os/arch:    linux/amd64
  NCC_* env var names: (values REDACTED for support-ticket safety)
-- 5. recent log tails (last 200 lines each) --
  (v2-api.log, v2-ui.log, v2-*-supervisor.log)

support bundle: ./ncc-support-20260529T163059Z.tar.gz
  attach this file to your support ticket; secrets and tokens have been redacted.
```

The bundle contains `report.txt`, `logs/v2-*.log` (last 1000 lines
each), and `config.redacted.yaml` (all values for keys matching
`password / secret / token / credential / api-key / client-id` are
replaced with `***REDACTED***`).

Pass `--no-bundle` to skip the tarball when you just want the report
on stdout.

### `/metrics` — Prometheus exposition

The api-server now exposes a Prometheus-compatible `/metrics` endpoint:

```text
ncc_build_info{version="2.1.1",stream="Release",go_version="go1.26.4",os="linux",arch="amd64"} 1
ncc_process_uptime_seconds 3601.42
ncc_run_active 0
ncc_runs_triggered_total 42
ncc_runs_completed_total 41
ncc_runs_failed_total 3
ncc_go_goroutines 18
ncc_go_memstats_alloc_bytes 6291456
ncc_ratelimit_allowed_total 1842
ncc_ratelimit_blocked_total 0
# NCC run metrics (v2.1.0) — read from the latest run-summary.json:
ncc_cluster_up{cluster="10.0.0.1"} 1
ncc_cluster_checks_total{cluster="10.0.0.1",severity="FAIL"} 0
ncc_cluster_health_score{cluster="10.0.0.1"} 95
ncc_last_run_clusters_failed 1
ncc_last_run_exit_code 2
```

**New in v2.1.0:** `/metrics` now also serves the last run's per-cluster
severity/health and run-level gauges, so Prometheus can scrape the api-server
directly instead of running a node_exporter textfile collector over the
`<cluster>.prom` files (those still work for CLI-only hosts). See
[`Prometheus.md`](Prometheus.md).

Auth-gated by default (same `X-API-Token` as the rest of the API).
Pass `--metrics-public` on the api-server to allow unauthenticated
scraping — useful on private networks behind a service mesh, where
vanilla Prometheus scrapers can't easily set custom headers.

### `ncc-api-server --health-check` — Docker / K8s probe mode

```bash
ncc-api-server --health-check --listen 127.0.0.1:8081
# exits 0 when /api/v1/health returns ok, 1 otherwise
```

Designed for `HEALTHCHECK` in Dockerfiles and `livenessProbe.exec.command`
in Kubernetes manifests — no need to ship `curl` or `wget` in your
runtime image. Reads the on-disk token automatically; works with
stack-aware default resolution if invoked from `<stack>/bin/`.

### Shell completions

```bash
# Bash (Linux)
ncc-orchestrator completion bash | sudo tee /etc/bash_completion.d/ncc-orchestrator

# Zsh (with compinit)
ncc-orchestrator completion zsh > "${fpath[1]}/_ncc-orchestrator"

# Fish
ncc-orchestrator completion fish > ~/.config/fish/completions/ncc-orchestrator.fish

# PowerShell
ncc-orchestrator completion powershell > $PROFILE.ncc-orchestrator.ps1
```

Generated on demand from cobra's command tree, so they stay in lockstep
with the actual subcommand / flag set automatically.

---

## Running individual components (API only / UI only)

The orchestrator (`ncc-orchestrator v2-start`) is the recommended
launcher because it manages both servers' lifecycles together. But each
binary in the stack is independently runnable for advanced scenarios —
running the API headless behind your own reverse proxy, hosting the SPA
on a CDN, embedding only the API into Kubernetes, etc.

Starting in v2.0.2, both `ncc-api-server` and `ncc-ui-server` detect
when they're running from `<stack-root>/bin/<self>` and auto-resolve
their path flags to the same install-dir the orchestrator would have
picked. So the **simplest possible standalone launch** from inside an
extracted stack is just:

```bash
cd ncc-v2-stack-linux-amd64/bin

# API only
./ncc-api-server --listen 127.0.0.1:8081 &
# [stack-aware] detected v2 stack at <root>; auto-resolved
#   repo-root, config-path, output-dir, log-dir, token-file-path,
#   orchestrator-bin

# UI only (point at the API you started above)
./ncc-ui-server --listen 0.0.0.0:8080 --backend-url http://127.0.0.1:8081
# [stack-aware] detected v2 stack at <root>; auto-resolved dir, api-token-file
```

Each server prints a `[stack-aware]` banner listing exactly which
flags were auto-resolved, so you can override any of them explicitly
without surprise (`--config-path /etc/ncc/config.yaml`, for example,
keeps your override and still auto-resolves the rest).

Outside a stack layout (Docker images that just `COPY` the binary
into `/usr/local/bin/`, dev checkouts, manual installs not under a
`bin/` directory), neither server activates stack-aware mode and the
original CWD-relative defaults stand — so existing deployments that
already pass explicit flags are unaffected.

**Subcommand mistakes are caught early.** Because users sometimes
confuse the binaries (e.g. `./ncc-api-server update --check`
expecting an updater), each sub-binary now refuses unknown
positional args with a clear redirect to the orchestrator:

```text
$ ./ncc-api-server update --check
ncc-api-server: unrecognized subcommand "update".
This binary is a sub-component of the Nutanix NCC Orchestrator stack and only accepts --flags.
For lifecycle commands like "update", run the orchestrator instead:
  /opt/ncc-v2-stack/bin/ncc-orchestrator update --check
```

Both sub-binaries accept `version` (prints buildinfo and exits 0) and
`--help` for ergonomics. Everything else exits 2 with the redirect.

### Use cases

| Scenario | Command |
| -------- | ------- |
| API-only, headless behind your own ingress | `./ncc-api-server --listen :8081 --cors-origin https://your-ui.example.com` |
| UI-only, pointing at a remote API | `./ncc-ui-server --listen :8080 --backend-url https://api.example.com --api-token-file /etc/ncc/token` |
| Build your own SPA against the API | `./ncc-api-server --listen :8081 --cors-origin http://localhost:5173` (then run your dev server) |
| Run only the orchestrator CLI for cron-driven runs | `ncc-orchestrator --config /etc/ncc/config.yaml run` |

For the full flag reference, run any of the binaries with `-h`. For
production deployment patterns (Kubernetes, behind an ingress, with
mTLS), see [`docs/V2_BACKEND_FRONTEND_MVP.md`](docs/V2_BACKEND_FRONTEND_MVP.md).

---

## Run with Docker Compose

The repo ships a `docker-compose.yml` that builds and runs the full v2
stack on a private Docker bridge network. Lifecycle is managed by
Compose's `service_healthy` gate, which uses the new
`ncc-api-server --health-check` probe — so the UI starts only after
the API has written its token file and is accepting requests.

```bash
git clone <this-repo>
cd <this-repo>

# 1. Provide a config (cluster credentials, secret references, etc.)
mkdir -p config
cp example_config.yaml config/config.yaml
$EDITOR config/config.yaml

# 2. (Optional) Set the password env var the API server forwards into runs:
echo "NCC_PASSWORD=<your-prism-password>" > .env

# 3. Build and start
docker compose up -d
# UI on http://localhost:8080 (proxies to ncc-api-server:8081)
# API on 127.0.0.1:8081  (loopback-only by default)

# 4. Verify
docker compose ps
docker compose logs -f ncc-api-server
docker compose exec ncc-api-server ncc-api-server --health-check --listen 127.0.0.1:8081

# 5. Tear down
docker compose down            # keeps volumes (history, token)
docker compose down -v         # nukes everything
```

Persistent state (output reports, runner logs, audit logs, schedule
state) lives in named volumes so a `docker compose down` won't wipe
history. The token file is shared between containers via a named
volume — never via env vars.

For Kubernetes deployment, see the [`Kubernetes`](#kubernetes) section
below and the [`helm/`](helm/) chart.

---

## Security posture

| Area                  | Default                                                                                                                |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Authentication        | Token-based (`X-API-Token`) constant-time compare (`crypto/subtle`); HMAC-signed sessions; optional local password accounts (bcrypt), SAML SSO, and LDAP / Active Directory |
| Authorization (RBAC)  | Three roles `viewer < operator < admin`: viewers read non-settings `GET`s, operators also trigger/cancel runs, settings/rotation are admin-only |
| Browser sessions      | Role-bearing httpOnly + `SameSite=Strict` session cookie (auto-`Secure` on HTTPS) with double-submit CSRF protection on mutations; ui-server forwards user sessions (no admin-token injection) |
| Personal access tokens| Self-service `ncc_pat_…` bearer tokens; SHA-256-hashed at rest, inherit owner's role (live re-resolve for local accounts), bounded or never-expiring, revocable, 25/user cap |
| CORS                  | Strict allowlist (default `http://localhost:8080`); wildcard origins rejected at startup; `/saml/*` exempt (signed assertion + relay-state cookie) |
| CSP                   | UI: `script-src 'self'`, no `unsafe-eval`; API: `default-src 'none'`                                                   |
| Prism TLS             | Verified by default; `--ca-bundle` (trust internal CA) or `--pin-sha256` (cert pinning) preferred over `--insecure-skip-verify` |
| UI transport          | **HTTPS by default** — self-signed cert auto-generated by `v2-start`, HTTP→HTTPS redirect on the same port; generate/renew or install your own cert in Settings → Access → HTTPS / TLS; `--ui-insecure-http` to opt out. Direct API HTTPS via `--tls-cert-file`/`--tls-key-file`; optional mTLS (`--tls-client-ca-file`) |
| Outbound notifications| Optional webhook HMAC signing (`webhook-secret` → `X-NCC-Signature`); SMTP TLS verify via `smtp-insecure-skip-verify`; dead-letter dir for failed deliveries |
| Security headers      | `X-Content-Type-Options`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Permissions-Policy`, HSTS on TLS   |
| Rate limiting         | Per-client token bucket on sensitive auth/mutation routes (`--rate-limit-per-minute`, default 60)                      |
| Path confinement      | All file I/O canonicalized under `--repo-root`; `..` and embedded `/` rejected on `/artifacts/{name}` and `/runs/{id}` |
| Secrets               | `secret://NAME` refs with `env` or `file` provider; plaintext-in-config triggers a startup warning                     |
| Vulnerability scans   | `govulncheck ./...` and `npm audit --omit=dev` clean (Go 1.26.4, DOMPurify ≥ 3.4.7 enforced); enforced in CI (`.github/workflows/ci.yml`) |

Release details and validation evidence: [`RELEASE_NOTES_v2.1.1.md`](RELEASE_NOTES_v2.1.1.md).

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
| [`docs/MIGRATION_v2.0.2_TO_v2.1.0.md`](docs/MIGRATION_v2.0.2_TO_v2.1.0.md)            | Upgrading from v2.0.2 (pre-RBAC/pre-backup) to v2.1.0                 |
| [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)                                  | TLS, Prism Central, API issues                                        |
| [`docs/MCP_SERVER.md`](docs/MCP_SERVER.md)                                            | Wire the orchestrator into AI tools via MCP                           |
| [`RELEASE_NOTES_v2.1.1.md`](RELEASE_NOTES_v2.1.1.md)                                  | Current release details, validation, and upgrade guidance              |
| [`docs/NIST_CSF_BASELINE.md`](docs/NIST_CSF_BASELINE.md)                                | NIST CSF 2.0 control baseline, evidence map, and gap plan             |
| [`docs/NIST_CSF_EVIDENCE_MANIFEST.json`](docs/NIST_CSF_EVIDENCE_MANIFEST.json)          | Machine-readable control-to-evidence mapping for compliance bundles    |
| [`docs/RELEASE_CHECKSUMS.md`](docs/RELEASE_CHECKSUMS.md)                              | How `--update` verifies downloads                                     |
| [`docs/SECURITY_AND_TRUST.md`](docs/SECURITY_AND_TRUST.md)                            | Verify SHA-256, run unsigned binaries on macOS/Windows/Linux, GPG     |
| [`deploy/README.md`](deploy/README.md)                                                | Provision Linux and Windows VMs with cloud-init or Sysprep            |
| [`k8s/README.md`](k8s/README.md)                                                     | Kubernetes deployment guide                                           |
| [`Prometheus.md`](Prometheus.md)                                                     | Prometheus setup: `.prom` textfile collector + scraping run metrics from the api-server `/metrics` |
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

- **Current GA:** [`v2.1.1`](RELEASE_NOTES_v2.1.1.md). See [`RELEASE_NOTES_v2.0.0.md`](RELEASE_NOTES_v2.0.0.md), [`RELEASE_NOTES_v2.0.1.md`](RELEASE_NOTES_v2.0.1.md), [`RELEASE_NOTES_v2.0.2.md`](RELEASE_NOTES_v2.0.2.md), [`RELEASE_NOTES_v2.1.0.md`](RELEASE_NOTES_v2.1.0.md), [`RELEASE_NOTES_v2.1.1.md`](RELEASE_NOTES_v2.1.1.md) for the cumulative change log.
- **Build provenance:** every binary embeds `Version`, `BuildDate`, `Stream`, `GoVersion`, and the git revision (via `-buildvcs=true`); inspect with `./ncc-orchestrator verify`. Releases additionally ship `release-attestation.json` (per-release manifest), CycloneDX SBOMs, and a SLSA build-provenance attestation produced by `.github/workflows/release.yml` (verify with `gh attestation verify`).
- **Checksums:** `dist/checksums.txt` (or the `checksums.txt` attached to the GitHub release) — SHA-256, sorted, includes every binary, every stack archive, `example_config.yaml`, `release-attestation.json`, every `bom-*.cdx.json`, and the matching `RELEASE_NOTES_v*.md`.
- **Docker:** `prajwalnutant/nutanix-ncc-orchestrator:2.1.1` (and `:latest`).

---

## Author

Prajwal Vernekar — [prajwal.vernekar@nutanix.com](mailto:prajwal.vernekar@nutanix.com)

## License

[MIT](LICENSE). Use at your own risk; this tool interacts with Nutanix APIs — ensure you have proper permissions.
