# Changelog

All notable changes to the Nutanix NCC Orchestrator are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

**Release checklist (for maintainers):** Ensure [`VERSION`](VERSION) matches the intended tag; default `main.Version` in code is `1.1.0` when not set via ldflags. Run `go vet ./...`, `go test ./...`, and `go build ./...` (and `go build ./cmd/ncc-mcp-server`). Confirm `k8s/` and `helm/` image tags match `VERSION`. Tag `v1.1.0` and create a GitHub release using [RELEASE_NOTES_v1.1.0.md](RELEASE_NOTES_v1.1.0.md); attach binaries and `checksums.txt` per [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md) so `--update` can verify downloads.

---

## [Unreleased]

### Added

- None yet.

### Changed

- None yet.

---

## [1.1.0] - 2026-04-21

### Added

- **`create-schedule` command** — New `ncc-orchestrator create-schedule` subcommand to create recurring NCC runs:
  - **Linux/macOS:** creates or updates a cron entry (`--type cron`, `--cron` or `--every`)
  - **Windows:** creates or updates a Scheduled Task (`--type windows`, `--every`)
  - Supports safe preview mode with `--print-only` (default true).
  - New scheduler actions: `--action list|remove|run-now`.
- **MCP scheduler tools** — New MCP tools `create_schedule`, `list_schedules`, and `delete_schedule` for schedule lifecycle operations from AI clients.
- **Run history snapshots** — Optional `--run-history` writes timestamped snapshots under `--run-history-dir` with retention controls (`--retain-last`, `--retain-days`).
- **Regression awareness** — New `regression-summary.json` compares current FAIL counts to previous `run-summary.json`; `--notify-on-regression` only sends notifications when FAIL count increases.
- **Drill-down diff and snapshots** — Added `checks-snapshot.json` and `drilldown-diff.json` with per-check change tracking (new/resolved FAILs, severity changes).
- **Flaky check detection** — Added `flaky-checks.json` with lookback-based severity transition analysis (`flaky-lookback-runs`, `flaky-min-transitions`).
- **Health and SLO exports** — Added per-cluster `health_score` plus `avg_health_score` / `min_health_score` in `run-summary.json`, and `slo-dashboard.json` for dashboards.
- **SARIF export** — New per-cluster output format `sarif` in `--outputs`.
- **Config schema + validation mode** — New `config-schema` and `validate-config` commands for config ergonomics in CI/CD.
- **Policy gates** — Added `policy-gates` support for release/CI rules such as `new-fails>0` or `min-health-score<90`, with violations written to `policy-gates.txt`.
- **Secrets provider** — Added `secret://` resolution for sensitive fields using `secrets-provider=env|file` and optional `secrets-file`.
- **Quiet hours and maintenance windows** — Added notification suppression via `quiet-hours` and `maintenance-windows`.

### Changed

- **ncc-run-record metadata** — Added `git_revision`, `hostname`, and `scheduler_source` for better traceability.
- **Adaptive resilience on 429** — Effective cluster concurrency now adapts down/up based on HTTP 429 pressure (`--adaptive-parallelism`).
- **Single-file report option** — `--single-report` writes `ncc-report-single.html` alongside `index.html`.
- **Strict validation by default** — Config validation now rejects unknown keys and enforces strict value typing by default.

---

## [1.0.0] - 2026-04-07

### Added

- **Nutanix API v4 (default)** — Cluster discovery, start-checks, and Prism task polling use v4 paths where applicable; configurable `nutanix-v4-api-version` (e.g. `v4.2`). Legacy v3 remains available via `ncc-api-version: Legacy` (alias `v1`).
- **Cluster resolution for v4** — `GET .../config/clusters` matches `--clusters` to the correct registered entity (name, extId, external address, or CVM IP); `nodeIps` for run-system-defined-checks come from that cluster’s CVMs (fixes Prism Central multi-cluster / NCC-40023 cases).
- **run-summary.json** — Per-cluster `clusters[]` with `ok`, severity counts, `checks_total`, and `error`; `exit_code` mirrors process exit (0 = all OK, 1 = all failed, 3 = partial success).
- **Exit code 3** — When at least one cluster succeeds and at least one fails, the process exits **3** (partial success). See README.
- **Semver for `--update`** — Version comparison uses [Masterminds/semver](https://github.com/Masterminds/semver); fallback to legacy parse if non-semver.
- **docs/TROUBLESHOOTING.md** — Common TLS, multi-cluster, and API issues.
- **ncc-run-record.json** — Versioned bundle (`schema_version`, orchestrator version, full `run` summary) for pipelines.
- **discover-clusters `--format`** — `lines` (default), `table`, or `json` (name, ext_id, address, api).
- **HTTP rate limits** — Logs `X-RateLimit-*` / `X-Api-Ratelimit-*` on 429; caps long `Retry-After` waits.
- **Helm chart** — `helm/ncc-orchestrator` for templated CronJob image and schedule.
- **Kustomize** — `k8s/kustomization.yaml`; apply with `kubectl apply -k k8s/`.
- **Tests** — Golden CSV and per-cluster HTML fixtures, `httptest` retry for 429, KB markdown helper in `internal/kblinks`.
- **MCP `get_report`** — For `*.log` files, turns `KB NNNN` into markdown links to portal.nutanix.com.

### Changed

- **Version and images:** VERSION set to 1.0.0; Kubernetes CronJob and job-debug use image tag 1.0.0. First stable semver release.
- **Partial multi-cluster runs** — Previously exited **0** when some clusters failed; now exits **3** so automation can detect partial failure.

### Fixed

- **Viper / discover-clusters** — Duplicate `BindPFlag` for `insecure-skip-verify` (and related keys) no longer overrides root flags; `--insecure-skip-verify` on the main command is honored.

---

## [0.1.13] - 2026-03-02

### Added

- **MCP: `list_run_artifacts` tool**  
  List files in an NCC run output directory (run-summary.json, index.html, per-cluster .log/.html/.csv). Optional `output_dir` (default `outputfiles`).

- **MCP: `get_report` tool**  
  Read the aggregated index.html or a specific cluster report file. Optional `output_dir`, `file` (e.g. `index` or `10.0.0.1.html`). Large reports are truncated for context.

- **MCP: Resources**  
  - `ncc://run-summary` — latest run-summary.json (application/json).  
  - `ncc://report` — latest aggregated index.html (text/html).  
  Clients can list and read these via MCP resources/list and resources/read.

- **Documentation**  
  - docs/MCP_SERVER.md: new tools and resources table, “Possible future additions” section.  
  - MCP tool descriptors for list_run_artifacts and get_report.

### Changed

- **Version and images:** VERSION set to 0.1.13; k8s CronJob and job-debug use image tag 0.1.13.

### Fixed

- None explicitly tracked for this release.

---

## [0.1.12] - 2026-02-12

### Added

- **Digest notifications**  
  New option `--notify-digest` (config: `notify-digest`) sends **one email, one webhook, and one Slack message per run** with a run overview (clusters OK/failed, duration) instead of per-cluster notifications. Optional attachment of the aggregated `index.html` to the digest email when `email-attach-html` is set; optional base64-encoded index in the webhook payload when `webhook-include-html` is set.

- **Run summary log**  
  At the end of each run, a single structured log line is emitted with `clusters_ok`, `clusters_failed`, `duration_s`, and `index_html` path for easier parsing and monitoring.

- **Notification retries**  
  Email and webhook sends are wrapped in a retry loop (3 attempts with jittered backoff using `retry-base-delay` / `retry-max-delay`) to improve reliability on transient failures.

- **Replay + HTML attach**  
  In replay mode, per-cluster HTML (and CSV) is generated **before** sending notifications. When `email-attach-html` is set, the generated HTML file is attached to the replay email; when `webhook-include-html` is set, the replay webhook payload includes the report as base64.

- **Config path validation**  
  `validateConfig` now requires non-empty (after trim) values for `output-dir-logs`, `output-dir-filtered`, `log-file`, and `prom-dir`, with clear error messages.

- **HTTP log redaction**  
  When `log-http` is enabled, request/response dumps redact the `Authorization` and `Cookie` headers, and mask JSON values for `"password"` / `"Password"` fields to avoid leaking secrets in logs.

- **Documentation**  
  - README: HTTP pool/timeouts description, example webhook payload, testing email and webhook (webhook.site, MailHog, Mailtrap), `--version` behavior, `notify-digest` and env vars.
  - k8s/README: Runbook for CronJob failures (logs, job-debug, NFS permissions, prune images).
  - k8s ConfigMap: `notify-digest: false` option added.

- **Unit tests**  
  New/updated tests for `validateConfig` (path validation), `checkOutputPermissions`, digest overview format, and `ParseSummary` (no-block and check-name cases).

### Changed

- Per-cluster email/webhook/Slack are **skipped** when `notify-digest` is true; digest notifications are sent after the aggregated HTML is written.
- Replay mode uses `sendEmailWithRetry` and `sendWebhookWithRetry` for consistency with the main run.

### Fixed

- None explicitly tracked for this release.

---

## [0.1.11] and earlier

See git history and [Releases](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases) for prior versions.

---

[1.1.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.1.0
[1.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.0.0
[0.1.13]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.13
[0.1.12]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.12
