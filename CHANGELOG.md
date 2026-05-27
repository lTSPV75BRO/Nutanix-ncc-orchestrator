# Changelog

All notable changes to the Nutanix NCC Orchestrator are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

**Release checklist (for maintainers):** Ensure [`VERSION`](VERSION) matches the intended tag; default `main.Version` in code is `2.0.0` when not set via ldflags. Run `go vet ./...`, `go test ./...`, and `go build ./...` (and `go build ./cmd/ncc-mcp-server`). Confirm `k8s/` and `helm/` image tags match `VERSION`. Tag `v2.0.0` and create a GitHub release using [RELEASE_NOTES_v2.0.0.md](RELEASE_NOTES_v2.0.0.md); attach binaries and `checksums.txt` per [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md) so `--update` can verify downloads.

---

## [2.0.0] - 2026-05-05 (release-candidate hardening through 2026-05-27)

### Added (late-cycle hardening, 2026-05)

- **`example_config.yaml`** — Repo-rooted, validator-clean (`ncc-orchestrator validate-config`) reference config that ships in `dist/` and inside every `ncc-v2-stack-*.tar.gz`/`.zip` archive. Uses `secret://NAME` references with `secrets-provider: env` so it works out-of-the-box once `NCC_PASSWORD` (and optionally `SMTP_PASSWORD`, `WEBHOOK_TOKEN`) are exported.
- **`DELETE /api/v1/runs/active`** — Cancel a stuck/in-flight orchestrator run from the UI; returns `409` with structured `error_code` when no run is active.
- **`GET /api/v1/runs/{id}`** — Single archived run metadata + embedded summary, including artifact bodies (`run-summary.json`, `ncc-run-record.json`, `regression-summary.json`, `checks-snapshot.json`, `run-meta.json`); rejects path traversal (`..`/`/`).
- **Header trigger button with live elapsed time** — Top-ribbon Run control now shows a pulsing primary-tinted button with spinning icon and `Running · Xm Ys` elapsed time, mirroring the Settings → Runs indicator.
- **Context-aware Dashboard empty states** — Alerts table now distinguishes "Run in progress" (with live output link), "All clusters clean", and onboarding "No alerts yet" states based on the active-run query and last summary.
- **Schedule input validator** — `action=create` now requires at least one of `cron` or `every`; covered by `TestValidateScheduleInput` table-driven test.
- **Audit + meta route surface** — Audit query params (`limit`, `since`, `source`) are validated server-side with structured `400` on bad input; CORS `Access-Control-Allow-Methods` now includes `DELETE`.

### Security (late-cycle hardening)

- **Go toolchain upgraded 1.26.2 → 1.26.3** — Fixes five Go standard-library CVEs surfaced by `govulncheck`:
  - `GO-2026-4976` ReverseProxy query forwarding (`net/http/httputil`)
  - `GO-2026-4971` `net.Dial` / `LookupPort` NUL-byte panic
  - `GO-2026-4918` HTTP/2 transport infinite loop on bad `SETTINGS_MAX_FRAME_SIZE`
  - `GO-2026-4977` and the related stdlib advisories
  - After upgrade: `govulncheck ./...` → **No vulnerabilities found**.
- **npm DOMPurify override `^3.4.7`** — Pinned via `package.json#overrides` to clear 5 transitive DOMPurify advisories in `monaco-editor` (ADD_TAGS/FORBID_TAGS bypasses, SAFE_FOR_TEMPLATES bypass, prototype pollution, mutation-XSS) without downgrading Monaco; after override: `npm audit --omit=dev` → **found 0 vulnerabilities**.
- **`yaml` patch update** via `npm audit fix` — Closes deeply-nested-collection stack-overflow advisory (`GHSA-48c2-rrv3-qjmp`).

### Frontend / UX (late-cycle hardening)

- **Theme overhaul** — Dark mode rewritten to a near-black neutral charcoal palette; light mode rewritten to a clean zinc palette with crisp white cards; Ant Design and `styles.css` variables aligned through `theme.tsx`.
- **Monaco editor local + themed** — `@monaco-editor/react` now loads `monaco-editor` locally (resolves CSP `script-src 'self'` violation that was blocking the editor from the CDN). Workers bundled via Vite `?worker` imports. Custom Monaco themes (`ncc-light`, `ncc-dark`, `ncc-it-pro`) registered to mirror the app palette; fallback `<textarea>` styled to match.
- **Form/Accessibility cleanup** — Added `id`, `name`, `htmlFor`, `aria-label`, and `autoComplete` across `ConfigSection`, `RunsSection`, `ScheduleSection`, `PolicyGateBuilderSection`, `AuditLogSection`, `LogsSection`, `DashboardPage`, `ApiExplorerSection`, `JsonOutputsSection`, `RawOutputsSection`, and `SecretsMigrationModal`; trigger-run password field is now wrapped in a real `<form>`; eliminates the DOM warnings for unassociated labels, missing `id`/`name`, password-not-in-form, and missing autocomplete.
- **Runs table redesign** — Replaced often-blank "Index" column with `Type`, `Status`, `Duration`, `Clusters`, and `Issues` columns; cells degrade to `—` for trigger entries.
- **Sparkline correctness** — Fixed timezone bug in `RecentRunsSparkline` (mixed UTC and local dates) via `localDateKey` helper; filtered to count only `history`/`summary` sources so trigger events no longer inflate the "runs in last 7 days" count.

### Build / Release

- **`binaryGO.txt` overhaul** — Preflight checks, consolidated variable declarations, local-arch symlinks, example-file copy, cleaner tar/zip archives, and end-to-end verification (`api/health` filter, version assertion).
- **Production verification (2026-05-27)** — `govulncheck ./...` clean, `npm audit --omit=dev` clean, `go test -race -count=1 ./...` clean, `gofmt -l .` clean, `go vet ./...` clean, `tsc --noEmit` clean, `vite build` clean.

### Added

- **Full v2 application stack in 2.0.0** — Release scope now explicitly includes API (`cmd/ncc-api-server`), UI (`cmd/ncc-ui-server` + `frontend`), and UI-integrated API proxy surface for `/api/v1/*`.
- **Build-from-scratch documentation** - Added `docs/BUILD_FROM_SCRATCH.md` with full setup flow for clean machines, including local build, frontend build, v2 stack startup, tests, packaging, and Kubernetes verification.
- **Release validation suite for v2.0.0** — Production checks and edge-case verification documented with reproducible command evidence.
- **CodeQL workflow alignment** — Added repository workflow that analyzes only `go` and `actions`, preventing JS/TS language-detection failures in this branch scope.
- **v2.0.0 release documentation set** — Added release notes, production-readiness checklist, and milestone summary documents for the v2 train.
- **Build and handover guides** — Added `docs/BUILD_FROM_SCRATCH.md` and `docs/ARCHITECTURE_AND_HANDOVER.md` for end-to-end onboarding, operations, and ownership transfer.
- **Clean v2 uninstall tooling** — Added `scripts/uninstall-v2-clean.sh` as the canonical Kubernetes/runtime cleanup entrypoint; legacy `scripts/uninstall-ncc-orchestrator.sh` now delegates to it.
- **CLI local uninstall command** — Added `ncc-orchestrator uninstall` for standalone local cleanup of artifacts/state created by the binary.
- **`v2-check` preflight command** — Added lightweight v2 runtime self-check for binary executability, config/path readiness, directory writability, and API/UI port bind availability before `v2-start`.
- **Scheduler health API** — Added `GET /api/v1/schedule/health` to expose scheduler runtime hints (`last_run`, `last_success`, `last_error`) and lock/log path metadata.

### Changed

- **Version baselines** — `VERSION`, default `main.Version`, Helm chart/appVersion, Helm values image tag, and Kubernetes manifest image tags aligned to `2.0.0`.
- **README release pointers** — Updated current release status links to `v2.0.0` documents and clarified current branch scope.
- **Documentation alignment across v2 docs** - Updated README, CONTRIBUTING, migration guide, and v2 architecture doc for consistent source-build and deployment instructions.
- **Kubernetes architecture docs** - Clarified API runner binary staging model (runner image -> API init container -> shared tools path) in design docs and release notes.
- **Kubernetes runtime model** — API now stages the runner binary via init container (`runner image -> /tools/ncc-orchestrator`) and validates `--orchestrator-bin` at startup (exists + executable).
- **Production readiness manifests** — Added startup/readiness/liveness probes to API and UI deployments to improve rollout safety and self-healing behavior.
- **Preflight probe handling** — `.ncc-preflight-check` moved to a persistent read/write sentinel approach (no create/delete churn) with legacy typo cleanup.
- **Performance and reliability optimizations** — Cached exclude-title matchers/regex, reduced config re-parse and allocations in secret validation, de-duplicated replay metadata parsing, and eliminated redundant Prometheus writes during replay.
- **Console error UX** — Consolidated duplicate startup error logging to a single user-facing error path.
- **Scheduler overlap safety** — `create-schedule` now supports lock-enabled cron generation with `--with-lock` (default true) to prevent overlapping scheduled runs.
- **Security response handling** — Added redacted-safe config response content and startup warning for plaintext config passwords.
- **Logs UX operability** — Added UI `Follow tail` and `Jump to latest` controls for Runner Logs and Live Logs.

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

[2.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.0
[1.1.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.1.0
[1.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.0.0
[0.1.13]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.13
[0.1.12]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.12
