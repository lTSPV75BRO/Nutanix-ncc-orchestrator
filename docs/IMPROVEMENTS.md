# Improvements backlog

This is the active backlog for NCC Orchestrator. It contains proposed work that
is not already shipped in v2.4.0. Completed work belongs in
[`CHANGELOG.md`](../CHANGELOG.md) and the release notes
([`v2.4.0`](releases/RELEASE_NOTES_v2.4.0.md)).

Priorities are operational: P0 blocks scale or reliability, P1 improves
security or operator experience, and P2 improves maintainability. Size is a
rough estimate: S, M, or L.

## P0 — reliability and scale

| Item | Scope | Size |
| --- | --- | --- |
| Server-side report pagination | Build only the requested `checks_snapshot`/`agg_rows` page instead of loading and filtering the complete report before slicing. | M |
| Kubernetes HA integration test | Exercise two API replicas, shared PVC access, CronJob execution, schedule updates, and pod rescheduling on separate nodes. | L |

## P1 — security and operations

| Item | Scope | Size |
| --- | --- | --- |
| Secret rotation workflow | Document and optionally guide rotation of API tokens, JWT secrets, backup keys, and Prism credentials across replicas. | M |
| Outbound URL policy | Provide an administrator-visible allowlist/policy for Prism Central, LDAP, OIDC, SMTP, webhook, and update-server destinations. | M |
| Signed image and SBOM verification | Publish Cosign signatures/SBOMs and optionally verify image provenance during deployment. | M |
| Signed trust for restored program files | Restore accepts program files only when this host ran them before (v2.4.0, `.ncc-trusted-stack.json`). Remaining: publish per-file SHA-256 in the signed release checksums so a backup from another server can be verified without `--trust-stack`. | M |

## P1 — operator experience

| Item | Scope | Size |
| --- | --- | --- |
| Notification delivery history | Every attempt is logged (System → Logs) and Settings → Notifications shows each channel's last attempt, last success, last error, tries and dead-letter count (v2.4.0). Remaining: a browsable history of past deliveries, and replaying dead-lettered messages from the UI. | S–M |
| Overdue schedules in the digest email | Data age is shown on the Overview, Findings, Insights and in notifications, with Prometheus gauges (v2.4.0). Remaining: an overdue-schedule line in the runner digest email. | S |
| Accessibility checks in CI (UX-10) | A scripted pass found an icon-only button in every Findings row and unnamed role pickers (both fixed in v2.4.0). Add axe-core to the frontend tests for each page and a keyboard walkthrough, so these are caught before release; add a table view for the trend and SLO charts (F7). | M |

## P2 — engineering and test coverage

| Item | Scope | Size |
| --- | --- | --- |
| Kubernetes API contract tests | Fake Kubernetes API errors, missing RBAC, suspended schedules, invalid cron, and concurrent updates. | M |
| Race and load test profile | Run race tests plus concurrent report, alert, login, and schedule requests against a representative large report. | M |
| Report package extraction | Move the remaining renderers from `goNCC.go` into focused `internal/report` packages with benchmarks. | L |
| Split the three largest files | `goNCC.go` (18.7k lines), API `main.go` (6.3k) and `AccessSection.tsx` (3.4k) make review and ownership hard and slow the test loop. Extract by area when touching it: Prism client, reporting, install/update and backup from the runner; route table, OpenAPI and report handlers from the API; one file per sign-in provider in the UI. | L |
| Path handling review (gosec G703) | CI now fails on high-severity gosec findings, except G703: its 80+ path-taint hits are paths from the server's own config and validated names. Review them by area, move request-derived paths to `os.Root`, mark the rest `#nosec G703 -- reason`, then drop the exclusion. | M |
| Support-bundle: Kubernetes state | The bundle (downloadable from Health & repair) has the doctor report, every process log, and redacted config and environment files. Remaining: Kubernetes events, CronJob/PVC state, and component versions per pod. | S–M |

## Performance sequence

Work in this order when performance is the goal:

1. Server-side report pagination and remove the dashboard's full-report follow-up.
2. Add a slim Insights payload and avoid rebuilding the same trend data.
3. Add single-flight, multi-slot Prism alert caching.
4. Optimize audit tailing and run-history sparkline reads.

Do not redo the existing preview pagination, alert pager, five-minute Prism
alert cache, or gzip API responses (`cmd/ncc-api-server/compress.go`); those
are already implemented.

## Deferred ideas

These ideas remain valid but are intentionally below the active priorities:

- Resumable/checkpointed runs and cluster-subset filters.
- Custom notification templates and Slack/Teams interactive messages.
- Multi-team profiles with separate configuration and output roots (dev /
  staging / prod environments now cover separate cluster sets and runner
  settings; separate output roots are still open).
- A lightweight history UI for browsing archived reports.
- A minimal-install guide, architecture diagram, and operator glossary.

## Current baseline

Already covered through v2.4.0 and therefore tracked elsewhere:

- Setup checklist, redesigned sign-in, OIDC, RBAC, alert lifecycle, and audit
  logging.
- Host and Kubernetes scheduling, readiness/liveness, graceful shutdown, and
  supervised updates.
- Report preview pagination, asynchronous large-report refresh, and the current
  Prism alert cache.
- Dependency security gates (govulncheck through `tools/vulngate`),
  SBOM/provenance release tooling, and cross-platform CI coverage.
- Environments (dev / staging / prod) with per-environment schedules run by
  the API server.
- Structured logs from every process, a Logs page, and run-time log level.
- Conditional (resourceVersion) Kubernetes schedule saves.
- Per-cluster data freshness and the stopped-schedule banner
  (`GET /api/v1/freshness`).
- Restore installs only program files this host has run (SEC-07).
- PrometheusRule alerts in the Helm chart (`prometheusRule.enabled`).
- "Finding" wording in the API descriptions, messages and guides; real
  plurals in CLI output.
- Every `io.ReadAll` reads through a size limit (`internal/readguard`
  test), and a `GITHUB_TOKEN` with scopes is reported.
- Directory profiles for several AD domains or forests, and refusal of
  directory/SAML users named like a local account.
- Report-cache state in `/report/data`, metrics and a UI notice.
- Version-skew check (live CronJob image) that blocks Run on a major.minor
  mismatch.
- Raw-log retention (`raw-log-retain-days`) and a retention dry-run preview.
- Two narrow Kubernetes Roles for the user Secret and runner CronJob.
- `v2-restore --verify-only` with per-file SHA-256 in the manifest.
- Session time left, "Stay signed in" notice and lockout countdowns.
- Data age on Findings, Insights and notifications, with Prometheus gauges.
- Setup-aware empty states; schedule drafts and host schedule saves that
  cannot overwrite a newer save.
- An Integrations page with guided setups for ServiceNow, Prometheus &
  Grafana, log tools, chat/email/webhooks, MCP and scripts.

## Audit of 2026-11-16 (v2.4.0)

Scope: all Go commands and `internal/` packages, the React frontend, the
Dockerfiles, Kubernetes and Helm manifests, and the GitHub workflows.
Method: govulncheck and osv-scanner, `go vet`, pattern searches (command
execution, TLS verification, unbounded reads, request size limits, path
handling, secret comparison, background goroutines), a read of the newer
code (directory profiles, freshness, restore trust, update fallback), and a
walk through the demo build with a scripted accessibility pass.

Result: no known-vulnerable dependency other than the reviewed
GO-2026-6909 (`scripts/govulncheck-accepted.txt`); no injection, traversal
or authentication bypass found. Request bodies are size-limited, servers set
header/read/write timeouts, security headers and a CSP are sent, containers
run as a non-root user and the Kubernetes pods drop privileges.

Fixed in this pass:

- A panic in a background job (scheduled backup, environment schedules,
  notification digest, alert sync, self-heal timer, user-store reload, cache
  refresh, Prism Central discovery or alert fetch) crashed the API server and
  stopped every schedule. They now recover, log the stack and count in
  `ncc_api_background_panics_total`; a failed cache refresh no longer leaves
  the entry stuck "loading".
- Prism Central discovery read whole responses into memory with no limit
  (now 64 MiB per page, failing rather than truncating).
- `GITHUB_TOKEN` was attached to any URL containing "github"; it is now sent
  only over HTTPS to `github.com` and `githubusercontent.com` hosts.
- The SAML clock-skew check used `http.DefaultClient` and followed
  redirects; it has its own client with a timeout and no redirects.
- Run ids with a backslash are rejected (a path separator on Windows).
- Schedule page: a new browser showed the form defaults as unsaved changes
  over the saved schedule, one click from overwriting it.
- Accessible names for the Findings row "open details" button and the
  per-user role picker; "cluster(s)" wording replaced with real plurals.

Everything else found is listed in the tables above.

## Audit of 2026-11-19 (v2.4.0, second pass)

Full write-up: [`docs/audit/`](audit/executive-summary.md). This pass added
scanners (gosec, golangci-lint, govulncheck, osv-scanner, gitleaks, trivy,
kubeconform, kube-linter, zizmor), the race detector, axe-core in a real
browser (with colour contrast), a live authorization probe, a kind cluster
deployment and a load test. The earlier note that GO-2026-6909 was open is
withdrawn: it is a reviewed false positive (`crewjam/saml` v0.6.3 has the
fix).

Fixed in this pass:

- **Helm chart crash-loop (K8S-02).** The API pod died on first start because
  the chart passed only some path flags and the container layout triggered
  the stack-aware defaults (also true for the published v2.3.0 image). The
  chart now passes the full flag set; `--users-db-secret`,
  `NCC_USERS_DB_SECRET` and `NCC_USERS_DB` suppress the `--users-db` default.
- **Helm API pod security context (K8S-01).** The `seed-config` init
  container ran as root; the pod now sets non-root, `fsGroup` and seccomp.
- **Windows installer (WIN-01).** The install directory under `ProgramData`
  was writable by every local user while the stack runs as SYSTEM; the
  installer now restricts it to SYSTEM and Administrators.
- **LDAP sign-in (COR-08).** A dropped or busy directory during the final
  password check counted as a wrong password and fed the account lockout.
- **Accessibility (A11Y-01).** Contrast, label-in-name, invalid ARIA role,
  heading order, target size and keyboard focus failures found with axe in a
  browser; 0 violations on 16 routes in all three themes (empty-data pages).
- **Publishing workflows (CI-01).** Dependency caches are explicitly off in
  `docker-publish.yml` and `release.yml`.
- **`/api/v1/health` cost (PERF-01).** Display paths are cached for 30 s
  (18 ms to 0.8 ms per authenticated call on Windows).
- **Troubleshooting logs (LOG-01).** LDAP steps, sign-in failures, backup and
  restore errors, unreadable run summaries and version-skew refusals are now
  logged with the user, client address and request id (never passwords).

New open items from this pass:

| Priority | Item | Scope | Size |
| --- | --- | --- | --- |
| P0 | Helm install smoke test in CI | Install the chart on a kind cluster from images built in the job and wait for the API and UI to be ready. K8S-02 shipped because nothing deploys the chart. Cover `helm template` plus `kubeconform` in the same job. | M |
| P0 | Chart storage options | `pvc.yaml` hard-codes `ReadWriteMany`, so the chart cannot run on RWO-only storage (kind, many default classes). Add `persistence.accessMode` and `persistence.storageClass`, with a documented single-replica RWO mode. | S |
| P1 | systemd hardening (LIN-01) | The unit written by `v2-install-service` runs as root with no sandboxing. Add a dedicated user and `NoNewPrivileges`, `ProtectSystem`, `PrivateTmp`, `ProtectKernel*`, `RestrictSUIDSGID`, `LockPersonality`; check `chcon`, `systemctl restart` and restore still work. Add `--no-same-owner` to the cloud-init `tar`. | M |
| P1 | Windows file ACLs in the product (WIN-02) | Apply a restrictive ACL from `v2-start` to the token file, user database, backups and config, so manual installs are covered as well as the installer. | M |
| P1 | Safer installers (WIN-03) | Do not delete the install directory when it holds a user database or backups; verify the SLSA attestation (`gh attestation verify`) or a cosign signature, not only a checksum fetched from the same release URL. | M |
| P1 | Container hardening defaults (HARD-01) | `readOnlyRootFilesystem: true` with `emptyDir` for `/tmp` (the API image already runs read-only with only `/data` writable), default CPU and memory requests, pod anti-affinity for the two-replica deployments, `unhealthyPodEvictionPolicy` on the PDBs. Test runs, backup and restore under it. | M |
| P1 | Secure cookie by default | Plain-HTTP host installs send a session cookie without `Secure`. Prefer auto-TLS (`--auto-tls-dir`) or a first-run warning in the UI. | M |
| P1 | Browser accessibility in CI | Run axe (including contrast) against the demo build in Playwright for every route and theme, with populated data; add keyboard-only and 400% zoom checks. Today only jsdom tests run in CI, which cannot see contrast. | M |
| P2 | Public diagnostic endpoints (SEC-10) | `meta/routes`, `openapi.json` and `metrics/rate-limit` are readable without sign-in so Swagger works. Require a session for the rate-limit counters. | S |
| P2 | Freshness scoping (FRESH-01) | `/api/v1/freshness` returns every schedule and the global overdue flag to cluster-scoped users; filter schedules to the environments they can see. | S |
| P2 | Trust ledger (SEC-07 note) | The ledger trusts each file against any recorded set, so a restore can mix files from different versions that each ran here. Prefer whole recorded sets. | S |
| P2 | Unreleased-image build | `Dockerfile.runner` needs a prebuilt `dist/` binary and cannot be built from a plain checkout; add a documented target or a multi-stage variant. | S |
| P2 | Flaky test | `SessionIdleGuard.test.tsx` ("offers Sign in again when the session cannot be extended") failed once in a loaded full run. Use fake timers. | S |
| P2 | Password hashing cost | bcrypt uses the library default (10); raise it and rehash on next sign-in. | S |

Checks that still need an environment this audit did not have (multi-node
Kubernetes with RWX storage and an enforcing CNI, a Rocky Linux systemd
host, a full Windows install, a screen reader, a large fleet) are collected
in [`docs/audit/environment-test-prompt.md`](audit/environment-test-prompt.md).

See [`docs/FEATURES_AND_CONFIG_FLAGS.md`](FEATURES_AND_CONFIG_FLAGS.md),
[`docs/SECURITY_AND_TRUST.md`](SECURITY_AND_TRUST.md),
[`docs/SCALING.md`](SCALING.md), and
[`docs/THIRD_PARTY_DEPENDENCIES.md`](THIRD_PARTY_DEPENDENCIES.md) for the
canonical operational and security details.

_Last reviewed: 2026-11-19._
