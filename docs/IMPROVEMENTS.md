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
| Report-cache observability | Expose cache state (`cold`, `warm`, `refreshing`, `stale`, `failed`) in `/report/data` and Prometheus; refetch when async refresh completes. | M |
| Server-side report pagination | Build only the requested `checks_snapshot`/`agg_rows` page instead of loading and filtering the complete report before slicing. | M |
| Version-skew protection | Host installs already flag mismatched components (`consistent` in the update status, with a warning on Updates) and supervised upgrades check versions before finishing. Remaining: compare container image versions in Kubernetes and block incompatible combinations. | S–M |
| Kubernetes HA integration test | Exercise two API replicas, shared PVC access, CronJob execution, schedule updates, and pod rescheduling on separate nodes. | L |
| Panic-proof run workers | API background work now recovers from panics (`guard`, `ncc_api_background_panics_total`). Remaining: the same for the run manager's wait goroutine and the detached restart helper, plus an alert rule on the counter. | S |
| Report retention | Run history (`retain-last`, `retain-days`) and generated artifacts (`artifact-retain-days`, `artifact-retain-max-files`) already prune. Remaining: raw NCC logs, and a dry-run preview of what would be removed. | M |

## P1 — security and operations

| Item | Scope | Size |
| --- | --- | --- |
| Narrow Kubernetes RBAC | Split user-store and CronJob permissions; remove unnecessary Job access and use a dedicated scheduler boundary. | M |
| Secret rotation workflow | Document and optionally guide rotation of API tokens, JWT secrets, backup keys, and Prism credentials across replicas. | M |
| Outbound URL policy | Provide an administrator-visible allowlist/policy for Prism Central, LDAP, OIDC, SMTP, webhook, and update-server destinations. | M |
| Signed image and SBOM verification | Publish Cosign signatures/SBOMs and optionally verify image provenance during deployment. | M |
| Kubernetes failure alerts | Provide PrometheusRule examples for failed CronJobs, stale reports, unavailable API replicas, PVC pressure, and version skew. | S |
| Isolated backup verification | Validate archive contents, secrets, report paths, and version compatibility without changing the live install. | M |
| Signed trust for restored program files | Restore accepts program files only when this host ran them before (v2.4.0, `.ncc-trusted-stack.json`). Remaining: publish per-file SHA-256 in the signed release checksums so a backup from another server can be verified without `--trust-stack`. | M |
| Helm parity with `k8s/` | The Kustomize manifests ship NetworkPolicies (default-deny, API/UI ingress, egress, kube-API) and a PodDisruptionBudget; the Helm chart has neither. Add both, on by default. | S–M |
| CI credential hygiene | `ci.yml` checkouts keep the job token in `.git/config` (the other workflows set `persist-credentials: false`). Set it there too, and pin first-party `actions/*` and `github/codeql-action` by commit SHA like the third-party actions. | S |
| Second static analyser in CI | CI runs golangci-lint, govulncheck, CodeQL and Trivy. Add gosec (or the golangci `gosec` linter) and secret scanning (gitleaks) so a regression in file permissions, `exec` or TLS settings fails the build. | S |
| GitHub token scope | `GITHUB_TOKEN` is now sent only over HTTPS to `github.com` / `githubusercontent.com` hosts (v2.4.0). Remaining: document that a fine-grained, read-only public-repo token is enough, and warn at startup when the token has more scope. | S |

## P1 — operator experience

| Item | Scope | Size |
| --- | --- | --- |
| Stale-report UX | Clearly show when stale data is being served, a refresh is in progress, or the UI has fallen back to a previous report. | S–M |
| Notification delivery history | Every attempt is logged (System → Logs) and Settings → Notifications shows each channel's last attempt, last success, last error, tries and dead-letter count (v2.4.0). Remaining: a browsable history of past deliveries, and replaying dead-lettered messages from the UI. | S–M |
| Operator-safe empty states | Give direct actions for missing config, missing clusters, failed first runs, missing schedules, and restricted cluster groups. | S |
| Session and lockout clarity | Show remaining session time and actionable account-lockout countdowns without exposing sensitive details. | S–M |
| One name for findings | The page is **Findings**, but its table is titled "Alerts" with "alerts shown", "Search alerts" and "Acknowledged/Resolved alerts". Pick one word for NCC results (keep "alerts" for Prism Central) and use it in the UI, docs and API descriptions. | S |
| Freshness beyond the Overview | Per-cluster data age and the stopped-schedule banner are on the Overview (v2.4.0). Remaining: the same age on Findings rows and the cluster filter, on Insights → Clusters, in the run report email, and as Prometheus gauges (`ncc_cluster_last_success_timestamp`, `ncc_schedule_overdue`). | M |
| Drafts that can't overwrite | The Schedule form now loads the saved schedule unless this browser holds an unsaved draft (v2.4.0). Remaining: the same rule for the other forms that keep drafts in local storage, and a "changed by someone else since you opened it" check on host schedule saves (Kubernetes already has it). | S–M |
| Accessibility checks in CI (UX-10) | A scripted pass found an icon-only button in every Findings row and unnamed role pickers (both fixed in v2.4.0). Add axe-core to the frontend tests for each page and a keyboard walkthrough, so these are caught before release; add a table view for the trend and SLO charts (F7). | M |
| Narrow-width toolbars | At ~800 px the Findings environment picker truncates to "All environme…" and the filter row wraps unevenly. Give toolbar selects a minimum width or move secondary filters into a "Filters" popover below 900 px. | S |
| Plural wording from the server | The UI no longer prints "cluster(s)" (v2.4.0), but setup-checklist details and some API messages still do (`onboarding.go`). Return counts and let the UI word them, or pluralise in Go. | S |

## P2 — engineering and test coverage

| Item | Scope | Size |
| --- | --- | --- |
| Kubernetes API contract tests | Fake Kubernetes API errors, missing RBAC, suspended schedules, invalid cron, and concurrent updates. | M |
| Race and load test profile | Run race tests plus concurrent report, alert, login, and schedule requests against a representative large report. | M |
| Report package extraction | Move the remaining renderers from `goNCC.go` into focused `internal/report` packages with benchmarks. | L |
| Split the three largest files | `goNCC.go` (18.7k lines), API `main.go` (6.3k) and `AccessSection.tsx` (3.4k) make review and ownership hard and slow the test loop. Extract by area when touching it: Prism client, reporting, install/update and backup from the runner; route table, OpenAPI and report handlers from the API; one file per sign-in provider in the UI. | L |
| Bounded reads everywhere | Prism Central discovery responses are now capped (v2.4.0). Add a lint rule (forbid bare `io.ReadAll` on network bodies) so new call sites cannot regress. | S |
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
- Directory profiles for several AD domains or forests, and refusal of
  directory/SAML users named like a local account.

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

See [`docs/FEATURES_AND_CONFIG_FLAGS.md`](FEATURES_AND_CONFIG_FLAGS.md),
[`docs/SECURITY_AND_TRUST.md`](SECURITY_AND_TRUST.md),
[`docs/SCALING.md`](SCALING.md), and
[`docs/THIRD_PARTY_DEPENDENCIES.md`](THIRD_PARTY_DEPENDENCIES.md) for the
canonical operational and security details.

_Last reviewed: 2026-11-16._
