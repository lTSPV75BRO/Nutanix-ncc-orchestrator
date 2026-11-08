# Improvements backlog

This is the active backlog for NCC Orchestrator. It contains proposed work that
is not already shipped in v2.3.x. Completed work belongs in
[`CHANGELOG.md`](../CHANGELOG.md) and the release notes
([`v2.3.0`](releases/RELEASE_NOTES_v2.3.0.md)).

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
| Report retention | Run history (`retain-last`, `retain-days`) and generated artifacts (`artifact-retain-days`, `artifact-retain-max-files`) already prune. Remaining: raw NCC logs, and a dry-run preview of what would be removed. | M |

## P1 — security and operations

| Item | Scope | Size |
| --- | --- | --- |
| Narrow Kubernetes RBAC | Split user-store and CronJob permissions; remove unnecessary Job access and use a dedicated scheduler boundary. | M |
| Secret rotation workflow | Document and optionally guide rotation of API tokens, JWT secrets, backup keys, and Prism credentials across replicas. | M |
| Outbound URL policy | Provide an administrator-visible allowlist/policy for Prism Central, LDAP, OIDC, SMTP, webhook, and update-server destinations. | M |
| Restore verifies release signatures (SEC-07) | A restore can replace stack binaries without checking them against the release checksums/signatures. | M |
| Signed image and SBOM verification | Publish Cosign signatures/SBOMs and optionally verify image provenance during deployment. | M |
| Kubernetes failure alerts | Provide PrometheusRule examples for failed CronJobs, stale reports, unavailable API replicas, PVC pressure, and version skew. | S |
| Isolated backup verification | Validate archive contents, secrets, report paths, and version compatibility without changing the live install. | M |

## P1 — operator experience

| Item | Scope | Size |
| --- | --- | --- |
| Stale-report UX | Clearly show when stale data is being served, a refresh is in progress, or the UI has fallen back to a previous report. | S–M |
| Notification delivery history | Every attempt, retry and final failure is now logged (System → Logs, API server). Remaining: a per-channel status view with last attempt, retry count and dead-letter state. | M |
| Operator-safe empty states | Give direct actions for missing config, missing clusters, failed first runs, missing schedules, and restricted cluster groups. | S |
| Session and lockout clarity | Show remaining session time and actionable account-lockout countdowns without exposing sensitive details. | S–M |

## P2 — engineering and test coverage

| Item | Scope | Size |
| --- | --- | --- |
| Kubernetes API contract tests | Fake Kubernetes API errors, missing RBAC, suspended schedules, invalid cron, and concurrent updates. | M |
| Race and load test profile | Run race tests plus concurrent report, alert, login, and schedule requests against a representative large report. | M |
| Report package extraction | Move the remaining renderers from `goNCC.go` into focused `internal/report` packages with benchmarks. | L |
| Support-bundle automation | Collect redacted health, events, component versions, CronJob/PVC state, and recent logs into one download (the logs are already redacted and listed by `GET /api/v1/logs`). | M |

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

Already covered by v2.3.x and therefore tracked elsewhere:

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

See [`docs/FEATURES_AND_CONFIG_FLAGS.md`](FEATURES_AND_CONFIG_FLAGS.md),
[`docs/SECURITY_AND_TRUST.md`](SECURITY_AND_TRUST.md),
[`docs/SCALING.md`](SCALING.md), and
[`docs/THIRD_PARTY_DEPENDENCIES.md`](THIRD_PARTY_DEPENDENCIES.md) for the
canonical operational and security details.

_Last reviewed: 2026-11-08._
