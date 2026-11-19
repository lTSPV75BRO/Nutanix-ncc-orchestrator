# Roadmap

Planned and proposed work that is not yet in v2.4.0. Finished work is in
[`CHANGELOG.md`](../CHANGELOG.md) and the
[release notes](releases/RELEASE_NOTES_v2.4.0.md). Findings from the 2026
quality and security review, with IDs, are in
[`audit/findings.md`](audit/findings.md).

**Priority:** P0 blocks reliability or scale, P1 improves security or operator
experience, P2 improves maintainability and coverage. **Size:** S under two
days, M under a week, L longer.

## P0: reliability and scale

| Item | Scope | Size |
| --- | --- | --- |
| Helm install smoke test in CI | Install the chart on a `kind` cluster from images built in the job and wait for the API and UI to be ready; run `helm template` with `kubeconform` in the same job. The chart's API pod could not start and nothing in CI noticed (CI-02). | M |
| Server-side report pagination | Build only the requested `checks_snapshot` or `agg_rows` page instead of loading and filtering the whole report before slicing. | M |
| Kubernetes HA integration test | Two API replicas, shared storage, CronJob execution, schedule updates and pod rescheduling on separate nodes; add a 7,000-cluster data set with memory figures. | L |

## P1: security and operations

| Item | Scope | Size |
| --- | --- | --- |
| systemd hardening (LIN-01) | The unit written by `v2-install-service` runs as root with no sandboxing. Add a dedicated user and `NoNewPrivileges`, `ProtectSystem`, `PrivateTmp`, `ProtectKernel*`, `RestrictSUIDSGID` and `LockPersonality`; check `chcon`, `systemctl restart`, restore and in-app update still work. | M |
| Windows file ACLs in the product (WIN-02) | Apply a restrictive ACL from `v2-start` to the token file, user database, backups and config, so manual installs are covered as well as the installer. | M |
| Safer installers (WIN-03) | Apply the Windows installer's "never delete an existing install" rule to the cloud-init script, and verify the SLSA attestation (`gh attestation verify`) or a cosign signature in both, not only a checksum fetched from the same release URL. | M |
| Container hardening defaults (HARD-01) | `readOnlyRootFilesystem: true` with an `emptyDir` for `/tmp` (the API image already runs read-only with only `/data` writable), default CPU and memory requests, pod anti-affinity for the two-replica deployments. Test runs, backup and restore under it. | M |
| Secure cookie by default (SEC-11) | A plain-HTTP host install sends the session cookie without `Secure`. Prefer auto-TLS or a first-run warning in the UI. | M |
| Secret rotation workflow | Document and optionally guide rotation of API tokens, JWT secrets, backup keys and Prism credentials across replicas. | M |
| Outbound URL policy | An administrator-visible allowlist for Prism Central, LDAP, OIDC, SMTP, webhook and update-server destinations. | M |
| Signed image and SBOM verification | Publish Cosign signatures and SBOMs and optionally verify image provenance during deployment. | M |
| Signed trust for restored program files | Restore accepts program files only when this host ran them before. Publish per-file SHA-256 in the signed release checksums so a backup from another server can be verified without `--trust-stack`; prefer whole recorded sets over per-file trust (SEC-07b). | M |

## P1: operator experience

| Item | Scope | Size |
| --- | --- | --- |
| Browser accessibility in CI (A11Y-02) | Run axe-core, including contrast, against the demo build for every route and theme with populated data, plus keyboard-only and 400% zoom checks. Today only jsdom tests run in CI, which cannot see contrast. Add a table view for the trend and SLO charts. | M |
| Notification delivery history | Settings shows each channel's last attempt, success, error and dead-letter count. Add a browsable history of past deliveries and replay of dead-lettered messages from the UI. | S-M |
| Overdue schedules in the digest | Data age is shown across the UI and in notifications; add an overdue-schedule line to the runner digest email. | S |

## P2: engineering and test coverage

| Item | Scope | Size |
| --- | --- | --- |
| DAST in CI | An OWASP ZAP baseline scan against a docker-compose stack on `main`, and an authenticated role-matrix replay. | M |
| Kubernetes API contract tests | Fake Kubernetes API errors, missing RBAC, suspended schedules, invalid cron and concurrent updates. | M |
| Report package extraction | Move the remaining renderers from `goNCC.go` into focused `internal/report` packages with benchmarks. | L |
| Split the largest files | `goNCC.go` (18.9k lines), the API `main.go` (6.3k) and `AccessSection.tsx` (3.4k). Extract by area when touching it. | L |
| Path handling review (gosec G703) | CI excludes G703 because of 80+ path-taint hits on server-config paths. Review by area, move request-derived paths to `os.Root`, mark the rest with a reason, then drop the exclusion. | M |
| Support bundle: Kubernetes state | Add Kubernetes events, CronJob and PVC state, and component versions per pod to the bundle. | S-M |
| Public diagnostic endpoints (SEC-10) | `meta/routes`, `openapi.json` and `metrics/rate-limit` are readable without sign-in for the Swagger page. Require a session for the rate-limit counters. | S |
| Freshness scoping (FRESH-01) | Filter `/api/v1/freshness` schedules to the environments a cluster-scoped user can see. | S |
| Runner image from a checkout | `Dockerfile.runner` expects a prebuilt `dist/` binary and cannot be built from a plain clone; add a documented target or a multi-stage variant. | S |
| Flaky test (TEST-02) | `SessionIdleGuard.test.tsx` failed once in a loaded run; use fake timers. | S |
| Password hashing cost | bcrypt uses the library default (10); raise it and rehash on the next sign-in. | S |

## Performance sequence

When performance is the goal, in this order:

1. Server-side report pagination, and remove the dashboard's full-report follow-up.
2. A slim Insights payload; avoid rebuilding the same trend data.
3. Single-flight, multi-slot Prism alert caching.
4. Faster audit tailing and run-history sparkline reads.

Preview pagination, the alert pager, the five-minute Prism alert cache and gzip
API responses already exist; do not redo them.

## Deferred ideas

- Resumable or checkpointed runs and cluster-subset filters.
- Custom notification templates; interactive Slack and Teams messages.
- Multi-team profiles with separate output roots (environments already cover
  separate cluster sets and runner settings).
- A lightweight history UI for browsing archived reports.
- A minimal-install guide, an architecture diagram and an operator glossary.

## Delivered in v2.4.0

Setup checklist and redesigned sign-in; OIDC, RBAC, directory profiles for
several AD domains or forests; alert lifecycle and audit log; host and
Kubernetes scheduling with environments; readiness and liveness probes,
graceful shutdown and supervised updates; per-cluster data freshness and the
stopped-schedule banner; raw-log retention with a dry-run preview;
`v2-restore --verify-only`; restore that installs only program files the host
has run; narrow Kubernetes Roles and PrometheusRule alerts in the Helm chart;
report-cache state and a runner version-skew check; structured logs from every
process with a Logs page and a run-time level; an Integrations page; and the
fixes from the quality and security review.
