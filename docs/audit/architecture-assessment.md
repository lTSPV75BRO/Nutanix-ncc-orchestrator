# Architecture assessment

| | |
| --- | --- |
| Scope | Branch `v2.2.1` at `b8e47e6d` plus the working-tree changes described in [`implementation-plan.md`](implementation-plan.md) |
| Date | 2026-10-26 |
| Method | Source reading, tracing of critical workflows, baseline build/test/lint/scan runs (see [`verification-results.md`](verification-results.md)) |
| Prior work | [`docs/security/security-assessment-2026-10.md`](../security/security-assessment-2026-10.md) (2026-10-23) — its findings are not repeated here unless still open |

## 1. What the system is

NCC Orchestrator runs Nutanix Cluster Check (NCC) across many Nutanix clusters
in parallel, collects the results, and presents them in a web UI with history,
alert triage, scheduling, RBAC/SSO, and an API for ServiceNow/Prometheus-style
integrations. It is an operations tool for infrastructure teams; the data it
handles includes Prism credentials, cluster health findings, and the
operators' own identities.

## 2. Components and stack

| Component | Path | Technology | Size |
| --- | --- | --- | --- |
| Runner (`ncc-orchestrator`) | `goNCC.go` + root `*.go` | Go 1.27.3, cobra/viper, zerolog, OTel | `goNCC.go` alone is 18,028 lines, 338 functions, 31 cobra commands |
| API server | `cmd/ncc-api-server` | Go `net/http`, no framework | `main.go` 5.9k lines, 79 routes |
| UI server | `cmd/ncc-ui-server` | Go static host + `httputil.ReverseProxy` | 1.1k lines |
| MCP server | `cmd/ncc-mcp-server` | `modelcontextprotocol/go-sdk`, stdio transport | 0.75k lines |
| Shared packages | `internal/*` | auth (JWT/PAT), httpclient, notify, model, nccparse, promtext, retryutil, runtimecaps, selfsigned, trace, v2layout; new: `procutil`, `graceful` | |
| Web app | `frontend/` | React 19, Ant Design 6, TanStack Query 5, react-router 8, Vite 8 (rolldown), TypeScript 7, Vitest 4 | ~23.8k lines |
| Deployment | `k8s/`, `helm/`, `deploy/`, `Dockerfile*` | Kustomize, Helm, Alpine 3.24 images (UID 10001), cloud-init, Windows Sysprep | |
| CI/CD | `.github/workflows` | build/vet/race tests, golangci-lint v2.15.0, govulncheck, npm audit, Trivy, CodeQL, SBOM + build provenance on release | |

Persistence is **filesystem-only**: no database. State lives in JSON/YAML
files under the install root (or a shared PVC in Kubernetes):
`config.yaml`, the user store (`users.json` or a Kubernetes Secret, optionally
AES-256-GCM envelope-encrypted), schedule/notification/backup-schedule state,
the alert lifecycle store, run artifacts under `outputfiles/` and raw NCC logs
under `nccfiles/`, audit JSONL, encrypted backup archives.

## 3. Runtime topology

```
browser ──► ncc-ui-server (:8080/443) ──/api/v1/*──► ncc-api-server (:8081) ──exec──► ncc-orchestrator
              static SPA + proxy                       RBAC, state files             Prism Central / Prism Element
              injects token (no-login mode)            run manager (queue, N slots)  REST (NCC run/poll/parse)
              or forwards session cookies              schedulers, notifications     writes artifacts + .prom
```

* The API server never talks to Prism for runs; it **executes the runner as a
  child process** per run (and for preflight, backup, restore, doctor, update).
  The runner writes artifacts; the API reads them back.
* Scheduled runs are launched by the OS scheduler (cron, systemd timer,
  Windows Task Scheduler, Kubernetes CronJob), not by the API.
* Authentication: static admin/viewer tokens, HMAC session cookies (legacy),
  HS256 JWTs (`auth_token`), hashed PATs, local accounts (bcrypt), LDAP/AD,
  SAML. Roles: viewer < operator < admin, plus cluster-group scoping.

## 4. Critical workflows traced

| Workflow | Path | Notes |
| --- | --- | --- |
| Login | `LoginPage` → `POST /auth/login` → `handleLogin` → loginGuard → local bcrypt → LDAP fallback → HMAC session + CSRF cookie + JWT cookie | Session validation: `withAuth` → legacy HMAC → `auth.Authenticate` (static/JWT/PAT) → `principalFromIdentity` (token generation check) → `authorizePrincipal` (must-change, RBAC, CSRF) |
| Trigger run | `RunsSection` → `POST /runs/trigger` → `sanitizeExtraArgs` → cluster-group scope → `submitRun` (dedupe, queue) → `launchRun` → `exec ncc-orchestrator --config … --output-dir-filtered .runs-live/<id>` → finalize/merge → notify | Extra-arg allowlist was out of sync with the runner (fixed, COR-02) |
| Scheduled run | OS scheduler → runner → heartbeat `.ncc-run-active-<pid>.json` → API `externalActiveRuns` | Heartbeats were deleted on Windows (fixed, COR-01) |
| Alert triage | `/alerts/ncc-dispositions` → lifecycle store under cross-process lock | Hardened in v2.2.1 |
| Backup / restore | `/settings/backups*`, `/settings/restore` → `ncc-orchestrator v2-backup / v2-restore --force` → self-heal → detached restart | No pre-restore snapshot (DATA-02); deadlines fixed (REL-01) |
| In-app update | `/settings/update/apply` → async job → pre-update backup → `update` (checksum-verified) → restart watcher | |

## 5. Strengths

* Defense in depth is visible throughout: path confinement with symlink
  checks, argument allowlists, CSRF double-submit, tiered `/health`, rate
  limiting, login guard shared across replicas, encrypted backups by default,
  hashed PATs, security headers/CSP on both servers, non-root containers,
  pinned CI actions where third-party, SBOM and build provenance on release.
* Extensive Go tests (476 test functions at baseline) including e2e tests over
  the real handler stack; regression tests accompany most past fixes.
* Clear operational tooling: `doctor`/self-heal, support bundle, Prometheus
  metrics, audit log with rotation and SIEM forwarding.

## 6. Weaknesses

| ID | Weakness | Impact |
| --- | --- | --- |
| ARCH-01 | **Monolithic runner** — `goNCC.go` is an 18k-line `package main` file mixing CLI, Prism client, parsing, reporting, notifications, install/update, backup/restore | High change risk, slow review, hard to test units in isolation; duplicate logic between runner and API (e.g. two email senders, two liveness probes) |
| ARCH-02 | **API server `main.go` (5.9k lines)** holds routing, handlers, OpenAPI catalog and config logic | Same; route catalog, `routeMinRole`, and handlers must be kept in sync by hand |
| ARCH-03 | **Two contracts kept in sync by convention**: API extra-arg allowlist vs runner flags; UI client timeouts vs server operation timeouts | Both drifted (COR-02, REL-01). Now guarded by a test (`TestExtraArgsAllowlistMatchesRunnerFlags`) and shared constants respectively |
| ARCH-04 | **File-based shared state across replicas** without optimistic concurrency (user store) | Lost updates possible (DATA-01) |
| ARCH-05 | **Platform coverage gap** — Windows is a supported target but was never exercised in CI | Windows-only defects shipped (COR-01, COR-03) |
| ARCH-06 | No graceful shutdown, no request IDs, readiness == liveness | See [`reliability-and-observability.md`](reliability-and-observability.md) |

## 7. Constraints

* No database is a deliberate choice (simple installs, Windows/VM/k8s parity);
  recommendations below keep it.
* The API delegates to the runner binary; process lifecycle correctness
  (liveness, identity, shutdown) is therefore central, especially on Windows.
* Production telemetry, real Prism endpoints and deployed clusters were not
  available to this audit.

## 8. Baseline (before changes)

Full detail in [`verification-results.md`](verification-results.md). In short,
on Windows 11 / Go 1.27.3: build and vet pass; **13 Go tests failed** (Windows
defects and non-portable tests); golangci-lint 0 issues; govulncheck 0
reachable vulnerabilities; frontend `tsc` passes, Vitest 44/45 (one timeout
flake), production build succeeds; osv-scanner reports 12 fixable advisories,
all in frontend dev tooling.
