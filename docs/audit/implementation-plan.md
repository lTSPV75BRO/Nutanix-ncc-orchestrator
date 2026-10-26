# Implementation plan

Severity: Critical/High/Medium/Low. Priority: P0 (contain now) – P3
(optional). Confidence: verified / strongly suspected / unverified. Effort
assumes one engineer familiar with the codebase, including tests and review.
Status: not started · in progress · implemented · verified · blocked.

No Critical or P0 findings were identified.

| ID | Finding or improvement | Severity | Priority | Confidence | Effort | Dependencies | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SEC-01 | Deleted local account kept its session and role until expiry; re-created name revived old tokens | High | P1 | verified | 1 d | — | **verified** |
| COR-01 | Windows: process liveness always "dead" (restore over running stack, duplicate supervisor, hidden scheduled runs, forced kills) | High | P1 | verified | 1.5 d | — | **verified** (Windows CI job pending first run) |
| COR-02 | Run extra-args allowlist out of sync with runner; UI/OpenAPI samples produced failing runs | High | P1 | verified | 0.5 d | — | **verified** (incl. live) |
| SEC-03 | Anonymous forced rotation of the admin password (by design) | High | P1 | verified | 0.5 d — | **verified** (off by default; `--allow-admin-self-reset` opt-in) |
| DATA-01 | User-store lost updates across API replicas (no optimistic concurrency) | Medium-High | P1 | strongly suspected | 3–4 d | — | **verified** (reload-before-write + file lock + Kubernetes resourceVersion precondition; 409 on conflict) |
| DATA-02 | Restore has no pre-restore snapshot / rollback | Medium | P1 | verified (code) | 2–3 d | — | **verified** (transactional restore with automatic rollback) |
| DEP-02 | npm dev tooling advisories (vite 8.3.1, undici, source-map-js, @vitest/mocker) | Medium | P1 | verified | 0.5 d | npm CLI | **blocked** (no npm here; commands in dependency-health) |
| REL-01 | Long operations cut off by server/proxy/client timeouts | Medium | P1 | verified | 1 d | — | **verified** |
| REL-02 | SMTP sends without timeouts (API and runner) | Medium | P1 | verified | 0.5 d | — | **verified** |
| REL-03 | No graceful shutdown in API/UI servers | Medium | P1 | verified (code) | 0.5 d | — | **verified** (unit) |
| COR-03 | Windows: secrets-file provider rejected every file | Medium | P1 | verified | 0.25 d | — | **verified** |
| OPS-01 | No Windows CI; golden fixtures broke on CRLF checkouts | Medium | P1 | verified | 0.5 d | — | **implemented** (job not yet run on GitHub) |
| SEC-02 | Operator-controlled `--prom-dir` escaped the install root | Medium | P1 | verified | 0.25 d | COR-02 | **verified** (incl. live) |
| SEC-04 | MCP `create_schedule` installed arbitrary commands | Medium | P2 | verified | 0.25 d | — | **verified** |
| SEC-05 | MCP passed Prism password on the command line | Medium | P2 | verified | 0.25 d | — | **verified** |
| SEC-06 | Runner email header injection via templated subject | Medium | P2 | verified | 0.25 d | — | **verified** |
| SEC-07 | Restore can replace stack binaries without release-signature check | Medium | P2 | verified (code) | 2 d | DATA-02 | not started |
| SEC-08 | UI→API transport honours `HTTP(S)_PROXY` (admin token via proxy) | Low-Medium | P2 | verified (code) | 0.25 d | — | **verified** (UI→API transport ignores proxy env) |
| REL-04 | Readiness probe does not check dependencies | Medium | P2 | verified (code) | 0.5 d | — | **verified** (`/api/v1/health/ready`; k8s/Helm readiness probes updated) |
| OBS-01 | No request/correlation IDs | Medium | P2 | verified (code) | 1 d | — | **verified** (X-Request-ID in responses, audit, errors, UI toasts) |
| OBS-02 | No per-route HTTP metrics | Medium | P2 | verified (code) | 1 d | — | **verified** (`ncc_http_requests_total`, `ncc_http_request_duration_seconds`) |
| UX-01 | Health pill showed red 0.0% with no data | Medium | P2 | verified (live) | 0.25 d | — | **verified** (incl. live) |
| UX-02 | Setup step titles broke mid-word | Low | P3 | verified (live) | 0.1 d | — | **verified** (live) |
| UX-05 | Run-arguments builder (F1) | Low | P2 | — | 3 d | COR-02 | **verified** (`GET /runs/extra-args` + Runs page controls) |
| UX-06/07 | Per-cluster freshness; stale-schedule banner (F3) | Medium | P2 | verified (code) | 2–3 d | — | not started |
| UX-10 | axe-core accessibility checks + keyboard walkthrough | Medium | P2 | unverified | 2 d | — | not started |
| OPS-02 | Dependabot; advisory full `npm audit` | Low | P2 | verified | 0.1 d | — | **implemented** |
| OPS-03 | Secret scanning in CI (gitleaks / push protection) | Low | P2 | — | 0.25 d | — | not started (manual scan done: clean) |
| COR-04 | Webhook sender nil-request panic | Low | P2 | verified | 0.1 d | — | **verified** |
| COR-05 | Runner always attempted SMTP AUTH | Low | P3 | verified | 0.1 d | REL-02 | **verified** |
| COR-06 | Post-restore path healing skipped Windows | Low | P3 | verified | 0.1 d | — | **verified** |
| COR-07 / SEC-09 | Unknown session role maps to admin (fail-open) | Low | P3 | verified (code) | 0.25 d | — | **verified** (unknown/empty role rejected) |
| UX-08 | `/runs/config-preference` 404 in token mode | Low | P3 | verified (live) | 0.1 d | — | **verified** (preference fetched only for local accounts; non-fatal) |
| TEST-01 | Monaco test timeout flake | Low | P3 | verified | 0.1 d | — | **verified** |
| DEP-01 | Go x/crypto, x/sys, x/term, go-ldap patch/minor bumps | Low | P3 | verified | 0.1 d | — | **verified** |
| MAINT-01 | Split `goNCC.go` / API `main.go` incrementally | Medium | P3 | verified | ongoing | — | not started |

| ONB-01 | First-time setup modal: browser-local state, shown to viewers, manual checkboxes, broken restore detection, full-page reloads | Medium | P1 | verified (live) | 2 d | — | **verified** (server-derived setup checklist) |
| LOGIN-01 | Sign-in form hidden on directory-only installs; login/first-password restore could not use encrypted backups | High | P1 | verified (code) | 1.5 d | — | **verified** (redesigned screens, shared restore dialog) |

## Suggested sequence for the remaining work

1. **This week:** DEP-02 (npm dev-tool updates, needs npm).
2. **Next:** SEC-07 (verify restored binaries against signed release checksums).
3. **Then:** OPS-03 (secret scanning in CI); UX-06/07 (per-cluster freshness, stale-schedule banner); UX-10 (accessibility pass).
