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
| DEP-02 | npm dev tooling advisories (vite 8.3.1, undici, source-map-js, @vitest/mocker) | Medium | P1 | verified | 0.5 d | npm CLI | **verified** (2026-11-19: osv-scanner on `frontend/package-lock.json`, 232 packages, 0 advisories) |
| REL-01 | Long operations cut off by server/proxy/client timeouts | Medium | P1 | verified | 1 d | — | **verified** |
| REL-02 | SMTP sends without timeouts (API and runner) | Medium | P1 | verified | 0.5 d | — | **verified** |
| REL-03 | No graceful shutdown in API/UI servers | Medium | P1 | verified (code) | 0.5 d | — | **verified** (unit) |
| COR-03 | Windows: secrets-file provider rejected every file | Medium | P1 | verified | 0.25 d | — | **verified** |
| OPS-01 | No Windows CI; golden fixtures broke on CRLF checkouts | Medium | P1 | verified | 0.5 d | — | **implemented** (job not yet run on GitHub) |
| SEC-02 | Operator-controlled `--prom-dir` escaped the install root | Medium | P1 | verified | 0.25 d | COR-02 | **verified** (incl. live) |
| SEC-04 | MCP `create_schedule` installed arbitrary commands | Medium | P2 | verified | 0.25 d | — | **verified** |
| SEC-05 | MCP passed Prism password on the command line | Medium | P2 | verified | 0.25 d | — | **verified** |
| SEC-06 | Runner email header injection via templated subject | Medium | P2 | verified | 0.25 d | — | **verified** |
| SEC-07 | Restore can replace stack binaries without release-signature check | Medium | P2 | verified (code) | 2 d | DATA-02 | **verified** (v2.4.0: stack restored only when every file matches the host's `.ncc-trusted-stack.json` ledger or the installed file; `--trust-stack` on the CLI) |
| SEC-08 | UI→API transport honours `HTTP(S)_PROXY` (admin token via proxy) | Low-Medium | P2 | verified (code) | 0.25 d | — | **verified** (UI→API transport ignores proxy env) |
| REL-04 | Readiness probe does not check dependencies | Medium | P2 | verified (code) | 0.5 d | — | **verified** (`/api/v1/health/ready`; k8s/Helm readiness probes updated) |
| OBS-01 | No request/correlation IDs | Medium | P2 | verified (code) | 1 d | — | **verified** (X-Request-ID in responses, audit, errors, UI toasts) |
| OBS-02 | No per-route HTTP metrics | Medium | P2 | verified (code) | 1 d | — | **verified** (`ncc_http_requests_total`, `ncc_http_request_duration_seconds`) |
| UX-01 | Health pill showed red 0.0% with no data | Medium | P2 | verified (live) | 0.25 d | — | **verified** (incl. live) |
| UX-02 | Setup step titles broke mid-word | Low | P3 | verified (live) | 0.1 d | — | **verified** (live) |
| UX-05 | Run-arguments builder (F1) | Low | P2 | — | 3 d | COR-02 | **verified** (`GET /runs/extra-args` + Runs page controls) |
| UX-06/07 | Per-cluster freshness; stale-schedule banner (F3) | Medium | P2 | verified (code) | 2–3 d | — | **verified** (v2.4.0: `/api/v1/freshness`, Overview data age and "Scheduled runs have stopped" banner) |
| UX-10 | axe-core accessibility checks + keyboard walkthrough | Medium | P2 | unverified | 2 d | — | **verified** for empty-data pages (live browser, see A11Y-01); populated pages, screen reader, 400% zoom open |
| OPS-02 | Dependabot; advisory full `npm audit` | Low | P2 | verified | 0.1 d | — | **implemented** |
| OPS-03 | Secret scanning in CI (gitleaks / push protection) | Low | P2 | — | 0.25 d | — | **verified** (gitleaks runs in CI `security-scan`; 2026-11-19 full-history scan, 264 commits, no leaks) |
| COR-04 | Webhook sender nil-request panic | Low | P2 | verified | 0.1 d | — | **verified** |
| COR-05 | Runner always attempted SMTP AUTH | Low | P3 | verified | 0.1 d | REL-02 | **verified** |
| COR-06 | Post-restore path healing skipped Windows | Low | P3 | verified | 0.1 d | — | **verified** |
| COR-07 / SEC-09 | Unknown session role maps to admin (fail-open) | Low | P3 | verified (code) | 0.25 d | — | **verified** (unknown/empty role rejected) |
| UX-08 | `/runs/config-preference` 404 in token mode | Low | P3 | verified (live) | 0.1 d | — | **verified** (preference fetched only for local accounts; non-fatal) |
| TEST-01 | Monaco test timeout flake | Low | P3 | verified | 0.1 d | — | **verified** |
| DEP-01 | Go x/crypto, x/sys, x/term, go-ldap patch/minor bumps | Low | P3 | verified | 0.1 d | — | **verified** |
| DEP-03 | `github.com/crewjam/saml` v0.6.3 advisory GO-2026-6909 | Low | P3 | verified false positive | — | vulndb fix version | **accepted**: the fix is in v0.5.2 and v0.6.3 contains it (nil checks for `Subject` and `Conditions` present in `validateAssertion`, checked in the module source); recorded in `scripts/govulncheck-accepted.txt` with review-by 2027-01-31; panics are also recovered into a logged 500 |
| FRESH-01 | `/api/v1/freshness` returns all schedules and the global overdue flag to cluster-scoped viewers | Low | P3 | verified (code) | 0.25 d | — | not started |
| COR-08 | LDAP: any failure of the user re-bind counted as a wrong password (network or busy directory fed the lockout) | Medium | P1 | verified (code) | 0.25 d | — | **verified** (unit test; not against a live directory) |
| LOG-01 | Troubleshooting logs: LDAP, sign-in failures, backup/restore errors, freshness, version skew | Low | P2 | — | 0.5 d | — | **implemented** |
| K8S-01 | Helm API pod had no security context; init container ran as root | Medium | P1 | verified (render, scanners) | 0.25 d | cluster test | **implemented** (not deployed) |
| HARD-01 | No read-only root filesystem, empty default resource limits, no pod anti-affinity (Helm, k8s/) | Low-Medium | P2 | verified (Trivy, kube-linter); API runs read-only in Docker | 1 d | cluster test | not started |
| SEC-10 | Public `meta/routes`, `openapi.json` and `metrics/rate-limit` | Low | P3 | verified (live) | 0.1 d | — | not started |
| K8S-02 | Helm API pod crash-looped on first start (stack-aware defaults vs container layout; chart args incomplete) | High | P0 | verified (live, kind) | 0.5 d | — | **verified** (kind cluster) |
| WIN-01 | Windows installer: install dir under ProgramData writable by any local user, run as SYSTEM | Medium | P1 | verified (this machine) | 0.25 d | — | **implemented** (ACL command tested on scratch dir; full install not run) |
| WIN-02 | Product sets no Windows ACLs on token/user DB/backups for non-installer installs | Medium | P2 | verified (code) | 1 d | — | not started |
| WIN-03 | Installers delete the install dir on re-run; checksum from same origin, no attestation check | Low | P3 | verified (code) | 0.5 d | — | not started |
| LIN-01 | systemd unit runs as root, no sandboxing | Medium | P2 | verified (code) | 1.5 d | Linux test host | not started |
| CI-01 | Cache poisoning exposure in publishing workflows | Low-Medium | P2 | verified (zizmor) | 0.1 d | — | **implemented** |
| SEC-11 | Default plain-HTTP host install: session cookie not `Secure` | Low | P3 | verified (live) | — | TLS setup | not started |
| PERF-01 | Authenticated `/health` 18 ms on Windows (path resolution per call) | Low | P3 | verified (measured) | 0.25 d | — | **verified** (0.8 ms) |
| A11Y-01 | WCAG A/AA failures found with axe in a real browser (contrast, label-in-name, ARIA role, headings, target size, scroll focus) | Medium | P1 | verified (live) | 1 d | — | **verified** (0 violations, 16 routes x 3 themes, empty-data pages) |
| TEST-02 | `SessionIdleGuard` test failed once under load | Low | P3 | strongly suspected (flake) | 0.25 d | — | not started |
| MAINT-01 | Split `goNCC.go` / API `main.go` incrementally | Medium | P3 | verified | ongoing | — | not started |

| ONB-01 | First-time setup modal: browser-local state, shown to viewers, manual checkboxes, broken restore detection, full-page reloads | Medium | P1 | verified (live) | 2 d | — | **verified** (server-derived setup checklist) |
| LOGIN-01 | Sign-in form hidden on directory-only installs; login/first-password restore could not use encrypted backups | High | P1 | verified (code) | 1.5 d | — | **verified** (redesigned screens, shared restore dialog) |

## Suggested sequence for the remaining work

1. **This week:** DEP-02 (npm dev-tool updates, needs npm).
2. **Next:** SEC-07 (verify restored binaries against signed release checksums).
3. **Then:** OPS-03 (secret scanning in CI); UX-06/07 (per-cluster freshness, stale-schedule banner); UX-10 (accessibility pass).
