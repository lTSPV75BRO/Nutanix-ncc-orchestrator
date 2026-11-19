# Findings register

Every finding from the 2026 reviews of v2.2.1 and v2.4.0, with its current
status. The narrative for the security items is in
[`security-assessment.md`](security-assessment.md); the method and results of
the checks are in [`README.md`](README.md).

**Severity:** Critical (severe compromise, data corruption or outage), High,
Medium, Low. **Priority:** P0 fix now, P1 before the next release, P2 planned,
P3 optional. No finding was rated Critical.

## Open

| ID | Area | Finding | Severity | Priority |
| --- | --- | --- | --- | --- |
| LIN-01 | Install | The systemd unit written by `v2-install-service` runs as root with no sandboxing (`NoNewPrivileges`, `ProtectSystem`, `PrivateTmp` unset). Needs testing on a systemd host because the service restarts itself with `systemctl` and runs `chcon`. | Medium | P2 |
| WIN-02 | Install | The product sets no Windows ACLs on the token, user database, backups or config. Only the installer restricts its directory, so manual installs under `ProgramData` inherit permissive rights. | Medium | P2 |
| WIN-03 | Install | Remaining part: the cloud-init script still deletes an existing install directory, and neither installer verifies the SLSA attestation (they check a SHA-256 fetched from the same release URL). The Windows installer no longer deletes an existing install. | Medium | P2 |
| HARD-01 | Deploy | No container sets `readOnlyRootFilesystem`, default CPU and memory requests are empty, and the two-replica deployments have no pod anti-affinity. The API image runs correctly read-only with only `/data` writable; restore, retention preview and runs were not exercised under it. | Low-Medium | P2 |
| A11Y-02 | UI | Accessibility is checked in a browser by hand. Populated pages (tables, charts, drawers), 400% zoom and a screen reader are not covered, and the browser pass is not in CI. | Medium | P2 |
| CI-02 | CI | Nothing installs the Helm chart in CI, so a chart that cannot start (see K8S-02) is not caught. | Medium | P1 |
| SEC-10 | API | `meta/routes`, `openapi.json` and `metrics/rate-limit` are readable without sign-in so the Swagger page works. | Low | P3 |
| SEC-11 | API | A plain-HTTP host install sends the session cookie without `Secure` unless TLS or `--cookie-secure` is set (the chart sets it). bcrypt uses the library default cost (10). | Low | P3 |
| FRESH-01 | API | `/api/v1/freshness` returns all schedules and the global overdue flag to cluster-scoped users. Metadata only. | Low | P3 |
| SEC-07b | Restore | The trusted-stack ledger judges each file against any recorded set, so a restore can mix files from different versions that each ran on the host. No new code is admitted. | Low | P3 |
| ENV-11 | Deploy | Once, both API replicas landed on the same node during a rollout surge despite the topology spread constraint. | Low | P3 |
| TEST-02 | Tests | `SessionIdleGuard.test.tsx` failed once in a loaded full run and passed on rerun. Use fake timers. | Low | P3 |
| MAINT-01 | Code | `goNCC.go` (18.9k lines), the API `main.go` (6.3k) and `AccessSection.tsx` are very large single files. Split by area when touching them. | Medium | P3 |

Accepted risk: **DEP-03**, `GO-2026-6909` in `crewjam/saml` v0.6.3, is a
reviewed false positive. The fix landed in v0.5.2 and v0.6.3 contains it (the
`Subject` and `Conditions` nil checks are in `validateAssertion`). It is
recorded in `scripts/govulncheck-accepted.txt` with a review-by date of
2027-01-31.

## Resolved

### Security

| ID | Finding | Fix |
| --- | --- | --- |
| SEC-01 | A deleted local account kept its session and role until expiry, and a re-created name revived old tokens. | Deletion records a tombstone; sessions issued before it are rejected. |
| SEC-02 | An operator could direct `--prom-dir` outside the install root. | Path-valued extra arguments are confined like config paths. |
| SEC-03 | Anyone who could reach the UI could force a reset of the admin password. | Off by default; opt in with `--allow-admin-self-reset`. |
| SEC-04 | The MCP `create_schedule` tool installed an arbitrary command. | Refused unless `NCC_MCP_ALLOW_SCHEDULE_COMMAND=true`. |
| SEC-05 | The MCP server passed the Prism password on the command line. | Passed only through `NCC_PASSWORD`. |
| SEC-06 | Email header injection through a templated subject in the runner. | Headers are flattened and RFC 2047 encoded; CR/LF addresses rejected. |
| SEC-07 | A restore could replace the stack binaries with unverified files. | Restore installs only program files this host has run (`.ncc-trusted-stack.json`). |
| SEC-08 | The UI-to-API transport honoured `HTTP(S)_PROXY`, sending the admin token through a proxy. | The transport ignores proxy variables. |
| SEC-09 | An unknown role in a signed session mapped to admin. | Unknown or empty roles are rejected. |
| K8S-01 | The Helm API pod had no security context; its init container ran as root. | Non-root, `fsGroup` and seccomp at pod level; restricted init container. |
| WIN-01 | The Windows install directory under `ProgramData` was writable by every local user while the stack ran as SYSTEM. | The installer restricts it to SYSTEM and Administrators. |
| CI-01 | The publishing workflows relied on a comment for "no cache" and could restore a poisoned cache. | Caches are disabled explicitly. |
| OPS-03 | No secret scanning in CI. | gitleaks runs in CI; a full-history scan is clean. |

### Correctness and reliability

| ID | Finding | Fix |
| --- | --- | --- |
| K8S-02 | The Helm API pod crash-looped on first start (also in the published 2.3.0 image). | The chart passes the full flag set; a configured Secret or `NCC_USERS_DB` suppresses the `--users-db` default. |
| ENV-03 | The Helm runner CronJob failed with `mkdir logs: permission denied`. | It runs from `/data` with explicit paths and `--skip-preflight-check`. |
| WIN-04 | The Windows installer wrote an invalid `config.yaml` (backslash paths in double quotes). | Paths are single-quoted. |
| COR-09 | `upsertYAMLScalar` dropped indentation, so a restore moved nested config keys to the top level. | Indentation is kept; a `$` in a value is literal. |
| COR-08 | A dropped or busy LDAP directory during sign-in counted as a wrong password and fed the lockout. | Only a refused credential counts; tested against a real LDAP conversation. |
| ENV-01 | Creating a user on a second replica returned HTTP 500 on a store conflict. | Retried once, then 409. |
| ENV-02 | The user list on a replica missed users another replica had just created. | Account-management requests refresh the shared store first. |
| COR-01 | Windows: process liveness always reported "dead". | Correct process-liveness check on Windows. |
| COR-02 | The run extra-argument allowlist was out of sync with the runner. | Allowlist aligned with the runner; Runs page argument builder. |
| COR-03 | Windows: the secrets-file provider rejected every file. | Permission check fixed. |
| COR-04, COR-05, COR-06 | Webhook sender nil-request panic; runner always attempted SMTP AUTH; post-restore path healing skipped Windows. | Fixed. |
| DATA-01 | Lost updates to the user store across API replicas. | Reload before write, file lock, Kubernetes `resourceVersion` precondition, 409 on conflict. |
| DATA-02 | A restore had no pre-restore snapshot or rollback. | Transactional restore with automatic rollback. |
| REL-01 to REL-04 | Long operations cut off by timeouts; SMTP without timeouts; no graceful shutdown; readiness did not check dependencies. | Timeouts, graceful shutdown and `/api/v1/health/ready`. |
| OBS-01, OBS-02 | No request ids or per-route metrics. | `X-Request-ID` and `ncc_http_request*` metrics. |
| LOG-01 | Thin logs for LDAP, sign-in failures, backup and restore errors, run summaries and version skew. | Structured warnings with user, client address and request id; never passwords. |
| ENV-04, ENV-05, ENV-06 | Helm PVC had no storage settings; backup key requirement was undocumented; cloud-init `tar` kept foreign owner ids. | `persistence.*` values, chart README, `--no-same-owner`. |
| ENV-08 | `v2-install-service` hosts had no log maintenance timer. | The timer is installed with the service. |
| ENV-10 | `helm upgrade` (Helm 4 server-side apply) conflicts with the API-edited CronJob schedule. | Documented in the chart README (`--force-conflicts`). |
| PERF-01 | Authenticated `/api/v1/health` took 18 ms on Windows. | Display paths cached for 30 s (0.8 ms). |
| OPS-01 | No Windows CI job; golden fixtures broke on CRLF checkouts. | Windows job added. |

### UI, dependencies and quality

| ID | Finding | Fix |
| --- | --- | --- |
| A11Y-01 | WCAG A/AA failures found with axe in a browser: colour contrast, label in name, ARIA role, heading order, target size, scroll focus. | Fixed; 0 violations on 16 routes in three themes (empty-data pages). |
| UX-01, UX-02, UX-05 to UX-08 | Health pill, setup step titles, run-arguments builder, per-cluster freshness and stale-schedule banner, config-preference 404. | Fixed or delivered. |
| ONB-01, LOGIN-01 | Browser-local setup modal; sign-in form hidden on directory-only installs. | Server-derived setup checklist; redesigned sign-in. |
| DEP-01, DEP-02 | Go module bumps; npm dev-tool advisories. | Updated; `npm audit` reports 0. |
| OPS-02 | Dependabot and a full `npm audit`. | Added. |
| TEST-01 | Monaco test timeout flake. | Fixed. |
