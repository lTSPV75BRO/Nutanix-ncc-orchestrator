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
| A11Y-02 | UI | Accessibility is checked in a browser by hand. Populated pages (tables, charts, drawers), 400% zoom and a screen reader are not covered, and the browser pass is not in CI. | Medium | P2 |
| REL-05 | Release | The published v2.3.0 files were uploaded by hand on 2026-11-08 and are not the CI build, so none has a build attestation and `gh attestation verify` fails for them. The CI rebuild of 2026-11-19 was attested but not published: "Publish GitHub Release" failed with `Resource not accessible by integration`. v2.2.1 `checksums.txt` has no attestation either. Publish 2.4.0 from the workflow only, and check one file with `gh attestation verify` before announcing it. | Medium | P1 |
| MCP-1 | MCP | `confirm: "APPLY"` is supplied by the model, and the HMAC proof is keyed by the PAT, so neither is a security boundary (documented). With `NCC_MCP_ALLOW_MUTATIONS=true`, the PAT's role is the only limit; use a least-privilege PAT and keep the host's tool approval on. | Low | P3 |
| SEC-07b | Restore | The trusted-stack ledger judges each file against any recorded set, so a restore can mix files from different versions that each ran on the host. No new code is admitted. | Low | P3 |
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
| SEC-04 | The MCP `create_schedule` tool installed an arbitrary command. | Custom commands are never accepted; schedules go through `PUT /api/v1/schedule`. |
| SEC-05 | The MCP server passed the Prism password on the command line. | MCP tools refuse password arguments; credentials stay on the API server (`secret://`). |
| SEC-06 | Email header injection through a templated subject in the runner. | Headers are flattened and RFC 2047 encoded; CR/LF addresses rejected. |
| SEC-07 | A restore could replace the stack binaries with unverified files. | Restore installs only program files this host has run (`.ncc-trusted-stack.json`). |
| SEC-13 | The MCP configuration tools returned the raw config file, including inline credentials, to the model. | Tool results are scrubbed in one place; only the redacted copy is returned. |
| SEC-14 | The "redacted" config view masked only password, token and secret lines. | Key-name based redaction (webhooks, API keys, HMAC, passphrases, URL credentials); references and switches kept. |
| SEC-15 | `backup_v2_stack` downloaded an archive, discarded it and reported success; restore accepted any path-like name. | It creates a server-side snapshot and returns its name; restore accepts only a backup name. |
| SEC-18 | The audit log API answered any signed-in viewer (users, addresses, token names, request details). | Reading it needs the operator role, matching the dashboard; regression test. |
| SEC-17 | A build with an embedded release key accepted a release that had no signature asset. | Signed builds require the signature when installing an update; unsigned builds are unchanged (signing stays opt-in at build time). |
| SEC-16 | The engine's check for a legacy `ncc-mcp-server` parent used `ps`, so it never matched on Windows. | The parent image name is read through the Windows API; tested from a parent named `ncc-mcp-server.exe`. |
| SEC-12 | **High.** The MCP server (0.1.13 to 2.3.0) ran tools and read resources with no credential. | Tools call the REST API only (no orchestrator subprocess). Requires `NCC_MCP_TOKEN`, HMAC proof (`ncc-mcp-proof-v1`), and API RBAC. Stamping version 2.4.0 on a 2.3.x binary is refused. |
| SEC-08 | The UI-to-API transport honoured `HTTP(S)_PROXY`, sending the admin token through a proxy. | The transport ignores proxy variables. |
| SEC-09 | An unknown role in a signed session mapped to admin. | Unknown or empty roles are rejected. |
| K8S-01 | The Helm API pod had no security context; its init container ran as root. | Non-root, `fsGroup` and seccomp at pod level; restricted init container. |
| WIN-01 | The Windows install directory under `ProgramData` was writable by every local user while the stack ran as SYSTEM. | The installer restricts it to SYSTEM and Administrators. |
| CI-01 | The publishing workflows relied on a comment for "no cache" and could restore a poisoned cache. | Caches are disabled explicitly. |
| WIN-03 | The installers could delete an existing install, and checked only a SHA-256 from the same release URL. | Neither deletes a non-empty directory without `NCC_FORCE_CLEAN=1`; both check the build attestation (`NCC_VERIFY_ATTESTATION` = `auto`, `require`, `off`). The Linux script was run in a container for 11 cases; the Windows change was checked by parsing it and running its `gh` step. |
| HARD-01 | Containers had a writable root filesystem and no default requests. | Read-only root filesystem with a `/tmp` `emptyDir` on every container (Helm and `k8s/`), default requests, `seccompProfile` and no service account token on the UI. A run, backup, verify, retention preview and restore pass under it on `kind`. Limits stay unset on purpose (sizing depends on fleet size). |
| SEC-10 | The rate-limit counters were readable without sign-in. | They need a viewer session or token. `openapi.json` and `meta/routes` stay public for the Swagger page. |
| SEC-11 | bcrypt used the library default cost (10); a plain-HTTP host install gave no hint that its cookie is not `Secure`. | Cost 12, with older hashes upgraded at the next sign-in (not a password change; sign-in timing stays equal for unknown names). System Health warns when sessions run over plain HTTP. |
| FRESH-01 | `/api/v1/freshness` returned every schedule and the global overdue flag to cluster-scoped users. | Environment schedules and the flag follow the caller's cluster groups; the main schedule stays visible. |
| OPS-03 | No secret scanning in CI. | gitleaks runs in CI; a full-history scan is clean. |

### Correctness and reliability

| ID | Finding | Fix |
| --- | --- | --- |
| K8S-02 | The Helm API pod crash-looped on first start (also in the published 2.3.0 image). | The chart passes the full flag set; a configured Secret or `NCC_USERS_DB` suppresses the `--users-db` default. |
| ENV-03 | The Helm runner CronJob failed with `mkdir logs: permission denied`. | It runs from `/data` with explicit paths and `--skip-preflight-check`. |
| WIN-04 | The Windows installer wrote an invalid `config.yaml` (backslash paths in double quotes). | Paths are single-quoted. |
| CI-02 | Nothing installed the Helm chart in CI. | `helm-smoke` job: `kubeconform` on the chart and `k8s/`, then `scripts/helm-smoke-test.sh` installs the chart on `kind` and runs it against a simulated cluster. |
| REL-06 | Both installers failed with their default settings: they downloaded from `.../download/2.4.0/`, but release tags start with `v`, so GitHub answered 404. | They try `v<version>`, then the bare version for mirrors. |
| ENV-11 | Once, both API replicas landed on one node during a rollout. | `matchLabelKeys: [pod-template-hash]` spreads each revision on its own. Six rollouts on two workers stayed spread; the original fault did not reproduce without it either. |
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
| ENV-12 | Settings, Run now failed the secrets preflight (`NCC_PREFLIGHT_VALIDATE_SECRETS`) on Helm installs while the CronJob succeeded. | The chart creates the ConfigMap with `skip-preflight-check: true` and `secrets.provider: env`. |
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
| TEST-02 | `SessionIdleGuard.test.tsx` failed once in a loaded run. | It already used fake timers; the likely cause is the one-second default of `waitFor`. Raised to five seconds for all tests. |
