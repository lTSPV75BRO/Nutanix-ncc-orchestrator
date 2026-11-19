# Security assessment

Builds on [`docs/security/security-assessment-2026-10.md`](../security/security-assessment-2026-10.md)
(backup encryption, user-store encryption, brute-force protection, path
disclosure, CSP/SRI, sessions/CSRF/CORS, rate limits, supply-chain incidents).
Those results were spot-checked and still hold; this document covers what
was found or verified in this audit. OWASP ASVS/Top 10 categories are noted
per finding.

## Threat model in brief

* **Assets:** Prism credentials (config/secrets file), the admin static token,
  the user store (bcrypt hashes, PAT hashes, SAML SP key, LDAP bind password),
  backups, and the ability to run the runner binary or install schedules (host
  code execution as the service account).
* **Actors:** unauthenticated network clients; viewer/operator/admin users;
  cluster-group-restricted users; automation holding PATs; MCP clients (AI
  assistants) on a developer machine; anyone who can edit files on the host.
* **Trust boundaries:** browser ↔ UI server ↔ API server ↔ runner child
  process ↔ Prism; API replicas ↔ shared state; MCP client ↔ MCP server.

## Findings

### SEC-01 — Deleting a local account did not end its sessions · High · P1 · verified · **fixed** (ASVS V3.3, A07)

* **Evidence:** `principalFromIdentity` / `principalFromHMACClaims` look the
  subject up in the user store to check the token generation and re-read the
  role. When the subject is **not found** they assumed an SSO/LDAP user and
  accepted the session with the role embedded in the token. A deleted local
  account's JWT or HMAC session therefore kept working, with its original
  role (possibly admin), until expiry: 6 h by default, up to 24 h via
  Settings, longer via `--session-ttl`. Re-creating the same username reset
  the token generation to 0, which re-validated the old account's tokens.
* **Reproduction:** `TestDeletedAccountSessionsAreRejected` fails on the
  baseline code (`JWT after delete: want 401, got 200`).
* **Fix:** deleting an account records a tombstone (`deleted_accounts`:
  name, last generation + 1, deletion time) in the persisted user document, so
  every replica sees it. Sessions for a missing subject issued at or before the
  deletion are rejected; sessions without `iat` fail closed; sessions issued
  after the deletion (an LDAP/SSO user who shares the name) are unaffected.
  Re-creating the name continues the generation. `upsertUser` never moves a
  generation backwards. Tombstones are pruned after 30 days and capped at
  1,000. PATs were already handled (`OwnerLocal` re-resolution).
* **Tests:** `TestDeletedAccountSessionsAreRejected`,
  `TestRecreatedAccountDoesNotAcceptOldSessions`,
  `TestDeletedAccountTombstonePersistsAndPrunes`.
* **Residual:** sessions issued by a replica whose clock runs ahead of the
  deleting replica's clock by more than the time between the two events could
  still pass; with the 30 s skew already tolerated for `iat` this is a narrow
  window. A deletion lost to DATA-01 (multi-replica lost update) would also
  lose its tombstone.

### SEC-02 — Operators could direct runner output outside the install root · Medium · P1 · verified · **fixed** (A01 path traversal)

`sanitizeExtraArgs` accepts `/` and `..` in values (`reSafeArg`), and
path-valued flags were passed through unconfined; `--prom-dir ../../x` made the
runner create directories and write `.prom` files anywhere the service account
can write. Path-valued extra args now go through `normalizeAndConfinePath`
(the same repo-root + symlink confinement as config paths). Live check: a
`--prom-dir ../../outside` trigger returns `400 path escapes repo root`.

### SEC-03 — Anonymous forced rotation of the admin password · High · P1 · verified (by design) · **fixed: off by default**

* **Evidence:** `POST /api/v1/auth/forgot-password` with `{"username":"admin"}`
  is unauthenticated and, by design, regenerates the built-in admin's
  password, forces a change, and invalidates every admin session. The only
  throttle is one reset per source IP per 60 s, and the source IP can be
  spoofed from private networks under the default `--trusted-proxy-cidrs
  private,loopback` (prior assessment A3).
* **Impact:** anyone who can reach the UI can keep logging the administrator
  out and make them fetch a new password from the host or Kubernetes Secret.
  The new password is never disclosed over the network, so this is an
  integrity/availability issue (A04 insecure design), not account takeover.
* **What changed (decided with the product owner):** the self-reset is now
  **off by default**. A forgot-password request for `admin` is queued like any
  other account's, gets the generic response, and changes nothing. The old
  behaviour is opt-in via `--allow-admin-self-reset` /
  `NCC_ALLOW_ADMIN_SELF_RESET=true`. Recovery uses `v2-reset-password --user
  admin` / `--reset-admin` on the host, which needs the same host access the
  network-triggered path already required to read the new password. The login
  page text and the feature/security docs were updated; the changelog carries
  an upgrade note.
* **Tests:** `TestForgotPasswordAdminSelfResetOffByDefault`; the e2e test
  `TestEndToEndAdminForgotPasswordSelfReset` now checks the default over the
  real handler stack (password still works, no file written, request queued)
  before opting in and running the original self-reset flow.

### SEC-04 — MCP `create_schedule` accepted an arbitrary command · Medium · P2 · verified · **fixed** (LLM tool abuse / A08)

The MCP tool's free-form `command` was installed as a recurring cron job or
Windows scheduled task: persistent code execution chosen by the model, which
text it reads (reports, tickets) can steer. The tool was annotated
non-destructive. It is now refused unless the MCP server runs with
`NCC_MCP_ALLOW_SCHEDULE_COMMAND=true`. Test:
`TestCreateScheduleRefusesCustomCommandByDefault`.

### SEC-05 — MCP passed Prism passwords on the command line · Medium · P2 · verified · **fixed** (A02 sensitive data exposure)

`run_ncc` and `discover_clusters` appended `--password <value>` (also setting
`NCC_PASSWORD`), so the password was readable by every local user through the
process list (`ps`, `/proc/<pid>/cmdline`, Task Manager). The password now
goes only through `NCC_PASSWORD`, which the runner reads via its `NCC_` env
binding. Test: `TestWithPasswordEnvReplacesAndOmits`.

### SEC-06 — Email header injection in runner notifications · Medium · P2 · verified · **fixed** (A03 injection)

The runner built `Subject:` (rendered from templates over run data such as
cluster and check names) and `From`/`To` with `fmt.Sprintf`; a CR/LF in that
data started new header lines (for example an injected `Bcc:`). The API
server's sender already encoded headers; the runner's did not. Header values
are now flattened and RFC 2047-encoded, and envelope addresses containing
CR/LF are rejected. Test: `TestSendEmailFlattensSubjectAndSkipsAuthWithoutUser`
(fake SMTP server asserts no injected header reaches the message).

### SEC-07 — Restore can replace stack binaries · Medium · P2 · verified (code) · **fixed** (v2.4.0)

**Fix (v2.4.0):** release checksums cover only the release archives, not the
individual files in a backup, so the restore checks against what this host
has itself run instead: `.ncc-trusted-stack.json` records the SHA-256 of each
installed stack set (on start, supervised upgrade and every `v2-backup`).
The stack is restored only when every file matches it or the installed file;
otherwise only data is restored. The ledger is never backed up or restored;
`v2-restore --trust-stack` is a command-line override the API never passes.
Tests: `trustedstack_test.go`.


A backup that contains `bin/` (stack snapshots) makes `v2-restore` write and
`chmod +x` new runner/API/UI binaries, which then run as the service account
after the automatic restart. Backups are authenticated only by the archive
key/passphrase, not signed by the release pipeline. An admin (or anyone holding
the backup key) can therefore achieve host code execution through a crafted
archive. Admin is highly privileged already, but host code execution is a
larger boundary than "manage NCC". Recommendation: on restore, accept stack
binaries only when their SHA-256 matches a signed release checksum
(`releasesign.go` already verifies release signatures); otherwise restore
data only and require a separate update.

### SEC-08 — UI server forwards the admin token through `HTTP(S)_PROXY` · Low-Medium · P2 · verified (code) · **fixed** (UI→API transport ignores proxy env)

`buildBackendTransport` uses `http.ProxyFromEnvironment`. In token-injection
mode every proxied request carries the admin token; if a pod or host sets
`HTTP_PROXY`/`HTTPS_PROXY` (common in corporate clusters) and the backend
address is not covered by `NO_PROXY`, the token transits the proxy, in clear
for an `http://` backend. Loopback is exempt automatically; Kubernetes service
names are not. Recommendation: `Proxy: nil` for the UI→API transport (the
backend is always internal), or document `NO_PROXY` for the service name.

### SEC-09 — Unknown role in a signed session maps to admin · Low · P3 · verified (code) · **fixed** (unknown/empty role rejected; TestSessionWithUnknownRoleIsRejected)

See COR-07. Fail-open default reachable only with the signing secret; change
to reject unknown roles (keep the legacy empty-role HMAC session mapping only
if still needed).

### Carried over from the prior assessment (still open)

* Default `--trusted-proxy-cidrs private,loopback` lets a client on a private
  network spoof one `X-Forwarded-For` hop (affects rate limits and the admin
  self-reset throttle). Narrow it per deployment.
* Encrypt the user store (`NCC_MASTER_KEY`, `--require-encrypted-user-store`)
  or confirm etcd encryption for Secrets.
* `/api/v1/audit` is viewer-readable; consider operator/admin only.

## Verified controls (spot checks)

| Area | Result |
| --- | --- |
| Injection (command) | Runner invoked with `exec` argv (no shell); extra args allowlisted, metacharacters rejected; schedule fields regex-validated |
| Path traversal | Config/related-file/backup paths confined (abs + `Rel` + symlink walk + `EvalSymlinks`); backup names `Base`-only; archive members validated in a first pass |
| XSS | React escaping; no `dangerouslySetInnerHTML`, `innerHTML`, `srcDoc` or iframes anywhere in `frontend/src`; strict CSP on UI (`script-src 'self'`) and API (`default-src 'none'`) |
| CSRF | Double-submit token on every cookie-authenticated mutation (HMAC and JWT cookies); SameSite Strict/Lax; the UI proxy enforces Origin |
| SSRF | Webhook/Slack/SMTP/LDAP/SAML targets are admin-configured; operators can only *test* configured channels; errors returned to operators are URL-redacted |
| AuthN | bcrypt; constant-time compares; generic login errors; lockout per account/source/account+source shared across replicas; PATs hashed, expiring by default |
| AuthZ | Route→minimum-role table; cluster-group scoping enforced on runs, alerts and dispositions; restricted callers cannot pin their own cluster list |
| Deserialization | JSON with size limits and `DisallowUnknownFields`; YAML parsed into typed structs |
| Containers | Non-root UID 10001, Alpine 3.24, `allowPrivilegeEscalation: false`, `runAsNonRoot`; NetworkPolicies (default-deny ingress, explicit egress) |
| CI | `contents: read` by default; release workflow scoped to tags with `id-token`/`attestations`; no `pull_request_target`; third-party actions SHA-pinned; publishing job disables dependency caches |

## Scans run in this audit

| Tool | Scope | Result |
| --- | --- | --- |
| govulncheck (latest) | Go call graph | 0 reachable; GO-2026-5932 (x/crypto/openpgp) not imported |
| osv-scanner v2 | go.mod + package-lock.json | 12 advisories, all npm dev tooling (see [`dependency-health.md`](dependency-health.md)) |
| gitleaks v8 | full git history (164 commits, ~102 MB) | 10 `generic-api-key` matches, all false positives: a test-only backup key placeholder (`backupschedule_test.go`) and prose mentioning "API token" in CHANGELOG/route descriptions/UI copy. No live credentials. |
| golangci-lint v2.15.0 (staticcheck all) | Windows and Linux targets | 0 issues |
| Not run here | Trivy, CodeQL, DAST | Run in CI (Trivy, CodeQL); no DAST target was available |

## Security automation recommendations

* Done: Windows test job; advisory full `npm audit`; Dependabot.
* Add gitleaks (or GitHub secret scanning with push protection) as a CI job;
  allowlist the test fixture above.
* Add a short DAST pass (OWASP ZAP baseline) against a docker-compose stack in
  CI on `main`.

## v2.4.0 re-audit (2026-11-19, HEAD ea76376c + uncommitted changes)

Phase 3. Evidence: `gosec` (CI flags: high severity, high confidence, G703 excluded) exit 0; `govulncheck`; `osv-scanner`; `gitleaks` over 264 commits (no leaks); reading of new endpoints, ledger and RBAC.

* **DEP-03 / GO-2026-6909** (`crewjam/saml` v0.6.3, nil-pointer panic in `validateAssertion`, no fixed version), reachable from the SAML ACS route. Bounded: `recoverPanic` returns a logged 500 (`logging_test.go`). Bump when upstream fixes it.
* **FRESH-01 (Low, verified by reading).** `GET /api/v1/freshness` filters `clusters` for cluster-scoped users but returns all `schedules` (environment names, intervals, last run) and the global `schedule_overdue` flag. Metadata only; not changed.
* **SEC-07 note (Low).** The ledger trusts each file against any recorded set, so a restore can combine files from different versions that each ran on this host. No new code is admitted; the combination never ran together. Not changed.
* Confirmed fine: `/api/v1/settings/retention/preview` is admin-only (prefix rule in `auth.go`); Kubernetes RBAC is two narrow Roles; restore never takes the ledger from an archive.
* **Troubleshooting without leaking:** new log lines carry usernames (already in the audit trail) and DNs, never passwords; `redactAttr` still masks secret-looking keys.
* Not tested: dynamic testing (ZAP or similar), authorization matrix by live requests, container image scan, CSRF/CORS probing.

### Live probe results (API built from this tree, 2026-11-19)

Run locally in hybrid auth mode against a scratch directory; one viewer and one operator account created through the API.

* **Unauthenticated sweep** of all 105 registered routes x GET/POST/PUT/DELETE (420 requests): 378 returned 401. Reachable without a session: health, readiness (503 when the runner binary is absent), `auth/me`, `auth/login`, `auth/logout`, `auth/forgot-password`, `openapi.json`, `meta/routes`, `metrics/rate-limit`, `tls/public`, and `/saml/*` (503 while unconfigured).
* **SEC-10 (Low, verified).** `/api/v1/meta/routes` (97 routes with minimum roles), `/api/v1/openapi.json` and `/api/v1/metrics/rate-limit` (limiter counters) are public by an explicit allow-list (`main.go:1051`) so the Swagger page works without sign-in. Information only; consider requiring a session for the rate-limit counters.
* **Role matrix** (viewer and operator, every route, all four methods): no privilege escalation found. A viewer got no success on any admin/operator mutation. Allowed exceptions are by design: viewer `PUT /runs/config-preference` and own personal access tokens; operator `POST /settings/notifications/test` and read of clusters, cluster groups and notification status.
* **CSRF:** cookie session without or with a wrong `X-CSRF-Token` -> 403.
* **CORS:** a foreign origin gets no allow-origin header and a 403 on preflight; the configured origin is allowed with credentials.
* **Lockout:** 5 wrong passwords -> 401, then 429 (also logged, see reliability doc). Oversized or malformed login bodies -> 400. Path traversal attempt -> 307 to the cleaned path, no file content.
* **K8S-01 (Medium, verified by render and Trivy/kube-linter, fixed).** The Helm API Deployment had no pod security context, so the `seed-config` init container (busybox) ran as the image default (root) with all capabilities, unlike the runner CronJob and UI. `helm/ncc-orchestrator/templates/api.yaml` now sets pod-level `runAsNonRoot`, uid/gid 1000, `fsGroup: 1000`, seccomp RuntimeDefault, and a restricted container context for the init container. Rendered and schema-validated; **not deployed to a cluster**, so confirm the API pod starts and can write `/data` on a real install.
* **HARD-01 (Low-Medium, recommendation, not changed).** No container sets `readOnlyRootFilesystem: true` (Trivy KSV-0014 on Helm and `k8s/`), and the Helm defaults leave CPU/memory requests and limits empty (`resources: {}`, documented). Enabling a read-only root needs writable `emptyDir` mounts for temp paths and must be tested on a cluster. Replica anti-affinity: the API/UI use `topologySpreadConstraints` (API) but kube-linter flags missing pod anti-affinity on both deployments.

### Correction, 2026-11-19

Earlier in this document DEP-03 (GO-2026-6909) is described as an open advisory with no fix. That was incomplete: the repository already records it as a reviewed false positive in `scripts/govulncheck-accepted.txt`, and the module source of `crewjam/saml` v0.6.3 contains the nil checks (`assertion.Subject == nil`, `assertion.Conditions == nil`). `govulncheck` still reports it because the vulnerability database has no fixed version. Status: accepted, review by 2027-01-31.

### Wider security pass, 2026-11-19

Tools run: `gosec` (high/high, as CI), `golangci-lint`, `govulncheck`, `osv-scanner`, `gitleaks`, `trivy` (filesystem and the built API and UI images), `kubeconform`, `kube-linter`, `zizmor` (GitHub Actions), a live authorization/CSRF/CORS/lockout probe, and a kind cluster deployment.

* **K8S-02 (High for Helm users, verified live, fixed).** The Helm chart's API pod crash-looped on first start. Cause: Kubernetes `args` replace the image's `CMD`, and the chart passed only a subset of flags. The container keeps `ncc-api-server` and `ncc-orchestrator` together in `/usr/local/bin`, which the stack-aware defaults mistake for an extracted v2 stack, so unset flags were rewritten to `/usr/local/...` (not writable by uid 1000) and `--users-db` was filled in, colliding with `--users-db-secret` ("mutually exclusive", fatal). Reproduced with the published v2.3.0 image as well as v2.4.0, so it predates this release. The raw `k8s/api-deployment.yaml` already passes the full flag set and is not affected. Fixes: (1) the chart now passes `--log-dir`, `--runner-log-path`, `--audit-log-path`, `--notifications-state-path`, `--token-file-path` and `--users-db ""` as `k8s/` does; (2) code: `--users-db-secret`, `NCC_USERS_DB_SECRET` or `NCC_USERS_DB` now suppress the stack-aware `users-db` default (previously `NCC_USERS_DB` was also silently overridden in a stack layout). Tests: `TestStackAwareKeepsUsersDBUnsetForConfiguredBackends`, `TestKubernetesAPIArgsPinWritablePaths` (also guards the `k8s/` manifest). Verified on a kind cluster: API pod Running as uid 1000, `/health` and `/health/ready` OK, user store created in a Kubernetes Secret through the narrowed RBAC.
* **WIN-01 (Medium, verified on this machine, fixed).** `deploy/windows/setup-ncc.ps1` installs to `C:\ProgramData\NCC\v2` and runs the stack as SYSTEM. The default `C:\ProgramData` ACL gives `BUILTIN\Users` create/write rights on subfolders (`(CI)(WD,AD,WEA,WA)`), so any local user could plant or replace binaries run by the SYSTEM task, or read the API token and user database. The installer now applies `icacls /inheritance:r` and grants only SYSTEM and Administrators (by SID). The command was tested on a scratch folder (before: Users writable; after: only Administrators and SYSTEM). The product itself still sets no Windows ACLs on its data files when installed any other way (WIN-02 below).
* **WIN-02 (Medium, recommendation).** `0600` file modes mean nothing on Windows; manual installs under ProgramData or other shared paths inherit permissive ACLs. Consider applying a restrictive ACL from `v2-start` on Windows for the token file, user database, backups and config.
* **WIN-03 (Low).** `setup-ncc.ps1` and the cloud-init script delete the existing install directory (`Remove-Item -Recurse` / `rm -rf`) before extracting, which removes user data and backups on a re-run. Both verify a SHA-256 from `checksums.txt` fetched from the same release URL, which detects corruption but not a tampered release; they do not verify the SLSA attestation that `release.yml` produces (`gh attestation verify`). Not changed.
* **LIN-01 (Medium, recommendation, not changed).** The systemd unit written by `v2-install-service` sets no `User=` (runs as root) and no sandboxing (`NoNewPrivileges`, `ProtectSystem`, `PrivateTmp`, ...). Hardening needs testing on a systemd host because the service restarts itself with `systemctl` and runs `chcon`; the tar extraction in cloud-init also lacks `--no-same-owner`.
* **CI-01 (Low-Medium, fixed).** `zizmor` flagged cache-poisoning risk in the publishing workflows: `docker-publish.yml` (setup-go and setup-node) and `release.yml` (setup-node) relied on comments saying "no cache" without setting it. Now `cache: false` / `package-manager-cache: false`. Remaining: `pages.yml` uses `workflow_run` (high by zizmor's rule); reviewed, it runs with `contents: read`, only for a successful Release run, and checks out the default branch.
* **SEC-11 (Low).** Default host installs serve plain HTTP, so the session cookie has no `Secure` flag unless `--cookie-secure` or TLS is enabled (the chart sets it). `ncc_csrf` has no `HttpOnly` by design (double-submit). bcrypt cost is the library default (10).
* **Images.** `trivy image` on the API and UI images built from this branch: 0 vulnerabilities in Alpine 3.24.4 and the Go binaries. The runner image could not be built locally (`Dockerfile.runner` expects a prebuilt `dist/` binary).
* **Read-only root filesystem (HARD-01 evidence).** The API image runs correctly with `--read-only --cap-drop ALL --security-opt no-new-privileges --tmpfs /tmp` and only `/data` writable, as uid 10001. Enabling `readOnlyRootFilesystem` in the chart is therefore feasible for the API; not changed because runs, backups and restores were not exercised under it.

### Windows installer, end to end on this machine, 2026-11-19

The real `deploy/windows/setup-ncc.ps1` was run elevated on this Windows 11 machine against a locally built `ncc-v2-stack-windows-amd64.zip` served from a local web server (so `NCC_RELEASE_BASE_URL` pointed at it), into `C:\ProgramData\NCC-audit\v2` on ports 28080 and 28081. Everything was removed afterwards (tasks, firewall rules, files, processes).

What passed: checksum verification; install; ACL on the install directory and on files the stack created (only Administrators and SYSTEM, inherited); scheduled tasks `NCC Orchestrator v2` and `NCC Log Maintenance` registered to run as SYSTEM; firewall rules limited to the Domain and Private profiles; API and UI running as SYSTEM and answering, readiness OK, UI redirects to HTTPS; `doctor` and `v2-check` OK; trusted-stack ledger recorded; encrypted backup, `--verify-only` (correct passphrase passes, wrong passphrase and a corrupted copy are rejected), restore while the stack runs with `--force --no-restart`; stopping the task and its processes and starting the task again brought the API back. A non-elevated shell, which carries the Users group, was denied access to the install directory (WIN-01 verified end to end).

Defects found by this run, all fixed:

* **WIN-04 (High for installs from this script, verified).** The installer wrote `output-dir: "C:\ProgramData\NCC\v2\outputfiles"` inside double quotes. In YAML `\P` and `\o` are invalid escapes, so `config.yaml` did not parse: `validate-config` failed with "found unknown escape character" and the backup verification reported "Config files FAIL". Any run or Settings save on a default Windows install would hit it. The paths are now single-quoted. After the fix the config parses (validate-config then only asks for the `NCC_PASSWORD` environment variable, as expected).
* **WIN-03 (verified, fixed).** Re-running the installer over a live install deleted the install directory item by item, stopped at the first locked executable (`ncc-api-server.exe`), and left a half-deleted install with the `backups` folder already gone. It now refuses when the directory is not empty unless `NCC_FORCE_CLEAN=1`, and a re-run was confirmed to leave the backup marker, user database and running stack untouched. Still open: the installers verify only a checksum from the same release URL, not the SLSA attestation.
* **COR-09 (Medium, verified, fixed).** `upsertYAMLScalar` replaced the whole matching line including its indentation, and its pattern could reach back over blank lines. Restoring a backup rewrote the nested `storage.run-history-dir` as an unindented top-level key. It is used by the post-restore path healing and by the CLI's config editing helpers (cluster mode, PC URL, clusters, username, insecure flag, secrets provider). It now keeps the indentation and only matches horizontal whitespace, and a `$` in a value is no longer read as a regexp group. Tests: `TestUpsertYAMLScalarKeepsIndentation`, `TestUpsertYAMLScalarTopLevelAndAppend`, `TestHealRestoredConfigPathsKeepsTheNestedSchema` (the first and last fail on the old code). Note: restore healing still rewrites absolute paths under the backup's original install directory to relative ones, even on the same host; that is the intended portability behaviour.

Not covered on Windows: a reboot (the task trigger is `AtStartup`; a stop/start of the task and processes was used instead), a Windows service account other than SYSTEM, upgrading from an older release, Sysprep/Unattend first boot.
