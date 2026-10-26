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

### SEC-07 — Restore can replace stack binaries · Medium · P2 · verified (code) · open

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
