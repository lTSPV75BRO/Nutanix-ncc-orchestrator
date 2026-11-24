# Security assessment

Two rounds of review, October 2026 (v2.2.1) and November 2026 (v2.4.0). This
page is the technical record: what was found, how it was verified and what is
left. The register with IDs and status is [`findings.md`](findings.md); the
checks and tools are listed in [`README.md`](README.md).

To report a vulnerability, see [`.github/SECURITY.md`](../../.github/SECURITY.md).

## Summary

- No Critical findings. No authentication bypass, injection or path traversal
  was found in the current code.
- One reachable advisory in the Go call graph (`GO-2026-6909`,
  `crewjam/saml`), a reviewed false positive. `npm audit` reports 0
  vulnerabilities; `gitleaks` over the full history finds no secrets; `gosec`
  (high severity and confidence, as CI runs it) and `golangci-lint` are clean.
- The built API and UI images scan clean (0 vulnerabilities in Alpine and the
  Go binaries).
- The serious defects were in deployment: a Helm chart whose API pod could not
  start, a Windows install directory writable by every local user, and a
  Windows installer that wrote an invalid config and could destroy backups.
  All are fixed.
- A crafted backup could make the server run its own program (SEC-19, High,
  found and fixed on 2026-11-24).
- Open: the first release from the new publishing workflow (REL-05); the Linux
  service is sandboxed but still root (LIN-02).

## Threat model in brief

- **Assets:** Prism credentials, the admin static token, the user store
  (bcrypt hashes, token hashes, SAML SP key, LDAP bind password), backups, and
  the ability to run the runner or install schedules (code execution as the
  service account).
- **Actors:** unauthenticated network clients; viewer, operator and admin
  users; cluster-group-restricted users; automation holding personal access
  tokens; AI assistants using the MCP server; anyone who can edit files on the
  host.
- **Trust boundaries:** browser, UI server, API server, runner child process,
  Prism; API replicas and their shared state; MCP client and MCP server.

## Verified controls

| Area | Result |
| --- | --- |
| Command injection | The runner is started with an argv list, never a shell. Extra arguments are allowlisted; schedule fields are regex-validated. |
| Path traversal | Config, related-file and backup paths are confined (absolute path, `Rel`, symlink walk). Backup names are base names only. Archive members are validated before extraction. A traversal attempt against the live API returned a redirect to the cleaned path and no file content. |
| XSS | React escaping; no `dangerouslySetInnerHTML`, `innerHTML`, `srcDoc` or iframes; strict CSP on the UI (`script-src 'self'`) and `default-src 'none'` on the API. |
| CSRF | Double-submit token on every cookie-authenticated mutation. A live request without the token, or with a wrong one, returned 403. |
| CORS | Explicit allowlist. A foreign origin gets no allow-origin header and a 403 on preflight; the configured origin is allowed with credentials. |
| SSRF | Webhook, Slack, SMTP, LDAP and SAML targets are admin-configured; operators can only test configured channels; errors are URL-redacted. |
| Authentication | bcrypt (cost 12), constant-time comparison, generic login errors, lockout per account, source and account+source shared across replicas, hashed and expiring tokens. A live run of five wrong passwords returned 401 and then 429. |
| Authorization | Route-to-minimum-role table plus cluster-group scoping. A live matrix of every route and method as viewer and as operator found no escalation. Intended exceptions: a viewer's own preference and tokens, an operator's notification test and read access to clusters. |
| Unauthenticated surface | Of 105 routes probed with four methods each, 378 of 420 requests returned 401. Reachable without a session: health, readiness, `auth/*` (login, logout, me, forgot-password), the OpenAPI document, `meta/routes`, `tls/public` and `/saml/*` (503 when unconfigured). `metrics/rate-limit` was reachable then and needs a session since 2026-11-23 (SEC-10). |
| Deserialization | JSON with size limits and `DisallowUnknownFields`; YAML into typed structs. |
| Containers | Non-root uid, read-only root filesystem, `allowPrivilegeEscalation: false`, capabilities dropped, NetworkPolicies (default-deny ingress). Images built from this branch scan clean. |
| CI | `contents: read` by default; release workflow scoped to tags with `id-token` and `attestations`; no `pull_request_target`; third-party actions pinned by SHA; publishing workflows disable dependency caches. One `zizmor` finding remains (`workflow_run` in `pages.yml`), reviewed: read-only token, only after a successful Release run, default-branch checkout. |

## Findings, November 2026 (v2.4.0)

### K8S-02: the Helm API pod crash-looped on first start (High, fixed)

Kubernetes `args` replace the image's `CMD`, and the chart passed only some of
the path flags. The container keeps `ncc-api-server` next to `ncc-orchestrator`
in `/usr/local/bin`, which the stack-aware defaults treat as an extracted v2
stack, so log, token and user-database paths pointed at `/usr/local` (not
writable by uid 1000), and `--users-db` was filled in and collided with
`--users-db-secret` ("mutually exclusive"). The published 2.3.0 image behaves
the same way. The plain `k8s/` manifests already passed every flag.

Fix: the chart passes the full flag set; `--users-db-secret`,
`NCC_USERS_DB_SECRET` and `NCC_USERS_DB` now suppress the stack-aware
`--users-db` default (previously `NCC_USERS_DB` was silently overridden too).
Tests: `TestStackAwareKeepsUsersDBUnsetForConfiguredBackends`,
`TestKubernetesAPIArgsPinWritablePaths`. Verified on a `kind` cluster: the pod
ran as uid 1000, passed readiness and created its Secret-backed user store
through the narrowed RBAC.

### ENV-03: the Helm runner CronJob could not write (High, fixed)

Found by the cluster test. The runner image's `WORKDIR` (`/workspace`) is not
writable by uid 1000, so a relative `logging.file` failed with `mkdir logs:
permission denied`. The CronJob now runs from `/data` with the explicit paths
and `--skip-preflight-check` that `k8s/runner-cronjob.yaml` uses
(`TestRunnerCronJobPinsWritablePaths`).

### WIN-01 to WIN-04: the Windows installer (High and Medium, fixed)

The real `setup-ncc.ps1` was run elevated against a locally built stack archive
(`NCC_RELEASE_BASE_URL` pointed at a local server), into a separate directory
and ports, and removed afterwards.

- **WIN-01.** The stack is installed under `C:\ProgramData` and runs as SYSTEM.
  The default `ProgramData` ACL gives `BUILTIN\Users` create and write rights
  on subfolders, so any local user could replace binaries or read the token and
  user database. The installer now applies `icacls /inheritance:r` and grants
  only SYSTEM and Administrators (by SID). Verified: a non-elevated shell was
  denied access, and files the stack created inherit the restricted ACL.
- **WIN-04.** The generated `config.yaml` held `"C:\ProgramData\..."` in double
  quotes; in YAML `\P` and `\o` are invalid escapes, so the file did not parse
  and `validate-config` failed. Paths are now single-quoted.
- **WIN-03.** Re-running the installer over a live install deleted the install
  directory item by item, failed on the first locked executable and left a
  half-removed install with the `backups` folder already gone. It now refuses
  unless `NCC_FORCE_CLEAN=1`; a re-run was confirmed to leave backups, the user
  database and the running stack untouched.
- **COR-09.** `upsertYAMLScalar` replaced a whole line including its
  indentation, so restoring a backup moved the nested `storage.run-history-dir`
  to the top level of the config. It is also used by the CLI's config helpers.
  Fixed, with tests that fail on the old code.

What passed on Windows: checksum verification, scheduled tasks running as
SYSTEM, firewall rules limited to the Domain and Private profiles, health and
readiness, `doctor`, an encrypted backup, `--verify-only` (a wrong passphrase
and a corrupted copy are rejected), restore while the stack runs, and recovery
after the task and its processes were stopped and started.

### COR-08: LDAP outages counted toward the lockout (Medium, fixed)

A failed bind as the user was always treated as a wrong password, so a dropped
or busy directory during the final check fed the account lockout and nothing
was logged. Only a directory that refuses the credentials counts now. Tested
against an in-process LDAP server speaking the real wire protocol (test-only
dependency `jimlambrt/gldap`): wrong password, busy directory, refused service
account and unknown user each behave as intended.

### ENV-01 and ENV-02: account management on a second replica (Medium, fixed)

On another API replica the user list, `PUT` and `POST` used the in-memory copy,
so a user another replica had just created was missing (404 on `PUT`), and a
store conflict on create came back as HTTP 500. These requests now refresh the
shared store first, retry a conflicting create once and answer 409.

### CI-01: cache poisoning in the publishing workflows (Low-Medium, fixed)

`docker-publish.yml` and `release.yml` said "no cache" in comments but relied on
action defaults that enable one. Caches are now disabled explicitly.

### SEC-12: MCP tools ran without a credential (High, fixed in 2.4.0)

**Affected: `ncc-mcp-server` in every release from 0.1.13 to 2.3.0.** Those
builds ran the orchestrator engine as a subprocess and did no authentication of
their own: whoever (or whatever) could start the process, or prompt the model
driving it, could call the engine with the OS account's privileges. The tool list
included `orchestrator_action` (`uninstall`, `reset-password`,
`install-update-watcher`, `quickstart`), upgrade apply, backup restore, schedules
and reads of any config path. The only gate was `confirm: "APPLY"`, which the
model supplies. The exposure is local (stdio, no listening port), which is why it
is High and not Critical, but a prompt-injected model is enough to reach it.

**Why the earlier reviews missed it.** The October and November rounds reviewed
the MCP tools one by one and fixed what each did (SEC-04: arbitrary schedule
command, SEC-05: password on the command line). They never asked the question
above the tools: what authenticates the caller? The live probes covered the API,
not the MCP process, and the dependency and release scans cannot see a design
flaw. It was found and fixed by the 2.4.0 rewrite, not by the review. The review
method now includes an explicit trust-boundary check for every entry point
(listener, CLI, subprocess, token), recorded in this page.

**Older builds cannot be fixed from the server.** A 2.3.0 or earlier MCP server
never contacts the API, so the API cannot refuse it and the blocked-versions list
cannot see it. What stops it: the 2.4.0 engine refuses to run when its parent
process is named `ncc-mcp-server` and has no valid proof and token (SEC-16
fixed that check on Windows). That is a name check, so a renamed old binary
bypasses it. Remove old `ncc-mcp-server` binaries and replace them with 2.4.0,
or stop launching them from your MCP host configuration.

`ncc-mcp-server` executed orchestrator tools and served resources as soon as
the host started the process. It now talks only to `ncc-api-server` over HTTPS
(or loopback HTTP): PAT (`NCC_MCP_TOKEN`), HMAC proof (`ncc-mcp-proof-v1`),
and API RBAC. A 2.3.x binary cannot mint the proof by changing its version
string. The API fails closed on a missing or forged proof; ordinary API
clients are unchanged. Each MCP-originated request is audited as `mcp.call`
with `channel=mcp`, PAT id/name when a PAT was used, and error/remediation on
failures.

Review of that rewrite (2026-11-20) found what it left behind, all fixed:

- **SEC-13 (Medium): the MCP server handed the model the raw configuration.**
  `get_sample_config` (and the `config-schema` action) returned the whole
  `GET /api/v1/settings/config` payload, which carries the file as written in
  `content` next to a masked `content_redacted`. Any credential written inline
  reached the model, and from there anything it talks to. Tool results are now
  scrubbed in one place before they are returned: the redacted copy replaces the
  raw one, and values under credential-like key names (`password`, `token`,
  `secret`, `api_key`, `webhook`, `hmac`, ...) and URLs with embedded credentials
  or webhook tokens are masked, whatever endpoint they came from.
- **SEC-14 (Medium): the redactor behind that copy missed most credentials.**
  `redactSensitiveText` masked only lines containing `password:`, `token:` or
  `secret:`, so a Slack webhook URL, an API key, an HMAC key, a passphrase or a
  bind password was shown in clear in the "redacted" view, in diagnostics and in
  the MCP tool. It now matches by key name, keeps `secret://` references,
  switches, numbers and durations, and masks URL userinfo and chat-webhook URLs.
- **SEC-15 (Low): `backup_v2_stack` did not back anything up.** It called the
  download endpoint (`GET /settings/backup`), streamed the archive to the MCP
  process, discarded it and answered `{"ok":true}`; under the default encryption
  policy it simply failed with 412. It now creates a server-side snapshot
  (`POST /settings/backups`, the server's backup key, no passphrase over MCP) and
  returns its name. `restore_v2_backup` accepts only a backup name
  (`manual-<time>.tar.gz.enc`), never a path.
- **What the controls are, and are not.** The HMAC proof is keyed by the PAT the
  client already sends, so anyone holding the PAT can compute it and a client that
  does not call itself `ncc-mcp-server` is not checked at all: it is a
  compatibility check, not authentication. `confirm: "APPLY"` is a value the
  model supplies, so it stops accidents, not a model that was told to apply a
  change. What bounds an MCP client is the PAT's role, the default-off
  `NCC_MCP_ALLOW_MUTATIONS`, the host's own approval prompt for destructive
  tools, and the audit log. Use a least-privilege PAT.

### Blocked dashboard versions

The API refuses dashboards on a blocked-versions list (`NCC_API_UI_VERSION_BLOCKED`)
and the dashboard shows an error screen with the code. The shipped list now comes
from scanning every tagged release ([`release-scan.md`](release-scan.md)); an
administrator can add entries under System, Updates & backups, Blocked versions.
Two things to know: builds before 2.4.0 cannot declare a version, so they are
refused by the "block unversioned" policy (on by default) and not by their
list entry; and the check is made on headers the client sends, so it stops an
honest outdated dashboard, not a hostile client. The refusal text used to tell
operators to upgrade to 2.3.0, which is itself unversioned and refused; it now
points at 2.4.0.

### SEC-16: the engine's old-MCP check did nothing on Windows (Medium, fixed)

The 2.4.0 engine refuses to run for an `ncc-mcp-server` parent that has no valid
proof, but it found the parent with `ps`, which does not exist on Windows, so on
Windows an old MCP server was never recognised. It now reads the parent's image
name through the Windows API. Verified by running the engine from a parent named
`ncc-mcp-server.exe`: refused with `mcp client upgrade required`.

### SEC-17: a signed build installed an unsigned release (Medium, fixed)

The updater checked the Ed25519 signature on `checksums.txt` only when the
release carried one. A build with an embedded release key therefore accepted a
release whose `checksums.txt.sig` had been removed, and trusted a checksum list
that came from the same place as the archive. A signed build now refuses a
release with no signature (`verify --online` stays advisory unless
`--require-signature` is given). Builds with no embedded key (the default for
unsigned and development builds) are unchanged, so signing remains something the
release maintainer must switch on: set `NCC_RELEASE_PUBKEY` when building and ship
`checksums.txt.sig` with every release.

### SEC-18: any signed-in viewer could read the audit log (Medium, fixed)

`GET /api/v1/audit` had no role rule of its own, so it fell to the default for reads
(viewer). A viewer, or an MCP assistant holding a viewer token, could read every
user's actions with client addresses, token names and request details, although the
dashboard shows the page to operators and up. Found while documenting what each MCP
tool needs; the earlier route sweep checked for missing authentication, not for
roles that were too low. Reading the audit log, and the CSV export, now needs the
operator role. The same review of the route table found `GET /api/v1/logs`
answering viewers; its sources are filtered per role in the handler (admin-only
sources are hidden), so it was left as is.

### SEC-19: a crafted backup could run code (High, fixed)

SEC-07 checks the program files in a backup, but a restore also wrote other paths
that the server runs or trusts later. A backup holding `run/upgrade-rollback/` and a
`run/v2-upgrade-state.json` that says a rollback was interrupted made the supervisor
copy foreign binaries over `bin/` and start them. This was proven on Windows with a
harmless marker program. Scheduler scripts in `logs/`, a schedule `log_path` with a
line break, and start settings that were empty on the host were further ways in
(shown by tests and reading, not run on a Linux host). Restoring needs the admin
role or a shell on the server, so the risk was an admin tricked into restoring a
tampered file.

A restore now refuses those paths whoever made the archive, refuses links and special
members, never marks data executable and keeps the host's own network settings. The
supervisor checks its saved copy against its own record before using it. A restore
stays an admin-level action: it replaces accounts and the audit log. An update begun by a
2.3.x supervisor that is itself restarted midway and then has to roll back is refused;
reinstall the old version instead.

### Entry-point review (2026-11-20)

| Entry point | Who can reach it | Check | Result |
| --- | --- | --- | --- |
| API, 129 catalogued routes plus the 18 prefix routes not in the catalogue | Network | Requested with no credential, garbage token, forged cookie, dashboard and MCP header claims | Only the documented public routes answer; SAML answers 503 when disabled |
| UI server | Network | Route list | Proxy for `/api/` and `/saml/`, static files; no debug endpoints |
| MCP server 2.4.0 | Local, through a host | Source | No subprocess or file access; API only, with a PAT |
| Engine CLI (`uninstall`, `v2-reset-password`, `v2-restore`, ...) | Local OS account | Source | Trusts the OS account by design, like any admin CLI; reachable by an MCP parent only with a valid proof and token (SEC-16) |
| Archive extraction (update, bootstrap, restore) | Release asset or backup file | Source | Every entry is checked to stay inside the destination; links are not created; per-file size limit. No limit on the total or on the entry count for updates. |
| Release updates | Release asset | Source | Checksum always; signature when the build is signed (SEC-17) |

### Authentication sweep of the API (2026-11-20)

All 129 routes in `/api/v1/meta/routes` were requested with no credentials, a
garbage token, a forged session cookie, a dashboard-claiming header set and an
MCP-claiming header set. Every route answered 401, 403, 404 or 405 except the
documented public ones: `health`, `health/ready`, `tls/public`, `openapi.json`,
`meta/routes`, `metrics/rate-limit`, `auth/me`, `auth/logout` and
`auth/forgot-password`. The metadata routes describe the API's shape and leave
nothing else readable. `metrics/rate-limit` has needed a session since (SEC-10).

### Smaller items

- **DEP-03.** `GO-2026-6909` in `crewjam/saml` v0.6.3: reviewed false positive
  (fix in v0.5.2, present in v0.6.3), no longer reported since 2026-11-24. Handler panics are also recovered into a
  logged 500.
- **SEC-07b, MCP-1, LIN-01, WIN-02.** Fixed on 2026-11-24; see
  [`findings.md`](findings.md).
- **FRESH-01, SEC-10, SEC-11, WIN-03, HARD-01.** Fixed on 2026-11-23; see
  [`findings.md`](findings.md).

### Recommendations, not yet implemented

- **LIN-02.** Run the API and UI as a dedicated user instead of root.
- **REL-05.** Publish 2.4.0 from the workflow and let its verification pass.

## Findings, October 2026 (v2.2.1)

- **SEC-01.** A deleted local account kept its session and role until expiry,
  and re-creating the name revived old tokens. Deleting an account now records
  a tombstone in the shared user document; sessions issued before it are
  rejected.
- **SEC-02.** Path-valued run arguments such as `--prom-dir ../../x` escaped
  the install root; they are now confined like config paths.
- **SEC-03.** `POST /api/v1/auth/forgot-password` could be used by anyone who
  could reach the UI to force the built-in admin's password to rotate. It is
  off by default; opt in with `--allow-admin-self-reset`. Recover with
  `v2-reset-password --user admin` on the host.
- **SEC-04, SEC-05.** The MCP `create_schedule` tool installed an arbitrary
  command (now never accepted; schedules use the API). MCP tools refuse
  password arguments; credentials stay on the API server.
- **SEC-06.** A CR/LF in a templated email subject could inject headers in the
  runner's mail. Headers are flattened and RFC 2047 encoded.
- **SEC-07.** A backup holding `bin/` could make a restore install new
  binaries. Restore now installs program files only when every file matches
  one this host has run (`.ncc-trusted-stack.json`, never backed up or
  restored); `v2-restore --trust-stack` is a command-line override the API
  never passes.
- **SEC-08, SEC-09.** The UI-to-API transport ignores `HTTP(S)_PROXY`; an
  unknown role in a signed session is rejected instead of mapping to admin.

Hardening delivered with those fixes:

- **Backups** are encrypted by default (`NCC_BACKUP_ENCRYPTION=required`;
  AES-256-GCM, scrypt for passphrases). The server refuses to write plaintext
  and checks the envelope after writing. See *Backup archive encryption* in
  [`docs/SECURITY_AND_TRUST.md`](../SECURITY_AND_TRUST.md).
- **User store**: `--require-encrypted-user-store` refuses to start without a
  master key; the admin health view reports `users_store_protection`.
- **Brute-force protection**: counters per account+source, per account and per
  source, progressive lockout, state shared across replicas, and
  `X-Forwarded-For` honoured only from `--trusted-proxy-cidrs`. Throttling
  responses are identical for existing and unknown users.
- **Path disclosure**: `/api/v1/health` is tiered (anonymous, signed in,
  admin); non-admins get stack-relative paths; error texts are redacted.
- **CSP and SRI**: only same-origin assets, Monaco and Swagger UI bundled
  locally, so SRI is not applicable; no CSP was weakened.
- **Rate limits** on password changes and resets, backup operations, update
  apply, support bundle and alert mutations; `ReadHeaderTimeout` 10 s and a
  64 KiB header cap.
- **Secrets in the repository**: no live credentials in the tree or history.

Carried over and still worth doing per deployment: narrow
`--trusted-proxy-cidrs` from the compatible default (`private,loopback`) to the
real proxy addresses; encrypt the user store or confirm etcd encryption for
Secrets; consider restricting `/api/v1/audit` to operators and admins.

## Scans

| Tool | Scope | Result |
| --- | --- | --- |
| `govulncheck` | Go call graph | One reachable advisory (`GO-2026-6909`, reviewed false positive). `GO-2026-5932` (`x/crypto/openpgp`) is flagged at module level but never called. |
| `osv-scanner` | `go.mod` and `frontend/package-lock.json` | Only `GO-2026-6909`. |
| `npm audit` | Frontend | 0 vulnerabilities. |
| `gitleaks` | Full git history | No leaks. |
| `gosec` | All Go code, high severity and confidence, G703 excluded as CI does | Clean. |
| `golangci-lint` v2.15 | All Go code | 0 issues. |
| `trivy` | Filesystem and the built API and UI images | 0 vulnerabilities, 0 secrets. Misconfigurations at the time: no read-only root filesystem (fixed since) and no default resource limits (left unset on purpose). |
| `kubeconform`, `kube-linter`, `helm lint` | Rendered Helm chart | 17 resources valid; remaining kube-linter findings are HARD-01 items and a CronJob TTL note. |
| `zizmor` | GitHub workflows | One reviewed finding (`workflow_run`). |

Not run: CodeQL and a DAST scan (OWASP ZAP) against a deployed stack; both are
on the roadmap. CodeQL runs in CI.
