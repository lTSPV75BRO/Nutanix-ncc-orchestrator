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
- Open recommendations: systemd sandboxing, Windows file ACLs in the product,
  container hardening defaults, and attestation checks in the installers.

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
| Authentication | bcrypt, constant-time comparison, generic login errors, lockout per account, source and account+source shared across replicas, hashed and expiring tokens. A live run of five wrong passwords returned 401 and then 429. |
| Authorization | Route-to-minimum-role table plus cluster-group scoping. A live matrix of every route and method as viewer and as operator found no escalation. Intended exceptions: a viewer's own preference and tokens, an operator's notification test and read access to clusters. |
| Unauthenticated surface | Of 105 routes probed with four methods each, 378 of 420 requests returned 401. Reachable without a session: health, readiness, `auth/*` (login, logout, me, forgot-password), the OpenAPI document, `meta/routes`, `metrics/rate-limit`, `tls/public` and `/saml/*` (503 when unconfigured). |
| Deserialization | JSON with size limits and `DisallowUnknownFields`; YAML into typed structs. |
| Containers | Non-root uid, `allowPrivilegeEscalation: false`, capabilities dropped, NetworkPolicies (default-deny ingress). Images built from this branch scan clean. |
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

### Smaller items

- **DEP-03.** `GO-2026-6909` in `crewjam/saml` v0.6.3: reviewed false positive
  (fix in v0.5.2, present in v0.6.3). Handler panics are also recovered into a
  logged 500.
- **FRESH-01, SEC-10, SEC-11, SEC-07b.** Low-severity items listed in
  [`findings.md`](findings.md).

### Recommendations, not yet implemented

- **LIN-01.** The systemd unit runs as root with no sandboxing. Try
  `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=full`, `ProtectHome`,
  `ProtectKernel*`, `RestrictSUIDSGID` and `LockPersonality` as a drop-in, and
  a dedicated user, on a systemd host, then check restart, restore, apply
  update and `chcon`.
- **WIN-02.** Apply restrictive ACLs from `v2-start` on Windows.
- **WIN-03.** Guard the cloud-init script's `rm -rf`; verify the SLSA
  attestation (`gh attestation verify`) or a cosign signature in both
  installers.
- **HARD-01.** `readOnlyRootFilesystem` with an `emptyDir` for `/tmp`, default
  requests and limits, pod anti-affinity. The API image already runs read-only
  with only `/data` writable.

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
  command (now refused unless `NCC_MCP_ALLOW_SCHEDULE_COMMAND=true`), and the
  MCP server put the Prism password on the command line (now only in the
  environment).
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
| `trivy` | Filesystem and the built API and UI images | 0 vulnerabilities, 0 secrets. Misconfigurations: no read-only root filesystem and no default resource limits (HARD-01). |
| `kubeconform`, `kube-linter`, `helm lint` | Rendered Helm chart | 17 resources valid; remaining kube-linter findings are HARD-01 items and a CronJob TTL note. |
| `zizmor` | GitHub workflows | One reviewed finding (`workflow_run`). |

Not run: CodeQL and a DAST scan (OWASP ZAP) against a deployed stack; both are
on the roadmap. CodeQL runs in CI.
