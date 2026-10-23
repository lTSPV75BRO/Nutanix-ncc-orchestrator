# Security assessment — October 2026 (v2.2.1)

| | |
| --- | --- |
| Scope | Repository `lTSPV75BRO/Nutanix-ncc-orchestrator`, branch `v2.2.1` at `7aca6a6a` plus the v2.2.1 changes |
| Date | 2026-10-23 |
| Method | Source review, targeted regression tests, dependency/manifest inventory, review of public advisories |
| Not available in this assessment | Running production deployments, cluster/etcd configuration, CI/CD runners, developer workstations, `govulncheck`/`npm audit`/Trivy (the assessment environment could not reach the Go vulnerability DB, the npm registry, or container registries) |

Classifications used: **Confirmed vulnerable**, **Remediated and verified**,
**Not affected — evidence documented**, **Potentially affected — verification
required**, **Not verifiable from repository access**.

A clean result from any scanner is not, by itself, evidence that the
application is unaffected; each conclusion below lists the evidence it rests
on.

---

## Part A — Application findings

### A1. Backup archives created unencrypted — **Remediated and verified**

* **Evidence.** `handleBackupCreate`, `handleBackup` (download), the scheduled
  backup loop and the pre-update rollback point only encrypted when a
  passphrase or server key happened to be present; otherwise they wrote
  plaintext `.tar.gz` archives. The archives contain the API token, bcrypt
  password and PAT hashes, the SAML SP private key, the LDAP bind password,
  and `config.yaml` (Prism credentials). The audit field `encrypted` reflected
  the *request*, not the result.
* **Fix.** `NCC_BACKUP_ENCRYPTION` policy, default **required**
  (`cmd/ncc-api-server/backups.go`):
  * manual snapshots use the per-request passphrase or the server key
    (`NCC_BACKUP_KEY_FILE` / `NCC_BACKUP_KEY` / `NCC_BACKUP_PASSPHRASE`);
  * downloads require a passphrase (the server key would be useless to the
    recipient);
  * scheduled and pre-update backups require the server key;
  * otherwise the request fails with `412 NCC_BACKUP_ENCRYPTION_REQUIRED`
    (fail securely) and nothing is written;
  * after creation the server checks the AES-GCM envelope on disk and deletes
    the archive if it is not sealed; the audit log records the actual result
    and the key source (`request_passphrase` / `server_key`), never key
    material;
  * `encryptBackupFile` now replaces the plaintext atomically (temp + fsync +
    rename) so a crash cannot leave a truncated "encrypted" file.
* **Crypto.** Unchanged and standard: AES-256-GCM (Go `crypto/cipher`), random
  96-bit nonce per archive, header (magic, mode, salt) authenticated as AAD,
  scrypt (N=2^15, r=8, p=1) for passphrases. No custom primitives, no static
  IVs. Keys are never stored in the archive.
* **Kubernetes.** `backup-key` (optional) in `ncc-v2-secrets` → `NCC_BACKUP_KEY`
  (manifests and Helm chart).
* **Tests.** `TestBackupEncryptionPlan`, `TestBackupFileEncryptedDetectsEnvelope`,
  `TestBackupEndpointsRefusePlaintextByDefault`,
  `TestBackupEnvelopeTamperMissingKeyAndWrongKey` (tampered ciphertext, tampered
  header, missing key, wrong key, no-key encryption), existing
  `TestEncryptedBackupRoundTrip`, `TestValidateBackupScheduleInput`.
* **Residual risk / actions.**
  * **Existing plaintext backups are not protected by this change.** Inventory
    `<install>/backups/*.tar.gz` (non-`.enc`) and any copies taken off the
    host. Re-encrypt: restore-verify each needed archive
    (`POST /api/v1/settings/backups/verify`), create a fresh encrypted
    snapshot, copy it off-host, then delete the plaintext archives and their
    copies. Treat secrets in plaintext archives that left the host as exposed
    and rotate them (API token, LDAP bind password, SAML SP key, Prism
    password).
  * Key management: keep a copy of the backup key outside the cluster/host;
    encrypted backups cannot be restored without it. Rotation: set the new key,
    take a new snapshot, keep the old key until old archives age out
    (archives record their mode; the passphrase/key must match the one used
    to create them).
  * Behavior change: installs without a key must configure one, or explicitly
    set `NCC_BACKUP_ENCRYPTION=optional` (audited as `encrypted:false`).

### A2. User store encryption (`users_store_encrypted: false`) — **Potentially affected — verification required** (control added)

* **Evidence.** The store holds bcrypt password hashes, SHA-256 PAT hashes,
  and reversible secrets (SAML SP private key, LDAP bind password). File
  installs keep it in a `0600` JSON file; Kubernetes keeps it in the
  `ncc-v2-users` Secret. Application-level AES-256-GCM envelope encryption
  existed but was opt-in (`NCC_MASTER_KEY` / `--users-db-key-file`); the
  health value was accurate.
* **What was implemented.**
  * `--require-encrypted-user-store` / `NCC_REQUIRE_ENCRYPTED_USER_STORE=true`
    refuses to start without a master key.
  * Kubernetes manifests and Helm now wire an optional `master-key` secret key
    to `NCC_MASTER_KEY`.
  * Health no longer discloses storage details to anonymous callers (A4); the
    admin view adds `users_store_protection`: `aes256gcm_envelope`,
    `kubernetes_secret_etcd_encryption_unverified`, or `file_0600_plaintext`.
    The application deliberately does not claim etcd encryption it cannot
    observe.
  * Password hashing is unchanged (bcrypt); encryption is defense in depth, not
    a replacement.
* **Tests.** `TestUserStoreEncryptionPolicy`, existing `secretsenc_test.go`.
* **External actions.** Kubernetes administrators must confirm an
  `EncryptionConfiguration` (or KMS provider) covers Secrets
  (`k8s/encryption-config.example.yaml`). File installs should set a master key
  held outside the protected disk and enable
  `--require-encrypted-user-store`.

### A3. Brute force / credential stuffing (`locked: false` on failures) — **Remediated and verified**

* **Evidence.** A per-account lockout existed (5 failures/15 min). `locked:false`
  on individual failures is expected below the threshold. Gaps found:
  1. State was per process; with the Helm default of 2 API replicas behind a
     Service the effective threshold multiplied.
  2. No per-source limit across usernames (password spraying).
  3. Account-wide lockout after 5 failures from anyone = trivial lockout DoS
     against `admin`.
  4. `cleanClientIP` trusted the **left-most** `X-Forwarded-For` hop from any
     private address, so a client could pick its own rate-limit identity.
  5. The lockout message differed in wording from other failures ("this
     account is locked").
* **Fix** (`loginguard.go`, `hardening.go`, `auth.go`):
  * Counters on account+source (threshold), account (4× threshold,
    `--login-lockout-account-threshold`), and source (4×,
    `--login-lockout-source-threshold`); progressive lockout doubling up to 8×
    the base duration; decay after the window.
  * Shared state file under a cross-process lock
    (`--login-guard-state-file`, automatic in Kubernetes) so limits hold
    across replicas.
  * `X-Forwarded-For` is honored only from `--trusted-proxy-cidrs` /
    `NCC_TRUSTED_PROXY_CIDRS` (default `private,loopback`, kept for
    compatibility) and walked right-to-left, returning the first untrusted
    hop.
  * Generic throttling response identical for existing and non-existing
    usernames; audit records username, `locked`, source IP — never passwords.
* **Tests.** `TestLoginGuardCredentialStuffingLocksSource`,
  `TestLoginGuardLimitsLockoutDenialOfService`,
  `TestLoginGuardProgressiveLockoutAndDecay`,
  `TestLoginGuardSuccessResetsAccountNotSource`,
  `TestLoginGuardSharedStateAcrossReplicas`,
  `TestClientIPIgnoresSpoofedForwardedFor`, `TestLoginLockoutResponseIsGeneric`,
  existing lockout tests.
* **Residual risk.** With the compatibility default, a client on a private
  network that connects *directly* to the API can still forge one
  `X-Forwarded-For` hop. Set `--trusted-proxy-cidrs` to the actual proxy /
  ingress / UI-pod CIDRs (Helm `api.trustedProxyCIDRs`).

### A4. Internal path disclosure in API responses — **Remediated and verified**

* **Evidence.** `GET /api/v1/health` is unauthenticated and returned
  `config_path` (`/data/config/config.yaml`), `log_dir`, `token_file`
  (`/data/auth/.ncc-api-token`), `orchestrator_bin` (`/tools/ncc-orchestrator`),
  `users_store_encrypted`, `token_source`, Go version and git revision —
  exactly the reported values from the Kubernetes manifests. Viewer-level
  endpoints also returned absolute paths: `/runs` (`path`), `/runs/active`
  (`runner_log` = `/data/logs/ncc-runner.log`, `output_dir`, `config_path`),
  `/report/data` (`report_source_dir`, `ncc_logs[].path`), run summaries
  (`index_html`). The UI server's 401 diagnostic returned the absolute token
  file path and its proxy errors returned backend addresses.
* **Fix.** Tiered health payload (anonymous: liveness, version, login options;
  authenticated: build/runtime + relative config path; admin: absolute paths
  and storage protection). `pathForCaller` returns stack-relative paths to
  non-admins; run artifacts are scrubbed of absolute paths; API error texts
  pass through `redactPaths`; UI-server diagnostics show only the token file
  name and a generic proxy error (details logged server-side).
* **Tests.** `TestHealthDoesNotDiscloseInternalsToAnonymousCallers`,
  `TestRunsAPIReturnsExplicitStatusAndNoAbsolutePaths`,
  `TestRedactPathsHidesAbsolutePaths`, updated
  `TestHandleHealthIncludesRuntimeCapabilities`.
* **Residual.** Admin-only endpoints still show paths by design. Other admin
  error messages were not exhaustively rewritten.

### A5. Content-Security-Policy and Subresource Integrity — **Not affected — evidence documented**

* `frontend/index.html` loads only same-origin assets; Vite bundles all JS/CSS;
  Monaco is bundled locally (`loader.config({ monaco })`) instead of the
  jsDelivr default; Swagger UI assets are vendored under `/docs/assets/`.
* No eligible external script/style resources ⇒ SRI is not applicable.
* UI CSP: `default-src 'self'; script-src 'self'; object-src 'none';
  frame-ancestors 'none'; base-uri 'self'; form-action 'self'` with
  `style-src 'self' 'unsafe-inline'` (required by Ant Design's CSS-in-JS). API
  responses: `default-src 'none'`. No CSP was weakened.
* Not verified: production bundle behavior under the policy (the frontend
  build could not run in this environment — see the test report).

### A6. Session, cookie, CSRF, CORS, authorization — **reviewed; one fix**

* Session TTL default **6 h** confirmed (`--session-ttl`, admin-adjustable,
  max 24 h). `ncc_session` (HttpOnly, SameSite=Strict, Secure on HTTPS);
  `auth_token` JWT (HttpOnly, SameSite=Lax, Secure on HTTPS); `ncc_csrf`
  (readable by design, SameSite=Strict) with double-submit validation on every
  mutating cookie-authenticated request — confirmed in `authorizePrincipal`.
* CORS: explicit allowlist plus same-host origins; credentials only for
  allowed origins.
* Authorization: alert acknowledge/resolve/history now enforce operator/viewer
  roles **and cluster-group scoping** (previously the dispositions list
  showed marks for every cluster to restricted viewers — fixed). Backup,
  restore and update remain admin-only.
* PATs: hashed at rest, role-bounded, expiring by default (90 d, max 365 d;
  non-expiring only when explicitly requested), revocable, audited.
* Note: `/api/v1/audit` is viewer-readable (unchanged); consider restricting
  it to operators/admins.

### A7. Rate limiting and abuse protection — **Remediated and verified**

* Added per-client limits for change-password, forgot-password, backup
  create/download/verify/restore, update apply, support bundle, and all alert
  mutations (one bucket per client). Read endpoints used by integrations are
  not throttled.
* HTTP server: `ReadHeaderTimeout` 10 s (slowloris), `MaxHeaderBytes` 64 KiB;
  JSON bodies remain capped (1 MiB, alert notes 500 chars, bulk 200 items).
* Test: `TestRateLimitCoversAlertMutationsAsOneBucket`.

### A8. Supply-chain hygiene — secrets in repository

* Pattern scan of the working tree and full git history (GitHub, AWS, Slack,
  NCC PAT, Railway-style tokens, private keys) found only placeholders.
  This is not a full secret-scanner run (gitleaks/trufflehog were not
  available).

---

## Part B — Named October 2026 incidents

| # | Incident | Status |
| --- | --- | --- |
| B1 | PHP2Shell (CVE-2026-109618 / CVE-2026-109619) | **Not affected — evidence documented** |
| B2 | Node.js Brotli decoder RCE (CVE-2026-111497, "Node2Shell") | **Remediated and verified** (build toolchain pins); runtime not affected |
| B3 | GitLab Self-Managed CVSS 10.0 (CVE-2026-110264) | **Not affected — evidence documented** for the repository; **Not verifiable from repository access** for any organization GitLab |
| B4 | Railway secrets exposure | **Not affected — evidence documented** |
| B5 | Zeabur platform breach | **Not affected — evidence documented** |
| B6 | eMudhra intermediate-CA key compromise | **Potentially affected — verification required** (deployed trust stores) |
| B7 | LiteLLM (1.107.0) / OpenCode incidents | **Not affected — evidence documented** (application); developer machines **Not verifiable from repository access** |
| B8 | Double Counter breach | **Not affected — evidence documented** |

### B1. PHP2Shell — Not affected

* **Reference.** CVE-2026-109618 and CVE-2026-109619, PHP security release
  2026-10-20; fixed in 8.5.12, 8.4.27, 8.3.36, 8.2.35
  ([php.net announcement](https://www.php.net/archive/2026.php#2026-10-20-1),
  [GHSA-w8hw-w9fx-j79w](https://github.com/php/php-src/security/advisories/GHSA-w8hw-w9fx-j79w),
  [GHSA-6wmf-4rv5-p3rp](https://github.com/php/php-src/security/advisories/GHSA-6wmf-4rv5-p3rp),
  [Qualys](https://blog.qualys.com/vulnerabilities-threat-research/2026/10/20/php-announces-critical-security-release-for-two-vulnerabilities)).
* **Evidence.** No `.php` files, no `composer.json`/`composer.lock`; container
  images are `alpine:3.24` (+ Go binaries) and `node:26.x-alpine` (build stage
  only) — no PHP packages installed; CI workflows install Go and Node only.
  The only "php" strings are a MIME example inside the vendored Swagger UI
  bundle and base64 integrity hashes.
* **Residual.** None for this application. Other PHP services on the same
  hosts are out of scope.

### B2. Node.js CVE-2026-111497 — build toolchain pinned; runtime not affected

* **Reference.** Out-of-bounds write in the bundled Brotli decoder; affects
  22.x < 22.23.4, 24.x < 24.22.1, 26.x < 26.12.1 and EOL lines; released
  2026-10-21
  ([Node.js October 2026 security releases](https://nodejs.org/en/blog/vulnerability/october-2026-security-releases),
  [Rescana](https://www.rescana.com/post/nodejs-cve-2026-111497-node2shell-brotli-rce-october-2026),
  [Aikido](https://www.aikido.dev/blog/node2shell-nodejs-brotli-rce-cve-2026-111497)).
  Exploitation requires attacker-controlled Brotli data reaching a Node.js
  decompression path (e.g. `fetch()` of a Brotli response, body-parser
  `inflate`).
* **Inventory.**
  * Runtime: **no Node.js** in any runtime image or server. API, UI and
    runner are Go binaries; the UI server serves pre-built static files.
  * Build: `Dockerfile.ui` used the floating `node:26-alpine`; CI, Docker
    publish and release workflows used `node-version: "26"` (floating).
  * Developer machine checked during this assessment: Node **v22.23.4**
    (patched release).
* **Fix.** Pinned `node:26.12.1-alpine` in `Dockerfile.ui` and
  `node-version: "26.12.1"` in `ci.yml`, `docker-publish.yml`, `release.yml`.
* **Verification.** Manifest diff; the image tag itself could not be pulled
  from this environment. Rebuild the UI image and confirm
  `node --version` in the build stage prints `v26.12.1` or later.
* **Residual.** Low: the build fetches packages from the npm registry over
  TLS and does not decompress untrusted Brotli content. Rebuild artifacts
  produced with older Node builds if you want provenance on the patched
  toolchain.

### B3. GitLab Self-Managed CVSS 10.0 — no GitLab usage in the repository

* **Reference.** CVE-2026-110264 (unauthenticated RCE, CVSS 10.0), fixed in
  19.5.1, 19.4.3, 19.3.6, 19.2.9, 19.1.9, 19.0.10, 18.11.13
  ([GitLab patch release 19.5.1](https://docs.gitlab.com/releases/patches/patch-release-gitlab-19-5-1-released/),
  [summary](https://dev.to/alanwest/gitlab-cve-2026-110264-is-a-cvss-10-rce-how-to-patch-self-hosted-gitlab-tonight-24f1)).
  Reports also mention CVE-2026-85706 and CVE-2026-97229 (regex overflow,
  fixed 2026-10-12).
* **Evidence.** No `.gitlab-ci.yml` or GitLab references; the remote is
  GitHub; CI is GitHub Actions.
* **External action.** If your organization mirrors this repository to, or
  deploys from, a self-managed GitLab, its administrators must confirm the
  instance runs a fixed version and review logs for the published
  indicators. No testing of external GitLab instances was performed.

### B4. Railway — not used

* **Reference.** Railway disclosure 2026-10-21: attackers may have accessed
  (and for some users decrypted) environment variables; all users told to
  rotate keys
  ([Railway disclosure](https://blog.railway.com/p/security-disclosure-wednesday-october-21st),
  [TechCrunch](https://techcrunch.com/2026/10/22/developer-platform-railway-urges-all-users-to-revoke-secret-keys-after-cyberattack/)).
* **Evidence.** No `railway.json`/`railway.toml`/Nixpacks config, no Railway
  references in code, docs, workflows or deployment manifests; no committed
  `.env` files; no Railway-style tokens in history.
* **External action.** If any team member deployed a fork/demo of this stack
  on Railway, rotate every secret that was set there (Prism password,
  `NCC_API_TOKEN`, `NCC_JWT_SECRET`, backup/master keys).

### B5. Zeabur — not used

* **Reference.** Zeabur preliminary report (2026-10-19): ~3.2 M environment
  variable records and production secrets read 2026-10-14/15; platform
  shutting down 2026-12-31
  ([Zeabur report](https://zeabur.com/blogs/october-2026-security-incident),
  [BleepingComputer](https://www.bleepingcomputer.com/news/security/zeabur-shutting-down-after-second-security-breach-in-two-months/)).
* **Evidence.** No Zeabur configuration or references anywhere in the
  repository or deployment manifests.
* **External action.** Same as B4 for any out-of-band deployment.

### B6. eMudhra intermediate CA compromise — verify deployed trust stores

* **Reference.** Private keys of eight eMudhra intermediate certificates
  reported compromised 2026-10-17; Mozilla blocked them via OneCRL and removed
  the issuing root from NSS 3.131.1; other vendors shipped trust-store
  updates
  ([Mozilla](https://blog.mozilla.org/security/2026/10/20/protecting-firefox-users-following-the-compromise-of-emudhra-intermediate-certificates/),
  [Cloudflare](https://blog.cloudflare.com/emudhra-certificate-compromise),
  [eMudhra statement](https://emudhra.com/en/post/certificate-security-incident)).
* **Evidence.** No eMudhra references and no certificate/key files in the
  repository. The Go services verify outbound TLS (Prism Central, GitHub
  releases, LDAP, SMTP/webhooks) with the **system trust store**
  (`x509.SystemCertPool` in `internal/httpclient`), i.e. Alpine's
  `ca-certificates` inside the images, plus optional custom CA bundles.
* **Fix.** All Dockerfiles now run `apk add --no-cache --upgrade
  ca-certificates`, so a rebuild picks up the updated bundle even when the
  base image already contains an older one.
* **Not verifiable here.** Whether currently deployed images/hosts still trust
  the removed root, and whether any customer-supplied CA bundle
  (`ca-bundle` config, LDAP CA, UI TLS certificate) chains to an affected
  eMudhra intermediate.
* **Actions.** Rebuild and redeploy images; update host trust stores on
  VM/Windows installs; check that the UI certificate and any configured CA
  bundles do not chain to eMudhra (`openssl x509 -noout -issuer` on each).
  Never print private keys while doing so. Do not "fix" TLS errors by
  enabling `insecure-skip-verify`.

### B7. LiteLLM / OpenCode — not dependencies

* **References.**
  * LiteLLM: release pipeline compromise; malicious `litellm==1.107.0` on PyPI
    and `ghcr.io/berriai/litellm:v1.107.0` (2026-10-21)
    ([Socket](https://socket.dev/blog/litellm-compromise),
    [BleepingComputer](https://www.bleepingcomputer.com/news/security/litellm-release-pipeline-hacked-to-distribute-info-stealer-malware/)).
  * OpenCode: Console breach affecting users with linked AI providers
    (2026-10-18); no malicious package version has been published
    ([StepSecurity](https://www.stepsecurity.io/blog/opencode-security-breach),
    [TechCrunch](https://techcrunch.com/2026/10/19/hackers-hit-ai-coding-tool-opencode-running-up-charges-on-users-ai-accounts/)).
* **Evidence.** The project has no Python dependencies (no
  `requirements*.txt`, `pyproject.toml`, `Pipfile`); `go.mod`/`go.sum`
  (verified hash-for-hash during this assessment's build) and
  `frontend/package-lock.json` contain no `litellm` or `opencode` packages;
  no LiteLLM container images are referenced; no OpenCode configuration is
  tracked. No potentially malicious code was executed during the review.
* **Not verifiable here.** Developer workstations and CI runners. A
  `.continue/` (Continue.dev) directory exists untracked on the developer
  machine; it is unrelated to these incidents.
* **Actions.** On developer machines, check for OpenCode
  (`~/.local/share/opencode/auth.json`, `%USERPROFILE%\.local\share\opencode`)
  and for `litellm==1.107.0`; rotate any AI-provider keys linked to OpenCode
  Console or present where the malicious LiteLLM build ran.

### B8. Double Counter — not integrated

* **Reference.** Discord verification bot breach (2026-10-04): Discord IDs,
  usernames, IPs, ~1 M emails
  ([Double Counter report](https://doublecounter.gg/blog/security-incident-october-2026),
  [Cybersecurity News](https://cybersecuritynews.com/discord-users-data-exposed/)).
* **Evidence.** No Discord, Double Counter, or verification-service
  integration in code, configuration, notifications (email/webhook/Slack
  only) or telemetry.
* **Residual.** None for the application; individuals affected should watch
  for phishing.

---

## Part C — Tests and tools

See the v2.2.1 test report in the change description. Summary of what ran in
this assessment:

* `go build ./...`, `go vet ./...`, `gofmt -l`, `go test ./...`,
  `go test -race` on the changed packages, `GOOS=windows go vet` — executed.
* Frontend `vitest`, `tsc -b`, `vite build`, `npm audit` — **not executed**
  (npm registry unreachable from the assessment environment). Frontend tests
  were added and must run in CI.
* `govulncheck`, Trivy, CodeQL — **not executed** here; they run in the
  repository's CI workflows.

## Part D — Outstanding external actions (owner: platform/admin)

1. Configure `NCC_BACKUP_KEY_FILE`/`backup-key` (or accept
   `NCC_BACKUP_ENCRYPTION=optional` explicitly); re-encrypt or destroy old
   plaintext backups; rotate secrets in any plaintext backup that left the
   host.
2. Confirm etcd encryption for Secrets (Kubernetes) or configure a user-store
   master key and `--require-encrypted-user-store` (VM/Windows).
3. Narrow `--trusted-proxy-cidrs` to real proxy/ingress CIDRs.
4. Rebuild and redeploy all images (Node pin, updated CA bundle).
5. Verify no self-managed GitLab, Railway, or Zeabur deployments of this
   stack exist outside the repository; rotate secrets if they do.
6. Check developer machines for OpenCode/LiteLLM exposure as described in B7.
