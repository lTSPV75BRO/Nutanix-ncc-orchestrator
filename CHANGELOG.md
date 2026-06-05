# Changelog

All notable changes to the Nutanix NCC Orchestrator are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

**Release checklist (for maintainers):** Ensure [`VERSION`](VERSION) matches the intended tag; default `main.Version` in code is `2.1.0` when not set via ldflags. Run `go vet ./...`, `go test -race ./...`, and `go build ./...` (and `go build ./cmd/ncc-mcp-server`). Confirm `k8s/` and `helm/` image tags match `VERSION`. Tag `v2.1.0` and create a GitHub release using the matching `RELEASE_NOTES_v*.md`; attach `ncc-orchestrator-*` standalone binaries, `ncc-v2-stack-*` archives, and `checksums.txt` only — **do not** attach standalone `ncc-api-server-*` / `ncc-ui-server-*` binaries (the v1.x self-updater would silently mis-select them; see [2.0.0] known-issue note below and the v2.0.1 selector fix).

---

## [2.1.0] - 2026-06-04

Major authentication release on top of a maintenance/hardening base. Adds full interactive **login and RBAC** (`admin` / `operator` / `viewer`) for the api-server — local password accounts, SAML SSO, and LDAP / Active Directory login, a first-run **admin bootstrap** (random password + forced change), runtime user & SSO management in the UI, role-bearing session cookies with CSRF, and a Kubernetes **Secret-backed user store that is encrypted at rest**. It also closes a checksum-verification gap in `v2-bootstrap`, adds notification observability and templating, hardens TLS / notifications / process supervision, exposes NCC run metrics over the api-server `/metrics` endpoint, adds OpenTelemetry tracing, refreshes dependencies, improves the Windows self-update experience, and carries out the first wave of the `goNCC.go` package extraction (six new `internal/` packages). No breaking changes; all v2.0.x invocations keep working. **Affiliation note:** independent open-source project; not affiliated with or endorsed by Nutanix, Inc.

### Added

- **Per-account login lockout (brute-force protection).** After `--login-lockout-threshold` failed logins (default 5) within `--login-lockout-window` (default 15m) an account is temporarily locked for `--login-lockout-duration` (default 15m), returning `429 NCC_API_ACCOUNT_LOCKED` with a `Retry-After` header — regardless of source IP, so it stops attackers who rotate IPs while grinding one username (the global per-IP limiter alone cannot). A successful login or any admin/self-service password reset clears the lockout. State is in-memory and garbage-collected so it can't grow unbounded under a username-spraying attack. Threshold `0` disables it. Pinned by `TestLoginGuardLockAndExpiry`, `TestHandleLoginLockoutEndToEnd`.
- **Session revocation ("sign out everywhere" + admin force-sign-out).** Local users get a **Sign out everywhere** item in the header menu (`POST /api/v1/auth/logout-all`) that bumps their token generation, instantly invalidating every session on every device; admins get a per-row **Sign out** action in Settings → Access (`PUT /api/v1/settings/users/<name>` with `{"revoke_sessions": true}`) to force-sign-out any user — useful for incident response without resetting their password. Pinned by `TestRevokeSessionsBumpsGeneration`.
- **Self-hosted Swagger UI (no external CDN).** The API docs page now serves its Swagger UI bundle/styles from the binary (vendored `swagger-ui-dist`, embedded and served from `/docs/assets/`) instead of `unpkg.com`. The docs Content-Security-Policy no longer trusts any external origin, removing a third-party runtime/supply-chain dependency. Pinned by `TestSwaggerAssetsSelfHosted`.
- **Audit log filtering + CSV export.** `GET /api/v1/audit` now also filters by `user` (exact, case-insensitive) and by date range (`since`/`until`, RFC3339 or `YYYY-MM-DD`), and supports `format=csv` to stream a downloadable, server-side-filtered CSV (stable columns + a JSON `details` column for extra fields). Settings → Access → Audit Log gains a user filter, a date-range picker, and a **CSV** export button alongside the existing action/failures filters and client-side search. Pinned by `TestAuditEntriesFiltering`, `TestHandleAuditCSVExport`.
- **Authentication, login, and full RBAC (admin / operator / viewer).** The api-server now enforces three ordered roles (`viewer < operator < admin`): viewers read non-settings `GET` endpoints, operators may also trigger/cancel/preflight runs, and `/api/v1/settings/*` plus token rotation remain admin-only. `routeMinRole` replaces the previous admin/viewer-only check. Pinned by `TestRouteMinRole`, `TestParseRole`.
- **Writable user database + first-run admin bootstrap, on by default.** `--users-db` (`$NCC_USERS_DB`) points at a server-managed `0600` JSON store of local accounts. **When no backend is configured, the server now defaults to `<repo-root>/.ncc-api-users.json` automatically** (a v2 stack resolves it to `<root>/.ncc-api-users.json`), so even a bare `ncc-api-server` run bootstraps login out of the box. On first launch with an empty store, the server **provisions an `admin` account with a random password**, prints it to the log, and writes it to a `0600` `.ncc-initial-admin-password` file (deleted once changed). An optional `--users-file` YAML is imported as a one-time seed when the store is empty. Pure token-only automation can opt out with `--disable-local-accounts` (`$NCC_DISABLE_LOCAL_ACCOUNTS=1`). Pinned by `TestBootstrapAdminAndPersistence`.
- **Configurable session lifetime (UI form).** Admins can set how long a signed-in session stays active from Settings → Access (1 minute–24h), persisted in the user database alongside the other auth state and applied to sessions minted afterward. `GET/PUT /api/v1/settings/session` exposes it (`ttl_min`/`ttl_sec`; `ttl_sec:0` reverts to the `--session-ttl` server default). `/api/v1/auth/me` now reports `session_ttl_sec`, `expires_at`, and `expires_in_sec`, and the UI uses these to keep the session fresh and return to the login screen promptly when it lapses. Pinned by `TestSessionPolicyPersistAndEffectiveTTL`.
- **`v2-backup` / `v2-restore` commands.** `v2-backup` captures the stateful parts of an install dir — `config.yaml` and its referenced files (`clusters-file`, `exclude-alert-titles-file`, `secrets-file`), the local user database (`.ncc-api-users.json`: accounts, bcrypt hashes, roles, runtime SAML/LDAP config, session policy), the API token (`.ncc-api-token`), the first-run admin password if still present, scheduler/notifications state **plus any other `.ncc-api-*` state file at the install-dir root (glob-swept so backups stay complete as features are added)**, the **portable `v2-start` settings** (`.ncc-v2-start.json`: CORS origins, listen addresses, advertise/backend URLs, auth mode, session TTL, rate limit, HTTP timeouts, self-heal — so a restore/`v2-restart` relaunches with the operator's flags instead of falling back to defaults; path-type flags are re-derived under the new install dir), the **JSONL audit log (`logs/ncc-audit.log`)**, and the **latest run's report artifacts** (the top-level files of `output-dir-filtered` — `run-summary.json`, `index.html`, and the SLO/drilldown/flaky/checks JSON — so a restored stack shows the most recent run on the dashboard immediately) — into a single `0600` tar.gz (regenerable binaries/frontend, raw NCC summaries, Prometheus textfiles, and the full run-history snapshots under `<output-dir-filtered>/runs/` are excluded to keep the archive to a single run's worth of artifacts). The `manifest.json` now records the exact **ncc-orchestrator version that created the backup** (plus stream, build date, and Go toolchain) alongside tool/created_at/install_dir; `v2-restore` reports it and **warns when the restoring binary is older than the backup's creator**. `v2-restore` extracts back into an install dir, confined to that dir (unsafe archive paths rejected) and refusing to overwrite a running stack or existing files unless `--force`. Restore is **OS- and version-agnostic**: a backup taken on Windows (drive letters / backslash paths) restores cleanly onto Linux/macOS — file-reference paths (`clusters-file`, `output-dir-*`, `log-file`, etc.) that pointed inside the backup's original install dir are automatically rebased to the target install dir and path separators normalized, with a warning for any absolute path that can't be re-homed (pinned by `TestHealRestoredConfigPaths`); restoring across orchestrator versions is allowed (only a warning when the backup is newer than the restoring binary). **Restart is now automatic**: when the stack is running, `v2-restore` stops and re-starts it for you (`v2-stop` then `v2-start --detach`, performed by the orchestrator binary itself) so the restored config/accounts/token load with no manual step; `--restart` forces a restart/start even when stopped and `--no-restart` suppresses it. A new **`v2-restart`** command exposes the same binary-driven stop+start. The **UI restore** now triggers this restart automatically via a detached orchestrator process (the api-server restores with `--no-restart`, then spawns `v2-restart` so it survives its own shutdown) and the page reconnects on its own once the restarted stack is healthy. The `manifest.json` also carries an **auth-provider summary** (`auth`) and both `v2-backup` and `v2-restore` print it explicitly — local-account count, and whether **SAML** (with its SP signing key) and **LDAP/AD** (with its bind password) are present/enabled — so the operator can confirm SSO/directory config and their secrets travelled with the archive (these live inside `.ncc-api-users.json` rather than separate files); restore re-reads the file that landed on disk and warns when a provider is enabled but its secret is missing. The UI exposes backup/restore from **Settings → Access → Backup & restore**: a **Download backup** button streams the archive from the admin-only `GET /api/v1/settings/backup`, and **Restore from backup** uploads an archive to `POST /api/v1/settings/restore` (multipart) which applies it with `--force` after a destructive-action confirmation, then prompts for a stack restart. Pinned by `TestCollectBackupEntriesIncludesAuditLogAndState`, `TestBackupRestoreRoundTripRecordsOrchestratorVersion`, `TestBackupManifestSurfacesSAMLAndLDAP`.
- **First-login restore.** The forced password-change screen shown to a fresh deployment's bootstrap admin now also offers a **"Restore from backup…"** button: uploading an existing deployment's archive recovers everything and the admin keeps their **original** password (the restored user database carries the old admin account with `must_change_password=false`), so no reset is required. Restore is the single endpoint allowed through the forced-change gate; it remains admin-only with CSRF enforced, and the stack restarts automatically while the UI reconnects on its own. Pinned by `TestRestoreReachableDuringForcedPasswordChange`.
- **Backups now also carry the latest run + start settings.** Beyond the existing config/accounts/state, `v2-backup` now captures the **latest run's report artifacts** (the top-level files of `output-dir-filtered` — `run-summary.json`, `index.html`, and the SLO/drilldown/flaky/checks JSON) so a restored stack shows the most recent run on the dashboard immediately, and the **portable `v2-start` settings** (`.ncc-v2-start.json`: CORS origins, listen addresses, advertise/backend URLs, auth mode, session TTL, rate limit, HTTP timeouts, self-heal). The start settings are persisted on every `v2-start` and replayed by `v2-restart`/the post-restore auto-restart, so a restore no longer silently drops the operator's CORS/listen/session flags. The large run-history snapshots, raw NCC summaries, and Prometheus textfiles stay excluded. Pinned by `TestV2StartStateRoundTrip`, `TestCollectBackupEntriesIncludesAuditLogAndState`.
- **Search box for local accounts.** Settings → Access → Local accounts gains a search field that filters the account table by username or role.
- **Themed 404 page.** Unknown routes now render a branded "page not found" screen (gradient hero, the requested path, and "Back to dashboard"/"Go back" actions) instead of silently redirecting to the dashboard.
- **Inactivity "stay logged in" dialog + session refresh.** After 60 minutes of no activity the UI shows a "Stay logged in?" dialog with a 60-second countdown; choosing to stay calls the new `POST /api/v1/auth/refresh` (re-issues the session cookie with a fresh expiry for the current principal), while ignoring it or the countdown lapsing signs the user out. Pinned by `TestSessionRefresh`.
- **Login form credential save + shorter password minimum.** The login inputs now carry `name`/`autocomplete` attributes so browser password managers reliably offer to save and autofill credentials, and the local-account password minimum is **8 characters** (login creation, admin reset, and self-service change).
- **Password recovery: offline reset command + self-service request queue.** `ncc-orchestrator v2-reset-password [--user <name>]` (wrapping the new `ncc-api-server --reset-password <name>` / `--reset-admin`) recovers a lost local password offline against either store backend (file or Kubernetes Secret): it writes a new random temporary password, forces a change at next login, invalidates that account's sessions (token-generation bump), recreates the built-in `admin` if it was wiped, and prints a restart reminder (a running server caches accounts in memory). For end users, the login page gains a **"Forgot password?"** link that posts to the public `POST /api/v1/auth/forgot-password` — always a generic 200 with no account enumeration — queuing a request admins resolve from **Settings → Access → Password reset requests** (`GET`/`DELETE /api/v1/settings/password-resets[/<name>]`); an admin password reset clears the matching request automatically. **Admin lockout self-recovery:** when the forgot-password username is the built-in `admin`, the server skips the queue and self-resets it the same way first-run setup does — it generates a fresh random password, forces a change at next login, invalidates existing admin sessions, and surfaces the new password through the server logs and the `.ncc-initial-admin-password` file (never over the network) so a locked-out operator can recover without another admin. A short per-IP cooldown (60s, `429 NCC_API_RATE_LIMITED`) blunts repeated force-rotation, and the login screen re-shows the password-retrieval hint immediately. The authenticated **Settings → Access → Reset password** dialog mirrors this for the `admin` row: instead of typing a temporary password it offers **Generate & reset**, which produces a random password (shown once, copyable, and written to the logs/bootstrap file). Pinned by `TestUserDBResetPassword`, `TestForgotPasswordAdminSelfReset`, and `TestEndToEndAdminForgotPasswordSelfReset`, `TestForgotPasswordQueueAndResolve`.
- **Kubernetes Secret store for the user database (encrypted at rest).** `--users-db-secret <name>` (`$NCC_USERS_DB_SECRET`) persists the user database — bcrypt hashes, roles, SAML SP private key, and the first-run admin password (`initial-admin-password` key) — inside a Kubernetes Secret instead of a PVC file, so it is encrypted at rest by etcd (KMS/secretbox/aescbc). A pluggable `userStoreBackend` abstraction backs both the file and Secret stores; the Secret path uses a small built-in in-cluster REST client (service-account token + CA) rather than `client-go`, keeping the static binary/image lean. Ships `k8s/rbac.yaml` (least-privilege: `create` namespace-scoped + `get`/`update`/`patch` restricted to the one Secret via `resourceNames`) and `k8s/encryption-config.example.yaml`. `k8s/api-deployment.yaml` now uses this by default. Pinned by `TestK8sSecretBackendRoundTrip`.
- **Forced password change.** Bootstrap and admin-reset accounts are flagged `must_change_password`; the server returns `403 NCC_API_PASSWORD_CHANGE_REQUIRED` for everything except `/api/v1/auth/{me,logout,change-password}` until the user sets a new password via `POST /api/v1/auth/change-password` (≥8 chars, current-password + CSRF verified). The UI shows a blocking change-password screen. Pinned by `TestForcedPasswordChangeFlow`.
- **Admin user management API + UI.** `GET/POST /api/v1/settings/users` and `PUT/DELETE /api/v1/settings/users/<name>` let admins list, create, role-assign, reset-password, and delete local accounts (last-admin lockout protection, `NCC_API_LAST_ADMIN`). Surfaced in a new Settings → **Access** tab. Pinned by `TestUserCRUDAndLastAdminProtection`.
- **Reserved built-in admin account.** The `admin` account's role is hardcoded to `admin`: it can never be demoted or deleted (`409 NCC_API_RESERVED_ADMIN`), regardless of how many other admins exist, and a hand-edited store/Secret that demotes it is coerced back to `admin` on load. The UI disables the role selector and delete button for that row. Pinned by `TestReservedAdminRoleImmutable`.
- **Local password accounts.** `POST /api/v1/auth/login` verifies the password and mints a role-bearing session; `POST /api/v1/auth/logout` clears it; `GET /api/v1/auth/me` reports the caller's identity, role, must-change state, and available login methods. `ncc-api-server --hash-password` generates bcrypt hashes for seed files. Pinned by `TestUserStoreVerify`, `TestHandleLoginAndSessionRole`, `TestHandleMeAnonymousAndAuthenticated`.
- **SAML SSO** via `github.com/crewjam/saml`. Two ways to configure: (1) startup flags `--saml-root-url`, `--saml-idp-metadata` (URL or file), `--saml-cert`, `--saml-key` (read-only at runtime); or (2) **runtime configuration in the Settings → Access UI** (`GET/PUT /api/v1/settings/sso`), persisted in the user database and hot-reloaded without a restart — the **SP signing keypair is generated server-side** (never uploaded through the browser) and the public SP metadata is served at `/saml/metadata`. IdP attribute values map to roles with the role attribute + role map (default role). Pinned by `TestParseRoleMap`, `TestSAMLRoleFromValues`, `TestSSOConfigPersistAndCertGeneration`.
- **LDAP / Active Directory login (end-to-end).** Users sign in on the normal username/password form with their AD/LDAP credentials. Authentication is **local-first, then AD fallback** — the built-in `admin` and break-glass local accounts keep working even when the directory is unreachable — using a **service-account bind + search + rebind** (default AD filter `(&(objectClass=user)(sAMAccountName=%s))`); an empty password is rejected before any bind so an anonymous bind can't masquerade as a login, and the username is escaped into the filter. AD groups map to local roles by group DN or CN (case-insensitive, highest match wins; e.g. `CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin`) with a default-role fallback. Configure via startup flags (`--ldap-url` [comma-separated for failover], `--ldap-base-dn`, `--ldap-bind-dn`, `--ldap-bind-password`, `--ldap-user-filter`, `--ldap-username-attribute`, `--ldap-group-attribute`, `--ldap-role-map`, `--ldap-default-role`, `--ldap-start-tls`, `--ldap-ca-file`, `--ldap-insecure-skip-verify`) or at runtime in **Settings → Access → LDAP / Active Directory** (`GET/PUT /api/v1/settings/ldap`), hot-reloaded without a restart. A **Test connection** button (`POST /api/v1/settings/ldap/test`) authenticates a sample user to validate connectivity and role mapping before saving. The bind password is write-only: stored only in the `0600` user-store file or the Kubernetes Secret (encrypted at rest) and never returned by the API. Pinned by `TestParseLDAPRoleMap`, `TestLDAPRoleFromGroups`, `TestLDAPEmptyPasswordRejected`, `TestHandleLoginLocalFirstThenLDAP`.
- **Role-bearing session cookie + CSRF.** Browser sessions use an httpOnly, Secure, `SameSite=Strict` `ncc_session` cookie; mutating cookie requests must echo a double-submit CSRF token (`X-CSRF-Token` header / readable `ncc_csrf` cookie). Static-token automation is exempt. `--cookie-insecure` is provided for local http dev. Pinned by `TestWithAuthCookieSessionRBACAndCSRF`, `TestStaticAdminTokenExemptFromCSRF`.
- **UI login screen, forced password-change gate, and role-aware controls.** The frontend bootstraps from `/api/v1/auth/me`, shows a login page (local form and/or "Sign in with SSO") when login is enabled, presents a blocking password-change screen for `must_change_password` accounts, hides the Settings nav/route from non-admins and the run-trigger button from viewers, and adds a user menu with role badge and sign-out. The Settings → Access tab manages local users and SAML/SSO. **External authentication (SAML and LDAP/AD) is presented under a single "External authentication" card with a provider dropdown** to switch which one you're editing; both providers remain independently configurable and can be enabled at the same time (the dropdown shows a live on/off badge for each), and switching does not disable or discard the other.
- **ui-server session forwarding.** `ncc-ui-server` auto-detects (via backend health) when interactive login is enabled and forwards each browser's session cookie/CSRF instead of injecting the shared admin token (avoiding privilege escalation). Override with `--login-mode {auto|on|off}`.
- **Audit entries are now attributed to the acting user + role.** `withAuth` stashes the resolved principal on the request context, so every audited action automatically records `user` (the account subject — a real username for local/SSO sessions, or the synthetic `static-admin-token` subject for token automation) and `role` alongside the client IP that was already captured. The admin account always reports role `admin`. The Settings → Audit table gains a **User** column, and the JSONL export carries the new fields.
- Sample [`example_users.yaml`](example_users.yaml) and an expanded RBAC/login/SSO section in [`docs/SECURITY_AND_TRUST.md`](docs/SECURITY_AND_TRUST.md).
- **Checksum verification for `v2-bootstrap` downloads.** `v2-bootstrap` now verifies every downloaded asset (the `ncc-v2-stack-*` archive, or the api/ui binaries + frontend archive in the legacy fallback) against the release `checksums.txt` before extracting/installing, matching the strictness `update` already enforced. Pinned by `TestVerifyDownloadedAsset`.
- **`--skip-checksum-verify` flag** on both `update` and `v2-bootstrap` as an explicit, clearly-warned escape hatch for air-gapped or internally-mirrored installs. Default is hard-fail on a missing checksum manifest or hash mismatch.
- **Windows self-update helper.** On Windows, `update` now writes an `apply-ncc-update.cmd` next to the binary that waits for the running process to exit, swaps in the downloaded `.new.exe`, and self-deletes — replacing the previous "copy the file yourself" instruction with a single command. Pinned by `TestWriteWindowsUpdateSwapHelper`. The helper is added to the `uninstall` cleanup set.
- **Notification delivery metrics.** Each run now records per-channel notification outcomes (email / webhook / slack) and, when `prom-enabled` is set, writes a run-level `notifications.prom` textfile exporting `nutanix_ncc_notification_attempts_total{channel=…}` and `nutanix_ncc_notification_failures_total{channel=…}`. Delivery failures were previously only logged; monitoring can now alert on them. A line is always emitted per channel (0 when unused) so alerting rules never break on a missing series. Pinned by `TestNotificationMetrics`, `TestWriteNotificationMetricsFile`, and `TestNotificationWrappers_SkipDisabled`.
- **Custom notification templates.** New optional config keys `email-subject-template`, `email-body-template`, and `webhook-template` accept Go `text/template` strings rendered against the run summary (`.Cluster`, `.FailCount`, `.WarnCount`, `.ErrCount`, `.InfoCount`, `.TotalChecks`, `.Overview`, `.StartedAt`, `.FinishedAt`, `.OutputFiles`). Empty = the built-in defaults. A broken template falls back to the default (logged, never drops the notification); an unknown field fails the template rather than emitting `<no value>`. Applied across the per-cluster, digest, and replay notification paths. Pinned by `TestRenderNotificationTemplate`, `TestApplyEmailTemplates`, and `TestSendWebhook_TemplateBody`.
- **NCC run metrics on the api-server `/metrics` endpoint.** The Prometheus rendering was split from the textfile writers (`promtext.RenderClusterChecks` / `RenderNotificationMetrics` / new `RenderRunSummaryMetrics`), and the api-server now reads the latest `run-summary.json` and serves per-cluster `ncc_cluster_up` / `ncc_cluster_checks_total{severity}` / `ncc_cluster_health_score` plus run-level `ncc_last_run_*` gauges. Prometheus can scrape the api-server directly instead of relying on a node_exporter textfile collector reading `<cluster>.prom`. Pinned by `TestRenderRunSummaryMetrics`.
- **Minimal, opt-in RBAC.** Setting `NCC_API_VIEWER_TOKEN` enables a read-only role: viewer-token holders may read non-settings `GET` endpoints but receive `403` on `/api/v1/settings/*` (which can expose secrets) and on any state-changing request; the full `NCC_API_TOKEN` / a session remains admin. When unset, behavior is unchanged. `/api/v1/health` reports `rbac_enabled`. Pinned by `TestWithAuthRBACViewer`.
- **Opt-in OpenTelemetry tracing.** New `internal/trace` package emits a span per cluster run when an OTLP endpoint is configured (`OTEL_EXPORTER_OTLP_ENDPOINT` or `NCC_OTEL_ENABLED=1`); otherwise the global no-op tracer makes it free. Errors are recorded on the span.
- **Mock-Prism integration tests.** `TestIntegration_DiscoverClustersV4_MockPC`, `TestIntegration_DiscoverClustersV3_MockPC`, and `TestIntegration_TaskPoll_MockPrism` exercise the v4/v3 discovery and task-poll HTTP round-trips against an in-process Prism Central.
- **Fuzz tests** for the NCC summary parser (`FuzzParseSummary`) and the HTTP-dump password redactor (`FuzzRedactJSONPasswordValue`). Their seed corpora are checked in.
- **CI quality gates.** New `.github/workflows/ci.yml` runs `go build` / `go vet` / `gofmt` / `go test -race`, `golangci-lint`, `govulncheck`, and a Trivy filesystem scan on every push to `main` and every PR.

### Security

- **Custom-CA and certificate-pinning options.** New `ca-bundle` (PEM of additional trusted CAs, verified against system roots + the bundle) and `pin-sha256` (comma-separated allowed server-cert SHA-256 fingerprints) provide safer alternatives to blanket `insecure-skip-verify`; pinning rejects a MITM cert that full-insecure mode would accept. Pinned by `TestNormalizePin` and `TestPinVerifier`.
- **Decoupled SMTP TLS verification.** New `smtp-insecure-skip-verify` controls SMTP STARTTLS verification independently of the Prism `insecure-skip-verify` flag, so a self-signed Prism cert no longer forces unverified mail delivery.
- **Optional webhook HMAC signing.** When `webhook-secret` is set, the webhook body is signed with HMAC-SHA256 and sent as `X-NCC-Signature: sha256=<hex>` so receivers can verify provenance. Pinned by `TestSignWebhookBody`.
- **Notification dead-lettering.** `notification-deadletter-dir` persists email/webhook/Slack payloads that fail to deliver after retries (with channel, cluster, and error) so a transient outage no longer silently loses the alert. Pinned by `TestWriteDeadLetter`.
- **Self-heal supervisor shell-injection hardening.** `startSelfHealSupervisor` now passes the service name through a shell-quoted `SERVICE_NAME` variable instead of concatenating it into the generated script's `echo` lines, closing a quoting/`$(...)` break-out vector.
- **Fixed a panic in the HTTP-dump password redactor.** `FuzzRedactJSONPasswordValue` found an out-of-range slice when a quoted value ended with a trailing backslash; the index is now clamped. This path runs whenever `log-http` is enabled.
- **Session invalidation on password change/reset.** Each local account carries a `token_gen` counter that is stamped into every session token it mints and bumped whenever the password is changed or reset. A changed/reset password therefore immediately invalidates all previously issued session cookies/bearer tokens (every other device is signed out); the acting user's change-password response re-issues a fresh cookie so their current session continues. Pinned by `TestSessionInvalidationOnPasswordChange` and `TestForcedPasswordChangeFlow`.
- **api-server Content-Security-Policy + response-header hardening.** Direct api-server responses now carry a `Content-Security-Policy` (`default-src 'none'` for API/JSON, a scoped policy for the two HTML help pages) plus `Cross-Origin-Opener-Policy: same-origin`, and the CORS `Access-Control-Allow-Headers` now includes `X-CSRF-Token` so the SPA's double-submit token works under cross-origin CORS. Pinned by `TestSecurityResponseHeaders`.

### Fixed

- **Login/SSO errors no longer masked by the UI-server token diagnostic.** `ncc-ui-server` previously rewrote *every* backend `401` into the "Backend rejected the UI server's API token…" message. It now only emits that diagnostic when the UI server itself supplied the rejected credential (token/minted-session modes) and never for `/api/v1/auth/*` or `/saml/*`, so a wrong password surfaces the backend's clear `invalid username or password` and SSO/session failures pass through unchanged.
- **Local accounts/roles/SSO config persist across `v2-stop`/`v2-start`.** `v2-start` now passes an explicit, install-dir-colocated `--users-db <install-dir>/.ncc-api-users.json` (new `--users-db` override flag) to the api-server, instead of relying on a working-directory-relative default that could differ between launches.
- **Clicking a cluster link no longer also opens the alert Details drawer.** In the dashboard Alerts table the cluster link (opens Prism in a new tab) was bubbling its click up to the row handler, which also opened the right-hand Details drawer. The link now stops propagation, matching the existing KB-link behavior; clicking elsewhere on the row still opens the drawer.

### Refactored

- **`goNCC.go` package extraction.** Six focused leaf packages were carved out of the ~15.5k-line `goNCC.go`, each re-exported from `main` via type/function aliases so the thousands of existing references and call sites compile unchanged:
  - `internal/model` — foundational shared types (`Config`, `ClusterCredential`, `NotificationSummary`, `ParsedBlock`, `FS`, `HTTPClient`, `OSFS`) and `ClusterHealthScore`.
  - `internal/promtext` — Prometheus textfile writers (`WritePrometheusFile`, `WriteNotificationMetricsFile`, `SanitizeLabel`).
  - `internal/retryutil` — the shared retry/backoff helpers (`JitteredBackoff`, `IsRetryableStatus`, `RetryAfterDelay`), a stdlib-only leaf so both `main` and `internal/notify` can reuse them without an import cycle.
  - `internal/notify` — the email/webhook/Slack senders, retry wrappers, `text/template` overrides, and the per-channel delivery-metrics accumulator (run-level counters now read via `notify.ResetMetrics` / `notify.SnapshotMetrics`).
  - `internal/nccparse` — the NCC summary parser (`SplitLines`, `ParseSummary`, `ValidateParsedAlertsAgainstPluginResults`) producing `model.ParsedBlock`.
  - `internal/httpclient` — the `*http.Client` builder (`New`, aliased as `NewHTTPClient`): connection pooling, TLS policy, and the optional `LoggingTransport` with Authorization/Cookie/password redaction. The os-backed `OSFS` moved next to the `FS` interface in `internal/model`.
  Behavior is identical and each package ships its own unit tests (notification, template, parser, and HTTP-redaction tests were relocated/added alongside their implementations). The full Go suite passes under `-race`.

### Changed

- **Interactive login + RBAC, enabled by default in containers.** When local accounts or SAML are configured, the api-server `auth-mode` transparently upgrades to `hybrid` so static tokens (automation) and cookie sessions (browsers) both work. `/api/v1/health` additionally reports `login_enabled`, `local_login`, and `saml_enabled`, and `rbac_enabled` now also reflects interactive login. `docker-compose.yml` / `Dockerfile.api` pass `--users-db` (file store) and `k8s/api-deployment.yaml` uses the Kubernetes Secret store (`--users-db-secret ncc-v2-users` + the `ncc-v2-api` service account), so the first-run admin bootstrap runs out of the box; `ncc-ui-server` auto-detects login and forwards browser sessions.
- **Dependency refresh.** Go modules updated (`github.com/modelcontextprotocol/go-sdk` 1.6.0→1.6.1, `golang.org/x/sys` 0.44→0.45, `mattn/go-colorable`, `mattn/go-runewidth`); OpenTelemetry SDK + OTLP/HTTP trace exporter added for the opt-in tracing. The Go directive was bumped `1.26.3 → 1.26.4` to pick up the stdlib `net/textproto` and `crypto/x509` fixes (`GO-2026-5039`, `GO-2026-5037`); `go vet`, `go test -race`, and `govulncheck` are clean. Frontend `npm audit` reports 0 vulnerabilities.
- Version bumped to `2.1.0` across `VERSION`, the orchestrator/api/ui `main.Version` defaults, the OpenAPI spec version, `binaryGO.txt`, `frontend/package.json`, the Helm chart, and the `k8s/` image tags.

### Remaining (tracked in IMPROVEMENTS.md)

- **`goNCC.go` slimming (continued).** The extraction (`internal/model`, `internal/promtext`, `internal/retryutil`, `internal/notify`, `internal/nccparse`, `internal/httpclient`) is complete for v2.1.0. `goNCC.go` is still large; the remaining obvious candidate is the report renderers (`generateHTML` / `generateCSV` / `generateMarkdown` / `generateJSON` / `generateSARIF`), which are coupled to the combined-HTML and drilldown code and need a coordinated move. It can follow the same alias-backed, behavior-preserving pattern. See [`IMPROVEMENTS.md`](IMPROVEMENTS.md).

---

## [2.0.2] - 2026-05-29

Maturity + patch release. Includes the v2.0.0 / v2.0.1 → v2.0.2 patch chain (path / CORS / IPv6 / extractor / uninstall), plus a substantial trust-and-operability layer: Windows VERSIONINFO, optional code-signing hooks, `verify` / `doctor` / `v2-status` subcommands, `ncc-api-server --health-check` mode, Prometheus `/metrics`, shell completions, CycloneDX SBOMs, SLSA build provenance, `release-attestation.json`, and `docker compose` one-command launch. **Affiliation note:** independent open-source project; not affiliated with or endorsed by Nutanix, Inc.

### Added

- **`ncc-orchestrator verify`** subcommand prints embedded version / git revision / build date / Go version / executable SHA-256, project URL, license, and an explicit non-affiliation disclaimer. Pinned by `TestRunVerifyCommand_OutputContract` and `TestSha256OfFile_RoundTrip`.
- **`ncc-orchestrator doctor`** subcommand: 5-section diagnostic (verify, v2-check, v2-status, environment, log tails) plus a redacted support tarball under `./ncc-support-<UTC-timestamp>.tar.gz`. Config secrets matching `password / secret / token / credential / api-key / client-id` are replaced with `***REDACTED***` before they enter the bundle. NCC_* env var **names** are listed but **values** are redacted.
- **`ncc-orchestrator v2-status`** subcommand reads `<install-dir>/run/v2-*.pid`, checks each PID is alive, probes `/api/v1/health`, and prints a status table (or JSON with `--json`). Tolerant of missing PID files.
- **`ncc-orchestrator completion bash|zsh|fish|powershell`** generates shell-completion scripts via cobra's built-in generators. Stays in lockstep with the actual subcommand / flag set automatically.
- **`ncc-api-server --health-check`** probe-only mode: connects to `/api/v1/health` using the on-disk token and exits 0/1. Designed for Docker `HEALTHCHECK` and Kubernetes `livenessProbe`; `Dockerfile.api`'s HEALTHCHECK now uses it instead of bundling `wget`.
- **`/metrics` Prometheus endpoint** on the api-server: build info, process uptime, run state + counters (`ncc_runs_triggered_total`, `ncc_runs_completed_total`, `ncc_runs_failed_total`), Go runtime stats, rate-limiter counters. Auth-gated by default; pass `--metrics-public` for token-free scraping on private networks. Content type `text/plain; version=0.0.4; charset=utf-8`.
- **Standalone API / UI server stack-aware mode** via the new `internal/v2layout` package. `ncc-api-server` and `ncc-ui-server` invoked from `<X>/bin/` now auto-resolve `--repo-root`, `--config-path`, `--output-dir`, `--log-dir`, `--token-file-path`, and `--orchestrator-bin` to matching paths inside the extracted stack; symlink-resolved consistently. Pinned by `TestApplyStackAwareDefaults_RespectsExplicitOverrides`, `TestUIExplicitFlagSet`, and the `internal/v2layout` test suite.
- **Helpful subcommand redirect** in `ncc-api-server` and `ncc-ui-server`: invoking with an orchestrator-only positional arg (`update`, `v2-start`, `doctor`, …) now exits 2 with a message pointing to `bin/ncc-orchestrator <subcommand>` instead of silently starting a server.
- **Windows VERSIONINFO** embedded in every Windows binary via `tools/winversioninfo` (uses `github.com/josephspurrier/goversioninfo`). ProductName, FileDescription, CompanyName, LegalCopyright, LegalTrademarks (with non-affiliation disclaimer), and version number all visible in Windows Properties → Details. `.syso` files are gitignored and regenerated each release.
- **Optional code-signing hooks** in `binaryGO.txt`: macOS Developer ID + notarization (`codesign` + `xcrun notarytool`), Windows Authenticode (`osslsigncode`), Linux GPG detached signatures. No-op unless the matching env vars are set. The Windows hook now embeds a publisher description (`-n`) and URL (`-i`) into the signature.
- **Self-signed Windows signing helpers** `scripts/sign-windows.sh` (macOS/Linux build host via openssl + osslsigncode) and `scripts/sign-windows.ps1` (native Windows via `New-SelfSignedCertificate` + `Set-AuthenticodeSignature`). Both apply a real Authenticode signature with the project publisher subject and export a public `.cer` for fleets to import into Trusted Publishers. `docs/SECURITY_AND_TRUST.md` documents what controls the SmartScreen "Publisher" line and how to trust a self-signed cert fleet-wide. Note: a CA-issued (OV/EV) certificate is still required to clear SmartScreen for public distribution.
- **`release-attestation.json`** emitted by `binaryGO.txt`: per-release provenance manifest with product, version, git revision, git dirty, build host OS/arch, project URL, license, non-affiliation disclaimer, and per-artifact SHA-256s.
- **CycloneDX 1.5 SBOMs** generated by `binaryGO.txt` via `cyclonedx-gomod` (one `bom-<binary>.cdx.json` per main package). Drop-in for Trivy / Grype / Dependency-Track. Generation step skips silently when `cyclonedx-gomod` is not installed.
- **`.github/workflows/release.yml`** — tag-driven release pipeline: builds via `binaryGO.txt`, generates SBOMs and `release-attestation.json`, signs every artifact with `actions/attest-build-provenance@v2` (SLSA build provenance), and publishes a GitHub Release. Verifiable with `gh attestation verify`.
- **`docker-compose.yml`** + updated `Dockerfile.api` — one-command stack launch (`docker compose up -d`). UI start is gated by the api-server's HEALTHCHECK, which uses the new `--health-check` mode. Token shared between containers via a named volume; no secret in env vars.
- **`docs/SECURITY_AND_TRUST.md`** documents the entire trust chain: SHA-256 verification, OS first-run guidance (Gatekeeper / SmartScreen / executable-bit), `verify` output schema, Windows file-properties dialog, GPG signature verification, SBOM consumption, and SLSA attestation verification.
- **README "Run on macOS / Windows / Linux (trust & verification)"** section + **"Running individual components (API only / UI only)"** section explain stack-aware standalone launches and OS security bypasses.

### Fixed

- **Windows: shipped `.exe` binaries are no longer reported as "not executable."** `v2-check`, `v2-start`, and the api-server's startup guard tested the Unix executable bit (`mode & 0o111`), which is always 0 on Windows — so `v2-check` failed with `orchestrator-bin / api-server / ui-server binary not executable` and `v2-start`'s api-server child exited 1 with `orchestrator binary is not executable: …\ncc-orchestrator.exe`, even after `Unblock-File`. The executability test now lives in `v2layout.IsExecutable` and is OS-aware: Windows checks the file extension against `PATHEXT` (default `.COM;.EXE;.BAT;.CMD`); Unix keeps the strict `mode & 0o111` check. Pinned by `TestIsExecutable`, `TestHasWindowsExecutableExt`, `TestHasWindowsExecutableExt_RespectsPATHEXT`.
- **`v2-check` / `v2-start` / `v2-stop` / `uninstall` now auto-detect the stack root from the running binary's location.** When invoked from `<X>/bin/<self>` and `<X>` looks like a v2 layout (contains `frontend-dist/` or `bin/ncc-api-server*`), the install-dir defaults to `<X>` instead of `<cwd>/.ncc-v2`. Secondary paths (`--config-path`, `--output-dir`, `--log-dir`, `--token-file`) default to `<install-dir>/<name>`. If `<install-dir>/config.yaml` is missing, falls back to `<install-dir>/example_config.yaml` with a warning. Resolves the user-reported `v2-check failed (5 issues)` when running from inside an extracted stack.
- **`v2-start` against the recommended layout no longer fails with `path escapes repo root`.** The api-server's `--repo-root` (path-traversal sandbox) was hardcoded to `os.Getwd()`. When the user ran `./ncc-orchestrator v2-start` from `<X>/bin/`, repo-root would be `<X>/bin` but the auto-resolved config-path / output-dir / token-file landed under `<X>` — outside the jail. `runV2Start` and `runV2Bootstrap`'s start-script generator now compute repo-root as the install-dir-or-CWD ancestor (the directory that contains both) and pre-resolve macOS `/tmp` → `/private/tmp` symlinks so the api-server's internal EvalSymlinks comparison sees a consistent prefix.
- **`v2-start` `--wait-ready` no longer hangs / times out when binding to a loopback IP.** `localHTTPURLFromListen` was rewriting `127.0.0.1:port` to `localhost:port` for both the wait-ready check and the UI→API backend URL. On macOS `localhost` resolves to `::1` (IPv6) first, but the api-server is bound IPv4-only, so the connection was refused even though the server was healthy. The helper now preserves the user-supplied host. The CORS allow-list separately gains the `http://localhost:port` form when the UI binds loopback so browsers reaching the UI under either name still succeed.
- **`orchestrator_bin` reported by `/api/v1/health` is now an absolute path that exists on disk.** Previously `resolveV2OrchestratorBin` returned `./ncc-orchestrator` (correct relative to the orchestrator's CWD), but the api-server interpreted it relative to its own CWD (= repo-root), producing a non-existent path. API-triggered runs would have failed to spawn the orchestrator. Helper now returns absolute, symlink-resolved paths.
- **Output extractor preserves executable mode bits** ([carry-over from in-flight 2.0.2 work]). `extractTarGzArchive` and `extractZipArchive` now use the mode in the archive header instead of hardcoded `0644`, so binaries inside `ncc-v2-stack-*.tar.gz` and `.zip` come out as `0755` after `update`.
- **Uninstall sweeps both legacy and new layouts.** `uninstall --remove-local` now adds install-dir-relative `outputfiles/`, `nccfiles/`, `promfiles/`, `logs/`, `.ncc-api-token`, `.ncc-api-schedule.json`, `.ncc-api-notifications.json` to the cleanup set in addition to the existing CWD-relative entries. Belt-and-braces for the rare `--remove-v2-runtime=false` path.

### Changed

- **`v2-check` / `v2-start` / `v2-stop` / `uninstall` flag defaults are now empty strings** instead of hardcoded `.ncc-v2` / `config.yaml` / `outputfiles` / `nccfiles` / `.ncc-api-token`. Empty values are resolved at runtime via the auto-detect logic above. Explicit values are honored as before.
- **README** quick-start updated to show the new no-flags flow first, with the explicit-flags form as the legacy / advanced fallback. Two new sections: "Run on macOS / Windows / Linux (trust & verification)" and "Running individual components (API only / UI only)".
- **Project attribution** rewritten across the codebase, embedded metadata, documentation, and `release-attestation.json`: explicit non-affiliation disclaimer ("not affiliated with or endorsed by Nutanix, Inc."), copyright attributed to the maintainer + contributors, license stated as MIT.
- **`Dockerfile.api`** HEALTHCHECK now uses `ncc-api-server --health-check` instead of bundling `wget`. Drops the runtime-image dependency on a curl-equivalent.

### Tests

- New unit tests: `TestResolveV2RepoRoot`, `TestIsPathAncestor`, `TestResolveV2PathToReal_NonExistentSuffix`, `TestLocalHTTPURLFromListen`, `TestLoopbackAltOriginFromListen` (in `goNCC_test.go`); `TestRunVerifyCommand_OutputContract`, `TestSha256OfFile_RoundTrip` (verify subcommand); `internal/v2layout/layout_test.go` (`TestDetectStackRoot`, `TestFindBinary`, `TestConfigPath`); `cmd/ncc-api-server/stack_aware_test.go`, `cmd/ncc-ui-server/stack_aware_test.go`; `tools/winversioninfo/main_test.go` (`TestSplitVersion`).
- Smoke matrix re-run from a fresh extract of `ncc-v2-stack-darwin-arm64.tar.gz`: update v1→v2, legacy `v2-bootstrap` from project root, `v2-start --detach --wait-ready` from `<X>/bin/`, CORS preflight from both `localhost` and `127.0.0.1` origins, UI proxy reachability, `v2-stop`, `uninstall --dry-run`, `--health-check`, `v2-status` (alive / dead-stale-pid), `doctor` (bundle redaction verified, plaintext-secret count = 0), `completion` for all four shells, `/metrics` (Prometheus exposition format with correct content-type), `cyclonedx-gomod` SBOM emit. All pass.

### Upgrade

```bash
# from anywhere a v2.0.0 / v2.0.1 binary lives
./ncc-orchestrator update --allow-major-upgrade
```

The package-level updater introduced in 2.0.1 handles 2.0.2 the same way: download `ncc-v2-stack-<os>-<arch>.tar.gz`, verify SHA-256, extract, and atomically install `bin/`, `frontend-dist/`, `example_config.yaml` over the resolved install-dir.

---

## [2.0.1] - 2026-05-27

Patch release with one substantial behavior change: **`update` now upgrades the v2 stack as a single package, irrespective of which binary (orchestrator, api-server, ui-server, or any renamed variant) was invoked.** All v1.x users should upgrade via this release rather than v2.0.0.

### Changed

- **`update` is now a package-level operation** ([#9]). When the selected release publishes a `ncc-v2-stack-<os>-<arch>.{tar.gz,zip}` archive (true for v2.0.0+), `update` downloads it, verifies the SHA-256 against `checksums.txt`, extracts to a private temp dir, and atomically installs `bin/*` + `frontend-dist/` + `example_config.yaml` into the resolved install directory. The running binary is self-replaced from the canonical-or-basename-matched entry in the extracted `bin/`. Install-dir auto-detection: running from `<X>/bin/<self>` → install over `<X>`; otherwise install into the binary's directory. This makes the upgrade path identical regardless of which binary you ran or how you renamed it. For legacy v1.x releases without a stack archive, falls back to the original single-binary update path.
- **Legacy single-binary selector hardening (fallback path).** `pickAssetForCurrentPlatform` now prefers assets whose name starts with the running executable's basename (e.g. `ncc-orchestrator-*`) before falling back to the legacy first-match behavior. Retained as defense in depth even though the package-archive path is now primary.

### Fixed

- **`v2-bootstrap` / `v2-start` failed against the v2.0.0 stack archives** because the layout-check (`hasBootstrappedV2Layout`) only accepted canonical binary names (`bin/ncc-api-server`), but v2.0.0 stack archives packaged platform-suffixed names (`bin/ncc-api-server-<os>-<arch>`). The lookup helpers now accept either form, so existing v2.0.0 stack archives bootstrap cleanly, and future archives can converge on canonical names.
- **`pickAssetForCurrentPlatform` regression on multi-binary releases** ([#9]) — The original P0: GitHub returns release assets alphabetically, so when v2.0.0 shipped three binaries per platform the v1.x self-updater silently overwrote `ncc-orchestrator` with the api-server binary. Replaced by the package-update flow above; the legacy code path also got the basename-prefix selector fix as a fallback.
- **v2.0.0 release-asset layout hotfix (2026-05-27 19:35Z)** — As an immediate mitigation for users still on v1.x updaters, the 12 standalone `ncc-api-server-*` and `ncc-ui-server-*` assets were removed from the v2.0.0 release; the api-server and ui-server binaries continue to ship inside `ncc-v2-stack-*` archives.
- **macOS `._*` resource-fork sidecars** are no longer included in tarballs (`COPYFILE_DISABLE=1` + `--no-mac-metadata` honored by BSD tar).

### Build / packaging policy

- `binaryGO.txt` step 6 renames binaries inside the stack archive to canonical names (`bin/ncc-orchestrator`, `bin/ncc-api-server`, `bin/ncc-ui-server`) without platform suffix. Matches the v2-bootstrap expected layout.
- `binaryGO.txt` step 7 enforces the no-standalone-server-binaries publishing policy at build time (excluded from `checksums.txt` and therefore from publishable assets). Documented inline with rationale.
- `binaryGO.txt` step 0 exports `LDFLAGS` so the variable survives step-by-step shell invocation.
- Version metadata bumped to `2.0.1` across `VERSION`, `goNCC.go`, `cmd/ncc-api-server/main.go` (default `Version` + OpenAPI `info.version`), `cmd/ncc-mcp-server/main.go`, `helm/ncc-orchestrator/Chart.yaml`, `helm/ncc-orchestrator/values.yaml`, and the three k8s manifests.

### Tests

Seven tests / twenty sub-cases added covering the package selector, install-dir resolution, layout-check naming tolerance, the v2.0.0 regression path, and the legacy-release fallback.

### Migration

- **From v1.x:** Run `ncc-orchestrator update --allow-major-upgrade` against v2.0.1 (or v2.0.0 since the hotfix). The fixed selector picks `ncc-orchestrator-*` correctly.
- **From v2.0.0:** Run `ncc-orchestrator update`. No major upgrade flag needed.
- **From a corrupted v2.0.0 install** (api-server downloaded in place of orchestrator): Re-download `ncc-orchestrator-<os>-<arch>` directly from the v2.0.1 release page or run the recovery `curl` snippet documented in `RELEASE_NOTES_v2.0.0.md` → Known issues.

[#9]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/issues/9

---

## [2.0.0] - 2026-05-05 (release-candidate hardening through 2026-05-27)

### Added (late-cycle hardening, 2026-05)

- **`example_config.yaml`** — Repo-rooted, validator-clean (`ncc-orchestrator validate-config`) reference config that ships in `dist/` and inside every `ncc-v2-stack-*.tar.gz`/`.zip` archive. Uses `secret://NAME` references with `secrets-provider: env` so it works out-of-the-box once `NCC_PASSWORD` (and optionally `SMTP_PASSWORD`, `WEBHOOK_TOKEN`) are exported.
- **`DELETE /api/v1/runs/active`** — Cancel a stuck/in-flight orchestrator run from the UI; returns `409` with structured `error_code` when no run is active.
- **`GET /api/v1/runs/{id}`** — Single archived run metadata + embedded summary, including artifact bodies (`run-summary.json`, `ncc-run-record.json`, `regression-summary.json`, `checks-snapshot.json`, `run-meta.json`); rejects path traversal (`..`/`/`).
- **Header trigger button with live elapsed time** — Top-ribbon Run control now shows a pulsing primary-tinted button with spinning icon and `Running · Xm Ys` elapsed time, mirroring the Settings → Runs indicator.
- **Context-aware Dashboard empty states** — Alerts table now distinguishes "Run in progress" (with live output link), "All clusters clean", and onboarding "No alerts yet" states based on the active-run query and last summary.
- **Schedule input validator** — `action=create` now requires at least one of `cron` or `every`; covered by `TestValidateScheduleInput` table-driven test.
- **Audit + meta route surface** — Audit query params (`limit`, `since`, `source`) are validated server-side with structured `400` on bad input; CORS `Access-Control-Allow-Methods` now includes `DELETE`.

### Security (late-cycle hardening)

- **Go toolchain upgraded 1.26.2 → 1.26.3** — Fixes five Go standard-library CVEs surfaced by `govulncheck`:
  - `GO-2026-4976` ReverseProxy query forwarding (`net/http/httputil`)
  - `GO-2026-4971` `net.Dial` / `LookupPort` NUL-byte panic
  - `GO-2026-4918` HTTP/2 transport infinite loop on bad `SETTINGS_MAX_FRAME_SIZE`
  - `GO-2026-4977` and the related stdlib advisories
  - After upgrade: `govulncheck ./...` → **No vulnerabilities found**.
- **npm DOMPurify override `^3.4.7`** — Pinned via `package.json#overrides` to clear 5 transitive DOMPurify advisories in `monaco-editor` (ADD_TAGS/FORBID_TAGS bypasses, SAFE_FOR_TEMPLATES bypass, prototype pollution, mutation-XSS) without downgrading Monaco; after override: `npm audit --omit=dev` → **found 0 vulnerabilities**.
- **`yaml` patch update** via `npm audit fix` — Closes deeply-nested-collection stack-overflow advisory (`GHSA-48c2-rrv3-qjmp`).

### Frontend / UX (late-cycle hardening)

- **Theme overhaul** — Dark mode rewritten to a near-black neutral charcoal palette; light mode rewritten to a clean zinc palette with crisp white cards; Ant Design and `styles.css` variables aligned through `theme.tsx`.
- **Monaco editor local + themed** — `@monaco-editor/react` now loads `monaco-editor` locally (resolves CSP `script-src 'self'` violation that was blocking the editor from the CDN). Workers bundled via Vite `?worker` imports. Custom Monaco themes (`ncc-light`, `ncc-dark`, `ncc-it-pro`) registered to mirror the app palette; fallback `<textarea>` styled to match.
- **Form/Accessibility cleanup** — Added `id`, `name`, `htmlFor`, `aria-label`, and `autoComplete` across `ConfigSection`, `RunsSection`, `ScheduleSection`, `PolicyGateBuilderSection`, `AuditLogSection`, `LogsSection`, `DashboardPage`, `ApiExplorerSection`, `JsonOutputsSection`, `RawOutputsSection`, and `SecretsMigrationModal`; trigger-run password field is now wrapped in a real `<form>`; eliminates the DOM warnings for unassociated labels, missing `id`/`name`, password-not-in-form, and missing autocomplete.
- **Runs table redesign** — Replaced often-blank "Index" column with `Type`, `Status`, `Duration`, `Clusters`, and `Issues` columns; cells degrade to `—` for trigger entries.
- **Sparkline correctness** — Fixed timezone bug in `RecentRunsSparkline` (mixed UTC and local dates) via `localDateKey` helper; filtered to count only `history`/`summary` sources so trigger events no longer inflate the "runs in last 7 days" count.

### Build / Release

- **`binaryGO.txt` overhaul** — Preflight checks, consolidated variable declarations, local-arch symlinks, example-file copy, cleaner tar/zip archives, and end-to-end verification (`api/health` filter, version assertion).
- **Production verification (2026-05-27)** — `govulncheck ./...` clean, `npm audit --omit=dev` clean, `go test -race -count=1 ./...` clean, `gofmt -l .` clean, `go vet ./...` clean, `tsc --noEmit` clean, `vite build` clean.

### Added

- **Full v2 application stack in 2.0.0** — Release scope now explicitly includes API (`cmd/ncc-api-server`), UI (`cmd/ncc-ui-server` + `frontend`), and UI-integrated API proxy surface for `/api/v1/*`.
- **Build-from-scratch documentation** - Added `docs/BUILD_FROM_SCRATCH.md` with full setup flow for clean machines, including local build, frontend build, v2 stack startup, tests, packaging, and Kubernetes verification.
- **Release validation suite for v2.0.0** — Production checks and edge-case verification documented with reproducible command evidence.
- **CodeQL workflow alignment** — Added repository workflow that analyzes only `go` and `actions`, preventing JS/TS language-detection failures in this branch scope.
- **v2.0.0 release documentation set** — Added release notes, production-readiness checklist, and milestone summary documents for the v2 train.
- **Build and handover guides** — Added `docs/BUILD_FROM_SCRATCH.md` and `docs/ARCHITECTURE_AND_HANDOVER.md` for end-to-end onboarding, operations, and ownership transfer.
- **Clean v2 uninstall tooling** — Added `scripts/uninstall-v2-clean.sh` as the canonical Kubernetes/runtime cleanup entrypoint; legacy `scripts/uninstall-ncc-orchestrator.sh` now delegates to it.
- **CLI local uninstall command** — Added `ncc-orchestrator uninstall` for standalone local cleanup of artifacts/state created by the binary.
- **`v2-check` preflight command** — Added lightweight v2 runtime self-check for binary executability, config/path readiness, directory writability, and API/UI port bind availability before `v2-start`.
- **Scheduler health API** — Added `GET /api/v1/schedule/health` to expose scheduler runtime hints (`last_run`, `last_success`, `last_error`) and lock/log path metadata.

### Changed

- **Version baselines** — `VERSION`, default `main.Version`, Helm chart/appVersion, Helm values image tag, and Kubernetes manifest image tags aligned to `2.0.0`.
- **README release pointers** — Updated current release status links to `v2.0.0` documents and clarified current branch scope.
- **Documentation alignment across v2 docs** - Updated README, CONTRIBUTING, migration guide, and v2 architecture doc for consistent source-build and deployment instructions.
- **Kubernetes architecture docs** - Clarified API runner binary staging model (runner image -> API init container -> shared tools path) in design docs and release notes.
- **Kubernetes runtime model** — API now stages the runner binary via init container (`runner image -> /tools/ncc-orchestrator`) and validates `--orchestrator-bin` at startup (exists + executable).
- **Production readiness manifests** — Added startup/readiness/liveness probes to API and UI deployments to improve rollout safety and self-healing behavior.
- **Preflight probe handling** — `.ncc-preflight-check` moved to a persistent read/write sentinel approach (no create/delete churn) with legacy typo cleanup.
- **Performance and reliability optimizations** — Cached exclude-title matchers/regex, reduced config re-parse and allocations in secret validation, de-duplicated replay metadata parsing, and eliminated redundant Prometheus writes during replay.
- **Console error UX** — Consolidated duplicate startup error logging to a single user-facing error path.
- **Scheduler overlap safety** — `create-schedule` now supports lock-enabled cron generation with `--with-lock` (default true) to prevent overlapping scheduled runs.
- **Security response handling** — Added redacted-safe config response content and startup warning for plaintext config passwords.
- **Logs UX operability** — Added UI `Follow tail` and `Jump to latest` controls for Runner Logs and Live Logs.

---

## [1.1.0] - 2026-04-21

### Added

- **`create-schedule` command** — New `ncc-orchestrator create-schedule` subcommand to create recurring NCC runs:
  - **Linux/macOS:** creates or updates a cron entry (`--type cron`, `--cron` or `--every`)
  - **Windows:** creates or updates a Scheduled Task (`--type windows`, `--every`)
  - Supports safe preview mode with `--print-only` (default true).
  - New scheduler actions: `--action list|remove|run-now`.
- **MCP scheduler tools** — New MCP tools `create_schedule`, `list_schedules`, and `delete_schedule` for schedule lifecycle operations from AI clients.
- **Run history snapshots** — Optional `--run-history` writes timestamped snapshots under `--run-history-dir` with retention controls (`--retain-last`, `--retain-days`).
- **Regression awareness** — New `regression-summary.json` compares current FAIL counts to previous `run-summary.json`; `--notify-on-regression` only sends notifications when FAIL count increases.
- **Drill-down diff and snapshots** — Added `checks-snapshot.json` and `drilldown-diff.json` with per-check change tracking (new/resolved FAILs, severity changes).
- **Flaky check detection** — Added `flaky-checks.json` with lookback-based severity transition analysis (`flaky-lookback-runs`, `flaky-min-transitions`).
- **Health and SLO exports** — Added per-cluster `health_score` plus `avg_health_score` / `min_health_score` in `run-summary.json`, and `slo-dashboard.json` for dashboards.
- **SARIF export** — New per-cluster output format `sarif` in `--outputs`.
- **Config schema + validation mode** — New `config-schema` and `validate-config` commands for config ergonomics in CI/CD.
- **Policy gates** — Added `policy-gates` support for release/CI rules such as `new-fails>0` or `min-health-score<90`, with violations written to `policy-gates.txt`.
- **Secrets provider** — Added `secret://` resolution for sensitive fields using `secrets-provider=env|file` and optional `secrets-file`.
- **Quiet hours and maintenance windows** — Added notification suppression via `quiet-hours` and `maintenance-windows`.

### Changed

- **ncc-run-record metadata** — Added `git_revision`, `hostname`, and `scheduler_source` for better traceability.
- **Adaptive resilience on 429** — Effective cluster concurrency now adapts down/up based on HTTP 429 pressure (`--adaptive-parallelism`).
- **Single-file report option** — `--single-report` writes `ncc-report-single.html` alongside `index.html`.
- **Strict validation by default** — Config validation now rejects unknown keys and enforces strict value typing by default.

---

## [1.0.0] - 2026-04-07

### Added

- **Nutanix API v4 (default)** — Cluster discovery, start-checks, and Prism task polling use v4 paths where applicable; configurable `nutanix-v4-api-version` (e.g. `v4.2`). Legacy v3 remains available via `ncc-api-version: Legacy` (alias `v1`).
- **Cluster resolution for v4** — `GET .../config/clusters` matches `--clusters` to the correct registered entity (name, extId, external address, or CVM IP); `nodeIps` for run-system-defined-checks come from that cluster’s CVMs (fixes Prism Central multi-cluster / NCC-40023 cases).
- **run-summary.json** — Per-cluster `clusters[]` with `ok`, severity counts, `checks_total`, and `error`; `exit_code` mirrors process exit (0 = all OK, 1 = all failed, 3 = partial success).
- **Exit code 3** — When at least one cluster succeeds and at least one fails, the process exits **3** (partial success). See README.
- **Semver for `--update`** — Version comparison uses [Masterminds/semver](https://github.com/Masterminds/semver); fallback to legacy parse if non-semver.
- **docs/TROUBLESHOOTING.md** — Common TLS, multi-cluster, and API issues.
- **ncc-run-record.json** — Versioned bundle (`schema_version`, orchestrator version, full `run` summary) for pipelines.
- **discover-clusters `--format`** — `lines` (default), `table`, or `json` (name, ext_id, address, api).
- **HTTP rate limits** — Logs `X-RateLimit-*` / `X-Api-Ratelimit-*` on 429; caps long `Retry-After` waits.
- **Helm chart** — `helm/ncc-orchestrator` for templated CronJob image and schedule.
- **Kustomize** — `k8s/kustomization.yaml`; apply with `kubectl apply -k k8s/`.
- **Tests** — Golden CSV and per-cluster HTML fixtures, `httptest` retry for 429, KB markdown helper in `internal/kblinks`.
- **MCP `get_report`** — For `*.log` files, turns `KB NNNN` into markdown links to portal.nutanix.com.

### Changed

- **Version and images:** VERSION set to 1.0.0; Kubernetes CronJob and job-debug use image tag 1.0.0. First stable semver release.
- **Partial multi-cluster runs** — Previously exited **0** when some clusters failed; now exits **3** so automation can detect partial failure.

### Fixed

- **Viper / discover-clusters** — Duplicate `BindPFlag` for `insecure-skip-verify` (and related keys) no longer overrides root flags; `--insecure-skip-verify` on the main command is honored.

---

## [0.1.13] - 2026-03-02

### Added

- **MCP: `list_run_artifacts` tool**  
  List files in an NCC run output directory (run-summary.json, index.html, per-cluster .log/.html/.csv). Optional `output_dir` (default `outputfiles`).

- **MCP: `get_report` tool**  
  Read the aggregated index.html or a specific cluster report file. Optional `output_dir`, `file` (e.g. `index` or `10.0.0.1.html`). Large reports are truncated for context.

- **MCP: Resources**  
  - `ncc://run-summary` — latest run-summary.json (application/json).  
  - `ncc://report` — latest aggregated index.html (text/html).  
  Clients can list and read these via MCP resources/list and resources/read.

- **Documentation**  
  - docs/MCP_SERVER.md: new tools and resources table, “Possible future additions” section.  
  - MCP tool descriptors for list_run_artifacts and get_report.

### Changed

- **Version and images:** VERSION set to 0.1.13; k8s CronJob and job-debug use image tag 0.1.13.

### Fixed

- None explicitly tracked for this release.

---

## [0.1.12] - 2026-02-12

### Added

- **Digest notifications**  
  New option `--notify-digest` (config: `notify-digest`) sends **one email, one webhook, and one Slack message per run** with a run overview (clusters OK/failed, duration) instead of per-cluster notifications. Optional attachment of the aggregated `index.html` to the digest email when `email-attach-html` is set; optional base64-encoded index in the webhook payload when `webhook-include-html` is set.

- **Run summary log**  
  At the end of each run, a single structured log line is emitted with `clusters_ok`, `clusters_failed`, `duration_s`, and `index_html` path for easier parsing and monitoring.

- **Notification retries**  
  Email and webhook sends are wrapped in a retry loop (3 attempts with jittered backoff using `retry-base-delay` / `retry-max-delay`) to improve reliability on transient failures.

- **Replay + HTML attach**  
  In replay mode, per-cluster HTML (and CSV) is generated **before** sending notifications. When `email-attach-html` is set, the generated HTML file is attached to the replay email; when `webhook-include-html` is set, the replay webhook payload includes the report as base64.

- **Config path validation**  
  `validateConfig` now requires non-empty (after trim) values for `output-dir-logs`, `output-dir-filtered`, `log-file`, and `prom-dir`, with clear error messages.

- **HTTP log redaction**  
  When `log-http` is enabled, request/response dumps redact the `Authorization` and `Cookie` headers, and mask JSON values for `"password"` / `"Password"` fields to avoid leaking secrets in logs.

- **Documentation**  
  - README: HTTP pool/timeouts description, example webhook payload, testing email and webhook (webhook.site, MailHog, Mailtrap), `--version` behavior, `notify-digest` and env vars.
  - k8s/README: Runbook for CronJob failures (logs, job-debug, NFS permissions, prune images).
  - k8s ConfigMap: `notify-digest: false` option added.

- **Unit tests**  
  New/updated tests for `validateConfig` (path validation), `checkOutputPermissions`, digest overview format, and `ParseSummary` (no-block and check-name cases).

### Changed

- Per-cluster email/webhook/Slack are **skipped** when `notify-digest` is true; digest notifications are sent after the aggregated HTML is written.
- Replay mode uses `sendEmailWithRetry` and `sendWebhookWithRetry` for consistency with the main run.

### Fixed

- None explicitly tracked for this release.

---

## [0.1.11] and earlier

See git history and [Releases](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases) for prior versions.

---

[2.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.0
[1.1.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.1.0
[1.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.0.0
[0.1.13]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.13
[0.1.12]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.12
