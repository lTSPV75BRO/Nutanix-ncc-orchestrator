# NCC Orchestrator — v2.1.0

**Release date:** 2026-08-10
**Type:** Authentication + autonomic-operations + maintenance/hardening release. Recommended for everyone on v2.0.x.

> **Affiliation:** This is an independent open-source project. It is **not** affiliated with or endorsed by Nutanix, Inc. NCC and Nutanix are trademarks of their respective owners. The project is MIT licensed; see [`LICENSE`](LICENSE).

> ## Known issue — please use [v2.1.1](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.1.1) (or newer) instead
>
> v2.1.0 has one known cosmetic issue, fixed in **v2.1.1**: a cluster whose NCC run fails outright (connection/auth/timeout) is surfaced in the Alerts table as a `FAIL`-severity `"NCC run failed"` row instead of `UNKNOWN`. This **does not lose or corrupt any data** — the row is visible, its Detail carries the real error — but it overstates the finding (conflating "NCC never ran" with "NCC found a real failing check") and can skew FAIL totals / regression "new failures" counts with connectivity noise. See [Known issues](#known-issue-ncc-run-failed-rows-tagged-fail-instead-of-unknown) below for the full description, workaround, and the v2.1.1 fix.
>
> **Upgrade in place:** `./ncc-orchestrator update` (or, from the UI, **Settings → Access → Software updates**).

v2.1.0 brings real **multi-user authentication and RBAC** to the api-server — local password accounts, **SAML SSO**, and **LDAP / Active Directory** login, a first-run admin bootstrap with a forced password change, runtime user/SSO/LDAP management in the UI, self-service **personal access tokens**, **cluster groups** for membership-based access control, and a Kubernetes Secret-backed user store that is encrypted at rest. The browser-facing UI now serves **HTTPS by default** (auto-generated self-signed cert, HTTP→HTTPS redirect, in-app certificate management).

On top of that, v2.1.0 makes the v2 stack **self-running and self-healing**: a single native **supervisor** (`v2-supervise`) with a cross-platform **boot-service installer** (`v2-install-service`) keeps the API + UI alive across crashes, hangs, *and* reboots; a **systemd-timer scheduler backend** joins cron; an active **`doctor --fix` self-heal subsystem** detects and remediates latent faults (config/storage/secrets/backups/runs/TLS/process/log) and is surfaced in a new **System Health** admin panel and a `/api/v1/health/diagnostics` endpoint; the api-server now serves **operational `/metrics`** with a starter **Grafana dashboard**; the file-backed user store gains optional **AES-256-GCM envelope encryption**; and the UI standardizes on **UTC-on-the-wire / browser-local** timestamps.

It also closes a download-integrity gap in `v2-bootstrap`, adds notification observability and templating, hardens TLS / notifications / process supervision, serves NCC run metrics over the api-server `/metrics` endpoint, adds OpenTelemetry tracing, adds `v2-backup`/`v2-restore`, improves the Windows self-update flow, refreshes dependencies, and begins splitting the monolithic `goNCC.go` into focused packages. There are no breaking changes — every v2.0.x invocation keeps working, and static-token automation is unaffected.

---

## Highlights

### Authentication, login & RBAC (admin / operator / viewer)

The api-server now supports real interactive login with three ordered roles — `viewer < operator < admin`:

- **viewer** reads non-settings `GET` endpoints; **operator** can also trigger/cancel/preflight runs; **admin** can do everything, including `/api/v1/settings/*`, user management, and token rotation.
- **First-run admin bootstrap (zero-config).** With a writable user database, an empty store provisions an `admin` account with a random password on first launch (printed to the log and stored for retrieval). The admin is forced to change it on first login, then can add users, assign roles, and configure SSO from the UI.
- **Local accounts, SAML SSO, and LDAP/AD.** Password accounts (bcrypt) are managed at runtime in **Settings → Access** (or `/api/v1/settings/users`). SAML can be set via startup flags or configured at runtime in the UI — the SP signing key is generated server-side and never uploaded through the browser; publish `<root>/saml/metadata` to your IdP. LDAP / Active Directory login is **local-first with AD fallback** (service-account bind + search + rebind, AD group→role mapping), configurable by flags or at runtime with a **Test connection** check. SAML and LDAP can be enabled together.
- **Secure sessions.** Browser sessions use an httpOnly, `SameSite=Strict` `ncc_session` cookie with double-submit CSRF protection; the cookie is marked `Secure` automatically when the UI is on HTTPS (the default) and non-`Secure` under `--ui-insecure-http` so plain-HTTP hosts can still log in. Static-token automation (`NCC_API_TOKEN` / `NCC_API_VIEWER_TOKEN`) keeps working unchanged — `auth-mode` auto-upgrades to `hybrid`.
- **`ncc-ui-server`** auto-detects when login is enabled and forwards each browser's session/CSRF instead of injecting the shared admin token, and proxies `/saml/*` to the api-server so the SP flow completes on the UI origin.

### HTTPS by default + in-app certificate management

The browser-facing `ncc-ui-server` now serves **HTTPS out of the box**. With no certificate supplied, `v2-start` generates a **self-signed** certificate (ECDSA P-256, SANs for the listen host plus `localhost`/loopback, stored under `<install-dir>/tls/`) and binds TLS automatically, and plain-HTTP requests on the same port are **308-redirected to HTTPS** (a first-byte peek demultiplexes TLS from HTTP — no separate HTTP listener). Admins manage the certificate from **Settings → Access → HTTPS / TLS**: generate/renew a self-signed cert for the current host (`POST /api/v1/settings/tls/generate`), install your own PEM cert + key from an internal PKI or public CA (`PUT /api/v1/settings/tls`), or revert (`DELETE`). Enabling HTTPS marks session cookies `Secure` and validates the SAML `SameSite=None` SP cookie, so **SSO works out of the box** on the default self-signed HTTPS. Opt out with `--ui-insecure-http` for a trusted loopback or a TLS-terminating proxy.

### Cluster groups (opt-in isolation)

Admins can segregate clusters into named **cluster groups** (Settings → Access) for membership-based access control. Membership is the union of local accounts, AD groups (by CN/DN, matched against `memberOf`), and individual AD users, and a group may also list **Prism Centrals** (every registered cluster is folded in automatically). The model is **opt-in isolation**: a non-admin in **no** group is **unrestricted** (a plain viewer sees every cluster's alerts with zero setup), while membership in **one or more** groups confines the caller to the union of those groups' clusters — run triggers are pinned to that set and report/dashboard data is filtered server-side. Assigning AD principals uses **live directory type-ahead**, and admins/static-token callers are always unrestricted.

### Concurrent cluster-group runs with overlap de-duplication

The run engine no longer serializes to a single in-flight run: up to `--max-concurrent-runs` (default 4) runs execute at once (extras queue), so one group can run while another's run is in progress. When two runs request **overlapping clusters**, the later run skips the clusters the first is already refreshing (surfaced as `skipped_clusters`) and runs only the remainder — a shared cluster is scanned once, and each group still sees that cluster's freshest result via cluster-group filtering. `GET /api/v1/runs/active` returns a `runs[]` list with per-run status/clusters/output, and Settings → Runs gains an **Active & Queued Runs** panel.

### Personal access tokens (self-service API credentials)

Any signed-in user can mint their own **bearer token** (`ncc_pat_…`) from the header user menu to call the API outside the browser. A PAT **inherits the owner's role** (re-resolved live for local accounts), is shown once, is SHA-256-hashed at rest, can **expire (7 days–1 year, default 90) or never expire**, is revocable, and is capped at 25 per user. Admins audit/revoke any user's token in Settings → Access.

### Backup / restore

New `v2-backup` / `v2-restore` (CLI or Settings → Access) capture and recover an install dir's stateful parts — config, the user database (accounts, roles, SAML/LDAP config, cluster groups, PATs, session policy), API token, audit log, portable `v2-start` settings, and the latest run's report — as a single `0600` tar.gz. Restore is OS- and version-agnostic, restarts the stack automatically, and **preserves host-specific networking/TLS** (CORS origins, advertise/backend URLs, listen addresses, `--ui-insecure-http`, UI TLS paths) so importing a backup from another host doesn't cause an `origin not allowed` lockout.

**Encrypted backups.** A backup is a secrets bundle, so `v2-backup --encrypt` seals the finished archive with **AES-256-GCM** and `v2-restore` transparently detects and decrypts it (an unencrypted archive restores unchanged). Key material is independent of the user-store master key: a **passphrase** (`--passphrase` / `NCC_BACKUP_PASSPHRASE`, scrypt with a per-archive salt) or a **raw 32-byte key** (`--key-file` / `NCC_BACKUP_KEY_FILE`, or inline `NCC_BACKUP_KEY`). A wrong key/passphrase or any tampering fails closed (GCM auth) rather than restoring garbage. **Restore also streams entries to disk under per-file/total/count caps**, so a malicious or corrupt archive can't exhaust memory or disk. The web UI (*Settings → Access → Backup & restore*) wires this end to end: **Download backup** takes an optional passphrase (the encrypted download saves as `…tar.gz.enc`) and **Restore from file…** accepts the passphrase for an encrypted archive — the passphrase reaches the child via `NCC_BACKUP_PASSPHRASE`, never a URL, process listing, or audit log. **Server-side snapshots** are covered too: create an encrypted snapshot with a passphrase, see which snapshots are encrypted in the list, and restore/download them (encrypted snapshots download as the opaque `.tar.gz.enc` envelope). The encrypt dialogs require a **passphrase confirmation** and a minimum length so a typo can't produce an unrecoverable archive, and **automated rollback points are sealed too** — when a backup key is configured in the api-server environment (`NCC_BACKUP_KEY_FILE` / `NCC_BACKUP_KEY` / `NCC_BACKUP_PASSPHRASE`), the updater's pre-update snapshot is written encrypted and the auto-rollback decrypts it transparently. An optional **retention cap** (`NCC_BACKUPS_RETAIN=N`) prunes the oldest *manual* snapshots after each create and never touches pre-update rollback points.

**Scheduled, optionally encrypted backups + per-snapshot verify.** *Settings → Access → Scheduled backups* runs server-side snapshots on a fixed cadence (hourly … weekly) with a retention count and optional **AES-256-GCM** encryption (`GET`/`PUT /api/v1/settings/backups/schedule`). It runs **in-process inside the api-server** on purpose — only that process holds the backup key (`NCC_BACKUP_*`), which a detached systemd timer/cron entry would not inherit, so it's the one place that can produce *encrypted* scheduled backups without writing secrets into a unit file (it's also cross-platform). Each run writes a timestamped `ncc-backup-<UTC>.tar.gz[.enc]` to `<install>/backups` (`v2-backup --output-dir`) and prunes older *scheduled* archives to the retention count, never touching `manual-*` or `pre-update-*` rollback points; cadence survives restarts. Separately, every server-side snapshot now has a **Verify** action (`POST /api/v1/settings/backups/verify`) that runs `v2-restore --verify-only` — gzip/tar integrity, manifest, confined paths — **without touching the live stack**, so you can confirm a snapshot is restorable before trusting it (for an encrypted snapshot it prompts for the passphrase and a pass also confirms the key decrypts it).

**Operational notifications.** Alerts now also fire on **operational** failures, not just runs, reusing the existing "run failure" toggle (no new config): a **failed backup snapshot** (manual, scheduled, or the updater's pre-update rollback point) and a **self-heal cycle that finds failing `doctor` checks** (only on a healthy→failing transition, so a persistent failure doesn't alert every cycle) — through the same Slack/webhook/email channels.

**`verify --online` + signed releases.** `ncc-orchestrator verify` can fetch the matching release's `checksums.txt` from GitHub and report **MATCH / MISMATCH / NOT FOUND** for this platform's binary, exiting non-zero on a mismatch (handy in CI / health checks). On top of that, releases can now be **cryptographically signed**: the maintainer embeds an Ed25519 **public key** in the binaries and publishes a detached `checksums.txt.sig`, and both `verify --online` and the in-app updater **verify that signature against the embedded key before trusting any hash** — closing the gap where an attacker who could swap `checksums.txt` could also swap its checksums. Verification is stdlib-only (no gpg/cosign), an `INVALID` signature always fails, `--require-signature` also fails on a missing one, and unsigned/dev builds degrade to `SKIPPED` (unchanged). The default `verify` stays offline.

**Notification delivery controls + scheduled digest.** A new **Settings → Notifications** admin tab configures channels (Slack/webhook/email, with per-channel Test buttons), which events alert, and the controls below. The api-server alert layer (run/backup/self-heal failures) gained production-grade delivery gates: **quiet hours** (recurring daily mute in a configurable timezone, with an `allow_failures` exemption), **maintenance windows** (absolute mute ranges), and **throttling** (per-event dedup + a global minimum interval) to prevent alert storms. A new opt-in **scheduled health digest** emails a periodic summary of the latest run (cluster ok/failed, FAIL/WARN/ERR totals, top policy violations) over the configured channels, reading the canonical run-summary the dashboard already serves.

**Audit forwarding to SIEM / syslog.** The JSONL audit stream can be shipped off-box for compliance and alerting via two flag-configured sinks (usable together): HTTP (`--audit-forward-http-url`, optional auth header and Splunk-HEC wrapping — Splunk/Elastic/Loki/webhook) and syslog (`--audit-forward-syslog host:port`, udp/tcp, RFC5424). Forwarding is best-effort and decoupled from request handling (bounded buffer + background worker), with a `ncc_audit_forward_dropped_total` metric.

**API explorer parity, with required roles.** The Settings → API explorer now merges `/api/v1/openapi.json` with the full `/api/v1/meta/routes` catalog, so every registered route is listed (the explorer previously showed only the OpenAPI subset and hid `/settings/backups*`, `/health/diagnostics`, and others), and each route now shows a **required-role tag** (viewer/operator/admin). The route catalog (`apiRouteCatalog`) is the **single source of truth**: the OpenAPI spec is gap-filled from it (summary, request example, and an `x-required-role` annotation per route), so Swagger (`/docs/ui`) and the explorer can't drift as routes are added.

### User database: file or Kubernetes Secret (encrypted at rest)

The user database (bcrypt hashes, roles, the must-change flag, the SAML SP private key, the first-run password) can be persisted as a local `0600` JSON file (`--users-db`, used by file/stack/`docker-compose` installs) or inside a **Kubernetes Secret** (`--users-db-secret`, used by `k8s/api-deployment.yaml`). The Secret path reads/creates/patches the Secret over the in-cluster API using the pod's service account — via a small built-in REST client, no `client-go`, so the static binary stays lean.

> **Encrypt it at rest.** Kubernetes Secrets are only base64-encoded by default. Enable etcd encryption-at-rest (KMS v2 recommended, or `secretbox`/`aescbc`) — see [`k8s/encryption-config.example.yaml`](k8s/encryption-config.example.yaml). The api-server runs with least-privilege RBAC ([`k8s/rbac.yaml`](k8s/rbac.yaml)): it can only get/update/patch the single `ncc-v2-users` Secret.

### Self-running & self-healing stack: supervisor + boot-service installer

A single native **supervisor** — `ncc-orchestrator v2-supervise` (also `v2-start --supervise`) — now launches and owns the `ncc-api-server` and `ncc-ui-server` children directly and keeps them alive: **liveness restarts**, **HTTP `--health-check` probes** that catch a hung-but-alive process, **exponential backoff** (capped at 30s), and **cooldown-and-resume** when the per-window restart budget is exhausted, with graceful SIGTERM→SIGKILL shutdown. New **`v2-install-service` / `v2-uninstall-service`** register that supervisor with the platform service manager for reboot persistence — a `Type=simple` **systemd** service on Linux (forcing the `bin_t` SELinux context on install so the unit can actually exec on an enforcing host — see Bug fixes below), a **Task Scheduler** task triggered *at system startup* on **Windows** (closing the gap where the Windows stack had no auto-start after reboot), and a **launchd** LaunchDaemon on **macOS**. The OS keeps the supervisor alive across reboots and the supervisor keeps the stack alive across crashes/hangs, so the old oneshot wrapper and detached sh supervisors are no longer needed. On start the supervisor **replays persisted `.ncc-v2-start.json` settings** so a reboot honors runtime changes (e.g. TLS enabled from Settings → Access). It also enforces a **single-instance guard**: a second `v2-supervise` refuses to start rather than crash-looping on the bound ports and deleting the live stack's pid files. Pinned by `TestSuperviseRestartsAndStops`, `TestSuperviseWaitTokenGatesStart`, `TestSuperviseRefusesSecondInstance`, `TestSystemdSupervisorUnit`.

### systemd-timer scheduler backend (alongside cron)

`create-schedule --type systemd` installs the recurring NCC scan as a systemd **timer** (`ncc-sched-<task>.timer`) driving a oneshot **service**, instead of a crontab entry — a better fit on a systemd host: the run executes with an explicit `WorkingDirectory` and clean environment (no cron `cwd`/relative-path footgun), output is captured per-run to the scheduler log, **overlapping runs are prevented automatically** (systemd won't start a second activation of a `Type=oneshot` service while the previous run is active — the flock is unneeded), and `Persistent=true` **replays a run missed while the box was off**. The interval/`--cron` is translated to `OnCalendar`, **including day-of-week** — cron numbers (`0`/`7` = Sunday), names, lists, ranges, and steps are enumerated to systemd weekday names so the cron/systemd numbering mismatch can't run on the wrong days (a restricted day-of-month *and* day-of-week together is rejected, since cron ORs them and systemd ANDs them). `--type auto` detection/cleanup covers both backends and a **coexistence guard** prevents scheduling the scan twice. Surfaced in the UI via a "systemd timer (Linux)" option in Settings → Schedule (Advanced); cron remains the default and existing installs are unchanged. Pinned by `TestCronToOnCalendar`, `TestOnCalendarFromSchedule`, `TestSanitizeSystemdName`, `TestBuildSystemdScheduleUnits`, `TestNormalizeScheduleTypeSystemd`.

> **Scheduling reliability fix.** Schedules created from the UI/API now anchor the run's **config and log paths to absolute, install-root paths** before they're applied, and the systemd backend refuses to install a **config-less** timer. This fixes scheduled runs that previously failed with `at least one cluster must be provided` (exit 2) and silently produced no output because the generated runner couldn't find a relative `config.yaml` from its own working directory.

### Active self-heal (`doctor --fix`) + System Health panel

A new check registry turns `ncc-orchestrator doctor` from a read-only diagnostic into an **autonomic self-heal** tool. Each check detects a latent fault and, with `--fix`, applies a **safe, idempotent, non-destructive** remediation across **config** (repairs duration artifacts, re-validates), **output-dir routing** (re-anchors relative `output-dir-*` to absolute paths — the scheduled-run footgun, at the source), **output dirs** (creates missing), **disk space**, **secret-file permissions** (chmod back to `0600`), **backups** (verify-after-create, restore preflight, retention, take-a-fresh-backup-if-stale), **runs** (failure classifier + bounded auto-retry with a safe mitigation, most-recent-run health, run-output freshness), **TLS** (cert expiry; auto-renews the stack-managed self-signed cert), **stale PID files**, and **log sizes** (rotate). `doctor --json` emits a machine-readable report and exits non-zero on any failing check. The api-server can run these on a timer (`--self-heal-interval`, with `--self-heal-auto-fix`) and exposes a new **admin-only `GET/POST /api/v1/health/diagnostics`** that merges the orchestrator checks with live **LDAP/AD bind, SAML SP cert, and clock-skew** probes into one ranked list. The new **Settings → System Health** tab visualizes it — per-category cards, status tags, a one-click **Heal now** (with a confirm describing exactly what the fixers do), a **Re-scan** button, and an "auto-heal loop on" badge — and exposes `ncc_selfheal_*` metrics. An encrypted user store with **no master key configured now fails fast** instead of bootstrapping a fresh admin over your real accounts. Pinned by `TestRunSelfHealFixesRoutingAndDirs`, `TestCheckSecretsPermsChmod`, `TestVerifyBackupArchiveAndRetention`, `TestClassifyRunOutput`, `TestDecideRunHeal`, `TestCheckStalePIDs`, `TestCheckLogSizes`, `TestCertExpiryStatus`, `TestResolveUserStoreBackendFailsFastOnEncryptedWithoutKey`.

### Operational `/metrics` + starter Grafana dashboard

The api-server `/metrics` endpoint now also exposes live operational series alongside the existing build/runtime/per-cluster metrics: concurrent-run gauges (`ncc_runs_running`, `ncc_runs_queued`, `ncc_runs_max_concurrent`), a run-duration summary (`ncc_run_duration_seconds_*`, `ncc_run_last_duration_seconds`), authentication counters (`ncc_auth_logins_total`, `ncc_auth_login_failures_total`, `ncc_auth_lockouts_total`), and in-app-update outcomes (`ncc_update_applied_total`, `ncc_update_failed_total`). **Backup/update observability gauges** were added too: a server-side backup inventory (`ncc_backups{kind}`, `ncc_backups_encrypted`, `ncc_backup_last_timestamp_seconds` — one query for a "no recent snapshot" alert) and an update-availability pair (`ncc_update_available` + `ncc_update_check_timestamp_seconds`) populated from the last UI update check, emitted only after a check has run (so a stale `0` never reads as "up to date") and never running the slow check at scrape time. A ready-to-import Grafana dashboard ships at `deploy/grafana/ncc-orchestrator-dashboard.json` (run throughput/duration, login success/failure/lockouts, rate-limiter, update outcomes, process health), and `Prometheus.md` documents the new series with example queries. Settings → Runs also surfaces a **run-queue ETA** (`queue_position` + `avg_run_duration_sec`).

### Envelope encryption for the file-backed user store (secrets at rest)

For non-Kubernetes installs, the local user database (`.ncc-api-users.json` — bcrypt hashes, PAT hashes, the SAML SP private key, the LDAP bind password) can now be transparently wrapped with **AES-256-GCM** (fresh random nonce per write). Supply a 32-byte master key via `NCC_MASTER_KEY` (base64/hex) or `--users-db-key-file` / `NCC_MASTER_KEY_FILE` (keep the key **off** the protected disk/backup so key and ciphertext aren't co-located). With no key configured the store stays plaintext (fully backward compatible); enabling a key never locks you out (a legacy plaintext store is read as-is and upgraded on the next write). In Kubernetes the recommended path remains the etcd/KMS-encrypted Secret backend. Pinned by `TestSealOpenRoundTrip`, `TestOpenWithWrongKeyFails`, `TestNoncesAreUnique`, `TestDecodeMasterKeyFormats`, `TestEncryptingBackendMigratesPlaintext`.

### Consistent timezones: UTC on the wire, local in the UI

Every timestamp the server produces is now UTC RFC3339, and the frontend routes all date/time rendering through a single `src/utils/datetime.ts` helper that parses backend values as instants (defensively treating any timezone-less string as UTC) and renders them in the **browser's local timezone**. This replaces per-component formatters across Dashboard, Insights, Settings, Runs, Schedule, Audit, Access, Tokens, and System Health, fixes hover tooltips that showed raw UTC strings, and fixes build-date labels that were off-by-one near local midnight.

### Schedule settings UI

Settings → Schedule now surfaces a human-readable **effective schedule** (e.g. "Every 4 hours" or the cron expression), the **exact absolute config file** the scheduled run uses, a friendly **backend** tag (systemd timer / cron / Windows Scheduled Task), and an **approximate next run**. The overlap-prevention (file-lock) toggle is shown disabled with a hint when the systemd backend is selected, since systemd gates overlapping activations natively. `GET /api/v1/schedule/health` now also returns `config`, `every`, and `cron`.

### Download integrity: `v2-bootstrap` now verifies checksums

`update` already verified every downloaded asset against the release `checksums.txt` (single-binary, stack-archive, and `--binary-url` paths). `v2-bootstrap` did **not** — it downloaded and extracted the `ncc-v2-stack-*` archive (and, in the legacy fallback, the api/ui binaries + frontend archive) without authentication.

v2.1.0 closes that gap: `v2-bootstrap` now verifies each downloaded asset against the release `checksums.txt` before extracting or installing, hard-failing on a missing checksum manifest or a hash mismatch. This makes the whole trust chain shipped in v2.0.2 (`checksums.txt`, `release-attestation.json`, SBOMs, SLSA provenance) actually enforced at install time.

Pinned by `TestVerifyDownloadedAsset` and the existing `TestVerifyAssetAgainstReleaseChecksum_TamperDetection`.

### `--skip-checksum-verify` escape hatch

Both `update` and `v2-bootstrap` accept `--skip-checksum-verify` for air-gapped or internally-mirrored installs where the release `checksums.txt` is unavailable. It prints an explicit, support-friendly warning so it is obvious when integrity was not checked. The default remains hard-fail.

### Windows self-update is now one command

On Windows you cannot overwrite a running `.exe` in place, so `update` previously dropped `ncc-orchestrator.new.exe` and told you to swap it by hand. v2.1.0 instead writes an `apply-ncc-update.cmd` next to the binary that:

1. waits for the running process to release the lock,
2. swaps the new binary over the old one, and
3. deletes itself.

Run it after exiting and the update completes with no manual file juggling. The helper is also added to the `uninstall` cleanup set. Pinned by `TestWriteWindowsUpdateSwapHelper`.

### Notification delivery metrics

Notification failures used to be visible only in the logs. v2.1.0 records per-channel outcomes for each run and, when `prom-enabled` is set, writes a run-level `notifications.prom` textfile:

```
nutanix_ncc_notification_attempts_total{channel="email"}  3
nutanix_ncc_notification_failures_total{channel="email"}  1
nutanix_ncc_notification_attempts_total{channel="webhook"} 3
nutanix_ncc_notification_failures_total{channel="webhook"} 0
nutanix_ncc_notification_attempts_total{channel="slack"}   0
nutanix_ncc_notification_failures_total{channel="slack"}   0
```

A line is always emitted per channel (0 when unused), so an alert like `increase(nutanix_ncc_notification_failures_total[1h]) > 0` never breaks on a missing series. Disabled channels are not counted.

### Custom notification templates

Three optional config keys let you override the notification content with Go `text/template` strings:

```yaml
email-subject-template: "NCC {{.Cluster}}: {{.FailCount}} FAIL / {{.WarnCount}} WARN"
email-body-template: "{{.Overview}}\nStarted: {{.StartedAt}}"
webhook-template: '{"text":"NCC {{.Cluster}} FAIL={{.FailCount}}"}'
```

Templates render against the run summary (`.Cluster`, `.FailCount`, `.WarnCount`, `.ErrCount`, `.InfoCount`, `.TotalChecks`, `.Overview`, `.StartedAt`, `.FinishedAt`, `.OutputFiles`) and apply to the per-cluster, digest, and replay paths. Leave a key empty for the built-in default. A broken template logs and falls back to the default (it never drops a notification); the webhook body is sent verbatim, so it must render valid JSON.

### NCC run metrics over `/metrics` (scrape instead of a textfile collector)

The Prometheus rendering was split out of the textfile writers, and the api-server now reads the latest `run-summary.json` and serves the last run's per-cluster and run-level metrics on its existing `/metrics` endpoint:

```
ncc_cluster_up{cluster="10.0.0.1"} 1
ncc_cluster_checks_total{cluster="10.0.0.1",severity="FAIL"} 0
ncc_cluster_health_score{cluster="10.0.0.1"} 95
ncc_last_run_clusters_failed 1
ncc_last_run_exit_code 2
```

Point Prometheus straight at the api-server (`--metrics-public` for token-free scraping on a private network) instead of running a node_exporter textfile collector over `<cluster>.prom`. The on-disk `.prom` textfiles still work.

### Minimal, opt-in RBAC for settings

Most endpoints are read-only views; the sensitive surface is settings and the mutating actions. Set `NCC_API_VIEWER_TOKEN` to hand out a **read-only** token: viewers can read non-settings `GET` endpoints but get `403` on `/api/v1/settings/*` and on any state-changing request. The full `NCC_API_TOKEN` (or a session) stays admin. Leave the viewer token unset and nothing changes. `/api/v1/health` reports `rbac_enabled`.

### Security hardening

- **`ca-bundle` / `pin-sha256`** — trust an internal Prism CA bundle, or pin allowed server-cert SHA-256 fingerprints, as safer alternatives to `insecure-skip-verify` (pinning still rejects a MITM cert).
- **`smtp-insecure-skip-verify`** — control SMTP STARTTLS verification independently of the Prism flag.
- **`webhook-secret`** — sign the webhook body with HMAC-SHA256 (`X-NCC-Signature: sha256=…`) so receivers can verify provenance.
- **`notification-deadletter-dir`** — persist notification payloads that fail to deliver after retries, so a transient outage doesn't silently lose the alert.
- **Self-heal supervisor** now passes the service name through a shell-quoted variable, closing a script break-out vector.
- **User-store master key scrubbed from child processes.** Orchestrator children spawned by the api-server (runs, backup/restore, restart, self-heal `doctor`) are launched with `NCC_MASTER_KEY` removed from their environment — they copy the sealed user DB as opaque bytes and never need the key, which previously leaked via `/proc/<pid>/environ` and child inheritance. Key-file sources are unaffected.
- A **fuzz test** (`FuzzRedactJSONPasswordValue`) found and we fixed a panic in the `log-http` password redactor.

### Opt-in OpenTelemetry tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` (or `NCC_OTEL_ENABLED=1`) to emit a span per cluster run over OTLP/HTTP. With no endpoint configured, tracing is a no-op.

### Dependency refresh & CI gates

- Go modules updated (`modelcontextprotocol/go-sdk` 1.6.0→1.6.1, `golang.org/x/sys` 0.44→0.45, `mattn/go-colorable`, `mattn/go-runewidth`); OpenTelemetry SDK + OTLP exporter added. The Go directive moved `1.26.3 → 1.26.4` to pick up stdlib fixes (`GO-2026-5039`, `GO-2026-5037`). `go vet`, the `-race` suite, and `govulncheck` are clean.
- New `.github/workflows/ci.yml` enforces build / vet / gofmt / `-race` tests, `golangci-lint`, `govulncheck`, and a Trivy filesystem scan on every push and PR.
- Frontend `npm audit`: 0 vulnerabilities.

---

## `goNCC.go` package extraction

The first wave of splitting the ~15.5k-line `goNCC.go` into focused `internal/` leaf packages landed in this release. Six packages were carved out, each re-exported from `main` via type/function aliases so the thousands of existing references and call sites compile unchanged:

- **`internal/model`** — foundational shared types (`Config`, `ClusterCredential`, `NotificationSummary`, `ParsedBlock`, `FS`, `HTTPClient`, `OSFS`) and `ClusterHealthScore`.
- **`internal/promtext`** — Prometheus textfile writers (`WritePrometheusFile`, `WriteNotificationMetricsFile`, `SanitizeLabel`).
- **`internal/retryutil`** — the shared retry/backoff helpers (`JitteredBackoff`, `IsRetryableStatus`, `RetryAfterDelay`). Extracted first as a stdlib-only leaf so both `main` and `internal/notify` reuse them without an import cycle.
- **`internal/notify`** — the email / webhook / Slack senders, retry wrappers, `text/template` overrides, and the per-channel delivery-metrics accumulator (run-level counters are read back via `notify.ResetMetrics` / `notify.SnapshotMetrics`).
- **`internal/nccparse`** — the NCC summary parser (`SplitLines`, `ParseSummary`, `ValidateParsedAlertsAgainstPluginResults`) producing `model.ParsedBlock`.
- **`internal/httpclient`** — the `*http.Client` builder (`New`, aliased as `NewHTTPClient`): connection pooling, TLS policy, and the optional `LoggingTransport` that redacts `Authorization` / `Cookie` headers and JSON password fields from debug dumps. The os-backed `OSFS` moved next to the `FS` interface in `internal/model`.

Behavior is identical — the orchestrator's existing test suite is unchanged and passes under `-race` — and each new package ships its own unit tests (the notification, template, parser, and HTTP-redaction tests were relocated/added alongside their implementations). Further slimming of `goNCC.go` (the report renderers) can follow the same alias-backed, behavior-preserving pattern; sequencing lives in [`IMPROVEMENTS.md`](IMPROVEMENTS.md).

---

## Bug fixes

- **Alerts table could lose data after a failed or merged concurrent run.** A cluster whose NCC run failed outright (network/auth/timeout) used to vanish from the Alerts table with no indication anything went wrong; it now shows a visible `FAIL` **"NCC run failed"** row with the real error as its detail. Separately, the code that reads/patches the `AGG` JSON embedded in `index.html` — including on every concurrent/scoped-run merge — used a regex that stopped at the *first* semicolon it found, which is common inside real NCC check details (e.g. "Description: X; Recommendation: Y"). That could silently truncate the report on read and, worse, **permanently corrupt the canonical report on merge** by splicing new data together with a dangling fragment of the old. It's replaced with a proper JSON-aware scanner. Finally, the startup permission probe that verifies the report directory is writable could leave a 0-byte `index.html` stub behind when a run aborted early (e.g. cluster discovery failing outright); the artifact-merge step used to read that stub as "this run legitimately found zero alerts" and wipe the entire canonical report. The probe no longer touches real report content, and merges now treat an empty artifact as "nothing to merge" rather than "replace with nothing."
- **Dashboard hero counts could disagree with the Alerts table for RBAC-restricted users.** The FAIL/ERR/WARN/INFO hero cards were computed before cluster-group access filtering was applied, so a viewer confined to a subset of clusters could see hero totals that didn't match what they could actually see in the table below. Both now derive from the same filtered, single source of truth.
- **Self-heal false positives from an unanchored install path and a process-identity mix-up.** The periodic self-heal timer now anchors its `doctor` checks to the api-server's actual install directory (`--install-dir`), and the runtime-drift detector's process-identity check now compares against the real binary name instead of the raw search pattern, closing a path where a legitimately-running process could be misclassified as drifted.
- **Restoring a backup taken on a different install directory left the dashboard permanently "Stale" even with a healthy scheduler.** `config.yaml`'s output-dir paths are absolute and restore round-trips the file byte-for-byte, so restoring a backup from one install path onto another left scheduled runs silently succeeding and writing fresh reports to the *old* directory — one the running dashboard never reads. The post-restore self-heal now also re-anchors `output-dir-filtered`/`output-dir-logs`/`run-history-dir` to the current install directory (reported in the restore response's `self_heal_notes`). **If you already hit this**, see [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md#dashboard-shows-stale--nd-ago-even-though-the-scheduler-log-shows-healthy-runs) for the manual fix (no reinstall required).
- **`v2-install-service` failed on SELinux-enforcing hosts (RHEL/Rocky/CentOS/Fedora) with `systemctl` reporting `status=203/EXEC`.** A binary under a home directory is labeled `admin_home_t` by SELinux policy default, and systemd execs units in the `init_t` domain, which is denied from executing it — a denial that's silently `dontaudited` on a targeted policy, so nothing shows up in `ausearch -m avc` even though the exec is blocked. `restorecon` (the previous best-effort fix) only resets a file to its policy-*default* context, which for a home dir is `admin_home_t`, not `bin_t` — so it never actually fixed this. `v2-install-service` now forces `bin_t` on the whole `bin/` directory with `chcon` and persists the rule with `semanage fcontext`. **If you already hit this on an existing install**, see [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md#v2-install-service--systemd-fails-with-status203exec-on-selinux-hosts) for the one-line workaround — no reinstall required.

---

## Known issues (fixed in v2.1.1)

### Known issue: "NCC run failed" rows tagged `FAIL` instead of `UNKNOWN`

**Issue:** When a cluster's NCC run fails outright (connection refused, DNS failure, auth rejection, timeout, etc.), v2.1.0 synthesizes a visible `"NCC run failed"` row in the Alerts table so the cluster doesn't silently vanish from the report (see Bug fixes above). That row is tagged **`FAIL`** severity — the same severity used for a genuine failing NCC check.

**Impact:** Cosmetic/classification only — **no data is lost, hidden, or corrupted**. However:
- The row is indistinguishable from a real NCC finding, so "the cluster is unreachable" reads the same as "NCC found an actual problem on this cluster."
- FAIL totals (Alerts table hero pill, dashboard hero cards) and the drilldown diff's `new_failures` / `resolved_failures` regression counters are inflated by connectivity/auth/timeout noise that has nothing to do with the cluster's actual NCC health.
- The row's Detail carries only the raw error, with no specific remediation guidance.
- Separately (unrelated root cause, same area of the UI): the Alerts table's severity priority ranks `ERR` ahead of `WARN` (`FAIL > ERR > WARN > INFO`) in the default sort and the dashboard's hero pills/filter-chip order, which does not match the intended priority of `WARN` ahead of `ERR`.

**Workaround (no upgrade required):** Treat any `"NCC run failed"` row as informational rather than a real check failure when reviewing FAIL counts or regression deltas — cross-reference the cluster's `run-summary.json` entry (`ok: false`, `error`, `error_class`) or the Runs table, which already correctly reports these as connectivity/run failures rather than fabricated FAIL counts.

**Permanent fix:** Shipped as **v2.1.1** — the synthetic `"NCC run failed"` row is now tagged **`UNKNOWN`** severity (a KB-less, informational classification already recognized by the Alerts table and dashboard), its Detail carries the real error plus an actionable, error-class-specific remediation hint (e.g. "reduce --max-parallel, increase --timeout" for a network/timeout failure), and the KB column is intentionally left empty (there's no real NCC KB article for "couldn't reach the cluster"). Severity priority is also corrected app-wide so `WARN` outranks `ERR`. See [`RELEASE_NOTES_v2.1.1.md`](RELEASE_NOTES_v2.1.1.md).

---

## Upgrade

From any v2.0.x install:

```bash
./ncc-orchestrator update
```

Or, on a running v2 stack, update **from the UI**: **Settings → Access → Software updates → Check for updates**, then **Back up & update** — the server takes a pre-update backup, installs the checksum-verified package (orchestrator + api + ui + frontend), and restarts the stack automatically; the page reconnects on its own.

On Windows, after the download completes, exit the program and run the generated `apply-ncc-update.cmd`. On macOS/Linux the running binary is replaced atomically in place.

Both `update` and `v2-bootstrap` now verify downloads against the release `checksums.txt` automatically; pass `--skip-checksum-verify` only for air-gapped mirrors.

**Login is now enabled by default for the bundled stack / containers.** On first start the api-server creates an `admin` account with a random password; retrieve it and change it on first login:

```bash
# Docker Compose (file store):
docker compose logs ncc-api-server | grep -A4 "FIRST-RUN ADMIN"

# Kubernetes (Secret store; apply k8s/rbac.yaml first):
kubectl -n ncc-orchestrator-v2 get secret ncc-v2-users \
  -o jsonpath='{.data.initial-admin-password}' | base64 -d
```

Headless/automation users are unaffected: static `NCC_API_TOKEN` keeps full admin access. To run without interactive login, omit `--users-db` / `--users-db-secret`.

**Coming from v2.0.2 specifically** (the last release before RBAC and backup/restore existed at all): see [`docs/MIGRATION_v2.0.2_TO_v2.1.0.md`](docs/MIGRATION_v2.0.2_TO_v2.1.0.md) for a full behavior-change rundown and what was verified for this upgrade path.

---

## Tests

- The bug-fix pass above adds `TestBuildClusterChecksSnapshotFromResultFailure`, `TestReadInlineJSONVarSemicolonInValue`, `TestReplaceInlineJSONVarSemicolonInOldValue`, `TestMergeIndexHTMLEmptyPerRunFileDoesNotWipeCanonical`, `TestMergeClusterArrayArtifactEmptyPerRunFileDoesNotWipeCanonical`, and new `TestCheckOutputPermissions` subtests, and was additionally verified by simulating a v2.0.2 → v2.1.0 upgrade end-to-end (old-format `config.yaml`, no pre-existing user database or backup-schedule state) — see [`docs/MIGRATION_v2.0.2_TO_v2.1.0.md`](docs/MIGRATION_v2.0.2_TO_v2.1.0.md).
- Full Go suite passes with `-race -count=1`, including the authentication/RBAC suite (`TestRouteMinRole`, `TestParseRole`, `TestUserStoreVerify`, `TestHandleLoginAndSessionRole`, `TestWithAuthCookieSessionRBACAndCSRF`, `TestStaticAdminTokenExemptFromCSRF`, `TestBootstrapAdminAndPersistence`, `TestForcedPasswordChangeFlow`, `TestUserCRUDAndLastAdminProtection`, `TestSSOConfigPersistAndCertGeneration`, `TestK8sSecretBackendRoundTrip`, and the end-to-end `TestEndToEndFirstRunAdminFlow`), the new `TestVerifyDownloadedAsset` and `TestWriteWindowsUpdateSwapHelper`, the viewer-token RBAC test (`TestWithAuthRBACViewer`), the mock-Prism integration tests (`TestIntegration_DiscoverClustersV4_MockPC`, `TestIntegration_DiscoverClustersV3_MockPC`, `TestIntegration_TaskPoll_MockPrism`), the security/observability unit tests (`TestSignWebhookBody`, `TestWriteDeadLetter`, `TestNormalizePin`, `TestPinVerifier`, `TestRenderRunSummaryMetrics`), and the two fuzz targets with checked-in corpora (`FuzzParseSummary`, `FuzzRedactJSONPasswordValue`), plus the `internal/` package suites (`internal/model`, `internal/promtext`, `internal/retryutil`, `internal/notify`, `internal/nccparse`, `internal/httpclient`, `internal/trace`). The autonomic-operations work adds the supervisor/boot-service suite (`TestSuperviseRestartsAndStops`, `TestSuperviseWaitTokenGatesStart`, `TestSuperviseRefusesSecondInstance`, `TestSystemdSupervisorUnit`), the systemd-scheduler suite (`TestCronToOnCalendar`, `TestOnCalendarFromSchedule`, `TestSanitizeSystemdName`, `TestBuildSystemdScheduleUnits`, `TestNormalizeScheduleTypeSystemd`), the self-heal/`doctor --fix` suite (`TestRunSelfHealFixesRoutingAndDirs`, `TestCheckSecretsPermsChmod`, `TestVerifyBackupArchiveAndRetention`, `TestClassifyRunOutput`, `TestMitigationArgsForClass`, `TestDecideRunHeal`, `TestCheckStalePIDs`, `TestCheckLogSizes`, `TestCertExpiryStatus`, `TestResolveUserStoreBackendFailsFastOnEncryptedWithoutKey`), and the user-store envelope-encryption suite (`TestSealOpenRoundTrip`, `TestOpenWithWrongKeyFails`, `TestNoncesAreUnique`, `TestDecodeMasterKeyFormats`, `TestEncryptingBackendMigratesPlaintext`).
- `go vet`, `gofmt -l`, `govulncheck`, and frontend `npm audit` all clean. CI now enforces these plus `golangci-lint` and a Trivy scan on every push and PR.

---

## Acknowledgements

This release was driven by post-release verification of v2.0.2 on Windows, which surfaced the executability bug fixed in v2.0.2 and prompted the self-update and download-integrity hardening shipped here.
