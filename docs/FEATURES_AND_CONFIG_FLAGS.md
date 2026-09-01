# Features and Config Flags (v2.1.1)

Comprehensive reference for NCC Orchestrator features, configuration keys, and CLI flags.

> Scope: covers the runtime configuration of the `ncc-orchestrator` CLI. For the v2 API/UI servers and React frontend, see [`../README.md`](../README.md) and [`../docs/ARCHITECTURE_AND_HANDOVER.md`](./ARCHITECTURE_AND_HANDOVER.md). For a ready-to-edit YAML template, see [`../example_config.yaml`](../example_config.yaml).

## 1) What this tool does

`ncc-orchestrator` runs Nutanix NCC checks across one or more clusters, collects and parses results, and generates automation-friendly artifacts plus human-readable reports.

## 2) Feature reference with examples

### 2.1 Multi-cluster parallel execution

Run checks across multiple clusters with bounded concurrency:

```bash
ncc-orchestrator --clusters "10.38.66.37,10.38.66.7" --username admin --password "$NCC_PASSWORD" --max-parallel 4
```

### 2.2 Cluster list from file (with optional per-cluster credentials)

`clusters-file` supports:
- `cluster`
- `cluster,username`
- `cluster,username,password`

Example `clusters.txt`:

```text
# cluster[,username[,password]]
10.38.66.37
10.38.66.7,admin
pc-aos01.example.local,svc-user,secret://pc_aos01_password
```

Run:

```bash
ncc-orchestrator --clusters-file clusters.txt --username admin --password "$NCC_PASSWORD"
```

### 2.3 Prism Central cluster discovery

Discover clusters using Prism Central (v4 default, v3 optional):

```bash
ncc-orchestrator discover-clusters \
  --prism-central-url https://pc:9440 \
  --username admin \
  --password "$NCC_PASSWORD" \
  --format table
```

Write to file for later runs:

```bash
ncc-orchestrator discover-clusters --prism-central-url https://pc:9440 --output clusters.txt
```

### 2.4 Replay mode

Regenerate reports/artifacts from existing logs without invoking NCC APIs:

```bash
ncc-orchestrator --config config.yaml --replay
```

### 2.5 Report outputs

Generate per-cluster files in one or more formats:
- `html`
- `csv`
- `json`
- `markdown`
- `sarif`

```bash
ncc-orchestrator --outputs html,csv,json,markdown,sarif
```

### 2.6 Aggregated dashboard and run artifacts

Outputs under `output-dir-filtered` include:
- `index.html` (aggregated dashboard)
- `run-summary.json`
- `ncc-run-record.json`
- `checks-snapshot.json`
- `drilldown-diff.json`
- `flaky-checks.json`
- `regression-summary.json`
- `slo-dashboard.json`
- `policy-gates.txt` (only on violations)

### 2.7 Policy gates (CI/CD enforcement)

Fail a run if thresholds are violated:

```bash
ncc-orchestrator --policy-gates "new-fails>0,fail-rate>2,min-health-score<90"
```

### 2.8 Flaky-check detection

Detect checks that change severity repeatedly across recent runs:

```bash
ncc-orchestrator --flaky-lookback-runs 10 --flaky-min-transitions 3
```

### 2.9 Per-cluster health score and trend

Health scoring and trend visualization are generated in run artifacts and dashboard output automatically.

### 2.10 Quiet hours and maintenance windows

Suppress notifications for planned windows:

```bash
ncc-orchestrator \
  --quiet-hours "22:00-06:00" \
  --maintenance-windows "2026-04-22T20:00:00Z/2026-04-22T23:00:00Z"
```

### 2.11 Notifications (email/webhook/slack)

Enable and configure one or more channels:

```bash
ncc-orchestrator \
  --webhook-enabled --webhook-url "https://hooks.example.com/ncc" \
  --email-enabled --smtp-server smtp.example.com --smtp-user ncc@example.com \
  --slack-enabled --slack-webhook-url "https://hooks.slack.com/services/..."
```

Digest mode sends one notification per run:

```bash
ncc-orchestrator --notify-digest
```

**Custom templates.** `email-subject-template`, `email-body-template`, and
`webhook-template` accept Go `text/template` strings rendered against the run
summary (`.Cluster`, `.FailCount`, `.WarnCount`, `.ErrCount`, `.InfoCount`,
`.TotalChecks`, `.Overview`, `.StartedAt`, `.FinishedAt`, `.OutputFiles`). Empty
= built-in defaults; a broken template logs and falls back to the default
(never drops a notification).

**Webhook HMAC signing.** Set `webhook-secret` (config/env only — never a CLI
flag) to sign the webhook body with HMAC-SHA256. The signature is sent as
`X-NCC-Signature: sha256=<hex>` so the receiver can verify it came from this
orchestrator:

```yaml
webhook-secret: "secret://WEBHOOK_HMAC"
```

**Dead-lettering.** When `--notification-deadletter-dir` is set, any
email/webhook/Slack payload that still fails after all retries is written there
as a JSON record (channel, cluster, error, payload), so a transient SMTP/webhook
outage does not silently lose an alert:

```bash
ncc-orchestrator --webhook-enabled --webhook-url "https://hooks.example.com/ncc" \
  --notification-deadletter-dir /var/lib/ncc/deadletter
```

### 2.11a TLS trust: custom CA bundle and certificate pinning

Instead of disabling verification with `--insecure-skip-verify`, trust an
internal Prism CA or pin the exact server certificate:

```bash
# Trust an internal CA (verified against system roots + this bundle)
ncc-orchestrator --ca-bundle /etc/ncc/prism-ca.pem ...

# Pin the server certificate by SHA-256 fingerprint (colons optional)
ncc-orchestrator --pin-sha256 "AA:BB:CC:...,11:22:33:..." ...
```

Pinning skips chain validation but accepts **only** the listed leaf
fingerprint(s), so a man-in-the-middle presenting a different certificate is
still rejected — unlike `--insecure-skip-verify`, which accepts any cert.
`--smtp-insecure-skip-verify` separately controls SMTP STARTTLS verification so
a self-signed mail relay does not force you to disable Prism verification too.

### 2.11b Run metrics over HTTP (api-server `/metrics`)

When the api-server is running, Prometheus can scrape the last run's per-cluster
and run-level metrics directly from its `/metrics` endpoint (built from the
latest `run-summary.json`), instead of pointing a node_exporter textfile
collector at the `<cluster>.prom` files:

```
ncc_cluster_up{cluster="10.0.0.1"} 1
ncc_cluster_checks_total{cluster="10.0.0.1",severity="FAIL"} 0
ncc_cluster_health_score{cluster="10.0.0.1"} 95
ncc_last_run_clusters_failed 1
ncc_last_run_exit_code 2
```

Pass `--metrics-public` to the api-server for token-free scraping on a private
network. The on-disk `.prom` textfiles (`--prom-enabled`) still work unchanged.

### 2.11c RBAC, login, and SSO (admin / operator / viewer)

The api-server enforces three ordered roles `viewer < operator < admin`:

- **viewer** — read non-settings `GET` endpoints only (plus reading the run schedule).
- **operator** — viewer plus the day-to-day *operation* of NCC: trigger/cancel/preflight
  runs, create/update/apply the run **schedule** (`PUT /api/v1/schedule`), send
  **test notifications** (`POST /api/v1/settings/notifications/test`; delivery
  errors are URL-redacted so secrets can't leak), and read **cluster topology**
  (`GET /api/v1/settings/clusters`, `GET /api/v1/settings/cluster-groups`).
  Operators reach a reduced Settings view in the UI (Connection, Schedule, Runs,
  Logs, Audit).
- **admin** — everything, including secret-bearing `/api/v1/settings/*` (config,
  users, SSO/LDAP, backups, cluster-group writes) and token rotation.

A caller's role can come from a static token or an interactive login:

- `NCC_API_TOKEN` → admin (automation/CI).
- `NCC_API_VIEWER_TOKEN` → viewer (dashboards/scrapers); must differ from the
  admin token.
- **First-run admin (zero-config, on by default)**: with a writable user
  database an empty store provisions an `admin` account with a random password
  on first launch (printed to the log; also persisted for retrieval). The admin
  must change the password on first login (`POST /api/v1/auth/change-password`),
  then can manage everything else from the UI. **The user database is enabled by
  default**: when no backend is configured by a flag, env, or stack path, the
  server falls back to `<repo-root>/.ncc-api-users.json`, so even a bare run
  bootstraps login. Opt out with `--disable-local-accounts`
  (`$NCC_DISABLE_LOCAL_ACCOUNTS=1`) for pure token-only automation. Two storage
  backends:
  - **File**: `--users-db` (`$NCC_USERS_DB`), a `0600` JSON file (default
    `<root>/.ncc-api-users.json`; used by a v2 stack, Docker Compose, and the
    default-on fallback). Password also written to a `0600`
    `.ncc-initial-admin-password` file.
  - **Kubernetes Secret**: `--users-db-secret <name>` (`$NCC_USERS_DB_SECRET`),
    read/created/patched in-cluster via the pod's service-account token
    (`--users-db-secret-key`, default `users.json`; `--users-db-secret-namespace`
    defaults to the pod namespace). The first-run password is stored under the
    Secret's `initial-admin-password` key. **Secrets are only base64-encoded
    unless you enable etcd encryption-at-rest (KMS/secretbox/aescbc)** — see
    `k8s/encryption-config.example.yaml` and `k8s/rbac.yaml` (least-privilege).
    Mutually exclusive with `--users-db`.
  - **Encryption at rest (file store, optional)**: supply a 32-byte master key
    via `NCC_MASTER_KEY` (base64/hex) or `--users-db-key-file` /
    `NCC_MASTER_KEY_FILE` to envelope-encrypt the file store with AES-256-GCM
    (protects the SAML SP key and LDAP bind password on disk and in backups).
    Unset → plaintext (default, backward compatible); enabling it migrates a
    plaintext store on the next write. Keep the key off the protected
    disk/backup. See `docs/SECURITY_AND_TRUST.md`.
- **Local accounts** (managed at runtime in Settings → Access, or via
  `/api/v1/settings/users`): create/list/role-assign/reset-password/delete
  bcrypt accounts; the last admin is protected from removal/demotion. Browsers
  sign in via `POST /api/v1/auth/login` (role-bearing httpOnly session cookie);
  `POST /api/v1/auth/logout` clears it; `GET /api/v1/auth/me` reports role and
  must-change state. `--users-file` (`$NCC_USERS_FILE`) is an optional one-time
  YAML seed (`{username, password_hash (bcrypt), role}`) imported into the
  database when empty; generate hashes with `ncc-api-server --hash-password`.
- **Personal access tokens** (self-service, any role) — a signed-in user mints
  their own bearer token from the header user menu → *Personal access tokens* to
  call the API outside the browser. `GET/POST /api/v1/auth/tokens` and
  `DELETE /api/v1/auth/tokens/{id}` list/create/revoke the caller's **own**
  tokens; the token inherits the owner's role, is shown **once** at creation, and
  is sent as `X-API-Token: <token>` or `Authorization: Bearer <token>` (prefixed
  `ncc_pat_`). Tokens carry an expiry (7 days–1 year, default 90, or **Never**
  for long-lived automation) and a 25-per-user cap; only a SHA-256 hash is stored. For local owners the role is
  re-resolved live each request (deleting the account or flagging a forced
  password change disables its tokens). Admins audit/revoke any user's token via
  `GET /api/v1/settings/tokens` and `DELETE /api/v1/settings/tokens/{id}`
  (Settings → Access → *Personal access tokens*). Tokens live in
  `.ncc-api-users.json`, so they persist across restarts and are backed up.
- **Session lifetime** (managed at runtime in Settings → Access, or via
  `GET/PUT /api/v1/settings/session`): admins set how long a signed-in session
  stays active (1 minute–24h), persisted in the user database and applied to
  sessions minted afterward. Send `{"ttl_min": <n>}` or `{"ttl_sec": <n>}`;
  `ttl_sec:0` reverts to the `--session-ttl` server default. `GET /api/v1/auth/me`
  reports `session_ttl_sec`, `expires_at`, and `expires_in_sec` so the UI can
  keep the session fresh and return to the login screen when it lapses.
- **SAML SSO** — either startup flags (`--saml-root-url`, `--saml-idp-metadata`,
  `--saml-cert`, `--saml-key`; read-only at runtime) **or** runtime config in
  Settings → Access (`/api/v1/settings/sso`), persisted in the user database and
  hot-reloaded, with the **SP keypair generated server-side** (publish the SP
  metadata URL `<root>/saml/metadata` to your IdP). Map IdP attribute values to
  roles with the role attribute + role map (default role). Endpoints:
  `/saml/metadata`, `/saml/login`, `/saml/acs`. Set the **root URL to the
  browser-facing UI origin** — `ncc-ui-server` proxies `/saml/*` to the api-server
  so the post-login cookie lands on the right host; the SP request-tracking cookie
  is `SameSite=None` (it survives the IdP's cross-site POST to `/saml/acs`, which
  requires HTTPS — the default), and `/saml/*` is exempt from the CORS origin
  allowlist.
- **LDAP / Active Directory** — users sign in on the normal username/password
  form with their AD credentials. Login is **local-first, then AD fallback** (so
  the built-in `admin` and break-glass local accounts work even if AD is down),
  using a **service-account bind + search + rebind**; an empty password is
  rejected before any bind. Configure via startup flags (`--ldap-url`
  [comma-separated for failover], `--ldap-base-dn`, `--ldap-bind-dn`,
  `--ldap-bind-password`, `--ldap-user-filter`, `--ldap-username-attribute`,
  `--ldap-group-attribute`, `--ldap-role-map`, `--ldap-default-role`,
  `--ldap-start-tls`, `--ldap-ca-file`, `--ldap-insecure-skip-verify`; read-only
  at runtime) **or** runtime config in Settings → Access
  (`GET/PUT /api/v1/settings/ldap`, with a **Test connection** check via
  `POST /api/v1/settings/ldap/test`), hot-reloaded without a restart. Map AD
  groups to roles by group DN or CN (case-insensitive, highest match wins;
  newline/semicolon-separated, e.g.
  `CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin`). The bind password
  is write-only (never returned by GET). SAML and LDAP can both be enabled at the
  same time.
- **Password recovery** — `ncc-orchestrator v2-reset-password [--user <name>]`
  (wrapping `ncc-api-server --reset-password <name>` / `--reset-admin`) resets a
  lost local password offline against either store backend, forcing a change at
  next login. End users can request a reset from the login page's **Forgot
  password?** link (public `POST /api/v1/auth/forgot-password`, always a generic
  200, no account enumeration); admins resolve the queue from Settings → Access
  (`GET`/`DELETE /api/v1/settings/password-resets[/<name>]`). **Admin lockout
  self-recovery:** if the forgot-password username is the built-in `admin`, the
  server skips the queue and self-resets it exactly like first-run setup — a
  fresh random password is generated, a change is forced at next login, existing
  admin sessions are invalidated, and the new password is surfaced only through
  the server logs and the `.ncc-initial-admin-password` file (never returned over
  the network), so a locked-out operator can recover even when no other admin
  exists. This matches the offline `v2-reset-password --user admin` outcome but
  is reachable from the login screen. A short per-IP cooldown (60s, returning
  `429 NCC_API_RATE_LIMITED`) blunts repeated force-rotation. The authenticated
  **Settings → Access → Reset password** dialog mirrors the same behaviour for
  the `admin` row — it offers **Generate & reset** (random password, shown once
  and copyable) instead of asking the admin to type one (`PUT
  /api/v1/settings/users/admin` with `{"generate_password": true}`).
- **Cluster groups (membership-based access control)** — on top of the role
  hierarchy, admins can segregate clusters into named groups (Settings → Access →
  *Cluster groups*, or `GET/PUT /api/v1/settings/cluster-groups`; a cluster may be
  in several groups). Membership is the union of **local accounts** (by username),
  **AD groups** (by CN or full DN, matched against the `memberOf` values
  captured in the session at login), and **individual AD users** (matched on the
  caller's `sAMAccountName`/UPN local part). The UI assigns AD principals with
  **live directory type-ahead** (admin-only `GET /api/v1/settings/ldap/search?q=<term>&type=group|user`,
  a service-account substring search; manual entry still works offline). A group
  may also list **Prism Centrals** — every cluster registered under a listed PC is
  folded into the group automatically (discovered via the orchestrator's
  `discover-clusters` using the active run config's credentials, cached with a
  ~10 min background refresh; preview/refresh via admin-only
  `GET /api/v1/settings/pc-clusters?pc=<url>`). Cluster groups are **opt-in
  isolation**: a non-admin in **no** group is **unrestricted** (a plain viewer
  sees every cluster's alerts out of the box), while membership in **one or more**
  groups confines the caller to the union of those groups' clusters. For scoped
  members, run triggers are pinned via `--clusters` (members may narrow to a
  subset; foreign requests are dropped), and `/api/v1/report/data` and the runs
  feed are filtered server-side to allowed clusters; scoping keys off **group
  membership**, not the resolved cluster count. **Raw multi-cluster artifacts
  (`/api/v1/artifacts*`) are restricted to unrestricted callers** (they embed
  every cluster); scoped members use the filtered dashboard. Admins and static
  tokens are unrestricted. `GET /api/v1/auth/me` reports `cluster_access_unrestricted` and
  `allowed_clusters`; `GET /api/v1/settings/clusters` enumerates clusters from the
  active config for assignment. Groups live in `.ncc-api-users.json`, so they
  persist across restarts and are covered by backup/restore.
- **UI HTTPS / TLS** — `ncc-ui-server` serves **HTTPS by default**. With no cert
  supplied, `v2-start` generates a **self-signed** cert (ECDSA P-256; SANs for the
  listen host + `localhost`/loopback; stored under `<install-dir>/tls/`) and
  308-redirects plain HTTP to HTTPS on the **same port** (a first-byte peek
  demultiplexes TLS from HTTP). `--ui-insecure-http` opts out (persisted in
  `.ncc-v2-start.json`). Admins manage the cert from Settings → Access → *HTTPS /
  TLS*: `POST /api/v1/settings/tls/generate` mints/renews a self-signed cert for
  the request host and restarts the stack; `PUT /api/v1/settings/tls` installs a
  PEM **cert + private key** (internal PKI or public CA), restarts, and marks
  session cookies `Secure`; `DELETE /api/v1/settings/tls` reverts. The private key
  is stored `0600` and never returned; cert metadata (subject/issuer/validity/SANs)
  is recorded for display. HTTPS is what makes the SAML `SameSite=None` cookie
  valid, so SSO works out of the box on the default self-signed HTTPS.
- **Backup / restore** — `v2-backup` / `v2-restore` (Settings → Access in the UI,
  or the CLI) capture and recover all stateful auth data (accounts, roles,
  SAML/LDAP config, cluster groups, token, session policy) plus config and audit
  log. Restore **preserves host-specific networking/TLS** (CORS origins,
  advertise/backend URLs, listen addresses, `--ui-insecure-http`, UI TLS paths) so
  importing a backup from another host doesn't trigger an `origin not allowed`
  lockout or a stale cert path. See §6.14a.
- **In-app software updates** — admins can check for and apply a new release from
  **Settings → Access → Software updates**. `GET /api/v1/settings/update` reports
  the current/latest version and `update_available` (networked check only with
  `?check=1`; plain GET is a cheap status poll). `POST /api/v1/settings/update/apply`
  runs a background job that takes a **pre-update backup** (to
  `<install-dir>/backups/`, aborting if it fails), applies the checksum-verified
  package update (orchestrator + api + ui + frontend), then **restarts the stack
  automatically** (`v2-restart`); the UI polls the phase and reconnects when the
  new version is live. Optional `target_version` / `skip_checksum_verify`. Requires
  a built orchestrator binary (not the dev `go run` fallback).

Mutating cookie-session requests require a double-submit CSRF token
(`X-CSRF-Token` header echoing the readable `ncc_csrf` cookie); static-token
automation is exempt. When local accounts, SAML, or LDAP are configured,
`auth-mode` auto-upgrades to `hybrid`, and the `ncc-ui-server` forwards each
user's session cookie instead of injecting the shared admin token
(`--login-mode auto|on|off`). `/api/v1/health` reports `rbac_enabled`,
`login_enabled`, `local_login`, `saml_enabled`, and `ldap_enabled`. See
`docs/SECURITY_AND_TRUST.md` for the full reference.

### 2.11d Distributed tracing (opt-in OpenTelemetry)

Set an OTLP endpoint to emit one span per cluster run over OTLP/HTTP; with no
endpoint configured tracing is a no-op:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318"   # or NCC_OTEL_ENABLED=1
ncc-orchestrator --clusters 10.0.0.1,10.0.0.2 ...
```

### 2.12 Secrets manager style references (`secret://`)

Resolve sensitive values from env or a file-backed secret map:

```bash
ncc-orchestrator --secrets-provider env --password secret://NCC_PRISM_PASSWORD
```

```bash
ncc-orchestrator --secrets-provider file --secrets-file secrets.yaml --password secret://prism_password
```

### 2.13 Scheduling helper

Create/list/remove scheduler entries:

```bash
ncc-orchestrator create-schedule --type cron --every 4h --config config.yaml --print-only
```

### 2.14 Strict config validation

Validate config files in automation pipelines:

```bash
ncc-orchestrator validate-config --config config.yaml
```

### 2.15 Secrets preflight validation

Validate `secret://` references and secret source accessibility before a run:

```bash
ncc-orchestrator validate-secrets --config config.yaml
```

### 2.16 Alert exclusion controls

Exclude selected alert titles from generated outputs/notifications:

```bash
ncc-orchestrator \
  --exclude-alert-titles "Disk health,Prism connectivity" \
  --exclude-alert-match-mode contains
```

Or load from file:

```bash
ncc-orchestrator --exclude-alert-titles-file exclude-alerts.txt
```

### 2.17 Config JSON schema generation

```bash
ncc-orchestrator config-schema --output config.schema.json
```

### 2.18 Update mode

Check/update local binary from GitHub or custom binary URLs:

```bash
ncc-orchestrator update
ncc-orchestrator update --check
ncc-orchestrator update --check --binary-url https://artifacts.example.com/ncc-orchestrator-linux-amd64 --target-version 1.2.4
ncc-orchestrator update --binary-url https://artifacts.example.com/ncc-orchestrator-linux-amd64 --binary-sha256 <sha256-hex>
ncc-orchestrator update --allow-major-upgrade
```

Notes:

- `--binary-sha256` is required for `--binary-url` install operations.
- GitHub release updates verify downloaded binaries against release checksum assets before replace.

### 2.19 Test dashboard generation (no API calls)

Generate synthetic aggregate dashboard and artifacts:

```bash
ncc-orchestrator gen-test-agg --clusters 25 --output-dir dist/test/outputfiles
```

### 2.20 Preflight check output (automation-friendly)

Run full preflight with structured JSON output:

```bash
ncc-orchestrator preflight-check --config config.yaml --format json
```

Each non-pass check includes a machine-readable `remediation_code` field so UI/automation can map failures to fix playbooks.

## 3) Configuration precedence

Highest to lowest precedence:
1. CLI flags
2. Environment variables (`NCC_*`)
3. Config file (`--config`)
4. Internal defaults

## 4) Config keys (config file + equivalent root flags)

| Key | Type | Default | Example |
|---|---|---|---|
| `cluster-source-mode` | string | `clusters` | `"pc"` |
| `clusters` | string | — | `"10.38.66.37,10.38.66.7"` |
| `clusters-file` | string | — | `"clusters.txt"` |
| `pcs` | string | — | `"10.10.10.10,10.10.10.11"` |
| `pcs-file` | string | — | `"pcs.txt"` |
| `prism-central-url` | string | — | `"https://10.10.10.10:9440"` |
| `discover-api-version` | string | `v4` | `"v3"` |
| `username` | string | `admin` | `"admin"` |
| `password` | string | — | `"secret://NCC_PRISM_PASSWORD"` |
| `ncc-api-version` | string | `v4` | `"Legacy"` |
| `nutanix-v4-api-version` | string | `v4.2` | `"v4.1"` |
| `insecure-skip-verify` | bool | `false` | `true` |
| `ca-bundle` | string | — | `"/etc/ncc/prism-ca.pem"` |
| `pin-sha256` | csv string | — | `"aa:bb:..,cc:dd:.."` |
| `timeout` | duration | `15m` | `"20m"` |
| `request-timeout` | duration | `20s` | `"30s"` |
| `poll-interval` | duration | `15s` | `"10s"` |
| `poll-jitter` | duration | `2s` | `"1s"` |
| `max-parallel` | int | `4` | `6` |
| `outputs` | csv string | `html,csv` | `"html,csv,json"` |
| `output-dir-logs` | string | `nccfiles` | `"dist/logs"` |
| `output-dir-filtered` | string | `outputfiles` | `"dist/output"` |
| `single-report` | bool | `false` | `true` |
| `log-file` | string | `logs/ncc-runner.log` | `"logs/ncc.json"` |
| `log-level` | string | `info` | `"debug"` |
| `log-http` | bool | `false` | `true` |
| `retry-max-attempts` | int | `6` | `8` |
| `retry-base-delay` | duration | `400ms` | `"500ms"` |
| `retry-max-delay` | duration | `8s` | `"12s"` |
| `retry-circuit-breaker` | int | `3` | `2` |
| `prom-enabled` | bool | `true` | `false` |
| `prom-dir` | string | `promfiles` | `"metrics"` |
| `run-history` | bool | `false` | `true` |
| `run-history-dir` | string | `<output-dir-filtered>/runs` | `"outputfiles/runs"` |
| `retain-last` | int | `0` | `20` |
| `retain-days` | int | `0` | `14` |
| `artifact-retain-days` | int | `0` | `14` |
| `artifact-retain-max-files` | int | `0` | `200` |
| `notify-on-regression` | bool | `false` | `true` |
| `adaptive-parallelism` | bool | `true` | `false` |
| `policy-gates` | csv string | — | `"new-fails>0,fail-rate>2"` |
| `quiet-hours` | string | — | `"22:00-06:00"` |
| `maintenance-windows` | csv string | — | `"start/end,start/end"` |
| `flaky-lookback-runs` | int | `6` | `10` |
| `flaky-min-transitions` | int | `2` | `3` |
| `severity-filter` | csv string | — | `"FAIL,WARN"` |
| `exclude-alert-titles` | csv string | — | `"Disk health,Prism connectivity"` |
| `exclude-alert-titles-file` | string | — | `"exclude-alerts.txt"` |
| `exclude-alert-match-mode` | string | `exact` | `"contains"` |
| `dry-run` | bool | `false` | `true` |
| `replay` | bool | `false` | `true` |
| `max-idle-conns` | int | `100` | `200` |
| `max-idle-conns-per-host` | int | `10` | `20` |
| `max-conns-per-host` | int | `0` | `50` |
| `idle-conn-timeout` | duration | `90s` | `"120s"` |
| `email-enabled` | bool | `false` | `true` |
| `email-attach-html` | bool | `false` | `true` |
| `notify-digest` | bool | `false` | `true` |
| `smtp-server` | string | — | `"smtp.gmail.com"` |
| `smtp-port` | int/string | `587` | `465` |
| `smtp-user` | string | — | `"ncc@example.com"` |
| `smtp-password` | string | — | `"secret://SMTP_PASSWORD"` |
| `email-from` | string | — | `"ncc@example.com"` |
| `email-to` | csv string | — | `"ops@example.com,sre@example.com"` |
| `email-use-tls` | bool | `true` | `false` |
| `smtp-insecure-skip-verify` | bool | `false` | `true` |
| `webhook-enabled` | bool | `false` | `true` |
| `webhook-include-html` | bool | `false` | `true` |
| `webhook-url` | string | — | `"https://hooks.example.com/ncc"` |
| `webhook-headers` | map | `{}` | `{"X-Token":"abc"}` |
| `webhook-template` | string | — | `'{"text":"NCC {{.Cluster}} FAIL={{.FailCount}}"}'` |
| `webhook-secret` | string | — | `"secret://WEBHOOK_HMAC"` |
| `notification-deadletter-dir` | string | — | `"/var/lib/ncc/deadletter"` |
| `slack-enabled` | bool | `false` | `true` |
| `slack-webhook-url` | string | — | `"https://hooks.slack.com/services/..."` |
| `slack-channel` | string | — | `"#infra-alerts"` |
| `email-subject-template` | string | — | `"NCC {{.Cluster}}: {{.FailCount}} FAIL"` |
| `email-body-template` | string | — | `"{{.Overview}}"` |
| `secrets-provider` | string | — | `"env"` or `"file"` |
| `secrets-file` | string | — | `"secrets.yaml"` |

## 5) Root command flags (full list)

Use:

```bash
ncc-orchestrator [flags]
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--adaptive-parallelism` | bool | `true`, `false` | `true` | When enabled, orchestration dynamically scales effective worker concurrency down/up based on observed HTTP 429 behavior, reducing sustained rate-limit pressure without fully stopping progress. |
| `--cluster-source-mode` | string | `clusters`, `pc` | `clusters` | Selects target source behavior. `clusters` uses direct PE entries. `pc` uses Prism Central targets (`--pcs`, `--pcs-file`, or `--prism-central-url`) and auto-discovers clusters before run. |
| `--clusters` | string | CSV of cluster IP/FQDN values | none | Primary target list when `clusters-file` is not used. Each entry is validated, duplicates are rejected, and values must be resolvable/valid cluster addresses. |
| `--clusters-file` | string | Path to text file | none | Alternate target source. Supported line formats: `cluster`, `cluster,username`, `cluster,username,password`. If provided and non-empty, it overrides/supersedes `--clusters`. |
| `--config` | string | Path to `.yaml`, `.yml`, or `.json` | none | Loads persistent config values from file before env/flag overrides are applied. Use this for production runs and scheduler jobs. |
| `--dry-run` | bool | `true`, `false` | `false` | Performs validation and prints effective setup without executing cluster checks. Use in CI preflight and change-review pipelines. |
| `--email-attach-html` | bool | `true`, `false` | `false` | Adds HTML report attachment to email notifications. Useful for operators who consume mail-only workflows, but may increase message size. |
| `--email-enabled` | bool | `true`, `false` | `false` | Enables SMTP email notifications. Requires SMTP and recipient fields to be correctly set. |
| `--email-from` | string | Valid email address | none | Sender address used in email notifications; should align with SMTP relay policy/domain requirements. |
| `--email-to` | string | Comma-separated email addresses | none | Recipient list for email notifications. Supports one or more addresses separated by commas. |
| `--email-use-tls` | bool | `true`, `false` | `true` | Enables STARTTLS for SMTP sessions. Keep enabled for production unless your SMTP endpoint explicitly requires plain mode in trusted networks. |
| `--smtp-insecure-skip-verify` | bool | `true`, `false` | `false` | Skips SMTP STARTTLS certificate verification, independent of `--insecure-skip-verify` (which only affects Prism). Use only for a trusted self-signed mail relay. |
| `--notification-deadletter-dir` | string | Writable directory path | none | When set, notification payloads that fail to deliver after retries (email/webhook/Slack) are written here as JSON (channel, cluster, error, payload) so a transient outage does not silently drop the alert. |
| `--flaky-lookback-runs` | int | Integer `>= 1` | `6` | Number of historical snapshots used for flaky-check detection. Higher values improve long-range sensitivity but may increase noise in unstable labs. |
| `--flaky-min-transitions` | int | Integer `>= 1` | `2` | Minimum severity transitions required before a check is marked flaky. Raise this to reduce false positives. |
| `--insecure-skip-verify` | bool | `true`, `false` | `false` | Disables TLS certificate verification. Use only in trusted lab/self-signed environments. Avoid in production; prefer `--ca-bundle` or `--pin-sha256`. |
| `--ca-bundle` | string | Path to PEM file | none | Adds the PEM-encoded CA certificate(s) in the file to the trust store (verified against system roots **plus** this bundle). Safer than `--insecure-skip-verify` for internal Prism CAs. |
| `--pin-sha256` | string | CSV of SHA-256 fingerprints (hex, colons optional) | none | Certificate pinning: the server cert is accepted only if its SHA-256 fingerprint matches one of these, independent of the system trust store. Rejects a MITM cert that `--insecure-skip-verify` would accept. |
| `--log-file` | string | Writable file path | `logs/ncc-runner.log` | Rotated JSON log output file. Use a persistent location for post-incident analysis. |
| `--log-http` | bool | `true`, `false` | `false` | Enables HTTP request/response logging for deep debugging. Can expose operationally sensitive payloads; keep off in normal production usage. |
| `--log-level` | string | `trace`, `debug`, `info`, `warn`, `error`, or numeric `0..5` | `info` | Controls verbosity. Use `debug`/`trace` for troubleshooting and `info` for steady-state operations. |
| `--maintenance-windows` | string | RFC3339 windows: `start/end[,start/end...]` | none | Suppresses notifications in explicit maintenance intervals. Best for planned change windows and patch operations. |
| `--max-parallel` | int | Integer `1..100` | `4` | Maximum concurrent clusters. Tune down for rate-limited APIs or constrained environments; tune up for faster completion in stable networks. |
| `--ncc-api-version` | string | `v4`, `Legacy`, `v1` (`v1` alias for Legacy) | `v4` | Selects NCC start-check API strategy. Use `v4` by default; use legacy mode for environments requiring Prism Gateway v1 start endpoints. |
| `--notify-digest` | bool | `true`, `false` | `false` | Sends one consolidated notification per run (email/webhook/slack) instead of per-cluster messages. Recommended for large estates. |
| `--notify-on-regression` | bool | `true`, `false` | `false` | Emits notifications only when FAIL count regresses compared to prior summary, reducing steady-state noise. |
| `--nutanix-v4-api-version` | string | Revision-like path token (examples: `v4.2`, `v4.1`, `v4.0.a1`) | `v4.2` | Controls v4 path segment for clustermgmt/monitoring/prism APIs. Set this to match your Prism API revision. |
| `--output-dir-filtered` | string | Writable directory path | `outputfiles` | Destination for filtered per-cluster files, aggregated dashboard, and run-level artifacts. |
| `--output-dir-logs` | string | Writable directory path | `nccfiles` | Destination for raw NCC summary logs per cluster (`<cluster>.log`). |
| `--outputs` | string | CSV subset of `html,csv,json,markdown,sarif` | `html,csv` | Per-cluster output formats to generate. Include `json`/`sarif` for automation pipelines and quality gates. |
| `--password` | string | Plain string or `secret://name` | prompt if omitted | Global Prism password fallback. If omitted and needed, interactive prompt is used. Per-cluster file passwords can override per target. |
| `--policy-gates` | string | CSV of expressions `<metric><op><number>` | none | Defines run-fail thresholds for automation control. Example: `new-fails>0,fail-rate>2,min-health-score<90`. |
| `--poll-interval` | duration string | Go duration (`5s`, `10s`, `1m`) | `15s` | Base interval between task-status polls. Shorter intervals improve responsiveness but increase API load. |
| `--poll-jitter` | duration string | Go duration (`0s` and above) | `2s` | Random additive delay on top of poll interval to reduce herd effects across concurrent cluster workers. |
| `--pcs` | string | CSV of Prism Central IP/FQDN/URL values | none | PC target list used in `pc` mode. Each PC is queried and all discovered clusters are added to the run target set (deduplicated). |
| `--pcs-file` | string | Path to text file | none | Alternate PC target source for `pc` mode (one PC per line; `#` comments allowed). |
| `--prism-central-url` | string | URL/IP/FQDN | none | Single-PC fallback target for `pc` mode when `--pcs`/`--pcs-file` are not set. |
| `--discover-api-version` | string | `v4`, `v3` | `v4` | API used for PC cluster discovery in `pc` mode. `v4` uses clustermgmt API and auto-falls back to `v3` on 404. |
| `--prom-enabled` | bool | `true`, `false` | `false` | Enables/disables writing Prometheus textfile metrics. |
| `--prom-dir` | string | Writable directory path | `promfiles` | Directory for Prometheus `.prom` metric files (used only when `--prom-enabled=true`). |
| `--quiet-hours` | string | `HH:MM-HH:MM` local-time range | none | Recurring daily notification suppression window. Ideal for predictable off-hours operations. |
| `--replay` | bool | `true`, `false` | `false` | Rebuilds reports/artifacts from existing logs without invoking NCC APIs. Useful for debugging and template iterations. |
| `--request-timeout` | duration string | Go duration (`5s`, `20s`, `60s`) | `20s` | Per-request HTTP timeout. Must be lower than overall run timeout; increase for slow links or overloaded control planes. |
| `--retain-days` | int | Integer `>= 0` (`0` = unlimited) | `0` | Run-history retention by age. Applies only when `--run-history` is enabled. |
| `--retain-last` | int | Integer `>= 0` (`0` = unlimited) | `0` | Run-history retention by count. Keeps only newest N snapshots when enabled. |
| `--retry-base-delay` | duration string | Go duration (`100ms`, `500ms`, `1s`) | `400ms` | Base backoff delay for retryable HTTP errors. Used with jitter and exponential growth. |
| `--retry-max-attempts` | int | Integer `>= 1` | `6` | Maximum HTTP retry attempts for retryable errors/statuses. |
| `--retry-max-delay` | duration string | Go duration (`1s`, `8s`, `30s`) | `8s` | Upper bound on retry backoff delay. Prevents unbounded wait times under prolonged failures. |
| `--retry-circuit-breaker` | int | Integer `>= 1` | `3` | Opens retry circuit and fails fast after N consecutive retryable failures. Helps avoid long noisy retry loops on unhealthy endpoints. |
| `--run-history` | bool | `true`, `false` | `false` | Persists timestamped run snapshots for trend and regression analysis across runs. |
| `--run-history-dir` | string | Writable directory path | `<output-dir-filtered>/runs` | Base path for saved run snapshots when run-history is enabled. |
| `--skip-preflight-check` | bool | `true`, `false` | `false` | Skips default preflight validation before run. Useful only for controlled/debug scenarios; not recommended for production runs. |
| `--artifact-retain-days` | int | Integer `>= 0` | `0` | Deletes generated artifacts older than N days from `output-dir-filtered` (`0` disables age-based deletion). |
| `--artifact-retain-max-files` | int | Integer `>= 0` | `0` | Keeps only the N newest generated artifacts in `output-dir-filtered` (`0` disables count-based deletion). |
| `--secrets-file` | string | Path to YAML/JSON key-value map | none | Secret map source when `--secrets-provider=file` is selected. |
| `--secrets-provider` | string | `env`, `file` | none | Enables `secret://` value resolution from process environment or file-backed key map. |
| `--severity-filter` | string | CSV subset of `FAIL,WARN,ERR,INFO` | empty (all) | Limits output rows/artifacts to selected severities. Useful for alert-focused reports but can hide context. |
| `--skip-preflight-check` | bool | `true`, `false` | `false` | Skips default preflight execution in run path. Keep `false` for production safety. |
| `--auto` | bool | `true`, `false` | `false` | Enables guided automation in run path: prints remediation runbooks and applies safe self-healing fixes before failing. |
| `--automation-level` | string | `advisory`, `safe-fix`, `full-auto` | `safe-fix` | Automation policy for run/quickstart. `advisory` suggests fixes only; `safe-fix` applies low-risk repairs; `full-auto` additionally tunes runtime knobs (`max-parallel`, timeout/retry settings) for stability. |
| `--exclude-alert-titles` | string | CSV list of alert titles | empty | Excludes matching alert titles from generated outputs/notifications. |
| `--exclude-alert-titles-file` | string | Path to line-delimited title file | empty | Loads exclusion titles from file (`#` comments and blank lines are ignored). |
| `--exclude-alert-match-mode` | string | `exact`, `contains`, `regex` | `exact` | Controls how exclusion titles are matched against alert names. |
| `--single-report` | bool | `true`, `false` | `false` | Writes a single-file report copy (`ncc-report-single.html`) in addition to regular outputs. |
| `--slack-channel` | string | Channel name (for example `#ops-alerts`) | none | Optional channel override in Slack notification payloads when webhook supports channel routing. |
| `--slack-enabled` | bool | `true`, `false` | `false` | Enables Slack notifications through incoming webhook integration. |
| `--slack-webhook-url` | string | HTTPS webhook URL or `secret://name` | none | Target Slack webhook endpoint. Prefer `secret://` indirection for production secrets handling. |
| `--smtp-password` | string | Plain string or `secret://name` | none | SMTP auth password for email notifications. |
| `--smtp-port` | string | Numeric port string (commonly `587` or `465`) | `587` | SMTP server port. `587` is typical for STARTTLS; `465` is typical for implicit TLS. |
| `--smtp-server` | string | Hostname or IP | none | SMTP relay host used for email delivery. |
| `--smtp-user` | string | Username/login string | none | SMTP authentication username. |
| `--timeout` | duration string | Go duration (`5m`, `15m`, `30m`) | `15m` | Per-cluster overall timeout budget (start + poll + summary + write). |
| `--username` | string | Prism username | `admin` | Global Prism username fallback. Can be overridden per cluster by `clusters-file` entries. |
| `--webhook-enabled` | bool | `true`, `false` | `false` | Enables generic webhook notifications for each event or digest summary. |
| `--webhook-headers` | map | `key=value` pairs (comma-separated) | empty map | Adds custom headers to webhook HTTP requests (tokens, tenant IDs, routing hints). |
| `--webhook-include-html` | bool | `true`, `false` | `false` | Embeds HTML report content as base64 in webhook payloads. Increases payload size. |
| `--webhook-url` | string | HTTP/HTTPS URL or `secret://name` | none | Destination webhook endpoint URL for outbound notifications. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints command usage and flag help. |

## 6) Subcommand flags

Note: legacy root flags `--env-info`, `--tc`, `--update`/`-u`, `--gen-test-agg`, and `--version`/`-v` are still accepted as deprecated aliases.

### 6.1 `discover-clusters`

```bash
ncc-orchestrator discover-clusters [flags]
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--discover-api-version` | string | `v4`, `v3` | `v4` | Selects discovery endpoint family. `v4` uses clustermgmt GET with pagination; `v3` uses legacy list API. |
| `--format` | string | `lines`, `table`, `json` | `lines` | Output renderer: `lines` for direct `clusters-file` usage, `table` for human review, `json` for automation pipelines. |
| `--insecure-skip-verify` | bool | `true`, `false` | `false` | Disables TLS verification for Prism Central API calls. Also required if `--prism-central-url` uses `http://` instead of `https://`. |
| `--output` | string | File path | none | Writes discovered addresses to file (one per line), useful to bootstrap `clusters-file`. |
| `--password` | string | Plain string or env-injected value | prompt/none | Prism Central password for discovery operation only. |
| `--prism-central-url` | string | URL such as `https://pc:9440` | none | Required Prism Central endpoint for cluster list queries. |
| `--username` | string | Username string | `admin` | Prism Central username for discovery API calls. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.2 `env-info`

```bash
ncc-orchestrator env-info
```

Prints all supported `NCC_*` environment variables with current values (sensitive values masked).

### 6.3 `terms`

```bash
ncc-orchestrator terms
```

Prints terms and conditions text and exits.

### 6.4 `update`

```bash
ncc-orchestrator update
```

By default, updates remain in the current major track (for example `v1.x` -> latest `v1.x`). Use `--allow-major-upgrade` to move across major versions (for example `v1` to `v2`) after migration review.

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--check` | bool | `true`, `false` | `false` | Check-only mode. Reports selected release/binary availability without downloading or replacing. |
| `--allow-major-upgrade` | bool | `true`, `false` | `false` | Explicitly permits major-version upgrades. Required for `v1.x` -> `v2.x` transitions. |
| `--repo` | string | `owner/repo` or GitHub repo URL | `lTSPV75BRO/Nutanix-ncc-orchestrator` | GitHub source repo used for release discovery/check/update. |
| `--binary-url` | string | Direct binary URL | empty | Use a non-GitHub/custom artifact URL for check/update operations. |
| `--binary-sha256` | string | 64-char SHA256 hex | empty | Required when installing via `--binary-url` (ignored for `--check`). Used to enforce artifact integrity. |
| `--target-version` | string | Semver-like value | empty | Target version hint, recommended with `--binary-url` for track comparisons/safety checks. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.5 `gen-test-agg`

```bash
ncc-orchestrator gen-test-agg --clusters 25 --output-dir dist/test/outputfiles
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--clusters` | int | Integer `>= 1` | none | Number of synthetic clusters to generate in aggregated artifacts. |
| `--output-dir` | string | Writable directory path | `outputfiles` | Destination directory for generated synthetic artifacts. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.6 `version`

```bash
ncc-orchestrator version
```

Prints version/build/Go metadata and exits.

### 6.7 `v2-bootstrap`

```bash
ncc-orchestrator v2-bootstrap --check
ncc-orchestrator v2-bootstrap --install-dir .ncc-v2
```

Automates v2 stack setup using release assets. It prefers a single `ncc-v2-stack-<os>-<arch>` archive and falls back to legacy split assets when needed.

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--check` | bool | `true`, `false` | `false` | Verifies required assets exist for the target platform without downloading. |
| `--repo` | string | `owner/repo` or GitHub URL | `lTSPV75BRO/Nutanix-ncc-orchestrator` | Source release repository used for asset discovery. |
| `--version` | string | Version tag or semver-like value | latest stable `v2` | Pins a specific release version for bootstrap. |
| `--install-dir` | string | Writable directory path | `.ncc-v2` | Destination where binaries, frontend bundle, and helper scripts are created. |
| `--config-path` | string | Config file path | `config.yaml` | Config path passed to generated API startup workflow. |
| `--output-dir` | string | Writable directory path | `outputfiles` | Output artifact directory passed to API server. |
| `--log-dir` | string | Writable directory path | `nccfiles` | Raw runner log directory passed to API server. |
| `--orchestrator-bin` | string | Executable path | `./ncc-orchestrator` | Runner binary path used by API server for trigger/preflight operations. |
| `--api-listen` | string | Listen address | `:8081` | API server bind address prepared by bootstrap scripts. |
| `--ui-listen` | string | Listen address | `:8080` | UI server bind address prepared by bootstrap scripts. |
| `--token-file` | string | File path | `.ncc-api-token` | Token file path shared between API and UI services. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.8 `v2-start`

```bash
ncc-orchestrator v2-start
ncc-orchestrator v2-start --install-dir .ncc-v2 --api-listen :18081 --ui-listen :18080
```

Starts bootstrapped v2 API and UI services together, streams logs, and stops both on Ctrl+C.

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--install-dir` | string | Existing bootstrap directory path | `.ncc-v2` | Directory containing bootstrapped `bin/` and `frontend-dist/` assets. |
| `--config-path` | string | Config file path | `config.yaml` | Config file passed to API server startup. |
| `--output-dir` | string | Writable directory path | `outputfiles` | Output artifact directory passed to API server. |
| `--log-dir` | string | Writable directory path | `nccfiles` | Raw runner log directory passed to API server. |
| `--orchestrator-bin` | string | Executable path | `./ncc-orchestrator` | Runner binary path used by API server. |
| `--api-listen` | string | Listen address | `:8081` | API server bind address. |
| `--ui-listen` | string | Listen address | `:8080` | UI server bind address. |
| `--token-file` | string | File path | `.ncc-api-token` | Token file path used for API/UI auth bridging. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.9 `create-schedule`

```bash
ncc-orchestrator create-schedule [flags]
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--action` | string | `create`, `list`, `remove`, `run-now` | `create` | Scheduler operation to execute. Use `list` to audit, `remove` for cleanup, `run-now` for immediate validation. |
| `--command` | string | Full shell command | auto-generated | Advanced override for the exact scheduled command line. Use only when default command generation is insufficient. |
| `--config` | string | Config file path | none | Config path passed through to scheduled runs to ensure deterministic behavior. |
| `--cron` | string | Standard 5-field cron expression | derived from `--every` when empty | Explicit cron schedule for `--type cron`. Takes precedence over `--every` derivation. |
| `--every` | duration | Go duration (`30m`, `4h`, `24h`) | `4h` | Used to derive schedule intervals (cron or Windows task cadence). |
| `--log-path` | string | File path | `logs/ncc-scheduler.log` | Output redirection path for scheduled command logs. |
| `--with-lock` | bool | `true`, `false` | `true` | Enables `flock`-based overlap protection for cron runs to prevent concurrent schedule collisions. |
| `--print-only` | bool | `true`, `false` | `true` | Safety preview mode. Keep true to inspect planned scheduler changes before applying. |
| `--task-name` | string | Task/marker name string | `ncc-orchestrator` | Identifier used in cron marker comments or Windows task naming. |
| `--type` | string | `auto`, `cron`, `windows` | `auto` | Scheduler backend. `auto` picks platform-appropriate implementation. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.10 `validate-config`

```bash
ncc-orchestrator validate-config --config config.yaml
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | none | Legacy helper for config-only validation. Prefer `preflight-check` for full checks + remediation hints. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.11 `config-schema`

```bash
ncc-orchestrator config-schema --output config.schema.json
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--output` | string | File path | stdout | Writes generated JSON schema to file; when omitted schema is printed to stdout. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.12 `validate-secrets`

```bash
ncc-orchestrator validate-secrets --config config.yaml
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | none | Legacy helper for secrets-only validation. Prefer `preflight-check` for full checks + remediation hints. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.13 `preflight-check`

```bash
ncc-orchestrator preflight-check --config config.yaml --format json
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | none | Runs preflight checks against this config. If omitted, report includes a warning that file-based checks are skipped. |
| `--format` | string | `json` | `json` | Structured output for UI/automation. Includes `checks[]`, `actionableHints[]`, and machine-readable `remediation_code` on non-pass checks. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.13a `quickstart`

```bash
ncc-orchestrator quickstart --config config.yaml --auto-fix
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | `config.yaml` | Config path to initialize/validate. Creates starter config if missing. |
| `--auto-fix` | bool | `true`, `false` | `true` | Applies safe setup fixes (missing dirs/files, starter config generation). |
| `--interactive` | bool | `true`, `false` | `false` | Prompts for common values (cluster mode/targets/username) and writes them to config before preflight. |
| `--setup-v2` | string | `ask`, `download`, `skip` | `ask` | Controls what quickstart does when v2 web components are missing. `ask` prompts user permission, `download` auto-downloads, `skip` prints manual link/command only. |
| `--install-dir` | string | Path | `.ncc-v2` | Target directory for v2 component bootstrap/download. |
| `--repo` | string | `owner/repo` or GitHub URL | `lTSPV75BRO/Nutanix-ncc-orchestrator` | Source repository for v2 release assets used by quickstart bootstrap. |
| `--assume-yes` | bool | `true`, `false` | `false` | Automatically accepts quickstart prompts (useful for hands-free onboarding scripts). |
| `--automation-level` | string | `advisory`, `safe-fix`, `full-auto` | `safe-fix` | Controls how aggressive quickstart automation should be. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

Bare-minimum first-time flow:

1. Creates starter `config.yaml` if missing.
2. Runs preflight and applies safe fixes.
3. Checks v2 components (API/UI/frontend).
4. Asks permission before downloading v2 assets (or prints direct release link + command).

### 6.14 v2 service runtime flags (API/UI)

These are runtime flags for v2 services (`cmd/ncc-api-server`, `cmd/ncc-ui-server`), commonly used in production deployments:

| Service | Flag | Default | Purpose |
|---|---|---|---|
| `ncc-api-server` | `--rate-limit-per-minute` | `60` | Per-client rate limit for sensitive mutation/auth routes (`0` disables). |
| `ncc-api-server` | `--max-concurrent-runs` | `4` | Maximum orchestrator runs executing at once. Triggers beyond this queue and start automatically as slots free; clusters already being refreshed by another active run are skipped so a shared cluster runs only once. |
| `ncc-api-server` | `--login-lockout-threshold` | `5` | Failed logins per account before a temporary lockout (`0` disables). |
| `ncc-api-server` | `--login-lockout-window` | `15m` | Rolling window for accumulating failed logins toward a lockout. |
| `ncc-api-server` | `--login-lockout-duration` | `15m` | How long a locked account stays locked after exceeding the threshold. |
| `ncc-api-server` | `--auth-mode` | `token` | API auth mode: `token`, `session`, `hybrid`. |
| `ncc-api-server` | `--token-file-path` | `.ncc-api-token` | Token file used by UI proxy and local tooling. |
| `ncc-api-server` | `--cookie-secure` / `--cookie-insecure` | auto | Force the session cookie `Secure` attribute on/off. Auto-set by `v2-start` to track whether the UI is on HTTPS (the default); set `--cookie-insecure` only when serving plain HTTP. |
| `ncc-ui-server` | `--allowed-origins` | `http://localhost:8080` | Browser origin allowlist for proxied API calls. |
| `ncc-ui-server` | `--api-auth-mode` | `token` | Backend auth forwarding mode (`token` or `session`). |
| `ncc-ui-server` | `--ui-insecure-http` | `false` | Serve plain HTTP instead of the default self-signed HTTPS (use only behind a trusted proxy/loopback). |

`v2-start` convenience mode flags (must-have operator controls):

| Group | Flag | Default | Purpose |
|---|---|---|---|
| Throughput/Stability | `--api-run-timeout` | `90m` | Maximum runtime for backend `runs/trigger` orchestration command. |
| Throughput/Stability | `--api-rate-limit-per-minute` | `60` | Per-client limiter for auth and mutation endpoints (`0` disables). |
| Throughput/Stability | `--api-read-timeout` | `15s` | API HTTP server read timeout. |
| Throughput/Stability | `--api-write-timeout` | `60s` | API HTTP server write timeout. |
| Throughput/Stability | `--api-idle-timeout` | `60s` | API HTTP keep-alive idle timeout. |
| Auth/Security | `--api-auth-mode` | `token` | API auth mode: `token`, `session`, `hybrid`. |
| Auth/Security | `--api-session-ttl` | `6h` | Session token TTL for `session`/`hybrid` mode (admins can override at runtime in Settings → Access). |
| Auth/Security | `--api-session-secret` | empty | Inline HMAC session secret (prefer file in production). |
| Auth/Security | `--api-session-secret-file` | empty | Reads session secret from file and passes to API process. |
| Auth/Security | `--api-cors-origins` | derived | Explicit API CORS allowlist (comma-separated). |
| Network/Topology | `--ui-backend-url` | derived from `--api-listen` | UI proxy target URL for API traffic (useful with LB/ingress). |
| Network/Topology | `--api-advertise-url` | empty | External API URL to print at startup for operators/users. |
| Network/Topology | `--ui-advertise-url` | empty | External UI URL to print at startup for operators/users. |
| TLS/mTLS | `--api-tls-cert-file`, `--api-tls-key-file` | empty | Enable HTTPS for API listener. |
| TLS/mTLS | `--api-tls-client-ca-file` | empty | Enable API mTLS client verification. |
| TLS/mTLS | `--ui-tls-cert-file`, `--ui-tls-key-file` | auto (self-signed) | UI HTTPS cert/key. **The UI serves HTTPS by default**: when empty, `v2-start` generates a self-signed cert under `<install-dir>/tls/` and redirects HTTP→HTTPS on the same port. Supply your own to use a CA/PKI cert, or pass `--ui-insecure-http` to serve plain HTTP. |
| TLS/mTLS | `--ui-insecure-http` | `false` | Serve the UI over plain HTTP instead of the default self-signed HTTPS (trusted loopback / TLS-terminating proxy only). Persisted in `.ncc-v2-start.json`. |
| TLS/mTLS | `--ui-backend-ca-file` | empty | Custom CA trust for UI->API TLS connection. |
| TLS/mTLS | `--ui-backend-client-cert-file`, `--ui-backend-client-key-file` | empty | UI client certificate pair for API mTLS. |
| TLS/mTLS | `--ui-backend-insecure-skip-verify` | `false` | Skip UI->API certificate verification (dev/troubleshooting only). |
| Operability | `--wait-ready` | `false` | Block until API health (and UI root if enabled) is reachable. |
| Operability | `--ready-timeout` | `20s` | Max wait time for readiness checks. |
| Operability | `--detach` | `false` | Run services in background with PID/log files. |
| Operability | `--api-log-file`, `--ui-log-file` | under `<install-dir>/logs/` | Custom detached log paths. |
| Operability | `--api-pid-file`, `--ui-pid-file` | under `<install-dir>/run/` | Custom detached PID file paths. |
| Operability | `--self-heal` | `false` | Detached mode only: monitor API/UI and auto-restart on unexpected process exits. Also restarts a hung-but-alive API server via health probes, applies exponential backoff between restarts, and cools down then resumes (instead of giving up) once the restart budget is exhausted. |
| Operability | `--self-heal-max-restarts` | `3` | Maximum restart attempts within the self-heal window before a cooldown. |
| Operability | `--self-heal-window` | `10m` | Rolling restart-budget window for the detached self-heal monitor (also the cooldown duration after the budget is exhausted, after which the monitor resumes). |
| Operability | `--self-heal-probe-interval` | `10s` | How often the self-heal monitor health-probes a still-alive API server (via the api-server's built-in `--health-check`) to detect hangs/deadlocks. API server only. |
| Operability | `--self-heal-unhealthy-threshold` | `3` | Consecutive failed health probes before the monitor restarts a hung-but-alive API server. |
| Mode | `--api-only` | `false` | Start only API (no UI server/frontend). |
| Existing | `--ui-allowed-origins` | empty | Additional browser origins for UI CORS checks (localhost always included). |

`v2-stop` operability flags:

| Flag | Default | Purpose |
|---|---|---|
| `--stop-timeout` | `5s` | Grace period after `SIGTERM` before force-kill. |
| `--api-pid-file`, `--ui-pid-file` | empty | PID path overrides when custom PID files are used. |
| `--force` | `false` | Immediate hard kill, no graceful wait. |

### 6.14a `v2-backup` / `v2-restore` / `v2-reset-password`

```bash
ncc-orchestrator v2-backup --install-dir .ncc-v2 --output-file ncc-backup.tar.gz
ncc-orchestrator v2-restore ncc-backup.tar.gz --install-dir .ncc-v2 --force --restart
ncc-orchestrator v2-reset-password --user admin --install-dir .ncc-v2
```

`v2-backup` writes a single `0600` `tar.gz` capturing the stateful contents of an
install dir:

- `config.yaml` and its referenced files (`clusters-file`,
  `exclude-alert-titles-file`, `secrets-file`)
- the local user database (`.ncc-api-users.json`: accounts, bcrypt hashes, roles,
  runtime **SAML and LDAP** config, session policy)
- the API token (`.ncc-api-token`) and first-run admin password if still present
- scheduler/notifications state and any other `.ncc-api-*` state at the root
- the **portable `v2-start` settings** (`.ncc-v2-start.json`: CORS origins,
  listen addresses, advertise/backend URLs, auth mode, session TTL, rate limit,
  HTTP timeouts, self-heal) so a restore reuses them on restart (path-type flags
  such as `--config-path`/dirs are re-derived under the new install dir)
- the JSONL audit log (`logs/ncc-audit.log`) when present
- the **latest run's report artifacts** — the top-level files of
  `output-dir-filtered` (`run-summary.json`, `index.html`, and the
  SLO/drilldown/flaky/checks JSON) — so a restored stack shows the most recent
  run on the dashboard immediately

The archive's `manifest.json` records the **ncc-orchestrator version** (plus
stream, build date, Go toolchain) that created it, and an **auth-provider
summary** (`auth`): the local-account count and whether **SAML** (with its SP
signing key) and **LDAP/AD** (with its bind password) are present/enabled.
Because SAML and LDAP config — including those secrets — live *inside*
`.ncc-api-users.json` rather than in their own files, `v2-backup` and
`v2-restore` print this summary so you can confirm SSO/directory config and its
secrets travelled with the archive; restore re-reads the restored database and
**warns when a provider is enabled but its secret is missing** (e.g. SAML
enabled without an SP key, or LDAP enabled with an anonymous bind). Regenerable
artifacts (binaries, frontend bundle, run/ pid files, raw NCC summaries under
`output-dir-logs`, Prometheus textfiles, and the full run-history snapshots
under `<output-dir-filtered>/runs/`) are excluded, keeping the archive to a
single run's worth of report data.

| Command | Flag | Default | Purpose |
|---|---|---|---|
| `v2-backup` | `--install-dir` | auto-detect (`.ncc-v2`) | Install dir to back up. |
| `v2-backup` | `--output-file` | `./ncc-backup-<UTC>.tar.gz` | Output archive path. |
| `v2-restore` | `--install-dir` | auto-detect (`.ncc-v2`) | Install dir to restore into. |
| `v2-restore` | `--input-file` | — | Archive to restore (or first positional arg). |
| `v2-restore` | `--force` | `false` | Overwrite existing files / proceed even if the stack is running. |
| `v2-restore` | `--restart` | `false` | Force a stack restart/start after restore even if it looks stopped (restart is automatic when the stack is running). |
| `v2-restore` | `--no-restart` | `false` | Suppress the automatic post-restore restart (e.g. staging a host for later). |
| `v2-restart` | `--install-dir` | auto-detect (`.ncc-v2`) | Stop and re-start the stack (`v2-stop` then `v2-start --detach`), performed by the binary. |
| `v2-reset-password` | `--user` | `admin` | Local account to reset. |
| `v2-reset-password` | `--users-db` / `--users-db-secret[-namespace]` | `<install-dir>/.ncc-api-users.json` | User store to reset against (file or Kubernetes Secret). |

`v2-restore` is confined to the install dir (unsafe archive paths rejected),
reports the orchestrator version that produced the backup, and **warns when the
restoring binary is older** than the backup's creator (it never refuses on a
version difference).

**First-login restore:** on a fresh deployment the bootstrap admin is normally
forced to set a new password before doing anything. The forced-change screen now
also offers a **"Restore from backup…"** button: uploading an existing
deployment's archive recovers everything and the admin keeps their **original**
password (the restored user database carries the old admin account with
`must_change_password=false`), so no password reset is required. The restore
endpoint is the single exception allowed through the forced-change gate, and it
is still confined to the admin role with CSRF enforced; the stack restarts
automatically and the UI reconnects on its own.

**Cross-OS portable:** a backup taken on Windows restores cleanly onto
Linux/macOS. During restore, file-reference paths (`clusters-file`,
`output-dir-logs`, `output-dir-filtered`, `log-file`, `run-history-dir`,
`prom-dir`, `secrets-file`, `ca-bundle`, …) that pointed inside the backup's
original install dir are rebased to the target install dir and backslashes are
normalized to forward slashes; absolute paths outside that dir are normalized
and flagged with a warning to review.

**Automatic restart:** when the stack is running, `v2-restore` stops and
re-starts it for you so the restored data loads with no manual step. Use
`--restart` to force a restart/start even when it looks stopped, or
`--no-restart` to suppress it. The standalone `v2-restart` command performs the
same binary-driven stop + `v2-start --detach`. The **UI restore** (Settings →
Access → Backup & restore) applies the archive and then restarts the stack
automatically via a detached orchestrator process; the page reconnects on its
own once the new stack is healthy.

### 6.14b `v2-check`

```bash
ncc-orchestrator v2-check --config-path /abs/config.yaml --api-listen :8081 --ui-listen :8080
```

| Flag | Type | Default | Detailed explanation |
|---|---|---|---|
| `--install-dir` | string | `.ncc-v2` | Runtime layout root used to validate API/UI binaries and frontend assets. |
| `--config-path` | string | `config.yaml` | Config file that must exist/read successfully for v2 startup. |
| `--output-dir` | string | `outputfiles` | Directory validated for write access before v2 runtime start. |
| `--log-dir` | string | `nccfiles` | NCC raw log directory validated for write access. |
| `--token-file` | string | `.ncc-api-token` | Token path parent is validated for write access. |
| `--orchestrator-bin` | string | `./ncc-orchestrator` | Runner binary path validated for existence + executability. |
| `--api-listen` | string | `:8081` | API listen address bind check to catch "address already in use". |
| `--ui-listen` | string | `:8080` | UI listen address bind check (skipped when `--api-only=true`). |
| `--api-only` | bool | `false` | Limits checks to API prerequisites only. |

Use this command before `v2-start` in production automation to fail fast on path/port misconfiguration.

Production-style startup example:

```bash
ncc-orchestrator v2-start \
  --api-listen :8081 \
  --ui-listen :8080 \
  --api-auth-mode hybrid \
  --api-session-secret-file /etc/ncc/session-secret.txt \
  --api-session-ttl 30m \
  --api-rate-limit-per-minute 300 \
  --api-read-timeout 20s \
  --api-write-timeout 75s \
  --api-idle-timeout 90s \
  --api-cors-origins https://ncc.example.com \
  --ui-backend-url https://ncc-api.internal:8443 \
  --ui-backend-ca-file /etc/ncc/ca.pem \
  --wait-ready --ready-timeout 45s
```

When `--api-only` is enabled, open `http://localhost:8081/` for backend status and API docs links (`openapi.json`, `meta/routes`).

Rate limiter operational metrics endpoint:

- `GET /api/v1/metrics/rate-limit`
- Returns:
  - configured limit/window
  - `allowed_total`
  - `blocked_total`
  - `evicted_total`
  - `active_buckets`

Large payload control for report endpoint:

- `GET /api/v1/report/data?limit=<n>&offset=<m>`
- Applies to large array fields (`checks_snapshot`, `agg_rows`) and returns per-field pagination metadata.

Machine-readable API errors:

Scheduler health endpoint:

- `GET /api/v1/schedule/health`
- Returns scheduler configuration and operational hints:
  - `last_run`
  - `last_success`
  - `last_error`
  - `log_path`, `lock_path`, `with_lock`

- All non-success API responses include a stable `error_code` field for automation/UI handling.
- Examples:
  - `NCC_API_BAD_REQUEST`
  - `NCC_API_UNAUTHORIZED`
  - `NCC_API_RATE_LIMITED`
  - `NCC_API_INTERNAL`

For complete v2 deployment examples, see:
- `README.md` (Run backend / Run frontend)
- `docs/V2_BACKEND_FRONTEND_MVP.md`
- `k8s/README.md`
- `Prometheus.md` (generalized monitoring patterns for CLI, v2, and Kubernetes)

## 7) Full config example

```yaml
clusters: "10.38.66.37,10.38.66.7"
# clusters-file: "clusters.txt"
username: "admin"
password: "secret://NCC_PRISM_PASSWORD"
ncc-api-version: "v4"
nutanix-v4-api-version: "v4.2"
insecure-skip-verify: false
# ca-bundle: "/etc/ncc/prism-ca.pem"   # trust an internal CA (safer than insecure-skip-verify)
# pin-sha256: ""                        # CSV of allowed server cert SHA-256 fingerprints (pinning)

timeout: "15m"
request-timeout: "20s"
poll-interval: "15s"
poll-jitter: "2s"
max-parallel: 4
adaptive-parallelism: true

outputs: "html,csv"
output-dir-logs: "nccfiles"
output-dir-filtered: "outputfiles"
single-report: false

log-file: "logs/ncc-runner.log"
log-level: "info"
log-http: false

retry-max-attempts: 6
retry-base-delay: "400ms"
retry-max-delay: "8s"

max-idle-conns: 100
max-idle-conns-per-host: 10
max-conns-per-host: 0
idle-conn-timeout: "90s"

prom-dir: "promfiles"
prom-enabled: true

run-history: false
run-history-dir: "outputfiles/runs"
retain-last: 0
retain-days: 0
notify-on-regression: false

policy-gates: "new-fails>0,fail-rate>2,min-health-score<90"
quiet-hours: ""
maintenance-windows: ""
flaky-lookback-runs: 6
flaky-min-transitions: 2
severity-filter: ""

dry-run: false
replay: false

email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: "secret://SMTP_PASSWORD"
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true
# smtp-insecure-skip-verify: false      # skip SMTP STARTTLS verify (independent of insecure-skip-verify)
# email-subject-template: ""            # Go text/template; empty = built-in default
# email-body-template: ""

# notification-deadletter-dir: "/var/lib/ncc/deadletter"  # persist failed notification payloads

webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
# webhook-template: ""                  # Go text/template for the body; empty = default JSON
# webhook-secret: "secret://WEBHOOK_HMAC"  # sign body: header X-NCC-Signature: sha256=<hmac>
webhook-headers:
  X-Auth-Token: "secret://WEBHOOK_TOKEN"

slack-enabled: false
slack-webhook-url: "secret://SLACK_WEBHOOK_URL"
slack-channel: "#ncc-alerts"

secrets-provider: "env"
secrets-file: ""
```

## 8) Environment variable mapping

Any config key can be provided via env with:
- prefix `NCC_`
- uppercase
- hyphens replaced by underscores

Examples:
- `username` -> `NCC_USERNAME`
- `clusters-file` -> `NCC_CLUSTERS_FILE`
- `request-timeout` -> `NCC_REQUEST_TIMEOUT`
- `webhook-include-html` -> `NCC_WEBHOOK_INCLUDE_HTML`
- `webhook-secret` -> `NCC_WEBHOOK_SECRET` (config/env only; no CLI flag, to keep the secret out of the process list)

Print current values:

```bash
ncc-orchestrator env-info
```

**Server-only environment variables** (api-server, not config keys):

| Variable | Purpose |
|---|---|
| `NCC_API_TOKEN` | Admin token for the api-server (full access). |
| `NCC_API_VIEWER_TOKEN` | Optional read-only viewer token (RBAC). Holders may read non-settings `GET` endpoints but get `403` on `/api/v1/settings/*` and any mutating request. Must differ from `NCC_API_TOKEN`. |
| `NCC_USERS_DB` | Path to the writable JSON user database file (enables login, first-run admin bootstrap, and runtime user/SSO management). Equivalent to `--users-db`. Defaults to `<root>/.ncc-api-users.json` inside a v2 stack. |
| `NCC_USERS_DB_SECRET` | Kubernetes Secret name to store the user database in (encrypted at rest by etcd). Equivalent to `--users-db-secret`. Mutually exclusive with `NCC_USERS_DB`. Requires in-cluster execution + RBAC (`k8s/rbac.yaml`). |
| `NCC_USERS_DB_SECRET_NAMESPACE` | Namespace of the Kubernetes Secret store (defaults to the pod's own namespace). Equivalent to `--users-db-secret-namespace`. |
| `NCC_MASTER_KEY` | 32-byte master key (base64 std/raw/url or hex) to envelope-encrypt the file-backed user store at rest with AES-256-GCM. Takes precedence over `--users-db-key-file`/`NCC_MASTER_KEY_FILE`. Unset → plaintext (default). |
| `NCC_MASTER_KEY_FILE` | Path to a file holding the 32-byte master key (base64/hex, or 32 raw bytes). Equivalent to `--users-db-key-file`. Keep it off the protected disk/backup. |
| `NCC_USERS_FILE` | Path to an optional read-only YAML seed of local accounts, imported once into the database when empty. Equivalent to `--users-file`. |
| `NCC_PASSWORD` | Read by `ncc-api-server --hash-password` as the password to hash (otherwise prompts on stdin). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Enables OpenTelemetry tracing (per-cluster spans) over OTLP/HTTP. |
| `NCC_OTEL_ENABLED` | Set to `1`/`true` to enable OTel tracing using the standard `OTEL_*` exporter env vars. |

## 9) Execution lifecycle (detailed)

This section explains what happens during a normal run.

### 9.1 Startup and config resolution

1. Parse CLI flags.
2. Load config file when `--config` is set.
3. Apply environment overrides (`NCC_*`).
4. Apply defaults for unset fields.
5. Resolve `secret://` references (when `secrets-provider` is configured).
6. Validate final effective config (strict validations, known keys, value types).

### 9.2 Cluster target preparation

- If `clusters-file` is set and readable, it becomes the primary source of target clusters.
- Effective per-cluster credentials are resolved in this order:
  1. Per-line username/password from `clusters-file` (if provided)
  2. Global `username` / `password`
  3. Interactive password prompt (if still missing and command is interactive)
- Duplicate clusters fail fast during validation.

### 9.3 Per-cluster run phases

For each cluster worker:

1. Start checks API call (v4 path by default, legacy fallback behavior where applicable).
2. Poll task status at `poll-interval +/- poll-jitter`.
3. Fetch NCC summary after completion.
4. Persist raw summary log under `output-dir-logs`.
5. Parse blocks and generate selected per-cluster outputs.

### 9.4 Aggregation and historical analytics

After cluster workers finish:

- Build aggregated `index.html`.
- Write run-level artifacts (`run-summary.json`, `ncc-run-record.json`).
- Compute diff/regression/flaky/SLO artifacts if prior snapshots exist.
- Evaluate policy gates.
- Emit notifications (unless suppressed by quiet-hours / maintenance windows).
- Optionally persist snapshot to run-history and apply retention.

## 10) Exit codes and automation behavior

| Exit code | Meaning | Common automation action |
|---|---|---|
| `0` | Success (no runner-level failure) | Mark pipeline green |
| `1` | Fatal/general error | Fail pipeline and inspect logs |
| `2` | Configuration error/validation failure | Stop early, fix config |
| `3` | Partial success (some clusters failed, some succeeded) | Mark unstable/partial and inspect failed clusters |

### 10.1 Policy-gate influence

Policy gate violations can force failure behavior in automation contexts by design. Always consume:

- `run-summary.json`
- `policy-gates.txt` (when present)

…instead of relying only on console output.

## 11) Feature deep dive (operational detail)

### 11.1 Policy gates: syntax and metric semantics

Supported operators:
- `>`
- `>=`
- `<`
- `<=`
- `==`
- `!=`

Supported metrics:
- `new-fails`: new FAIL checks compared to previous snapshot
- `resolved-fails`: FAIL checks resolved since previous snapshot
- `fail-rate`: current FAIL percentage (0..100)
- `clusters-failed`: number of clusters with runner-level failure
- `regressions`: binary indicator (`1` yes, `0` no)
- `flaky-checks`: detected flaky checks count
- `min-health-score`: minimum cluster health score in run
- `avg-health-score`: average health score across successful clusters

Example gate sets by environment:

```text
# Strict production
new-fails>0,fail-rate>1,min-health-score<95

# Lab / staging
new-fails>5,fail-rate>5,min-health-score<85
```

### 11.2 Flaky detection tuning strategy

Use these controls together:

- `flaky-lookback-runs`: larger values increase sensitivity to long-term instability.
- `flaky-min-transitions`: higher values reduce noise.

Recommended starting points:
- Small environments: `lookback=6`, `transitions=2`
- Large/variable environments: `lookback=10-15`, `transitions=3-4`

### 11.3 Health score usage

Use health score for:
- ranking worst clusters first
- SLO threshold checks (`min-health-score` gate)
- trend analysis over historical snapshots

Use both `min-health-score` and `avg-health-score` gates to avoid blind spots (single critical cluster vs broad degradation).

### 11.4 Quiet hours vs maintenance windows

- `quiet-hours`: recurring daily local-time suppression window (good for nighttime)
- `maintenance-windows`: explicit RFC3339 intervals (good for planned maintenance)

When both are configured, suppression applies if **either** condition matches.

### 11.5 Replay mode: expected behavior

Replay does not call NCC start/poll APIs. It consumes existing logs to regenerate:
- per-cluster outputs
- aggregated dashboard
- run artifacts (where derivable from available data)

Use replay when:
- validating report template changes
- testing notification payloads safely
- rebuilding artifacts after a non-data code change

## 12) Artifact guide (field-level orientation)

### 12.1 `run-summary.json`

Primary machine-readable run result:
- run-level timing and status
- per-cluster outcomes (ok/error)
- severity counts
- effective exit code

### 12.2 `ncc-run-record.json`

Wraps run summary with:
- schema metadata
- orchestrator version/build context
- stable envelope useful for long-term ingestion

### 12.3 `checks-snapshot.json`

Per-check state snapshot used as baseline for:
- regressions
- drill-down diffs
- flaky detection

### 12.4 `drilldown-diff.json`

Diff of current snapshot vs prior snapshot:
- newly failing checks
- resolved checks
- severity transitions

### 12.5 `flaky-checks.json`

Checks with repeated severity transitions in lookback window.

### 12.6 `regression-summary.json`

Compact summary of FAIL deltas and directional movement.

### 12.7 `slo-dashboard.json`

SLO-friendly export of cluster-level health/status metrics for dashboard ingestion.

## 13) Flag tuning playbooks (by objective)

### 13.1 Reduce API pressure / throttling

```bash
ncc-orchestrator \
  --max-parallel 2 \
  --adaptive-parallelism \
  --retry-max-attempts 8 \
  --retry-base-delay 600ms \
  --retry-max-delay 15s
```

### 13.2 Faster feedback in CI

```bash
ncc-orchestrator \
  --timeout 8m \
  --request-timeout 15s \
  --poll-interval 8s \
  --outputs json,csv \
  --policy-gates "new-fails>0,fail-rate>2"
```

### 13.3 Stable nightly operations

```bash
ncc-orchestrator \
  --max-parallel 4 \
  --run-history \
  --retain-last 30 \
  --notify-digest \
  --quiet-hours "23:00-06:00"
```

## 14) Security and compliance guidance

### 14.1 Credential handling best practices

- Prefer `secret://` + `secrets-provider` over plaintext passwords.
- Prefer environment-based secret injection in CI/CD.
- Avoid embedding static secrets in tracked config files.
- Use masked logging defaults; do not enable `--log-http` in production unless necessary.

### 14.2 TLS guidance

- Keep `insecure-skip-verify=false` for production.
- Use `true` only for controlled lab/self-signed test environments.

### 14.3 Artifact sensitivity

Report artifacts can include check names/details that may be operationally sensitive. Apply directory permissions and retention controls according to policy.

## 15) Troubleshooting matrix

| Symptom | Likely cause | Check | Fix |
|---|---|---|---|
| Immediate exit with config error | Invalid key/type or missing required values | `validate-config` output | Fix config and re-run validation |
| Cluster starts but poll fails | Timeout/network/API permissions | Logs + request timeout values | Increase timeout, validate reachability/credentials |
| Many 429 responses | Too much concurrency | logs + rate-limit behavior | Lower `max-parallel`, keep adaptive parallelism on |
| Empty/partial aggregated report | No parsable logs or replay source mismatch | raw logs in `output-dir-logs` | Ensure logs exist and are complete, rerun |
| No notifications | Suppression window active or channel misconfig | quiet-hours/windows + channel config | Adjust windows and validate endpoints |

## 16) Production-ready baseline profiles

### 16.1 Conservative production profile

```yaml
max-parallel: 3
adaptive-parallelism: true
timeout: "20m"
request-timeout: "25s"
poll-interval: "15s"
retry-max-attempts: 8
retry-base-delay: "500ms"
retry-max-delay: "12s"
run-history: true
retain-last: 30
notify-digest: true
policy-gates: "new-fails>0,fail-rate>2,min-health-score<90"
```

### 16.2 Fast feedback profile (pre-merge checks)

```yaml
max-parallel: 2
timeout: "8m"
request-timeout: "15s"
poll-interval: "8s"
outputs: "json,csv"
policy-gates: "new-fails>0"
notify-digest: false
```

## 17) Common gotchas

1. `clusters-file` overrides `clusters` when present and non-empty.
2. Per-cluster credentials in `clusters-file` override global username/password for that cluster.
3. `replay` requires existing logs/artifacts; it does not pull fresh NCC data.
4. `severity-filter` affects output view/content; use full severity set for baseline investigations.
5. `run-history` can grow quickly on frequent schedules; set `retain-last` and/or `retain-days`.
