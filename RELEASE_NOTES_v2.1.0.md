# NCC Orchestrator — v2.1.0

**Release date:** 2026-06-04
**Type:** Authentication + maintenance/hardening release. Recommended for everyone on v2.0.x.

> **Affiliation:** This is an independent open-source project. It is **not** affiliated with or endorsed by Nutanix, Inc. NCC and Nutanix are trademarks of their respective owners. The project is MIT licensed; see [`LICENSE`](LICENSE).

v2.1.0 brings real **multi-user authentication and RBAC** to the api-server — local password accounts, **SAML SSO**, and **LDAP / Active Directory** login, a first-run admin bootstrap with a forced password change, runtime user/SSO/LDAP management in the UI, self-service **personal access tokens**, **cluster groups** for membership-based access control, and a Kubernetes Secret-backed user store that is encrypted at rest. The browser-facing UI now serves **HTTPS by default** (auto-generated self-signed cert, HTTP→HTTPS redirect, in-app certificate management). On top of that it closes a download-integrity gap in `v2-bootstrap`, adds notification observability and templating, hardens TLS / notifications / process supervision, serves NCC run metrics over the api-server `/metrics` endpoint, adds OpenTelemetry tracing, adds `v2-backup`/`v2-restore`, improves the Windows self-update flow, refreshes dependencies, and begins splitting the monolithic `goNCC.go` into focused packages. There are no breaking changes — every v2.0.x invocation keeps working, and static-token automation is unaffected.

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

### User database: file or Kubernetes Secret (encrypted at rest)

The user database (bcrypt hashes, roles, the must-change flag, the SAML SP private key, the first-run password) can be persisted as a local `0600` JSON file (`--users-db`, used by file/stack/`docker-compose` installs) or inside a **Kubernetes Secret** (`--users-db-secret`, used by `k8s/api-deployment.yaml`). The Secret path reads/creates/patches the Secret over the in-cluster API using the pod's service account — via a small built-in REST client, no `client-go`, so the static binary stays lean.

> **Encrypt it at rest.** Kubernetes Secrets are only base64-encoded by default. Enable etcd encryption-at-rest (KMS v2 recommended, or `secretbox`/`aescbc`) — see [`k8s/encryption-config.example.yaml`](k8s/encryption-config.example.yaml). The api-server runs with least-privilege RBAC ([`k8s/rbac.yaml`](k8s/rbac.yaml)): it can only get/update/patch the single `ncc-v2-users` Secret.

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

---

## Tests

- Full Go suite passes with `-race -count=1`, including the authentication/RBAC suite (`TestRouteMinRole`, `TestParseRole`, `TestUserStoreVerify`, `TestHandleLoginAndSessionRole`, `TestWithAuthCookieSessionRBACAndCSRF`, `TestStaticAdminTokenExemptFromCSRF`, `TestBootstrapAdminAndPersistence`, `TestForcedPasswordChangeFlow`, `TestUserCRUDAndLastAdminProtection`, `TestSSOConfigPersistAndCertGeneration`, `TestK8sSecretBackendRoundTrip`, and the end-to-end `TestEndToEndFirstRunAdminFlow`), the new `TestVerifyDownloadedAsset` and `TestWriteWindowsUpdateSwapHelper`, the viewer-token RBAC test (`TestWithAuthRBACViewer`), the mock-Prism integration tests (`TestIntegration_DiscoverClustersV4_MockPC`, `TestIntegration_DiscoverClustersV3_MockPC`, `TestIntegration_TaskPoll_MockPrism`), the security/observability unit tests (`TestSignWebhookBody`, `TestWriteDeadLetter`, `TestNormalizePin`, `TestPinVerifier`, `TestRenderRunSummaryMetrics`), and the two fuzz targets with checked-in corpora (`FuzzParseSummary`, `FuzzRedactJSONPasswordValue`), plus the `internal/` package suites (`internal/model`, `internal/promtext`, `internal/retryutil`, `internal/notify`, `internal/nccparse`, `internal/httpclient`, `internal/trace`).
- `go vet`, `gofmt -l`, `govulncheck`, and frontend `npm audit` all clean. CI now enforces these plus `golangci-lint` and a Trivy scan on every push and PR.

---

## Acknowledgements

This release was driven by post-release verification of v2.0.2 on Windows, which surfaced the executability bug fixed in v2.0.2 and prompted the self-update and download-integrity hardening shipped here.
