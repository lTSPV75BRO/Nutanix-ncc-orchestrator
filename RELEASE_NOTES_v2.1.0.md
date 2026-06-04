# NCC Orchestrator — v2.1.0

**Release date:** 2026-06-04
**Type:** Authentication + maintenance/hardening release. Recommended for everyone on v2.0.x.

> **Affiliation:** This is an independent open-source project. It is **not** affiliated with or endorsed by Nutanix, Inc. NCC and Nutanix are trademarks of their respective owners. The project is MIT licensed; see [`LICENSE`](LICENSE).

v2.1.0 brings real **multi-user authentication and RBAC** to the api-server — local password accounts and SAML SSO, a first-run admin bootstrap with a forced password change, runtime user & SSO management in the UI, and a Kubernetes Secret-backed user store that is encrypted at rest. On top of that it closes a download-integrity gap in `v2-bootstrap`, adds notification observability and templating, hardens TLS / notifications / process supervision, serves NCC run metrics over the api-server `/metrics` endpoint, adds OpenTelemetry tracing, improves the Windows self-update flow, refreshes dependencies, and begins splitting the monolithic `goNCC.go` into focused packages. There are no breaking changes — every v2.0.x invocation keeps working, and static-token automation is unaffected.

---

## Highlights

### Authentication, login & RBAC (admin / operator / viewer)

The api-server now supports real interactive login with three ordered roles — `viewer < operator < admin`:

- **viewer** reads non-settings `GET` endpoints; **operator** can also trigger/cancel/preflight runs; **admin** can do everything, including `/api/v1/settings/*`, user management, and token rotation.
- **First-run admin bootstrap (zero-config).** With a writable user database, an empty store provisions an `admin` account with a random password on first launch (printed to the log and stored for retrieval). The admin is forced to change it on first login, then can add users, assign roles, and configure SSO from the UI.
- **Local accounts + SAML SSO.** Password accounts (bcrypt) are managed at runtime in **Settings → Access** (or `/api/v1/settings/users`). SAML can be set via startup flags or configured at runtime in the UI — the SP signing key is generated server-side and never uploaded through the browser; publish `<root>/saml/metadata` to your IdP.
- **Secure sessions.** Browser sessions use an httpOnly, Secure, `SameSite=Strict` `ncc_session` cookie with double-submit CSRF protection. Static-token automation (`NCC_API_TOKEN` / `NCC_API_VIEWER_TOKEN`) keeps working unchanged — `auth-mode` auto-upgrades to `hybrid`.
- **`ncc-ui-server`** auto-detects when login is enabled and forwards each browser's session/CSRF instead of injecting the shared admin token.

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
