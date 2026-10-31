# NCC Orchestrator — v2.2.1

**Release status:** Release candidate. Planned date: set when the `v2.2.1` tag is pushed (the release workflow publishes on tag push).

> **Affiliation:** This is an independent open-source project. It is not
> affiliated with or endorsed by Nutanix, Inc. The project is MIT licensed.

v2.2.1 is a maintenance and security release on top of v2.2.0. It fixes alert
acknowledgements that disappeared after new reports, the "—" run status,
in-app upgrades on Windows, and broken container builds; adds a persistent
alert history with an API and dashboard views; and hardens backups, login,
notifications, and API responses.

**Before upgrading:** backups are now encrypted by default and the in-app
updater needs a server-side backup key. See [Upgrade](#upgrade).

## Bug fixes

### NCC acknowledge/resolve marks survived only until the next report

Root causes (all fixed):

- The marks file `outputfiles/ncc-alert-dispositions.json` lived in the
  runner's output directory. With `artifact-retain-days` or
  `artifact-retain-max-files` set, the runner deleted it after each run.
- Two API replicas on a shared RWX volume (Helm default) raced on
  read-modify-write; the first dashboard load after a new run rewrote the
  file and could overwrite a mark saved by the other replica. Both replicas
  also used the same temp file name for their atomic rename.
- "Reopen" deleted the record, and history was capped at 20 entries.

Marks now live in a persistent alert lifecycle store
(`<repo-root>/.ncc-api-alert-lifecycle.json`), outside the pruned directory,
updated under a cross-process file lock with unique temp files. The old file
is imported once on first start and kept current as a mirror (rollback
safety). Artifact retention now also protects `feature-flags.json` and the
`aggregated-data.json` sidecar that large runs use.

### Successful runs showed "—"

The runner wrote `exit_code` with `omitempty`, so exit code 0 disappeared and
the API could not report success. `run-summary.json` now always carries
`exit_code` and `status`; `/api/v1/runs` returns an explicit `status`
(`success`, `partial`, `failed`, `triggered`, `trigger_failed`, `unknown`) and
`status_source`. Summaries from older runners are recognized; summaries
without a completion marker show **Unknown**, never success.

### In-app software upgrade failed on Windows

Windows refuses to overwrite a running `.exe`, and during an in-app update the
API server, UI server, and orchestrator binaries are all running. The update
failed part-way (after removing `frontend-dist`), and the single-binary path
returned success while only staging a `.new.exe`. The installer now:

- moves running executables aside (allowed on Windows) and activates the new
  files, retrying transient sharing violations (antivirus);
- stages every file first and rolls all replaced files back on any failure;
- reports a manual-swap fallback as an error, never as success;
- returns errors (instead of success) when a release has no package for the
  platform;
- starts the restarter detached from the parent console and, when allowed,
  outside the parent's job object.

### Excessive `/api/v1/runs/active` polling

Five independent timers polled this endpoint (one every 3 s on the Runs page
outside React Query, plus 5 s/15 s/30 s/10 s elsewhere). One shared poller now
serves every view: 3 s while runs are active or queued, 30 s when idle,
paused in hidden tabs, exponential backoff on errors, and request
cancellation.

### `docker compose build` failed

- `Dockerfile.api` and `Dockerfile.ui` built with `golang:1.27.1-alpine` while
  `go.mod` requires Go 1.27.3; the official Go images set `GOTOOLCHAIN=local`,
  so the build stopped with `go.mod requires go >= 1.27.3`. Both now use
  `golang:1.27.3-alpine`.
- `Dockerfile.ui` did not copy `example_config.yaml`, which the dashboard's
  config editor imports, so the compose UI build had failed since v2.1.0.

Published images were not affected (they package prebuilt binaries).
`release_dockerfiles_test.go` now checks both.

## Alert lifecycle and history (new)

- Each NCC alert instance has a server-generated UUIDv4 `id`; the underlying
  finding has a stable `finding_id`.
- Operator state (`open`/`acknowledged`/`resolved`) is separate from
  detection (`detected`/`not_detected`): a check that disappears is not
  "resolved".
- A resolved check found again by a newer run closes the resolved instance and
  opens a new linked one (`previous_id`/`next_id`); severity escalation
  reopens an acknowledgement.
- Ingestion is idempotent per run; duplicate and out-of-order reports are
  ignored; partial runs never hide other clusters' alerts.
- New endpoints: `GET /api/v1/alerts/ncc`, `GET /api/v1/alerts/ncc/events`,
  `GET /api/v1/alerts/ncc/{id}`,
  `POST /api/v1/alerts/ncc/{id}/{acknowledge|resolve|reopen}` — all
  cluster-group scoped. The v2.2.0 `/api/v1/alerts/ncc-dispositions` endpoint
  is unchanged for existing clients (plus additive fields).
- **OpenAPI:** `/api/v1/openapi.json` publishes typed schemas for the three
  read endpoints (`NCCAlert`, `NCCAlertEvent`, `NCCAlertList`,
  `NCCAlertEventPage`, `NCCAlertDetail`, …) with operation ids, so client
  generators and ServiceNow spoke builders can import them. Tests check the
  schemas against the live responses.
- **Dashboard:** the NCC alert panel has a **Lifecycle** card — how long the
  alert has been open, when a run last reported it, how many times it came
  back after being resolved (with who resolved it last and their note), and
  severity changes.
- **Insights:** a new **Alert aging and recurrence** card — open alerts by age
  per cluster, median time to acknowledge and to resolve, findings that keep
  coming back, and open alerts the latest runs no longer report (resolvable
  in place, since they are not on the Dashboard).
- Retention: `--alert-history-retain-days` (default 365) for closed
  instances; active alerts are never pruned.
- Prometheus: `ncc_alerts_active{state,detection,severity}`,
  `ncc_alerts_store_up`, `ncc_alerts_last_ingested_run_timestamp_seconds`,
  `ncc_alerts_events_total`.

## Security

Details and evidence: [`docs/security/security-assessment-2026-10.md`](../security/security-assessment-2026-10.md).

- **Backups are encrypted by default** (`NCC_BACKUP_ENCRYPTION=required`):
  snapshots, downloads, scheduled backups, and pre-update rollback points are
  sealed with AES-256-GCM or refused (`412 NCC_BACKUP_ENCRYPTION_REQUIRED`).
  The audit log records the actual result.
- **Health endpoint**: anonymous callers no longer see config/log/token/binary
  paths or storage details; non-admin responses use stack-relative paths.
- **Login protection**: limits per account+source, per account, and per source;
  progressive lockout; shared counters across replicas; generic responses.
  `X-Forwarded-For` is honored only from `--trusted-proxy-cidrs` and parsed
  right-to-left (the old left-most parsing allowed rate-limit spoofing).
- `--require-encrypted-user-store`; optional `backup-key` / `master-key`
  secret wiring in the Kubernetes manifests and Helm chart.
- More endpoints rate limited; HTTP `ReadHeaderTimeout` and header size limit.
- Restricted viewers no longer see other cluster groups' NCC marks.
- **Email notifications** (CodeQL `go/email-injection`): sender and recipient
  addresses are parsed as email addresses, the subject is MIME-encoded, and
  the body is base64-encoded, so line breaks in run or cluster data cannot
  inject headers or extra recipients. An invalid `from`/`to` address now fails
  the send with a clear error instead of producing a malformed message.
- **Admin password resets** (CodeQL `go/clear-text-logging`): the
  self-service and Settings resets no longer write the new password to the
  server log, which may be forwarded to shared log systems. Read it from
  `.ncc-initial-admin-password` (0600) or, on Kubernetes, the Secret's
  `initial-admin-password` key; the log says where.
- **UI `--health-check`** (CodeQL `go/disabled-certificate-check`): over HTTPS
  the probe now verifies the server against the certificate it is configured
  to serve instead of skipping verification.
- **Dependencies:** `dompurify` ≥ 3.4.17 via an npm override
  (GHSA-cg2g-xh64-v4q7, GHSA-mrv2-3pqr-j5jr, GHSA-c6gr-qvv4-5v3f, reached through
  `monaco-editor`); Go 1.27.3 and `golang.org/x/net` 0.60.0.
- **Build toolchain** pinned to Node.js 26.12.1 (CVE-2026-111497).
- **CA trust store (eMudhra):** all images run `apk add --upgrade
  ca-certificates`. The published `deploy/docker` images used to copy back a
  bundle saved *before* that upgrade, which would have discarded any newer
  trust-store update; they now reinstall `ca-certificates-bundle` after it.
  Verified on 2026-10-23: images built from `alpine:3.24`
  (`ca-certificates-bundle-20261020-r0`) do not contain the three roots that
  NSS 3.131.1 removed (emSign Root CA - G3, emSign Root CA - C1,
  emSign ECC Root CA - C3); `emSign Root CA - G1`, which Mozilla kept, remains.

October 2026 incidents (PHP2Shell, Node.js CVE-2026-111497, GitLab
CVE-2026-110264, Railway, Zeabur, eMudhra, LiteLLM/OpenCode, Double Counter)
are assessed individually in the security assessment.

CodeQL alerts for cookie `Secure` handling (the flag follows whether the stack
serves HTTPS) and for certificate pinning (`--pin-sha256` replaces chain
validation with a fingerprint check) were reviewed and dismissed with
justifications.

## Performance and reliability

- gzip for API JSON responses (honors `Accept-Encoding`, skips small and
  already-encoded bodies). Measured on a synthetic 3,000-row report payload:
  576,428 → 13,770 bytes. Real payloads compress less; no production
  measurement was taken.
- Audit log keeps `--audit-log-keep` rotated generations (default 10;
  previously 1, so every rotation destroyed the prior history).
- Leftovers of earlier updates are cleaned up on the next update.
- The periodic self-heal check now logs errors instead of dropping them.

## CI and build

- `ci.yml` was rejected by GitHub on every push (`runner.temp` in a job-level
  `env`), so CI had not been running; it is fixed and now runs build, vet,
  race tests, golangci-lint, govulncheck, frontend tests/audit/build, and
  Trivy.
- GitHub Actions moved off the deprecated Node 20 runtime (checkout,
  setup-go, setup-node v7; CodeQL v4; attest-build-provenance v4).
  Third-party actions are pinned to commit SHAs; publishing workflows use no
  dependency caches and do not persist git credentials; Docker publishing
  skips instead of failing when Docker Hub secrets are not set.
- golangci-lint v2.15.0 (v2.13 cannot analyze Go 1.27) with a
  [`.golangci.yml`](../../.golangci.yml); existing findings were fixed (unused code
  removed, dead assignments, unchecked errors).

## Documentation

- New: [`docs/integrations/third-party-alert-integrations.md`](../integrations/third-party-alert-integrations.md)
  (ServiceNow, Prometheus, API-only vs. full-stack, PATs, sync, errors).
- [`README.md`](../../README.md) rewritten. Corrected instructions: a scan is
  `ncc-orchestrator --config <file>` (there is no `run` subcommand); release
  archives have no top-level folder; the bundled template refers to four
  secrets; Docker Compose serves the UI over HTTP.
- Updated: `Prometheus.md`, `docs/FEATURES_AND_CONFIG_FLAGS.md`,
  `docs/SECURITY_AND_TRUST.md`, Kubernetes `secret.yaml` instructions; Go
  requirement is 1.27.3+.

## Configuration and migration

| Change | Action |
| --- | --- |
| Backup encryption required by default | Set `NCC_BACKUP_KEY_FILE` (recommended), `NCC_BACKUP_KEY`, or `NCC_BACKUP_PASSPHRASE` on the API server — or enter a passphrase per backup. In Kubernetes add `backup-key` to `ncc-v2-secrets`. To keep plaintext backups, set `NCC_BACKUP_ENCRYPTION=optional` explicitly. **In-app updates refuse to run without a server backup key** (the pre-update backup must be encrypted). |
| Alert marks storage | Automatic one-time import from `outputfiles/ncc-alert-dispositions.json`; no action. |
| Trusted proxies | Default `private,loopback` keeps v2.2.0 behavior; narrow `--trusted-proxy-cidrs` / `NCC_TRUSTED_PROXY_CIDRS` (Helm `api.trustedProxyCIDRs`). |
| Health consumers | Scripts that read paths from `/api/v1/health` must authenticate as admin. |
| Email notifications | Make sure `from` and every `to` address are valid email addresses (`name@example.com` or `Name <name@example.com>`); invalid ones now fail the send. |
| Admin password reset | The new password is in `.ncc-initial-admin-password` (or the Kubernetes Secret), no longer in the server log. |
| Manual UI health checks | `ncc-ui-server --health-check` over HTTPS needs the real `--tls-cert-file`/`--tls-key-file` (or `--auto-tls-dir`) the UI serves; placeholder paths now fail. The stack supervisor already passes the real paths. |

### Breaking changes

- Plaintext backups are refused unless a key/passphrase is configured or
  `NCC_BACKUP_ENCRYPTION=optional` is set.
- `/api/v1/health` returns fewer fields to anonymous callers.
- Run rows' `path` is relative (`runs/<id>`) instead of absolute.
- Invalid email sender/recipient addresses fail the send.
- Admin password resets are no longer printed in the server log.

## Upgrade

1. Configure a backup key (see above) **before** using the in-app updater.
2. Upgrade as usual (in-app update, `ncc-orchestrator update`, or new images /
   `helm upgrade`).
3. Kubernetes: rebuild/pull the 2.2.1 images; optionally add `backup-key` and
   `master-key` to the secret.

## Rollback

- The previous `outputfiles/ncc-alert-dispositions.json` stays current, so
  v2.2.0 still sees today's marks; the new history and alert ids are ignored
  by v2.2.0 and reused if you upgrade again.
- Encrypted backups need the same key/passphrase to restore with any version.
- v2.2.0 does not know `NCC_BACKUP_ENCRYPTION` and will create plaintext
  backups again.

## Verification

- CI on pull request #10: build, vet and race tests; golangci-lint;
  govulncheck; frontend tests, `npm audit --omit=dev`, and build; Trivy; and
  CodeQL (Go and Actions) all pass.
- Linux (`golang:1.27.3`): `go test -race ./...` passes in every package.
- Node.js 26.12.1 (`node:26.12.1-alpine`): `npm ci`, `npm audit --omit=dev`
  (0 vulnerabilities), 45/45 frontend tests, `tsc -b && vite build`.
- Containers: the published API image builds, starts healthy, and its CA
  bundle matches its package; `docker compose up --build` starts both
  services healthy, and the UI serves the app and proxies the API.
- Windows: the updater tests, including replacing a running `.exe`, pass when
  run natively on Windows. The stack supervisor keeps a healthy TLS UI running
  (`TestSuperviseKeepsHealthyTLSUIRunning`, Windows and Linux).

## Known issues and residual risks

- Windows-only updater tests run only on a Windows host; there is no Windows
  CI job, and an end-to-end in-app update on a real Windows install has not
  been performed.
- The bundled `example_config.yaml` resolves all four `secret://` references
  (`NCC_PASSWORD`, `SMTP_PASSWORD`, `WEBHOOK_HMAC`, `SLACK_WEBHOOK_URL`) even
  when email, webhook, or Slack are off; export them or delete the blocks you
  do not use.
- When the API is unreachable, the UI server can take about 10 s to start
  listening (backend login detection); a UI restarted during an API outage is
  unavailable for that time.
- The UI server's API proxy still uses `httputil.ReverseProxy.Director`
  (deprecated in favor of `Rewrite`); it works, and the migration is planned
  separately because `Rewrite` changes forwarding-header handling.
- Existing plaintext backups are not re-encrypted automatically.
- Kubernetes Secret encryption at rest depends on cluster configuration.
