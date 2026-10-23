# NCC Orchestrator — v2.2.1

**Release status:** Release candidate. Planned date: set when the `v2.2.1` tag is pushed (the release workflow publishes on tag push).

> **Affiliation:** This is an independent open-source project. It is not
> affiliated with or endorsed by Nutanix, Inc. The project is MIT licensed.

v2.2.1 is a maintenance and security release on top of v2.2.0. It fixes alert
acknowledgements that disappeared after new reports, the "—" run status, and
in-app upgrades on Windows, adds a persistent alert history API, and hardens
backups, login, and API responses.

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
- Retention: `--alert-history-retain-days` (default 365) for closed
  instances; active alerts are never pruned.
- Prometheus: `ncc_alerts_active{state,detection,severity}`,
  `ncc_alerts_store_up`, `ncc_alerts_last_ingested_run_timestamp_seconds`,
  `ncc_alerts_events_total`.

## Security

Details and evidence: [`docs/security/security-assessment-2026-10.md`](docs/security/security-assessment-2026-10.md).

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
- Build toolchain pinned to Node.js 26.12.1 (CVE-2026-111497); images upgrade
  `ca-certificates` at build (eMudhra intermediate CA distrust).

October 2026 incidents (PHP2Shell, Node.js CVE-2026-111497, GitLab
CVE-2026-110264, Railway, Zeabur, eMudhra, LiteLLM/OpenCode, Double Counter)
are assessed individually in the security assessment.

## Performance and reliability

- gzip for API JSON responses (honors `Accept-Encoding`, skips small and
  already-encoded bodies). Measured on a synthetic 3,000-row report payload:
  576,428 → 13,770 bytes. Real payloads compress less; no production
  measurement was taken.
- Audit log keeps `--audit-log-keep` rotated generations (default 10;
  previously 1, so every rotation destroyed the prior history).
- Leftovers of earlier updates are cleaned up on the next update.

## Documentation

- New: [`docs/integrations/third-party-alert-integrations.md`](docs/integrations/third-party-alert-integrations.md)
  (ServiceNow, Prometheus, API-only vs. full-stack, PATs, sync, errors).
- Updated: `Prometheus.md`, `docs/FEATURES_AND_CONFIG_FLAGS.md`, `README.md`,
  Kubernetes `secret.yaml` instructions.

## Configuration and migration

| Change | Action |
| --- | --- |
| Backup encryption required by default | Set `NCC_BACKUP_KEY_FILE` (recommended), `NCC_BACKUP_KEY`, or `NCC_BACKUP_PASSPHRASE` on the API server — or enter a passphrase per backup. In Kubernetes add `backup-key` to `ncc-v2-secrets`. To keep plaintext backups, set `NCC_BACKUP_ENCRYPTION=optional` explicitly. **In-app updates refuse to run without a server backup key** (the pre-update backup must be encrypted). |
| Alert marks storage | Automatic one-time import from `outputfiles/ncc-alert-dispositions.json`; no action. |
| Trusted proxies | Default `private,loopback` keeps v2.2.0 behavior; narrow `--trusted-proxy-cidrs` / `NCC_TRUSTED_PROXY_CIDRS` (Helm `api.trustedProxyCIDRs`). |
| Health consumers | Scripts that read paths from `/api/v1/health` must authenticate as admin. |

### Breaking changes

- Plaintext backups are refused unless a key/passphrase is configured or
  `NCC_BACKUP_ENCRYPTION=optional` is set.
- `/api/v1/health` returns fewer fields to anonymous callers.
- Run rows' `path` is relative (`runs/<id>`) instead of absolute.

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

## Known issues and residual risks

- Frontend type check, unit tests, and production build were not run in the
  environment that prepared this release (npm registry unreachable); they run
  in CI (`ci.yml`, `release.yml`) and must pass before tagging.
- The Windows file-replacement behavior is verified with a simulation on Linux
  and by `update_install_windows_test.go`, which runs only on the new
  `windows-latest` CI job; an end-to-end in-app update on a real Windows host
  has not been performed.
- Existing plaintext backups are not re-encrypted automatically.
- Kubernetes Secret encryption at rest depends on cluster configuration.
- `govulncheck`, Trivy, and `npm audit` were not run locally; they run in CI.
