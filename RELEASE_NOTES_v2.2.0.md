# NCC Orchestrator — v2.2.0

**Release status:** Unreleased development release

> **Affiliation:** This is an independent open-source project. It is not
> affiliated with or endorsed by Nutanix, Inc. The project is MIT licensed.

## Prism Central alerts

v2.2.0 adds Prism Central serviceability alerts to the dashboard. The API reads
configured `pcs` / `prism-central-url` targets using the active configuration's
credentials and queries:

`/api/monitoring/{nutanix-v4-api-version}/serviceability/alerts`

Requests use paginated `$page` / `$limit` parameters, server-side unresolved
filtering by default (`$filter=isResolved eq false`), the existing Prism TLS
settings, bounded retries, concurrent target fetching, and a configurable
server-side cache. The dashboard displays unresolved alerts first and loads
the complete alert history in the background. Partial target failures are
reported without hiding NCC findings.

The new viewer-accessible `GET /api/v1/alerts` endpoint returns normalized PC
alert rows with severity, cluster, status, timestamps, detail, and KB
metadata. Cluster-group restrictions are applied when a PC alert identifies a
configured cluster by name, UUID, or IP.

PC alerts that only carry a cluster UUID are resolved through Prism Central
discovery (`ext_id`, name, and address). The response includes a `cluster_map`
dictionary so the dashboard shows the cluster name and IP instead of the UUID.
Access checks match any of those identities.

## Dashboard source selector

The Alerts table now supports:

- **NCC** — findings from the latest NCC report
- **PC** — live Prism Central alerts

The selection is URL-persisted and preserves existing severity, search,
cluster, changed, flaky, and resolved-status filters. The redundant Source
column is omitted; NCC and PC keep separate table layouts. PC cluster names
are resolved from discovery while links still target the mapped IP on port
`9440`.

## API filtering and compatibility

`GET /api/v1/alerts` accepts `resolved=No`, `resolved=Yes`, or
`resolved=all`. The default is `No`, minimizing the initial payload. The
frontend uses the unresolved response immediately and requests `all` as a
background cache warm-up for the resolved/all-status views. The cache TTL is
configurable with `pc-alerts-cache-ttl` (default `5m`; `0` disables caching).

## Compatibility

No existing NCC report or configuration keys are removed. Prism Central alert
retrieval is enabled when an existing `pcs` or `prism-central-url` target is
configured; otherwise the dashboard continues to show NCC findings normally.

## VM provisioning

The release includes [`deploy/README.md`](deploy/README.md) with cloud-init
and Windows Sysprep templates. They download and checksum-verify the selected
`ncc-v2-stack` archive, install the full CLI/API/UI stack, create configuration
and output locations, enable boot startup, and leave credential placeholders
for secure image or first-boot secret injection.

## Unified logging

The VM templates route runner, API, UI, scheduler, and supervisor output below
the install's `logs/` directory. Supervised API/UI logs rotate at 50 MiB,
retain five compressed backups for up to 30 days, and the existing
`doctor --fix` log-size check can rotate any oversized `logs/*.log` file.

## Canonical configuration and self-heal

`example_config.yaml` now uses schema version 1 with deployment-neutral nested
sections for runner, storage, API, UI, deployment, logging, and
notifications. Legacy flat keys remain supported through normalization.
`validate-config`, `preflight-check`, and `doctor` share the same schema and
secret validation path. `doctor --fix` can safely add a missing schema version,
while unsupported versions fail closed.

The Settings UI edits canonical nested values and, in Kubernetes, persists the
active configuration on the shared PVC so API and CronJob executions use the
same file.

## Stateless hybrid authentication

v2.2.0 API replicas authenticate without a local session file. Interactive
sessions are HMAC-SHA256 JWTs signed with `NCC_JWT_SECRET` (cookie `auth_token`
or `Authorization: Bearer`). Personal access tokens remain `ncc_pat_` secrets
whose SHA-256 hashes live in the shared user store. Set the same
`NCC_JWT_SECRET` (and `NCC_API_TOKEN` / `NCC_API_STATIC_TOKEN`) on every
replica. On a host/VM install, if the JWT secret is omitted the process
generates one in memory and logs a warning that sessions will not survive a
restart or another replica. **Kubernetes requires `jwt-secret`** in
`ncc-v2-secrets` (or the Helm `secretName`); the API refuses to start without
it so two pods cannot mint incompatible session cookies.

The user store reloads from its Kubernetes Secret every two seconds so account,
group, and PAT changes propagate across API replicas. Scheduled backups take an
advisory lock on the shared PVC so only one replica snapshots at a time.
Archives are written under `/data/backups` and include the PVC `config/`,
`auth/`, and `logs/` layout.

The UI sends cookies with every request (`credentials: include`) and treats a
401 on a protected route as a sign-out (redirect to `/login`). Personal access
tokens are managed from the header user menu against `/api/v1/users/me/pats`;
the plaintext `ncc_pat_…` value is shown once. Kubernetes UI pods use
`--login-mode on` so every replica forwards the browser session instead of
injecting the shared admin token. `NCC_CORS_ORIGIN` overrides `--cors-origin`
when the browser origin is not the default.

## Kubernetes operations

Kubernetes deployments serve HTTPS from the UI pods (`--auto-tls-dir
/data/tls`), the same self-signed-by-default model as Linux. Settings →
Access → HTTPS / TLS generate/upload writes `/data/tls/ui.crt`+`ui.key`;
pods hot-reload. Revert restores the self-signed pair. Session cookies are
marked `Secure`. Optional Ingress is TLS passthrough to that certificate.

System Health runs PVC-safe doctor checks (config, storage, secrets, backups,
runs, logs) plus API probes for JWT, the user Secret, Secure cookies, the UI
TLS files on `/data/tls`, and a writable `/data` volume. Host supervisor, PID, SELinux, and
in-process TLS-file checks are omitted because Deployments and the runner
CronJob own process lifecycle.

Installed-component versions come from image-tag environment variables
(`NCC_IMAGE_TAG`, `NCC_ORCHESTRATOR_IMAGE_TAG`, `NCC_UI_IMAGE_TAG`) rather
than looking for `ncc-ui-server` inside the API image. Matching tags are
consistent; mismatched tags warn you to roll API, UI, and runner images
together. In-place binary updates remain disabled on Kubernetes.

Default API replicas are **2** (`k8s/api-deployment.yaml` and Helm
`api.replicas`). Provision `jwt-secret` before applying, then rebuild and
roll the API/UI images.
