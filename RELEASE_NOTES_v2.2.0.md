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
alert rows with source, severity, cluster, status, timestamps, detail, and KB
metadata. Cluster-group restrictions are applied when a PC alert identifies a
configured cluster.

## Dashboard source selector

The Alerts table now supports:

- **NCC** — findings from the latest NCC report
- **PC** — live Prism Central alerts

The selection is URL-persisted, source-labelled in the table and details
drawer, and preserves existing severity, search, cluster, changed, flaky, and
resolved-status filters. PC cluster names remain readable while links resolve
through the configured cluster-name-to-IP mapping.

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
replica; if the JWT secret is omitted the process generates one in memory and
logs a warning that sessions will not survive a restart or another pod.

The UI sends cookies with every request (`credentials: include`) and treats a
401 on a protected route as a sign-out (redirect to `/login`). Personal access
tokens are managed from the header user menu against `/api/v1/users/me/pats`;
the plaintext `ncc_pat_…` value is shown once. On Linux systemd, Windows
services, or extra API replicas, set the same `NCC_JWT_SECRET` (and
`NCC_API_TOKEN` / `NCC_API_STATIC_TOKEN`) in the process environment so
sessions and automation tokens verify everywhere. `NCC_CORS_ORIGIN` overrides
`--cors-origin` when the browser origin is not the default.
