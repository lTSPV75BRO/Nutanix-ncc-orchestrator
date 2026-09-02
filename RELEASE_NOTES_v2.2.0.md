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
