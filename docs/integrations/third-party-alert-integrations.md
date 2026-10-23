# Third-party alert integrations (ServiceNow, Prometheus, and others)

Applies to NCC Orchestrator **v2.2.1** and later. Everything in this guide is
taken from the shipped API (`cmd/ncc-api-server`). Where an integration needs
something the product does not provide yet, the section says so and labels it
**Proposed enhancement**.

Contents

1. [Architecture and integration options](#1-architecture-and-integration-options)
2. [Deployment modes: API-only and full stack](#2-deployment-modes)
3. [Authentication](#3-authentication)
4. [Least-privilege access](#4-least-privilege-access)
5. [Alert, history, and run endpoints](#5-endpoints)
6. [Alert identity and lifecycle](#6-alert-identity-and-lifecycle)
7. [Polling, incremental sync, rate limits, retries](#7-polling-incremental-sync-rate-limits-retries)
8. [Errors and troubleshooting](#8-errors-and-troubleshooting)
9. [TLS and network](#9-tls-and-network)
10. [ServiceNow](#10-servicenow)
11. [Prometheus](#11-prometheus)
12. [Versioning and compatibility](#12-versioning-and-compatibility)

---

## 1. Architecture and integration options

```text
                 Nutanix clusters / Prism Central
                              │
                ncc-orchestrator (runner / CronJob)
                              │ writes run-summary.json + report
                              ▼
          ncc-api-server  ── /api/v1/alerts/ncc  (alert instances, lifecycle state)
          (port 8081)     ── /api/v1/alerts/ncc/events (append-only history feed)
                          ── /api/v1/runs, /api/v1/report/data (run results)
                          ── /api/v1/alerts (Prism Central alerts, proxied)
                          ── /metrics (Prometheus text format)
                              │
              ┌───────────────┼────────────────────┐
              ▼               ▼                    ▼
        ServiceNow       Prometheus /         any REST client
        (MID Server      Alertmanager         (SIEM, ticketing,
         + script)                             chat-ops)
```

| Option | Status | Direction | Use for |
| --- | --- | --- | --- |
| REST polling of `/api/v1/alerts/ncc` + `/events` | **Exists** | pull | Ticket sync (ServiceNow incidents/events), SIEM |
| Write-back `POST /api/v1/alerts/ncc/{id}/{acknowledge,resolve,reopen}` | **Exists** | push into NCC | Close the loop when a ticket changes |
| Prometheus scrape of `/metrics` | **Exists** | pull | Dashboards, Alertmanager rules on aggregate state |
| Run-summary notifications (email, generic webhook with HMAC signature, Slack) | **Exists** | push | Per-run summaries; *not* per-alert lifecycle events |
| Per-alert lifecycle webhooks (push each `acknowledged`/`resolved`/`recurred` event) | **Proposed enhancement** | push | Event-driven ServiceNow without polling |
| Alertmanager-compatible webhook receiver in NCC | **Proposed enhancement** | — | Not implemented |

Prism Central alerts (`GET /api/v1/alerts`, source `PC`) keep their own
acknowledged/resolved flags inside Prism Central and are **not** part of the
NCC lifecycle store described below.

## 2. Deployment modes

### API-only mode

Only `ncc-api-server` runs (no UI server). Integrations call it directly:

```text
https://<api-host>:8081/api/v1/...
```

* Start it with `--auth-mode token` (or `hybrid`) and an admin token
  (`NCC_API_TOKEN`), or enable local accounts so you can issue personal access
  tokens (section 3).
* Serve TLS with `--tls-cert-file` / `--tls-key-file`, or put it behind a
  TLS-terminating reverse proxy.

### Full-stack mode (host/VM)

`ncc-ui-server` (browser UI, default `:8080`/HTTPS) proxies `/api/v1/*` to the
API server, **but it removes `Authorization` and `X-API-Token` headers from
proxied requests** (it authenticates browser sessions, not integrations).
Third-party systems must therefore call the **API server directly**
(`:8081`), not the UI URL.

### Full-stack mode on Kubernetes

* The API Service is `ncc-v2-api` (ClusterIP, port 8081). The shipped
  `NetworkPolicy` `ncc-v2-api-ingress-from-ui` only admits traffic from the UI
  pods.
* **In-cluster clients** (for example Prometheus): add a NetworkPolicy rule
  that admits the client's namespace/pods to port 8081, then use
  `http://ncc-v2-api.ncc-orchestrator-v2.svc:8081`.
* **Clients outside the cluster** (for example a ServiceNow MID Server):
  publish the API through a dedicated Ingress/LoadBalancer with TLS, restrict
  source IPs at the Ingress, and add a matching NetworkPolicy rule. Do not
  route integrations through the UI Ingress (headers are stripped, see above).
* Several API replicas share the PVC. Alert lifecycle state, login throttling
  counters, and backups are coordinated through file locks on the shared
  volume, so any replica can serve any request.
* Set `NCC_TRUSTED_PROXY_CIDRS` (Helm: `api.trustedProxyCIDRs`) to the
  ingress/UI pod CIDRs so per-client rate limits see real client addresses.

## 3. Authentication

All API requests carry a credential in **one** of these headers:

```http
Authorization: Bearer <token>
X-API-Token: <token>
```

| Credential | Format | Role | Lifetime | Recommended for integrations |
| --- | --- | --- | --- | --- |
| Personal access token (PAT) | `ncc_pat_<64 hex>` | role of the account that created it | 1–365 days (default 90), or non-expiring if created with `expires_in_days <= 0` | **Yes** |
| Static admin token | `NCC_API_TOKEN` / token file | admin | until rotated | Bootstrap only |
| Static viewer token | `NCC_API_VIEWER_TOKEN` | viewer | until rotated | Read-only scrapers when no user store exists |
| Browser session (cookie + CSRF) | `auth_token` / `ncc_session` cookies | user's role | session TTL (default 6 h) | No (interactive only) |

PATs are stored only as SHA-256 hashes; the plaintext is shown once.

### Create a PAT for an integration

1. As an admin, create a dedicated local account for the integration
   (Settings → Access → Users), for example `svc-servicenow`, with the lowest
   role that works (section 4).
2. Sign in as that account (or use an existing session for it) and create the
   token:

   ```http
   POST /api/v1/auth/tokens
   Content-Type: application/json

   {"name": "servicenow-prod", "expires_in_days": 90}
   ```

   ```json
   {
     "success": true,
     "message": "store this token now — it will not be shown again",
     "data": {
       "id": "Zq3mY2Vk9hT1",
       "name": "servicenow-prod",
       "role": "operator",
       "token": "ncc_pat_9f2c…(64 hex)…",
       "created_at": "2026-10-23T09:00:00Z",
       "expires_at": "2027-01-21T09:00:00Z"
     }
   }
   ```

3. Store the token in the consumer's secret store (ServiceNow: a Basic/Bearer
   credential record or Connection & Credential alias; Prometheus: a mounted
   file referenced by `authorization.credentials_file`). Never commit it.

### Rotate and revoke

* **Rotate:** create a new token, switch the consumer to it, verify, then
  revoke the old one. Overlapping validity avoids downtime.
* **Revoke your own token:** `DELETE /api/v1/auth/tokens/{id}`.
* **List your tokens:** `GET /api/v1/auth/tokens` (metadata only).
* **Admin view of all tokens:** `GET /api/v1/settings/tokens`,
  revoke any with `DELETE /api/v1/settings/tokens/{id}` (admin).
* Deleting the owning local account, or forcing it to change its password,
  stops its tokens from working; role changes apply to its tokens immediately.

Every create/revoke is written to the audit log (`auth.token.create`,
`auth.token.revoke`).

## 4. Least-privilege access

| Integration need | Minimum role | Endpoints |
| --- | --- | --- |
| Read alerts, history, runs, metrics | **viewer** | `GET /api/v1/alerts/ncc*`, `GET /api/v1/runs*`, `GET /api/v1/report/data`, `GET /metrics` |
| Acknowledge / resolve / reopen NCC alerts from the ticket | **operator** | `POST /api/v1/alerts/ncc/{id}/{action}`, `POST /api/v1/alerts/ncc-dispositions` |
| Anything under `/api/v1/settings/*` | admin | Do not grant to integrations |

**Cluster groups** apply to integrations exactly as to users: a non-admin
account that is a member of cluster groups only sees and can only act on
alerts for those clusters (out-of-scope alerts return `404`). Use this to give
a team's ServiceNow instance only its clusters.

## 5. Endpoints

All responses use one envelope:

```json
{"success": true, "data": { … }, "message": "optional", "error": "only on failure", "error_code": "only on failure"}
```

Timestamps are RFC 3339 UTC (alert fields use nanosecond precision).

### 5.1 `GET /api/v1/alerts/ncc` — alert instances (viewer)

| Query parameter | Meaning |
| --- | --- |
| `state` | `open`, `acknowledged`, or `resolved` |
| `detection` | `detected`, `not_detected`, or `unknown` |
| `cluster` | cluster address or name (case-insensitive) |
| `check` | NCC check title (the `Detailed information for …:` wrapper is ignored) |
| `finding_id` | stable finding identity (section 6) |
| `updated_since` | RFC 3339; only instances updated at/after this time |
| `include_closed` | `true` to include closed (historical) instances |
| `limit` | 1–1000, default 100 |
| `cursor` | `next_cursor` from the previous page |

Results are ordered by `(updated_at, id)` ascending, so paging with `cursor`
is stable even while alerts change.

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "id": "3f6b1c2e-8d4a-4f0b-9a51-2b7e9c0d4e11",
        "finding_id": "f_5c0e8a3b9d21f4a6e7c1b2d3",
        "correlation_key": "ncc|10.0.0.21|cvm memory usage check",
        "source": "NCC",
        "type": "ncc_check",
        "cluster": "10.0.0.21",
        "cluster_name": "prod-cluster-a",
        "check": "CVM memory usage check",
        "severity": "FAIL",
        "state": "acknowledged",
        "detection": "detected",
        "closed": false,
        "first_seen_at": "2026-10-20T10:00:00Z",
        "first_seen_run": "2026-10-20T10:00:00Z",
        "last_seen_at": "2026-10-23T10:00:00Z",
        "last_seen_run": "2026-10-23T10:00:00Z",
        "acknowledged_at": "2026-10-20T10:15:12.481Z",
        "acknowledged_by": "jdoe",
        "note": "INC0012345",
        "created_at": "2026-10-20T10:01:03.112Z",
        "updated_at": "2026-10-23T10:01:02.904Z",
        "revision": 6,
        "last_actor": "jdoe",
        "last_state_change_at": "2026-10-20T10:15:12.481Z"
      }
    ],
    "count": 1,
    "next_cursor": "",
    "last_run": "2026-10-23T10:00:00Z"
  }
}
```

Additional fields that appear when relevant: `resolved_at`, `resolved_by`,
`resolved_run`, `not_detected_since_run`, `reopened` + `reopen_reason`,
`previous_id` / `next_id` (links between episodes), `closed_at`,
`close_reason` (`recurred` or `resolved_not_detected`), `legacy_migrated`.

### 5.2 `GET /api/v1/alerts/ncc/events` — lifecycle event feed (viewer)

Append-only, ordered by `seq` (monotonic per installation).

| Query parameter | Meaning |
| --- | --- |
| `cursor` | the `next_cursor` (a `seq` number) from the previous call; omit for the beginning |
| `limit` | 1–5000, default 500 |
| `alert_id` | only events of one instance |

```json
{
  "success": true,
  "data": {
    "items": [
      {"id": "b5d7…", "alert_id": "3f6b1c2e-…", "correlation_key": "ncc|10.0.0.21|cvm memory usage check",
       "type": "detected", "at": "2026-10-20T10:01:03.112Z", "actor": "NCC Orchestrator",
       "run_at": "2026-10-20T10:00:00Z", "to_state": "open", "to_severity": "FAIL", "seq": 41},
      {"id": "0c2a…", "alert_id": "3f6b1c2e-…", "correlation_key": "ncc|10.0.0.21|cvm memory usage check",
       "type": "acknowledged", "at": "2026-10-20T10:15:12.481Z", "actor": "jdoe",
       "from_state": "open", "to_state": "acknowledged", "note": "INC0012345", "seq": 42}
    ],
    "count": 2,
    "next_cursor": "42",
    "has_more": false
  }
}
```

Event types: `detected`, `redetected`, `not_detected`, `severity_changed`,
`acknowledged`, `resolved`, `reopened`, `note_updated`, `recurred`, `closed`,
`migrated`.

### 5.3 `GET /api/v1/alerts/ncc/{id}` — one instance with history (viewer)

Returns `{"alert": {…}, "events": [ … ], "instances": [ {id, state, created_at, closed_at, close_reason}, … ]}`
where `instances` lists earlier/later episodes of the same finding.

### 5.4 `POST /api/v1/alerts/ncc/{id}/{acknowledge|resolve|reopen}` (operator)

Optional body `{"note": "INC0012345 – vendor case 789"}` (max 500 characters,
`Content-Type: application/json`). Returns the updated instance.

* Idempotent: repeating the current state with the same note records nothing.
* Acting on a **closed** instance returns `409` with the id of its successor.
* Unknown or out-of-scope id returns `404`.

### 5.5 Compatibility endpoint `GET|POST /api/v1/alerts/ncc-dispositions`

The v2.2.0 endpoint keeps its request/response shape (cluster + check keyed)
and now also returns `id`, `finding_id`, `detection`, `severity`,
`first_seen_at`, `last_seen_at`. `POST` additionally accepts `id` (or `id` in
each `items[]` entry). New integrations should use 5.1–5.4.

### 5.6 Runs and reports (viewer)

| Endpoint | Notes |
| --- | --- |
| `GET /api/v1/runs?limit=&source=history\|summary\|trigger&since=` | Each row has `status`: `success`, `partial` (some clusters failed), `failed`, `triggered`, `trigger_failed`, or `unknown`, plus `status_source` (`exit_code`, `legacy_summary`, `audit`, `none`). |
| `GET /api/v1/runs/{id}` | One archived run with its artifacts. |
| `GET /api/v1/runs/active` | In-flight and queued runs. |
| `GET /api/v1/runs/summary` | Latest `run-summary.json`. |
| `GET /api/v1/report/data?limit=&offset=` | Latest report (`agg_rows`, `checks_snapshot` paginated with `limit` ≤ 5000 + `offset`). |
| `GET /api/v1/alerts?resolved=no\|yes\|all&refresh=1` | Prism Central alerts (cached; `refresh=1` bypasses the cache). |

Paths in these responses are relative to the stack root for non-admin
callers; absolute filesystem paths are only returned to admins.

## 6. Alert identity and lifecycle

| Field | Stable across runs? | Use |
| --- | --- | --- |
| `id` (UUIDv4) | Per **instance** (episode) | Primary key for ticket correlation and write-back |
| `finding_id` | Yes, per cluster + check | Group episodes of the same underlying problem; dedupe |
| `correlation_key` | Yes (human readable) | Diagnostics; do not parse |

* IDs are generated server-side from a CSPRNG and checked for uniqueness in
  the store; they are never reused, and a new episode never overwrites an old
  one.
* **Operator state** (`open` / `acknowledged` / `resolved`) is separate from
  **detection** (`detected` / `not_detected`). A finding that disappears from a
  run is `not_detected`; it is *not* treated as resolved.

Lifecycle rules applied when a new run is ingested (idempotent per run;
older or duplicate runs are ignored):

| Situation | Result |
| --- | --- |
| New finding | New instance, `state=open`, event `detected` |
| Still reported | `last_seen_*` updated; acknowledged/resolved kept |
| Severity changes | Event `severity_changed`; an **escalation** (e.g. WARN → FAIL) reopens an acknowledged instance (`reopened=true`) |
| Not reported by a run that checked that cluster | `detection=not_detected`, event `not_detected`; state unchanged |
| Reported again later | Event `redetected`, same instance, acknowledgement kept |
| **Resolved**, then reported by a newer run | Resolved instance is **closed** (`close_reason=recurred`), a **new** open instance is created with `previous_id` → old id and `reopened=true` (event `recurred`) |
| **Resolved**, then a newer run confirms it is gone | Instance closed (`close_reason=resolved_not_detected`) |
| Cluster unreachable / not part of the run | No change (a partial run never hides other clusters' alerts) |

History retention: closed instances and their events are kept for
`--alert-history-retain-days` (default 365); active instances are never
pruned.

## 7. Polling, incremental sync, rate limits, retries

**Recommended polling**

* Runs complete every few hours at most; poll alert changes every **5 minutes**
  (polling faster gains nothing: a completed run is ingested on the next API
  request or by the background sync within a minute).
* Use the **event feed** for incremental sync: persist `next_cursor` after you
  have durably processed a page, and resume from it. Because `seq` is
  monotonic and events are append-only, at-least-once processing is easy:
  make your handler idempotent on `event.id`.
* Alternatively use `GET /api/v1/alerts/ncc?updated_since=<last high-water mark>`
  and page with `cursor`; keep the high-water mark from the last item's
  `updated_at`.
* Run a periodic **full reconciliation** (e.g. nightly, `include_closed=false`)
  to repair drift.

**Rate limits**

* Read endpoints used for sync are not per-client rate limited.
* Alert write-backs (`POST /api/v1/alerts/ncc…`), login, token, backup,
  restore, and update endpoints are limited per client IP (default 60/min,
  `--rate-limit-per-minute`). Over the limit the API returns `429` with a
  `Retry-After` header and `error_code: NCC_API_RATE_LIMITED`.

**Timeouts and retries**

* Client timeout: 30 s for list calls.
* Retry `429`, `502`, `503`, `504`, and network errors with exponential
  backoff and jitter (for example 2 s, 4 s, 8 s, … capped at 5 min), honoring
  `Retry-After`. `503` can occur briefly when another replica holds the alert
  store lock.
* Do not retry `400`, `401`, `403`, `404`, `409`, `412` without fixing the
  request.
* Responses are gzip-compressed when the client sends
  `Accept-Encoding: gzip`.

## 8. Errors and troubleshooting

| HTTP | `error_code` | Meaning / action |
| --- | --- | --- |
| 400 | `NCC_API_BAD_REQUEST` | Invalid parameter (e.g. bad `state`, malformed `cursor`). |
| 401 | `NCC_API_UNAUTHORIZED` (body may be `{"error":"Unauthorized"}`) | Missing/invalid/expired token. Check the header and token expiry. |
| 403 | `NCC_API_FORBIDDEN` | Role too low (e.g. viewer trying to acknowledge). |
| 404 | `NCC_API_NOT_FOUND` | Unknown alert id, or alert outside the caller's cluster groups. |
| 409 | `NCC_API_CONFLICT` | Acting on a closed alert instance; use `next_id`. |
| 415 | `NCC_API_UNSUPPORTED_MEDIA_TYPE` | Send `Content-Type: application/json`. |
| 429 | `NCC_API_RATE_LIMITED` | Back off per `Retry-After`. |
| 500/503 | `NCC_API_INTERNAL` / `NCC_API_SERVER_ERROR` | Retry with backoff; check `ncc-api-server` logs. |

Troubleshooting checklist

* `401` through the UI URL → call the API server directly (section 2).
* `401` with a valid PAT → the owning account was disabled, or the token
  expired/was revoked (`GET /api/v1/auth/tokens` as that account).
* No alerts listed → `GET /api/v1/runs/summary` must have a `timestamp`; the
  store ingests the latest completed run on the next request or within a
  minute.
* Alerts missing for some clusters → the integration account is in cluster
  groups that do not include those clusters.

## 9. TLS and network

* Always use HTTPS between integrations and the API. Verify the server
  certificate; **never** disable verification. For a private CA, import the
  CA into the client trust store (ServiceNow: the MID Server's Java trust
  store; Prometheus: `tls_config.ca_file`).
* Restrict network access to the API port to the integration hosts.
* After the October 2026 eMudhra intermediate-CA compromise, refresh client
  trust stores/images so revoked certificates are no longer trusted (see
  `docs/security/security-assessment-2026-10.md`).

## 10. ServiceNow

The examples below use only existing NCC endpoints. The ServiceNow side
(tables, field names, MID Server, scheduled jobs) is **illustrative** — adapt
to your instance's data model (Incident vs. Event Management) and naming.

### 10.1 Recommended pattern: scheduled pull via a MID Server

1. Create an NCC local account `svc-servicenow` with role `operator` (or
   `viewer` if you do not write state back) and, optionally, put it in the
   cluster groups that the ServiceNow assignment group owns.
2. Create a PAT for it; store it in a ServiceNow credential.
3. Create a REST Message `NCC Orchestrator` with base URL
   `https://<api-host>:8081/api/v1`, using the MID Server if the API is on a
   private network.
4. A Scheduled Script Execution (every 5 minutes) pulls the event feed and
   upserts incidents.

### 10.2 Field mapping

| NCC field | Incident field (example) | Notes |
| --- | --- | --- |
| `id` | `correlation_id` | **Deduplication key** (one incident per alert instance) |
| `finding_id` | custom `u_ncc_finding_id` | Links recurrences of the same problem |
| `cluster_name` / `cluster` | `cmdb_ci` (lookup) | Map to the cluster CI |
| `check` | `short_description` | Prefix with `NCC:` |
| `severity` | `impact` / `urgency` | e.g. FAIL→1/2, ERR→2/2, WARN→3/3, INFO→ do not open |
| `first_seen_at` | `u_first_seen` / work note | |
| `last_seen_at`, `detection` | work note | |
| `state` | `state` | open→New/In Progress, acknowledged→On Hold or In Progress, resolved→Resolved |
| `previous_id` | `parent_incident` / related list | Link a recurrence to the earlier incident |

### 10.3 Example scheduled script (illustrative)

```javascript
// ServiceNow Scheduled Script Execution (server-side JavaScript).
// Illustrative only: adapt table/field names, error handling, and logging.
(function syncNccAlerts() {
  var CURSOR_PROP = 'x_ncc.events_cursor';
  var cursor = gs.getProperty(CURSOR_PROP, '');
  var more = true;
  while (more) {
    var rm = new sn_ws.RESTMessageV2('NCC Orchestrator', 'events');   // GET /alerts/ncc/events
    rm.setQueryParameter('limit', '500');
    if (cursor) rm.setQueryParameter('cursor', cursor);
    rm.setHttpTimeout(30000);
    var resp = rm.execute();
    var code = resp.getStatusCode();
    if (code === 429 || code >= 500) { gs.warn('NCC sync: retry later, HTTP ' + code); return; }
    if (code !== 200) { gs.error('NCC sync failed: HTTP ' + code + ' ' + resp.getBody()); return; }
    var data = JSON.parse(resp.getBody()).data;
    data.items.forEach(function (evt) { applyEvent(evt); });           // idempotent on evt.id
    cursor = data.next_cursor;
    gs.setProperty(CURSOR_PROP, cursor);                                // commit only after processing
    more = data.has_more;
  }

  function applyEvent(evt) {
    var inc = new GlideRecord('incident');
    inc.addQuery('correlation_id', evt.alert_id);
    inc.query();
    var exists = inc.next();
    if (evt.type === 'detected' || evt.type === 'recurred') {
      if (exists) return;                                               // dedupe
      var alert = fetchAlert(evt.alert_id);                             // GET /alerts/ncc/{id}
      if (!alert || alert.severity === 'INFO') return;
      inc.initialize();
      inc.correlation_id = alert.id;
      inc.short_description = 'NCC: ' + alert.check + ' on ' + (alert.cluster_name || alert.cluster);
      inc.u_ncc_finding_id = alert.finding_id;
      inc.work_notes = 'Detected by NCC run ' + alert.first_seen_run + (alert.previous_id ? ' (recurrence of ' + alert.previous_id + ')' : '');
      inc.insert();
    } else if (exists && evt.type === 'resolved') {
      inc.state = 6; inc.close_notes = 'Resolved in NCC by ' + evt.actor; inc.update();
    } else if (exists && (evt.type === 'reopened' || evt.type === 'redetected')) {
      inc.work_notes = 'NCC: ' + evt.type + (evt.reason ? ' – ' + evt.reason : ''); inc.update();
    } else if (exists && evt.type === 'closed') {
      inc.work_notes = 'NCC instance closed (' + (evt.reason || '') + ')'; inc.update();
    }
  }

  function fetchAlert(id) {
    var rm = new sn_ws.RESTMessageV2('NCC Orchestrator', 'alert');     // GET /alerts/ncc/${id}
    rm.setStringParameterNoEscape('id', id);
    var r = rm.execute();
    return r.getStatusCode() === 200 ? JSON.parse(r.getBody()).data.alert : null;
  }
})();
```

### 10.4 Updating NCC when the incident changes (write-back)

Use a Business Rule (async) on `incident` that calls:

```http
POST /api/v1/alerts/ncc/{correlation_id}/acknowledge     (incident moved to In Progress / On Hold)
POST /api/v1/alerts/ncc/{correlation_id}/resolve         (incident Resolved)
POST /api/v1/alerts/ncc/{correlation_id}/reopen          (incident reopened)
Content-Type: application/json

{"note": "INC0012345 by jdoe"}
```

* Calls are idempotent, so retries after a timeout are safe.
* Guard against echo loops: ignore lifecycle events whose `actor` is the
  integration account (`svc-servicenow`) when you process the feed.
* A `409` means NCC already closed that instance because the issue recurred;
  read `next_id` from `GET /api/v1/alerts/ncc/{id}` and link the new
  incident.

### 10.5 Reopen semantics

If NCC finds a resolved problem again, it opens a **new** instance
(`recurred`) rather than reusing the old id. Create a new incident (or reopen
the old one, if your process prefers) and link both via `previous_id`.

### 10.6 Event Management alternative

If you use ServiceNow Event Management instead of incidents, map each
lifecycle event to an `em_event`: `message_key = alert id`, `node = cluster`,
`type = check`, `severity` from the table above, and send `severity=0`
(clear) for `resolved`/`closed`. Event Management's own deduplication on
`message_key` then matches NCC's instance identity.

## 11. Prometheus

### 11.1 What exists

`ncc-api-server` serves Prometheus text format at `GET /metrics`. Besides the
run and per-cluster metrics documented in [`Prometheus.md`](../../Prometheus.md),
v2.2.1 adds aggregate alert lifecycle gauges:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `ncc_alerts_active` | gauge | `state`, `detection`, `severity` | Active (not closed) NCC alert instances |
| `ncc_alerts_store_up` | gauge | — | `1` if the lifecycle store is readable |
| `ncc_alerts_last_ingested_run_timestamp_seconds` | gauge | — | Run last applied to the lifecycle |
| `ncc_alerts_events_total` | gauge | — | Lifecycle events retained |

Per-alert series are intentionally **not** exported (unbounded cardinality);
use the REST API for per-alert detail.

### 11.2 Scrape configuration

```yaml
scrape_configs:
  - job_name: ncc-api
    scheme: https
    metrics_path: /metrics
    scrape_interval: 60s
    static_configs:
      - targets: ["ncc-api.example.internal:8081"]
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/ncc-viewer-pat   # a viewer PAT
    tls_config:
      ca_file: /etc/prometheus/secrets/ncc-ca.pem
```

`--metrics-public` removes authentication from `/metrics`; only use it on an
isolated network.

### 11.3 Example alerting rules

```yaml
groups:
  - name: ncc
    rules:
      - alert: NCCUnacknowledgedFailures
        expr: sum(ncc_alerts_active{state="open",detection="detected",severity="FAIL"}) > 0
        for: 30m
        labels: {severity: critical}
        annotations:
          summary: "{{ $value }} NCC FAIL findings are open and unacknowledged"
      - alert: NCCRunsStale
        expr: time() - ncc_alerts_last_ingested_run_timestamp_seconds > 2 * 86400
        labels: {severity: warning}
        annotations:
          summary: "No NCC run has been ingested for 2 days"
      - alert: NCCAlertStoreDown
        expr: ncc_alerts_store_up == 0
        for: 10m
        labels: {severity: warning}
```

### 11.4 Identity, resolution, and history: NCC vs Prometheus

| Concept | NCC | Prometheus / Alertmanager |
| --- | --- | --- |
| Identity | UUID per alert instance + stable `finding_id` | Label set of the firing series |
| Resolution | Operator action (`resolved`), confirmed or reopened by later runs | Automatic when the expression stops matching |
| Acknowledgement | Persistent operator state with actor and note | Silences (time-bounded, label-matched) |
| History | Append-only event log, retained (default 365 days) | Not retained by Alertmanager; TSDB keeps samples only |

Because Prometheus rules operate on aggregate gauges, a Prometheus alert
"resolves" when counts drop — for example when someone acknowledges the last
open FAIL — not when the underlying NCC finding is fixed. Use the REST API
(or ServiceNow) for per-finding tracking.

### 11.5 Exporter for per-alert labels (Proposed enhancement)

If you need one series per alert, write a small exporter that polls
`/api/v1/alerts/ncc` and emits `ncc_alert_info{id,cluster,check,state,severity} 1`.
This is not shipped; consider label cardinality before doing so.

## 12. Versioning and compatibility

* All endpoints are under `/api/v1`. Within v1, changes are additive: new
  fields and endpoints may appear; existing fields keep their meaning.
  Clients must ignore unknown fields.
* Breaking changes would ship under a new prefix (`/api/v2`) with an overlap
  period.
* `GET /api/v1/meta/routes` and `GET /api/v1/openapi.json` (both
  unauthenticated) list every route with its minimum role.
* v2.2.1 behavior changes relevant to integrations:
  * `/api/v1/health` returns build/runtime details only to authenticated
    callers and filesystem paths only to admins.
  * Run rows include explicit `status` / `status_source`.
  * `path`-like fields in run/report responses are relative for non-admins.
