# API, integration and operational reliability; observability

## Fixed in this audit

### REL-01 — Long operations were cut off by HTTP and client timeouts · Medium · P1 · verified · fixed

| Operation | Server work | API server limit (before) | UI proxy limit (before) | Browser client (before) |
| --- | --- | --- | --- | --- |
| Preflight | up to 120 s | write 60 s | write 60 s | **30 s** |
| Backup create / verify / download, support bundle | up to 2 min | write 60 s | write 60 s | **30 s** (create/verify) |
| Restore (named) | up to 3 min | write 60 s | write 60 s | **30 s** |
| Restore (upload, ≤50 MiB) | upload + 3 min | **read 15 s** for the whole body | read 15 s | none |

The operation kept running server-side while the user saw "did not answer in
time" or a dropped connection, and could retry an operation that had in fact
succeeded. Fix: `extendDeadlines` in each long handler (per-request
read/write deadlines via `http.ResponseController`; the gzip middleware gained
`Unwrap` so the controller can reach the connection), matching per-path
deadlines in the UI proxy, and `LONG_CALL_TIMEOUT_MS` in the frontend client.
Tests: `TestExtendDeadlinesOutlastsServerWriteTimeout` (passes),
`TestServerWriteTimeoutCutsLongHandlerWithoutExtension` (control) and the
Vitest "long-operation timeouts" suite. Removing `Unwrap` makes the first test
fail, which shows the middleware detail matters.

### REL-02 — SMTP sends had no timeout · Medium · P1 · verified · fixed

`net/smtp` has no deadlines. A relay that accepts TCP and then stalls hung the
test-notification request, the API's run-finished notification goroutine, and
the runner's notification stage (scheduled CLI runs never exited). Both senders
now use `notify.SendMail`, which dials with a timeout and sets a 30 s deadline
on the whole exchange. Test: `TestSendMailTimesOutOnStalledServer`.

### REL-03 — No graceful shutdown · Medium · P1 · verified (code) · fixed

Neither server handled SIGTERM; Kubernetes rolling updates, `systemctl stop`
and `v2-stop` dropped in-flight requests mid-response. `internal/graceful`
now stops accepting connections and drains for up to 20 s (below the
Kubernetes default 30 s termination grace). The supervisor's own stop grace
and kill escalation are unchanged. Test:
`TestServeUntilDrainsInFlightRequest`.

## Fixed after the initial audit

REL-04 (readiness endpoint `/api/v1/health/ready`, used by the Kubernetes/Helm
readiness probes), OBS-01 (`X-Request-ID` on every response, in audit lines,
error bodies and UI toasts) and OBS-02 (`ncc_http_requests_total`,
`ncc_http_request_duration_seconds`) are implemented; the table below keeps
their original description for reference.

## Open findings

| ID | Finding | Severity | Priority | Recommendation |
| --- | --- | --- | --- | --- |
| REL-04 | Readiness == liveness: both probes hit `/api/v1/health`, which returns `ok` without checking that the user store loaded, the runner binary is executable, or state directories are writable | Medium | P2 | Add `/api/v1/health/ready` checking those (cheap, cached a few seconds); point `readinessProbe` at it, keep liveness on `/health` |
| OBS-01 | No request/correlation ID; audit lines and logs cannot be joined to a client request | Medium | P2 | Accept/generate `X-Request-ID` in the UI proxy and API, include it in audit entries and error envelopes |
| OBS-02 | No HTTP RED metrics (rate/errors/duration per route) — `/metrics` has rich domain metrics (runs, auth, backups, rate limiter, self-heal, Go runtime) but nothing per endpoint | Medium | P2 | Add `ncc_http_requests_total{route,code}` and a latency histogram using the existing hand-rolled exposition (no client_golang needed) |
| OBS-03 | Runner has OTel tracing; API/UI servers do not | Low | P3 | Optional: OTel HTTP spans in the API with the same exporter config |
| REL-05 | Run concurrency is per replica (`--max-concurrent-runs`), so N API replicas allow N× concurrent Prism load | Low | P3 | Document, or use a lease in the shared state directory |
| REL-06 | Webhook sends use a fresh `http.Client` with 10 s timeout and no retry in the API path (runner path has retry + dead-letter) | Low | P3 | Reuse the runner's retry/dead-letter for API-originated notifications |

## Current controls (verified)

* Outbound Prism calls: shared `internal/httpclient` with timeouts, jittered
  exponential backoff, circuit breaker, adaptive parallelism on 429.
* Notifications (runner): retry with backoff, HMAC-signed webhooks,
  dead-letter directory.
* Scheduled tasks: OS scheduler; overlapping runs prevented by the run
  manager's per-cluster de-duplication and the heartbeat files.
* Self-healing supervisor with health probes (hung-process restart) and
  restart budget.
* Audit log: JSONL, rotation with generations, optional syslog/HTTP
  forwarding, drop counter metric.

## Recommended alerts

| Signal | Source | Alert |
| --- | --- | --- |
| No successful run within 2× schedule interval | `ncc_run_started_seconds`, run status | page/ticket |
| Run failures | `ncc_runs_failed_total` rate | warn |
| Auth anomalies | `ncc_auth_login_failures_total`, `ncc_auth_lockouts_total` | warn on spikes |
| Backup freshness | `ncc_backup_last_timestamp_seconds` > 26 h | warn |
| Audit forwarding loss | `ncc_audit_forward_dropped_total` > 0 | warn |
| Rate limiter saturation | `ncc_ratelimit_blocked_total` | info |
| API 5xx / latency | OBS-02 (to add) | warn |
