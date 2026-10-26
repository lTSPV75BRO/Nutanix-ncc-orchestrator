# Feature roadmap

Necessary fixes are tracked in [`implementation-plan.md`](implementation-plan.md);
this document lists **optional product improvements** grounded in how the
application works today. Effort: S (≤2 days), M (≤1 week), L (>1 week).

## F1 — Run-arguments builder · P2 · M

* **Problem:** run options are a free-text field; the allowlist drifted from
  the runner, and every run started with the suggested flags failed.
* **Solution:** `GET /api/v1/runs/extra-args` (generated from
  `extraArgsAllowed`, with descriptions and value formats); the Runs page renders
  chips/selects; free text stays under "Advanced".
* **Benefit / users:** operators; fewer failed runs and support tickets.
* **Dependencies:** COR-02 (done). **Security:** server-side validation stays
  authoritative. **Acceptance:** each option round-trips to a run that passes
  flag parsing; invalid combinations are blocked before submit.

## F2 — Safe restore with automatic rollback · P1 · M

* **Problem:** a failed restore can leave a half-restored install (DATA-02).
* **Solution:** encrypted `pre-restore-<stamp>` snapshot, automatic rollback
  on failure, shown as rollback candidate; optional "data only" restore that
  never replaces binaries (also addresses SEC-07).
* **Users:** admins. **Risk:** disk usage for an extra snapshot (bounded by
  retention). **Acceptance:** failure injection leaves the install
  byte-identical to before.

## F3 — Data freshness and trust indicators · P2 · S–M

* **Problem:** stale or partial data can look current (per-cluster failures,
  silent schedule failures).
* **Solution:** per-cluster "last successful data" column and badge;
  dashboard banner when the schedule missed two intervals; PC alerts cache age.
* **Users:** everyone reading the dashboard. **Acceptance:** a cluster that
  failed its last run is visibly marked; a stopped schedule raises the banner
  within 2× interval.

## F4 — HA-safe user store · P1 · M

* **Problem:** lost updates across replicas (DATA-01).
* **Solution:** read-modify-write under lock with optimistic concurrency
  (Kubernetes `resourceVersion`, file lock + reload); move PAT `last_used_at`
  out of the hot document.
* **Acceptance:** concurrency test from the testing plan passes; Helm default
  of 2 replicas becomes safe.

## F5 — Request IDs and per-route metrics · P2 · S

* **Problem:** cannot correlate a user's failed action with logs/audit;
  no latency/error SLOs per endpoint.
* **Solution:** `X-Request-ID` propagation (UI proxy → API → runner env),
  included in audit lines and error envelopes; `ncc_http_requests_total` and a
  duration histogram.
* **Acceptance:** an error toast shows a request ID that finds the matching
  audit/log line.

## F6 — Readiness endpoint · P2 · S

`/api/v1/health/ready` checking the user store, runner binary and state
directories; Kubernetes readiness uses it. Acceptance: a pod with an unwritable
PVC stays out of the Service.

## F7 — Accessible charts and table views · P3 · S

Table toggle or text summary for trend/SLO charts; axe-core checks in CI.

## F8 — Notification reliability parity · P3 · S

Reuse the runner's retry + dead-letter for API-originated notifications
(run-finished, operational failures); show the dead-letter count in System Health.

## F9 — Admin self-reset policy UI · P3 · S

Expose the `--allow-admin-self-reset` state on Settings → Access (read-only,
with the CLI recovery instructions), so admins know which recovery path applies.

## Not recommended now

* A database backend: file state meets current scale; F4 fixes the concrete
  multi-replica risk with far less change.
* Rewriting the runner monolith in one step: split `goNCC.go` incrementally
  (Prism client, reporting, install/update, backup) when touching those areas.
* Server-Sent Events for live runs: polling is already consolidated and cheap.
