# Performance and scalability

No production telemetry, load-test environment, or real Prism clusters were
available, so **production capacity cannot be confirmed**. What follows
separates measurements from code-level observations.

## Measured

### Frontend bundle (production build, Vite 8 / rolldown)

| Asset group | Raw | gzip | Loaded |
| --- | --- | --- | --- |
| `index-*.js` (entry) | 440 kB | 134 kB | initial |
| `react-vendor` | 257 kB | 82 kB | initial (modulepreload) |
| antd chunks preloaded with the entry (`ContextIsolator`, `tag`, `typography`, `select`, `notify`, …) | ~600 kB | ~190 kB | initial |
| **Initial total (approx.)** | **~1.3 MB** | **~400 kB** | |
| Monaco (`editor.api` 2.7 MB, `MonacoCodeEditor` 1.3 MB, language workers) | ~4.5 MB | ~1.1 MB | **lazy** — only the config editor |
| Route chunks (Dashboard 62 kB, Insights 44 kB, Settings sections 15–163 kB) | | | lazy per route/section |

Build time 15.5 s cold. Code splitting is already good: Monaco and every
Settings section load on demand, and nothing heavy is preloaded. The Vite
warning about >900 kB chunks refers to the lazy Monaco chunks only.

Core Web Vitals (LCP/INP/CLS) were not measured: there is no production
deployment to measure against.

### Test-suite timing (developer machine)

Full Go suite ~45 s; Vitest 6–45 s depending on cold transform cache (the
cold run is what exposed the Monaco test timeout, fixed).

## Code-level observations (not measured)

| ID | Observation | Assessment | Priority |
| --- | --- | --- | --- |
| PERF-01 | Active-runs polling: one shared poller (3 s while a run is active, 30 s idle, paused in hidden tabs, error backoff) | Good. Server-Sent Events would remove the polling but is not justified by evidence | P3 |
| PERF-02 | `/api/v1/audit` reads the whole current audit file (rotates at 5 MiB) and parses newest-first per request | Bounded (≤5 MiB, ≤50k entries); fine at expected volumes. Index or tail-read if audit volume grows | P3 |
| PERF-03 | `/report/data` supports pagination; `/runs` and trends read run-summary files from disk per request | Fine for hundreds of runs; retention (`retain-last`/`retain-days`) bounds growth. Measure with a large `runs/` history before optimizing | P3 |
| PERF-04 | Runner concurrency: `--max-parallel`, adaptive parallelism on HTTP 429, retry with jittered backoff and a circuit breaker; API run manager caps concurrent runs (default 4) and queues the rest | Sound backpressure design | — |
| PERF-05 | Long operations shell out to the runner (preflight, backup, restore, doctor) | Correct isolation; their HTTP deadlines were too short (REL-01, fixed) | — |
| PERF-06 | API JSON responses are gzip-compressed (≥1 KiB) | Good | — |

## Scalability

* **Stateless HTTP tier:** JWTs validate on any replica sharing
  `NCC_JWT_SECRET`; login-guard counters are shared through a locked state
  file. The API can run more than one replica (Helm default: 2).
* **Shared-state tier is the limit:** the user store and other JSON state are
  read-modify-write documents. With several replicas, writes can be lost
  (DATA-01). Fix that before raising replica counts or putting heavy PAT
  traffic on multiple replicas.
* **Run execution scales vertically:** runs execute as child processes of
  whichever API replica received the trigger. Concurrency is per replica
  (`--max-concurrent-runs`), not global, so N replicas allow N×4 concurrent
  runs against Prism. Consider a shared run lease if Prism load matters.

## Recommended measurements

1. Load test: k6/vegeta against `/api/v1/report/data`, `/runs`, `/alerts/ncc-dispositions`
   with a realistic history (e.g. 500 runs, 200 clusters); targets p95 < 500 ms
   at 20 rps per replica.
2. Lighthouse/Web Vitals on Dashboard and Insights over the UI server with
   gzip; targets LCP < 2.5 s and INP < 200 ms on a mid-range laptop.
3. Runner: timed run against a cluster simulator with 50–200 clusters to
   validate `max-parallel` and adaptive backoff under Prism 429s.

## v2.4.0 re-audit (2026-11-19, HEAD ea76376c + uncommitted changes)

Phase 5. Evidence: production build (`vite build`, measured 2026-11-19 on this machine).

* Initial load: 37 JS chunks in `index.html`, 1,305,862 bytes raw, about 428,543 bytes gzip (gzip -9 estimate); CSS 116,497 bytes raw, about 20,335 bytes gzip.
* Monaco (`MonacoCodeEditor` 1.21 MB and `editor.api` 2.74 MB raw) is not in the initial load; it is lazy. Pages are code-split (27 `lazy()` uses).
* Largest eager chunks: `react-vendor` 256 kB, `ContextIsolator` 199 kB, `table` 196 kB, `index` 186 kB, `ui` 121 kB (raw).
* Not measured: Core Web Vitals, API latency, query cost, memory, behaviour at many clusters. The freshness history is cached for up to one minute per run directory, but its cost was not measured.

Update 2026-11-19: measured API latency on an empty local install (40 sequential requests per endpoint): `/health` p50 21 ms / p95 23 ms, `/freshness` 7 / 9 ms, `/report/data` 18 / 22 ms, `/settings/clusters` 4 / 5 ms. This says nothing about large fleets; no load or concurrency test was run.

Update 2026-11-19 (Windows, localhost, empty install):

* **PERF-01 (Low, measured, fixed).** Authenticated `GET /api/v1/health` took p50 18.2 ms against 0.8 ms anonymous, because every call resolved about ten display paths with symlink evaluation. The UI polls this endpoint. The resolved paths are now cached for 30 s (`healthpaths.go`; file access still resolves and confines paths on every call). After: p50 0.8 ms; under 50 concurrent clients for 8 s throughput rose from 261 to 46,861 requests/s (p95 2.3 ms). Linux was not measured and probably showed a smaller gain.
* Load (50 concurrent clients, 10 s, static token, empty install, before the fix): `/report/data` 254 req/s (p95 295 ms), `/freshness` 756 req/s (p95 136 ms). No errors or 429s. No test with a large fleet's data was run.
