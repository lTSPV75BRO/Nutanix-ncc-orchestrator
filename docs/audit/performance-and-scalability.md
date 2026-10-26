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
