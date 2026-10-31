# Scaling to thousands of clusters

Tested for v2.3.0 with **7,000 simulated clusters** and 30 findings each
(**210,000 findings per run**), running the real orchestrator, api-server
and UI against [`tools/fakeprism`](../tools/fakeprism), a stand-in for Prism
Element. Measured on one Windows 11 workstation; Linux servers are typically
faster on file I/O.

## Results

| Path | Before v2.3.0 fixes | After |
| --- | --- | --- |
| Orchestrator CPU for a 7,000-cluster run | 80 CPU-s | 44 CPU-s |
| Orchestrator wall time (fake clusters, 1 s poll) | 91 s at `max-parallel` 100 (the old maximum) | 21 s at 500 |
| `GET /report/data` first request after a run | 32 s | 2.8 s (and pre-warmed in the background, so usually ~0.2 s) |
| `GET /report/data` repeat requests | 32 s | 0.1 s (1,500-row page), 0.6 s (all 210k rows, 6.7 MB gzip) |
| Findings shown in the UI | **none** above 20,000 rows | all 210,000 |
| Alert lists / handling (`/alerts/ncc*`) | 1.5 s per request | 0.07–0.4 s |
| First alert-history ingest of a run (140k alerts) | 37 s | 0.9 s |
| Findings page: all rows on screen | 21 s (cold), 9 s (warm) | 2.1 s (first 1,500 rows at 0.5 s) |
| Findings search while typing | page froze (>45 s per keystroke) | results in 0.1–0.15 s |
| Browser memory on Findings | 454 MB | ~200 MB |

What changed:

- **Large reports are read correctly.** Above 20,000 findings the runner keeps
  the rows in `aggregated-data.json` instead of `index.html`; the report API
  only read `index.html`, so big fleets saw no findings. Run-history snapshots
  now include the file too.
- **The api-server parses each report file once per run** (cached by path,
  size and modification time, shared between requests and replicas'
  writes), instead of re-reading and re-parsing it — and every raw NCC log —
  on each request. The cache is warmed in the background after each run.
- **Alert history** (`.ncc-api-alert-lifecycle.json`) is kept parsed in memory,
  written compact, indexed by alert ID (ingesting a run was quadratic), and
  each request skips the run ingest when it has already been done.
- **The orchestrator** parses each summary from memory instead of reading
  back the file it just wrote, keeps cluster metadata with the result instead
  of re-reading 7,000 logs at the end, drops finished progress bars, splits
  lines without copying, and allows `max-parallel` up to **1,000** (was 100).
- **The UI** builds table rows once per report (search only filters them),
  looks up each cluster's log directly, works out KB links only for rows on
  screen, and no longer refetches the preview after the full report loads.

## Running NCC on thousands of clusters

An NCC run takes ~3 minutes on each cluster and the orchestrator cannot make
that shorter, so the time for a fleet is set by how many clusters run at once:

    run time ≈ ceil(clusters ÷ max-parallel) × (NCC time + polling lag)

| 7,000 clusters, ~3 min NCC | Time |
| --- | --- |
| `max-parallel: 4` (the default before v2.3.0) | ~90 hours |
| `max-parallel: 100` (the old maximum) | ~3.7 hours |
| `max-parallel: 0` (auto → 500, the default now) | ~45 minutes |
| `max-parallel: 1000` | ~23 minutes |

How the orchestrator keeps that safe and fast (v2.3.0):

- **Automatic concurrency.** `max-parallel: 0` (the default) runs one cluster
  per slot up to 500. Each in-flight cluster is mostly an idle wait on its
  NCC task, so this costs little on the orchestrator.
- **Backs off when pushed back.** With `adaptive-parallelism` (default on),
  an HTTP 429 or 503 cuts the clusters in flight to 3/4 (at most once every
  5 s); each success adds one back.
- **Cluster slots only cover the cluster.** A slot is held while NCC is
  started, polled and its summary read. Writing reports and sending
  per-cluster email/webhook/Slack notifications runs in a separate pool, so a
  slow mail server no longer delays other clusters (measured: a 3-second
  webhook per cluster made a run 18% slower before, ~1.5% now).
- **Polling lands on completion.** Status polls use the task's own percentage
  to estimate when it finishes: never less often than `poll-interval`, and
  near the end just after the estimate (no more often than a fifth of the
  interval, and at most once a second). At the 15 s default this removes most
  of the ~7.5 s average delay per cluster in noticing that NCC finished.
- **Connections are reused.** The HTTP pool keeps one idle connection per
  cluster in flight, so a poll does not pay for a new TLS handshake.
- **Transient failures get a second chance.** Clusters that failed with a
  timeout, network error or rate limit are run again at the end of the run
  (`retry-failed-clusters`, default 1, up to 5). Authentication and parse
  errors are not retried.
- **Long runs can resume.** Finished clusters are recorded in
  `<output-dir-filtered>/.ncc-run-journal.jsonl`. If a run is interrupted
  (reboot, crash, pod eviction), start it again with `--resume` (or
  `resume: true`): clusters already finished are rebuilt from their saved raw
  summaries and only the rest are contacted. Only used for the same cluster
  list and within 24 hours; a run that completes removes the journal.

Measured with `tools/fakeprism -task-duration 15s` (NCC time scaled to 1/12,
`poll-interval` 1.25 s to match 15 s at full scale):

| Test | Before | After |
| --- | --- | --- |
| 3,000 clusters, max-parallel 500 (ideal 90 s) | 97.9 s | 96.4 s |
| same, with a 3 s webhook per cluster | 115.7 s | 99.4 s |
| 7,000 clusters, max-parallel auto (ideal 210 s) | — | 223 s, 7,000 ok |
| 600 clusters, killed at 40 s, then `--resume` | full rerun | reused 200, finished the other 400 |
| 3 unreachable clusters among 53 | failed once | retried at the end of the run |

## Sizing

The 7,000-cluster row is measured; the smaller rows are scaled from it
(memory grows roughly with the number of findings).

| Fleet | max-parallel | api-server memory | Orchestrator peak memory | Disk per run |
| --- | --- | --- | --- | --- |
| up to ~500 clusters | 4–50 | < 256 MiB | < 300 MiB | ~15 MB |
| ~2,000 clusters | 100–300 | ~300–500 MiB | ~700 MiB | ~60 MB |
| ~7,000 clusters | 300–700 | ~1 GiB | ~2 GiB | ~200 MB plus ~90 MB per kept run snapshot |

- **max-parallel.** A worker spends almost all its time waiting for the
  cluster's NCC task (polling every `poll-interval`), so concurrency is cheap
  on the orchestrator side. Total time ≈ ceil(clusters ÷ max-parallel) × the
  time one NCC run takes; with real NCC runs of 10–20 min, 7,000 clusters at
  500 take ~2.5–5 h, against 12–23 h at the old maximum of 100. Lower it if
  Prism, the network or a proxy rate-limits (adaptive parallelism also backs
  off on HTTP 429).
- **poll-interval.** Requests per second to all clusters ≈ max-parallel ÷
  poll-interval. Keep the default 15 s for large fleets (500 ÷ 15 s ≈ 33 req/s).
- **Memory.** The api-server keeps the latest report and alert history parsed
  in memory. In Kubernetes give it a 2 GiB limit at this scale and set
  `GOMEMLIMIT` to ~90% of the limit (the shipped `k8s/api-deployment.yaml`
  does; Helm: `apiGoMemLimit`). The runner job needs ~2 GiB at 7,000 clusters.
- **Disk.** Use `retain-last` / `retain-days` for run history and
  `artifact-retain-*` for per-cluster files; each large run snapshot includes
  the ~90 MB aggregated rows file.
- **Browser.** The Findings page holds every finding of the latest run in
  memory (~200 MB at 210k findings) so filtering and sorting stay instant;
  any current desktop browser handles this.

## Reproducing the test

```bash
go build -o ncc-orchestrator .
go build -o ncc-api-server ./cmd/ncc-api-server
go run ./tools/fakeprism -make-config /tmp/ncc-scale -clusters 7000 -parallel 500
go run ./tools/fakeprism -task-duration 3m &   # answers every 127.x.y.z:9440 (Linux, Windows)
cd /tmp/ncc-scale && NCC_PASSWORD=x ../ncc-orchestrator --config config.yaml
```

Then point an api-server at that folder (`--repo-root /tmp/ncc-scale
--config-path config.yaml`) and a UI server at the api-server. Use
`-findings` and `-passes` to change the report size and `-delay` to simulate
slow clusters; `-task-duration` sets how long each simulated NCC run takes and
`-hook-delay` how slowly the stand-in webhook (`https://127.0.0.1:9440/hook`)
answers.

To see where a large run spends its time, set `NCC_PROFILE_DIR=<dir>` for the
orchestrator: it writes `cpu.pprof` and `heap.pprof` there for
`go tool pprof`.
