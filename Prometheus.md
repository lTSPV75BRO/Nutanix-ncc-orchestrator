# Nutanix NCC Orchestrator + Prometheus

This guide is generalized for all deployment styles:

- CLI-only (`ncc-orchestrator` on a host)
- v2 stack (API/UI + runner)
- Kubernetes

For complete repository setup/build instructions before enabling monitoring, see:

- `docs/BUILD_FROM_SCRATCH.md`

## Monitoring surfaces

| Surface | Produced by | Endpoint / file | Best for |
| --- | --- | --- | --- |
| NCC check metrics | `ncc-orchestrator` | `prom-dir/*.prom` (textfile format) | Cluster check health, severity trends, stale run detection |
| NCC run metrics over HTTP | `ncc-api-server` | `GET /metrics` (`ncc_cluster_*`, `ncc_last_run_*`) | Scraping last-run per-cluster severity/health **without** a textfile collector |
| API runtime + lifecycle metrics | `ncc-api-server` | `GET /metrics` (`ncc_build_info`, `ncc_runs_*`, Go runtime, rate-limiter) | Backend health, run counters |
| API rate-limiter metrics (JSON) | `ncc-api-server` | `GET /api/v1/metrics/rate-limit` | Backend traffic tuning (`rate-limit-per-minute`) |

Recommended: scrape both where available. **New in v2.1.0:** if you run the
api-server, you can scrape `GET /metrics` directly for the latest run's
per-cluster metrics and skip the node_exporter textfile collector entirely (the
`.prom` files still work for CLI-only hosts).

### NCC run metrics on `ncc-api-server` `/metrics` (no textfile collector)

The api-server reads the latest `run-summary.json` and exposes the last run's
metrics in the `ncc_` namespace alongside its own build/runtime metrics:

| Metric | Type | Description |
| --- | --- | --- |
| `ncc_cluster_up{cluster}` | gauge | `1` if the cluster's last NCC run succeeded, else `0` |
| `ncc_cluster_checks_total{cluster,severity}` | gauge | Last-run checks per severity (`FAIL`/`WARN`/`ERR`/`INFO`) |
| `ncc_cluster_checks_count{cluster}` | gauge | Total checks for the cluster in the last run |
| `ncc_cluster_health_score{cluster}` | gauge | Cluster health score (`0..100`) from the last run |
| `ncc_last_run_clusters_ok` / `ncc_last_run_clusters_failed` | gauge | Cluster pass/fail counts for the last run |
| `ncc_last_run_exit_code` | gauge | Exit code of the last run |
| `ncc_last_run_duration_seconds` | gauge | Wall-clock duration of the last run |
| `ncc_last_run_timestamp_seconds` | gauge | Unix epoch of the last run |

Scrape it like any target (add `--metrics-public` to the api-server for
token-free scraping on a private network; otherwise send the API token):

```yaml
scrape_configs:
  - job_name: ncc-api
    metrics_path: /metrics
    static_configs:
      - targets: ["ncc-api-server:8081"]
    # Easiest: run the api-server with --metrics-public on a trusted network so
    # no credential is needed. Otherwise send the admin token as a header
    # (Prometheus 2.50+ http_headers):
    # http_headers:
    #   X-API-Token: { values: ["<NCC_API_TOKEN>"] }
```

## Reference architecture

```text
Nutanix Prism targets
        |
        v
ncc-orchestrator run (manual/scheduler/cron)
        |
        +--> prom-dir/*.prom  ---------+
        |                               |
        |                        node_exporter textfile
        |                               |
        +-------------------------------+------> Prometheus --> Alertmanager/Grafana
                                        |
                         ncc-api-server /api/v1/metrics/rate-limit (optional scrape)
```

## Key NCC textfile metrics

| Metric | Type | Description |
| --- | --- | --- |
| `nutanix_ncc_check_result` | gauge | One time-series per check row/severity/cluster |
| `nutanix_ncc_check_summary_total` | gauge | Per-cluster severity counts |
| `nutanix_ncc_check_total` | gauge | Total checks per cluster |
| `nutanix_ncc_check_problem_total` | gauge | Count of non-INFO checks (`FAIL+WARN+ERR`) |
| `nutanix_ncc_check_problem_ratio` | gauge | Ratio of non-INFO checks to total checks |
| `nutanix_ncc_run_has_failures` | gauge | `1` if any FAIL exists for the cluster in the latest run |
| `nutanix_ncc_run_has_warnings` | gauge | `1` if any WARN exists for the cluster in the latest run |
| `nutanix_ncc_run_has_errors` | gauge | `1` if any ERR exists for the cluster in the latest run |
| `nutanix_ncc_run_has_problems` | gauge | `1` if any non-INFO checks exist in the latest run |
| `nutanix_ncc_run_health_score` | gauge | Cluster run health score (`0..100`) |
| `nutanix_ncc_check_unique_total` | gauge | Number of unique check names in the latest run |
| `nutanix_ncc_check_duplicate_total` | gauge | Number of duplicate check rows (`total-unique`) |
| `nutanix_ncc_check_detail_bytes_total` | gauge | Total detail text bytes across all check rows |
| `nutanix_ncc_check_detail_bytes_avg` | gauge | Average detail text bytes per check row |
| `nutanix_ncc_check_severity_ratio` | gauge | Ratio per severity (`severity` label) |
| `nutanix_ncc_last_run_timestamp_seconds` | gauge | Unix timestamp of latest metrics generation |

## Topology A: CLI-only host

### 1) Configure `prom-dir`

Set `prom-dir` in config to a writable directory.

```yaml
# config.yaml (excerpt)
prom-enabled: true
prom-dir: "/var/lib/node_exporter/textfile"
output-dir-logs: "/opt/ncc/nccfiles"
output-dir-filtered: "/opt/ncc/outputfiles"
```

Disable textfile metrics completely when not needed:

```yaml
prom-enabled: false
```

### 2) Configure node_exporter textfile collector

Run node_exporter with:

```bash
--collector.textfile.directory=/var/lib/node_exporter/textfile
```

Prepare directory:

```bash
sudo mkdir -p /var/lib/node_exporter/textfile
sudo chown node_exporter:node_exporter /var/lib/node_exporter/textfile
```

### 3) Run once and verify

```bash
NCC_PASSWORD='***' ./ncc-orchestrator --config /opt/ncc/config.yaml
ls -la /var/lib/node_exporter/textfile/*.prom
curl -s localhost:9100/metrics | rg nutanix_ncc_
```

### 4) Schedule runs

```bash
./ncc-orchestrator create-schedule --type cron --every 4h --config /opt/ncc/config.yaml --print-only=false
```

## Topology B: v2 API + UI + runner

Use Topology A for runner textfile metrics, and optionally scrape API runtime metrics:

```yaml
scrape_configs:
  - job_name: ncc-api-rate-limit
    metrics_path: /api/v1/metrics/rate-limit
    static_configs:
      - targets: ["ncc-api-host:8081"]
```

Notes:

- Endpoint returns JSON (not Prometheus exposition). Scrape with JSON-capable pipeline/exporter, or keep it for ops automation and dashboards via API polling.
- Keep standard Prometheus scraping for node_exporter textfile metrics.

## Topology C: Kubernetes

Typical pattern:

- Runner writes `.prom` files to a shared volume.
- node_exporter DaemonSet/sidecar reads textfile directory.
- Prometheus scrapes node_exporter target(s).

If v2 API is exposed in-cluster, also collect `/api/v1/metrics/rate-limit` through your API monitoring path.

## Prometheus scrape baseline

```yaml
global:
  scrape_interval: 5m

scrape_configs:
  - job_name: node-exporter
    static_configs:
      - targets: ["localhost:9100"]

rule_files:
  - /etc/prometheus/ncc.rules.yml
```

## Alert rules example (textfile metrics)

```yaml
groups:
  - name: ncc
    rules:
      - alert: NCCFailures
        expr: sum by (cluster) (nutanix_ncc_check_summary_total{severity="FAIL"}) > 0
        for: 30m
        labels:
          severity: critical
      - alert: NCCWarnings
        expr: sum by (cluster) (nutanix_ncc_check_summary_total{severity="WARN"}) > 0
        for: 30m
        labels:
          severity: warning
      - alert: NCCStaleData
        expr: (time() - node_textfile_mtime_seconds{file=~".*\\.prom"}) > 21600
        for: 1h
        labels:
          severity: warning
```

## Useful queries

- `sum by (cluster, severity) (nutanix_ncc_check_summary_total{severity=~"FAIL|WARN|ERR"})`
- `nutanix_ncc_check_result{severity!="INFO"}`
- `nutanix_ncc_check_total`
- `nutanix_ncc_run_health_score`
- `nutanix_ncc_check_problem_ratio`
- `time() - nutanix_ncc_last_run_timestamp_seconds`

## Troubleshooting

| Issue | Check / fix |
| --- | --- |
| No NCC metrics visible | Verify files in `prom-dir` and inspect node_exporter metrics for `nutanix_ncc_` |
| Stale-data alerts firing | Verify scheduler run cadence and latest `.prom` mtime |
| Permission errors writing `.prom` | Ensure runner user can write to `prom-dir` |
| API limiter tuning unclear | Query `/api/v1/metrics/rate-limit` and reduce `--max-parallel` / tune API limiter |

## Security notes

- Use `NCC_PASSWORD` or `secret://` + `secrets-provider`; avoid plaintext secrets in command lines.
- Keep `insecure-skip-verify=false` in production unless explicitly required for lab/self-signed environments.
