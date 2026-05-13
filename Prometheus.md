# Nutanix NCC Orchestrator + Prometheus

Production-oriented monitoring pattern:

`ncc-orchestrator` -> `.prom` files (`prom-dir`) -> `node_exporter` textfile collector -> Prometheus -> Grafana/Alertmanager

## Architecture

```text
Nutanix Prism targets
        |
        v
ncc-orchestrator run (cron/scheduler)
        |
        v
prom-dir/*.prom (textfile metrics)
        |
        v
node_exporter --collector.textfile.directory=...
        |
        v
Prometheus scrape (:9100) -> rules/alerts -> Grafana dashboards
```

## Key metrics

| Metric | Type | Description | Example |
| --- | --- | --- | --- |
| `nutanix_ncc_check_result` | gauge | One time-series per check row/severity/cluster | `nutanix_ncc_check_result{severity="FAIL"}` |
| `nutanix_ncc_check_summary_total` | gauge | Per-cluster counts by severity | `sum by (cluster,severity) (nutanix_ncc_check_summary_total)` |
| `nutanix_ncc_check_total` | gauge | Total checks per cluster | `nutanix_ncc_check_total` |

## Prerequisites

- Linux bastion/runner host
- `node_exporter` with textfile collector
- `ncc-orchestrator` binary
- Valid Nutanix credentials (prefer env/file secrets, not CLI plaintext)

## 1) Configure node_exporter textfile collector

Install and run `node_exporter` with:

```bash
--collector.textfile.directory=/var/lib/node_exporter/textfile
```

Create directory and ownership:

```bash
sudo mkdir -p /var/lib/node_exporter/textfile
sudo chown node_exporter:node_exporter /var/lib/node_exporter/textfile
```

Verify collector is active:

```bash
curl -s localhost:9100/metrics | rg node_textfile_mtime_seconds
```

## 2) Configure orchestrator for Prometheus output

Use config-driven execution (recommended):

```yaml
# config.yaml (excerpt)
prom-dir: "/var/lib/node_exporter/textfile"
output-dir-logs: "/opt/ncc/nccfiles"
output-dir-filtered: "/opt/ncc/outputfiles"
```

Run once:

```bash
NCC_PASSWORD='***' ./ncc-orchestrator --config /opt/ncc/config.yaml
```

Verify files:

```bash
ls -la /var/lib/node_exporter/textfile/*.prom
```

> No manual `sed` fixups should be required for `.prom` format on current versions.

## 3) Schedule production runs

Prefer built-in scheduler helper:

```bash
./ncc-orchestrator create-schedule --type cron --every 4h --config /opt/ncc/config.yaml --print-only
./ncc-orchestrator create-schedule --type cron --every 4h --config /opt/ncc/config.yaml --print-only=false
```

Or use cron directly:

```cron
0 */4 * * * NCC_PASSWORD='***' /opt/ncc/ncc-orchestrator --config /opt/ncc/config.yaml >> /var/log/ncc-orchestrator.log 2>&1
```

## 4) Prometheus scrape config

Example `/etc/prometheus/prometheus.yml` excerpt:

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

## 5) Alert rules example

`/etc/prometheus/ncc.rules.yml`:

```yaml
groups:
  - name: ncc
    rules:
      - alert: NCCFailures
        expr: sum by (cluster) (nutanix_ncc_check_summary_total{severity="FAIL"}) > 0
        for: 30m
        labels:
          severity: critical
        annotations:
          summary: "NCC FAIL on {{ $labels.cluster }}"

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

Reload Prometheus and verify targets:

```bash
curl -s http://localhost:9090/targets
```

## 6) Useful Grafana queries

- Fails/warns/errors by cluster: `sum by (cluster, severity) (nutanix_ncc_check_summary_total{severity=~"FAIL|WARN|ERR"})`
- Active non-INFO checks table: `nutanix_ncc_check_result{severity!="INFO"}`
- Total checks by cluster: `nutanix_ncc_check_total`

## Troubleshooting

| Issue | Check/Fix |
| --- | --- |
| No NCC metrics visible | `ls /var/lib/node_exporter/textfile/*.prom` and `curl -s localhost:9100/metrics \| rg nutanix_ncc_` |
| Stale metrics alert firing | Verify scheduler/cron last run and file mtimes under `prom-dir` |
| Prometheus target down | Confirm node_exporter is up and reachable on `:9100` |
| Permission errors writing `.prom` | Ensure runner user can write to `prom-dir` and directory ownership is correct |

## Security notes

- Use `NCC_PASSWORD` or `secret://` with `secrets-provider`; avoid plaintext passwords in cron/CLI.
- Keep `insecure-skip-verify=false` in production unless lab-only/self-signed scenarios require override.
