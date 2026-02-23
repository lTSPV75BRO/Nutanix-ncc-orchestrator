# Nutanix NCC Orchestrator

A CLI tool to run NCC (Nutanix Cluster Check) across multiple clusters in parallel, aggregate results, and generate HTML/CSV reports. Built in Go for efficiency and cross-platform support.

**Contents:** [Features](#features) · [Installation](#installation) · [Usage](#usage) · [Configuration](#configuration) · [Kubernetes](#kubernetes-deployment) · [Scripts](#scripts) · [Building](#building-and-contributing)

---

## Author

Prajwal Vernekar (prajwal.vernekar@nutanix.com)

---

## Features
- **Parallel execution** on multiple Nutanix clusters via Prism Gateway API (start checks, poll status, fetch summaries).
- **Configurable** via YAML/JSON config file, environment variables (`NCC_*`), or CLI flags.
- **Outputs**: HTML reports (styled), CSV, and optional JSON; aggregated `index.html` plus per-cluster pages.
- **Reliability**: Retry logic, progress bars, rotated JSON logging, and a **preflight check** that verifies output paths are writable before running.
- **Replay mode** (`--replay`): Regenerate reports from existing logs without calling the NCC API.
- **Notifications**: Optional email, webhook, and Slack notifications.
- **Prometheus**: Writes `.prom` files for scraping; see [Prometheus.md](Prometheus.md) for monitoring setup.

## Installation

### Prerequisites
- **Go 1.24+** (for building from source; see [go.mod](go.mod)).
- **Nutanix Prism** API access (username, password, cluster IPs).

### From Source
1. Clone the repo: `git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git`
2. Navigate to the directory: `cd Nutanix-ncc-orchestrator`
3. Build: `go build -ldflags "-w -s -X main.BuildDate=$(date -u '+%Y-%m-%dT%H:%M:%SZ') -X main.Stream=Beta -X main.GoVersion=$(go version | cut -d ' ' -f 3)" -o ncc-orchestrator`
4. Run: `./ncc-orchestrator --help` Or `./ncc-orchestrator --version`
   
  > Add .exe for windows binary.

### Binary Releases
Download pre-built binaries for Linux/Windows/macOS from the [Releases](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases) page.

### Docker image and CI
The [GitHub Action](.github/workflows/docker-publish.yml) builds and pushes the image to Docker Hub on push to `main` (and on release). The **image tag is the same as the code version**:

- **Version source**: the [`VERSION`](VERSION) file (e.g. `0.1.12`). Update this file when you want to release a new image version.
- **Triggers**: push to `main` (when Go code, Dockerfile, or VERSION change) and on GitHub release.
- **Image**: `prajwalnutant/nutanix-ncc-orchestrator:<version>` and `:latest`.
- **Secrets**: In the repo settings, add `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (Docker Hub → Account → Security → New Access Token) so the workflow can push.

## Usage
Basic command:
- `ncc-orchestrator --clusters "10.0.1.1,10.0.2.1" --username admin --password yourpassword`

Full options: Run `ncc-orchestrator --help` for all flags. To see current env values: `ncc-orchestrator --env-info`. Run `ncc-orchestrator --version` to print version, stream, build date, and Go version, then exit. Run **`ncc-orchestrator -u`** or **`--update`** to fetch the latest release from GitHub and update the binary in place if a matching OS/arch asset is available.

### Configuration
Config file (YAML/JSON), CLI flags, and **environment variables** (prefix `NCC_`) are supported. Env overrides config file; flags override both.

Create a `config.yaml` with any of the options below. Run with: `ncc-orchestrator --config config.yaml`

| Option | Default | Description |
|--------|---------|-------------|
| `clusters` | — | Comma-separated Prism cluster IPs or FQDNs |
| `username` | `admin` | Prism Gateway username |
| `password` | — | Prism password (prefer env `NCC_PASSWORD`) |
| `insecure-skip-verify` | `false` | Skip TLS verify (lab/self-signed only) |
| `timeout` | `15m` | Per-cluster overall timeout |
| `request-timeout` | `20s` | Per HTTP request timeout |
| `poll-interval` | `15s` | Polling interval for NCC task status |
| `poll-jitter` | `2s` | Jitter added to poll interval |
| `max-parallel` | `4` | Max concurrent clusters |
| `outputs` | `html,csv` | Comma-separated: html, csv, json |
| `output-dir-logs` | `nccfiles` | Directory for raw NCC summary logs |
| `output-dir-filtered` | `outputfiles` | Directory for filtered HTML/CSV |
| `log-file` | `logs/ncc-runner.log` | Rotated JSON log path |
| `log-level` | — | 0–5 or trace/debug/info/warn/error |
| `log-http` | `false` | Dump HTTP request/response (debug) |
| `retry-max-attempts` | `6` | Max retries per HTTP call |
| `retry-base-delay` | `400ms` | Base backoff delay |
| `retry-max-delay` | `8s` | Max backoff cap |
| `prom-dir` | `promfiles` | Directory for Prometheus .prom files |
| `severity-filter` | — | Comma-separated FAIL,WARN,ERR,INFO; empty = all |
| `dry-run` | `false` | Validate config only, no checks |
| `replay` | `false` | Replay from existing logs (no NCC API) |
| `max-idle-conns` | `100` | HTTP client connection pool: max idle conns total |
| `max-idle-conns-per-host` | `10` | Max idle conns per host |
| `max-conns-per-host` | `0` | Max conns per host (0 = unlimited) |
| `idle-conn-timeout` | `90s` | Idle connection timeout before close |
| `email-enabled` | `false` | Enable email notifications |
| `email-attach-html` | `false` | Attach per-cluster (or digest) HTML report to notification email |
| `notify-digest` | `false` | Send one email/webhook/Slack per run with run overview (and optional index.html attach) instead of per-cluster |
| `smtp-server`, `smtp-port`, `smtp-user`, `smtp-password`, `email-from`, `email-to`, `email-use-tls` | — | SMTP settings |
| `webhook-enabled` | `false` | Enable webhook notifications |
| `webhook-include-html` | `false` | Include per-cluster HTML report as base64 in webhook JSON (brief overview always in payload) |
| `webhook-url`, `webhook-headers` | — | Webhook endpoint and headers |
| `slack-enabled` | `false` | Enable Slack notifications |
| `slack-webhook-url`, `slack-channel` | — | Slack webhook and channel |

### Environment variables (NCC_ prefix)
Any config key can be set via env: **`NCC_`** + key in UPPER_SNAKE (hyphens become underscores). Examples:

- `NCC_CONFIG` — Config file path  
- `NCC_CLUSTERS` — Comma-separated cluster list  
- `NCC_USERNAME`, `NCC_PASSWORD` — Prism credentials  
- `NCC_INSECURE_SKIP_VERIFY` — true/false  
- `NCC_TIMEOUT`, `NCC_REQUEST_TIMEOUT`, `NCC_POLL_INTERVAL`, `NCC_POLL_JITTER`  
- `NCC_MAX_PARALLEL`, `NCC_OUTPUTS`  
- `NCC_OUTPUT_DIR_LOGS`, `NCC_OUTPUT_DIR_FILTERED`, `NCC_LOG_FILE`, `NCC_LOG_LEVEL`, `NCC_LOG_HTTP`  
- `NCC_RETRY_MAX_ATTEMPTS`, `NCC_RETRY_BASE_DELAY`, `NCC_RETRY_MAX_DELAY`  
- `NCC_PROM_DIR`, `NCC_SEVERITY_FILTER`, `NCC_DRY_RUN`, `NCC_REPLAY`  
- `NCC_MAX_IDLE_CONNS`, `NCC_MAX_IDLE_CONNS_PER_HOST`, `NCC_MAX_CONNS_PER_HOST`, `NCC_IDLE_CONN_TIMEOUT`  
- `NCC_EMAIL_ENABLED`, `NCC_EMAIL_ATTACH_HTML`, `NCC_NOTIFY_DIGEST`, `NCC_SMTP_SERVER`, `NCC_SMTP_PORT`, `NCC_SMTP_USER`, `NCC_SMTP_PASSWORD`, `NCC_EMAIL_FROM`, `NCC_EMAIL_TO`, `NCC_EMAIL_USE_TLS`  
- `NCC_WEBHOOK_ENABLED`, `NCC_WEBHOOK_INCLUDE_HTML`, `NCC_WEBHOOK_URL`, `NCC_WEBHOOK_HEADERS`  
- `NCC_SLACK_ENABLED`, `NCC_SLACK_WEBHOOK_URL`, `NCC_SLACK_CHANNEL`  

Run **`ncc-orchestrator --env-info`** to print all possible env vars and their current values.

### Example webhook payload

When webhook is enabled, the app sends a JSON POST with a structure like:

```json
{
  "Cluster": "10.0.1.1",
  "StartedAt": "2025-02-05T10:00:00Z",
  "FinishedAt": "2025-02-05T10:15:00Z",
  "FailCount": 2,
  "WarnCount": 5,
  "ErrCount": 0,
  "InfoCount": 10,
  "TotalChecks": 17,
  "OutputFiles": ["10.0.1.1.log"],
  "Overview": "NCC run completed for cluster 10.0.1.1. FAIL: 2, WARN: 5, ERR: 0, INFO: 10. Total: 17 checks.",
  "ReportHTMLBase64": "<base64-encoded HTML if webhook-include-html is true>"
}
```

In **digest mode** (`notify-digest: true`), one payload per run is sent; `Cluster` is `"run"` and counts reflect clusters OK/failed and total checks.

### Testing email and webhook

**Webhook (no real NCC run):**

1. Get a request-inspection URL, e.g. [webhook.site](https://webhook.site) — open it and copy your unique URL.
2. Ensure you have at least one filtered log so replay can send a payload (e.g. from a previous run: `outputfiles/<cluster>.log` and optionally `nccfiles/<cluster>.log`).
3. Run in **replay** mode with webhook enabled and your URL:

   ```bash
   ./ncc-orchestrator --config config.yaml --replay \
     --webhook-enabled --webhook-url "https://webhook.site/your-unique-id"
   ```

   Or with a config file that has `webhook-enabled: true` and `webhook-url: "https://..."`.  
   On webhook.site you’ll see the POST body (JSON with `Cluster`, `Overview`, `FailCount`, etc.). Use `--webhook-include-html` to also send the report as base64.

**Email:**

1. Use a test SMTP endpoint so you don’t send to real mailboxes:
   - **[Mailtrap](https://mailtrap.io)** or similar: create an inbox, use their SMTP host/port/user/pass in config.
   - **Local MailHog** (Docker): `docker run -d -p 1025:1025 -p 8025:8025 mailhog/mailhog` then SMTP host `localhost`, port `1025`; open http://localhost:8025 to read caught emails.
2. In your config (or flags): `email-enabled: true`, `smtp-server`, `smtp-port`, `smtp-user`, `smtp-password`, `email-from`, `email-to`.
3. Trigger a notification:
   - **Replay** (no NCC API): `./ncc-orchestrator --config config.yaml --replay` (with email settings in config).
   - **Real run**: run against one cluster; when the run finishes, email is sent (or one digest email if `notify-digest: true`).

**Quick webhook test with replay:**

```bash
# 1) One cluster in config, and existing log at outputfiles/<that-cluster>.log (or nccfiles/<cluster>.log so replay can build filtered)
# 2) Set webhook URL (env or config)
export NCC_WEBHOOK_ENABLED=true
export NCC_WEBHOOK_URL="https://webhook.site/your-unique-id"
./ncc-orchestrator --config config.yaml --replay
```

Check the webhook URL page for the POST. Use `--log-level debug` if you need more detail in logs.

## Kubernetes deployment

Run the NCC Orchestrator on Kubernetes with a **CronJob** (e.g. every 4 hours), a shared **PVC** (e.g. NFS RWX) for logs and reports, and a **Deployment** (Nginx) serving the HTML report. Optional LoadBalancer Service (e.g. MetalLB) for external access.

- **Manifests**: [`k8s/`](k8s/) — namespace, ConfigMap, Secret, PVC, CronJob, Deployment, Service. **One-off / replay**: [`k8s/job-debug.yaml`](k8s/job-debug.yaml) runs with `--replay` for debugging or regenerating from existing logs.
- **Full guide**: **[k8s/README.md](k8s/README.md)** — architecture, prerequisites (e.g. StorageClass, MetalLB), deployment steps, troubleshooting (permissions, TLS, logs, getting job logs).

**Quick start (set config and secret first):**

```bash
kubectl apply -f k8s/namespace.yaml -f k8s/configmap.yaml -f k8s/nginx-configmap.yaml
kubectl create secret generic ncc-orchestrator-credentials -n ncc-orchestrator --from-literal=password=YOUR_PRISM_PASSWORD
kubectl apply -f k8s/pvc.yaml -f k8s/cronjob.yaml -f k8s/deployment.yaml -f k8s/service.yaml
```

Report UI: `http://<LoadBalancer-EXTERNAL-IP>`. To **uninstall** (delete namespace and all resources): see [Scripts](#scripts) below.

## Scripts

Helper scripts (run from repo root; set `KUBECONFIG` if needed):

| Script | Purpose |
|--------|---------|
| **[scripts/uninstall-ncc-orchestrator.sh](scripts/uninstall-ncc-orchestrator.sh)** | Delete the `ncc-orchestrator` namespace and everything in it. Use `--force` to skip confirmation, `--dry-run` to preview. After uninstall you can be asked to prune NCC images from worker nodes, or use `--prune-images` to do it without prompt (set `SSH_KEY` if needed). |
| **[scripts/prune-ncc-images-workers.sh](scripts/prune-ncc-images-workers.sh)** | Remove NCC container images from worker nodes via SSH (e.g. to clear old image with same tag). Set `SSH_KEY` and optionally `NODE_IPS`; see script header. |

Example:

```bash
export KUBECONFIG=~/kubecon/mycluster.conf
./scripts/uninstall-ncc-orchestrator.sh --dry-run   # preview
./scripts/uninstall-ncc-orchestrator.sh --force     # uninstall
```

## Building and Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## See also
- [CHANGELOG.md](CHANGELOG.md) — Version history and release notes.
- [Prometheus.md](Prometheus.md) — Prometheus/Grafana monitoring using NCC Orchestrator `.prom` output.
- [k8s/README.md](k8s/README.md) — Full Kubernetes deployment and troubleshooting.

## License
MIT License. See [LICENSE](LICENSE) for details.

## Disclaimer
Use at your own risk. This tool interacts with Nutanix APIs—ensure you have proper permissions.
