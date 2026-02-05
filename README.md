# Nutanix NCC Orchestrator

A CLI tool to run NCC (Nutanix Cluster Check) across multiple clusters in parallel, aggregate results, and generate HTML/CSV reports. Built in Go for efficiency and cross-platform support.


## Author

The script was created by Prajwal Vernekar (prajwal.vernekar@nutanix.com)

-------------------------------


## Features
- Parallel execution on multiple Nutanix clusters.
- API integration with Prism Gateway for starting checks, polling status, and fetching summaries.
- Configurable via YAML/JSON, environment variables, or CLI flags.
- Output formats: HTML reports with styling, CSV exports.
- Retry logic, logging, and progress bars for reliability.
- Replay mode to generate reports from existing logs without re-running checks.
- Prometheus exporter for data visualisation

## Installation
### Prerequisites
- Go 1.24+ (for building binaries from source).
- Nutanix Prism API access (username, password, cluster IPs).

### From Source
1. Clone the repo: `git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git`
2. Navigate to the directory: `cd Nutanix-ncc-orchestrator`
3. Build: `go build -ldflags "-w -s -X main.BuildDate=$(date -u '+%Y-%m-%dT%H:%M:%SZ') -X main.Stream=Beta -X main.GoVersion=$(go version | cut -d ' ' -f 3)" -o ncc-orchestrator`
4. Run: `./ncc-orchestrator --help` Or `./ncc-orchestrator --version`
   
  > Add .exe for windows binary.

### Binary Releases
Download pre-built binaries for Linux/Windows/macOS from the [Releases](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases) page.

## Usage
Basic command:
- `ncc-orchestrator --clusters "10.0.1.1,10.0.2.1" --username admin --password yourpassword`

Full options: Run `ncc-orchestrator --help` for details on flags like `--config`, `--insecure-skip-verify`, `--max-parallel`, etc.

### Configuration
Create a `config.yaml`:

```
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv  
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
```

Run with: `ncc-orchestrator --config config.yaml`

## Kubernetes deployment

You can run the NCC Orchestrator on Kubernetes with a **CronJob** (e.g. every 4 hours), a shared **NFS volume** for logs and reports, and a **webserver** (Nginx) that serves the generated HTML report. MetalLB can assign an external IP to the report UI.

- **Manifests**: All Kubernetes manifests are in the [`k8s/`](k8s/) directory.
- **Full guide**: See **[k8s/README.md](k8s/README.md)** for:
  - Architecture and manifest list
  - Prerequisites (MetalLB, StorageClass `nfs-storage`, Docker image)
  - Step-by-step deployment and configuration
  - Troubleshooting (permissions, TLS, logs)

**Quick start (after editing config and secret):**

```bash
kubectl apply -f k8s/namespace.yaml -f k8s/configmap.yaml -f k8s/nginx-configmap.yaml
kubectl create secret generic ncc-orchestrator-credentials -n ncc-orchestrator --from-literal=password=YOUR_PRISM_PASSWORD
kubectl apply -f k8s/pvc.yaml -f k8s/cronjob.yaml -f k8s/deployment.yaml -f k8s/service.yaml
```

The report is then available at the LoadBalancer external IP (e.g. `http://<EXTERNAL-IP>`).

## Building and Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License
MIT License. See [LICENSE](LICENSE) for details.

## Disclaimer
Use at your own risk. This tool interacts with Nutanix APIs—ensure you have proper permissions.
