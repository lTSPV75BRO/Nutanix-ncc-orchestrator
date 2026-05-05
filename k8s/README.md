# Running NCC Orchestrator on Kubernetes

Deploy the Nutanix NCC Orchestrator on Kubernetes so that NCC checks run on a schedule (every 4 hours), reports are written to a shared volume, and a webserver serves the HTML report UI (including `index.html` and per-cluster pages).

---

## Kustomize and Helm

- **Kustomize:** `kubectl apply -k k8s/` uses [`kustomization.yaml`](kustomization.yaml) in this directory (same manifests, shared namespace/labels).
- **Helm (CronJob only):** [`helm/ncc-orchestrator/README.md`](../helm/ncc-orchestrator/README.md).

## Table of contents

- [Architecture](#architecture)
- [Manifest files](#manifest-files)
- [Prerequisites](#prerequisites)
- [Deployment steps](#deployment-steps)
- [Configuration reference](#configuration-reference)
- [Verification and usage](#verification-and-usage)
- [Uninstall](#uninstall)
- [Troubleshooting](#troubleshooting)
- [Runbook](#runbook)
- [Summary](#summary)

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                  namespace: ncc-orchestrator             │
                    │                                                         │
  MetalLB           │   ┌─────────────┐         ┌──────────────────────────┐   │
  (external IP)     │   │  Service    │         │  CronJob (every 4h)     │   │
  ───────────────►  │   │ LoadBalancer│         │  - init: create dirs     │   │
                    │   └──────┬──────┘         │  - main: ncc-orchestrator│   │
                    │          │                └────────────┬─────────────┘   │
                    │          │                             │                 │
                    │          ▼                             │                 │
                    │   ┌─────────────┐                      │                 │
                    │   │ Deployment  │                      │                 │
                    │   │ (nginx)     │                      │                 │
                    │   └──────┬──────┘                      │                 │
                    │          │                              │                 │
                    │          │    ┌─────────────────────────┴───────────────┐  │
                    │          │    │  PVC (nfs-storage, RWX)               │  │
                    │          └────►  /data/logs     (app logs)           │  │
                    │               │  /data/nccfiles  (raw NCC output)      │  │
                    │               │  /data/outputfiles (HTML/CSV) ◄── served│  │
                    │               │  /data/promfiles (Prometheus .prom)    │  │
                    │               └───────────────────────────────────────┘  │
                    │                                                         │
                    │   ConfigMap (config.yaml)   Secret (NCC_PASSWORD)        │
                    └─────────────────────────────────────────────────────────┘
```

- **CronJob**: Runs `ncc-orchestrator --config /config/config.yaml` every 4 hours. Reads config from a ConfigMap and Prism password from a Secret. Writes logs, NCC output, HTML/CSV reports, and Prometheus files under `/data` on a shared PVC.
- **Deployment + Service**: Nginx serves the `outputfiles` directory from the same PVC. The Service is `type: LoadBalancer` so MetalLB can assign an external IP.
- **PVC**: Single persistent volume (StorageClass `nfs-storage`, ReadWriteMany) so both the CronJob and the webserver see the same files.

---

## Manifest files

| File | Purpose |
|------|--------|
| `namespace.yaml` | Namespace `ncc-orchestrator`. |
| `configmap.yaml` | `config.yaml` content (clusters, username, paths, timeouts, email/webhook options). Edit `clusters` and other settings here. |
| `nginx-configmap.yaml` | Nginx server config: serve from `/usr/share/nginx/html/outputfiles`. |
| `secret.yaml` | Prism password (key `password`), used as env `NCC_PASSWORD` in the CronJob. Use a placeholder in the file and set the real value via `kubectl create secret` or by editing before applying. |
| `pvc.yaml` | PersistentVolumeClaim (e.g. 5Gi, `nfs-storage`, RWX). Holds `/data/logs`, `/data/nccfiles`, `/data/outputfiles`, `/data/promfiles`. |
| `cronjob.yaml` | CronJob schedule `15 */4 * * *`, pod spec with init container (create dirs), main container (ncc-orchestrator image), `fsGroup: 1000` for NFS. |
| `deployment.yaml` | Nginx deployment, same PVC and `fsGroup: 1000`, nginx config from ConfigMap. |
| `service.yaml` | LoadBalancer Service for the web deployment; annotate with your MetalLB pool name. |

---

## Prerequisites

1. **Kubernetes cluster** with:
   - **MetalLB** and an **IPAddressPool** (or legacy address pool). The web Service is `type: LoadBalancer`; set the annotation `metallb.io/address-pool` (or `metallb.universe.tf/address-pool`) in `service.yaml` to your pool name.
   - **StorageClass `nfs-storage`** that supports **ReadWriteMany**. The PVC uses this class; change `storageClassName` in `pvc.yaml` if your class has a different name.

2. **Docker image**: The CronJob uses `prajwalnutant/nutanix-ncc-orchestrator:2.0.0` from Docker Hub. To use another tag or a private image, edit the `image` field in `cronjob.yaml`.

3. **Prism**: At least one Prism cluster reachable from the cluster (IP or FQDN), and credentials (username + password).

---

## Deployment steps

### 1. Create namespace and config

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/nginx-configmap.yaml
```

### 2. Edit configuration

Edit `k8s/configmap.yaml` (inside the `config.yaml: |` block):

- **`clusters`**: Comma-separated Prism IPs or hostnames (e.g. `"10.48.52.75,10.48.52.76"`).
- **`username`**: Prism user (e.g. `admin`).
- **`password`**: Leave empty; the app reads `NCC_PASSWORD` from the Secret.
- **`insecure-skip-verify`**: Set to `true` if your Prism uses self-signed or lab certificates (avoids `x509: cannot validate certificate ... doesn't contain any IP SANs`).

Re-apply after editing:

```bash
kubectl apply -f k8s/configmap.yaml
```

### 3. Set the Prism password (Secret)

**Option A – create Secret from literal (recommended, no password in repo):**

```bash
kubectl create secret generic ncc-orchestrator-credentials -n ncc-orchestrator \
  --from-literal=password=YOUR_PRISM_PASSWORD
```

**Option B – apply secret from file:**

Edit `k8s/secret.yaml`, set `stringData.password` to your password, then:

```bash
kubectl apply -f k8s/secret.yaml
```

Do not commit real passwords to git. The sample in `secret.yaml` uses a placeholder.

### 4. Set MetalLB pool (optional)

Edit `k8s/service.yaml` and set the annotation to your IPAddressPool name, e.g.:

```yaml
annotations:
  metallb.io/address-pool: "metallb"
```

Use `metallb.universe.tf/address-pool` if your MetalLB version expects that annotation.

### 5. Create PVC and deploy workload

```bash
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/cronjob.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

Check that the Service gets an external IP:

```bash
kubectl get svc -n ncc-orchestrator
```

### 6. (Optional) Use a different image

To use another tag or a private registry image, edit `spec.jobTemplate.spec.template.spec.containers[0].image` in `k8s/cronjob.yaml`, then:

```bash
kubectl apply -f k8s/cronjob.yaml
```

---

## Configuration reference

The app is configured via the `config.yaml` stored in the ConfigMap. Key fields used in this setup:

| Key | Example | Description |
|-----|--------|-------------|
| `clusters` | `"10.0.1.1,10.0.1.2"` | Comma-separated Prism IPs or FQDNs. |
| `username` | `"admin"` | Prism username. |
| `password` | `""` | Leave empty; use Secret + `NCC_PASSWORD`. |
| `insecure-skip-verify` | `true` / `false` | Set `true` for lab/self-signed certs. |
| `output-dir-logs` | `"/data/nccfiles"` | Raw NCC output (must be on PVC). |
| `output-dir-filtered` | `"/data/outputfiles"` | HTML/CSV reports (served by nginx). |
| `log-file` | `"/data/logs/ncc-runner.log"` | Application log file. |
| `prom-dir` | `"/data/promfiles"` | Prometheus `.prom` files. |
| `outputs` | `"html"` or `"html,csv"` | Output formats. |
| `timeout` | `"15m"` | Per-cluster overall timeout. |
| `max-parallel` | `4` | Number of clusters processed in parallel. |

Email and webhook sections in the sample config are optional; leave disabled or configure as needed.

---

## Verification and usage

### CronJob

```bash
kubectl get cronjob -n ncc-orchestrator
```

Default schedule: **every 4 hours** at minute 15 (`15 */4 * * *`). To run once immediately:

```bash
kubectl create job -n ncc-orchestrator ncc-manual-1 --from=cronjob/ncc-orchestrator
```

### Web UI

- Get the LoadBalancer external IP: `kubectl get svc -n ncc-orchestrator`
- Open **http://&lt;EXTERNAL-IP&gt;** in a browser.

You will see the NCC report (e.g. `index.html`) after at least one successful CronJob run. Until then, nginx may return 404.

Alternatively, port-forward:

```bash
kubectl port-forward -n ncc-orchestrator svc/ncc-orchestrator-web 8080:80
```

Then open http://localhost:8080.

### Logs

List jobs and follow logs for a one-off job:

```bash
kubectl get jobs -n ncc-orchestrator
kubectl logs -n ncc-orchestrator job/<job-name> -f
```

Replace `<job-name>` with the name from the first command (e.g. `ncc-manual-1`).

---

## Uninstall

To remove the application and the entire namespace (CronJob, Deployment, Service, ConfigMap, Secret, PVC, Jobs, etc.):

```bash
# From the repo root; KUBECONFIG must point at your cluster
export KUBECONFIG=~/kubecon/wolverine.conf   # or your kubeconfig
./scripts/uninstall-ncc-orchestrator.sh
```

You will be prompted to confirm unless you pass `--force`. After the namespace is deleted, you will be asked whether to **prune NCC container images from worker nodes** (removes old/same-tag images via SSH). Set `SSH_KEY` (and optionally `NODE_IPS`) if needed for prune.

```bash
./scripts/uninstall-ncc-orchestrator.sh --force
```

To uninstall and always run the image prune (no prompt for prune):

```bash
./scripts/uninstall-ncc-orchestrator.sh --force --prune-images
```

To only see what would be deleted:

```bash
./scripts/uninstall-ncc-orchestrator.sh --dry-run
```

To uninstall a different namespace (e.g. a custom name):

```bash
NAMESPACE=my-ncc-ns ./scripts/uninstall-ncc-orchestrator.sh --force
```

---

## Troubleshooting

### Permission denied on `/data` (logs or promfiles)

If the CronJob fails with errors like `permission denied` when writing to `/data/logs` or creating `promfiles`, the NFS volume may be mounted with restricted ownership. The manifests set **`securityContext.fsGroup: 1000`** so the volume is group-writable. If your NFS export uses a different anon GID (e.g. `65534`), set `fsGroup` to that value in both `cronjob.yaml` and `deployment.yaml` pod specs.

### TLS certificate errors

If you see:

```text
x509: cannot validate certificate for 10.x.x.x because it doesn't contain any IP SANs
```

set **`insecure-skip-verify: true`** in the ConfigMap `config.yaml` (for lab/self-signed Prism certs only). Re-apply the ConfigMap; the next CronJob run will pick it up.

### Failed CronJob runs – how to get logs

`kubectl logs` expects a **Pod** name (or a Job via the `job/` prefix), not a Job name by itself. Use one of these:

**Logs for a specific Job (e.g. failed run):**
```bash
# Use the job/ prefix so kubectl finds the Job's pod(s)
kubectl logs -n ncc-orchestrator job/ncc-orchestrator-29510880 --all-containers=true
```

**Or use the job's label (same result):**
```bash
kubectl logs -n ncc-orchestrator -l job-name=ncc-orchestrator-29510880 --all-containers=true
```

**If the pod is already gone** (garbage-collected), inspect the Job and events:
```bash
kubectl describe job ncc-orchestrator-29510880 -n ncc-orchestrator
```

**Logs from the most recent Job** (any status):
```bash
JOB=$(kubectl get jobs -n ncc-orchestrator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
kubectl logs -n ncc-orchestrator job/$JOB --all-containers=true
```
(Jobs are listed by creation time; adjust sort if needed, e.g. `--sort-by=.metadata.creationTimestamp` and take the last one.)

**Reproduce and follow logs** with the one-off debug Job (see `job-debug.yaml`):
```bash
kubectl delete job ncc-debug -n ncc-orchestrator --ignore-not-found=true
kubectl apply -f k8s/job-debug.yaml -n ncc-orchestrator
kubectl logs -n ncc-orchestrator -f job/ncc-debug --all-containers=true
```

### Job fails immediately

- Check **Secret** exists and has key `password`:  
  `kubectl get secret ncc-orchestrator-credentials -n ncc-orchestrator -o yaml`
- Check **ConfigMap** and that `clusters` and paths are correct:  
  `kubectl get configmap ncc-orchestrator-config -n ncc-orchestrator -o yaml`
- Inspect **pod logs** (use `job/<job-name>` or the label as above):  
  `kubectl logs -n ncc-orchestrator -l job-name=<job-name> --all-containers=true`

### Changing the schedule

Edit `spec.schedule` in `k8s/cronjob.yaml` (standard cron format). Example for every 6 hours at minute 0: `0 */6 * * *`. Then run `kubectl apply -f k8s/cronjob.yaml`.

---

## Runbook

When a **CronJob run fails** or reports are missing:

1. **Logs**  
   Get logs from the failing job:  
   `kubectl logs -n ncc-orchestrator job/<job-name> --all-containers=true`  
   (Use `kubectl get jobs -n ncc-orchestrator` to find the job name, or use the label `job-name=ncc-orchestrator-<cron-id>`.)

2. **One-off debug job**  
   Run with `--replay` to regenerate from existing logs without calling the API:  
   `kubectl apply -f k8s/job-debug.yaml -n ncc-orchestrator`  
   Then: `kubectl logs -n ncc-orchestrator -f job/ncc-debug --all-containers=true`  
   See [k8s/job-debug.yaml](job-debug.yaml) for the command and volume mounts.

3. **NFS / permissions**  
   If you see "permission denied" on `/data/logs`, `/data/outputfiles`, or `/data/promfiles`, check NFS export and `fsGroup` (e.g. `securityContext.fsGroup: 1000` in `cronjob.yaml` and `deployment.yaml`). Adjust to match your NFS anon GID if needed.

4. **Prune NCC images (optional)**  
   After uninstall or to clear old NCC images on workers:  
   `./scripts/prune-ncc-images-workers.sh`  
   Set `SSH_KEY` and optionally `NODE_IPS`; see script header.

---

## Summary

| Component | Purpose |
|-----------|--------|
| **CronJob** | Runs NCC checks every 4 hours; writes to `/data/logs`, `/data/nccfiles`, `/data/outputfiles`, `/data/promfiles`. |
| **Deployment** | Nginx serves `outputfiles` (index.html and other HTML/CSV) from the PVC. |
| **Service** | LoadBalancer; MetalLB assigns an external IP from your pool. |
| **ConfigMap** | `config.yaml` (clusters, username, paths, TLS, options). |
| **Secret** | Prism password → env `NCC_PASSWORD` for the CronJob. |
| **PVC** | Shared storage (StorageClass `nfs-storage`, RWX). |

The app runs with `--config /config/config.yaml`; the report is served by the webserver at the Service’s external IP after each run.
