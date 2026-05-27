# Running NCC Orchestrator on Kubernetes

This guide is the canonical Kubernetes deployment documentation for this repository.
Use **`k8s/`** as the single entrypoint:

```bash
kubectl apply -k k8s/
```

The deployed namespace is **`ncc-orchestrator-v2`**.

If you are starting from a fresh machine and need full source build steps first, read:

- `docs/BUILD_FROM_SCRATCH.md`

---

## Table of contents

- [Architecture](#architecture)
- [What gets deployed](#what-gets-deployed)
- [Prerequisites](#prerequisites)
- [Manifest reference](#manifest-reference)
- [Deployment steps](#deployment-steps)
- [Post-deploy verification](#post-deploy-verification)
- [Operations runbook](#operations-runbook)
- [Troubleshooting](#troubleshooting)
- [Upgrade and rollback](#upgrade-and-rollback)
- [Uninstall](#uninstall)

---

## Architecture

```text
Users / Browser
      |
      v
Service (LoadBalancer) -> UI Deployment (`ncc-ui-server`)
      |                               |
      |                               +--> serves frontend static app
      |                               +--> proxies /api/v1/*
      v
API Service (ClusterIP) -> API Deployment (`ncc-api-server`)
      |
      +--> triggers runner jobs / reads artifacts

CronJob (`ncc-orchestrator`) ---> shared PVC (/data/*)
                                  - /data/logs
                                  - /data/nccfiles
                                  - /data/outputfiles
                                  - /data/promfiles
```

All components run in namespace **`ncc-orchestrator-v2`**.

---

## What gets deployed

Applying `k8s/` creates:

- Runner CronJob: **`ncc-v2-runner`**
- API Deployment + Service: **`ncc-v2-api`**
- UI Deployment + Service: **`ncc-v2-ui`**
- ConfigMap: **`ncc-v2-config`**
- Secret: **`ncc-v2-secrets`**
- PVC: **`ncc-v2-data`**
- NetworkPolicies:
  - **`ncc-v2-default-deny-ingress`**
  - **`ncc-v2-ui-ingress`**
  - **`ncc-v2-api-ingress-from-ui`**

---

## Prerequisites

1. **Kubernetes access**
   - Working `kubectl` context
   - Permission to create namespace, deployments, cronjobs, services, pvc, secrets

2. **Storage**
   - StorageClass supporting `ReadWriteMany` (default in manifests: `nfs-storage`)
   - Update `k8s/pvc.yaml` if your class differs

3. **Ingress / exposure (optional)**
   - Default UI service is `LoadBalancer` with MetalLB annotation in `k8s/ui-service.yaml`
   - If not using MetalLB, change service type to `NodePort` or use `port-forward`

4. **Published images**
   - API image must include:
     - `ncc-api-server`
   - Runner image must include:
     - `ncc-orchestrator` at `/usr/local/bin/ncc-orchestrator`
   - Note: API deployment stages the runner binary from the runner image via init container.
   - UI image must include:
     - `ncc-ui-server`
     - frontend build at `/app/frontend/dist`
   - Update image references in:
     - `k8s/api-deployment.yaml`
     - `k8s/ui-deployment.yaml`
     - `k8s/runner-cronjob.yaml`

5. **Prism credentials**
   - Set in secret `ncc-v2-secrets` (`prism-password`)

---

## Manifest reference

| File | Purpose |
| ------ | --------- |
| `kustomization.yaml` | Single apply entrypoint (`kubectl apply -k k8s/`) |
| `namespace.yaml` | Creates `ncc-orchestrator-v2` namespace |
| `configmap.yaml` | Runtime `config.yaml` consumed by runner/API |
| `secret.yaml` | `prism-password` and `api-token` |
| `pvc.yaml` | Shared RWX storage for logs/artifacts/history |
| `runner-cronjob.yaml` | Scheduled NCC runs |
| `api-deployment.yaml` | Backend API server deployment |
| `api-service.yaml` | Internal API service (`ClusterIP`) |
| `ui-deployment.yaml` | UI server + frontend deployment |
| `ui-service.yaml` | External UI service (`LoadBalancer`) |
| `networkpolicy-default-deny-ingress.yaml` | Baseline deny-all ingress policy |
| `networkpolicy-ui-ingress.yaml` | Allows UI ingress on TCP 8080 |
| `networkpolicy-api-ingress.yaml` | Allows API ingress from UI pods on TCP 8081 |

---

## Deployment steps

### 1) Configure images

Edit these files to your image registry/tag:

- `k8s/runner-cronjob.yaml`
- `k8s/api-deployment.yaml`
- `k8s/ui-deployment.yaml`

### 2) Configure runtime settings

Edit `k8s/configmap.yaml`:

- `clusters`
- `username`
- `ncc-api-version` / `nutanix-v4-api-version`
- output/retry/notification settings

### 3) Set secrets

Edit `k8s/secret.yaml` values:

- `prism-password`
- `api-token`

Or override using `kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`.

### 4) Apply

```bash
kubectl apply -k k8s/
```

### 5) Confirm resources

```bash
kubectl get all -n ncc-orchestrator-v2
kubectl get pvc -n ncc-orchestrator-v2
kubectl get cronjob -n ncc-orchestrator-v2
kubectl get networkpolicy -n ncc-orchestrator-v2
```

---

## Post-deploy verification

### API health

```bash
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-api --tail=100
kubectl port-forward -n ncc-orchestrator-v2 svc/ncc-v2-api 8081:8081
curl -sS http://localhost:8081/api/v1/health
```

Optional hardening tuning:

- API defaults to route-level rate limiting for sensitive endpoints (`--rate-limit-per-minute=60`).
- Set `--rate-limit-per-minute=0` only for trusted internal benchmarking.

### UI health

```bash
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-ui --tail=100
kubectl port-forward -n ncc-orchestrator-v2 svc/ncc-v2-ui 8080:80
```

Open: `http://localhost:8080`

### Runner sanity

```bash
kubectl create job -n ncc-orchestrator-v2 ncc-v2-manual-1 --from=cronjob/ncc-v2-runner
kubectl get jobs -n ncc-orchestrator-v2
kubectl logs -n ncc-orchestrator-v2 job/ncc-v2-manual-1 --all-containers=true
```

---

## Operations runbook

### Trigger an on-demand run

```bash
kubectl create job -n ncc-orchestrator-v2 ncc-v2-manual-$(date +%s) --from=cronjob/ncc-v2-runner
```

### Tail API/UI logs

```bash
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-api -f
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-ui -f
```

### Check recent runner jobs

```bash
kubectl get jobs -n ncc-orchestrator-v2 --sort-by=.metadata.creationTimestamp
```

### Inspect artifacts on PVC

Use a temporary debug pod mounting `ncc-v2-data`, or expose via API/UI artifact endpoints.

---

## Troubleshooting

### 1) Pods CrashLoopBackOff

- Check image path/tag in deployment/cronjob manifests
- Confirm `ncc-orchestrator` exists in runner image at `/usr/local/bin/ncc-orchestrator`
- Check startup logs:

  ```bash
  kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-api --previous
  kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-ui --previous
  ```

### 2) Runner cannot write to `/data`

- Verify PVC is bound:

  ```bash
  kubectl get pvc -n ncc-orchestrator-v2
  ```

- Check storage class RWX support
- Validate volume mount names in all manifests

### 3) UI loads but API calls fail

- Confirm API service name/port (`ncc-v2-api:8081`)
- Verify UI `--backend-url` in `k8s/ui-deployment.yaml`
- Check API auth mode/token files in UI and API pods
- If custom labels were changed, verify `NetworkPolicy` selectors still match `ui` and `api` pods

### 4) No scheduled runs happening

- Check cron expression in `k8s/runner-cronjob.yaml`
- Confirm cronjob exists and is not suspended:

  ```bash
  kubectl get cronjob -n ncc-orchestrator-v2 -o wide
  ```

- Create manual job from cronjob and inspect logs

### 5) LoadBalancer has no external IP

- Check MetalLB annotation in `k8s/ui-service.yaml`
- If unavailable, use port-forward or switch service type

---

## Upgrade and rollback

### Upgrade

1. Update image tags in:
   - `k8s/runner-cronjob.yaml`
   - `k8s/api-deployment.yaml`
   - `k8s/ui-deployment.yaml`
2. Apply:

   ```bash
   kubectl apply -k k8s/
   ```

3. Restart deployments if needed:

   ```bash
   kubectl rollout restart deploy/ncc-v2-api -n ncc-orchestrator-v2
   kubectl rollout restart deploy/ncc-v2-ui -n ncc-orchestrator-v2
   ```

4. Verify API/UI health and run one manual runner job.

### Rollback

1. Revert image tags/config in manifests (git checkout previous tag/commit).
2. Apply:

   ```bash
   kubectl apply -k k8s/
   ```

3. If required, restart deployments/runner and verify endpoints/jobs.

---

## Uninstall

From repo root:

```bash
./scripts/uninstall-v2-clean.sh --force
```

Default namespace in uninstall script is `ncc-orchestrator-v2`.

To preview only:

```bash
./scripts/uninstall-v2-clean.sh --dry-run
```
