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
Ingress (TLS) -> UI Service (ClusterIP) -> UI Deployment (`ncc-ui-server`)
      |                               |
      |                               +--> serves frontend static app
      |                               +--> proxies /api/v1/*
      v
API Service (ClusterIP) -> API Deployment (`ncc-api-server`)
      |
      +--> triggers runner jobs / reads artifacts

CronJob (`ncc-v2-runner`) ---> shared PVC (/data/*)
                                  - /data/config
                                  - /data/auth
                                  - /data/logs
                                  - /data/backups
                                  - /data/nccfiles
                                  - /data/outputfiles
                                  - /data/promfiles
```

All components run in namespace **`ncc-orchestrator-v2`**.

---

## What gets deployed

Applying `k8s/` creates:

- Runner CronJob: **`ncc-v2-runner`**
- API Deployment + Service: **`ncc-v2-api`** (default **2** replicas)
- UI Deployment + Service: **`ncc-v2-ui`** (default **2** replicas)
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
   - An Ingress controller and TLS Secret named `ncc-v2-ui-tls`
   - Change `k8s/ingress.yaml` hostname and TLS settings for your environment
   - To have cert-manager issue the certificate, uncomment the
     `cert-manager.io/cluster-issuer` annotation in `k8s/ingress.yaml`

4. **Published images**
   - API image must include:
     - `ncc-api-server`
   - Runner image must include:
     - `ncc-orchestrator` at `/usr/local/bin/ncc-orchestrator`
   - API deployment stages the runner binary from the API image contract.
   - UI image must include:
     - `ncc-ui-server`
     - frontend build at `/app/frontend/dist`
   - Update image references in:
     - `k8s/api-deployment.yaml`
     - `k8s/ui-deployment.yaml`
     - `k8s/runner-cronjob.yaml`

5. **Credentials**
   - Provision `ncc-v2-secrets` out-of-band; do not commit credentials.
   - Include `jwt-secret` (`openssl rand -base64 32`). The API Deployment
     requires this key so replicas share session JWTs; pods will not start
     without it.
   - Enable etcd encryption and use External Secrets/CSI where available.

---

## Manifest reference

| File | Purpose |
| ------ | --------- |
| `kustomization.yaml` | Single apply entrypoint (`kubectl apply -k k8s/`) |
| `namespace.yaml` | Creates `ncc-orchestrator-v2` namespace |
| `configmap.yaml` | Runtime `config.yaml` consumed by runner/API |
| `secret.yaml` | `prism-password`, `api-token`, and required `jwt-secret` (shared HS256 key for stateless session JWTs across API replicas) |
| `pvc.yaml` | Shared RWX storage for logs/artifacts/history |
| `runner-cronjob.yaml` | Scheduled NCC runs |
| `api-deployment.yaml` | Backend API server (2 replicas, `--cookie-secure`, required `jwt-secret`) |
| `api-service.yaml` | Internal API service (`ClusterIP`) |
| `ui-deployment.yaml` | UI server + frontend (`--login-mode on`) |
| `ui-service.yaml` | Internal UI service (`ClusterIP`) |
| `ingress.yaml` | TLS-enabled external UI entrypoint (`ncc-v2-ui-tls`; optional cert-manager) |
| `networkpolicy-default-deny-ingress.yaml` | Baseline deny-all ingress policy |
| `networkpolicy-ui-ingress.yaml` | Allows UI ingress on TCP 8080 |
| `networkpolicy-api-ingress.yaml` | Allows API ingress from UI pods on TCP 8081 |
| `networkpolicy-egress.yaml` | DNS, Prism, HTTPS webhook, and SMTP egress baseline |

---

## Deployment steps

### 1) Configure images

Edit the image tags in `k8s/kustomization.yaml`, or create an environment
overlay with your registry, tags, pull secrets, and immutable digests.

### 2) Configure runtime settings

Edit `k8s/configmap.yaml`:

- `clusters`
- `username`
- `ncc-api-version` / `nutanix-v4-api-version`
- output/retry/notification settings

### 3) Set secrets

Provision the empty Secret template using the `kubectl create secret` command
shown in `k8s/secret.yaml`, or connect it to External Secrets/CSI. Include a
`jwt-secret` key (`openssl rand -base64 32`) so API replicas share session JWTs.

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
kubectl get ingress -n ncc-orchestrator-v2
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

Open: `https://localhost:8080` when using Ingress TLS, or the port-forward
URL above for a temporary HTTP check.

### Runner sanity

```bash
kubectl create job -n ncc-orchestrator-v2 ncc-v2-manual-1 --from=cronjob/ncc-v2-runner
kubectl get jobs -n ncc-orchestrator-v2
kubectl logs -n ncc-orchestrator-v2 job/ncc-v2-manual-1 --all-containers=true
```

### Horizontal scaling

API and UI default to two replicas. Sessions are stateless JWTs signed with
`jwt-secret`. The user database lives in Secret `ncc-v2-users` and is reloaded
every two seconds. Scheduled backups flock `/data/backups/.ncc-backup.lock`.

Keep API, UI, and runner **image tags in lockstep**. The API reports component
versions from `NCC_IMAGE_TAG` / `NCC_ORCHESTRATOR_IMAGE_TAG` /
`NCC_UI_IMAGE_TAG`; it does not exec `ncc-ui-server` from the API image.

### TLS certificates

HTTPS is terminated at Ingress. Do not upload certs from Settings → Access;
manage `ncc-v2-ui-tls` (or enable cert-manager on the Ingress). Session
cookies are `Secure`.

### System Health and backups

Settings → System Health runs PVC-safe doctor checks plus Kubernetes runtime
probes. Host supervisor/PID checks are not used.

Scheduled and manual snapshots write to `/data/backups` on the shared PVC
and include `config/`, `auth/`, and `logs/`. Restore still requires a
Deployment rollout to load restored state:

```bash
kubectl -n ncc-orchestrator-v2 rollout restart deployment/ncc-v2-api deployment/ncc-v2-ui
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
- Confirm `ncc-v2-secrets` includes `jwt-secret` (API logs
  `NCC_JWT_SECRET is required in Kubernetes` if it is missing)
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

### 5) Ingress has no address

- Check the Ingress controller and TLS Secret (`ncc-v2-ui-tls`)
- Uncomment the cert-manager annotation in `k8s/ingress.yaml` if you want
  the cluster to issue the certificate
- For temporary access, use `kubectl port-forward svc/ncc-v2-ui 8080:80`

### 6) Settings → HTTPS / TLS is disabled

This is expected. Kubernetes terminates TLS at Ingress; in-app certificate
upload returns `409`. Manage the Ingress secret instead.

### 7) Installed components report “ui-server: Component not found”

Upgrade the API image. Current builds report UI/orchestrator versions from
image-tag env vars instead of execing `ncc-ui-server` from the API pod. Keep
API, UI, and runner tags aligned.

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
