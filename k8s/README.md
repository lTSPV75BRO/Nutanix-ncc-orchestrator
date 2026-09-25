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
LoadBalancer :443 or Ingress (TLS passthrough)
      |
      v
UI Service -> UI Deployment (`ncc-ui-server`, HTTPS on :8080)
      |                               |
      |                               +--> self-signed or BYO cert on /data/tls
      |                               +--> serves frontend static app
      |                               +--> proxies /api/v1/*
      v
API Service (ClusterIP) -> API Deployment (`ncc-api-server`)
      |
      +--> triggers runner jobs / reads artifacts

CronJob (`ncc-v2-runner`) ---> shared PVC (/data/*)
                                  - /data/config
                                  - /data/auth
                                  - /data/tls
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

3. **Exposure**
   - Default is a UI **LoadBalancer** on ports 80 (redirect) and 443 (HTTPS).
   - After apply, patch ConfigMap `ui-origin` to `https://<EXTERNAL-IP>`.
   - Ingress is optional (`k8s/ingress.yaml`) and not applied by kustomize.

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
| `ui-deployment.yaml` | UI server + frontend (`--login-mode on`, `--auto-tls-dir /data/tls`) |
| `ui-service.yaml` | UI LoadBalancer (`:80` redirect, `:443` HTTPS) |
| `ingress.yaml` | Optional hostname entrypoint (not applied by default) |
| `networkpolicy-default-deny-ingress.yaml` | Baseline deny-all ingress policy |
| `networkpolicy-ui-ingress.yaml` | Allows UI ingress on TCP 8080 |
| `networkpolicy-api-ingress.yaml` | Allows API ingress from UI pods on TCP 8081 |
| `networkpolicy-egress.yaml` | DNS, Prism, HTTPS webhook, and SMTP egress baseline |

---

## Deployment steps

1. Set image tags in `k8s/kustomization.yaml` if they are not already `2.2.0`.
2. Put Prism credentials and `jwt-secret` in `ncc-v2-secrets` (see `k8s/secret.yaml`).
3. Apply:

```bash
kubectl apply -k k8s/
kubectl get svc ncc-v2-ui -n ncc-orchestrator-v2 -w
```

4. When `EXTERNAL-IP` appears, point the UI origin at it (replace the IP):

```bash
IP=$(kubectl get svc ncc-v2-ui -n ncc-orchestrator-v2 -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
kubectl -n ncc-orchestrator-v2 patch configmap ncc-v2-config --type merge \
  -p "{\"data\":{\"ui-origin\":\"https://${IP}\"}}"
kubectl -n ncc-orchestrator-v2 rollout restart deploy/ncc-v2-api deploy/ncc-v2-ui
```

5. Open `https://<EXTERNAL-IP>` and accept the self-signed warning. Replace the
   cert later from **Settings → Access → HTTPS / TLS**.

Ingress is optional (`kubectl apply -f k8s/ingress.yaml`) and is not part of
`kubectl apply -k k8s/`.

---

## Post-deploy verification

```bash
kubectl get deploy,svc,pods -n ncc-orchestrator-v2
kubectl exec -n ncc-orchestrator-v2 deploy/ncc-v2-api -c api -- wget -qO- http://localhost:8081/api/v1/health
IP=$(kubectl get svc ncc-v2-ui -n ncc-orchestrator-v2 -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -skSI "https://${IP}/" | head
```

Port-forward if you have no LoadBalancer:

```bash
kubectl port-forward -n ncc-orchestrator-v2 svc/ncc-v2-ui 8443:443
# then https://localhost:8443
```

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

UI pods serve HTTPS out of the box, same as Linux: they mint a self-signed
certificate under `/data/tls` on first start. Generate/renew or paste a BYO
PEM pair from **Settings → Access → HTTPS / TLS**; pods reload the files
without a stack restart. Revert restores the self-signed pair (HTTPS stays
on). Session cookies are `Secure`. Use `https://<LoadBalancer-IP>`.

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

- Confirm the Ingress class exists (`kubectl get ingressclass`). This
  repository’s example uses `nginx`; some platforms only have
  `kommander-traefik`.
- For LoadBalancer access, publish Service port **443** (not only 80).
- For temporary access, use `kubectl port-forward svc/ncc-v2-ui 8443:443`
  and open `https://localhost:8443`

### 6) Settings → HTTPS / TLS

The form is enabled. Certificates live on the shared PVC (`/data/tls`).
After generate/upload, reload the browser (accept the self-signed warning
once). If login loops, you are still on `http://` — use the HTTPS URL and
publish Service port 443.

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
