# Migration guide: v1 -> v2

This guide covers migrating from v1-style CLI-only usage to v2 full stack (runner + API + UI).

---

## Audience

- **Path A (CLI-focused):** keep using `ncc-orchestrator` only.
- **Path B (full v2):** adopt API/UI, scheduled runs in Kubernetes, and artifact APIs.

---

## What changes in v2

### New components

- `ncc-orchestrator` (runner/CLI)
- `ncc-api-server` (backend API)
- `ncc-ui-server` (frontend host + reverse proxy)
- React frontend bundle

### New deployment model on Kubernetes

- Single entrypoint: `kubectl apply -k k8s/`
- Namespace: `ncc-orchestrator-v2`
- Components: runner cronjob + api deployment + ui deployment + shared pvc

### Backward-compat notes

- `ncc-orchestrator update` updates runner binary, but full v2 operation needs API/UI services too.
- Existing output artifacts can still be reused/replayed where compatible.

---

## Pre-migration checklist

1. Backup current v1 config and outputs:
   - `config.yaml`
   - `nccfiles/`
   - `outputfiles/`
   - `logs/`
2. Capture current behavior baseline:
   - typical runtime
   - alert volumes
   - fail/warn trends
3. Prepare new secrets/token values for v2.
4. Confirm image availability for:
   - runner
   - api
   - ui

---

## Migration paths

## Path A: stay CLI-first (minimal change)

If you do not need API/UI immediately:

1. Upgrade binary:
   ```bash
   ncc-orchestrator update --check
   ncc-orchestrator update
   ```
2. Validate config:
   ```bash
   ncc-orchestrator validate-config --config config.yaml
   ```
3. Run and verify outputs as before.

This path avoids API/UI rollout for now.

---

## Path B: full v2 cutover (recommended)

### Step 1: prepare manifests

- Edit `k8s/configmap.yaml` (`clusters`, `username`, api version, output settings).
- Edit `k8s/secret.yaml` (`prism-password`, `api-token`).
- Edit images in:
  - `k8s/runner-cronjob.yaml`
  - `k8s/api-deployment.yaml`
  - `k8s/ui-deployment.yaml`

### Step 2: deploy to v2 namespace

```bash
kubectl apply -k k8s/
```

### Step 3: verify services

```bash
kubectl get all -n ncc-orchestrator-v2
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-api --tail=100
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-ui --tail=100
```

### Step 4: run a manual job

```bash
kubectl create job -n ncc-orchestrator-v2 ncc-v2-manual-1 --from=cronjob/ncc-v2-runner
kubectl logs -n ncc-orchestrator-v2 job/ncc-v2-manual-1 --all-containers=true
```

### Step 5: validate API/UI

```bash
kubectl port-forward -n ncc-orchestrator-v2 svc/ncc-v2-api 8081:8081
curl -sS http://localhost:8081/api/v1/health

kubectl port-forward -n ncc-orchestrator-v2 svc/ncc-v2-ui 8080:80
# open http://localhost:8080
```

### Step 6: enable scheduled production runs

- Confirm cron schedule in `k8s/runner-cronjob.yaml`
- Monitor first 2-3 scheduled runs
- Confirm notifications and policies behave as expected

---

## Data and compatibility notes

- v2 stores outputs/artifacts on shared PVC paths under `/data/*`.
- If migrating historical outputs, copy old files into the new PVC paths before enabling dashboards.
- Validate any automation scripts that parsed old file locations or old endpoint paths.

---

## Cutover checklist (go-live)

- [ ] API `/api/v1/health` returns healthy
- [ ] UI loads and can fetch report/runs data
- [ ] Manual runner job succeeds
- [ ] CronJob creates scheduled jobs
- [ ] Notifications (email/webhook/slack) tested
- [ ] Policy gate behavior validated
- [ ] Run summary/artifacts are generated and retained as expected

---

## Rollback plan

If migration causes issues:

1. Scale down/remove v2 API/UI deployments (or uninstall v2 namespace).
2. Revert to previous stable runner binary/config.
3. Restore backup config and historical outputs.
4. Re-run with v1 workflow while addressing migration gaps.

---

## Common migration issues

### API works, UI fails
- Check UI `--backend-url` and token file wiring.

### Runner fails in K8s
- Check secret keys and `NCC_PASSWORD` mapping.
- Validate PVC write permissions.

### No jobs triggered
- Confirm cronjob exists and is not suspended.
- Trigger manual job from cronjob and inspect logs.

### Unexpected report differences
- Compare `severity-filter`, exclusions, and policy gates between old/new config.

---

## Related docs

- Kubernetes deployment: `k8s/README.md`
- v2 architecture/MVP: `docs/V2_BACKEND_FRONTEND_MVP.md`
- Feature and flag reference: `docs/FEATURES_AND_CONFIG_FLAGS.md`
