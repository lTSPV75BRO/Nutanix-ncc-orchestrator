# Helm chart: ncc-orchestrator

Renders the **CronJob** that runs `ncc-orchestrator` on a schedule. You still need **ConfigMap**, **Secret** (password), **PVC**, and **namespace** from `k8s/` manifests or your own.

This Helm chart is currently **runner-only** (CronJob). For full v2 API/UI/frontend deployment, use `k8s/` manifests (`kubectl apply -k k8s/`).

## Install

```bash
helm install ncc-orchestrator ./helm/ncc-orchestrator \
  --namespace ncc-orchestrator-v2 --create-namespace \
  --set image.tag=2.0.0
```

Apply prerequisites first (namespace, config, secret, PVC, storage), or use **`kubectl apply -k k8s/`** with [`k8s/kustomization.yaml`](../../k8s/kustomization.yaml).

## Values

See `values.yaml` for `image.repository`, `image.tag`, `cronjob.schedule`, and resource names (`configMapName`, `pvcName`, `secretName`).
