# Kustomize

Kustomize entrypoint lives next to the manifests so paths stay valid:

```bash
kubectl apply -k k8s/
```

This uses [`k8s/kustomization.yaml`](../k8s/kustomization.yaml) (namespace `ncc-orchestrator`, optional common labels).

### Prerequisites

- Edit `k8s/secret.yaml` (password) before applying.
- Storage class in `pvc.yaml` must match your cluster.

### Helm

For a **CronJob-only** install with templated image and schedule, see [`helm/ncc-orchestrator/README.md`](../../helm/ncc-orchestrator/README.md).
