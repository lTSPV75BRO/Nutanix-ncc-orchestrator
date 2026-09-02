# NCC Orchestrator Helm chart

This chart renders the Kubernetes-native NCC v2 stack: API and UI
Deployments, internal Services, the TLS Ingress, RBAC, and the authoritative
runner CronJob. Kubernetes controllers own scheduling, restarts, and image
rollouts.

Prerequisites are a ConfigMap named by `configMapName`, a PVC named by
`pvcName`, a TLS Secret, and credentials provisioned out-of-band under
`secretName`.

```bash
helm install ncc-orchestrator ./helm/ncc-orchestrator \
  --namespace ncc-orchestrator-v2 --create-namespace \
  --set images.runner.tag=2.2.0 \
  --set images.api.tag=2.2.0 \
  --set images.ui.tag=2.2.0
```

Set `ui.origin`, `ingress.host`, `ingress.tlsSecret`, storage names, and
destination-specific NetworkPolicies in an environment values file. Pin
production images by digest where supported. Never commit credentials or
private TLS keys.
