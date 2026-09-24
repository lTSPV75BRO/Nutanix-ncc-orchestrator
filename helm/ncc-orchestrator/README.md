# NCC Orchestrator Helm chart

This chart renders the Kubernetes-native NCC v2 stack: API and UI
Deployments, internal Services, the TLS Ingress, RBAC, and the authoritative
runner CronJob. Kubernetes controllers own scheduling, restarts, and image
rollouts. Default replica counts are **2** for both API and UI.

Prerequisites are a ConfigMap named by `configMapName`, a PVC named by
`pvcName`, a TLS Secret named by `ingress.tlsSecret`, and credentials
provisioned out-of-band under `secretName`. That Secret **must** include
`jwt-secret` (`openssl rand -base64 32`) so API replicas share session JWTs;
the API container will not start without it.

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

Keep `images.api.tag`, `images.ui.tag`, and `images.runner.tag` in lockstep.
The API reports installed-component versions from those tags
(`NCC_IMAGE_TAG` / `NCC_ORCHESTRATOR_IMAGE_TAG` / `NCC_UI_IMAGE_TAG`).

HTTPS is terminated at Ingress. In-app Settings → Access → HTTPS / TLS
upload is not supported (`409`). To let cert-manager issue the certificate:

```yaml
ingress:
  certManager:
    enabled: true
    clusterIssuer: letsencrypt-prod
```

The API Deployment passes `--cookie-secure` and the UI Deployment uses
`--login-mode on` so session cookies work across replicas behind TLS.
Scheduled backups write to `/data/backups` on the PVC with an advisory lock.
See [`k8s/README.md`](../../k8s/README.md) for the full operations runbook.
