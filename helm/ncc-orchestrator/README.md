# NCC Orchestrator Helm chart

This chart renders the Kubernetes-native NCC v2 stack: API and UI
Deployments, internal Services, optional Ingress (TLS passthrough to the
UI certificate), RBAC, and the authoritative runner CronJob. Kubernetes
controllers own scheduling, restarts, and image rollouts; the recurring
CronJob schedule can also be viewed, changed and paused from Settings →
Schedule (the chart sets `NCC_RUNNER_CRONJOB=ncc-orchestrator` and grants the
API `get`/`patch` on that CronJob). `helm upgrade` puts `cronjob.schedule`
back, so set a schedule you want to keep in your values file. Default replica
counts are **2** for both API and UI.

Prerequisites are a ConfigMap named by `configMapName`, a PVC named by
`pvcName`, and credentials provisioned out-of-band under `secretName`. That
Secret **must** include `jwt-secret` (`openssl rand -base64 32`) so API
replicas share session JWTs; the API container will not start without it.
UI pods mint a self-signed certificate on the PVC (`/data/tls`) and serve
HTTPS; Settings → Access can replace it with a BYO PEM pair.

```bash
helm install ncc-orchestrator ./helm/ncc-orchestrator \
  --namespace ncc-orchestrator-v2 --create-namespace \
  --set images.runner.tag=2.4.0 \
  --set images.api.tag=2.4.0 \
  --set images.ui.tag=2.4.0
```

Set `ui.origin`, `ingress.host`, `ingress.tlsSecret` and storage names in
an environment values file.

NetworkPolicies match `k8s/`: by default all ingress is denied except the
UI port (from `networkPolicy.uiIngressFrom` and in-cluster namespaces) and
the API from UI pods; add other API clients such as Prometheus with
`networkPolicy.apiIngressFrom`. Egress restriction is opt-in
(`networkPolicy.egress.enabled: true`): list your destinations in
`networkPolicy.egress.cidrs` and `ports` (add 389/636 for LDAP). The API
must reach the Kubernetes API; with Cilium installed a CiliumNetworkPolicy
for it is added automatically (`ciliumKubeAPI: auto`), otherwise add the API
server address and port yourself. Set `networkPolicy.enabled: false` if your
CNI does not enforce NetworkPolicy or you manage policies elsewhere.

A PodDisruptionBudget (`minAvailable: 1`) is created for the API and the UI
when they run more than one replica (`podDisruptionBudget.enabled`). Pin
production images by digest where supported. Never commit credentials or
private TLS keys.

Keep `images.api.tag`, `images.ui.tag`, and `images.runner.tag` in lockstep.
The API reports installed-component versions from those tags
(`NCC_IMAGE_TAG` / `NCC_ORCHESTRATOR_IMAGE_TAG` / `NCC_UI_IMAGE_TAG`).

HTTPS is terminated by the UI LoadBalancer (self-signed on first start, BYO
from Settings). Ingress is off by default (`ingress.enabled: false`). To add
a hostname later:

```yaml
ingress:
  enabled: true
  className: nginx   # or kommander-traefik
  host: ncc.example.com
```

The API Deployment passes `--cookie-secure` and the UI Deployment uses
`--login-mode on` so session cookies work across replicas behind TLS.
Scheduled backups write to `/data/backups` on the PVC with an advisory lock.
See [`k8s/README.md`](../../k8s/README.md) for the full operations runbook.
