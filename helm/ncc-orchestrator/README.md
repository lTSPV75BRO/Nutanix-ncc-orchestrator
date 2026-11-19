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

The chart creates the ConfigMap named by `configMapName` (a starter `config.yaml`
with no clusters; edit it in Settings or replace the ConfigMap) and the PVC named
by `pvcName`. Credentials are provisioned out-of-band under `secretName`. That
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

Storage: `/data` is one PersistentVolumeClaim shared by the API replicas, so
`persistence.accessMode` defaults to `ReadWriteMany` and needs a storage class
that supports it (NFS, CephFS, ...). Set `persistence.storageClass` to such a
class; left empty, the cluster default is used, which is often ReadWriteOnce
only, and the claim then never binds on a multi-node cluster. On RWO-only
storage run one replica of each component (`--set api.replicas=1 --set
ui.replicas=1 --set persistence.accessMode=ReadWriteOnce`).

Backups: every backup the API creates is encrypted by default
(`NCC_BACKUP_ENCRYPTION=required`). Add a `backup-key` entry to the
credentials Secret (it is optional and absent by default) or each request
must carry a passphrase; without either, `POST /api/v1/settings/backups`
answers HTTP 412. Set `NCC_BACKUP_ENCRYPTION=optional` only if plaintext
archives are acceptable.

Upgrades: Helm 4 applies objects with server-side apply. The API changes the
runner CronJob schedule (Settings, Schedule), and an operator may edit the
ConfigMap with `kubectl apply`, so `helm upgrade` can stop with a field
conflict on `.spec.schedule` or the ConfigMap. Either set `cronjob.schedule`
to the live value, or pass `--force-conflicts` to let the chart take
ownership again.

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

With the Prometheus Operator, `prometheusRule.enabled: true` adds alert
rules for a failed runner Job, no successful run (or no new report) for
`staleReportHours` (26 by default), no or too few API replicas, the data
volume below `pvcFreeRatio` free (10%), API replicas on different versions,
and any recovered background-job panic. The Kubernetes rules need
kube-state-metrics and kubelet volume stats; the `ncc_*` rules need
Prometheus to scrape the API's `/metrics` with a `namespace` label. Add
`prometheusRule.labels` if your Prometheus selects rules by label. The rules
are listed in [`docs/PROMETHEUS.md`](../../docs/PROMETHEUS.md).

Keep `images.api.tag`, `images.ui.tag`, and `images.runner.tag` in lockstep.
The API reports installed-component versions from those tags
(`NCC_IMAGE_TAG` / `NCC_ORCHESTRATOR_IMAGE_TAG` / `NCC_UI_IMAGE_TAG`), using
the live CronJob's runner image when it can read it. Settings → Updates
lists any difference, and **Run now** is refused (HTTP 409,
`NCC_API_VERSION_SKEW`) while the runner's major.minor differs from the API
server's. A patch difference or a non-version tag such as `latest` does not
block runs.

RBAC gives the API service account two narrow Roles and nothing else:
`ncc-v2-users-secret` (`get`/`patch` on Secret `ncc-v2-users` for the user
database, plus `create` on Secrets for the first start, which Kubernetes
cannot limit to a name) and `ncc-v2-runner-cronjob` (`get`/`patch` on the
`ncc-orchestrator` CronJob for Settings → Schedule and the version check).
The API never lists, watches or deletes resources and never creates Jobs or
Pods; the runner service account has no API token. `helm upgrade` from an
earlier chart removes the old single `ncc-v2-api` Role.

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
