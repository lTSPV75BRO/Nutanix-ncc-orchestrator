# Environment test results (v2.4.0)

Date: 2026-11-19  
Checkout: branch `v2.4.0` at `9139d8fde5dda6772cb5b14c69e8aef49f75d76a` (`git describe`: `v2.3.0-41-g9139d8fd`; binaries stamped `Version=2.4.0`).  
Kubernetes: `KUBECONFIG=temp/mum.conf` (Mumbai DKP, Kubernetes v1.35.2, Cilium, 3 control-plane + 2 workers). Throwaway namespace **`ncc-test` only**. Existing `ncc-orchestrator-v2` was not modified.  
Linux: `ssh root@10.21.88.27` (Rocky Linux 10.2, SELinux Enforcing, firewalld active, hostname `ncc-server`). A live v2.4.0 supervisor was already running from `/root/test` on ports 8080/8081.  
Fake Prism: in-cluster `ncc-fakeprism:2.4.0` (built from `tools/fakeprism`), not a real Prism.  
Images: built on the Rocky host (`deploy/docker/Dockerfile.{api,ui,runner}`), imported with `ctr -n k8s.io images import` on both workers. Not pushed to a registry.  
No commits, tags, or pushes were made.

## Summary

| Test | Result | Note |
| --- | --- | --- |
| Setup / build | PASS | `go 1.27.4`, Node 26.13.1, `bash scripts/binaryGO.txt --os linux --arch amd64 --skip-tests --set-version 2.4.0` |
| A1 Default install | PASS | 2+2 Ready on two workers at first start; PVC Bound RWX `nfs-storage`; `/api/v1/health/ready` `ready:true` |
| A2 K8S-02 / K8S-01 | PASS | 0 restarts; logs had no "mutually exclusive" or "permission denied"; uid 1000 + pod `RuntimeDefault` |
| A3 Two replicas / user store | FAIL | Store conflict returned **HTTP 500** not 409; replica lists diverged; session cookie across replicas **did** work |
| A3 pod delete | PASS | Deleting **one** API pod: ClusterIP from UI stayed 40/40 OK. (Deleting both via a bad selector broke a local port-forward only.) |
| A4 RBAC + Schedule | PASS | `can-i` matches the chart README; pause (`action=remove`) and cron change (`20 */4 * * *`) applied |
| A5 NetworkPolicy | PASS | API 8081 blocked from unrelated pods; allowed from UI; UI 443 allowed same-ns and other-ns. Egress-on not applied (see A10 helm conflict) |
| A6 Runner CronJob / Run now / skew | FAIL | CronJob: `mkdir logs: permission denied`. API trigger: preflight secrets failure. Version skew **PASS** (HTTP 409 `NCC_API_VERSION_SKEW`) |
| A7 Read-only root fs | FAIL | API/UI Ready with `readOnlyRootFilesystem: true` + `/tmp` emptyDir; health OK; encrypted backup create+verify OK; support-bundle 200. Restore `--no-restart`, retention preview, update-check, and a successful run were **not** all proven. Runner init/container ROFS not set. |
| A8 Resources / PDB / drain | BLOCKED (drain) / PASS (partial) | API 256Mi request / 2Gi limit + `GOMEMLIMIT=1800MiB`. PDBs `minAvailable: 1`. **Did not drain** a production worker. kube-linter: 20 findings |
| A9 Scale 500 / 2k / 7k | PASS (500, 2000) / BLOCKED (7000, kubectl top) | `gen-test-agg`; hey 15s c=20 via port-forward. metrics-server CrashLoopBackOff. 7k not generated |
| A10 Rolling update | PASS | `rollout restart` while probing ClusterIP from UI: **82 ok / 0 fail**; no panic in API logs. `helm upgrade` **failed** SSA conflicts |
| A11 PrometheusRule + trivy | PASS | `PrometheusRule/ncc-orchestrator` accepted (8 alerts). `trivy image` runner 2.4.0: 0 HIGH/CRITICAL |
| B1 Install / SELinux / checksum | PASS (partial) | Checksum OK; `getenforce Enforcing`; `ausearch` no AVCs; `tar -xzf` as root left uid **502:20**. Did not replace the live `/root/test` unit |
| B2 LIN-01 hardening drop-in | BLOCKED | Generated unit is root, `NoNewPrivileges=no`, no `ProtectSystem`. Drop-in not applied to the shared live service |
| B3 Reboot / kill -9 / log timer | FAIL (timer) / BLOCKED (reboot, kill -9) | No `ncc-log-maintenance.timer`. `doctor --fix --only-checks log-sizes` OK. Reboot skipped (shared host) |
| B4 Backup/restore host round-trip | BLOCKED | Not run against a second install dir. In-cluster encrypted backup create+verify **did** work |
| B5 LDAP / LDAPS / StartTLS | BLOCKED | OpenLDAP/389-ds not stood up in this pass |
| B6 TLS and cookies | PASS (partial) | UI HTTPS has HSTS. Session `Secure` cookies observed on the **cluster** API (`--cookie-secure`). Failed-login on the host did not Set-Cookie (expected) |
| B7 Firewall / nmap | PASS | firewalld ports include 8080/8081 (plus 9800/9900, nfs, ssh, cockpit). nmap localhost: 22, 8080, 8081 |
| B8 OWASP ZAP + role matrix | BLOCKED | ZAP not installed; role matrix not replayed here |
| C1–C4 Browser a11y | BLOCKED | No interactive display / axe-core / screen reader in this environment |

## Evidence for FAIL / BLOCKED

### A3 — user-store conflicts and replica drift (FAIL)

Commands: concurrent `POST /api/v1/settings/users` to port-forwards of both API pods; then 24 concurrent `PUT /api/v1/settings/users/raceuser`; `GET` list from each replica.

Observed:

- Concurrent creates: one replica **200** (`user2`), the other **500** with body  
  `error":"the user store was changed by another API replica at the same time; nothing was saved — retry the action"`, `error_code":"NCC_API_INTERNAL"`.  
  Expected: HTTP **409** and `NCC_API_STORE_CONFLICT` (as `writeUserStoreError` already does for PUT/DELETE).
- After the stampede: replica A listed `admin, raceuser, user2`; replica B listed `admin, user2` only. PUT on the stale replica returned **404 user not found**.
- Login with the bootstrap admin password on replica 1, then `Cookie: ncc_session=…` on replica 2: `GET /api/v1/auth/me` **200** `authenticated:true`. JWT sharing **works**.

Cause (verified in code): `handleUsers` POST maps `upsertUser` errors to 500 instead of `writeUserStoreError`; GET list uses in-memory copy without a mandatory reload from the Secret.

Suggested diff (not committed):

```diff
--- cmd/ncc-api-server/usersapi.go
+++ cmd/ncc-api-server/usersapi.go
@@ -63,8 +63,8 @@
 		if err := s.users.upsertUser(username, hash, role, mustChange); err != nil {
-			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
+			writeUserStoreError(w, err)
 			return
 		}
```

Also reload the Secret-backed store on GET (or on a short TTL) so replica B cannot 404 a user replica A just wrote.

Bootstrap password was printed to API logs (`FIRST-RUN ADMIN CREATED`) and stored in Secret key `initial-admin-password` (by design). `NCC_PASSWORD` is Prism, not the UI admin password.

### A6 — CronJob cannot write `logs/` (FAIL)

```
kubectl -n ncc-test create job test-run2 --from=cronjob/ncc-orchestrator
kubectl -n ncc-test logs -l job-name=test-run2
```

Observed:

```
ERR output permissions check failed ... "cannot create directory logs: mkdir logs: permission denied"
Error: output permissions: cannot create directory logs: mkdir logs: permission denied
```

Job `test-run` hit `BackoffLimitExceeded` in ~5s. Image `WORKDIR` is `/workspace` (not a volume); pod `runAsUser: 1000`; fs is not writable. Chart mounts the PVC at `/data`.

Suggested chart change (not committed): set `workingDir: /data` on the runner container (and/or `emptyDir` on `/tmp` + `/workspace`), and point `logging.file` at `/data/logs/...`.

API `POST /api/v1/runs/trigger` (config pointed at fakeprism) returned **202** then `run-meta.json` `success: false` `exit status 2` / `preflight-check failed` (`NCC_PREFLIGHT_VALIDATE_SECRETS`) despite `skip-preflight-check: true` in `/data/config/config.yaml`. Findings list stayed empty (`agg_rows` total 0 until `gen-test-agg`).

Version skew (this part **PASS**):

```
kubectl -n ncc-test patch cronjob ncc-orchestrator --type=json \
  -p='[{"op":"replace","path":"/spec/jobTemplate/spec/template/spec/containers/0/image","value":"docker.io/prajwalnutant/nutanix-ncc-orchestrator:1.9.0"}]'
wget --post-data='{"config_path":"/data/config/config.yaml"}' http://ncc-v2-api:8081/api/v1/runs/trigger
```

HTTP **409**, log: `run refused: runner and API server versions differ` `error_code=NCC_API_VERSION_SKEW`. Tag restored to `2.4.0`.

### A7 — ROFS not fully exercised (FAIL vs “PASS only if all work”)

Patched API and UI:

- `securityContext.readOnlyRootFilesystem: true`
- `emptyDir` at `/tmp`
- pods Ready, `GET /api/v1/health/ready` ready
- `POST /api/v1/settings/backups` without passphrase: **412** (`NCC_BACKUP_ENCRYPTION=required`, no server key in the test Secret)
- with passphrase: **200** `manual-…tar.gz.enc`; verify **200**
- `POST /api/v1/health/diagnostics/support-bundle`: **200**
- update URL `/api/v1/update`: **404**
- CronJob/init ROFS not applied; restore `--no-restart` and retention preview not run

Writable paths actually used: `/data` (PVC), `/tmp` (emptyDir), Kubernetes API (Secret/CronJob). Image layers stay read-only.

### A8 drain (BLOCKED)

Workers run production pods. `kubectl drain` was not executed. PDBs exist (`ncc-v2-api` / `ncc-v2-ui`, `minAvailable: 1`, `ALLOWED DISRUPTIONS: 1`).

`kube-linter lint` on `helm template` (20 errors), remaining: no-read-only-root-fs (api/ui/runner/init), unset CPU/memory (until A8 patch), no-anti-affinity (chart uses topologySpread instead), pdb-unhealthy-pod-eviction-policy, CronJob ttl vs history limits.

After the resource patch both API replicas scheduled on **the same worker** (`…zhxbf`), so topology spread did not hold through the rollout (maxSkew/DoNotSchedule vs surge).

### A9 7000 clusters and `kubectl top` (BLOCKED)

`metrics-server` in `kube-system` is `CrashLoopBackOff` (9953 restarts). No pod memory samples.

`gen-test-agg --clusters 500` and `2000` on the API pod wrote `/data/outputfiles/index.html` (5746 and 23000 rows). 7000 not generated (time + 10Gi PVC caution).

hey (15s, c=20, Authorization bearer, through `kubectl port-forward svc/ncc-v2-api`), **500-cluster** report still on disk:

| Endpoint | n | RPS (n/15s) | p50 | p95 | p99 | 5xx/429 |
| --- | --- | --- | --- | --- | --- | --- |
| `/api/v1/report/data` | 1145 × 200 | ~76 | 185 ms | 517 ms | 1394 ms | none |
| `/api/v1/freshness` | 4650 × 200 | ~310 | 48 ms | 149 ms | 232 ms | none |

Documented 7k figure (~1 GiB API RSS) was **not** compared. Port-forward adds latency vs in-cluster; Windows audit had sub-1 ms cached `/health` — here `/health` through pf was ~75 ms (kubelet hop), not a regression of the handler.

### A10 `helm upgrade` (conflict) vs rollout (PASS)

```
helm upgrade ncc-test ./helm/ncc-orchestrator --reuse-values --set prometheusRule.enabled=true
```

Failed: SSA conflict on ConfigMap `ncc-orchestrator-config` (kubectl apply overlay) and CronJob `.spec.schedule` (API Schedule pause/resume). PrometheusRule was still created.

`kubectl -n ncc-test rollout restart deploy/ncc-v2-api` while wget `/api/v1/health` via the UI pod to `ncc-v2-api:8081`: **ok=82 fail=0**. Logs: no panic.

### B1 ownership / B2 / B3 (partial)

```
expected=bddfc2bd10c517ac1e30ba4c5e8e29d24e79447d0d0fb3fbab8ecbb0bfecbd3a
actual=  bddfc2bd10c517ac1e30ba4c5e8e29d24e79447d0d0fb3fbab8ecbb0bfecbd3a
CHECKSUM_OK
ls -ln /opt/ncc-v2-audit/bin  -> uid 502 gid 20 (Mac packer), extracted as root without --no-same-owner
getenforce -> Enforcing
ausearch -m avc -ts recent -> <no matches>
```

Live unit `/etc/systemd/system/ncc-orchestrator.service` (`WorkingDirectory=/root/test`, `ExecStart=… v2-supervise --install-dir /root/test`): `User=` (root), `NoNewPrivileges=no`, `PrivateTmp=no`, `ProtectSystem=no`. Confirms LIN-01. Drop-in and dedicated-user were **not** applied so the shared test stack stayed up.

`systemctl list-timers`: `ncc-sched-ncc-orchestrator.timer` only — **no** `ncc-log-maintenance.timer` (cloud-init writes that timer; this host was installed via `v2-install-service`). `ncc-orchestrator doctor --fix --only-checks log-sizes --install-dir /root/test`: `[ ok ] Log file sizes`.

Reboot and `kill -9` of supervisor children: not run.

### B5, B8, C\* (BLOCKED)

No OpenLDAP container, no ZAP (`zap.sh` / `zap-cli` missing), no display for axe-core / keyboard / 320px / VoiceOver.

## New findings

### ENV-01 — User-store conflict mapped as 500 `NCC_API_INTERNAL`
- Severity: **Medium** · Confidence: **verified**
- Evidence: A3 concurrent POST; `usersapi.go` POST branch.
- Fix: call `writeUserStoreError` (409 `NCC_API_STORE_CONFLICT`) and retry once in the handler.

### ENV-02 — API replicas serve a stale in-memory user list
- Severity: **High** · Confidence: **verified**
- Evidence: GET `/api/v1/settings/users` differed across pods; PUT 404 on the replica that missed the create.
- Fix: reload Secret (`loadVersioned`) before GET/mutate, or watch/poll `resourceVersion`.

### ENV-03 — Runner CronJob WORKDIR is not writable
- Severity: **High** · Confidence: **verified**
- Evidence: `mkdir logs: permission denied` in `test-run2` logs; image `WORKDIR /workspace`.
- Fix: `workingDir: /data` (and writable `logs` under the PVC) in `helm/ncc-orchestrator/templates/cronjob.yaml`.

### ENV-04 — Chart PVC has no `storageClassName`
- Severity: **Medium** · Confidence: **verified**
- Evidence: template requests RWX but omits class; cluster default is `nutanix-volume` (typically RWO). Install only Bound RWX after adding `storageClassName: nfs-storage` at apply time (reverted in the tree).
- Fix: values key `persistence.storageClass`.

### ENV-05 — Default credentials Secret cannot satisfy `NCC_BACKUP_ENCRYPTION=required`
- Severity: **Low** · Confidence: **verified**
- Evidence: POST `/api/v1/settings/backups` 412 until a client passphrase; `backup-key` optional and absent.
- Fix: document that Kubernetes installs must set `backup-key` or `NCC_BACKUP_ENCRYPTION=optional`.

### ENV-06 — cloud-init `tar -xzf` preserves foreign UIDs
- Severity: **Low** · Confidence: **verified**
- Evidence: extract to `/opt/ncc-v2-audit` owned by `502:20`.
- Fix: `tar --no-same-owner -xzf` in `deploy/cloud-init/ncc-v2.yaml`.

### ENV-07 — Host unit has no sandboxing (LIN-01 still open)
- Severity: **Medium** · Confidence: **verified** (inspection only)
- Evidence: `systemctl show ncc-orchestrator` `NoNewPrivileges=no` `ProtectSystem=no` `User=` empty.
- Fix: emit a hardened drop-in from `v2-install-service`; do not require operators to edit the generated unit.

### ENV-08 — `ncc-log-maintenance.timer` missing on v2-install-service hosts
- Severity: **Low** · Confidence: **verified**
- Evidence: `systemctl list-timers` has schedule timer only; cloud-init is what creates log-maintenance.
- Fix: install the timer from `v2-install-service` as well.

### ENV-09 — kube-linter still flags ROFS / resources / PDB eviction policy
- Severity: **Low** · Confidence: **verified**
- Evidence: 20 kube-linter hits on the rendered chart. Matches HARD-01 remaining work.
- Fix: chart `readOnlyRootFilesystem`, default requests/limits, `unhealthyPodEvictionPolicy`.

## What was not run (and why)

- **A5 egress NetworkPolicy** (`networkPolicy.egress.enabled=true`, LDAP 389/636, webhook ports): `helm upgrade` hit SSA conflicts; not forced with `--force`.
- **A8 `kubectl drain`**: production workers.
- **A9 7,000 clusters** and **`kubectl top`**: time/PVC; metrics-server down.
- **A7 restore `--no-restart`, retention preview, runner+init ROFS, UI “Run now” click**: API trigger used instead of UI; restore skipped after 412/policy.
- **B2 drop-in / dedicated user, B3 reboot and kill -9, B4 second-directory restore, B5 LDAP, B8 ZAP, C1–C4**: live shared `/root/test` stack; no ZAP; no browser/display.
- **nmap from outside the host** toward “API internal only”: scan was `127.0.0.1`. firewalld **does** publish 8081/tcp on `public`.

## Commands worth repeating

```bash
export KUBECONFIG=/Volumes/Mac/Programs/Nutanix-ncc/temp/mum.conf
kubectl -n ncc-test get pods,pvc,pdb,prometheusrule,cronjob,networkpolicy
kubectl -n ncc-test exec deploy/ncc-v2-ui -- wget -qO- http://ncc-v2-api:8081/api/v1/health/ready
```

Leave `ncc-test` in place until someone `helm uninstall ncc-test -n ncc-test` and `kubectl delete ns ncc-test`. LoadBalancer IP `10.136.70.66` is the throwaway UI, not production `ncc-orchestrator-v2` (`10.136.70.65`).
