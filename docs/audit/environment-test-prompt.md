# Environment tests that need a cluster and a Linux host

This is a prompt for an engineer or an AI coding assistant that has access to
a real Kubernetes cluster and a Rocky Linux host. It covers what the
2026-11-19 audit could not run on a Windows workstation. Copy everything
below the line into the assistant.

---

## Prompt

You are testing NCC Orchestrator, branch `v2.4.0` of
`https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator`. An audit on a
Windows machine already ran the unit tests, linters, scanners, a live API
probe, a browser accessibility scan and a single-node `kind` deployment. Your
job is to run the tests that need a real cluster, a systemd host with SELinux,
and a larger data set, and to report what passes, what fails and what you
could not run.

### Ground rules

1. Work on a throwaway namespace (`ncc-test`) and throwaway hosts. Do not touch
   production data, real Prism clusters or real credentials. Use the fake Prism
   server in `tools/fakeprism` for anything that talks to Nutanix.
2. Do not push, tag, open pull requests or change any branch. Check out
   `v2.4.0` read-only. If a fix is obvious, describe it in your report with the
   exact diff; do not commit it.
3. Never print or store real secrets in the report. Generated test passwords
   and tokens are fine, but redact them in the output.
4. For each test, record PASS, FAIL or BLOCKED, with the command you ran and
   the key lines of output. A FAIL needs the exact observed versus expected
   behaviour and a guess at the cause. Do not report a test as passing unless
   you ran it.
5. Write the report to `docs/audit/environment-test-results.md` in your
   checkout (do not commit it) and also paste a summary table in your reply.

### Setup

- Go 1.27.4 and Node 26 (frontend). `git clone`, `git checkout v2.4.0`.
- Build: `go build ./...`. Frontend: `cd frontend && npm ci && npm run build`.
- Runner image: `Dockerfile.runner` copies a prebuilt binary, so first run
  `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/ncc-orchestrator-linux-amd64 ./cmd/ncc-orchestrator`
  (create `dist/`; check `.dockerignore` line 23 excludes it and use
  `--build-arg`/a temporary ignore edit that you do not commit). Build the API
  and UI images from `build/docker/Dockerfile.api` and `Dockerfile.ui`.
- Fake Prism: `go run ./tools/fakeprism` (read its flags first).

### Already known, so you can compare

- `go test -race ./...`, `golangci-lint`, `gosec`, `govulncheck`, `trivy` and
  `gitleaks` are clean on Windows. 403 frontend tests pass.
- With the fixed Helm chart on a single-node `kind` cluster (one replica,
  `ReadWriteOnce` storage patched in), the API pod ran as uid 1000 and passed
  readiness, and the Secret-backed user store was created.
- Before the fix, the chart's API pod crash-looped because the container
  layout triggered the stack-aware defaults (finding K8S-02). Confirm it stays
  fixed.
- The API image ran correctly with `--read-only --cap-drop ALL
  --security-opt no-new-privileges --tmpfs /tmp` and only `/data` writable.
- Authenticated `/api/v1/health` answers in under 1 ms after caching
  (it was 18 ms on Windows).
- The Windows installer was run end to end on a Windows 11 machine (install,
  ACLs, SYSTEM tasks, backup, verify, restore, re-run refusal). Windows is not
  part of this prompt.

### Part A: Kubernetes (multi-node, RWX storage, a CNI that enforces NetworkPolicy)

A1. **Default install.** Pre-create the namespace objects the chart expects
(ConfigMap `ncc-orchestrator-config` from `examples/`, Secret
`ncc-orchestrator-credentials` with `api-token`, `jwt-secret`, `password`).
`helm install` with default values (2 API and 2 UI replicas, RWX PVC from your
storage class). Expect: all pods Ready, API and UI replicas on different nodes
(topology spread), PVC bound, `GET /api/v1/health/ready` returns ready.
Report the storage class used.

A2. **Regression K8S-02 and K8S-01.** Confirm no crash-loop on first start;
the API logs must not contain "mutually exclusive" or "permission denied". With
`kubectl get pod -o yaml` confirm the pod-level `securityContext`
(`runAsNonRoot`, uid/gid 1000, `fsGroup` 1000, seccomp `RuntimeDefault`) and
the init container's restricted context.

A3. **Two replicas and the shared user store.** Create users concurrently from
two replicas (port-forward to each pod, or use `kubectl exec`) and confirm no
lost updates: conflicting writes must return HTTP 409 and succeed on retry.
Sign in on replica 1, send the session cookie to replica 2: it must be valid
(same `NCC_JWT_SECRET`). Delete a pod during a request loop and report
errors seen and recovery time.

A4. **RBAC.** With `kubectl auth can-i --as=system:serviceaccount:<ns>:ncc-v2-api`
verify the API can get/patch only the Secret `ncc-v2-users` and get/patch the
runner CronJob, and cannot list or read other Secrets, create Jobs or pods.
Confirm Settings, Schedule can still pause and change the CronJob schedule.

A5. **NetworkPolicy enforcement.** From a throwaway pod (for example
`nicolaka/netshoot`) in the same namespace and in another namespace, test:
UI port from allowed sources, API port 8081 from the UI pods only, API from an
unrelated pod (must be blocked), and the egress rules if enabled
(`networkPolicy.egress.enabled=true`; LDAP 389/636 and webhook ports must be
configurable). Report which tests were blocked or allowed and whether it
matches the chart README.

A6. **Runner CronJob.** Point the config at the fake Prism server, trigger the
CronJob with `kubectl create job --from=cronjob/ncc-orchestrator test-run`.
Expect a run summary in the output volume and Findings visible in the UI. Test
"Run now" from the UI. Then deliberately set the runner image tag to a
different major.minor and confirm the API refuses with HTTP 409
`NCC_API_VERSION_SKEW` and logs "run refused: runner and API server versions
differ".

A7. **Read-only root filesystem (HARD-01).** Set `readOnlyRootFilesystem: true`
on the api, ui, runner and init containers, adding `emptyDir` mounts for `/tmp`
where needed. Exercise: sign-in, a run, backup create, backup verify, restore
(`--no-restart`), retention preview, support bundle, update check. List every
path that needs to be writable. Report PASS only if all of them work.

A8. **Resource and availability settings.** Set requests and limits (api 256Mi
request, 2Gi limit with `apiGoMemLimit: 1800MiB`) and confirm no OOM during A9.
Check the PodDisruptionBudgets with `kubectl drain` of one node (the API must
stay available) and `kubectl-linter` or `kube-linter lint` on the rendered
chart; list findings that remain.

A9. **Scale.** Generate a fleet with the fake Prism server or by writing
synthetic run summaries (see `docs/SCALING.md`) at 500, 2,000 and 7,000
clusters. With `hey -z 30s -c 50` against `/api/v1/report/data`,
`/api/v1/freshness` and the Findings listing endpoint, record requests per
second, p50/p95/p99 and the API pod memory (`kubectl top`). Compare with the
documented 7,000-cluster figure (~1 GiB). Report any 5xx or 429.

A10. **Rolling update and shutdown.** `helm upgrade` with a new image tag while
running a request loop; report dropped requests. Confirm graceful shutdown
(in-flight requests complete, no panic in logs).

A11. **Chart extras.** If the Prometheus Operator CRDs exist, confirm the
`PrometheusRule` is accepted and the alerts load. Run `trivy image` on the
runner image you built.

### Part B: Rocky Linux host (systemd, SELinux enforcing, firewalld)

B1. **Install.** Follow `deploy/cloud-init/ncc-v2.yaml` by hand (or boot a VM
with it) using a release tarball built from this branch
(`scripts/binaryGO.txt` shows the build matrix). Confirm checksum verification
works, the service starts, `getenforce` is `Enforcing`, and the UI and API
answer. Check `ausearch -m avc -ts recent` for denials and report each one.
Note who owns the extracted files (cloud-init's `tar -xzf` runs as root without
`--no-same-owner`).

B2. **Service hardening (LIN-01).** The generated unit runs as root with no
sandboxing. As a systemd drop-in (do not edit the generated file), try:
`NoNewPrivileges=yes`, `PrivateTmp=yes`, `ProtectSystem=full`,
`ProtectHome=read-only`, `ProtectKernelTunables=yes`, `ProtectKernelModules=yes`,
`ProtectControlGroups=yes`, `RestrictSUIDSGID=yes`, `LockPersonality=yes`,
`ReadWritePaths=<install dir>`. Then re-run: UI sign-in, a run against fake
Prism, backup create and restore with restart, "apply update" watcher, and
`systemctl restart` triggered from the UI. Report which directives break
anything and why. Also try running as a dedicated user and list what then
fails.

B3. **Reboot and self-heal.** Reboot; confirm the stack returns without
intervention. `kill -9` the supervisor's child processes and confirm they
restart with backoff. Confirm the `ncc-log-maintenance.timer` runs and rotates
logs (`doctor --fix --only-checks log-sizes`).

B4. **Backup and restore round trip.** Create plaintext (if allowed by
`NCC_BACKUP_ENCRYPTION`) and encrypted backups, run verify-only on each, then
restore into a second install directory. Restore a backup that contains the
program files: it must be accepted only for files this host has run
(`.ncc-trusted-stack.json`), and `--trust-stack` must be required otherwise.
Confirm a restore rolls back automatically if it fails half way (corrupt the
archive on purpose).

B5. **LDAP / Active Directory sign-in.** Run an OpenLDAP or 389-ds container
with a service account, a user, a wrong-password case, a disabled account and a
group mapped to the admin role. Check: good sign-in and role mapping; wrong
password gives the generic 401 and counts toward the lockout; a disabled
account is rejected; **stopping the directory in the middle of the test must
not count toward the lockout** (finding COR-08) and must log "ldap: could not
connect" or "ldap: user bind failed for a non-credential reason". Repeat with
LDAPS using a private CA and with StartTLS. Check the new log lines at
`--log-level debug`.

B6. **TLS and cookies.** Run with auto TLS (`--auto-tls-dir`) and behind a
reverse proxy that terminates TLS (`--cookie-secure`). Confirm the session
cookie has `Secure`, `HttpOnly` and `SameSite=Strict` as appropriate, and
that plain HTTP installs warn about the missing `Secure` flag.

B7. **Firewall and exposure.** With `firewalld`, confirm only the UI and API
ports are opened, and that the API port is not reachable from outside if the
operator keeps it internal. Run `nmap -sV` against the host and report open
ports.

B8. **Dynamic security scan.** Run OWASP ZAP (baseline and an authenticated
scan with a viewer account) against the UI and API. Report alerts of medium or
higher. Repeat the role matrix: for every route in `/api/v1/meta/routes`,
as viewer and operator, check the denial matches the documented minimum role.

### Part C: Browser checks (if you have a browser and a display)

C1. Build the demo (`frontend`, demo mode) or run the stack with the fake Prism
data so the Overview, Findings, Insights, Runs, Cluster and Settings pages
contain real rows. Run axe-core (WCAG 2.0 to 2.2 A and AA plus best practice,
including colour contrast) on each route in the dark, light and IT
Professional themes. The 2026-11-19 pass found 0 violations on empty-data
pages; report anything new on populated pages, especially tables, charts, tags
and drawers.
C2. Keyboard only: every action reachable with Tab, Shift+Tab, Enter, Space and
Escape; visible focus; no focus traps; drawers and modals return focus.
C3. 400% zoom and a 320 px wide viewport: no horizontal scrolling, no clipped
content.
C4. Screen reader (NVDA, VoiceOver or Orca): sign in, open a finding, change a
schedule, and read the sidebar and the health indicator.

### Report format

Start with a summary table (test id, result, one-line note). Then a section per
FAIL or BLOCKED test with evidence. Then a list of new findings, each with:
id, title, severity (Critical, High, Medium, Low), confidence (verified,
strongly suspected, unverified), evidence, and a recommended fix. Finish with a
short list of anything you did not run and why. Keep every claim tied to
something you executed.
