# Troubleshooting

Use this when runs fail before or during NCC checks.

## TLS / certificate errors

- **Symptom:** `tls: failed to verify certificate` or `x509: ... certificate is not standards compliant`.
- **Lab / self-signed:** Set `insecure-skip-verify: true` in config or pass `--insecure-skip-verify` on the CLI.
- **Production:** Fix Prism certificates (SANs, chain) and keep `insecure-skip-verify: false`.

## Prism Central and multiple clusters

- **Symptom:** `NCC-40023` or “invalid node IP(s)” for the cluster you expect.
- **Cause:** The v4 API lists **all** registered clusters (e.g. AOS + Prism Central). The orchestrator must match your `--clusters` address to the **correct** entity and send **CVM IPs** for that cluster.
- **Fix:** Use an address that matches the target cluster in Prism (PC VM IP for PC, external IP or CVM IP for AOS). Use `ncc-orchestrator discover-clusters` to list valid addresses.

## `--insecure-skip-verify` ignored

- Ensure you are not only setting it in a config file that is not loaded; or upgrade to **v1.0.0+**, which fixed a viper binding bug with `discover-clusters` subcommand flags overwriting root flags.

## Exit codes

| Code | Meaning |
|------|----------------|
| 0 | Success (all clusters completed without runner-level error) |
| 1 | Failure (e.g. all clusters failed, or other error) |
| 2 | Configuration / validation error |
| 3 | Partial success (some clusters OK, some failed) |

See **`outputfiles/run-summary.json`** for `exit_code` and per-cluster `clusters[]`.

## Reading the server logs

Start here for most problems. The API server writes every message twice: to
standard error (readable text, for `journalctl` and `kubectl logs`) and to
`logs/ncc-api.log` as JSON lines (rotated at 50 MB, ten files kept). Secret-like
values are masked. The same files are on **System → Logs** in the UI.

- **Raise the level** without a restart on **System → Logs**, or start with
  `--log-level debug` (`NCC_LOG_LEVEL`). Debug adds per-step detail such as
  directory lookups. Set it back to `info` afterwards.
- **Follow one request.** Every response carries an `X-Request-ID`; the same id
  is on the request's log line, its audit entry and the error toast in the UI.
  Search the log for it: `grep <id> logs/ncc-api.log`.
- **Useful fields** on each JSON line: `level`, `msg`, `subsystem` (`auth`,
  `ldap`, `freshness`, `runs`, ...), `username`, `client_ip`, `request_id` and
  `error`.
- **Audit trail.** Changes and sign-ins are in `logs/ncc-audit.log` and also
  appear in the server log as `msg="audit"`; failed actions are warnings and
  include the error where it is safe to show.
- **Runner output** is in `logs/ncc-runner.log` and, per cluster, in
  `nccfiles/`. **System → Health & repair → Support bundle** collects the
  doctor report, process logs and redacted config in one download.

What to look for:

| Symptom | Log line to find |
| --- | --- |
| Directory (AD/LDAP) users cannot sign in | `ldap: could not connect` (network or TLS), `ldap: service-account bind failed` (bind DN or password), `ldap: user search failed`, `ldap: user filter matched several entries` (tighten the user filter), `ldap: user bind rejected` (wrong password, locked or disabled account; the server's message says which, for example AD data code 775 is locked and 52e a bad password; at `info`), `ldap: user not found` (at `debug`) |
| Sign-in fails for everyone | `sign-in failed` warnings give the user, client address, whether a local account and a directory were tried, and whether the account locked; `sign-in could not reach the directory` means the directory, not the password |
| A user is locked out | `sign-in failed ... locked=true`; the lockout counts only refused credentials, not directory outages |
| "Last checked" or data age is missing for a cluster | `freshness: run summary is not valid JSON` or `has no valid timestamp` names the file that is being skipped |
| Run now answers 409 `NCC_API_VERSION_SKEW` | `run refused: runner and API server versions differ` shows both versions; roll out matching image tags |
| A backup or restore failed | The `settings.backup.create` or `settings.backup.restore` audit entry now carries a trimmed, path-redacted error; the orchestrator's own output is in the `orchestrator command failed` warning |

## GitHub `--update` rate limits

- Set **`GITHUB_TOKEN`** in the environment for higher API rate limits when calling `GET /repos/.../releases/latest`.
- The token only reads public releases. Use a **fine-grained token with public repository read-only access** (no repository or account permissions), or a classic token with **no scopes**. If GitHub reports that the token carries scopes (for example `repo` or `workflow`), the update check prints a warning naming them; replace the token with a narrower one.
- The token is sent only over HTTPS to `api.github.com`, `github.com` and `githubusercontent.com` hosts.

## v2 web stack: login, HTTPS, and SSO

### Browser warns about the certificate / "Not secure"

- **Cause:** `ncc-ui-server` serves **HTTPS by default** with a **self-signed** certificate (auto-generated by `v2-start` under `<install-dir>/tls/`). Browsers don't trust self-signed certs.
- **Fix:** Accept the warning for an internal host, import the cert into your trust store, or install a CA/PKI-signed cert from **Settings → Access → HTTPS / TLS** (`PUT /api/v1/settings/tls`). To regenerate the self-signed cert for the current hostname/IP, use **Generate self-signed certificate** (`POST /api/v1/settings/tls/generate`). To run plain HTTP behind a trusted proxy/loopback on a host/VM, start with `--ui-insecure-http`. **On Kubernetes** the same Settings form writes `/data/tls`; UI pods hot-reload. Publish Service port 443 (or Ingress passthrough) so `Secure` cookies stick.

### Login succeeds but the page bounces back to the login screen

- **Cause:** Session cookies are marked `Secure` when HTTPS is active; a browser will **refuse to store a `Secure` cookie over plain HTTP** on a non-localhost host, so the session is dropped and the forced password-change screen never shows.
- **Fix:** Use the default HTTPS URL (`https://<host>:8080`), or run the UI with `--ui-insecure-http` (which issues non-`Secure` cookies) if you intentionally serve plain HTTP. `v2-start` wires `--cookie-secure`/`--cookie-insecure` on the api-server automatically based on whether UI TLS is active. On non-localhost HTTP the login page also warns, links to the HTTPS URL, and lets you copy the certificate fingerprint or download the PEM (`GET /api/v1/tls/public`, no private key).

### Splash spinner never finishes after Generate cert / first load

- **Cause:** After a certificate change the browser may hang on `/auth/me` or `/health` while it re-evaluates TLS.
- **Fix:** Wait for Retry / Sign in (splash times out after ~6–8s) instead of an infinite spinner. Header health is the API reached through this UI and is clickable to retry.

### Alerts pager does not change the rows

- **Cause:** Older UI builds showed pagination outside the table but still passed every row to the virtual table, so page 2 looked identical to page 1.
- **Fix:** Upgrade to v2.4.0. The table now receives only the current page slice; changing page or page size replaces the visible alerts.

### SAML SSO returns `origin not allowed` (`NCC_API_FORBIDDEN`)

- **Cause:** The IdP POSTs its assertion to `/saml/acs` as a cross-site request whose `Origin` isn't in the CORS allowlist.
- **Fix:** Upgrade to the current build — `/saml/*` is exempt from the CORS origin allowlist (the signed assertion + relay-state cookie are the security boundary), `ncc-ui-server` proxies `/saml/*` to the api-server, and the SP cookie is `SameSite=None`. Ensure the **SAML root URL is the browser-facing UI origin** (e.g. `https://ncc.example.com`) and the UI is on **HTTPS** (required for the `SameSite=None; Secure` cookie).

### Operator Settings toasts “this action requires the admin role” without a click

- **Cause:** Older UIs loaded Schedule’s config picker from admin-only `GET /api/v1/settings/configs`, and Settings kept hidden tabs mounted so that call ran on Connection/Runs/Logs/Audit as well.
- **Fix:** Upgrade to the current v2.4.0 build. Schedule uses `GET /api/v1/runs/configs` (operator+). Hidden Settings tabs unmount.

### In-app update from 2.1.0 copies files but the old version keeps serving

- **Cause:** 2.1.0’s detached restart script joined `if`/`then` with `; `, producing invalid `then;` so `systemctl restart` never ran. The new binaries sat on disk under the still-running old API.
- **Fix:** Current v2.4.0 joins that script with newlines and installs `ncc-orchestrator-apply-update.path` (`v2-install-update-watcher`) so a binary replace restarts the supervisor even when the running API cannot. After the first upgrade to this build (one manual `systemctl restart` if the watcher was not already present), later in-app updates restart on their own. Rollback last update restores binaries only when the pre-update snapshot was taken with `--include-stack`.

### A viewer/operator can't see any alerts

- **Cause:** The account is a member of a **cluster group** that currently resolves to no (or the wrong) clusters. Cluster groups are **opt-in isolation** — membership *restricts* visibility to that group's clusters.
- **Fix:** Remove the user from cluster groups to give them unrestricted (all-cluster) visibility, or add the intended clusters/Prism Centrals to their group. An **ungrouped** viewer/operator already sees every cluster's alerts.

### `origin not allowed` after restoring a backup

- **Cause (older builds):** Restore overwrote the host's `.ncc-v2-start.json` networking with the backup's values, dropping the local `ui-allowed-origins`.
- **Fix:** The current `v2-restore` **preserves host-specific networking/TLS** (CORS origins, advertise/backend URLs, listen addresses, `--ui-insecure-http`, UI TLS paths). Upgrade and re-restore, or add the current origin under Settings/`--cors-origin` and restart.

## Alerts table

### Prism Central alerts are unavailable

- **Symptom:** The dashboard shows NCC findings, but the PC · Beta source is
  empty or reports that Prism Central alerts are unavailable.
- **Fix:** Confirm Settings → Features has **Prism Central alerts** on
  (`GET /api/v1/features`, `pc_alerts`). Then confirm `pcs` or
  `prism-central-url` is configured, the active username/password can view
  alerts, and the Prism Central endpoint is reachable. For internal
  certificates, configure `ca-bundle` or use the existing TLS policy. The
  API caches successful PC responses briefly; use Refresh after correcting
  configuration.

### Insights or PC · Beta shows “turned off”

- **Cause:** An administrator turned that view off in Settings → Features.
- **Fix:** An administrator can enable it from that page or from the note
  on the page itself (`PUT /api/v1/features`). Health checks keep running
  while the view is off.

### A resolved NCC finding came back

- **Expected:** A later run that still reports the same cluster and check
  clears the resolve mark and moves the row to **Returned**
  (`GET /api/v1/alerts/ncc-dispositions`). Acknowledge is not cleared
  automatically.
- **If the mark never returns:** Confirm the later run finished after the
  resolve (`run_at` / report timestamp) and that the check title still
  matches. Marks live in `outputfiles/ncc-alert-dispositions.json`.

### New API routes are missing from System → Developer → API explorer

- **Cause:** An older API catalog or UI bundle is still running, so
  `/api/v1/features` and `/api/v1/alerts/ncc-dispositions` are not listed
  on the backend page (`GET /api/v1/meta/routes`) or in the UI explorer.
- **Fix:** Deploy the current v2.4.0 API and UI together, then refresh
  the route list. The live mux, OpenAPI, and explorer presets all include
  those paths.

### PC alerts take a long time to appear

- **Expected behavior:** The dashboard first requests unresolved alerts with
  `GET /api/v1/alerts?resolved=No`, so the initial table can render without
  downloading the full history. It then warms the `resolved=all` response in
  the background and shows a loading indicator while that request is active.
- **If unresolved alerts are also slow:** Check the API server log and
  Prism Central reachability. The API applies `$filter=isResolved eq false`
  before pagination, fetches configured PC targets concurrently, and caches
  each filter response independently.
- **Tune caching:** Set `pc-alerts-cache-ttl: 5m` (or a longer duration) in
  the config for fewer Prism Central requests. Set it to `0` to disable the
  cache. Use `?refresh=1` only when a forced refresh is required.

### PC alerts show a cluster UUID instead of the name

- **Cause:** Prism Central often identifies the cluster by `clusterUUID` when
  `clusterName` is empty. Older builds displayed that UUID as the cluster
  column.
- **Fix:** Upgrade to the current v2.4.0 API/UI. Discovery now maps `ext_id`
  to name and IP (`cluster_map` on `GET /api/v1/alerts`). Confirm
  `discover-clusters` can list the Prism Central, then Refresh the PC source.

### Historical `"NCC run failed"` rows show `FAIL` severity (fixed in v2.1.1)

- **Symptom:** A cluster that couldn't be reached at all (connection refused, DNS failure, auth rejection, timeout) shows up in the Alerts table as a `"NCC run failed"` row tagged **`FAIL`** — indistinguishable from a real failing NCC check — and the FAIL totals (Alerts hero pill, dashboard hero cards) and the drilldown diff's "new failures" count include it.
- **Cause:** v2.1.0 introduced this synthetic row so a cluster whose run fails outright no longer silently vanishes from the report, but tagged it `FAIL` — the same severity as a genuine finding — which overstates "NCC never ran against this cluster" as "NCC found a real problem." No data is lost or corrupted; it's a classification issue only.
- **Fix (upgrade):** v2.1.1 and later tag this row `UNKNOWN` instead, add an actionable remediation hint to its Detail (alongside the real error), and leave the KB column empty (there's no real KB article for a connectivity failure). Severity priority is also corrected so `WARN` outranks `ERR` in the Alerts table's default sort and the dashboard's hero pills/filter chips. Upgrade with `ncc-orchestrator update` (or **Settings → Access → Software updates** in the UI); the fix applies to runs going forward, it does not rewrite historical reports.
- **Workaround (no upgrade required):** Cross-reference the cluster's entry in `run-summary.json` (`ok: false`, `error`, `error_class`) or the Runs table — both already correctly report these as connectivity/run failures rather than fabricated FAIL counts — instead of trusting Alerts-table FAIL totals when a `"NCC run failed"` row is present.
- **Not affected:** Real NCC check findings (a genuine `FAIL`/`WARN`/`ERR`/`INFO` from NCC itself) are unaffected either way; this only concerns the synthetic row generated when the run against a cluster couldn't happen at all.

## `v2-install-service` / systemd fails with `status=203/EXEC` on SELinux hosts

- **Symptom:** `systemctl status ncc-orchestrator.service` shows `Active: activating (auto-restart) ... (Result: exit-code)` and `Main PID ... (code=exited, status=203/EXEC)`. `journalctl -u ncc-orchestrator.service` shows `Unable to locate executable '<path>/bin/ncc-orchestrator': Permission denied` even though the file exists, is `0755`, and **running it directly in an interactive shell works fine**. `ausearch -m avc` shows no denials, so it looks like there's no SELinux problem at all.
- **Cause:** On RHEL/Rocky/CentOS/Fedora with SELinux **enforcing**, a binary under a home directory (e.g. `/root/<install-dir>/bin/...`) is labeled `admin_home_t` by policy default. `systemd` execs services in the `init_t` domain, which the targeted policy denies from executing `admin_home_t` content — but that specific denial is `dontaudited`, so it never shows up in the audit log even though it's actually being blocked. Interactive shells run unconfined (`unconfined_t`), which is why running the same binary by hand "just works" and makes this look like a permissions/corruption bug instead of SELinux. Builds before the fix below only ran `restorecon` during `v2-install-service`, which resets a file to its *policy default* context (`admin_home_t` here) rather than forcing it — so it didn't help.
- **Fix (upgrade):** Builds containing the `v2-install-service` SELinux fix relabel the whole `<install-dir>/bin/` directory to `bin_t` with `chcon` and persist it with `semanage fcontext` (so a later `restorecon`/`fixfiles` won't silently revert it) whenever the service is (re)installed. Re-run `ncc-orchestrator v2-install-service --install-dir <dir> --now` after upgrading to pick up the relabel, or just restart the service if it's already installed with a fixed binary in `bin/`.
- **Immediate workaround (any build, no reboot/reinstall required):**

  ```bash
  chcon -R -t bin_t /path/to/install-dir/bin
  # Optional but recommended: persist across future relabels/security scans
  semanage fcontext -a -t bin_t '/path/to/install-dir/bin(/.*)?'
  restorecon -R -v /path/to/install-dir/bin
  systemctl restart ncc-orchestrator.service   # or your --service-name
  ```

  Confirm with `ls -laZ /path/to/install-dir/bin` (expect `..._t:bin_t:s0` on each binary) and `systemctl status ncc-orchestrator.service` (expect `Active: active (running)`).
- **Not affected:** Hosts with SELinux `disabled`/`permissive` (`getenforce`), and non-systemd platforms (Windows Task Scheduler, macOS launchd).

## Scheduled runs (cron / systemd timer)

### A scheduled run produces no output / the dashboard data goes stale

- **Symptom:** The schedule is installed and the timer fires on time, but no new reports appear and `doctor` warns the latest run is hours old. The scheduler log (`logs/ncc-scheduler.log`, or `journalctl -u ncc-sched-<task>.service`) shows `configuration validation failed: at least one cluster must be provided` (exit 2).
- **Cause (older builds):** The generated runner executed the orchestrator without an **absolute** `--config` (and, for systemd, from a `WorkingDirectory` that is not the install dir), so `config.yaml` was never found and the run aborted before any output was written.
- **Fix:** Upgrade to the current build and re-save the schedule from **Settings → Schedule** (or re-run `create-schedule` with an absolute `--config /path/to/config.yaml`). The api-server now anchors the schedule's config/log paths to absolute install-root paths, and the systemd backend refuses to install a config-less timer. Confirm in **Settings → Schedule** that **Config file** shows the expected absolute path (or inspect the generated runner under `logs/`).

### Dashboard shows "Stale · Nd ago" even though the scheduler log shows healthy runs

- **Symptom:** `tail logs/ncc-scheduler.log` (or `journalctl -u ncc-sched-<task>.service`) shows recent, successful runs — "report written for N successful cluster(s)" — but Settings/Dashboard still shows the last run as several days old, and new alerts never appear in the Alerts table.
- **Cause:** `config.yaml`'s `output-dir-filtered`, `output-dir-logs`, and `run-history-dir` are **absolute** paths, and `v2-backup`/`v2-restore` round-trip `config.yaml` byte-for-byte. Restoring a backup that was taken on a **different install directory** (e.g. a backup from `/root/ncc-orchestrator` restored onto `/root/test`) leaves those three values pointing at the **old** install's directory. The scheduled run then keeps succeeding — it just silently writes every report to the old, unserved directory instead of the current install's `outputfiles/`, which is what the live api-server's dashboard actually reads. Builds before the fix below only re-anchored the *scheduler's pointer* to `config.yaml` on restore (fixing a related "Scheduler Health looks stale" issue), not the output-dir values recorded **inside** `config.yaml` itself.
- **Confirm this is the cause:** compare `output-dir-filtered:` in `config.yaml` against the install directory (`dirname` of `config.yaml`) — if the run recorded in the scheduler log is fresh but `<install-dir>/outputfiles/run-summary.json`'s `timestamp` is not, check whether the path in `config.yaml` actually matches `<install-dir>/outputfiles`.
- **Fix (upgrade):** Builds containing the restore self-heal fix re-anchor `output-dir-filtered`/`output-dir-logs`/`run-history-dir` to the current install directory automatically whenever a backup is restored (`POST /api/v1/settings/restore`), reported in the response's `self_heal_notes`.
- **Immediate workaround (any build):** Edit `config.yaml` and point the three keys back at the current install directory, e.g.:

  ```yaml
  output-dir-filtered: /path/to/install-dir/outputfiles
  output-dir-logs: /path/to/install-dir/nccfiles
  run-history-dir: /path/to/install-dir/outputfiles/runs
  ```

  Then trigger a run (wait for the next scheduled tick, or run `ncc-orchestrator --config /path/to/install-dir/config.yaml` once by hand) so a fresh report lands in the directory the dashboard actually serves. Reports already written to the old directory aren't automatically moved — copy them over first if you want to keep that history, or just let the next run replace them.

### A systemd timer run is marked `failed` but reports were still written

- **Symptom:** `systemctl status ncc-sched-<task>.service` shows `failed` / a non-zero `ExecMainStatus`, yet the dashboard shows fresh data.
- **Cause:** Exit code **3** is *partial success* — some clusters failed (e.g. an unreachable Prism Central) while others succeeded and a report was written. systemd treats any non-zero exit as a unit failure.
- **Fix:** Expected when some targets are unreachable. Fix routing/credentials for the failing clusters, or remove them from `config.yaml`, so the run exits 0. The failing clusters are listed in `logs/ncc-scheduler.log` and `outputfiles/run-summary.json`.

## Dashboard shows "This dashboard version is blocked" (`NCC_API_UI_VERSION_BLOCKED`)

- **What it means:** the API refused this dashboard because its version is on the
  blocked-versions list (a release with a known vulnerability, or an entry an
  administrator added). The screen shows the error code, the advisory ids, the
  version that matched and a request id to quote.
- **Fix:** upgrade `ncc-ui-server` and `frontend-dist` to 2.4.0 or later (a stack
  archive replaces both), then reload the page. Dashboards before 2.4.0 cannot
  declare a version, so they are refused as a group; the plain error text reaches
  them as a failed sign-in or a toast.
- **Look at the list:** System, Updates & backups, Blocked versions (admins). It
  shows the shipped entries with their advisories and lets you add or remove your
  own, and switch off refusing undeclared versions if you must run an old dashboard
  against this API for a while.
- **API clients are not affected:** only requests that identify as the dashboard
  (`X-Requested-With: ncc-ui`, `X-NCC-UI-Version`) are checked. MCP and script
  clients use their token and are limited by its role.

## Windows

### `config.yaml` does not parse (`found unknown escape character`)

- **Cause:** a config written by a version 2.3.x `setup-ncc.ps1` held paths such
  as `"C:\ProgramData\NCC\v2\outputfiles"` in double quotes. In YAML a backslash
  starts an escape, and `\P` and `\o` are not valid ones.
- **Fix:** change the double quotes around those values to single quotes (or use
  forward slashes), then run `ncc-orchestrator validate-config --config <file>`.
  The 2.4.0 installer writes single-quoted paths.

### `setup-ncc.ps1` refuses to run

- **Cause:** the install directory already has files. The installer no longer
  deletes an existing install, because it holds the user database, token and
  backups.
- **Fix:** to upgrade, use **System → Updates & backups** or
  `ncc-orchestrator update`. To start over on purpose, back up first, then set
  `NCC_FORCE_CLEAN=1` and run the installer again.

### Other users cannot open the install directory

- **Cause:** by design. The installer limits the directory to SYSTEM and
  Administrators because the stack runs as SYSTEM and the directory holds the API
  token and user database.
- **Fix:** open the dashboard in a browser instead, or use an elevated prompt for
  files in the install directory.

## Kubernetes

### Chart installs, but the API pod crash-loops or the runner Job fails (older charts)

- **Symptoms:** the API log says `--users-db and --users-db-secret are mutually
  exclusive` or `write token file: ... permission denied`; the runner Job log
  says `cannot create directory logs: mkdir logs: permission denied`.
- **Cause:** Kubernetes `args` replace the image's default command, and charts
  before 2.4.0 passed only some of the path flags. The container layout then made
  the stack-aware defaults point at `/usr/local`, which uid 1000 cannot write, and
  the runner started in the image's read-only `/workspace`.
- **Fix:** upgrade to the 2.4.0 chart, which passes every path flag and runs the
  runner from `/data`. The `k8s/` manifests already did.

### Settings, Run now fails with `NCC_PREFLIGHT_VALIDATE_SECRETS` (older charts)

- **Cause:** the starter `config.yaml` in charts before 2.4.0 did not set
  `skip-preflight-check` or `secrets.provider: env`, so the API's preflight could not
  find the Prism password that the credentials Secret injects, although the CronJob
  (which passes `--skip-preflight-check`) worked.
- **Fix:** upgrade the chart, or add `skip-preflight-check: true` under
  `runner.execution` and `secrets.provider: env` to the ConfigMap.

### The PVC stays `Pending`

- **Cause:** the chart asks for `ReadWriteMany` (two API replicas share `/data`),
  and the cluster's default storage class is often `ReadWriteOnce` only.
- **Fix:** set `persistence.storageClass` to an RWX class (NFS, CephFS, ...). On
  RWO-only storage run one replica of each component and set
  `persistence.accessMode=ReadWriteOnce`. See
  [`helm/ncc-orchestrator/README.md`](../helm/ncc-orchestrator/README.md).

### `helm upgrade` fails with a field conflict

- **Cause:** Helm 4 uses server-side apply. The API changes the runner CronJob
  schedule (Settings → Schedule), and someone may have edited the ConfigMap with
  `kubectl apply`, so Helm sees fields owned by another manager.
- **Fix:** set `cronjob.schedule` to the live value, or pass `--force-conflicts`
  to take ownership again.

### Backups answer 412 `NCC_BACKUP_ENCRYPTION_REQUIRED`

- **Cause:** backups are encrypted by default and the credentials Secret has no
  `backup-key`.
- **Fix:** add a `backup-key` entry, send a passphrase with the request, or set
  `NCC_BACKUP_ENCRYPTION=optional` if plaintext archives are acceptable.

### API pods CrashLoop with `NCC_JWT_SECRET is required`

- **Cause:** Kubernetes API replicas must share one HS256 key. The Secret key
  `jwt-secret` is required; an in-memory fallback would break logins across pods.
- **Fix:** Add the key and roll the Deployment:

  ```bash
  kubectl -n ncc-orchestrator-v2 create secret generic ncc-v2-secrets \
    --from-literal=prism-password='...' \
    --from-literal=api-token='...' \
    --from-literal=jwt-secret="$(openssl rand -base64 32)" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n ncc-orchestrator-v2 rollout restart deployment/ncc-v2-api
  ```

### Settings → Access → HTTPS cannot upload a certificate

- **Cause (host/VM):** local accounts / writable user store is not enabled.
- **Fix:** Enable local login so the user database can persist TLS policy.
- **Kubernetes:** the form is supported. Files land on `/data/tls`; publish
  Service port 443 (or Ingress passthrough). A LoadBalancer that only has
  port 80 cannot keep `Secure` session cookies.

### Installed components: `ui-server: Component not found`

- **Cause:** Older API images exec `ncc-ui-server` from the API container, which
  does not ship that binary.
- **Fix:** Upgrade the API image. Current builds report versions from
  `NCC_IMAGE_TAG` / `NCC_ORCHESTRATOR_IMAGE_TAG` / `NCC_UI_IMAGE_TAG`. Keep
  those three tags the same when rolling out.

### Scheduled backups fail or write an empty archive

- **Cause:** Older builds treated `/data/config` as the install dir, so
  `auth/` and `logs/` files were outside the backup confinement path.
- **Fix:** Upgrade the API image. Backups now use the PVC root (`/data/backups`)
  and include the nested layout. Only one replica snapshots at a time
  (`.ncc-backup.lock`).

