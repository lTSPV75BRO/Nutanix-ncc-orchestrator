# Findings register

Every finding from the 2026 reviews of v2.2.1 and v2.4.0, with its current
status. The narrative for the security items is in
[`security-assessment.md`](security-assessment.md); the method and results of
the checks are in [`README.md`](README.md).

**Severity:** Critical (severe compromise, data corruption or outage), High,
Medium, Low. **Priority:** P0 fix now, P1 before the next release, P2 planned,
P3 optional. No finding was rated Critical.

## Open

| ID | Area | Finding | Severity | Priority |
| --- | --- | --- | --- | --- |
| REL-05 | Release | Fixed in the workflow, not yet proven by a release: the published v2.3.0 files were uploaded by hand and none has a build attestation (v2.2.0 and v2.2.1 miss some). `release.yml` now attests every file, publishes from a draft and verifies it afterwards. Before the 2.4.0 tag the owner must create the `release` environment, the signing secrets and `NCC_RELEASE_PUBKEY` (see [`RELEASE_CHECKSUMS.md`](../RELEASE_CHECKSUMS.md)). The old releases cannot be fixed in place. | Medium | P1 |
| LIN-02 | Install | The sandboxed Linux service (LIN-01) still runs as root and keeps CAP_SYS_ADMIN, because it writes units, calls `systemctl` and runs `chcon`. `ProtectSystem`, `ProtectHome` and `PrivateTmp` are opt-in. A run with SELinux enforcing is not tested. Planned as phase U5 of the [upgrade helper](../UPGRADE_HELPER_PLAN.md). | Low | P3 |
| WIN-05 | Install | An install folder that ordinary Windows users can write to is reported, not closed, because other software may share it. The permissions (WIN-02) were not tested as SYSTEM or elevated. | Low | P3 |
| A11Y-03 | UI | A real screen-reader pass (NVDA, VoiceOver) is still manual. Zoom to 400% is tested (reflow at 320 px, which covers 200%); enlarging only the text is not. | Low | P3 |

Accepted risks: none. **DEP-03** (`GO-2026-6909` in `crewjam/saml`, a reviewed false
positive) is no longer reported by `govulncheck` since 2026-11-24, so its entry in
`scripts/govulncheck-accepted.txt` was removed.

## Resolved

### Security

| ID | Finding | Fix |
| --- | --- | --- |
| SEC-01 | A deleted local account kept its session and role until expiry, and a re-created name revived old tokens. | Deletion records a tombstone; sessions issued before it are rejected. |
| SEC-02 | An operator could direct `--prom-dir` outside the install root. | Path-valued extra arguments are confined like config paths. |
| SEC-03 | Anyone who could reach the UI could force a reset of the admin password. | Off by default; opt in with `--allow-admin-self-reset`. |
| SEC-04 | The MCP `create_schedule` tool installed an arbitrary command. | Custom commands are never accepted; schedules go through `PUT /api/v1/schedule`. |
| SEC-05 | The MCP server passed the Prism password on the command line. | MCP tools refuse password arguments; credentials stay on the API server (`secret://`). |
| SEC-06 | Email header injection through a templated subject in the runner. | Headers are flattened and RFC 2047 encoded; CR/LF addresses rejected. |
| SEC-19 | **High.** A crafted backup could plant files that the server runs later: `run/upgrade-rollback/` (the supervisor copied it over `bin/` and ran it; proven with a harmless program), scheduler scripts in `logs/`, and schedule or log paths that inject lines into a systemd unit or crontab. It bypassed SEC-07. | A restore never takes `run/`, `tls/`, `backups/`, other files in `bin/`, scripts, programs or keys, refuses links, and keeps this host's listener, origin, backend and HTTPS settings. The supervisor rolls back only to a copy it saved or a version this server ran. Schedule and log paths are checked. |
| SEC-07b | The trusted-stack ledger judged each file alone, so a restore could mix files of versions that each ran here; other spellings of a program path skipped the check. | Program files are restored only when the result is exactly one recorded set. Aliases (case, trailing dot, 8.3 name) count as program files. |
| MCP-1 | `confirm: "APPLY"` is supplied by the model, so nothing asked the person. | Mutating tools ask through MCP elicitation when the client supports it (`NCC_MCP_CONFIRM`). A malicious host can still accept by itself and clients without elicitation are unchanged under `auto`, so the PAT's role stays the hard limit. |
| UPG-01 | An upgrade could not change the supervisor, the service definitions or the schedule units, and a rollback did not cover them. | The update helper (`cmd/ncc-orchestrator/upgrade_*.go`): installed and tested first, then upgrades everything from outside the service and puts all of it back on failure, crash or reboot. Adoption brings 2.3.x and earlier hosts up. `scripts/upgrade-e2e.sh` rehearses it on Linux, macOS and Windows in CI. |
| WIN-07 | **Medium.** The Windows supervisor task relied on `RestartOnFailure`, but Task Scheduler applies it only when the program cannot be started: a supervisor that died stayed down until the next boot or doctor run. | A second task, `ncc-orchestrator-watchdog`, looks every minute, stops the servers that were left behind and starts the supervisor's task again. Not after `v2-stop`, `schtasks /End`, a disabled task or during an upgrade. Existing hosts get it by adoption or `doctor --fix`. `v2-stop` on Windows now removes the pid file of a supervisor that had died. Rehearsed in `scripts/upgrade-e2e.sh`. |
| INS-02 | **Medium.** `deploy/cloud-init/ncc-v2.yaml` called `v2-install-service` with flags it does not have, so the first boot failed with "unknown flag" (since 2.2.0). `deploy/windows/setup-ncc.ps1` registered `v2-start --detach --self-heal`, which Windows refuses, so its task never started the stack. Both put a reference to `NCC_PASSWORD` into the configuration without giving the service that variable. | Both use `v2-install-service` (new `--api-listen`, `--ui-listen`) and store the password in `secrets.json` (owner only), which the service and schedules read. A test checks the flags; `scripts/vm-template-test.sh` runs both templates in CI. |
| LIN-01 | The systemd units ran as root with no sandboxing. | Sandboxed: exposure 9.6 to 5.9 in `systemd-analyze security`. Tested on systemd 239, 252 and 257 (start, runs, schedules, update, log maintenance). Reinstall the service to apply it. |
| WIN-02 | The product set no Windows permissions on its secrets; only the installer did. | `internal/winacl`: applied at start, on every secret write and after updates and restores; the `secrets-perms` doctor check repairs it. |
| SEC-07 | A restore could replace the stack binaries with unverified files. | Restore installs only program files this host has run (`.ncc-trusted-stack.json`). |
| SEC-13 | The MCP configuration tools returned the raw config file, including inline credentials, to the model. | Tool results are scrubbed in one place; only the redacted copy is returned. |
| SEC-14 | The "redacted" config view masked only password, token and secret lines. | Key-name based redaction (webhooks, API keys, HMAC, passphrases, URL credentials); references and switches kept. |
| SEC-15 | `backup_v2_stack` downloaded an archive, discarded it and reported success; restore accepted any path-like name. | It creates a server-side snapshot and returns its name; restore accepts only a backup name. |
| SEC-18 | The audit log API answered any signed-in viewer (users, addresses, token names, request details). | Reading it needs the operator role, matching the dashboard; regression test. |
| SEC-17 | A build with an embedded release key accepted a release that had no signature asset. | Signed builds require the signature when installing an update; unsigned builds are unchanged (signing stays opt-in at build time). |
| SEC-16 | The engine's check for a legacy `ncc-mcp-server` parent used `ps`, so it never matched on Windows. | The parent image name is read through the Windows API; tested from a parent named `ncc-mcp-server.exe`. |
| SEC-12 | **High.** The MCP server (0.1.13 to 2.3.0) ran tools and read resources with no credential. | Tools call the REST API only (no orchestrator subprocess). Requires `NCC_MCP_TOKEN`, HMAC proof (`ncc-mcp-proof-v1`), and API RBAC. Stamping version 2.4.0 on a 2.3.x binary is refused. |
| SEC-08 | The UI-to-API transport honoured `HTTP(S)_PROXY`, sending the admin token through a proxy. | The transport ignores proxy variables. |
| SEC-09 | An unknown role in a signed session mapped to admin. | Unknown or empty roles are rejected. |
| K8S-01 | The Helm API pod had no security context; its init container ran as root. | Non-root, `fsGroup` and seccomp at pod level; restricted init container. |
| WIN-01 | The Windows install directory under `ProgramData` was writable by every local user while the stack ran as SYSTEM. | The installer restricts it to SYSTEM and Administrators. |
| CI-01 | The publishing workflows relied on a comment for "no cache" and could restore a poisoned cache. | Caches are disabled explicitly. |
| WIN-03 | The installers could delete an existing install, and checked only a SHA-256 from the same release URL. | Neither deletes a non-empty directory without `NCC_FORCE_CLEAN=1`; both check the build attestation (`NCC_VERIFY_ATTESTATION` = `auto`, `require`, `off`). The Linux script was run in a container for 11 cases; the Windows change was checked by parsing it and running its `gh` step. |
| HARD-01 | Containers had a writable root filesystem and no default requests. | Read-only root filesystem with a `/tmp` `emptyDir` on every container (Helm and `k8s/`), default requests, `seccompProfile` and no service account token on the UI. A run, backup, verify, retention preview and restore pass under it on `kind`. Limits stay unset on purpose (sizing depends on fleet size). |
| SEC-10 | The rate-limit counters were readable without sign-in. | They need a viewer session or token. `openapi.json` and `meta/routes` stay public for the Swagger page. |
| SEC-11 | bcrypt used the library default cost (10); a plain-HTTP host install gave no hint that its cookie is not `Secure`. | Cost 12, with older hashes upgraded at the next sign-in (not a password change; sign-in timing stays equal for unknown names). System Health warns when sessions run over plain HTTP. |
| FRESH-01 | `/api/v1/freshness` returned every schedule and the global overdue flag to cluster-scoped users. | Environment schedules and the flag follow the caller's cluster groups; the main schedule stays visible. |
| OPS-03 | No secret scanning in CI. | gitleaks runs in CI; a full-history scan is clean. |

### Correctness and reliability

| ID | Finding | Fix |
| --- | --- | --- |
| K8S-02 | The Helm API pod crash-looped on first start (also in the published 2.3.0 image). | The chart passes the full flag set; a configured Secret or `NCC_USERS_DB` suppresses the `--users-db` default. |
| ENV-03 | The Helm runner CronJob failed with `mkdir logs: permission denied`. | It runs from `/data` with explicit paths and `--skip-preflight-check`. |
| WIN-04 | The Windows installer wrote an invalid `config.yaml` (backslash paths in double quotes). | Paths are single-quoted. |
| CI-02 | Nothing installed the Helm chart in CI. | `helm-smoke` job: `kubeconform` on the chart and `k8s/`, then `scripts/helm-smoke-test.sh` installs the chart on `kind` and runs it against a simulated cluster. |
| REL-06 | Both installers failed with their default settings: they downloaded from `.../download/2.4.0/`, but release tags start with `v`, so GitHub answered 404. | They try `v<version>`, then the bare version for mirrors. |
| ENV-11 | Once, both API replicas landed on one node during a rollout. | `matchLabelKeys: [pod-template-hash]` spreads each revision on its own. Six rollouts on two workers stayed spread; the original fault did not reproduce without it either. |
| REL-07 | On a systemd host, a restore, TLS change or unsupervised update through the API left the stack stopped: the restart script ran inside the service and was killed with it. | Inside the service it now queues one `systemctl --no-block restart`, which systemd finishes. On EL9 and Debian 12 the old code fails and the new one passes for an uploaded and a server-side restore, a TLS change and an unsupervised update. |
| COR-10 | Windows: the final state of an update could be lost while the dashboard read the state file, so a finished update looked stuck. | The rename is retried. |
| COR-11 | `v2-start --wait-ready` stopped a healthy stack that served the default self-signed certificate. | An untrusted certificate counts as up. |
| COR-12 | Re-creating a schedule after a restore or by `doctor` turned a cron schedule into "every 4 hours" and switched its lock on; without `task_name` it looked for `ncc-sched-<nil>`; a restore left a file named `<nil>`. `create-schedule` accepted a cron text with extra fields, which become a command in the crontab. | Cron and lock are kept, missing values are empty, a schedule saved only as a preview is not installed, and the cron text must be five fields or a macro. |
| INS-01 | A server started by hand beside the service survived a restart from the UI: the stopping supervisor deleted its pid file. `doctor` called a schedule of type `auto` unknown, warned after a schedule was removed, and looked for a `ncc-scheduler.sh` that nothing writes. | The supervisor removes a pid file only when it names its own process or a dead one. The check resolves `auto`, accepts removed and preview-only schedules, and looks at the crontab entry, the systemd units or the Windows task itself. |
| WIN-06 | **Medium.** On Windows the schedule and the supervisor used the same task name by default, so saving a schedule replaced the supervisor task (the stack did not come back after a reboot), installing the service replaced the schedule, and removing or uninstalling the schedule deleted the supervisor. The UI also never showed a Windows schedule as installed. | The default schedule's task is `ncc-orchestrator-schedule`. The supervisor's task is never listed, replaced or removed as a schedule; a schedule under the old name is moved when the service is installed or the schedule is saved. |
| REL-08 | **Medium.** Release candidates were not supported: nothing checked that the tag equals `VERSION` (a `v2.4.0-rc.1` tag would have built programs that call themselves 2.4.0), a candidate would have been published as Latest, 2.4.0 candidates of the MCP server were treated as older than 2.4.0, an install on a candidate was never offered the next one, and no images were built for a release the workflow published. | `scripts/release-tag-check.sh`; pre-releases are published as such; the updater follows candidates from a candidate or with `--include-prerelease`; `release.yml` calls the image build. |
| REL-09 | **Medium.** Wrong release keys were found only at the end of the build, or not at all: a PEM public key broke the Go link step, another invalid public key produced programs that skip signature checks, nothing checked that the signing key belongs to the public key, and openssl keys were refused. | `scripts/release-signing-keys.sh` test-signs with both keys first; `release-pubkey`; `binaryGO.txt` refuses an invalid `NCC_RELEASE_PUBKEY`; PEM accepted (`releasesign.go`). |
| COR-09 | `upsertYAMLScalar` dropped indentation, so a restore moved nested config keys to the top level. | Indentation is kept; a `$` in a value is literal. |
| COR-08 | A dropped or busy LDAP directory during sign-in counted as a wrong password and fed the lockout. | Only a refused credential counts; tested against a real LDAP conversation. |
| ENV-01 | Creating a user on a second replica returned HTTP 500 on a store conflict. | Retried once, then 409. |
| ENV-02 | The user list on a replica missed users another replica had just created. | Account-management requests refresh the shared store first. |
| COR-01 | Windows: process liveness always reported "dead". | Correct process-liveness check on Windows. |
| COR-02 | The run extra-argument allowlist was out of sync with the runner. | Allowlist aligned with the runner; Runs page argument builder. |
| COR-03 | Windows: the secrets-file provider rejected every file. | Permission check fixed. |
| COR-04, COR-05, COR-06 | Webhook sender nil-request panic; runner always attempted SMTP AUTH; post-restore path healing skipped Windows. | Fixed. |
| DATA-01 | Lost updates to the user store across API replicas. | Reload before write, file lock, Kubernetes `resourceVersion` precondition, 409 on conflict. |
| DATA-02 | A restore had no pre-restore snapshot or rollback. | Transactional restore with automatic rollback. |
| REL-01 to REL-04 | Long operations cut off by timeouts; SMTP without timeouts; no graceful shutdown; readiness did not check dependencies. | Timeouts, graceful shutdown and `/api/v1/health/ready`. |
| OBS-01, OBS-02 | No request ids or per-route metrics. | `X-Request-ID` and `ncc_http_request*` metrics. |
| LOG-01 | Thin logs for LDAP, sign-in failures, backup and restore errors, run summaries and version skew. | Structured warnings with user, client address and request id; never passwords. |
| ENV-12 | Settings, Run now failed the secrets preflight (`NCC_PREFLIGHT_VALIDATE_SECRETS`) on Helm installs while the CronJob succeeded. | The chart creates the ConfigMap with `skip-preflight-check: true` and `secrets.provider: env`. |
| ENV-04, ENV-05, ENV-06 | Helm PVC had no storage settings; backup key requirement was undocumented; cloud-init `tar` kept foreign owner ids. | `persistence.*` values, chart README, `--no-same-owner`. |
| ENV-08 | `v2-install-service` hosts had no log maintenance timer. | The timer is installed with the service. |
| ENV-10 | `helm upgrade` (Helm 4 server-side apply) conflicts with the API-edited CronJob schedule. | Documented in the chart README (`--force-conflicts`). |
| PERF-01 | Authenticated `/api/v1/health` took 18 ms on Windows. | Display paths cached for 30 s (0.8 ms). |
| OPS-01 | No Windows CI job; golden fixtures broke on CRLF checkouts. | Windows job added. |

### UI, dependencies and quality

| ID | Finding | Fix |
| --- | --- | --- |
| A11Y-01 | WCAG A/AA failures found with axe in a browser: colour contrast, label in name, ARIA role, heading order, target size, scroll focus. | Fixed; 0 violations on 16 routes in three themes (empty-data pages). |
| A11Y-02 | Accessibility was checked by hand, without data, 400% zoom or CI. | `npm run test:a11y`: 163 Playwright and axe-core checks on the demo build (every route and tab in three themes, overlays, 320 px reflow, keyboard, screen-reader proxies), CI job `a11y`. It found and fixed popups that stayed invisible with reduced motion, focus lost after dialogs, contrast, headings, unlabelled controls and reflow. |
| MAINT-01 | `goNCC.go` (18.9k lines), the API `main.go` (6.3k) and `AccessSection.tsx` (3.4k) were very large. | Split into 36, 17 and 10 files by area as pure moves; every declaration and comment is identical to before. Two single functions stay large (`newRootCmd`, `writeAggregatedHTMLSingle`). |
| UX-01, UX-02, UX-05 to UX-08 | Health pill, setup step titles, run-arguments builder, per-cluster freshness and stale-schedule banner, config-preference 404. | Fixed or delivered. |
| ONB-01, LOGIN-01 | Browser-local setup modal; sign-in form hidden on directory-only installs. | Server-derived setup checklist; redesigned sign-in. |
| DEP-01, DEP-02 | Go module bumps; npm dev-tool advisories. | Updated; `npm audit` reports 0. |
| OPS-02 | Dependabot and a full `npm audit`. | Added. |
| TEST-01 | Monaco test timeout flake. | Fixed. |
| TEST-02 | `SessionIdleGuard.test.tsx` failed once in a loaded run. | It already used fake timers; the likely cause is the one-second default of `waitFor`. Raised to five seconds for all tests. |
