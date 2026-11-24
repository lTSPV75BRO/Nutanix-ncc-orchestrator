# NCC Orchestrator — v2.4.0

**Status:** release candidate 2 (`2.4.0-rc.2`).

Since `2.4.0-rc.1`: `verify --online` works. In rc.1 it prints "SKIPPED", although the
release is signed correctly; use rc.2.

## Trying a release candidate

A candidate is a GitHub pre-release. It is never marked Latest, and stable installs are
not offered it. On a test host that runs 2.3.0 or later:

```bash
ncc-orchestrator update --target-version 2.4.0-rc.2
ncc-orchestrator v2-restart
```

From rc.1 use **Updates & backups** or `ncc-orchestrator upgrade`. From a candidate,
updates offer newer candidates and then 2.4.0. Images have the tag `2.4.0-rc.2`. The VM
templates install the version in `NCC_VERSION`. Please report
problems with the output of `ncc-orchestrator upgrade --status` and a support bundle
(`ncc-orchestrator doctor`).

Not tested yet: an upgrade on a real Windows Server, Rocky or macOS host (CI rehearses
it on hosted Windows, macOS and Ubuntu machines; in a systemd container 2.3.0 was
updated to rc.1 and rc.1 to rc.2 and back, from GitHub), and an upgrade from the dashboard.

## Upgrades that cover everything

From 2.4.0 on an upgrade is carried out by the **update helper**, which the service
manager starts only while there is an upgrade to do:

- The new version's helper is installed and tested first. If that fails, nothing was
  changed.
- It then replaces the whole stack, including the supervisor, and brings the service,
  schedule and log timer definitions and the configuration up to the new version.
- When the result is not healthy it puts everything back, also after a crash or reboot
  in between. **Roll back** stays available for 24 hours.
- It works with systemd, the Windows Task Scheduler, launchd and stacks started with
  `v2-start --detach`. Lines added to a systemd unit by hand are kept.
- `ncc-orchestrator upgrade` has `--plan`, `--status`, `--rollback`, `--to` and
  `--bundle` (hosts without internet access). The dashboard shows the plan and progress.
- Helm: `helm upgrade` checks the new images first and the running version afterwards.

**From 2.3.x and earlier:** the step to 2.4.0 is still carried out by the old version.
Two minutes after 2.4.0 starts, and when no run is going on, it installs the helper and
brings the service up to its definitions (one short restart, with rollback). Turn this
off with `NCC_UPGRADE_ADOPT=0`. See "Upgrades" in
[`FEATURES_AND_CONFIG_FLAGS.md`](../FEATURES_AND_CONFIG_FLAGS.md).

## Windows service

- A supervisor that died is started again within a minute by a second scheduled task,
  `ncc-orchestrator-watchdog`. Task Scheduler's own restart setting does not do that. A
  stop with `v2-stop` or `schtasks /End` is respected.
- `v2-stop` and `v2-restart` work from an Administrator prompt; they failed with "Access
  is denied" because the service runs as SYSTEM.
- A schedule no longer replaces the service's task (both were named
  `ncc-orchestrator`). On an affected host run `v2-install-service` as Administrator and
  save the schedule again.

## VM templates

The cloud-init template and `setup-ncc.ps1` failed at the first boot. Both now install
the service with `v2-install-service` and keep the Prism password in `secrets.json`,
which only the administrator can read. `NCC_CONFIG` is gone; the configuration is
`config.yaml` in the install folder. CI runs both templates.

## Local signing tests

An unsigned local build no longer fails looking for the Linux GPG public key.
For a self-signed Windows test certificate, set
`NCC_WINDOWS_TIMESTAMP_URL=none` to avoid contacting a timestamp service.
This is for local testing only; public releases should use a trusted
certificate and timestamp service.

## Free release signing

Every release now carries an Ed25519 signature over `checksums.txt`, verified
by `ncc-orchestrator verify --online --require-signature`. Linux binaries and
stack archives also have GPG detached signatures, with their public key in
the release. Windows and macOS downloads remain checksum- and
provenance-verified but unsigned until publisher identities are available.
The release job tests both keys before it builds, and an Ed25519 key made with openssl
(PEM) is accepted. A release is published from a draft only after every file verified.

## SSO works through the UI proxy

SAML and OpenID Connect redirects and callbacks are no longer refused as an
unversioned dashboard when they pass through `ncc-ui-server`. These identity
requests do not originate from the SPA, so they cannot supply its version
header; only the public OIDC login/callback and `/saml/` routes bypass the
version gate.

Production dashboard bundles also embed their `frontend/package.json` version
in `X-NCC-UI-Version`, rather than relying on a fallback value.

## Deployment agents resolve releases from GitHub

The copy-paste deployment prompt and runbook now require GitHub's authoritative
`releases/latest` API to determine the published version. They explicitly reject
web-search snippets, README badges, branch versions, and Git tags as release evidence.
When GitHub cannot be queried, the agent stops for a verified archive or internal mirror
instead of guessing a GA version.

## See how old the data is

The dashboard keeps showing a cluster's findings from its last good run when
a later run can't reach it, and a stopped schedule leaves yesterday's results
looking current. The Overview now makes both visible:

- Every cluster shows when it was last checked; the time turns amber once it
  is more than a day old.
- A cluster the latest run couldn't reach says how old its data is, for
  example "connection refused · data from 3d ago". Hover the time to see when
  it last returned data and how many runs in a row failed.
- Clusters the latest run didn't include (another environment's) appear with
  results from their own newest run, so filtering by environment works even
  when that environment didn't run last.
- **Scheduled runs have stopped:** when a schedule (the main one or an
  environment's) goes two intervals without a run, a banner says which one
  and when it last ran. Operators get a link to the schedule.

Automation can read the same data from `GET /api/v1/freshness`.

## A page for each cluster

Click a cluster name on the Overview, Findings, Run details or Insights to
open its page: health, failing checks, warnings and data age; why the last
run couldn't reach it; its open findings (filter to needs attention or
acknowledged); versions, environment and when it was last checked; and what
closed recently. Operators can rerun just that cluster from there.

Other detail views were tidied to match:

- The finding details drawer uses the same panels as the rest of the app,
  starting with what was found and a row of actions: open the KB article,
  open the cluster in Prism or on its page, or show the same check on every
  cluster.
- Pressing Enter on the sign-in form signs in when SAML or OIDC buttons are
  also shown.
- Run details show cluster names next to addresses, and the "some clusters
  could not be checked" banner lists which ones and why.
- Insights reports a partial run as "Partial · exit 3", as Runs does, and a
  "Good" health score is green rather than orange.

## Safer restores

Backups taken before an update include the program files, so a rollback can
bring the previous version back. A restore now puts program files back only
when this server has run that exact version, as a whole, before. Otherwise it restores the
data, keeps the installed version running, and says so; install the version
you want from its release. This stops a tampered backup from running its own
code on the server after the restart. **Rollback last update** keeps working,
because the backup taken before the update records the old version as
trusted.

Scripts, programs, keys and the supervisor's working files are never taken from a
backup, and a restore keeps this server's own network and HTTPS settings.

Moving to a new server: data restores as before; install the version you
want there. If you trust a backup from another server, restore it on the
command line with `v2-restore --trust-stack`.

## Tighter permissions on the host

On Linux the service now runs in a systemd sandbox. Existing installs get it from the
update helper after the upgrade. On Windows the stack limits its
secrets, backups and logs to SYSTEM, Administrators and the account that runs it, and
warns when the install folder itself is open to other users.

## MCP talks only to the API

`ncc-mcp-server` used to exec `ncc-orchestrator` as soon as the host started it.
It now is an MCP stdio client of **`ncc-api-server`**: every tool and resource
uses the same REST APIs as the dashboard, with a personal access token
(`NCC_MCP_TOKEN`), a time-bound HMAC proof, and API RBAC. HTTP MCP hosts must
send that token on each request. Set `NCC_MCP_API_URL` when the API is not on
loopback port 8081. Put the token in the MCP host environment, not in tool
arguments. Passwords and custom schedule commands are refused.

The API records a dedicated audit action `mcp.call` (`channel=mcp`, `mcp_tool`)
for those requests. Filter with `GET /api/v1/audit?channel=mcp`. PAT-authenticated
calls include `pat_id`, `pat_name`, and the owning user. Failed calls include
the error description and remediation. UI traffic is tagged with `ui_version`
(from `X-NCC-UI-Version`) and `ui_server_version` (stamped by ncc-ui-server).
The API refuses blacklisted UI builds: **2.2.1** is builtin (node2shell),
unversioned dashboards are refused by default, and admins can add more
versions under Settings → Audit log.

A leftover 2.3.0 MCP binary is refused even if someone rebuilds it with version
`2.4.0`: the stack requires a proof that only the 2.4.0 source produces, and a
PAT the API accepts.

To attach Cursor or Claude: copy [`examples/mcp/cursor.mcp.json`](../../examples/mcp/cursor.mcp.json)
or [`examples/mcp/claude_desktop.json`](../../examples/mcp/claude_desktop.json), set the PAT and API
URL, and reload MCP. Assistants should use the skill in
[`skills/ncc-mcp/`](../../skills/ncc-mcp/). Step-by-step:
[`MCP_SERVER.md`](../MCP_SERVER.md#import-in-five-minutes).
Triggering a run (`run_ncc`, `replay_reports`) now needs the same mutation
opt-in and `confirm: "APPLY"` as upgrades. MCP will not skip TLS verification
or follow a crafted API path off the configured host.

## Hardening from the November audit

A full review of the code, dependencies, deployment files, installers and web UI
(details in [`docs/audit/`](../audit/README.md)) found no authentication bypass,
injection or path traversal and no reachable vulnerable dependency (one
advisory is a reviewed false positive). It did find, and this release fixes,
problems in installation and deployment: the Helm chart's API pod could not
start, the runner CronJob could not write its logs, the Windows installer
wrote an invalid `config.yaml`, left its directory writable by every local
user and could delete backups on a re-run, and LDAP outages counted toward the
sign-in lockout. Also fixed:

- A crash in a background job (scheduled backup, environment schedule,
  digest and others) no longer stops the API server; it is logged and
  counted in `ncc_api_background_panics_total`.
- The GitHub token used for update checks is only ever sent to GitHub.
- Very large Prism Central discovery responses can't exhaust memory.
- The Schedule page shows the saved schedule in a new browser instead of
  the form defaults as "unsaved changes".
- Better screen-reader names on Findings and Users & roles.
- A crash while starting or finishing a run fails that run instead of
  leaving it "running" with its clusters locked.
- Certificate pins (`pin-sha256`) are checked on resumed TLS sessions too.
- The Helm chart gets the same NetworkPolicies and PodDisruptionBudgets as
  `k8s/` (ingress rules and PDBs on by default, egress opt-in). The runner
  CronJob writes under `/data`; the seeded config skips secret preflight so
  Run now works with `NCC_PASSWORD` from the credentials Secret.
- CI adds gosec and gitleaks, drops the job token from checkouts and pins
  every action by commit SHA.
- The Findings page says "finding" throughout, and its toolbar fits at
  800 px.
- Account management on a second API replica no longer shows a stale user
  list or answers 500 on a store conflict (now 409).
- Restoring a backup keeps nested config keys where they were.
- `v2-install-service` installs the log maintenance timer.
- Accessibility failures found with axe-core in a browser: colour contrast,
  labels, ARIA, heading order, target size and keyboard focus.
- More troubleshooting logs: LDAP steps, sign-in failures, backup and
  restore errors, unreadable run summaries and version-skew refusals.

## Blocked versions, MCP and the audit log

- Every tagged release was scanned for known vulnerabilities
  ([`docs/audit/release-scan.md`](../audit/release-scan.md)). 2.0.0 to 2.2.1 are on the
  blocked-versions list; a blocked dashboard shows an error screen with the code
  `NCC_API_UI_VERSION_BLOCKED`, and admins manage the list under System, Updates &
  backups, Blocked versions.
- The MCP server no longer returns raw configuration to the model, its backup tool
  creates a real server-side snapshot, and restore accepts only a backup name.
- The audit log page shows When, Action, Result, User and Client, with the full JSON
  entry one click away; automatic request records are hidden unless enabled.

## Update-check reliability

When the GitHub REST API is rate limited, update checks now resolve the latest
release tag through GitHub's public latest-release redirect. The UI reports
the actual latest version instead of retaining a stale release value.

The result is explicitly marked as degraded because release asset metadata is
not available from the fallback. The **Back up and update** action remains
disabled until a complete, verifiable release response is available. Set
`GITHUB_TOKEN` on the API/orchestrator process for higher GitHub API limits and
normal update installation behavior.

## CodeQL coverage

CodeQL now analyzes the Go services, React frontend, and GitHub Actions
workflows. Runtime cgroup counters are bounded before conversion to `int`, and
the OIDC cookie security policy is explicit for the supported HTTP development
and TLS-terminating proxy modes. The loopback-only throwaway fake OIDC provider
is excluded from production security scanning.

## Security maintenance

The Go toolchain requirement is now `1.27.4`, which fixes
`GO-2026-6859` affecting HTTP/1 servers through a `net/http` header race.
The Go and frontend dependencies are current as of 2026-11-24.

## SAML test provider

Added `tools/fakesaml`, a separate development-only SAML identity provider.
It registers NCC's SP metadata, seeds `alice`, `oscar`, and `vera` with the
NCC role groups, and provides a landing page with a direct **Launch NCC login**
button. It supports HTTPS with generated or persistent self-signed
certificates. Use `-signing-cert` and `-signing-key` to persist the IdP
signing pair; this keeps the certificate published in metadata aligned with
assertions after a provider restart.

## Fixes from environment testing

A pass on a multi-node Kubernetes cluster and a Rocky Linux host with SELinux
enforcing, and an end-to-end Windows install, found and fixed: the Helm API pod
crash-loop and the runner CronJob `mkdir logs: permission denied`, stale user
lists and HTTP 500 instead of 409 on account changes across API replicas, an
invalid generated `config.yaml` and an install-wiping re-run on Windows, LDAP
outages counting toward the lockout, and the missing log maintenance timer on
`v2-install-service` hosts. See the changelog for the full list.

## Validation

- Go vet, tests, and builds pass.
- Frontend tests and production build pass.
- `npm audit` reports zero vulnerabilities.
- Helm lint and template rendering pass; CI installs the chart on kind.
- CI rehearses upgrades, rollbacks and failures on Ubuntu, macOS and Windows
  (`scripts/upgrade-e2e.sh`) and runs the VM templates
  (`scripts/vm-template-test.sh`).
