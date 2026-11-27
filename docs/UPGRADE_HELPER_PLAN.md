# Upgrade helper: design and plan

**Status:** built for 2.4.0 (phases U0 to U5; U5 is LIN-02, the `ncc` service on Linux).
Written 2026-11-24. [How it was built](#how-it-was-built) lists where the code
differs from this plan. Operating it: `FEATURES_AND_CONFIG_FLAGS.md`, "Upgrades".

Goal: one click (or one command) moves an install from any supported version to
the next one, including the parts an upgrade cannot change today (the
supervisor, its service definition, scheduled tasks), on every platform, and
puts everything back if the result is not healthy.

## What is missing today

The supervisor carries out upgrades (`supervise_upgrade.go`): snapshot, install,
restart the API and UI, health checks, rollback. It cannot finish its own:

| Gap | Effect |
| --- | --- |
| The supervisor does not restart itself. | It runs the old program until the next service restart (`supervisor_restart_pending`). |
| Service definitions change only when `v2-install-service` is run by hand. | The 2.4.0 systemd sandbox (LIN-01) and Windows folder permissions (WIN-02) do not reach upgraded hosts. |
| Schedule units change only when the schedule is saved again. | Same, for scheduled runs. |
| The old version's code runs the upgrade. | A bug in the 2.3 upgrade code cannot be fixed by 2.4. |
| Rollback covers programs, not service definitions or migrated configuration. | A bad unit cannot be put back automatically. |
| The service must be able to write its own units. | It has to run as root (LIN-02). |

## The idea

A small **update helper**, outside the service, that does nothing until an
upgrade is requested. Upgrades run in two stages:

1. **Helper first.** The running API or supervisor fetches the *target
   version's* helper, verifies it, installs it with its own service definition,
   and starts it in probe mode. Nothing else has changed yet. If the probe
   fails, the old helper is put back and the upgrade stops.
2. **Then the stack.** The helper, now running the target version's code,
   upgrades the supervisor, API, UI, frontend, service definitions, schedules
   and configuration, checks the result, and commits or rolls back.

So upgrade logic is always the *new* version's, run by a process that survives
every restart it causes.

```mermaid
flowchart LR
  UI[UI / CLI: upgrade to X] --> S1
  subgraph S1[Stage 1: API or supervisor]
    B[backup] --> D[download and verify helper X] --> I[install helper + its unit] --> P[probe]
  end
  P -- fails --> K[old helper back, nothing changed]
  P -- healthy --> S2
  subgraph S2[Stage 2: helper X]
    F[preflight] --> G[stage and verify stack X] --> N[snapshot] --> A[stop, swap, migrate] --> V[start and verify]
  end
  V -- healthy --> C[commit, go dormant]
  V -- not healthy --> R[restore snapshot, start, verify] --> Z[rolled back, go dormant]
```

### The helper

- A subcommand, `ncc-orchestrator upgrade-helper`, not a new program to ship and
  sign. It runs from its **own copy**, `<install>/helper/ncc-upgrade-helper`,
  so replacing `bin/` never touches the running file, and the helper can be a
  version ahead of the stack.
- **Dormant** means no process. It is started by the service manager on request
  and exits when the upgrade is committed, rolled back or refused.
- It never downloads anything the request tells it to. The request names a
  version, an id and who asked. Repository, paths and service name come from
  the helper's own definition.

### Per platform

| Platform | Where it lives while dormant | Started by | Survives | Finishes after a reboot |
| --- | --- | --- | --- | --- |
| Linux, systemd | `ncc-orchestrator-upgrade.service` (oneshot, root) and `.path` watching `run/upgrade/request.json` | the path unit | `systemctl restart` of the stack (own cgroup) | enabled unit with `ConditionPathExists=` on the active journal |
| Windows | task `ncc-orchestrator-upgrade`, SYSTEM, no trigger, no time limit | `schtasks /Run` by the supervisor (SYSTEM) | ending the supervisor task | a boot trigger added at the start of stage 2 and removed at the end |
| macOS | LaunchDaemon `…ncc-orchestrator.upgrade` with `WatchPaths`, no `KeepAlive` | launchd | `launchctl kickstart -k` of the stack | `RunAtLoad`; exits at once without an active journal |
| Started by hand, no service | nothing installed | a detached process from the API or CLI | the API's exit | `v2-start` resumes an active journal before starting servers |
| Kubernetes, Helm | nothing; hook Jobs exist only during `helm upgrade` | `pre-upgrade` and `post-upgrade` hooks | pod replacement | Helm; `--atomic` rolls back |
| Kubernetes, kustomize | nothing | an optional Job manifest before and after `kubectl apply` | pod replacement | `kubectl rollout undo` |
| Plain containers | nothing; the image is the unit of upgrade | not applicable | not applicable | the previous image tag |

In Kubernetes and containers the program files are immutable, so the helper's
job there is the checks around the rollout: before it, a backup, volume space,
version skew and permissions; after it, that the API, UI and runner all report
the target version. The UI shows the new version and the exact command to run.
It gets no permission to change Deployments.

### Stage 1 in detail

| Step | Done by | Fails when | Then |
| --- | --- | --- | --- |
| Refuse if a run, restore or upgrade is active | API | one is active | say which, change nothing |
| Pre-update backup | API | backup fails | stop |
| Download the target release's orchestrator for this platform | API or supervisor | network, rate limit | stop |
| Verify: checksum, release signature, `upgrade-manifest.json` allows this `from` version | API or supervisor | any mismatch, or an unsigned release on a build that embeds a key | stop |
| Install as `helper/…new`, keep the previous as `helper/…prev` | supervisor | no write access | stop, offer the manual command |
| Rewrite the helper's unit, task or plist from the *new* helper (`upgrade-helper print-definition`) | supervisor | service manager refuses | previous helper and definition back |
| Probe: start under that definition with `--probe` | service manager | not up, wrong version, exits, or a check fails within 60 s | previous helper and definition back |

**"Running and stable"** is the probe: started by the service manager under its
real definition (not from a shell), reports the target version, proves it can
write the unit folder, replace a file in `bin/`, query and control the stack
service, reach its own state folder, sees enough disk, and stays up 15 seconds
with a heartbeat. This catches a sandbox, SELinux or permission problem before
anything in the stack changes.

### Stage 2 in detail

The helper writes a journal (`run/upgrade/journal.json`, fsync per step) so it
can resume. `v2-upgrade-state.json` keeps its format, so the UI needs no new
polling.

1. **Preflight**: lock, disk space, versions, trusted-stack ledger, no run.
2. **Stage**: download the stack archive, verify signature and checksums,
   unpack to `run/upgrade/stage/`. The live files are untouched; this is the
   slow part and happens while everything still runs.
3. **Snapshot**: `bin/`, `frontend-dist/`, **service, schedule and helper
   definitions as installed**, and the configuration files a migration will
   change. Hashes go into the ledger (`UpgradeSnapshot`, SEC-19).
4. **Stop** the stack. Expected downtime from here to step 7 is under a minute.
5. **Swap** program files (existing transaction: aside-rename, SELinux label).
6. **Migrate**, each step idempotent, recorded and reversible: service
   definition, schedule units or tasks, file permissions, configuration schema.
   Local edits to a unit are kept as a drop-in, never overwritten silently: a
   definition that does not match the one we generated is reported and kept
   unless the person chose "replace".
7. **Start** the service with the new supervisor.
8. **Verify**: supervisor, API and UI report the target version, pass health
   probes and stay up for the verify window; the schedule is still installed;
   the non-disruptive doctor checks pass; sign-in state and run history load.
9. **Commit**: record the set in the ledger, write the audit entry, keep the
   snapshot for 24 hours for a manual **Roll back**, go dormant.

Any failure from step 4 on restores the snapshot (programs, definitions,
configuration), starts the previous version and verifies *that*. If even that
fails, the state says so and names the backup. The new helper stays installed
either way: it must be able to manage the previous version.

### Upgrading from 2.3.x (and any version without the helper)

Old code cannot do stage 1. It does not have to: a 2.3.x upgrade already ends
with the new API running. So the 2.4 API, at start, **adopts** the host:

1. Wait until `v2-upgrade-state.json` is `done` or absent, and the stack has
   been healthy for two minutes. Never interfere with a 2.3 upgrade in progress.
2. Install and probe the helper (stage 1 without the download: the program is
   already verified and installed).
3. Run stage 2 with nothing to swap: snapshot definitions, migrate service and
   schedules, restart the supervisor, verify, commit or put the old definitions
   back.

That delivers LIN-01 and the new supervisor to 2.3.x hosts with no manual step.
If the API is not privileged (hand-started, no service), it shows a
**Finish upgrade** card with the one command to run instead. Setting
`NCC_UPGRADE_ADOPT=0` turns adoption off. The existing apply-update watcher
stays for one more minor version, so `ncc-orchestrator update` on the command
line keeps working; from then on that command hands over to the helper.

### What the person sees

- **Updates & backups → Upgrade**: version, notes, and a **plan** before
  anything happens: checks passed or failed, expected downtime, and what will
  change (programs, service definition diff, schedules, configuration keys).
- One progress list across both stages that survives the restart; the page
  keeps polling and reconnects by itself.
- The end states are *Upgraded*, *Rolled back (why)* or *Nothing was changed
  (why)*, each with the log and, for 24 hours, **Roll back**.
- Command line, same engine: `ncc-orchestrator upgrade --plan | --to X |
  --status | --rollback`, and `--bundle FILE` for hosts without internet access
  (stack archive, checksums and signature, uploaded or copied).
- Channel: stable installs see stable releases. An install running a release
  candidate sees candidates until the release is out. Others opt in with
  `--include-prerelease` (done for RC 1).

### Release manifest

Each release adds `upgrade-manifest.json` (listed in the signed checksums):
`version`, `min_from`, `helper_protocol`, `migrations`, `config_schema`,
`downtime_hint`. It lets an old install say "go to 2.6 first" instead of
failing halfway.

### Security rules

- Request folder: only the service can write it; content is treated as hostile
  (size limit, strict version pattern, unknown fields rejected).
- No downgrade, except to the set saved in the snapshot and recorded in the
  ledger.
- Signature required whenever the running build embeds a release key; no flag
  in a request can relax this.
- Helper, journal and snapshot folders are writable by root or SYSTEM only.
- Every stage writes an audit entry: who asked, from, to, outcome.
- This is the piece LIN-02 waits for. With unit files and `systemctl` owned by
  the helper, the API and UI can run as an unprivileged user and the service
  can turn on `ProtectSystem=strict`.

## Work plan

| Phase | Content | Target | Size | Done when |
| --- | --- | --- | --- | --- |
| U0 | Pre-release support in the updater and release workflow | RC 1 | S | built; proven when RC 1 is published and an install updates to it |
| U1 | Helper core (journal, stage 1 and 2, rollback), Linux systemd, adoption from 2.3.x | RC 2 | L | 2.3.0 → RC 2 in a systemd container ends on the sandboxed unit with no manual step; every fault below rolls back |
| U2 | Windows tasks and macOS launchd | RC 2 or 3 | M | same on Windows Server 2022 and macOS; supervisor, folder permissions and schedule migrated |
| U3 | CLI, hand-started stacks, UI plan and progress, `--bundle` | RC 3 | M | one flow in the UI on every host type; offline upgrade works |
| U4 | Helm hooks, kustomize Jobs, UI command for containers | RC 3 | M | kind test: failed post-check with `--atomic` returns to the old revision |
| U5 | LIN-02: unprivileged API and UI, `ProtectSystem` | 2.4.0 (was 2.5.0) | L | built: the service runs as `ncc`; the update helper and the service helper are root |

### Tests

Fault injection at every step, each must end in *Upgraded*, *Rolled back* or
*Nothing was changed*, never in between:

- kill the helper, the supervisor, the API; reboot the host, at each step;
- full disk while staging, at snapshot and at swap;
- wrong checksum, wrong or missing signature, truncated archive;
- new API that crashes, hangs, or answers unhealthy; new supervisor that exits;
- a unit the service manager rejects; SELinux enforcing; a read-only `/etc`;
- locally edited unit; schedule of each type; two upgrade requests at once;
- a run started during staging; clock jump; file locked by antivirus (Windows).

Upgrade matrix per release candidate: from 2.2.1, 2.3.0 and the previous
candidate, on Rocky 9, Ubuntu 24.04, Debian 12 (systemd containers, and one
real Rocky host), Windows Server 2022, macOS, kind; service and hand-started.
The Linux part runs in CI.

## Decisions

1. The helper ships in **2.4.0**.
2. Adoption is **on** by default (`NCC_UPGRADE_ADOPT=0` turns it off).
3. Locally edited units are **kept**: added lines move to a drop-in, and a unit
   with a changed command or user is left alone and reported.

## How it was built

Code: `cmd/ncc-orchestrator/upgrade_*.go` (engine, stage 1, platforms, CLI) and
`cmd/ncc-api-server/upgradehelper.go`.

| Plan | Built |
| --- | --- |
| Path unit on `request.json`; boot trigger on Windows added for stage 2 | One file, `run/upgrade/wake`, exists while there is work. systemd and launchd watch it. On Windows the helper is started on request and at boot, and every minute while there is work: Task Scheduler does not restart a program that was killed. |
| Probe for 15 s with a heartbeat | 5 s after the checks; the job's heartbeat is every 15 s. |
| The helper downloads the stack | Stage 1 downloads, the helper verifies the signature and checksum again before it unpacks. The helper has no network code path. |
| The helper's program comes from the release's orchestrator asset | It comes from the same verified stack archive. |
| `helper_protocol`, `min_from`, `migrations`, `config_schema`, `downtime_hint` in the manifest | `schema`, `version`, `helper_protocol` and optional `min_from`; the rest had no reader. |
| Replace a locally edited unit on request | Not built: edit or remove the unit and run `v2-install-service`. |
| Helm pre-upgrade hook checks backup, space and skew | It pulls and starts the new images once. The post-upgrade hook waits for five answers in a row with the new version. |
| `ncc-orchestrator update` hands over to the helper | Unchanged: it replaces programs only. `upgrade` is the new command. |

Since then: on Windows the service has a second task, `ncc-orchestrator-watchdog`
(WIN-07); upgrades, rollback and adoption include it.

Known limits: a Windows task with a name of its own is found only when it is
`ncc-orchestrator`; the 2.3.x to 2.4.0 step itself is still carried out by the
2.3.x supervisor, and adoption finishes it two minutes after 2.4.0 starts; a
manual rollback exists for upgrades the helper carried out.

Rehearsed with `scripts/upgrade-e2e.sh` (also in CI on Ubuntu, macOS and
Windows): upgrade with a locally added unit line, rollback by hand, a release
that does not start, a changed package, the helper killed while installing and
while checking; each from a service manager and from `v2-start`. With a service
manager it also kills the supervisor. Adoption was
rehearsed from 2.2.1 and 2.3.0 under systemd 257, and the Helm hooks on kind.

### U5 (LIN-02)

Code: `serviceuser.go` (who the service runs as, the unprivileged unit),
`serviceowner*.go` (the file layout and its record for a rollback),
`serviceops.go` (the service helper), `servicecron.go` (a schedule in root's
crontab).

| Plan | Built |
| --- | --- |
| Only the helper is root | Two root units: the update helper as before, and a service helper (`<service>-ops.path` on `run/ops/req/`) for what the service used to do as root: schedule units, restarts after a restore or a TLS change, and starting an upgrade, rollback or adoption. Requests are validated JSON files; `--bundle` is refused from the service. |
| `ProtectSystem` | `ProtectSystem=strict`, programs read-only, no capabilities (`CAP_NET_BIND_SERVICE` for a port below 1024), a system call filter; exposure 1.6, was 5.2. |
| Service runs as `ncc` | `useradd --system`, group `ncc`; the programs and the trusted-stack ledger stay root's in a sticky install folder. Opt out: `--service-user root`, `NCC_SERVICE_USER=root`, `/etc/ncc-orchestrator/service-user`. |
| Migration | The update helper replaces units of 2.3.x, 2.4.0-rc and 2.4.0 (root), records and changes the owners, adds the service helper, and moves a crontab schedule to a timer of `ncc`. A rollback restores the units, owners and crontab. Locally edited units are kept. |

Rehearsed in the systemd container (systemd 257): a fresh install as `ncc`
(capabilities 0, exposure 1.6); schedule save, remove and an invalid calendar
through `PUT /api/v1/schedule`; a scheduled run against `tools/fakeprism`;
restore and TLS change restarting the stack; log maintenance; the in-app update
handed from the supervisor to the service helper and stage 1; the full
`scripts/upgrade-e2e.sh`; adoption of a root install of 2.4.0-rc.3 with a crontab
schedule, and its rollback to the identical root unit, owners and crontab; the
root opt-out. Not rehearsed: an enforcing SELinux host.

Known limits: folders outside the install that the configuration names are not
given to `ncc` (the install warns); the `ncc` user stays after a rollback or an
uninstall; self-heal fix-ups that need root (`systemctl`, `chcon`) are reported,
not applied, by the `ncc` service.
