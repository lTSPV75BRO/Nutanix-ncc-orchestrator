# Migration guide: v2.0.2 -> v2.1.0

v2.1.0 is a **fully additive** release on top of v2.0.2: no `config.yaml` key was
removed or renamed, no on-disk artifact format changed incompatibly, and every
new feature (login/RBAC, backup/restore, self-heal, `/metrics`, notification
controls, ...) is either opt-in or safely defaults itself into existence the
first time the new binaries start against an old install. There is no manual
data-migration step to run.

This guide exists because v2.0.2 **predates RBAC and the backup/restore
feature entirely** — it has no user database, no backup-schedule state, and no
concept of login. The sections below spell out exactly what a v2.0.2 install
will see the first time it starts on v2.1.0, since "nothing breaks" still
means "something new turns on."

If you're moving from v1 (CLI-only) rather than v2.0.x, see
[`docs/MIGRATION_v1_TO_v2.md`](MIGRATION_v1_TO_v2.md) instead.

---

## TL;DR

1. Run `./ncc-orchestrator update` (or **Settings → Access → Software updates**
   from the UI on a running v2.0.2 stack). A pre-update backup is taken
   automatically before the new binaries are installed.
2. On first start after the upgrade, **login is now enabled by default** and
   the api-server bootstraps an `admin` account with a random password —
   retrieve and change it (see [Behavior change #1](#1-login--rbac-is-now-on-by-default)
   below).
3. Everything else — your existing `config.yaml`, `outputfiles/`, scheduled
   cron/systemd entries, notification config, and any automation using
   `NCC_API_TOKEN` — keeps working unchanged.

---

## What's new in v2.1.0 that v2.0.2 never had

| Area | v2.0.2 | v2.1.0 |
|---|---|---|
| Login / accounts | none — token-only automation | local accounts, SAML, LDAP/AD, RBAC (`admin`/`operator`/`viewer`) |
| User database | doesn't exist | `.ncc-api-users.json` (or a Kubernetes Secret), created on first start |
| Cluster access control | none — every token sees every cluster | opt-in **cluster groups**; unrestricted by default |
| Backup / restore | none | `v2-backup` / `v2-restore`, optional AES-256-GCM encryption, scheduled snapshots |
| Self-heal | none | `doctor --fix` autonomic remediation + System Health panel |
| Supervisor / boot service | detached shell scripts | native `v2-supervise` + `v2-install-service` |
| `/metrics` | build/runtime only | + run/auth/backup/update operational series |

Because none of this existed in v2.0.2, there is nothing for the upgrade to
"migrate" — each feature's persistence layer treats "the file/field doesn't
exist" as "first run" and bootstraps sane defaults (see
[How each feature's storage behaves on upgrade](#how-each-features-storage-behaves-on-upgrade)).

---

## Behavior changes to expect

### 1. Login / RBAC is now on by default

The api-server now defaults its user-database backend **on** even if you pass
no `--users-db` / `--users-db-secret` flag at all (`cmd/ncc-api-server/main.go`,
`defaultUsersDBPath`). Concretely, on the first start after upgrading:

- A local `.ncc-api-users.json` (or a `ncc-v2-users` Kubernetes Secret, in the
  Secret-store deployment) is created if it doesn't already exist.
- Since that store is empty, an `admin` account is bootstrapped with a random
  password, printed once to the log and written to
  `.ncc-initial-admin-password` (deleted after first login):

  ```bash
  # Docker Compose (file store):
  docker compose logs ncc-api-server | grep -A4 "FIRST-RUN ADMIN"

  # Bare stack / systemd:
  cat <install-dir>/.ncc-initial-admin-password

  # Kubernetes (Secret store; apply k8s/rbac.yaml first):
  kubectl -n ncc-orchestrator-v2 get secret ncc-v2-users \
    -o jsonpath='{.data.initial-admin-password}' | base64 -d
  ```

- The UI now presents a login screen and forces a password change on first
  sign-in.

**Automation is unaffected.** A static `NCC_API_TOKEN` still authenticates as
full admin with no login involved — `auth-mode` just transparently upgrades to
`hybrid` so both mechanisms work side by side. If you run headless/automation
only and don't want interactive login at all, start the api-server with
`--disable-local-accounts` to keep the pre-2.1.0, token-only behavior exactly.

### 2. Cluster access is unrestricted until you opt in

There is no `config.yaml` key or persisted flag for this — "unrestricted" is
simply what happens when zero **cluster groups** exist. A v2.0.2 install has
no groups (the concept didn't exist), so every account (including the
bootstrapped admin and any new local/SAML/LDAP account) sees every cluster
exactly like a v2.0.2 token did. Nothing to configure unless you want to start
segmenting access via **Settings → Access → Cluster groups**.

### 3. Backup/restore and self-heal are opt-in, zero-config

- `v2-backup` / `v2-restore` and the **Settings → Access → Backup & restore**
  UI work immediately with no setup — encryption (`NCC_BACKUP_PASSPHRASE` /
  `NCC_BACKUP_KEY_FILE` / `NCC_BACKUP_KEY`) is optional and defaults to plain
  (unencrypted, same as any v2.0.2-era tarball would have been) if unset.
- Scheduled backups default to **disabled** (`GET /api/v1/settings/backups/schedule`
  returns `{"enabled": false, "every": "24h", "retain": 7}` the first time it's
  read, since `.ncc-api-backup-schedule.json` doesn't exist yet) — turn it on
  from the UI or `PUT` the endpoint when you're ready.
- `--self-heal-interval` defaults to `0` (off); `doctor`/`doctor --fix` are
  always available on demand regardless.

### 4. HTTPS by default for the UI

If you don't supply a certificate, `v2-start` now generates a self-signed one
under `<install-dir>/tls/` and the UI listener serves HTTPS with an HTTP→HTTPS
redirect. Use `--ui-insecure-http` to keep plain HTTP (e.g. behind a
TLS-terminating proxy, or on a trusted loopback-only host).

---

## How each feature's storage behaves on upgrade

This is the detail behind "nothing breaks" — every new persistence layer is
designed to tolerate a completely absent file:

| File / store | If missing (v2.0.2 upgrade case) | Where |
|---|---|---|
| `.ncc-api-users.json` / Secret | Loaded as an empty store, then bootstrapped with a first-run `admin` | `cmd/ncc-api-server/users.go` |
| `.ncc-api-backup-schedule.json` | Defaults to `{enabled: false, every: "24h", retain: 7}` | `cmd/ncc-api-server/backupschedule.go` |
| `<install>/backups/` | Auto-created (`0700`) on first backup | `cmd/ncc-api-server/backupschedule.go` |
| `.ncc-v2-start.json` | Missing file just means "no persisted overrides"; flags/defaults apply | `goNCC.go` |
| `config.yaml` (v2.0.2-era, missing v2.1.0-only keys like `ca-bundle`, `webhook-secret`, `notification-deadletter-dir`, `email-subject-template`, ...) | Each is individually defaulted; nothing is required | `goNCC.go` (`bindConfig` / `validateConfigFileRawTypes`) |

The one **fail-fast** (intentional) exception: if a user store is already
AES-256-GCM-encrypted (a magic-prefixed file) but no `NCC_MASTER_KEY` /
`--users-db-key-file` is configured, the server refuses to start rather than
silently bootstrapping a fresh admin over real accounts. This can only happen
if you've already enabled store encryption on a *previous* v2.1.0+ run and
then removed the key — it cannot happen on a first v2.0.2 → v2.1.0 upgrade,
since a v2.0.2 install has no store (encrypted or otherwise) to begin with.

---

## What was actually tested for this release

Before cutting v2.1.0, the upgrade path was exercised end-to-end against a
simulated v2.0.2 install (old-format `example_config.yaml` from the v2.0.2
tree, with **no** `.ncc-api-users.json`, `.ncc-api-backup-schedule.json`, or
`backups/` directory present) running the v2.1.0 `ncc-orchestrator` /
`ncc-api-server` binaries:

- `validate-config` against the unmodified v2.0.2 `example_config.yaml`
  succeeds (no "unknown config key" errors, no missing-field failures).
- `ncc-api-server` starts cleanly, logs and bootstraps the first-run `admin`
  account, and `/api/v1/health` reports `rbac_enabled`, `login_enabled`, and
  `local_login` all `true`.
- Logging in as the bootstrapped admin succeeds and correctly reports
  `must_change_password: true`.
- `GET /api/v1/settings/backups/schedule` returns the documented defaults with
  no pre-existing state file, and a manual `GET /api/v1/settings/backup`
  produces a valid `.tar.gz`.
- `GET /api/v1/report/data` against a freshly-upgraded, empty `outputfiles/`
  returns a well-formed empty report rather than erroring.

No code changes were required for the upgrade path itself — this release's
[bug fixes](../RELEASE_NOTES_v2.1.0.md#bug-fixes) address a separate,
unrelated issue (the Alerts table losing data after a failed/merged run,
regardless of which version you upgraded from) that was found and fixed
during this verification pass.

---

## Rollback

If you need to go back to v2.0.2 after upgrading:

1. Stop the stack (`v2-stop`, or `systemctl stop ncc-orchestrator`).
2. Restore the pre-update backup automatically taken by `update`
   (`<install>/backups/pre-update-<timestamp>.tar.gz[.enc]`) with
   `v2-restore`, or manually re-extract a previously downloaded v2.0.2
   archive over the install directory.
3. A v2.0.2 binary simply ignores the new `.ncc-api-users.json` /
   `.ncc-api-backup-schedule.json` files if you don't delete them — it never
   reads them, so there is no cleanup required for a rollback.
