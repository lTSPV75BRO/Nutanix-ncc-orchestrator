# Run checks on a schedule, and back up the installation

## Schedule runs

**Settings → Schedule** installs a real scheduler entry on the server, so
runs happen even when nobody has the dashboard open. Operators and admins can
change it.

1. Pick how often: an interval preset (for example every 4 hours), **Run
   every** *n* hours or days, or a **Custom cron expression**
   (`0 6 * * *` = every day at 06:00). The page shows the next three run
   times so you can check it.
2. **Scheduler type:** pick the one for the server's operating system:

   | Server | Scheduler |
   | --- | --- |
   | Linux | **systemd timer** (recommended) or **Cron** |
   | macOS | **Cron** |
   | Windows | **Scheduled Task** |
   | Kubernetes | A **CronJob** (no choice needed) |

3. Turn on **Prevent overlapping runs**, so a slow run is never started
   twice.
4. **Preview** shows the exact entry that would be installed without changing
   anything. **Save schedule** installs it.

The top of the page shows the next run, the last run and its outcome. **Pause
scheduled runs** stops them without removing the schedule (on Kubernetes it
suspends the CronJob). **Remove** takes the entry off the server.

**Several environments** (for example `prod` and `lab` clusters with
different settings) can each have their own timetable under **Environment
schedules** on the same page.

From the command line, the same thing:

```bash
ncc-orchestrator create-schedule --type systemd --every 4h --config config.yaml --print-only
```

Drop `--print-only` to install it. Types: `cron`, `systemd`, `windows`.

## Back up and restore

Backups hold everything needed to rebuild the installation: users and roles,
sign-in settings (LDAP, OIDC, SAML), cluster groups, API tokens, the config
(including notifications and schedule), and the audit log.

**First, set a backup key.** Backups are encrypted, and in-app updates refuse
to run without a key because they take a backup first. Set one of these on the
API server (a file is the safest):

| Variable | Value |
| --- | --- |
| `NCC_BACKUP_KEY_FILE` | Path to a file holding the key (recommended) |
| `NCC_BACKUP_KEY` | The key itself |
| `NCC_BACKUP_PASSPHRASE` | A passphrase |

On Kubernetes, add `backup-key` to the `ncc-v2-secrets` Secret. Keep a copy of
the key somewhere other than the server: without it a backup can't be
restored. To keep plain backups instead, set `NCC_BACKUP_ENCRYPTION=optional`.

**Take and schedule backups** on **System → Updates & backups**:

- **Create snapshot** saves a backup on the server under `backups/`.
- **Scheduled backups:** turn on **Enable scheduled backups**, pick the
  **Frequency** (every hour to every month) and **Keep newest** (how many to
  keep; 0 keeps all). **Save and back up now** saves and takes the first one.
- **Download backup** to copy one off the server. Do this regularly, or copy
  the `backups/` folder with your usual backup tool.

A failed scheduled backup sends a **Run failures** notification if you turned
those on ([Notifications](NOTIFICATIONS.md)).

**Restore** on the same page from a snapshot on the server or an uploaded
file. Restoring on a different server keeps that server's own network and
HTTPS settings, so you aren't locked out by an old address or certificate.

From the command line:

```bash
ncc-orchestrator v2-backup --install-dir .ncc-v2 --output-file ncc-backup.tar.gz
ncc-orchestrator v2-restore ncc-backup.tar.gz --install-dir .ncc-v2 --force --restart
```

Every option: [`FEATURES_AND_CONFIG_FLAGS.md` §6.14a](../FEATURES_AND_CONFIG_FLAGS.md#614a-v2-backup--v2-restore--v2-reset-password).

## Updates

**System → Updates & backups → Updates** checks for a new release and applies
it: it takes a backup, installs the checksum-verified release and restarts.
**Rollback last update** goes back to the previous version. On Kubernetes,
update the image tags with Helm or Kustomize instead.
