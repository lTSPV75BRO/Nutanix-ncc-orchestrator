# Upgrade without internet access (dark sites)

For servers that can't reach GitHub or Docker Hub. You download the release on
a machine that has internet access, check it, carry it to the server, and the
update helper installs it there. The checks, the backup, the health check and
the automatic rollback are the same as for an online upgrade.

- [What you need](#what-you-need)
- [1. Download the release on a connected machine](#1-download-the-release-on-a-connected-machine)
- [2. Check the files](#2-check-the-files)
- [3. Copy them to the server](#3-copy-them-to-the-server)
- [4. Install](#4-install)
- [5. Check, or roll back](#5-check-or-roll-back)
- [Stop the server looking for updates](#stop-the-server-looking-for-updates)
- [Kubernetes and Helm](#kubernetes-and-helm)
- [Docker Compose](#docker-compose)
- [Troubleshooting](#troubleshooting)

## What you need

- A machine with internet access, to download the release.
- A way to move files to the server: USB drive, SCP through a jump host, or a
  file share.
- On the server: root (Linux) or an Administrator prompt (Windows). The
  dashboard can't install a carried-in release; this is done on the command
  line.
- Version 2.4.0 or later on the server. Older versions have no `--bundle`:
  first install 2.4.0 by hand ([README](../../README.md#quick-start)), then
  use this guide.

## 1. Download the release on a connected machine

Open the release on the
[releases page](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases)
and download these four files into one folder. Keep the names exactly as they
are.

| File | What it is |
| --- | --- |
| The stack archive for the server's platform (see below) | The programs and the web UI |
| `checksums.txt` | The SHA-256 of every file in the release |
| `checksums.txt.sig` | The release signature over `checksums.txt` |
| `upgrade-manifest.json` | The upgrade rules: which versions can upgrade to this one |

| Server | Stack archive |
| --- | --- |
| Linux, x86-64 | `ncc-v2-stack-linux-amd64.tar.gz` |
| Linux, ARM64 | `ncc-v2-stack-linux-arm64.tar.gz` |
| Windows, x86-64 | `ncc-v2-stack-windows-amd64.zip` |
| Windows, ARM64 | `ncc-v2-stack-windows-arm64.zip` |
| macOS, Intel | `ncc-v2-stack-darwin-amd64.tar.gz` |
| macOS, Apple silicon | `ncc-v2-stack-darwin-arm64.tar.gz` |

With the GitHub CLI (replace the version and the archive name):

```bash
gh release download v2.4.0 -R lTSPV75BRO/Nutanix-ncc-orchestrator -D ncc-upgrade \
  -p ncc-v2-stack-linux-amd64.tar.gz -p checksums.txt -p checksums.txt.sig -p upgrade-manifest.json
```

**Skipping versions.** A release can require a minimum version to upgrade
from (`min_from` in `upgrade-manifest.json`). If the install says
`upgrade to vX first`, carry in that release too and install the two in
order.

## 2. Check the files

The server checks everything again before it installs anything, so this step
is optional. It catches a bad download before you carry it across.

```bash
cd ncc-upgrade
sha256sum -c --ignore-missing checksums.txt    # Linux; on macOS: shasum -a 256 -c --ignore-missing checksums.txt
gh attestation verify ncc-v2-stack-linux-amd64.tar.gz -R lTSPV75BRO/Nutanix-ncc-orchestrator
```

On Windows, compare the hash with the archive's line in `checksums.txt`:

```powershell
(Get-FileHash .\ncc-v2-stack-windows-amd64.zip -Algorithm SHA256).Hash.ToLower()
```

More about signatures and attestations: [`RELEASE_CHECKSUMS.md`](../RELEASE_CHECKSUMS.md).

## 3. Copy them to the server

Put all four files in **one folder** on the server, for example
`/var/tmp/ncc-upgrade` or `C:\ncc-upgrade`. The update helper looks for
`checksums.txt`, `checksums.txt.sig` and `upgrade-manifest.json` next to the
archive.

## 4. Install

Run `ncc-orchestrator` from the install folder, the folder you unpacked the
stack into (the examples use `/opt/ncc` and `C:\ncc`). Linux:

```bash
sudo /opt/ncc/ncc-orchestrator upgrade --bundle /var/tmp/ncc-upgrade/ncc-v2-stack-linux-amd64.tar.gz
```

Windows, from an Administrator PowerShell:

```powershell
C:\ncc\ncc-orchestrator.exe upgrade --bundle C:\ncc-upgrade\ncc-v2-stack-windows-amd64.zip
```

If the install folder is not found, add `--install-dir <folder>`.

What happens, in order:

1. The signature on `checksums.txt` is checked with the release key built
   into your current version, then the archive's checksum, then the upgrade
   rules in `upgrade-manifest.json`. Anything wrong stops here and nothing
   changes.
2. A backup is taken (skip it with `--skip-backup`, not recommended).
3. The new version's update helper is installed and tested, then it stops the
   service, replaces the programs, the web UI and the service definitions, and
   starts the new version.
4. If the new version doesn't come up healthy, the helper puts the previous
   version back by itself, also after a crash or a reboot in between.

Service definitions edited on this host are kept unless you add
`--replace-edited`.

## 5. Check, or roll back

```bash
sudo /opt/ncc/ncc-orchestrator upgrade --status
```

**System → Updates & backups** shows the new version and the upgrade's
steps. For 24 hours the previous version is kept, and rolling back needs no
internet access either:

```bash
sudo /opt/ncc/ncc-orchestrator upgrade --rollback
```

## Stop the server looking for updates

A server without internet access can't reach GitHub, so its update check only
fails. Turn it off under **Settings → Features → Update checks**. Upgrades
from a carried-in release still work.

## Kubernetes and Helm

The chart pulls three images, plus `busybox` for its init containers:

| Values key | Image |
| --- | --- |
| `images.runner`, `image` | `docker.io/prajwalnutant/nutanix-ncc-orchestrator:<version>` |
| `images.api` | `docker.io/prajwalnutant/ncc-api-server:<version>` |
| `images.ui` | `docker.io/prajwalnutant/ncc-ui-server:<version>` |
| `initImage` | `busybox:1.37` |

1. **Copy the images to your internal registry**, from a machine that can
   reach both. With `skopeo` (keeps every platform of the image):

   ```bash
   V=2.4.0; REG=registry.internal.example.com/ncc
   for i in nutanix-ncc-orchestrator ncc-api-server ncc-ui-server; do
     skopeo copy --all docker://docker.io/prajwalnutant/$i:$V docker://$REG/$i:$V
   done
   skopeo copy --all docker://docker.io/library/busybox:1.37 docker://$REG/busybox:1.37
   ```

   With no machine that reaches both, use `docker save` on the connected
   side, carry the `.tar` files across, then `docker load`, `docker tag` and
   `docker push` inside.
2. **Take the chart from the release.** Download *Source code (tar.gz)* from
   the release page; the chart is in `helm/ncc-orchestrator`.
3. **Upgrade**, pointing every image at your registry:

   ```bash
   helm upgrade ncc ./helm/ncc-orchestrator --reuse-values \
     --set images.runner.repository=$REG/nutanix-ncc-orchestrator --set images.runner.tag=$V \
     --set image.repository=$REG/nutanix-ncc-orchestrator --set image.tag=$V \
     --set images.api.repository=$REG/ncc-api-server --set images.api.tag=$V \
     --set images.ui.repository=$REG/ncc-ui-server --set images.ui.tag=$V \
     --set initImage=$REG/busybox:1.37
   ```

The pre-upgrade checks run inside the cluster and need no internet access.
They stop the upgrade before anything changes if an image can't be pulled.
The chart has no image pull secrets, so the nodes must already be able to
pull from the internal registry (node or container runtime credentials).

## Docker Compose

`docker-compose.yml` builds its images from source. Build them on a connected
machine from the release's source code, move them across with
`docker save` and `docker load`, then `docker compose up -d` on the server.

## Troubleshooting

| Message | What to do |
| --- | --- |
| `checksums.txt of the release has to be in the same folder` | Copy `checksums.txt` next to the archive. |
| `checksums.txt.sig of the release has to be in the same folder` | Copy `checksums.txt.sig` too. The release is refused without its signature. |
| `checksums.txt lists upgrade-manifest.json, which has to be in the same folder` | Copy `upgrade-manifest.json` too. |
| `… is not listed in checksums.txt` | The archive was renamed (for example `(1)` added by a browser). Use the release's file name. |
| `the checksum of … is wrong` | The copy is incomplete or damaged. Copy it again and check it (step 2). |
| `… is not a release package for this platform` / `do not run on this host` | Wrong archive for the server: check the table in step 1 (`uname -m` shows `x86_64` or `aarch64`). |
| `v… is older than the installed v…` | The server already has a newer version. |
| `upgrade to vX first` | This release can't be installed from your version. Carry in vX, install it, then this one. |
| A Kubernetes pre-upgrade check fails on an image | The image or tag is missing in the internal registry, or the nodes can't pull from it. |

Full design of the update helper: [`UPGRADE_HELPER_PLAN.md`](../UPGRADE_HELPER_PLAN.md).
