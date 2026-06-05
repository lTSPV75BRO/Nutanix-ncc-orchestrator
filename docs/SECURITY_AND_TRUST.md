# Security and Trust

This document explains how to verify that the `ncc-orchestrator` binaries
you downloaded are authentic and how to run them on macOS / Windows /
Linux when the OS warns that the binary is "untrusted".

> **Project attribution.** `ncc-orchestrator` is an independent
> open-source tool, MIT licensed, maintained by [Prajwal Vernekar](https://github.com/lTSPV75BRO).
> It interacts with Nutanix products via their public APIs but is
> **not affiliated with or endorsed by Nutanix, Inc.** Any embedded
> metadata, signatures, or attestations in this project's releases
> reflect this open-source project, not Nutanix.

> **TL;DR**
>
> 1. Compare the SHA-256 of every file you downloaded against the
>    `checksums.txt` published with the GitHub release.
> 2. On macOS, run `xattr -d com.apple.quarantine <file>` (or right-click
>    "Open" once). On Windows, right-click → Properties → "Unblock".
>    On Linux, `chmod +x <file>`.
> 3. Run `ncc-orchestrator verify` to print the embedded build metadata
>    (version, git revision, vendor, self-hash) for the file you have
>    on disk.
> 4. (Optional) Verify the GPG-signed git tag (`git tag -v v2.0.2`) and
>    release-attestation.json provenance once published.

---

## What ships with each release

Every GitHub release publishes the following on its release page:

| File | Purpose |
| ---- | ------- |
| `ncc-orchestrator-<os>-<arch>` (or `.exe`) | Self-contained CLI (also handles `update`, `verify`, `v2-bootstrap`). |
| `ncc-v2-stack-<os>-<arch>.tar.gz` / `.zip` | Full stack: `bin/`, `frontend-dist/`, `example_config.yaml`. |
| `example_config.yaml` | Reference configuration (works as `config.yaml` if you copy it). |
| `RELEASE_NOTES_v<X.Y.Z>.md` | Per-release notes, including known issues and upgrade guidance. |
| `checksums.txt` | One line per artifact: `<sha256>  <filename>`. |
| `release-attestation.json` | Single-file machine-readable provenance (vendor, git rev, build host, full artifact set with sizes/hashes). |

Optionally, when the maintainer's personal signing keys are configured
at build time:

- macOS binaries are signed with the maintainer's Apple Developer ID and notarized.
- Windows `.exe` files are signed with the maintainer's Authenticode certificate.
- Linux binaries / stack archives ship a `<file>.asc` GPG detached signature against the maintainer's published public key.

These signing chains attest to the project maintainer's identity (the
github.com/lTSPV75BRO account / `Prajwal Vernekar` per LICENSE), **not**
to Nutanix. The release page tells you which (if any) signing chains
are active for that release.

---

## Step 1 — Verify SHA-256 against `checksums.txt`

Every binary in the release has its SHA-256 listed in
`checksums.txt`. Compare yours before running anything.

### macOS / Linux

```bash
shasum -a 256 ncc-orchestrator-darwin-arm64
# expected: copy the matching line from checksums.txt
shasum -a 256 -c <(grep ncc-orchestrator-darwin-arm64 checksums.txt)
# OK  if the line ends with "OK"
```

### Windows (PowerShell)

```powershell
Get-FileHash .\ncc-orchestrator-windows-amd64.exe -Algorithm SHA256
# Compare the Hash field against checksums.txt
```

If the hashes do not match, **do not run the binary**. Re-download or
report the discrepancy on the [issue tracker](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/issues).

---

## Step 2 — Make the OS happy about an unsigned binary

Even when you've verified the SHA-256, the OS does not yet know that
*you* trust the binary. Each OS has its own one-time "unblock" gesture.

### macOS — Gatekeeper / quarantine

When a binary arrives via Safari, Mail, or `curl -O`, macOS adds a
`com.apple.quarantine` extended attribute. Gatekeeper then refuses to
run it ("cannot be opened because the developer cannot be verified")
**unless** the binary is notarized by Apple.

```bash
# 1. Make it executable.
chmod +x ncc-orchestrator-darwin-arm64

# 2. Strip the quarantine bit (one of the following two works).
xattr -d com.apple.quarantine ncc-orchestrator-darwin-arm64
# or recursively for an extracted stack:
xattr -dr com.apple.quarantine ncc-v2-stack/

# 3. Run.
./ncc-orchestrator-darwin-arm64 verify
```

If you'd rather use the GUI: in Finder, right-click the binary, choose
"Open", and click "Open" on the resulting dialog. macOS remembers your
decision for that file.

If your release is notarized, none of the above is required — Gatekeeper
will accept the binary on the first launch.

### Windows — SmartScreen / "Mark of the Web"

Windows attaches a "Mark of the Web" alternate data stream (`Zone.Identifier`)
to files downloaded from the Internet. SmartScreen then blocks the file
with "Windows protected your PC".

#### Method A: GUI (per file)

1. Right-click the `.exe` → **Properties**.
2. At the bottom of the **General** tab, tick **Unblock**.
3. Click **Apply** / **OK**.

#### Method B: PowerShell (scriptable)

```powershell
# unblock a single binary
Unblock-File .\ncc-orchestrator-windows-amd64.exe

# unblock everything in an extracted stack
Get-ChildItem -Recurse .\ncc-v2-stack-windows-amd64 | Unblock-File
```

#### Method C: SmartScreen prompt

When you double-click a blocked binary the first time, Windows shows a
"Windows protected your PC" dialog. Click **More info** → **Run anyway**.

If your release is signed with a code-signing certificate (especially an
EV certificate), SmartScreen accepts the binary without any of the above.

#### What controls the SmartScreen "Publisher" line?

The publisher name shown in the *"Windows protected your PC"* dialog and
the UAC prompt comes **only** from an Authenticode digital signature's
certificate subject. The embedded `VERSIONINFO` (CompanyName, ProductName,
etc., visible in **Properties → Details** and via
`(Get-Item file).VersionInfo.CompanyName`) is *not* used by SmartScreen.
So there is no metadata-only way to replace "Unknown publisher" — it
requires code-signing:

| Certificate | SmartScreen result |
| --- | --- |
| **EV code-signing** (CA-issued) | Real publisher, no warning immediately (instant reputation). |
| **OV / standard code-signing** (CA-issued) | Real publisher; warning persists until the cert/app builds download reputation. |
| **Self-signed** | "Unknown publisher" for the public; trusted only on machines that import the cert (see below). |
| **Unsigned** (default release) | "Unknown publisher"; verify via `verify` + `checksums.txt`. |

#### Optional: self-signed signing for managed fleets

For enterprises that deploy to machines they control, the repo ships
helpers that apply a **real Authenticode signature** using a self-signed
code-signing certificate, plus export the public certificate so it can be
trusted fleet-wide:

```bash
# From a macOS/Linux build host (needs openssl + osslsigncode):
./scripts/sign-windows.sh --dist dist
# -> signs dist/*-windows-*.exe and writes dist/ncc-codesign-public.cer
```

```powershell
# Or natively on Windows (PowerShell):
.\scripts\sign-windows.ps1 -Dist dist
# -> signs the .exe files and writes dist\ncc-codesign-public.cer
```

Then import `ncc-codesign-public.cer` into the **Trusted Publishers** (and,
for SmartScreen/Defender, the **Trusted Root Certification Authorities**)
store on the target machines:

```powershell
# Per machine (run elevated):
Import-Certificate -FilePath ncc-codesign-public.cer -CertStoreLocation Cert:\LocalMachine\TrustedPublisher
Import-Certificate -FilePath ncc-codesign-public.cer -CertStoreLocation Cert:\LocalMachine\Root

# Or fleet-wide via certutil / GPO / Intune:
certutil -addstore TrustedPublisher ncc-codesign-public.cer
certutil -addstore Root ncc-codesign-public.cer
```

Once the certificate is trusted, the binaries' signature validates
(`signtool verify /pa file.exe`, or **Properties → Digital Signatures**)
and the publisher name from the certificate subject is shown instead of
"Unknown publisher". Self-signed certificates are **not** trusted by
machines that have not imported them, so this is unsuitable for public
distribution — use a CA-issued (OV/EV) certificate for that, pointing
`binaryGO.txt`'s `NCC_WINDOWS_PFX_PATH` hook at the issued `.pfx`.

### Linux — executable bit

Linux does no signature verification by default. The only required
step is making the file executable:

```bash
chmod +x ncc-orchestrator-linux-amd64
./ncc-orchestrator-linux-amd64 verify
```

If your release ships GPG detached signatures (`<file>.asc`), verify
them as a sanity check:

```bash
gpg --verify ncc-orchestrator-linux-amd64.asc ncc-orchestrator-linux-amd64
```

---

## Step 3 — Confirm the binary's self-reported provenance

Every release embeds machine-readable build metadata via Go's link-time
flags and `-buildvcs=true`. The orchestrator surfaces it in two places:

### `ncc-orchestrator verify`

The most user-friendly form: prints version, git revision, executable
path, **and the SHA-256 of the binary on disk**, plus a URL pointing at
the matching GitHub release tag.

```text
$ ncc-orchestrator verify
ncc-orchestrator verify
-----------------------
version:           2.0.2
stream:            Release
build_date:        2026-05-29T20:00:00Z
go_version:        go1.26.3
os_arch:           darwin/arm64
git_revision:      914c71d27fb1...
git_dirty:         false
executable_path:   /usr/local/bin/ncc-orchestrator
executable_sha256: 23ee3cad876c...
license:           MIT
project_url:       https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator
affiliation:       independent open-source project; not affiliated with or endorsed by Nutanix, Inc.
verify:            compare executable_sha256 against checksums.txt at
                   https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.2
```

### `go version -m <binary>`

Works against any Go binary, no special tooling needed. Prints the Go
toolchain, module path, dependency tree, and the `vcs.*` settings from
`-buildvcs=true`:

```text
$ go version -m ncc-orchestrator-darwin-arm64
ncc-orchestrator-darwin-arm64: go1.26.3
        path    goncc
        mod     goncc   (devel)
        build   -buildmode=exe
        build   -trimpath
        build   vcs=git
        build   vcs.revision=914c71d2...
        build   vcs.time=2026-05-29T20:00:00Z
        build   vcs.modified=false
```

### Windows file Properties dialog

On Windows, right-click the `.exe` → **Properties** → **Details** tab.
Starting in v2.0.3 the dialog shows:

| Field | Value |
| ----- | ----- |
| File description | NCC Orchestrator (CLI + v2 lifecycle manager) |
| File version | 2.0.2.0 |
| Product name | NCC Orchestrator |
| Product version | 2.0.2 |
| Company | ncc-orchestrator (open-source project) |
| Copyright | (c) 2025-2026 Prajwal Vernekar and contributors. MIT licensed; see LICENSE. |
| Legal trademarks | NCC and Nutanix are trademarks of their respective owners; this project is not affiliated with or endorsed by Nutanix, Inc. |
| Original filename | ncc-orchestrator.exe |

The same metadata is embedded into `ncc-api-server.exe` and
`ncc-ui-server.exe`, so a cursory inspection from File Explorer is
enough to confirm the binaries originate from this open-source
project rather than from Nutanix or some other publisher.

---

## Step 4 — (Optional) Verify the signed git tag

Once we publish a maintainer GPG public key, every release tag is
GPG-signed. After importing that key:

```bash
git fetch --tags
git tag -v v2.0.2
# expected: "Good signature from <maintainer>"
```

This proves the **source revision** the release was built from is the
one the maintainer intended.

---

## How `release-attestation.json` fits in

`release-attestation.json` is shipped alongside `checksums.txt` and
contains the same hashes plus extra provenance fields the maintainer
controls — build host OS/arch, Go toolchain version, project URL, and
an explicit `affiliation` disclaimer — bound to the exact git revision
the release was built from. It's intended for IT-team automation that
wants to verify "this binary belongs to release X from this open-source
project" in one machine-readable file rather than parsing two text
blobs.

```json
{
  "product": "NCC Orchestrator",
  "version": "2.0.2",
  "stream": "Release",
  "git_revision": "914c71d27fb10cd4...",
  "git_dirty": false,
  "go_version": "go1.26.3",
  "project_url": "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator",
  "license": "MIT (see LICENSE in the source repository)",
  "affiliation": "independent open-source project; not affiliated with or endorsed by Nutanix, Inc.",
  "artifacts": [
    {"name": "ncc-orchestrator-darwin-arm64", "size": 17481216, "sha256": "..."},
    {"name": "ncc-v2-stack-darwin-arm64.tar.gz", "size": 28567444, "sha256": "..."}
  ]
}
```

---

## Automatic verification during `update` and `v2-bootstrap`

The manual `checksums.txt` comparison in Step 1 is the canonical trust
anchor, but the binary also performs it for you whenever it downloads
release artifacts.

- **`ncc-orchestrator update`** — every download path (single-binary,
  the `ncc-v2-stack-*` package, and `--binary-url`) hashes the bytes it
  received and compares them against the release `checksums.txt` (for
  `--binary-url`, against the `--binary-sha256` you supply). It refuses
  to install on a mismatch, and refuses to install at all if the release
  ships no checksum manifest.
- **`ncc-orchestrator v2-bootstrap`** — as of v2.1.0, this verifies the
  `ncc-v2-stack-*` archive (or the api/ui binaries + frontend archive in
  the legacy fallback) against the release `checksums.txt` before
  extracting or installing, with the same hard-fail behavior.

### `--skip-checksum-verify` (use sparingly)

Both commands accept `--skip-checksum-verify` for air-gapped or
internally-mirrored installs where the release `checksums.txt` is not
reachable. When set, the command prints a clear warning that integrity
was **not** verified:

```text
warning: --skip-checksum-verify set; NOT verifying ncc-v2-stack-linux-amd64.tar.gz against release checksums.txt
```

Prefer mirroring `checksums.txt` alongside the artifacts and leaving
verification on. Only use the flag when you have already verified the
bytes out-of-band.

---

## Troubleshooting

### macOS: "killed: 9" right after launching

You stripped the quarantine bit but the binary still won't run on
Apple Silicon. Make sure you're using the `darwin-arm64` build (not
`darwin-amd64`); a Rosetta 2 mismatch usually surfaces as a SIGKILL.

```bash
file ncc-orchestrator
# should say: Mach-O 64-bit executable arm64
```

### macOS: notarization rejection

If you signed the binaries yourself and `xcrun notarytool submit` failed,
the most common cause is missing the `--options runtime` flag during
`codesign`. Re-sign with the hardened runtime and resubmit.

### Windows: SmartScreen still blocks after Unblock-File

Some enterprise SmartScreen policies require an EV (Extended Validation)
code-signing certificate before they allow new signatures to bypass the
warning. Either install your own CA's certificate as Trusted Publisher or
use the SmartScreen "More info → Run anyway" dialog.

### Windows: AV quarantines the binary

If your AV product quarantines `ncc-orchestrator.exe` immediately on
download, submit the file as a false positive (the embedded VERSIONINFO
metadata makes this fast for the AV vendor) and use a temporarily
allow-listed path until they update their definitions.

### Linux: SELinux / AppArmor blocks file write

If `ncc-api-server` fails with a permission error writing to
`outputfiles/`, check SELinux contexts (`ls -Z`) or AppArmor profiles.
The api-server only needs read access to `--config-path` and read+write
under `--output-dir`, `--log-dir`, and the token file path.

---

## Runtime security hardening (v2.1.0+)

The trust steps above cover the binary you run. These options cover how it
talks to Prism, SMTP, and webhook receivers at runtime, and who can change
state through the api-server.

### Prism TLS: prefer a CA bundle or pinning over `insecure-skip-verify`

`--insecure-skip-verify` accepts **any** certificate (including a
man-in-the-middle's). For self-signed or internal-CA Prism deployments, prefer:

- `--ca-bundle <file.pem>` — trust the internal CA. The server cert is verified
  against the system roots **plus** this bundle.
- `--pin-sha256 <fp[,fp...]>` — certificate pinning. The server cert is accepted
  only if its SHA-256 fingerprint matches one you supply (colons optional),
  independent of the system trust store. A MITM presenting a different cert is
  rejected.

Capture a fingerprint to pin with:

```bash
echo | openssl s_client -connect <prism>:9440 2>/dev/null \
  | openssl x509 -noout -fingerprint -sha256
```

### SMTP TLS verification is decoupled

`--smtp-insecure-skip-verify` controls SMTP STARTTLS verification independently
of the Prism `--insecure-skip-verify` flag, so a self-signed mail relay no
longer forces you to disable Prism certificate verification.

### Outbound webhook authenticity (HMAC)

Set `webhook-secret` (config/env only — `NCC_WEBHOOK_SECRET`, never a CLI flag)
to sign the webhook body with HMAC-SHA256. The orchestrator sends
`X-NCC-Signature: sha256=<hex>`; the receiver recomputes the HMAC over the raw
body with the shared secret and rejects mismatches.

### Notification dead-lettering

`--notification-deadletter-dir` persists any email/webhook/Slack payload that
fails to deliver after retries (channel, cluster, error, payload) as a
`0600` JSON file, so a transient outage does not silently lose an alert. Treat
that directory as sensitive — payloads can contain check details.

### api-server access control (RBAC + login)

The api-server supports three authorization levels, ordered
`viewer < operator < admin`:

| Role | May do | Denied |
| ---- | ------ | ------ |
| **viewer** | Read non-settings `GET` endpoints (runs, reports, artifacts, logs, health, metrics) and read the run schedule | `/api/v1/settings/*`, run trigger/cancel, any mutation |
| **operator** | Everything a viewer can, plus: trigger/cancel/preflight runs; **create/update/apply the run schedule** (`PUT /api/v1/schedule`); **send test notifications** (`POST /api/v1/settings/notifications/test`); **read cluster topology** (`GET /api/v1/settings/clusters` and `GET /api/v1/settings/cluster-groups`) | Secret-bearing settings (config, users, SSO/LDAP, sessions, notifications config, backups, cluster-group **writes**), token rotation |
| **admin** | Everything | — |

The operator scope is deliberately limited to **operating** NCC (running it, scheduling it, verifying alerting, and seeing which clusters/groups exist to scope runs) without exposing or changing any secret. The carved-out operator endpoints either return no secrets (cluster topology is just names/membership) or are run-adjacent actions; the test-notification path **redacts URLs** from delivery errors so a Slack/webhook secret can't leak to a non-admin (the full error is still logged server-side). In the UI, operators reach a reduced **Settings** view (Connection, Schedule, Runs, Logs, Audit); the secret-bearing tabs (Config, Access, Developer) stay admin-only.

A role can be presented three ways:

1. **Static admin token** (`NCC_API_TOKEN`) — full admin. For automation/CI.
2. **Static viewer token** (`NCC_API_VIEWER_TOKEN`) — read-only. Hand to
   dashboards/scrapers. Must differ from the admin token.
3. **Interactive login** (browser) — a role-bearing, signed **session cookie**
   minted by either local password accounts or SAML SSO.

`/api/v1/health` reports `rbac_enabled`, `login_enabled`, `local_login`, and
`saml_enabled`. Routes enforce a minimum role; insufficient roles get `403`.

#### Cluster groups (membership-based access control)

On top of the role hierarchy, admins can segregate clusters into **cluster
groups** (Settings → Access → *Cluster groups*, or
`GET/PUT /api/v1/settings/cluster-groups`). Each group lists clusters plus
members, where membership is the union of:

- **local accounts** (by username),
- **Active Directory groups** (by CN or full DN; matched case-insensitively
  against the `memberOf` values captured in the user's session at login), and
- **individual Active Directory users** (matched against the caller's canonical
  subject, i.e. their `sAMAccountName`; a UPN's local part is accepted too).

A group may also list **Prism Centrals** (URLs/addresses). Every cluster
registered under a listed PC is folded into the group automatically: the
api-server discovers the PC's clusters via the orchestrator's `discover-clusters`
(reusing the active run config's credentials — no extra secrets are stored), and
both the cluster **name and address** are granted so filtering matches whichever
identity the reports/runs use. Discovery results are cached and refreshed in the
background (≈10 min TTL), so newly-registered clusters become accessible without
an admin edit; admins can preview/refresh a PC's clusters via
`GET /api/v1/settings/pc-clusters?pc=<url>` (admin-only).

When assigning AD principals, the UI offers **live directory type-ahead**: as the
admin types, it queries `GET /api/v1/settings/ldap/search?q=<term>&type=group|user`
(admin-only; binds the configured service account) and returns matching groups
(stored by full DN) and users (stored by `sAMAccountName`). The field still
accepts manual entry, so it works even when the directory is unreachable.

A cluster may belong to multiple groups. The model is:

- **Admins and static-token callers are unrestricted** — they see, trigger, and
  act on every cluster, exactly as before.
- **Non-admins are confined to the union of clusters in the groups they belong
  to.** Run triggers are pinned to that set via `--clusters` (a member may
  further narrow to a subset; requesting a cluster outside their groups is
  dropped, and a member in no group gets `403`). Report/dashboard data
  (`/api/v1/report/data`, the runs feed) is **filtered server-side** down to the
  allowed clusters, and members cannot supply their own `--clusters` /
  `--cluster-file` to escape the scope.
- **Ungrouped clusters are admin-only** — a cluster that is in no group is never
  granted to a non-admin.
- **Raw multi-cluster artifacts are admin-only.** The pre-rendered
  `index.html`, CSV/JSON exports, and NCC logs (`GET /api/v1/artifacts*`) embed
  every cluster and cannot be filtered after the fact, so they are restricted to
  unrestricted callers; members use the filtered dashboard instead.

`GET /api/v1/auth/me` returns `cluster_access_unrestricted` and (when
restricted) `allowed_clusters`, so the UI can label the visible scope and hide
admin-only download buttons. Cluster groups live inside the user database
(`.ncc-api-users.json`), so they persist across `v2-stop`/`v2-start` and are
captured by backup/restore automatically.

**Concurrent runs across groups.** Different groups can run at the same time:
the run engine allows up to `--max-concurrent-runs` (default 4) orchestrator
processes at once and queues the rest. Because each group's run is pinned to its
own `--clusters` set, a run by group A and a run by group B proceed in parallel.
When their cluster sets **overlap**, the later run skips the clusters the earlier
run already claimed (tracked by a per-cluster in-flight owner map) and executes
only the remainder — a shared cluster is scanned **once**, never twice
simultaneously. Each run writes to an isolated per-run output directory and its
per-cluster results are merged latest-wins back into the canonical report on
completion, so each group continues to see only its own clusters' data through
the same server-side cluster-group filtering. This does not widen any
principal's visibility: skipped/shared clusters are still subject to each
caller's `allowed_clusters` when reading reports.

#### First-run admin bootstrap (zero-config)

When the api-server runs with a **writable user database** — `--users-db`
(or `$NCC_USERS_DB`), which defaults to `<root>/.ncc-api-users.json`
automatically when launched from inside a v2 stack — and that store is empty,
the server **provisions an initial `admin` account with a random password** on
first launch. The password is printed to the server log and also written to a
`0600` `.ncc-initial-admin-password` file next to the database:

```
==================================================================
 FIRST-RUN ADMIN CREATED
   username: admin
   password: 8s2Qd…(random)…Xv
   You MUST change this password on first login.
==================================================================
```

The bootstrap admin is flagged `must_change_password`: on first login the UI
forces a password change (and the API blocks every other action with
`403 NCC_API_PASSWORD_CHANGE_REQUIRED`) until `POST /api/v1/auth/change-password`
succeeds (new password ≥ 8 chars; current password + CSRF verified). Once
changed, the `.ncc-initial-admin-password` file is deleted. That admin can then
add more local users, assign roles, and configure SAML from the UI.

The **only** endpoint allowed through this forced-change block is a backup
restore (`POST /api/v1/settings/restore`): the first-login screen offers a
"Restore from backup…" button so a fresh deployment can recover an existing one
instead of setting a new password (the restored user database replaces the
bootstrap admin with the original account, `must_change_password=false`). The
exception is still gated by the admin role and CSRF, so a non-admin flagged
account cannot use it.

The database holds bcrypt password hashes, roles, the must-change flag, and the
runtime SAML config (including the SP private key). It can be persisted two ways:

- **`--users-db <path>`** — a local `0600` JSON file, written atomically. Used
  for file/dev/stack and `docker-compose` installs.
- **`--users-db-secret <name>`** (mutually exclusive with `--users-db`) — a
  **Kubernetes Secret**, read/created/patched by the api-server over the
  in-cluster API using its projected service-account token. The user-database
  JSON lives under the `users.json` key (override with `--users-db-secret-key`);
  the first-run admin password is written under `initial-admin-password` and
  removed once changed. The namespace defaults to the pod's own
  (`--users-db-secret-namespace` to override). This path uses a small built-in
  REST client — no `client-go`, so the static binary/image stays lean.

> **Encryption at rest — read this.** Kubernetes Secrets are only
> base64-encoded by default, **not encrypted**. For the Secret store to be
> "well encrypted," enable **etcd encryption-at-rest** on the cluster: an
> external **KMS v2** provider (recommended) or `secretbox`/`aescbc` via a
> kube-apiserver `EncryptionConfiguration`. See
> [`k8s/encryption-config.example.yaml`](../k8s/encryption-config.example.yaml).
> On managed clusters (EKS/GKE/AKS), enable the provider's KMS/envelope
> encryption for Secrets. After enabling, re-encrypt existing Secrets with
> `kubectl get secrets -A -o json | kubectl replace -f -`.

The api-server is granted **least-privilege RBAC**
([`k8s/rbac.yaml`](../k8s/rbac.yaml)): `create` at the namespace scope (which
Kubernetes cannot restrict by name) plus `get`/`update`/`patch` limited to the
single `ncc-v2-users` Secret via `resourceNames`. It cannot read other Secrets.

The bundled deployments enable this by default. `docker-compose.yml` and
`Dockerfile.api` use the file store on the persistent auth volume
(`/app/data/auth/.ncc-api-users.json`); `k8s/api-deployment.yaml` uses the
Kubernetes Secret store (`--users-db-secret ncc-v2-users`). Retrieve the
first-run password:

```bash
# docker compose (file store): from the logs
docker compose logs ncc-api-server | grep -A4 "FIRST-RUN ADMIN"

# Kubernetes (Secret store): from the Secret (or the pod logs)
kubectl -n ncc-orchestrator-v2 get secret ncc-v2-users \
  -o jsonpath='{.data.initial-admin-password}' | base64 -d
kubectl -n ncc-orchestrator-v2 logs deploy/ncc-v2-api | grep -A4 "FIRST-RUN ADMIN"
```

#### Local password accounts

Admins manage accounts at runtime from **Settings → Access** (or the API):

- `GET /api/v1/settings/users` — list accounts (no hashes)
- `POST /api/v1/settings/users` — create `{username,password,role}` (the new
  account must change its password on first login by default)
- `PUT /api/v1/settings/users/<name>` — change role and/or reset password
- `DELETE /api/v1/settings/users/<name>` — remove an account

The last remaining admin cannot be demoted or deleted
(`409 NCC_API_LAST_ADMIN`). In addition, the built-in `admin` account is
reserved: its role is hardcoded to `admin` and can never be changed, and the
account can never be deleted (`409 NCC_API_RESERVED_ADMIN`), regardless of how
many other admins exist. A hand-edited store/Secret that demotes `admin` is
coerced back to `admin` on load. `POST /api/v1/auth/login` `{username,password}`
verifies the bcrypt hash and sets the session cookie; `POST /api/v1/auth/logout`
clears it; `GET /api/v1/auth/me` reports the caller's role and must-change state.

For automated/provisioned installs you can pre-seed accounts with a read-only
YAML file (`--users-file` / `$NCC_USERS_FILE`); it is imported **once** into the
database when the store is empty (and then ignored), bypassing the random-admin
bootstrap. Generate hashes with `ncc-api-server --hash-password`.

#### Password recovery

There are two recovery paths, depending on who is locked out.

**Lost admin (or any) password — offline CLI reset.** With host/cluster access
to the user store you can reset any local account without logging in. Use the
orchestrator wrapper (it locates the api binary and the stack's user store):

```bash
# Reset the built-in admin (defaults to --user admin)
ncc-orchestrator v2-reset-password

# Reset a specific user
ncc-orchestrator v2-reset-password --user alice

# Against a Kubernetes Secret store instead of a file
ncc-orchestrator v2-reset-password --user admin --users-db-secret ncc-v2-users
```

or call the api-server directly with the same store flags the server uses:

```bash
ncc-api-server --reset-admin --users-db /path/to/.ncc-api-users.json
ncc-api-server --reset-password alice --users-db-secret ncc-v2-users
```

The reset writes a **new random temporary password** to the store (printed to
the console), flags the account `must_change_password`, and **bumps the token
generation so every existing session for that account is invalidated**. If the
built-in `admin` was wiped from the store it is **recreated**. Because a running
api-server caches accounts in memory, **restart it** (`v2-stop` then `v2-start`,
or restart the pod) for the new password to take effect; the user then logs in
with the temporary password and is forced to change it.

**A user forgot their password — self-service request queue (no email).** The
login page has a **"Forgot password?"** link. Submitting a username calls the
public `POST /api/v1/auth/forgot-password`, which **always returns a generic
200** (it never reveals whether the account exists — no enumeration) and records
a request **only** for an existing local account. Admins see pending requests in
**Settings → Access → Password reset requests** (`GET /api/v1/settings/password-resets`),
verify each one out-of-band, and either **Reset password** (sharing the
temporary password securely — this clears the request automatically) or
**Dismiss** it (`DELETE /api/v1/settings/password-resets/<name>`). The queue is
persisted in the user store and is rate-limited via the global limiter; no admin
emails are stored, so the queue + the `auth.forgot_password` audit line are the
notification surface.

**The admin is locked out — self-service via the same link.** When the
forgot-password username is the built-in `admin`, queuing a request is useless
(there may be no other admin to action it), so the server instead self-resets
the admin exactly like first-run setup: it generates a **new random password**,
forces a change at next login, and **invalidates all existing admin sessions**
(token-generation bump). The new password is surfaced only through the
**server logs** and the sibling **`.ncc-initial-admin-password`** file — it is
**never returned over the network** (the HTTP response only confirms a reset was
generated and tells the operator where to look). Recovery therefore still
requires host/log access, the same trust boundary as the first-run bootstrap
password and the offline `v2-reset-password --user admin` command. The trade-off
is that an unauthenticated caller who can reach the login page can force-rotate
the admin password (a nuisance/DoS that repeatedly invalidates the real admin's
sessions) but **cannot learn the new password or take over the account**; every
attempt is recorded with `admin_self_reset` in the `auth.forgot_password` audit
line. A dedicated **per-IP cooldown (60s)** rejects rapid repeats with
`429 NCC_API_RATE_LIMITED` on top of the global rate limiter; front a public
deployment with network controls to blunt rotation from many source IPs.

An authenticated admin can produce the same outcome from **Settings → Access →
Reset password** on the `admin` row: the dialog offers **Generate & reset**
(`PUT /api/v1/settings/users/admin` with `{"generate_password": true}`), which
generates a random password, forces a change, invalidates admin sessions, and
writes the bootstrap file — additionally returning the new password **once** in
the response so the acting admin can share it securely (this path is authorized
and CSRF-protected, unlike the anonymous login-screen route which never returns
the password).

#### Brute-force lockout and session revocation

Login attempts are protected at two layers. The global per-IP rate limiter
throttles bursts, and a **per-account lockout** (`--login-lockout-threshold`,
default 5 within `--login-lockout-window` 15m) temporarily locks an account for
`--login-lockout-duration` (15m), returning `429 NCC_API_ACCOUNT_LOCKED` with a
`Retry-After` header. Because it keys on the username rather than the IP, it
stops an attacker who rotates source IPs while grinding one account — something
the IP limiter alone cannot. A successful login or any password reset clears the
lockout; set the threshold to `0` to disable. The built-in `admin` is not
exempt (so it is also protected), and remains recoverable via the
forgot-password self-reset or the offline `v2-reset-password` command if locked.

Every session is an HMAC token stamped with the account's **token generation**;
bumping that generation invalidates all outstanding sessions for the user at
once. Users can self-serve this with **Sign out everywhere**
(`POST /api/v1/auth/logout-all`), and admins can force it for any account from
Settings → Access (`PUT /api/v1/settings/users/<name>` with
`{"revoke_sessions": true}`) — useful for incident response without rotating the
user's password.

#### SAML SSO

SAML can be configured two ways:

1. **Startup flags** (read-only at runtime): `--saml-root-url`,
   `--saml-idp-metadata` (URL or file), `--saml-cert`, `--saml-key`.
2. **Runtime, in the UI** (Settings → Access, or `GET/PUT /api/v1/settings/sso`):
   paste/point at the IdP metadata and role mapping; the config is persisted in
   the user database and the SP is **hot-reloaded without a restart**. The SP
   signing keypair is **generated on the server** the first time SAML is enabled
   — the private key never leaves the server and is never uploaded through the
   browser. Hand your IdP the SP metadata URL shown in the UI
   (`<root>/saml/metadata`).

Either way the server exposes `/saml/metadata` and `/saml/acs`; `/saml/login`
starts the SP-initiated flow. Map IdP group/role attribute values to local roles
with the role attribute + role map (e.g. `ncc-admins=admin,ncc-ops=operator`);
unmatched users fall back to the default role (`viewer`). SAML requires the
api-server to be reachable at the external root URL over TLS.

#### LDAP / Active Directory

Users can sign in on the normal username/password login form with their AD/LDAP
credentials. Login is **local-first, then AD fallback**: the built-in `admin`
and any break-glass local accounts always work even if the directory is down or
misconfigured. AD users get a normal role session but no local password
(self-service password change is not offered for them).

Authentication uses a **service-account bind + search + rebind**: the server
binds the read-only service account, searches `base_dn` with the user filter
(default `(&(objectClass=user)(|(sAMAccountName=%s)(userPrincipalName=%s)))`, so
users can sign in with either `jdoe` or `jdoe@corp.example.com`), then re-binds as the found
user DN to verify the password. An empty password is rejected up front so an
LDAP anonymous bind can never be mistaken for a successful login, and the login
name is escaped into the search filter.

Configure it two ways:

1. **Startup flags** (read-only at runtime): `--ldap-url` (comma-separated for
   failover), `--ldap-base-dn`, `--ldap-bind-dn`, `--ldap-bind-password`,
   `--ldap-user-filter`, `--ldap-username-attribute`, `--ldap-group-attribute`,
   `--ldap-role-map`, `--ldap-default-role`, `--ldap-start-tls`,
   `--ldap-ca-file`, `--ldap-insecure-skip-verify`.
2. **Runtime, in the UI** (Settings → Access, or `GET/PUT /api/v1/settings/ldap`):
   the config is persisted in the user database and hot-reloaded without a
   restart. A **Test connection** button (`POST /api/v1/settings/ldap/test`)
   authenticates a sample user to verify connectivity and role mapping before
   saving.

Map AD groups to local roles by group DN or CN, one per line (or
semicolon-separated):
`CN=NCC-Admins,OU=Groups,DC=corp,DC=example,DC=com=admin`. Matching is
case-insensitive and the highest matching role wins; unmatched users fall back
to the default role (`viewer`). Prefer `ldaps://` (or StartTLS); the bind
password is a secret stored only in the `0600` user-store file or the Kubernetes
Secret (encrypted at rest) and is **never returned** by the API. `InsecureSkipVerify`
is supported but discouraged.

#### Session cookies and CSRF

Browser sessions use an **httpOnly, Secure, `SameSite=Strict`** cookie
(`ncc_session`). Mutating requests authenticated by cookie must echo a
double-submit CSRF token: the readable `ncc_csrf` cookie value in the
`X-CSRF-Token` header (the UI does this automatically). Static-token automation
sends no cookie and is exempt. `--cookie-insecure` drops the `Secure` attribute
for local http development only.

When local accounts, SAML, or LDAP/AD are configured, `auth-mode` is
auto-upgraded to `hybrid` so static tokens (automation) and cookie sessions
(browsers) both work.
The `ncc-ui-server` detects login is enabled (via health) and stops injecting
the shared admin token for browser traffic, forwarding each user's own session
cookie instead — preventing privilege escalation. Override with the ui-server's
`--login-mode {auto|on|off}`.

For unauthenticated Prometheus scraping of `/metrics` on a private network, use
the api-server's `--metrics-public` flag instead of sharing a token.

---

## Reporting suspected tampering

If a binary you downloaded does not match `checksums.txt`, or
`ncc-orchestrator verify` reports a SHA-256 that does not appear in the
release manifest, **stop using it** and open an issue with:

1. The exact filename you downloaded.
2. The SHA-256 you observed (from `shasum`/`Get-FileHash`).
3. The download URL, browser/CLI, and approximate time.
4. The output of `ncc-orchestrator verify` (if the binary still launches).

We will compare against the published manifest, audit the release page,
and either invalidate the affected build or document a benign explanation
(e.g. mid-publish snapshot).
