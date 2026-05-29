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
