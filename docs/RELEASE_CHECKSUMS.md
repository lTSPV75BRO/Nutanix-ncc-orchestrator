# Release checklist: checksums + maintainer release flow (`v1` and `v2`)

For **`ncc-orchestrator -u`** to verify downloads, the GitHub release must include a **checksum file** asset that the tool can find and parse.

---

## Release signatures in the free pipeline

The tag-triggered release workflow signs `checksums.txt` with an Ed25519
release key and publishes `checksums.txt.sig`. It embeds the matching public
key in release binaries, so an operator can run:

```bash
ncc-orchestrator verify --online --require-signature
```

Linux artifacts also have GPG detached signatures (`.asc`) and the release
publishes `ncc-release-signing.gpg` for importing the corresponding public
key. macOS and Windows binaries are checksum- and provenance-verified but
remain unsigned until Apple Developer ID and CA-issued Authenticode
credentials are available.

The workflow runs in the GitHub environment `release` and requires these
secrets (environment or repository secrets):

- `NCC_RELEASE_SIGN_KEY_BASE64`
- `NCC_LINUX_GPG_PRIVATE_KEY_BASE64`
- `NCC_LINUX_GPG_KEY_ID`
- `NCC_LINUX_GPG_PASSPHRASE`

`NCC_RELEASE_PUBKEY` is public and belongs in a GitHub Actions **variable**
(environment or repository), not a secret. Never commit or print any private key or passphrase.

### How a release is published (maintainer view)

Push a `v*` tag. `.github/workflows/release.yml` then runs three jobs:

1. `preflight` fails at once if a **published** release for the tag already
   exists. The workflow never edits a published release: delete it in the
   GitHub UI (the tag stays) or use a new tag. A leftover **draft** from a
   failed run is replaced.
2. `build` (holds the signing secrets, cannot write releases) builds, signs and
   attests exactly the files to publish (`scripts/stage-release-assets.sh`).
3. `publish` (holds `contents: write`, no secrets) creates a **draft** release
   with `gh release create --verify-tag` (no target commit is sent), uploads the
   files, runs `scripts/verify-published-release.sh` on the draft, and only then
   publishes. If a check fails after publishing, the release is turned back
   into a draft.

Never upload assets by hand: a file that is not attested by the release
workflow fails verification, and `gh attestation verify` fails for it.

Audit any release (draft or published) from a workstation:

```bash
scripts/verify-published-release.sh -R lTSPV75BRO/Nutanix-ncc-orchestrator v2.4.0
```

It downloads every asset and checks `checksums.txt`, a build attestation from
`release.yml` for the tag on every asset (`gh attestation verify
--signer-workflow`), and the GPG signatures. Releases published before this
pipeline (v2.3.0 and earlier) are expected to fail the attestation check.

Why the old flow failed (v2.3.0, most likely): the action updated the existing release and
re-sent its stale `target_commitish`; the commits between that and the tag
changed `.github/workflows/*`, which `GITHUB_TOKEN` cannot do.

### Release candidates

1. `bash scripts/binaryGO.txt --set-version 2.4.0-rc.1` stamps `VERSION`, the
   chart, the manifests and the frontend, then builds; commit the stamps.
2. Tag that commit `v2.4.0-rc.1` and push the tag. The workflow refuses a tag that
   is not `v` plus `VERSION`.
3. The release is marked pre-release and never Latest, and images
   `:2.4.0-rc.1` are pushed. Stable installs are not offered it.
4. Try it with `ncc-orchestrator update --target-version 2.4.0-rc.1` (2.3.0 and
   later). From a candidate, updates find the next candidate and then the release.
5. Before the final tag, stamp `2.4.0` the same way.

### One-time maintainer setup

Create the environment first (Settings, Environments, New environment,
`release`; optionally add yourself as a required reviewer and limit it to
`v*` tags), then add the secrets above to it and `NCC_RELEASE_PUBKEY` as a
repository or environment variable.

Details of the keys:


Generate the Ed25519 update-signing key on a trusted machine, then keep its
private file offline except when loading the encrypted GitHub Actions secret:

```bash
go run ./cmd/ncc-orchestrator release-keygen --out ncc-release-signing.key
base64 -w 0 ncc-release-signing.key
```

Use the resulting base64 text for `NCC_RELEASE_SIGN_KEY_BASE64`; copy the
public key printed by `release-keygen` into `NCC_RELEASE_PUBKEY`. On macOS,
use `base64 < ncc-release-signing.key | tr -d '\n'` instead of `base64 -w 0`.

A key made with openssl works too: store base64 of the unencrypted PKCS#8 PEM file
(`openssl genpkey -algorithm ed25519`) as the secret, and the PEM public key
(`openssl pkey -in key.pem -pubout`) or its one-line form as the variable. To see
the one-line form, or to check that both belong together:

```bash
go run ./cmd/ncc-orchestrator release-pubkey --key key.pem --public "$(cat key.pub.pem)"
```

Before building, the release job runs `scripts/release-signing-keys.sh`: it signs a
test file with both keys and stops if the Ed25519 pair does not match, or if the GPG
key, its ID or its passphrase is wrong.

Create a dedicated, passphrase-protected GPG signing key for Linux artifacts,
record its full fingerprint, and export it only for the Actions secret:

```bash
gpg --quick-generate-key 'NCC release signing <releases@example.invalid>' ed25519 sign 2y
gpg --list-secret-keys --keyid-format LONG
gpg --armor --export-secret-keys YOUR_FULL_FINGERPRINT | base64 -w 0
```

Set the fingerprint as `NCC_LINUX_GPG_KEY_ID` and its passphrase as
`NCC_LINUX_GPG_PASSPHRASE`. Preserve an encrypted offline backup of both
private keys before enabling the release workflow.

---

## 0. Choose release track first (`v1` vs `v2`)

Maintainers should decide release track before building artifacts:

- **`v1.x` track**: CLI-only distribution.
- **`v2.x` track**: CLI + API server + UI server + frontend bundle.

Suggested branch/tag convention:

- `v1` maintenance branch -> tags like `v1.2.3`
- `v2` maintenance branch -> tags like `v2.4.0`

For each tag, update release notes and validate the matching track behavior.

---

## 1. Build binaries and name them so `-u` can match

The update logic picks an asset whose **name** contains the current OS and arch (e.g. `linux`, `amd64`). Use names like:

- `ncc-orchestrator-linux-amd64`
- `ncc-orchestrator-darwin-amd64`
- `ncc-orchestrator-darwin-arm64`
- `ncc-orchestrator-windows-amd64.exe`

No extension is required for Unix; Windows typically uses `.exe`. These must be **raw binaries** (not inside a `.tar.gz` or `.zip`) if you want the tool to replace itself; otherwise `-u` will only print the archive URL.

---

## 2. Generate a checksum file

Create a text file with **one line per binary**: **64‑character SHA256 hash**, then **two spaces**, then the **exact filename** (same as the release asset name).

**Linux / macOS (GNU coreutils):**

```bash
cd dist
sha256sum ncc-orchestrator-linux-amd64 ncc-orchestrator-darwin-amd64 ncc-orchestrator-darwin-arm64 > checksums.txt
```

**macOS (BSD):**

```bash
cd dist
shasum -a 256 ncc-orchestrator-* > checksums.txt
# shasum outputs "hash  filename" — correct format
```

**Manual format (if you generate hashes yourself):**

```text
<64 hex chars>  ncc-orchestrator-linux-amd64
<64 hex chars>  ncc-orchestrator-darwin-amd64
...
```

- The parser accepts **`hash  filename`** or **`hash *filename`** (second field can have a leading `*`).
- Hash must be **64 hex characters** (SHA256). Optional `0x` prefix is stripped.
- **Filename must match exactly** the release asset name (e.g. `ncc-orchestrator-linux-amd64`).

---

## 3. Name the checksum asset so the tool finds it

Upload the checksum file as a **release asset** whose name contains one of:

- **`checksum`** (e.g. `checksums.txt`, `ncc-orchestrator-checksums.txt`)
- **`sha256`** (e.g. `SHA256SUMS`, `ncc-orchestrator-linux-amd64.sha256`)

The tool downloads the **first** such asset and uses it to verify the binary it downloaded.

---

## 4. Upload to the GitHub release

When you create the release (e.g. v2.4.0):

1. Upload each **binary** (e.g. `ncc-orchestrator-linux-amd64`, …).
2. Upload `checksums.txt`, `checksums.txt.sig`, and, for Linux, the `.asc`
   detached signatures plus `ncc-release-signing.gpg`.

Then **`ncc-orchestrator -u`** will:

1. Fetch the latest release and pick the asset for the current OS/arch.
2. Look for an asset whose name contains `checksum` or `sha256`.
3. Download that file and find the line matching the binary filename.
4. Download the binary and compare its SHA256 with the hash from the file.
5. If they match, replace the running binary (or on Windows, write `.new.exe`).

If no checksum asset is found, the tool still downloads and replaces the binary but **does not** verify it (no “Checksum verified.” message).

---

## 4A. Required assets per track

### `v1.x` release (CLI-only)

Minimum required assets:

- `ncc-orchestrator-linux-amd64`
- `ncc-orchestrator-linux-arm64`
- `ncc-orchestrator-darwin-amd64`
- `ncc-orchestrator-darwin-arm64`
- `ncc-orchestrator-windows-amd64.exe`
- `ncc-orchestrator-windows-arm64.exe`
- `checksums.txt`

### `v2.x` release (full stack)

Include all **v1 CLI assets** above, plus:

Preferred (less confusing for users):

- one stack bundle per platform:
  - `ncc-v2-stack-linux-amd64.tar.gz`
  - `ncc-v2-stack-linux-arm64.tar.gz`
  - `ncc-v2-stack-darwin-amd64.tar.gz`
  - `ncc-v2-stack-darwin-arm64.tar.gz`
  - `ncc-v2-stack-windows-amd64.zip`
  - `ncc-v2-stack-windows-arm64.zip`

Expected bundle layout:

- `bin/ncc-api-server` (or `.exe`)
- `bin/ncc-ui-server` (or `.exe`)
- `frontend-dist/` (built UI files)

Legacy fallback (still supported by bootstrap):

- `ncc-api-server-<os>-<arch>` binaries (raw)
- `ncc-ui-server-<os>-<arch>` binaries (raw)
- frontend build archive (zip or tar.gz)

Why this matters:

- `ncc-orchestrator update` upgrades CLI binary.
- `ncc-orchestrator v2-bootstrap` prefers a single `ncc-v2-stack-<os>-<arch>` bundle.
- If stack bundle is missing, it falls back to API binary + UI binary + frontend archive.

If `v2` assets are missing, bootstrap will fail with a "required v2 assets not found" message.

---

## 4B. Quick maintainer release procedure

### Release `v1.x`

1. Checkout `v1` maintenance branch and bump version metadata.
2. Build CLI binaries for all supported OS/arch targets.
3. Generate `checksums.txt`.
4. Create tag `v1.x.y` and publish release with CLI binaries + checksums.
5. Validate:
   - `ncc-orchestrator update --check` selects `v1.x.y` when current major is `v1`.

### Release `v2.x`

1. Checkout `main` and bump version metadata.
2. Build CLI binaries.
3. Build `ncc-api-server` and `ncc-ui-server` binaries for supported OS/arch.
4. Build frontend distribution and package it (`.zip` or `.tar.gz`).
5. Generate `checksums.txt` for CLI binaries (and optionally for other binaries).
6. Create tag `v2.x.y` and publish release with all required v2 assets (the current release is `v2.4.0`).
7. Validate:
   - `ncc-orchestrator update --check --allow-major-upgrade`
   - `ncc-orchestrator v2-bootstrap --check`
   - `ncc-orchestrator v2-start --help`

---

## 5. Optional: automate in CI

Example step to build multiple binaries and a checksum file in GitHub Actions:

```yaml
- name: Build binaries
  run: |
    VERSION="${{ steps.version.outputs.VERSION }}"
    for goos in linux darwin windows; do
      for goarch in amd64 arm64; do
        [[ $goos == windows && $goarch == arm64 ]] && continue
        out="dist/ncc-orchestrator-${goos}-${goarch}"
        [[ $goos == windows ]] && out="${out}.exe"
        GOOS=$goos GOARCH=$goarch go build -ldflags "-w -s -X main.Version=${VERSION} ..." -o "$out" .
      done
    done

- name: Generate checksums
  run: |
    cd dist
    sha256sum ncc-orchestrator-* > checksums.txt   # Linux runner
    # Or on macOS runner: shasum -a 256 ncc-orchestrator-* > checksums.txt

- name: Upload release assets
  uses: softprops/action-gh-release@v1
  with:
    files: dist/*
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

Then create the release (tag + publish); the workflow can upload the `dist/` contents (binaries + `checksums.txt`) as release assets.

---

## Summary

|Step|What to do|
|---|---|
|1|Pick target track: `v1` (CLI-only) or `v2` (CLI + API/UI/frontend).|
|2|Build binaries named e.g. `ncc-orchestrator-<os>-<arch>` (or `.exe` on Windows).|
|3|Generate checksums with `<64-char SHA256>  <exact filename>`.|
|4|For `v2`, also upload `ncc-api-server`, `ncc-ui-server`, and frontend archive assets.|
|5|Upload assets + checksum file to the release tag (`v1.x.y` or `v2.x.y`).|

Then **`ncc-orchestrator -u`** will verify the download using that checksum file when present.
