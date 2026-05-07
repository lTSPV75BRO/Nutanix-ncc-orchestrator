# Release checklist: checksums + maintainer release flow (`v1` and `v2`)

For **`ncc-orchestrator -u`** to verify downloads, the GitHub release must include a **checksum file** asset that the tool can find and parse.

---

## 0. Choose release track first (`v1` vs `v2`)

Maintainers should decide release track before building artifacts:

- **`v1.x` track**: CLI-only distribution.
- **`v2.x` track**: CLI + API server + UI server + frontend bundle.

Suggested branch/tag convention:

- `v1` maintenance branch -> tags like `v1.2.3`
- `v2` maintenance branch -> tags like `v2.0.1`

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

When you create the release (e.g. v2.0.0):

1. Upload each **binary** (e.g. `ncc-orchestrator-linux-amd64`, …).
2. Upload the **checksum file** (e.g. `checksums.txt`).

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

1. Checkout `v2` branch and bump version metadata.
2. Build CLI binaries.
3. Build `ncc-api-server` and `ncc-ui-server` binaries for supported OS/arch.
4. Build frontend distribution and package it (`.zip` or `.tar.gz`).
5. Generate `checksums.txt` for CLI binaries (and optionally for other binaries).
6. Create tag `v2.x.y` and publish release with all required v2 assets.
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
