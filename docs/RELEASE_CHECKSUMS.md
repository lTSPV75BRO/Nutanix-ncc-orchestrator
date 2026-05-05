# Release checklist: checksum verification for `--update`

For **`ncc-orchestrator -u`** to verify downloads, the GitHub release must include a **checksum file** asset that the tool can find and parse.

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
```
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

| Step | What to do |
|------|------------|
| 1 | Build binaries named e.g. `ncc-orchestrator-<os>-<arch>` (or `.exe` on Windows). |
| 2 | Generate a file with lines: `<64-char SHA256>  <exact filename>`. |
| 3 | Name that file so it contains `checksum` or `sha256` (e.g. `checksums.txt`). |
| 4 | Upload binaries and the checksum file as assets of the GitHub release. |

Then **`ncc-orchestrator -u`** will verify the download using that checksum file when present.
