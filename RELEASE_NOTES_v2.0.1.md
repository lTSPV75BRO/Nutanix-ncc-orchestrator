# Release notes — v2.0.1

**Date:** 2026-05-27
**Type:** Patch release (P0 update-path fix + UX polish)

This release fixes the P0 silent-corruption regression in the v1.x → v2.0.0 self-updater **and** rebuilds the `update` semantics around a single, name-invariant abstraction: **`update` upgrades the v2 stack package as a whole, irrespective of which binary you invoke or how it was renamed**. All v1.x users should upgrade via this release.

## TL;DR

- `update` now downloads the **stack archive** (`ncc-v2-stack-<os>-<arch>.{tar.gz,zip}`) and reinstalls every v2 component atomically (orchestrator + api-server + ui-server + frontend-dist + example_config.yaml). The running binary self-replaces in place.
- Works the same whether you run `update` from `ncc-orchestrator`, `ncc-api-server`, `ncc-ui-server`, or any renamed binary — the package selection is independent of the invocation name.
- Falls back to the legacy single-binary update path for v1.x releases that don't ship a stack archive.
- `v2-bootstrap` and `v2-start` are robust against both canonical (`bin/ncc-api-server`) and legacy platform-suffixed (`bin/ncc-api-server-<os>-<arch>`) names.
- v2.0.0 → v2.0.1 is a trivial in-place patch.
- v1.x → v2.0.0 users who already ran the buggy path can recover by running `update` again (the v2.0.0 release was already hotfixed in place on 2026-05-27 19:35Z) or by following the recovery instructions in `RELEASE_NOTES_v2.0.0.md` → Known issues.

## What was broken

Before v2.0.1, `pickAssetForCurrentPlatform` in `goNCC.go` selected the first release asset whose name contained both the running platform's GOOS and GOARCH strings. v2.0.0 shipped three binaries per platform:

- `ncc-orchestrator-<os>-<arch>` ← the actual orchestrator
- `ncc-api-server-<os>-<arch>` ← REST API server (different `package main`)
- `ncc-ui-server-<os>-<arch>` ← UI proxy server

GitHub returns release assets in alphabetical order, so `ncc-api-server-…` matched first and was downloaded in place of `ncc-orchestrator-…`. The api-server binary boots an HTTP listener on `:8081` regardless of CLI arguments, so any subsequent invocation looked like:

```
$ ./ncc-orchestrator update --check
2026/05/28 00:57:07 api auth token ready (source=generated, token_file=…/.ncc-api-token)
2026/05/28 00:57:07 ncc-api-server listening on :8081 (auth_mode=token, tls=false)
```

…instead of the expected update-check output. The user's orchestrator binary was silently and permanently replaced.

## What was fixed

### 1. `update` is now a package-level, name-invariant operation

The core abstraction shift in v2.0.1. Previously `update` was a "self-replace this one binary" operation; the v1.x selector picked an asset by matching the running executable's basename against asset names. That broke catastrophically when v2.0.0 shipped three binaries per platform.

In v2.0.1, `update` instead selects the **stack archive** (`ncc-v2-stack-<os>-<arch>.{tar.gz,zip}`) — a single asset that bundles every v2 component. The install flow:

1. Download the stack archive for the platform.
2. Verify the SHA-256 against the release's `checksums.txt`. Refuses to install if no checksum manifest is found on the release.
3. Extract to a private temp directory.
4. Verify the extracted layout (`bin/ncc-api-server`, `bin/ncc-ui-server`, `frontend-dist/`).
5. Atomically copy `bin/*`, `frontend-dist/`, and `example_config.yaml` into the install directory.
6. Atomically self-replace the running binary by matching its basename against the canonical names in the extracted `bin/` (falls back to `bin/ncc-orchestrator` if no match).

Install-dir resolution is deterministic and binary-name-invariant:

- Running from `<X>/bin/<self>` → install over `<X>` (refreshes an existing bootstrapped stack).
- Running from anywhere else → install into the binary's directory (flat layout).

For legacy releases (v1.x) that do not publish a stack archive, the code falls back to the original single-binary update flow with the prefix-match selector fix (see #4 below) preserved as a safety net.

### 2. Legacy single-binary selector hardening (fallback path)

When the stack archive path doesn't apply (legacy v1.x releases), `pickAssetForCurrentPlatform` now prefers assets whose name starts with `<exeBase>-` over plain first-match. This is the v2.0.0 regression fix retained as defense in depth.

### 3. `v2-bootstrap` / `v2-start` accept both binary naming conventions

`hasBootstrappedV2Layout`, `resolveV2RuntimeLayout`, and `resolveV2APIBinary` now accept either canonical (`bin/ncc-api-server`) or platform-suffixed (`bin/ncc-api-server-<os>-<arch>`) names. This unblocks bootstrap against legacy v2.0.0 stack archives that ship platform-suffixed names, while remaining the preferred lookup for future stack archives that ship canonical names.

### 4. Release-asset layout policy enforced in `binaryGO.txt`

The packaging script now enforces two policies that prevent the v1.x→v2.0.0 regression from ever recurring:

- **Stack archives use canonical binary names** (no platform suffix inside `bin/`). `cp src/ncc-api-server-<os>-<arch> bin/ncc-api-server`. Matches what `v2-bootstrap` writes and what every layout-resolver expects.
- **Standalone `ncc-api-server-*` / `ncc-ui-server-*` are excluded from the publishable asset set** (release upload + `checksums.txt`). They continue to ship inside the stack archives. Documented inline with rationale that cannot be reverted without intent.
- `COPYFILE_DISABLE=1` + `--no-mac-metadata` are passed to tar so macOS-built archives no longer carry `._*` resource-fork sidecars.

### 5. v2.0.0 release was hotfixed in place

For users still on v1.x updaters who run `update --allow-major-upgrade`, the 12 standalone `ncc-api-server-*` and `ncc-ui-server-*` assets were deleted from the v2.0.0 release on 2026-05-27 19:35Z. The v1.x selector now alphabetically lands on `ncc-orchestrator-*` as the first match, so the regression cannot trigger against the v2.0.0 release going forward.

### 6. Regression locked in with tests

`goNCC_test.go` adds seven tests with twenty total cases:

- `TestPickAssetForPlatform_PrefersExeBasenamePrefix` (7 sub-cases) — legacy single-binary selector fix.
- `TestPickAssetForPlatform_ArchiveOnlyRelease`, `TestPickAssetForPlatform_NoMatch`, `TestPickAssetForPlatform_TrimmedV200Release` — selector edge cases.
- `TestPickStackAssetForPlatform` (6 sub-cases) + `TestPickStackAssetForPlatform_NoStackInRelease` — pins the package-archive selection on every supported platform and the legacy-release fallback behavior.
- `TestResolvePackageInstallDir` (4 sub-cases) — pins the install-dir auto-detection (binary-name-invariant by construction).
- `TestHasBootstrappedV2Layout_AcceptsBothNamingConventions` (4 sub-cases) — both `bin/ncc-api-server` and `bin/ncc-api-server-<os>-<arch>` layouts pass the post-bootstrap check.

## How to upgrade

```bash
# From v2.0.0 (in-place, no flag needed)
ncc-orchestrator update

# From v1.x (requires --allow-major-upgrade)
ncc-orchestrator update --allow-major-upgrade
```

Both paths now correctly download `ncc-orchestrator-<os>-<arch>` from the v2.0.1 release. The downloader verifies SHA-256 against `checksums.txt` before replacing the binary in place.

If you previously ran the buggy v1.x updater against v2.0.0 and ended up with the api-server binary, recover by:

```bash
# Replace darwin-arm64 with your platform
curl -fL -o ncc-orchestrator \
  https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.0.1/ncc-orchestrator-darwin-arm64
chmod +x ncc-orchestrator
xattr -c ncc-orchestrator 2>/dev/null    # macOS only, clears quarantine

shasum -a 256 ncc-orchestrator
# Compare against the matching line in:
#   https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.0.1/checksums.txt
```

Verify the recovered binary is the orchestrator (not the api-server) by checking the embedded Go buildinfo:

```bash
go version -m ./ncc-orchestrator | grep '^\s*path\s'
# Must show:    path	goncc                            ← orchestrator ✓
# Not:          path	goncc/cmd/ncc-api-server         ← api-server ✗
```

## What is NOT changed in v2.0.1

- API contract: identical to v2.0.0. No new endpoints, no breaking changes.
- UI: no frontend changes.
- Configuration: no new flags, no deprecated flags.
- Performance / footprint: identical (one file diff in the orchestrator binary, plus version metadata).

## Verification

| Check | Result |
|---|---|
| `go vet ./...` | clean |
| `go test -count=1 -short ./...` | all packages pass (added: 4 tests / 10 sub-cases) |
| `go build ./...` + `go build ./cmd/ncc-mcp-server` | clean |
| Frontend `tsc --noEmit -p tsconfig.json` | clean |
| `govulncheck ./...` | clean (unchanged from v2.0.0) |
| `npm audit --production` | clean (unchanged from v2.0.0) |
| Release-asset layout | enforced by `binaryGO.txt` step 7 |

## Acknowledgements

The regression was reported during the v2.0.0 release-day verification by the maintainer observing that `update --check` printed api-server startup logs instead of update output. Root-cause analysis traced the issue to alphabetical first-match selection in `pickAssetForCurrentPlatform`, fixed in this release.
