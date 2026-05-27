# Release notes — v2.0.1

**Date:** 2026-05-27
**Type:** Patch release (single user-visible fix)

This release fixes a P0 silent-corruption regression in the v1.x self-updater that affected users moving from v1.x to v2.0.0. **All v1.x users should upgrade via this release rather than v2.0.0.**

## TL;DR

- Fixes `update --allow-major-upgrade` so v1.x → v2.x upgrades pick the correct binary even when the release ships multiple binaries per platform.
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

### 1. Selector now prefers the running executable's basename

`pickAssetForCurrentPlatform` was refactored into a testable inner function `pickAssetForPlatform(rel, goos, goarch, exeBase)`. The new selection order:

1. **Prefix-match (preferred):** non-archive asset whose name starts with `<exeBase>-` (e.g. `ncc-orchestrator-`) and contains both `goos` and `goarch`. This is the path that protects against the v2.0.0 regression.
2. **Legacy first-match (fallback):** any other non-archive asset whose name contains both `goos` and `goarch`. Preserves v1.x behavior for forks and renamed binaries.
3. **Archive (last resort):** any `.tar.gz` / `.zip` asset for the platform. Returned for inspection so the caller emits a "download and extract" hint rather than overwriting in place.

### 2. Release-asset layout policy enforced in `binaryGO.txt`

The release packaging step (step 7) now excludes standalone `ncc-api-server-*` and `ncc-ui-server-*` binaries from `checksums.txt` (and therefore from publishable assets). The api-server and ui-server binaries continue to ship **inside** the `ncc-v2-stack-*` archives. The exclusion is documented in `binaryGO.txt` with an explanatory comment so it cannot be reverted without intent.

### 3. v2.0.0 release was hotfixed in place

For users still on v1.x updaters who run `update --allow-major-upgrade`, the 12 standalone `ncc-api-server-*` and `ncc-ui-server-*` assets were deleted from the v2.0.0 release on 2026-05-27 19:35Z. The v1.x selector now alphabetically lands on `ncc-orchestrator-*` as the first match, so the regression cannot trigger against the v2.0.0 release going forward.

### 4. Regression locked in with tests

`goNCC_test.go` adds four tests with ten total cases:

- `TestPickAssetForPlatform_PrefersExeBasenamePrefix` — seven sub-cases covering the full v2.0.0 asset layout for orchestrator, api-server, ui-server, plus renamed-binary and empty-exeBase fallbacks.
- `TestPickAssetForPlatform_ArchiveOnlyRelease` — verifies archive-only releases return the `.tar.gz`.
- `TestPickAssetForPlatform_NoMatch` — verifies empty results for unsupported platforms.
- `TestPickAssetForPlatform_TrimmedV200Release` — pins the post-hotfix v2.0.0 asset layout as the canonical going-forward policy.

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
