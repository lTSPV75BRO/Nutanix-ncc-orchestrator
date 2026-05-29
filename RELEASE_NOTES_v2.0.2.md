# Nutanix NCC Orchestrator — v2.0.2

**Release date:** 2026-05-29
**Type:** Patch release. Recommended for everyone running v2.0.0 / v2.0.1, especially anyone using the documented `cd <stack>/bin && ./ncc-orchestrator v2-start` flow.

This release fixes a P0 chain that surfaced once users started exercising the recommended "extract the stack, run from `bin/`" UX rather than the project-root flow used during the v2.0.0 / v2.0.1 release tests. None of the fixes change on-the-wire behavior; existing scripts that pass explicit `--install-dir` / `--config-path` / etc. continue to work.

---

## What was broken

Cumulatively, running `./ncc-orchestrator v2-check` or `./ncc-orchestrator v2-start` from `<X>/bin/` after extracting a v2.0.1 stack archive produced one of these failures:

```
v2-check failed (5 issues)
- api-server binary not executable under install dir: <cwd>/.ncc-v2
- ui-server binary not executable under install dir: <cwd>/.ncc-v2
- frontend-dist missing under install dir: <cwd>/.ncc-v2
- config-path not readable: <cwd>/config.yaml ...
- ui-listen bind failed (:8080): listen tcp :8080: bind: address already in use
```

```
Error: wait-ready api health check failed: endpoint http://localhost:18093/api/v1/health
not ready: ... dial tcp [::1]:18093: connect: connection refused
```

```
2026/.../... path escapes repo root: <install-dir>/example_config.yaml
```

These were caused by four independent bugs. v2.0.2 fixes all four.

---

## Fixes

### 1. `v2-check` / `v2-start` / `v2-stop` / `uninstall` auto-detect the stack root

When the orchestrator is invoked from `<X>/bin/<self>` and `<X>` carries the v2 layout markers (`frontend-dist/` or `bin/ncc-api-server*`), the install-dir defaults to `<X>` instead of the legacy `<cwd>/.ncc-v2`. Secondary paths default to `<install-dir>/<name>` (config, outputfiles, nccfiles, .ncc-api-token). When `config.yaml` is absent, the orchestrator now falls back to `<install-dir>/example_config.yaml` with an informational warning so first-run UX never blocks on a missing config.

Pinned by `TestDefaultV2InstallDir_*` in `goNCC_test.go`.

### 2. `--repo-root` for the api-server now contains everything it touches

The api-server's `--repo-root` is its path-traversal sandbox: every file path it accepts (config, output-dir, log-dir, token-file, downloaded artifacts) must live inside it. v2.0.0 / v2.0.1 hardcoded repo-root to `os.Getwd()`. With the v2.0.2 install-dir auto-detect, `<install-dir>` may be a *parent* of CWD (when the user is in `<X>/bin/`), so config / outputs / logs land outside the jail and the api-server rejects them.

`runV2Start` and `runV2Bootstrap` now compute `repo-root = ancestor(install-dir, cwd)` (whichever contains the other; defaults to install-dir when they're siblings) and pre-resolve symlinks. The latter is important on macOS, where `/tmp` is a symlink to `/private/tmp`: the api-server `EvalSymlinks` its `rootAbs` but compares against the user-supplied (unresolved) abs path, so without orchestrator-side resolution the prefix check fails legitimate paths.

`resolveV2PathToReal` walks up the path until it finds an existing ancestor for not-yet-created paths (output-dir / log-dir / token-file before mkdir) so partial paths still resolve consistently.

Pinned by `TestResolveV2RepoRoot`, `TestIsPathAncestor`, `TestResolveV2PathToReal_NonExistentSuffix`.

### 3. `wait-ready` and the UI→API backend URL preserve the user's bound IP

`localHTTPURLFromListen` always rewrote `127.0.0.1:port` to `localhost:port`. On macOS, `localhost` resolves to `::1` (IPv6) before `127.0.0.1` (IPv4), so the wait-ready probe and the UI server's backend connection both target the wrong address family when the api-server is bound IPv4-only — yielding `connection refused` even though the api-server is healthy.

The helper now preserves the user-supplied host (`127.0.0.1`, `192.168.x.y`, `my-host`, etc.). For CORS, a new `loopbackAltOriginFromListen` adds `http://localhost:port` to the allow-list when the UI binds to a loopback IP, so browsers can reach the UI under either name without "blocked by CORS" errors.

Pinned by `TestLocalHTTPURLFromListen`, `TestLoopbackAltOriginFromListen`.

### 4. `orchestrator_bin` is an absolute, executable path

`resolveV2OrchestratorBin` returned `./ncc-orchestrator` when the binary lived in CWD (correct for the orchestrator) but the api-server interpreted it relative to its own CWD (= repo-root), so any API-triggered run would have failed to spawn the orchestrator. Fixed to always return an absolute, symlink-resolved path.

Verified live by hitting `/api/v1/health` and checking the reported `orchestrator_bin` is executable.

### 5. Side fixes shipped alongside

- **Extractor preserves the executable bit.** `extractTarGzArchive` / `extractZipArchive` use the mode in the archive header instead of hardcoded `0644`. Binaries inside `ncc-v2-stack-*.tar.gz` come out `0755` after `update`. Pinned by `TestExtractTarGzArchive_PreservesExecutableBit`.
- **`uninstall --remove-local` sweeps both layouts.** Adds install-dir-relative entries (`<install-dir>/{outputfiles,nccfiles,promfiles,logs,.ncc-api-token,.ncc-api-schedule.json,.ncc-api-notifications.json}`) in addition to the existing CWD-relative ones. Belt-and-braces for `--remove-v2-runtime=false`.
- **`COPYFILE_DISABLE=1` + `--no-mac-metadata`** in `binaryGO.txt` so macOS-built tarballs don't carry `._*` resource forks.

---

## Backward-compatibility

- Scripts that passed explicit `--install-dir`, `--config-path`, `--output-dir`, `--log-dir`, `--token-file`, `--orchestrator-bin` continue to work unchanged.
- Legacy `v2-bootstrap --install-dir .ncc-v2 ; v2-check` from a project root still auto-detects nothing and falls back to `.ncc-v2` (the historical default), so existing first-run guides keep working. The example_config.yaml fallback simply removes a footgun.
- The package-level `update` flow introduced in 2.0.1 is unchanged; users on 2.0.0 / 2.0.1 just run:
  ```bash
  ./ncc-orchestrator update --allow-major-upgrade
  ```

---

## Tests

- Full Go unit suite passes with `-race -count=1` (374 sub-cases, 0 failures).
- `go vet`, `gofmt -l`, `govulncheck`, `npm audit`, `tsc --noEmit`, frontend `vite build` all clean.
- End-to-end smoke matrix from a fresh `ncc-v2-stack-darwin-arm64.tar.gz` extract:
  - `update` from a fake v1.0.0 binary → installs the v2.0.1 stack and self-replaces the renamed entrypoint.
  - Legacy `v2-bootstrap --install-dir .ncc-v2` + `v2-check` from project root → `status: ok` with example_config.yaml fallback warning.
  - `v2-start --detach --wait-ready --api-listen 127.0.0.1:18093 --ui-listen 127.0.0.1:18094` from `<X>/bin/` → readiness checks pass; `/api/v1/health` returns `status: ok`; `orchestrator_bin` is absolute and executable; CORS preflight allowed for both `http://localhost:18094` and `http://127.0.0.1:18094`; UI proxy returns 200; `v2-stop` cleanly stops both.
  - `uninstall --force --dry-run` from `<X>/bin/` → enumerates removals across both legacy and install-dir-relative entries.

---

## Upgrade

From any v2.0.0 / v2.0.1 install:

```bash
./ncc-orchestrator update --allow-major-upgrade
```

Or manually re-extract the stack archive over the previous one — both forms are supported.

After upgrade, the recommended runtime invocation is now flag-free:

```bash
cd ncc-v2-stack-<os>-<arch>/bin
./ncc-orchestrator v2-start --api-listen :8081 --ui-listen :8080
```

Add `--detach --self-heal` to background with restart supervision.

---

## Acknowledgements

The path-traversal failure was reported by a user running the documented `cd bin && ./ncc-orchestrator v2-check` flow on macOS, with logs that pinpointed all five symptoms in a single output and made the diagnosis straightforward. The IPv6 wait-ready bug surfaced from the same investigation. Thank you.
