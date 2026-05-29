# Release notes - v2.0.0

**Date:** 2026-05-05
**Last asset update:** 2026-05-27 (asset-layout hotfix; see [Known issues](#known-issues-post-publication-hotfix))

> ## Superseded — please use [v2.0.2](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.2) (or newer) instead
>
> v2.0.0 has **two cumulative known issues** fixed in later patch releases. New users should download v2.0.2; existing v2.0.0 users should upgrade in place.
>
> | Issue | Fixed in | Symptom |
> |---|---|---|
> | v1.x self-updater silently overwrites `ncc-orchestrator` with `ncc-api-server` | **v2.0.1** (selector + package-level update) | `ncc-orchestrator update` later prints `ncc-api-server listening on :8081` instead of update output. |
> | `cd <stack>/bin && ./ncc-orchestrator v2-check`/`v2-start` reports `5 issues` (binaries not executable, frontend-dist missing, config not readable) and `wait-ready` fails with `connection refused`/`path escapes repo root` | **v2.0.2** (install-dir auto-detect + repo-root resolution + IPv4/IPv6 wait-ready + CORS loopback alt + orchestrator_bin abs path) | Affects the documented "extract the stack, run from `bin/`" flow on macOS especially. |
>
> **Upgrade in place from v2.0.0:**
>
> ```bash
> # The v2.0.1 update path is package-level: it downloads the v2.0.2
> # stack archive, verifies SHA-256 against checksums.txt, and atomically
> # replaces orchestrator + api-server + ui-server + frontend-dist +
> # example_config.yaml in your install dir.
> ./ncc-orchestrator update
> ```
>
> **Fresh install (preferred):** download `ncc-v2-stack-<os>-<arch>.tar.gz` from v2.0.2, verify its SHA-256 against the v2.0.2 `checksums.txt`, extract, then `cd bin && ./ncc-orchestrator v2-start --api-listen :8081 --ui-listen :8080`. No path flags needed.
>
> See [v2.0.1 release notes](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.1) for the self-updater/bootstrap fixes and [v2.0.2 release notes](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.2) for the install-dir / repo-root / wait-ready / CORS fixes.

This release finalizes the v2.0.0 production baseline for the full v2 stack (orchestrator runtime + API + UI + UI-integrated proxy) and aligns release/version assets across docs, manifests, and packaging.

> **Important for v1.x users:** Do **not** run `ncc-orchestrator update --allow-major-upgrade` against this release with a v1.x binary built before 2026-05-27. The v1.x self-updater contains a release-asset selector bug that picks the wrong binary when a release ships multiple binaries per platform. See [Known issues](#known-issues-post-publication-hotfix) below for the mitigation already applied to this release and recovery instructions if you ran the buggy path.

## Known issues (post-publication hotfix)

**Issue:** The v1.x self-updater (`pickAssetForCurrentPlatform` in `goNCC.go`) selects the first release asset whose name contains both the GOOS and GOARCH strings. v2.0.0 originally shipped three binaries per platform (`ncc-orchestrator-…`, `ncc-api-server-…`, `ncc-ui-server-…`); GitHub returns assets in alphabetical order, so the selector matched `ncc-api-server-…` first and silently overwrote the orchestrator binary with the api-server binary. The replaced binary boots an HTTP listener on `:8081` regardless of CLI arguments.

**Impact:** Any v1.x user who ran `ncc-orchestrator update --allow-major-upgrade` against v2.0.0 between 2026-05-27 19:05Z and 2026-05-27 19:35Z received an api-server binary in place of the orchestrator. Stack archives (`ncc-v2-stack-*`) and `ncc-orchestrator-…` standalone assets were always correct.

**Mitigation applied to this release (2026-05-27 19:35Z):** The 12 standalone `ncc-api-server-*` and `ncc-ui-server-*` assets have been removed from the v2.0.0 release. The v1.x selector now picks `ncc-orchestrator-*` as the first match. The api-server and ui-server binaries remain available **inside the stack archives** (`ncc-v2-stack-{linux,darwin,windows}-{amd64,arm64}.{tar.gz,zip}`) — extract them from `bin/` inside the archive.

**Permanent fix:** Shipped as v2.0.1 — `pickAssetForCurrentPlatform` now prefers assets whose name starts with the running executable's basename, so future multi-binary releases cannot trigger the same regression. v2.0.1 also reworked `update` to be a package-level operation (downloads the entire `ncc-v2-stack-*` archive and atomically reinstalls every component), which is name-invariant by construction.

### Additional issues fixed in v2.0.2

The following issues were identified after v2.0.1 shipped, while exercising the documented "extract `ncc-v2-stack-<os>-<arch>.tar.gz`, then `cd bin && ./ncc-orchestrator v2-start`" flow. They affect v2.0.0 and v2.0.1 equally; both are fixed in **v2.0.2**.

| # | Issue | Symptom | v2.0.2 fix |
|---|---|---|---|
| 1 | `v2-check` / `v2-start` / `v2-stop` / `uninstall` defaulted `--install-dir` to `<cwd>/.ncc-v2` even when run from inside an extracted stack | `v2-check failed (5 issues)` listing missing binaries / frontend-dist / config-path | Auto-detect: when invoked from `<X>/bin/<self>` and `<X>` carries v2 layout markers, install-dir defaults to `<X>`. Secondary paths default to `<install-dir>/<name>`. Falls back to `example_config.yaml` if `config.yaml` is absent. |
| 2 | `v2-start` from `<X>/bin/` failed with `path escapes repo root` | api-server immediately exited; logs showed the rejection | api-server's `--repo-root` (path-traversal sandbox) was hardcoded to `os.Getwd()`; v2.0.2 uses the `ancestor(install-dir, cwd)` and pre-resolves macOS `/tmp` → `/private/tmp` symlinks. |
| 3 | `wait-ready` hung / failed with `connection refused` on macOS when binding API to `127.0.0.1:<port>` | health probe targeted `localhost:<port>` which resolved to `::1` (IPv6) but server was bound IPv4-only | `localHTTPURLFromListen` now preserves the user-supplied host. CORS allow-list separately gains the `http://localhost:<port>` form when the UI binds to a loopback IP. |
| 4 | `/api/v1/health` reported `orchestrator_bin` as a non-existent path | API-triggered runs would have failed to spawn the orchestrator | `resolveV2OrchestratorBin` now returns absolute, symlink-resolved paths. |
| 5 | Extracted binaries occasionally lost the executable bit | "binary not executable" errors after `update` | `extractTarGzArchive` / `extractZipArchive` now use the archive's stored mode bits instead of hardcoded `0644`. |
| 6 | `uninstall --remove-local` only swept legacy CWD-relative paths | data under `<install-dir>/{outputfiles,nccfiles,…}` survived a non-`--remove-v2-runtime` uninstall | Cleanup set now includes both legacy CWD-relative and v2.0.2 install-dir-relative entries. |

**Upgrade to v2.0.2:**

```bash
# Package-level update (works from any v2.0.0 / v2.0.1 install)
./ncc-orchestrator update
```

The v2.0.1 updater downloads `ncc-v2-stack-<os>-<arch>.tar.gz` from the latest release, verifies SHA-256 against `checksums.txt`, and atomically replaces every v2 component in the install directory. After upgrade, the recommended invocation simplifies to:

```bash
cd <install-dir>/bin
./ncc-orchestrator v2-start --api-listen :8081 --ui-listen :8080
# (no path flags needed; install-dir is auto-detected)
```

**Recovery for users who already ran the buggy update:** Re-download the correct orchestrator binary directly:

```bash
# Replace darwin-arm64 with your platform
curl -fL -o ncc-orchestrator \
  https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.0.0/ncc-orchestrator-darwin-arm64
chmod +x ncc-orchestrator
xattr -c ncc-orchestrator 2>/dev/null    # macOS only, clears quarantine

# Verify checksum
shasum -a 256 ncc-orchestrator
# expected (darwin-arm64): 9f2361c911912e8a30b96d374a0bfbed2538954027e95488a7b68084d9d0e66f
# Full manifest: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download/v2.0.0/checksums.txt

./ncc-orchestrator version   # should report "Stream: Release" and the canonical SHA
```

If the recovered binary still misbehaves, inspect its embedded Go buildinfo: `go version -m ./ncc-orchestrator` must show `path goncc` (the orchestrator), **not** `path goncc/cmd/ncc-api-server` (the api-server). If you see the latter, the file is still the wrong asset — re-download.

## Highlights

- **Production verification completed**: full validation run executed on branch `v2.0.0` (`go test ./...`, `go vet ./...`, `go build ./...`, and `go build ./cmd/ncc-mcp-server`).
- **Edge-case validation completed**: targeted tests for exclusion modes, regex validation, retention behavior, secrets hardening, and retry circuit breaker passed.
- **Release/documentation alignment**: README release pointers, feature references, changelog, and release docs now target `v2.0.0`.
- **Version consistency updates**: default binary version, Helm chart/appVersion, Helm values image tag, and Kubernetes image tags now use `2.0.0`.
- **CodeQL workflow fix**: repository CodeQL scan now targets branch-appropriate languages (`go`, `actions`), avoiding JS/TS "no source found" failures for this scope.
- **API hardening uplift**: added per-client rate limiting for sensitive auth/mutation routes (`--rate-limit-per-minute`, default `60`).
- **API/UI/proxy stack hardening**: API and UI server security headers normalized, stricter token/session handling, safer forwarded-header behavior, and route-level proxy restrictions for `/api/v1/*`.
- **Preflight automation contract**: `preflight-check` JSON now includes machine-readable `remediation_code` on non-pass checks.
- **Frontend/API Explorer hardening**: sensitive request headers/body are no longer persisted in browser storage; external absolute URLs are blocked by default.
- **Kubernetes network hardening**: added default-deny ingress NetworkPolicy plus scoped UI/API ingress policies.

## Post-baseline updates (v2.0.0 line)

The following operational enhancements were added after the initial v2.0.0 baseline and are included in current branch state:

- **Beginner-first quickstart automation**:
  - New `quickstart` flow for bare-minimum installs (binary-only environments).
  - Supports guided setup, safe auto-fixes, and optional interactive prompts.
  - Can detect missing v2 components and ask user permission before auto-download.
- **Automation policy levels**:
  - `--automation-level advisory|safe-fix|full-auto` added for run/quickstart workflows.
  - `full-auto` now applies conservative runtime stability tuning (parallelism/timeouts/retry guardrails).
- **Detached v2 self-healing**:
  - `v2-start --self-heal` with restart-budget controls (`--self-heal-max-restarts`, `--self-heal-window`).
  - `v2-stop` now accounts for supervisor PID files created by detached self-heal monitors.
- **Prometheus export controls**:
  - Added optional metrics toggle: `prom-enabled` / `--prom-enabled`.
  - Prometheus directory checks/writes are skipped cleanly when disabled.
- **Expanded `.prom` metric set** for richer dashboards/alerts:
  - health/quality signals (`run_health_score`, problem ratio, failure/warn/error presence)
  - payload/shape signals (`check_unique_total`, `check_duplicate_total`, detail byte metrics)
  - per-severity ratio metric (`nutanix_ncc_check_severity_ratio`).
- **Documentation overhaul for reproducible builds**:
  - Added `docs/BUILD_FROM_SCRATCH.md` as canonical end-to-end setup/build/run/deploy guide.
  - Expanded migration and contributor guidance to include validation gates, phased cutover, and rollback checks.
  - Aligned architecture notes with current Kubernetes API runner-binary staging model.
- **Scheduler overlap protection and observability**:
  - `create-schedule` now supports lock-enabled cron generation (`--with-lock=true`) to prevent overlapping runs.
  - Added `/api/v1/schedule/health` for scheduler health snapshots (`last_run`, `last_success`, `last_error`, lock/log metadata).
- **v2 startup pre-check command**:
  - Added `v2-check` to validate runtime binaries, absolute path readiness, directory writability, and API/UI port bind availability before `v2-start`.
- **Security posture improvements**:
  - Startup warns when config includes plaintext `password` (recommend `NCC_PASSWORD` or `secret://` source).
  - Config API responses now expose redacted-safe content variants for sensitive values.
- **Logs UX control enhancements in UI**:
  - Added `Follow tail` and `Jump to latest` controls for Runner Logs and Live Logs to improve incident review workflows.

## Scope note for v2.0.0

This release line includes the orchestrator runtime plus the v2 API/UI and frontend components:

- `cmd/ncc-api-server`
- `cmd/ncc-ui-server` (includes UI-layer API proxy routes under `/api/v1/*`)
- `frontend`

## Validation snapshot (2026-05-05)

- `go test ./...` -> pass
- `go vet ./...` -> pass
- `go build ./...` -> pass
- `go build ./cmd/ncc-mcp-server` -> pass
- `go test ./... -run "TestDoWithRetryCircuitBreaker|TestValidateSecretsFileHardening|TestFilterBlocksByTitle|TestApplyArtifactRetentionPolicies|TestBindConfigExcludeAlertMatchModeInvalid|TestWriteExcludedAlertsAuditJSONSchemaVersion"` -> pass
- `go run . validate-config --config config.yaml` -> pass
- `go run . validate-secrets --config config.yaml` -> expected failure when `secret://` refs are present without a configured provider
- `NCC_SECRETS_PROVIDER=env ... go run . validate-secrets --config config.yaml` -> pass
- `go test -race ./...` -> pass
- `npm test` -> pass
- `npm run build` -> pass
- `kubectl kustomize k8s` -> pass

## Release-readiness validation (2026-05-27, late-cycle hardening)

| Gate                                    | Result                                                                                             |
| --------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `govulncheck ./...` (Go stdlib + deps)  | **No vulnerabilities found** after `go 1.26.2 → 1.26.3` toolchain bump                              |
| `npm audit --omit=dev`                  | **found 0 vulnerabilities** after `dompurify` `^3.4.7` override and `yaml` patch                    |
| `go test -count=1 -race -timeout=180s ./...` | pass (`goncc`, `goncc/cmd/ncc-api-server`, `goncc/internal/kblinks`)                            |
| `go vet ./...`                          | clean                                                                                              |
| `gofmt -l .`                            | clean                                                                                              |
| `tsc --noEmit` (frontend)               | clean                                                                                              |
| `vite build` (frontend)                 | clean                                                                                              |
| Secret scan                             | clean (only template placeholders, K8s `secretKeyRef`, and test fixtures)                          |
| API smoke + edge-case suite (57 checks) | 100% pass: public endpoints, token enforcement (no/bad/mutated tokens), method gating (405 on disallowed verbs), CORS preflight (allowed/forbidden origins), invalid-input rejection (`limit=-5/abc`, `since=garbage`, `source=invalid`), path traversal rejection (`..%2F`, `sub%2Fevil`), structured `error_code` envelopes, `DELETE /runs/active` 409 when no active run, security headers (`X-Content-Type-Options`, `X-Frame-Options`, `Cache-Control`), schedule input validation (`action=create` requires cron or every) |

## What changed in late-cycle hardening (after 2026-05-05 baseline)

### Security uplift

- **Go stdlib 1.26.2 → 1.26.3** — closes 5 stdlib CVEs (`GO-2026-4976` ReverseProxy query forwarding, `GO-2026-4971` `net.Dial`/`LookupPort` NUL-byte panic, `GO-2026-4918` HTTP/2 transport infinite loop, plus two related stdlib advisories).
- **DOMPurify `^3.4.7` pinned via `package.json#overrides`** — clears 5 transitive Monaco-bundled DOMPurify advisories (ADD_TAGS/FORBID_TAGS bypasses, SAFE_FOR_TEMPLATES bypass, prototype pollution, mutation-XSS) without downgrading Monaco.
- **`yaml` patch** — closes deeply-nested-collection stack-overflow (`GHSA-48c2-rrv3-qjmp`).
- **Schedule validator tightened** — `action=create` now requires `cron` or `every`; empty `{}` PUT now correctly returns `400`. Covered by `TestValidateScheduleInput` table-driven test.

### Release ergonomics

- **`example_config.yaml` is now first-class** — Curated, validator-clean reference config lives at the repo root, ships in `dist/`, and is bundled inside every `ncc-v2-stack-*.tar.gz`/`.zip` archive. Uses `secret://NAME` refs with `secrets-provider: env` so it validates and runs out-of-the-box once `NCC_PASSWORD` (and optional `SMTP_PASSWORD`, `WEBHOOK_TOKEN`) are exported. Verified with `ncc-orchestrator validate-config --config example_config.yaml`.

### New endpoints / API surface

- `DELETE /api/v1/runs/active` — cancel an active run (409 when none active).
- `GET /api/v1/runs/{id}` — single archived run details with embedded artifacts; rejects `..`/`/` in id.
- `Access-Control-Allow-Methods` now correctly includes `DELETE`.

### Frontend / UX

- Header trigger button now mirrors Settings → Runs indicator (spinning icon + `Running · Xm Ys`).
- Dashboard alerts table empty state is now context-aware (in-progress / clean / no-runs).
- Runs table replaces blank "Index" column with `Type`/`Status`/`Duration`/`Clusters`/`Issues`.
- Monaco editor now loads locally (CSP-compliant `script-src 'self'`) with custom `ncc-light` / `ncc-dark` / `ncc-it-pro` themes.
- Theme overhaul: near-black charcoal dark mode, clean zinc light mode, aligned across `theme.tsx` and `styles.css`.
- Form accessibility: every interactive form field across ConfigSection, RunsSection, ScheduleSection, PolicyGateBuilderSection, AuditLogSection, LogsSection, DashboardPage, ApiExplorerSection, JsonOutputsSection, RawOutputsSection, and SecretsMigrationModal now has `id`/`name`/`htmlFor`/`aria-label`/`autoComplete` as appropriate; password fields are wrapped in real `<form>` elements.
- Sparkline timezone bug fixed (`localDateKey`) and now counts only completed `history`/`summary` events, not `trigger` button presses.

## Upgrade notes from v1.1.0

- Update image/binary tag to `2.0.0`.
- Use [docs/PRODUCTION_READINESS_v2.0.0.md](docs/PRODUCTION_READINESS_v2.0.0.md) as the release gate checklist.
- Use this file as the GitHub release description body for tag `v2.0.0`.

## Artifacts and deployment versions

- **Orchestrator version:** `2.0.0`
- **MCP server version:** `2.0.0`
- **Docker image:** `prajwalnutant/nutanix-ncc-orchestrator:2.0.0`
- **Helm chart:** `helm/ncc-orchestrator` chart/appVersion `2.0.0`
- **Kubernetes manifests:** full v2 stack under `k8s/` (`runner-cronjob`, `api-deployment`, `ui-deployment`) plus NetworkPolicies.

## GitHub release checklist (v2.0.0)

1. Ensure `VERSION` is `2.0.0`.
2. Ensure changelog `2.0.0` section is present in [CHANGELOG.md](CHANGELOG.md).
3. Build and attach binaries for Linux/macOS/Windows targets.
4. Generate and attach `checksums.txt` (see [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md)).
5. Tag as `v2.0.0`.
6. After publish, verify:
   - release assets are downloadable
   - Docker tag `prajwalnutant/nutanix-ncc-orchestrator:2.0.0` exists
   - `ncc-orchestrator update --check` can discover and verify assets
