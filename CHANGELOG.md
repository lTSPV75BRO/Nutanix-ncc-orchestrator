# Changelog

All notable changes to the Nutanix NCC Orchestrator are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

**Release checklist (for maintainers):** Ensure [`VERSION`](VERSION) matches the intended tag; default `main.Version` in code is `2.1.0` when not set via ldflags. Run `go vet ./...`, `go test -race ./...`, and `go build ./...` (and `go build ./cmd/ncc-mcp-server`). Confirm `k8s/` and `helm/` image tags match `VERSION`. Tag `v2.1.0` and create a GitHub release using the matching `RELEASE_NOTES_v*.md`; attach `ncc-orchestrator-*` standalone binaries, `ncc-v2-stack-*` archives, and `checksums.txt` only — **do not** attach standalone `ncc-api-server-*` / `ncc-ui-server-*` binaries (the v1.x self-updater would silently mis-select them; see [2.0.0] known-issue note below and the v2.0.1 selector fix).

---

## [2.1.0] - 2026-06-01

Maintenance and quality release. Closes a checksum-verification gap in `v2-bootstrap`, adds notification observability and templating, refreshes dependencies, improves the Windows self-update experience, and carries out the first wave of the `goNCC.go` package extraction (five new `internal/` packages). No breaking changes; all v2.0.x invocations keep working. **Affiliation note:** independent open-source project; not affiliated with or endorsed by Nutanix, Inc.

### Added

- **Checksum verification for `v2-bootstrap` downloads.** `v2-bootstrap` now verifies every downloaded asset (the `ncc-v2-stack-*` archive, or the api/ui binaries + frontend archive in the legacy fallback) against the release `checksums.txt` before extracting/installing, matching the strictness `update` already enforced. Pinned by `TestVerifyDownloadedAsset`.
- **`--skip-checksum-verify` flag** on both `update` and `v2-bootstrap` as an explicit, clearly-warned escape hatch for air-gapped or internally-mirrored installs. Default is hard-fail on a missing checksum manifest or hash mismatch.
- **Windows self-update helper.** On Windows, `update` now writes an `apply-ncc-update.cmd` next to the binary that waits for the running process to exit, swaps in the downloaded `.new.exe`, and self-deletes — replacing the previous "copy the file yourself" instruction with a single command. Pinned by `TestWriteWindowsUpdateSwapHelper`. The helper is added to the `uninstall` cleanup set.
- **Notification delivery metrics.** Each run now records per-channel notification outcomes (email / webhook / slack) and, when `prom-enabled` is set, writes a run-level `notifications.prom` textfile exporting `nutanix_ncc_notification_attempts_total{channel=…}` and `nutanix_ncc_notification_failures_total{channel=…}`. Delivery failures were previously only logged; monitoring can now alert on them. A line is always emitted per channel (0 when unused) so alerting rules never break on a missing series. Pinned by `TestNotificationMetrics`, `TestWriteNotificationMetricsFile`, and `TestNotificationWrappers_SkipDisabled`.
- **Custom notification templates.** New optional config keys `email-subject-template`, `email-body-template`, and `webhook-template` accept Go `text/template` strings rendered against the run summary (`.Cluster`, `.FailCount`, `.WarnCount`, `.ErrCount`, `.InfoCount`, `.TotalChecks`, `.Overview`, `.StartedAt`, `.FinishedAt`, `.OutputFiles`). Empty = the built-in defaults. A broken template falls back to the default (logged, never drops the notification); an unknown field fails the template rather than emitting `<no value>`. Applied across the per-cluster, digest, and replay notification paths. Pinned by `TestRenderNotificationTemplate`, `TestApplyEmailTemplates`, and `TestSendWebhook_TemplateBody`.

### Refactored

- **`goNCC.go` package extraction.** Five focused leaf packages were carved out of the ~15.5k-line `goNCC.go`, each re-exported from `main` via type/function aliases so the thousands of existing references and call sites compile unchanged:
  - `internal/model` — foundational shared types (`Config`, `ClusterCredential`, `NotificationSummary`, `ParsedBlock`, `FS`, `HTTPClient`) and `ClusterHealthScore`.
  - `internal/promtext` — Prometheus textfile writers (`WritePrometheusFile`, `WriteNotificationMetricsFile`, `SanitizeLabel`).
  - `internal/retryutil` — the shared retry/backoff helpers (`JitteredBackoff`, `IsRetryableStatus`, `RetryAfterDelay`), a stdlib-only leaf so both `main` and `internal/notify` can reuse them without an import cycle.
  - `internal/notify` — the email/webhook/Slack senders, retry wrappers, `text/template` overrides, and the per-channel delivery-metrics accumulator (run-level counters now read via `notify.ResetMetrics` / `notify.SnapshotMetrics`).
  - `internal/nccparse` — the NCC summary parser (`SplitLines`, `ParseSummary`, `ValidateParsedAlertsAgainstPluginResults`) producing `model.ParsedBlock`.
  Behavior is identical and each package ships its own unit tests (notification, template, and parser tests were relocated alongside their implementations). The full Go suite passes under `-race`.

### Changed

- **Dependency refresh.** Go modules updated (`github.com/modelcontextprotocol/go-sdk` 1.6.0→1.6.1, `golang.org/x/sys` 0.44→0.45, `mattn/go-colorable`, `mattn/go-runewidth`); `go vet`, `go test`, and `govulncheck` clean. Frontend `npm audit` reports 0 vulnerabilities. GitHub Actions remain on current major pins (floating tags receive patches automatically).
- Version bumped to `2.1.0` across `VERSION`, the orchestrator/api/ui `main.Version` defaults, the OpenAPI spec version, `binaryGO.txt`, `frontend/package.json`, the Helm chart, and the `k8s/` image tags.

### Remaining (tracked in IMPROVEMENTS.md)

- **`goNCC.go` slimming (continued).** The headline extraction (`internal/model`, `internal/promtext`, `internal/retryutil`, `internal/notify`, `internal/nccparse`) is complete for v2.1.0. `goNCC.go` is still large; further increments (e.g. the report renderers and the HTTP client) can follow the same alias-backed, behavior-preserving pattern. See [`IMPROVEMENTS.md`](IMPROVEMENTS.md).

---

## [2.0.2] - 2026-05-29

Maturity + patch release. Includes the v2.0.0 / v2.0.1 → v2.0.2 patch chain (path / CORS / IPv6 / extractor / uninstall), plus a substantial trust-and-operability layer: Windows VERSIONINFO, optional code-signing hooks, `verify` / `doctor` / `v2-status` subcommands, `ncc-api-server --health-check` mode, Prometheus `/metrics`, shell completions, CycloneDX SBOMs, SLSA build provenance, `release-attestation.json`, and `docker compose` one-command launch. **Affiliation note:** independent open-source project; not affiliated with or endorsed by Nutanix, Inc.

### Added

- **`ncc-orchestrator verify`** subcommand prints embedded version / git revision / build date / Go version / executable SHA-256, project URL, license, and an explicit non-affiliation disclaimer. Pinned by `TestRunVerifyCommand_OutputContract` and `TestSha256OfFile_RoundTrip`.
- **`ncc-orchestrator doctor`** subcommand: 5-section diagnostic (verify, v2-check, v2-status, environment, log tails) plus a redacted support tarball under `./ncc-support-<UTC-timestamp>.tar.gz`. Config secrets matching `password / secret / token / credential / api-key / client-id` are replaced with `***REDACTED***` before they enter the bundle. NCC_* env var **names** are listed but **values** are redacted.
- **`ncc-orchestrator v2-status`** subcommand reads `<install-dir>/run/v2-*.pid`, checks each PID is alive, probes `/api/v1/health`, and prints a status table (or JSON with `--json`). Tolerant of missing PID files.
- **`ncc-orchestrator completion bash|zsh|fish|powershell`** generates shell-completion scripts via cobra's built-in generators. Stays in lockstep with the actual subcommand / flag set automatically.
- **`ncc-api-server --health-check`** probe-only mode: connects to `/api/v1/health` using the on-disk token and exits 0/1. Designed for Docker `HEALTHCHECK` and Kubernetes `livenessProbe`; `Dockerfile.api`'s HEALTHCHECK now uses it instead of bundling `wget`.
- **`/metrics` Prometheus endpoint** on the api-server: build info, process uptime, run state + counters (`ncc_runs_triggered_total`, `ncc_runs_completed_total`, `ncc_runs_failed_total`), Go runtime stats, rate-limiter counters. Auth-gated by default; pass `--metrics-public` for token-free scraping on private networks. Content type `text/plain; version=0.0.4; charset=utf-8`.
- **Standalone API / UI server stack-aware mode** via the new `internal/v2layout` package. `ncc-api-server` and `ncc-ui-server` invoked from `<X>/bin/` now auto-resolve `--repo-root`, `--config-path`, `--output-dir`, `--log-dir`, `--token-file-path`, and `--orchestrator-bin` to matching paths inside the extracted stack; symlink-resolved consistently. Pinned by `TestApplyStackAwareDefaults_RespectsExplicitOverrides`, `TestUIExplicitFlagSet`, and the `internal/v2layout` test suite.
- **Helpful subcommand redirect** in `ncc-api-server` and `ncc-ui-server`: invoking with an orchestrator-only positional arg (`update`, `v2-start`, `doctor`, …) now exits 2 with a message pointing to `bin/ncc-orchestrator <subcommand>` instead of silently starting a server.
- **Windows VERSIONINFO** embedded in every Windows binary via `tools/winversioninfo` (uses `github.com/josephspurrier/goversioninfo`). ProductName, FileDescription, CompanyName, LegalCopyright, LegalTrademarks (with non-affiliation disclaimer), and version number all visible in Windows Properties → Details. `.syso` files are gitignored and regenerated each release.
- **Optional code-signing hooks** in `binaryGO.txt`: macOS Developer ID + notarization (`codesign` + `xcrun notarytool`), Windows Authenticode (`osslsigncode`), Linux GPG detached signatures. No-op unless the matching env vars are set. The Windows hook now embeds a publisher description (`-n`) and URL (`-i`) into the signature.
- **Self-signed Windows signing helpers** `scripts/sign-windows.sh` (macOS/Linux build host via openssl + osslsigncode) and `scripts/sign-windows.ps1` (native Windows via `New-SelfSignedCertificate` + `Set-AuthenticodeSignature`). Both apply a real Authenticode signature with the project publisher subject and export a public `.cer` for fleets to import into Trusted Publishers. `docs/SECURITY_AND_TRUST.md` documents what controls the SmartScreen "Publisher" line and how to trust a self-signed cert fleet-wide. Note: a CA-issued (OV/EV) certificate is still required to clear SmartScreen for public distribution.
- **`release-attestation.json`** emitted by `binaryGO.txt`: per-release provenance manifest with product, version, git revision, git dirty, build host OS/arch, project URL, license, non-affiliation disclaimer, and per-artifact SHA-256s.
- **CycloneDX 1.5 SBOMs** generated by `binaryGO.txt` via `cyclonedx-gomod` (one `bom-<binary>.cdx.json` per main package). Drop-in for Trivy / Grype / Dependency-Track. Generation step skips silently when `cyclonedx-gomod` is not installed.
- **`.github/workflows/release.yml`** — tag-driven release pipeline: builds via `binaryGO.txt`, generates SBOMs and `release-attestation.json`, signs every artifact with `actions/attest-build-provenance@v2` (SLSA build provenance), and publishes a GitHub Release. Verifiable with `gh attestation verify`.
- **`docker-compose.yml`** + updated `Dockerfile.api` — one-command stack launch (`docker compose up -d`). UI start is gated by the api-server's HEALTHCHECK, which uses the new `--health-check` mode. Token shared between containers via a named volume; no secret in env vars.
- **`docs/SECURITY_AND_TRUST.md`** documents the entire trust chain: SHA-256 verification, OS first-run guidance (Gatekeeper / SmartScreen / executable-bit), `verify` output schema, Windows file-properties dialog, GPG signature verification, SBOM consumption, and SLSA attestation verification.
- **README "Run on macOS / Windows / Linux (trust & verification)"** section + **"Running individual components (API only / UI only)"** section explain stack-aware standalone launches and OS security bypasses.

### Fixed

- **Windows: shipped `.exe` binaries are no longer reported as "not executable."** `v2-check`, `v2-start`, and the api-server's startup guard tested the Unix executable bit (`mode & 0o111`), which is always 0 on Windows — so `v2-check` failed with `orchestrator-bin / api-server / ui-server binary not executable` and `v2-start`'s api-server child exited 1 with `orchestrator binary is not executable: …\ncc-orchestrator.exe`, even after `Unblock-File`. The executability test now lives in `v2layout.IsExecutable` and is OS-aware: Windows checks the file extension against `PATHEXT` (default `.COM;.EXE;.BAT;.CMD`); Unix keeps the strict `mode & 0o111` check. Pinned by `TestIsExecutable`, `TestHasWindowsExecutableExt`, `TestHasWindowsExecutableExt_RespectsPATHEXT`.
- **`v2-check` / `v2-start` / `v2-stop` / `uninstall` now auto-detect the stack root from the running binary's location.** When invoked from `<X>/bin/<self>` and `<X>` looks like a v2 layout (contains `frontend-dist/` or `bin/ncc-api-server*`), the install-dir defaults to `<X>` instead of `<cwd>/.ncc-v2`. Secondary paths (`--config-path`, `--output-dir`, `--log-dir`, `--token-file`) default to `<install-dir>/<name>`. If `<install-dir>/config.yaml` is missing, falls back to `<install-dir>/example_config.yaml` with a warning. Resolves the user-reported `v2-check failed (5 issues)` when running from inside an extracted stack.
- **`v2-start` against the recommended layout no longer fails with `path escapes repo root`.** The api-server's `--repo-root` (path-traversal sandbox) was hardcoded to `os.Getwd()`. When the user ran `./ncc-orchestrator v2-start` from `<X>/bin/`, repo-root would be `<X>/bin` but the auto-resolved config-path / output-dir / token-file landed under `<X>` — outside the jail. `runV2Start` and `runV2Bootstrap`'s start-script generator now compute repo-root as the install-dir-or-CWD ancestor (the directory that contains both) and pre-resolve macOS `/tmp` → `/private/tmp` symlinks so the api-server's internal EvalSymlinks comparison sees a consistent prefix.
- **`v2-start` `--wait-ready` no longer hangs / times out when binding to a loopback IP.** `localHTTPURLFromListen` was rewriting `127.0.0.1:port` to `localhost:port` for both the wait-ready check and the UI→API backend URL. On macOS `localhost` resolves to `::1` (IPv6) first, but the api-server is bound IPv4-only, so the connection was refused even though the server was healthy. The helper now preserves the user-supplied host. The CORS allow-list separately gains the `http://localhost:port` form when the UI binds loopback so browsers reaching the UI under either name still succeed.
- **`orchestrator_bin` reported by `/api/v1/health` is now an absolute path that exists on disk.** Previously `resolveV2OrchestratorBin` returned `./ncc-orchestrator` (correct relative to the orchestrator's CWD), but the api-server interpreted it relative to its own CWD (= repo-root), producing a non-existent path. API-triggered runs would have failed to spawn the orchestrator. Helper now returns absolute, symlink-resolved paths.
- **Output extractor preserves executable mode bits** ([carry-over from in-flight 2.0.2 work]). `extractTarGzArchive` and `extractZipArchive` now use the mode in the archive header instead of hardcoded `0644`, so binaries inside `ncc-v2-stack-*.tar.gz` and `.zip` come out as `0755` after `update`.
- **Uninstall sweeps both legacy and new layouts.** `uninstall --remove-local` now adds install-dir-relative `outputfiles/`, `nccfiles/`, `promfiles/`, `logs/`, `.ncc-api-token`, `.ncc-api-schedule.json`, `.ncc-api-notifications.json` to the cleanup set in addition to the existing CWD-relative entries. Belt-and-braces for the rare `--remove-v2-runtime=false` path.

### Changed

- **`v2-check` / `v2-start` / `v2-stop` / `uninstall` flag defaults are now empty strings** instead of hardcoded `.ncc-v2` / `config.yaml` / `outputfiles` / `nccfiles` / `.ncc-api-token`. Empty values are resolved at runtime via the auto-detect logic above. Explicit values are honored as before.
- **README** quick-start updated to show the new no-flags flow first, with the explicit-flags form as the legacy / advanced fallback. Two new sections: "Run on macOS / Windows / Linux (trust & verification)" and "Running individual components (API only / UI only)".
- **Project attribution** rewritten across the codebase, embedded metadata, documentation, and `release-attestation.json`: explicit non-affiliation disclaimer ("not affiliated with or endorsed by Nutanix, Inc."), copyright attributed to the maintainer + contributors, license stated as MIT.
- **`Dockerfile.api`** HEALTHCHECK now uses `ncc-api-server --health-check` instead of bundling `wget`. Drops the runtime-image dependency on a curl-equivalent.

### Tests

- New unit tests: `TestResolveV2RepoRoot`, `TestIsPathAncestor`, `TestResolveV2PathToReal_NonExistentSuffix`, `TestLocalHTTPURLFromListen`, `TestLoopbackAltOriginFromListen` (in `goNCC_test.go`); `TestRunVerifyCommand_OutputContract`, `TestSha256OfFile_RoundTrip` (verify subcommand); `internal/v2layout/layout_test.go` (`TestDetectStackRoot`, `TestFindBinary`, `TestConfigPath`); `cmd/ncc-api-server/stack_aware_test.go`, `cmd/ncc-ui-server/stack_aware_test.go`; `tools/winversioninfo/main_test.go` (`TestSplitVersion`).
- Smoke matrix re-run from a fresh extract of `ncc-v2-stack-darwin-arm64.tar.gz`: update v1→v2, legacy `v2-bootstrap` from project root, `v2-start --detach --wait-ready` from `<X>/bin/`, CORS preflight from both `localhost` and `127.0.0.1` origins, UI proxy reachability, `v2-stop`, `uninstall --dry-run`, `--health-check`, `v2-status` (alive / dead-stale-pid), `doctor` (bundle redaction verified, plaintext-secret count = 0), `completion` for all four shells, `/metrics` (Prometheus exposition format with correct content-type), `cyclonedx-gomod` SBOM emit. All pass.

### Upgrade

```bash
# from anywhere a v2.0.0 / v2.0.1 binary lives
./ncc-orchestrator update --allow-major-upgrade
```

The package-level updater introduced in 2.0.1 handles 2.0.2 the same way: download `ncc-v2-stack-<os>-<arch>.tar.gz`, verify SHA-256, extract, and atomically install `bin/`, `frontend-dist/`, `example_config.yaml` over the resolved install-dir.

---

## [2.0.1] - 2026-05-27

Patch release with one substantial behavior change: **`update` now upgrades the v2 stack as a single package, irrespective of which binary (orchestrator, api-server, ui-server, or any renamed variant) was invoked.** All v1.x users should upgrade via this release rather than v2.0.0.

### Changed

- **`update` is now a package-level operation** ([#9]). When the selected release publishes a `ncc-v2-stack-<os>-<arch>.{tar.gz,zip}` archive (true for v2.0.0+), `update` downloads it, verifies the SHA-256 against `checksums.txt`, extracts to a private temp dir, and atomically installs `bin/*` + `frontend-dist/` + `example_config.yaml` into the resolved install directory. The running binary is self-replaced from the canonical-or-basename-matched entry in the extracted `bin/`. Install-dir auto-detection: running from `<X>/bin/<self>` → install over `<X>`; otherwise install into the binary's directory. This makes the upgrade path identical regardless of which binary you ran or how you renamed it. For legacy v1.x releases without a stack archive, falls back to the original single-binary update path.
- **Legacy single-binary selector hardening (fallback path).** `pickAssetForCurrentPlatform` now prefers assets whose name starts with the running executable's basename (e.g. `ncc-orchestrator-*`) before falling back to the legacy first-match behavior. Retained as defense in depth even though the package-archive path is now primary.

### Fixed

- **`v2-bootstrap` / `v2-start` failed against the v2.0.0 stack archives** because the layout-check (`hasBootstrappedV2Layout`) only accepted canonical binary names (`bin/ncc-api-server`), but v2.0.0 stack archives packaged platform-suffixed names (`bin/ncc-api-server-<os>-<arch>`). The lookup helpers now accept either form, so existing v2.0.0 stack archives bootstrap cleanly, and future archives can converge on canonical names.
- **`pickAssetForCurrentPlatform` regression on multi-binary releases** ([#9]) — The original P0: GitHub returns release assets alphabetically, so when v2.0.0 shipped three binaries per platform the v1.x self-updater silently overwrote `ncc-orchestrator` with the api-server binary. Replaced by the package-update flow above; the legacy code path also got the basename-prefix selector fix as a fallback.
- **v2.0.0 release-asset layout hotfix (2026-05-27 19:35Z)** — As an immediate mitigation for users still on v1.x updaters, the 12 standalone `ncc-api-server-*` and `ncc-ui-server-*` assets were removed from the v2.0.0 release; the api-server and ui-server binaries continue to ship inside `ncc-v2-stack-*` archives.
- **macOS `._*` resource-fork sidecars** are no longer included in tarballs (`COPYFILE_DISABLE=1` + `--no-mac-metadata` honored by BSD tar).

### Build / packaging policy

- `binaryGO.txt` step 6 renames binaries inside the stack archive to canonical names (`bin/ncc-orchestrator`, `bin/ncc-api-server`, `bin/ncc-ui-server`) without platform suffix. Matches the v2-bootstrap expected layout.
- `binaryGO.txt` step 7 enforces the no-standalone-server-binaries publishing policy at build time (excluded from `checksums.txt` and therefore from publishable assets). Documented inline with rationale.
- `binaryGO.txt` step 0 exports `LDFLAGS` so the variable survives step-by-step shell invocation.
- Version metadata bumped to `2.0.1` across `VERSION`, `goNCC.go`, `cmd/ncc-api-server/main.go` (default `Version` + OpenAPI `info.version`), `cmd/ncc-mcp-server/main.go`, `helm/ncc-orchestrator/Chart.yaml`, `helm/ncc-orchestrator/values.yaml`, and the three k8s manifests.

### Tests

Seven tests / twenty sub-cases added covering the package selector, install-dir resolution, layout-check naming tolerance, the v2.0.0 regression path, and the legacy-release fallback.

### Migration

- **From v1.x:** Run `ncc-orchestrator update --allow-major-upgrade` against v2.0.1 (or v2.0.0 since the hotfix). The fixed selector picks `ncc-orchestrator-*` correctly.
- **From v2.0.0:** Run `ncc-orchestrator update`. No major upgrade flag needed.
- **From a corrupted v2.0.0 install** (api-server downloaded in place of orchestrator): Re-download `ncc-orchestrator-<os>-<arch>` directly from the v2.0.1 release page or run the recovery `curl` snippet documented in `RELEASE_NOTES_v2.0.0.md` → Known issues.

[#9]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/issues/9

---

## [2.0.0] - 2026-05-05 (release-candidate hardening through 2026-05-27)

### Added (late-cycle hardening, 2026-05)

- **`example_config.yaml`** — Repo-rooted, validator-clean (`ncc-orchestrator validate-config`) reference config that ships in `dist/` and inside every `ncc-v2-stack-*.tar.gz`/`.zip` archive. Uses `secret://NAME` references with `secrets-provider: env` so it works out-of-the-box once `NCC_PASSWORD` (and optionally `SMTP_PASSWORD`, `WEBHOOK_TOKEN`) are exported.
- **`DELETE /api/v1/runs/active`** — Cancel a stuck/in-flight orchestrator run from the UI; returns `409` with structured `error_code` when no run is active.
- **`GET /api/v1/runs/{id}`** — Single archived run metadata + embedded summary, including artifact bodies (`run-summary.json`, `ncc-run-record.json`, `regression-summary.json`, `checks-snapshot.json`, `run-meta.json`); rejects path traversal (`..`/`/`).
- **Header trigger button with live elapsed time** — Top-ribbon Run control now shows a pulsing primary-tinted button with spinning icon and `Running · Xm Ys` elapsed time, mirroring the Settings → Runs indicator.
- **Context-aware Dashboard empty states** — Alerts table now distinguishes "Run in progress" (with live output link), "All clusters clean", and onboarding "No alerts yet" states based on the active-run query and last summary.
- **Schedule input validator** — `action=create` now requires at least one of `cron` or `every`; covered by `TestValidateScheduleInput` table-driven test.
- **Audit + meta route surface** — Audit query params (`limit`, `since`, `source`) are validated server-side with structured `400` on bad input; CORS `Access-Control-Allow-Methods` now includes `DELETE`.

### Security (late-cycle hardening)

- **Go toolchain upgraded 1.26.2 → 1.26.3** — Fixes five Go standard-library CVEs surfaced by `govulncheck`:
  - `GO-2026-4976` ReverseProxy query forwarding (`net/http/httputil`)
  - `GO-2026-4971` `net.Dial` / `LookupPort` NUL-byte panic
  - `GO-2026-4918` HTTP/2 transport infinite loop on bad `SETTINGS_MAX_FRAME_SIZE`
  - `GO-2026-4977` and the related stdlib advisories
  - After upgrade: `govulncheck ./...` → **No vulnerabilities found**.
- **npm DOMPurify override `^3.4.7`** — Pinned via `package.json#overrides` to clear 5 transitive DOMPurify advisories in `monaco-editor` (ADD_TAGS/FORBID_TAGS bypasses, SAFE_FOR_TEMPLATES bypass, prototype pollution, mutation-XSS) without downgrading Monaco; after override: `npm audit --omit=dev` → **found 0 vulnerabilities**.
- **`yaml` patch update** via `npm audit fix` — Closes deeply-nested-collection stack-overflow advisory (`GHSA-48c2-rrv3-qjmp`).

### Frontend / UX (late-cycle hardening)

- **Theme overhaul** — Dark mode rewritten to a near-black neutral charcoal palette; light mode rewritten to a clean zinc palette with crisp white cards; Ant Design and `styles.css` variables aligned through `theme.tsx`.
- **Monaco editor local + themed** — `@monaco-editor/react` now loads `monaco-editor` locally (resolves CSP `script-src 'self'` violation that was blocking the editor from the CDN). Workers bundled via Vite `?worker` imports. Custom Monaco themes (`ncc-light`, `ncc-dark`, `ncc-it-pro`) registered to mirror the app palette; fallback `<textarea>` styled to match.
- **Form/Accessibility cleanup** — Added `id`, `name`, `htmlFor`, `aria-label`, and `autoComplete` across `ConfigSection`, `RunsSection`, `ScheduleSection`, `PolicyGateBuilderSection`, `AuditLogSection`, `LogsSection`, `DashboardPage`, `ApiExplorerSection`, `JsonOutputsSection`, `RawOutputsSection`, and `SecretsMigrationModal`; trigger-run password field is now wrapped in a real `<form>`; eliminates the DOM warnings for unassociated labels, missing `id`/`name`, password-not-in-form, and missing autocomplete.
- **Runs table redesign** — Replaced often-blank "Index" column with `Type`, `Status`, `Duration`, `Clusters`, and `Issues` columns; cells degrade to `—` for trigger entries.
- **Sparkline correctness** — Fixed timezone bug in `RecentRunsSparkline` (mixed UTC and local dates) via `localDateKey` helper; filtered to count only `history`/`summary` sources so trigger events no longer inflate the "runs in last 7 days" count.

### Build / Release

- **`binaryGO.txt` overhaul** — Preflight checks, consolidated variable declarations, local-arch symlinks, example-file copy, cleaner tar/zip archives, and end-to-end verification (`api/health` filter, version assertion).
- **Production verification (2026-05-27)** — `govulncheck ./...` clean, `npm audit --omit=dev` clean, `go test -race -count=1 ./...` clean, `gofmt -l .` clean, `go vet ./...` clean, `tsc --noEmit` clean, `vite build` clean.

### Added

- **Full v2 application stack in 2.0.0** — Release scope now explicitly includes API (`cmd/ncc-api-server`), UI (`cmd/ncc-ui-server` + `frontend`), and UI-integrated API proxy surface for `/api/v1/*`.
- **Build-from-scratch documentation** - Added `docs/BUILD_FROM_SCRATCH.md` with full setup flow for clean machines, including local build, frontend build, v2 stack startup, tests, packaging, and Kubernetes verification.
- **Release validation suite for v2.0.0** — Production checks and edge-case verification documented with reproducible command evidence.
- **CodeQL workflow alignment** — Added repository workflow that analyzes only `go` and `actions`, preventing JS/TS language-detection failures in this branch scope.
- **v2.0.0 release documentation set** — Added release notes, production-readiness checklist, and milestone summary documents for the v2 train.
- **Build and handover guides** — Added `docs/BUILD_FROM_SCRATCH.md` and `docs/ARCHITECTURE_AND_HANDOVER.md` for end-to-end onboarding, operations, and ownership transfer.
- **Clean v2 uninstall tooling** — Added `scripts/uninstall-v2-clean.sh` as the canonical Kubernetes/runtime cleanup entrypoint; legacy `scripts/uninstall-ncc-orchestrator.sh` now delegates to it.
- **CLI local uninstall command** — Added `ncc-orchestrator uninstall` for standalone local cleanup of artifacts/state created by the binary.
- **`v2-check` preflight command** — Added lightweight v2 runtime self-check for binary executability, config/path readiness, directory writability, and API/UI port bind availability before `v2-start`.
- **Scheduler health API** — Added `GET /api/v1/schedule/health` to expose scheduler runtime hints (`last_run`, `last_success`, `last_error`) and lock/log path metadata.

### Changed

- **Version baselines** — `VERSION`, default `main.Version`, Helm chart/appVersion, Helm values image tag, and Kubernetes manifest image tags aligned to `2.0.0`.
- **README release pointers** — Updated current release status links to `v2.0.0` documents and clarified current branch scope.
- **Documentation alignment across v2 docs** - Updated README, CONTRIBUTING, migration guide, and v2 architecture doc for consistent source-build and deployment instructions.
- **Kubernetes architecture docs** - Clarified API runner binary staging model (runner image -> API init container -> shared tools path) in design docs and release notes.
- **Kubernetes runtime model** — API now stages the runner binary via init container (`runner image -> /tools/ncc-orchestrator`) and validates `--orchestrator-bin` at startup (exists + executable).
- **Production readiness manifests** — Added startup/readiness/liveness probes to API and UI deployments to improve rollout safety and self-healing behavior.
- **Preflight probe handling** — `.ncc-preflight-check` moved to a persistent read/write sentinel approach (no create/delete churn) with legacy typo cleanup.
- **Performance and reliability optimizations** — Cached exclude-title matchers/regex, reduced config re-parse and allocations in secret validation, de-duplicated replay metadata parsing, and eliminated redundant Prometheus writes during replay.
- **Console error UX** — Consolidated duplicate startup error logging to a single user-facing error path.
- **Scheduler overlap safety** — `create-schedule` now supports lock-enabled cron generation with `--with-lock` (default true) to prevent overlapping scheduled runs.
- **Security response handling** — Added redacted-safe config response content and startup warning for plaintext config passwords.
- **Logs UX operability** — Added UI `Follow tail` and `Jump to latest` controls for Runner Logs and Live Logs.

---

## [1.1.0] - 2026-04-21

### Added

- **`create-schedule` command** — New `ncc-orchestrator create-schedule` subcommand to create recurring NCC runs:
  - **Linux/macOS:** creates or updates a cron entry (`--type cron`, `--cron` or `--every`)
  - **Windows:** creates or updates a Scheduled Task (`--type windows`, `--every`)
  - Supports safe preview mode with `--print-only` (default true).
  - New scheduler actions: `--action list|remove|run-now`.
- **MCP scheduler tools** — New MCP tools `create_schedule`, `list_schedules`, and `delete_schedule` for schedule lifecycle operations from AI clients.
- **Run history snapshots** — Optional `--run-history` writes timestamped snapshots under `--run-history-dir` with retention controls (`--retain-last`, `--retain-days`).
- **Regression awareness** — New `regression-summary.json` compares current FAIL counts to previous `run-summary.json`; `--notify-on-regression` only sends notifications when FAIL count increases.
- **Drill-down diff and snapshots** — Added `checks-snapshot.json` and `drilldown-diff.json` with per-check change tracking (new/resolved FAILs, severity changes).
- **Flaky check detection** — Added `flaky-checks.json` with lookback-based severity transition analysis (`flaky-lookback-runs`, `flaky-min-transitions`).
- **Health and SLO exports** — Added per-cluster `health_score` plus `avg_health_score` / `min_health_score` in `run-summary.json`, and `slo-dashboard.json` for dashboards.
- **SARIF export** — New per-cluster output format `sarif` in `--outputs`.
- **Config schema + validation mode** — New `config-schema` and `validate-config` commands for config ergonomics in CI/CD.
- **Policy gates** — Added `policy-gates` support for release/CI rules such as `new-fails>0` or `min-health-score<90`, with violations written to `policy-gates.txt`.
- **Secrets provider** — Added `secret://` resolution for sensitive fields using `secrets-provider=env|file` and optional `secrets-file`.
- **Quiet hours and maintenance windows** — Added notification suppression via `quiet-hours` and `maintenance-windows`.

### Changed

- **ncc-run-record metadata** — Added `git_revision`, `hostname`, and `scheduler_source` for better traceability.
- **Adaptive resilience on 429** — Effective cluster concurrency now adapts down/up based on HTTP 429 pressure (`--adaptive-parallelism`).
- **Single-file report option** — `--single-report` writes `ncc-report-single.html` alongside `index.html`.
- **Strict validation by default** — Config validation now rejects unknown keys and enforces strict value typing by default.

---

## [1.0.0] - 2026-04-07

### Added

- **Nutanix API v4 (default)** — Cluster discovery, start-checks, and Prism task polling use v4 paths where applicable; configurable `nutanix-v4-api-version` (e.g. `v4.2`). Legacy v3 remains available via `ncc-api-version: Legacy` (alias `v1`).
- **Cluster resolution for v4** — `GET .../config/clusters` matches `--clusters` to the correct registered entity (name, extId, external address, or CVM IP); `nodeIps` for run-system-defined-checks come from that cluster’s CVMs (fixes Prism Central multi-cluster / NCC-40023 cases).
- **run-summary.json** — Per-cluster `clusters[]` with `ok`, severity counts, `checks_total`, and `error`; `exit_code` mirrors process exit (0 = all OK, 1 = all failed, 3 = partial success).
- **Exit code 3** — When at least one cluster succeeds and at least one fails, the process exits **3** (partial success). See README.
- **Semver for `--update`** — Version comparison uses [Masterminds/semver](https://github.com/Masterminds/semver); fallback to legacy parse if non-semver.
- **docs/TROUBLESHOOTING.md** — Common TLS, multi-cluster, and API issues.
- **ncc-run-record.json** — Versioned bundle (`schema_version`, orchestrator version, full `run` summary) for pipelines.
- **discover-clusters `--format`** — `lines` (default), `table`, or `json` (name, ext_id, address, api).
- **HTTP rate limits** — Logs `X-RateLimit-*` / `X-Api-Ratelimit-*` on 429; caps long `Retry-After` waits.
- **Helm chart** — `helm/ncc-orchestrator` for templated CronJob image and schedule.
- **Kustomize** — `k8s/kustomization.yaml`; apply with `kubectl apply -k k8s/`.
- **Tests** — Golden CSV and per-cluster HTML fixtures, `httptest` retry for 429, KB markdown helper in `internal/kblinks`.
- **MCP `get_report`** — For `*.log` files, turns `KB NNNN` into markdown links to portal.nutanix.com.

### Changed

- **Version and images:** VERSION set to 1.0.0; Kubernetes CronJob and job-debug use image tag 1.0.0. First stable semver release.
- **Partial multi-cluster runs** — Previously exited **0** when some clusters failed; now exits **3** so automation can detect partial failure.

### Fixed

- **Viper / discover-clusters** — Duplicate `BindPFlag` for `insecure-skip-verify` (and related keys) no longer overrides root flags; `--insecure-skip-verify` on the main command is honored.

---

## [0.1.13] - 2026-03-02

### Added

- **MCP: `list_run_artifacts` tool**  
  List files in an NCC run output directory (run-summary.json, index.html, per-cluster .log/.html/.csv). Optional `output_dir` (default `outputfiles`).

- **MCP: `get_report` tool**  
  Read the aggregated index.html or a specific cluster report file. Optional `output_dir`, `file` (e.g. `index` or `10.0.0.1.html`). Large reports are truncated for context.

- **MCP: Resources**  
  - `ncc://run-summary` — latest run-summary.json (application/json).  
  - `ncc://report` — latest aggregated index.html (text/html).  
  Clients can list and read these via MCP resources/list and resources/read.

- **Documentation**  
  - docs/MCP_SERVER.md: new tools and resources table, “Possible future additions” section.  
  - MCP tool descriptors for list_run_artifacts and get_report.

### Changed

- **Version and images:** VERSION set to 0.1.13; k8s CronJob and job-debug use image tag 0.1.13.

### Fixed

- None explicitly tracked for this release.

---

## [0.1.12] - 2026-02-12

### Added

- **Digest notifications**  
  New option `--notify-digest` (config: `notify-digest`) sends **one email, one webhook, and one Slack message per run** with a run overview (clusters OK/failed, duration) instead of per-cluster notifications. Optional attachment of the aggregated `index.html` to the digest email when `email-attach-html` is set; optional base64-encoded index in the webhook payload when `webhook-include-html` is set.

- **Run summary log**  
  At the end of each run, a single structured log line is emitted with `clusters_ok`, `clusters_failed`, `duration_s`, and `index_html` path for easier parsing and monitoring.

- **Notification retries**  
  Email and webhook sends are wrapped in a retry loop (3 attempts with jittered backoff using `retry-base-delay` / `retry-max-delay`) to improve reliability on transient failures.

- **Replay + HTML attach**  
  In replay mode, per-cluster HTML (and CSV) is generated **before** sending notifications. When `email-attach-html` is set, the generated HTML file is attached to the replay email; when `webhook-include-html` is set, the replay webhook payload includes the report as base64.

- **Config path validation**  
  `validateConfig` now requires non-empty (after trim) values for `output-dir-logs`, `output-dir-filtered`, `log-file`, and `prom-dir`, with clear error messages.

- **HTTP log redaction**  
  When `log-http` is enabled, request/response dumps redact the `Authorization` and `Cookie` headers, and mask JSON values for `"password"` / `"Password"` fields to avoid leaking secrets in logs.

- **Documentation**  
  - README: HTTP pool/timeouts description, example webhook payload, testing email and webhook (webhook.site, MailHog, Mailtrap), `--version` behavior, `notify-digest` and env vars.
  - k8s/README: Runbook for CronJob failures (logs, job-debug, NFS permissions, prune images).
  - k8s ConfigMap: `notify-digest: false` option added.

- **Unit tests**  
  New/updated tests for `validateConfig` (path validation), `checkOutputPermissions`, digest overview format, and `ParseSummary` (no-block and check-name cases).

### Changed

- Per-cluster email/webhook/Slack are **skipped** when `notify-digest` is true; digest notifications are sent after the aggregated HTML is written.
- Replay mode uses `sendEmailWithRetry` and `sendWebhookWithRetry` for consistency with the main run.

### Fixed

- None explicitly tracked for this release.

---

## [0.1.11] and earlier

See git history and [Releases](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases) for prior versions.

---

[2.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v2.0.0
[1.1.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.1.0
[1.0.0]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v1.0.0
[0.1.13]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.13
[0.1.12]: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v0.1.12
