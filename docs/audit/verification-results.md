# Verification results

Environment: Windows 11 Pro (10.0.26300), Go 1.27.3 windows/amd64, Git Bash.
Node: no standalone Node/npm installed; frontend commands ran on the Node
**v24.18.1** runtime embedded in the Cursor editor (`ELECTRON_RUN_AS_NODE=1`)
against the existing `frontend/node_modules`. CI pins Node 26.12.1, so frontend
results should be re-confirmed in CI. No Docker daemon, no C compiler (so no
`-race`), no Prism clusters. Network access to proxy.golang.org, vuln.go.dev,
the npm registry and osv.dev was available.

Every result below is from a command actually executed in this audit. Nothing
is reported as passing that was not run.

## Baseline (HEAD `b8e47e6d`, before changes)

| Check | Command | Result |
| --- | --- | --- |
| Build | `go build ./...` | pass |
| Vet | `go vet ./...` | pass |
| Format | `gofmt -l` on tracked files | 151 files listed: **checkout artifact**, not real drift — the Windows checkout uses CRLF (`core.autocrlf=true`); `git show HEAD:goNCC.go \| gofmt -l` is clean |
| Go tests | `go test -count=1 ./...` | **FAIL — 13 tests**: `TestGenerateCSVMatchesGolden`, `TestGenerateHTMLMatchesGolden`, `TestResolveUnderBase`, `TestBuildPreflightReportPathPermissionFailure`, `TestBuildPreflightReportIncludesSecurityWarnings`, `TestSystemdApplyUpdateWatcherUnits`, `TestRelativeAndAbsolutizeOutputDirKeys`, `TestValidateSecretsFileHardening` (root package); `TestExternalActiveRunsLiveAndStale`, `TestNormalizeConfigOutputDirPaths`, `TestPostRestoreSelfHealIncludesConfigOutputDirNormalizationNote`, `TestHealthDoesNotDiscloseInternalsToAnonymousCallers`, `TestNormalizeAndConfinePathRejectsSymlinkEscape` (api-server). All other 15 packages pass. |
| Lint | golangci-lint v2.15.0 on a clean HEAD worktree | 0 issues |
| Go vulns | govulncheck (latest) | 0 reachable; 1 module-level (GO-2026-5932, package not imported) |
| Deps (all) | osv-scanner v2 (go.mod + package-lock.json) | 12 fixable advisories, all npm dev tooling |
| Type-check | `tsc -b` | pass |
| Frontend tests | `vitest run` | 44/45 — `MonacoCodeEditor.test.tsx` timed out at 5 s in the cold full run; passes alone in ~2.4 s |
| Frontend build | `vite build` | pass (15.5 s); entry 440 kB / 134 kB gzip |

## Final (working tree after changes)

| Check | Result |
| --- | --- |
| `go mod verify` | all modules verified |
| `go build ./...` | pass |
| `go vet ./...` (windows, `GOOS=linux`, `GOOS=darwin`) | pass on all three |
| `GOOS=linux go test -c` for every package | all compile |
| gofmt (CR-stripped) on every changed/new Go file | clean |
| `go test -count=1 ./...` (Windows) | **18/18 packages pass, 0 failures** (was 13 failing tests). 11 skips: 9 pre-existing Unix-only tests plus `TestSystemdApplyUpdateWatcherUnits` (Linux-only) and `TestNormalizeAndConfinePathRejectsSymlinkEscape` (needs the Windows symlink privilege) |
| golangci-lint v2.15.0 (windows and `GOOS=linux`) | 0 issues on both |
| govulncheck | 0 reachable (unchanged) |
| osv-scanner | 12 advisories, all npm dev tooling — **open, blocked on npm CLI** (see dependency-health) |
| gitleaks v8, full history (164 commits) | 10 matches, all false positives (test placeholder key, prose) |
| `tsc -b` | pass |
| `vitest run` | **49/49** (11 files) |
| `vite build` | pass |
| `go test -race` | **not run** — no C compiler for cgo on this machine; runs in CI on Linux |

## Red/green evidence for fixes

| Fix | Proof |
| --- | --- |
| SEC-01 session revocation | `TestDeletedAccountSessionsAreRejected` fails with the `auth.go` change stashed (`JWT after delete: want 401, got 200`), passes with it |
| REL-01 deadlines | `TestExtendDeadlinesOutlastsServerWriteTimeout` fails with the `compress.go` `Unwrap` stashed (`EOF`), passes with it; control test shows the cut-off without the extension |
| COR-01 Windows liveness | `TestExternalActiveRunsLiveAndStale` failed at baseline on Windows, passes now; `TestCheckStalePIDs` exercises a live unrelated pid |
| COR-02 run args | Against a freshly built runner, 8/10 old allowlisted flags exit with "unknown flag"/"needs an argument"; all 7 new flags parse (`Output formats: [csv json]`, "All settings validated successfully") |

## Live smoke test (built binaries)

`ncc-api-server` (token mode) + `ncc-ui-server` serving the production
frontend build, driven in the built-in browser:

* `GET /api/v1/health` → `ok`, version 2.2.1.
* `POST /runs/trigger` with `--no-html` → **400** "extra arg --no-html is not a
  runner flag (use --outputs csv,json to skip per-cluster HTML)".
* `--prom-dir ../../outside` → **400** "path escapes repo root".
* `--outputs csv,json --dry-run` → **202**; the runner's live output shows the
  flags parsed and the configuration validated.
* Runs page placeholder shows `e.g. --outputs csv,json --dry-run`.
* Dashboard on an empty install: health pill renders "—" with
  `aria-label="Health: no completed checks yet"` (was red "0.0%").
* First-time setup: all step titles render on one line at 1366 px; vertical
  list without horizontal overflow at 375 px.
* Console: the only errors were the two deliberate 400s and a pre-existing
  404 on `/runs/config-preference` in token mode (UX-08).

## Requires manual or environment validation

* Windows CI job on GitHub-hosted runners (new; not yet executed there).
* Multi-replica Kubernetes behaviour (tombstones are shared through the user
  store; DATA-01 remains open).
* Graceful shutdown under a real SIGTERM (unit-tested with a cancelled
  context; Windows has no SIGTERM, so only Ctrl-C/service stop apply there).
* SMTP against a real relay with STARTTLS (fake-server tests cover timeout,
  RequireTLS, header encoding, no-AUTH relay).
* Frontend on Node 26.12.1 (CI).
* npm dev-dependency updates and a clean `npm audit`.
