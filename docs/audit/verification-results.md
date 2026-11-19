# Verification results

Environment: Windows 11 Pro (10.0.26300), Go 1.27.4 windows/amd64, Git Bash.
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

## Delta audit, 2026-11-19 (branch v2.4.0 at ea76376c)

Run from the `.claude/worktrees/v2.4.0` worktree; no source code changed.

| Check | Result |
| --- | --- |
| `go build ./...`, `go vet ./...` | exit 0 |
| `go test ./...` | exit 0 (no `-race`: no gcc on this host) |
| Frontend `tsc -p tsconfig.app.json --noEmit` | exit 0 |
| Frontend `vitest run` (v5.1.0) | 48 files, 396 tests, all passed |
| `govulncheck ./...` | 1 reachable: GO-2026-6909 in `crewjam/saml` v0.6.3, no fixed version (see DEP-03). 1 module-level advisory in `x/crypto/openpgp`, not called |
| `osv-scanner` on `go.mod` and `frontend/package-lock.json` | 300 packages; only GO-2026-6909; 0 npm advisories |
| `gitleaks detect` (full history, 264 commits) | no leaks |

Not run: `go test -race`, end-to-end/browser tests, axe-core accessibility
checks (UX-10 still open), performance measurements, container image scan.
Reviewed by reading: `cmd/ncc-orchestrator/retention.go` (raw-log retention
skips symlinks, the newest log, logs of clusters in the latest report, and does
nothing when the log folder is the report folder); no defect found.

### Re-audit additions, 2026-11-19

| Check | Result |
| --- | --- |
| `golangci-lint` v2.15.0 `run ./...` | 0 issues |
| `gosec` v2.30.0, high/high, G703 excluded (CI flags) | exit 0 |
| `go build`, `go vet`, `go test ./...` after changes | all exit 0 |
| `vite build` | built in 12.6 s; initial JS 1,305,862 B raw (about 428,543 B gzip), CSS 116,497 B raw |
| New test | `TestLDAPUserBindIsOperational` passes |

Code changed (uncommitted): `cmd/ncc-api-server/ldap.go`, `auth.go`, `backups.go`, `freshness.go`, `versionskew.go`, new `ldap_bind_test.go`. The frontend suite was not re-run after these Go-only changes. Still not run: `-race`, e2e, accessibility, live LDAP, load or latency tests.

### Tools installed and extra checks, 2026-11-19

Installed with the user's go-ahead: WinLibs gcc 16.2 and Helm 4.3.2 and Trivy (winget); `kubeconform`, `kube-linter` (`go install`); `axe-core` 4.14.1 (npm dev dependency, MPL-2.0).

| Check | Result |
| --- | --- |
| `go test -race -count=1 ./...` | exit 0, no data races |
| Frontend `tsc` and `vitest run` | tsc exit 0; 50 files, 402 tests pass (6 new axe tests) |
| `npm audit` | 0 vulnerabilities |
| `helm lint` / `helm template` / `kubeconform -strict` | chart lints; 17 resources valid (before and after the K8S-01 fix) |
| `kube-linter` on the rendered chart | 21 findings before the fix (see below); the missing `runAsNonRoot` finding is gone after it |
| `trivy fs` (vuln, secret, misconfig; HIGH and CRITICAL) | 0 vulnerabilities, 0 secrets; misconfigurations: `readOnlyRootFilesystem` unset (KSV-0014, 11), default security context in Helm API (KSV-0118, fixed), placeholder `secret://` reference in `k8s/configmap.yaml` (KSV-0109, a reference not a secret) |
| Live probe: API built from this tree, run locally (hybrid auth, scratch dir) | see security-assessment.md |
| Latency (40 sequential requests each, empty install, token auth, localhost) | `/health` p50 21 ms p95 23 ms; `/freshness` p50 7 ms p95 9 ms; `/report/data` p50 18 ms p95 22 ms; `/settings/clusters` p50 4 ms p95 5 ms |

New log line confirmed live: `sign-in failed` with request ID, user, client IP and lock state.

Still not run: Docker image build and image scan (Docker Desktop daemon not running), browser colour-contrast and keyboard checks, live LDAP, load testing at fleet scale, deployment to a cluster.

### Final run, 2026-11-19 (after all changes in this session)

| Check | Result |
| --- | --- |
| `go build`, `go vet` | pass |
| `golangci-lint` v2.15.0 | 0 issues |
| `gosec` high/high (CI flags) | exit 0 |
| `go test -race -count=1 ./...` | pass |
| `govulncheck` | only GO-2026-6909 (reviewed false positive) |
| `helm lint`, `helm template`, `kubeconform -strict` | pass, 17 resources valid |
| Frontend `tsc`, `vitest` | tsc clean; 51 files, 403 tests pass |
| `npm audit` | 0 vulnerabilities |
| axe-core in a real browser, 16 routes x 3 themes | 0 violations (empty-data pages) |
| kind deployment of the fixed chart | API pod Running, health and readiness OK |
| `zizmor` | 1 finding left (`workflow_run` in pages.yml, reviewed) |

Changed files outside `docs/audit`: Go (`ldap.go`, `auth.go`, `backups.go`, `freshness.go`, `versionskew.go`, `main.go`, `healthpaths.go` and tests), `helm/.../api.yaml`, `deploy/windows/setup-ncc.ps1`, two workflows, frontend components/theme/styles/tests, `frontend/package.json` and lockfile (axe-core), `go.mod`/`go.sum` (test-only dependencies). Nothing committed.

Still not run: runner image build; NetworkPolicy enforcement; a real multi-replica RWX install; Windows full installer run; Linux systemd install; a screen reader; populated-data accessibility scan; large-fleet load test.

### Windows installer test, 2026-11-19

Ran `setup-ncc.ps1` elevated against a locally built stack archive (see security-assessment.md, "Windows installer, end to end"). Install, ACLs, SYSTEM scheduled tasks, firewall rules, health and readiness, `doctor`, encrypted backup, verify-only (including wrong passphrase and a corrupted file), restore, refusal to overwrite a live install, and task stop/start recovery all behaved as described there. Two installer defects (WIN-04, WIN-03) and one config-editing defect (COR-09) were found and fixed; after the fixes the same sequence passed with a valid generated config. After the fixes: `go build`, `go vet`, `golangci-lint` (0 issues), `gosec`, `go test -race ./...` all pass. Not run: a reboot, Sysprep/Unattend, upgrade from an older release.

### Environment tests run by Cursor, 2026-11-19

Run by Cursor on a multi-node Cilium cluster (Kubernetes 1.35, throwaway namespace) and a Rocky Linux 10 host (SELinux enforcing, firewalld), from the prompt that was in this folder. Its prompt and result files were removed after the fixes below; the outcome is kept here.

* **Passed:** default two-replica install with RWX storage (the chart works once a storage class is set); K8S-02 and K8S-01 regressions (no restarts, uid 1000); a session cookie issued by one replica accepted by another; deleting one API pod; RBAC matches the chart README and Schedule pause/cron change work; NetworkPolicy (API blocked from unrelated pods, allowed from the UI); runner version skew returns HTTP 409 `NCC_API_VERSION_SKEW` with the new log line; rolling restart with 82 of 82 probes OK; PrometheusRule accepted; `trivy image` on the runner image 0 HIGH/CRITICAL; 500 and 2,000 cluster data sets (500 clusters through a port-forward: `/report/data` p50 185 ms, p95 517 ms, about 76 req/s; `/freshness` p50 48 ms, 310 req/s, no 5xx or 429); API and UI ran with `readOnlyRootFilesystem` plus a `/tmp` emptyDir (health, encrypted backup, verify and support bundle worked); checksum verification, SELinux enforcing with no AVC denials, firewalld ports, and `doctor --fix` log sizes on the host.
* **Failed, now fixed:** ENV-01 (HTTP 500 on a store conflict), ENV-02 (stale user list), ENV-03 (CronJob `mkdir logs`), ENV-04 (no PVC storage class setting), ENV-05 (backup key undocumented), ENV-06 (tar ownership), ENV-08 (no log maintenance timer). Fixes are covered by unit and manifest tests; none was re-run on the cluster or host.
* **Not run, still open:** drain of a node (production workers), 7,000 clusters and pod memory (metrics-server down), restore, retention preview and a successful run under read-only root, egress NetworkPolicy, second-directory restore, reboot and `kill -9` of supervisor children, LDAP and LDAPS, OWASP ZAP and the authenticated role matrix on that host, and all browser checks (these last two were covered on Windows in this audit). A run through the API with the fake Prism failed the secret preflight; the CronJob now skips it like `k8s/` does, and the API-triggered case was not reproduced here.
