# Testing strategy and quality gates

## Current suite

| Layer | Tooling | Size | Notes |
| --- | --- | --- | --- |
| Go unit/integration | `go test` | 476 test functions at baseline → 499 now | Includes golden-file report tests, httptest handler tests, e2e tests over the real API handler stack (`e2e_test.go`, `clustergroups_e2e_test.go`), fuzz corpus for JSON password redaction |
| Go race | `go test -race` (CI, Linux) | whole module | Not runnable on the audit machine (no C compiler for cgo) |
| Frontend | Vitest + Testing Library + jsdom | 11 files, 45 → 49 tests | API client, run status, alert lifecycle, report utils, a few components/pages |
| Static analysis | golangci-lint v2 (staticcheck all), `go vet`, `gofmt`, `tsc -b` | | 0 lint issues |
| Security | govulncheck, `npm audit --omit=dev`, Trivy fs, CodeQL | | |

## Gaps found

1. **No Windows execution in CI** although Windows is a supported install
   target; 13 tests failed on Windows at baseline, 3 of them hiding real
   product defects (COR-01, COR-03, COR-06). → Windows job added.
2. **Two contracts were untested** (API allowlist ↔ runner flags; client
   timeouts ↔ server timeouts). → Contract test and timeout tests added.
3. **A test asserted the bug:** `TestSanitizeExtraArgs` treated
   `--output-dir … --gen-test-agg` as safe. → Rewritten against real flags.
4. **Frontend coverage is thin** around the main pages (Dashboard, Insights,
   Settings sections) — mostly utilities are tested.
5. **No browser end-to-end tests** for the critical workflows.
6. **A timing-sensitive test** (Monaco dynamic import under a 5 s default)
   failed in a cold full run. → Given an explicit 30 s budget; it passes in ~2 s.

## Tests added in this audit

| Test | Covers |
| --- | --- |
| `internal/procutil`: `TestExistsSelf`, `TestExistsExitedChild`, `TestExistsRunningChild`, `TestExistsRejectsNonPositive` | Cross-platform liveness (COR-01) |
| `TestWindowsImageIdentity` | Pid reuse detection on Windows |
| `TestDeletedAccountSessionsAreRejected`, `TestRecreatedAccountDoesNotAcceptOldSessions`, `TestDeletedAccountTombstonePersistsAndPrunes` | SEC-01 (first one fails on the baseline code) |
| `TestForgotPasswordAdminSelfResetOffByDefault`, updated e2e `TestEndToEndAdminForgotPasswordSelfReset` | SEC-03 new default and opt-in |
| `TestSanitizeExtraArgs` (rewritten), `TestExtraArgsAllowlistMatchesRunnerFlags`, `TestConfineExtraArgPaths` | COR-02, SEC-02 |
| `TestExtendDeadlinesOutlastsServerWriteTimeout`, `TestServerWriteTimeoutCutsLongHandlerWithoutExtension` | REL-01 (first fails without the gzip `Unwrap`) |
| `TestSendMailTimesOutOnStalledServer`, `TestSendMailRejectsCRLFInEnvelope`, `TestSendMailRequireTLSWithoutSTARTTLS`, `TestSendEmailFlattensSubjectAndSkipsAuthWithoutUser` | REL-02, SEC-06, COR-05 (fake SMTP server) |
| `TestSendWebhookJSONRejectsMalformedURL` | COR-04 |
| `TestServeUntilDrainsInFlightRequest`, `TestServeUntilReturnsServeError` | REL-03 |
| MCP: `TestWithPasswordEnvReplacesAndOmits`, `TestCreateScheduleRefusesCustomCommandByDefault`, `TestScheduleCommandAllowedOptIn` | SEC-04, SEC-05 |
| Vitest: long-operation timeouts (preflight, backup outlive 30 s; ordinary calls still time out) | REL-01 client side |
| Vitest: `healthPillValue` | UX-01 |

No assertion was weakened. Seven portability fixes changed how tests build
inputs (OS-absolute paths, `%q` for YAML strings, LF-pinned golden files) or
skip platform-specific cases (systemd units on non-Linux; symlinks without the
privilege) — each with a comment explaining why.

## Recommended next tests (prioritized)

| Priority | Test | Why |
| --- | --- | --- |
| P1 | Multi-replica user-store test: two `userDB` instances on one backend, interleaved revoke/`touchTokenLastUsed`, assert the revocation survives | Guards the DATA-01 fix |
| P1 | Restore failure injection (size cap on the n-th file) asserting rollback to a pre-restore snapshot | Guards the DATA-02 fix |
| P2 | Playwright e2e against docker-compose: login → trigger run (stub runner) → Active Runs → report → acknowledge alert → logout; run on Chromium in CI | Critical workflow regression |
| P2 | axe-core checks in Vitest for Dashboard, Insights, Settings → Access | Accessibility regressions |
| P2 | Authorization matrix test generated from `apiRouteCatalog()`: for each route × role, assert 401/403/2xx per `routeMinRoleFor` | Broken access control regressions |
| P3 | Load test (k6) for `/report/data`, `/runs`, `/alerts/ncc-dispositions` with a large history | Capacity evidence |

## Quality gates (recommended for `main`)

Blocking: Go build/vet/gofmt; `go test -race` (Linux); `go test` (Windows);
golangci-lint; govulncheck; Vitest; `tsc -b`; `vite build`;
`npm audit --omit=dev`; Trivy (HIGH/CRITICAL, fixed only).
Advisory: full `npm audit`; CodeQL; gitleaks (to add).
Branch protection should require the blocking jobs and one review.
