# Code quality and correctness

Severity: Critical / High / Medium / Low. Priority: P0–P3. Confidence:
**verified** (reproduced or proven by a failing test), **strongly suspected**
(follows directly from the code, not reproduced), **unverified**.

Security-specific findings are in [`security-assessment.md`](security-assessment.md);
data-integrity findings in [`data-reliability-and-integrity.md`](data-reliability-and-integrity.md).

## COR-01 — Windows: every live process was reported dead · High · P1 · verified · fixed

* **Where:** `goNCC.go` `processIsAlive`, `waitForPIDExit`;
  `cmd/ncc-api-server/externalruns.go` `processAlive`.
* **Evidence:** both used `os.FindProcess` + `Process.Signal(syscall.Signal(0))`.
  On Windows `Signal` supports only `Kill` and returns an error for any other
  signal. The baseline test `TestExternalActiveRunsLiveAndStale` failed on
  Windows with "expected 1 external run, got 0" for the test's *own* pid. The
  runner's comment claimed it "assumes alive on Windows"; the code did the
  opposite. `waitForPIDExit` on Windows simply slept the whole timeout and
  returned "still running".
* **Impact (Windows installs):**
  * `v2StackRunning` always false → `v2-restore` could overwrite a running
    install's config/user store/binaries;
  * supervisor single-instance guard ineffective → two supervisors fighting
    over ports and pid files;
  * `v2-stop` always waited `stopTimeout` and then force-killed;
  * the api-server deleted the heartbeat file of every running scheduled run,
    hiding it from Active Runs;
  * self-heal treated every pid file as stale.
* **Fix:** new `internal/procutil` (`OpenProcess` + zero-timeout
  `WaitForSingleObject` on Windows; signal 0 with EPERM-as-alive elsewhere),
  used by all callers. Windows pid identity now compares the executable image
  name (`QueryFullProcessImageName`), so stale pid files whose pid was reused
  are still detected (shell-hosted helper loops stay "unknown", as before).
* **Tests:** `internal/procutil` (self, exited child, running child),
  `TestWindowsImageIdentity`, and the previously failing
  `TestExternalActiveRunsLiveAndStale` / `TestCheckStalePIDs` now pass on Windows.

## COR-02 — Run extra-args allowlist did not match the runner · High · P1 · verified · fixed

* **Where:** `cmd/ncc-api-server/hardening.go` `sanitizeExtraArgs`; samples in
  `RunsSection.tsx` (placeholder), `ApiExplorerSection.tsx`, OpenAPI catalog.
* **Evidence:** against a freshly built runner, 8 of the 10 allowlisted flags
  exit with an error:
  `--no-html`, `--output-dir`, `--log-dir`, `--cluster-file`, `--compare-with`,
  `--maintenance-window` → `unknown flag`; `--quiet-hours`, `--gen-test-agg`
  (allowlisted as value-less) → `flag needs an argument`. The UI placeholder
  suggested `--no-html --output-dir outputfiles` and the API Explorer / OpenAPI
  samples used `--no-html`, so following the product's own guidance produced a
  failed run. The existing test asserted the broken pair as "safe args".
* **Fix:** allowlist rebuilt from flags that exist on the runner root command
  (`--clusters`, `--prom-dir`, `--outputs`, `--severity-filter`,
  `--quiet-hours`, `--maintenance-windows`, `--dry-run`); retired names return
  400 with an actionable hint; samples updated; path-valued values confined
  (SEC-02).
* **Tests:** `TestSanitizeExtraArgs` (rewritten with real flags and retired
  names), `TestExtraArgsAllowlistMatchesRunnerFlags` (parses `goNCC.go` so the
  two cannot drift again), `TestConfineExtraArgPaths`; live check through the
  UI proxy (see verification results).

## COR-03 — Windows: file secrets provider rejected every secrets file · Medium · P1 · verified · fixed

* **Where:** `goNCC.go` `validateSecretsFileHardening`.
* **Evidence:** baseline `TestValidateSecretsFileHardening` failed on Windows:
  a file written `0600` is reported as `0666`, so the "permissions too open"
  check always fired. (This failure was easy to miss: its FAIL line was
  interleaved with prompt output in the test log.)
* **Fix:** skip the POSIX mode-bit check on Windows (NTFS ACLs govern access;
  the self-heal permission check already skipped Windows).

## COR-04 — Webhook sender could dereference a nil request · Low · P2 · verified · fixed

`sendWebhookJSON` ignored the `http.NewRequest` error; a malformed stored URL
made `req.Header.Set` panic. Now returns an error. Test:
`TestSendWebhookJSONRejectsMalformedURL`.

## COR-05 — Runner email always attempted AUTH · Low · P3 · verified by reading net/smtp · fixed

The runner built `smtp.PlainAuth` even with no SMTP user, so unauthenticated
relays (no `AUTH` extension) failed with "server doesn't support AUTH". AUTH is
now used only when a user is configured (matching the API server's sender).
Test: `TestSendEmailFlattensSubjectAndSkipsAuthWithoutUser`.

## COR-06 — Post-restore path healing skipped Windows · Low · P3 · verified · fixed

`normalizeConfigOutputDirPaths` rewrites `/root/...` paths from a Linux backup,
but tested `filepath.IsAbs`, which is false for `/root/...` on Windows. Now
also accepts slash-rooted values. Baseline tests
`TestNormalizeConfigOutputDirPaths` and
`TestPostRestoreSelfHealIncludesConfigOutputDirNormalizationNote` now pass on
Windows.

## Non-portable tests (fixed)

Seven baseline failures were tests, not product code: golden fixtures checked
out with CRLF (`.gitattributes` now pins `testdata/golden/*` to LF), POSIX
paths assumed absolute, Windows paths embedded unescaped in YAML, a systemd
unit test (Linux-only, now skipped elsewhere), a symlink test needing a
privilege (skipped when unavailable), and a path assertion on JSON output. The
assertions keep their intent; none were weakened to pass.

## Open items

| ID | Finding | Severity | Priority | Confidence |
| --- | --- | --- | --- | --- |
| COR-07 | Unknown role string in a validly signed JWT/HMAC session maps to **admin** (`principalFromIdentity`, `principalFromHMACClaims`, `setAuthTokenCookie`). Only reachable with the signing secret, but it fails open | Low | P3 | verified (code) |
| COR-08 | `/api/v1/runs/config-preference` returns 404 in token-only mode while the Runs page requests it on every load (console error, no user impact) | Low | P3 | verified (live) |
| COR-09 | `upsertUser` used to rebuild the account without `TokenGen`/`RunConfigPath` (not reachable today because callers reject existing users; fixed defensively with SEC-01) | Low | P3 | verified (code) |
| MAINT-01 | `goNCC.go` (18k lines) and `cmd/ncc-api-server/main.go` (5.9k lines) concentrate unrelated concerns; duplicated helpers across runner and API (email, liveness) already diverged once | Medium | P3 | verified |

## Things checked and found sound

Path confinement (`normalizeAndConfinePath`: abs, `Rel`, symlink walk,
`EvalSymlinks`); backup archive extraction (two-pass, size caps, no
`..`/absolute names, non-regular entries never become links); JWT validation
(algorithm pinned, `exp` required, `iat` skew bound); PAT handling (hash at rest,
live role/deletion re-resolution, fail-closed expiry parsing); CSRF on every
cookie-authenticated mutation including the JWT cookie; JSON body limits and
`DisallowUnknownFields`; SMTP header construction in the API sender.
