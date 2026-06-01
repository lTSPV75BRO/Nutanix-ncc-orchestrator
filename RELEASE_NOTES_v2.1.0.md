# NCC Orchestrator — v2.1.0

**Release date:** 2026-06-01
**Type:** Maintenance + quality release. Recommended for everyone on v2.0.x.

> **Affiliation:** This is an independent open-source project. It is **not** affiliated with or endorsed by Nutanix, Inc. NCC and Nutanix are trademarks of their respective owners. The project is MIT licensed; see [`LICENSE`](LICENSE).

v2.1.0 is a consolidation release. It closes a download-integrity gap in `v2-bootstrap`, adds notification observability and templating, improves the Windows self-update flow, refreshes dependencies, and begins splitting the monolithic `goNCC.go` into focused packages. There are no breaking changes — every v2.0.x invocation keeps working.

---

## Highlights

### Download integrity: `v2-bootstrap` now verifies checksums

`update` already verified every downloaded asset against the release `checksums.txt` (single-binary, stack-archive, and `--binary-url` paths). `v2-bootstrap` did **not** — it downloaded and extracted the `ncc-v2-stack-*` archive (and, in the legacy fallback, the api/ui binaries + frontend archive) without authentication.

v2.1.0 closes that gap: `v2-bootstrap` now verifies each downloaded asset against the release `checksums.txt` before extracting or installing, hard-failing on a missing checksum manifest or a hash mismatch. This makes the whole trust chain shipped in v2.0.2 (`checksums.txt`, `release-attestation.json`, SBOMs, SLSA provenance) actually enforced at install time.

Pinned by `TestVerifyDownloadedAsset` and the existing `TestVerifyAssetAgainstReleaseChecksum_TamperDetection`.

### `--skip-checksum-verify` escape hatch

Both `update` and `v2-bootstrap` accept `--skip-checksum-verify` for air-gapped or internally-mirrored installs where the release `checksums.txt` is unavailable. It prints an explicit, support-friendly warning so it is obvious when integrity was not checked. The default remains hard-fail.

### Windows self-update is now one command

On Windows you cannot overwrite a running `.exe` in place, so `update` previously dropped `ncc-orchestrator.new.exe` and told you to swap it by hand. v2.1.0 instead writes an `apply-ncc-update.cmd` next to the binary that:

1. waits for the running process to release the lock,
2. swaps the new binary over the old one, and
3. deletes itself.

Run it after exiting and the update completes with no manual file juggling. The helper is also added to the `uninstall` cleanup set. Pinned by `TestWriteWindowsUpdateSwapHelper`.

### Notification delivery metrics

Notification failures used to be visible only in the logs. v2.1.0 records per-channel outcomes for each run and, when `prom-enabled` is set, writes a run-level `notifications.prom` textfile:

```
nutanix_ncc_notification_attempts_total{channel="email"}  3
nutanix_ncc_notification_failures_total{channel="email"}  1
nutanix_ncc_notification_attempts_total{channel="webhook"} 3
nutanix_ncc_notification_failures_total{channel="webhook"} 0
nutanix_ncc_notification_attempts_total{channel="slack"}   0
nutanix_ncc_notification_failures_total{channel="slack"}   0
```

A line is always emitted per channel (0 when unused), so an alert like `increase(nutanix_ncc_notification_failures_total[1h]) > 0` never breaks on a missing series. Disabled channels are not counted.

### Custom notification templates

Three optional config keys let you override the notification content with Go `text/template` strings:

```yaml
email-subject-template: "NCC {{.Cluster}}: {{.FailCount}} FAIL / {{.WarnCount}} WARN"
email-body-template: "{{.Overview}}\nStarted: {{.StartedAt}}"
webhook-template: '{"text":"NCC {{.Cluster}} FAIL={{.FailCount}}"}'
```

Templates render against the run summary (`.Cluster`, `.FailCount`, `.WarnCount`, `.ErrCount`, `.InfoCount`, `.TotalChecks`, `.Overview`, `.StartedAt`, `.FinishedAt`, `.OutputFiles`) and apply to the per-cluster, digest, and replay paths. Leave a key empty for the built-in default. A broken template logs and falls back to the default (it never drops a notification); the webhook body is sent verbatim, so it must render valid JSON.

### Dependency refresh

- Go modules updated (`modelcontextprotocol/go-sdk` 1.6.0→1.6.1, `golang.org/x/sys` 0.44→0.45, `mattn/go-colorable`, `mattn/go-runewidth`). `go vet`, the `-race` test suite, and `govulncheck` are clean.
- Frontend `npm audit`: 0 vulnerabilities.
- GitHub Actions remain on current major pins (floating tags receive patch updates automatically).

---

## `goNCC.go` package extraction

The first wave of splitting the ~15.5k-line `goNCC.go` into focused `internal/` leaf packages landed in this release. Five packages were carved out, each re-exported from `main` via type/function aliases so the thousands of existing references and call sites compile unchanged:

- **`internal/model`** — foundational shared types (`Config`, `ClusterCredential`, `NotificationSummary`, `ParsedBlock`, `FS`, `HTTPClient`) and `ClusterHealthScore`.
- **`internal/promtext`** — Prometheus textfile writers (`WritePrometheusFile`, `WriteNotificationMetricsFile`, `SanitizeLabel`).
- **`internal/retryutil`** — the shared retry/backoff helpers (`JitteredBackoff`, `IsRetryableStatus`, `RetryAfterDelay`). Extracted first as a stdlib-only leaf so both `main` and `internal/notify` reuse them without an import cycle.
- **`internal/notify`** — the email / webhook / Slack senders, retry wrappers, `text/template` overrides, and the per-channel delivery-metrics accumulator (run-level counters are read back via `notify.ResetMetrics` / `notify.SnapshotMetrics`).
- **`internal/nccparse`** — the NCC summary parser (`SplitLines`, `ParseSummary`, `ValidateParsedAlertsAgainstPluginResults`) producing `model.ParsedBlock`.

Behavior is identical — the orchestrator's existing test suite is unchanged and passes under `-race` — and each new package ships its own unit tests (the notification, template, and parser tests were relocated alongside their implementations). Further slimming of `goNCC.go` can follow the same alias-backed, behavior-preserving pattern; sequencing lives in [`IMPROVEMENTS.md`](IMPROVEMENTS.md).

---

## Upgrade

From any v2.0.x install:

```bash
./ncc-orchestrator update
```

On Windows, after the download completes, exit the program and run the generated `apply-ncc-update.cmd`. On macOS/Linux the running binary is replaced atomically in place.

Both `update` and `v2-bootstrap` now verify downloads against the release `checksums.txt` automatically; pass `--skip-checksum-verify` only for air-gapped mirrors.

---

## Tests

- Full Go suite passes with `-race -count=1`, including new `TestVerifyDownloadedAsset` and `TestWriteWindowsUpdateSwapHelper`, plus the new `internal/` package suites: `internal/model`, `internal/promtext`, `internal/retryutil`, `internal/notify` (notification metrics, templates, and sender tests — `TestMetrics`, `TestRenderTemplate`, `TestApplyEmailTemplates`, `TestSendWebhook_TemplateBody`, `TestWrappers_SkipDisabled`, `TestWebhookWithRetry_RecordsSuccess`), and `internal/nccparse` (`TestParseSummary`, `TestDetectSeverity`, `TestSplitLines`, `TestValidateParsedAlertsAgainstPluginResults`).
- `go vet`, `gofmt -l`, `govulncheck`, and frontend `npm audit` all clean.

---

## Acknowledgements

This release was driven by post-release verification of v2.0.2 on Windows, which surfaced the executability bug fixed in v2.0.2 and prompted the self-update and download-integrity hardening shipped here.
