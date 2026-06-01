# NCC Orchestrator — v2.1.0

**Release date:** 2026-06-01
**Type:** Maintenance + quality release. Recommended for everyone on v2.0.x.

> **Affiliation:** This is an independent open-source project. It is **not** affiliated with or endorsed by Nutanix, Inc. NCC and Nutanix are trademarks of their respective owners. The project is MIT licensed; see [`LICENSE`](LICENSE).

v2.1.0 is a consolidation release. It closes a download-integrity gap in `v2-bootstrap`, improves the Windows self-update flow, refreshes dependencies, and refreshes the project backlog. There are no breaking changes — every v2.0.x invocation keeps working.

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

### Dependency refresh

- Go modules updated (`modelcontextprotocol/go-sdk` 1.6.0→1.6.1, `golang.org/x/sys` 0.44→0.45, `mattn/go-colorable`, `mattn/go-runewidth`). `go vet`, the `-race` test suite, and `govulncheck` are clean.
- Frontend `npm audit`: 0 vulnerabilities.
- GitHub Actions remain on current major pins (floating tags receive patch updates automatically).

---

## Deferred: `goNCC.go` package extraction

Splitting the ~15.5k-line `goNCC.go` into focused `internal/` packages (notifications, Prometheus textfile, parser) was scoped for this release but **deliberately deferred**. Those subsystems depend on pervasive shared types — `Config`, `FS`, `ParsedBlock`, `NotificationSummary`, `HTTPClient` — that are used throughout `goNCC.go` and the `cmd/*` servers. A clean extraction must first move those foundational types into a shared `internal/model` package and update hundreds of references.

Doing that under a maintenance bump carried significant regression risk for no user-facing benefit, so it now leads the backlog in [`IMPROVEMENTS.md`](IMPROVEMENTS.md) with a recommended sequencing (extract shared types first, then move one subsystem at a time, behind a green `-race` suite).

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

- Full Go suite passes with `-race -count=1`, including new `TestVerifyDownloadedAsset` and `TestWriteWindowsUpdateSwapHelper`.
- `go vet`, `gofmt -l`, `govulncheck`, and frontend `npm audit` all clean.

---

## Acknowledgements

This release was driven by post-release verification of v2.0.2 on Windows, which surfaced the executability bug fixed in v2.0.2 and prompted the self-update and download-integrity hardening shipped here.
