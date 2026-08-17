# NCC Orchestrator — v2.1.1

**Release date:** 2026-08-17
**Type:** Patch release (Alerts-table severity-classification fix). Recommended for everyone on v2.1.0.

> **Affiliation:** This is an independent open-source project. It is **not** affiliated with or endorsed by Nutanix, Inc. NCC and Nutanix are trademarks of their respective owners. The project is MIT licensed; see [`LICENSE`](LICENSE).

v2.1.1 fixes a severity-classification issue introduced in v2.1.0: a cluster whose NCC run fails outright (connection refused, DNS failure, auth rejection, timeout, etc.) was surfaced in the Alerts table as a **`FAIL`**-severity `"NCC run failed"` row — the same severity used for a genuine failing NCC check. That overstated the finding (conflating "NCC never ran against this cluster" with "NCC found a real problem on this cluster") and skewed FAIL totals and drilldown-diff regression counts with connectivity noise. There was never any data loss or corruption — see [`RELEASE_NOTES_v2.1.0.md` → Known issues](RELEASE_NOTES_v2.1.0.md#known-issues-fixed-in-v211) for the full writeup and the workaround that applied before this fix.

No breaking changes — every v2.1.0 invocation keeps working.

---

## What changed

### `"NCC run failed"` rows are now tagged `UNKNOWN`, not `FAIL`

The synthetic row that keeps a connection/auth/timeout failure visible in the Alerts table (instead of the cluster silently vanishing from the report — see v2.1.0's Bug fixes) is now tagged **`UNKNOWN`** severity — a classification the Alerts table and dashboard already recognize and render distinctly (a neutral pill, no KB link expected). `UNKNOWN` correctly represents "NCC never actually ran any check against this cluster," rather than `FAIL`, which implied a real finding.

This also means:

- **FAIL totals and regression counts are no longer inflated by connectivity noise.** The Alerts hero pill, dashboard hero cards, and the drilldown diff's `new_failures` / `resolved_failures` counters now only reflect genuine NCC check outcomes; a cluster that couldn't be reached no longer counts as a "new failure."
- **The row's Detail now carries an actionable remediation hint alongside the real error**, generated from the same error classification used elsewhere (`classifyClusterError`): for example, a network/timeout failure now suggests reducing `--max-parallel` and increasing `--timeout`/`--request-timeout`, while an auth failure suggests checking for an account lockout in Prism. Previously the Detail was just the raw error string.
- **The KB column stays intentionally empty** for this row — there's no real NCC KB article for "the orchestrator couldn't reach the cluster," so no fabricated KB link is shown.

### Severity priority: `WARN` now outranks `ERR`

Independently of the above, the Alerts table's default severity sort and the dashboard's hero pills/filter-chip order previously ranked `ERR` ahead of `WARN` (`FAIL > ERR > WARN > INFO`). This is corrected app-wide to `FAIL > WARN > ERR > INFO > UNKNOWN`, in both `ClusterTable.tsx` (`severityRank`, summary pills) and `DashboardPage.tsx` (`SEVERITY_META`, which drives the hero cards and severity filter chips).

---

## Bug fixes

- **A cluster that failed to run showed as a `FAIL` alert, misrepresenting "NCC never ran" as "NCC found a real failing check".** See "What changed" above. Pinned by `TestBuildClusterChecksSnapshotFromResultFailure`, `TestRunFailedRemediation`.

---

## Backward compatibility

- **API / data contract:** `checks-snapshot.json`'s `CheckSnapshotEntry` gains a new, optional `detail` field (`omitempty`) — purely additive, existing consumers that don't read it are unaffected. The `AGG` row shape is unchanged (it already carried a `Detail` field). No endpoints, config keys, or CLI flags changed.
- **Existing reports:** Any `"NCC run failed"` rows already recorded in an existing `checks-snapshot.json` / `AGG` blob from a v2.1.0 run keep their original `FAIL` tag until the next run regenerates them — this fix applies going forward, it does not rewrite historical reports.
- **UI:** No new pages or settings. The only visible changes are the tag/color/KB state of `"NCC run failed"` rows and the relative sort position of `WARN` vs `ERR`.

---

## Tests

| Check | Result |
|---|---|
| `gofmt -l .` | clean |
| `go build ./...` | clean |
| `go vet ./...` | clean |
| `go test ./... -short -count=1` | all packages pass |
| Frontend `tsc --noEmit -p tsconfig.json` | clean |
| Frontend `npm test` (vitest) | all pass |
| Frontend `npm run build` | clean |

---

## Upgrade

From v2.1.0:

```bash
./ncc-orchestrator update
```

Or, on a running v2 stack, update **from the UI**: **Settings → Access → Software updates → Check for updates**, then **Back up & update** — the server takes a pre-update backup, installs the checksum-verified package (orchestrator + api + ui + frontend), and restarts the stack automatically; the page reconnects on its own.

On Windows, after the download completes, exit the program and run the generated `apply-ncc-update.cmd`. On macOS/Linux the running binary is replaced atomically in place.

Both `update` and `v2-bootstrap` verify downloads against the release `checksums.txt` automatically; pass `--skip-checksum-verify` only for air-gapped mirrors.

---

## Acknowledgements

Reported and root-caused during routine dashboard review of live NCC run failures. Fixed and released the same day.
