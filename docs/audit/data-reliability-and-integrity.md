# Data reliability, integrity and trustworthiness

## Data flow

```
Prism Central / Element (NCC run + task polling)
   └─► runner: parse NCC output (internal/nccparse) ─► per-cluster logs (nccfiles/)
        └─► aggregate ─► run-summary.json, ncc-run-record.json, regression/drilldown/flaky/SLO JSON,
                         index.html, CSV/JSON/MD/SARIF, Prometheus .prom
             └─► API reads artifacts ─► /report/data, /runs, /alerts/ncc-dispositions (+ lifecycle store)
                  └─► UI (Dashboard, Insights, Runs) — timestamps UTC on the wire, rendered local
```

The authoritative source is Prism/NCC. Everything downstream is derived and
regenerable except **operator state**: alert dispositions/lifecycle, user
store, schedules, notification settings, config.

## Strengths

* Run status is explicit (`success/partial/failed/triggered/trigger_failed/unknown`)
  and `exit_code` is always written (fixed in v2.2.1), so a missing value is
  never silently shown as success.
* Per-cluster errors are classified (auth/connection/timeout) and surfaced; the
  Dashboard shows when the last run finished and the last runtime error.
* Scoped runs write to per-run directories and merge, so concurrent runs for
  different cluster groups do not clobber each other.
* State files use atomic temp+rename writes and cross-process locks (alert
  lifecycle, login guard); backups are AES-256-GCM sealed and verified after
  creation.
* The UI normalises timezone-less timestamps as UTC before rendering local
  time (`utils/datetime.ts`).

## Findings

### DATA-01 — User store writes can be lost across API replicas · Medium-High · P1 · strongly suspected · **fixed** (see CHANGELOG: reload-before-write, file lock, resourceVersion precondition; tests in userstore_guard_test.go)

* **Where:** `cmd/ncc-api-server/users.go` (`saveLocked`, `refreshIfStale`),
  `k8ssecret.go` (`save` → merge PATCH without `resourceVersion`).
* **Mechanism:** each replica holds the whole user document in memory,
  refreshes from the backend at most every 2 s, and on every mutation
  serialises its in-memory copy and overwrites the stored document. There is
  no read-before-write and no optimistic-concurrency precondition. A write on
  replica B within ~2 s of a write on replica A silently reverts A's change.
* **Why it matters:** PAT use records `last_used_at` (one write per token per
  minute), so a CI job calling the API through replica B can, by timing alone,
  undo a user deletion, a role demotion, a **token revocation**, or a
  "sign out everywhere" just made on replica A. Two replicas are the Helm default.
* **Not reproduced:** needs two live replicas; the conclusion follows directly
  from the code.
* **Recommendation:** (1) re-load and re-apply under the lock immediately
  before every mutation (read-modify-write) instead of saving the cached
  document; (2) Kubernetes backend: include `metadata.resourceVersion` in the
  PATCH and retry on 409; file backend: hold the existing cross-process state
  lock (`lockStateFile`) across load-modify-save; (3) stop persisting
  `last_used_at` on the hot path, or keep it in a separate document.
  Regression test: two `userDB` instances over one backend, interleaved
  revoke/touch, assert the revocation survives.

### DATA-02 — Restore has no automatic pre-restore snapshot · Medium · P1 · verified (code) · **fixed** (transactional restore with rollback; restore_txn_test.go)

* **Where:** `handleRestore`, `handleBackupRestoreNamed` → `v2-restore --force`
  (`goNCC.go` `runV2Restore`).
* **Evidence:** restore validates the whole archive before writing (good),
  but then streams files over the live install one by one with `--force`. A
  failure mid-way (disk full, size cap on a later file, process killed) leaves
  a mix of old and restored files, and nothing captures the pre-restore state.
  The in-app updater takes a pre-update backup; restore does not.
* **Recommendation:** take an encrypted `pre-restore-<stamp>` snapshot
  (same mechanism as `pre-update-*`) before writing, and on failure restore it
  automatically; surface it in the backups list as the rollback candidate.

### DATA-03 — Deleted accounts' sessions stayed valid · High · P1 · verified · **fixed**

Covered as SEC-01 in [`security-assessment.md`](security-assessment.md). The
fix adds `deleted_accounts` tombstones to the persisted user document; they
are pruned after 30 days and capped at 1,000 entries.

### DATA-04 — Scheduled-run heartbeats deleted on Windows · High · P1 · verified · **fixed**

Covered as COR-01: running scheduled runs were invisible (and their heartbeat
files removed) on Windows, so Active Runs under-reported in-progress work.

## Freshness and transparency

| Signal | Present | Gap / recommendation |
| --- | --- | --- |
| Last run time and status on Dashboard | yes | — |
| Per-cluster last successful check time | partial (per-run) | Show "last successful data" per cluster so a cluster that failed the last 3 runs is not shown with stale findings as if current (P2) |
| Stale-data warning when no run completed within the schedule interval | schedule health shows last success | Add a dashboard banner when `now − last_success > 2 × interval` (P2) |
| Partial-run indicator | `status: partial` | — |
| Prism Central alerts source/age | cache TTL 5 m, labelled Beta | Show the cache age next to PC alerts (P3) |

## Recovery

| Item | Current | Recommendation |
| --- | --- | --- |
| Backups | Manual + scheduled + pre-update; encrypted by default; verify endpoint | Add pre-restore (DATA-02); document an off-host copy procedure |
| RPO/RTO | Not defined | Suggested: RPO 24 h (daily scheduled backup), RTO 1 h (restore + restart); test quarterly with `/backups/verify` and a real restore into a scratch install |
| Kubernetes | PVC + Secret | Volume snapshots of the PVC; etcd encryption for Secrets (prior assessment A2) |
| Run artifacts | Regenerable; retention-pruned | Keep `run-history` enabled where trend continuity matters |
