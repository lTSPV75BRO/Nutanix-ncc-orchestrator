# Branch and workspace baseline

Recorded 2026-11-19 (Phase 0 of the v2.4.0 audit request).

## Repository

| Item | Value |
| --- | --- |
| Remote `origin` | `https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git` (fetch and push) |
| Main checkout | `Nutanix-ncc-orchestrator` (branch `main` at `7f46d41a`, tag `v2.3.0`) |
| Working tree for this audit | `.claude/worktrees/v2.4.0` (git worktree) |

## Target branch

| Item | Value |
| --- | --- |
| Branch | `v2.4.0` (exists locally and on `origin`) |
| Commit | `ea76376cbd6de803faf38fe3b66cdeb370c9c5f9` |
| Commit subject | fix(ui): align list columns and stop showing log file paths |
| Commit date | 2026-11-18T23:04:02+05:30 |
| Upstream | `origin/v2.4.0` at the same SHA; ahead 0, behind 0, not diverged |
| Relation to `main` | 34 commits ahead, 0 behind; merge base `7f46d41a` (v2.3.0) |
| `VERSION` file | 2.4.0 (no `v2.4.0` tag exists yet; latest tag is `v2.3.0`) |
| Working-tree status | clean (no uncommitted changes) |

`v2.4.0` is checked out in the worktree above, so `git checkout v2.4.0` from
the main checkout is refused by git. The audit works inside that worktree.
No branch was switched, created, reset, stashed or cleaned.

The main checkout has only untracked `.claude/` and `Claude outputs/`; they
were left alone.

## Existing audit material

`docs/audit/` already holds 14 documents from the 2026-10-26 audit (on
v2.2.1), updated on this branch. Its backlog (`implementation-plan.md`) still
lists open items: DEP-02 (blocked, no npm), UX-10 (accessibility checks),
OPS-03 (secret scanning in CI) and MAINT-01 (split `goNCC.go` and API
`main.go`). This pass continues from that backlog rather than starting over.

## Baseline commands (Go 1.27.4, Windows)

| Command | Result |
| --- | --- |
| `go build ./...` | exit 0 |
| `go vet ./...` | exit 0 |
| `go test ./...` | exit 0, no failing packages |

Not run at baseline: frontend tests and type-check (no standalone Node here;
see the toolchain note), `go test -race` (no gcc), scanners.

## Rules in force

All work stays on `v2.4.0`. No push, no PR, no tag, no merge or rebase, and
no commit unless the user authorizes it.
