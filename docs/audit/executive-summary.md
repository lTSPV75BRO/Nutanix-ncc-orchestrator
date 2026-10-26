# Executive summary — application audit (2026-10-26)

**Scope:** the whole repository on branch `v2.2.1` (runner, API server, UI
server, MCP server, React frontend, deployment manifests, CI). **Method:**
source review, workflow tracing, baseline and final build/test/lint/scan runs,
and a live smoke test of the built binaries. Details are in the other documents
in this folder; [`verification-results.md`](verification-results.md) lists
every command run.

## Overall health

A well-engineered, security-conscious codebase with strong defaults
(encrypted backups, hashed PATs, CSRF, CSP, path confinement, rate limiting,
non-root containers, SBOM and provenance) and a large test suite. The defects
found cluster in three places: **Windows** (a supported platform never
exercised in CI), **contracts kept in sync by hand** (API run arguments vs
runner flags; client vs server timeouts), and **multi-replica shared state**.
No Critical issue was found.

## Production readiness

Suitable for production on **a single API replica** (Linux or Windows) after
the npm dev-tool updates and the deployment checks in
[`production-readiness.md`](production-readiness.md). **Multi-replica API
deployments** (the Helm default of 2) carry a lost-update risk in the user
store until DATA-01 is fixed. This judgement rests on code review and tests,
not on observation of a production environment.

## Most important verified findings

| | Finding | Status |
| --- | --- | --- |
| High | **Deleted users kept their sessions** (with their role, possibly admin) for up to the session TTL; re-creating the name revived old tokens | Fixed and verified |
| High | **Windows: every live process looked dead**, so restore could overwrite a running stack, two supervisors could start, scheduled runs vanished from Active Runs, and stops always force-killed | Fixed and verified |
| High | **8 of 10 allowed run options made the runner exit** ("unknown flag"), including the options the UI and API docs suggested | Fixed and verified (live) |
| High | **Anyone on the network could force-rotate the admin password** (by design, throttled per IP) | Now off by default (`--allow-admin-self-reset` to opt in); verified |
| Medium-High | **User-store writes could be lost across API replicas** (could undo a revocation or deletion) | Fixed and verified (reproduced with two replicas in tests) |
| Medium | Long operations (preflight, backup, restore) were cut off by 30–60 s timeouts while still running; SMTP had no timeouts; no graceful shutdown | Fixed and verified |
| Medium | MCP server could install arbitrary scheduled commands and exposed Prism passwords in the process list; runner emails allowed header injection; operators could write runner output outside the install root | Fixed and verified |

## Dependencies

Go: no reachable vulnerabilities; four patch/minor updates applied. npm:
production dependencies clean; **12 advisories in build/test tooling** (vite
8.3.1, including a Windows command-injection chain in the dev server; undici;
source-map-js; @vitest/mocker). Updating them needs the npm CLI, which this
environment lacked; the exact commands are in
[`dependency-health.md`](dependency-health.md). Dependabot is now configured.

## Highest-value UX improvements

Implemented: the Dashboard no longer shows a red "0.0%" health score when
there is no data, the first-time setup steps are readable, and run-option hints
point at options that work. Recommended next: a run-options builder, per-cluster
freshness indicators, a stale-schedule banner, and an accessibility pass.

## Changes made

Commits were not created; all changes are in the working tree for review.
40 files modified and 12 source/config files added (plus the 14 documents
in this folder): code fixes listed above, 23 new Go test functions and 4 new
Vitest tests, Windows CI job, Dependabot, changelog and docs updates. All Go tests pass on Windows (13 failing at baseline); lint
clean; frontend type-check, tests (49/49) and build pass.

## Outstanding risks and blockers

1. Apply the npm dev-tool updates (needs npm).
2. Verify restored binaries against signed release checksums (SEC-07).
3. Not verified here: race detector, Windows CI on GitHub-hosted runners,
   Node 26 frontend run, real Prism/SMTP/Kubernetes behaviour.

## Follow-up work (same review cycle)

Also implemented after the initial audit: a server-derived setup checklist
replacing the broken first-time setup modal; redesigned sign-in and
first-password screens (directory-only sign-in, encrypted-backup restore);
transactional restore with rollback; HA-safe user store; run options builder;
readiness endpoint; request IDs and per-route HTTP metrics.

## Recommended next steps

See the prioritized backlog in [`implementation-plan.md`](implementation-plan.md)
and the optional features in [`feature-roadmap.md`](feature-roadmap.md).
