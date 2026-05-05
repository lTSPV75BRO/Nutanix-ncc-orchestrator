# Milestone plan - v1.1.0

> Status: implemented in release prep for `v1.1.0` with additional production hardening and API enhancements validated on 2026-05-05.

This document defines a practical implementation plan for `v1.1.0`.
It prioritizes user-facing impact, operational reliability, and low-risk delivery.

## Goals

- Improve scheduling automation across local hosts and Kubernetes.
- Make run output easier for automation and historical analysis.
- Reduce noisy alerts by highlighting regressions instead of static failures.
- Improve MCP ergonomics so AI clients can operate scheduling and report flows safely.

## Priority scale

- `P0` - Must-have for `v1.1.0`
- `P1` - Strong candidate; include if `P0` remains on track
- `P2` - Backlog stretch items

## Effort scale

- `S` - Small (about 1-2 dev days)
- `M` - Medium (about 3-5 dev days)
- `L` - Large (about 1-2+ weeks)

## Proposed scope

| Priority | Theme | Feature | Effort | Why it matters | Dependencies |
|---|---|---|---|---|---|
| `P0` | Scheduler UX | `create-schedule --list`, `--remove`, `--run-now` | `M` | Completes schedule lifecycle; users can manage without manual cron/schtasks edits. | Current `create-schedule` command |
| `P0` | Kubernetes automation | `create-schedule --type k8s-cronjob` (`--output-yaml` and optional `--apply`) | `L` | One command bootstrap for periodic runs in K8s; fewer manual manifest steps. | Existing `k8s/` manifests, kube context access |
| `P0` | Run history | Timestamped run folders + `latest` pointer; retention flags (`--retain-last`, `--retain-days`) | `M` | Safer report retention and easier audits/comparisons. | Output path migration plan |
| `P0` | Regression awareness | Diff vs previous run (`new FAIL`, `resolved FAIL`, severity delta) + `--notify-on-regression` | `L` | Reduces notification fatigue and focuses operators on changes. | Stable run identity and persisted history |
| `P1` | MCP scheduler ops | Add MCP tools: `create_schedule`, `list_schedules`, `delete_schedule` | `M` | Makes Cursor/AI workflows fully self-service for scheduling. | Scheduler backend from `P0` |
| `P1` | Metadata quality | Enrich `ncc-run-record.json` with git SHA, host, scheduler source | `S` | Better traceability for automation and incident reviews. | Existing run-record schema |
| `P1` | Resilience | Adaptive parallelism under repeated HTTP 429 | `M` | Improves success rate in rate-limited environments. | Existing retry and 429 detection |
| `P2` | Security/export | SARIF output for pipeline consumption | `M` | Integrates with security/compliance tooling. | Report normalization |
| `P2` | Report packaging | Optional single-file HTML bundle | `M` | Easier sharing and archival for stakeholders. | Current HTML generation path |
| `P2` | Config ergonomics | JSON Schema for `config.yaml` + validation mode | `M` | Better upfront validation and IDE hints. | Config model completeness |

## Recommended implementation order

1. **Scheduler lifecycle completion (`P0`)**
   - Add list/remove/run-now to existing local scheduler implementation.
   - Keep side effects explicit; default preview mode remains safe.
2. **Run history + retention (`P0`)**
   - Introduce run directory structure and cleanup strategy.
   - Preserve current output compatibility via `latest` pointer.
3. **Regression diff + targeted notifications (`P0`)**
   - Compute diff from previous run summary.
   - Gate notifications using regression-only mode.
4. **Kubernetes scheduler creation (`P0`)**
   - Generate CronJob YAML from flags.
   - Add optional apply path, but keep dry-run/print as default.
5. **MCP scheduling tools (`P1`)**
   - Expose same scheduler APIs through MCP.
   - Return structured machine-readable responses.

## Acceptance criteria for v1.1.0

- A user can create, list, remove, and test periodic schedules on Linux/macOS and Windows without editing system schedulers manually.
- A user can generate (and optionally apply) a Kubernetes CronJob from CLI parameters.
- Runs are retained with deterministic history semantics and configurable cleanup.
- Regression-only mode correctly highlights new failures and can suppress non-regression notifications.
- MCP supports schedule lifecycle operations with clear error messages and safe defaults.

## Suggested release checklist additions (v1.1.0)

- Add integration tests for scheduler commands:
  - cron upsert/list/remove behavior
  - windows task command generation
  - k8s CronJob manifest generation
- Add migration notes for output retention/history layout.
- Include at least one regression-diff example in `README.md`.
- Add MCP docs section for schedule management tools.

## Post-milestone hardening completed

The following hardening/operational items were completed during final release readiness:

- Alert exclusion controls with file input and match modes (`exact|contains|regex`)
- Exclusion audit artifact (`excluded-alerts.json`) with schema versioning
- Secrets file hardening checks (permissions/symlink/size)
- Retry circuit breaker (`retry-circuit-breaker`) to fail fast on repeated retryable failures
- Failure classification in run summaries for machine-readable triage
- Extended policy gate metrics for auth/network/timeout and cluster-failure classes
- Artifact retention controls for generated outputs
- Historical trend API for run history (`/api/v1/report/trends`)
