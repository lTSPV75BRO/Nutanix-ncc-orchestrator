# Release notes – v1.1.0

**Date:** 2026-05-07

This release focuses on automation quality, regression intelligence, and production operations hardening.

## Highlights

- **Alert exclusion framework**: added `exclude-alert-titles`, `exclude-alert-titles-file`, and `exclude-alert-match-mode` (`exact|contains|regex`) for report/notification filtering.
- **Exclusion audit trail**: new `excluded-alerts.json` artifact with `schema_version`, totals, and per-cluster excluded entries.
- **Secrets hardening**: file-based secrets now enforce hardening checks (regular file, non-symlink, owner-only permissions, size cap).
- **Retry circuit breaker**: new `retry-circuit-breaker` fails fast after consecutive retryable failures to prevent long noisy retries.
- **Failure classifications**: run summaries now include per-cluster `error_class` and run-level `failure_classes` aggregation.
- **Artifact retention policies**: new `artifact-retain-days` and `artifact-retain-max-files` cleanups for generated outputs.
- **Policy gates for CI/CD**: new `policy-gates` can fail runs using explicit thresholds (`new-fails>0`, `fail-rate>2`, `min-health-score<90`, `flaky-checks>0`).
- **Drill-down diff reports**: new `drilldown-diff.json` shows per-cluster new FAILs, resolved FAILs, new/removed checks, and severity changes versus previous run snapshot.
- **Flaky check detection**: new `flaky-checks.json` detects check severity oscillation across recent runs (`flaky-lookback-runs`, `flaky-min-transitions`).
- **Cluster health score**: per-cluster `health_score` (0-100) plus run-level `avg_health_score` and `min_health_score` in `run-summary.json`.
- **SLO dashboard export**: new `slo-dashboard.json` for downstream dashboards and BI tools.
- **Secrets provider support**: `secret://` references now resolve from environment (`secrets-provider=env`) or file-backed secret map (`secrets-provider=file`, `secrets-file`).
- **Quiet hours and maintenance windows**: suppress per-cluster/replay/digest notifications with `quiet-hours` and `maintenance-windows`.
- **Strict validation default**: config validation now rejects unknown keys and enforces strict config-file typing by default.
- **Command model refresh**: moved operational root flags to subcommands (`terms`, `env-info`, `update`, `gen-test-agg`, `version`) with backward-compatible deprecated aliases.
- **Preflight-by-default runtime safety**: preflight checks are integrated into normal execution with explicit `--skip-preflight-check` override.
- **Track-aware updater**: `update --check`, `--repo`, `--binary-url`, and `--target-version` support safer release checks and custom binary sources.
- **Major upgrade guardrails**: updater keeps `v1.x` users on `v1.x` by default and requires explicit opt-in for `v1 -> v2`.
- **v2 migration helpers from CLI**: added `v2-bootstrap` (asset bootstrap) and `v2-start` (start API+UI together) to simplify optional migration testing.

## New run artifacts

Runs now emit these additional artifacts in `outputfiles/`:

- `checks-snapshot.json`
- `drilldown-diff.json`
- `flaky-checks.json`
- `slo-dashboard.json`
- `policy-gates.txt` (only when policy violations occur)
- `excluded-alerts.json`

## Upgrade notes from v1.0.0

- Update image/binary tag to `1.1.0`.
- Review config for new optional keys:
  - `policy-gates`
  - `quiet-hours`
  - `maintenance-windows`
  - `flaky-lookback-runs`
  - `flaky-min-transitions`
  - `secrets-provider`
  - `secrets-file`
  - `exclude-alert-titles`
  - `exclude-alert-titles-file`
  - `exclude-alert-match-mode`
  - `retry-circuit-breaker`
  - `artifact-retain-days`
  - `artifact-retain-max-files`
- If you use strict config validation in CI (`validate-config`), ensure custom keys are removed or migrated.
- If using secret references (`secret://...`), configure `secrets-provider` first.
- If you use old operational root flags (`--env-info`, `--update`, `--tc`, `--gen-test-agg`, `--version`), migrate to subcommands; aliases remain available but deprecated.
- For mixed-track environments, use `ncc-orchestrator update --check` before applying updates.

## Production verification snapshot (2026-05-05)

- `go test ./...` passed
- `go vet ./...` passed
- `go build ./...` passed
- `go test ./...` re-validated after updater/subcommand and preflight UX enhancements
- Edge-case coverage added for:
  - retry circuit breaker trip behavior
  - secrets-file hardening failures
  - alert exclusion match mode behavior (`exact|contains|regex`) and invalid regex handling
  - artifact retention policy behavior and protected artifact preservation

## Artifacts and deployment

- **Orchestrator version:** `1.1.0`
- **MCP server version:** `1.1.0`
- **Docker image:** `prajwalnutant/nutanix-ncc-orchestrator:1.1.0`
- **Helm chart:** `helm/ncc-orchestrator` chart/appVersion `1.1.0`
- **Kubernetes manifests:** `k8s/cronjob.yaml` and `k8s/job-debug.yaml` use image tag `1.1.0`

## MCP server notes

MCP tooling remains aligned for run/discovery/report/scheduler operations. For setup and tool details, see [docs/MCP_SERVER.md](docs/MCP_SERVER.md).

## GitHub release checklist (v1.1.0)

Before clicking **Publish release** on GitHub:

1. Ensure `VERSION` is `1.1.0`.
2. Ensure changelog `1.1.0` section is present in [CHANGELOG.md](CHANGELOG.md).
3. Build and attach binaries for Linux/macOS/Windows targets.
4. Generate and attach `checksums.txt` (see [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md)).
5. Tag as `v1.1.0`.
6. Use this file as the GitHub release description body.
7. After publish, verify:
   - release assets are downloadable
   - Docker tag `prajwalnutant/nutanix-ncc-orchestrator:1.1.0` exists
   - `ncc-orchestrator --update` can discover and verify assets
   - `ncc-orchestrator update --check` reports expected `v1.1.0` track

## Maintainer notes for this refreshed v1.1.0 release

- Keep release scope v1-focused: CLI runtime and docs for v1 operation.
- If publishing v2 helper assets as part of shared repository releases, ensure v1 CLI assets remain present and checksummed.
- `dist/example_config.yaml` has been refreshed to v1.1.0 defaults and should be attached as an example asset when possible.
