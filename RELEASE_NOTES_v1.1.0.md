# Release notes – v1.1.0

**Date:** 2026-04-21

This release focuses on automation quality, regression intelligence, and production operations hardening.

## Highlights

- **Policy gates for CI/CD**: new `policy-gates` can fail runs using explicit thresholds (`new-fails>0`, `fail-rate>2`, `min-health-score<90`, `flaky-checks>0`).
- **Drill-down diff reports**: new `drilldown-diff.json` shows per-cluster new FAILs, resolved FAILs, new/removed checks, and severity changes versus previous run snapshot.
- **Flaky check detection**: new `flaky-checks.json` detects check severity oscillation across recent runs (`flaky-lookback-runs`, `flaky-min-transitions`).
- **Cluster health score**: per-cluster `health_score` (0-100) plus run-level `avg_health_score` and `min_health_score` in `run-summary.json`.
- **SLO dashboard export**: new `slo-dashboard.json` for downstream dashboards and BI tools.
- **Secrets provider support**: `secret://` references now resolve from environment (`secrets-provider=env`) or file-backed secret map (`secrets-provider=file`, `secrets-file`).
- **Quiet hours and maintenance windows**: suppress per-cluster/replay/digest notifications with `quiet-hours` and `maintenance-windows`.
- **Strict validation default**: config validation now rejects unknown keys and enforces strict config-file typing by default.

## New run artifacts

Runs now emit these additional artifacts in `outputfiles/`:

- `checks-snapshot.json`
- `drilldown-diff.json`
- `flaky-checks.json`
- `slo-dashboard.json`
- `policy-gates.txt` (only when policy violations occur)

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
- If you use strict config validation in CI (`validate-config`), ensure custom keys are removed or migrated.
- If using secret references (`secret://...`), configure `secrets-provider` first.

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
