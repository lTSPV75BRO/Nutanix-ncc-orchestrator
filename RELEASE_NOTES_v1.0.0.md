# Release notes – v1.0.0

**Date:** 2026-04-07

First **stable** release (semver **1.0.0**).

## Highlights

- **Nutanix API v4 (default)** — Orchestrator uses v4 cluster management and monitoring paths by default; `nutanix-v4-api-version` (e.g. `v4.2`) controls the path segment. **Legacy** v3 APIs remain available via `ncc-api-version: Legacy` (alias `v1`).
- **Cluster discovery** — v4 paginated cluster list with v3 fallback on 404.
- **Task polling** — Prism task status via v4-style paths with fallback where needed.
- **Prism Central multi-cluster** — v4 cluster list is matched to your `--clusters` value (not only the first row); `nodeIps` use CVM addresses from the matched cluster (avoids NCC-40023 when PC and AOS are both registered).
- **run-summary.json** — Per-cluster stats (`clusters[]`), `exit_code`, and `exit_code` **3** for partial success (some clusters OK, some failed).
- **ncc-run-record.json** — Versioned machine-readable run record (`schema_version`, orchestrator metadata) alongside `run-summary.json`.
- **discover-clusters** — `--format lines|table|json` for Prism Central cluster lists.
- **HTTP 429** — Rate-limit header logging and bounded `Retry-After` backoff.
- **Deploy** — Helm chart [`helm/ncc-orchestrator`](helm/ncc-orchestrator); Kustomize via `kubectl apply -k k8s/`.
- **`--update`** — Semver comparison via Masterminds/semver; `GITHUB_TOKEN` still recommended for API rate limits.
- **Troubleshooting** — [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

## Upgrade from 0.1.x

- Update [`VERSION`](VERSION) or pull image `prajwalnutant/nutanix-ncc-orchestrator:1.0.0`.
- Review `config.yaml` for `ncc-api-version` (default **v4**) and `nutanix-v4-api-version` to match your Prism / PC version.
- **TLS:** For lab or self-signed Prism certificates, set `insecure-skip-verify: true` (or fix server TLS); otherwise TLS verification may fail.

## Artifacts

- **Orchestrator:** Version **1.0.0** (from [VERSION](VERSION); default in code when not set via ldflags).
- **Docker:** `prajwalnutant/nutanix-ncc-orchestrator:1.0.0` (and `:latest` when built from main).
- **Kubernetes:** `k8s/cronjob.yaml` and `k8s/job-debug.yaml` reference image tag **1.0.0**; [`k8s/kustomization.yaml`](k8s/kustomization.yaml) for `kubectl apply -k k8s/`.

## MCP server

- **ncc-mcp-server** remains a separate binary; see [docs/MCP_SERVER.md](docs/MCP_SERVER.md). Align orchestrator binary with this release for consistent behavior.
