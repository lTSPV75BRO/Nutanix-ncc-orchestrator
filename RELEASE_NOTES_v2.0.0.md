# Release notes - v2.0.0

**Date:** 2026-05-05

This release finalizes the v2.0.0 production baseline for the Go orchestrator runtime and aligns release/version assets across docs, manifests, and packaging.

## Highlights

- **Production verification completed**: full validation run executed on branch `v2.0.0` (`go test ./...`, `go vet ./...`, `go build ./...`, and `go build ./cmd/ncc-mcp-server`).
- **Edge-case validation completed**: targeted tests for exclusion modes, regex validation, retention behavior, secrets hardening, and retry circuit breaker passed.
- **Release/documentation alignment**: README release pointers, feature references, changelog, and release docs now target `v2.0.0`.
- **Version consistency updates**: default binary version, Helm chart/appVersion, Helm values image tag, and Kubernetes image tags now use `2.0.0`.
- **CodeQL workflow fix**: repository CodeQL scan now targets branch-appropriate languages (`go`, `actions`), avoiding JS/TS "no source found" failures for this scope.
- **API hardening uplift**: added per-client rate limiting for sensitive auth/mutation routes (`--rate-limit-per-minute`, default `60`).
- **API/UI/proxy stack hardening**: API and UI server security headers normalized, stricter token/session handling, safer forwarded-header behavior, and route-level proxy restrictions for `/api/v1/*`.
- **Preflight automation contract**: `preflight-check` JSON now includes machine-readable `remediation_code` on non-pass checks.
- **Frontend/API Explorer hardening**: sensitive request headers/body are no longer persisted in browser storage; external absolute URLs are blocked by default.
- **Kubernetes network hardening**: added default-deny ingress NetworkPolicy plus scoped UI/API ingress policies.

## Scope note for v2.0.0

This release line includes the orchestrator runtime plus the v2 API/UI and frontend components:

- `cmd/ncc-api-server`
- `cmd/ncc-ui-server`
- `frontend`

## Validation snapshot (2026-05-05)

- `go test ./...` -> pass
- `go vet ./...` -> pass
- `go build ./...` -> pass
- `go build ./cmd/ncc-mcp-server` -> pass
- `go test ./... -run "TestDoWithRetryCircuitBreaker|TestValidateSecretsFileHardening|TestFilterBlocksByTitle|TestApplyArtifactRetentionPolicies|TestBindConfigExcludeAlertMatchModeInvalid|TestWriteExcludedAlertsAuditJSONSchemaVersion"` -> pass
- `go run . validate-config --config config.yaml` -> pass
- `go run . validate-secrets --config config.yaml` -> expected failure when `secret://` refs are present without a configured provider
- `NCC_SECRETS_PROVIDER=env ... go run . validate-secrets --config config.yaml` -> pass
- `go test -race ./...` -> pass
- `npm test` -> pass
- `npm run build` -> pass
- `kubectl kustomize k8s` -> pass

## Upgrade notes from v1.1.0

- Update image/binary tag to `2.0.0`.
- Use [docs/PRODUCTION_READINESS_v2.0.0.md](docs/PRODUCTION_READINESS_v2.0.0.md) as the release gate checklist.
- Use this file as the GitHub release description body for tag `v2.0.0`.

## Artifacts and deployment versions

- **Orchestrator version:** `2.0.0`
- **MCP server version:** `2.0.0`
- **Docker image:** `prajwalnutant/nutanix-ncc-orchestrator:2.0.0`
- **Helm chart:** `helm/ncc-orchestrator` chart/appVersion `2.0.0`
- **Kubernetes manifests:** full v2 stack under `k8s/` (`runner-cronjob`, `api-deployment`, `ui-deployment`) plus NetworkPolicies.

## GitHub release checklist (v2.0.0)

1. Ensure `VERSION` is `2.0.0`.
2. Ensure changelog `2.0.0` section is present in [CHANGELOG.md](CHANGELOG.md).
3. Build and attach binaries for Linux/macOS/Windows targets.
4. Generate and attach `checksums.txt` (see [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md)).
5. Tag as `v2.0.0`.
6. After publish, verify:
   - release assets are downloadable
   - Docker tag `prajwalnutant/nutanix-ncc-orchestrator:2.0.0` exists
   - `ncc-orchestrator update --check` can discover and verify assets
