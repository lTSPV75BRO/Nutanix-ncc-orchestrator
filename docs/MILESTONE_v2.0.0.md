# Milestone summary - v2.0.0

> Status: completed on 2026-05-05 and validated with production + edge-case checks.

This document captures the v2.0.0 release milestone outcomes for the current branch scope.

## Goals

- Finalize a production-ready orchestrator baseline for release tag `v2.0.0`.
- Ensure version consistency across runtime code, packaging, Kubernetes, Helm, and release docs.
- Validate core behavior and edge conditions with reproducible checks.
- Eliminate release confusion by aligning documentation to current branch scope.

## Delivered in v2.0.0

| Theme | Outcome |
|---|---|
| Release alignment | README, changelog, release notes, and readiness docs now point to `v2.0.0` |
| Version surfaces | `VERSION`, default runtime version, Helm chart/values, and k8s image tags aligned to `2.0.0` |
| Quality gate execution | Full test/vet/build suite executed successfully |
| Edge-case validation | Targeted tests for retries, secrets, exclusions, retention, and audit schema all passed |
| Security scanning hygiene | CodeQL workflow updated to branch-appropriate languages (`go`, `actions`) |

## Validation evidence

- `go test ./...`
- `go vet ./...`
- `go build ./...`
- `go build ./cmd/ncc-mcp-server`
- `go test ./... -run "TestDoWithRetryCircuitBreaker|TestValidateSecretsFileHardening|TestFilterBlocksByTitle|TestApplyArtifactRetentionPolicies|TestBindConfigExcludeAlertMatchModeInvalid|TestWriteExcludedAlertsAuditJSONSchemaVersion"`
- `go run . validate-config --config config.yaml`
- `go run . validate-secrets --config config.yaml` (expected failure path without provider)
- `NCC_SECRETS_PROVIDER=env ... go run . validate-secrets --config config.yaml` (pass path)

## Acceptance status

| Acceptance criterion | Status |
|---|---|
| Version and release docs are consistent | ✅ |
| Production checks pass | ✅ |
| Edge-case checks pass | ✅ |
| Deployment manifests/charts reflect release version | ✅ |
| Release artifacts and checklist are documented | ✅ |

## Follow-up (post release)

- Publish GitHub release `v2.0.0` with binaries and `checksums.txt`.
- Confirm Docker tag `prajwalnutant/nutanix-ncc-orchestrator:2.0.0`.
- Run one post-release `--update` smoke test against the published assets.
