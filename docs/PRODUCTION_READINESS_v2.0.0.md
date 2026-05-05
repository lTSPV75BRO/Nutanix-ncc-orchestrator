# Production readiness - v2.0.0

Checklist and status for releasing **Nutanix NCC Orchestrator** v2.0.0 to production.

**Date:** 2026-05-05  
**Version:** 2.0.0

---

## 1. Version and consistency

| Item | Status | Notes |
|------|--------|--------|
| `VERSION` file | ✅ | Set to `2.0.0` |
| `main.Version` default in `goNCC.go` | ✅ | Default `2.0.0` when ldflags not set |
| CHANGELOG.md | ✅ | `2.0.0` release section added |
| RELEASE_NOTES_v2.0.0.md | ✅ | Release notes created |
| README release status | ✅ | Points to `v2.0.0` release/readiness docs |
| k8s image tags | ✅ | `k8s/cronjob.yaml` and `k8s/job-debug.yaml` use `2.0.0` |
| Helm chart/appVersion | ✅ | `helm/ncc-orchestrator/Chart.yaml` set to `2.0.0` |
| Helm values default tag | ✅ | `helm/ncc-orchestrator/values.yaml` tag set to `2.0.0` |

---

## 2. Build and tests

| Item | Status | Notes |
|------|--------|--------|
| `go test ./...` | ✅ | Passed |
| `go vet ./...` | ✅ | Passed |
| `go build ./...` | ✅ | Passed |
| `go build ./cmd/ncc-mcp-server` | ✅ | Passed |
| `validate-config --config config.yaml` | ✅ | Passed |

---

## 3. Edge-case checks executed (2026-05-05)

| Scenario | Result | Evidence |
|------|--------|--------|
| Retry circuit breaker opens on repeated retryable failures | ✅ | `TestDoWithRetryCircuitBreaker` |
| Secrets-file hardening rejects unsafe file conditions | ✅ | `TestValidateSecretsFileHardening` |
| Alert exclusion match modes (`exact\|contains\|regex`) | ✅ | `TestFilterBlocksByTitle` |
| Invalid exclusion regex validation | ✅ | `TestBindConfigExcludeAlertMatchModeInvalid` |
| Artifact retention policy prune/keep behavior | ✅ | `TestApplyArtifactRetentionPolicies` |
| Exclusion audit schema stamping | ✅ | `TestWriteExcludedAlertsAuditJSONSchemaVersion` |
| Secrets preflight without provider | ✅ | `validate-secrets` fails fast as expected |
| Secrets preflight with env provider | ✅ | `NCC_SECRETS_PROVIDER=env ... validate-secrets` passes |

---

## 4. Security and operational readiness

| Item | Status | Notes |
|------|--------|--------|
| Secret redaction and guardrails | ✅ | Existing masking and strict validations retained |
| Secrets preflight UX | ✅ | `validate-secrets` command available for CI/manual checks |
| Retry resilience controls | ✅ | Backoff + circuit breaker (`retry-circuit-breaker`) |
| Failure classification | ✅ | Machine-readable `error_class` and `failure_classes` |
| Policy gates | ✅ | Supports CI/CD thresholds for quality enforcement |
| Artifact retention | ✅ | Supports age/count cleanup in output directory |
| Exclusion audit artifact | ✅ | `excluded-alerts.json` with schema metadata |

---

## 5. Documentation and release surfaces

| Item | Status | Notes |
|------|--------|--------|
| README | ✅ | Current release pointers and scope note updated |
| Feature/flag reference | ✅ | `docs/FEATURES_AND_CONFIG_FLAGS.md` versioned for `v2.0.0` |
| Changelog | ✅ | `2.0.0` section and release link added |
| Release notes | ✅ | `RELEASE_NOTES_v2.0.0.md` added |
| K8s docs | ✅ | Image-tag reference updated to `2.0.0` |
| Helm docs | ✅ | Example install tag updated to `2.0.0` |
| Checksum docs | ✅ | Release example aligned to `v2.0.0` |

---

## 6. Release artifact checklist (maintainers)

1. Tag release: `v2.0.0`.
2. Build binaries for Linux/macOS/Windows targets.
3. Generate and upload `checksums.txt` with binaries (see [docs/RELEASE_CHECKSUMS.md](./RELEASE_CHECKSUMS.md)).
4. Publish GitHub release using [RELEASE_NOTES_v2.0.0.md](../RELEASE_NOTES_v2.0.0.md).
5. Verify Docker image `prajwalnutant/nutanix-ncc-orchestrator:2.0.0` is available.
6. Verify `ncc-orchestrator -u` can discover and verify release assets.

---

## Summary

| Area | Ready |
|------|--------|
| Version consistency | ✅ |
| Build and tests | ✅ |
| Edge-case checks | ✅ |
| Security and operational controls | ✅ |
| Documentation and release assets | ✅ |

**Verdict:** v2.0.0 is production-ready for release.
