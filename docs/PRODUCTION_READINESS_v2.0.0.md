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
| k8s image tags | ✅ | `k8s/api-deployment.yaml`, `k8s/ui-deployment.yaml`, and `k8s/runner-cronjob.yaml` use `2.0.0` |
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

## 7. Late-cycle hardening verification (2026-05-27)

This pass re-validated the full v2.0.0 stack after a late-cycle hardening sprint covering security, accessibility, theming, and API correctness.

| Gate | Result | Evidence |
|------|--------|----------|
| Go stdlib vulnerability scan | ✅ | `govulncheck ./...` → **No vulnerabilities found** after `go 1.26.2 → 1.26.3` toolchain bump (closes `GO-2026-4976`, `GO-2026-4971`, `GO-2026-4918`, and 2 related stdlib CVEs) |
| npm vulnerability scan | ✅ | `npm audit --omit=dev` → **found 0 vulnerabilities** after pinning `dompurify ^3.4.7` via `package.json#overrides` and patching `yaml` |
| Secret scan | ✅ | No hardcoded credentials; only template placeholders, K8s `secretKeyRef`, and test fixtures |
| Race-enabled Go tests | ✅ | `go test -count=1 -race -timeout=180s ./...` passes (`goncc`, `goncc/cmd/ncc-api-server`, `goncc/internal/kblinks`) |
| `go vet ./...` | ✅ | clean |
| `gofmt -l .` | ✅ | clean (reformatted two test files during the pass) |
| Frontend type-check (`tsc --noEmit`) | ✅ | clean |
| Frontend production build (`vite build`) | ✅ | clean |
| Auth enforcement | ✅ | All protected routes return `401` for missing/wrong tokens; constant-time compare via `crypto/subtle` |
| Method gating | ✅ | `DELETE/POST/PATCH` on read-only routes returns `405` with structured envelope |
| Input validation | ✅ | `audit?limit=-5/abc` → `400`; `runs?since=garbage` → `400`; `runs?source=invalid` → `400`; `schedule PUT {}` (action=create without cron/every) → `400` |
| Path traversal | ✅ | `/api/v1/artifacts/..%2F…`, `/sub%2F…`, `/runs/..%2F…`, `/runs/sub%2F…` → `400 invalid …` (defense-in-depth: canonicalized variants still resolve to `404`) |
| CORS posture | ✅ | Default `cors-origin=http://localhost:8080` rejects unknown origins with `403`; allowed origin returns `204` with `Allow-Methods: GET, PUT, POST, DELETE, OPTIONS` |
| Security headers | ✅ | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Permissions-Policy`, `Content-Security-Policy` (UI: `script-src 'self'`, API: `default-src 'none'`), `Strict-Transport-Security` on TLS |
| Cancel-active-run | ✅ | `DELETE /api/v1/runs/active` → `409 NCC_API_CONFLICT` when no run active; documented in `meta/routes` |
| Run-by-id | ✅ | `GET /api/v1/runs/{id}` → `404` for missing id, `400` for `..`/`/` |
| Structured error envelopes | ✅ | All error responses include `success=false`, `error`, and `error_code` (`NCC_API_UNAUTHORIZED`, `NCC_API_BAD_REQUEST`, `NCC_API_NOT_FOUND`, `NCC_API_CONFLICT`, `NCC_API_FORBIDDEN`) |
| Concurrent run prevention | ✅ | Backend single-flight gate returns `409` on second trigger while first is active |

### Smoke + edge-case suite

A 57-check end-to-end suite against a live `ncc-api-server` covered public/protected endpoints, token + method + input + path-traversal hardening, CORS preflight (allowed and forbidden origins), security headers, structured error codes, the cancel-active endpoint, and concurrent-trigger behavior. All real-behavior checks passed. See `RELEASE_NOTES_v2.0.0.md` § "Release-readiness validation (2026-05-27)".

---

## Summary

| Area | Ready |
|------|--------|
| Version consistency | ✅ |
| Build and tests | ✅ |
| Edge-case checks | ✅ |
| Security and operational controls | ✅ |
| Documentation and release assets | ✅ |
| Late-cycle hardening (2026-05-27) | ✅ |

**Verdict:** v2.0.0 is production-ready for release.
