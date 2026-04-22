# Production readiness – v1.1.0

Checklist and status for releasing **Nutanix NCC Orchestrator** v1.1.0 to production.

**Date:** 2026-04-21  
**Version:** 1.1.0

---

## 1. Version and consistency

| Item | Status | Notes |
|------|--------|--------|
| `VERSION` file | ✅ | Set to `1.1.0` |
| `main.Version` default in `goNCC.go` | ✅ | Default `1.1.0` when ldflags not set |
| MCP `serverVersion` default | ✅ | `1.1.0` |
| CHANGELOG.md | ✅ | `1.1.0` release section added |
| RELEASE_NOTES_v1.1.0.md | ✅ | Release notes created |
| k8s image tags | ✅ | `k8s/cronjob.yaml` and `k8s/job-debug.yaml` use `1.1.0` |
| Helm chart/appVersion | ✅ | `helm/ncc-orchestrator/Chart.yaml` set to `1.1.0` |
| Helm values default tag | ✅ | `helm/ncc-orchestrator/values.yaml` tag set to `1.1.0` |

---

## 2. Build and tests

| Item | Status | Notes |
|------|--------|--------|
| `go test ./...` | ✅ | Passes for orchestrator and internal packages |
| `go build ./...` | ✅ | Passes |
| `go build ./cmd/ncc-mcp-server` | ✅ | Passes |
| `validate-config --config config.yaml` | ✅ | Strict validation passes with updated keys |

---

## 3. Security and secrets

| Item | Status | Notes |
|------|--------|--------|
| Secret redaction in logs | ✅ | Existing HTTP/header/password redaction retained |
| Secrets provider support | ✅ | `secret://` refs via `secrets-provider=env|file` and optional `secrets-file` |
| Strict config validation | ✅ | Unknown keys and type mismatches fail fast |
| TLS guardrails | ✅ | Existing TLS warnings and validation retained |

---

## 4. Operational readiness

| Item | Status | Notes |
|------|--------|--------|
| Policy gates | ✅ | CI/CD gating based on run metrics (`policy-gates`) |
| Regression summary | ✅ | `regression-summary.json` retained |
| Drill-down diff | ✅ | `drilldown-diff.json` added |
| Flaky checks | ✅ | `flaky-checks.json` added with lookback controls |
| Health score | ✅ | Per-cluster health score + run aggregate min/avg |
| SLO export | ✅ | `slo-dashboard.json` added |
| Quiet hours/windows | ✅ | Notification suppression during planned windows |
| Run history retention | ✅ | Retention controls retained (`retain-last`, `retain-days`) |

---

## 5. Documentation and config surfaces

| Item | Status | Notes |
|------|--------|--------|
| README | ✅ | New features, config keys, env vars, and artifacts documented |
| CHANGELOG | ✅ | `1.1.0` release notes added |
| Release notes | ✅ | `RELEASE_NOTES_v1.1.0.md` added |
| MCP docs | ✅ | Tool and artifact descriptions updated |
| Root `config.yaml` | ✅ | New options included |
| K8s ConfigMap embedded config | ✅ | New options included |
| Dummy config generation | ✅ | New options included in YAML/JSON templates |

---

## 6. Kubernetes/Helm checks

| Item | Status | Notes |
|------|--------|--------|
| CronJob image tag | ✅ | `1.1.0` |
| Debug job image tag | ✅ | `1.1.0` |
| Helm default image tag | ✅ | `1.1.0` |
| Helm chart version | ✅ | `1.1.0` |
| Exit code semantics | ✅ | 0/1/2/3 unchanged and documented |

---

## 7. Release artifact checklist (maintainers)

1. Tag release: `v1.1.0`.
2. Build binaries for Linux/macOS/Windows targets.
3. Generate and upload `checksums.txt` with binaries (see [docs/RELEASE_CHECKSUMS.md](docs/RELEASE_CHECKSUMS.md)).
4. Publish GitHub release using [RELEASE_NOTES_v1.1.0.md](../RELEASE_NOTES_v1.1.0.md).
5. Verify Docker image `prajwalnutant/nutanix-ncc-orchestrator:1.1.0` is available.
6. Verify `ncc-orchestrator -u` can discover and verify release assets.

---

## Summary

| Area | Ready |
|------|--------|
| Version consistency | ✅ |
| Build and tests | ✅ |
| Security and strict validation | ✅ |
| Operational reporting and gates | ✅ |
| Documentation and configs | ✅ |
| Kubernetes and Helm alignment | ✅ |

**Verdict:** v1.1.0 is production-ready for release.
