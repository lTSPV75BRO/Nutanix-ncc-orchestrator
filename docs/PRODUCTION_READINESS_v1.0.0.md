# Production readiness – v1.0.0

Checklist and status for releasing **Nutanix NCC Orchestrator** v1.0.0 to production.

**Date:** 2026-04-07  
**Version:** 1.0.0

---

## 1. Version and consistency

| Item | Status | Notes |
|------|--------|--------|
| `VERSION` file | ✅ | Set to `1.0.0` |
| `main.Version` default in `goNCC.go` | ✅ | Default `1.0.0` when ldflags not set |
| CHANGELOG.md | ✅ | [1.0.0] section and release link added |
| RELEASE_NOTES_v1.0.0.md | ✅ | Release notes for 1.0.0 |
| k8s/cronjob.yaml image tag | ✅ | `1.0.0` |
| k8s/job-debug.yaml image tag | ✅ | `1.0.0` |
| k8s/README.md image reference | ✅ | Updated to `1.0.0` |
| README.md version example | ✅ | VERSION example `1.0.0` |
| Helm chart `Chart.yaml` | ✅ | Chart and `appVersion` `1.0.0` ([helm/ncc-orchestrator](../helm/ncc-orchestrator/)) |
| GitHub Actions Docker build | ✅ | `main.Stream=Release` and `main.Version` from `VERSION` or release tag |

**Action:** When cutting the release, build with ldflags so `--version` shows `1.0.0` (CI uses `VERSION` from file or release tag).

---

## 2. Build and tests

| Item | Status | Notes |
|------|--------|--------|
| `go build ./...` | ✅ | Passes |
| `go test ./...` | ✅ | Passes (main package tests; MCP server has no tests) |
| MCP server build | ✅ | `go build -o ncc-mcp-server ./cmd/ncc-mcp-server/` succeeds |
| Go version | ✅ | go.mod specifies **1.26.1** |

**Optional:** Add unit tests for MCP server handlers (e.g. `list_run_artifacts`, `get_report`, resource handlers) in a follow-up.

---

## 3. Security

| Item | Status | Notes |
|------|--------|--------|
| Password in logs | ✅ | `maskPassword` used; HTTP dumps redact `Authorization`, `Cookie`, and JSON `password`/`Password` when `log-http` enabled |
| Credentials via env | ✅ | `NCC_PASSWORD` preferred; doc recommends env over config for secrets |
| TLS | ✅ | TLS 1.2 minimum; `insecure-skip-verify` documented for lab/self-signed only |
| MCP password in args | ⚠️ | Tool args can carry password; doc says prefer config/env. No server-side logging of tool args in MCP. |
| Config files | ✅ | No secrets in repo; k8s uses Secret for password |

**Recommendation:** In production, use `NCC_PASSWORD` (or K8s Secret) and avoid passing `password` in MCP tool arguments.

---

## 4. Configuration and validation

| Item | Status | Notes |
|------|--------|--------|
| Config path validation | ✅ | `output-dir-logs`, `output-dir-filtered`, `log-file`, `prom-dir` must be non-empty |
| Preflight | ✅ | Output dirs checked for writability before run |
| Dry run | ✅ | `--dry-run` validates config without running checks |
| Cluster validation | ✅ | Duplicate and invalid cluster entries rejected |

---

## 5. Documentation

| Item | Status | Notes |
|------|--------|--------|
| README | ✅ | Installation, usage, config, K8s, Docker, env vars |
| CHANGELOG | ✅ | Keep a Changelog format; 1.0.0 entry |
| Release notes | ✅ | RELEASE_NOTES_v1.0.0.md |
| MCP server | ✅ | docs/MCP_SERVER.md – tools, resources, Cursor config, troubleshooting |
| K8s | ✅ | k8s/README.md – runbook, image tag, ConfigMap/Secret |
| Release checksums | ✅ | docs/RELEASE_CHECKSUMS.md for `-u` verification |

---

## 6. Kubernetes

| Item | Status | Notes |
|------|--------|--------|
| Exit code 3 (partial) | ⚠️ | v1.0.0 exits **3** if some clusters succeed and some fail. CronJob `successPolicy` / alerting should treat **0** as full success; **3** may be acceptable if partial reports are OK. |
| CronJob image | ✅ | `1.0.0` |
| Debug job image | ✅ | `1.0.0` |
| Concurrency | ✅ | `concurrencyPolicy: Forbid` to avoid overlapping runs |
| Backoff / history | ✅ | `backoffLimit: 1`, job history limits set |
| Secrets | ✅ | Password from Secret; not in ConfigMap |
| PVC | ✅ | Shared storage for logs and outputs; NFS runbook in k8s/README |
| Kustomize / Helm | ✅ | `kubectl apply -k k8s/` ([k8s/kustomization.yaml](../k8s/kustomization.yaml)); Helm chart `helm/ncc-orchestrator` |

**Note:** If using a private or custom image, update `image` and `imagePullPolicy`/secrets as needed.

---

## 7. MCP server

| Item | Status | Notes |
|------|--------|--------|
| Tools | ✅ | run_ncc, discover_clusters, get_run_summary, replay_reports, list_run_artifacts, get_report |
| Resources | ✅ | ncc://run-summary, ncc://report (report URI may need client-side verification in some clients) |
| Stdio transport | ✅ | Suitable for Cursor/Claude Desktop |
| Binary discovery | ✅ | NCC_ORCHESTRATOR_BIN or same-dir or PATH |
| Error handling | ✅ | Tool errors return IsError and message; resources use ResourceNotFoundError when file missing |

**Known limitation:** Some MCP clients may not list or resolve `ncc://report`; `get_report` tool remains the fallback for reading the aggregated HTML.

---

## 8. Release artifacts (maintainers)

Before publishing the GitHub release:

1. **VERSION** – Already `1.0.0`.
2. **Tag:** `v1.0.0`.
3. **Build binaries** for target OS/arch (see docs/RELEASE_CHECKSUMS.md); name e.g. `ncc-orchestrator-linux-amd64`, `ncc-orchestrator-darwin-arm64`, etc.
4. **Checksums:** Generate `checksums.txt` (SHA256) and upload with binaries so `ncc-orchestrator -u` can verify.
5. **Release description:** Paste or adapt RELEASE_NOTES_v1.0.0.md.
6. **Docker:** CI will build and push `prajwalnutant/nutanix-ncc-orchestrator:1.0.0` and `:latest` on push to main or on release (if workflow triggers on release).

---

## 9. Post-release

- [ ] Verify Docker image appears on Docker Hub with tag `1.0.0`.
- [ ] Verify `ncc-orchestrator -u` sees the new version and (if checksums uploaded) verifies download.
- [ ] Update any internal runbooks or automation that pin the image or binary version to 1.0.0.

---

## Summary

| Area | Ready |
|------|--------|
| Version consistency | ✅ |
| Build and tests | ✅ |
| Security (credentials, TLS, redaction) | ✅ |
| Config and validation | ✅ |
| Documentation | ✅ |
| Kubernetes manifests | ✅ |
| MCP server | ✅ |

**Verdict:** v1.0.0 is **production-ready** for release. Proceed with tagging, building release assets, and publishing the GitHub release and Docker image.
