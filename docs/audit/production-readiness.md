# Production readiness

A successful local build and test run is not proof of production readiness.
This assessment covers what can be judged from the repository; deployed
environments, real Prism endpoints, and production configuration were not
available.

## Checklist

| Area | Status | Evidence / gap |
| --- | --- | --- |
| Reproducible builds | ✅ | `go.sum` verified (`go mod verify`), `npm ci` from lockfile, pinned toolchains (Go 1.27.3, Node 26.12.1), version stamped via ldflags |
| Artifact integrity & provenance | ✅ | SBOM + `attest-build-provenance` on release; `checksums.txt`; updater verifies checksums; release signing helper (`releasesign.go`) |
| Container hardening | ✅ | Alpine 3.24, UID 10001, `runAsNonRoot`, `allowPrivilegeEscalation: false`, CA bundle upgraded at build; NetworkPolicies default-deny ingress + explicit egress; PDB |
| Least-privilege CI | ✅ | `contents: read` default; release-only `id-token`/`attestations`; no `pull_request_target`; publishing job without caches |
| Secrets injection | ✅ / ⚠ | Kubernetes Secrets for token/JWT/backup/master keys; ⚠ etcd encryption must be confirmed per cluster; ⚠ user-store envelope encryption optional by default |
| Config validation at startup | ✅ | `validatePathConfig`, orchestrator binary checks, `--require-encrypted-user-store`; runner `validate-config` / `preflight-check` |
| Health checks | ✅ | Readiness uses `/api/v1/health/ready` (user store, runner binary, output volume); liveness stays on `/api/v1/health` |
| Graceful shutdown | ✅ (new) | 20 s drain on SIGTERM in API and UI servers (REL-03) |
| Rolling deployment | ✅ | Stateless HTTP tier with shared JWT secret; user-store writes reload first and are conditional on the Secret's resourceVersion, so replicas cannot overwrite each other |
| Backward compatibility | ✅ / ⚠ | Mostly additive: new optional JSON field (`deleted_accounts`), new opt-in flags/env vars, stricter run-argument validation for flags that never worked. ⚠ One intentional behaviour change: anonymous admin self-reset is off by default (`--allow-admin-self-reset` restores it); called out in the changelog's upgrade notes |
| Database migrations | n/a | File-based state; the user-store document gains an optional field that older versions ignore |
| Rollback | ✅ | Pre-update backups and in-app rollback; restores are transactional and roll back on failure |
| Windows | ✅ (new) | Liveness/identity and secrets-file fixes; Windows CI job added (not yet run on GitHub-hosted runners) |
| Observability | ✅ | Domain metrics, per-route HTTP metrics, request IDs in responses and audit lines |
| Security scanning | ✅ | govulncheck, npm audit, Trivy, CodeQL in CI; ⚠ dev-dependency advisories pending npm update (DEP-02) |
| Feature flags for risky changes | ✅ | Optional views behind admin flags; risky behaviours are opt-in (`--allow-admin-self-reset`, `NCC_MCP_ALLOW_SCHEDULE_COMMAND`) |

## Deployment-time checks (per environment)

1. `ncc-orchestrator v2-check` / `doctor` on the host or in the API pod.
2. `GET /api/v1/health` as admin: confirm `users_store_protection`,
   `backup_encryption_required`, `backup_server_key_configured`.
3. Narrow `--trusted-proxy-cidrs` to the real proxy/ingress/UI pod CIDRs.
4. Make sure operators know the admin recovery path (`v2-reset-password
   --user admin` on the host / in the API pod); enable `--allow-admin-self-reset`
   only where the login page is unreachable by untrusted clients.
5. Configure a backup key, schedule daily encrypted backups, copy them off-host,
   and do a restore drill into a scratch install.
6. Kubernetes: confirm etcd encryption for Secrets; set `NCC_JWT_SECRET`
   identically on all API replicas; check `NO_PROXY` covers the API Service
   if pods have `HTTP(S)_PROXY` (SEC-08).
7. Alerting on the signals in [`reliability-and-observability.md`](reliability-and-observability.md).

## Verdict

**Suitable for production use, including the Helm default of two API
replicas**, on Linux or Windows, once the npm dev-tool updates are applied and
the deployment-time checks above are done. This rests on code review, unit and
integration tests, and local smoke tests; it is not a substitute for a staging
soak, a restore drill, and a multi-replica test in the target environment.
