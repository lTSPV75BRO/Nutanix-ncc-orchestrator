# v2 Backend + Frontend MVP

This document describes the `v2` split architecture:

- Backend API service: `cmd/ncc-api-server`
- Frontend app: `frontend/` (React + Vite + TypeScript)
- Optional static UI server: `cmd/ncc-ui-server`

## Goals

- GET-heavy API surface for observability and reads
- Mutations allowed only for application settings:
  - config content updates
  - scheduler settings updates
  - run trigger action
- No direct cluster-management API endpoints

## Backend API (MVP)

Base path: `/api/v1`

- `GET /health`
- `POST /auth/session` (loopback bootstrap; returns short-lived bearer token)
- `POST /auth/rotate` (rotate API token)
- `GET /settings/config`
- `PUT /settings/config`
- `GET /settings/notifications`
- `PUT /settings/notifications`
- `POST /settings/notifications/test`
- `GET /schedule`
- `PUT /schedule`
- `GET /runs/summary`
- `GET /runs`
- `GET /runs/active`
- `POST /runs/trigger`
- `GET /report/data`
- `GET /artifacts`
- `GET /artifacts/{name}`
  - add `?download=1` to stream file download
- `GET /logs/runner`

## Run backend

```bash
go run ./cmd/ncc-api-server \
  --listen :8081 \
  --repo-root . \
  --config-path config.yaml \
  --output-dir outputfiles \
  --log-dir nccfiles \
  --orchestrator-bin ./ncc-orchestrator
```

Auth token behavior:

- If `NCC_API_TOKEN` is set, backend uses it.
- If not set, backend generates a secure random token at startup.
- Active token is written to `--token-file-path` (default `.ncc-api-token`) for UI proxy use.

Security defaults (strict):

- `--auth-mode` defaults to `token` (`session` and `hybrid` are supported).
- Wildcard CORS is rejected; set explicit `--cors-origin` allowlist.
- Mutating endpoints require `Content-Type: application/json`.
- Request JSON decoding is strict (unknown fields rejected).
- API server applies request timeouts and run timeout (`--run-timeout`).
- Sensitive debug details are hidden unless `--debug-expose=true`.
- Paths are confined to `--repo-root` and config paths must be `.yaml`/`.yml`.
- Trigger `extra_args` are allowlisted and shell metacharacters are rejected.

## Run frontend

Option A: frontend dev workflow (React/Vite)

```bash
cd frontend
npm install
npm run dev
```

Option B: with UI static server (for built assets)

```bash
cd frontend
npm install
npm run build
```

Then serve:

```bash
go run ./cmd/ncc-ui-server \
  --listen :8080 \
  --dir ./frontend/dist \
  --backend-url http://localhost:8081 \
  --api-token-file .ncc-api-token \
  --api-auth-mode token \
  --allowed-origins http://localhost:8080
```

Option C: any static web server (example)

```bash
python3 -m http.server 8080 --directory frontend/dist
```

Open: `http://localhost:8080`

## Notes

- `PUT /settings/config` always runs strict validation and then writes config:
  - `ncc-orchestrator validate-config --config <tmp-file>`
- Schedule API persists state in file-backed `.ncc-api-schedule.json`.
- Notifications API persists state in file-backed `.ncc-api-notifications.json`.
- `PUT /settings/notifications` supports optional integrations:
  - Slack incoming webhook
  - generic webhook
  - SMTP email
- Notification events: run success, run failure, and policy violations (`policy-gates.txt`).
- Notification responses include per-channel delivery status (`last_delivery`) for Slack/webhook/email.
- `POST /settings/notifications/test` can send manual channel tests (`all|slack|webhook|email`).
- `PUT /schedule` supports `apply=true` to invoke:
  - `ncc-orchestrator create-schedule ...`
- `POST /runs/trigger` starts orchestrator process asynchronously.
- Frontend calls `/api/v1/*` via `ncc-ui-server` reverse proxy; token is injected server-side.
- `ncc-ui-server` can inject either API token (`--api-auth-mode token`) or short-lived session bearer (`--api-auth-mode session`).
- Trigger status hides command/cwd/env unless API server is started with `--debug-expose=true`.
- Live Logs panel reads `logs/ncc-runner.log` via `GET /logs/runner`.
- Report dashboard data is exposed through `GET /report/data` aggregating:
  - `run-summary.json`
  - `checks-snapshot.json`
  - `drilldown-diff.json`
  - `flaky-checks.json`
  - `regression-summary.json`
  - `slo-dashboard.json`
  - `policy-gates.txt` (as `policy_violations`)

## API discoverability and explorer

`v2` is designed to be self-discoverable for UI and automation clients:

- OpenAPI spec endpoint: `GET /api/v1/openapi.json`
- Route metadata endpoint: `GET /api/v1/meta/routes`
- Frontend API Explorer consumes OpenAPI first and falls back to route metadata.

Recommended integration order for external clients:

1. Fetch `/api/v1/openapi.json` and generate/validate client contracts.
2. Use `/api/v1/meta/routes` for lightweight route-health checks.
3. Validate auth mode (`token`, `session`, or `hybrid`) before invoking mutations.

## Config-referenced file lifecycle

`v2` allows editing selected config-referenced files through API + UI:

- clusters source files (for `clusters-file`)
- alert exclusion list files (for `exclude-alert-titles-file`)
- secrets map files (for `secrets-file`, when provider is `file`)

Guardrails:

- Content is validated before save (format + semantic checks).
- `log-file` is intentionally excluded from editable config-referenced files.
- Path handling is repo-root constrained to reduce accidental path traversal.

## Preflight and execution model

Preflight checks are executed by the `ncc-orchestrator` binary, not by backend-native cluster logic.

- UI action: `Preflight Check` button calls backend endpoint.
- Backend delegates to: `ncc-orchestrator preflight-check`.
- CLI default run path performs preflight unless `--skip-preflight-check` is set.

Preflight includes:

- `validate-config`
- `validate-secrets`
- output/log/prom directory write probes via `.ncc-prefight-check`
- safety advisories (for example insecure TLS, HTTP payload logging, high parallelism)
- machine-readable `remediation_code` on non-pass checks for UI/automation fix mapping

Preflight JSON contract (abridged):

```json
{
  "ok": false,
  "checks": [
    {
      "id": "validate-secrets",
      "status": "fail",
      "remediation_code": "NCC_PREFLIGHT_VALIDATE_SECRETS"
    }
  ]
}
```

## Upgrade path: v1 to v2

By design, updater behavior is major-track aware:

- `v1.x` binaries update to latest `v1.x` by default.
- To move from `v1` to `v2`, user must explicitly enable major upgrade.

### Important: what `ncc-orchestrator update` does (and does not do)

- `ncc-orchestrator update` updates only the `ncc-orchestrator` CLI binary.
- It does **not** auto-install or auto-start:
  - `cmd/ncc-api-server`
  - `cmd/ncc-ui-server`
  - `frontend` dependencies/build output
- For `v2` UI/API usage, you must install/run backend and UI components separately.

### Easy install paths for users

#### Path A: CLI-only user (keep using v1-style flow)

If you only need CLI runs and reports:

1. Check updates:
   `ncc-orchestrator update --check`
2. Stay on same major track by default (`v1.x` -> latest `v1.x`).
3. Apply update:
   `ncc-orchestrator update`

No backend/frontend installation is required for this path.

#### Path B: Move to full v2 stack (API + UI + frontend)

Use this when you want web UI, API endpoints, API explorer, and settings management.

1. Upgrade CLI with explicit major opt-in:
   `ncc-orchestrator update --check --allow-major-upgrade`
   `ncc-orchestrator update --allow-major-upgrade`
2. Bootstrap v2 binaries/artifacts (no `go run`, no `npm build`):
   `ncc-orchestrator v2-bootstrap --check`
   `ncc-orchestrator v2-bootstrap`
   - Preferred release asset style is a single `ncc-v2-stack-<os>-<arch>` bundle.
3. Start both services together:
   `ncc-orchestrator v2-start`
4. Open:
   `http://localhost:8080`

Optional flags for customized bootstrap:

- `--version 2.0.0` to pin release version
- `--install-dir /opt/ncc-v2` to install in custom location
- `--api-listen :18081` and `--ui-listen :18080` for custom ports
- `--orchestrator-bin /usr/local/bin/ncc-orchestrator` for explicit CLI binary path
- `--repo owner/repo` or `--repo https://github.com/owner/repo` for alternate release source

`v2-start` can also be customized with similar flags:

- `ncc-orchestrator v2-start --install-dir /opt/ncc-v2`
- `ncc-orchestrator v2-start --api-listen :18081 --ui-listen :18080`
- `ncc-orchestrator v2-start --config-path /etc/ncc/config.yaml`

Fallback (manual source workflow):

If release assets for your OS/arch are missing, use the source workflow documented in `Run backend` and `Run frontend` sections above.

### One-line answer for migration question

If you are migrating from `v1` to `v2`: the updater can upgrade the CLI binary, but you still need to install/run backend + frontend services manually.

Migration checklist:

1. Stand up `cmd/ncc-api-server` and `cmd/ncc-ui-server`.
2. Validate token/session auth mode, CORS allowlist, and reverse proxy wiring.
3. Confirm config compatibility with `validate-config` and `preflight-check`.
4. Verify report/artifact/log endpoints on `/api/v1/*`.
5. Update automation from CLI-only workflows to API/UI-assisted workflows as needed.
6. Keep rollback-ready `v1` binary and config snapshots until cutover is complete.

## Operational hardening checklist (recommended)

- Use explicit `--cors-origin` values; do not rely on permissive defaults.
- Keep `--debug-expose=false` in production.
- Keep API route limiting enabled (`--rate-limit-per-minute`, default `60`) for sensitive auth/mutation routes.
- Rotate API tokens regularly (`POST /auth/rotate`).
- Restrict filesystem access to minimum required `--repo-root`.
- Enable retention controls for run-history and generated artifacts.
- Run preflight checks in CI/CD before applying schedule or release changes.
- Keep API Explorer external URL mode disabled by default unless explicitly needed.

## Kubernetes deployment process

For Kubernetes, use `k8s/` as the canonical entrypoint to deploy full v2 components:

```bash
kubectl apply -k k8s/
```

`k8s/` (internally sourcing v2 manifests) includes:

- `runner-cronjob.yaml`: scheduled runner execution
- `api-deployment.yaml` + `api-service.yaml`: backend API
- `ui-deployment.yaml` + `ui-service.yaml`: UI server + frontend hosting/proxy
- `configmap.yaml`, `secret.yaml`, `pvc.yaml`: shared config/secrets/state
- NetworkPolicy set:
  - default deny ingress
  - UI ingress allow (port 8080)
  - API ingress allow only from UI pods (port 8081)

Important:

- Update image names in `api-deployment.yaml` and `ui-deployment.yaml` to your published v2 images.
- API pod must include `ncc-api-server` + `ncc-orchestrator`.
- UI pod must include `ncc-ui-server` + built frontend files at `/app/frontend/dist`.


## Migration runbook

For operational migration steps, see [MIGRATION_v1_TO_v2.md](./MIGRATION_v1_TO_v2.md).
