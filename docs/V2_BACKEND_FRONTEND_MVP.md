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
