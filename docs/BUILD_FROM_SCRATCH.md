# Build From Scratch Guide

This guide is for operators and contributors who want to build, run, test, and deploy the full NCC v2 stack from a clean machine.

If you are taking over engineering ownership, read this first:

- `docs/ARCHITECTURE_AND_HANDOVER.md`

It covers:

- Local prerequisites
- Source build of all binaries
- Frontend build
- Running CLI-only and full v2 stack
- Validation and tests
- Packaging and release basics
- Kubernetes deployment
- developer takeover context

---

## 1) What you are building

The repository contains four main runtime components:

- `ncc-orchestrator` (core runner CLI; executes NCC checks and writes artifacts)
- `ncc-api-server` (backend API for config/run/schedule/report endpoints)
- `ncc-ui-server` (UI proxy + static file host for frontend)
- `frontend` (React + Vite TypeScript app)

For deeper code architecture and ownership boundaries, see:

- `docs/ARCHITECTURE_AND_HANDOVER.md`

Supporting assets:

- Kubernetes manifests under `k8s/`
- Helm chart (CronJob-focused) under `helm/ncc-orchestrator/`
- Scripts under `scripts/`
- Extended references under `docs/`

---

## 2) Prerequisites

Install these first:

- Go (use version required by `go.mod`)
- Node.js + npm (for frontend build/tests)
- Git
- `kubectl` (for Kubernetes deployment flow)
- Optional: Docker (image build/push workflows)

Environment/access prerequisites:

- Reachable Nutanix Prism endpoint(s)
- Valid Prism credentials
- Optional Prism Central access for `discover-clusters`
- For secure production use: TLS certificates and secret-management plan

---

## 3) Clone and inspect repository

```bash
git clone https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator.git
cd Nutanix-ncc-orchestrator
git checkout v2.1.1
```

Sanity-check important paths:

```bash
ls -la
ls -la cmd
ls -la docs
ls -la k8s
```

---

## 4) Build binaries from source

Build all Go binaries:

```bash
go build ./...
```

Build named binaries explicitly:

```bash
go build -o ncc-orchestrator .
go build -o ncc-api-server ./cmd/ncc-api-server
go build -o ncc-ui-server ./cmd/ncc-ui-server
go build -o ncc-mcp-server ./cmd/ncc-mcp-server
```

Optional release-style build metadata for `ncc-orchestrator`:

```bash
go build -ldflags "-w -s \
  -X main.BuildDate=$(date -u '+%Y-%m-%dT%H:%M:%SZ') \
  -X main.Stream=Release \
  -X main.GoVersion=$(go version | cut -d ' ' -f 3)" \
  -o ncc-orchestrator .
```

Verify binaries:

```bash
./ncc-orchestrator version
./ncc-api-server --help
./ncc-ui-server --help
```

---

## 5) Build frontend from source

```bash
cd frontend
npm install
npm run build
cd ..
```

For frontend development mode:

```bash
cd frontend
npm run dev
```

---

## 6) Create a first runnable config

Start from the example:

```bash
cp dist/example_config.yaml config.yaml
```

Edit at minimum:

- `clusters` or `clusters-file`
- `username`
- `ncc-api-version` / `nutanix-v4-api-version`
- output/log/prom directories if you need custom paths

Set password via env (recommended):

```bash
export NCC_PASSWORD='REPLACE_ME'
```

Validate config + secrets:

```bash
./ncc-orchestrator validate-config --config config.yaml
./ncc-orchestrator validate-secrets --config config.yaml
```

If using `secret://` values, configure:

- `secrets-provider: env` with matching env vars, or
- `secrets-provider: file` and `secrets-file`

---

## 7) Run CLI-only mode first

This is the quickest end-to-end sanity check:

```bash
./ncc-orchestrator --config config.yaml
```

Outputs to verify:

- `outputfiles/index.html`
- `outputfiles/run-summary.json`
- `outputfiles/ncc-run-record.json`
- `nccfiles/*.log`

Optional replay run:

```bash
./ncc-orchestrator --config config.yaml --replay
```

---

## 8) Run full v2 stack locally

### 8.1 Start API server

```bash
./ncc-api-server \
  --listen :8081 \
  --repo-root . \
  --config-path ./config.yaml \
  --output-dir ./outputfiles \
  --log-dir ./nccfiles \
  --runner-log-path ./logs/ncc-runner.log \
  --orchestrator-bin ./ncc-orchestrator \
  --auth-mode token \
  --cors-origin http://localhost:8080
```

Health check:

```bash
curl -sS http://localhost:8081/api/v1/health
```

### 8.2 Start UI server with built frontend

```bash
./ncc-ui-server \
  --listen :8080 \
  --dir ./frontend/dist \
  --backend-url http://localhost:8081 \
  --api-token-file ./.ncc-api-token \
  --api-auth-mode token \
  --allowed-origins http://localhost:8080
```

Open `http://localhost:8080`.

### 8.3 Trigger run through API

```bash
curl -sS -X POST "http://localhost:8081/api/v1/runs/trigger" \
  -H "Authorization: Bearer $(cat .ncc-api-token)" \
  -H "Content-Type: application/json" \
  -d '{"config_path":"config.yaml"}'
```

---

## 9) Run automated test suite

Backend and CLI:

```bash
go test ./...
go vet ./...
```

API-specific:

```bash
go test ./cmd/ncc-api-server
```

Frontend:

```bash
cd frontend
npm test
npm run build
cd ..
```

Optional race tests:

```bash
go test -race ./...
```

---

## 10) Build artifacts for release

Typical release maintainer steps:

1. Ensure `VERSION` matches intended release
2. Run validation suite (`go test`, `go vet`, `go build`, frontend tests/build)
3. Build platform binaries
4. Generate `checksums.txt` (see `docs/RELEASE_CHECKSUMS.md`)
5. Update docs/release notes/changelog
6. Tag release and publish assets

Use:

- `CHANGELOG.md`
- `RELEASE_NOTES_v2.1.1.md`
- `docs/RELEASE_CHECKSUMS.md`

---

## 11) Kubernetes deployment from source-built understanding

Use this when you want the full stack in cluster (runner + API + UI):

```bash
kubectl apply -k k8s/
```

Before apply:

- Update images in:
  - `k8s/runner-cronjob.yaml`
  - `k8s/api-deployment.yaml`
  - `k8s/ui-deployment.yaml`
- Set values in:
  - `k8s/configmap.yaml`
  - `k8s/secret.yaml`

Post-deploy verification:

```bash
kubectl get all -n ncc-orchestrator-v2
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-api --tail=100
kubectl logs -n ncc-orchestrator-v2 deploy/ncc-v2-ui --tail=100
kubectl get cronjob -n ncc-orchestrator-v2
```

Create a manual run from cronjob:

```bash
kubectl create job -n ncc-orchestrator-v2 ncc-v2-manual-1 --from=cronjob/ncc-v2-runner
kubectl logs -n ncc-orchestrator-v2 job/ncc-v2-manual-1 --all-containers=true
```

See `k8s/README.md` for the full operations runbook.

---

## 12) Common pitfalls (and fixes)

- Binary path mismatch in API:
  - Ensure `--orchestrator-bin` points to an executable path.
- Path confinement startup errors:
  - Keep API paths within `--repo-root`, or set `--repo-root /` for absolute container paths.
- Missing auth token:
  - Check `.ncc-api-token` generation and UI `--api-token-file`.
- Frontend loads but API fails:
  - Verify UI `--backend-url`, API CORS allowlist, and auth mode consistency.
- No metrics in Prometheus:
  - Verify `prom-enabled`, `prom-dir`, and node_exporter textfile collector setup.

---

## 13) Recommended next docs

- `README.md` (overall quickstart and feature map)
- `docs/FEATURES_AND_CONFIG_FLAGS.md` (full flag/config reference)
- `docs/V2_BACKEND_FRONTEND_MVP.md` (API/UI architecture details)
- `docs/MIGRATION_v1_TO_v2.md` (v1 to v2 cutover)
- `k8s/README.md` (Kubernetes deployment + operations)
- `Prometheus.md` (monitoring and alerting)
