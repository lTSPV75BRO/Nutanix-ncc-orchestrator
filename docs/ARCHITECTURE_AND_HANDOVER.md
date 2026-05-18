# Architecture and Developer Handover

This document is the ground-up technical handover for engineers taking ownership of the project.

Use this together with:

- `docs/BUILD_FROM_SCRATCH.md` (environment setup and execution)
- `docs/FEATURES_AND_CONFIG_FLAGS.md` (complete flags and config contract)
- `k8s/README.md` (cluster deployment and runbook)

---

## 1) System overview

NCC v2 is a split system with one execution core and two service layers:

- **Execution core**: `ncc-orchestrator` (runner)
- **Control plane API**: `ncc-api-server`
- **UI proxy + static host**: `ncc-ui-server`
- **Web app**: `frontend`

High-level flow:

1. User interacts with UI at `ncc-ui-server`.
2. UI server proxies `/api/v1/*` to `ncc-api-server`, injecting token/session auth.
3. API server validates request, resolves paths, and executes `ncc-orchestrator` for run/preflight/schedule flows.
4. Runner talks to Prism APIs, parses results, writes artifacts (`outputfiles`, `nccfiles`, `.prom`).
5. API and UI read artifacts and surface run/report state.

---

## 2) Repository architecture map

Top-level ownership map:

- `goNCC.go`
  - Main CLI entry and runtime orchestration logic
  - Subcommands (`discover-clusters`, `v2-start`, `quickstart`, `validate-*`, `preflight-check`)
  - Output generation and policy/notification/metrics logic
- `goNCC_test.go`
  - Core test coverage for runner behavior and contracts
- `cmd/ncc-api-server/`
  - API routes, auth, path confinement, run trigger, config/schedule/notification persistence
- `cmd/ncc-ui-server/`
  - Reverse proxy to API, token/session auth injection, static asset hosting
- `frontend/`
  - React app (dashboard, runs, settings, API explorer, report views)
- `k8s/`
  - Production deployment manifests (runner CronJob + API + UI + PVC + policies)
- `docs/`
  - Operator and developer references

---

## 3) Runtime component responsibilities

### 3.1 `ncc-orchestrator` (runner)

Primary responsibilities:

- Reads config from file/env/flags (with precedence)
- Resolves cluster targets (direct list or `clusters-file`)
- Optional cluster discovery via Prism Central (`discover-clusters`)
- Executes NCC checks, polls status, parses check output
- Writes artifacts:
  - `run-summary.json`
  - `ncc-run-record.json`
  - `index.html`, per-cluster reports, CSV/JSON/Markdown/SARIF
  - trend/drilldown/slo/policy outputs
  - Prometheus textfile metrics
- Handles retries/adaptive parallelism/policy gates/notifications
- Exposes bootstrap flows (`quickstart`, `v2-bootstrap`, `v2-start`, `v2-stop`)

### 3.2 `ncc-api-server`

Primary responsibilities:

- Exposes `/api/v1/*` for UI and automation clients
- Auth modes: `token`, `session`, `hybrid`
- Path confinement under `--repo-root`
- Delegates execution tasks to runner binary:
  - `validate-config`
  - `preflight-check`
  - run trigger
  - schedule apply
- Serves report/artifact/trend/run state from filesystem artifacts
- Persists settings metadata:
  - schedule state
  - notification state
  - API token file

### 3.3 `ncc-ui-server`

Primary responsibilities:

- Hosts frontend static assets
- Proxies `/api/v1/*` to backend API
- Injects auth from token file or session bootstrap flow
- Enforces allowed origin controls and safe proxy behavior

### 3.4 `frontend`

Primary responsibilities:

- Operator UX for:
  - dashboard trends and summaries
  - active/previous runs
  - config editing and validation
  - notifications and schedule settings
  - API explorer and raw outputs
- Treats API as source of truth; no cluster-side direct calls

---

## 4) Control flow and data flow

## 4.1 Run trigger flow

1. UI posts `POST /api/v1/runs/trigger`.
2. API validates body and auth.
3. API builds safe command (allowlisted extra args only).
4. API executes runner process with timeout controls.
5. Runner writes artifacts under configured dirs.
6. UI polls run status endpoints and reads summary/report data.

## 4.2 Preflight flow

1. UI/CLI requests preflight.
2. Runner performs checks:
   - config validation
   - secrets validation
   - writable path checks
   - operational advisories
3. Results include machine-readable `remediation_code`.

## 4.3 Artifact consumption flow

1. Runner writes output snapshots.
2. API exposes aggregate and artifact endpoints.
3. UI reads report/trend/runs data from API.

---

## 5) Build and packaging architecture

Build layers:

- **Go layer**: all binaries via `go build ./...`
- **Frontend layer**: static bundle via `npm run build`
- **Container layer**:
  - runner image
  - API image
  - UI image
- **Release layer**:
  - per-platform binaries
  - `checksums.txt`
  - tagged release notes/changelog

Important release contracts:

- `VERSION` must match release tag intent.
- Checksums are required for updater integrity verification.
- Docs and manifests must align with image tags and binary expectations.

---

## 6) Kubernetes architecture (v2)

Namespace: `ncc-orchestrator-v2`

Workloads:

- `CronJob` runner executes scheduled checks.
- `Deployment` API serves control/report endpoints.
- `Deployment` UI serves frontend and API proxy.
- Shared PVC backs outputs/logs/token/report artifacts.

Current binary wiring model:

- Runner image contains `ncc-orchestrator`.
- API pod stages runner binary via init container into `/tools/ncc-orchestrator`.
- API executes `--orchestrator-bin /tools/ncc-orchestrator`.

Why this matters:

- API image can stay focused on API binary.
- Runner binary provenance remains tied to runner image.

---

## 7) Security model

Security controls by layer:

- **Runner**
  - secret resolution abstraction (`env`/`file`)
  - preflight checks for unsafe setup
- **API**
  - auth modes + token/session controls
  - strict JSON decode for mutation routes
  - path confinement under repo root
  - sanitized extra args on process execution
  - CORS allowlist, no wildcard mode
- **UI**
  - proxy restrictions to API path scope
  - controlled auth header injection
- **Kubernetes**
  - namespace isolation
  - secret/config separation
  - network policies and service boundaries

---

## 8) Operational contracts and invariants

When changing code, preserve these unless intentionally versioned:

- `run-summary.json` and `ncc-run-record.json` machine readability
- API route behavior under `/api/v1/*`
- Auth compatibility for token/session/hybrid
- Preflight `remediation_code` values
- Update/checksum verification path
- Path confinement and safe-arg execution guards

---

## 9) Developer takeover checklist

New owner first-week checklist:

1. Read:
   - `README.md`
   - `docs/BUILD_FROM_SCRATCH.md`
   - this document
   - `docs/FEATURES_AND_CONFIG_FLAGS.md`
2. Build all binaries and frontend from clean machine.
3. Run CLI-only run + preflight + replay.
4. Run local API+UI stack and trigger run via API.
5. Run `go test ./...`, `go vet ./...`, frontend tests/build.
6. Validate k8s manifests with `kubectl kustomize k8s`.
7. Perform one end-to-end deploy in non-prod cluster.
8. Execute rollback drill using migration guide.

Owner readiness criteria:

- Can explain each component boundary and failure mode.
- Can patch API/runner contract safely and update docs/tests.
- Can ship a release with checksums and aligned docs.

---

## 10) Where to debug by symptom

- **Run not starting**:
  - API logs (`cmd/ncc-api-server`)
  - orchestrator binary path + executable checks
  - auth mode/token header issues
- **Run starts but no report**:
  - runner logs (`logs/ncc-runner.log`, `nccfiles`)
  - output dir permissions and path config
- **UI empty but API healthy**:
  - UI proxy backend URL and token file
  - browser/API CORS/auth response mismatches
- **K8s works manually but cron fails**:
  - CronJob schedule/suspend
  - PVC write access
  - secret mapping (`NCC_PASSWORD`)

---

## 11) Change management guidance

Safe process for major changes:

1. Update architecture note in this doc first (what changes and why).
2. Add/adjust tests before modifying contracts.
3. Implement code changes.
4. Update operator docs (`README`, migration, k8s, feature flags).
5. Validate local + k8s flows.
6. Document rollout and rollback in release notes/changelog.
