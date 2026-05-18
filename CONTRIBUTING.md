# Contributing to Nutanix NCC Orchestrator

Thanks for your interest. Contributions are welcome via pull requests.

For first-time setup and full local build instructions, start with:

- `docs/BUILD_FROM_SCRATCH.md`

## Branching and scope

- Use topic branches: `feature/<name>` or `fix/<name>`.
- For v2 stack work, target branch `v2.0.0` unless maintainers request otherwise.
- Keep changes focused: one feature/fix per PR whenever possible.

## Repository map

Main components you may touch:

- `goNCC.go` - orchestrator runner CLI
- `cmd/ncc-api-server` - backend API service
- `cmd/ncc-ui-server` - UI proxy/static server
- `frontend/` - React + Vite + TypeScript UI
- `k8s/` - full v2 Kubernetes manifests
- `docs/` - user/operator/developer documentation

## Development workflow

1. Fork the repo and create a branch:
   - `git checkout -b feature/your-feature`
2. Implement changes with tests.
3. Run local validation before opening PR (see Required checks).
4. Update docs and release notes when behavior/flags/contracts change.
5. Push branch and open PR with:
   - what changed
   - why it changed
   - how it was validated

## Local environment expectations

- Keep secrets out of committed files (`NCC_PASSWORD`, API tokens, smtp/webhook credentials).
- Prefer env vars or local secret files ignored by git.
- Validate configs with:
  - `ncc-orchestrator validate-config --config <path>`
  - `ncc-orchestrator validate-secrets --config <path>`

## Required checks before PR

- `go test ./...`
- `go vet ./...`
- `go build ./...`
- `go test .` (root CLI path sanity)
- If MCP code changed: `go build ./cmd/ncc-mcp-server`
- If frontend changed: `cd frontend && npm test && npm run build`
- If Kubernetes manifests changed: `kubectl kustomize k8s`

## Documentation requirements

If your change affects behavior, flags, APIs, setup, operations, or security:

- Update user-facing docs as needed, typically:
  - `docs/FEATURES_AND_CONFIG_FLAGS.md`
  - `docs/V2_BACKEND_FRONTEND_MVP.md`
  - `docs/MIGRATION_v1_TO_v2.md`
  - `Prometheus.md` (if metrics/monitoring changed)
- For v2 release-line changes, update:
  - `RELEASE_NOTES_v2.0.0.md`
  - `CHANGELOG.md`

## Testing expectations by area

- Runner (`goNCC.go`) changes:
  - add/update unit tests in `goNCC_test.go`
  - verify output artifact compatibility (`run-summary.json`, dashboards, policy outputs)
- API server changes:
  - add/update tests in `cmd/ncc-api-server/*_test.go`
  - validate auth, path confinement, request validation, and error contract behavior
- Frontend changes:
  - update UI tests and ensure `npm run build` passes
  - keep API usage aligned to `/api/v1/*` contracts
- Kubernetes changes:
  - verify `k8s/README.md` instructions still match manifests
  - ensure image assumptions and volume wiring are explicit

## Security and reliability guardrails

- Do not weaken auth defaults or path confinement rules without explicit maintainer agreement.
- Keep mutation endpoints strict (`application/json`, unknown field rejection, allowlisted args).
- Preserve machine-readable fields used by automation (`error_code`, `remediation_code`, run summary schema).
- Avoid introducing high-cardinality metrics in Prometheus outputs.

## Specific contributor notes

- Quickstart and automation:
  - Keep beginner-first UX intact (`quickstart`, `--auto`, automation levels).
  - Avoid breaking first-run bootstrap behavior.
- Prometheus:
  - Keep cardinality safe for production.
  - Respect optional metric export (`prom-enabled`).
- API/CLI contracts:
  - Preserve machine-readable fields (`error_code`, `remediation_code`) where applicable.

## Code Style

- Follow Go best practices.
- Run `gofmt -w` on changed Go files.
- Add/adjust tests for new behavior and regressions.
- Prefer clear, operationally actionable error messages.

## Commit and PR guidance

- Prefer small, reviewable commits with clear intent.
- Include docs updates in the same PR when user-facing behavior changes.
- In PR description include:
  - impact scope (CLI/API/UI/K8s/docs)
  - backward compatibility notes
  - rollout/rollback notes if operational behavior changed
  - exact commands you used to validate

## Reporting Issues

Use GitHub Issues for bugs or suggestions.
