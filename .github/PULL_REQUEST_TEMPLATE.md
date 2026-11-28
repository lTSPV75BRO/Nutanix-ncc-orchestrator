## What changed and why

<!-- One or two sentences on the change, and the problem it solves. -->

## Scope

- [ ] CLI / runner
- [ ] API server
- [ ] UI
- [ ] Kubernetes, Helm or deploy scripts
- [ ] Docs only

## How it was validated

<!-- Commands you ran and what you saw. For an installer or deploy change, say which
OS or cluster you ran it on. -->

- [ ] `go build ./... && go vet ./... && go test -race -count=1 ./...`
- [ ] `cd frontend && npm test && npm run build` (if the UI changed)
- [ ] `helm lint` and `helm template` (if the chart changed)
- [ ] Added or updated tests for new behaviour and for any bug fixed

## Checklist

- [ ] Docs updated (`docs/`, `README.md`) and, for user-visible changes, `CHANGELOG.md` and the release notes
- [ ] No secrets, tokens or customer data in the diff or in logs the change adds
- [ ] Backward compatibility noted below if flags, API fields, config keys or file formats changed

## Compatibility notes

<!-- Upgrade steps, deprecations, or "none". -->
