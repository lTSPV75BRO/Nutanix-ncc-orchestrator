# NCC Orchestrator — v2.4.0

**Status:** Unreleased.

## Update-check reliability

When the GitHub REST API is rate limited, update checks now resolve the latest
release tag through GitHub's public latest-release redirect. The UI reports
the actual latest version instead of retaining a stale release value.

The result is explicitly marked as degraded because release asset metadata is
not available from the fallback. The **Back up and update** action remains
disabled until a complete, verifiable release response is available. Set
`GITHUB_TOKEN` on the API/orchestrator process for higher GitHub API limits and
normal update installation behavior.

## CodeQL coverage

CodeQL now analyzes the Go services, React frontend, and GitHub Actions
workflows. Runtime cgroup counters are bounded before conversion to `int`, and
the OIDC cookie security policy is explicit for the supported HTTP development
and TLS-terminating proxy modes. The loopback-only throwaway fake OIDC provider
is excluded from production security scanning.

## Security maintenance

The Go toolchain requirement is now `1.27.4`, which fixes
`GO-2026-6859` affecting HTTP/1 servers through a `net/http` header race.

## Validation

- Go vet, tests, and builds pass.
- Frontend tests and production build pass.
- `npm audit` reports zero vulnerabilities.
- Helm lint and template rendering pass.
