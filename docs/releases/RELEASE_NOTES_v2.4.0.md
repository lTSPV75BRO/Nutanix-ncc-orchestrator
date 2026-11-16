# NCC Orchestrator — v2.4.0

**Status:** Unreleased.

## See how old the data is

The dashboard keeps showing a cluster's findings from its last good run when
a later run can't reach it, and a stopped schedule leaves yesterday's results
looking current. The Overview now makes both visible:

- Every cluster shows when it was last checked; the time turns amber once it
  is more than a day old.
- A cluster the latest run couldn't reach says how old its data is, for
  example "connection refused · data from 3d ago". Hover the time to see when
  it last returned data and how many runs in a row failed.
- Clusters the latest run didn't include (another environment's) appear with
  results from their own newest run, so filtering by environment works even
  when that environment didn't run last.
- **Scheduled runs have stopped:** when a schedule (the main one or an
  environment's) goes two intervals without a run, a banner says which one
  and when it last ran. Operators get a link to the schedule.

Automation can read the same data from `GET /api/v1/freshness`.

## Safer restores

Backups taken before an update include the program files, so a rollback can
bring the previous version back. A restore now puts program files back only
when this server has run that exact version before. Otherwise it restores the
data, keeps the installed version running, and says so; install the version
you want from its release. This stops a tampered backup from running its own
code on the server after the restart. **Rollback last update** keeps working,
because the backup taken before the update records the old version as
trusted.

Moving to a new server: data restores as before; install the version you
want there. If you trust a backup from another server, restore it on the
command line with `v2-restore --trust-stack`.

## Hardening from the November audit

A full pass over the code, deployment files and UI (details in
[`IMPROVEMENTS.md`](../IMPROVEMENTS.md#audit-of-2026-11-16-v240)) found no
vulnerability, and fixed these:

- A crash in a background job (scheduled backup, environment schedule,
  digest and others) no longer stops the API server; it is logged and
  counted in `ncc_api_background_panics_total`.
- The GitHub token used for update checks is only ever sent to GitHub.
- Very large Prism Central discovery responses can't exhaust memory.
- The Schedule page shows the saved schedule in a new browser instead of
  the form defaults as "unsaved changes".
- Better screen-reader names on Findings and Users & roles.

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

## SAML test provider

Added `tools/fakesaml`, a separate development-only SAML identity provider.
It registers NCC's SP metadata, seeds `alice`, `oscar`, and `vera` with the
NCC role groups, and provides a landing page with a direct **Launch NCC login**
button. It supports HTTPS with generated or persistent self-signed
certificates. Use `-signing-cert` and `-signing-key` to persist the IdP
signing pair; this keeps the certificate published in metadata aligned with
assertions after a provider restart.

## Validation

- Go vet, tests, and builds pass.
- Frontend tests and production build pass.
- `npm audit` reports zero vulnerabilities.
- Helm lint and template rendering pass.
