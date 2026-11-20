# Release vulnerability scan

Every tagged release was scanned for known vulnerabilities on 2026-11-20, so an
operator running an old version can see what it carries and the dashboard can
refuse the dangerous ones. The result feeds the shipped blocked-versions list
(`internal/uiversion`); see *Blocked dashboard versions* in
[`security-assessment.md`](security-assessment.md).

## Method

For each tag the source was exported and scanned with `govulncheck` (Go call
graph, so only vulnerabilities the code can reach) and, for releases with a
dashboard, `osv-scanner` on `frontend/package-lock.json` (npm packages that ship
in the bundle; development-only packages are left out). The tool versions and
vulnerability databases are those of the scan date.

## Results

| Release | Go (reachable) | Dashboard npm packages | Blocked |
| --- | --- | --- | --- |
| 0.1.4 to 0.1.12 | none | no dashboard | no dashboard |
| 0.1.13 | `go-sdk` 1.4.0: GO-2026-4773 (cross-site tool execution for HTTP servers without authorization), GO-2026-4770; fixed in 1.4.1 | no dashboard | no dashboard |
| 1.0.0, 1.1.0 | none | no dashboard | no dashboard |
| 2.0.0, 2.0.1, 2.0.2 | none | react-router 7.15.1 (5 advisories, highest CVSS 8.7: GHSA-chx6-hx7r-mcp5), DOMPurify 3.4.7 (9) | yes, high |
| 2.1.0, 2.1.1 | `x/net` 0.57.0: GO-2026-6617, -6612, -6611, -6610, -6603 (HTTP/2 denial of service); `grpc`: GO-2026-6348; `crewjam/saml` 0.5.1: GO-2026-6909 | DOMPurify 3.4.13 (5) | yes, medium |
| 2.2.0 | `x/net` 0.59.0: the same five; `crewjam/saml` 0.5.1: GO-2026-6909 | DOMPurify 3.4.16 (3) | yes, medium |
| 2.2.1 | `crewjam/saml` 0.5.1: GO-2026-6909; the node2shell issue, recorded by the project and not reproduced by this scan | none | yes, high |
| 2.3.0 | none real: `GO-2026-6909` is reported but `crewjam/saml` 0.6.3 contains the fix (see `scripts/govulncheck-accepted.txt`) | none | no |
| 2.4.0 | the same reviewed false positive | none | no |

GO-2026-6909 is a real finding for 2.1.0 to 2.2.1 (`crewjam/saml` 0.5.1; the fix
is in 0.5.2) and a false positive from 2.3.0 on (0.6.3).

## What this does not cover

- **Go standard library and the Go version used to release.** The scan ran with
  the current toolchain, so it cannot say which standard-library advisories the
  published binaries carried. Scan a published binary with
  `govulncheck -mode=binary <file>` for that.
- **Findings from the code review.** Application-level defects (for example the
  deleted-account session bug fixed in 2.3.0, the Helm chart crash fixed in
  2.4.0, or an installer that wiped backups) are not in a scanner's database; they
  are in [`findings.md`](findings.md) with the release that fixed each one.
- **Containers and Helm charts.** Images and charts were not rescanned per
  release.
- **Blocking is by what the client declares.** Dashboards before 2.4.0 send no
  version, so they are refused as a group by the "block unversioned" policy, not
  one by one.

## Repeating the scan

```bash
for tag in $(git tag); do
  mkdir -p /tmp/scan/$tag && git archive $tag | tar -x -C /tmp/scan/$tag
  (cd /tmp/scan/$tag && govulncheck ./...)
  [ -f /tmp/scan/$tag/frontend/package-lock.json ] && \
    osv-scanner scan source --lockfile /tmp/scan/$tag/frontend/package-lock.json
done
```

Add the releases it flags to `shipped` in `internal/uiversion/uiversion.go`, with
their advisory ids.
