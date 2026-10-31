# Security policy

## Supported versions

| Version | Security fixes |
| --- | --- |
| 2.3.x | Yes (current) |
| 2.2.x | Yes, until 2.3.0 has been out for 90 days |
| < 2.2 | No; upgrade first |

## Reporting a vulnerability

Please report privately; do not open a public issue.

- **Preferred:** GitHub private vulnerability reporting —
  [Security → Report a vulnerability](https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/security/advisories/new).
- **Email:** prajwal.vernekar@nutanix.com, subject `SECURITY: <short summary>`.

Include the version (`ncc-orchestrator version`), deployment type (stack
archive, Docker Compose, Kubernetes/Helm), steps to reproduce and the impact
you observed. Never send real cluster credentials, tokens or customer data.

## What happens next

| Step | Target |
| --- | --- |
| Acknowledge the report | 3 business days |
| Triage and severity (CVSS 3.1/4.0) | 7 days |
| Fix released: Critical / High | 14 / 30 days |
| Fix released: Medium / Low | 90 days / next release |

We coordinate disclosure with you, publish a GitHub Security Advisory (with a
CVE where applicable) when the fix ships, and credit you unless you ask us not
to. The full process is in
[`docs/compliance/INCIDENT_RESPONSE.md`](../docs/compliance/INCIDENT_RESPONSE.md).

## Scope

In scope: the `ncc-orchestrator`, `ncc-api-server`, `ncc-ui-server` and
`ncc-mcp-server` binaries, the dashboard, published container images, the
Helm chart and Kubernetes manifests in this repository.

Out of scope: Nutanix Prism / NCC themselves (report those to Nutanix), and
findings that need an already-compromised host or admin account.

## Verifying releases

Every release ships SHA-256 checksums, an Ed25519 signature over them and
CycloneDX SBOMs. See [`docs/RELEASE_CHECKSUMS.md`](../docs/RELEASE_CHECKSUMS.md)
and [`docs/SECURITY_AND_TRUST.md`](../docs/SECURITY_AND_TRUST.md).
