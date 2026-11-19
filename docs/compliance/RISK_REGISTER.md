# Risk register

NIST CSF 2.0: GV.RM-01, ID.RA-05, GV.SC-07. Reviewed every release
(see [`CONTROL_OWNERSHIP.md`](CONTROL_OWNERSHIP.md)); last review **2026-11-19**
for v2.4.0.

Likelihood and impact are rated 1 (low) to 3 (high); **score = L × I**.
Risks scoring 6 or more need a named mitigation before release.

## Vulnerability-management SLA

| Severity (CVSS) | Runtime dependency or our code | Build/test tooling only |
| --- | --- | --- |
| Critical (9.0+) | Fix or mitigate in 7 days | 14 days |
| High (7.0–8.9) | 30 days | 30 days |
| Medium (4.0–6.9) | 90 days | Next release |
| Low | Next release | Next release |

CI enforces this: `govulncheck` (reachable Go vulnerabilities), `npm audit
--omit=dev` (runtime npm) and `npm audit --audit-level=high` (all npm,
including dev tooling) fail the build, as does Trivy on Critical/High
findings. Dependabot opens update PRs weekly. An advisory that cannot be fixed
in time goes in the exceptions table below with an expiry date.

## Risks

| ID | Risk | L | I | Score | Mitigation | Status |
| --- | --- | --- | --- | --- | --- | --- |
| R-01 | Theft of the Prism credentials the runner uses | 2 | 3 | 6 | Secrets via env, files or `secret://` providers, never in config; encrypted user store; least-privilege Prism account recommended | Mitigated |
| R-02 | Brute force or credential stuffing on local accounts | 2 | 3 | 6 | SP 800-63B policy (15+ chars, blocklist), bcrypt, per-user and per-IP throttling with lockout; OIDC/SAML SSO lets the organisation enforce MFA | Mitigated |
| R-03 | Stolen session or API token | 2 | 2 | 4 | Short-lived signed sessions, CSRF on cookie sessions, PATs stored as SHA-256 hashes, admin can revoke sessions and tokens | Mitigated |
| R-04 | Tampered release binary or image | 1 | 3 | 3 | SHA-256 checksums, Ed25519 signature, `verify` command, SBOMs, images built in CI | Mitigated |
| R-05 | Vulnerable dependency (runtime) | 2 | 3 | 6 | SLA above, govulncheck/Trivy/CodeQL gates, Dependabot | Mitigated |
| R-06 | Vulnerable build/test tooling on developer machines and CI | 2 | 2 | 4 | `npm audit --audit-level=high` now blocks CI; dev dependencies never ship in images | Mitigated (2026-10-31) |
| R-07 | Compromised third-party CI action | 1 | 3 | 3 | Every action pinned by commit SHA; least-privilege workflow `permissions`; checkouts do not keep the job token; publishing workflows disable dependency caches; `zizmor` reviewed the workflows | Mitigated (2026-11-19) |
| R-08 | Loss of run history, users or config | 2 | 2 | 4 | Encrypted scheduled backups, restore with validation and self-heal, persistent volumes | Mitigated |
| R-09 | Path traversal or command injection through API inputs | 1 | 3 | 3 | Path confinement, extra-args allowlist, CodeQL path-injection queries, regression tests | Mitigated |
| R-10 | Exposure of the API to untrusted networks | 2 | 3 | 6 | API loopback-only in Compose, default-deny NetworkPolicies on Kubernetes, TLS, token or session auth on every route | Mitigated |
| R-11 | Insider misuse by an operator or admin | 1 | 2 | 2 | Roles (viewer/operator/admin), audit log of every change, SIEM forwarding | Mitigated |
| R-12 | Single maintainer (bus factor) | 2 | 2 | 4 | Architecture/handover guide, build-from-scratch guide, automated release script | Accepted |
| R-13 | A deployment or installer defect leaves the stack unable to start, exposed to local users or without its backups | 2 | 3 | 6 | v2.4.0 review fixed the Helm API pod start-up and runner CronJob paths, the Windows installer ACL, config and re-run behaviour; regression tests pin the Helm and `k8s/` flags; a Helm install test in CI is on the roadmap | Mitigated, one control planned |

## Exceptions (accepted risks)

| ID | Item | Why accepted | Compensating control | Expires |
| --- | --- | --- | --- | --- |
| EX-01 | GO-2026-5932: `golang.org/x/crypto/openpgp` is deprecated, no fix available | The package is never imported; govulncheck confirms no reachable path | govulncheck in CI flags any future import | 2027-04-30 |
| EX-02 | Existing local passwords shorter than 15 characters keep working | SP 800-63B forbids forced rotation without evidence of compromise | Policy applies at the next change; admins can force a reset per user | Review 2027-04-30 |
| EX-03 | GitHub-owned actions referenced by major tag, not SHA | Closed 2026-11-19: every action is now pinned by commit SHA | Dependabot updates the pins weekly | Closed |
| EX-04 | GO-2026-6909: `govulncheck` reports a nil-pointer panic in `crewjam/saml` v0.6.3 because the vulnerability database has no fixed version | False positive: the fix landed in v0.5.2 and v0.6.3 contains it (nil checks for `Subject` and `Conditions` in `validateAssertion`); recorded in `scripts/govulncheck-accepted.txt`; handler panics are recovered into a logged 500 | `tools/vulngate` fails the build when the entry is stale or past its review date | 2027-01-31 |

## History

| Date | Change |
| --- | --- |
| 2026-10-31 | Register created for v2.3.0. Closed 10 Dependabot alerts (vite, vitest, undici, source-map-js; dev tooling) and two govulncheck module findings (x/net). R-06 mitigated by making the dev-tooling audit blocking. |
| 2026-11-19 | Reviewed for v2.4.0 after the quality and security review ([`docs/audit/`](../audit/README.md)). Added R-13 and EX-04; R-07 mitigated and EX-03 closed (all actions pinned by SHA). |
