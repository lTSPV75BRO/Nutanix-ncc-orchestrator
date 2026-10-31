# Secure Software Development Framework (NIST SP 800-218)

How NCC Orchestrator meets the SSDF v1.1 practices. The SSDF is the basis of
the US CISA *Secure Software Development Attestation Form*; this table is the
evidence a customer or auditor needs to complete or review one. It is a
self-assessment, not a third-party certification.

Status: **Met** (practice in place with evidence), **Partial** (in place with
a known gap, tracked in [`RISK_REGISTER.md`](RISK_REGISTER.md)).

## PO — Prepare the organization

| Practice | Status | How | Evidence |
| --- | --- | --- | --- |
| PO.1 Security requirements defined | Met | Security and trust model, hardening rules and password policy are documented and tested | [`SECURITY_AND_TRUST.md`](../SECURITY_AND_TRUST.md), [`passwordpolicy.go`](../../cmd/ncc-api-server/passwordpolicy.go) |
| PO.2 Roles and responsibilities | Met | Owner, reviewer and cadence per control area | [`CONTROL_OWNERSHIP.md`](CONTROL_OWNERSHIP.md) |
| PO.3 Supporting toolchains | Met | Pinned Go toolchain (`go.mod`) and Node version; reproducible release script; CI on every PR | [`ci.yml`](../../.github/workflows/ci.yml), [`binaryGO.txt`](../../scripts/binaryGO.txt) |
| PO.4 Security checks defined | Met | Build fails on lint, vet, race tests, govulncheck, npm audit (runtime + high dev), Trivy, CodeQL | [`ci.yml`](../../.github/workflows/ci.yml), [`codeql.yml`](../../.github/workflows/codeql.yml) |
| PO.5 Secure development environments | Partial | Least-privilege workflow permissions; third-party actions pinned by SHA; GitHub-owned actions on major tags (EX-03) | [`RISK_REGISTER.md`](RISK_REGISTER.md) |

## PS — Protect the software

| Practice | Status | How | Evidence |
| --- | --- | --- | --- |
| PS.1 Code protected from tampering | Met | Code on GitHub with signed-in pushes; release artifacts built in CI from the tagged commit | [`release.yml`](../../.github/workflows/release.yml) |
| PS.2 Release integrity verifiable | Met | SHA-256 checksums, Ed25519 signature over them, build-provenance attestation, `ncc-orchestrator verify` | [`releasesign.go`](../../cmd/ncc-orchestrator/releasesign.go), [`RELEASE_CHECKSUMS.md`](../RELEASE_CHECKSUMS.md) |
| PS.3 Releases archived with provenance | Met | GitHub Releases keep every version's binaries, SBOMs, checksums, signature and attestation | [`release.yml`](../../.github/workflows/release.yml), [`docs/releases/`](../releases/README.md) |

## PW — Produce well-secured software

| Practice | Status | How | Evidence |
| --- | --- | --- | --- |
| PW.1 Design meets security requirements | Met | Threat-driven design: separate UI/API processes, token or session auth on every API route, path confinement | [`ARCHITECTURE_AND_HANDOVER.md`](../ARCHITECTURE_AND_HANDOVER.md) |
| PW.2 Design reviewed | Met | Security assessment of the whole stack (October 2026) with fixes tracked | [`security-assessment-2026-10.md`](../security/security-assessment-2026-10.md) |
| PW.4 Reuse well-secured components | Met | Standard library crypto, bcrypt, maintained SAML/LDAP libraries; dependencies inventoried in SBOMs | `go.mod`, [`package-lock.json`](../../frontend/package-lock.json) |
| PW.5 Secure coding practices | Met | golangci-lint, `go vet`, TypeScript strict mode, request size limits, CSRF, CSP headers | [`.golangci.yml`](../../.golangci.yml), [`hardening.go`](../../cmd/ncc-api-server/hardening.go) |
| PW.6 Secure build configuration | Met | `CGO_ENABLED=0`, `-trimpath`, stamped version info; non-root container images | [`binaryGO.txt`](../../scripts/binaryGO.txt), [`Dockerfile.api`](../../build/docker/Dockerfile.api) |
| PW.7 Code review and analysis | Met | CodeQL (with a path-injection model), golangci-lint, pull-request review | [`codeql-config.yml`](../../.github/codeql/codeql-config.yml) |
| PW.8 Executable testing | Met | Go race tests, end-to-end API tests, hardening and lockout tests, frontend tests | [`e2e_test.go`](../../cmd/ncc-api-server/e2e_test.go), [`server_hardening_test.go`](../../cmd/ncc-api-server/server_hardening_test.go) |
| PW.9 Secure defaults | Met | Auth required; encrypted backups by default; API loopback-only in Compose; default-deny NetworkPolicies | [`docker-compose.yml`](../../docker-compose.yml), [`k8s/`](../../k8s/README.md) |

## RV — Respond to vulnerabilities

| Practice | Status | How | Evidence |
| --- | --- | --- | --- |
| RV.1 Identify and confirm vulnerabilities | Met | Disclosure policy, Dependabot alerts, scheduled CodeQL, CI scanners | [`SECURITY.md`](../../.github/SECURITY.md), [`dependabot.yml`](../../.github/dependabot.yml) |
| RV.2 Assess, prioritise and fix | Met | CVSS-based SLA; exceptions recorded with expiry | [`RISK_REGISTER.md`](RISK_REGISTER.md) |
| RV.3 Root-cause analysis | Met | Post-incident review step; each fix lands with a regression test | [`INCIDENT_RESPONSE.md`](INCIDENT_RESPONSE.md) |
