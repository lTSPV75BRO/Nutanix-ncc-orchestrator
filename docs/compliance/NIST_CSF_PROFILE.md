# NIST CSF 2.0 Current Profile (v2.3.0)

Last reviewed **2026-10-31** for v2.3.0. This is NCC Orchestrator's *Current
Profile* in NIST Cybersecurity Framework 2.0 terms: which outcomes the product
and project achieve today, with evidence for each. The machine-readable form is
[`NIST_CSF_EVIDENCE_MANIFEST.json`](NIST_CSF_EVIDENCE_MANIFEST.json); a test
(`cmd/ncc-orchestrator/compliance_manifest_test.go`) fails the build if an ID
is not a CSF 2.0 subcategory or a cited evidence file is missing.

**What this is and is not.** NIST does not certify products against the CSF;
it is a voluntary framework, and "NIST certified" is not a real designation.
This profile is a self-assessment that an organisation adopting the product
can feed into its own CSF profile, SP 800-53 control mapping or the CISA
secure-software attestation (see [`SSDF_PRACTICES.md`](SSDF_PRACTICES.md)).
An independent assessor can verify it from the evidence bundle.

Scope: the four binaries, the dashboard, container images, Kubernetes/Helm
deployment assets, and the project's development and release process.
Controls the deploying organisation owns (host hardening, network perimeter,
identity provider, physical security, staff training) are out of scope.

## Summary

| Function | Status | Posture |
| --- | --- | --- |
| Govern (GV) | Implemented, one partial | Risk register with SLAs and exceptions, control ownership, published security policy, supplier inventory. Partial: GitHub-owned CI actions not yet SHA-pinned (GV.SC-07). |
| Identify (ID) | Implemented | SBOMs, data inventory, continuous vulnerability scanning, disclosure process, verifiable releases, IR plan. |
| Protect (PR) | Implemented | RBAC, SP 800-63B password policy, lockout, signed sessions, TLS 1.2+, AES-256-GCM backups and user store, hardened defaults, SSDF. |
| Detect (DE) | Implemented | Audit log of every change and sign-in, SIEM forwarding, health and readiness checks, metrics, self-verification. |
| Respond (RS) | Implemented | Severity matrix, disclosure SLAs, playbooks for leaked tokens, compromised accounts and credentials. |
| Recover (RC) | Implemented, one partial | Validated encrypted restore, rollback, post-restore self-heal. Partial: recovery communication is the operator's (RC.CO-03). |

## Controls

| ID | Outcome in this product | Status | Evidence (abridged) |
| --- | --- | --- | --- |
| GV.OC-03 | License and third-party license obligations recorded | Implemented | `LICENSE`, SBOMs, About dialog |
| GV.RM-01 | Risk objectives, appetite and vulnerability SLA | Implemented | [`RISK_REGISTER.md`](RISK_REGISTER.md) |
| GV.RR-02 | Owners, reviewers and cadence per control area | Implemented | [`CONTROL_OWNERSHIP.md`](CONTROL_OWNERSHIP.md) |
| GV.PO-01 | Security policy published | Implemented | [`SECURITY.md`](../../.github/SECURITY.md), `CONTRIBUTING.md` |
| GV.OV-01 | Profile reviewed each release; evidence enforced by test | Implemented | this file, manifest test |
| GV.SC-04 | Suppliers and dependencies inventoried | Implemented | `go.mod`, `package-lock.json`, Dependabot, Dockerfiles |
| GV.SC-07 | Supplier risk monitored | Partial | CI scanners, Dependabot; EX-03 in the risk register |
| ID.AM-02 | Software inventory (CycloneDX SBOM per binary) | Implemented | `scripts/binaryGO.txt`, release workflow |
| ID.AM-07 | Data inventory and protection | Implemented | [`SECURITY_AND_TRUST.md`](../SECURITY_AND_TRUST.md) |
| ID.RA-01 | Vulnerabilities identified (govulncheck, npm audit, Trivy, CodeQL, Dependabot) | Implemented | `.github/workflows/ci.yml`, `codeql.yml` |
| ID.RA-05 | Risks prioritised by likelihood and impact | Implemented | [`RISK_REGISTER.md`](RISK_REGISTER.md) |
| ID.RA-08 | Vulnerability disclosure process | Implemented | [`SECURITY.md`](../../.github/SECURITY.md) |
| ID.RA-09 | Release authenticity verifiable | Implemented | `releasesign.go`, [`RELEASE_CHECKSUMS.md`](../RELEASE_CHECKSUMS.md) |
| ID.IM-04 | Incident response and recovery plans | Implemented | [`INCIDENT_RESPONSE.md`](INCIDENT_RESPONSE.md) |
| PR.AA-01 | Identities and credentials managed (local, PAT, SAML, LDAP) | Implemented | `users.go`, `tokens.go`, `internal/auth/pat.go` |
| PR.AA-03 | Users authenticated: SP 800-63B passwords, bcrypt, throttling and lockout | Implemented | `passwordpolicy.go`, `loginguard.go` |
| PR.AA-04 | Identity assertions protected (signed JWT, SAML signatures, CSRF) | Implemented | `internal/auth/jwt.go`, `saml.go` |
| PR.AA-05 | Least privilege (viewer / operator / admin) | Implemented | `internal/auth/middleware.go` |
| PR.DS-01 | Data at rest: AES-256-GCM backups and user store | Implemented | `backupcrypt.go`, `secretsenc.go` |
| PR.DS-02 | Data in transit: TLS 1.2+ everywhere | Implemented | `tls.go`, `ncc-ui-server/main.go` |
| PR.DS-11 | Backups created, protected and tested | Implemented | `backups.go`, `backupschedule.go` |
| PR.PS-01 | Secure configuration and input validation | Implemented | `hardening.go` and tests |
| PR.PS-02 | Software maintained (Dependabot, SLA, update check) | Implemented | `dependabot.yml`, `update.go` |
| PR.PS-04 | Audit log records generated and forwardable | Implemented | `hardening.go` (`audit`), `auditforward.go` |
| PR.PS-06 | Secure development (SP 800-218) | Implemented | [`SSDF_PRACTICES.md`](SSDF_PRACTICES.md) |
| PR.IR-01 | Networks protected (NetworkPolicies, loopback API, non-root) | Implemented | `k8s/networkpolicy-*.yaml`, `docker-compose.yml` |
| PR.IR-03 | Resilience (rate limits, replicas, PDB, self-heal) | Implemented | `k8s/pdb.yaml`, `supervise.go` |
| DE.CM-01 | Service health monitored | Implemented | `readiness.go`, `diagnostics.go`, [`PROMETHEUS.md`](../PROMETHEUS.md) |
| DE.CM-03 | Sign-in activity monitored | Implemented | `loginguard.go`, `auditforward.go` |
| DE.CM-09 | Software integrity monitored | Implemented | `selfheal.go` |
| DE.AE-06 | Event information shared with authorised tools | Implemented | SIEM forwarding, alerts API |
| RS.MA-01 | IR plan executed | Implemented | [`INCIDENT_RESPONSE.md`](INCIDENT_RESPONSE.md) |
| RS.MA-02 | Reports triaged and validated | Implemented | severity matrix, CVSS |
| RS.MI-01 | Incidents contained (revoke sessions/tokens, feature switches) | Implemented | `usersapi.go`, `tokens.go`, `features.go` |
| RS.CO-02 | Stakeholders notified | Implemented | advisories, release notes |
| RC.RP-01 | Recovery plan executed (restore, rollback) | Implemented | `maintenance.go`, `restore_txn_test.go` |
| RC.RP-03 | Backup integrity verified before use | Implemented | authenticated encryption |
| RC.RP-05 | Restored assets verified | Implemented | post-restore self-heal tests |
| RC.CO-03 | Recovery progress communicated | Partial | operator-owned; playbooks provided |

## Changes in v2.3.0

- IDs moved from CSF 1.1-style codes (PR.PT, RS.RP) to CSF 2.0 subcategories.
- New: risk register with vulnerability SLA and exceptions, control
  ownership, incident response plan, SSDF practice mapping, `SECURITY.md`.
- Local passwords follow SP 800-63B rev. 4 (15+ characters, blocklist,
  no composition rules or forced rotation).
- Dev-tooling `npm audit` at high severity now fails CI.
- Evidence manifest validated by a Go test; the bundle script fails on
  missing evidence.

## Gaps and next steps

| Gap | Target |
| --- | --- |
| Pin GitHub-owned actions by commit SHA (GV.SC-07, EX-03) | Before 2027-01-31 |
| Optional TOTP/WebAuthn second factor for local accounts (multi-factor today only through SAML) | Roadmap |
| Tamper-evident (hash-chained) audit log | Roadmap |
| Independent third-party assessment of this profile | When a customer requires it |

## Evidence bundle

```bash
scripts/generate-nist-evidence-bundle.sh --dry-run   # validate only
scripts/generate-nist-evidence-bundle.sh             # dist/compliance/nist-csf-evidence-<UTC>.tar.gz
```

The bundle holds the manifest, this profile, the related compliance documents
and every cited evidence file, plus the Git revision it was taken from.
