# NIST CSF Baseline (v2.1.1)

This document is the repository's first structured NIST Cybersecurity Framework
(CSF 2.0) baseline. It maps currently implemented capabilities to CSF functions,
tracks evidence locations, and calls out gaps needed for audit-ready posture.

Scope:
- Product implementation and operational controls visible in this repository
- Runtime stack (`ncc-orchestrator`, `ncc-api-server`, `ncc-ui-server`)
- Supporting documentation and test evidence paths

Non-goals:
- This is not a formal certification report
- This does not claim full compliance with NIST SP 800-53 / 800-171

## Maturity Snapshot

| Function | Status | Current posture |
| --- | --- | --- |
| Govern (GV) | Partial | Security controls exist, but control ownership / exception governance is not fully codified. |
| Identify (ID) | Partial | Good technical visibility, but risk register and impact classification are not formalized as artifacts. |
| Protect (PR) | Strong | Strong implementation coverage: authN/Z, RBAC, TLS, encryption, hardening checks. |
| Detect (DE) | Strong | Audit logs, diagnostics, SIEM forwarding, health checks, and observable runtime signals are implemented. |
| Respond (RS) | Partial | Self-heal/remediation paths exist; formal incident response runbooks and process evidence are limited. |
| Recover (RC) | Strong | Backup/restore, encrypted snapshots, and restore-time safety checks are implemented. |

## Control Coverage Register

| CSF Function | Capability | Status | Primary implementation/evidence |
| --- | --- | --- | --- |
| PR | Role-based access control | Implemented | `cmd/ncc-api-server/auth.go`, `cmd/ncc-api-server/users*.go`, `docs/SECURITY_AND_TRUST.md` |
| PR | Authentication modes (token/session/local/SAML/LDAP) | Implemented | `cmd/ncc-api-server/auth.go`, `saml.go`, `ldap.go`, `frontend/src/pages/LoginPage.tsx` |
| PR | Session/PAT security controls | Implemented | `cmd/ncc-api-server/tokens*.go`, `docs/SECURITY_AND_TRUST.md` |
| PR | Data-at-rest protection for backups | Implemented | `backupcrypt.go`, `cmd/ncc-api-server/backups*.go`, `docs/SECURITY_AND_TRUST.md` |
| PR | TLS cert management and secure defaults | Implemented | `cmd/ncc-api-server/tls.go`, `cmd/ncc-ui-server/main.go`, `docs/TROUBLESHOOTING.md` |
| DE | Audit logging and filtering | Implemented | `cmd/ncc-api-server/main.go` (audit routes), `frontend/src/features/settings/AuditLogSection.tsx` |
| DE | SIEM/syslog forwarding | Implemented | `cmd/ncc-api-server/auditforward.go` |
| DE | Continuous health diagnostics/self-heal checks | Implemented | `cmd/ncc-api-server/diagnostics.go`, `selfheal.go`, `frontend/src/features/settings/SystemHealthSection.tsx` |
| RC | Backup/restore operations | Implemented | `cmd/ncc-api-server/backups.go`, `cmd/ncc-api-server/maintenance.go` |
| RC | Post-restore validation and self-heal | Implemented | `cmd/ncc-api-server/maintenance.go`, `cmd/ncc-api-server/maintenance_restore_selfheal_test.go` |
| GV | Control ownership register | Partial | This baseline document (owner workflow not yet automated) |
| ID | Risk register and asset criticality mapping | Missing | Not currently tracked as structured repo artifacts |
| RS | Incident response playbooks and communication matrix | Missing | Not currently tracked as structured repo artifacts |
| GV/ID/RS | Formal evidence package per release | Partial | Scattered evidence across tests/docs; no unified compliance bundle yet |

## Priority Gap Closure Plan

### P0 (start now)
- Create a per-control ownership matrix (owner, reviewer, cadence, escalation).
- Add a security exceptions register (accepted risks, expiration, compensating controls).
- Establish a lightweight risk register (threat, impact, likelihood, mitigation, status).

### P1 (next release cycle)
- Define and publish incident response runbooks (triage, containment, recovery, postmortem).
- Add an automated compliance evidence bundle that exports:
  - test run summaries,
  - audit sample extracts,
  - diagnostics/self-heal snapshots,
  - backup/restore verification output.

### P2 (quarterly)
- Execute documented RTO/RPO recovery drills and retain signed evidence artifacts.
- Add periodic control effectiveness review notes to release readiness artifacts.

## Update Procedure

Update this file for each release candidate:
1. Re-validate control rows against code/docs/tests.
2. Adjust status (`Implemented`, `Partial`, `Missing`) with rationale.
3. Add/remove evidence paths as files move.
4. Record any material changes in release notes.

Suggested ownership:
- Primary: Security/Platform maintainer
- Reviewer: Release owner

## Evidence Bundle Automation

This baseline is paired with a machine-readable manifest:
- `docs/NIST_CSF_EVIDENCE_MANIFEST.json`

Generate an auditable bundle (manifest + baseline + referenced evidence files):

```bash
scripts/generate-nist-evidence-bundle.sh
```

Dry-run validation only:

```bash
scripts/generate-nist-evidence-bundle.sh --dry-run
```

