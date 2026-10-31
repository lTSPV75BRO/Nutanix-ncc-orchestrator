# Compliance

Security-framework mapping and governance records for NCC Orchestrator.

| Document | Framework | What it covers |
| --- | --- | --- |
| [`NIST_CSF_PROFILE.md`](NIST_CSF_PROFILE.md) | NIST CSF 2.0 | Current Profile: every outcome the product meets, with status and evidence |
| [`NIST_CSF_EVIDENCE_MANIFEST.json`](NIST_CSF_EVIDENCE_MANIFEST.json) | NIST CSF 2.0 | Machine-readable profile; validated by a Go test on every build |
| [`SSDF_PRACTICES.md`](SSDF_PRACTICES.md) | NIST SP 800-218 | Secure development practices; basis for the CISA attestation form |
| [`RISK_REGISTER.md`](RISK_REGISTER.md) | CSF GV.RM, ID.RA | Risks, vulnerability SLA, accepted exceptions with expiry |
| [`INCIDENT_RESPONSE.md`](INCIDENT_RESPONSE.md) | CSF RS, RC | Severity, disclosure steps, operational playbooks |
| [`CONTROL_OWNERSHIP.md`](CONTROL_OWNERSHIP.md) | CSF GV.RR, GV.OV | Owners, reviewers, cadence, release checklist |
| [`../../.github/SECURITY.md`](../../.github/SECURITY.md) | CSF ID.RA-08 | How to report a vulnerability, response targets |

## About "NIST certification"

NIST publishes frameworks and guidelines but does not certify products or
organisations against the CSF or SP 800-218. What this folder provides is a
documented, evidence-backed self-assessment that:

- an adopting organisation can include in its own CSF profile or SP 800-53
  control mapping;
- supports completing or reviewing a CISA Secure Software Development
  Attestation;
- an independent assessor can verify from the evidence bundle.

Password handling follows NIST SP 800-63B rev. 4 (see
[`SECURITY_AND_TRUST.md`](../SECURITY_AND_TRUST.md#password-policy-nist-sp-800-63b)).

## Evidence bundle

```bash
scripts/generate-nist-evidence-bundle.sh --dry-run   # validate the manifest
scripts/generate-nist-evidence-bundle.sh             # write dist/compliance/*.tar.gz
```
