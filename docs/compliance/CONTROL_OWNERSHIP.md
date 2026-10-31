# Control ownership

NIST CSF 2.0: GV.RR-02, GV.OV-01. Who owns each control area, who reviews it,
and how often. While the project has a single maintainer, *owner* and
*reviewer* can be the same person; an outside reviewer (a second maintainer or
the consuming organisation's security team) is recommended for releases.

| Area | CSF | Owner | Reviewer | Cadence | Evidence of review |
| --- | --- | --- | --- | --- | --- |
| Profile and evidence manifest | GV.OV | Maintainer | Release reviewer | Every release | `compliance_manifest_test.go` passes; profile date updated |
| Risk register and exceptions | GV.RM, ID.RA | Maintainer | Release reviewer | Every release; exceptions at expiry | History row in [`RISK_REGISTER.md`](RISK_REGISTER.md) |
| Dependency and supplier risk | GV.SC | Maintainer | Dependabot + CI | Weekly (Dependabot), every PR (CI) | Merged Dependabot PRs; green CI |
| Identity and access | PR.AA | Maintainer | Release reviewer | Every release touching auth | Auth, lockout and password-policy tests |
| Data protection and backups | PR.DS | Maintainer | Release reviewer | Every release; restore drill quarterly | Backup tests; drill entry in the register |
| Secure development (SSDF) | PR.PS | Maintainer | Pull-request reviewer | Every PR | CI gates in [`ci.yml`](../../.github/workflows/ci.yml) |
| Logging and monitoring | PR.PS-04, DE | Maintainer | Site operator | Every release | Audit-log tests; operator SIEM checks |
| Vulnerability response | ID.RA-08, RS | Maintainer (incident lead) | Reporter / advisory | Per report | GitHub Security Advisories |
| Recovery | RC | Site operator | Maintainer | Quarterly drill | Drill entry in the register |

## Release checklist (compliance)

Before tagging a release:

1. `go test ./...` passes (includes the manifest test) and CI is green.
2. No open Dependabot or code-scanning alert above the SLA in
   [`RISK_REGISTER.md`](RISK_REGISTER.md), or each is an unexpired exception.
3. Update the profile date and any status changes in
   [`NIST_CSF_PROFILE.md`](NIST_CSF_PROFILE.md).
4. Generate the evidence bundle and attach it to the release if a customer
   needs it: `scripts/generate-nist-evidence-bundle.sh`.
