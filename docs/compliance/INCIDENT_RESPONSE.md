# Incident response and recovery plan

NIST CSF 2.0: ID.IM-04, RS.MA-01/02, RS.MI-01, RS.CO-02, RC.RP-01, RC.CO-03.
Two kinds of incident are covered:

- **Product vulnerability**: a security flaw in NCC Orchestrator itself
  (reported through [`SECURITY.md`](../../.github/SECURITY.md), found by a
  scanner, or seen in the wild).
- **Operational incident**: something wrong in a running installation, such as
  a leaked token, a suspected compromised account, or lost data.

## Roles

| Role | Who | Does |
| --- | --- | --- |
| Incident lead | Project maintainer | Declares the incident, owns decisions and the timeline |
| Fixer | Maintainer or contributor | Reproduces, fixes and adds the regression test |
| Communicator | Incident lead unless delegated | Advisory, release notes, reporter updates |
| Site operator | Whoever runs the installation | Containment and recovery steps on their deployment |

## Severity

| Level | Examples | First response |
| --- | --- | --- |
| SEV-1 Critical | Remote code execution, auth bypass, credential disclosure exploitable without an account | Same day; fix within 7 days |
| SEV-2 High | Privilege escalation between roles, stored XSS, secret exposure to an authenticated user | 3 days; fix within 30 days |
| SEV-3 Medium | DoS that needs an account, information leak of non-secret data | Fix within 90 days |
| SEV-4 Low | Hardening gaps, defence-in-depth | Next release |

## Product vulnerability: steps

1. **Intake (≤3 business days).** Acknowledge the reporter. Open a draft
   GitHub Security Advisory (private) and track work there.
2. **Triage (≤7 days).** Reproduce on the latest release. Score with CVSS,
   set severity, and identify affected versions.
3. **Contain.** If a workaround exists (a feature switch in Settings → Features,
   a config flag, a NetworkPolicy), publish it in the advisory draft for
   affected users.
4. **Fix.** Fix on a private fork of the advisory, with a regression test that
   fails before the fix. Run the full CI gate.
5. **Release.** Ship a patch release for each supported line
   ([`SECURITY.md`](../../.github/SECURITY.md)). Release notes name the
   advisory; the advisory gets a CVE where applicable.
6. **Disclose.** Publish the advisory, credit the reporter, and notify users
   through the release notes. The in-app update check surfaces the new version.
7. **Review (≤14 days after release).** Write a short blameless note: cause,
   why tests missed it, what changed. Add or update a row in
   [`RISK_REGISTER.md`](RISK_REGISTER.md).

## Operational incident: playbooks

**Leaked API token or PAT.** Settings → Access → revoke the token; check the
audit log (System → Audit log, filter by user) for its use. If the shared
stack token leaked, replace it (the `.ncc-api-token` file, or the
`api-token` key of the Kubernetes secret) and restart the API and UI.

**Suspected compromised account.** Settings → Access → *Sign out* the user
(ends all their sessions), then reset the password to a new temporary one
(forces a change at next sign-in) or delete the account. Review the audit log and forwarded SIEM
events for actions taken by that account.

**Locked-out sole admin.** On the host: `ncc-orchestrator v2-reset-password
--user admin` (Kubernetes: run `ncc-api-server --reset-admin` in the API pod).

**Leaked Prism credentials.** Rotate the password in Prism, update the secret
(`NCC_PASSWORD`, the Kubernetes secret, or the `secret://` provider), then run
a preflight check.

**Data loss or corruption.** System → Maintenance → restore the latest
encrypted backup (or `ncc-orchestrator v2-restore`). Restores are validated
and followed by self-heal checks; confirm with System → Health.

**Bad upgrade.** Roll back to the previous release using the steps in that
release's notes ([`docs/releases/`](../releases/README.md)); the updater keeps
an encrypted pre-update backup.

## Communication

| Audience | Channel | When |
| --- | --- | --- |
| Reporter | Advisory thread or email | Each step above |
| Users | GitHub Security Advisory, release notes, in-app update notice | At release |
| Site stakeholders | The operator's own process; audit log and SIEM exports as evidence | During and after recovery |

Recovery progress for a site incident is communicated by its operator; the
project supplies the playbooks above and the audit trail (RC.CO-03 is
**Partial** for that reason).

## Exercises

Run a tabletop exercise of one playbook each release cycle and a backup
restore drill each quarter; record the date and outcome in the history table
of [`RISK_REGISTER.md`](RISK_REGISTER.md).
