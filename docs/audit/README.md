# Quality and security review

An independent review of the code, dependencies, deployment files, installers
and web UI, done in October 2026 (v2.2.1) and again in November 2026 (v2.4.0).
It looked for defects, security weaknesses and operational gaps, fixed what was
confirmed, and recorded what is left.

| Document | What it holds |
| --- | --- |
| This page | Scope, method, results by area, measurements, and what was not verified |
| [`findings.md`](findings.md) | Every finding with severity, priority and status (open and resolved) |
| [`security-assessment.md`](security-assessment.md) | The technical record of the security findings, controls and scans |
| [`release-scan.md`](release-scan.md) | Known vulnerabilities in every tagged release, and which are blocked |

## Result in one paragraph

The application code is in good shape: build, vet, lint, `gosec` and the full
test suite under the race detector pass, no known-vulnerable dependency is
reachable, and the live probes found no authentication or authorization bypass.
The serious problems were around the code, in deployment and installation: the
Helm chart's API pod could not start, the Windows installer produced an invalid
config, left its install directory writable by every local user and could wipe
backups, and a few multi-replica and restore paths misbehaved. All of those are
fixed and covered by tests. What remains is hardening work (systemd sandboxing,
container defaults, Windows file ACLs, installer attestation checks) and
checks that need larger environments. See [`findings.md`](findings.md).

## Scope and versions

| Round | Date | Version reviewed | Focus |
| --- | --- | --- | --- |
| 1 | October 2026 | v2.2.1 | Full application review; security hardening; correctness fixes |
| 2 | November 2026 | v2.4.0 (the 34 commits since v2.3.0, then the whole tree) | Re-audit, scanners, live probes, browser accessibility, Windows installer, Kubernetes and Linux environment tests |

Severity is Critical, High, Medium or Low; priority is P0 (fix now) to P3
(optional). No finding was rated Critical.

## Method

- **Static checks:** `go build`, `go vet`, `golangci-lint`, `gosec`,
  `govulncheck`, `osv-scanner`, `npm audit`, `gitleaks`, `trivy`
  (filesystem and images), `kubeconform`, `kube-linter`, `zizmor`, `helm lint`.
- **Tests:** the Go suite with `-race`; the frontend type-check and Vitest
  suite (51 files, 403 tests); new regression tests for every defect fixed.
- **Live testing:** the API run locally and probed for authorization, CSRF,
  CORS, lockout and path traversal; the built UI scanned with axe-core in a real
  browser (including colour contrast) on 16 routes in three themes; the Windows
  installer run end to end; the Helm chart installed on a `kind` cluster; a
  multi-node cluster and a Rocky Linux host tested separately.
- **Trust-boundary check of every entry point** (added after SEC-12 was missed): for each listener, CLI,
  subprocess and token, who can reach it and what authenticates them. Done for the API (all 129 routes swept without credentials), the UI server (proxy and static files only), the MCP server and the engine's MCP guard.
- **Reading the code** of the newer features (directory profiles, freshness,
  restore trust, version-skew check, retention, backup verification) and the
  deployment files.

## Results by area

### Code correctness and reliability

Defects found and fixed are in [`findings.md`](findings.md). The notable ones:
LDAP outages counted toward the account lockout, a restore rewrote nested
config keys, account changes on a second API replica saw stale data or answered
500 instead of 409, and a long list of Windows-specific problems (process
liveness, secrets-file permissions, path healing). Troubleshooting logs were
extended: LDAP steps, sign-in failures, backup and restore errors, unreadable
run summaries and runner version-skew refusals are now logged as structured
warnings with the user, client address and request id, and never passwords.

### Security

See [`security-assessment.md`](security-assessment.md). No Critical findings;
one accepted false positive in the dependency scan; deployment, installer and
container hardening fixed; systemd sandboxing and Windows ACLs open.

### Dependencies

- Go 1.27.4. `govulncheck` and `osv-scanner` report only `GO-2026-6909` in
  `crewjam/saml` v0.6.3, a reviewed false positive. `npm audit` reports 0.
- Added for testing only: `axe-core` (frontend dev dependency, MPL-2.0) and
  `jimlambrt/gldap` with two small helpers (Go test dependencies).
- Not reviewed: licence compatibility beyond
  [`docs/THIRD_PARTY_DEPENDENCIES.md`](../THIRD_PARTY_DEPENDENCIES.md), and a
  comparison against the latest versions.

### Performance

Measured on a Windows workstation, empty install, localhost, so treat the
numbers as a baseline, not a capacity figure.

| Measurement | Result |
| --- | --- |
| Initial page load (production build) | 37 JavaScript chunks, 1.31 MB raw, about 429 kB gzip; CSS 116 kB raw, about 20 kB gzip. The code editor (Monaco, about 4 MB) is loaded only when needed. |
| `GET /api/v1/health`, signed in | 18 ms before, 0.8 ms after caching display paths (fixed, PERF-01); about 47,000 requests/s with 50 clients afterwards |
| `/api/v1/freshness` | p50 7 ms; about 750 requests/s with 50 clients |
| `/api/v1/report/data` | p50 18 ms; about 250 requests/s with 50 clients |
| 500-cluster data set on a Kubernetes cluster, through a port-forward | `/report/data` p50 185 ms, p95 517 ms, about 76 req/s; `/freshness` p50 48 ms, about 310 req/s; no 5xx or 429 |

Not measured: a 7,000-cluster data set with memory figures, Core Web Vitals,
and behaviour under sustained concurrent writes. The documented sizing is in
[`docs/SCALING.md`](../SCALING.md).

### Accessibility

Target: WCAG 2.2 AA. An axe-core scan in a real browser found failures that
jsdom tests cannot see: colour contrast (selected menu item 2.4:1, active tab,
empty-state title, light-theme warning tags), labels that did not contain the
visible text, an invalid ARIA role on a button, heading levels, 14 px password
toggles and unfocusable scrollable code blocks. All are fixed; the re-scan
shows 0 violations on 16 routes in the dark, light and IT Professional themes.
Keyboard order was walked on the first screen (skip link first, visible focus
everywhere). Not covered: pages with populated data, 400% zoom, and screen
readers. The rules are in [`docs/ui/DESIGN_SYSTEM.md`](../ui/DESIGN_SYSTEM.md).

### Deployment and installers

| Target | Tested | Result |
| --- | --- | --- |
| Helm chart on a `kind` cluster | Install, readiness, user store in a Secret, RBAC | Found K8S-02 (API pod could not start); fixed and re-verified |
| Windows installer on Windows 11 | Install, ACLs, SYSTEM scheduled tasks, firewall, health, backup, verify, restore, re-run, task restart | Found WIN-01, WIN-03, WIN-04 and COR-09; fixed and re-verified |
| Multi-node Kubernetes cluster (Cilium, Kubernetes 1.35) | Install on RWX storage, two replicas, RBAC, NetworkPolicy, rollout, PrometheusRule, version-skew refusal, 500 and 2,000 cluster data sets | Found ENV-01 to ENV-06; the rest passed |
| Rocky Linux 10 (SELinux enforcing, firewalld) | Checksum verification, SELinux, firewall ports, `doctor` | Found ENV-06 and ENV-08; LIN-01 confirmed |

The cluster and Linux fixes were verified by unit and manifest tests; they were
not re-run on those systems.

## Not verified

These need an environment that was not available or time that was not spent:

- Node drain on a live cluster, and a 7,000-cluster data set with pod memory.
- Egress NetworkPolicy.
- A reboot and `kill -9` of supervisor children on Linux; a systemd drop-in with
  sandboxing; LDAP against a real directory on Linux.
- A Windows reboot, a service account other than SYSTEM, an upgrade from an older
  release, and a Sysprep first boot.
- OWASP ZAP (DAST), a screen reader, and accessibility of pages with data.

Each is listed as a follow-up in [`docs/ROADMAP.md`](../ROADMAP.md) or
[`findings.md`](findings.md).
