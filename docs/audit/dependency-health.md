# Dependency health and update plan

Data gathered 2026-11-04 with `go list -m -u all`, the npm registry `latest`
dist-tags, `govulncheck` (golang.org/x/vuln, latest) and `osv-scanner` v2
against `go.mod` and `frontend/package-lock.json`. No dependency is
deprecated on its registry. Lockfiles were not regenerated.

## Runtimes and toolchains

| Item | Declared / pinned | Status |
| --- | --- | --- |
| Go | `go 1.27.3` (go.mod), `golang:1.27.3-alpine` builders | Current |
| Node (build only) | `engines >=26`, CI and `Dockerfile.ui` pin `26.12.1` | Includes the 2026-10-21 security release (CVE-2026-111497). Runtime images contain no Node. |
| Base image | `alpine:3.24` + `apk add --upgrade ca-certificates` | Supported |
| golangci-lint | v2.15.0 (CI) | Current; first version that reads Go 1.27 export data |

## Go modules (direct)

| Module | Installed | Latest | Action |
| --- | --- | --- | --- |
| golang.org/x/crypto | 0.58.0 | 0.58.0 | Current (bcrypt/scrypt users) |
| golang.org/x/sys | 0.49.0 | 0.49.0 | Current |
| golang.org/x/term | 0.47.0 | 0.47.0 | Current |
| github.com/go-ldap/ldap/v3 | 3.4.15 | 3.4.15 | Current |
| go.opentelemetry.io/otel/* | 1.46.0 | 1.47.0 | Leave to Dependabot (minor; pulls grpc/genproto churn) |
| github.com/golang-jwt/jwt/v5 | 5.3.1 | 5.3.1 | Current |
| github.com/crewjam/saml | 0.5.1 | 0.5.1 | Current; low release cadence — watch advisories (XML signature handling via goxmldsig 1.6.1) |
| github.com/modelcontextprotocol/go-sdk | 1.8.0 | 1.8.0 | Current |
| spf13/cobra, pflag, viper; rs/zerolog; Masterminds/semver; yaml v3; lumberjack; mpb; goversioninfo | — | current | None |

`govulncheck`: **0 reachable vulnerabilities**. One module-level advisory,
GO-2026-5932 ("x/crypto/openpgp is unmaintained"), has no fixed version and
does not apply: `go mod why golang.org/x/crypto/openpgp` reports the main
module does not need that package.

## npm (frontend)

Production dependencies: no advisories (`osv-scanner`; CI also runs
`npm audit --omit=dev`). React, React Router, TanStack Query, GSAP, and their
compatible transitive dependencies were updated to their current tested
releases. Ant Design 6.7.0, Vitest 5.1.0, jsdom 31 and Vite 8.3.6 followed on
2026-11-07 (see the upgrade notes in
[`THIRD_PARTY_DEPENDENCIES.md`](../THIRD_PARTY_DEPENDENCIES.md)).

**Status: clean as of 2026-11-07.** `osv-scanner` reports no advisories for
the 232 packages in `frontend/package-lock.json`, including development/build
tooling (2026-11-04: `npm audit` clean across the previous 256).

## Prioritized update plan

| Order | Group | Risk | Gate |
| --- | --- | --- | --- |
| 1 | npm dev tooling security fixes (**done**) | Low (patch/minor) | vitest, tsc, vite build, `npm audit` |
| 2 | Go x/* + go-ldap patch/minor (**done**) | Low | full Go suite, govulncheck, golangci-lint (all pass) |
| 3 | npm runtime minors (react, antd, react-query, react-router) (**done**) | Low–medium (UI regressions) | vitest + manual smoke of Dashboard/Insights/Settings |
| 4 | OpenTelemetry 1.47 | Low | runner trace export smoke test |
| 5 | Vitest 5 (**done**) | Medium (major) | migration guide, test suite |

## Automation added

* `.github/dependabot.yml`: weekly grouped minor/patch PRs for Go modules, npm
  and GitHub Actions; majors excluded (manual, guided upgrades).
* CI: an advisory `npm audit --audit-level=high` across **all** dependencies
  (the blocking job audits production only), so dev-tool advisories like the
  ones above are visible in every run.

## Supply chain notes

* Third-party actions are pinned by commit SHA (Trivy, golangci-lint, docker
  login); first-party `actions/*` use major tags (acceptable; Dependabot now
  tracks them).
* Releases produce an SBOM and build-provenance attestations
  (`actions/attest-build-provenance`); checksums are verified by the updater.
* `package.json` overrides `dompurify` to a patched version; keep the
  override until Monaco's transitive range includes it.
* License check: dependencies are MIT/BSD/Apache-2.0/ISC (Go modules and the
  main npm packages). A full license inventory (e.g. from the release SBOM)
  was not produced here.
