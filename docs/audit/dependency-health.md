# Dependency health and update plan

Data gathered 2026-10-26 with `go list -m -u all`, the npm registry `latest`
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
| golang.org/x/crypto | 0.57.0 | 0.58.0 | **Upgraded** (bcrypt/scrypt users; minor, API-compatible) |
| golang.org/x/sys | 0.48.0 | 0.49.0 | **Upgraded** |
| golang.org/x/term | 0.46.0 | 0.47.0 | **Upgraded** (pulled x/text 0.43.0, x/sync 0.24.0) |
| github.com/go-ldap/ldap/v3 | 3.4.14 | 3.4.15 | **Upgraded** (patch) |
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
`npm audit --omit=dev`). Minor updates available: react 19.3.0→19.3.1, antd
6.6.5→6.6.8, @tanstack/react-query 5.104.0→5.104.5, react-router 8.4.0→8.5.0.

**Development/build dependencies with advisories (12, all fixable):**

| Package | Installed | Fixed in | Advisories | Exposure |
| --- | --- | --- | --- | --- |
| vite | 8.3.1 | 8.3.3 (latest 8.3.5) | GHSA-587h-7wmj-h5pm (7.5), GHSA-698v-4r3x-3vcv, GHSA-r825-w8mm-5pxw, GHSA-rq7h-c2jc-7f22, GHSA-vfpm-58rq-9qcg, GHSA-9jrq-w75r-8gcw | Dev server only (`vite dev`). GHSA-587h/698v chain to OS command injection through `launch-editor` **on Windows**, the primary developer platform here. Not in production images. |
| undici (via jsdom) | 8.11.2 | 8.12.2 | GHSA-xjj6-f33q-929c (7.5), GHSA-4qv8-2p9j-2685, GHSA-f72v-6w45-q472 | Test environment only |
| source-map-js | 1.2.1 | 1.2.2 | GHSA-68fv-2mgg-jv7q (8.7) | Build tooling |
| @vitest/mocker | 4.1.11 | 4.1.12 | GHSA-qxg6-89mg-rqv8, GHSA-xp26-3454-mjgp | Test runner |

**Status: blocked in this environment** — the npm CLI is not installed on the
audit machine (only an Electron-embedded Node runtime was available, which was
enough to run tsc/vitest/vite but not to update a lockfile). Run, on a machine
with npm and Node ≥ 26.12.1:

```bash
npm --prefix frontend install -D vite@8.3.5 vitest@4 @vitest/coverage-v8@4 jsdom@30
```

```bash
npm --prefix frontend update source-map-js undici @vitest/mocker
```

then `npm --prefix frontend ci && npm --prefix frontend test && npm --prefix frontend run build`
and confirm `npm audit` is clean. Vitest 5 is a major upgrade: do it separately,
after reading its migration guide.

## Prioritized update plan

| Order | Group | Risk | Gate |
| --- | --- | --- | --- |
| 1 | npm dev tooling security fixes (table above) | Low (patch/minor) | vitest, tsc, vite build, `npm audit` |
| 2 | Go x/* + go-ldap patch/minor (**done**) | Low | full Go suite, govulncheck, golangci-lint (all pass) |
| 3 | npm runtime minors (react, antd, react-query, react-router) | Low–medium (UI regressions) | vitest + manual smoke of Dashboard/Insights/Settings |
| 4 | OpenTelemetry 1.47 | Low | runner trace export smoke test |
| 5 | Vitest 5 | Medium (major) | migration guide, test suite |

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
