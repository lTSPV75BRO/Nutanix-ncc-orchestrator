# Third-party dependency inventory

This document maps the third-party packages used by NCC Orchestrator v2.3.0.
It is derived from [`go.mod`](../go.mod), [`go.sum`](../go.sum),
[`frontend/package.json`](../frontend/package.json), and
[`frontend/package-lock.json`](../frontend/package-lock.json).

The Go versions below are the versions selected by the module graph. Frontend
entries show the declared range; the lockfile is authoritative for the exact
installed version. Transitive packages are summarized by role rather than
duplicated in full: the complete resolved graph is available through the
commands in [Auditing and generating an SBOM](#auditing-and-generating-an-sbom).

## Runtime dependency map

```text
NCC Orchestrator
├── Go server and CLI
│   ├── HTTP/API: Go standard library
│   ├── Configuration: spf13/viper → afero, cast, mapstructure, fsnotify
│   ├── CLI: spf13/cobra → pflag, mousetrap
│   ├── Authentication:
│   │   ├── golang-jwt/jwt → x/crypto, x/sys
│   │   ├── coreos/go-oidc → oauth2, go-jose, protobuf
│   │   ├── crewjam/saml → goxmldsig, etree, xml-roundtrip-validator
│   │   └── go-ldap/ldap → go-asn1-ber, go-ntlmssp
│   ├── MCP: modelcontextprotocol/go-sdk → google/jsonschema-go, uuid
│   ├── Observability: OpenTelemetry → OTLP, gRPC, protobuf, x/net
│   ├── Logging/progress: zerolog, mpb → ewma, runewidth, stripansi
│   ├── Config parsing: yaml.v3
│   ├── Version metadata: goversioninfo → rsrc
│   └── Files/terminal: lumberjack, x/term, x/sys
└── Frontend
    ├── UI: React + React DOM + Ant Design → icons
    ├── Routing/data: react-router + TanStack React Query
    ├── Editor/config: Monaco Editor + YAML
    └── Animation: GSAP
```

## Go runtime and build packages

| Package | Version | Used for | Exposure |
| --- | --- | --- | --- |
| `github.com/Masterminds/semver/v3` | 3.5.0 | Version comparison and update checks | Runtime |
| `github.com/coreos/go-oidc/v3` | 3.21.0 | OpenID Connect discovery and token verification | Runtime |
| `github.com/crewjam/saml` | 0.5.1 | SAML authentication | Runtime |
| `github.com/go-jose/go-jose/v4` | 4.1.7 | JOSE/JWK signing and verification | Runtime |
| `github.com/go-ldap/ldap/v3` | 3.4.15 | LDAP/Active Directory authentication | Runtime |
| `github.com/golang-jwt/jwt/v5` | 5.3.1 | JWT sessions and tokens | Runtime |
| `github.com/josephspurrier/goversioninfo` | 1.7.0 | Windows executable version metadata | Build |
| `github.com/modelcontextprotocol/go-sdk` | 1.8.0 | MCP server and tool/resource protocol | Runtime |
| `github.com/rs/zerolog` | 1.35.1 | Structured logging | Runtime |
| `github.com/spf13/cobra` | 1.10.2 | CLI commands and subcommands | Runtime |
| `github.com/spf13/pflag` | 1.0.10 | POSIX-style CLI flags | Runtime |
| `github.com/spf13/viper` | 1.21.0 | YAML/config and environment loading | Runtime |
| `github.com/vbauerster/mpb/v7` | 7.5.3 | CLI progress display | Runtime |
| `go.opentelemetry.io/otel` | 1.46.0 | Tracing API and instrumentation | Runtime |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp` | 1.46.0 | OTLP/HTTP trace export | Runtime |
| `go.opentelemetry.io/otel/sdk` | 1.46.0 | Tracer provider and sampling | Runtime |
| `go.opentelemetry.io/otel/trace` | 1.46.0 | Trace types and propagation | Runtime |
| `go.yaml.in/yaml/v3` | 3.0.5 | YAML parsing and serialization | Runtime |
| `golang.org/x/crypto` | 0.58.0 | Password hashing and cryptographic helpers | Runtime |
| `golang.org/x/oauth2` | 0.37.0 | OAuth/OIDC token exchange | Runtime |
| `golang.org/x/sys` | 0.49.0 | OS-specific process and system APIs | Runtime |
| `golang.org/x/term` | 0.47.0 | Terminal detection and password input | Runtime |
| `gopkg.in/natefinch/lumberjack.v2` | 2.2.1 | Rotating log files | Runtime |

The Go module also records indirect dependencies for XML signatures, LDAP
encoding, OTel transport, gRPC/protobuf, configuration backends, terminal
output, and platform-specific build resources. They must be reviewed through
the resolved module graph rather than upgraded independently.

## Frontend runtime packages

| Package | Declared version | Used for |
| --- | --- | --- |
| `@ant-design/icons` | ^6.3.4 | UI icons |
| `@monaco-editor/react` | ^4.7.0 | React integration for Monaco |
| `@tanstack/react-query` | ^5.107.1 | Server-state fetching, caching, and mutations |
| `antd` | ^6.7.0 | Accessible component library |
| `gsap` | ^3.15.1 | Opening-title animation |
| `monaco-editor` | ^0.57.0 | YAML/JSON editor engine |
| `react` / `react-dom` | ^19.3.1 | UI runtime |
| `react-router` | ^8.5.1 | Client-side routing |
| `yaml` | ^2.9.1 | Client-side YAML parsing and validation |

## Frontend development and build packages

| Package | Declared version | Used for | Shipped to production? |
| --- | --- | --- | --- |
| `@testing-library/jest-dom` | ^7.0.1 | DOM assertions | No |
| `@testing-library/react` | ^16.3.3 | Component tests | No |
| `@types/node` | ^26.6.6 | TypeScript Node types | No |
| `@types/react` / `@types/react-dom` | ^19.3.0 | TypeScript React types | No |
| `@vitejs/plugin-react` | ^6.2.1 | Vite React transform | No |
| `@vitest/coverage-v8` | ^5.1.0 | Optional test coverage | No |
| `aria-query` | ^5.3.2 | Accessibility test support | No |
| `jsdom` | ^31.0.0 | Browser test environment | No |
| `typescript` | ^7.0.2 | Type checking and compilation | No |
| `vite` | ^8.3.6 | Frontend dev server and production bundling | No |
| `vitest` | ^5.1.0 | Test runner | No |

`dompurify` is pinned through the npm `overrides` field to ensure the
transitive editor dependency uses the selected patched version.

### Upgrade notes (Ant Design 6.7, Vitest 5)

Ant Design 6.7 and Vitest 5 were held back because the cluster-alert
pagination test stayed on page 1. The cause was neither of them:
`@rc-component/pagination` 1.7 (which antd's `^1.4.0` range already allows,
so any fresh install picked it up) moved the page click handler from the
`<li title="2">` to the `<a role="button" aria-label="Page 2">` inside it.
Browsers were unaffected; the test clicked the `<li>`. Tests should click
pages by role and name: `getByRole("button", { name: "Page 2" })`.

Ant Design 6.7 deprecates several props the app used; each now has its
replacement, so the suite runs without antd warnings:

| Deprecated | Replacement |
| --- | --- |
| `Drawer` `width` | `size` (number or string) |
| `Input` / `InputNumber` `addonAfter` | `Space.Compact` with `Space.Addon`; inside a form, bind the input with an inner `Form.Item noStyle name=…` |
| `List` | `ItemList` from `src/ui` for short static lists (antd's replacement, `Listy`, is a virtual list) |
| `Button` `iconPosition` | `iconPlacement` |
| `Steps` `items.description` | `items.content` |
| `rowKey` function's `index` argument | a key computed into the data |
| a `disabled` `RangePicker` with an empty value | `allowEmpty` while disabled |

Vitest 5 hides console output from passing tests; run
`vitest run --silent=false` to see warnings.

## License and operational ownership

- The project is MIT licensed; package license notices remain the responsibility
  of each upstream project.
- Go and frontend dependency updates are managed by Dependabot configuration
  in [`.github/dependabot.yml`](../.github/dependabot.yml).
- Runtime dependency changes require the Go tests, race tests, build, and the
  relevant authentication/API tests.
- Frontend dependency changes require `npm ci`, `npm test`, `npm run build`,
  and both production and full `npm audit`.
- Release builds can emit CycloneDX SBOMs through
  [`scripts/binaryGO.txt`](../scripts/binaryGO.txt). SBOMs, checksums, and
  provenance belong with release artifacts.

## Auditing and generating an SBOM

From the repository root:

```bash
go list -m all
go mod graph
go version -m ./dist/bin/ncc-orchestrator
govulncheck ./...

npm --prefix frontend ci
npm --prefix frontend ls --all
npm --prefix frontend audit
npm --prefix frontend audit --omit=dev
```

For a CycloneDX Go SBOM:

```bash
cyclonedx-gomod mod -licenses -output dist/bom-ncc-orchestrator.cdx.json
```

The generated SBOM is build output and should not be committed to source
control unless it is being attached to a release artifact.
