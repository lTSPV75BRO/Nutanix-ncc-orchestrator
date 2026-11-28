# Documentation

Everything about running, integrating, securing and developing NCC
Orchestrator. If you are new, start with the
[README](../README.md) and then the [setup guides](setup/README.md).

## Run it

| Guide | What it covers |
| --- | --- |
| [Setup guides](setup/README.md) | Clusters, sign-in (LDAP, OIDC, SAML), HTTPS, notifications, schedules and backups, upgrades without internet access, integrations |
| [Configuration reference](FEATURES_AND_CONFIG_FLAGS.md) | Every config key, environment variable and flag |
| [Kubernetes](../k8s/README.md), [Helm chart](../helm/ncc-orchestrator/README.md) | Manifests, chart values, runbook |
| [VM provisioning](../deploy/README.md) | cloud-init and Windows Sysprep |
| [Scaling](SCALING.md) | Sizing and tuning for large fleets |
| [Prometheus and Grafana](PROMETHEUS.md) | Metrics, alert rules, dashboards |
| [Troubleshooting](TROUBLESHOOTING.md) | Reading the logs, common failures and fixes |

## Integrate it

| Guide | What it covers |
| --- | --- |
| [Alert integrations](integrations/third-party-alert-integrations.md) | ServiceNow, SIEMs, scripts; the findings API and event feed |
| [API and UI architecture](API_AND_UI_ARCHITECTURE.md) | How the API and UI fit together; the OpenAPI spec is served at `/api/v1/openapi.json` |
| [AI assistants](AI_INTEGRATION.md), [MCP server](MCP_SERVER.md) | Schemas, jobs, tools, safety defaults |
| [Agent prompt](AGENT_PROMPT.md) | A copy-paste prompt that makes an AI agent deploy, harden, tune and fix the stack on a machine, VM, Kubernetes or cloud, asking you first |
| [Agent runbook](AGENT_RUNBOOK.md) | For AI agents and automation: deploy, connect MCP, harden, troubleshoot, tune and maintain, step by step with checks |

## Secure it

| Guide | What it covers |
| --- | --- |
| [Security and trust](SECURITY_AND_TRUST.md) | Verifying release binaries, runtime hardening, access control, backup encryption |
| [Release checksums](RELEASE_CHECKSUMS.md) | Checksums, SBOMs and provenance attestations |
| [Compliance](compliance/README.md) | NIST CSF 2.0 profile, SSDF practices, risk register, incident response |
| [Reporting a vulnerability](../.github/SECURITY.md) | Contact and response targets |

## Build and contribute

| Guide | What it covers |
| --- | --- |
| [Build from scratch](BUILD_FROM_SCRATCH.md) | A clean machine to a running stack and a tested build |
| [Architecture and handover](ARCHITECTURE_AND_HANDOVER.md) | Components, control flow, packaging, where to debug by symptom |
| [Web UI design system](ui/DESIGN_SYSTEM.md) | Navigation, tokens, components, accessibility rules |
| [Third-party dependencies](THIRD_PARTY_DEPENDENCIES.md) | Direct dependencies and licences |
| [Contributing](../CONTRIBUTING.md) | Workflow, checks and documentation requirements |

## Project

| Document | What it covers |
| --- | --- |
| [Roadmap](ROADMAP.md) | Planned and proposed work by priority |
| [Upgrade helper](UPGRADE_HELPER_PLAN.md) | How upgrades include the supervisor and service definitions on every platform: design, what was built, known limits |
| [Quality and security review](audit/README.md) | Method, results and measurements of the 2026 review; [findings register](audit/findings.md); [security assessment](audit/security-assessment.md); [release vulnerability scan](audit/release-scan.md) |
| [Release notes](releases/README.md), [changelog](../CHANGELOG.md) | What changed in each version |
