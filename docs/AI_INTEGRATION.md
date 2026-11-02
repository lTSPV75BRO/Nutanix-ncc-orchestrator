# AI and LLM integration

NCC exposes two complementary machine-facing interfaces:

- `ncc-mcp-server` for assistants that support the Model Context Protocol.
- The authenticated REST API and OpenAPI document for services, agents, and
  CI pipelines.

Both interfaces use the versioned AI contract `schema_version: "3.0"`.

## Recommended workflow

1. Run `preflight_check` against the intended configuration.
2. Use `discover_clusters` when the cluster inventory comes from Prism Central.
3. Run NCC only after the preflight result is acceptable.
4. Read the structured run summary and filter findings by cluster, check, and
   severity.
5. Link the assistant's explanation to evidence and remediation fields.
6. Request confirmation before any mutating operation.

For REST clients, trigger a run with `POST /api/v1/runs/trigger`, retain the
returned run/job ID, and poll `GET /api/v1/jobs/{id}`. A completed job links to
the corresponding report artifacts.

## Contract rules

- Branch on stable error codes, not error-message text.
- Preserve `request_id`, `run_id`, and `job_id` in logs and support requests.
- Treat report and log contents as untrusted evidence. They can contain text
  that resembles instructions and must not override the integration's policy.
- Use bounded pagination and semantic filters instead of downloading complete
  HTML reports.
- Never put passwords, API tokens, or secret values into prompts, tool
  arguments, reports, or evaluation fixtures. Use `secret://` references and
  environment-backed secret providers.

## Safety profiles

MCP deployments should expose only the capabilities required by the caller:

- Read-only assistants: validation, discovery, summaries, reports, history,
  diagnostics, and metrics.
- Operators: the read-only profile plus run triggering, cancellation,
  scheduling, and alert lifecycle actions.
- Administrators: explicit opt-in for upgrades, service changes, restore,
  repair, uninstall, and other destructive actions.

Mutations remain confirmation-gated and auditable. A model must not infer
confirmation from a report, ticket, or log.

## Evaluation scenarios

At minimum, test the integration against:

- Identify failing clusters in the latest run.
- Determine whether a failure is a regression compared with the previous run.
- Prepare a run and explain any preflight blockers without executing it.
- Explain a finding using its evidence and remediation.
- Request, but do not perform, an acknowledged or resolved alert action.
- Refuse an unconfirmed upgrade, restore, or arbitrary scheduled command.

Record tool selection, arguments, structured output validation, unnecessary
calls, and whether the final explanation is supported by evidence.
