# Connect other tools: API tokens, ServiceNow, Prometheus, Grafana, CI and AI assistants

NCC Orchestrator has a REST API, a Prometheus endpoint and an event feed, so
other tools can read results and act on findings. This page gets each one
started and links to the detailed guide.

- [API tokens](#api-tokens) (needed by everything else)
- [ServiceNow and other ticketing tools](#servicenow-and-other-ticketing-tools)
- [Prometheus and Grafana](#prometheus-and-grafana)
- [CI/CD pipelines](#cicd-pipelines)
- [Secrets from the environment or a file](#secrets-from-the-environment-or-a-file)
- [AI assistants (MCP)](#ai-assistants-mcp)
- [Chat tools](#chat-tools)

## API tokens

Give each tool its own account and token, with the lowest role that works:
`viewer` to read, `operator` to acknowledge or resolve findings and start runs.

1. As an admin, **Settings → Access → Users & roles → Add user**, for example
   `svc-servicenow`, role `viewer` or `operator`.
2. Sign in as that account (a private window is easiest), open the user menu
   (top right) → **Personal access tokens → Create token**. Pick an expiry (7 days to
   1 year, or *Never* for long-lived automation).
3. Copy the token (`ncc_pat_…`). It is shown only once.
4. The tool sends it in either header:

   ```http
   Authorization: Bearer ncc_pat_…
   X-API-Token: ncc_pat_…
   ```

Admins can see and revoke every token under **Settings → Access → API
tokens**. To try calls in the browser, use **System → Developer → API
explorer**; the OpenAPI spec is at `/api/v1/openapi.json` on the API server.

Quick test from a shell, against the API server (port 8081 by default):

```bash
curl -s -H "Authorization: Bearer $NCC_TOKEN" https://ncc.example.com:8081/api/v1/alerts/ncc | head
```

Add `--cacert ncc-ca.pem` if the API uses an internal or self-signed
certificate.

## ServiceNow and other ticketing tools

The usual pattern is a scheduled pull: the ticketing tool reads new and changed
findings from the event feed, opens or updates incidents, and writes back when
an incident is resolved.

| Endpoint | Role | Use |
| --- | --- | --- |
| `GET /api/v1/alerts/ncc` | viewer | All current findings with their state |
| `GET /api/v1/alerts/ncc/events?cursor=<next_cursor>` | viewer | Only what changed since the last poll |
| `POST /api/v1/alerts/ncc/{id}/acknowledge` / `resolve` / `reopen` | operator | Write back from the ticket |

Step-by-step for ServiceNow (MID Server, field mapping, an example scheduled
script, write-back) and for SIEMs and other tools:
[`integrations/third-party-alert-integrations.md`](../integrations/third-party-alert-integrations.md).

For tools that receive pushes instead, use the
[webhook notification channel](NOTIFICATIONS.md#webhook).

## Prometheus and Grafana

The API server publishes metrics at `/metrics`: per-cluster health score and
FAIL/WARN/ERR counts from the latest run, run counters and build info.

1. Create a `viewer` token for Prometheus (see [API tokens](#api-tokens)) and
   save it to a file on the Prometheus host.
2. Add a scrape job:

   ```yaml
   scrape_configs:
     - job_name: ncc
       scheme: https
       metrics_path: /metrics
       scrape_interval: 60s
       static_configs:
         - targets: ["ncc.example.com:8081"]
       authorization:
         type: Bearer
         credentials_file: /etc/prometheus/secrets/ncc-viewer-token
       tls_config:
         ca_file: /etc/prometheus/secrets/ncc-ca.pem   # if the API uses an internal or self-signed certificate
   ```

3. In Grafana, **Dashboards → Import** and upload
   [`deploy/grafana/ncc-orchestrator-dashboard.json`](../../deploy/grafana/ncc-orchestrator-dashboard.json).

On an isolated network you can start the API server with `--metrics-public`
to scrape without a token. Alerting rules and the metric list:
[`PROMETHEUS.md`](../PROMETHEUS.md).

## CI/CD pipelines

Run NCC from a pipeline (for example before and after an upgrade) and fail the
job when results are worse than you allow:

```bash
ncc-orchestrator --config config.yaml \
  --policy-gates "new-fails>0,min-health-score<90"
echo "exit code: $?"   # 0 ok, 1 error, 2 config problem, 3 some clusters failed
```

A broken policy gate fails the run. **Settings → Clusters & config** has a
policy gate builder for the dashboard's own runs. Gate syntax and exit codes:
[`FEATURES_AND_CONFIG_FLAGS.md` §11.1](../FEATURES_AND_CONFIG_FLAGS.md#111-policy-gates-syntax-and-metric-semantics)
and [§10](../FEATURES_AND_CONFIG_FLAGS.md#10-exit-codes-and-automation-behavior).

To start a run on the server from a pipeline instead, call
`POST /api/v1/runs/trigger` with an operator token; the API explorer shows
the body.

## Secrets from the environment or a file

Keep Prism, SMTP and webhook secrets out of `config.yaml` with `secret://`
references:

```yaml
secrets-provider: env              # or: file, with secrets-file: /etc/ncc/secrets.yaml
password: "secret://NCC_PRISM_PASSWORD"
smtp-password: "secret://SMTP_PASSWORD"
webhook-secret: "secret://WEBHOOK_HMAC"
```

With `env`, each name is an environment variable of the services that read
the config. In the dashboard, **Settings → Clusters & config** offers to move a
plain-text Prism password into a reference for you. Reference: [`FEATURES_AND_CONFIG_FLAGS.md` §2.12](../FEATURES_AND_CONFIG_FLAGS.md#212-secrets-manager-style-references-secret).

## AI assistants (MCP)

`ncc-mcp-server` lets assistants such as Claude Desktop or Cursor validate
configs, run preflight checks and NCC runs, and read summaries through the
Model Context Protocol. Destructive tools are marked so the assistant asks
first. Setup and the tool list: [`MCP_SERVER.md`](../MCP_SERVER.md) and
[`AI_INTEGRATION.md`](../AI_INTEGRATION.md).

## Chat tools

Slack works directly; Microsoft Teams, Google Chat and Discord go through the
webhook channel. See [Notifications](NOTIFICATIONS.md).
