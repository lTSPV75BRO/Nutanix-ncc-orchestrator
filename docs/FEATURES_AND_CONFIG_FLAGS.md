# Features and Config Flags (v2.0.0)

Comprehensive reference for NCC Orchestrator features, configuration keys, and CLI flags.

> Scope note (v1): this repository branch is Go-only for runtime components. The v2 API/UI servers and React frontend are intentionally not part of this scope.

## 1) What this tool does

`ncc-orchestrator` runs Nutanix NCC checks across one or more clusters, collects and parses results, and generates automation-friendly artifacts plus human-readable reports.

## 2) Feature reference with examples

### 2.1 Multi-cluster parallel execution

Run checks across multiple clusters with bounded concurrency:

```bash
ncc-orchestrator --clusters "10.38.66.37,10.38.66.7" --username admin --password "$NCC_PASSWORD" --max-parallel 4
```

### 2.2 Cluster list from file (with optional per-cluster credentials)

`clusters-file` supports:
- `cluster`
- `cluster,username`
- `cluster,username,password`

Example `clusters.txt`:

```text
# cluster[,username[,password]]
10.38.66.37
10.38.66.7,admin
pc-aos01.example.local,svc-user,secret://pc_aos01_password
```

Run:

```bash
ncc-orchestrator --clusters-file clusters.txt --username admin --password "$NCC_PASSWORD"
```

### 2.3 Prism Central cluster discovery

Discover clusters using Prism Central (v4 default, v3 optional):

```bash
ncc-orchestrator discover-clusters \
  --prism-central-url https://pc:9440 \
  --username admin \
  --password "$NCC_PASSWORD" \
  --format table
```

Write to file for later runs:

```bash
ncc-orchestrator discover-clusters --prism-central-url https://pc:9440 --output clusters.txt
```

### 2.4 Replay mode

Regenerate reports/artifacts from existing logs without invoking NCC APIs:

```bash
ncc-orchestrator --config config.yaml --replay
```

### 2.5 Report outputs

Generate per-cluster files in one or more formats:
- `html`
- `csv`
- `json`
- `markdown`
- `sarif`

```bash
ncc-orchestrator --outputs html,csv,json,markdown,sarif
```

### 2.6 Aggregated dashboard and run artifacts

Outputs under `output-dir-filtered` include:
- `index.html` (aggregated dashboard)
- `run-summary.json`
- `ncc-run-record.json`
- `checks-snapshot.json`
- `drilldown-diff.json`
- `flaky-checks.json`
- `regression-summary.json`
- `slo-dashboard.json`
- `policy-gates.txt` (only on violations)

### 2.7 Policy gates (CI/CD enforcement)

Fail a run if thresholds are violated:

```bash
ncc-orchestrator --policy-gates "new-fails>0,fail-rate>2,min-health-score<90"
```

### 2.8 Flaky-check detection

Detect checks that change severity repeatedly across recent runs:

```bash
ncc-orchestrator --flaky-lookback-runs 10 --flaky-min-transitions 3
```

### 2.9 Per-cluster health score and trend

Health scoring and trend visualization are generated in run artifacts and dashboard output automatically.

### 2.10 Quiet hours and maintenance windows

Suppress notifications for planned windows:

```bash
ncc-orchestrator \
  --quiet-hours "22:00-06:00" \
  --maintenance-windows "2026-04-22T20:00:00Z/2026-04-22T23:00:00Z"
```

### 2.11 Notifications (email/webhook/slack)

Enable and configure one or more channels:

```bash
ncc-orchestrator \
  --webhook-enabled --webhook-url "https://hooks.example.com/ncc" \
  --email-enabled --smtp-server smtp.example.com --smtp-user ncc@example.com \
  --slack-enabled --slack-webhook-url "https://hooks.slack.com/services/..."
```

Digest mode sends one notification per run:

```bash
ncc-orchestrator --notify-digest
```

### 2.12 Secrets manager style references (`secret://`)

Resolve sensitive values from env or a file-backed secret map:

```bash
ncc-orchestrator --secrets-provider env --password secret://NCC_PRISM_PASSWORD
```

```bash
ncc-orchestrator --secrets-provider file --secrets-file secrets.yaml --password secret://prism_password
```

### 2.13 Scheduling helper

Create/list/remove scheduler entries:

```bash
ncc-orchestrator create-schedule --type cron --every 4h --config config.yaml --print-only
```

### 2.14 Strict config validation

Validate config files in automation pipelines:

```bash
ncc-orchestrator validate-config --config config.yaml
```

### 2.15 Secrets preflight validation

Validate `secret://` references and secret source accessibility before a run:

```bash
ncc-orchestrator validate-secrets --config config.yaml
```

### 2.16 Alert exclusion controls

Exclude selected alert titles from generated outputs/notifications:

```bash
ncc-orchestrator \
  --exclude-alert-titles "Disk health,Prism connectivity" \
  --exclude-alert-match-mode contains
```

Or load from file:

```bash
ncc-orchestrator --exclude-alert-titles-file exclude-alerts.txt
```

### 2.17 Config JSON schema generation

```bash
ncc-orchestrator config-schema --output config.schema.json
```

### 2.18 Update mode

Check/update local binary from GitHub or custom binary URLs:

```bash
ncc-orchestrator update
ncc-orchestrator update --check
ncc-orchestrator update --check --binary-url https://artifacts.example.com/ncc-orchestrator-linux-amd64 --target-version 1.2.4
ncc-orchestrator update --binary-url https://artifacts.example.com/ncc-orchestrator-linux-amd64 --binary-sha256 <sha256-hex>
ncc-orchestrator update --allow-major-upgrade
```

Notes:

- `--binary-sha256` is required for `--binary-url` install operations.
- GitHub release updates verify downloaded binaries against release checksum assets before replace.

### 2.19 Test dashboard generation (no API calls)

Generate synthetic aggregate dashboard and artifacts:

```bash
ncc-orchestrator gen-test-agg --clusters 25 --output-dir dist/test/outputfiles
```

### 2.20 Preflight check output (automation-friendly)

Run full preflight with structured JSON output:

```bash
ncc-orchestrator preflight-check --config config.yaml --format json
```

Each non-pass check includes a machine-readable `remediation_code` field so UI/automation can map failures to fix playbooks.

## 3) Configuration precedence

Highest to lowest precedence:
1. CLI flags
2. Environment variables (`NCC_*`)
3. Config file (`--config`)
4. Internal defaults

## 4) Config keys (config file + equivalent root flags)

| Key | Type | Default | Example |
|---|---|---|---|
| `cluster-source-mode` | string | `clusters` | `"pc"` |
| `clusters` | string | — | `"10.38.66.37,10.38.66.7"` |
| `clusters-file` | string | — | `"clusters.txt"` |
| `pcs` | string | — | `"10.10.10.10,10.10.10.11"` |
| `pcs-file` | string | — | `"pcs.txt"` |
| `prism-central-url` | string | — | `"https://10.10.10.10:9440"` |
| `discover-api-version` | string | `v4` | `"v3"` |
| `username` | string | `admin` | `"admin"` |
| `password` | string | — | `"secret://NCC_PRISM_PASSWORD"` |
| `ncc-api-version` | string | `v4` | `"Legacy"` |
| `nutanix-v4-api-version` | string | `v4.2` | `"v4.1"` |
| `insecure-skip-verify` | bool | `false` | `true` |
| `timeout` | duration | `15m` | `"20m"` |
| `request-timeout` | duration | `20s` | `"30s"` |
| `poll-interval` | duration | `15s` | `"10s"` |
| `poll-jitter` | duration | `2s` | `"1s"` |
| `max-parallel` | int | `4` | `6` |
| `outputs` | csv string | `html,csv` | `"html,csv,json"` |
| `output-dir-logs` | string | `nccfiles` | `"dist/logs"` |
| `output-dir-filtered` | string | `outputfiles` | `"dist/output"` |
| `single-report` | bool | `false` | `true` |
| `log-file` | string | `logs/ncc-runner.log` | `"logs/ncc.json"` |
| `log-level` | string | `info` | `"debug"` |
| `log-http` | bool | `false` | `true` |
| `retry-max-attempts` | int | `6` | `8` |
| `retry-base-delay` | duration | `400ms` | `"500ms"` |
| `retry-max-delay` | duration | `8s` | `"12s"` |
| `retry-circuit-breaker` | int | `3` | `2` |
| `prom-dir` | string | `promfiles` | `"metrics"` |
| `run-history` | bool | `false` | `true` |
| `run-history-dir` | string | `<output-dir-filtered>/runs` | `"outputfiles/runs"` |
| `retain-last` | int | `0` | `20` |
| `retain-days` | int | `0` | `14` |
| `artifact-retain-days` | int | `0` | `14` |
| `artifact-retain-max-files` | int | `0` | `200` |
| `notify-on-regression` | bool | `false` | `true` |
| `adaptive-parallelism` | bool | `true` | `false` |
| `policy-gates` | csv string | — | `"new-fails>0,fail-rate>2"` |
| `quiet-hours` | string | — | `"22:00-06:00"` |
| `maintenance-windows` | csv string | — | `"start/end,start/end"` |
| `flaky-lookback-runs` | int | `6` | `10` |
| `flaky-min-transitions` | int | `2` | `3` |
| `severity-filter` | csv string | — | `"FAIL,WARN"` |
| `exclude-alert-titles` | csv string | — | `"Disk health,Prism connectivity"` |
| `exclude-alert-titles-file` | string | — | `"exclude-alerts.txt"` |
| `exclude-alert-match-mode` | string | `exact` | `"contains"` |
| `dry-run` | bool | `false` | `true` |
| `replay` | bool | `false` | `true` |
| `max-idle-conns` | int | `100` | `200` |
| `max-idle-conns-per-host` | int | `10` | `20` |
| `max-conns-per-host` | int | `0` | `50` |
| `idle-conn-timeout` | duration | `90s` | `"120s"` |
| `email-enabled` | bool | `false` | `true` |
| `email-attach-html` | bool | `false` | `true` |
| `notify-digest` | bool | `false` | `true` |
| `smtp-server` | string | — | `"smtp.gmail.com"` |
| `smtp-port` | int/string | `587` | `465` |
| `smtp-user` | string | — | `"ncc@example.com"` |
| `smtp-password` | string | — | `"secret://SMTP_PASSWORD"` |
| `email-from` | string | — | `"ncc@example.com"` |
| `email-to` | csv string | — | `"ops@example.com,sre@example.com"` |
| `email-use-tls` | bool | `true` | `false` |
| `webhook-enabled` | bool | `false` | `true` |
| `webhook-include-html` | bool | `false` | `true` |
| `webhook-url` | string | — | `"https://hooks.example.com/ncc"` |
| `webhook-headers` | map | `{}` | `{"X-Token":"abc"}` |
| `slack-enabled` | bool | `false` | `true` |
| `slack-webhook-url` | string | — | `"https://hooks.slack.com/services/..."` |
| `slack-channel` | string | — | `"#infra-alerts"` |
| `secrets-provider` | string | — | `"env"` or `"file"` |
| `secrets-file` | string | — | `"secrets.yaml"` |

## 5) Root command flags (full list)

Use:

```bash
ncc-orchestrator [flags]
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--adaptive-parallelism` | bool | `true`, `false` | `true` | When enabled, orchestration dynamically scales effective worker concurrency down/up based on observed HTTP 429 behavior, reducing sustained rate-limit pressure without fully stopping progress. |
| `--cluster-source-mode` | string | `clusters`, `pc` | `clusters` | Selects target source behavior. `clusters` uses direct PE entries. `pc` uses Prism Central targets (`--pcs`, `--pcs-file`, or `--prism-central-url`) and auto-discovers clusters before run. |
| `--clusters` | string | CSV of cluster IP/FQDN values | none | Primary target list when `clusters-file` is not used. Each entry is validated, duplicates are rejected, and values must be resolvable/valid cluster addresses. |
| `--clusters-file` | string | Path to text file | none | Alternate target source. Supported line formats: `cluster`, `cluster,username`, `cluster,username,password`. If provided and non-empty, it overrides/supersedes `--clusters`. |
| `--config` | string | Path to `.yaml`, `.yml`, or `.json` | none | Loads persistent config values from file before env/flag overrides are applied. Use this for production runs and scheduler jobs. |
| `--dry-run` | bool | `true`, `false` | `false` | Performs validation and prints effective setup without executing cluster checks. Use in CI preflight and change-review pipelines. |
| `--email-attach-html` | bool | `true`, `false` | `false` | Adds HTML report attachment to email notifications. Useful for operators who consume mail-only workflows, but may increase message size. |
| `--email-enabled` | bool | `true`, `false` | `false` | Enables SMTP email notifications. Requires SMTP and recipient fields to be correctly set. |
| `--email-from` | string | Valid email address | none | Sender address used in email notifications; should align with SMTP relay policy/domain requirements. |
| `--email-to` | string | Comma-separated email addresses | none | Recipient list for email notifications. Supports one or more addresses separated by commas. |
| `--email-use-tls` | bool | `true`, `false` | `true` | Enables STARTTLS for SMTP sessions. Keep enabled for production unless your SMTP endpoint explicitly requires plain mode in trusted networks. |
| `--flaky-lookback-runs` | int | Integer `>= 1` | `6` | Number of historical snapshots used for flaky-check detection. Higher values improve long-range sensitivity but may increase noise in unstable labs. |
| `--flaky-min-transitions` | int | Integer `>= 1` | `2` | Minimum severity transitions required before a check is marked flaky. Raise this to reduce false positives. |
| `--insecure-skip-verify` | bool | `true`, `false` | `false` | Disables TLS certificate verification. Use only in trusted lab/self-signed environments. Avoid in production. |
| `--log-file` | string | Writable file path | `logs/ncc-runner.log` | Rotated JSON log output file. Use a persistent location for post-incident analysis. |
| `--log-http` | bool | `true`, `false` | `false` | Enables HTTP request/response logging for deep debugging. Can expose operationally sensitive payloads; keep off in normal production usage. |
| `--log-level` | string | `trace`, `debug`, `info`, `warn`, `error`, or numeric `0..5` | `info` | Controls verbosity. Use `debug`/`trace` for troubleshooting and `info` for steady-state operations. |
| `--maintenance-windows` | string | RFC3339 windows: `start/end[,start/end...]` | none | Suppresses notifications in explicit maintenance intervals. Best for planned change windows and patch operations. |
| `--max-parallel` | int | Integer `1..100` | `4` | Maximum concurrent clusters. Tune down for rate-limited APIs or constrained environments; tune up for faster completion in stable networks. |
| `--ncc-api-version` | string | `v4`, `Legacy`, `v1` (`v1` alias for Legacy) | `v4` | Selects NCC start-check API strategy. Use `v4` by default; use legacy mode for environments requiring Prism Gateway v1 start endpoints. |
| `--notify-digest` | bool | `true`, `false` | `false` | Sends one consolidated notification per run (email/webhook/slack) instead of per-cluster messages. Recommended for large estates. |
| `--notify-on-regression` | bool | `true`, `false` | `false` | Emits notifications only when FAIL count regresses compared to prior summary, reducing steady-state noise. |
| `--nutanix-v4-api-version` | string | Revision-like path token (examples: `v4.2`, `v4.1`, `v4.0.a1`) | `v4.2` | Controls v4 path segment for clustermgmt/monitoring/prism APIs. Set this to match your Prism API revision. |
| `--output-dir-filtered` | string | Writable directory path | `outputfiles` | Destination for filtered per-cluster files, aggregated dashboard, and run-level artifacts. |
| `--output-dir-logs` | string | Writable directory path | `nccfiles` | Destination for raw NCC summary logs per cluster (`<cluster>.log`). |
| `--outputs` | string | CSV subset of `html,csv,json,markdown,sarif` | `html,csv` | Per-cluster output formats to generate. Include `json`/`sarif` for automation pipelines and quality gates. |
| `--password` | string | Plain string or `secret://name` | prompt if omitted | Global Prism password fallback. If omitted and needed, interactive prompt is used. Per-cluster file passwords can override per target. |
| `--policy-gates` | string | CSV of expressions `<metric><op><number>` | none | Defines run-fail thresholds for automation control. Example: `new-fails>0,fail-rate>2,min-health-score<90`. |
| `--poll-interval` | duration string | Go duration (`5s`, `10s`, `1m`) | `15s` | Base interval between task-status polls. Shorter intervals improve responsiveness but increase API load. |
| `--poll-jitter` | duration string | Go duration (`0s` and above) | `2s` | Random additive delay on top of poll interval to reduce herd effects across concurrent cluster workers. |
| `--pcs` | string | CSV of Prism Central IP/FQDN/URL values | none | PC target list used in `pc` mode. Each PC is queried and all discovered clusters are added to the run target set (deduplicated). |
| `--pcs-file` | string | Path to text file | none | Alternate PC target source for `pc` mode (one PC per line; `#` comments allowed). |
| `--prism-central-url` | string | URL/IP/FQDN | none | Single-PC fallback target for `pc` mode when `--pcs`/`--pcs-file` are not set. |
| `--discover-api-version` | string | `v4`, `v3` | `v4` | API used for PC cluster discovery in `pc` mode. `v4` uses clustermgmt API and auto-falls back to `v3` on 404. |
| `--prom-dir` | string | Writable directory path | `promfiles` | Directory for Prometheus `.prom` metric files used by pull-based monitoring stacks. |
| `--quiet-hours` | string | `HH:MM-HH:MM` local-time range | none | Recurring daily notification suppression window. Ideal for predictable off-hours operations. |
| `--replay` | bool | `true`, `false` | `false` | Rebuilds reports/artifacts from existing logs without invoking NCC APIs. Useful for debugging and template iterations. |
| `--request-timeout` | duration string | Go duration (`5s`, `20s`, `60s`) | `20s` | Per-request HTTP timeout. Must be lower than overall run timeout; increase for slow links or overloaded control planes. |
| `--retain-days` | int | Integer `>= 0` (`0` = unlimited) | `0` | Run-history retention by age. Applies only when `--run-history` is enabled. |
| `--retain-last` | int | Integer `>= 0` (`0` = unlimited) | `0` | Run-history retention by count. Keeps only newest N snapshots when enabled. |
| `--retry-base-delay` | duration string | Go duration (`100ms`, `500ms`, `1s`) | `400ms` | Base backoff delay for retryable HTTP errors. Used with jitter and exponential growth. |
| `--retry-max-attempts` | int | Integer `>= 1` | `6` | Maximum HTTP retry attempts for retryable errors/statuses. |
| `--retry-max-delay` | duration string | Go duration (`1s`, `8s`, `30s`) | `8s` | Upper bound on retry backoff delay. Prevents unbounded wait times under prolonged failures. |
| `--retry-circuit-breaker` | int | Integer `>= 1` | `3` | Opens retry circuit and fails fast after N consecutive retryable failures. Helps avoid long noisy retry loops on unhealthy endpoints. |
| `--run-history` | bool | `true`, `false` | `false` | Persists timestamped run snapshots for trend and regression analysis across runs. |
| `--run-history-dir` | string | Writable directory path | `<output-dir-filtered>/runs` | Base path for saved run snapshots when run-history is enabled. |
| `--artifact-retain-days` | int | Integer `>= 0` | `0` | Deletes generated artifacts older than N days from `output-dir-filtered` (`0` disables age-based deletion). |
| `--artifact-retain-max-files` | int | Integer `>= 0` | `0` | Keeps only the N newest generated artifacts in `output-dir-filtered` (`0` disables count-based deletion). |
| `--secrets-file` | string | Path to YAML/JSON key-value map | none | Secret map source when `--secrets-provider=file` is selected. |
| `--secrets-provider` | string | `env`, `file` | none | Enables `secret://` value resolution from process environment or file-backed key map. |
| `--severity-filter` | string | CSV subset of `FAIL,WARN,ERR,INFO` | empty (all) | Limits output rows/artifacts to selected severities. Useful for alert-focused reports but can hide context. |
| `--skip-preflight-check` | bool | `true`, `false` | `false` | Skips default preflight execution in run path. Keep `false` for production safety. |
| `--exclude-alert-titles` | string | CSV list of alert titles | empty | Excludes matching alert titles from generated outputs/notifications. |
| `--exclude-alert-titles-file` | string | Path to line-delimited title file | empty | Loads exclusion titles from file (`#` comments and blank lines are ignored). |
| `--exclude-alert-match-mode` | string | `exact`, `contains`, `regex` | `exact` | Controls how exclusion titles are matched against alert names. |
| `--single-report` | bool | `true`, `false` | `false` | Writes a single-file report copy (`ncc-report-single.html`) in addition to regular outputs. |
| `--slack-channel` | string | Channel name (for example `#ops-alerts`) | none | Optional channel override in Slack notification payloads when webhook supports channel routing. |
| `--slack-enabled` | bool | `true`, `false` | `false` | Enables Slack notifications through incoming webhook integration. |
| `--slack-webhook-url` | string | HTTPS webhook URL or `secret://name` | none | Target Slack webhook endpoint. Prefer `secret://` indirection for production secrets handling. |
| `--smtp-password` | string | Plain string or `secret://name` | none | SMTP auth password for email notifications. |
| `--smtp-port` | string | Numeric port string (commonly `587` or `465`) | `587` | SMTP server port. `587` is typical for STARTTLS; `465` is typical for implicit TLS. |
| `--smtp-server` | string | Hostname or IP | none | SMTP relay host used for email delivery. |
| `--smtp-user` | string | Username/login string | none | SMTP authentication username. |
| `--timeout` | duration string | Go duration (`5m`, `15m`, `30m`) | `15m` | Per-cluster overall timeout budget (start + poll + summary + write). |
| `--username` | string | Prism username | `admin` | Global Prism username fallback. Can be overridden per cluster by `clusters-file` entries. |
| `--webhook-enabled` | bool | `true`, `false` | `false` | Enables generic webhook notifications for each event or digest summary. |
| `--webhook-headers` | map | `key=value` pairs (comma-separated) | empty map | Adds custom headers to webhook HTTP requests (tokens, tenant IDs, routing hints). |
| `--webhook-include-html` | bool | `true`, `false` | `false` | Embeds HTML report content as base64 in webhook payloads. Increases payload size. |
| `--webhook-url` | string | HTTP/HTTPS URL or `secret://name` | none | Destination webhook endpoint URL for outbound notifications. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints command usage and flag help. |

## 6) Subcommand flags

Note: legacy root flags `--env-info`, `--tc`, `--update`/`-u`, `--gen-test-agg`, and `--version`/`-v` are still accepted as deprecated aliases.

### 6.1 `discover-clusters`

```bash
ncc-orchestrator discover-clusters [flags]
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--discover-api-version` | string | `v4`, `v3` | `v4` | Selects discovery endpoint family. `v4` uses clustermgmt GET with pagination; `v3` uses legacy list API. |
| `--format` | string | `lines`, `table`, `json` | `lines` | Output renderer: `lines` for direct `clusters-file` usage, `table` for human review, `json` for automation pipelines. |
| `--insecure-skip-verify` | bool | `true`, `false` | `false` | Disables TLS verification for Prism Central API calls. Also required if `--prism-central-url` uses `http://` instead of `https://`. |
| `--output` | string | File path | none | Writes discovered addresses to file (one per line), useful to bootstrap `clusters-file`. |
| `--password` | string | Plain string or env-injected value | prompt/none | Prism Central password for discovery operation only. |
| `--prism-central-url` | string | URL such as `https://pc:9440` | none | Required Prism Central endpoint for cluster list queries. |
| `--username` | string | Username string | `admin` | Prism Central username for discovery API calls. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.2 `env-info`

```bash
ncc-orchestrator env-info
```

Prints all supported `NCC_*` environment variables with current values (sensitive values masked).

### 6.3 `terms`

```bash
ncc-orchestrator terms
```

Prints terms and conditions text and exits.

### 6.4 `update`

```bash
ncc-orchestrator update
```

By default, updates remain in the current major track (for example `v1.x` -> latest `v1.x`). Use `--allow-major-upgrade` to move across major versions (for example `v1` to `v2`) after migration review.

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--check` | bool | `true`, `false` | `false` | Check-only mode. Reports selected release/binary availability without downloading or replacing. |
| `--allow-major-upgrade` | bool | `true`, `false` | `false` | Explicitly permits major-version upgrades. Required for `v1.x` -> `v2.x` transitions. |
| `--repo` | string | `owner/repo` or GitHub repo URL | `lTSPV75BRO/Nutanix-ncc-orchestrator` | GitHub source repo used for release discovery/check/update. |
| `--binary-url` | string | Direct binary URL | empty | Use a non-GitHub/custom artifact URL for check/update operations. |
| `--binary-sha256` | string | 64-char SHA256 hex | empty | Required when installing via `--binary-url` (ignored for `--check`). Used to enforce artifact integrity. |
| `--target-version` | string | Semver-like value | empty | Target version hint, recommended with `--binary-url` for track comparisons/safety checks. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.5 `gen-test-agg`

```bash
ncc-orchestrator gen-test-agg --clusters 25 --output-dir dist/test/outputfiles
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--clusters` | int | Integer `>= 1` | none | Number of synthetic clusters to generate in aggregated artifacts. |
| `--output-dir` | string | Writable directory path | `outputfiles` | Destination directory for generated synthetic artifacts. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.6 `version`

```bash
ncc-orchestrator version
```

Prints version/build/Go metadata and exits.

### 6.7 `create-schedule`

```bash
ncc-orchestrator create-schedule [flags]
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--action` | string | `create`, `list`, `remove`, `run-now` | `create` | Scheduler operation to execute. Use `list` to audit, `remove` for cleanup, `run-now` for immediate validation. |
| `--command` | string | Full shell command | auto-generated | Advanced override for the exact scheduled command line. Use only when default command generation is insufficient. |
| `--config` | string | Config file path | none | Config path passed through to scheduled runs to ensure deterministic behavior. |
| `--cron` | string | Standard 5-field cron expression | derived from `--every` when empty | Explicit cron schedule for `--type cron`. Takes precedence over `--every` derivation. |
| `--every` | duration | Go duration (`30m`, `4h`, `24h`) | `4h` | Used to derive schedule intervals (cron or Windows task cadence). |
| `--log-path` | string | File path | `logs/ncc-scheduler.log` | Output redirection path for scheduled command logs. |
| `--print-only` | bool | `true`, `false` | `true` | Safety preview mode. Keep true to inspect planned scheduler changes before applying. |
| `--task-name` | string | Task/marker name string | `ncc-orchestrator` | Identifier used in cron marker comments or Windows task naming. |
| `--type` | string | `auto`, `cron`, `windows` | `auto` | Scheduler backend. `auto` picks platform-appropriate implementation. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.8 `validate-config`

```bash
ncc-orchestrator validate-config --config config.yaml
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | none | Validates config keys/types/constraints and exits without running NCC checks. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.9 `config-schema`

```bash
ncc-orchestrator config-schema --output config.schema.json
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--output` | string | File path | stdout | Writes generated JSON schema to file; when omitted schema is printed to stdout. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.10 `validate-secrets`

```bash
ncc-orchestrator validate-secrets --config config.yaml
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | none | Validates `secret://` references and secret-source accessibility (`env`/`file`) without running NCC checks. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.11 `preflight-check`

```bash
ncc-orchestrator preflight-check --config config.yaml --format json
```

| Flag | Type | Possible values | Default | Detailed explanation |
|---|---|---|---|---|
| `--config` | string | Path to YAML/JSON config | none | Runs preflight checks against this config. If omitted, report includes a warning that file-based checks are skipped. |
| `--format` | string | `json` | `json` | Structured output for UI/automation. Includes `checks[]`, `actionableHints[]`, and machine-readable `remediation_code` on non-pass checks. |
| `--help` / `-h` | bool | `true`, `false` | `false` | Prints subcommand help. |

### 6.12 v2 service runtime flags (API/UI)

These are runtime flags for v2 services (`cmd/ncc-api-server`, `cmd/ncc-ui-server`), commonly used in production deployments:

| Service | Flag | Default | Purpose |
|---|---|---|---|
| `ncc-api-server` | `--rate-limit-per-minute` | `60` | Per-client rate limit for sensitive mutation/auth routes (`0` disables). |
| `ncc-api-server` | `--auth-mode` | `token` | API auth mode: `token`, `session`, `hybrid`. |
| `ncc-api-server` | `--token-file-path` | `.ncc-api-token` | Token file used by UI proxy and local tooling. |
| `ncc-ui-server` | `--allowed-origins` | `http://localhost:8080` | Browser origin allowlist for proxied API calls. |
| `ncc-ui-server` | `--api-auth-mode` | `token` | Backend auth forwarding mode (`token` or `session`). |

For complete v2 deployment examples, see:
- `README.md` (Run backend / Run frontend)
- `docs/V2_BACKEND_FRONTEND_MVP.md`
- `k8s/README.md`

## 7) Full config example

```yaml
clusters: "10.38.66.37,10.38.66.7"
# clusters-file: "clusters.txt"
username: "admin"
password: "secret://NCC_PRISM_PASSWORD"
ncc-api-version: "v4"
nutanix-v4-api-version: "v4.2"
insecure-skip-verify: false

timeout: "15m"
request-timeout: "20s"
poll-interval: "15s"
poll-jitter: "2s"
max-parallel: 4
adaptive-parallelism: true

outputs: "html,csv"
output-dir-logs: "nccfiles"
output-dir-filtered: "outputfiles"
single-report: false

log-file: "logs/ncc-runner.log"
log-level: "info"
log-http: false

retry-max-attempts: 6
retry-base-delay: "400ms"
retry-max-delay: "8s"

max-idle-conns: 100
max-idle-conns-per-host: 10
max-conns-per-host: 0
idle-conn-timeout: "90s"

prom-dir: "promfiles"

run-history: false
run-history-dir: "outputfiles/runs"
retain-last: 0
retain-days: 0
notify-on-regression: false

policy-gates: "new-fails>0,fail-rate>2,min-health-score<90"
quiet-hours: ""
maintenance-windows: ""
flaky-lookback-runs: 6
flaky-min-transitions: 2
severity-filter: ""

dry-run: false
replay: false

email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: "secret://SMTP_PASSWORD"
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true

webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "secret://WEBHOOK_TOKEN"

slack-enabled: false
slack-webhook-url: "secret://SLACK_WEBHOOK_URL"
slack-channel: "#ncc-alerts"

secrets-provider: "env"
secrets-file: ""
```

## 8) Environment variable mapping

Any config key can be provided via env with:
- prefix `NCC_`
- uppercase
- hyphens replaced by underscores

Examples:
- `username` -> `NCC_USERNAME`
- `clusters-file` -> `NCC_CLUSTERS_FILE`
- `request-timeout` -> `NCC_REQUEST_TIMEOUT`
- `webhook-include-html` -> `NCC_WEBHOOK_INCLUDE_HTML`

Print current values:

```bash
ncc-orchestrator env-info
```

## 9) Execution lifecycle (detailed)

This section explains what happens during a normal run.

### 9.1 Startup and config resolution

1. Parse CLI flags.
2. Load config file when `--config` is set.
3. Apply environment overrides (`NCC_*`).
4. Apply defaults for unset fields.
5. Resolve `secret://` references (when `secrets-provider` is configured).
6. Validate final effective config (strict validations, known keys, value types).

### 9.2 Cluster target preparation

- If `clusters-file` is set and readable, it becomes the primary source of target clusters.
- Effective per-cluster credentials are resolved in this order:
  1. Per-line username/password from `clusters-file` (if provided)
  2. Global `username` / `password`
  3. Interactive password prompt (if still missing and command is interactive)
- Duplicate clusters fail fast during validation.

### 9.3 Per-cluster run phases

For each cluster worker:

1. Start checks API call (v4 path by default, legacy fallback behavior where applicable).
2. Poll task status at `poll-interval +/- poll-jitter`.
3. Fetch NCC summary after completion.
4. Persist raw summary log under `output-dir-logs`.
5. Parse blocks and generate selected per-cluster outputs.

### 9.4 Aggregation and historical analytics

After cluster workers finish:

- Build aggregated `index.html`.
- Write run-level artifacts (`run-summary.json`, `ncc-run-record.json`).
- Compute diff/regression/flaky/SLO artifacts if prior snapshots exist.
- Evaluate policy gates.
- Emit notifications (unless suppressed by quiet-hours / maintenance windows).
- Optionally persist snapshot to run-history and apply retention.

## 10) Exit codes and automation behavior

| Exit code | Meaning | Common automation action |
|---|---|---|
| `0` | Success (no runner-level failure) | Mark pipeline green |
| `1` | Fatal/general error | Fail pipeline and inspect logs |
| `2` | Configuration error/validation failure | Stop early, fix config |
| `3` | Partial success (some clusters failed, some succeeded) | Mark unstable/partial and inspect failed clusters |

### 10.1 Policy-gate influence

Policy gate violations can force failure behavior in automation contexts by design. Always consume:

- `run-summary.json`
- `policy-gates.txt` (when present)

…instead of relying only on console output.

## 11) Feature deep dive (operational detail)

### 11.1 Policy gates: syntax and metric semantics

Supported operators:
- `>`
- `>=`
- `<`
- `<=`
- `==`
- `!=`

Supported metrics:
- `new-fails`: new FAIL checks compared to previous snapshot
- `resolved-fails`: FAIL checks resolved since previous snapshot
- `fail-rate`: current FAIL percentage (0..100)
- `clusters-failed`: number of clusters with runner-level failure
- `regressions`: binary indicator (`1` yes, `0` no)
- `flaky-checks`: detected flaky checks count
- `min-health-score`: minimum cluster health score in run
- `avg-health-score`: average health score across successful clusters

Example gate sets by environment:

```text
# Strict production
new-fails>0,fail-rate>1,min-health-score<95

# Lab / staging
new-fails>5,fail-rate>5,min-health-score<85
```

### 11.2 Flaky detection tuning strategy

Use these controls together:

- `flaky-lookback-runs`: larger values increase sensitivity to long-term instability.
- `flaky-min-transitions`: higher values reduce noise.

Recommended starting points:
- Small environments: `lookback=6`, `transitions=2`
- Large/variable environments: `lookback=10-15`, `transitions=3-4`

### 11.3 Health score usage

Use health score for:
- ranking worst clusters first
- SLO threshold checks (`min-health-score` gate)
- trend analysis over historical snapshots

Use both `min-health-score` and `avg-health-score` gates to avoid blind spots (single critical cluster vs broad degradation).

### 11.4 Quiet hours vs maintenance windows

- `quiet-hours`: recurring daily local-time suppression window (good for nighttime)
- `maintenance-windows`: explicit RFC3339 intervals (good for planned maintenance)

When both are configured, suppression applies if **either** condition matches.

### 11.5 Replay mode: expected behavior

Replay does not call NCC start/poll APIs. It consumes existing logs to regenerate:
- per-cluster outputs
- aggregated dashboard
- run artifacts (where derivable from available data)

Use replay when:
- validating report template changes
- testing notification payloads safely
- rebuilding artifacts after a non-data code change

## 12) Artifact guide (field-level orientation)

### 12.1 `run-summary.json`

Primary machine-readable run result:
- run-level timing and status
- per-cluster outcomes (ok/error)
- severity counts
- effective exit code

### 12.2 `ncc-run-record.json`

Wraps run summary with:
- schema metadata
- orchestrator version/build context
- stable envelope useful for long-term ingestion

### 12.3 `checks-snapshot.json`

Per-check state snapshot used as baseline for:
- regressions
- drill-down diffs
- flaky detection

### 12.4 `drilldown-diff.json`

Diff of current snapshot vs prior snapshot:
- newly failing checks
- resolved checks
- severity transitions

### 12.5 `flaky-checks.json`

Checks with repeated severity transitions in lookback window.

### 12.6 `regression-summary.json`

Compact summary of FAIL deltas and directional movement.

### 12.7 `slo-dashboard.json`

SLO-friendly export of cluster-level health/status metrics for dashboard ingestion.

## 13) Flag tuning playbooks (by objective)

### 13.1 Reduce API pressure / throttling

```bash
ncc-orchestrator \
  --max-parallel 2 \
  --adaptive-parallelism \
  --retry-max-attempts 8 \
  --retry-base-delay 600ms \
  --retry-max-delay 15s
```

### 13.2 Faster feedback in CI

```bash
ncc-orchestrator \
  --timeout 8m \
  --request-timeout 15s \
  --poll-interval 8s \
  --outputs json,csv \
  --policy-gates "new-fails>0,fail-rate>2"
```

### 13.3 Stable nightly operations

```bash
ncc-orchestrator \
  --max-parallel 4 \
  --run-history \
  --retain-last 30 \
  --notify-digest \
  --quiet-hours "23:00-06:00"
```

## 14) Security and compliance guidance

### 14.1 Credential handling best practices

- Prefer `secret://` + `secrets-provider` over plaintext passwords.
- Prefer environment-based secret injection in CI/CD.
- Avoid embedding static secrets in tracked config files.
- Use masked logging defaults; do not enable `--log-http` in production unless necessary.

### 14.2 TLS guidance

- Keep `insecure-skip-verify=false` for production.
- Use `true` only for controlled lab/self-signed test environments.

### 14.3 Artifact sensitivity

Report artifacts can include check names/details that may be operationally sensitive. Apply directory permissions and retention controls according to policy.

## 15) Troubleshooting matrix

| Symptom | Likely cause | Check | Fix |
|---|---|---|---|
| Immediate exit with config error | Invalid key/type or missing required values | `validate-config` output | Fix config and re-run validation |
| Cluster starts but poll fails | Timeout/network/API permissions | Logs + request timeout values | Increase timeout, validate reachability/credentials |
| Many 429 responses | Too much concurrency | logs + rate-limit behavior | Lower `max-parallel`, keep adaptive parallelism on |
| Empty/partial aggregated report | No parsable logs or replay source mismatch | raw logs in `output-dir-logs` | Ensure logs exist and are complete, rerun |
| No notifications | Suppression window active or channel misconfig | quiet-hours/windows + channel config | Adjust windows and validate endpoints |

## 16) Production-ready baseline profiles

### 16.1 Conservative production profile

```yaml
max-parallel: 3
adaptive-parallelism: true
timeout: "20m"
request-timeout: "25s"
poll-interval: "15s"
retry-max-attempts: 8
retry-base-delay: "500ms"
retry-max-delay: "12s"
run-history: true
retain-last: 30
notify-digest: true
policy-gates: "new-fails>0,fail-rate>2,min-health-score<90"
```

### 16.2 Fast feedback profile (pre-merge checks)

```yaml
max-parallel: 2
timeout: "8m"
request-timeout: "15s"
poll-interval: "8s"
outputs: "json,csv"
policy-gates: "new-fails>0"
notify-digest: false
```

## 17) Common gotchas

1. `clusters-file` overrides `clusters` when present and non-empty.
2. Per-cluster credentials in `clusters-file` override global username/password for that cluster.
3. `replay` requires existing logs/artifacts; it does not pull fresh NCC data.
4. `severity-filter` affects output view/content; use full severity set for baseline investigations.
5. `run-history` can grow quickly on frequent schedules; set `retain-last` and/or `retain-days`.
