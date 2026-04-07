# Release notes – v0.1.12

**Release date:** 2026-02-12

This release adds digest notifications, notification retries, run summary logging, replay HTML attachment, config path validation, HTTP log redaction, and documentation/runbook updates.

---

## New features

### Digest notifications (`--notify-digest`)

- Send **one email, one webhook, and one Slack message per run** with a run overview (clusters OK/failed, duration) instead of one per cluster.
- Optional: attach the aggregated `index.html` to the digest email (`email-attach-html`) or include it as base64 in the webhook payload (`webhook-include-html`).
- Config: `notify-digest: true` or env `NCC_NOTIFY_DIGEST=true`. See [README](README.md#testing-email-and-webhook) for testing.

### Run summary log

- At the end of each run, a single structured log line is written with:
  - `clusters_ok`, `clusters_failed`, `duration_s`, `index_html` path.
- Useful for monitoring and scripting.

### Notification retries

- Email and webhook sends are retried up to 3 times with jittered backoff (using `retry-base-delay` / `retry-max-delay`) on failure.
- Reduces impact of transient network or 5xx errors.

### Replay + HTML attach

- In **replay** mode, per-cluster HTML and CSV are generated **before** sending notifications.
- When `email-attach-html` is set, the generated HTML is attached to the replay email.
- When `webhook-include-html` is set, the replay webhook payload includes the report as base64.

### Config path validation

- `output-dir-logs`, `output-dir-filtered`, `log-file`, and `prom-dir` must be non-empty (after trim); otherwise validation fails with a clear error.

### HTTP log redaction (`log-http`)

- When `log-http` is enabled, request/response dumps no longer expose:
  - `Authorization` and `Cookie` headers (replaced with `[REDACTED]`).
  - JSON values for `"password"` / `"Password"` fields (masked).

---

## Documentation

- **README:** HTTP pool/timeouts, example webhook payload, **Testing email and webhook** (webhook.site, MailHog, Mailtrap), `--version` behavior, `notify-digest` and env vars.
- **k8s/README:** **Runbook** for CronJob failures: logs, one-off debug job, NFS/permissions, prune images.
- **k8s ConfigMap:** `notify-digest: false` option and comment added.
- **CHANGELOG.md:** Version history and release notes.

---

## Other changes

- Per-cluster email/webhook/Slack are skipped when `notify-digest` is true.
- New unit tests for `validateConfig` (path validation), `checkOutputPermissions`, digest overview format, and `ParseSummary` edge cases.

---

## Upgrade

- No breaking changes. New flags and config keys are additive; defaults preserve previous behavior.
- Optional: set `notify-digest: true` in config or via `NCC_NOTIFY_DIGEST=true` to use digest notifications.
- Optional: set `notify-digest: false` in `k8s/configmap.yaml` if you deploy via Kubernetes (default is already `false`).

---

## Checksums / Docker

- **Version:** 0.1.12 (from [VERSION](VERSION)).
- **Docker:** `prajwalnutant/nutanix-ncc-orchestrator:0.1.12` (and `:latest` when built from main).
- Build and test: `go build ./...` and `go test ./...` pass.
