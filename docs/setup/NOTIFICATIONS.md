# Set up notifications: email, Slack, webhooks and other chat tools

NCC Orchestrator can tell you when a run fails, a policy gate is broken, a
backup or automatic repair fails, and send a regular health digest. Everything
here is on **Settings → Notifications** (admins change it; operators can send
test messages).

- [How it works](#how-it-works)
- [Email](#email)
- [Slack](#slack)
- [Webhook](#webhook) (your own endpoint, automation tools)
- [Microsoft Teams, Google Chat, Discord and others](#microsoft-teams-google-chat-discord-and-others)
- [Choose what to send](#choose-what-to-send)
- [Quiet hours and maintenance windows](#quiet-hours-and-maintenance-windows)
- [Check delivery](#check-delivery)
- [Troubleshooting](#troubleshooting)

## How it works

There are two kinds of message, and both use the channels you set up on this
page:

- **Alerts, digests and tests** come from the API server: run failures,
  policy violations, successful runs (if you want them), failed backups,
  failing automatic repairs, and the health digest.
- **Run reports** come from the runner after each run, with the FAIL, WARN,
  ERR and INFO counts per cluster.

The switch at the top (**On** / **Paused**) pauses alerts and digests from the
server. Run reports keep going while it is paused.

The runner tries each channel 3 times before a run report counts as failed.
Settings are saved in the main config file (`config.yaml`), so they are part
of backups.

## Email

You need an SMTP server that will accept mail from the NCC Orchestrator host.

1. **Settings → Notifications → Where to send → Email**, turn it on.
2. Fill in:

   | Field | Example | Notes |
   | --- | --- | --- |
   | Mail server | `smtp.example.com` | |
   | Port | `587` | `587` = STARTTLS (usual), `465` = TLS from the start, `25` = internal relay |
   | Sender | `ncc@example.com` | Must be an address the server lets you send as |
   | Recipients | `sre@example.com, oncall@example.com` | Comma-separated |
   | Username / Password | | Leave empty for a relay that needs no sign-in. The password is never shown again. |

3. **Save**, then **Send test** on the Email card.

**Common mail servers**

| Service | Server and port | Notes |
| --- | --- | --- |
| Internal relay (Exchange connector, Postfix) | your relay, `25` or `587` | Usually the simplest: allow the NCC host's IP on the relay, no username. |
| Microsoft 365 | `smtp.office365.com`, `587` | The mailbox needs *Authenticated SMTP* turned on. If your tenant blocks SMTP sign-in, use an Exchange connector / relay instead. |
| Gmail / Google Workspace | `smtp.gmail.com`, `587` | Use an *app password* (needs 2-step verification), not the account password. |
| Amazon SES | `email-smtp.<region>.amazonaws.com`, `587` | Use SES SMTP credentials and a verified sender. |

**TLS.** Mail is sent over TLS when the server offers it. These config file
keys (Settings → Clusters & config) change that:

- `email-use-tls: true` (default) refuses a server that doesn't offer
  STARTTLS.
- `smtp-insecure-skip-verify: true` accepts a relay with a self-signed
  certificate, without turning off certificate checks for Prism.

## Slack

1. In Slack: **Apps → Incoming WebHooks** (or create an app at
   <https://api.slack.com/apps> → *Incoming Webhooks* → *Add New Webhook to
   Workspace*) and pick the channel. Copy the webhook URL
   (`https://hooks.slack.com/services/…`).
2. **Settings → Notifications → Slack**, turn it on, paste the **Webhook URL**.
   **Channel** (for example `#sre-alerts`) is optional; most new Slack
   webhooks always post to the channel they were created for.
3. **Save**, then **Send test**.

Treat the webhook URL like a password: anyone with it can post to the channel.
It is never shown again after saving, and it is hidden in logs.

## Webhook

The webhook posts JSON to any HTTP(S) endpoint: your own service, an
automation tool (Power Automate, Zapier, n8n, Logic Apps), or an incident tool
that accepts generic webhooks.

1. **Settings → Notifications → Webhook**, turn it on, enter the **URL**.
2. **Save**, then **Send test**.

**What is sent.** Alerts, digests and tests:

```json
{
  "event": "run_failure",
  "title": "NCC run failed",
  "time": "2026-11-08T06:00:12Z",
  "repo_root": "/opt/ncc",
  "details": { "error": "exit status 2", "started_at": "2026-11-08T05:58:40Z" }
}
```

`event` is one of `run_failure`, `run_success`, `policy_violations`,
`backup_failure`, `selfheal_failure`, `digest` or `manual_test`.

Run reports from the runner carry the per-cluster counts: `Cluster`,
`StartedAt`, `FinishedAt`, `FailCount`, `WarnCount`, `ErrCount`, `InfoCount`,
`TotalChecks`, `OutputFiles` and `Overview`.

**Prove the message came from NCC Orchestrator.** Set a shared secret in the
config file:

```yaml
webhook-secret: "secret://WEBHOOK_HMAC"   # or the NCC_WEBHOOK_SECRET environment variable
```

Every request then carries `X-NCC-Signature: sha256=<hex>`, an HMAC-SHA256 of
the raw body. Check it on the receiving side:

```python
import hashlib, hmac

def is_from_ncc(raw_body: bytes, header: str, secret: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header or "")
```

**Extra headers** (for example an API key your endpoint wants) go in the
config file:

```yaml
webhook-headers:
  X-Api-Key: "the-receivers-key"
```

Header values are sent as written, so keep `config.yaml` readable only by the
service account.

## Microsoft Teams, Google Chat, Discord and others

These tools expect their own message format. You have two options:

- **Use a workflow tool in between (works for everything).** Point the
  Webhook channel at a Power Automate / Teams *Workflows* flow ("When a Teams
  webhook request is received"), a Logic App, Zapier or n8n, and let the flow
  post the `title` and `details` to the channel.
- **Shape the run reports directly.** `webhook-template` in the config file is
  a Go template for the runner's run reports, so they can be posted straight
  to the tool:

  ```yaml
  # Google Chat (incoming webhook URL from the space's Apps & integrations)
  webhook-template: '{"text":"NCC {{.Cluster}}: {{.FailCount}} FAIL, {{.WarnCount}} WARN, {{.ErrCount}} ERR"}'

  # Discord (channel Settings → Integrations → Webhooks)
  webhook-template: '{"content":"NCC {{.Cluster}}: {{.FailCount}} FAIL, {{.WarnCount}} WARN"}'
  ```

  The template must produce valid JSON; if it fails, the default JSON is sent
  instead. Alerts, digests and tests from the server are not templated, so
  with this option use the tool for run reports and email or Slack for alerts.

`email-subject-template` and `email-body-template` work the same way for run
report emails. The fields are listed under [Webhook](#webhook).

## Choose what to send

Under **What to send**:

| Setting | Sends |
| --- | --- |
| Run failures | A run that failed, plus failed backups and failing automatic repairs |
| Policy violations | A run that broke a policy gate |
| Successful runs | A message after every run. Can be noisy. |
| Send a digest | A summary of the latest run (clusters, FAIL and WARN totals, top policy violations) every 6 or 12 hours, daily or weekly. **Send digest now** sends one immediately. |
| Run report grouping | **One per cluster** or **One per run** |
| Only when failures increase | Skip a cluster's report unless it has more FAILs than last time |

To cut noise further, under **Quiet times**:

- **Ignore repeats of the same alert for** a period (0 turns it off).
- **Leave at least this long between alerts** (0 turns it off).

## Quiet hours and maintenance windows

- **Quiet hours** hold alerts every day between two times, in the time zone
  you pick. Turn on **Let failures through** to still get failure alerts.
- **Maintenance windows** hold alerts for a planned period, for example an
  AOS upgrade. Add a start, an end and a note.

## Check delivery

Each channel card shows its delivery status:

- **Delivering** with the time of the last message that got through.
- **Failing** with the error from the last attempt, and when it last worked.
- **Not sent yet** before the first message.

The status covers both run reports and server alerts. When
`notification-deadletter-dir` is set in the config, messages that failed
every retry are saved there as JSON and counted as **Undelivered**, so an
outage doesn't lose them silently.

## Troubleshooting

| Problem | What to check |
| --- | --- |
| **Send test** is greyed out | Save first: tests use the saved settings. |
| Email: `x509: certificate signed by unknown authority` | The relay uses an internal certificate. Set `smtp-insecure-skip-verify: true`, or add the CA to the server's trust store. |
| Email: `535 authentication failed` | Wrong password, Microsoft 365 SMTP sign-in turned off, or Gmail needs an app password. |
| Email: `554` / relay denied | The relay doesn't accept mail from this host or sender. Ask the mail team to allow the NCC host IP and sender address. |
| Slack: `invalid_token` or `no_service` | The webhook was removed or the app uninstalled. Create a new webhook URL. |
| Webhook: timeout | The NCC host can't reach the endpoint. Check the firewall and any outbound proxy. |
| Alerts stop at night | Quiet hours are on. |
| Run reports arrive but alerts don't | The top switch is **Paused**, or the alert type is off under **What to send**. |

## Configure in the config file instead

All of this can also be set in `config.yaml` (useful for automated installs
and for the runner on its own): `email-enabled`, `smtp-server`, `smtp-port`,
`smtp-user`, `smtp-password`, `email-from`, `email-to`, `slack-enabled`,
`slack-webhook-url`, `webhook-enabled`, `webhook-url`, `quiet-hours`,
`maintenance-windows`, `notify-digest` and more. Store passwords and URLs as
`secret://` references. The full list is in
[`FEATURES_AND_CONFIG_FLAGS.md` §2.11](../FEATURES_AND_CONFIG_FLAGS.md#211-notifications-emailwebhookslack).
