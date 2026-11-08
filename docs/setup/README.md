# Setup guides

Step-by-step guides for setting up NCC Orchestrator after it is installed.
Each one follows the dashboard's own page and field names. To install it
first, see the [README quick start](../../README.md#quick-start) or the
[download page](https://ltspv75bro.github.io/Nutanix-ncc-orchestrator/download.html).

| Guide | What it covers |
| --- | --- |
| [Connect your clusters](CLUSTERS.md) | Prism Element lists, Prism Central discovery, Prism credentials and certificates, environments |
| [Sign-in](SIGN_IN.md) | Roles, local accounts, Active Directory / LDAP, OpenID Connect (Entra ID, Okta, Google, Keycloak), SAML (Entra ID, Okta, ADFS), cluster groups |
| [HTTPS and certificates](HTTPS.md) | Your own certificate, trusting the self-signed one, Kubernetes Ingress |
| [Notifications](NOTIFICATIONS.md) | Email (Microsoft 365, Gmail, relays), Slack, webhooks, Teams / Google Chat / Discord, quiet hours, delivery status |
| [Schedule, backups and updates](SCHEDULE_AND_BACKUPS.md) | Scheduled runs on Linux, macOS, Windows and Kubernetes, backup keys, restores, in-app updates |
| [Other tools](INTEGRATIONS.md) | API tokens, ServiceNow, Prometheus and Grafana, CI/CD policy gates, secrets, AI assistants (MCP) |

## A typical first day

1. Sign in as `admin` and change the password.
2. [Connect your clusters](CLUSTERS.md) and start a first run from **Runs → New run**.
3. [Install a trusted certificate](HTTPS.md) for the dashboard's address.
4. [Turn on directory or SSO sign-in](SIGN_IN.md) and map your groups to roles.
5. [Set up notifications](NOTIFICATIONS.md) and send a test.
6. [Schedule runs and backups](SCHEDULE_AND_BACKUPS.md).

## Reference

- Every config key, environment variable and flag:
  [`FEATURES_AND_CONFIG_FLAGS.md`](../FEATURES_AND_CONFIG_FLAGS.md)
- Security model and binary verification:
  [`SECURITY_AND_TRUST.md`](../SECURITY_AND_TRUST.md)
- Common problems: [`TROUBLESHOOTING.md`](../TROUBLESHOOTING.md)
- Kubernetes and Helm: [`k8s/README.md`](../../k8s/README.md)
