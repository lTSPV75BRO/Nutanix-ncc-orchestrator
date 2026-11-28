package main

import (
	"os"
	"path/filepath"
	"strings"
)

func writeDummyConfig(path string) error {
	ext := strings.ToLower(filepath.Ext(path))
	dummy := ""
	switch ext {
	case ".yaml", ".yml":
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "REPLACE_WITH_CLUSTER_IP"        # Real Prism Element IP or DNS name. Leave this sample and the run stops with an explanation.
# clusters-file: ""                        # Optional: cluster or cluster,username[,password] per line; overrides clusters when set
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias)
nutanix-v4-api-version: v4.2              # v4 path revision (v4.2 default; e.g. v4.1, v4.0.a1)
cluster-source-mode: clusters             # clusters (manual list) or pc (discover from Prism Central)
pcs: ""                                   # Comma-separated Prism Central IPs/FQDNs/URLs when mode is pc
pcs-file: ""                              # Optional file, one Prism Central per line
prism-central-url: ""                     # Optional explicit PC base URL
discover-api-version: v4                  # v4 (default) or v3 for PC cluster discovery
pc-alerts-cache-ttl: "5m"                 # Prism Central alert cache. 0 disables it
ca-bundle: ""                             # PEM file of extra trusted CAs
pin-sha256: ""                            # Comma-separated server-cert SHA-256 pins
dry-run: false                            # Validate only; do not start NCC checks
replay: false                             # Read saved reports instead of contacting clusters
gen-test-agg: 0                           # Leave 0. Above 0 fabricates aggregate rows and is not a scan
skip-preflight-check: false
severity-filter: ""                       # Empty = all. Otherwise FAIL,WARN,ERR,INFO
prom-enabled: false
prom-dir: "outputfiles/prom"
max-idle-conns: 0                         # 0 = built-in HTTP pool default
max-idle-conns-per-host: 0
max-conns-per-host: 0                     # 0 = unlimited
idle-conn-timeout: "90s"
notification-deadletter-dir: ""           # Where undelivered notifications are kept
email-subject-template: ""                # Optional Go text/template
email-body-template: ""
smtp-insecure-skip-verify: false
webhook-template: ""                      # Optional Go text/template; must render JSON
webhook-secret: ""                        # HMAC secret; sent as X-NCC-Signature

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 0                           # Clusters checked at once; 0 = auto (one per cluster, up to 500)
retry-failed-clusters: 1                  # Re-run clusters that timed out / hit network errors (0-5)
resume: false                             # Reuse clusters an interrupted run already finished
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
artifact-retain-days: 0                   # Remove generated artifacts older than N days (0 = unlimited)
artifact-retain-max-files: 0              # Keep only N newest generated artifacts (0 = unlimited)
raw-log-retain-days: 0                    # Remove raw NCC logs older than N days (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s
policy-gates: ""                          # e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0
quiet-hours: ""                           # Local HH:MM-HH:MM notification quiet window
maintenance-windows: ""                   # RFC3339 windows start/end[,start/end...]
flaky-lookback-runs: 6                    # Runs to inspect for flaky checks
flaky-min-transitions: 2                  # Minimum severity transitions to mark flaky
exclude-alert-titles: ""                  # Comma-separated NCC alert titles to exclude from final reports/HTML
exclude-alert-titles-file: ""             # Optional file with one alert title per line
exclude-alert-match-mode: "exact"         # exact (default), contains, regex

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  

# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true
# email-subject-template: ""              # Optional Go text/template, e.g. "NCC {{.Cluster}}: {{.FailCount}} FAIL"
# email-body-template: ""                 # Optional Go text/template for the email body
# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"
# webhook-template: ""                     # Optional Go text/template for the webhook body (must render valid JSON)

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
secrets-provider: ""                      # env or file
secrets-file: ""                          # YAML/JSON key-value map when secrets-provider=file

`
	case ".json":
		dummy = `{
  "clusters": "REPLACE_WITH_CLUSTER_IP",
  "clusters-file": "",
  "username": "admin",
  "password": "",
  "ncc-api-version": "v4",
  "nutanix-v4-api-version": "v4.2",
  "cluster-source-mode": "clusters",
  "pcs": "",
  "pcs-file": "",
  "prism-central-url": "",
  "discover-api-version": "v4",
  "pc-alerts-cache-ttl": "5m",
  "ca-bundle": "",
  "pin-sha256": "",
  "dry-run": false,
  "replay": false,
  "gen-test-agg": 0,
  "skip-preflight-check": false,
  "severity-filter": "",
  "prom-enabled": false,
  "prom-dir": "outputfiles/prom",
  "max-idle-conns": 0,
  "max-idle-conns-per-host": 0,
  "max-conns-per-host": 0,
  "idle-conn-timeout": "90s",
  "notification-deadletter-dir": "",
  "email-subject-template": "",
  "email-body-template": "",
  "smtp-insecure-skip-verify": false,
  "webhook-template": "",
  "webhook-secret": "",
  "insecure-skip-verify": false,
  "timeout": "15m",
  "request-timeout": "30s",
  "poll-interval": "15s",
  "poll-jitter": "2s",
  "max-parallel": 0,
  "retry-failed-clusters": 1,
  "resume": false,
  "outputs": "html,csv",
  "output-dir-logs": "nccfiles",
  "output-dir-filtered": "outputfiles",
  "single-report": false,
  "run-history": false,
  "run-history-dir": "outputfiles/runs",
  "retain-last": 0,
  "retain-days": 0,
  "artifact-retain-days": 0,
  "artifact-retain-max-files": 0,
  "raw-log-retain-days": 0,
  "notify-on-regression": false,
  "adaptive-parallelism": true,
  "policy-gates": "",
  "quiet-hours": "",
  "maintenance-windows": "",
  "flaky-lookback-runs": 6,
  "flaky-min-transitions": 2,
  "exclude-alert-titles": "",
  "exclude-alert-titles-file": "",
  "exclude-alert-match-mode": "exact",
  "log-file": "logs/ncc-runner.log",
  "log-level": "2",
  "log-http": false,
  "retry-max-attempts": 6,
  "retry-base-delay": "400ms",
  "retry-max-delay": "8s",
  "retry-circuit-breaker": 3,
  "email-enabled": false,
  "email-attach-html": false,
  "notify-digest": false,
  "smtp-server": "smtp.example.com",
  "smtp-port": 587,
  "smtp-user": "ncc@example.com",
  "smtp-password": "",
  "email-from": "ncc@example.com",
  "email-to": "ops@example.com,sre@example.com",
  "email-use-tls": true,
  "webhook-enabled": false,
  "webhook-include-html": false,
  "webhook-url": "https://hooks.example.com/ncc",
  "webhook-headers": {
    "X-Auth-Token": "changeme"
  },
  "slack-enabled": false,
  "slack-webhook-url": "",
  "slack-channel": "",
  "secrets-provider": "",
  "secrets-file": ""
}
`
	default:
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "REPLACE_WITH_CLUSTER_IP"        # Real Prism Element IP or DNS name. Leave this sample and the run stops with an explanation.
# clusters-file: ""                        # Optional: cluster or cluster,username[,password] per line; overrides clusters when set
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env
ncc-api-version: v4                       # v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias)
nutanix-v4-api-version: v4.2              # v4 path revision (v4.2 default; e.g. v4.1, v4.0.a1)
cluster-source-mode: clusters             # clusters (manual list) or pc (discover from Prism Central)
pcs: ""                                   # Comma-separated Prism Central IPs/FQDNs/URLs when mode is pc
pcs-file: ""                              # Optional file, one Prism Central per line
prism-central-url: ""                     # Optional explicit PC base URL
discover-api-version: v4                  # v4 (default) or v3 for PC cluster discovery
pc-alerts-cache-ttl: "5m"                 # Prism Central alert cache. 0 disables it
ca-bundle: ""                             # PEM file of extra trusted CAs
pin-sha256: ""                            # Comma-separated server-cert SHA-256 pins
dry-run: false                            # Validate only; do not start NCC checks
replay: false                             # Read saved reports instead of contacting clusters
gen-test-agg: 0                           # Leave 0. Above 0 fabricates aggregate rows and is not a scan
skip-preflight-check: false
severity-filter: ""                       # Empty = all. Otherwise FAIL,WARN,ERR,INFO
prom-enabled: false
prom-dir: "outputfiles/prom"
max-idle-conns: 0                         # 0 = built-in HTTP pool default
max-idle-conns-per-host: 0
max-conns-per-host: 0                     # 0 = unlimited
idle-conn-timeout: "90s"
notification-deadletter-dir: ""           # Where undelivered notifications are kept
email-subject-template: ""                # Optional Go text/template
email-body-template: ""
smtp-insecure-skip-verify: false
webhook-template: ""                      # Optional Go text/template; must render JSON
webhook-secret: ""                        # HMAC secret; sent as X-NCC-Signature

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 0                           # Clusters checked at once; 0 = auto (one per cluster, up to 500)
retry-failed-clusters: 1                  # Re-run clusters that timed out / hit network errors (0-5)
resume: false                             # Reuse clusters an interrupted run already finished
outputs: "html,csv"                       # One or more: html,csv,json,markdown,sarif
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  
single-report: false                      # Also write ncc-report-single.html
run-history: false                        # Save each run snapshot under run-history-dir
run-history-dir: "outputfiles/runs"       # History base directory
retain-last: 0                            # Keep last N snapshots (0 = unlimited)
retain-days: 0                            # Keep snapshots newer than N days (0 = unlimited)
notify-on-regression: false               # Notify only when FAIL count increases
adaptive-parallelism: true                # Reduce/increase effective concurrency on 429s

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  

# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  
retry-circuit-breaker: 3                  # Fail fast after N consecutive retryable failures

# Email notifications
email-enabled: false
email-attach-html: false
notify-digest: false
smtp-server: "smtp.example.com"
smtp-port: 587
smtp-user: "ncc@example.com"
smtp-password: ""
email-from: "ncc@example.com"
email-to: "ops@example.com,sre@example.com"
email-use-tls: true
# email-subject-template: ""              # Optional Go text/template, e.g. "NCC {{.Cluster}}: {{.FailCount}} FAIL"
# email-body-template: ""                 # Optional Go text/template for the email body
# Webhook notifications
webhook-enabled: false
webhook-include-html: false
webhook-url: "https://hooks.example.com/ncc"
webhook-headers:
  X-Auth-Token: "changeme"
# webhook-template: ""                     # Optional Go text/template for the webhook body (must render valid JSON)

# Slack notifications
slack-enabled: false
slack-webhook-url: ""
slack-channel: ""
`
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(dummy), 0644)
}
