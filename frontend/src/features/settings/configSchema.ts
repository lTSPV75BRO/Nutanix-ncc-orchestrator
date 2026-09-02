import type { ReactNode } from "react";

export type FieldType =
  | "string"
  | "password"
  | "boolean"
  | "int"
  | "select"
  | "duration"
  | "list-csv"
  | "textarea";

export type FieldDef = {
  key: string;
  label: string;
  type: FieldType;
  help?: string;
  placeholder?: string;
  options?: { value: string; label: string }[];
  min?: number;
  max?: number;
  step?: number;
  required?: boolean;
  /**
   * Optional explicit autoComplete attribute for browser hints. Defaults to
   * "off" for non-username/email/password fields so browsers don't silently
   * suggest the user's email for "PC IP" or similar config text inputs.
   */
  autoComplete?: string;
};

export type SectionDef = {
  id: string;
  title: string;
  description: string;
  /**
   * Identifier for the icon shown in the section nav. Mapped to an actual icon
   * component in ConfigSection.tsx so this file stays free of JSX.
   */
  icon: "cluster" | "perf" | "outputs" | "logs" | "policy" | "notify" | "secrets";
  fields: FieldDef[];
};

export const SECTIONS: SectionDef[] = [
  {
    id: "cluster",
    title: "Cluster Targets",
    description: "Define which clusters to scan and how to authenticate against them.",
    icon: "cluster",
    fields: [
      {
        key: "cluster-source-mode",
        label: "Cluster source",
        type: "select",
        help: "How clusters are discovered. 'clusters' uses the manual/file list below; 'pc' discovers clusters from Prism Central(s).",
        options: [
          { value: "", label: "Auto (default)" },
          { value: "clusters", label: "Manual list / file" },
          { value: "pc", label: "Prism Central discovery" },
        ],
      },
      {
        key: "pcs",
        label: "Prism Central(s)",
        type: "list-csv",
        help: "Comma-separated list of PC IPs/FQDNs. Used when cluster-source-mode is 'pc'.",
        placeholder: "10.48.58.121, pc.example.com",
      },
      {
        key: "pcs-file",
        label: "Prism Central file",
        type: "string",
        help: "Path to a file listing one Prism Central IP/FQDN per line.",
        placeholder: "/path/to/pcs.txt",
      },
      {
        key: "prism-central-url",
        label: "Prism Central URL",
        type: "string",
        help: "Optional explicit base URL when not derivable from the PC list.",
        placeholder: "https://pc.example.com:9440",
      },
      {
        key: "discover-api-version",
        label: "PC discovery API version",
        type: "select",
        help: "API version used when discovering clusters from Prism Central.",
        options: [
          { value: "v4", label: "v4 (default)" },
          { value: "Legacy", label: "Legacy (Prism Gateway v1)" },
        ],
      },
      {
        key: "clusters",
        label: "Clusters (manual)",
        type: "list-csv",
        help: "Comma-separated cluster IPs/FQDNs. Used when cluster-source-mode is 'list'.",
      },
      {
        key: "clusters-file",
        label: "Clusters file",
        type: "string",
        help: "Path to a file containing one 'cluster[,user[,password]]' per line.",
        placeholder: "/path/to/clusters.txt",
      },
      {
        key: "username",
        label: "Username",
        type: "string",
        required: true,
        placeholder: "admin",
        autoComplete: "username",
      },
      {
        key: "password",
        label: "Password",
        type: "password",
        help: "Plaintext password is discouraged. Prefer setting a secrets-provider below.",
      },
      {
        key: "insecure-skip-verify",
        label: "Skip TLS verification",
        type: "boolean",
        help: "Bypass certificate validation. Insecure — only enable for self-signed lab clusters.",
      },
      {
        key: "ca-bundle",
        label: "CA bundle (PEM file)",
        type: "string",
        help: "Path to a PEM file of extra trusted CAs — a safer alternative to skipping TLS verification.",
        placeholder: "/etc/ncc/prism-ca.pem",
      },
      {
        key: "pin-sha256",
        label: "Pinned cert SHA-256(s)",
        type: "list-csv",
        help: "Comma-separated allowed server-certificate SHA-256 fingerprints (certificate pinning).",
      },
      {
        key: "ncc-api-version",
        label: "NCC API version",
        type: "select",
        options: [
          { value: "v4", label: "v4 (default)" },
          { value: "Legacy", label: "Legacy (Prism Gateway v1)" },
        ],
      },
      {
        key: "nutanix-v4-api-version",
        label: "v4 path revision",
        type: "string",
        placeholder: "v4.2",
        help: "v4.2 default; e.g. v4.1, v4.0.a1.",
      },
      {
        key: "pc-alerts-cache-ttl",
        label: "Prism Central alert cache TTL",
        type: "duration",
        placeholder: "5m",
        help: "Cache duration for Prism Central alerts. Set 0 to disable caching.",
      },
    ],
  },
  {
    id: "perf",
    title: "Performance & Reliability",
    description: "Tune concurrency, request timing, and retry behavior.",
    icon: "perf",
    fields: [
      { key: "max-parallel", label: "Max parallel clusters", type: "int", min: 1, max: 64, help: "Number of clusters processed concurrently." },
      { key: "timeout", label: "Per-cluster timeout", type: "duration", placeholder: "15m" },
      { key: "request-timeout", label: "HTTP request timeout", type: "duration", placeholder: "30s" },
      { key: "poll-interval", label: "Poll interval", type: "duration", placeholder: "15s", help: "How often to poll for task status." },
      { key: "poll-jitter", label: "Poll jitter", type: "duration", placeholder: "2s", help: "Random jitter to avoid herd behavior." },
      {
        key: "adaptive-parallelism",
        label: "Adaptive parallelism",
        type: "boolean",
        help: "Automatically reduce concurrency when receiving HTTP 429 responses.",
      },
      { key: "retry-max-attempts", label: "Retry max attempts", type: "int", min: 0, max: 20 },
      { key: "retry-base-delay", label: "Retry base delay", type: "duration", placeholder: "400ms" },
      { key: "retry-max-delay", label: "Retry max delay", type: "duration", placeholder: "8s" },
      { key: "retry-circuit-breaker", label: "Circuit breaker", type: "int", min: 0, max: 50, help: "Fail fast after N consecutive retryable failures (0 to disable)." },
      { key: "max-idle-conns", label: "Max idle connections", type: "int", min: 0, help: "HTTP transport idle connection pool size (0 = default)." },
      { key: "max-idle-conns-per-host", label: "Max idle conns / host", type: "int", min: 0 },
      { key: "max-conns-per-host", label: "Max conns / host", type: "int", min: 0, help: "0 = unlimited." },
      { key: "idle-conn-timeout", label: "Idle conn timeout", type: "duration", placeholder: "90s" },
    ],
  },
  {
    id: "outputs",
    title: "Outputs & Retention",
    description: "Where artifacts go and how long they're kept.",
    icon: "outputs",
    fields: [
      {
        key: "outputs",
        label: "Output formats",
        type: "list-csv",
        help: "One or more of: html, csv, json, markdown, sarif.",
        placeholder: "html, csv",
      },
      { key: "output-dir-logs", label: "Logs directory", type: "string", placeholder: "nccfiles", help: "Directory for raw NCC summary text per cluster." },
      { key: "output-dir-filtered", label: "Output directory", type: "string", placeholder: "outputfiles", help: "Directory for generated HTML/CSV/JSON reports." },
      { key: "single-report", label: "Single combined HTML report", type: "boolean", help: "Also write ncc-report-single.html." },
      { key: "run-history", label: "Save run history", type: "boolean", help: "Snapshot each run under run-history-dir for trend analysis." },
      { key: "run-history-dir", label: "Run history directory", type: "string", placeholder: "outputfiles/runs" },
      { key: "retain-last", label: "Keep last N snapshots", type: "int", min: 0, help: "0 means unlimited." },
      { key: "retain-days", label: "Keep snapshots ≤ N days", type: "int", min: 0, help: "0 means unlimited." },
      { key: "artifact-retain-days", label: "Artifact retention (days)", type: "int", min: 0, help: "0 means unlimited." },
      { key: "artifact-retain-max-files", label: "Artifact max files", type: "int", min: 0, help: "0 means unlimited." },
      { key: "prom-enabled", label: "Prometheus textfiles", type: "boolean", help: "Write per-run Prometheus .prom textfile-collector metrics." },
      { key: "prom-dir", label: "Prometheus directory", type: "string", placeholder: "outputfiles/prom", help: "Directory for .prom textfile-collector files." },
    ],
  },
  {
    id: "policy",
    title: "Policy & Filtering",
    description: "Gates, regression alerts, flaky-check tuning, and exclusions.",
    icon: "policy",
    fields: [
      {
        key: "policy-gates",
        label: "Policy gates (raw)",
        type: "string",
        help: "CSV of metric>value rules. Use the Policy Gates Builder above for a guided UI.",
        placeholder: "new-fails>0, fail-rate>2",
      },
      { key: "notify-on-regression", label: "Notify only on regression", type: "boolean", help: "Send notifications only when FAIL count increases." },
      { key: "severity-filter", label: "Severity filter", type: "list-csv", help: "Only include checks with these severities (e.g. FAIL, WARN, ERR, INFO). Empty = all." },
      { key: "quiet-hours", label: "Quiet hours (local)", type: "string", placeholder: "22:00-06:00" },
      { key: "maintenance-windows", label: "Maintenance windows", type: "string", help: "RFC3339 windows: start/end[,start/end…]." },
      { key: "flaky-lookback-runs", label: "Flaky lookback runs", type: "int", min: 1, max: 50 },
      { key: "flaky-min-transitions", label: "Flaky min transitions", type: "int", min: 1, max: 20 },
      { key: "exclude-alert-titles", label: "Exclude alert titles", type: "list-csv", help: "Comma-separated NCC alert titles to drop from reports." },
      { key: "exclude-alert-titles-file", label: "Exclude titles file", type: "string", placeholder: "/path/to/exclusions.txt" },
      {
        key: "exclude-alert-match-mode",
        label: "Exclude match mode",
        type: "select",
        options: [
          { value: "exact", label: "exact" },
          { value: "contains", label: "contains" },
          { value: "regex", label: "regex" },
        ],
      },
    ],
  },
  {
    id: "logs",
    title: "Logging",
    description: "Runner log file location and verbosity.",
    icon: "logs",
    fields: [
      { key: "log-file", label: "Log file path", type: "string", placeholder: "logs/ncc-runner.log" },
      {
        key: "log-level",
        label: "Log level",
        type: "select",
        help: "Verbosity. Higher levels reduce noise.",
        options: [
          { value: "0", label: "0 — trace" },
          { value: "1", label: "1 — debug" },
          { value: "2", label: "2 — info (default)" },
          { value: "3", label: "3 — warn" },
          { value: "4", label: "4 — error" },
        ],
      },
      { key: "log-http", label: "Log HTTP request/response dumps", type: "boolean", help: "Debugging only. Generates large logs and may include sensitive data." },
    ],
  },
  {
    id: "notify",
    title: "Notifications",
    description: "Email, webhook, and Slack delivery channels.",
    icon: "notify",
    fields: [
      { key: "email-enabled", label: "Email enabled", type: "boolean" },
      { key: "email-attach-html", label: "Attach HTML report", type: "boolean" },
      { key: "notify-digest", label: "Send digest only", type: "boolean", help: "Summarize multiple events into a single email." },
      { key: "smtp-server", label: "SMTP server", type: "string", placeholder: "smtp.example.com" },
      { key: "smtp-port", label: "SMTP port", type: "int", min: 1, max: 65535, placeholder: "587" },
      { key: "smtp-user", label: "SMTP username", type: "string" },
      { key: "smtp-password", label: "SMTP password", type: "password" },
      { key: "email-use-tls", label: "Use STARTTLS", type: "boolean" },
      { key: "smtp-insecure-skip-verify", label: "Skip SMTP TLS verify", type: "boolean", help: "Skip STARTTLS cert verification (independent of cluster insecure-skip-verify)." },
      { key: "email-from", label: "From address", type: "string" },
      { key: "email-to", label: "To addresses", type: "list-csv", help: "Comma-separated list of recipients." },
      { key: "email-subject-template", label: "Email subject template", type: "textarea", help: "Go text/template; empty = built-in. Fields: .Cluster .FailCount .WarnCount .ErrCount .InfoCount .TotalChecks .Overview .StartedAt .FinishedAt .OutputFiles." },
      { key: "email-body-template", label: "Email body template", type: "textarea", help: "Go text/template for the email body; empty = built-in default." },

      { key: "webhook-enabled", label: "Webhook enabled", type: "boolean" },
      { key: "webhook-include-html", label: "Include HTML body in webhook", type: "boolean" },
      { key: "webhook-url", label: "Webhook URL", type: "string", placeholder: "https://hooks.example.com/ncc" },
      { key: "webhook-template", label: "Webhook body template", type: "textarea", help: "Go text/template; output is sent verbatim and must be valid JSON. Empty = default JSON encoding." },
      { key: "webhook-secret", label: "Webhook HMAC secret", type: "password", help: "When set, the body is signed: header X-NCC-Signature: sha256=<hmac>." },

      { key: "slack-enabled", label: "Slack enabled", type: "boolean" },
      { key: "slack-webhook-url", label: "Slack webhook URL", type: "password", help: "Treated as sensitive; masked in the UI." },
      { key: "slack-channel", label: "Slack channel", type: "string", placeholder: "#sre-alerts" },
      { key: "notification-deadletter-dir", label: "Dead-letter directory", type: "string", help: "Persist notification payloads that fail to deliver after retries.", placeholder: "/var/lib/ncc/deadletter" },
    ],
  },
  {
    id: "secrets",
    title: "Secrets",
    description: "Resolve passwords and tokens from secure providers instead of inline plaintext.",
    icon: "secrets",
    fields: [
      {
        key: "secrets-provider",
        label: "Secrets provider",
        type: "select",
        help: "Where the runner should fetch sensitive values from.",
        options: [
          { value: "", label: "(disabled — inline values)" },
          { value: "env", label: "Environment variables" },
          { value: "file", label: "YAML/JSON file" },
        ],
      },
      {
        key: "secrets-file",
        label: "Secrets file",
        type: "string",
        help: "Required when provider is 'file'. YAML or JSON map of secrets.",
        placeholder: "/etc/ncc/secrets.yaml",
      },
    ],
  },
];

export function fieldByKey(key: string): { section: SectionDef; field: FieldDef } | null {
  for (const s of SECTIONS) {
    const f = s.fields.find((x) => x.key === key);
    if (f) return { section: s, field: f };
  }
  return null;
}

/** Keys we recognise. Anything else falls into "Other settings". */
export const KNOWN_KEYS: string[] = SECTIONS.flatMap((s) => s.fields.map((f) => f.key));

/** Helper used only by the icon mapping in ConfigSection.tsx. */
export function _unused_marker(): ReactNode {
  return null;
}
