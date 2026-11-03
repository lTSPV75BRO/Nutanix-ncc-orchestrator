// Every runner setting the config file accepts (see validateConfigFileRawTypes
// and normalizeCanonicalConfig in cmd/ncc-orchestrator/goNCC.go). Each field
// knows its flat key and its path in the nested `schema-version: 1` layout, so
// the form reads and writes whichever shape the open file uses.

export type FieldType =
  | "string"
  | "password"
  | "boolean"
  | "int"
  | "select"
  | "duration"
  /** Free-form list: YAML list in nested files, comma-separated in flat ones. */
  | "list"
  /** List restricted to `options`. */
  | "multiselect"
  /** String-to-string map (webhook headers). */
  | "keyvalue"
  | "textarea";

export type FieldDef = {
  key: string;
  label: string;
  type: FieldType;
  /** Where the value lives in a nested (`schema-version: 1`) file. */
  path: string[];
  /** Older nested spellings the runner also reads; used if the file has one. */
  altPaths?: string[][];
  /** Sub-heading inside the section. */
  group: string;
  help?: string;
  placeholder?: string;
  /** The runner's built-in value, shown when the file does not set the key. */
  defaultValue?: string;
  options?: { value: string; label: string }[];
  min?: number;
  max?: number;
  required?: boolean;
  /** Full row instead of half. */
  wide?: boolean;
  autoComplete?: string;
};

export type SectionIcon = "cluster" | "connection" | "perf" | "outputs" | "policy" | "notify" | "logs" | "secrets";

export type SectionDef = {
  id: string;
  title: string;
  description: string;
  icon: SectionIcon;
  fields: FieldDef[];
};

const R = (...p: string[]) => ["runner", ...p];

export const SECTIONS: SectionDef[] = [
  {
    id: "targets",
    title: "Clusters",
    description: "Which clusters each run checks: a list you keep, or everything a Prism Central manages.",
    icon: "cluster",
    fields: [
      {
        key: "cluster-source-mode",
        label: "Find clusters from",
        type: "select",
        path: R("targets", "mode"),
        group: "Source",
        defaultValue: "clusters",
        options: [
          { value: "clusters", label: "A list of clusters" },
          { value: "pc", label: "Prism Central discovery" },
        ],
      },
      {
        key: "clusters",
        label: "Clusters",
        type: "list",
        path: R("targets", "clusters"),
        group: "Cluster list",
        wide: true,
        help: "Prism Element IP addresses or host names. Type one and press Enter.",
        placeholder: "10.0.0.10, cluster-a.example.com",
      },
      {
        key: "clusters-file",
        label: "Clusters file",
        type: "string",
        path: R("targets", "clusters-file"),
        group: "Cluster list",
        wide: true,
        help: "One `cluster[,user[,password]]` per line. Added to the list above.",
        placeholder: "clusters.txt",
      },
      {
        key: "pcs",
        label: "Prism Centrals",
        type: "list",
        path: R("targets", "pcs"),
        group: "Prism Central discovery",
        wide: true,
        help: "Prism Central IP addresses or host names. Used when clusters come from Prism Central.",
        placeholder: "10.0.0.20, pc.example.com",
      },
      {
        key: "pcs-file",
        label: "Prism Central file",
        type: "string",
        path: R("targets", "pcs-file"),
        group: "Prism Central discovery",
        help: "One Prism Central per line.",
        placeholder: "pcs.txt",
      },
      {
        key: "prism-central-url",
        label: "Prism Central URL",
        type: "string",
        path: R("targets", "prism-central-url"),
        group: "Prism Central discovery",
        help: "Used when the list above is empty.",
        placeholder: "https://pc.example.com:9440",
      },
      {
        key: "discover-api-version",
        label: "Discovery API",
        type: "select",
        path: R("targets", "discover-api-version"),
        group: "Prism Central discovery",
        defaultValue: "v4",
        options: [
          { value: "v4", label: "v4" },
          { value: "v3", label: "v3" },
        ],
      },
    ],
  },
  {
    id: "connection",
    title: "Sign-in & connection",
    description: "The account runs sign in with, certificate checks, and which Nutanix APIs to call.",
    icon: "connection",
    fields: [
      {
        key: "username",
        label: "Username",
        type: "string",
        path: R("credentials", "username"),
        group: "Credentials",
        defaultValue: "admin",
        required: true,
        autoComplete: "username",
      },
      {
        key: "password",
        label: "Password",
        type: "password",
        path: R("credentials", "password"),
        group: "Credentials",
        help: "Prefer a secret reference such as secret://NCC_PASSWORD over the password itself.",
      },
      {
        key: "insecure-skip-verify",
        label: "Skip certificate checks",
        type: "boolean",
        path: R("connection", "insecure-skip-verify"),
        group: "Certificates",
        defaultValue: "false",
        help: "Accept any certificate. Only for lab clusters with self-signed certificates.",
      },
      {
        key: "ca-bundle",
        label: "Trusted CA file",
        type: "string",
        path: R("connection", "ca-bundle"),
        group: "Certificates",
        help: "PEM file of extra CAs to trust. Safer than skipping certificate checks.",
        placeholder: "/etc/ncc/prism-ca.pem",
      },
      {
        key: "pin-sha256",
        label: "Pinned certificates",
        type: "list",
        path: R("connection", "pin-sha256"),
        group: "Certificates",
        wide: true,
        help: "SHA-256 fingerprints of the only server certificates to accept.",
      },
      {
        key: "ncc-api-version",
        label: "NCC API",
        type: "select",
        path: R("connection", "ncc-api-version"),
        group: "Nutanix APIs",
        defaultValue: "v4",
        options: [
          { value: "v4", label: "v4" },
          { value: "Legacy", label: "Legacy (Prism Gateway v1)" },
        ],
      },
      {
        key: "nutanix-v4-api-version",
        label: "v4 API revision",
        type: "string",
        path: R("connection", "nutanix-v4-api-version"),
        group: "Nutanix APIs",
        defaultValue: "v4.2",
        help: "For example v4.1 or v4.0.a1.",
      },
      {
        key: "pc-alerts-cache-ttl",
        label: "Prism Central alert cache",
        type: "duration",
        path: ["api", "cache", "pc-alerts-cache-ttl"],
        group: "Nutanix APIs",
        defaultValue: "5m",
        help: "How long Prism Central alerts are reused. 0 turns caching off.",
      },
    ],
  },
  {
    id: "execution",
    title: "Run behavior",
    description: "Timeouts, parallelism, retries, and what a run does when something goes wrong.",
    icon: "perf",
    fields: [
      { key: "timeout", label: "Per-cluster timeout", type: "duration", path: R("execution", "timeout"), group: "Timing", defaultValue: "15m" },
      { key: "request-timeout", label: "Request timeout", type: "duration", path: R("execution", "request-timeout"), group: "Timing", defaultValue: "20s", help: "For one HTTP request." },
      { key: "poll-interval", label: "Poll interval", type: "duration", path: R("execution", "poll-interval"), group: "Timing", defaultValue: "15s", help: "How often to check whether NCC has finished." },
      { key: "poll-jitter", label: "Poll variation", type: "duration", path: R("execution", "poll-jitter"), group: "Timing", defaultValue: "2s", help: "Spreads checks so clusters are not all polled at once." },
      {
        key: "max-parallel",
        label: "Clusters at once",
        type: "int",
        path: R("execution", "max-parallel"),
        group: "Parallelism",
        defaultValue: "0",
        min: 0,
        max: 1000,
        help: "0 = automatic: one per cluster, up to 500.",
      },
      {
        key: "adaptive-parallelism",
        label: "Back off when clusters are busy",
        type: "boolean",
        path: R("execution", "adaptive-parallelism"),
        group: "Parallelism",
        defaultValue: "true",
        help: "Send fewer requests at once when a cluster asks for that.",
      },
      {
        key: "retry-failed-clusters",
        label: "Retry failed clusters",
        type: "int",
        path: R("execution", "retry-failed-clusters"),
        group: "Recovery",
        defaultValue: "1",
        min: 0,
        max: 5,
        help: "Extra passes at the end of a run for clusters that timed out or hit a network error.",
      },
      {
        key: "resume",
        label: "Resume interrupted runs",
        type: "boolean",
        path: R("execution", "resume"),
        group: "Recovery",
        defaultValue: "false",
        help: "Skip clusters an interrupted run already finished (same list, within 24 hours).",
      },
      { key: "retry-max-attempts", label: "Request retries", type: "int", path: R("retry", "max-attempts"), group: "Recovery", defaultValue: "6", min: 0, max: 20 },
      { key: "retry-base-delay", label: "First retry after", type: "duration", path: R("retry", "base-delay"), group: "Recovery", defaultValue: "400ms" },
      { key: "retry-max-delay", label: "Longest retry wait", type: "duration", path: R("retry", "max-delay"), group: "Recovery", defaultValue: "8s" },
      {
        key: "retry-circuit-breaker",
        label: "Stop after failures in a row",
        type: "int",
        path: R("retry", "circuit-breaker"),
        group: "Recovery",
        defaultValue: "3",
        min: 0,
        max: 50,
        help: "0 never stops retrying.",
      },
      {
        key: "auto",
        label: "Fix problems automatically",
        type: "boolean",
        path: R("automation", "enabled"),
        group: "Self-healing",
        defaultValue: "false",
        help: "When the connection check fails, apply the fixes the level below allows, then try again.",
      },
      {
        key: "automation-level",
        label: "What it may change",
        type: "select",
        path: R("automation", "level"),
        group: "Self-healing",
        defaultValue: "safe-fix",
        help: "Full auto also raises low timeouts and retries and caps clusters at once at 12 for that run.",
        options: [
          { value: "advisory", label: "Advisory: suggest fixes only" },
          { value: "safe-fix", label: "Safe fixes: create missing folders" },
          { value: "full-auto", label: "Full auto: safe fixes and run tuning" },
        ],
      },
      { key: "dry-run", label: "Dry run", type: "boolean", path: R("execution", "dry-run"), group: "Run mode", defaultValue: "false", help: "Check the config and print the plan without running NCC." },
      { key: "skip-preflight-check", label: "Skip connection check", type: "boolean", path: R("execution", "skip-preflight-check"), group: "Run mode", defaultValue: "false", help: "Start without checking that clusters are reachable." },
      { key: "replay", label: "Replay saved reports", type: "boolean", path: R("execution", "replay"), group: "Run mode", defaultValue: "false", help: "Read saved results instead of contacting clusters." },
      { key: "gen-test-agg", label: "Sample rows", type: "int", path: R("outputs", "gen-test-agg"), group: "Run mode", defaultValue: "0", min: 0, help: "Leave at 0. Any other value adds made-up rows and does not scan clusters." },
      { key: "max-idle-conns", label: "Idle connections", type: "int", path: R("http-pool", "max-idle-conns"), group: "HTTP connections", defaultValue: "0", min: 0, help: "0 uses the built-in limit." },
      { key: "max-idle-conns-per-host", label: "Idle connections per cluster", type: "int", path: R("http-pool", "max-idle-conns-per-host"), group: "HTTP connections", defaultValue: "0", min: 0 },
      { key: "max-conns-per-host", label: "Connections per cluster", type: "int", path: R("http-pool", "max-conns-per-host"), group: "HTTP connections", defaultValue: "0", min: 0, help: "0 means no limit." },
      { key: "idle-conn-timeout", label: "Close idle connections after", type: "duration", path: R("http-pool", "idle-conn-timeout"), group: "HTTP connections", defaultValue: "90s" },
    ],
  },
  {
    id: "outputs",
    title: "Reports & history",
    description: "What each run saves, where, and for how long.",
    icon: "outputs",
    fields: [
      {
        key: "outputs",
        label: "Report formats",
        type: "multiselect",
        path: R("outputs", "formats"),
        group: "Reports",
        defaultValue: "html,csv",
        wide: true,
        options: [
          { value: "html", label: "HTML" },
          { value: "csv", label: "CSV" },
          { value: "json", label: "JSON" },
          { value: "markdown", label: "Markdown" },
          { value: "sarif", label: "SARIF" },
        ],
      },
      { key: "single-report", label: "Also save one combined HTML report", type: "boolean", path: R("outputs", "single-report"), group: "Reports", defaultValue: "false" },
      { key: "output-dir-filtered", label: "Reports folder", type: "string", path: ["storage", "output-dir"], group: "Reports", defaultValue: "outputfiles" },
      { key: "output-dir-logs", label: "NCC logs folder", type: "string", path: ["storage", "logs-dir"], group: "Reports", defaultValue: "nccfiles", help: "Each cluster's raw NCC output." },
      { key: "run-history", label: "Keep run history", type: "boolean", path: R("history", "enabled"), group: "Run history", defaultValue: "false", help: "Needed for Insights trends, regressions, and flaky checks." },
      { key: "run-history-dir", label: "Run history folder", type: "string", path: ["storage", "run-history-dir"], group: "Run history", placeholder: "outputfiles/runs" },
      { key: "retain-last", label: "Runs to keep", type: "int", path: R("history", "retain-last"), group: "Run history", defaultValue: "0", min: 0, help: "0 keeps every run." },
      { key: "retain-days", label: "Keep runs for (days)", type: "int", path: R("history", "retain-days"), group: "Run history", defaultValue: "0", min: 0, help: "0 means no age limit." },
      { key: "artifact-retain-days", label: "Keep report files for (days)", type: "int", path: R("history", "artifact-retain-days"), group: "Run history", defaultValue: "0", min: 0, help: "0 means no age limit." },
      { key: "artifact-retain-max-files", label: "Most report files to keep", type: "int", path: R("history", "artifact-retain-max-files"), group: "Run history", defaultValue: "0", min: 0, help: "0 means no limit." },
      { key: "prom-enabled", label: "Write Prometheus metrics", type: "boolean", path: ["prometheus", "enabled"], group: "Prometheus", defaultValue: "false", help: "A metrics file after each run, for the node exporter's textfile collector." },
      { key: "prom-dir", label: "Metrics folder", type: "string", path: ["storage", "prom-dir"], group: "Prometheus", defaultValue: "promfiles" },
    ],
  },
  {
    id: "filtering",
    title: "Filtering & policy",
    description: "Which findings reports include, and the conditions that fail a run.",
    icon: "policy",
    fields: [
      {
        key: "severity-filter",
        label: "Severities to report",
        type: "multiselect",
        path: R("filtering", "severity"),
        group: "What to report",
        wide: true,
        help: "Leave empty to report every severity.",
        placeholder: "All severities",
        options: [
          { value: "FAIL", label: "FAIL" },
          { value: "WARN", label: "WARN" },
          { value: "ERR", label: "ERR" },
          { value: "INFO", label: "INFO" },
        ],
      },
      {
        key: "exclude-alert-titles",
        label: "Leave out alerts",
        type: "list",
        path: R("filtering", "exclude-alert-titles"),
        group: "What to report",
        wide: true,
        help: "NCC alert titles to drop from reports.",
      },
      { key: "exclude-alert-titles-file", label: "Leave-out list file", type: "string", path: R("filtering", "exclude-alert-titles-file"), group: "What to report", help: "One title per line.", placeholder: "exclusions.txt" },
      {
        key: "exclude-alert-match-mode",
        label: "Match titles by",
        type: "select",
        path: R("filtering", "exclude-alert-match-mode"),
        group: "What to report",
        defaultValue: "exact",
        options: [
          { value: "exact", label: "Exact title" },
          { value: "contains", label: "Contains the text" },
          { value: "regex", label: "Regular expression" },
        ],
      },
      {
        key: "policy-gates",
        label: "Policy gates",
        type: "string",
        path: R("policy", "gates"),
        altPaths: [R("filtering", "policy-gates")],
        group: "Policy",
        wide: true,
        help: "Rules such as new-fails>0 that fail the run. Build them with the gate builder below.",
        placeholder: "new-fails>0, fail-rate>2",
      },
      { key: "notify-on-regression", label: "Notify only on regressions", type: "boolean", path: R("policy", "notify-on-regression"), group: "Policy", defaultValue: "false", help: "Send notifications only when the FAIL count goes up." },
      { key: "flaky-lookback-runs", label: "Runs to look back", type: "int", path: R("policy", "flaky-lookback-runs"), altPaths: [R("filtering", "flaky-lookback-runs")], group: "Flaky checks", defaultValue: "6", min: 1, max: 50 },
      { key: "flaky-min-transitions", label: "Changes that make a check flaky", type: "int", path: R("policy", "flaky-min-transitions"), altPaths: [R("filtering", "flaky-min-transitions")], group: "Flaky checks", defaultValue: "2", min: 1, max: 20 },
    ],
  },
  {
    id: "notify",
    title: "Notifications",
    description: "What the runner sends after each run. The Notifications page edits the same settings and can send tests.",
    icon: "notify",
    fields: [
      { key: "notify-digest", label: "One message per run", type: "boolean", path: ["notifications", "email", "digest"], group: "When to send", defaultValue: "false", help: "Instead of one per cluster." },
      { key: "quiet-hours", label: "Quiet hours", type: "string", path: ["notifications", "quiet-hours"], group: "When to send", help: "Local time, HH:MM-HH:MM.", placeholder: "22:00-06:00" },
      { key: "maintenance-windows", label: "Maintenance windows", type: "list", path: ["notifications", "maintenance-windows"], group: "When to send", wide: true, help: "start/end in RFC 3339, for example 2026-11-08T01:00:00Z/2026-11-08T04:00:00Z." },
      { key: "notification-deadletter-dir", label: "Undelivered messages folder", type: "string", path: ["storage", "notification-deadletter-dir"], group: "When to send", wide: true, help: "Keeps messages that still failed after retries.", placeholder: "/var/lib/ncc/deadletter" },

      { key: "email-enabled", label: "Send email", type: "boolean", path: ["notifications", "email", "enabled"], altPaths: [["notifications", "email-enabled"]], group: "Email", defaultValue: "false" },
      { key: "email-attach-html", label: "Attach the HTML report", type: "boolean", path: ["notifications", "email", "attach-html"], group: "Email", defaultValue: "false" },
      { key: "smtp-server", label: "Mail server", type: "string", path: ["notifications", "email", "smtp-server"], altPaths: [["notifications", "smtp-server"]], group: "Email", placeholder: "smtp.example.com" },
      { key: "smtp-port", label: "Port", type: "int", path: ["notifications", "email", "smtp-port"], altPaths: [["notifications", "smtp-port"]], group: "Email", defaultValue: "587", min: 1, max: 65535, help: "587 for STARTTLS, 465 for TLS." },
      { key: "smtp-user", label: "Mail username", type: "string", path: ["notifications", "email", "smtp-user"], group: "Email" },
      { key: "smtp-password", label: "Mail password", type: "password", path: ["notifications", "email", "smtp-password"], group: "Email" },
      { key: "email-use-tls", label: "Use STARTTLS", type: "boolean", path: ["notifications", "email", "use-tls"], group: "Email", defaultValue: "true" },
      { key: "smtp-insecure-skip-verify", label: "Skip mail certificate checks", type: "boolean", path: ["notifications", "email", "insecure-skip-verify"], group: "Email", defaultValue: "false" },
      { key: "email-from", label: "From", type: "string", path: ["notifications", "email", "from"], group: "Email", placeholder: "ncc@example.com" },
      { key: "email-to", label: "To", type: "list", path: ["notifications", "email", "to"], group: "Email", placeholder: "ops@example.com" },
      {
        key: "email-subject-template",
        label: "Subject template",
        type: "textarea",
        path: ["notifications", "email", "subject-template"],
        group: "Email",
        wide: true,
        help: "Go template. Empty uses the built-in subject. Fields: .Cluster .FailCount .WarnCount .ErrCount .InfoCount .TotalChecks .Overview .StartedAt .FinishedAt .OutputFiles.",
      },
      { key: "email-body-template", label: "Body template", type: "textarea", path: ["notifications", "email", "body-template"], group: "Email", wide: true, help: "Go template. Empty uses the built-in body." },

      { key: "webhook-enabled", label: "Send webhooks", type: "boolean", path: ["notifications", "webhook", "enabled"], group: "Webhook", defaultValue: "false" },
      { key: "webhook-include-html", label: "Include the HTML report", type: "boolean", path: ["notifications", "webhook", "include-html"], group: "Webhook", defaultValue: "false" },
      { key: "webhook-url", label: "Webhook URL", type: "string", path: ["notifications", "webhook", "url"], altPaths: [["notifications", "webhook-url"]], group: "Webhook", wide: true, placeholder: "https://hooks.example.com/ncc" },
      { key: "webhook-headers", label: "Extra headers", type: "keyvalue", path: ["notifications", "webhook", "headers"], group: "Webhook", wide: true, help: "Sent with every webhook call, for example an auth token." },
      { key: "webhook-secret", label: "Signing secret", type: "password", path: ["notifications", "webhook", "secret"], group: "Webhook", help: "Signs the body in an X-NCC-Signature header (HMAC SHA-256)." },
      { key: "webhook-template", label: "Body template", type: "textarea", path: ["notifications", "webhook", "template"], group: "Webhook", wide: true, help: "Go template that must produce JSON. Empty sends the standard JSON." },

      { key: "slack-enabled", label: "Send to Slack", type: "boolean", path: ["notifications", "slack", "enabled"], group: "Slack", defaultValue: "false" },
      { key: "slack-webhook-url", label: "Slack webhook URL", type: "password", path: ["notifications", "slack", "webhook-url"], group: "Slack" },
      { key: "slack-channel", label: "Channel", type: "string", path: ["notifications", "slack", "channel"], group: "Slack", placeholder: "#sre-alerts" },
    ],
  },
  {
    id: "logging",
    title: "Logging",
    description: "The runner's own log file and how much it writes.",
    icon: "logs",
    fields: [
      { key: "log-file", label: "Log file", type: "string", path: ["logging", "file"], group: "Runner log", defaultValue: "logs/ncc-runner.log", wide: true },
      {
        key: "log-level",
        label: "Detail",
        type: "select",
        path: ["logging", "level"],
        group: "Runner log",
        defaultValue: "2",
        options: [
          { value: "0", label: "Trace" },
          { value: "1", label: "Debug" },
          { value: "2", label: "Info" },
          { value: "3", label: "Warnings" },
          { value: "4", label: "Errors" },
        ],
      },
      { key: "log-http", label: "Log HTTP requests", type: "boolean", path: ["logging", "http"], group: "Runner log", defaultValue: "false", help: "For debugging only: large, and may contain sensitive data." },
    ],
  },
  {
    id: "secrets",
    title: "Secrets",
    description: "Where secret:// references in this file are looked up.",
    icon: "secrets",
    fields: [
      {
        key: "secrets-provider",
        label: "Look up secrets in",
        type: "select",
        path: ["secrets", "provider"],
        group: "Provider",
        options: [
          { value: "env", label: "Environment variables" },
          { value: "file", label: "A YAML or JSON file" },
        ],
        placeholder: "Not used",
      },
      { key: "secrets-file", label: "Secrets file", type: "string", path: ["secrets", "file"], group: "Provider", help: "Needed when secrets come from a file.", placeholder: "/etc/ncc/secrets.yaml" },
    ],
  },
];

export const ALL_FIELDS: FieldDef[] = SECTIONS.flatMap((s) => s.fields);

/** Flat keys the form edits. Anything else in the file is listed under "Other keys". */
export const KNOWN_KEYS: string[] = ALL_FIELDS.map((f) => f.key);

/** The section's fields in display order, grouped under their sub-headings. */
export function groupFields(fields: FieldDef[]): { group: string; fields: FieldDef[] }[] {
  const out: { group: string; fields: FieldDef[] }[] = [];
  for (const f of fields) {
    const last = out[out.length - 1];
    if (last && last.group === f.group) last.fields.push(f);
    else out.push({ group: f.group, fields: [f] });
  }
  return out;
}

const DURATION = /^(0|([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+)$/;

/** Mirrors Go's time.ParseDuration closely enough for form feedback. */
export function isValidDuration(v: string): boolean {
  return DURATION.test(v.trim());
}

/** A field's problem with the given display value, or null. */
export function fieldError(field: FieldDef, value: string): string | null {
  const v = value.trim();
  if (!v) return null;
  if (field.type === "duration" && !isValidDuration(v)) return "Use a duration such as 30s, 5m or 1h30m.";
  if (field.key === "quiet-hours" && !/^([01]\d|2[0-3]):[0-5]\d-([01]\d|2[0-3]):[0-5]\d$/.test(v)) return "Use HH:MM-HH:MM.";
  if ((field.key === "webhook-url" || field.key === "prism-central-url") && !/^https?:\/\/\S+$/i.test(v)) return "Use an http:// or https:// URL.";
  return null;
}

/** Fields whose label, key, path, group or help contain the query. */
export function matchesQuery(field: FieldDef, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return [field.label, field.key, field.path.join("."), field.group, field.help ?? ""].some((s) => s.toLowerCase().includes(q));
}
