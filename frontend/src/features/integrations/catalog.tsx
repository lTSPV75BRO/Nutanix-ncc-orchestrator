// What each third-party integration needs, written for someone who has not
// used the tool before. Every endpoint, header and metric named here exists in
// the API server; the third-party side (ServiceNow tables, Splunk inputs, …)
// is an example to adapt and says so. Snippets use {{API}} (the API server's
// base URL), {{HOST}} and {{SCHEME}}, filled in from the page's address field.
import type { ReactNode } from "react";
import {
  BellOutlined,
  CodeOutlined,
  FileSearchOutlined,
  LineChartOutlined,
  RobotOutlined,
  ToolOutlined,
} from "@ant-design/icons";

export type IntegrationCategory = "Ticketing" | "Monitoring" | "Logs & security" | "Chat & email" | "AI assistants" | "Scripts";

export const CATEGORIES: IntegrationCategory[] = ["Ticketing", "Monitoring", "Logs & security", "Chat & email", "AI assistants", "Scripts"];

export type Snippet = {
  /** What the snippet is, e.g. "prometheus.yml". */
  label: string;
  code: string;
  /** One line under the snippet. */
  note?: string;
};

/** In-app shortcut a step offers. */
export type StepAction = "access" | "token" | "notifications" | "test" | "developer";

export type Step = {
  id: string;
  title: string;
  /** Where this step happens, e.g. "In ServiceNow" or "In NCC Orchestrator". */
  where: string;
  body: ReactNode;
  snippets?: Snippet[];
  action?: StepAction;
  optional?: boolean;
};

export type Integration = {
  id: string;
  name: string;
  /** Other products the same guide covers, shown under the name. */
  alsoFor?: string;
  category: IntegrationCategory;
  icon: ReactNode;
  /** One sentence: what it does for you. */
  summary: string;
  youGet: string[];
  youNeed: string[];
  time: string;
  level: "Easy" | "Moderate" | "Advanced";
  /** Which way data moves, in words. */
  direction: string;
  /** Lowest NCC role the integration's account needs. */
  role: "viewer" | "operator" | "admin" | "none";
  /** Glossary keys (TERMS) used in the guide. */
  terms: string[];
  /** Repository doc for the full reference. */
  doc: string;
  steps: Step[];
};

export const REPO_DOCS = "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/blob/main/";

/** Plain-language meanings of the words the guides use. */
export const TERMS: Record<string, { term: string; meaning: string }> = {
  api: {
    term: "API address",
    meaning:
      "The web address other software uses to talk to NCC Orchestrator. It is the API server (port 8081 by default), not the address of this web page: the web page removes tokens from requests, so tools must call the API server directly.",
  },
  token: {
    term: "Access token",
    meaning:
      "A long password for software instead of people (it starts with ncc_pat_). It has the same permissions as the account that created it, can expire, and can be revoked at any time. It is shown once, so copy it straight into the other tool's password store.",
  },
  serviceAccount: {
    term: "Service account",
    meaning:
      "An NCC account used only by the integration, such as svc-servicenow. Giving each tool its own account means you can see what it did in the audit log and switch it off without affecting people.",
  },
  role: {
    term: "Role",
    meaning:
      "What an account may do. Viewer can only read. Operator can also acknowledge and resolve findings and start runs. Admin can change settings; never give admin to an integration.",
  },
  finding: {
    term: "Finding",
    meaning:
      "One problem NCC reported on one cluster, such as a failed check. Each has a stable id, a state (open, acknowledged, resolved) and a history of what happened to it.",
  },
  eventFeed: {
    term: "Event feed",
    meaning:
      "A list of everything that happened to findings (detected, acknowledged, resolved, …), oldest first. A tool asks for new events since the last one it saw, so it never misses or repeats one.",
  },
  cursor: {
    term: "Cursor",
    meaning:
      "A bookmark in the event feed. The API returns next_cursor with each page; the tool saves it and sends it back next time to get only what is new.",
  },
  polling: {
    term: "Polling",
    meaning: "The other tool asks NCC for changes on a timer, for example every 5 minutes. NCC does not need to reach the tool, only the tool needs to reach NCC.",
  },
  midServer: {
    term: "MID Server",
    meaning:
      "A small ServiceNow program installed inside your network. ServiceNow (in the cloud) uses it to reach systems it cannot reach directly, such as an NCC API server on a private network.",
  },
  scrape: {
    term: "Scrape",
    meaning: "Prometheus reads a page of numbers (/metrics) from NCC every minute or so and stores them, so you can graph them and alert on them.",
  },
  metric: {
    term: "Metric",
    meaning: "A named number that changes over time, such as how many FAIL findings are open (ncc_alerts_active).",
  },
  alertRule: {
    term: "Alert rule",
    meaning: "A condition Prometheus checks on the metrics, such as \"FAIL findings open for 30 minutes\". When it is true, Alertmanager notifies people.",
  },
  forwarder: {
    term: "Forwarder",
    meaning:
      "An agent from your log tool (Splunk Universal Forwarder, Elastic Filebeat) that watches a file on a server and sends new lines to the log tool.",
  },
  webhook: {
    term: "Webhook",
    meaning: "A web address that accepts messages. NCC sends a short JSON message to it after each run; the receiving tool decides what to do with it.",
  },
  hmac: {
    term: "Signature (HMAC)",
    meaning:
      "Proof that a webhook message came from NCC and was not changed. NCC signs the message body with a secret you both know and sends the result in the X-NCC-Signature header.",
  },
  mcp: {
    term: "MCP",
    meaning:
      "Model Context Protocol: a standard way for AI assistants (Claude Desktop, Cursor, VS Code and others) to use tools. The NCC MCP server lets the assistant read results and, if you allow it, start runs.",
  },
  openapi: {
    term: "OpenAPI",
    meaning: "A machine-readable description of every API endpoint. Many tools can import it to build requests for you.",
  },
};

const ACCOUNT_WHERE = "In NCC Orchestrator (admin)";

function accountStep(name: string, role: "viewer" | "operator", why: string): Step {
  return {
    id: "account",
    title: `Create a ${role} account for the integration`,
    where: ACCOUNT_WHERE,
    action: "access",
    body: (
      <>
        <p>
          Go to <b>Settings → Access → Users &amp; roles</b> and add a local account named <code>{name}</code> with the{" "}
          <b>{role}</b> role. {why}
        </p>
        <p className="integration-hint">
          To limit it to some clusters, add it to their cluster groups: the integration then sees and changes only those clusters' findings.
        </p>
      </>
    ),
  };
}

function tokenStep(name: string, storeIn: string): Step {
  return {
    id: "token",
    title: "Create an access token for that account",
    where: "In NCC Orchestrator",
    action: "token",
    body: (
      <>
        <p>
          Sign in as <code>{name}</code>, open the user menu (top right) and choose <b>Personal access tokens → Create token</b>, then <b>Generate</b>. Name it after
          where it will be used and pick an expiry (90 days is a good default; set a reminder to rotate it).
        </p>
        <p>
          The token is shown <b>once</b>. Copy it straight into {storeIn}. Never paste it into chat, tickets or code.
        </p>
      </>
    ),
  };
}

function testStep(where: string, path: string, expect: string): Step {
  return {
    id: "reach",
    title: "Check the tool's server can reach NCC",
    where,
    action: "test",
    body: (
      <>
        <p>
          Run this on the machine the tool runs on (or one in the same network), with the token in the <code>NCC_TOKEN</code> environment variable.
          {" "}{expect}
        </p>
        <ul className="integration-outcomes">
          <li>
            <b>200</b> with JSON: it works.
          </li>
          <li>
            <b>401</b>: the token is wrong, expired or revoked.
          </li>
          <li>
            <b>403/404</b>: the account's role or cluster groups do not allow it.
          </li>
          <li>
            <b>Timeout or "connection refused"</b>: a firewall, the wrong port, or you used the web page's address instead of the API server's.
          </li>
          <li>
            <b>Certificate error</b>: the tool does not trust the API server's HTTPS certificate; add your CA to it (do not turn verification off).
          </li>
        </ul>
      </>
    ),
    snippets: [
      {
        label: "Linux / macOS",
        code: `curl -sS -w "\\nHTTP %{http_code}\\n" -H "Authorization: Bearer $NCC_TOKEN" "{{API}}${path}"`,
      },
      {
        label: "Windows PowerShell",
        code: `Invoke-RestMethod -Headers @{ Authorization = "Bearer $env:NCC_TOKEN" } -Uri "{{API}}${path}"`,
      },
    ],
  };
}

const SERVICENOW_SCRIPT = `// Scheduled Script Execution, every 5 minutes. Example only: adapt the
// table and field names to your instance. Uses REST Message "NCC Orchestrator".
(function syncNccFindings() {
  var CURSOR_PROP = 'x_ncc.events_cursor';
  var cursor = gs.getProperty(CURSOR_PROP, '');
  var more = true;
  while (more) {
    var rm = new sn_ws.RESTMessageV2('NCC Orchestrator', 'events');
    rm.setQueryParameter('limit', '500');
    if (cursor) rm.setQueryParameter('cursor', cursor);
    rm.setHttpTimeout(30000);
    var resp = rm.execute();
    var code = resp.getStatusCode();
    if (code === 429 || code >= 500) { gs.warn('NCC sync: retry later, HTTP ' + code); return; }
    if (code !== 200) { gs.error('NCC sync failed: HTTP ' + code + ' ' + resp.getBody()); return; }
    var data = JSON.parse(resp.getBody()).data;
    data.items.forEach(applyEvent);
    cursor = data.next_cursor;
    gs.setProperty(CURSOR_PROP, cursor);   // save only after processing
    more = data.has_more;
  }

  function applyEvent(evt) {
    if (evt.actor === 'svc-servicenow') return;   // our own write-backs
    var inc = new GlideRecord('incident');
    inc.addQuery('correlation_id', evt.alert_id);
    inc.query();
    var exists = inc.next();
    if (evt.type === 'detected' || evt.type === 'recurred') {
      if (exists) return;
      var alert = fetchAlert(evt.alert_id);
      if (!alert || alert.severity === 'INFO') return;
      inc.initialize();
      inc.correlation_id = alert.id;
      inc.short_description = 'NCC: ' + alert.check + ' on ' + (alert.cluster_name || alert.cluster);
      inc.work_notes = 'Detected by NCC run ' + alert.first_seen_run;
      inc.insert();
    } else if (exists && evt.type === 'resolved') {
      inc.state = 6; inc.close_notes = 'Resolved in NCC by ' + evt.actor; inc.update();
    } else if (exists && (evt.type === 'reopened' || evt.type === 'redetected')) {
      inc.work_notes = 'NCC: ' + evt.type + (evt.reason ? ' - ' + evt.reason : ''); inc.update();
    }
  }

  function fetchAlert(id) {
    var rm = new sn_ws.RESTMessageV2('NCC Orchestrator', 'alert');
    rm.setStringParameterNoEscape('id', id);
    var r = rm.execute();
    return r.getStatusCode() === 200 ? JSON.parse(r.getBody()).data.alert : null;
  }
})();`;

const POLL_SCRIPT = `#!/usr/bin/env python3
"""Append new NCC finding events to a JSON-lines file for a log forwarder.

Run every 5 minutes. Needs NCC_TOKEN in the environment. Standard library only.
"""
import json, os, pathlib, urllib.request

API = "{{API}}"
OUT = pathlib.Path(os.environ.get("NCC_EVENTS_FILE", "/var/log/ncc/events.jsonl"))
CURSOR = OUT.with_suffix(".cursor")

cursor = CURSOR.read_text().strip() if CURSOR.exists() else ""
OUT.parent.mkdir(parents=True, exist_ok=True)
while True:
    url = f"{API}/api/v1/alerts/ncc/events?limit=500" + (f"&cursor={cursor}" if cursor else "")
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + os.environ["NCC_TOKEN"]})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)["data"]
    with OUT.open("a", encoding="utf-8") as f:
        for event in data["items"]:
            f.write(json.dumps(event) + "\\n")
    cursor = data["next_cursor"] or cursor
    CURSOR.write_text(cursor)          # saved only after the events are written
    if not data.get("has_more"):
        break
`;

const SCRAPE_CONFIG = `scrape_configs:
  - job_name: ncc-orchestrator
    scheme: {{SCHEME}}
    metrics_path: /metrics
    scrape_interval: 60s
    static_configs:
      - targets: ["{{HOST}}"]
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/ncc-token   # the token, alone in a file
    # tls_config:
    #   ca_file: /etc/prometheus/secrets/ncc-ca.pem         # if your CA is private`;

const ALERT_RULES = `groups:
  - name: ncc
    rules:
      - alert: NCCFailFindingsOpen
        expr: sum(ncc_alerts_active{state="open",detection="detected",severity="FAIL"}) > 0
        for: 30m
        labels: {severity: critical}
        annotations:
          summary: "{{ $value }} NCC FAIL findings are open and not acknowledged"
      - alert: NCCNoRecentRun
        expr: time() - ncc_alerts_last_ingested_run_timestamp_seconds > 2 * 86400
        labels: {severity: warning}
        annotations:
          summary: "No NCC run results for 2 days"
      - alert: NCCClusterNotReached
        expr: ncc_cluster_up == 0
        for: 1h
        labels: {severity: warning}
        annotations:
          summary: "The last NCC run could not check {{ $labels.cluster }}"`;

const WEBHOOK_VERIFY = `import hashlib, hmac

def is_from_ncc(body: bytes, header: str, secret: str) -> bool:
    """header is the X-NCC-Signature request header, e.g. "sha256=ab12..."."""
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header or "")`;

export const INTEGRATIONS: Integration[] = [
  {
    id: "servicenow",
    name: "ServiceNow",
    alsoFor: "Jira Service Management and other ticketing tools work the same way",
    category: "Ticketing",
    icon: <ToolOutlined />,
    summary: "Open an incident for each new NCC finding, and close it when the finding is resolved.",
    youGet: [
      "One incident per finding, never duplicates (the finding id is the correlation id).",
      "Incidents close when someone resolves the finding in NCC.",
      "Optional: resolving the incident in ServiceNow resolves the finding in NCC.",
    ],
    youNeed: ["A ServiceNow admin who can create REST Messages and scheduled scripts", "An NCC admin to create the service account", "A MID Server if the API server is on a private network"],
    time: "About 45 minutes",
    level: "Advanced",
    direction: "ServiceNow asks NCC for changes every 5 minutes (and can send updates back).",
    role: "operator",
    terms: ["api", "serviceAccount", "token", "role", "finding", "eventFeed", "cursor", "polling", "midServer"],
    doc: "docs/integrations/third-party-alert-integrations.md#10-servicenow",
    steps: [
      accountStep("svc-servicenow", "operator", "Operator lets ServiceNow resolve findings when incidents close; choose viewer if it should only read."),
      tokenStep("svc-servicenow", "a ServiceNow credential record (Connections & Credentials)"),
      testStep("On the MID Server (or any machine ServiceNow uses)", "/api/v1/alerts/ncc?limit=1", "You should see one open finding, or an empty list."),
      {
        id: "rest-message",
        title: "Create the REST Message",
        where: "In ServiceNow",
        body: (
          <>
            <p>
              In <b>System Web Services → Outbound → REST Message</b>, create <code>NCC Orchestrator</code> with the endpoint below. Set
              authentication to the credential from step 2 (header <code>Authorization: Bearer &lt;token&gt;</code>), and pick the MID Server
              if NCC is on a private network.
            </p>
            <p>Add two HTTP methods (GET): <code>events</code> and <code>alert</code>.</p>
          </>
        ),
        snippets: [
          { label: "Endpoint", code: "{{API}}/api/v1" },
          { label: "Method \"events\"", code: "{{API}}/api/v1/alerts/ncc/events" },
          { label: "Method \"alert\"", code: "{{API}}/api/v1/alerts/ncc/${id}", note: "${id} is a ServiceNow variable; keep it as written." },
        ],
      },
      {
        id: "script",
        title: "Schedule the sync script",
        where: "In ServiceNow",
        body: (
          <p>
            Create a <b>Scheduled Script Execution</b> that runs every 5 minutes with this script. It reads new events, creates an incident
            for each new FAIL, ERR or WARN finding, and closes it when the finding is resolved. It is an example: adjust the table and fields
            to how your team uses incidents.
          </p>
        ),
        snippets: [{ label: "Scheduled script (example)", code: SERVICENOW_SCRIPT }],
      },
      {
        id: "writeback",
        title: "Send incident changes back to NCC",
        where: "In ServiceNow",
        optional: true,
        body: (
          <>
            <p>
              Add an async <b>Business Rule</b> on <code>incident</code> that calls NCC when the state changes. The incident's correlation id
              is the finding id. Repeating a call is safe. A <b>409</b> means NCC already replaced that finding because the problem came back;
              the new id is in <code>next_id</code> from <code>GET /api/v1/alerts/ncc/&#123;id&#125;</code>.
            </p>
          </>
        ),
        snippets: [
          {
            label: "Calls to make",
            code: `POST {{API}}/api/v1/alerts/ncc/<correlation_id>/acknowledge   (incident In Progress / On Hold)
POST {{API}}/api/v1/alerts/ncc/<correlation_id>/resolve       (incident Resolved)
POST {{API}}/api/v1/alerts/ncc/<correlation_id>/reopen        (incident reopened)
Content-Type: application/json

{"note": "INC0012345 by jdoe"}`,
          },
        ],
      },
      {
        id: "verify",
        title: "Check it end to end",
        where: "In both",
        body: (
          <p>
            Run the scheduled script once (<b>Execute Now</b>). An incident should appear for each open FAIL/ERR/WARN finding. Resolve one
            finding on the Findings page and run the script again: its incident closes. Every change the integration makes shows in the NCC
            audit log under <code>svc-servicenow</code>.
          </p>
        ),
      },
    ],
  },
  {
    id: "prometheus",
    name: "Prometheus & Grafana",
    alsoFor: "Alertmanager, Grafana, and anything that reads Prometheus metrics",
    category: "Monitoring",
    icon: <LineChartOutlined />,
    summary: "Graph cluster health over time and get alerted when FAIL findings stay open or runs stop.",
    youGet: [
      "Numbers for every cluster: health score, checks by severity, whether the last run reached it.",
      "Counts of open, acknowledged and resolved findings.",
      "Alerts through Alertmanager (email, PagerDuty, Opsgenie, Slack…) when something needs attention.",
    ],
    youNeed: ["Access to the Prometheus configuration (or the Helm values on Kubernetes)", "An NCC admin to create a viewer account"],
    time: "About 20 minutes",
    level: "Moderate",
    direction: "Prometheus reads numbers from NCC every minute.",
    role: "viewer",
    terms: ["api", "token", "scrape", "metric", "alertRule"],
    doc: "docs/PROMETHEUS.md",
    steps: [
      accountStep("svc-prometheus", "viewer", "Prometheus only reads, so viewer is enough."),
      tokenStep("svc-prometheus", "a file on the Prometheus server (for example /etc/prometheus/secrets/ncc-token), readable only by Prometheus"),
      testStep("On the Prometheus server", "/metrics", "You should see lines starting with ncc_."),
      {
        id: "scrape",
        title: "Tell Prometheus to read NCC's metrics",
        where: "In Prometheus",
        body: (
          <p>
            Add this job to <code>prometheus.yml</code> and reload Prometheus. On <b>Status → Targets</b> the <code>ncc-orchestrator</code>{" "}
            target should turn <b>UP</b> within a minute.
          </p>
        ),
        snippets: [{ label: "prometheus.yml", code: SCRAPE_CONFIG }],
      },
      {
        id: "kubernetes",
        title: "On Kubernetes: use the chart's built-in rules",
        where: "In your Helm values",
        optional: true,
        body: (
          <p>
            If you run the Prometheus Operator, the chart can create the scrape target's alert rules for you. Also allow Prometheus through the
            API's NetworkPolicy (it only admits the UI by default), and scrape <code>http://ncc-v2-api.&lt;namespace&gt;.svc:8081</code>.
          </p>
        ),
        snippets: [
          {
            label: "values.yaml",
            code: `prometheusRule:
  enabled: true
  labels:
    release: kube-prometheus-stack   # the label your Prometheus selects rules by
  staleReportHours: 26`,
          },
        ],
      },
      {
        id: "rules",
        title: "Add alert rules",
        where: "In Prometheus",
        body: (
          <p>
            These rules alert when FAIL findings stay open, when no run results arrive for two days, and when a cluster could not be checked.
            Alertmanager then routes them to your usual on-call channel. Acknowledging the findings in NCC clears the first alert.
          </p>
        ),
        snippets: [{ label: "ncc-rules.yml", code: ALERT_RULES }],
      },
      {
        id: "grafana",
        title: "Build a Grafana dashboard",
        where: "In Grafana",
        optional: true,
        body: (
          <p>
            Add your Prometheus as a data source (<b>Connections → Data sources</b>), create a dashboard, and start with these queries.
          </p>
        ),
        snippets: [
          {
            label: "Panel queries",
            code: `ncc_cluster_health_score                                  # health 0-100 per cluster
sum by (cluster) (ncc_cluster_checks_total{severity="FAIL"}) # FAIL checks per cluster
sum by (state) (ncc_alerts_active)                          # findings by state
time() - ncc_last_run_timestamp_seconds                     # seconds since the last run`,
          },
        ],
      },
    ],
  },
  {
    id: "siem",
    name: "Splunk, Elastic & other log tools",
    alsoFor: "Any SIEM or log platform that can read a file",
    category: "Logs & security",
    icon: <FileSearchOutlined />,
    summary: "Send every finding change (detected, acknowledged, resolved…) to your log or security platform.",
    youGet: [
      "A permanent, searchable record of every finding and who handled it.",
      "Correlation with your other logs and security events.",
      "No duplicates or gaps: a bookmark (cursor) is saved after each batch.",
    ],
    youNeed: ["A Linux or Windows server with Python 3 and your tool's forwarder", "An NCC admin to create a viewer account"],
    time: "About 30 minutes",
    level: "Moderate",
    direction: "A small script asks NCC for new events every 5 minutes and writes them to a file your forwarder sends on.",
    role: "viewer",
    terms: ["api", "token", "finding", "eventFeed", "cursor", "polling", "forwarder"],
    doc: "docs/integrations/third-party-alert-integrations.md#52-get-apiv1alertsnccevents--lifecycle-event-feed-viewer",
    steps: [
      accountStep("svc-siem", "viewer", "The script only reads, so viewer is enough."),
      tokenStep("svc-siem", "the server's secret store, or an environment variable NCC_TOKEN readable only by the script's user"),
      testStep("On the forwarder server", "/api/v1/alerts/ncc/events?limit=1", "You should see one event, or an empty list on a new install."),
      {
        id: "script",
        title: "Save the polling script",
        where: "On the forwarder server",
        body: (
          <p>
            Save this as <code>ncc_events.py</code>. Each run appends new events to <code>/var/log/ncc/events.jsonl</code> (change it with{" "}
            <code>NCC_EVENTS_FILE</code>) and remembers where it stopped.
          </p>
        ),
        snippets: [{ label: "ncc_events.py", code: POLL_SCRIPT }],
      },
      {
        id: "schedule",
        title: "Run it every 5 minutes",
        where: "On the forwarder server",
        body: <p>Use cron on Linux or Task Scheduler on Windows.</p>,
        snippets: [
          { label: "Linux crontab", code: "*/5 * * * * NCC_TOKEN=$(cat /etc/ncc/token) /usr/bin/python3 /opt/ncc/ncc_events.py" },
          {
            label: "Windows (run once, as admin)",
            code: 'schtasks /Create /SC MINUTE /MO 5 /TN "NCC events" /TR "py C:\\ncc\\ncc_events.py"',
            note: "Set NCC_TOKEN and NCC_EVENTS_FILE as system environment variables for the task's account.",
          },
        ],
      },
      {
        id: "forwarder",
        title: "Point your forwarder at the file",
        where: "In your log tool",
        body: <p>Each line is one JSON event. Examples for the two most common forwarders; adjust index and paths to your setup.</p>,
        snippets: [
          {
            label: "Splunk Universal Forwarder — inputs.conf",
            code: `[monitor:///var/log/ncc/events.jsonl]
sourcetype = _json
index = ncc`,
          },
          {
            label: "Elastic Filebeat — filebeat.yml",
            code: `filebeat.inputs:
  - type: filestream
    id: ncc-events
    paths: [/var/log/ncc/events.jsonl]
    parsers:
      - ndjson:
          target: ""`,
          },
        ],
      },
      {
        id: "verify",
        title: "Search for the events",
        where: "In your log tool",
        body: (
          <p>
            Search for <code>type=detected</code> (Splunk) or <code>type:detected</code> (Kibana). Useful fields: <code>alert_id</code>,{" "}
            <code>type</code>, <code>actor</code>, <code>to_state</code>, <code>to_severity</code>, <code>at</code>.
          </p>
        ),
      },
    ],
  },
  {
    id: "notifications",
    name: "Slack, email & webhooks",
    alsoFor: "Any service that can receive a JSON webhook",
    category: "Chat & email",
    icon: <BellOutlined />,
    summary: "Post a summary after each run, a daily digest, and alerts when runs fail, to a channel or inbox.",
    youGet: ["A message after each run with what failed and on which clusters.", "Optional daily digest.", "Delivery status and retries, visible on the Notifications page."],
    youNeed: ["An NCC admin", "A Slack incoming webhook, an SMTP server, or a webhook URL"],
    time: "About 10 minutes",
    level: "Easy",
    direction: "NCC sends messages out; nothing needs to reach NCC.",
    role: "none",
    terms: ["webhook", "hmac"],
    doc: "docs/FEATURES_AND_CONFIG_FLAGS.md",
    steps: [
      {
        id: "slack",
        title: "Slack: create an incoming webhook",
        where: "In Slack",
        body: (
          <p>
            In Slack, open <b>Apps → Incoming Webhooks</b> (or create an app with incoming webhooks turned on), pick the channel, and copy the
            webhook URL. Treat it like a password: anyone with it can post to the channel.
          </p>
        ),
      },
      {
        id: "email",
        title: "Email: get your SMTP details",
        where: "From your mail team",
        body: <p>You need the SMTP server and port, whether it uses TLS, a sender address, and an account if it requires sign-in.</p>,
        optional: true,
      },
      {
        id: "configure",
        title: "Add the channel in NCC",
        where: "In NCC Orchestrator (admin)",
        action: "notifications",
        body: (
          <p>
            Open <b>Settings → Notifications</b>, turn on the channel, paste the webhook URL or SMTP details, choose which events to send, and
            save. Use <b>Send test</b>: the message should arrive within a few seconds, and the channel's status turns green.
          </p>
        ),
      },
      {
        id: "verify-signature",
        title: "Webhooks: check messages really come from NCC",
        where: "In the receiving service",
        optional: true,
        body: (
          <p>
            If you set a signing secret on the webhook, NCC signs each message and sends <code>X-NCC-Signature: sha256=…</code>. Reject messages
            whose signature does not match. Keep the secret out of code and logs.
          </p>
        ),
        snippets: [{ label: "Python check", code: WEBHOOK_VERIFY }],
      },
    ],
  },
  {
    id: "mcp",
    name: "AI assistants (MCP)",
    alsoFor: "Claude Desktop, Cursor, VS Code and other MCP clients",
    category: "AI assistants",
    icon: <RobotOutlined />,
    summary: "Ask an AI assistant about your NCC results in plain language: what failed, where, and what changed.",
    youGet: [
      "Questions like \"Which clusters failed the last run and why?\" answered from real results.",
      "Read-only by default: the assistant can do only what its token's role allows, and starting runs is off until you turn it on.",
      "Nothing extra runs on the NCC server: the program runs on the computer with the assistant and calls the API with its own token.",
    ],
    youNeed: [
      "A personal access token for an account with the viewer role (operator only if the assistant may start runs)",
      "Go 1.27.4 or newer on any computer, once, to build the small ncc-mcp-server program. It is optional and not part of the stack download",
      "The API server's address. Use HTTPS unless the assistant runs on the same computer as the API",
    ],
    time: "About 15 minutes",
    level: "Moderate",
    direction: "The assistant starts ncc-mcp-server on your computer; it asks NCC's API for results using your token.",
    role: "viewer",
    terms: ["mcp", "token", "role"],
    doc: "docs/MCP_SERVER.md#import-in-five-minutes",
    steps: [
      accountStep("svc-assistant", "viewer", "Use operator only if the assistant may start runs. Every call is limited to this account's role and appears in the audit log."),
      tokenStep("svc-assistant", "the assistant's MCP config in step 4"),
      {
        id: "build",
        title: "Build the MCP program",
        where: "On the computer running the assistant",
        body: (
          <>
            <p>
              <code>ncc-mcp-server</code> is a separate, optional program, so it is not in the stack download. Build it once from the project's source code with Go
              1.27.4 or newer. Older builds (2.3.0 and earlier) had no sign-in and must not be used; this one needs the token.
            </p>
            <p className="integration-hint">Keep the built file somewhere only you can change, and note its full path for step 4.</p>
          </>
        ),
        snippets: [
          {
            label: "macOS or Linux",
            code: `git clone <your copy of the NCC Orchestrator repository> ncc-orchestrator
cd ncc-orchestrator
go build -trimpath -o ncc-mcp-server ./cmd/ncc-mcp-server`,
          },
          {
            label: "Windows (PowerShell)",
            code: `git clone <your copy of the NCC Orchestrator repository> ncc-orchestrator
cd ncc-orchestrator
go build -trimpath -o ncc-mcp-server.exe ./cmd/ncc-mcp-server`,
          },
        ],
      },
      testStep("On the computer running the assistant", "/api/v1/alerts/ncc?limit=1", "You should see one open finding, or an empty list."),
      {
        id: "configure",
        title: "Add it to your assistant",
        where: "In the AI assistant",
        body: (
          <>
            <p>
              Open the assistant's MCP settings file (Cursor: <code>~/.cursor/mcp.json</code>; Claude Desktop: <b>Settings → Developer → Edit Config</b>) and add the
              server with its full path and the token. Restart the assistant.
            </p>
            <p className="integration-hint">
              If the API server uses plain HTTP and is not on this computer, the program refuses to connect. Put HTTPS in front of it (see the HTTPS guide) or forward the port
              over SSH and use <code>http://127.0.0.1:&lt;port&gt;</code>.
            </p>
          </>
        ),
        snippets: [
          {
            label: "MCP config",
            code: `{
  "mcpServers": {
    "ncc-orchestrator": {
      "command": "/absolute/path/to/ncc-mcp-server",
      "env": {
        "NCC_MCP_API_URL": "{{API}}",
        "NCC_MCP_TOKEN": "paste the token here",
        "NCC_MCP_PROFILE": "readonly"
      }
    }
  }
}`,
          },
        ],
      },
      {
        id: "ask",
        title: "Ask it something",
        where: "In the AI assistant",
        body: <p>Try these. The assistant shows which NCC tool it uses before answering.</p>,
        snippets: [
          {
            label: "Example questions",
            code: `Summarize the last NCC run: which clusters failed and why?
List the FAIL findings on <cluster name> and what each check means.
What changed between the last two runs?`,
          },
        ],
      },
      {
        id: "changes",
        title: "Allow changes (only if you need them)",
        where: "In the MCP config",
        optional: true,
        body: (
          <p>
            Starting runs or changing schedules needs three things: an <b>operator</b> token, <code>"NCC_MCP_ALLOW_MUTATIONS": "true"</code> in <code>env</code>, and the
            assistant's confirmation of each change. Keep <code>NCC_MCP_PROFILE</code> at <code>readonly</code> otherwise. The confirmation is a safeguard against
            mistakes, not a lock: what really limits the assistant is the token's role, so use the smallest role that works. Treat report text as data, not
            instructions to the assistant.
          </p>
        ),
      },
    ],
  },
  {
    id: "scripts",
    name: "Your own scripts (REST API)",
    alsoFor: "Ansible, PowerShell, Python, Power Automate, n8n…",
    category: "Scripts",
    icon: <CodeOutlined />,
    summary: "Read findings, start runs or acknowledge findings from any script or automation tool.",
    youGet: ["Every action in this UI is available over the API.", "A machine-readable description (OpenAPI) that many tools import directly."],
    youNeed: ["An account with the role your script needs", "curl, PowerShell or any HTTP client"],
    time: "About 10 minutes",
    level: "Easy",
    direction: "Your script calls NCC whenever it runs.",
    role: "viewer",
    terms: ["api", "token", "role", "finding", "openapi"],
    doc: "docs/integrations/third-party-alert-integrations.md#5-endpoints",
    steps: [
      accountStep("svc-automation", "viewer", "Use operator only if the script acknowledges findings or starts runs."),
      tokenStep("svc-automation", "your automation tool's secret store, or the NCC_TOKEN environment variable"),
      testStep("Where the script runs", "/api/v1/alerts/ncc?limit=1", "You should see one open finding, or an empty list."),
      {
        id: "examples",
        title: "Common calls",
        where: "In your script",
        body: <p>Responses are JSON with the data under <code>data</code>. Lists are paged: pass <code>next_cursor</code> back as <code>cursor</code>.</p>,
        snippets: [
          {
            label: "Open findings on one cluster",
            code: `curl -sS -H "Authorization: Bearer $NCC_TOKEN" "{{API}}/api/v1/alerts/ncc?cluster=<cluster address>&limit=100"`,
          },
          {
            label: "Acknowledge a finding (operator)",
            code: `curl -sS -X POST -H "Authorization: Bearer $NCC_TOKEN" -H "Content-Type: application/json" \\
  -d '{"note": "CHG0042 in progress"}' "{{API}}/api/v1/alerts/ncc/<finding id>/acknowledge"`,
          },
          {
            label: "Start a run (operator)",
            code: `curl -sS -X POST -H "Authorization: Bearer $NCC_TOKEN" -H "Content-Type: application/json" \\
  -d '{}' "{{API}}/api/v1/runs/trigger"`,
          },
          {
            label: "PowerShell: open findings",
            code: `$h = @{ Authorization = "Bearer $env:NCC_TOKEN" }
(Invoke-RestMethod -Headers $h -Uri "{{API}}/api/v1/alerts/ncc?limit=100").data.items |
  Select-Object cluster_name, check, severity, state`,
          },
        ],
      },
      {
        id: "openapi",
        title: "Import the API description",
        where: "In your tool",
        optional: true,
        action: "developer",
        body: (
          <p>
            Tools like Postman, Power Automate custom connectors and code generators can import the OpenAPI description. Admins can also try
            every call in <b>System → Developer → API explorer</b>.
          </p>
        ),
        snippets: [{ label: "OpenAPI description", code: "{{API}}/api/v1/openapi.json" }],
      },
    ],
  },
];

export function integrationById(id: string | undefined): Integration | undefined {
  return INTEGRATIONS.find((i) => i.id === id);
}

export type ApiAddress = { api: string; host: string; scheme: string };

/** Splits a typed address into the parts snippets use; tolerates a missing scheme or trailing slash. */
export function parseApiAddress(raw: string): ApiAddress {
  let text = raw.trim().replace(/\/+$/, "");
  if (!/^https?:\/\//i.test(text)) text = `https://${text}`;
  try {
    const u = new URL(text);
    const scheme = u.protocol.replace(":", "");
    const host = u.port ? `${u.hostname}:${u.port}` : u.hostname;
    const path = u.pathname.replace(/\/+$/, "");
    return { api: `${scheme}://${host}${path}`, host, scheme };
  } catch {
    return { api: text, host: text.replace(/^https?:\/\//i, ""), scheme: "https" };
  }
}

/** Default guess: this page's host on the API server's default port. */
export function defaultApiAddress(loc: { protocol: string; hostname: string }): string {
  return `${loc.protocol}//${loc.hostname}:8081`;
}

export function fillSnippet(code: string, addr: ApiAddress): string {
  return code.split("{{API}}").join(addr.api).split("{{HOST}}").join(addr.host).split("{{SCHEME}}").join(addr.scheme);
}
