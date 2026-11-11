// Sample data for the public read-only demo (frontend/vite.demo.config.ts).
// Every value is invented: cluster names, addresses and findings only look
// like a real fleet. Timestamps are relative to page load so the demo always
// shows a "recent" run. Typed against api/types.ts so drift fails tsc.
import type {
  AuditLogData,
  AutoHealer,
  BackupEntry,
  BackupScheduleResponse,
  ClusterGroup,
  ComponentsData,
  ConfigListData,
  DiagnosticsData,
  ExtraArgOption,
  FeatureFlags,
  HealHistoryData,
  HealthData,
  MeData,
  NCCAlert,
  NCCDisposition,
  NotificationState,
  NotificationStatusResponse,
  OnboardingStatus,
  PCAlertsData,
  PlatformData,
  ReportData,
  ReportTrendsData,
  RunActiveData,
  RunByIdData,
  RunInfo,
  ScheduleHealthData,
  ScheduleState,
  SessionPolicy,
  SupervisorData,
  TrendPoint,
  UpdateStatus,
  UserAccount,
} from "../api/types";

export const DEMO_VERSION = "2.4.0";

const NOW = Date.now();
const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
const iso = (t: number) => new Date(t).toISOString();

/** Deterministic PRNG so every visitor sees the same fleet. */
function rng(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 2 ** 32;
  };
}

type DemoCluster = {
  name: string;
  address: string;
  aos: string;
  ncc: string;
  /** Unreachable clusters have an error instead of checks. */
  error?: string;
  errorClass?: string;
};

const SAMPLE_CLUSTERS: DemoCluster[] = [
  { name: "prod-sjc-01", address: "10.20.1.11", aos: "7.3.1", ncc: "5.2.0" },
  { name: "prod-sjc-02", address: "10.20.1.12", aos: "7.3.1", ncc: "5.2.0" },
  { name: "prod-dfw-01", address: "10.30.1.11", aos: "7.3.0.5", ncc: "5.2.0" },
  { name: "prod-ams-01", address: "10.40.1.11", aos: "7.3.1", ncc: "5.2.0" },
  { name: "dr-fra-01", address: "10.40.2.11", aos: "7.0.1.7", ncc: "5.1.2" },
  { name: "vdi-sjc-01", address: "10.20.3.11", aos: "7.3.1", ncc: "5.2.0" },
  { name: "dev-lab-01", address: "10.90.0.11", aos: "7.3.1", ncc: "5.2.0" },
  {
    name: "dr-fra-02",
    address: "10.40.2.21",
    aos: "7.0.1.7",
    ncc: "5.1.2",
    error: "dial tcp 10.40.2.21:9440: i/o timeout",
    errorClass: "network",
  },
];

/**
 * `?fleet=7000` (kept for the tab) swaps the eight sample clusters for a
 * generated fleet of that size, to try the UI at scale.
 */
function fleetSize(): number {
  const KEY = "ncc-demo-fleet";
  let raw: string | null = null;
  try {
    raw = new URLSearchParams(window.location.search).get("fleet");
    if (raw !== null) sessionStorage.setItem(KEY, raw);
    else raw = sessionStorage.getItem(KEY);
  } catch {
    /* storage blocked: sample fleet */
  }
  const n = Math.floor(Number(raw));
  return Number.isFinite(n) && n > SAMPLE_CLUSTERS.length ? Math.min(n, 10000) : 0;
}

function generatedFleet(count: number): DemoCluster[] {
  const kinds = ["prod", "prod", "prod", "dr", "vdi", "dev"];
  const sites = ["sjc", "dfw", "ams", "fra", "sin", "syd", "iad", "lhr", "nrt", "gru"];
  const r = rng(7);
  return Array.from({ length: count }, (_, i) => {
    const kind = kinds[i % kinds.length];
    const site = sites[Math.floor(i / kinds.length) % sites.length];
    const address = `10.${20 + Math.floor(i / 62500)}.${Math.floor(i / 250) % 250}.${(i % 250) + 1}`;
    const c: DemoCluster = {
      name: `${kind}-${site}-${String(i + 1).padStart(4, "0")}`,
      address,
      aos: r() < 0.8 ? "7.3.1" : "7.0.1.7",
      ncc: r() < 0.85 ? "5.2.0" : "5.1.2",
    };
    if (r() < 0.02) {
      c.error = `dial tcp ${address}:9440: i/o timeout`;
      c.errorClass = "network";
    }
    return c;
  });
}

const FLEET = fleetSize();
const CLUSTERS: DemoCluster[] = FLEET ? generatedFleet(FLEET) : SAMPLE_CLUSTERS;

type CheckTemplate = { check: string; severity: "FAIL" | "WARN" | "ERR" | "INFO"; detail: (c: DemoCluster, n: number) => string };

const CHECKS: CheckTemplate[] = [
  { check: "disk_usage_check", severity: "FAIL", detail: (_c, n) => `Storage container 'default-container' usage is ${85 + (n % 9)}%, above the 85% threshold.` },
  { check: "ntp_server_reachability_check", severity: "FAIL", detail: (c) => `NTP server 10.0.0.123 is not reachable from CVM ${cvm(c, 2)}.` },
  { check: "degraded_node_check", severity: "FAIL", detail: (c) => `Node ${cvm(c, 3)} was marked degraded 2 times in the last 24 hours.` },
  { check: "nic_link_down_check", severity: "FAIL", detail: (c) => `NIC eth3 on host ${host(c, 1)} is down (link was up at last check).` },
  { check: "cvm_memory_usage_check", severity: "WARN", detail: (c, n) => `CVM ${cvm(c, 1)} memory usage is ${90 + (n % 6)}% (threshold 90%).` },
  { check: "dimm_ce_check", severity: "WARN", detail: (c) => `Correctable ECC errors on DIMM A2 of host ${host(c, 2)} (34 in 24 h).` },
  { check: "stale_recovery_points_check", severity: "WARN", detail: (_c, n) => `${3 + (n % 5)} recovery points are older than the 30-day retention policy.` },
  { check: "cvm_time_drift_check", severity: "WARN", detail: (c) => `CVM ${cvm(c, 4)} clock differs from the cluster leader by 1.8 s.` },
  { check: "hypervisor_disk_space_check", severity: "WARN", detail: (c) => `Host ${host(c, 3)} /scratch partition is 82% used.` },
  { check: "cluster_services_status", severity: "ERR", detail: () => "The check could not complete: Genesis did not answer within 120 s." },
  { check: "ipmi_sel_log_fetch_check", severity: "ERR", detail: (c) => `Could not read the IPMI SEL from ${host(c, 1)}: connection refused.` },
  { check: "protection_domain_snapshot_schedule_check", severity: "INFO", detail: () => "Protection domain 'pd-gold' has no schedule; snapshots are manual." },
  { check: "aos_ncc_version_compatibility_check", severity: "INFO", detail: (c) => `NCC ${c.ncc} is compatible with AOS ${c.aos}; a newer NCC is available.` },
  { check: "vm_memory_overcommit_check", severity: "INFO", detail: (_c, n) => `${2 + (n % 4)} VMs have memory overcommit enabled.` },
];

function octets(c: DemoCluster) {
  return c.address.split(".").slice(0, 3).join(".");
}
function cvm(c: DemoCluster, n: number) {
  return `${octets(c)}.${30 + n}`;
}
function host(c: DemoCluster, n: number) {
  return `${octets(c)}.${50 + n}`;
}

type Row = { cluster: string; severity: string; check: string; detail: string; clusterName: string; clusterVersion: string; nccVersion: string };

const PLUGINS_PER_CLUSTER = 468;

function buildRows(): Row[] {
  const rows: Row[] = [];
  CLUSTERS.forEach((c, ci) => {
    if (c.error) return;
    const r = rng(1000 + ci * 17);
    CHECKS.forEach((t, ti) => {
      // dev-lab is noisy; prod-sjc-02 is clean apart from INFO findings.
      let p = t.severity === "FAIL" ? 0.32 : t.severity === "WARN" ? 0.5 : t.severity === "ERR" ? 0.22 : 0.6;
      if (c.name === "dev-lab-01") p = Math.min(0.95, p * 1.8);
      if (c.name === "prod-sjc-02" && t.severity !== "INFO") p = 0;
      if (r() < p) {
        rows.push({
          cluster: c.address,
          severity: t.severity,
          check: t.check,
          detail: t.detail(c, ci + ti),
          clusterName: c.name,
          clusterVersion: c.aos,
          nccVersion: c.ncc,
        });
      }
    });
  });
  return rows;
}

export const AGG_ROWS = buildRows();

const ROWS_BY_CLUSTER = new Map<string, Row[]>();
for (const row of AGG_ROWS) {
  const list = ROWS_BY_CLUSTER.get(row.cluster);
  if (list) list.push(row);
  else ROWS_BY_CLUSTER.set(row.cluster, [row]);
}

const COUNTS = new Map<string, { fail: number; warn: number; err: number; info: number }>();
for (const row of AGG_ROWS) {
  let k = COUNTS.get(row.cluster);
  if (!k) COUNTS.set(row.cluster, (k = { fail: 0, warn: 0, err: 0, info: 0 }));
  if (row.severity === "FAIL") k.fail++;
  else if (row.severity === "WARN") k.warn++;
  else if (row.severity === "ERR") k.err++;
  else k.info++;
}

function countsFor(address: string) {
  const { fail, warn, err, info } = COUNTS.get(address) ?? { fail: 0, warn: 0, err: 0, info: 0 };
  const score = Math.max(40, 100 - fail * 9 - warn * 3 - err * 4);
  return { fail, warn, err, info, score };
}

const LATEST_RUN_AT = NOW - 47 * MIN;
const LATEST_RUN_ID = runId(LATEST_RUN_AT);

function runId(t: number) {
  const d = new Date(t);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}-${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}`;
}

const clusterSummaries = CLUSTERS.map((c) => {
  if (c.error) return { address: c.address, ok: false, error: c.error, error_class: c.errorClass };
  const k = countsFor(c.address);
  return {
    address: c.address,
    ok: true,
    fail_count: k.fail,
    warn_count: k.warn,
    err_count: k.err,
    info_count: k.info,
    checks_total: PLUGINS_PER_CLUSTER,
    health_score: k.score,
  };
});

const okClusters = clusterSummaries.filter((c) => c.ok);
const scores = okClusters.map((c) => c.health_score ?? 0);
const total = (s: string) => AGG_ROWS.filter((r) => r.severity === s).length;

export const RUN_SUMMARY = {
  schema_version: "1",
  run_id: LATEST_RUN_ID,
  timestamp: iso(LATEST_RUN_AT),
  duration_s: 412.6,
  clusters_ok: okClusters.length,
  clusters_failed: CLUSTERS.length - okClusters.length,
  failed_clusters: CLUSTERS.filter((c) => c.error).map((c) => c.address),
  clusters: clusterSummaries,
  exit_code: 3,
  status: "partial",
  index_html: "outputfiles/index.html",
  total_checks: okClusters.length * PLUGINS_PER_CLUSTER,
  avg_health_score: Math.round(scores.reduce((a, b) => a + b, 0) / scores.length),
  min_health_score: Math.min(...scores),
  failure_classes: { network: CLUSTERS.filter((c) => c.error).length },
  source: "scheduled",
};

const CHECKS_SNAPSHOT = {
  timestamp: iso(LATEST_RUN_AT),
  clusters: CLUSTERS.map((c) => {
    if (c.error) {
      return { address: c.address, checks: [{ check_name: "cluster_reachability", severity: "ERR", detail: c.error }] };
    }
    const k = countsFor(c.address);
    return {
      address: c.address,
      checks: (ROWS_BY_CLUSTER.get(c.address) ?? []).map((r) => ({
        finding_id: findingId(r.cluster, r.check),
        check_name: r.check,
        severity: r.severity,
        detail: r.detail,
      })),
      fail_count: k.fail,
      warn_count: k.warn,
      err_count: k.err,
      info_count: k.info,
      checks_total: PLUGINS_PER_CLUSTER,
      health_score: k.score,
    };
  }),
};

function findingId(cluster: string, check: string) {
  let h = 2166136261;
  for (const ch of `${cluster}\0${check}`) h = Math.imul(h ^ ch.charCodeAt(0), 16777619) >>> 0;
  return `finding-${h.toString(16).padStart(8, "0")}${(h ^ 0x5bd1e995).toString(16).padStart(8, "0")}`;
}

const failRows = AGG_ROWS.filter((r) => r.severity === "FAIL");

const DRILLDOWN = {
  timestamp: iso(LATEST_RUN_AT),
  previous_timestamp: iso(LATEST_RUN_AT - DAY),
  new_fail_count: 2,
  resolved_fail_count: 1,
  clusters: [
    { address: failRows[0]?.cluster ?? "10.20.1.11", new_failures: [failRows[0]?.check ?? "disk_usage_check"] },
    { address: failRows[1]?.cluster ?? "10.90.0.11", new_failures: [failRows[1]?.check ?? "nic_link_down_check"], resolved_failures: ["cvm_time_drift_check"] },
  ],
};

const FLAKY = {
  timestamp: iso(LATEST_RUN_AT),
  lookback_runs: 10,
  min_transitions: 3,
  total_flaky_checks: 2,
  checks: [
    { cluster: "10.90.0.11", check_name: "cvm_time_drift_check", transitions: 5, observations: 10, states: ["PASS", "WARN", "PASS", "WARN", "PASS", "WARN"], current: "WARN" },
    { cluster: "10.30.1.11", check_name: "ntp_server_reachability_check", transitions: 3, observations: 10, states: ["PASS", "FAIL", "PASS", "FAIL"], current: "FAIL" },
  ],
};

const SLO = {
  timestamp: iso(LATEST_RUN_AT),
  duration_s: RUN_SUMMARY.duration_s,
  clusters: okClusters.map((c) => ({
    address: c.address,
    checks_total: PLUGINS_PER_CLUSTER,
    fail_count: c.fail_count ?? 0,
    warn_count: c.warn_count ?? 0,
    err_count: c.err_count ?? 0,
    info_count: c.info_count ?? 0,
    fail_rate_percent: Math.round(((c.fail_count ?? 0) / PLUGINS_PER_CLUSTER) * 10000) / 100,
    health_score: c.health_score ?? 0,
    status: (c.fail_count ?? 0) > 0 ? "breach" : "ok",
  })),
};

const nccTotal = okClusters.length * PLUGINS_PER_CLUSTER;
const nonPass = AGG_ROWS.length;

export const REPORT: ReportData = {
  run_summary: RUN_SUMMARY,
  ncc_summary_counts: {
    total_plugins: nccTotal,
    pass: nccTotal - nonPass,
    fail: total("FAIL"),
    error: total("ERR"),
    warn: total("WARN"),
    info: total("INFO"),
    unknown: 0,
  },
  ncc_cluster_summary: CLUSTERS.filter((c) => !c.error).map((c) => {
    const k = countsFor(c.address);
    const pass = PLUGINS_PER_CLUSTER - k.fail - k.warn - k.err - k.info;
    return {
      address: c.address,
      log_name: c.name,
      pass,
      fail: k.fail,
      warn: k.warn,
      error: k.err,
      info: k.info,
      unknown: 0,
      total_plugins: PLUGINS_PER_CLUSTER,
      health_rate: Math.round((pass / PLUGINS_PER_CLUSTER) * 1000) / 10,
    };
  }),
  checks_snapshot: CHECKS_SNAPSHOT,
  drilldown_diff: DRILLDOWN,
  flaky_checks: FLAKY,
  regression_summary: { has_regression: true, delta_fail_total: 1, previous_timestamp: iso(LATEST_RUN_AT - DAY) },
  slo_dashboard: SLO,
  policy_violations: ["fail_total 9 exceeds max_fail 5 (policy: production)"].map((s) => s.replace("9", String(total("FAIL")))),
  agg_rows: AGG_ROWS,
  diff_flags: {},
  flaky_keys: Object.fromEntries(FLAKY.checks.map((f) => [`${f.cluster}|${f.check_name}`, true])),
  cluster_links: CLUSTERS.map((c) => ({ cluster: c.address, name: c.name, url: `https://${c.address}:9440/` })),
  artifact_links: {},
  report_meta: { generated_at: iso(LATEST_RUN_AT), orchestrator_version: DEMO_VERSION },
  ncc_logs: [],
  pagination: {},
};

// ---------------------------------------------------------------- runs

const RUN_COUNT = 21;

function historicalRun(i: number): RunInfo {
  // i = 0 is the latest run; older runs are daily at the same time.
  const at = LATEST_RUN_AT - i * DAY + (i % 3) * 7 * MIN;
  const r = rng(77 + i);
  const failed = i === 0 ? 1 : r() < 0.25 ? 1 : 0;
  const fail = i === 0 ? total("FAIL") : Math.max(0, Math.round(total("FAIL") + 3 - i * 0.2 + (r() - 0.5) * 4));
  const warn = i === 0 ? total("WARN") : Math.max(0, Math.round(total("WARN") + 4 - i * 0.15 + (r() - 0.5) * 5));
  const err = i === 0 ? total("ERR") : Math.round(r() * 4);
  const info = i === 0 ? total("INFO") : Math.round(10 + r() * 6);
  const avg = i === 0 ? RUN_SUMMARY.avg_health_score : Math.min(99, Math.round(RUN_SUMMARY.avg_health_score - 6 + i * 0.25 + (r() - 0.5) * 4));
  return {
    id: runId(at),
    path: `outputfiles/history/${runId(at)}`,
    mod_time: iso(at + 7 * MIN),
    has_index: true,
    source: "history",
    timestamp: iso(at),
    duration_s: i === 0 ? RUN_SUMMARY.duration_s : Math.round(380 + r() * 90),
    clusters_ok: CLUSTERS.length - failed,
    clusters_failed: failed,
    total_checks: (CLUSTERS.length - failed) * PLUGINS_PER_CLUSTER,
    avg_health_score: avg,
    min_health_score: Math.max(40, avg - 22),
    fail_total: fail,
    warn_total: warn,
    err_total: err,
    info_total: info,
    exit_code: failed ? 3 : 0,
    success: !failed,
    status: failed ? "partial" : "success",
    status_source: "exit_code",
    run_source: i % 7 === 3 ? "manual" : "scheduled",
  };
}

export const RUNS: RunInfo[] = Array.from({ length: RUN_COUNT }, (_, i) => historicalRun(i));

export function runById(id: string): RunByIdData | undefined {
  const run = RUNS.find((r) => r.id === id);
  if (!run) return undefined;
  const isLatest = run.id === LATEST_RUN_ID;
  return {
    run,
    artifacts: {
      "run-summary.json": isLatest ? RUN_SUMMARY : { ...RUN_SUMMARY, timestamp: run.timestamp, run_id: run.id, clusters: clusterSummaries },
      "checks-snapshot.json": CHECKS_SNAPSHOT,
      "regression-summary.json": REPORT.regression_summary,
    },
  };
}

export const TRENDS: ReportTrendsData = {
  points: RUNS.slice()
    .reverse()
    .map(
      (r): TrendPoint => ({
        timestamp: r.timestamp ?? r.mod_time,
        duration_s: r.duration_s ?? 0,
        clusters_ok: r.clusters_ok ?? 0,
        clusters_failed: r.clusters_failed ?? 0,
        total_checks: r.total_checks ?? 0,
        avg_health_score: r.avg_health_score ?? 0,
        min_health_score: r.min_health_score ?? 0,
        fail_total: r.fail_total ?? 0,
        warn_total: r.warn_total ?? 0,
        err_total: r.err_total ?? 0,
        info_total: r.info_total ?? 0,
      }),
    ),
  count: RUN_COUNT,
  limit: 30,
  report_source_dir: "outputfiles",
};

export const RUN_ACTIVE: RunActiveData = {
  active: false,
  started_at: "",
  last_error: "",
  output_dir: "outputfiles",
  config_path: "config.yaml",
  runs: [],
  running_count: 0,
  queued_count: 0,
  max_concurrent: 2,
  avg_run_duration_sec: 418,
};

// ---------------------------------------------------------------- alerts

export const NCC_ALERTS: NCCAlert[] = AGG_ROWS.filter((r) => r.severity === "FAIL" || r.severity === "WARN").map((r, i) => {
  const firstSeen = LATEST_RUN_AT - ((i * 3) % 12) * DAY;
  const state: NCCAlert["state"] = i % 5 === 1 ? "acknowledged" : "open";
  return {
    id: `0b8f${String(1000 + i)}-6c1e-4f5a-9d2b-${String(100000000000 + i * 7919).slice(-12)}`,
    finding_id: findingId(r.cluster, r.check),
    correlation_key: `${r.cluster}|${r.check}`,
    source: "NCC",
    type: "ncc_check",
    cluster: r.cluster,
    cluster_name: r.clusterName,
    check: r.check,
    severity: r.severity,
    state,
    detection: "detected",
    closed: false,
    first_seen_at: iso(firstSeen),
    first_seen_run: runId(firstSeen),
    last_seen_at: iso(LATEST_RUN_AT),
    last_seen_run: LATEST_RUN_ID,
    acknowledged_at: state === "acknowledged" ? iso(LATEST_RUN_AT - 3 * HOUR) : undefined,
    acknowledged_by: state === "acknowledged" ? "alex.ops" : undefined,
    note: state === "acknowledged" ? "Ticket INC-20418 opened with the hardware team." : undefined,
    created_at: iso(firstSeen),
    updated_at: iso(LATEST_RUN_AT),
    revision: state === "acknowledged" ? 3 : 1,
    reopened: i % 7 === 4,
  };
});

export const NCC_DISPOSITIONS: NCCDisposition[] = NCC_ALERTS.filter((a) => a.state === "acknowledged").map((a) => ({
  cluster: a.cluster,
  check: a.check,
  status: "acknowledged",
  by: a.acknowledged_by ?? "alex.ops",
  at: a.acknowledged_at ?? iso(NOW),
  note: a.note,
  id: a.id,
  finding_id: a.finding_id,
  detection: "detected",
  severity: a.severity,
  first_seen_at: a.first_seen_at,
  last_seen_at: a.last_seen_at,
}));

export const PC_ALERTS: PCAlertsData = {
  alerts: [],
  source: "PC",
  fetched_at: iso(NOW),
  configured: false,
  cluster_map: Object.fromEntries(CLUSTERS.map((c) => [c.address, { name: c.name, address: c.address }])),
};

// ---------------------------------------------------------------- identity, health

export const ME: MeData = {
  authenticated: true,
  login_enabled: true,
  local_enabled: true,
  saml_enabled: false,
  oidc_enabled: false,
  ldap_enabled: false,
  auth_mode: "session",
  username: "demo",
  role: "admin",
  is_admin: true,
  can_operate: true,
  must_change_password: false,
  is_local: true,
  session_ttl_sec: 8 * 3600,
  expires_at: iso(NOW + 365 * DAY),
  expires_in_sec: 365 * 86400,
  cluster_access_unrestricted: true,
};

export const HEALTH: HealthData = {
  status: "ok",
  runtime: { mode: "host", kubernetes: false },
  time: iso(NOW),
  config_path: "config.yaml",
  output_dir: "outputfiles",
  log_dir: "logs",
  auth_mode: "session",
  schedule_state: "installed",
  version: DEMO_VERSION,
  build_date: "2026-11-01T00:00:00Z",
  stream: "Demo",
  go_version: "go1.27.4",
  os: "linux",
  arch: "amd64",
  login_enabled: true,
  local_login: true,
  saml_enabled: false,
  oidc_enabled: false,
  ldap_enabled: false,
  intro_title: true,
  cookie_secure: true,
};

export const FEATURES: FeatureFlags = {
  insights: true,
  pc_alerts: true,
  run_comparison: true,
  flaky_checks: true,
  slo: true,
  ncc_log_index: true,
  pc_discovery: true,
  alert_handling: true,
  self_heal: true,
  update_checks: true,
  intro_title: true,
};

export const ONBOARDING: OnboardingStatus = {
  steps: [
    { id: "clusters", title: "Add clusters", description: "List Prism Element clusters or discover them from Prism Central.", done: true, tab: "config", action_label: "Open config" },
    { id: "credentials", title: "Store credentials", description: "Save the Prism password as a secret.", done: true, tab: "config", action_label: "Open secrets" },
    { id: "first_run", title: "Run NCC", description: "Run the checks once to see results.", done: true, tab: "runs", action_label: "Start a run" },
    { id: "schedule", title: "Schedule runs", description: "Run the checks every day.", done: true, tab: "schedule", action_label: "Open schedule" },
    { id: "notifications", title: "Send alerts", description: "Notify Slack, email or a webhook.", done: true, optional: true, tab: "notifications", action_label: "Open notifications" },
  ],
  required_done: 4,
  required_total: 4,
  completed: true,
  dismissed: true,
};

export const COMPONENTS: ComponentsData = {
  components: {
    orchestrator: { version: DEMO_VERSION, status: "ok" },
    "api-server": { version: DEMO_VERSION, status: "ok" },
    "ui-server": { version: DEMO_VERSION, status: "ok" },
    consistent: true,
  },
};

// ---------------------------------------------------------------- settings

export const CONFIG_YAML = `# Sample configuration (demo). See docs/FEATURES_AND_CONFIG_FLAGS.md.
clusters:
${CLUSTERS.map((c) => `  - ${c.address}   # ${c.name}`).join("\n")}
username: admin
password: \${secret:NCC_PASSWORD}

parallel: 4
retries: 2
timeout: 30m

output:
  dir: outputfiles
  formats: [html, csv, json, sarif]

policy:
  max_fail: 5
  fail_on_regression: true

notify:
  slack:
    webhook_url: \${secret:SLACK_WEBHOOK_URL}
    on: [failure, policy_violations]
`;

export const CONFIG_LIST: ConfigListData = {
  items: [{ path: "config.yaml", resolved: "/opt/ncc/config.yaml", exists: true, is_active: true }],
  default_path: "config.yaml",
};

export const SCHEDULE: ScheduleState = {
  type: "systemd",
  action: "install",
  cron: "0 2 * * *",
  config: "config.yaml",
  log_path: "logs/scheduled-run.log",
  with_lock: true,
  task_name: "ncc-orchestrator-scan",
  print_only: false,
  updated_at: iso(NOW - 20 * DAY),
};

export const SCHEDULE_HEALTH: ScheduleHealthData = {
  configured: true,
  saved: true,
  installed: true,
  state_file_exists: true,
  task_name: "ncc-orchestrator-scan",
  type: "systemd",
  action: "install",
  with_lock: true,
  log_path: "logs/scheduled-run.log",
  last_updated_at: SCHEDULE.updated_at,
  last_run: iso(LATEST_RUN_AT),
  last_success: iso(LATEST_RUN_AT + 7 * MIN),
  log_exists: true,
  log_size: 48213,
  log_mod_time: iso(LATEST_RUN_AT + 7 * MIN),
  detector: "systemd",
};

export const USERS: UserAccount[] = [
  { username: "admin", role: "admin", created_at: iso(NOW - 90 * DAY), updated_at: iso(NOW - 30 * DAY) },
  { username: "demo", role: "admin", created_at: iso(NOW - 60 * DAY) },
  { username: "alex.ops", role: "operator", created_at: iso(NOW - 45 * DAY) },
  { username: "sam.noc", role: "viewer", created_at: iso(NOW - 12 * DAY) },
];

export const CLUSTER_GROUPS: ClusterGroup[] = [
  { name: "Production", clusters: CLUSTERS.filter((c) => c.name.startsWith("prod")).map((c) => c.address), local_users: ["alex.ops"] },
  { name: "Disaster recovery", clusters: CLUSTERS.filter((c) => c.name.startsWith("dr")).map((c) => c.address) },
  { name: "VDI and lab", clusters: CLUSTERS.filter((c) => /^(vdi|dev)/.test(c.name)).map((c) => c.address), local_users: ["sam.noc"] },
];

export const CLUSTER_INVENTORY = { clusters: CLUSTERS.map((c) => c.address) };

export const SESSION_POLICY: SessionPolicy = { ttl_sec: 28800, ttl_min: 480, default_ttl_sec: 28800, min_sec: 300, max_sec: 86400, source: "runtime" };

export const NOTIFICATIONS: NotificationState = {
  enabled: true,
  events: { run_success: false, run_failure: true, policy_violations: true },
  slack: { enabled: true, webhook_url: "https://hooks.slack.com/services/T000/B000/demo", channel: "#nutanix-health", username: "NCC Orchestrator" },
  webhook: { enabled: true, url: "https://events.example.com/ncc" },
  digest: { enabled: true, every: "24h", last_sent_at: iso(NOW - 10 * HOUR) },
  last_delivery: {
    slack: { last_attempt_at: iso(LATEST_RUN_AT + 7 * MIN), last_success_at: iso(LATEST_RUN_AT + 7 * MIN), last_event: "policy_violations", success: true, total_success: 42, total_failure: 0 },
  },
  updated_at: iso(NOW - 15 * DAY),
};

// Slack delivers; the team's own event endpoint has been returning 503 since
// yesterday, so its run reports are piling up in the dead-letter folder.
export const NOTIFICATION_STATUS: NotificationStatusResponse = {
  dead_letter_configured: true,
  updated_at: iso(LATEST_RUN_AT + 7 * MIN),
  channels: [
    { channel: "email", enabled: false, state: "never", dead_letters: 0 },
    {
      channel: "webhook",
      enabled: true,
      state: "failing",
      last_attempt_at: iso(LATEST_RUN_AT + 6 * MIN),
      last_success_at: iso(NOW - 26 * HOUR),
      last_error: "webhook status 503",
      attempts: 3,
      source: "runner",
      event: "run_report",
      dead_letters: 4,
    },
    {
      channel: "slack",
      enabled: true,
      state: "ok",
      last_attempt_at: iso(LATEST_RUN_AT + 7 * MIN),
      last_success_at: iso(LATEST_RUN_AT + 7 * MIN),
      attempts: 1,
      source: "api",
      event: "policy_violations",
      dead_letters: 0,
    },
  ],
};

export const BACKUPS: { backups: BackupEntry[]; encryption_policy: { required: boolean; server_key_configured: boolean } } = {
  backups: [0, 1, 2].map((i) => ({
    name: `ncc-backup-${runId(NOW - (i + 1) * DAY).slice(0, 8)}-030000.tar.gz.enc`,
    kind: "manual" as const,
    size: 1_840_000 + i * 12_000,
    mod_time: iso(NOW - (i + 1) * DAY),
    encrypted: true,
  })),
  encryption_policy: { required: false, server_key_configured: true },
};

export const BACKUP_SCHEDULE: BackupScheduleResponse = {
  schedule: { enabled: true, every: "24h", encrypt: true, retain: 7, last_run_at: iso(NOW - DAY), last_status: "ok" },
  key_configured: true,
  next_run: iso(NOW + 5 * HOUR),
};

export const UPDATE_STATUS: UpdateStatus = {
  current_version: DEMO_VERSION,
  latest_version: DEMO_VERSION,
  update_available: false,
  supported: false,
  check_skipped: "Updates are disabled in the demo.",
  job: { in_progress: false, phase: "idle" },
  supervised_upgrades: true,
};

export const SUPERVISOR: SupervisorData = {
  mode: "supervised",
  running: true,
  pid: 4120,
  state: {
    pid: 4120,
    version: DEMO_VERSION,
    started_at: iso(NOW - 6 * DAY),
    updated_at: iso(NOW - 10_000),
    policy: { probe_interval: "15s", max_restarts: 5, window: "10m" },
    children: {
      api: { name: "api", state: "running", pid: 4133, started_at: iso(NOW - 2 * DAY), restarts_total: 1, restarts_in_window: 0, probe_failures: 0, last_probe_at: iso(NOW - 9_000), last_probe_ms: 4, last_probe_ok: true, last_exit_at: iso(NOW - 2 * DAY), last_exit_reason: "unhealthy", last_exit_detail: "health probe failed 3 times", probed: true },
      ui: { name: "ui", state: "running", pid: 4134, started_at: iso(NOW - 6 * DAY), restarts_total: 0, restarts_in_window: 0, probe_failures: 0, last_probe_at: iso(NOW - 9_000), last_probe_ms: 3, last_probe_ok: true, probed: true },
    },
    events: [
      { at: iso(NOW - 6 * DAY), child: "supervisor", event: "started" },
      { at: iso(NOW - 2 * DAY), child: "api", event: "restarted", detail: "health probe failed 3 times; restarted in 2.1 s" },
    ],
  },
};

export const PLATFORM: PlatformData = {
  host: {
    os: "linux",
    arch: { binary: "amd64", native: "amd64", emulated: false, known: true },
    cpus: 8,
    gomaxprocs: 8,
    go_version: "go1.27.4",
    memory: { total_bytes: 16 * 2 ** 30, available_bytes: 9.4 * 2 ** 30 },
    cgroup: {},
    kubernetes: false,
    service_manager: "systemd",
  },
  api_process: {
    pid: 4133,
    uptime_seconds: 2 * 86400,
    goroutines: 41,
    heap_alloc_bytes: 38 * 2 ** 20,
    sys_bytes: 71 * 2 ** 20,
    gc_cycles: 1290,
    memory_limit: 0,
    tuning: { memory_limit_source: "none", gomaxprocs: 8 },
    open_files: 37,
    file_limit_soft: 65536,
    file_limit_hard: 65536,
  },
};

export const AUTOHEAL: { healers: AutoHealer[] } = {
  healers: [
    { id: "supervisor", title: "Process supervisor", description: "Restarts the API or UI server when it crashes or stops answering health probes.", state: "on" },
    { id: "stale_lock", title: "Stale run lock", description: "Clears a run lock left behind by a process that no longer exists.", state: "on" },
    { id: "diagnostics", title: "Self-diagnostics fixes", description: "Repairs permissions, missing folders and stale state files found by diagnostics.", state: "on" },
  ],
};

export const DIAGNOSTICS: DiagnosticsData = {
  generated_at: iso(NOW - 5 * MIN),
  fix_applied: false,
  overall: "ok",
  summary: { ok: 14, warn: 1, fail: 0 },
  checks: [
    { id: "output_dir_writable", title: "Output folder is writable", category: "storage", status: "ok", message: "outputfiles/ is writable.", source: "orchestrator" },
    { id: "token_file_perms", title: "API token file permissions", category: "security", status: "ok", message: "Mode 0600.", source: "api" },
    { id: "tls_cert_expiry", title: "UI certificate expiry", category: "security", status: "warn", message: "The self-signed certificate expires in 24 days.", hint: "Generate a new certificate in Settings → Access → TLS.", source: "api" },
    { id: "schedule_installed", title: "Scheduled scan installed", category: "schedule", status: "ok", message: "systemd timer ncc-orchestrator-scan is active.", source: "orchestrator" },
  ],
};

export const HEAL_HISTORY: HealHistoryData = { entries: [], total: 0, by_check: [], unverified_7d: 0, recurring_after: 3 };

export const AUDIT: AuditLogData = {
  path: "logs/audit.log",
  size: 92_311,
  mod_time: iso(NOW - 3 * HOUR),
  limit: 200,
  count: 6,
  max_bytes: 10 * 2 ** 20,
  filters: { action: "", failures: false },
  entries: [
    { ts: iso(NOW - 3 * HOUR), action: "ncc_alert.acknowledge", success: true, user: "alex.ops", role: "operator", method: "POST", path: "/api/v1/alerts/ncc/…/acknowledge" },
    { ts: iso(LATEST_RUN_AT), action: "run.start", success: true, user: "scheduler", method: "", path: "" },
    { ts: iso(NOW - DAY), action: "backup.create", success: true, user: "scheduler" },
    { ts: iso(NOW - 2 * DAY), action: "auth.login", success: true, username: "alex.ops", client: "10.1.4.22" },
    { ts: iso(NOW - 2 * DAY - HOUR), action: "auth.login", success: false, username: "admin", client: "10.1.9.7" },
    { ts: iso(NOW - 3 * DAY), action: "settings.notifications.update", success: true, user: "admin", role: "admin" },
  ],
};

export const EXTRA_ARGS: { options: ExtraArgOption[] } = {
  options: [
    { flag: "--checks", takes_value: true, label: "Only these checks", help: "Comma-separated NCC check names.", example: "disk_usage_check" },
    { flag: "--parallel", takes_value: true, label: "Parallel clusters", help: "How many clusters to check at once.", example: "4" },
  ],
};

/** One runner (zerolog) JSON line. */
export function runnerLine(t: number, level: string, message: string, fields: Record<string, unknown> = {}): string {
  return JSON.stringify({ level, component: "runner", pid: 4182, ...fields, time: iso(t), message });
}

/** One API server (slog) JSON line. */
function apiLine(t: number, level: string, msg: string, fields: Record<string, unknown> = {}): string {
  return JSON.stringify({ time: iso(t), level, msg, ...fields });
}

const RUN_FIELDS = { run_id: LATEST_RUN_ID, run_source: "schedule" };

export const RUNNER_LOG = {
  path: "logs/ncc-runner.log",
  exists: true,
  content: [
    runnerLine(LATEST_RUN_AT, "info", "run started", { ...RUN_FIELDS, clusters: CLUSTERS.length, config: "config.yaml" }),
    ...CLUSTERS.flatMap((c, i) => {
      const start = LATEST_RUN_AT + 2_000 + i * 900;
      const lines = [runnerLine(start, "info", "cluster run started", { ...RUN_FIELDS, cluster: c.address, cluster_name: c.name })];
      if (c.error) {
        lines.push(runnerLine(start + 30_000, "error", "cluster run failed", { ...RUN_FIELDS, cluster: c.address, error: c.error, error_class: "network", duration_s: 30.0 }));
      } else {
        const k = countsFor(c.address);
        lines.push(runnerLine(start + 3 * MIN, "debug", "results fetched", { ...RUN_FIELDS, cluster: c.address, fetch_s: 2.4 }));
        lines.push(runnerLine(start + 3 * MIN + 5_000, "info", "cluster run completed", { ...RUN_FIELDS, cluster: c.address, findings: k.fail + k.warn, fail_or_error: k.fail, warn: k.warn, duration_s: 185.2, health_score: k.score }));
      }
      return lines;
    }),
    runnerLine(LATEST_RUN_AT + 7 * MIN, "info", "digest email sent", { ...RUN_FIELDS, recipients: 3 }),
    runnerLine(LATEST_RUN_AT + 7 * MIN + 1_000, "warn", "run finished partial", { ...RUN_FIELDS, exit_code: 3, failed_clusters: CLUSTERS.filter((c) => c.error).length }),
  ].join("\n"),
};

export const API_LOG = {
  path: "logs/ncc-api.log",
  exists: true,
  content: [
    apiLine(NOW - 3 * HOUR, "INFO", "ncc-api-server starting", { listen: ":8080", version: "v2.4.0", log_level: "info", auth: "local" }),
    apiLine(NOW - 3 * HOUR + 1_000, "INFO", "self-heal timer armed", { interval: "6h0m0s" }),
    apiLine(LATEST_RUN_AT - 1_000, "INFO", "environment schedule fired", { environment: "prod", schedule: "0 */6 * * *" }),
    apiLine(LATEST_RUN_AT, "INFO", "run accepted", { run_id: LATEST_RUN_ID, source: "schedule", environment: "prod" }),
    apiLine(LATEST_RUN_AT + 200, "INFO", "runner process started", { run_id: LATEST_RUN_ID, pid: 4182, config: "configs/environments/prod.yaml" }),
    apiLine(LATEST_RUN_AT + 7 * MIN + 1_200, "WARN", "run finished partial", { run_id: LATEST_RUN_ID, exit_code: 3, duration_s: 421.7 }),
    apiLine(NOW - 30 * MIN, "INFO", "http request", { method: "POST", path: "/api/v1/auth/login", status: 200, duration_ms: 84.2, user: "demo", request_id: "req-7f3a91" }),
    apiLine(NOW - 22 * MIN, "WARN", "http request", { method: "POST", path: "/api/v1/auth/login", status: 401, duration_ms: 61.0, request_id: "req-81bc02", error: "Wrong username or password.", error_code: "NCC_API_UNAUTHORIZED" }),
    apiLine(NOW - 20 * MIN, "INFO", "audit", { action: "settings.notifications.update", user: "demo", ok: true, request_id: "req-82d114" }),
    apiLine(NOW - 19 * MIN, "WARN", "notification not delivered", { channel: "slack", host: "hooks.slack.com", attempts: 3, error: "Post \"https://hooks.slack.com/…\": context deadline exceeded" }),
    apiLine(NOW - 12 * MIN, "ERROR", "http request", { method: "GET", path: "/api/v1/report/trends", status: 500, duration_ms: 3120.5, user: "alex.ops", request_id: "req-90ae55", error: "read logs/run_history.jsonl: file is locked" }),
    apiLine(NOW - 5 * MIN, "INFO", "scheduled backup written", { path: "backups/ncc-backup-latest.tar.gz", size_bytes: 482113 }),
  ].join("\n"),
};

export const UI_LOG = {
  path: "logs/v2-ui.log",
  exists: true,
  content: [
    `${iso(NOW - 3 * HOUR)} level=INFO msg="ncc-ui-server listening" addr=:5173`,
    `${iso(NOW - 22 * MIN)} level=WARN msg="http request" method=POST path="/api/v1/auth/login" status=401 duration_ms=63.4 request_id=req-81bc02`,
    `${iso(NOW - 12 * MIN)} level=ERROR msg="http request" method=GET path="/api/v1/report/trends" status=500 duration_ms=3124.0 request_id=req-90ae55`,
  ].join("\n"),
};

export const SUPERVISOR_LOG = {
  path: "logs/v2-supervisor.log",
  exists: true,
  content: [
    `${iso(NOW - 3 * HOUR)} INFO starting ncc-api-server (pid 3920)`,
    `${iso(NOW - 3 * HOUR + 1_500)} INFO starting ncc-ui-server (pid 3944)`,
    `${iso(NOW - 3 * HOUR + 3_000)} INFO health check ok: http://127.0.0.1:8080/healthz`,
  ].join("\n"),
};

export const LOG_SOURCES = [
  { id: "runner", label: "Runner", description: "Health-check runs: one line per cluster step, with run ID and timings.", format: "json", files: ["logs/ncc-runner.log"], admin_only: false },
  { id: "api", label: "API server", description: "Every API request, run, schedule, notification and setting change, with request ID and user.", format: "json", files: ["logs/ncc-api.log"], admin_only: true },
  { id: "ui", label: "UI server", description: "Web UI server: failed and slow requests, and API writes.", format: "text", files: ["logs/v2-ui.log"], admin_only: true },
  { id: "supervisor", label: "Supervisor", description: "Process start, stop, restart and health checks.", format: "text", files: ["logs/v2-supervisor.log"], admin_only: true },
] as const;

// ----------------------------------------------------------- environments

function envYaml(description: string, clusters: string[], extra = ""): string {
  return [
    "schema-version: 1",
    `description: ${description}`,
    "runner:",
    "  targets:",
    "    clusters:",
    ...clusters.map((c) => `      - ${c}`),
    extra,
  ]
    .filter(Boolean)
    .join("\n") + "\n";
}

const byKind = (...kinds: string[]) => CLUSTERS.filter((c) => kinds.includes(c.name.split("-")[0])).map((c) => c.address);

/** Runner-only environment files, configs/environments/<name>.yaml. */
export const ENVIRONMENT_FILES: Record<string, string> = {
  prod: envYaml("Production and VDI clusters", byKind("prod", "vdi"), "  execution:\n    timeout: 20m\n  automation:\n    enabled: false"),
  dr: envYaml("Disaster-recovery sites", byKind("dr"), "  execution:\n    timeout: 30m"),
  dev: envYaml("Lab and development clusters", byKind("dev"), "  automation:\n    enabled: true"),
};

export const ENVIRONMENT_UPDATED = iso(NOW - 6 * DAY);

/** Older full config files that can be turned into environments. */
export const IMPORTABLE_CONFIGS = ["configs/legacy-staging.yaml"];

export const LEGACY_STAGING_CLUSTERS = byKind("dev").slice(0, 1);

export const ENVIRONMENT_SCHEDULES = [
  { id: "es-prod", environment: "prod", enabled: true, every: "1d", at: "02:00", created_at: iso(NOW - 20 * DAY) },
  { id: "es-dev", environment: "dev", enabled: true, every: "7d", at: "06:00", created_at: iso(NOW - 20 * DAY) },
];
