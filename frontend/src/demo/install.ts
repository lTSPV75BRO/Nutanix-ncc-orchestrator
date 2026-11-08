// Demo backend for the public website. Imported first by the demo build only
// (frontend/vite.demo.config.ts); the normal build never includes it.
//
// It replaces window.fetch for /api/* with a small in-browser server built on
// ./fixtures: visitors sign in with a sample account, and what they do (alert
// handling, runs, users, notification and schedule settings, backups, feature
// switches) is kept in sessionStorage, so it survives a reload and resets when
// the tab closes or they choose Reset. Nothing leaves the browser.
import { parse as parseYaml } from "yaml";
import * as fx from "./fixtures";
import type {
  AuditLogEntry,
  BackupEntry,
  EnvironmentInfo,
  EnvironmentSchedule,
  NCCAlert,
  NCCAlertEvent,
  NCCDisposition,
  RunActiveData,
  RunInfo,
  UserAccount,
  UserRole,
} from "../api/types";

/** Every sample account signs in with this password. */
export const DEMO_PASSWORD = "demo";
const RUN_SECONDS = 45;
const STORE_KEY = "ncc-demo-state-v1";

const NOT_IN_DEMO =
  "Not available in the demo. Download NCC Orchestrator to try it on your own clusters.";

type AlertMark = { state: NCCAlert["state"]; by: string; at: string; note?: string };
type ActiveRun = { id: string; startedAt: number; clusters: string[]; user: string; environment?: string; source?: string };
type EnvFile = { content: string; updated_at: string };

type DemoState = {
  user: { username: string; role: UserRole } | null;
  users: UserAccount[];
  alerts: Record<string, AlertMark>;
  runs: RunInfo[];
  active: ActiveRun | null;
  backups: BackupEntry[];
  audit: AuditLogEntry[];
  environments: Record<string, EnvFile>;
  envSchedules: EnvironmentSchedule[];
  /** GET answers replaced by a save (path → body). */
  saved: Record<string, unknown>;
};

const clone = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;

function freshState(): DemoState {
  return {
    user: null,
    users: clone(fx.USERS),
    alerts: {},
    runs: [],
    active: null,
    backups: clone(fx.BACKUPS.backups),
    audit: [],
    environments: Object.fromEntries(Object.entries(fx.ENVIRONMENT_FILES).map(([k, content]) => [k, { content, updated_at: fx.ENVIRONMENT_UPDATED }])),
    envSchedules: clone(fx.ENVIRONMENT_SCHEDULES),
    saved: {},
  };
}

function load(): DemoState {
  try {
    const raw = sessionStorage.getItem(STORE_KEY);
    if (raw) return { ...freshState(), ...(JSON.parse(raw) as Partial<DemoState>) };
  } catch {
    // private mode or blocked storage: start fresh each load
  }
  return freshState();
}

let state = load();

function save() {
  try {
    sessionStorage.setItem(STORE_KEY, JSON.stringify(state));
  } catch {
    // storage unavailable: the demo still works for this page view
  }
  updateBadge();
}

export function resetDemo() {
  try {
    sessionStorage.removeItem(STORE_KEY);
  } catch {
    // ignore
  }
  state = freshState();
  window.location.hash = "#/";
  window.location.reload();
}

const nowIso = () => new Date().toISOString();

function runIdAt(t: number) {
  const d = new Date(t);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}-${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}`;
}

function audit(action: string, success = true, extra: Partial<AuditLogEntry> = {}) {
  state.audit.unshift({
    ts: nowIso(),
    action,
    success,
    user: state.user?.username,
    role: state.user?.role,
    client: "203.0.113.10",
    ...extra,
  } as AuditLogEntry);
  state.audit = state.audit.slice(0, 100);
}

// ---------------------------------------------------------- environments

const ENV_NAME = /^[a-z0-9][a-z0-9-]{0,39}$/;
const ENV_KEYS = new Set(["schema-version", "description", "runner"]);

/** What the server reports about one environment file; error when it can't be used. */
function envInfo(name: string, content: string, updated_at?: string): EnvironmentInfo {
  const path = `configs/environments/${name}.yaml`;
  let doc: Record<string, unknown>;
  try {
    doc = (parseYaml(content) ?? {}) as Record<string, unknown>;
  } catch (e) {
    return { name, path, cluster_count: 0, updated_at, error: `Not valid YAML: ${(e as Error).message.split("\n")[0]}` };
  }
  if (!doc || typeof doc !== "object" || Array.isArray(doc)) return { name, path, cluster_count: 0, updated_at, error: "The file must be a YAML mapping." };
  const extra = Object.keys(doc).filter((k) => !ENV_KEYS.has(k));
  const runner = (doc.runner ?? {}) as Record<string, unknown>;
  const targets = (runner.targets ?? {}) as Record<string, unknown>;
  const clusters = Array.isArray(targets.clusters) ? targets.clusters.map(String) : [];
  const pcs = Array.isArray(targets.pcs) ? targets.pcs.map(String) : [];
  return {
    name,
    path,
    description: typeof doc.description === "string" ? doc.description : undefined,
    mode: pcs.length && !clusters.length ? "pc" : "clusters",
    clusters,
    cluster_count: clusters.length,
    prism_centrals: pcs.length ? pcs : undefined,
    updated_at,
    ...(extra.length ? { error: `Holds server settings (${extra.join(", ")}). An environment file may only set runner options.` } : {}),
  };
}

function envList(): EnvironmentInfo[] {
  return Object.entries(state.environments)
    .map(([name, f]) => envInfo(name, f.content, f.updated_at))
    .sort((a, b) => a.name.localeCompare(b.name));
}

const DURATION = /^([1-9][0-9]*)(m|h|d)$/;

function nextScheduleRun(e: EnvironmentSchedule, from = Date.now()): string | undefined {
  if (!e.enabled) return undefined;
  const m = DURATION.exec(e.every);
  if (!m) return undefined;
  const n = Number(m[1]);
  const ms = n * (m[2] === "m" ? 60_000 : m[2] === "h" ? 3600_000 : 86400_000);
  if (m[2] === "d" && e.at) {
    const [hh, mm] = e.at.split(":").map(Number);
    const d = new Date(from);
    d.setHours(hh, mm, 0, 0);
    while (d.getTime() <= from) d.setDate(d.getDate() + 1);
    if (n > 1 && e.last_run_at) {
      const earliest = new Date(e.last_run_at).getTime() + ms - 3600_000;
      while (d.getTime() < earliest) d.setDate(d.getDate() + 1);
    }
    return d.toISOString();
  }
  const last = e.last_run_at ? new Date(e.last_run_at).getTime() : from;
  return new Date(Math.max(from + 60_000, last + ms)).toISOString();
}

function envSchedules() {
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return {
    items: state.envSchedules.map((e) => ({ ...e, next_run_at: nextScheduleRun(e) })),
    server_timezone: tz,
    utc_offset_min: -new Date().getTimezoneOffset(),
  };
}

// ------------------------------------------------------------------ runs

const ALL_CLUSTERS = fx.CLUSTER_INVENTORY.clusters;

function liveOutput(run: ActiveRun, elapsed: number): string {
  const lines = [
    `${new Date(run.startedAt).toISOString()} INFO  run ${run.id} started (${run.source ?? "manual"}, ${run.user}) for ${run.clusters.length} clusters` +
      (run.environment ? ` from the ${run.environment} environment` : ""),
  ];
  const per = (RUN_SECONDS - 5) / run.clusters.length;
  run.clusters.forEach((c, i) => {
    const startAt = 2 + i * per * 0.5;
    const doneAt = startAt + per;
    const at = (s: number) => new Date(run.startedAt + s * 1000).toISOString();
    if (elapsed >= startAt) lines.push(`${at(startAt)} INFO  ${c}: running NCC health checks`);
    if (elapsed >= doneAt) {
      lines.push(
        c === "10.40.2.21"
          ? `${at(doneAt)} ERROR ${c}: dial tcp ${c}:9440: i/o timeout`
          : `${at(doneAt)} INFO  ${c}: NCC finished`,
      );
    }
  });
  return lines.join("\n");
}

function finishRun(run: ActiveRun, status: "partial" | "cancelled") {
  const latest = fx.RUNS[0];
  const end = status === "cancelled" ? Date.now() : run.startedAt + RUN_SECONDS * 1000;
  state.runs.unshift({
    ...latest,
    id: run.id,
    path: `outputfiles/history/${run.id}`,
    mod_time: new Date(end).toISOString(),
    timestamp: new Date(run.startedAt).toISOString(),
    duration_s: Math.round((end - run.startedAt) / 1000),
    source: "trigger",
    run_source: run.source ?? "manual",
    environment: run.environment,
    clusters_ok: Math.max(0, run.clusters.length - (run.clusters.includes("10.40.2.21") ? 1 : 0)),
    clusters_failed: run.clusters.includes("10.40.2.21") ? 1 : 0,
    status,
    success: false,
    exit_code: status === "cancelled" ? 130 : 3,
    ...(status === "cancelled" ? { total_checks: 0, fail_total: 0, warn_total: 0, err_total: 0, info_total: 0 } : {}),
  });
  audit(status === "cancelled" ? "run.cancel" : "run.finish", status !== "cancelled", { path: `/api/v1/runs/${run.id}` });
  state.active = null;
}

/** Advances the simulated run; called on every request. */
function tick() {
  const run = state.active;
  if (run && Date.now() - run.startedAt >= RUN_SECONDS * 1000) {
    finishRun(run, "partial");
    save();
  }
}

function activeData(): RunActiveData {
  const run = state.active;
  if (!run) return fx.RUN_ACTIVE;
  const elapsed = (Date.now() - run.startedAt) / 1000;
  const out = liveOutput(run, elapsed);
  return {
    ...fx.RUN_ACTIVE,
    active: true,
    started_at: new Date(run.startedAt).toISOString(),
    elapsed_seconds: Math.round(elapsed),
    elapsed_human: `${Math.round(elapsed)}s`,
    expected_deadline: new Date(run.startedAt + 10 * 60_000).toISOString(),
    live_output: out,
    running_count: 1,
    runs: [
      {
        id: run.id,
        status: "running",
        started_at: new Date(run.startedAt).toISOString(),
        elapsed_sec: Math.round(elapsed),
        clusters: run.clusters,
        all_clusters: run.clusters.length === ALL_CLUSTERS.length,
        live_output: out,
        source: run.source ?? "manual",
      },
    ],
  };
}

function allRuns(): RunInfo[] {
  const running: RunInfo[] = state.active
    ? [{ id: state.active.id, mod_time: new Date(state.active.startedAt).toISOString(), has_index: false, source: "trigger", timestamp: new Date(state.active.startedAt).toISOString(), status: "running", run_source: state.active.source ?? "manual", environment: state.active.environment }]
    : [];
  return [...running, ...state.runs, ...fx.RUNS];
}

/** The newest finished run's id and time, for summaries. */
function latestSummary() {
  const r = state.runs.find((x) => x.status !== "cancelled");
  return r ? { ...fx.RUN_SUMMARY, run_id: r.id, timestamp: r.timestamp } : fx.RUN_SUMMARY;
}

// ---------------------------------------------------------------- alerts

function alertWithMark(a: NCCAlert): NCCAlert {
  const m = state.alerts[a.id];
  if (!m) return a;
  return {
    ...a,
    state: m.state,
    note: m.note ?? a.note,
    updated_at: m.at,
    last_state_change_at: m.at,
    ...(m.state === "acknowledged" ? { acknowledged_at: m.at, acknowledged_by: m.by } : {}),
    ...(m.state === "resolved" ? { resolved_at: m.at, resolved_by: m.by, closed_at: m.at } : {}),
  };
}

const alerts = () => fx.NCC_ALERTS.map(alertWithMark);

function markAlert(a: NCCAlert, action: "acknowledge" | "resolve" | "reopen", note?: string) {
  const to: NCCAlert["state"] = action === "acknowledge" ? "acknowledged" : action === "resolve" ? "resolved" : "open";
  state.alerts[a.id] = { state: to, by: state.user?.username ?? "demo", at: nowIso(), note };
  audit(`ncc_alert.${action}`, true, { path: `/api/v1/alerts/ncc/${a.id}/${action}` });
}

function dispositions(): NCCDisposition[] {
  return alerts()
    .filter((a) => a.state !== "open")
    .map((a) => ({
      cluster: a.cluster,
      check: a.check,
      status: a.state === "resolved" ? "resolved" : "acknowledged",
      by: (a.state === "resolved" ? a.resolved_by : a.acknowledged_by) ?? "alex.ops",
      at: (a.state === "resolved" ? a.resolved_at : a.acknowledged_at) ?? a.updated_at,
      note: a.note,
      id: a.id,
      finding_id: a.finding_id,
      detection: "detected",
      severity: a.severity,
      first_seen_at: a.first_seen_at,
      last_seen_at: a.last_seen_at,
    }));
}

function alertEvents(a: NCCAlert): NCCAlertEvent[] {
  const events: NCCAlertEvent[] = [
    { id: `${a.id}-1`, alert_id: a.id, correlation_key: a.correlation_key, type: "detected", at: a.created_at, actor: "runner", run_at: a.first_seen_run, to_state: "open", seq: 1 },
  ];
  const base = fx.NCC_ALERTS.find((x) => x.id === a.id);
  if (base?.state === "acknowledged") {
    events.push({ id: `${a.id}-2`, alert_id: a.id, correlation_key: a.correlation_key, type: "acknowledged", at: base.acknowledged_at ?? base.updated_at, actor: base.acknowledged_by ?? "", from_state: "open", to_state: "acknowledged", note: base.note, seq: 2 });
  }
  const m = state.alerts[a.id];
  if (m) {
    const type = m.state === "open" ? "reopened" : m.state;
    events.push({ id: `${a.id}-3`, alert_id: a.id, correlation_key: a.correlation_key, type, at: m.at, actor: m.by, to_state: m.state, note: m.note, seq: events.length + 1 });
  }
  return events;
}

// ------------------------------------------------------------------ http

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "x-request-id": "demo" },
  });
}
const ok = (data: unknown, message?: string) => json(200, { success: true, data, ...(message ? { message } : {}) });
const fail = (status: number, error: string, code = "demo") => json(status, { success: false, error, error_code: code });

function me() {
  if (!state.user) {
    return { authenticated: false, login_enabled: true, local_enabled: true, saml_enabled: false, oidc_enabled: false, ldap_enabled: false, auth_mode: "session" };
  }
  const { username, role } = state.user;
  return { ...fx.ME, username, role, is_admin: role === "admin", can_operate: role !== "viewer", expires_at: new Date(Date.now() + 8 * 3600_000).toISOString() };
}

type Handler = (url: URL) => unknown;

const gets: Record<string, Handler> = {
  "/api/v1/health": () => ({ ...fx.HEALTH, time: nowIso() }),
  "/api/v1/tls/public": () => ({ enabled: true, self_signed: true }),
  "/api/v1/auth/me": () => me(),
  "/api/v1/features": () => fx.FEATURES,
  "/api/v1/onboarding": () => fx.ONBOARDING,
  "/api/v1/components": () => fx.COMPONENTS,
  "/api/v1/runs": (u) => {
    const limit = Number(u.searchParams.get("limit") || 0);
    const runs = allRuns();
    return limit > 0 ? runs.slice(0, limit) : runs;
  },
  "/api/v1/runs/active": () => activeData(),
  "/api/v1/runs/summary": () => latestSummary(),
  "/api/v1/runs/extra-args": () => fx.EXTRA_ARGS,
  "/api/v1/runs/config-preference": () => ({ path: "config.yaml", environment: "" }),
  "/api/v1/environments": () => ({ items: envList(), main_config: "config.yaml", dir: "configs/environments" }),
  "/api/v1/environment-schedules": () => envSchedules(),
  "/api/v1/settings/environments/import": () => ({ items: fx.IMPORTABLE_CONFIGS }),
  "/api/v1/runs/configs": () => fx.CONFIG_LIST,
  "/api/v1/artifacts": () => [],
  "/api/v1/report/data": () => ({ ...fx.REPORT, run_summary: latestSummary() }),
  "/api/v1/report/trends": () => fx.TRENDS,
  "/api/v1/logs/runner": () =>
    state.active ? { ...fx.RUNNER_LOG, content: `${fx.RUNNER_LOG.content}\n${liveOutput(state.active, (Date.now() - state.active.startedAt) / 1000)}` } : fx.RUNNER_LOG,
  "/api/v1/audit": () => {
    const entries = [...state.audit, ...fx.AUDIT.entries];
    return { ...fx.AUDIT, entries, count: entries.length, mod_time: state.audit[0]?.ts ?? fx.AUDIT.mod_time };
  },
  "/api/v1/alerts": () => fx.PC_ALERTS,
  "/api/v1/alerts/ncc": (u) => {
    const includeClosed = u.searchParams.get("include_closed") === "true";
    const items = alerts().filter((a) => includeClosed || a.state !== "resolved");
    return { items, count: items.length, next_cursor: "", last_run: latestSummary().timestamp };
  },
  "/api/v1/alerts/ncc-dispositions": () => ({ items: dispositions() }),
  "/api/v1/schedule": () => fx.SCHEDULE,
  "/api/v1/schedule/health": () => fx.SCHEDULE_HEALTH,
  "/api/v1/health/diagnostics": () => fx.DIAGNOSTICS,
  "/api/v1/health/history": () => fx.HEAL_HISTORY,
  "/api/v1/health/autoheal": () => fx.AUTOHEAL,
  "/api/v1/health/supervisor": () => fx.SUPERVISOR,
  "/api/v1/health/platform": () => fx.PLATFORM,
  "/api/v1/health/migrations/config": () => ({ result: { config_path: "config.yaml", layout: "nested", changed: false, applied: false, message: "The configuration is already in the current layout." } }),
  "/api/v1/settings/config": () => ({ path: "config.yaml", content: fx.CONFIG_YAML }),
  "/api/v1/settings/configs": () => fx.CONFIG_LIST,
  "/api/v1/settings/config-files": () => ({ items: [] }),
  "/api/v1/settings/users": () => ({ users: state.users }),
  "/api/v1/settings/password-resets": () => ({ requests: [] }),
  "/api/v1/settings/cluster-groups": () => ({ groups: fx.CLUSTER_GROUPS }),
  "/api/v1/settings/clusters": () => fx.CLUSTER_INVENTORY,
  "/api/v1/settings/session": () => fx.SESSION_POLICY,
  "/api/v1/settings/notifications": () => fx.NOTIFICATIONS,
  "/api/v1/settings/backups": () => ({ ...fx.BACKUPS, backups: state.backups }),
  "/api/v1/settings/backups/schedule": () => fx.BACKUP_SCHEDULE,
  "/api/v1/settings/update": () => fx.UPDATE_STATUS,
  "/api/v1/settings/sso": () => ({ enabled: false }),
  "/api/v1/settings/oidc": () => ({ enabled: false }),
  "/api/v1/settings/ldap": () => ({ enabled: false }),
  "/api/v1/settings/tls": () => ({ enabled: true, self_signed: true }),
  "/api/v1/settings/tokens": () => ({ tokens: [] }),
  "/api/v1/users/me/pats": () => ({ tokens: [] }),
};

/** Answered before sign-in, as on a real server. */
const PUBLIC = new Set(["/api/v1/health", "/api/v1/tls/public", "/api/v1/auth/me", "/api/v1/features"]);

/** Saves whose GET answer becomes the merged body (simple settings documents). */
const MERGE_SAVES: Record<string, string> = {
  "/api/v1/features": "features.update",
  "/api/v1/onboarding": "onboarding.update",
  "/api/v1/settings/notifications": "settings.notifications.update",
  "/api/v1/settings/session": "settings.session.update",
  "/api/v1/settings/cluster-groups": "settings.cluster_groups.update",
  "/api/v1/settings/config": "settings.config.update",
  "/api/v1/runs/config-preference": "runs.config_preference",
};

function getAnswer(path: string, url: URL): unknown {
  const base = gets[path](url);
  const saved = state.saved[path];
  if (saved && base && typeof base === "object") return { ...(base as object), ...(saved as object) };
  return base;
}

async function readBody(init?: RequestInit): Promise<Record<string, unknown>> {
  if (!init?.body || typeof init.body !== "string") return {};
  try {
    return JSON.parse(init.body) as Record<string, unknown>;
  } catch {
    return {};
  }
}

async function write(method: string, path: string, url: URL, init?: RequestInit): Promise<Response> {
  const body = await readBody(init);

  if (path === "/api/v1/auth/login") {
    const username = String(body.username ?? "").trim();
    const account = state.users.find((u) => u.username === username);
    if (!account || body.password !== DEMO_PASSWORD) {
      audit("auth.login", false, { username, user: undefined, role: undefined });
      save();
      return fail(401, `Incorrect username or password. In the demo, every account's password is "${DEMO_PASSWORD}".`, "NCC_API_INVALID_CREDENTIALS");
    }
    state.user = { username: account.username, role: account.role };
    audit("auth.login");
    save();
    return ok({ username: account.username, role: account.role, expires_at: new Date(Date.now() + 8 * 3600_000).toISOString(), ttl_sec: 8 * 3600 });
  }
  if (path === "/api/v1/auth/forgot-password") {
    return ok({ admin_reset: false }, "In the demo, every account's password is \"demo\".");
  }
  if (!state.user) return fail(401, "Sign in to continue.", "NCC_API_UNAUTHORIZED");
  const role = state.user.role;

  if (path === "/api/v1/auth/logout" || path === "/api/v1/auth/logout-all") {
    audit("auth.logout");
    state.user = null;
    save();
    return ok({});
  }
  if (path === "/api/v1/auth/refresh") {
    return ok({ expires_at: new Date(Date.now() + 8 * 3600_000).toISOString(), ttl_sec: 8 * 3600 });
  }
  if (path === "/api/v1/auth/change-password") return ok({});

  if (role === "viewer") return fail(403, "Viewers can look but not change anything. Sign in as demo (admin) or alex.ops (operator) to try changes.", "NCC_API_FORBIDDEN");

  // ---- runs (operator+)
  const runEnv = typeof body.environment === "string" && body.environment ? body.environment : "";
  const envTargets = (): { clusters: string[]; error?: string } => {
    if (!runEnv) return { clusters: ALL_CLUSTERS };
    const f = state.environments[runEnv];
    if (!f) return { clusters: [], error: `There is no ${runEnv} environment.` };
    const info = envInfo(runEnv, f.content);
    if (info.error) return { clusters: [], error: `The ${runEnv} environment can't be used: ${info.error}` };
    if (!info.clusters?.length) return { clusters: [], error: `The ${runEnv} environment lists no clusters.` };
    return { clusters: info.clusters };
  };
  if (path === "/api/v1/runs/preflight") {
    const t = envTargets();
    if (t.error) return fail(400, t.error);
    const file = runEnv ? `configs/environments/${runEnv}.yaml` : "config.yaml";
    return ok({
      ok: true,
      failed: 0,
      warn: 1,
      config_path: file,
      environment: runEnv || undefined,
      actionableHints: [],
      checks: [
        {
          id: "config",
          status: "pass",
          title: "Config file is valid",
          message: runEnv ? `${file} on top of config.yaml lists ${t.clusters.length} clusters.` : `config.yaml lists ${t.clusters.length} clusters.`,
        },
        { id: "credentials", status: "pass", title: "Cluster credentials are set", message: "Read from the secrets provider." },
        { id: "reachability", status: "warn", title: "One cluster didn't answer", message: "10.40.2.21:9440 timed out. The run continues without it." },
        { id: "disk", status: "pass", title: "Enough disk space", message: "41 GB free for reports." },
      ],
    });
  }
  if (path === "/api/v1/runs/trigger") {
    if (state.active) return fail(409, "A run is already in progress.", "NCC_API_RUN_IN_PROGRESS");
    const t = envTargets();
    if (t.error) return fail(400, t.error);
    const asked = Array.isArray(body.clusters) ? (body.clusters as string[]) : [];
    const outside = asked.filter((c) => !t.clusters.includes(c));
    if (runEnv && outside.length) return fail(400, `${outside.slice(0, 3).join(", ")}${outside.length > 3 ? " …" : ""} not in the ${runEnv} environment.`);
    const picked = asked.length ? asked : t.clusters;
    const startedAt = Date.now();
    state.active = { id: runIdAt(startedAt), startedAt, clusters: picked, user: state.user.username, environment: runEnv || undefined };
    audit("run.start", true, { path: "/api/v1/runs/trigger" });
    save();
    return ok({
      run_id: state.active.id,
      started: true,
      started_at: new Date(startedAt).toISOString(),
      config_path: runEnv ? `configs/environments/${runEnv}.yaml` : "config.yaml",
      environment: runEnv || undefined,
      clusters: picked,
      running_count: 1,
    });
  }
  if (path === "/api/v1/runs/active" && method === "DELETE") {
    const run = state.active;
    if (!run) return fail(404, "No run is in progress.");
    finishRun(run, "cancelled");
    save();
    return ok({ run_id: run.id, cancelled: 1, poll_endpoint: "/api/v1/runs/active" });
  }

  // ---- alerts (operator+)
  if (path === "/api/v1/alerts/ncc-dispositions") {
    const action = body.action as "acknowledge" | "resolve" | "reopen";
    const items = (Array.isArray(body.items) ? body.items : [{ cluster: body.cluster, check: body.check }]) as Array<{ cluster: string; check: string; id?: string }>;
    for (const it of items) {
      const a = alerts().find((x) => (it.id ? x.id === it.id : x.cluster === it.cluster && x.check === it.check));
      if (a) markAlert(a, action, typeof body.note === "string" ? body.note : undefined);
    }
    save();
    return ok({ items: dispositions(), count: items.length });
  }
  const action = /^\/api\/v1\/alerts\/ncc\/([^/]+)\/(acknowledge|resolve|reopen)$/.exec(path);
  if (action) {
    const a = alerts().find((x) => x.id === decodeURIComponent(action[1]));
    if (!a) return fail(404, "Alert not found.");
    markAlert(a, action[2] as "acknowledge" | "resolve" | "reopen", typeof body.note === "string" ? body.note : undefined);
    save();
    return ok(alertWithMark(a));
  }

  if (path === "/api/v1/schedule") {
    if (role !== "admin" && role !== "operator") return fail(403, "Your role can't change the schedule.");
    const schedule = { ...fx.SCHEDULE, ...(state.saved[path] as object), ...body, updated_at: nowIso() };
    delete (schedule as Record<string, unknown>).apply;
    state.saved[path] = schedule;
    audit("schedule.update");
    save();
    return ok({ schedule, applied: Boolean(body.apply), output: "Installed the systemd timer ncc-orchestrator-scan.timer (demo)." });
  }

  if (path === "/api/v1/environment-schedules") {
    const items = (Array.isArray(body.items) ? body.items : []) as EnvironmentSchedule[];
    for (const e of items) {
      if (!state.environments[e.environment]) return fail(400, `There is no ${e.environment || "(unnamed)"} environment.`);
      if (!DURATION.exec(e.every)) return fail(400, `"${e.every}" is not an interval such as 4h or 1d.`);
      if (e.at && !/^([01][0-9]|2[0-3]):[0-5][0-9]$/.test(e.at)) return fail(400, `"${e.at}" is not a time such as 02:00.`);
    }
    const before = new Map(state.envSchedules.map((e) => [e.id, e]));
    state.envSchedules = items.map((e, i) => {
      const old = e.id ? before.get(e.id) : undefined;
      return {
        ...(old ?? { created_at: nowIso() }),
        id: old?.id ?? `es-${Date.now().toString(36)}-${i}`,
        environment: e.environment,
        enabled: Boolean(e.enabled),
        every: e.every,
        at: /d$/.test(e.every) ? e.at || "" : "",
      };
    });
    audit("environment_schedules.update");
    save();
    return ok(envSchedules());
  }

  // ---- admin only from here
  if (role !== "admin") return fail(403, "Only admins can change this. Sign in as demo to try it.", "NCC_API_FORBIDDEN");

  if (path === "/api/v1/settings/users" && method === "POST") {
    const username = String(body.username ?? "").trim();
    if (!username) return fail(400, "Enter a username.");
    if (state.users.some((u) => u.username === username)) return fail(409, `${username} already exists.`);
    state.users.push({ username, role: (body.role as UserRole) ?? "viewer", created_at: nowIso(), must_change_password: Boolean(body.must_change_password) });
    audit("settings.users.create", true, { path: `/api/v1/settings/users/${username}` });
    save();
    return ok({ username });
  }
  const user = /^\/api\/v1\/settings\/users\/([^/]+)$/.exec(path);
  if (user) {
    const name = decodeURIComponent(user[1]);
    const u = state.users.find((x) => x.username === name);
    if (!u) return fail(404, "User not found.");
    if (method === "DELETE") {
      if (name === state.user.username) return fail(400, "You can't delete the account you're signed in with.");
      state.users = state.users.filter((x) => x.username !== name);
      audit("settings.users.delete", true, { path });
      save();
      return ok({});
    }
    if (body.role) u.role = body.role as UserRole;
    u.updated_at = nowIso();
    audit("settings.users.update", true, { path });
    save();
    return ok(body.generate_password ? { temporary_password: DEMO_PASSWORD } : {});
  }
  if (path === "/api/v1/settings/notifications/test") {
    audit("settings.notifications.test");
    save();
    return ok({ channel: body.channel ?? "all", last_delivery: { success: true, last_attempt_at: nowIso(), last_success_at: nowIso() } }, "Sent a test message (demo: nothing actually left the browser).");
  }
  if (path === "/api/v1/settings/backups" && method === "POST") {
    const backup: BackupEntry = { name: `ncc-backup-${runIdAt(Date.now())}.tar.gz.enc`, kind: "manual", size: 1_852_000, mod_time: nowIso(), encrypted: true };
    state.backups.unshift(backup);
    audit("backup.create");
    save();
    return ok({ backup });
  }
  if (path === "/api/v1/settings/backups/schedule") {
    const schedule = { ...fx.BACKUP_SCHEDULE.schedule, ...((state.saved[path] as { schedule?: object })?.schedule ?? {}), ...body };
    state.saved[path] = { schedule };
    audit("backup.schedule.update");
    save();
    return ok({ schedule });
  }
  if (path === "/api/v1/settings/environments/import" && method === "POST") {
    const name = String(body.name ?? "").trim();
    if (!ENV_NAME.test(name)) return fail(400, "Use lowercase letters, digits and dashes, for example prod or eu-staging.");
    if (state.environments[name]) return fail(409, `The ${name} environment already exists.`);
    if (!fx.IMPORTABLE_CONFIGS.includes(String(body.path))) return fail(400, "Pick one of the listed config files.");
    const content = `schema-version: 1\ndescription: Imported from ${String(body.path)}\nrunner:\n  targets:\n    clusters:\n${fx.LEGACY_STAGING_CLUSTERS.map((c) => `      - ${c}\n`).join("")}  execution:\n    timeout: 15m\n`;
    state.environments[name] = { content, updated_at: nowIso() };
    audit("settings.environments.import", true, { path: `/api/v1/settings/environments/${name}` });
    save();
    return ok({ info: envInfo(name, content, nowIso()), dropped: ["api-server", "notifications", "storage"] });
  }
  const envPath = /^\/api\/v1\/settings\/environments\/([^/]+)$/.exec(path);
  if (envPath) {
    const name = decodeURIComponent(envPath[1]);
    if (method === "DELETE") {
      if (!state.environments[name]) return fail(404, `There is no ${name} environment.`);
      delete state.environments[name];
      state.envSchedules = state.envSchedules.filter((e) => e.environment !== name);
      audit("settings.environments.delete", true, { path });
      save();
      return ok({ name });
    }
    if (!ENV_NAME.test(name)) return fail(400, "Use lowercase letters, digits and dashes, for example prod or eu-staging.");
    if (body.create && state.environments[name]) return fail(409, `The ${name} environment already exists.`);
    if (!body.create && !state.environments[name]) return fail(404, `There is no ${name} environment.`);
    const content = String(body.content ?? "");
    const info = envInfo(name, content, nowIso());
    if (info.error) return fail(400, info.error, "NCC_API_CONFIG_INVALID");
    state.environments[name] = { content, updated_at: info.updated_at as string };
    audit(body.create ? "settings.environments.create" : "settings.environments.update", true, { path });
    save();
    return ok({ info });
  }
  if (MERGE_SAVES[path]) {
    const merged = { ...(getAnswer(path, url) as object), ...body, updated_at: nowIso() };
    state.saved[path] = merged;
    audit(MERGE_SAVES[path]);
    save();
    return ok(merged);
  }
  return fail(403, NOT_IN_DEMO, "demo_not_available");
}

async function route(method: string, url: URL, init?: RequestInit): Promise<Response> {
  tick();
  const path = url.pathname.replace(/\/+$/, "");
  if (method !== "GET" && method !== "HEAD") return write(method, path, url, init);

  if (!state.user && !PUBLIC.has(path)) return fail(401, "Sign in to continue.", "NCC_API_UNAUTHORIZED");
  if (gets[path]) return ok(getAnswer(path, url));
  if (!state.user) return fail(401, "Sign in to continue.", "NCC_API_UNAUTHORIZED");

  const run = /^\/api\/v1\/runs\/([^/]+)$/.exec(path);
  if (run) {
    const id = decodeURIComponent(run[1]);
    const mine = allRuns().find((r) => r.id === id && (r.source === "trigger"));
    if (mine) {
      const latest = fx.runById(fx.RUNS[0].id);
      return ok({ run: mine, artifacts: mine.status === "partial" ? latest?.artifacts ?? {} : {} });
    }
    const data = fx.runById(id);
    return data ? ok(data) : fail(404, "Run not found.");
  }
  const env = /^\/api\/v1\/settings\/environments\/([^/]+)$/.exec(path);
  if (env) {
    if (state.user.role !== "admin") return fail(403, "Only admins can open environment files.", "NCC_API_FORBIDDEN");
    const name = decodeURIComponent(env[1]);
    const f = state.environments[name];
    if (!f) return fail(404, `There is no ${name} environment.`);
    return ok({ name, path: `configs/environments/${name}.yaml`, content: f.content, info: envInfo(name, f.content, f.updated_at) });
  }
  const alert = /^\/api\/v1\/alerts\/ncc\/([^/]+)$/.exec(path);
  if (alert) {
    const a = alerts().find((x) => x.id === decodeURIComponent(alert[1]));
    if (a) {
      return ok({ alert: a, events: alertEvents(a), instances: [{ id: a.id, state: a.state, created_at: a.created_at, closed_at: a.closed_at ?? "", close_reason: a.state === "resolved" ? "manual" : "" }] });
    }
  }
  if (import.meta.env.DEV) console.info("[demo] no fixture for", path);
  return fail(404, NOT_IN_DEMO, "demo_not_available");
}

const realFetch = window.fetch.bind(window);

window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
  const raw = input instanceof Request ? input.url : String(input);
  const url = new URL(raw, window.location.href);
  if (url.origin === window.location.origin && url.pathname.startsWith("/api/")) {
    const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
    // A short delay so loading states look like a real server, not a flash.
    await new Promise((r) => setTimeout(r, method === "GET" ? 120 : 350));
    return route(method, url, init);
  }
  return realFetch(input, init);
};

// ----------------------------------------------------------------- badge

let badge: HTMLDivElement | null = null;

/** Types into a React-controlled input so the form sees the value. */
function fill(selector: string, value: string) {
  const el = document.querySelector<HTMLInputElement>(selector);
  if (!el) return;
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
  setter?.call(el, value);
  el.dispatchEvent(new Event("input", { bubbles: true }));
}

const ACCOUNTS: Array<{ username: string; label: string }> = [
  { username: "demo", label: "Admin" },
  { username: "alex.ops", label: "Operator" },
  { username: "sam.noc", label: "Viewer" },
];

function updateBadge() {
  if (!badge) return;
  const site = new URL("../", window.location.href).href;
  const signedIn = Boolean(state.user);
  badge.classList.toggle("is-open", !signedIn);
  badge.innerHTML = signedIn
    ? `<strong>Demo</strong><span>Sample data · changes stay in this tab</span>` +
      `<button type="button" data-reset>Reset</button><a href="${site}">About</a><a href="${site}download.html">Download</a>`
    : `<div class="ncc-demo-head"><strong>Demo</strong><span>Pick an account. Password: <code>${DEMO_PASSWORD}</code></span></div>` +
      `<div class="ncc-demo-accounts">${ACCOUNTS.map(
        (a) => `<button type="button" data-fill="${a.username}"><b>${a.username}</b><small>${a.label}</small></button>`,
      ).join("")}</div>` +
      `<div class="ncc-demo-links"><a href="${site}">About</a><a href="${site}download.html">Download</a></div>`;
}

function addBadge() {
  badge = document.createElement("div");
  badge.className = "ncc-demo-badge";
  badge.setAttribute("role", "note");
  badge.addEventListener("click", (e) => {
    const t = (e.target as HTMLElement).closest("button");
    if (!t) return;
    if (t.hasAttribute("data-reset")) {
      resetDemo();
      return;
    }
    const name = t.getAttribute("data-fill");
    if (name) {
      fill('input[autocomplete="username"]', name);
      fill('input[autocomplete="current-password"]', DEMO_PASSWORD);
      document.querySelector<HTMLButtonElement>('form button[type="submit"]')?.focus();
    }
  });
  const style = document.createElement("style");
  style.textContent = `
.ncc-demo-badge{position:fixed;right:12px;bottom:12px;z-index:2147483000;display:flex;gap:10px;align-items:center;
  font:500 12px/1.3 system-ui,-apple-system,"Segoe UI",sans-serif;padding:8px 12px;border-radius:999px;
  background:rgba(15,23,42,.94);color:#e2e8f0;box-shadow:0 4px 18px rgba(0,0,0,.25);backdrop-filter:blur(6px)}
.ncc-demo-badge strong{background:#f59e0b;color:#111827;border-radius:999px;padding:2px 8px;font-weight:700}
.ncc-demo-badge a,.ncc-demo-badge button[data-reset]{color:#c4b5fd;text-decoration:none;background:none;border:0;padding:0;font:inherit;cursor:pointer}
.ncc-demo-badge a:hover,.ncc-demo-badge button[data-reset]:hover{text-decoration:underline}
.ncc-demo-badge.is-open{flex-direction:column;align-items:stretch;gap:10px;border-radius:14px;padding:12px;width:280px}
.ncc-demo-head{display:flex;gap:8px;align-items:center}
.ncc-demo-head code{background:rgba(255,255,255,.12);border-radius:4px;padding:0 4px}
.ncc-demo-accounts{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
.ncc-demo-accounts button{display:flex;flex-direction:column;align-items:flex-start;gap:2px;padding:6px 8px;border-radius:8px;
  border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.04);color:#f8fafc;font:inherit;cursor:pointer;text-align:left}
.ncc-demo-accounts button:hover{border-color:#8b5cf6;background:rgba(139,92,246,.18)}
.ncc-demo-accounts small{color:#94a3b8;font-size:11px}
.ncc-demo-links{display:flex;gap:12px}
@media (max-width:600px){.ncc-demo-badge:not(.is-open) span{display:none}}`;
  document.head.appendChild(style);
  document.body.appendChild(badge);
  updateBadge();
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", addBadge);
else addBadge();
