// Read-only demo backend. Imported first by the demo build only
// (frontend/vite.demo.config.ts); the normal build never includes it.
// It replaces window.fetch for /api/* with answers from ./fixtures, refuses
// every change with a friendly message, and adds a "Demo" badge.
import * as fx from "./fixtures";

const READ_ONLY =
  "This is a read-only demo with sample data. Download NCC Orchestrator to run checks on your own clusters.";

type Handler = (url: URL) => unknown;

const exact: Record<string, Handler> = {
  "/api/v1/health": () => ({ ...fx.HEALTH, time: new Date().toISOString() }),
  "/api/v1/tls/public": () => ({ enabled: true, self_signed: true }),
  "/api/v1/auth/me": () => fx.ME,
  "/api/v1/features": () => fx.FEATURES,
  "/api/v1/onboarding": () => fx.ONBOARDING,
  "/api/v1/components": () => fx.COMPONENTS,
  "/api/v1/runs": (u) => {
    const limit = Number(u.searchParams.get("limit") || 0);
    return limit > 0 ? fx.RUNS.slice(0, limit) : fx.RUNS;
  },
  "/api/v1/runs/active": () => fx.RUN_ACTIVE,
  "/api/v1/runs/summary": () => fx.RUN_SUMMARY,
  "/api/v1/runs/extra-args": () => fx.EXTRA_ARGS,
  "/api/v1/runs/config-preference": () => ({ path: "config.yaml" }),
  "/api/v1/runs/configs": () => fx.CONFIG_LIST,
  "/api/v1/artifacts": () => [],
  "/api/v1/report/data": () => fx.REPORT,
  "/api/v1/report/trends": () => fx.TRENDS,
  "/api/v1/logs/runner": () => fx.RUNNER_LOG,
  "/api/v1/audit": () => fx.AUDIT,
  "/api/v1/alerts": () => fx.PC_ALERTS,
  "/api/v1/alerts/ncc": () => ({ items: fx.NCC_ALERTS, count: fx.NCC_ALERTS.length, next_cursor: "", last_run: fx.RUN_SUMMARY.timestamp }),
  "/api/v1/alerts/ncc-dispositions": () => ({ items: fx.NCC_DISPOSITIONS }),
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
  "/api/v1/settings/users": () => ({ users: fx.USERS }),
  "/api/v1/settings/password-resets": () => ({ requests: [] }),
  "/api/v1/settings/cluster-groups": () => ({ groups: fx.CLUSTER_GROUPS }),
  "/api/v1/settings/clusters": () => fx.CLUSTER_INVENTORY,
  "/api/v1/settings/session": () => fx.SESSION_POLICY,
  "/api/v1/settings/notifications": () => fx.NOTIFICATIONS,
  "/api/v1/settings/backups": () => fx.BACKUPS,
  "/api/v1/settings/backups/schedule": () => fx.BACKUP_SCHEDULE,
  "/api/v1/settings/update": () => fx.UPDATE_STATUS,
  "/api/v1/settings/sso": () => ({ enabled: false }),
  "/api/v1/settings/oidc": () => ({ enabled: false }),
  "/api/v1/settings/ldap": () => ({ enabled: false }),
  "/api/v1/settings/tls": () => ({ enabled: true, self_signed: true }),
  "/api/v1/settings/tokens": () => ({ tokens: [] }),
  "/api/v1/users/me/pats": () => ({ tokens: [] }),
};

/** Requests that change nothing on a real server and may succeed here. */
const harmlessWrites = new Set(["/api/v1/auth/refresh"]);

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "x-request-id": "demo" },
  });
}

function route(method: string, url: URL): Response {
  const path = url.pathname.replace(/\/+$/, "");
  if (method !== "GET" && method !== "HEAD") {
    if (harmlessWrites.has(path)) {
      return json(200, { success: true, data: { expires_at: fx.ME.expires_at, ttl_sec: fx.ME.session_ttl_sec } });
    }
    return json(403, { success: false, error: READ_ONLY, error_code: "demo_read_only" });
  }
  const handler = exact[path];
  if (handler) return json(200, { success: true, data: handler(url) });

  const run = /^\/api\/v1\/runs\/([^/]+)$/.exec(path);
  if (run) {
    const data = fx.runById(decodeURIComponent(run[1]));
    return data ? json(200, { success: true, data }) : json(404, { success: false, error: "run not found" });
  }
  const alert = /^\/api\/v1\/alerts\/ncc\/([^/]+)$/.exec(path);
  if (alert) {
    const a = fx.NCC_ALERTS.find((x) => x.id === decodeURIComponent(alert[1]));
    if (a) {
      const events = [
        { id: `${a.id}-1`, alert_id: a.id, correlation_key: a.correlation_key, type: "detected", at: a.created_at, actor: "runner", run_at: a.first_seen_run, to_state: "open", seq: 1 },
        ...(a.state === "acknowledged"
          ? [{ id: `${a.id}-2`, alert_id: a.id, correlation_key: a.correlation_key, type: "acknowledged", at: a.acknowledged_at ?? a.updated_at, actor: a.acknowledged_by ?? "", from_state: "open", to_state: "acknowledged", note: a.note, seq: 2 }]
          : []),
      ];
      return json(200, { success: true, data: { alert: a, events, instances: [{ id: a.id, state: a.state, created_at: a.created_at, closed_at: "", close_reason: "" }] } });
    }
  }
  if (import.meta.env.DEV) console.info("[demo] no fixture for", path);
  return json(404, { success: false, error: "Not available in the demo.", error_code: "demo_not_available" });
}

const realFetch = window.fetch.bind(window);

window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
  const raw = input instanceof Request ? input.url : String(input);
  const url = new URL(raw, window.location.href);
  if (url.origin === window.location.origin && url.pathname.startsWith("/api/")) {
    const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
    // A short delay so loading states look like a real server, not a flash.
    await new Promise((r) => setTimeout(r, 120));
    return route(method, url);
  }
  return realFetch(input, init);
};

function addBadge() {
  const site = new URL("../", window.location.href).href;
  const badge = document.createElement("div");
  badge.className = "ncc-demo-badge";
  badge.setAttribute("role", "note");
  badge.innerHTML =
    `<strong>Demo</strong><span>Sample data · read-only</span>` +
    `<a href="${site}">About</a><a href="${site}download.html">Download</a>`;
  const style = document.createElement("style");
  style.textContent = `
.ncc-demo-badge{position:fixed;right:12px;bottom:12px;z-index:2147483000;display:flex;gap:10px;align-items:center;
  font:500 12px/1.2 system-ui,-apple-system,"Segoe UI",sans-serif;padding:8px 12px;border-radius:999px;
  background:rgba(15,23,42,.92);color:#e2e8f0;box-shadow:0 4px 18px rgba(0,0,0,.25);backdrop-filter:blur(6px)}
.ncc-demo-badge strong{background:#f59e0b;color:#111827;border-radius:999px;padding:2px 8px;font-weight:700}
.ncc-demo-badge a{color:#7dd3fc;text-decoration:none}.ncc-demo-badge a:hover{text-decoration:underline}
@media (max-width:600px){.ncc-demo-badge span{display:none}}`;
  document.head.appendChild(style);
  document.body.appendChild(badge);
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", addBadge);
else addBadge();
