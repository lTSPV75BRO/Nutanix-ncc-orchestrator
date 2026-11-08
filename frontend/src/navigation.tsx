// Single source of truth for where things live in the app: the sidebar, the
// Settings/System section routes, legacy /settings?tab= redirects, and every
// in-app link to a section all read from here.
import type { ReactNode } from "react";
import {
  AlertOutlined,
  ApiOutlined,
  AuditOutlined,
  BarChartOutlined,
  BellOutlined,
  CalendarOutlined,
  CloudSyncOutlined,
  CodeOutlined,
  ControlOutlined,
  DashboardOutlined,
  FileTextOutlined,
  HeartOutlined,
  SettingOutlined,
  TeamOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";

/** Keys of the sections SettingsPage can render (the former Settings tabs). */
export type SectionKey =
  | "connection"
  | "config"
  | "schedule"
  | "runs"
  | "logs"
  | "audit"
  | "notifications"
  | "health"
  | "updates"
  | "features"
  | "access"
  | "developer";

/** viewer: anyone signed in (read-only for viewers); operator; admin. */
export type SectionAccess = "viewer" | "operator" | "admin";

export type SectionDef = {
  key: SectionKey;
  path: string;
  label: string;
  /** Page title shown in the top bar. */
  title: string;
  /** One line under the title saying what the section is for. */
  description?: string;
  icon: ReactNode;
  group: "main" | "settings" | "system";
  /** Lowest role that may open it (viewers see none of the sections). */
  access: SectionAccess;
  /** Only exists when interactive sign-in is enabled. */
  needsLogin?: boolean;
};

export const SECTIONS: SectionDef[] = [
  // Viewers may read run history (GET /api/v1/runs); starting/cancelling stays operator-only.
  { key: "runs", path: "/runs", label: "Runs", title: "Runs", icon: <ThunderboltOutlined />, group: "main", access: "viewer" },
  { key: "config", path: "/settings/config", label: "Clusters & config", title: "Clusters & config", description: "Which clusters to check, how to sign in to them, and how runs behave.", icon: <SettingOutlined />, group: "settings", access: "admin" },
  { key: "schedule", path: "/settings/schedule", label: "Schedule", title: "Schedule", description: "Run NCC automatically on a timetable.", icon: <CalendarOutlined />, group: "settings", access: "operator" },
  { key: "notifications", path: "/settings/notifications", label: "Notifications", title: "Notifications", description: "Who hears about runs and failures, and how.", icon: <BellOutlined />, group: "settings", access: "admin" },
  { key: "access", path: "/settings/access", label: "Access", title: "Access", description: "Accounts, sign-in methods, cluster groups, API tokens and HTTPS.", icon: <TeamOutlined />, group: "settings", access: "admin", needsLogin: true },
  { key: "features", path: "/settings/features", label: "Features", title: "Features", description: "Turn optional parts of the app on or off for everyone.", icon: <ControlOutlined />, group: "settings", access: "admin" },
  { key: "connection", path: "/system/status", label: "Status", title: "System status", description: "Whether the API server is up, what it is running and on what, and what the supervisor is doing.", icon: <ApiOutlined />, group: "system", access: "operator" },
  { key: "health", path: "/system/health", label: "Health & repair", title: "Health & repair", description: "Checks of this installation, what heals automatically, and the history of repairs.", icon: <HeartOutlined />, group: "system", access: "admin" },
  { key: "updates", path: "/system/updates", label: "Updates & backups", title: "Updates & backups", description: "Update the software (checked, with automatic rollback), back up and restore, and migrate the config.", icon: <CloudSyncOutlined />, group: "system", access: "admin" },
  { key: "logs", path: "/system/logs", label: "Logs", title: "Logs", description: "Live output from the runner and the servers.", icon: <FileTextOutlined />, group: "system", access: "operator" },
  { key: "audit", path: "/system/audit", label: "Audit log", title: "Audit log", description: "Who did what, and when.", icon: <AuditOutlined />, group: "system", access: "operator" },
  { key: "developer", path: "/system/developer", label: "Developer", title: "Developer tools", description: "The API explorer, saved reports and raw run files.", icon: <CodeOutlined />, group: "system", access: "admin" },
];

/** Where signed-in people land; `/` redirects here. */
export const OVERVIEW_PATH = "/overview";

/** Top-level pages that are not Settings sections. */
export const PAGES = [
  { path: OVERVIEW_PATH, label: "Overview", title: "Overview", icon: <DashboardOutlined /> },
  { path: "/findings", label: "Findings", title: "Findings", icon: <AlertOutlined /> },
  { path: "/insights", label: "Insights", title: "Insights", icon: <BarChartOutlined /> },
] as const;

/** Query keys of a shared findings view (the former Dashboard's link format). */
export const FINDINGS_SHARE_KEYS = ["q", "sev", "clusters", "mode", "source", "resolved"] as const;

/**
 * The Dashboard used to live at `/` and shared links carried its filters
 * (`/?sev=FAIL&source=ncc`). Those now belong to Findings.
 */
export function findingsRedirect(search: string): string | null {
  const params = new URLSearchParams(search);
  return FINDINGS_SHARE_KEYS.some((k) => params.has(k)) ? `/findings?${params.toString()}` : null;
}

const BY_KEY = new Map(SECTIONS.map((s) => [s.key, s]));

/**
 * Sections that were merged into another one, and the tab they open there.
 * Maintenance (updates, backups) is part of Updates & backups.
 */
const SECTION_ALIASES: Record<string, { key: SectionKey; tab: string; path: string }> = {
  maintenance: { key: "updates", tab: "updates", path: "/system/maintenance" },
};

/**
 * Tabs that moved to another section: Health & repair used to hold updates,
 * backups and migrations (now Updates & backups) and the supervisor and
 * platform (now Status).
 */
const MOVED_TABS: Partial<Record<SectionKey, Record<string, SectionKey>>> = {
  health: { updates: "updates", backups: "updates", migrations: "updates", supervisor: "connection", platform: "connection" },
};

/** The section key and extra query an old key or tab such as `maintenance` maps to. */
function resolveAlias(key: string, params: Record<string, string | undefined> = {}): [string, Record<string, string | undefined>] {
  const alias = SECTION_ALIASES[key];
  if (alias) {
    const { focus, ...rest } = params;
    return [alias.key, { tab: focus === "backup" ? "backups" : alias.tab, ...rest }];
  }
  const moved = MOVED_TABS[key as SectionKey];
  if (moved) {
    const { focus, ...rest } = params;
    if (focus === "backup") return ["updates", { ...rest, tab: "backups" }];
    const to = params.tab ? moved[params.tab] : undefined;
    if (to) return [to, params];
  }
  return [key, params];
}

export function sectionByKey(key: string): SectionDef | undefined {
  return BY_KEY.get(resolveAlias(key)[0] as SectionKey);
}

/**
 * Where an old section URL (e.g. /system/maintenance?focus=backup, or
 * /system/health?tab=backups) now lives.
 */
export function aliasRedirect(pathname: string, search: string): string | null {
  const p = pathname.replace(/\/+$/, "");
  const params = Object.fromEntries(new URLSearchParams(search));
  const entry = Object.entries(SECTION_ALIASES).find(([, a]) => a.path === p);
  if (entry) return sectionHref(entry[0], params);
  const s = sectionByPath(p);
  if (s && MOVED_TABS[s.key] && resolveAlias(s.key, params)[0] !== s.key) return sectionHref(s.key, params);
  return null;
}

export function sectionByPath(pathname: string): SectionDef | undefined {
  const p = pathname.replace(/\/+$/, "");
  return SECTIONS.find((s) => s.path === p);
}

export type Viewer = { isAdmin: boolean; canOperate: boolean; loginEnabled: boolean };

export function canOpenSection(s: SectionDef, v: Viewer): boolean {
  if (s.needsLogin && !v.loginEnabled) return false;
  if (s.access === "viewer") return true;
  return s.access === "admin" ? v.isAdmin : v.isAdmin || v.canOperate;
}

export function visibleSections(v: Viewer): SectionDef[] {
  return SECTIONS.filter((s) => canOpenSection(s, v));
}

/**
 * Link to a section, e.g. `sectionHref("health", { tab: "backups" })`.
 * Unknown keys fall back to the Settings landing so a stale key never 404s.
 */
export function sectionHref(key: string, params?: Record<string, string | undefined>): string {
  const [resolved, query] = resolveAlias(key, params);
  const base = sectionByKey(resolved)?.path ?? "/settings";
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v) qs.set(k, v);
  }
  const s = qs.toString();
  return s ? `${base}?${s}` : base;
}

/** First section a viewer lands on from `/settings` with no section. */
export function settingsLanding(v: Viewer): string {
  const order: SectionKey[] = ["config", "schedule", "connection"];
  for (const key of order) {
    const s = sectionByKey(key);
    if (s && canOpenSection(s, v)) return s.path;
  }
  return OVERVIEW_PATH;
}

/**
 * Where an old `/settings?tab=X&…` URL now lives. Other query parameters
 * (focus, dev, action, …) are kept.
 */
export function legacySettingsTarget(search: string, v: Viewer): string {
  const params = new URLSearchParams(search);
  const tab = (params.get("tab") || "").trim();
  params.delete("tab");
  const s = sectionByKey(tab);
  if (s && canOpenSection(s, v) && (SECTION_ALIASES[tab] || MOVED_TABS[s.key])) {
    return sectionHref(tab, Object.fromEntries(params));
  }
  const base = s && canOpenSection(s, v) ? s.path : settingsLanding(v);
  const rest = params.toString();
  return rest ? `${base}?${rest}` : base;
}

/** Title for the top bar. */
/** The navigation icon of the page at `pathname`, for its title. */
export function pageIcon(pathname: string): ReactNode {
  if (/^\/runs\/[^/]+/.test(pathname)) return SECTIONS.find((s) => s.key === "runs")?.icon ?? null;
  const s = sectionByPath(pathname);
  if (s) return s.icon;
  return PAGES.find((pg) => pg.path === (pathname.replace(/\/+$/, "") || "/"))?.icon ?? null;
}

export function pageTitle(pathname: string): string {
  if (/^\/runs\/[^/]+/.test(pathname)) return "Run details";
  const s = sectionByPath(pathname);
  if (s) return s.title;
  const p = PAGES.find((pg) => pg.path === (pathname.replace(/\/+$/, "") || "/"));
  return p?.title ?? "";
}
