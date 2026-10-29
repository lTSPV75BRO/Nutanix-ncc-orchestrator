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
  CodeOutlined,
  ControlOutlined,
  DashboardOutlined,
  FileTextOutlined,
  HeartOutlined,
  SettingOutlined,
  TeamOutlined,
  ThunderboltOutlined,
  ToolOutlined,
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
  | "features"
  | "access"
  | "maintenance"
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
  { key: "connection", path: "/system/status", label: "Status", title: "System status", description: "Whether the API server is up, what it is running, and where it keeps its files.", icon: <ApiOutlined />, group: "system", access: "operator" },
  { key: "health", path: "/system/health", label: "Health", title: "System health", description: "Self-checks for this installation, with safe fixes you can apply.", icon: <HeartOutlined />, group: "system", access: "admin" },
  { key: "logs", path: "/system/logs", label: "Logs", title: "Logs", description: "Live output from the runner and the servers.", icon: <FileTextOutlined />, group: "system", access: "operator" },
  { key: "audit", path: "/system/audit", label: "Audit log", title: "Audit log", description: "Who did what, and when.", icon: <AuditOutlined />, group: "system", access: "operator" },
  { key: "maintenance", path: "/system/maintenance", label: "Maintenance", title: "Maintenance", description: "Updates, backups and restore.", icon: <ToolOutlined />, group: "system", access: "admin" },
  { key: "developer", path: "/system/developer", label: "Developer", title: "Developer tools", description: "The API explorer, saved reports and raw run files.", icon: <CodeOutlined />, group: "system", access: "admin" },
];

/** Top-level pages that are not Settings sections. */
export const PAGES = [
  { path: "/", label: "Overview", title: "Overview", icon: <DashboardOutlined /> },
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

export function sectionByKey(key: string): SectionDef | undefined {
  return BY_KEY.get(key as SectionKey);
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
 * Link to a section, e.g. `sectionHref("maintenance", { focus: "backup" })`.
 * Unknown keys fall back to the Settings landing so a stale key never 404s.
 */
export function sectionHref(key: string, params?: Record<string, string | undefined>): string {
  const base = sectionByKey(key)?.path ?? "/settings";
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params ?? {})) {
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
  return "/";
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
  const base = s && canOpenSection(s, v) ? s.path : settingsLanding(v);
  const rest = params.toString();
  return rest ? `${base}?${rest}` : base;
}

/** Title for the top bar. */
export function pageTitle(pathname: string): string {
  if (/^\/runs\/[^/]+/.test(pathname)) return "Run details";
  const s = sectionByPath(pathname);
  if (s) return s.title;
  const p = PAGES.find((pg) => pg.path === (pathname.replace(/\/+$/, "") || "/"));
  return p?.title ?? "";
}
