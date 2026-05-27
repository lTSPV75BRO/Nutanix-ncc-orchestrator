import { lazy, Suspense, useEffect, useRef, useState } from "react";
import {
  BgColorsOutlined,
  BarChartOutlined,
  DashboardOutlined,
  PlayCircleOutlined,
  SettingOutlined,
  ThunderboltOutlined,
  WifiOutlined,
} from "@ant-design/icons";
import { Button, Dropdown, Layout, Spin, Tooltip } from "antd";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, Navigate, NavLink, Route, Routes, useLocation } from "react-router-dom";
import { THEME_OPTIONS, useAppTheme, type AppThemeSelection } from "./theme";
import { api, ApiError } from "./api/client";
import type { RunActiveData, RunConflictData } from "./api/types";
import { notify, notifyError } from "./notify";

const { Header, Content } = Layout;
const DashboardPage = lazy(() => import("./pages/DashboardPage").then((m) => ({ default: m.DashboardPage })));
const SettingsPage = lazy(() => import("./pages/SettingsPage").then((m) => ({ default: m.SettingsPage })));
const InsightsPage = lazy(() => import("./pages/InsightsPage").then((m) => ({ default: m.InsightsPage })));

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", icon: <DashboardOutlined /> },
  { to: "/insights", label: "Insights", icon: <BarChartOutlined /> },
  { to: "/settings", label: "Settings", icon: <SettingOutlined /> },
] as const;

const APP_BASE_TITLE = "NCC Orchestrator v2";
const RUN_TOAST_KEY = "ncc-run-active";

type HealthStatePayload = {
  state: "ok" | "warn" | "err";
  label: string;
  authMode?: string;
};

function deriveHealthState(
  isError: boolean,
  data: { status?: string; auth_mode?: string } | undefined,
): HealthStatePayload {
  if (isError) return { state: "err", label: "API offline" };
  const status = data?.status;
  if (status === "ok") return { state: "ok", label: "API healthy", authMode: data?.auth_mode };
  if (status) return { state: "warn", label: `API ${status}`, authMode: data?.auth_mode };
  return { state: "warn", label: "Connecting…" };
}

/**
 * Keep `document.title` in sync with API health and active-run state so users
 * notice issues even when the tab is in the background.
 */
function useDocumentTitleSync(health: HealthStatePayload, runActive: boolean) {
  useEffect(() => {
    let prefix = "";
    if (runActive) prefix = "● ";
    else if (health.state === "err") prefix = "⚠ ";
    document.title = `${prefix}${APP_BASE_TITLE}`;
    return () => {
      document.title = APP_BASE_TITLE;
    };
  }, [health.state, runActive]);
}

function HeaderHealthPill({ health }: { health: HealthStatePayload }) {
  return (
    <Tooltip title={health.authMode ? `Auth: ${health.authMode}` : "Hover to refresh status"}>
      <span className={`header-health-pill ${health.state}`} role="status" aria-live="polite">
        <span className="header-health-dot" />
        <WifiOutlined className="header-health-icon" />
        <span className="header-health-label">{health.label}</span>
      </span>
    </Tooltip>
  );
}

/**
 * Compact build chip for the brand block. Shows "v2.0.0" by default and a rich
 * tooltip with build date, stream, Go toolchain, and OS/arch. Useful when
 * users open support tickets — copy/pasting the tooltip captures the exact
 * binary in use.
 */
function BrandVersionTag({
  version,
  buildDate,
  stream,
  goVersion,
  os,
  arch,
}: {
  version?: string;
  buildDate?: string;
  stream?: string;
  goVersion?: string;
  os?: string;
  arch?: string;
}) {
  if (!version) {
    return <span className="brand-version">v2</span>;
  }
  const shortVersion = version.split("-")[0];
  const built = (() => {
    if (!buildDate || buildDate === "unknown") return "";
    const d = new Date(buildDate);
    if (Number.isNaN(d.getTime())) return buildDate;
    return d.toISOString().slice(0, 10);
  })();
  const tooltip = (
    <div style={{ fontSize: 12, lineHeight: 1.5 }}>
      <div><strong>{version}</strong></div>
      {built ? <div>Built {built}</div> : null}
      {stream ? <div>Stream: {stream}</div> : null}
      {goVersion ? <div>Go: {goVersion}</div> : null}
      {os || arch ? <div>{[os, arch].filter(Boolean).join("/")}</div> : null}
    </div>
  );
  return (
    <Tooltip title={tooltip}>
      <span className="brand-version" aria-label={`API server ${version}`}>
        v{shortVersion}
      </span>
    </Tooltip>
  );
}

/** Format milliseconds as "5m 23s" / "1h 12m 4s" / "42s". Mirrors the helper
 *  used by the Settings → Runs section so the header pill reads the same way. */
function formatElapsedShort(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "0s";
  const totalSec = Math.floor(ms / 1000);
  const s = totalSec % 60;
  const m = Math.floor(totalSec / 60) % 60;
  const h = Math.floor(totalSec / 3600);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function HeaderTriggerRunButton() {
  const queryClient = useQueryClient();
  const active = useQuery({
    queryKey: ["runs-active"],
    queryFn: api.runActive,
    refetchInterval: (q) => {
      const data = q.state.data as RunActiveData | undefined;
      // Poll faster while a run is active, slower when idle.
      return data?.active ? 3000 : 15000;
    },
    staleTime: 1500,
  });

  const isActive = Boolean(active.data?.active);
  const startMsRef = useRef<number | null>(null);
  const wasActiveRef = useRef<boolean | null>(null);

  // Tick once per second while active so the rendered "Running · Xs" label
  // updates smoothly even though the polled `active` data is only refreshed
  // every 3s. Mirrors RunsSection's elapsed-tick.
  const [, setElapsedTick] = useState(0);
  useEffect(() => {
    if (!isActive) return;
    const id = window.setInterval(() => setElapsedTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, [isActive]);

  // Detect transitions and emit lifecycle toasts mirroring the Runs page.
  useEffect(() => {
    if (wasActiveRef.current === null) {
      // First poll: if a run was already in progress when the page loaded
      // (e.g. user refreshed mid-run), seed startMsRef from the API so the
      // header pill can display elapsed time without waiting for the next
      // transition.
      if (isActive && active.data?.started_at) {
        startMsRef.current = new Date(active.data.started_at).getTime();
      }
      wasActiveRef.current = isActive;
      return;
    }
    const wasActive = wasActiveRef.current;
    if (isActive && !wasActive) {
      const startedAt = active.data?.started_at;
      startMsRef.current = startedAt ? new Date(startedAt).getTime() : Date.now();
    }
    if (!isActive && wasActive) {
      const elapsedMs = startMsRef.current ? Date.now() - startMsRef.current : 0;
      startMsRef.current = null;
      const errorText = (active.data?.last_error || "").trim();
      notify.close(RUN_TOAST_KEY);
      const seconds = Math.max(1, Math.round(elapsedMs / 1000));
      if (errorText) {
        notify.error({
          message: "Run failed",
          description: `${errorText.split(/\r?\n/)[0].slice(0, 200)} · Elapsed ${seconds}s`,
          duration: 10,
        });
      } else {
        notify.success({
          message: "Run completed",
          description: `Finished in ${seconds}s. Reports will refresh shortly.`,
          duration: 5,
        });
      }
      // Invalidate downstream queries so dashboards refresh.
      void queryClient.invalidateQueries({ queryKey: ["report-data"] });
      void queryClient.invalidateQueries({ queryKey: ["runs"] });
      void queryClient.invalidateQueries({ queryKey: ["artifacts"] });
    }
    wasActiveRef.current = isActive;
  }, [isActive, active.data, queryClient]);

  const triggerNow = async () => {
    if (isActive) {
      notify.warning({
        message: "A run is already in progress",
        description: "Wait for the current run to finish before triggering another.",
      });
      return;
    }
    notify.loading({
      key: RUN_TOAST_KEY,
      message: "Starting run…",
      description: "Triggering an NCC run from the header.",
    });
    try {
      const out = await api.runTrigger({ extra_args: [] });
      notify.loading({
        key: RUN_TOAST_KEY,
        message: "Run accepted",
        description: `PID ${out.pid} · Started ${
          out.started_at ? new Date(out.started_at).toLocaleTimeString() : "just now"
        }. Live logs are visible on Settings → Runs.`,
      });
      startMsRef.current = out.started_at ? new Date(out.started_at).getTime() : Date.now();
      void active.refetch();
    } catch (e) {
      notify.close(RUN_TOAST_KEY);
      // If the backend rejected because a run is already in flight, render a
      // structured warning toast with the running run's start time, elapsed
      // duration, PID and runner log path instead of a generic error.
      if (e instanceof ApiError && e.status === 409 && e.data && typeof e.data === "object") {
        const d = e.data as Partial<RunConflictData>;
        const startedAt = d.started_at ? new Date(d.started_at).toLocaleString() : "—";
        const elapsed = d.elapsed_human || (d.elapsed_seconds != null ? `${d.elapsed_seconds}s` : "—");
        const lines: string[] = [
          `Started: ${startedAt}`,
          `Elapsed: ${elapsed}`,
        ];
        if (d.pid && d.pid > 0) lines.push(`PID: ${d.pid}`);
        if (d.overdue) lines.push("Status: exceeded expected duration");
        if (d.hint) lines.push(d.hint);
        notify.warning({
          message: "Cannot start another run — one is already in progress",
          description: lines.join(" · "),
          duration: 10,
        });
        void active.refetch();
        return;
      }
      notifyError(e, "Failed to trigger run");
    }
  };

  // While a run is active, show a spinning <PlayCircleOutlined /> + live
  // elapsed time so the header matches the "Running · 5m 23s" pill rendered
  // on Settings → Runs. When idle, show the lightning bolt + "Trigger".
  const elapsedMs = isActive && startMsRef.current ? Date.now() - startMsRef.current : 0;
  const tooltipTitle = isActive
    ? `A run is currently in progress · ${formatElapsedShort(elapsedMs)} elapsed`
    : "Trigger NCC run with the active config";

  return (
    <Tooltip title={tooltipTitle}>
      <Button
        type={isActive ? "default" : "primary"}
        icon={isActive ? <PlayCircleOutlined spin /> : <ThunderboltOutlined />}
        onClick={triggerNow}
        disabled={isActive}
        className={`header-trigger-btn${isActive ? " header-trigger-btn-running" : ""}`}
      >
        {isActive ? `Running · ${formatElapsedShort(elapsedMs)}` : "Trigger"}
      </Button>
    </Tooltip>
  );
}

export default function App() {
  const location = useLocation();
  const { selectedTheme, setTheme } = useAppTheme();
  const themeLabel = THEME_OPTIONS.find((opt) => opt.value === selectedTheme)?.label ?? "Theme";

  const healthQuery = useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    refetchInterval: 30_000,
    staleTime: 15_000,
  });
  const healthState = deriveHealthState(
    healthQuery.isError,
    healthQuery.data as { status?: string; auth_mode?: string } | undefined,
  );

  const runActiveQuery = useQuery({
    queryKey: ["runs-active"],
    queryFn: api.runActive,
    refetchInterval: 5000,
    staleTime: 1500,
  });
  const runActive = Boolean((runActiveQuery.data as RunActiveData | undefined)?.active);

  const healthRecord = healthQuery.data as
    | {
        version?: string;
        build_date?: string;
        stream?: string;
        go_version?: string;
        os?: string;
        arch?: string;
      }
    | undefined;

  useDocumentTitleSync(healthState, runActive);

  const isActive = (to: string): boolean => {
    if (to === "/") return location.pathname === "/" || location.pathname === "";
    return location.pathname.startsWith(to);
  };

  return (
    <Layout className="app-shell">
      <Header className="app-header">
        <div className="app-header-inner">
          <Link to="/" className="brand-block" aria-label="NCC Orchestrator home">
            <span className="brand-logo-wrap">
              <img src="/logo.svg" alt="" className="brand-logo" />
            </span>
            <span className="brand-text">
              <span className="brand-name">NCC Orchestrator</span>
              <span className="brand-tagline">
                <BrandVersionTag
                  version={healthRecord?.version}
                  buildDate={healthRecord?.build_date}
                  stream={healthRecord?.stream}
                  goVersion={healthRecord?.go_version}
                  os={healthRecord?.os}
                  arch={healthRecord?.arch}
                />
                <span className="brand-tagline-text">Cluster Healthcheck Platform</span>
              </span>
            </span>
          </Link>

          <nav className="header-nav" aria-label="Primary">
            {NAV_ITEMS.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={`nav-pill${isActive(item.to) ? " active" : ""}`}
                end={item.to === "/"}
              >
                <span className="nav-pill-icon">{item.icon}</span>
                <span className="nav-pill-label">{item.label}</span>
              </NavLink>
            ))}
          </nav>

          <div className="header-actions">
            <HeaderTriggerRunButton />
            <HeaderHealthPill health={healthState} />
            <Dropdown
              placement="bottomRight"
              trigger={["click"]}
              menu={{
                selectedKeys: [selectedTheme],
                items: THEME_OPTIONS.map((opt) => ({ key: opt.value, label: opt.label })),
                onClick: ({ key }) => setTheme(key as AppThemeSelection),
              }}
            >
              <Button
                aria-label="Theme menu"
                title={`Theme: ${themeLabel}`}
                icon={<BgColorsOutlined />}
                className="header-icon-btn"
              />
            </Dropdown>
          </div>
        </div>
      </Header>
      <Content className="app-content">
        <div className="app-content-inner">
          <Suspense fallback={<Spin size="large" />}>
            <Routes>
              <Route path="/" element={<DashboardPage />} />
              <Route path="/settings" element={<SettingsPage />} />
              <Route path="/insights" element={<InsightsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </Suspense>
        </div>
      </Content>
    </Layout>
  );
}
