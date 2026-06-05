import { lazy, Suspense, useEffect, useRef, useState } from "react";
import {
  BgColorsOutlined,
  BarChartOutlined,
  DashboardOutlined,
  LockOutlined,
  LogoutOutlined,
  PlayCircleOutlined,
  SettingOutlined,
  ThunderboltOutlined,
  UserOutlined,
  WifiOutlined,
} from "@ant-design/icons";
import { Button, Dropdown, Layout, Spin, Tag, Tooltip } from "antd";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, Navigate, NavLink, Route, Routes, useLocation } from "react-router-dom";
import { THEME_OPTIONS, useAppTheme, type AppThemeSelection } from "./theme";
import { api, ApiError } from "./api/client";
import type { MeData, RunActiveData, RunConflictData, UserRole } from "./api/types";
import { notify, notifyError } from "./notify";
import { AuthContext, useAuth, type AuthValue } from "./auth/AuthContext";
import { LoginPage } from "./pages/LoginPage";
import { ChangePasswordPage, ChangePasswordModal } from "./pages/ChangePasswordPage";
import { SessionIdleGuard } from "./components/SessionIdleGuard";

const { Header, Content } = Layout;
const DashboardPage = lazy(() => import("./pages/DashboardPage").then((m) => ({ default: m.DashboardPage })));
const SettingsPage = lazy(() => import("./pages/SettingsPage").then((m) => ({ default: m.SettingsPage })));
const InsightsPage = lazy(() => import("./pages/InsightsPage").then((m) => ({ default: m.InsightsPage })));
const NotFoundPage = lazy(() => import("./pages/NotFoundPage").then((m) => ({ default: m.NotFoundPage })));

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
  const { canOperate } = useAuth();
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

  // Viewers (read-only role) cannot trigger runs, so hide the button entirely.
  if (!canOperate) return null;

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

/** Header control showing the signed-in user, their role, and a logout action.
 *  Rendered only when interactive login is enabled. */
function HeaderUserMenu({
  username,
  role,
  canChangePassword,
  onChangePassword,
  onLogout,
  onLogoutEverywhere,
}: {
  username: string;
  role: UserRole;
  canChangePassword: boolean;
  onChangePassword: () => void;
  onLogout: () => void;
  onLogoutEverywhere: () => void;
}) {
  const roleColor = role === "admin" ? "gold" : role === "operator" ? "blue" : "default";
  const items = [
    {
      key: "who",
      label: (
        <span>
          Signed in as <strong>{username || "user"}</strong>
        </span>
      ),
      disabled: true,
    },
    { type: "divider" as const },
    ...(canChangePassword
      ? [{ key: "change-password", icon: <LockOutlined />, label: "Change password" }]
      : []),
    { key: "logout", icon: <LogoutOutlined />, label: "Sign out" },
    ...(canChangePassword
      ? [{ key: "logout-all", icon: <LogoutOutlined />, label: "Sign out everywhere", danger: true }]
      : []),
  ];
  return (
    <Dropdown
      placement="bottomRight"
      trigger={["click"]}
      menu={{
        items,
        onClick: ({ key }) => {
          if (key === "logout") onLogout();
          else if (key === "logout-all") onLogoutEverywhere();
          else if (key === "change-password") onChangePassword();
        },
      }}
    >
      <Button className="header-user-btn" icon={<UserOutlined />}>
        <span className="header-user-name">{username || "user"}</span>
        <Tag color={roleColor} className="header-user-role">
          {role || "user"}
        </Tag>
      </Button>
    </Dropdown>
  );
}

export default function App() {
  const location = useLocation();
  const queryClient = useQueryClient();
  const { selectedTheme, setTheme } = useAppTheme();
  const themeLabel = THEME_OPTIONS.find((opt) => opt.value === selectedTheme)?.label ?? "Theme";

  const meQuery = useQuery({
    queryKey: ["me"],
    queryFn: api.me,
    staleTime: 30_000,
    // Keep the session state fresh so the app maintains the logged-in view and
    // flips to the login screen promptly once the session expires. When a
    // session expiry is known, re-check shortly after it lapses (clamped so we
    // never poll faster than every 15s or slower than every 5 min).
    refetchInterval: (q) => {
      const data = q.state.data as MeData | undefined;
      if (!data?.login_enabled) return 5 * 60_000;
      const left = data.expires_in_sec;
      if (typeof left === "number") {
        return Math.min(5 * 60_000, Math.max(15_000, (left + 2) * 1000));
      }
      return 60_000;
    },
    refetchOnWindowFocus: true,
  });
  const me = meQuery.data as MeData | undefined;
  const loginEnabled = Boolean(me?.login_enabled);
  const authenticated = Boolean(me?.authenticated);
  const role: UserRole = (me?.role as UserRole) ?? "";
  // When login is disabled (single-user/token deployments) every visitor has
  // full access, matching the pre-RBAC behavior.
  const isAdmin = loginEnabled ? Boolean(me?.is_admin) : true;
  const canOperate = loginEnabled ? Boolean(me?.can_operate) : true;
  const authValue: AuthValue = { me: me ?? null, role, isAdmin, canOperate, loginEnabled, authenticated };
  const [changePwOpen, setChangePwOpen] = useState(false);

  const handleLogout = async () => {
    try {
      await api.logout();
    } catch {
      // ignore; we clear client state regardless (cookies may already be gone)
    }
    // Drop every cached (authenticated) query so no stale data lingers, then do
    // a clean re-bootstrap. A hard navigation guarantees the UI returns to the
    // login screen regardless of any in-flight query or cached auth state.
    queryClient.clear();
    window.location.assign("/");
  };

  const handleLogoutEverywhere = async () => {
    try {
      await api.logoutAll();
      notify.success("Signed out of all sessions on every device.");
    } catch (e) {
      notifyError(e, "Could not sign out other sessions");
    }
    queryClient.clear();
    window.location.assign("/");
  };

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

  // Don't fire authenticated data queries until we know the viewer may access
  // them: either login is disabled (single-user/token deployments) or the
  // session is authenticated. Otherwise polling endpoints like /runs/active
  // 401 on every tick before login and flood the browser console.
  const appReady = !meQuery.isLoading && (authenticated || !loginEnabled);

  const runActiveQuery = useQuery({
    queryKey: ["runs-active"],
    queryFn: api.runActive,
    enabled: appReady,
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

  // Login gate: when the backend requires login and this browser has no
  // authenticated session, show the full-screen login instead of the app.
  if (loginEnabled && !authenticated && !meQuery.isLoading) {
    return (
      <LoginPage
        localEnabled={Boolean(me?.local_enabled)}
        samlEnabled={Boolean(me?.saml_enabled)}
        bootstrapPending={Boolean(me?.bootstrap_pending)}
        onSuccess={() => {
          void queryClient.invalidateQueries();
          void meQuery.refetch();
        }}
        onBootstrapReset={() => void meQuery.refetch()}
      />
    );
  }

  // Forced password-change gate: the bootstrap admin (and admin-reset accounts)
  // must set a new password before doing anything else.
  if (loginEnabled && authenticated && me?.must_change_password) {
    return (
      <ChangePasswordPage
        forced
        username={me?.username}
        onSuccess={() => {
          void queryClient.invalidateQueries();
          void meQuery.refetch();
        }}
      />
    );
  }

  // Settings is admin-only; hide its nav pill from non-admins.
  const navItems = NAV_ITEMS.filter((item) => item.to !== "/settings" || isAdmin);

  return (
    <AuthContext.Provider value={authValue}>
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
            {navItems.map((item) => (
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
            {loginEnabled && authenticated ? (
              <HeaderUserMenu
                username={me?.username ?? ""}
                role={role}
                canChangePassword={Boolean(me?.is_local)}
                onChangePassword={() => setChangePwOpen(true)}
                onLogout={handleLogout}
                onLogoutEverywhere={handleLogoutEverywhere}
              />
            ) : null}
          </div>
        </div>
      </Header>
      <Content className="app-content">
        <div className="app-content-inner">
          <Suspense fallback={<Spin size="large" />}>
            <Routes>
              <Route path="/" element={<DashboardPage />} />
              <Route
                path="/settings"
                element={isAdmin ? <SettingsPage /> : <Navigate to="/" replace />}
              />
              <Route path="/insights" element={<InsightsPage />} />
              <Route path="*" element={<NotFoundPage />} />
            </Routes>
          </Suspense>
        </div>
      </Content>
      <ChangePasswordModal
        open={changePwOpen}
        onClose={() => setChangePwOpen(false)}
        onSuccess={() => {
          notify.success("Password changed.");
          void meQuery.refetch();
        }}
      />
      {loginEnabled && authenticated && !me?.must_change_password ? (
        <SessionIdleGuard
          onLogout={handleLogout}
          onStayLoggedIn={() => void meQuery.refetch()}
        />
      ) : null}
    </Layout>
    </AuthContext.Provider>
  );
}
