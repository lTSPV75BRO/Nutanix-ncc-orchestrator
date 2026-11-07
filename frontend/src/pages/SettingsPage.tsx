import { lazy, Suspense, useEffect } from "react";
import type { ReactNode } from "react";
import { Navigate, useSearchParams } from "react-router";
import { Button, Space, Tabs, Tooltip } from "antd";
import {
  ApiOutlined,
  AuditOutlined,
  BellOutlined,
  CalendarOutlined,
  ControlOutlined,
  FileTextOutlined,
  CloudSyncOutlined,
  HeartOutlined,
  LinkOutlined,
  ReloadOutlined,
  SettingOutlined,
  TeamOutlined,
} from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { useLocalStorageState } from "../hooks/useLocalStorageState";
import { ErrorStateCard, LoadingStateCard } from "../components/UxStates";
import { notify, notifyError } from "../notify";
import { localDateKey as localDayKey, relativeTime } from "../utils/datetime";
import { sectionByKey } from "../navigation";
import { RunActivityChart } from "../features/status/RunActivity";
import { StatusTabs } from "../features/status/StatusTabs";
import { KeyValueList, middleEllipsis, Panel, StatTile, StatusBadge, type KeyValueItem } from "../ui";

const FeaturesSection = lazy(() =>
  import("../features/settings/FeaturesSection").then(({ FeaturesSection: Component }) => ({ default: Component })),
);
const ConfigSection = lazy(() =>
  import("../features/settings/ConfigSection").then(({ ConfigSection: Component }) => ({ default: Component })),
);
const ScheduleSection = lazy(() =>
  import("../features/settings/ScheduleSection").then(({ ScheduleSection: Component }) => ({ default: Component })),
);
const LogsSection = lazy(() =>
  import("../features/settings/LogsSection").then(({ LogsSection: Component }) => ({ default: Component })),
);
const JsonOutputsSection = lazy(() =>
  import("../features/settings/JsonOutputsSection").then(({ JsonOutputsSection: Component }) => ({ default: Component })),
);
const RawOutputsSection = lazy(() =>
  import("../features/settings/RawOutputsSection").then(({ RawOutputsSection: Component }) => ({ default: Component })),
);
const ApiExplorerSection = lazy(() =>
  import("../features/settings/ApiExplorerSection").then(({ ApiExplorerSection: Component }) => ({ default: Component })),
);
const AuditLogSection = lazy(() =>
  import("../features/settings/AuditLogSection").then(({ AuditLogSection: Component }) => ({ default: Component })),
);
const AccessSection = lazy(() =>
  import("../features/settings/AccessSection").then(({ AccessSection: Component }) => ({ default: Component })),
);
const NotificationsSection = lazy(() =>
  import("../features/settings/NotificationsSection").then(({ NotificationsSection: Component }) => ({ default: Component })),
);
const HealthRepairPage = lazy(() =>
  import("../features/health/HealthRepairPage").then(({ HealthRepairPage: Component }) => ({ default: Component })),
);
const UpdatesBackupsPage = lazy(() =>
  import("../features/updates/UpdatesBackupsPage").then(({ UpdatesBackupsPage: Component }) => ({ default: Component })),
);

function lazySection(children: ReactNode) {
  return <Suspense fallback={<LoadingStateCard rows={4} />}>{children}</Suspense>;
}

/**
 * Compact bar-sparkline of *completed* runs per day for the trailing N days.
 *
 * Counting policy:
 *   - We only count entries whose source is "history" (archived per-run dir)
 *     or "summary" (the latest in-place run-summary.json). These are the only
 *     sources that represent an actual orchestrator run that produced
 *     artifacts.
 *   - We deliberately ignore source="trigger" entries. A trigger is a record
 *     of a user clicking "Trigger Run" — it does NOT mean a run completed.
 *     Many triggers may produce zero runs (e.g. a 409 "already in progress"
 *     rejection still gets logged as a successful API call). Including them
 *     used to inflate the count (e.g. "2 runs" when only 1 actually ran).
 *   - We dedupe by minute-precision timestamp so the same run doesn't get
 *     double-counted when both its archived copy ("history") and the
 *     in-place run-summary.json ("summary") happen to coexist on disk.
 *
 * We request `source=history` from the API to keep the wire payload small,
 * but the post-processing here also tolerates "summary" so the in-flight
 * latest run shows up before the orchestrator archives it.
 */
/** A server path row: truncated on one line, full path on hover, copy button. */
function pathItem(label: string, value: string, hint: string): KeyValueItem {
  const missing = !value || value === "-";
  return {
    label: (
      <Tooltip title={hint}>
        <span className="status-path-label">{label}</span>
      </Tooltip>
    ),
    value: missing ? "Not configured" : middleEllipsis(value),
    title: missing ? undefined : value,
    mono: !missing,
    copy: missing ? undefined : value,
  };
}

/** System → Status: is the API up, what is it running, and which paths does it use. */
function ConnectionTab({
  health,
  report,
  backendConfigPath,
}: {
  health: ReturnType<typeof useQuery>;
  report: ReturnType<typeof useQuery>;
  backendConfigPath: string;
}) {
  const data = (health.data as Record<string, unknown> | undefined) ?? {};
  const status = String(data.status ?? "unknown");
  const authMode = String(data.auth_mode ?? "-");
  const tokenSource = String(data.token_source ?? "-");
  const outputDir = String(data.output_dir ?? "-");
  const logDir = String(data.log_dir ?? "-");
  const tokenFile = String(data.token_file ?? "-");
  const orchestratorBin = String(data.orchestrator_bin ?? "-");
  const version = String(data.version ?? "");
  const buildDate = String(data.build_date ?? "");
  const stream = String(data.stream ?? "");
  const goVersion = String(data.go_version ?? "");
  const osName = String(data.os ?? "");
  const arch = String(data.arch ?? "");
  const rawLastChecked = data.time ? new Date(data.time as string) : null;
  const lastChecked = rawLastChecked && !Number.isNaN(rawLastChecked.getTime()) ? rawLastChecked : null;
  const isOk = status === "ok";

  const buildLabel = (() => {
    if (!buildDate || buildDate === "unknown") return "";
    return localDayKey(buildDate) || buildDate;
  })();

  return (
    <div className="status-page">
      <div className="overview-runline">
        <div className="overview-runline-text">
          <StatusBadge tone={isOk ? "ok" : "fail"}>{isOk ? "Connected" : "Disconnected"}</StatusBadge>
          <span>
            {isOk ? "The API server is answering health checks." : "The API server is not answering. Check that the services are running."}
          </span>
          {lastChecked ? (
            <Tooltip title={lastChecked.toLocaleString()}>
              <span>· checked {relativeTime(lastChecked.toISOString())}</span>
            </Tooltip>
          ) : null}
        </div>
        <div className="overview-runline-actions">
          <Button
            icon={<ReloadOutlined />}
            onClick={async () => {
              await Promise.all([health.refetch(), report.refetch()]);
              notify.success("Status refreshed.");
            }}
            loading={health.isFetching || report.isFetching}
          >
            Refresh
          </Button>
        </div>
      </div>

      <div className="overview-stats">
        <StatTile label="API" value={isOk ? "Healthy" : status} tone={isOk ? "ok" : "fail"} />
        <StatTile
          label="Version"
          value={version ? `v${version.split("-")[0]}` : "—"}
          hint={[buildLabel && `Built ${buildLabel}`, stream].filter(Boolean).join(" · ") || undefined}
        />
        <StatTile label="Authentication" value={authMode} hint={tokenSource !== "-" ? `Token from ${tokenSource}` : undefined} />
        <StatTile label="Platform" value={[osName, arch].filter(Boolean).join("/") || "—"} hint={goVersion ? `Go ${goVersion.replace(/^go/, "")}` : undefined} />
      </div>

      <div className="insights-grid">
        <Panel title="Server paths" description="Files and folders this server uses. Hover for what each is; copy with the icon.">
          <KeyValueList
            items={[
              pathItem("Config file", backendConfigPath, "The configuration file in use."),
              pathItem("Output folder", outputDir, "Where run results and reports are saved."),
              pathItem("Log folder", logDir, "NCC logs for each run."),
              pathItem("Token file", tokenFile, "API token on disk, used by the UI and CLI clients."),
              pathItem("Orchestrator", orchestratorBin, "The program the API server runs for checks and schedule installs."),
            ]}
          />
        </Panel>
        <Panel title="Run activity" description="Runs per day over the last 7 days, by outcome.">
          <RunActivityChart />
        </Panel>
      </div>
    </div>
  );
}

function DeveloperTab({ onError }: { onError: (e: unknown) => void }) {
  const [section, setSection] = useLocalStorageState("settings.developer.section", "api");
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedDevTab = (searchParams.get("dev") || "").trim();
  const activeSection = requestedDevTab || section;

  useEffect(() => {
    if (!requestedDevTab && section) {
      const next = new URLSearchParams(searchParams);
      next.set("dev", section);
      setSearchParams(next, { replace: true });
    }
  }, [requestedDevTab, section, searchParams, setSearchParams]);

  return (
      <Tabs
        activeKey={activeSection}
        onChange={(nextSection) => {
          setSection(nextSection);
          const next = new URLSearchParams(searchParams);
          next.set("dev", nextSection);
          setSearchParams(next, { replace: true });
        }}
        items={[
          { key: "api", label: "API explorer", children: lazySection(<ApiExplorerSection onError={onError} />) },
          { key: "json", label: "Saved reports", children: lazySection(<JsonOutputsSection onError={onError} />) },
          { key: "raw", label: "Run files", children: lazySection(<RawOutputsSection onError={onError} />) },
        ]}
      />
  );
}

/**
 * The former Settings tabs. With `section` (the app's routes, see
 * navigation.tsx) it renders just that section — the sidebar is the
 * navigation. Without it, it renders the legacy tab bar driven by `?tab=`.
 */
export function SettingsPage({ isAdmin = true, section }: { isAdmin?: boolean; section?: string }) {
  const [tab, setTab] = useLocalStorageState("settings.activeTab", "connection");
  const [devTab] = useLocalStorageState("settings.developer.section", "api");
  const [searchParams, setSearchParams] = useSearchParams();
  const health = useQuery({ queryKey: ["health"], queryFn: api.health });
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData });
  const backendConfigPath = (health.data as { config_path?: string } | undefined)?.config_path ?? "";
  const isKubernetes = Boolean(
    (health.data as { runtime?: { kubernetes?: boolean } } | undefined)?.runtime?.kubernetes,
  );

  useEffect(() => {
    if (health.error) notifyError(health.error, "Could not reach the API");
  }, [health.error]);

  useEffect(() => {
    if (report.error) notifyError(report.error, "Could not load report data");
  }, [report.error]);

  const apiOk = (health.data as { status?: string } | undefined)?.status === "ok";
  const loginEnabled = Boolean((health.data as { login_enabled?: boolean } | undefined)?.login_enabled);
  const tabLabel = (icon: ReactNode, text: string, dotColor?: string) => (
    <span className="settings-tab-label">
      <span className="settings-tab-icon">{icon}</span>
      <span className="settings-tab-text">{text}</span>
      {dotColor ? <span className="settings-tab-dot" style={{ background: dotColor }} /> : null}
    </span>
  );

  const items = [
        {
          key: "connection",
          label: tabLabel(<ApiOutlined />, "Connection", apiOk ? "var(--ui-ok)" : "var(--ui-fail)"),
          children: (
            <StatusTabs
              isAdmin={isAdmin}
              overview={<ConnectionTab health={health} report={report} backendConfigPath={backendConfigPath} />}
            />
          ),
        },
        {
          key: "config",
          label: tabLabel(<SettingOutlined />, "Config"),
          children: lazySection(<ConfigSection onError={notifyError} />),
        },
        {
          key: "schedule",
          label: tabLabel(<CalendarOutlined />, "Schedule"),
          children: lazySection(
            <ScheduleSection
              backendConfigPath={backendConfigPath}
              onError={notifyError}
              isKubernetes={isKubernetes}
            />,
          ),
        },
        // Runs has its own page (/runs, pages/RunsPage.tsx) since the redesign.
        {
          key: "logs",
          label: tabLabel(<FileTextOutlined />, "Logs"),
          children: lazySection(<LogsSection onError={notifyError} />),
        },
        {
          key: "audit",
          label: tabLabel(<AuditOutlined />, "Audit"),
          children: lazySection(<AuditLogSection onError={notifyError} />),
        },
        {
          key: "notifications",
          label: tabLabel(<BellOutlined />, "Notifications"),
          children: lazySection(<NotificationsSection />),
        },
        {
          key: "health",
          label: tabLabel(<HeartOutlined />, "Health & repair"),
          children: lazySection(<HealthRepairPage />),
        },
        {
          key: "updates",
          label: tabLabel(<CloudSyncOutlined />, "Updates & backups"),
          children: lazySection(<UpdatesBackupsPage isKubernetes={isKubernetes} />),
        },
        {
          key: "features",
          label: tabLabel(<ControlOutlined />, "Features"),
          children: lazySection(<FeaturesSection />),
        },
        {
          key: "developer",
          label: tabLabel(<LinkOutlined />, "Advanced"),
          children: lazySection(<DeveloperTab onError={notifyError} />),
        },
  ];
  if (loginEnabled) {
    // Insert Access just before Developer so the admin tabs end
    // Access → Developer.
    const developerIdx = items.findIndex((it) => it.key === "developer");
    const insertAt = developerIdx >= 0 ? developerIdx : items.length;
    items.splice(insertAt, 0, {
      key: "access",
      label: tabLabel(<TeamOutlined />, "Access"),
          children: lazySection(<AccessSection isKubernetes={isKubernetes} />),
    });
  }

  // Operators (non-admins who can run NCC) get a reduced set of tabs: the
  // operational ones (Connection, Schedule, Runs, Logs, Audit) that only read
  // non-secret data or perform run/schedule actions they are allowed. The
  // secret-bearing tabs (Config, Access, Developer) remain admin-only, matching
  // the server-side RBAC on those endpoints.
  const operatorTabs = new Set(["connection", "schedule", "runs", "logs", "audit"]);
  const visibleItems = isAdmin ? items : items.filter((it) => operatorTabs.has(it.key));
  const requestedTab = (searchParams.get("tab") || "").trim();
  const requestedTabAllowed = visibleItems.some((it) => it.key === requestedTab);
  const storedTabAllowed = visibleItems.some((it) => it.key === tab);
  const activeTab = requestedTabAllowed
    ? requestedTab
    : storedTabAllowed
      ? tab
      : (visibleItems[0]?.key ?? "connection");

  // Keep the remembered tab in sync with the currently active tab.
  useEffect(() => {
    if (section) return;
    if (tab !== activeTab) {
      setTab(activeTab);
    }
  }, [section, activeTab, tab, setTab]);

  // Keep the URL query param in sync so deep links like /settings?tab=runs
  // work and the current tab is shareable/bookmarkable.
  useEffect(() => {
    if (section) return;
    if ((searchParams.get("tab") || "") === activeTab) return;
    const next = new URLSearchParams(searchParams);
    next.set("tab", activeTab);
    setSearchParams(next, { replace: true });
  }, [section, activeTab, searchParams, setSearchParams]);

  useEffect(() => {
    const focus = (searchParams.get("focus") || "").trim();
    if (!focus) return;
    const tryScroll = () => {
      const el = document.getElementById(`settings-${focus}`);
      if (!el) return false;
      el.scrollIntoView({ behavior: "smooth", block: "start" });
      return true;
    };
    if (tryScroll()) return;
    const t1 = window.setTimeout(tryScroll, 250);
    const t2 = window.setTimeout(tryScroll, 800);
    return () => {
      window.clearTimeout(t1);
      window.clearTimeout(t2);
    };
  }, [section, activeTab, searchParams]);

  if (health.isLoading && !health.data) {
    return (
      <Space orientation="vertical" size={16} style={{ width: "100%" }}>
        <LoadingStateCard rows={3} />
        <LoadingStateCard rows={6} />
      </Space>
    );
  }
  if (health.isError && !health.data) {
    return (
      <ErrorStateCard
        title="Unable to load settings health context"
        error={String((health.error as Error | undefined)?.message || "Unknown error")}
        onRetry={() => {
          void health.refetch();
        }}
      />
    );
  }

  if (section) {
    const item = visibleItems.find((it) => it.key === section);
    // Not available to this role or on this install (e.g. Access without
    // sign-in): send them to the Settings landing instead of a blank page.
    if (!item) return <Navigate to="/settings" replace />;
    const description = sectionByKey(section)?.description;
    return (
      <div className="section-page" data-section={section}>
        {description ? <p className="section-intro">{description}</p> : null}
        {item.children}
      </div>
    );
  }

  return (
    <Tabs
      destroyOnHidden
      activeKey={activeTab}
      onChange={(nextTab) => {
        setTab(nextTab);
        const next = new URLSearchParams(searchParams);
        next.set("tab", nextTab);
        next.delete("focus");
        if (nextTab !== "developer") {
          next.delete("dev");
        } else if (!next.get("dev")) {
          next.set("dev", devTab);
        }
        setSearchParams(next, { replace: true });
      }}
      size="large"
      className="settings-tabs"
      items={visibleItems}
    />
  );
}
