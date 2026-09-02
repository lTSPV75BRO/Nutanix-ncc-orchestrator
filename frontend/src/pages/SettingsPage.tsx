import { lazy, Suspense, useEffect } from "react";
import type { ReactNode } from "react";
import { useSearchParams } from "react-router";
import {
  Button,
  Card,
  Col,
  Row,
  Space,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  ApartmentOutlined,
  ApiOutlined,
  AuditOutlined,
  BellOutlined,
  CalendarOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  CodeOutlined,
  FileTextOutlined,
  HeartOutlined,
  KeyOutlined,
  LinkOutlined,
  ReloadOutlined,
  SettingOutlined,
  TeamOutlined,
  ThunderboltOutlined,
  ToolOutlined,
} from "@ant-design/icons";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { useLocalStorageState } from "../hooks/useLocalStorageState";
import { ErrorStateCard, LoadingStateCard } from "../components/UxStates";
import { notify, notifyError } from "../notify";
import { localDateKey as localDayKey, relativeTime } from "../utils/datetime";

const ConfigSection = lazy(() =>
  import("../features/settings/ConfigSection").then(({ ConfigSection: Component }) => ({ default: Component })),
);
const ScheduleSection = lazy(() =>
  import("../features/settings/ScheduleSection").then(({ ScheduleSection: Component }) => ({ default: Component })),
);
const RunsSection = lazy(() =>
  import("../features/runs/RunsSection").then(({ RunsSection: Component }) => ({ default: Component })),
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
const MaintenanceSection = lazy(() =>
  import("../features/settings/AccessSection").then(({ MaintenanceSection: Component }) => ({ default: Component })),
);
const NotificationsSection = lazy(() =>
  import("../features/settings/NotificationsSection").then(({ NotificationsSection: Component }) => ({ default: Component })),
);
const SystemHealthSection = lazy(() =>
  import("../features/settings/SystemHealthSection").then(({ SystemHealthSection: Component }) => ({ default: Component })),
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
function RecentRunsSparkline({ days = 7 }: { days?: number }) {
  // Pull only sources that correspond to real runs. The backend supports
  // ?source= filtering; "history" is the archived (most authoritative) feed.
  // We also fetch unfiltered and post-filter so a brand-new run that's still
  // sitting at outputDir/run-summary.json (source="summary") shows up before
  // it's archived. Fetching unfiltered is cheap and yields fresher results.
  const runsQuery = useQuery({
    queryKey: ["runs", "sparkline", days],
    queryFn: () => api.runs({ limit: 200 }),
    staleTime: 30_000,
  });

  // Local-day key (YYYY-MM-DD in the user's timezone). We *deliberately* don't
  // use toISOString().slice(0,10) here — that returns the UTC date, which can
  // be off-by-one for users in IST/PST/etc. when local midnight straddles a
  // UTC boundary. Bucketing must be in the user's local TZ so a run that
  // happened "today afternoon" actually lands in the "today" bar.
  const localDateKey = (d: Date) => {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  };

  const buckets: { key: string; label: string; count: number }[] = [];
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  for (let i = days - 1; i >= 0; i -= 1) {
    const d = new Date(today.getTime() - i * 24 * 3600 * 1000);
    buckets.push({
      key: localDateKey(d),
      label: d.toLocaleDateString(undefined, { weekday: "short" }),
      count: 0,
    });
  }
  const windowStart = today.getTime() - (days - 1) * 24 * 3600 * 1000;
  const cutoff = today.getTime() + 24 * 3600 * 1000;

  const list = (runsQuery.data ?? []) as Array<{
    mod_time?: string;
    timestamp?: string;
    source?: string;
    success?: boolean;
  }>;
  // history wins when the same minute also has a summary entry pointing to
  // the same run on disk.
  const ordered = [...list].sort((a, b) => {
    const rank: Record<string, number> = { history: 2, summary: 1 };
    return (rank[b.source ?? ""] ?? 0) - (rank[a.source ?? ""] ?? 0);
  });
  const seenMinute = new Set<string>();
  for (const r of ordered) {
    // Hard filter: triggers are not runs. Anything else with no recognizable
    // source (e.g. future API additions) is also ignored to avoid surprises.
    if (r.source !== "history" && r.source !== "summary") continue;
    const iso = r.timestamp || r.mod_time;
    if (!iso) continue;
    const ts = new Date(iso);
    const t = ts.getTime();
    if (Number.isNaN(t)) continue;
    if (t < windowStart || t > cutoff) continue;
    const minuteKey = ts.toISOString().slice(0, 16);
    if (seenMinute.has(minuteKey)) continue;
    seenMinute.add(minuteKey);
    const dayKey = localDateKey(ts);
    const bucket = buckets.find((b) => b.key === dayKey);
    if (bucket) bucket.count += 1;
  }

  const total = buckets.reduce((acc, b) => acc + b.count, 0);
  const isLoading = runsQuery.isLoading;
  const max = Math.max(1, ...buckets.map((b) => b.count));
  const width = 220;
  const height = 48;
  const gap = 4;
  const barW = (width - gap * (buckets.length - 1)) / buckets.length;

  return (
    <div className="recent-runs-spark">
      <div className="recent-runs-meta">
        <Typography.Text strong>Recent runs</Typography.Text>
        <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
          {isLoading ? "loading…" : `${total} run${total === 1 ? "" : "s"} · last ${days} days`}
        </Typography.Text>
      </div>
      <svg width={width} height={height} role="img" aria-label={`Run frequency over the last ${days} days`}>
        {buckets.map((b, i) => {
          const h = b.count > 0 ? Math.max(2, (b.count / max) * (height - 4)) : 2;
          const x = i * (barW + gap);
          const y = height - h;
          return (
            <g key={b.key}>
              <title>{`${b.label} (${b.key}): ${b.count} run${b.count === 1 ? "" : "s"}`}</title>
              <rect
                x={x}
                y={y}
                width={barW}
                height={h}
                rx={2}
                fill={b.count > 0 ? "var(--menu-selected-border, #1677ff)" : "var(--control-border, #d9d9d9)"}
                opacity={b.count > 0 ? 0.9 : 0.4}
              />
            </g>
          );
        })}
      </svg>
      <div className="recent-runs-axis">
        {buckets.map((b) => (
          <span key={b.key}>{b.label.charAt(0)}</span>
        ))}
      </div>
    </div>
  );
}

function PathRow({ icon, label, value, hint }: { icon: React.ReactNode; label: string; value: string; hint?: string }) {
  const display = value && value !== "-" ? value : "(not configured)";
  const isMissing = !value || value === "-";
  return (
    <div className="resolved-path-row">
      <div className="resolved-path-label">
        <span className="resolved-path-icon">{icon}</span>
        <Typography.Text strong>{label}</Typography.Text>
        {hint ? (
          <Tooltip title={hint}>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              ⓘ
            </Typography.Text>
          </Tooltip>
        ) : null}
      </div>
      <Typography.Text
        copyable={!isMissing ? { text: value } : false}
        type={isMissing ? "secondary" : undefined}
        className="mono"
        style={{ fontSize: 13, wordBreak: "break-all" }}
      >
        {display}
      </Typography.Text>
    </div>
  );
}

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
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <Card className="page-card connection-status-card">
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} md={14}>
            <Space size={16} align="start">
              <div className={`connection-orb ${isOk ? "ok" : "err"}`}>
                {isOk ? <CheckCircleOutlined /> : <CloseCircleOutlined />}
              </div>
              <div>
                <Typography.Title level={4} style={{ margin: 0 }}>
                  {isOk ? "Connected" : "Disconnected"}
                </Typography.Title>
                <Typography.Text type="secondary">
                  {isOk
                    ? "API server is responding to health probes."
                    : "API server is not reachable. Check that v2 services are running."}
                </Typography.Text>
                <div style={{ marginTop: 8 }}>
                  <Space size={[8, 8]} wrap>
                    <Tag icon={<KeyOutlined />} color={authMode === "token" ? "processing" : "default"}>
                      Auth: {authMode}
                    </Tag>
                    <Tag color="default">Token: {tokenSource}</Tag>
                    {version ? (
                      <Tooltip
                        title={
                          <div style={{ fontSize: 12, lineHeight: 1.5 }}>
                            <div><strong>{version}</strong></div>
                            {buildLabel ? <div>Built {buildLabel}</div> : null}
                            {stream ? <div>Stream: {stream}</div> : null}
                            {goVersion ? <div>Go: {goVersion}</div> : null}
                            {osName || arch ? <div>{[osName, arch].filter(Boolean).join("/")}</div> : null}
                          </div>
                        }
                      >
                        <Tag color="geekblue">
                          v{version.split("-")[0]}{buildLabel ? ` · built ${buildLabel}` : ""}
                        </Tag>
                      </Tooltip>
                    ) : null}
                    {lastChecked ? (
                      <Tooltip title={lastChecked.toLocaleString()}>
                        <Tag>Checked {relativeTime(lastChecked.toISOString())}</Tag>
                      </Tooltip>
                    ) : null}
                  </Space>
                </div>
              </div>
            </Space>
          </Col>
          <Col xs={24} md={10}>
            <Space
              orientation="vertical"
              size={12}
              style={{ display: "flex", alignItems: "flex-end", width: "100%" }}
            >
              <RecentRunsSparkline />
              <Space size={8} wrap>
                <Button
                  icon={<ReloadOutlined />}
                  onClick={async () => {
                    await health.refetch();
                    notify.success("Health refreshed.");
                  }}
                  loading={health.isFetching}
                >
                  Refresh Health
                </Button>
                <Button
                  type="primary"
                  icon={<ThunderboltOutlined />}
                  onClick={async () => {
                    await report.refetch();
                    notify.success("Report data refreshed.");
                  }}
                  loading={report.isFetching}
                >
                  Refresh Report
                </Button>
              </Space>
            </Space>
          </Col>
        </Row>
      </Card>

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Resolved Paths
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          The absolute paths the API server is configured to use. Click any value to copy it.
        </Typography.Text>
        <div className="resolved-paths-grid">
          <PathRow
            icon={<FileTextOutlined />}
            label="Config file"
            value={backendConfigPath}
            hint="Active YAML config consumed by the orchestrator."
          />
          <PathRow
            icon={<ApartmentOutlined />}
            label="Output directory"
            value={outputDir}
            hint="Where run artifacts (run-summary.json, NCC logs, …) are written."
          />
          <PathRow
            icon={<FileTextOutlined />}
            label="Log directory"
            value={logDir}
            hint="Per-run NCC plugin summary logs land here."
          />
          <PathRow
            icon={<KeyOutlined />}
            label="Token file"
            value={tokenFile}
            hint="API token persisted on disk. Used by UI and CLI clients."
          />
          <PathRow
            icon={<CodeOutlined />}
            label="Orchestrator binary"
            value={orchestratorBin}
            hint="The exec target the API server invokes for runs and schedule installs."
          />
        </div>
      </Card>
    </Space>
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
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Developer Tools
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Low-level utilities for debugging API endpoints and inspecting raw artifacts.
      </Typography.Text>
      <Tabs
        style={{ marginTop: 12 }}
        activeKey={activeSection}
        onChange={(nextSection) => {
          setSection(nextSection);
          const next = new URLSearchParams(searchParams);
          next.set("dev", nextSection);
          setSearchParams(next, { replace: true });
        }}
        items={[
          { key: "api", label: "API Explorer", children: lazySection(<ApiExplorerSection onError={onError} />) },
          { key: "json", label: "JSON Artifacts", children: lazySection(<JsonOutputsSection onError={onError} />) },
          { key: "raw", label: "Raw Files", children: lazySection(<RawOutputsSection onError={onError} />) },
        ]}
      />
    </Card>
  );
}

export function SettingsPage({ isAdmin = true }: { isAdmin?: boolean }) {
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
    if (health.error) notifyError(health.error, "Failed to fetch API health");
  }, [health.error]);

  useEffect(() => {
    if (report.error) notifyError(report.error, "Failed to fetch report data");
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
          label: tabLabel(<ApiOutlined />, "Connection", apiOk ? "#22c55e" : "#ef4444"),
          children: <ConnectionTab health={health} report={report} backendConfigPath={backendConfigPath} />,
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
        {
          key: "runs",
          label: tabLabel(<ThunderboltOutlined />, "Runs"),
          children: lazySection(<RunsSection backendConfigPath={backendConfigPath} onError={notifyError} />),
        },
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
          label: tabLabel(<HeartOutlined />, "System Health"),
          children: lazySection(<SystemHealthSection />),
        },
        {
          key: "maintenance",
          label: tabLabel(<ToolOutlined />, "Maintenance"),
          children: lazySection(<MaintenanceSection isKubernetes={isKubernetes} />),
        },
        {
          key: "developer",
          label: tabLabel(<LinkOutlined />, "Developer"),
          children: lazySection(<DeveloperTab onError={notifyError} />),
        },
  ];
  if (loginEnabled) {
    // Insert Access just before Maintenance so the trailing admin tabs read
    // Access → Maintenance → Developer.
    const maintenanceIdx = items.findIndex((it) => it.key === "maintenance");
    const insertAt = maintenanceIdx >= 0 ? maintenanceIdx : items.length - 1;
    items.splice(insertAt, 0, {
      key: "access",
      label: tabLabel(<TeamOutlined />, "Access"),
      children: lazySection(<AccessSection />),
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
    if (tab !== activeTab) {
      setTab(activeTab);
    }
  }, [activeTab, tab, setTab]);

  // Keep the URL query param in sync so deep links like /settings?tab=runs
  // work and the current tab is shareable/bookmarkable.
  useEffect(() => {
    if ((searchParams.get("tab") || "") === activeTab) return;
    const next = new URLSearchParams(searchParams);
    next.set("tab", activeTab);
    setSearchParams(next, { replace: true });
  }, [activeTab, searchParams, setSearchParams]);

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

  return (
    <Tabs
      activeKey={activeTab}
      onChange={(nextTab) => {
        setTab(nextTab);
        const next = new URLSearchParams(searchParams);
        next.set("tab", nextTab);
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
