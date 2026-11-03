import { lazy, Suspense, type ReactNode } from "react";
import { Card, Skeleton, Tabs } from "antd";
import { useSearchParams } from "react-router";

const SupervisorPanel = lazy(() => import("../health/SupervisorPanel").then((m) => ({ default: m.SupervisorPanel })));
const PlatformPanel = lazy(() => import("../health/SupervisorPanel").then((m) => ({ default: m.PlatformPanel })));

function Lazy({ children }: { children: ReactNode }) {
  return <Suspense fallback={<Card className="page-card"><Skeleton active /></Card>}>{children}</Suspense>;
}

export const STATUS_TABS = ["overview", "supervisor", "platform"] as const;
export type StatusTab = (typeof STATUS_TABS)[number];

export function statusTabFrom(params: URLSearchParams): StatusTab {
  const tab = (params.get("tab") || "").trim();
  return (STATUS_TABS as readonly string[]).includes(tab) ? (tab as StatusTab) : "overview";
}

/**
 * System → Status: what is running and on what. Operators see the overview;
 * admins also get the supervisor (restarts, crashes, upgrades) and platform
 * details, whose endpoints are admin-only.
 */
export function StatusTabs({ isAdmin, overview }: { isAdmin: boolean; overview: ReactNode }) {
  const [params, setParams] = useSearchParams();
  if (!isAdmin) return <>{overview}</>;
  return (
    <Tabs
      className="repair-tabs"
      activeKey={statusTabFrom(params)}
      destroyOnHidden
      onChange={(next) => {
        const p = new URLSearchParams(params);
        p.set("tab", next);
        setParams(p, { replace: true });
      }}
      items={[
        { key: "overview", label: "Overview", children: overview },
        { key: "supervisor", label: "Supervisor", children: <Lazy><SupervisorPanel /></Lazy> },
        { key: "platform", label: "Platform", children: <Lazy><PlatformPanel /></Lazy> },
      ]}
    />
  );
}
