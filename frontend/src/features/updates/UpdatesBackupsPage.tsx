import { lazy, Suspense, type ReactNode } from "react";
import { Card, Skeleton, Space, Tabs } from "antd";
import { useSearchParams } from "react-router";
import { MigrationsPanel } from "./MigrationsPanel";

const Cards = lazy(() =>
  import("../settings/AccessSection").then((m) => ({
    default: ({ part, isKubernetes }: { part: "updates" | "backups"; isKubernetes: boolean }) =>
      part === "updates" ? (
        <m.UpdatesCard isKubernetes={isKubernetes} />
      ) : (
        <Space orientation="vertical" size={16} style={{ width: "100%" }}>
          <m.BackupRestoreCard />
          <m.ScheduledBackupCard />
        </Space>
      ),
  })),
);

/** Tabs of Updates & backups, in order. */
export const UPDATES_TABS = ["updates", "backups", "migrations"] as const;
export type UpdatesTab = (typeof UPDATES_TABS)[number];

/** The tab from `?tab=`; old `?focus=backup` links open Backups. */
export function updatesTabFrom(params: URLSearchParams): UpdatesTab {
  const tab = (params.get("tab") || "").trim();
  if ((UPDATES_TABS as readonly string[]).includes(tab)) return tab as UpdatesTab;
  if (params.get("focus") === "backup") return "backups";
  return "updates";
}

function Lazy({ children }: { children: ReactNode }) {
  return <Suspense fallback={<Card className="page-card"><Skeleton active /></Card>}>{children}</Suspense>;
}

/**
 * Updates & backups: software updates (carried out by the supervisor, with
 * health checks and automatic rollback), backups and restore, and config
 * migrations, all changes to what is installed.
 */
export function UpdatesBackupsPage({ isKubernetes = false }: { isKubernetes?: boolean }) {
  const [params, setParams] = useSearchParams();
  const tab = updatesTabFrom(params);
  return (
    <Tabs
      className="repair-tabs"
      activeKey={tab}
      destroyOnHidden
      onChange={(next) => {
        const p = new URLSearchParams(params);
        p.set("tab", next);
        p.delete("focus");
        setParams(p, { replace: true });
      }}
      items={[
        { key: "updates", label: "Updates", children: <Lazy><Cards part="updates" isKubernetes={isKubernetes} /></Lazy> },
        { key: "backups", label: "Backups", children: <Lazy><Cards part="backups" isKubernetes={isKubernetes} /></Lazy> },
        { key: "migrations", label: "Migrations", children: <MigrationsPanel /> },
      ]}
    />
  );
}
