import { Alert, Badge, Card, Skeleton, Space, Table, Tabs, Tag, Typography } from "antd";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router";
import { api } from "../../api/client";
import type { AutoHealer, HealHistoryEntry } from "../../api/types";
import { formatTime } from "../../utils/datetime";
import { SystemHealthSection } from "../settings/SystemHealthSection";
import { EmptyState, ItemList, Panel, StatusBadge, type Tone } from "../../ui";

/**
 * Tabs of Health & repair, in order. Updates, backups and migrations moved to
 * Updates & backups, the supervisor and platform to Status (navigation.tsx
 * redirects the old ?tab= links).
 */
export const HEALTH_TABS = ["checks", "automatic", "history"] as const;
export type HealthTab = (typeof HEALTH_TABS)[number];

export function healthTabFrom(params: URLSearchParams): HealthTab {
  const tab = (params.get("tab") || "").trim();
  if ((HEALTH_TABS as readonly string[]).includes(tab)) return tab as HealthTab;
  return "checks";
}

const STATE_TAG: Record<AutoHealer["state"], { tone: Tone; label: string }> = {
  on: { tone: "ok", label: "On" },
  partial: { tone: "info", label: "Report only" },
  off: { tone: "neutral", label: "Off" },
};

function statText(key: string, value: unknown): string | null {
  if (value === undefined || value === null || value === "") return null;
  // runs/fixes count every doctor run, on demand ones included.
  const labels: Record<string, string> = {
    runs: "check runs",
    fixes: "fixes applied",
    retries: "retries",
    last_run: "last check run",
  };
  if (key === "interval" || key === "auto_fix" || key === "last_summary") return null;
  if (key === "last_run") return `${labels[key]} ${formatTime(String(value))}`;
  return `${String(value)} ${labels[key] ?? key}`;
}

export function AutoHealPanel() {
  const q = useQuery({ queryKey: ["health", "autoheal"], queryFn: api.autoHeal, staleTime: 30_000 });
  if (q.isLoading) return <Card className="page-card"><Skeleton active /></Card>;
  if (q.isError) return <Alert type="error" showIcon title="Couldn't load automatic healing" description={String((q.error as Error)?.message ?? "")} />;
  const healers = q.data?.healers ?? [];
  return (
    <Panel
      title="Automatic healing"
      description="What repairs itself without anyone clicking Fix. Server flags are set when the API server starts; the rest are settings."
      flush
    >
      <ItemList
        items={healers}
        itemKey={(h) => h.id}
        render={(h) => {
          const stats = Object.entries(h.stats ?? {})
            .map(([k, v]) => statText(k, v))
            .filter((x): x is string => Boolean(x));
          return (
            <div className="repair-healer" data-healer={h.id}>
              <Typography.Text strong>{h.title}</Typography.Text>
              <Typography.Paragraph type="secondary" className="health-check-text">
                {h.description}
                {h.detail ? ` ${h.detail}` : ""}
              </Typography.Paragraph>
              {stats.length ? (
                <Typography.Text type="secondary" className="repair-healer-meta">
                  Since the server started: {stats.join(" · ")}
                </Typography.Text>
              ) : null}
              {h.how_to_change ? (
                <Typography.Text type="secondary" className="repair-healer-meta">
                  To change: {h.how_to_change}
                </Typography.Text>
              ) : null}
            </div>
          );
        }}
        actions={(h) => {
          const tag = STATE_TAG[h.state] ?? STATE_TAG.off;
          return <StatusBadge tone={tag.tone}>{tag.label}</StatusBadge>;
        }}
      />
    </Panel>
  );
}

export function HealHistoryPanel() {
  const q = useQuery({ queryKey: ["health", "history"], queryFn: () => api.healHistory(200), staleTime: 15_000 });
  if (q.isLoading) return <Card className="page-card"><Skeleton active /></Card>;
  if (q.isError) return <Alert type="error" showIcon title="Couldn't load the repair history" description={String((q.error as Error)?.message ?? "")} />;
  const data = q.data;
  const recurring = (data?.by_check ?? []).filter((c) => c.recurring);
  if (!data?.entries.length) {
    return (
      <Panel>
        <EmptyState
          title="Nothing repaired yet"
          description="Fixes from this page, the background checks and doctor --fix show up here."
        />
      </Panel>
    );
  }
  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {recurring.length ? (
        <Alert
          type="warning"
          showIcon
          title="Repairs that keep coming back"
          description={`${recurring.map((c) => `${c.title} (${c.fixes_24h}× in 24h)`).join(", ")}. Something keeps undoing these: look for a script, another install or a permissions change.`}
        />
      ) : null}
      {data.unverified_7d > 0 ? (
        <Alert type="info" showIcon title={`${data.unverified_7d} fix${data.unverified_7d === 1 ? "" : "es"} this week did not pass the re-check`} description="The check still failed right after its fix. Open Checks for what is left." />
      ) : null}
      <Card className="page-card" size="small" title={`Repairs (${data.total})`}>
        <Table<HealHistoryEntry>
          size="small"
          rowKey={(r) => `${r.at}-${r.id}`}
          dataSource={data.entries}
          pagination={{ pageSize: 20, size: "small", hideOnSinglePage: true }}
          scroll={{ x: 640 }}
          columns={[
            { title: "When", dataIndex: "at", width: 170, render: (v: string) => formatTime(v) },
            { title: "Check", dataIndex: "title", width: 220 },
            { title: "What was done", dataIndex: "fix_message" },
            {
              title: "Re-check",
              dataIndex: "verified",
              width: 110,
              render: (v: boolean | undefined) =>
                v === undefined ? <Tag>not re-checked</Tag> : v ? <Tag color="success">passed</Tag> : <Tag color="warning">still failing</Tag>,
            },
          ]}
        />
      </Card>
    </Space>
  );
}

/**
 * Health & repair: the doctor checks, what heals automatically, and the
 * history of repairs (`?tab=`).
 */
export function HealthRepairPage() {
  const [params, setParams] = useSearchParams();
  const tab = healthTabFrom(params);
  const diag = useQuery({ queryKey: ["health", "diagnostics"], queryFn: api.diagnostics, staleTime: 30_000 });
  const issues = (diag.data?.summary.warn ?? 0) + (diag.data?.summary.fail ?? 0);
  const label = (text: string, count = 0, danger = false) => (
    <span className="repair-tab-label">
      {text}
      {count > 0 ? <Badge count={count} size="small" color={danger ? undefined : "var(--ui-warn)"} /> : null}
    </span>
  );
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
        { key: "checks", label: label("Checks", issues, (diag.data?.summary.fail ?? 0) > 0), children: <SystemHealthSection /> },
        { key: "automatic", label: label("Automatic healing"), children: <AutoHealPanel /> },
        { key: "history", label: label("History"), children: <HealHistoryPanel /> },
      ]}
    />
  );
}
