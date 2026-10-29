import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Button, Segmented, Table } from "antd";
import { DownloadOutlined, FileTextOutlined, ReloadOutlined, ThunderboltOutlined } from "@ant-design/icons";
import { api } from "../api/client";
import type { ArtifactInfo, RunInfo } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { ActiveRuns } from "../features/runs/ActiveRuns";
import { NewRunDrawer } from "../features/runs/NewRunDrawer";
import { RunsTable } from "../features/runs/RunsTable";
import { useRunsActive } from "../hooks/useRunsActive";
import { EmptyState, Panel } from "../ui";
import { formatDateTime } from "../utils/datetime";
import { runStatusTone } from "../utils/runStatus";

type Filter = "all" | "problems";

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(value >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

/**
 * Runs: start a run (drawer), watch runs in progress, and browse history;
 * archived runs open their detail page. Viewers get the read-only parts.
 */
export function RunsPage() {
  const navigate = useNavigate();
  const { canOperate, me } = useAuth();
  const clusterRestricted = me?.cluster_access_unrestricted === false;
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [filter, setFilter] = useState<Filter>("all");
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, staleTime: 15_000 });
  const backendConfigPath = (health.data as { config_path?: string } | undefined)?.config_path ?? "";
  const runs = useQuery({ queryKey: ["runs", "history"], queryFn: () => api.runs(), staleTime: 15_000 });
  // Raw multi-cluster files are for unrestricted callers only.
  const artifacts = useQuery({
    queryKey: ["artifacts"],
    queryFn: api.artifacts,
    staleTime: 30_000,
    enabled: !clusterRestricted,
  });

  // Refresh history (and the files of the latest run) when a run finishes.
  const active = useRunsActive();
  const runningCount = (active.data?.runs ?? []).filter((r) => r.status === "running").length;
  const prevRunning = useRef(runningCount);
  useEffect(() => {
    if (runningCount < prevRunning.current) {
      void runs.refetch();
      void artifacts.refetch();
    }
    prevRunning.current = runningCount;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runningCount]);

  const all = runs.data ?? [];
  const problems = useMemo(() => all.filter((r) => ["fail", "warn"].includes(runStatusTone(r).tone)), [all]);
  const shown: RunInfo[] = filter === "problems" ? problems : all;

  return (
    <div className="runs-page">
      <div className="overview-runline">
        <div className="overview-runline-text">
          <span>Every run, scheduled or manual. Open a run for its clusters, checks and files.</span>
        </div>
        <div className="overview-runline-actions">
          <Button icon={<ReloadOutlined />} onClick={() => void Promise.all([runs.refetch(), artifacts.refetch(), active.refetch()])} loading={runs.isFetching}>
            Refresh
          </Button>
          {canOperate ? (
            <Button type="primary" icon={<ThunderboltOutlined />} onClick={() => setDrawerOpen(true)}>
              New run
            </Button>
          ) : null}
        </div>
      </div>

      <ActiveRuns />

      <Panel
        title="History"
        description={runs.data ? `${all.length} run${all.length === 1 ? "" : "s"}` : undefined}
        flush
        extra={
          <Segmented<Filter>
            size="small"
            value={filter}
            onChange={setFilter}
            options={[
              { value: "all", label: "All" },
              { value: "problems", label: `With problems${problems.length ? ` (${problems.length})` : ""}` },
            ]}
            aria-label="Filter runs"
          />
        }
      >
        <RunsTable
          runs={shown}
          loading={runs.isLoading}
          onOpen={(r) => navigate(`/runs/${encodeURIComponent(r.id)}`)}
          pagination={shown.length > 15 ? { pageSize: 15, showSizeChanger: false } : false}
          empty={
            filter === "problems" ? (
              <EmptyState title="No runs with problems" description="Every recorded run finished cleanly." />
            ) : (
              <EmptyState
                icon={<ThunderboltOutlined />}
                title="No runs yet"
                description="Start a run, or set up a schedule, and it appears here."
                actions={
                  canOperate ? (
                    <Button type="primary" onClick={() => setDrawerOpen(true)}>
                      New run
                    </Button>
                  ) : null
                }
              />
            )
          }
        />
      </Panel>

      {!clusterRestricted && (artifacts.data?.length ?? 0) > 0 ? (
        <Panel title="Files from the latest run" description="Reports and exports written by the most recent run." flush>
          <Table<ArtifactInfo>
            size="middle"
            rowKey="name"
            dataSource={artifacts.data}
            pagination={(artifacts.data?.length ?? 0) > 10 ? { pageSize: 10, showSizeChanger: false } : false}
            scroll={{ x: 560 }}
            columns={[
              {
                title: "File",
                dataIndex: "name",
                key: "name",
                render: (v: string) => (
                  <a href={`/api/v1/artifacts/${encodeURIComponent(v)}`} target="_blank" rel="noreferrer">
                    <FileTextOutlined /> {v}
                  </a>
                ),
              },
              { title: "Size", dataIndex: "size", key: "size", width: 110, render: (v: number) => formatBytes(v) },
              { title: "Modified", dataIndex: "mod_time", key: "mod_time", width: 200, render: (v: string) => formatDateTime(v) },
              {
                title: <span className="sr-only">Download</span>,
                key: "dl",
                width: 120,
                render: (_: unknown, row) => (
                  <Button size="small" icon={<DownloadOutlined />} href={`/api/v1/artifacts/${encodeURIComponent(row.name)}?download=1`}>
                    Download
                  </Button>
                ),
              },
            ]}
          />
        </Panel>
      ) : null}

      {canOperate ? <NewRunDrawer open={drawerOpen} onClose={() => setDrawerOpen(false)} backendConfigPath={backendConfigPath} /> : null}
    </div>
  );
}
