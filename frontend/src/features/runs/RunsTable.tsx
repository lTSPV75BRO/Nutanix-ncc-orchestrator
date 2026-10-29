import { Table, Tooltip } from "antd";
import type { ColumnsType, TableProps } from "antd/es/table";
import { RightOutlined } from "@ant-design/icons";
import type { RunInfo } from "../../api/types";
import { StatusBadge } from "../../ui";
import { formatDateTime, relativeTime } from "../../utils/datetime";
import { formatDuration } from "../../utils/freshness";
import { runStatusTone } from "../../utils/runStatus";

/** Archived runs (outputDir/runs/<id>) have a detail page; the "latest" and trigger rows do not. */
export function hasRunDetail(r: RunInfo): boolean {
  return r.source === "history";
}

function trigger(r: RunInfo): string {
  const src = (r.run_source || "").toLowerCase();
  if (src === "scheduled") return "Scheduled";
  if (src === "manual") return "Manual";
  if (src === "external") return "External";
  if (r.source === "trigger") return "Manual";
  return "—";
}

function findings(r: RunInfo): string {
  if (r.source === "trigger" || (r.fail_total == null && r.warn_total == null && r.err_total == null)) return "—";
  const fail = (r.fail_total ?? 0) + (r.err_total ?? 0);
  const warn = r.warn_total ?? 0;
  if (fail === 0 && warn === 0) return "None";
  return [fail ? `${fail} fail` : "", warn ? `${warn} warn` : ""].filter(Boolean).join(" · ");
}

/**
 * Run history table shared by the Overview (last few) and the Runs page.
 * Rows with a detail page are clickable (and keyboard-activatable).
 */
export function RunsTable({
  runs,
  loading,
  onOpen,
  pagination = false,
  empty,
}: {
  runs: RunInfo[];
  loading?: boolean;
  onOpen?: (r: RunInfo) => void;
  pagination?: TableProps<RunInfo>["pagination"];
  empty?: React.ReactNode;
}) {
  const columns: ColumnsType<RunInfo> = [
    {
      title: "Started",
      key: "started",
      render: (_: unknown, r) => {
        const at = r.timestamp || r.mod_time;
        return (
          <span className="runs-started">
            <Tooltip title={formatDateTime(at)}>{relativeTime(at)}</Tooltip>
            {r.source === "summary" ? <span className="ui-field-help"> · latest</span> : null}
          </span>
        );
      },
    },
    {
      title: "Status",
      key: "status",
      render: (_: unknown, r) => {
        const st = runStatusTone(r);
        const badge = <StatusBadge tone={st.tone}>{st.label}</StatusBadge>;
        return st.hint ? <Tooltip title={st.hint}>{badge}</Tooltip> : badge;
      },
    },
    { title: "Trigger", key: "trigger", render: (_: unknown, r) => trigger(r) },
    { title: "Duration", key: "duration", render: (_: unknown, r) => (r.source === "trigger" ? "—" : formatDuration(r.duration_s)) },
    {
      title: "Clusters",
      key: "clusters",
      render: (_: unknown, r) => {
        const ok = r.clusters_ok ?? 0;
        const total = ok + (r.clusters_failed ?? 0);
        return r.source === "trigger" || total === 0 ? "—" : `${ok}/${total}`;
      },
    },
    { title: "Findings", key: "findings", render: (_: unknown, r) => findings(r) },
    {
      title: <span className="sr-only">Open</span>,
      key: "open",
      width: 40,
      render: (_: unknown, r) => (onOpen && hasRunDetail(r) ? <RightOutlined className="runs-open" aria-hidden="true" /> : null),
    },
  ];

  return (
    <Table<RunInfo>
      className="runs-table"
      size="middle"
      rowKey={(r) => `${r.source ?? ""}:${r.id}`}
      loading={loading}
      pagination={pagination}
      dataSource={runs}
      columns={columns}
      locale={empty ? { emptyText: empty } : undefined}
      scroll={{ x: 680 }}
      rowClassName={(r) => (onOpen && hasRunDetail(r) ? "is-clickable" : "")}
      onRow={(r) =>
        onOpen && hasRunDetail(r)
          ? {
              onClick: () => onOpen(r),
              onKeyDown: (e) => {
                if (e.key === "Enter") onOpen(r);
              },
              tabIndex: 0,
              "aria-label": `Open run ${r.id}`,
            }
          : {}
      }
    />
  );
}
