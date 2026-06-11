import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Alert,
  Button,
  Card,
  DatePicker,
  Empty,
  Input,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  CopyOutlined,
  DownloadOutlined,
  FileExcelOutlined,
  FileTextOutlined,
  ReloadOutlined,
  SearchOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import dayjs, { type Dayjs } from "dayjs";
import { api, type AuditQuery } from "../../api/client";
import type { AuditLogEntry } from "../../api/types";
import { notify } from "../../notify";
import { formatDateTime, relativeTime } from "../../utils/datetime";

type Props = {
  onError: (e: unknown) => void;
};

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = n;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i += 1;
  }
  return `${value.toFixed(value >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}


const ACTION_PRESETS: { label: string; value: string }[] = [
  { label: "All actions", value: "" },
  { label: "Settings (config / files)", value: "settings" },
  { label: "Schedule changes", value: "schedule" },
  { label: "Runs (preflight + trigger)", value: "runs" },
];

function actionTone(action: string): "blue" | "green" | "orange" | "purple" | "default" {
  if (action.startsWith("runs.")) return "blue";
  if (action.startsWith("settings.")) return "green";
  if (action.startsWith("schedule.")) return "orange";
  if (action.startsWith("auth.")) return "purple";
  return "default";
}

type AuditRow = AuditLogEntry & { __idx: number };

export function AuditLogSection({ onError }: Props) {
  const [actionFilter, setActionFilter] = useState<string>("");
  const [onlyFailures, setOnlyFailures] = useState(false);
  const [limit, setLimit] = useState(200);
  const [search, setSearch] = useState("");
  const [userFilter, setUserFilter] = useState("");
  const [range, setRange] = useState<[Dayjs | null, Dayjs | null] | null>(null);
  const [exporting, setExporting] = useState(false);
  const [selected, setSelected] = useState<AuditLogEntry | null>(null);

  const since = range?.[0] ? range[0].format("YYYY-MM-DD") : undefined;
  const until = range?.[1] ? range[1].format("YYYY-MM-DD") : undefined;

  const queryParams: AuditQuery = {
    limit,
    action: actionFilter || undefined,
    failures: onlyFailures,
    user: userFilter.trim() || undefined,
    since,
    until,
  };

  const audit = useQuery({
    queryKey: ["audit", actionFilter, onlyFailures, limit, userFilter.trim(), since, until],
    queryFn: () => api.audit(queryParams),
    staleTime: 5_000,
  });

  // Surface load errors exactly once per error change, not on every render
  // (otherwise notifyError fires for every re-render in an infinite spiral).
  useEffect(() => {
    if (audit.error) onError(audit.error);
  }, [audit.error, onError]);

  const data = audit.data;
  const entries = useMemo<AuditRow[]>(() => {
    const list = (data?.entries ?? []) as AuditLogEntry[];
    // Tag with a stable index so the Table can use a deterministic rowKey
    // (audit lines aren't unique by ts/action alone).
    return list.map((e, idx) => ({ ...e, __idx: idx }));
  }, [data]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return entries;
    return entries.filter((e) => JSON.stringify(e).toLowerCase().includes(q));
  }, [entries, search]);

  const successCount = filtered.filter((e) => e.success).length;
  const failureCount = filtered.length - successCount;

  const columns = useMemo<ColumnsType<AuditRow>>(
    () => [
      {
        title: "When",
        key: "ts",
        width: 150,
        render: (_, row) => (
          <Tooltip title={formatDateTime(row.ts)}>
            <Typography.Text style={{ fontSize: 12 }}>{relativeTime(row.ts)}</Typography.Text>
          </Tooltip>
        ),
      },
      {
        title: "Action",
        key: "action",
        render: (_, row) => <Tag color={actionTone(row.action)}>{row.action}</Tag>,
      },
      {
        title: "Result",
        key: "success",
        width: 110,
        align: "center",
        render: (_, row) =>
          row.success ? (
            <Tag icon={<CheckCircleOutlined />} color="success">
              success
            </Tag>
          ) : (
            <Tag icon={<CloseCircleOutlined />} color="error">
              failed
            </Tag>
          ),
      },
      {
        title: "User",
        key: "user",
        width: 150,
        render: (_, row) => {
          const user = (row.user ?? row.username) as string | undefined;
          const role = row.role as string | undefined;
          if (!user) return <span>—</span>;
          return (
            <Space size={4} wrap>
              <Typography.Text style={{ fontSize: 12 }}>{user}</Typography.Text>
              {role ? (
                <Tag color={role === "admin" ? "gold" : role === "operator" ? "blue" : "default"}>{role}</Tag>
              ) : null}
            </Space>
          );
        },
      },
      {
        title: "Client",
        key: "client",
        width: 150,
        render: (_, row) => (
          <Typography.Text type="secondary" className="mono" style={{ fontSize: 12 }}>
            {row.client || "—"}
          </Typography.Text>
        ),
      },
      {
        title: "Auth",
        key: "auth_mode",
        width: 90,
        render: (_, row) => (row.auth_mode ? <Tag>{row.auth_mode}</Tag> : <span>—</span>),
      },
      {
        title: "Endpoint",
        key: "path",
        ellipsis: true,
        render: (_, row) => (
          <Typography.Text type="secondary" className="mono" style={{ fontSize: 12 }}>
            {row.method ? `${row.method} ` : ""}
            {row.path || "—"}
          </Typography.Text>
        ),
      },
    ],
    [],
  );

  const downloadJsonl = () => {
    const lines = filtered.map(({ __idx: _omit, ...rest }) => JSON.stringify(rest));
    const blob = new Blob([lines.join("\n") + (lines.length ? "\n" : "")], {
      type: "application/x-ndjson",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `audit-${new Date().toISOString().replace(/[:.]/g, "-")}.jsonl`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    notify.success("Audit entries exported as JSONL.");
  };

  const exportCsv = async () => {
    setExporting(true);
    try {
      const csv = await api.auditExportCSV({ ...queryParams, limit: Math.max(limit, 1000) });
      const blob = new Blob([csv], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `audit-${new Date().toISOString().replace(/[:.]/g, "-")}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      notify.success("Audit entries exported as CSV (server-side filtered).");
    } catch (e) {
      onError(e);
    } finally {
      setExporting(false);
    }
  };

  const copyEntry = (entry: AuditLogEntry) => {
    const { __idx: _omit, ...rest } = entry as AuditRow;
    void navigator.clipboard.writeText(JSON.stringify(rest, null, 2)).then(() => {
      notify.success("Audit entry copied to clipboard.");
    });
  };

  return (
    <Card className="page-card">
      <Space size={12} align="center" style={{ marginBottom: 8 }}>
        <span className="config-section-icon">
          <FileTextOutlined />
        </span>
        <div>
          <Typography.Title level={4} style={{ margin: 0 }}>
            Audit Log
          </Typography.Title>
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            Persisted JSONL trail of config saves, schedule changes, and run triggers. Useful for change reviews and support tickets.
          </Typography.Text>
        </div>
      </Space>

      <Space size={[8, 8]} wrap style={{ marginTop: 12, width: "100%" }}>
        <Select
          id="audit-action-filter"
          aria-label="Filter audit entries by action"
          value={actionFilter}
          onChange={setActionFilter}
          options={ACTION_PRESETS}
          style={{ width: 220 }}
        />
        <Select
          id="audit-limit"
          aria-label="Audit entries limit"
          value={limit}
          onChange={setLimit}
          options={[
            { label: "Last 50", value: 50 },
            { label: "Last 200", value: 200 },
            { label: "Last 500", value: 500 },
            { label: "Last 1000", value: 1000 },
          ]}
          style={{ width: 140 }}
        />
        <Space size={6} align="center">
          <Switch id="audit-failures-only" aria-label="Show failures only" checked={onlyFailures} onChange={setOnlyFailures} />
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            <label htmlFor="audit-failures-only">Failures only</label>
          </Typography.Text>
        </Space>
        <Input
          id="audit-user-filter"
          name="audit-user-filter"
          aria-label="Filter by user"
          allowClear
          placeholder="User (exact)"
          value={userFilter}
          onChange={(e) => setUserFilter(e.target.value)}
          style={{ width: 160 }}
          autoComplete="off"
        />
        <DatePicker.RangePicker
          aria-label="Filter by date range"
          value={range as never}
          onChange={(v) => setRange(v as [Dayjs | null, Dayjs | null] | null)}
          allowEmpty={[true, true]}
          disabledDate={(d) => d && d.isAfter(dayjs().endOf("day"))}
          style={{ width: 250 }}
        />
        <Input
          id="audit-search"
          name="audit-search"
          aria-label="Search audit entries (client-side)"
          allowClear
          prefix={<SearchOutlined />}
          placeholder="Search shown…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{ width: 200 }}
          autoComplete="off"
        />
        <Tooltip title="Refresh">
          <Button icon={<ReloadOutlined />} onClick={() => void audit.refetch()} loading={audit.isFetching} />
        </Tooltip>
        <Tooltip title="Download displayed entries as JSONL">
          <Button icon={<DownloadOutlined />} onClick={downloadJsonl} disabled={filtered.length === 0} />
        </Tooltip>
        <Tooltip title="Export server-side filtered entries as CSV">
          <Button icon={<FileExcelOutlined />} onClick={() => void exportCsv()} loading={exporting}>
            CSV
          </Button>
        </Tooltip>
      </Space>

      <Space size={[8, 8]} wrap style={{ marginTop: 12 }}>
        <Tag color="processing">{filtered.length} shown</Tag>
        <Tag color="success">{successCount} success</Tag>
        <Tag color="error">{failureCount} failed</Tag>
        {data?.path ? (
          <Tooltip title={data.path}>
            <Tag>
              File: <Typography.Text className="mono" style={{ fontSize: 11 }} copyable={{ text: data.path }}>{data.path}</Typography.Text>
            </Tag>
          </Tooltip>
        ) : null}
        {data ? <Tag>Size: {formatBytes(data.size || 0)}</Tag> : null}
        {data?.mod_time ? (
          <Tooltip title={formatDateTime(data.mod_time)}>
            <Tag>Last entry: {relativeTime(data.mod_time)}</Tag>
          </Tooltip>
        ) : null}
      </Space>

      {audit.isError ? (
        <Alert
          type="error"
          showIcon
          style={{ marginTop: 12 }}
          message="Could not load audit entries"
          description={String(audit.error)}
        />
      ) : null}

      {!audit.isLoading && filtered.length === 0 ? (
        <Empty
          style={{ marginTop: 24 }}
          description={
            entries.length === 0
              ? "No audit entries yet. Trigger a run or save a config change to populate this log."
              : "No entries match your filters."
          }
        />
      ) : (
        <Table<AuditRow>
          rowKey="__idx"
          columns={columns}
          dataSource={filtered}
          loading={audit.isLoading}
          size="small"
          style={{ marginTop: 12 }}
          pagination={{
            defaultPageSize: 25,
            showSizeChanger: true,
            pageSizeOptions: [10, 25, 50, 100],
          }}
          onRow={(row) => ({
            onClick: () => setSelected(row),
            style: { cursor: "pointer" },
          })}
        />
      )}

      <Modal
        title={selected ? `Audit entry · ${selected.action}` : "Audit entry"}
        open={Boolean(selected)}
        onCancel={() => setSelected(null)}
        footer={
          selected
            ? [
                <Button key="copy" icon={<CopyOutlined />} onClick={() => copyEntry(selected)}>
                  Copy JSON
                </Button>,
                <Button key="close" type="primary" onClick={() => setSelected(null)}>
                  Close
                </Button>,
              ]
            : null
        }
        width={640}
      >
        {selected ? (
          <pre
            style={{
              margin: 0,
              padding: 12,
              borderRadius: 6,
              background: "var(--surface-muted, rgba(0,0,0,0.04))",
              fontSize: 12,
              maxHeight: 420,
              overflow: "auto",
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
            }}
          >
            {(() => {
              const { __idx: _omit, ...rest } = selected as AuditRow;
              return JSON.stringify(rest, null, 2);
            })()}
          </pre>
        ) : null}
      </Modal>
    </Card>
  );
}
