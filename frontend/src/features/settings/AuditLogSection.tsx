import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Alert,
  Button,
  Card,
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
  FileTextOutlined,
  ReloadOutlined,
  SearchOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { api } from "../../api/client";
import type { AuditLogEntry } from "../../api/types";
import { notify } from "../../notify";

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

function relativeTime(iso: string): string {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const diff = Date.now() - t;
  const abs = Math.abs(diff);
  const s = Math.floor(abs / 1000);
  if (s < 60) return diff >= 0 ? `${s}s ago` : `in ${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return diff >= 0 ? `${m}m ago` : `in ${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return diff >= 0 ? `${h}h ago` : `in ${h}h`;
  const d = Math.floor(h / 24);
  return diff >= 0 ? `${d}d ago` : `in ${d}d`;
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

export function AuditLogSection({ onError }: Props) {
  const [actionFilter, setActionFilter] = useState<string>("");
  const [onlyFailures, setOnlyFailures] = useState(false);
  const [limit, setLimit] = useState(200);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<AuditLogEntry | null>(null);

  const audit = useQuery({
    queryKey: ["audit", actionFilter, onlyFailures, limit],
    queryFn: () => api.audit({ limit, action: actionFilter || undefined, failures: onlyFailures }),
    staleTime: 5_000,
  });

  if (audit.error) onError(audit.error);

  const data = audit.data;
  const entries: AuditLogEntry[] = data?.entries ?? [];

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return entries;
    return entries.filter((e) => {
      const blob = JSON.stringify(e).toLowerCase();
      return blob.includes(q);
    });
  }, [entries, search]);

  const successCount = filtered.filter((e) => e.success).length;
  const failureCount = filtered.length - successCount;

  const columns: ColumnsType<AuditLogEntry> = [
    {
      title: "When",
      key: "ts",
      width: 150,
      render: (_, row) => (
        <Tooltip title={row.ts}>
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
          <Tag icon={<CheckCircleOutlined />} color="success">success</Tag>
        ) : (
          <Tag icon={<CloseCircleOutlined />} color="error">failed</Tag>
        ),
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
      render: (_, row) => row.auth_mode ? <Tag>{row.auth_mode}</Tag> : <span>—</span>,
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
  ];

  const downloadJsonl = () => {
    const blob = new Blob(
      [filtered.map((e) => JSON.stringify(e)).join("\n") + "\n"],
      { type: "application/x-ndjson" },
    );
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

  const copyEntry = (entry: AuditLogEntry) => {
    void navigator.clipboard.writeText(JSON.stringify(entry, null, 2)).then(() => {
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
          value={actionFilter}
          onChange={setActionFilter}
          options={ACTION_PRESETS}
          style={{ width: 220 }}
        />
        <Select
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
          <Switch checked={onlyFailures} onChange={setOnlyFailures} />
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            Failures only
          </Typography.Text>
        </Space>
        <Input
          allowClear
          prefix={<SearchOutlined />}
          placeholder="Search entries…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{ width: 240 }}
        />
        <Tooltip title="Refresh">
          <Button icon={<ReloadOutlined />} onClick={() => void audit.refetch()} loading={audit.isFetching} />
        </Tooltip>
        <Tooltip title="Download displayed entries as JSONL">
          <Button icon={<DownloadOutlined />} onClick={downloadJsonl} disabled={filtered.length === 0} />
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
          <Tooltip title={data.mod_time}>
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
        <Table
          rowKey={(row) => `${row.ts}-${row.action}-${row.path ?? ""}-${Math.random().toString(36).slice(2, 8)}`}
          columns={columns}
          dataSource={filtered}
          loading={audit.isLoading}
          size="small"
          style={{ marginTop: 12 }}
          pagination={{ defaultPageSize: 25, showSizeChanger: true, pageSizeOptions: [10, 25, 50, 100] }}
          onRow={(row) => ({ onClick: () => setSelected(row), style: { cursor: "pointer" } })}
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
            {JSON.stringify(selected, null, 2)}
          </pre>
        ) : null}
      </Modal>
    </Card>
  );
}
