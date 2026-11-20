import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Card, DatePicker, Drawer, Dropdown, Empty, Input, Select, Space, Switch, Table, Tooltip, Typography } from "antd";
import { CodeOutlined, CopyOutlined, DownloadOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import dayjs, { type Dayjs } from "dayjs";
import { useSearchParams } from "react-router";
import { api, type AuditQuery } from "../../api/client";
import type { AuditLogEntry } from "../../api/types";
import { notify } from "../../notify";
import { formatDateTime, relativeTime } from "../../utils/datetime";
import { StatusBadge } from "../../ui";

type Props = {
  onError: (e: unknown) => void;
};

/** Records the server writes by itself for every request; hidden unless asked for. */
const AUTOMATIC_ACTIONS = ["http.request", "mcp.call"];

const ACTION_PRESETS: { label: string; value: string }[] = [
  { label: "All changes", value: "" },
  { label: "Sign-ins and tokens", value: "auth." },
  { label: "Settings", value: "settings." },
  { label: "Users and access", value: "settings.users." },
  { label: "Config files", value: "settings.config" },
  { label: "Backups", value: "settings.backup." },
  { label: "Notifications", value: "settings.notifications." },
  { label: "Updates", value: "settings.update" },
  { label: "Schedules", value: "schedule." },
  { label: "Runs", value: "runs." },
  { label: "Health and repairs", value: "health.diagnostics." },
  { label: "Blocked dashboard versions", value: "ui.blocked" },
  { label: "MCP assistant calls", value: "mcp.call" },
];

type AuditRow = AuditLogEntry & { __idx: number };

/** The entry as stored: no UI bookkeeping, pretty-printed. */
function entryJson(entry: AuditLogEntry): string {
  const { __idx: _omit, ...rest } = entry as AuditRow;
  return JSON.stringify(rest, null, 2);
}

function userOf(row: AuditLogEntry): string {
  return String(row.user ?? row.username ?? "");
}

export function AuditLogSection({ onError }: Props) {
  const [searchParams, setSearchParams] = useSearchParams();
  const [actionFilter, setActionFilter] = useState<string>("");
  const [onlyFailures, setOnlyFailures] = useState(false);
  const [showAutomatic, setShowAutomatic] = useState(false);
  const [limit, setLimit] = useState(200);
  const [search, setSearch] = useState("");
  const [userFilter, setUserFilter] = useState("");
  const [range, setRange] = useState<[Dayjs | null, Dayjs | null] | null>(null);
  const [selected, setSelected] = useState<AuditLogEntry | null>(null);

  useEffect(() => {
    const action = (searchParams.get("action") || "").trim();
    const user = (searchParams.get("user") || "").trim();
    const searchText = (searchParams.get("search") || "").trim();
    const failures = (searchParams.get("failures") || "").trim() === "1";
    if (action && action !== actionFilter) setActionFilter(action);
    if (user && user !== userFilter) setUserFilter(user);
    if (searchText && searchText !== search) setSearch(searchText);
    if (failures && failures !== onlyFailures) setOnlyFailures(true);
    // Intentionally on mount/param change only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const since = range?.[0] ? range[0].format("YYYY-MM-DD") : undefined;
  const until = range?.[1] ? range[1].format("YYYY-MM-DD") : undefined;

  // Asking for an automatic record by name always shows it.
  const explicitAutomatic = AUTOMATIC_ACTIONS.some((a) => actionFilter.startsWith(a));
  const exclude = showAutomatic || explicitAutomatic ? undefined : AUTOMATIC_ACTIONS.join(",");

  const queryParams: AuditQuery = {
    limit,
    action: actionFilter || undefined,
    exclude,
    failures: onlyFailures,
    user: userFilter.trim() || undefined,
    since,
    until,
  };

  const audit = useQuery({
    queryKey: ["audit", actionFilter, onlyFailures, limit, userFilter.trim(), since, until, exclude ?? ""],
    queryFn: () => api.audit(queryParams),
    staleTime: 5_000,
  });

  useEffect(() => {
    const next = new URLSearchParams(searchParams);
    if (actionFilter) next.set("action", actionFilter);
    else next.delete("action");
    if (userFilter.trim()) next.set("user", userFilter.trim());
    else next.delete("user");
    if (onlyFailures) next.set("failures", "1");
    else next.delete("failures");
    if (search.trim()) next.set("search", search.trim());
    else next.delete("search");
    if (next.toString() !== searchParams.toString()) setSearchParams(next, { replace: true });
  }, [actionFilter, onlyFailures, userFilter, search, searchParams, setSearchParams]);

  // Surface load errors once per error change, not on every render.
  useEffect(() => {
    if (audit.error) onError(audit.error);
  }, [audit.error, onError]);

  const data = audit.data;
  const entries = useMemo<AuditRow[]>(() => {
    const list = (data?.entries ?? []) as AuditLogEntry[];
    // A stable index gives the Table a deterministic rowKey (entries are not
    // unique by time and action alone).
    return list.map((e, idx) => ({ ...e, __idx: idx }));
  }, [data]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return entries;
    return entries.filter((e) => JSON.stringify(e).toLowerCase().includes(q));
  }, [entries, search]);

  const failureCount = filtered.filter((e) => !e.success).length;

  const columns = useMemo<ColumnsType<AuditRow>>(
    () => [
      {
        title: "When",
        key: "ts",
        width: 130,
        render: (_, row) => (
          <Tooltip title={formatDateTime(row.ts)}>
            <span className="audit-when">{relativeTime(row.ts)}</span>
          </Tooltip>
        ),
      },
      {
        title: "Action",
        key: "action",
        render: (_, row) => <span className="mono audit-action">{row.action}</span>,
      },
      {
        title: "Result",
        key: "success",
        width: 120,
        render: (_, row) =>
          row.success ? <StatusBadge tone="ok">Succeeded</StatusBadge> : <StatusBadge tone="fail">Failed</StatusBadge>,
      },
      {
        title: "User",
        key: "user",
        width: 170,
        render: (_, row) => {
          const user = userOf(row);
          if (!user) return <span className="audit-muted">—</span>;
          return (
            <span className="audit-user">
              {user}
              {row.role ? <span className="audit-muted"> · {String(row.role)}</span> : null}
            </span>
          );
        },
      },
      {
        title: "Client",
        key: "client",
        width: 140,
        render: (_, row) => <span className="mono audit-muted">{row.client || "—"}</span>,
      },
      {
        title: <span className="visually-hidden">Details</span>,
        key: "json",
        width: 52,
        align: "right",
        render: (_, row) => (
          <Tooltip title="View JSON">
            <Button
              type="text"
              size="small"
              icon={<CodeOutlined />}
              aria-label={`View JSON for ${row.action}`}
              onClick={(e) => {
                e.stopPropagation();
                setSelected(row);
              }}
            />
          </Tooltip>
        ),
      },
    ],
    [],
  );

  const downloadJsonl = () => {
    const lines = filtered.map((e) => JSON.stringify(JSON.parse(entryJson(e))));
    const blob = new Blob([lines.join("\n") + (lines.length ? "\n" : "")], { type: "application/x-ndjson" });
    saveBlob(blob, `audit-${stamp()}.jsonl`);
    notify.success("Audit entries exported as JSONL.");
  };

  const exportCsv = async () => {
    try {
      const csv = await api.auditExportCSV({ ...queryParams, limit: Math.max(limit, 1000) });
      saveBlob(new Blob([csv], { type: "text/csv" }), `audit-${stamp()}.csv`);
      notify.success("Audit entries exported as CSV.");
    } catch (e) {
      onError(e);
    }
  };

  const copyText = (text: string, done: string) => {
    void navigator.clipboard.writeText(text).then(() => notify.success(done));
  };

  const requestId = selected ? String(selected.request_id ?? "") : "";

  return (
    <Card className="page-card">
      <div className="config-section-head">
        <h2 className="ui-panel-title">Audit log</h2>
        <p className="ui-panel-desc">
          Who changed what, newest first. Select a row, or use the code button, to see the full JSON entry.
        </p>
      </div>

      <div className="audit-toolbar">
        <Select
          id="audit-action-filter"
          aria-label="Filter audit entries by action"
          value={actionFilter}
          onChange={setActionFilter}
          options={ACTION_PRESETS}
          style={{ width: 220 }}
        />
        <Input
          id="audit-user-filter"
          name="audit-user-filter"
          aria-label="Filter by user"
          allowClear
          placeholder="User"
          value={userFilter}
          onChange={(e) => setUserFilter(e.target.value)}
          style={{ width: 140 }}
          autoComplete="off"
        />
        <DatePicker.RangePicker
          aria-label="Filter by date range"
          value={range as never}
          onChange={(v) => setRange(v as [Dayjs | null, Dayjs | null] | null)}
          allowEmpty={[true, true]}
          disabledDate={(d) => d && d.isAfter(dayjs().endOf("day"))}
          style={{ width: 240 }}
        />
        <Input
          id="audit-search"
          name="audit-search"
          aria-label="Search the entries shown"
          allowClear
          prefix={<SearchOutlined />}
          placeholder="Search"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{ width: 170 }}
          autoComplete="off"
        />
        <span className="audit-switch">
          <Switch id="audit-failures-only" size="small" aria-label="Failures only" checked={onlyFailures} onChange={setOnlyFailures} />
          <label htmlFor="audit-failures-only">Failures only</label>
        </span>
        <Tooltip title="Every API request and MCP call the server records on its own. Off by default.">
          <span className="audit-switch">
            <Switch id="audit-automatic" size="small" aria-label="Show automatic request records" checked={showAutomatic} onChange={setShowAutomatic} />
            <label htmlFor="audit-automatic">Automatic requests</label>
          </span>
        </Tooltip>
        <span className="audit-toolbar-end">
          <Select
            id="audit-limit"
            aria-label="Number of entries"
            value={limit}
            onChange={setLimit}
            options={[50, 200, 500, 1000].map((n) => ({ label: `Last ${n}`, value: n }))}
            style={{ width: 110 }}
          />
          <Tooltip title="Refresh">
            <Button icon={<ReloadOutlined />} aria-label="Refresh" onClick={() => void audit.refetch()} loading={audit.isFetching} />
          </Tooltip>
          <Dropdown
            menu={{
              items: [
                { key: "jsonl", label: "Shown entries (JSONL)", disabled: filtered.length === 0, onClick: downloadJsonl },
                { key: "csv", label: "Filtered on the server (CSV)", onClick: () => void exportCsv() },
              ],
            }}
          >
            <Button icon={<DownloadOutlined />}>Export</Button>
          </Dropdown>
        </span>
      </div>

      <p className="audit-summary">
        {filtered.length} {filtered.length === 1 ? "entry" : "entries"}
        {failureCount > 0 ? <span className="audit-failed"> · {failureCount} failed</span> : null}
        {data?.mod_time ? <span className="audit-muted"> · last written {relativeTime(data.mod_time)}</span> : null}
      </p>

      {audit.isError ? (
        <Alert type="error" showIcon title="Couldn't load audit entries" description={String(audit.error)} />
      ) : null}

      {!audit.isLoading && filtered.length === 0 ? (
        <Empty
          description={
            entries.length === 0
              ? "Nothing recorded yet. Save a setting or start a run and it appears here."
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
          className="audit-table"
          pagination={{ defaultPageSize: 25, showSizeChanger: true, pageSizeOptions: [10, 25, 50, 100] }}
          onRow={(row) => ({
            onClick: () => setSelected(row),
            style: { cursor: "pointer" },
          })}
        />
      )}

      <Drawer
        title={selected ? <span className="mono">{selected.action}</span> : "Audit entry"}
        open={Boolean(selected)}
        onClose={() => setSelected(null)}
        size={560}
        extra={
          selected ? (
            <Space>
              {requestId ? (
                <Button size="small" onClick={() => copyText(requestId, "Request ID copied.")}>
                  Copy request ID
                </Button>
              ) : null}
              <Button size="small" icon={<CopyOutlined />} onClick={() => copyText(entryJson(selected), "Audit entry copied.")}>
                Copy JSON
              </Button>
            </Space>
          ) : null
        }
      >
        {selected ? (
          <>
            <dl className="audit-facts">
              <div>
                <dt>When</dt>
                <dd>{formatDateTime(selected.ts)}</dd>
              </div>
              <div>
                <dt>Result</dt>
                <dd>
                  {selected.success ? <StatusBadge tone="ok">Succeeded</StatusBadge> : <StatusBadge tone="fail">Failed</StatusBadge>}
                </dd>
              </div>
              <div>
                <dt>User</dt>
                <dd>{userOf(selected) || "—"}</dd>
              </div>
              <div>
                <dt>Client</dt>
                <dd className="mono">{selected.client || "—"}</dd>
              </div>
            </dl>
            {!selected.success && (selected.error || selected.error_code) ? (
              <Alert
                type="error"
                showIcon
                title={String(selected.error || selected.error_code)}
                description={selected.remediation ? String(selected.remediation) : selected.error_code ? `Error code ${String(selected.error_code)}` : undefined}
                style={{ marginBottom: 12 }}
              />
            ) : null}
            <Typography.Text type="secondary" className="audit-json-label">
              Full entry
            </Typography.Text>
            <pre className="audit-json" tabIndex={0} aria-label="Audit entry as JSON">
              {entryJson(selected)}
            </pre>
          </>
        ) : null}
      </Drawer>
    </Card>
  );
}

function stamp(): string {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
