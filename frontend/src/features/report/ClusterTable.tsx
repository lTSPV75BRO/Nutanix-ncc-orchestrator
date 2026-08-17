import { useEffect, useMemo, useState } from "react";
import {
  Button,
  Card,
  Drawer,
  Empty,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  CopyOutlined,
  ExpandAltOutlined,
  LinkOutlined,
  CheckOutlined,
} from "@ant-design/icons";
import { asArray, asRecord, displayClusterName, resolveClusterName } from "../../utils/report";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify } from "../../notify";

type Props = {
  checksSnapshot: unknown;
  aggRows?: unknown[];
  diffFlags?: Record<string, unknown>;
  flakyKeys?: Record<string, unknown>;
  nccLogs?: Array<{ name: string; path: string }>;
  filterText: string;
  selectedClusters: string[];
  clusterNameMap: Record<string, string>;
  severityFilters: Array<"FAIL" | "WARN" | "ERR" | "INFO">;
  compareMode: "all" | "changed" | "flaky";
  onSummaryChange?: (summary: {
    total: number;
    fail: number;
    err: number;
    warn: number;
    info: number;
    unknown: number;
  }) => void;
};

type Severity = "FAIL" | "WARN" | "ERR" | "INFO" | "UNKNOWN";
type Density = "compact" | "comfortable";

type RowRecord = {
  key: string;
  clusterName: string;
  cluster: string;
  alert: string;
  severity: Severity;
  isUnknownSeverity: boolean;
  detail: string;
  kb: string;
  clusterVersion: string;
  nccVersion: string;
  isChanged: boolean;
  isFlaky: boolean;
  logName: string;
  logPath: string;
};

const SEVERITY_TAG_COLOR: Record<Severity, string> = {
  FAIL: "error",
  WARN: "warning",
  ERR: "volcano",
  INFO: "processing",
  UNKNOWN: "default",
};

function rowKey(cluster: string, check: string): string {
  return `${(cluster || "").trim().toLowerCase()}||${(check || "").trim().toLowerCase()}`;
}

function parseKB(detail: string): string {
  const m = (detail || "").match(/(https?:\/\/[^\s)]+portal\.nutanix\.com\/kb\/\d+|https?:\/\/[^\s)]+)/i);
  return m ? m[1] : "";
}

function kbLabel(url: string): string {
  const m = url.match(/\/kb\/(\d+)/i);
  return m ? `KB ${m[1]}` : "KB";
}

function normalizeCheckTitle(check: string): string {
  return check.replace(/^detailed information for\s*/i, "").replace(/:$/, "").trim();
}

function isIPv4Like(value: string): boolean {
  return /^\d{1,3}(?:\.\d{1,3}){3}$/.test(value);
}

function normalizeClusterHost(cluster: string): string {
  let clean = cluster.trim();
  if (!clean) return "";
  clean = clean.replace(/^https?:\/\//i, "");
  clean = clean.replace(/\/+$/, "");
  clean = clean.replace(/:\d+$/, "");
  return clean;
}

function clusterPrismURL(cluster: string, clusterNameMap: Record<string, string>): string {
  const raw = cluster.trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  const clean = normalizeClusterHost(raw);
  if (isIPv4Like(clean)) return `https://${clean}:9440`;
  const mappedIP = Object.entries(clusterNameMap).find(([key, name]) => isIPv4Like(key) && name === raw)?.[0];
  if (mappedIP) return `https://${mappedIP}:9440`;
  return `https://${clean}:9440`;
}

function matchingLogForCluster(
  logs: Array<{ name: string; path: string }>,
  cluster: string,
): { name: string; path: string } | undefined {
  const key = (cluster || "").trim();
  if (!key) return undefined;
  return logs.find((l) => l.name.includes(key) || l.path.includes(key));
}

function previewDetail(detail: string): string {
  if (!detail) return "—";
  return detail
    .replace(/\s+/g, " ")
    .replace(/^node\s+\S+:\s*/i, "")
    .trim()
    .slice(0, 220);
}

export function ClusterTable({
  checksSnapshot,
  aggRows,
  diffFlags,
  flakyKeys,
  nccLogs,
  filterText,
  selectedClusters,
  clusterNameMap,
  severityFilters,
  compareMode,
  onSummaryChange,
}: Props) {
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useLocalStorageState("dashboard.alerts.rowsPerPage", 100);
  const [density, setDensity] = useLocalStorageState<Density>("dashboard.alerts.density", "comfortable");
  const [drawerRow, setDrawerRow] = useState<RowRecord | null>(null);
  const [copyState, setCopyState] = useState<"idle" | "copied">("idle");

  const rows = useMemo<RowRecord[]>(() => {
    const needle = filterText.toLowerCase();
    const parsedTokens = {
      sev: (() => {
        const m = needle.match(/\b(?:sev|severity):([a-z]+)/);
        return m ? m[1].toUpperCase() : "";
      })(),
      cluster: (() => {
        const m = needle.match(/\bcluster:([^\s]+)/);
        return m ? m[1] : "";
      })(),
      changed: /\bchanged:(true|1|yes)\b/.test(needle),
      flaky: /\bflaky:(true|1|yes)\b/.test(needle),
      terms: needle
        .split(/\s+/)
        .filter(Boolean)
        .filter((t) => !/^(sev|severity|cluster|changed|flaky):/.test(t)),
    };
    const selectedSet = new Set((selectedClusters || []).map((v) => v.toLowerCase()));
    const dflags = diffFlags || {};
    const fkeys = flakyKeys || {};
    const pickDiff = (cluster: string, alert: string) => {
      const keys = [rowKey(cluster, alert)];
      for (const k of keys) {
        const diff = asRecord(dflags[k]);
        if (Object.keys(diff).length > 0) return diff;
      }
      return {};
    };
    const mapToRow = (raw: Record<string, unknown>, idx: number): RowRecord => {
      const cluster = String(raw.cluster || raw.address || "-");
      const alert = normalizeCheckTitle(String(raw.check_name ?? raw.check ?? "-"));
      const severityRaw = String(raw.severity || "UNKNOWN").toUpperCase();
      const isUnknownSeverity = !["FAIL", "WARN", "ERR", "INFO"].includes(severityRaw);
      const severity = (isUnknownSeverity ? "ERR" : severityRaw) as Severity;
      const detail = String(raw.detail || "");
      const diff = pickDiff(cluster, alert);
      const isChanged = Boolean(diff.new_fail || diff.resolved_fail || diff.severity_changed);
      const isFlaky = Boolean(fkeys[rowKey(cluster, alert)]);
      const log = matchingLogForCluster(nccLogs || [], cluster);
      return {
        key: `${cluster}-${alert}-${idx}`,
        clusterName: resolveClusterName(displayClusterName(raw), clusterNameMap),
        cluster,
        alert,
        severity,
        isUnknownSeverity,
        detail,
        kb: parseKB(detail),
        clusterVersion: String(raw.clusterVersion || raw.cluster_version || ""),
        nccVersion: String(raw.nccVersion || raw.ncc_version || ""),
        isChanged,
        isFlaky,
        logName: log?.name || "",
        logPath: log?.path || "",
      };
    };

    let baseRows: RowRecord[] = [];
    const baseAgg = (aggRows || []).map((r) => asRecord(r));
    if (baseAgg.length > 0) {
      baseRows = baseAgg.map((r, idx) => mapToRow(r, idx));
    } else {
      const snapshot = asRecord(checksSnapshot);
      const legacyClusters = asArray(snapshot.clusters).map((c) => asRecord(c));
      if (legacyClusters.length > 0) {
        baseRows = legacyClusters.flatMap((cluster, cidx) =>
          asArray(cluster.checks).map((chk, idx) =>
            mapToRow(
              asRecord({
                ...asRecord(chk),
                cluster: displayClusterName(cluster),
                detail: asRecord(chk).detail || "",
              }),
              cidx * 10000 + idx,
            ),
          ),
        );
      } else {
        baseRows = asArray(checksSnapshot).map((r, idx) => mapToRow(asRecord(r), idx));
      }
    }

    return baseRows.filter((r) => {
      if (severityFilters.length > 0 && !severityFilters.includes(r.severity as "FAIL" | "WARN" | "ERR" | "INFO")) return false;
      if (parsedTokens.sev && r.severity !== parsedTokens.sev) return false;
      if (parsedTokens.cluster && !`${r.clusterName} ${r.cluster}`.toLowerCase().includes(parsedTokens.cluster)) return false;
      if (
        selectedSet.size > 0 &&
        !selectedSet.has(r.clusterName.toLowerCase()) &&
        !selectedSet.has(resolveClusterName(r.cluster, clusterNameMap).toLowerCase())
      )
        return false;
      if (parsedTokens.changed && !r.isChanged) return false;
      if (parsedTokens.flaky && !r.isFlaky) return false;
      if (compareMode === "changed" && !r.isChanged) return false;
      if (compareMode === "flaky" && !r.isFlaky) return false;
      const hay = `${r.clusterName} ${r.cluster} ${r.severity} ${r.alert}`.toLowerCase();
      return parsedTokens.terms.every((t) => hay.includes(t));
    });
  }, [
    checksSnapshot,
    aggRows,
    diffFlags,
    flakyKeys,
    nccLogs,
    filterText,
    selectedClusters,
    clusterNameMap,
    severityFilters,
    compareMode,
  ]);

  const severityCounts = useMemo(() => {
    const counts: Record<Severity, number> = { FAIL: 0, WARN: 0, ERR: 0, INFO: 0, UNKNOWN: 0 };
    rows.forEach((r) => {
      if (r.isUnknownSeverity) counts.UNKNOWN += 1;
      counts[r.severity] = (counts[r.severity] || 0) + 1;
    });
    return counts;
  }, [rows]);

  // Priority order: FAIL, then WARN (ahead of ERR — a warning on a known
  // check is generally more actionable than an unclassified runtime error),
  // then ERR, then INFO, then UNKNOWN lowest. Keep in sync with
  // DashboardPage's SEVERITY_META ordering.
  const severityRank: Record<Severity, number> = { FAIL: 5, WARN: 4, ERR: 3, INFO: 2, UNKNOWN: 1 };

  const columns: ColumnsType<RowRecord> = [
    {
      title: "Severity",
      dataIndex: "severity",
      key: "severity",
      width: 110,
      sorter: (a, b) => severityRank[a.severity] - severityRank[b.severity],
      defaultSortOrder: "descend",
      render: (value: Severity, row) => {
        const sev = row.isUnknownSeverity ? "UNKNOWN" : value;
        const className = row.isUnknownSeverity ? "severity-pill severity-pill-unknown" : "severity-pill";
        return (
          <Tag className={className} color={SEVERITY_TAG_COLOR[sev]}>
            {sev}
          </Tag>
        );
      },
    },
    {
      title: "Cluster",
      dataIndex: "clusterName",
      key: "clusterName",
      width: 240,
      sorter: (a, b) => a.clusterName.localeCompare(b.clusterName),
      render: (_, row) => {
        const url = clusterPrismURL(row.cluster, clusterNameMap);
        return (
          <Space orientation="vertical" size={0}>
            {url ? (
              <a
                href={url}
                target="_blank"
                rel="noreferrer"
                className="cluster-link kb-like-link"
                onClick={(e) => e.stopPropagation()}
              >
                {row.clusterName}
              </a>
            ) : (
              <Typography.Text strong>{row.clusterName}</Typography.Text>
            )}
            <Typography.Text type="secondary" style={{ fontSize: 12 }} className="mono">
              {row.cluster}
            </Typography.Text>
            {(row.clusterVersion || row.nccVersion) && (
              <Space size={4} wrap style={{ marginTop: 2 }}>
                {row.clusterVersion ? (
                  <Tag style={{ fontSize: 11, lineHeight: "16px", padding: "0 6px" }}>{row.clusterVersion}</Tag>
                ) : null}
                {row.nccVersion ? (
                  <Tag style={{ fontSize: 11, lineHeight: "16px", padding: "0 6px" }}>NCC {row.nccVersion}</Tag>
                ) : null}
              </Space>
            )}
          </Space>
        );
      },
    },
    {
      title: "Alert",
      dataIndex: "alert",
      key: "alert",
      width: 280,
      sorter: (a, b) => a.alert.localeCompare(b.alert),
      render: (value: string, row) => (
        <Space orientation="vertical" size={2} style={{ width: "100%" }}>
          <Typography.Text strong style={{ wordBreak: "break-word" }}>
            {value}
          </Typography.Text>
          <Space size={4} wrap>
            {row.isChanged ? <Tag color="gold" style={{ margin: 0 }}>changed</Tag> : null}
            {row.isFlaky ? <Tag color="purple" style={{ margin: 0 }}>flaky</Tag> : null}
          </Space>
        </Space>
      ),
    },
    {
      title: "KB",
      key: "kb",
      width: 100,
      render: (_, row) =>
        row.kb ? (
          <a href={row.kb} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
            <Tag color="processing" icon={<LinkOutlined />} style={{ margin: 0 }}>
              {kbLabel(row.kb)}
            </Tag>
          </a>
        ) : (
          <Typography.Text type="secondary">—</Typography.Text>
        ),
    },
    {
      title: "Detail",
      dataIndex: "detail",
      key: "detail",
      render: (value: string) => (
        <Typography.Paragraph
          type="secondary"
          ellipsis={{ rows: density === "compact" ? 1 : 2, tooltip: false }}
          style={{ margin: 0 }}
        >
          {previewDetail(value)}
        </Typography.Paragraph>
      ),
    },
    {
      title: " ",
      key: "actions",
      width: 50,
      align: "center",
      render: (_, row) => (
        <Tooltip title="Open details">
          <Button
            type="text"
            size="small"
            icon={<ExpandAltOutlined />}
            onClick={(e) => {
              e.stopPropagation();
              setDrawerRow(row);
            }}
          />
        </Tooltip>
      ),
    },
  ];

  const copyDetail = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopyState("copied");
      notify.success("Detail copied to clipboard.");
      setTimeout(() => setCopyState("idle"), 1600);
    } catch {
      notify.warning("Could not access clipboard.");
    }
  };

  const totalRows = rows.length;
  const fail = severityCounts.FAIL || 0;
  const err = severityCounts.ERR || 0;
  const warn = severityCounts.WARN || 0;
  const info = severityCounts.INFO || 0;
  const unknown = severityCounts.UNKNOWN || 0;

  useEffect(() => {
    onSummaryChange?.({
      total: totalRows,
      fail,
      err,
      warn,
      info,
      unknown,
    });
  }, [onSummaryChange, totalRows, fail, err, warn, info, unknown]);

  return (
    <Card className="alerts-card page-card">
      <div className="alerts-header">
        <div>
          <Typography.Title level={4} className="section-title" style={{ marginBottom: 4 }}>
            Alerts
          </Typography.Title>
          <Typography.Text type="secondary">
            {totalRows.toLocaleString()} {totalRows === 1 ? "alert" : "alerts"} match current filters
          </Typography.Text>
        </div>
        <Space size={[8, 8]} wrap className="alerts-summary-pills">
          <Tag color="error">FAIL {fail}</Tag>
          <Tag color="warning">WARN {warn}</Tag>
          <Tag color="volcano">ERR {err}</Tag>
          <Tag color="processing">INFO {info}</Tag>
          {unknown > 0 ? (
            <Tag className="severity-pill severity-pill-unknown" color="default">
              UNKNOWN {unknown}
            </Tag>
          ) : null}
          <Tooltip title={density === "compact" ? "Switch to comfortable rows" : "Switch to compact rows"}>
            <Button
              size="small"
              onClick={() => setDensity(density === "compact" ? "comfortable" : "compact")}
            >
              {density === "compact" ? "Compact" : "Comfortable"}
            </Button>
          </Tooltip>
        </Space>
      </div>

      {totalRows === 0 ? (
        <Empty
          style={{ padding: "32px 0" }}
          description={
            <Space orientation="vertical" size={2} align="center">
              <Typography.Text strong>No alerts match the current filters</Typography.Text>
              <Typography.Text type="secondary">
                Try clearing severity chips or the cluster filter.
              </Typography.Text>
            </Space>
          }
        />
      ) : (
        <Table<RowRecord>
          virtual
          className="alerts-table"
          tableLayout="fixed"
          rowKey="key"
          columns={columns}
          dataSource={rows}
          rowClassName={(_, index) => (index % 2 === 0 ? "alerts-row-even" : "alerts-row-odd")}
          onRow={(row) => ({ onClick: () => setDrawerRow(row), style: { cursor: "pointer" } })}
          pagination={{
            current: page,
            pageSize: rowsPerPage,
            total: rows.length,
            onChange: (nextPage, nextPageSize) => {
              setPage(nextPage);
              if (nextPageSize && nextPageSize !== rowsPerPage) setRowsPerPage(nextPageSize);
            },
            showSizeChanger: true,
            pageSizeOptions: [50, 100, 200, 500],
            showTotal: (total, range) => `${range[0]}–${range[1]} of ${total}`,
          }}
          size={density === "compact" ? "small" : "middle"}
          scroll={{ x: 1200, y: 620 }}
        />
      )}

      <Drawer
        open={Boolean(drawerRow)}
        title={
          drawerRow ? (
            <Space size={8} wrap>
              <Tag
                color={SEVERITY_TAG_COLOR[drawerRow.severity]}
                className={drawerRow.isUnknownSeverity ? "severity-pill severity-pill-unknown" : "severity-pill"}
              >
                {drawerRow.isUnknownSeverity ? "UNKNOWN" : drawerRow.severity}
              </Tag>
              <Typography.Text strong>{drawerRow.clusterName}</Typography.Text>
              <Typography.Text type="secondary">·</Typography.Text>
              <Typography.Text>{drawerRow.alert}</Typography.Text>
            </Space>
          ) : null
        }
        onClose={() => setDrawerRow(null)}
        width={720}
        extra={
          drawerRow ? (
            <Space size={8}>
              <Button
                icon={copyState === "copied" ? <CheckOutlined /> : <CopyOutlined />}
                onClick={() => copyDetail(drawerRow.detail || "")}
              >
                {copyState === "copied" ? "Copied" : "Copy detail"}
              </Button>
              {drawerRow.kb ? (
                <Button type="primary" icon={<LinkOutlined />} href={drawerRow.kb} target="_blank">
                  {kbLabel(drawerRow.kb)}
                </Button>
              ) : null}
            </Space>
          ) : null
        }
      >
        {drawerRow ? (
          <Space orientation="vertical" size={16} style={{ width: "100%" }}>
            <div>
              <Typography.Text strong>Cluster</Typography.Text>
              <div>
                <a
                  href={clusterPrismURL(drawerRow.cluster, clusterNameMap)}
                  target="_blank"
                  rel="noreferrer"
                  className="kb-like-link"
                >
                  {drawerRow.clusterName}
                </a>
                <Typography.Text type="secondary" style={{ marginLeft: 8 }} className="mono">
                  ({drawerRow.cluster})
                </Typography.Text>
              </div>
              <Space size={6} wrap style={{ marginTop: 6 }}>
                {drawerRow.clusterVersion ? <Tag>{drawerRow.clusterVersion}</Tag> : null}
                {drawerRow.nccVersion ? <Tag>NCC {drawerRow.nccVersion}</Tag> : null}
                {drawerRow.isChanged ? <Tag color="gold">changed</Tag> : null}
                {drawerRow.isFlaky ? <Tag color="purple">flaky</Tag> : null}
                {drawerRow.logName ? (
                  <Tooltip title={drawerRow.logPath}>
                    <Tag>{drawerRow.logName}</Tag>
                  </Tooltip>
                ) : null}
              </Space>
            </div>
            <div>
              <Typography.Text strong>Detail</Typography.Text>
              <pre className="alert-detail-block">{drawerRow.detail || "(no detail)"}</pre>
            </div>
          </Space>
        ) : null}
      </Drawer>
    </Card>
  );
}
