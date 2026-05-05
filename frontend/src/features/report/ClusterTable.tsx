import { useMemo, useState } from "react";
import { Button, Card, Modal, Select, Space, Table, Tag, Tooltip, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { asArray, asRecord, displayClusterName, resolveClusterName } from "../../utils/report";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

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
};

type SortField = "clusterName" | "cluster" | "alert" | "severity";
type SortOrder = "asc" | "desc";
type Severity = "FAIL" | "WARN" | "ERR" | "INFO" | "UNKNOWN";
type RowRecord = {
  key: string;
  clusterName: string;
  cluster: string;
  alert: string;
  severity: Severity;
  detail: string;
  kb: string;
  clusterVersion: string;
  nccVersion: string;
  isChanged: boolean;
  isFlaky: boolean;
  logName: string;
  logPath: string;
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
  return m ? `KB-${m[1]}` : "KB";
}

function normalizeCheckTitle(check: string): string {
  return check.replace(/^detailed information for\s*/i, "").replace(/:$/, "").trim();
}

function clusterPrismURL(cluster: string): string {
  const clean = cluster.trim();
  if (!clean) return "";
  if (/^https?:\/\//i.test(clean)) return clean;
  return `https://${clean}:9440`;
}

function matchingLogForCluster(logs: Array<{ name: string; path: string }>, cluster: string): { name: string; path: string } | undefined {
  const key = (cluster || "").trim();
  if (!key) return undefined;
  const hit = logs.find((l) => l.name.includes(key) || l.path.includes(key));
  return hit;
}

export function ClusterTable({ checksSnapshot, aggRows, diffFlags, flakyKeys, nccLogs, filterText, selectedClusters, clusterNameMap, severityFilters, compareMode }: Props) {
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useLocalStorageState("dashboard.alerts.rowsPerPage", 100);
  const [sortField, setSortField] = useLocalStorageState<SortField>("dashboard.alerts.sortField", "clusterName");
  const [sortOrder, setSortOrder] = useLocalStorageState<SortOrder>("dashboard.alerts.sortOrder", "desc");
  const [detailModalRow, setDetailModalRow] = useState<RowRecord | null>(null);

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
      const severity = (["FAIL", "WARN", "ERR", "INFO"].includes(severityRaw) ? severityRaw : "UNKNOWN") as Severity;
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
      if (selectedSet.size > 0 && !selectedSet.has(r.clusterName.toLowerCase()) && !selectedSet.has(resolveClusterName(r.cluster, clusterNameMap).toLowerCase())) return false;
      if (parsedTokens.changed && !r.isChanged) return false;
      if (parsedTokens.flaky && !r.isFlaky) return false;
      if (compareMode === "changed" && !r.isChanged) return false;
      if (compareMode === "flaky" && !r.isFlaky) return false;
      const hay = `${r.clusterName} ${r.cluster} ${r.severity} ${r.alert} ${r.detail}`.toLowerCase();
      return parsedTokens.terms.every((t) => hay.includes(t));
    });
  }, [checksSnapshot, aggRows, diffFlags, flakyKeys, nccLogs, filterText, selectedClusters, clusterNameMap, severityFilters, compareMode]);

  const sortedRows = useMemo(() => {
    const severityRank: Record<string, number> = { FAIL: 5, WARN: 4, ERR: 3, INFO: 2, UNKNOWN: 1 };
    const copy = [...rows];
    copy.sort((a, b) => {
      let cmp = 0;
      if (sortField === "clusterName") {
        cmp = a.clusterName.localeCompare(b.clusterName);
      } else if (sortField === "cluster") {
        cmp = a.cluster.localeCompare(b.cluster);
      } else if (sortField === "alert") {
        cmp = a.alert.localeCompare(b.alert);
      } else {
        cmp = (severityRank[a.severity] || 0) - (severityRank[b.severity] || 0);
      }
      return sortOrder === "asc" ? cmp : -cmp;
    });
    return copy;
  }, [rows, sortField, sortOrder]);

  const sorterFor = (field: SortField) => {
    if (sortField !== field) return null;
    return sortOrder === "asc" ? "ascend" : "descend";
  };

  const columns = useMemo<ColumnsType<RowRecord>>(() => {
    const cols: ColumnsType<RowRecord> = [];
    cols.push({
      title: "Cluster Name",
      dataIndex: "clusterName",
      key: "clusterName",
      width: 240,
      sorter: true,
      sortOrder: sorterFor("clusterName"),
      render: (_, row) => {
        const clusterVersion = row.clusterVersion.trim();
        const nccVersion = row.nccVersion.trim();
        return (
          <div className="mono">
            <div style={{ fontWeight: 600 }}>{row.clusterName}</div>
            {(clusterVersion || nccVersion) && (
              <div style={{ color: "rgba(255,255,255,0.65)", fontSize: 12 }}>
                {clusterVersion ? <div>{`Version: ${clusterVersion}`}</div> : null}
                {nccVersion ? <div>{`NCC: ${nccVersion}`}</div> : null}
              </div>
            )}
          </div>
        );
      },
    });
    cols.push({
      title: "Cluster",
      dataIndex: "cluster",
      key: "cluster",
      sorter: true,
      sortOrder: sorterFor("cluster"),
      render: (value: string) => {
        const clusterURL = clusterPrismURL(value);
        const label = resolveClusterName(value, clusterNameMap);
        return clusterURL ? (
          <a href={clusterURL} target="_blank" rel="noreferrer">
            {label}
          </a>
        ) : (
          label
        );
      },
    });
    cols.push({
      title: "Alert",
      dataIndex: "alert",
      key: "alert",
      sorter: true,
      sortOrder: sorterFor("alert"),
      render: (value: string, row) => (
        <Space size={6} wrap>
          <span>{value}</span>
          {row.isChanged ? <Tag color="gold">changed</Tag> : null}
          {row.isFlaky ? <Tag color="purple">flaky</Tag> : null}
        </Space>
      ),
    });
    cols.push({
      title: "Severity",
      dataIndex: "severity",
      key: "severity",
      width: 110,
      sorter: true,
      sortOrder: sorterFor("severity"),
      render: (value: Severity) => {
        const sev = String(value ?? "UNKNOWN").toUpperCase();
        const color = sev === "FAIL" ? "error" : sev === "WARN" ? "warning" : sev === "ERR" ? "magenta" : sev === "INFO" ? "processing" : "default";
        return <Tag className="severity-pill" color={color}>{sev}</Tag>;
      },
    });
    cols.push({
      title: "KB",
      key: "kb",
      width: 100,
      render: (_, row) =>
        row.kb ? (
          <a href={row.kb} target="_blank" rel="noreferrer">
            {kbLabel(row.kb)}
          </a>
        ) : (
          "-"
        ),
    });
    cols.push({
      title: "NCC Details",
      dataIndex: "detail",
      key: "detail",
      width: 480,
      render: (value: string, row) => (
        <Typography.Paragraph
          style={{ marginBottom: 0, maxWidth: 460 }}
          ellipsis={{ rows: 2, tooltip: value || "-" }}
        >
          {value || "-"}{" "}
          <Button type="link" size="small" onClick={() => setDetailModalRow(row)}>
            view
          </Button>
        </Typography.Paragraph>
      ),
    });
    cols.push({
      title: "Actions",
      key: "actions",
      width: 230,
      render: (_, row) => {
        const detail = row.detail || "-";
        return (
          <Space size={8} wrap>
            <Button size="small" onClick={() => navigator.clipboard.writeText(detail)}>
              Copy detail
            </Button>
            <Button size="small" onClick={() => setDetailModalRow(row)}>
              Details
            </Button>
            {row.logName ? (
              <Tooltip title={row.logPath}>
                <Typography.Text className="mono" style={{ fontSize: 12 }}>
                  {row.logName}
                </Typography.Text>
              </Tooltip>
            ) : null}
          </Space>
        );
      },
    });
    return cols;
  }, [sortField, sortOrder, clusterNameMap]);

  const pageRows = useMemo(() => sortedRows.slice((page - 1) * rowsPerPage, page * rowsPerPage), [sortedRows, page, rowsPerPage]);
  const dataSource = pageRows;

  const severityCounts = useMemo(() => {
    const counts: Record<Severity, number> = { FAIL: 0, WARN: 0, ERR: 0, INFO: 0, UNKNOWN: 0 };
    sortedRows.forEach((r) => {
      counts[r.severity] = (counts[r.severity] || 0) + 1;
    });
    return counts;
  }, [sortedRows]);

  return (
    <Card className="alerts-card page-card">
      <Typography.Title level={4} className="section-title">
        Alerts
      </Typography.Title>
      <Space size={[8, 8]} wrap style={{ marginBottom: 10 }}>
        <Tag color="error">{`FAIL ${severityCounts.FAIL}`}</Tag>
        <Tag color="warning">{`WARN ${severityCounts.WARN}`}</Tag>
        <Tag color="magenta">{`ERR ${severityCounts.ERR}`}</Tag>
        <Tag color="processing">{`INFO ${severityCounts.INFO}`}</Tag>
      </Space>
      <Space size={[8, 8]} wrap style={{ marginBottom: 12 }}>
        <Select
          size="middle"
          style={{ width: 100 }}
          value={rowsPerPage}
          onChange={(e) => {
            setRowsPerPage(Number(e) || 100);
            setPage(1);
          }}
          options={[100, 200, 300, 400, 500].map((n) => ({ label: `${n} rows`, value: n }))}
        />
        <Typography.Text type="secondary">Showing {rows.length} alerts</Typography.Text>
        <Select
          size="middle"
          style={{ width: 170 }}
          value={sortField}
          onChange={(value) => {
            setSortField(value as SortField);
            setPage(1);
          }}
          options={[
            { label: "Sort: Severity", value: "severity" },
            { label: "Sort: Cluster Name", value: "clusterName" },
            { label: "Sort: Cluster", value: "cluster" },
            { label: "Sort: Alert", value: "alert" },
          ]}
        />
        <Select
          size="middle"
          style={{ width: 130 }}
          value={sortOrder}
          onChange={(value) => {
            setSortOrder(value as SortOrder);
            setPage(1);
          }}
          options={[
            { label: "Desc", value: "desc" },
            { label: "Asc", value: "asc" },
          ]}
        />
      </Space>
      <Table<RowRecord>
        bordered
        className="alerts-table"
        columns={columns}
        dataSource={dataSource}
        rowClassName={(_, index) => (index % 2 === 0 ? "alerts-row-even" : "alerts-row-odd")}
        pagination={{
          current: page,
          pageSize: rowsPerPage,
          total: sortedRows.length,
          onChange: (nextPage, nextPageSize) => {
            setPage(nextPage);
            if (nextPageSize && nextPageSize !== rowsPerPage) setRowsPerPage(nextPageSize);
          },
          showSizeChanger: true,
          pageSizeOptions: [100, 200, 300, 400, 500],
        }}
        onChange={(_, __, sorter) => {
          if (!Array.isArray(sorter) && sorter?.field) {
            const field = String(sorter.field);
            if (field === "clusterName" || field === "cluster" || field === "alert" || field === "severity") {
              setSortField(field as SortField);
              setSortOrder(sorter.order === "ascend" ? "asc" : "desc");
            }
          }
        }}
        size="middle"
        scroll={{ x: 1200 }}
      />
      <Modal
        open={Boolean(detailModalRow)}
        title={`${detailModalRow?.clusterName || detailModalRow?.cluster || "Cluster"} - ${detailModalRow?.alert || "Alert"}`}
        onCancel={() => setDetailModalRow(null)}
        footer={[
          <Button key="copy" onClick={() => navigator.clipboard.writeText(detailModalRow?.detail || "")}>
            Copy detail
          </Button>,
          <Button key="close" type="primary" onClick={() => setDetailModalRow(null)}>
            Close
          </Button>,
        ]}
        width={860}
      >
        <pre>{detailModalRow?.detail || "-"}</pre>
      </Modal>
    </Card>
  );
}
