import { useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  Button,
  Card,
  Descriptions,
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
  CheckOutlined,
  ClockCircleOutlined,
  ClusterOutlined,
  CopyOutlined,
  ExpandAltOutlined,
  ExportOutlined,
  LinkOutlined,
} from "@ant-design/icons";
import { asArray, asRecord, displayClusterName, resolveClusterName } from "../../utils/report";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify } from "../../notify";
import { formatDateTime, relativeTime } from "../../utils/datetime";

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
  pcAlerts?: unknown[];
  alertSource: "NCC" | "PC";
  pcResolvedFilter: "all" | "No" | "Yes";
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
  clusterIP: string;
  alert: string;
  entityName: string;
  entityType: string;
  lastOccurred: string;
  lastOccurredTime: number;
  status: string;
  impactType: string;
  alertType: string;
  acknowledged: string;
  resolved: string;
  source: "NCC" | "PC";
  severity: Severity;
  isUnknownSeverity: boolean;
  detail: string;
  kb: string;
  kbLinks: string[];
  createdAt: string;
  updatedAt: string;
  resolvedAt: string;
  autoResolved: boolean;
  extId: string;
  rootCause: string;
  serviceName: string;
  clusterUUID: string;
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

function collectKBUrls(raw: Record<string, unknown>, detail: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const add = (value: string) => {
    const url = value.trim();
    if (!url || seen.has(url)) return;
    seen.add(url);
    out.push(url);
  };
  add(parseKB(detail));
  const arts = raw.kb_articles ?? raw.kbArticles;
  if (Array.isArray(arts)) {
    for (const item of arts) {
      if (typeof item === "string") add(item);
      else if (item && typeof item === "object") {
        const rec = asRecord(item);
        add(String(rec.url ?? rec.uri ?? rec.link ?? rec.href ?? ""));
      }
    }
  } else if (typeof arts === "string") {
    add(arts);
  }
  return out;
}

function formatStamp(value: string): string {
  if (!value || value === "—") return "—";
  const abs = formatDateTime(value);
  const rel = relativeTime(value);
  if (rel === "—" || rel === abs) return abs;
  return `${abs} (${rel})`;
}

function pcCopyText(row: RowRecord): string {
  const lines = [
    row.alert,
    `Severity: ${row.isUnknownSeverity ? "UNKNOWN" : row.severity}`,
    `Cluster: ${row.clusterName}${row.clusterIP ? ` (${row.clusterIP})` : ""}`,
    `Entity: ${row.entityName}${row.entityType && row.entityType !== "—" ? ` (${row.entityType})` : ""}`,
    `Status: ${row.status}`,
    `Resolved: ${row.resolved}`,
    `Acknowledged: ${row.acknowledged}`,
    row.lastOccurred && row.lastOccurred !== "—" ? `Last occurred: ${formatDateTime(row.lastOccurred)}` : "",
    row.serviceName ? `Service: ${row.serviceName}` : "",
    row.alertType && row.alertType !== "—" ? `Alert type: ${row.alertType}` : "",
    "",
    row.detail || "",
    row.rootCause && row.rootCause !== row.detail ? `\nRoot cause:\n${row.rootCause}` : "",
    row.kbLinks.length ? `\nKB:\n${row.kbLinks.join("\n")}` : "",
  ];
  return lines.filter((line) => line !== "").join("\n").trim();
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

function clusterPrismURL(
  cluster: string,
  clusterNameMap: Record<string, string>,
  displayName = "",
): string {
  const candidates = [cluster, displayName].map((value) => value.trim()).filter(Boolean);
  for (const candidate of candidates) {
    const clean = normalizeClusterHost(candidate);
    if (isIPv4Like(clean)) return `https://${clean}:9440`;
  }
  const mappedIP = Object.entries(clusterNameMap).find(
    ([key, name]) =>
      isIPv4Like(normalizeClusterHost(key)) &&
      candidates.some((candidate) => name.trim().toLowerCase() === candidate.toLowerCase()),
  )?.[0];
  return mappedIP ? `https://${normalizeClusterHost(mappedIP)}:9440` : "";
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

function PCMetric({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="pc-alert-metric">
      <div className="pc-alert-metric-label">{label}</div>
      <div className="pc-alert-metric-value">{children}</div>
    </div>
  );
}

function PCAlertInspector({
  row,
  clusterNameMap,
}: {
  row: RowRecord;
  clusterNameMap: Record<string, string>;
}) {
  const prismURL = clusterPrismURL(row.cluster, clusterNameMap, row.clusterName);
  const clusterSubtitle = row.clusterIP && row.clusterIP !== row.clusterName ? row.clusterIP : "";
  const showRootCause = Boolean(row.rootCause) && row.rootCause !== row.detail;
  const timeline = [
    { label: "Created", value: row.createdAt },
    { label: "Last occurred", value: row.lastOccurred },
    { label: "Updated", value: row.updatedAt },
    { label: "Resolved", value: row.resolvedAt },
  ].filter((item) => item.value && item.value !== "—");

  return (
    <div className="pc-alert-inspector">
      <div className="pc-alert-metric-grid">
        <PCMetric label="Status">{row.status !== "—" ? row.status : row.resolved === "Yes" ? "Resolved" : "Open"}</PCMetric>
        <PCMetric label="Resolved">
          <Tag color={row.resolved === "Yes" ? "success" : "processing"} style={{ margin: 0 }}>
            {row.autoResolved ? "Auto resolved" : row.resolved}
          </Tag>
        </PCMetric>
        <PCMetric label="Acknowledged">
          <Tag color={row.acknowledged === "Yes" ? "success" : "default"} style={{ margin: 0 }}>
            {row.acknowledged}
          </Tag>
        </PCMetric>
        <PCMetric label="Impact">{row.impactType !== "—" ? row.impactType.replace(/_/g, " ") : "—"}</PCMetric>
      </div>

      <div className="pc-alert-pair">
        <Card size="small" className="pc-alert-entity-card" title={<Space size={6}><ClusterOutlined /> Entity</Space>}>
          <Typography.Text strong>{row.entityName || "—"}</Typography.Text>
          <div>
            <Typography.Text type="secondary">{row.entityType !== "—" ? row.entityType : "Unknown type"}</Typography.Text>
          </div>
        </Card>
        <Card size="small" className="pc-alert-entity-card" title={<Space size={6}><ExportOutlined /> Cluster</Space>}>
          {prismURL ? (
            <a href={prismURL} target="_blank" rel="noreferrer">
              {row.clusterName}
            </a>
          ) : (
            <Typography.Text strong>{row.clusterName}</Typography.Text>
          )}
          {clusterSubtitle ? (
            <div>
              <Typography.Text type="secondary" className="mono">
                {clusterSubtitle}
              </Typography.Text>
            </div>
          ) : null}
          {row.clusterUUID ? (
            <Typography.Paragraph type="secondary" copyable={{ text: row.clusterUUID }} style={{ margin: "6px 0 0", fontSize: 12 }}>
              {row.clusterUUID}
            </Typography.Paragraph>
          ) : null}
          {prismURL ? (
            <div style={{ marginTop: 8 }}>
              <a href={prismURL} target="_blank" rel="noreferrer">
                Open Prism
              </a>
            </div>
          ) : null}
        </Card>
      </div>

      {timeline.length > 0 ? (
        <Card size="small" title={<Space size={6}><ClockCircleOutlined /> Timeline</Space>}>
          <div className="pc-alert-timeline">
            {timeline.map((item) => (
              <div key={item.label} className="pc-alert-timeline-item">
                <div className="pc-alert-metric-label">{item.label}</div>
                <div>{formatStamp(item.value)}</div>
              </div>
            ))}
          </div>
        </Card>
      ) : null}

      <Card size="small" title="Alert message">
        <Typography.Paragraph className="pc-alert-message" style={{ margin: 0 }}>
          {row.detail || "(no message)"}
        </Typography.Paragraph>
      </Card>

      {showRootCause ? (
        <Card size="small" title="Root cause">
          <Typography.Paragraph className="pc-alert-message" style={{ margin: 0 }}>
            {row.rootCause}
          </Typography.Paragraph>
        </Card>
      ) : null}

      {row.kbLinks.length > 0 ? (
        <Card size="small" title="Knowledge base">
          <Space size={[8, 8]} wrap>
            {row.kbLinks.map((url) => (
              <a key={url} href={url} target="_blank" rel="noreferrer">
                <Tag color="processing" icon={<LinkOutlined />} style={{ margin: 0 }}>
                  {kbLabel(url)}
                </Tag>
              </a>
            ))}
          </Space>
        </Card>
      ) : null}

      <Descriptions bordered size="small" column={{ xs: 1, sm: 2 }} title="Identifiers">
        <Descriptions.Item label="Alert type">{row.alertType !== "—" ? row.alertType : "—"}</Descriptions.Item>
        <Descriptions.Item label="Service">{row.serviceName || "—"}</Descriptions.Item>
        <Descriptions.Item label="Alert ID">
          {row.extId ? (
            <Typography.Text copyable={{ text: row.extId }} className="mono">
              {row.extId}
            </Typography.Text>
          ) : (
            "—"
          )}
        </Descriptions.Item>
      </Descriptions>
    </div>
  );
}

function displayPCStatus(raw: Record<string, unknown>): string {
  const status = String(raw.status ?? "").toUpperCase();
  if (status === "AUTO_RESOLVED" || raw.isAutoResolved === true || raw.auto_resolved === true) {
    const resolved = String(raw.resolvedTime ?? raw.resolved_at ?? "").trim();
    return resolved ? `Auto Resolved (${new Date(resolved).toLocaleString()})` : "Auto Resolved";
  }
  if (status === "OPEN") return "—";
  return String(raw.status ?? "—").replace(/_/g, " ");
}

const ALERTS_TABLE_MIN_Y = 240;
const ALERTS_TABLE_FALLBACK_Y = 620;

function useAlertsTableScrollY(enabled: boolean, density: Density) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [tableY, setTableY] = useState(ALERTS_TABLE_FALLBACK_Y);

  useLayoutEffect(() => {
    if (!enabled) return;
    const host = hostRef.current;
    if (!host) return;

    const measure = () => {
      const header =
        host.querySelector<HTMLElement>(".ant-table-header") ??
        host.querySelector<HTMLElement>(".ant-table-thead");
      const pagination =
        host.querySelector<HTMLElement>(".ant-table-pagination") ??
        host.querySelector<HTMLElement>(".ant-pagination");
      const headerH = header?.getBoundingClientRect().height ?? (density === "compact" ? 39 : 47);
      const pagH = pagination?.getBoundingClientRect().height ?? 64;
      const hostH = host.clientHeight;
      if (hostH <= 0) {
        setTableY(ALERTS_TABLE_FALLBACK_Y);
        return;
      }
      setTableY(Math.max(ALERTS_TABLE_MIN_Y, Math.floor(hostH - headerH - pagH)));
    };

    measure();
    const ro = typeof ResizeObserver !== "undefined" ? new ResizeObserver(measure) : null;
    ro?.observe(host);
    window.addEventListener("resize", measure);
    return () => {
      ro?.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [enabled, density]);

  return { hostRef, tableY };
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
  pcAlerts,
  alertSource,
  pcResolvedFilter,
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
      const cluster = String(raw.cluster || raw.address || raw.cluster_name || "-");
      const alert = normalizeCheckTitle(String(raw.check_name ?? raw.check ?? raw.title ?? raw.alert ?? "-"));
      const entityName = String(
        raw.entity_name ??
          raw.entityName ??
          raw.source_entity_name ??
          asRecord(raw.sourceEntity).name ??
          raw.entity ??
          cluster ??
          "-",
      );
      const entityType = String(raw.entity_type ?? raw.entityType ?? asRecord(raw.sourceEntity).type ?? "—");
      const lastOccurred = String(
        raw.last_occurred ??
          raw.lastOccurred ??
          raw.lastUpdatedTime ??
          raw.updated_at ??
          raw.creationTime ??
          raw.created_at ??
          "—",
      );
      const lastOccurredTime = Date.parse(lastOccurred) || 0;
      const status = String(raw.source || "").toUpperCase() === "PC" ? displayPCStatus(raw) : String(raw.status ?? "—");
      const impactType = String(raw.impact_type ?? raw.impactType ?? "—");
      const alertType = String(raw.alert_type ?? raw.alertType ?? "—");
      const acknowledged = raw.acknowledged === true ? "Yes" : raw.acknowledged === false ? "No" : "—";
      const resolved = raw.resolved === true ? "Yes" : raw.resolved === false ? "No" : "—";
      const severityRaw = String(raw.severity || "UNKNOWN").toUpperCase();
      const isUnknownSeverity = !["FAIL", "WARN", "ERR", "INFO"].includes(severityRaw);
      const severity = (isUnknownSeverity ? "ERR" : severityRaw) as Severity;
      const detail = String(raw.detail || raw.message || "");
      const kbLinks = collectKBUrls(raw, detail);
      const diff = pickDiff(cluster, alert);
      const isChanged = Boolean(diff.new_fail || diff.resolved_fail || diff.severity_changed);
      const isFlaky = Boolean(fkeys[rowKey(cluster, alert)]);
      const log = matchingLogForCluster(nccLogs || [], cluster);
      return {
        key: `${cluster}-${alert}-${idx}`,
        clusterName: resolveClusterName(displayClusterName(raw), clusterNameMap),
        cluster,
        clusterIP: String(raw.cluster_ip ?? raw.address ?? ""),
        alert,
        entityName,
        entityType,
        lastOccurred,
        lastOccurredTime,
        status,
        impactType,
        alertType,
        acknowledged,
        resolved,
        source: String(raw.source || "NCC").toUpperCase() === "PC" ? "PC" : "NCC",
        severity,
        isUnknownSeverity,
        detail,
        kb: kbLinks[0] || "",
        kbLinks,
        createdAt: String(raw.created_at ?? raw.creationTime ?? ""),
        updatedAt: String(raw.updated_at ?? raw.lastUpdatedTime ?? ""),
        resolvedAt: String(raw.resolved_at ?? raw.resolvedTime ?? ""),
        autoResolved: raw.auto_resolved === true || raw.isAutoResolved === true,
        extId: String(raw.ext_id ?? raw.extId ?? ""),
        rootCause: String(raw.root_cause ?? raw.rootCauseAnalysis ?? ""),
        serviceName: String(raw.service_name ?? raw.serviceName ?? ""),
        clusterUUID: String(raw.cluster_uuid ?? raw.clusterUUID ?? ""),
        clusterVersion: String(raw.clusterVersion || raw.cluster_version || ""),
        nccVersion: String(raw.nccVersion || raw.ncc_version || ""),
        isChanged,
        isFlaky,
        logName: log?.name || "",
        logPath: log?.path || "",
      };
    };

    let baseRows: RowRecord[] = [];
    const activeSourceFilter = String(alertSource || "NCC").toUpperCase();
    const baseAgg = [
      ...(aggRows || []).map((r) => ({ ...asRecord(r), source: "NCC" })),
      ...(pcAlerts || []).map((r) => ({ ...asRecord(r), source: "PC" })),
    ].filter((r) => activeSourceFilter === "ALL" || r.source === activeSourceFilter);
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
      if (alertSource === "PC" && pcResolvedFilter !== "all" && r.resolved !== pcResolvedFilter) return false;
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
    pcAlerts,
    alertSource,
    pcResolvedFilter,
    compareMode,
  ]);

  const { hostRef, tableY } = useAlertsTableScrollY(rows.length > 0, density);

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

  const titleColumn = {
    title: "Alert",
    dataIndex: "alert",
    key: "alert",
    width: 280,
    sorter: (a: RowRecord, b: RowRecord) => a.alert.localeCompare(b.alert),
    render: (value: string, row: RowRecord) => (
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
  };
  const severityColumn = {
    title: "Severity",
    dataIndex: "severity",
    key: "severity",
    width: 110,
    sorter: (a: RowRecord, b: RowRecord) => severityRank[a.severity] - severityRank[b.severity],
    defaultSortOrder: "descend" as const,
    render: (value: Severity, row: RowRecord) => {
      const sev = row.isUnknownSeverity ? "UNKNOWN" : value;
      const className = row.isUnknownSeverity ? "severity-pill severity-pill-unknown" : "severity-pill";
      return <Tag className={className} color={SEVERITY_TAG_COLOR[sev]}>{sev}</Tag>;
    },
  };
  const actionColumn = {
    title: " ",
    key: "actions",
    width: 50,
    align: "center" as const,
    render: (_: unknown, row: RowRecord) => (
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
  };

  const nccColumns: ColumnsType<RowRecord> = [
    severityColumn,
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
              <a href={url} target="_blank" rel="noreferrer" className="cluster-link kb-like-link" onClick={(e) => e.stopPropagation()}>
                {row.clusterName}
              </a>
            ) : <Typography.Text strong>{row.clusterName}</Typography.Text>}
            <Typography.Text type="secondary" style={{ fontSize: 12 }} className="mono">{row.cluster}</Typography.Text>
            {(row.clusterVersion || row.nccVersion) && (
              <Space size={4} wrap style={{ marginTop: 2 }}>
                {row.clusterVersion ? <Tag style={{ fontSize: 11, lineHeight: "16px", padding: "0 6px" }}>{row.clusterVersion}</Tag> : null}
                {row.nccVersion ? <Tag style={{ fontSize: 11, lineHeight: "16px", padding: "0 6px" }}>NCC {row.nccVersion}</Tag> : null}
              </Space>
            )}
          </Space>
        );
      },
    },
    titleColumn,
    {
      title: "KB",
      key: "kb",
      width: 100,
      render: (_, row) => row.kb ? (
        <a href={row.kb} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
          <Tag color="processing" icon={<LinkOutlined />} style={{ margin: 0 }}>{kbLabel(row.kb)}</Tag>
        </a>
      ) : <Typography.Text type="secondary">—</Typography.Text>,
    },
    {
      title: "Detail",
      dataIndex: "detail",
      key: "detail",
      render: (value: string) => (
        <Typography.Paragraph type="secondary" ellipsis={{ rows: density === "compact" ? 1 : 2, tooltip: false }} style={{ margin: 0 }}>
          {previewDetail(value)}
        </Typography.Paragraph>
      ),
    },
    actionColumn,
  ];

  const pcColumns: ColumnsType<RowRecord> = [
    {
      title: "Title",
      dataIndex: "alert",
      key: "title",
      width: 280,
      sorter: (a, b) => a.alert.localeCompare(b.alert),
    },
    severityColumn,
    {
      title: "Entity Name",
      dataIndex: "entityName",
      key: "entityName",
      width: 220,
      sorter: (a, b) => a.entityName.localeCompare(b.entityName),
    },
    {
      title: "Entity Type",
      dataIndex: "entityType",
      key: "entityType",
      width: 150,
    },
    {
      title: "Cluster",
      dataIndex: "clusterName",
      key: "clusterName",
      width: 240,
      sorter: (a, b) => a.clusterName.localeCompare(b.clusterName),
      render: (_, row) => {
        const url = clusterPrismURL(row.cluster, clusterNameMap);
        const subtitle = row.clusterIP && row.clusterIP !== row.clusterName
          ? row.clusterIP
          : (row.cluster && row.cluster !== row.clusterName ? row.cluster : "");
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
            {subtitle ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }} className="mono">
                {subtitle}
              </Typography.Text>
            ) : null}
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
      title: "Last Occurred",
      dataIndex: "lastOccurred",
      key: "lastOccurred",
      width: 190,
      sorter: (a, b) => a.lastOccurredTime - b.lastOccurredTime,
      render: (value: string) => (value === "—" ? value : formatDateTime(value)),
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 130,
    },
    {
      title: "Resolved",
      dataIndex: "resolved",
      key: "resolved",
      width: 110,
      render: (value: string) => <Tag color={value === "Yes" ? "green" : "blue"}>{value}</Tag>,
    },
    {
      title: "Impact Type",
      dataIndex: "impactType",
      key: "impactType",
      width: 150,
    },
    actionColumn,
  ];
  const columns = alertSource === "PC" ? pcColumns : nccColumns;

  const copyDetail = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopyState("copied");
      notify.success("Alert copied to clipboard.");
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
          <Typography.Title level={4} className="tile-title">
            Alerts
          </Typography.Title>
          <Typography.Text type="secondary" className="tile-subtitle">
            {totalRows.toLocaleString()} {totalRows === 1 ? "alert" : "alerts"} match current filters
          </Typography.Text>
        </div>
        <Space size={[8, 8]} wrap className="alerts-summary-pills">
          <Tag color="error" className="severity-pill">FAIL {fail}</Tag>
          <Tag color="warning" className="severity-pill">WARN {warn}</Tag>
          <Tag color="volcano" className="severity-pill">ERR {err}</Tag>
          <Tag color="processing" className="severity-pill">INFO {info}</Tag>
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
        <div className="alerts-table-host" ref={hostRef} data-scroll-y={tableY}>
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
            scroll={{ x: 1200, y: tableY }}
          />
        </div>
      )}

      <Drawer
        open={Boolean(drawerRow)}
        title={
          drawerRow ? (
            <Space orientation="vertical" size={4} style={{ width: "100%" }}>
              <Typography.Text type="secondary" style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: 0.5 }}>
                {drawerRow.source === "PC" ? "Prism Central alert" : "NCC alert"}
              </Typography.Text>
              <Typography.Title level={4} style={{ margin: 0, paddingRight: 24 }}>
                {drawerRow.alert}
              </Typography.Title>
              <Space size={6} wrap>
                <Tag
                  color={SEVERITY_TAG_COLOR[drawerRow.severity]}
                  className={drawerRow.isUnknownSeverity ? "severity-pill severity-pill-unknown" : "severity-pill"}
                >
                  {drawerRow.isUnknownSeverity ? "UNKNOWN" : drawerRow.severity}
                </Tag>
                <Tag color={drawerRow.source === "PC" ? "purple" : "blue"}>{drawerRow.source}</Tag>
                {drawerRow.status !== "—" ? <Tag>{drawerRow.status}</Tag> : null}
              </Space>
            </Space>
          ) : null
        }
        onClose={() => setDrawerRow(null)}
        width={drawerRow?.source === "PC" ? 840 : 720}
        extra={
          drawerRow ? (
            <Space size={8} wrap>
              <Button
                icon={copyState === "copied" ? <CheckOutlined /> : <CopyOutlined />}
                onClick={() =>
                  copyDetail(drawerRow.source === "PC" ? pcCopyText(drawerRow) : drawerRow.detail || "")
                }
              >
                {copyState === "copied" ? "Copied" : drawerRow.source === "PC" ? "Copy alert" : "Copy detail"}
              </Button>
              {drawerRow.source === "PC" && clusterPrismURL(drawerRow.cluster, clusterNameMap, drawerRow.clusterName) ? (
                <Button
                  icon={<ExportOutlined />}
                  href={clusterPrismURL(drawerRow.cluster, clusterNameMap, drawerRow.clusterName)}
                  target="_blank"
                >
                  Open Prism
                </Button>
              ) : null}
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
          drawerRow.source === "PC" ? (
            <PCAlertInspector row={drawerRow} clusterNameMap={clusterNameMap} />
          ) : (
            <Space orientation="vertical" size={16} style={{ width: "100%" }}>
              <Space size={6} wrap>
                <Typography.Text strong>Cluster:</Typography.Text>
                <Typography.Text>{drawerRow.clusterName}</Typography.Text>
                {drawerRow.clusterVersion ? <Tag>{drawerRow.clusterVersion}</Tag> : null}
                {drawerRow.nccVersion ? <Tag>NCC {drawerRow.nccVersion}</Tag> : null}
                {drawerRow.isChanged ? <Tag color="gold">changed</Tag> : null}
                {drawerRow.isFlaky ? <Tag color="purple">flaky</Tag> : null}
                {drawerRow.logName ? <Tooltip title={drawerRow.logPath}><Tag>{drawerRow.logName}</Tag></Tooltip> : null}
              </Space>
              <Card
                size="small"
                title="Alert detail"
                style={{ borderRadius: 8 }}
                styles={{ body: { background: "rgba(0, 0, 0, 0.02)" } }}
              >
                <Typography.Paragraph style={{ margin: 0, whiteSpace: "pre-wrap" }}>
                  {drawerRow.detail || "(no detail)"}
                </Typography.Paragraph>
              </Card>
            </Space>
          )
        ) : null}
      </Drawer>
    </Card>
  );
}
