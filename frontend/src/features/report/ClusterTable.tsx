import { useDeferredValue, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Drawer,
  Empty,
  Grid,
  Input,
  Pagination,
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
  FullscreenExitOutlined,
  FullscreenOutlined,
  LinkOutlined,
} from "@ant-design/icons";
import type { NCCDisposition } from "../../api/types";
import { asArray, asRecord, displayClusterName, nccDispositionKey, resolveClusterName } from "../../utils/report";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify } from "../../notify";
import { formatDateTime, relativeTime } from "../../utils/datetime";
import { safeExternalUrl } from "../../utils/url";
import { NCCLifecyclePanel } from "./NCCLifecyclePanel";

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
  dispositions?: NCCDisposition[];
  /** Left out when alert handling is turned off: no checkboxes, bulk bar or handling buttons. */
  onDisposition?: (input: {
    action: "acknowledge" | "resolve" | "reopen";
    note?: string;
    run_at?: string;
    cluster?: string;
    check?: string;
    items?: Array<{ cluster: string; check: string }>;
  }) => void;
  dispositionPending?: boolean;
  runAt?: string;
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
type HandlingFilter = "all" | "open" | "acknowledged" | "resolved" | "returned";

const HANDLING_FILTERS: Array<{
  value: HandlingFilter;
  label: string;
  hint: string;
  tone: string;
}> = [
  { value: "all", label: "All", hint: "Every alert that matches the current filters.", tone: "all" },
  { value: "open", label: "Needs attention", hint: "Open alerts that still need a decision.", tone: "open" },
  { value: "acknowledged", label: "Acknowledged", hint: "Noted, and still open.", tone: "acked" },
  { value: "resolved", label: "Resolved", hint: "Marked done. If a later run finds the same alert, it moves to Returned.", tone: "resolved" },
  { value: "returned", label: "Returned", hint: "Resolved earlier, then found again on a later run.", tone: "returned" },
];

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
  /** The report row this came from; KB links are read from it on demand (kbLinksOf). */
  raw: Record<string, unknown>;
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
  /** Lower-cased "name address severity alert", built once for search. */
  searchText: string;
  /** Lower-cased display name and resolved address name, for the cluster filter. */
  clusterKeys: string[];
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
    // Values come from report files and Prism Central; only web links pass.
    const url = safeExternalUrl(value);
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
    kbLinksOf(row).length ? `\nKB:\n${kbLinksOf(row).join("\n")}` : "",
  ];
  return lines.filter((line) => line !== "").join("\n").trim();
}

// KB links come from parsing each row's detail text. Only the rows on screen
// need them, so they are worked out on first use rather than for all 200k
// rows of a fleet-scale report up front.
const kbLinkCache = new WeakMap<RowRecord, string[]>();
function kbLinksOf(row: RowRecord): string[] {
  let links = kbLinkCache.get(row);
  if (!links) {
    links = collectKBUrls(row.raw, row.detail);
    kbLinkCache.set(row, links);
  }
  return links;
}
const kbOf = (row: RowRecord): string => kbLinksOf(row)[0] || "";

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

type NCCLog = { name: string; path: string };

/**
 * Builds a cluster → raw NCC log lookup. Logs are named "<cluster>.log", so
 * an exact name is found directly; anything else falls back to the substring
 * match, once per cluster. Scanning every log for every row was 7,000 × 200,000
 * comparisons at fleet scale and froze the page.
 */
function logLookup(logs: NCCLog[]): (cluster: string) => NCCLog | undefined {
  const byName = new Map<string, NCCLog>();
  for (const l of logs) byName.set(l.name.replace(/\.log$/i, "").trim(), l);
  const seen = new Map<string, NCCLog | undefined>();
  return (cluster: string) => {
    const key = (cluster || "").trim();
    if (!key) return undefined;
    if (seen.has(key)) return seen.get(key);
    const found = byName.get(key) ?? logs.find((l) => l.name.includes(key) || l.path.includes(key));
    seen.set(key, found);
    return found;
  };
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
                Open in Prism
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

      {kbLinksOf(row).length > 0 ? (
        <Card size="small" title="Knowledge base">
          <Space size={[8, 8]} wrap>
            {kbLinksOf(row).map((url) => (
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

/**
 * Cluster name (linked to Prism when known) over one muted line with the
 * address and versions, so a row stays two lines tall. Both lines truncate
 * with the full text on hover.
 */
function ClusterCell({ row, url, subtitle }: { row: RowRecord; url?: string; subtitle?: string }) {
  const sub = subtitle && subtitle !== row.clusterName ? subtitle : "";
  return (
    <div className="alerts-cluster-cell">
      {url ? (
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          className="cluster-link kb-like-link"
          title={row.clusterName}
          onClick={(e) => e.stopPropagation()}
        >
          {row.clusterName}
        </a>
      ) : (
        <span className="alerts-cluster-name" title={row.clusterName}>
          {row.clusterName}
        </span>
      )}
      {sub ? (
        <span className="alerts-cluster-meta" title={sub}>
          {sub}
        </span>
      ) : null}
    </div>
  );
}

/** AOS and NCC versions stacked as labelled lines, so neither is truncated into the other. */
function VersionsCell({ row }: { row: RowRecord }) {
  if (!row.clusterVersion && !row.nccVersion) {
    return <Typography.Text type="secondary">—</Typography.Text>;
  }
  return (
    <dl className="alerts-versions">
      <div>
        <dt>AOS</dt>
        <dd title={row.clusterVersion || undefined}>{row.clusterVersion || "—"}</dd>
      </div>
      <div>
        <dt>NCC</dt>
        <dd title={row.nccVersion || undefined}>{row.nccVersion || "—"}</dd>
      </div>
    </dl>
  );
}

const SELECTION_COLUMN_WIDTH = 42;
const DETAIL_MIN_WIDTH = 260;
const PC_WIDE_ONLY_COLUMNS = new Set(["entityType", "impactType"]);

const ALERTS_TABLE_MIN_Y = 240;
const ALERTS_TABLE_FALLBACK_Y = 620;

function useAlertsTableScrollY(enabled: boolean, density: Density, expanded: boolean) {
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
      const headerH = header?.getBoundingClientRect().height ?? (density === "compact" ? 39 : 47);
      const hostH = host.clientHeight;
      if (hostH <= 0) {
        setTableY(expanded ? Math.max(ALERTS_TABLE_MIN_Y, Math.floor(window.innerHeight - 280)) : ALERTS_TABLE_FALLBACK_Y);
        return;
      }
      // Pagination lives outside this host so it cannot be clipped. scroll.y
      // is only the table body; do not subtract a pager that is not in here.
      setTableY(Math.max(ALERTS_TABLE_MIN_Y, Math.floor(hostH - headerH)));
    };

    measure();
    const frame = window.requestAnimationFrame(measure);
    const ro = typeof ResizeObserver !== "undefined" ? new ResizeObserver(measure) : null;
    ro?.observe(host);
    window.addEventListener("resize", measure);
    return () => {
      window.cancelAnimationFrame(frame);
      ro?.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [enabled, density, expanded]);

  return { hostRef, tableY };
}

function handlingLabel(status: string): { text: string; color: string } {
  if (status === "acknowledged") return { text: "Acknowledged", color: "gold" };
  if (status === "resolved") return { text: "Resolved", color: "green" };
  if (status === "reopened") return { text: "Returned", color: "magenta" };
  return { text: "Open", color: "default" };
}

function NCCAlertInspector({
  row,
  mark,
  note,
  onNote,
  pending,
  onAction,
}: {
  row: RowRecord;
  mark?: NCCDisposition;
  note: string;
  onNote: (value: string) => void;
  pending?: boolean;
  /** Absent when alert handling is turned off: marks are shown read-only. */
  onAction?: (action: "acknowledge" | "resolve" | "reopen") => void;
}) {
  const current = handlingLabel(mark?.status || "");
  const history = [...(mark?.history || [])].reverse();
  const hasMark = mark?.status === "acknowledged" || mark?.status === "resolved" || mark?.status === "reopened";
  return (
    <div className="ncc-alert-inspector">
      {mark?.status === "reopened" ? (
        <Alert
          type="warning"
          showIcon
          title="This alert came back"
          description={mark.reason || "It was resolved, then found again on a later run."}
        />
      ) : null}

      <Card size="small" className="ncc-handling-card" title="Handling" extra={<Tag color={current.color}>{current.text}</Tag>}>
        {mark?.by && mark.status ? (
          <Typography.Paragraph className="ncc-handling-who">
            {current.text} by <Typography.Text strong>{mark.by}</Typography.Text>
            {mark.at ? ` · ${formatDateTime(mark.at)}` : ""}
            {mark.run_at ? ` · run ${formatDateTime(mark.run_at)}` : ""}
          </Typography.Paragraph>
        ) : (
          <Typography.Paragraph type="secondary" className="ncc-handling-who">
            Nobody has acknowledged or resolved this alert yet.
          </Typography.Paragraph>
        )}
        {mark?.note ? <Typography.Paragraph className="ncc-handling-note">{mark.note}</Typography.Paragraph> : null}
        {mark?.id ? (
          <Typography.Paragraph type="secondary" className="ncc-handling-id">
            Alert ID <Typography.Text code copyable>{mark.id}</Typography.Text>
          </Typography.Paragraph>
        ) : null}
        {onAction ? (
          <>
            <Input.TextArea
              value={note}
              onChange={(event) => onNote(event.target.value)}
              maxLength={500}
              rows={2}
              placeholder="Optional note"
            />
            <Space wrap className="ncc-handling-actions">
              <Button disabled={pending} onClick={() => onAction("acknowledge")}>
                Acknowledge
              </Button>
              <Button type="primary" disabled={pending} onClick={() => onAction("resolve")}>
                Resolve
              </Button>
              {hasMark ? (
                <Button disabled={pending} onClick={() => onAction("reopen")}>
                  Reopen
                </Button>
              ) : null}
            </Space>
            <Typography.Paragraph type="secondary" className="ncc-handling-hint">
              Acknowledge notes that you have seen it. Resolve marks it done until a later run finds the same alert.
            </Typography.Paragraph>
          </>
        ) : (
          <Typography.Paragraph type="secondary" className="ncc-handling-hint">
            Alert handling is turned off under Settings → Features, so marks are read-only.
          </Typography.Paragraph>
        )}
      </Card>

      <NCCLifecyclePanel
        cluster={row.cluster}
        check={row.alert}
        pending={pending}
        onResolve={onAction ? () => onAction("resolve") : undefined}
      />

      <div className="pc-alert-metric-grid">
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">Cluster</div>
          <div className="pc-alert-metric-value">{row.clusterName}</div>
          {row.cluster && row.cluster !== row.clusterName ? (
            <Typography.Text type="secondary" className="mono">{row.cluster}</Typography.Text>
          ) : null}
        </div>
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">Versions</div>
          <div className="pc-alert-metric-value">{row.clusterVersion || "AOS not reported"}</div>
          <Typography.Text type="secondary">{row.nccVersion ? `NCC ${row.nccVersion}` : "NCC not reported"}</Typography.Text>
        </div>
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">Status</div>
          <Space size={4} wrap>
            {row.isChanged ? <Tag color="gold">Changed</Tag> : null}
            {row.isFlaky ? <Tag color="purple">Flaky</Tag> : null}
            {!row.isChanged && !row.isFlaky ? <Typography.Text type="secondary">None</Typography.Text> : null}
          </Space>
        </div>
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">Log</div>
          <div className="pc-alert-metric-value">{row.logName || "—"}</div>
        </div>
      </div>

      {kbLinksOf(row).length > 0 ? (
        <Card size="small" title="Knowledge base">
          <Space size={[8, 8]} wrap>
            {kbLinksOf(row).map((url) => (
              <a key={url} href={url} target="_blank" rel="noreferrer">
                <Tag color="processing" icon={<LinkOutlined />} style={{ margin: 0 }}>{kbLabel(url)}</Tag>
              </a>
            ))}
          </Space>
        </Card>
      ) : null}

      <Card size="small" title="Alert detail">
        <Typography.Paragraph className="pc-alert-message" style={{ margin: 0 }}>
          {row.detail || "No additional detail."}
        </Typography.Paragraph>
      </Card>

      {history.length > 0 ? (
        <Card size="small" title="History">
          <ol className="ncc-handling-history">
            {history.map((event, index) => {
              const label = handlingLabel(event.status).text;
              return (
                <li key={`${event.at}-${event.status}-${index}`}>
                  <Typography.Text strong>{label}</Typography.Text>
                  <Typography.Text type="secondary">
                    {" "}by {event.by || "unknown"}
                    {event.at ? ` · ${formatDateTime(event.at)}` : ""}
                  </Typography.Text>
                  {event.note ? <div>{event.note}</div> : null}
                  {event.reason ? <div>{event.reason}</div> : null}
                </li>
              );
            })}
          </ol>
        </Card>
      ) : null}
    </div>
  );
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
  dispositions,
  onDisposition,
  dispositionPending,
  runAt,
  onSummaryChange,
}: Props) {
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useLocalStorageState("dashboard.alerts.rowsPerPage", 100);
  const [density, setDensity] = useLocalStorageState<Density>("dashboard.alerts.density", "comfortable");
  const [expanded, setExpanded] = useLocalStorageState("dashboard.alerts.expanded", false);
  const [handling, setHandling] = useLocalStorageState<HandlingFilter>("dashboard.alerts.handling", "all");
  const [legacyNeeds, setLegacyNeeds] = useLocalStorageState("dashboard.alerts.needsAttention", false);
  const [drawerRow, setDrawerRow] = useState<RowRecord | null>(null);
  const [handlingNote, setHandlingNote] = useState("");
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [copyState, setCopyState] = useState<"idle" | "copied">("idle");
  const screens = Grid.useBreakpoint();
  // NCC rows can be marked unless alert handling is off (no onDisposition).
  const canHandle = alertSource !== "PC" && Boolean(onDisposition);

  // Rows are built once per data change; typing in search only re-filters
  // them (below). The search text is deferred so the input stays responsive
  // while 200k rows are filtered.
  const baseRows = useMemo<RowRecord[]>(() => {
    const findLog = logLookup(nccLogs || []);
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
    // A fleet report repeats ~20 check titles and 7,000 clusters across 200k
    // rows; normalize/resolve each distinct value once.
    const once = <T,>(fn: (key: string) => T) => {
      const memo = new Map<string, T>();
      return (key: string) => {
        let v = memo.get(key);
        if (v === undefined) {
          v = fn(key);
          memo.set(key, v);
        }
        return v;
      };
    };
    const titleOf = once(normalizeCheckTitle);
    const nameOf = once((value) => resolveClusterName(value, clusterNameMap));
    const mapToRow = (raw: Record<string, unknown>, idx: number, sourceHint?: string): RowRecord => {
      const sourceRaw = String(sourceHint ?? raw.source ?? "");
      const cluster = String(raw.cluster || raw.address || raw.cluster_name || "-");
      const alert = titleOf(String(raw.check_name ?? raw.check ?? raw.title ?? raw.alert ?? "-"));
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
      const status = sourceRaw.toUpperCase() === "PC" ? displayPCStatus(raw) : String(raw.status ?? "—");
      const impactType = String(raw.impact_type ?? raw.impactType ?? "—");
      const alertType = String(raw.alert_type ?? raw.alertType ?? "—");
      const acknowledged = raw.acknowledged === true ? "Yes" : raw.acknowledged === false ? "No" : "—";
      const resolved = raw.resolved === true ? "Yes" : raw.resolved === false ? "No" : "—";
      const severityRaw = String(raw.severity || "UNKNOWN").toUpperCase();
      const isUnknownSeverity = !["FAIL", "WARN", "ERR", "INFO"].includes(severityRaw);
      const severity = (isUnknownSeverity ? "ERR" : severityRaw) as Severity;
      const detail = String(raw.detail || raw.message || "");
      const diff = pickDiff(cluster, alert);
      const isChanged = Boolean(diff.new_fail || diff.resolved_fail || diff.severity_changed);
      const isFlaky = Boolean(fkeys[rowKey(cluster, alert)]);
      const log = findLog(cluster);
      const clusterName = nameOf(displayClusterName(raw));
      return {
        key: `${cluster}-${alert}-${idx}`,
        clusterName,
        searchText: `${clusterName} ${cluster} ${severity} ${alert}`.toLowerCase(),
        clusterKeys: [clusterName.toLowerCase(), nameOf(cluster).toLowerCase()],
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
        source: sourceRaw.toUpperCase() === "PC" ? "PC" : "NCC",
        severity,
        isUnknownSeverity,
        detail,
        raw,
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

    let built: RowRecord[] = [];
    const activeSourceFilter = String(alertSource || "NCC").toUpperCase();
    // No per-row copies: the source travels as an argument.
    const nccRows = activeSourceFilter === "PC" ? [] : aggRows || [];
    const pcRows = activeSourceFilter === "NCC" ? [] : pcAlerts || [];
    if (nccRows.length + pcRows.length > 0) {
      built = new Array<RowRecord>(nccRows.length + pcRows.length);
      nccRows.forEach((r, idx) => {
        built[idx] = mapToRow(asRecord(r), idx, "NCC");
      });
      pcRows.forEach((r, i) => {
        const idx = nccRows.length + i;
        built[idx] = mapToRow(asRecord(r), idx, "PC");
      });
    } else {
      const snapshot = asRecord(checksSnapshot);
      const legacyClusters = asArray(snapshot.clusters).map((c) => asRecord(c));
      if (legacyClusters.length > 0) {
        built = legacyClusters.flatMap((cluster, cidx) =>
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
        built = asArray(checksSnapshot).map((r, idx) => mapToRow(asRecord(r), idx));
      }
    }
    return built;
  }, [checksSnapshot, aggRows, diffFlags, flakyKeys, nccLogs, clusterNameMap, pcAlerts, alertSource]);

  const deferredFilterText = useDeferredValue(filterText);
  const rows = useMemo<RowRecord[]>(() => {
    const needle = deferredFilterText.toLowerCase();
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
    return baseRows.filter((r) => {
      if (severityFilters.length > 0 && !severityFilters.includes(r.severity as "FAIL" | "WARN" | "ERR" | "INFO")) return false;
      if (parsedTokens.sev && r.severity !== parsedTokens.sev) return false;
      if (parsedTokens.cluster && !`${r.clusterName} ${r.cluster}`.toLowerCase().includes(parsedTokens.cluster)) return false;
      if (selectedSet.size > 0 && !r.clusterKeys.some((k) => selectedSet.has(k))) return false;
      if (parsedTokens.changed && !r.isChanged) return false;
      if (parsedTokens.flaky && !r.isFlaky) return false;
      if (compareMode === "changed" && !r.isChanged) return false;
      if (compareMode === "flaky" && !r.isFlaky) return false;
      if (alertSource === "PC" && pcResolvedFilter !== "all" && r.resolved !== pcResolvedFilter) return false;
      return parsedTokens.terms.every((t) => r.searchText.includes(t));
    });
  }, [baseRows, deferredFilterText, selectedClusters, severityFilters, alertSource, pcResolvedFilter, compareMode]);

  const dispositionByKey = useMemo(() => {
    const map = new Map<string, NCCDisposition>();
    for (const item of dispositions || []) {
      map.set(nccDispositionKey(item.cluster, item.check), item);
    }
    return map;
  }, [dispositions]);
  const markFor = (row: RowRecord) =>
    dispositionByKey.get(nccDispositionKey(row.cluster, row.alert)) ||
    dispositionByKey.get(nccDispositionKey(row.clusterName, row.alert));
  const handlingStatus = (row: RowRecord) =>
    dispositionByKey.get(nccDispositionKey(row.cluster, row.alert))?.status ||
    dispositionByKey.get(nccDispositionKey(row.clusterName, row.alert))?.status ||
    "";
  const handlingCounts = useMemo(() => {
    const counts = { all: rows.length, open: 0, acknowledged: 0, resolved: 0, returned: 0 };
    if (dispositionByKey.size === 0) {
      // Nothing handled yet (the common case on a new run): all open.
      counts.open = rows.length;
      return counts;
    }
    for (const row of rows) {
      const status =
        dispositionByKey.get(nccDispositionKey(row.cluster, row.alert))?.status ||
        dispositionByKey.get(nccDispositionKey(row.clusterName, row.alert))?.status ||
        "";
      if (status === "acknowledged") counts.acknowledged += 1;
      else if (status === "resolved") counts.resolved += 1;
      else {
        counts.open += 1;
        if (status === "reopened") counts.returned += 1;
      }
    }
    return counts;
  }, [rows, dispositionByKey]);
  const shownRows = useMemo(() => {
    if (alertSource === "PC" || handling === "all") return rows;
    return rows.filter((r) => {
      const status = handlingStatus(r);
      if (handling === "open") return status !== "acknowledged" && status !== "resolved";
      if (handling === "acknowledged") return status === "acknowledged";
      if (handling === "resolved") return status === "resolved";
      return status === "reopened";
    });
  }, [rows, handling, alertSource, dispositionByKey]);

  const { hostRef, tableY } = useAlertsTableScrollY(shownRows.length > 0, density, expanded);

  const filterResetKey = [
    alertSource,
    pcResolvedFilter,
    compareMode,
    filterText,
    selectedClusters.join("\0"),
    severityFilters.join("\0"),
    handling,
  ].join("|");
  useEffect(() => {
    setPage(1);
    setSelectedKeys([]);
  }, [filterResetKey]);

  useEffect(() => {
    if (!legacyNeeds) return;
    setHandling("open");
    setLegacyNeeds(false);
  }, [legacyNeeds, setHandling, setLegacyNeeds]);

  useEffect(() => {
    setHandlingNote("");
  }, [drawerRow?.key]);

  useEffect(() => {
    document.documentElement.classList.toggle("alerts-expanded", expanded);
    return () => document.documentElement.classList.remove("alerts-expanded");
  }, [expanded]);

  useEffect(() => {
    if (!expanded) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || drawerRow) return;
      setExpanded(false);
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, [expanded, drawerRow, setExpanded]);

  const pageSize = Math.max(1, rowsPerPage);
  const pageCount = Math.max(1, Math.ceil(shownRows.length / pageSize) || 1);
  const currentPage = Math.min(page, pageCount);
  const pagedRows = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return shownRows.slice(start, start + pageSize);
  }, [shownRows, currentPage, pageSize]);

  useEffect(() => {
    if (page > pageCount) setPage(pageCount);
  }, [page, pageCount]);

  useEffect(() => {
    const body = hostRef.current?.querySelector(".ant-table-body");
    if (body) body.scrollTop = 0;
  }, [currentPage, pageSize, hostRef]);

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
    width: 240,
    sorter: (a: RowRecord, b: RowRecord) => a.alert.localeCompare(b.alert),
    render: (value: string, row: RowRecord) => (
      <Space orientation="vertical" size={2} style={{ width: "100%" }}>
        <Typography.Text strong style={{ wordBreak: "break-word" }}>
          {value}
        </Typography.Text>
        {row.isChanged || row.isFlaky ? (
          <Space size={4} wrap>
            {row.isChanged ? <Tag color="gold" style={{ margin: 0 }}>changed</Tag> : null}
            {row.isFlaky ? <Tag color="purple" style={{ margin: 0 }}>flaky</Tag> : null}
          </Space>
        ) : null}
      </Space>
    ),
  };
  const severityColumn = {
    title: "Severity",
    dataIndex: "severity",
    key: "severity",
    width: 96,
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
    width: 44,
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
      width: 180,
      sorter: (a, b) => a.clusterName.localeCompare(b.clusterName),
      render: (_, row) => (
        <ClusterCell row={row} url={clusterPrismURL(row.cluster, clusterNameMap)} subtitle={row.cluster} />
      ),
    },
    {
      title: "Versions",
      key: "versions",
      width: 130,
      sorter: (a, b) =>
        a.clusterVersion.localeCompare(b.clusterVersion, undefined, { numeric: true }) ||
        a.nccVersion.localeCompare(b.nccVersion, undefined, { numeric: true }),
      render: (_, row) => <VersionsCell row={row} />,
    },
    titleColumn,
    {
      title: "KB",
      key: "kb",
      width: 90,
      render: (_, row) => kbOf(row) ? (
        <a href={kbOf(row)} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
          <Tag color="processing" icon={<LinkOutlined />} style={{ margin: 0 }}>{kbLabel(kbOf(row))}</Tag>
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
    {
      title: "Handling",
      key: "handling",
      width: 110,
      render: (_, row) => {
        const mark = markFor(row);
        if (mark?.status === "acknowledged") {
          return <Tag color="gold">Acknowledged</Tag>;
        }
        if (mark?.status === "resolved") {
          return <Tag color="green">Resolved</Tag>;
        }
        if (mark?.status === "reopened") {
          return (
            <Tooltip title={mark.reason || "This alert was resolved, then found again on a later run."}>
              <Tag color="magenta">Returned</Tag>
            </Tooltip>
          );
        }
        return <Typography.Text type="secondary">Open</Typography.Text>;
      },
    },
    actionColumn,
  ];

  const pcColumns: ColumnsType<RowRecord> = [
    {
      title: "Title",
      dataIndex: "alert",
      key: "title",
      width: 250,
      sorter: (a, b) => a.alert.localeCompare(b.alert),
    },
    severityColumn,
    {
      title: "Entity name",
      dataIndex: "entityName",
      key: "entityName",
      width: 190,
      sorter: (a, b) => a.entityName.localeCompare(b.entityName),
    },
    {
      title: "Entity type",
      dataIndex: "entityType",
      key: "entityType",
      width: 130,
    },
    {
      title: "Cluster",
      dataIndex: "clusterName",
      key: "clusterName",
      width: 200,
      sorter: (a, b) => a.clusterName.localeCompare(b.clusterName),
      render: (_, row) => {
        const url = clusterPrismURL(row.cluster, clusterNameMap);
        const subtitle = row.clusterIP && row.clusterIP !== row.clusterName
          ? row.clusterIP
          : (row.cluster && row.cluster !== row.clusterName ? row.cluster : "");
        return <ClusterCell row={row} url={url} subtitle={subtitle} />;
      },
    },
    {
      title: "Last occurred",
      dataIndex: "lastOccurred",
      key: "lastOccurred",
      width: 170,
      sorter: (a, b) => a.lastOccurredTime - b.lastOccurredTime,
      render: (value: string) => (value === "—" ? value : formatDateTime(value)),
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
    },
    {
      title: "Resolved",
      dataIndex: "resolved",
      key: "resolved",
      width: 96,
      render: (value: string) => <Tag color={value === "Yes" ? "green" : "blue"}>{value}</Tag>,
    },
    {
      title: "Impact type",
      dataIndex: "impactType",
      key: "impactType",
      width: 130,
    },
    actionColumn,
  ];
  // Below 1600px (a 14" laptop) the PC table drops the two least-used
  // columns rather than scroll sideways; both stay in the details drawer.
  const columns =
    alertSource === "PC"
      ? pcColumns.filter((c) => screens.xxl || !PC_WIDE_ONLY_COLUMNS.has(String(c.key)))
      : nccColumns;
  // Fixed columns plus room for the flexible Detail column (and checkboxes).
  const scrollX =
    columns.reduce((sum, c) => sum + (typeof c.width === "number" ? c.width : DETAIL_MIN_WIDTH), 0) +
    (canHandle ? SELECTION_COLUMN_WIDTH : 0);

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
    <Card className={`alerts-card page-card${expanded ? " alerts-card-expanded" : ""}`}>
      <div className="alerts-header">
        <div>
          <Typography.Title level={4} className="tile-title">
            Alerts
          </Typography.Title>
          <Typography.Text type="secondary" className="tile-subtitle">
            {shownRows.length.toLocaleString()} {shownRows.length === 1 ? "alert" : "alerts"} shown
            {shownRows.length !== totalRows ? ` · ${totalRows.toLocaleString()} match the dashboard filters` : ""}
          </Typography.Text>
        </div>
        {/* Severity totals sit above the filters; no second copy here. */}
        <Space size={[8, 8]} wrap className="alerts-summary-pills">
          <Tooltip title={density === "compact" ? "Switch to comfortable rows" : "Switch to compact rows"}>
            <Button
              size="small"
              onClick={() => setDensity(density === "compact" ? "comfortable" : "compact")}
            >
              {density === "compact" ? "Compact" : "Comfortable"}
            </Button>
          </Tooltip>
          <Tooltip title={expanded ? "Leave full page (Esc)" : "Show the alert list across the page. Filters and details stay available."}>
            <Button
              size="small"
              aria-label={expanded ? "Exit full page" : "Expand alerts"}
              icon={expanded ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
              onClick={() => setExpanded(!expanded)}
            >
              {expanded ? "Exit full page" : "Full page"}
            </Button>
          </Tooltip>
        </Space>
      </div>
      {alertSource !== "PC" ? (
        <div className="alerts-handling-bar">
          <div className="alerts-handling-filters" role="radiogroup" aria-label="NCC handling filter">
            {HANDLING_FILTERS.map((item) => {
              const count = handlingCounts[item.value];
              const selected = handling === item.value;
              return (
                <Tooltip key={item.value} title={item.hint}>
                  <button
                    type="button"
                    role="radio"
                    aria-checked={selected}
                    aria-label={`${item.label}, ${count} ${count === 1 ? "alert" : "alerts"}`}
                    className={`alerts-handling-chip alerts-handling-chip-${item.tone}${selected ? " is-selected" : ""}${count === 0 && item.value !== "all" ? " is-empty" : ""}`}
                    onClick={() => setHandling(item.value)}
                  >
                    <span className="alerts-handling-chip-label">{item.label}</span>
                    <span className="alerts-handling-chip-count">{count}</span>
                  </button>
                </Tooltip>
              );
            })}
          </div>
          <Typography.Text type="secondary" className="alerts-handling-hint">
            {handlingCounts.returned > 0
              ? `${handlingCounts.returned} resolved ${handlingCounts.returned === 1 ? "alert was" : "alerts were"} found again`
              : "Resolved alerts return here if a later run finds them again"}
          </Typography.Text>
        </div>
      ) : null}
      {canHandle && selectedKeys.length > 0 ? (
        <div className="alerts-bulk-bar">
          <Typography.Text strong>{selectedKeys.length} selected</Typography.Text>
          <Button
            size="small"
            disabled={dispositionPending}
            onClick={() => {
              const items = shownRows.filter((row) => selectedKeys.includes(row.key)).map((row) => ({ cluster: row.cluster, check: row.alert }));
              onDisposition?.({ action: "acknowledge", run_at: runAt, items });
              setSelectedKeys([]);
            }}
          >
            Acknowledge
          </Button>
          <Button
            size="small"
            type="primary"
            disabled={dispositionPending}
            onClick={() => {
              const items = shownRows.filter((row) => selectedKeys.includes(row.key)).map((row) => ({ cluster: row.cluster, check: row.alert }));
              onDisposition?.({ action: "resolve", run_at: runAt, items });
              setSelectedKeys([]);
            }}
          >
            Resolve
          </Button>
          <Button
            size="small"
            disabled={dispositionPending}
            onClick={() => {
              const items = shownRows.filter((row) => selectedKeys.includes(row.key)).map((row) => ({ cluster: row.cluster, check: row.alert }));
              onDisposition?.({ action: "reopen", run_at: runAt, items });
              setSelectedKeys([]);
            }}
          >
            Reopen
          </Button>
          <Button size="small" type="text" onClick={() => setSelectedKeys([])}>
            Clear
          </Button>
        </div>
      ) : null}

      {shownRows.length === 0 ? (
        <Empty
          style={{ padding: "32px 0" }}
          description={
            <Space orientation="vertical" size={2} align="center">
              <Typography.Text strong>
                {totalRows > 0 ? "No alerts in this handling view" : "No alerts match the current filters"}
              </Typography.Text>
              <Typography.Text type="secondary">
                {totalRows > 0 ? "Choose All to see acknowledged and resolved rows again." : "Try clearing severity chips or the cluster filter."}
              </Typography.Text>
            </Space>
          }
        />
      ) : (
        <>
          <div className="alerts-table-host" ref={hostRef} data-scroll-y={tableY}>
            <Table<RowRecord>
              virtual
              className="alerts-table"
              tableLayout="fixed"
              rowKey="key"
              columns={columns}
              dataSource={expanded ? shownRows : pagedRows}
              rowClassName={(_, index) => (index % 2 === 0 ? "alerts-row-even" : "alerts-row-odd")}
              onRow={(row) => ({
                onClick: (event) => {
                  const target = event.target as HTMLElement | null;
                  if (target?.closest("a, button, .ant-checkbox, .ant-table-selection-column")) return;
                  setDrawerRow(row);
                },
                style: { cursor: "pointer" },
              })}
              rowSelection={
                !canHandle
                  ? undefined
                  : {
                      selectedRowKeys: selectedKeys,
                      preserveSelectedRowKeys: true,
                      columnWidth: SELECTION_COLUMN_WIDTH,
                      onChange: (keys) => setSelectedKeys(keys.map(String)),
                    }
              }
              pagination={false}
              size={density === "compact" ? "small" : "middle"}
              scroll={{ x: scrollX, y: tableY }}
            />
          </div>
          {expanded ? (
            <Typography.Text type="secondary" className="alerts-expanded-count">
              Showing all {shownRows.length.toLocaleString()} {shownRows.length === 1 ? "alert" : "alerts"}
            </Typography.Text>
          ) : (
            <Pagination
              className="alerts-pagination"
              current={currentPage}
              pageSize={pageSize}
              total={shownRows.length}
              showSizeChanger
              pageSizeOptions={[50, 100, 200, 500]}
              showTotal={(total, range) => `${range[0]}–${range[1]} of ${total}`}
              onChange={(nextPage, nextPageSize) => {
                setPage(nextPage);
                if (nextPageSize && nextPageSize !== rowsPerPage) setRowsPerPage(nextPageSize);
              }}
            />
          )}
        </>
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
                  rel="noopener noreferrer"
                >
                  Open in Prism
                </Button>
              ) : null}
              {drawerRow.source === "PC" && kbOf(drawerRow) ? (
                <Button type="primary" icon={<LinkOutlined />} href={kbOf(drawerRow)} target="_blank" rel="noopener noreferrer">
                  {kbLabel(kbOf(drawerRow))}
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
            <NCCAlertInspector
              row={drawerRow}
              mark={markFor(drawerRow)}
              note={handlingNote}
              onNote={setHandlingNote}
              pending={dispositionPending}
              onAction={
                onDisposition
                  ? (action) =>
                      onDisposition({
                        cluster: drawerRow.cluster,
                        check: drawerRow.alert,
                        action,
                        note: handlingNote.trim(),
                        run_at: runAt,
                      })
                  : undefined
              }
            />
          )
        ) : null}
      </Drawer>
    </Card>
  );
}
