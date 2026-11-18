import { useDeferredValue, useEffect, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent, type PointerEvent as ReactPointerEvent, type ReactNode, type ThHTMLAttributes } from "react";
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Drawer,
  Empty,
  Grid,
  Input,
  Pagination,
  Segmented,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { AimOutlined, AppstoreOutlined, BarsOutlined, CheckOutlined, ClockCircleOutlined, CloseCircleOutlined, ClusterOutlined, ColumnWidthOutlined, CopyOutlined, DownOutlined, ExclamationCircleOutlined, ExportOutlined, FileTextOutlined, FullscreenExitOutlined, FullscreenOutlined, HistoryOutlined, InfoCircleOutlined, MessageOutlined, ProfileOutlined, QuestionCircleOutlined, ReadOutlined, RightOutlined, SearchOutlined, TableOutlined, ToolOutlined, WarningOutlined } from "@ant-design/icons";
import { Link } from "react-router";
import { KeyValueList, Panel, StatusBadge, type KeyValueItem } from "../../ui";
import { clusterPath } from "../../navigation";
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
  /** Search the table for one check across every cluster (the drawer's "This check on all clusters"). */
  onShowCheck?: (check: string) => void;
  selectedClusters: string[];
  /** Only rows on these clusters (an environment's list); null for all. */
  environmentClusters?: string[] | null;
  clusterNameMap: Record<string, string>;
  severityFilters: Array<"FAIL" | "WARN" | "ERR" | "INFO">;
  pcAlerts?: unknown[];
  alertSource: "NCC" | "PC";
  pcResolvedFilter: "all" | "No" | "Yes";
  compareMode: "all" | "changed" | "flaky";
  dispositions?: NCCDisposition[];
  /**
   * Left out when alert handling is turned off or the user cannot change
   * marks (viewer): no checkboxes, bulk bar or handling buttons.
   */
  onDisposition?: (input: {
    action: "acknowledge" | "resolve" | "reopen";
    note?: string;
    run_at?: string;
    cluster?: string;
    check?: string;
    items?: Array<{ cluster: string; check: string }>;
  }) => void;
  dispositionPending?: boolean;
  /** Why marks are read-only when onDisposition is left out. */
  readOnlyReason?: string;
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
  { value: "all", label: "All", hint: "Every finding that matches the current filters.", tone: "all" },
  { value: "open", label: "Needs attention", hint: "Open findings that still need a decision.", tone: "open" },
  { value: "acknowledged", label: "Acknowledged", hint: "Noted, and still open.", tone: "acked" },
  { value: "resolved", label: "Resolved", hint: "Marked done. If a later run finds it again, it moves to Returned.", tone: "resolved" },
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
    .replace(/^(FAIL|WARN|WARNING|ERR|ERROR|INFO)\s*:\s*/i, "")
    .trim()
    .slice(0, 220);
}

/**
 * What can be done from a finding: read the KB article (the main action when
 * there is one), open the cluster in Prism or on its own page, and, for NCC,
 * see the same check on every cluster.
 */
function FindingActions({
  row,
  clusterNameMap,
  onShowCheck,
}: {
  row: RowRecord;
  clusterNameMap: Record<string, string>;
  /** Shows this check on every cluster in place; without it the link opens Findings. */
  onShowCheck?: (check: string) => void;
}) {
  const kbLinks = kbLinksOf(row);
  const prismURL = clusterPrismURL(row.cluster, clusterNameMap, row.clusterName);
  const card = (icon: ReactNode, title: string, sub: string, trail: ReactNode) => (
    <>
      <span className="finding-link-icon" aria-hidden="true">
        {icon}
      </span>
      <span className="finding-link-body">
        <span className="finding-link-title">{title}</span>
        <span className="finding-link-sub">{sub}</span>
      </span>
      <span className="finding-link-trail" aria-hidden="true">
        {trail}
      </span>
    </>
  );
  const checkHref = `/findings?q=${encodeURIComponent(row.alert)}`;
  return (
    <nav className="finding-links" aria-label="Next steps">
      {kbLinks.length > 0 ? (
        kbLinks.map((url, i) => (
          <a
            key={url}
            className={`finding-link${i === 0 ? " is-primary" : ""}`}
            href={url}
            target="_blank"
            rel="noopener noreferrer"
          >
            {card(<ReadOutlined />, kbLabel(url), "Nutanix knowledge base", <ExportOutlined />)}
          </a>
        ))
      ) : (
        <div className="finding-link is-empty">
          {card(<ReadOutlined />, "No KB article", "This result names no article", null)}
        </div>
      )}
      {prismURL ? (
        <a className="finding-link" href={prismURL} target="_blank" rel="noopener noreferrer">
          {card(<ExportOutlined />, "Open in Prism", prismURL.replace(/^https?:\/\//, ""), <ExportOutlined />)}
        </a>
      ) : null}
      <Link className="finding-link" to={clusterPath(row.cluster)}>
        {card(<ClusterOutlined />, "Cluster page", row.clusterName, <RightOutlined />)}
      </Link>
      {row.source === "NCC" ? (
        onShowCheck ? (
          <button type="button" className="finding-link" onClick={() => onShowCheck(row.alert)}>
            {card(<SearchOutlined />, "This check on all clusters", "Search the findings table", <RightOutlined />)}
          </button>
        ) : (
          <Link className="finding-link" to={checkHref}>
            {card(<SearchOutlined />, "This check on all clusters", "Open in Findings", <RightOutlined />)}
          </Link>
        )
      ) : null}
    </nav>
  );
}

function PCAlertInspector({
  row,
  clusterNameMap,
}: {
  row: RowRecord;
  clusterNameMap: Record<string, string>;
}) {
  const clusterSubtitle = row.clusterIP && row.clusterIP !== row.clusterName ? row.clusterIP : "";
  const showRootCause = Boolean(row.rootCause) && row.rootCause !== row.detail;
  const timeline = [
    { label: "Created", value: row.createdAt },
    { label: "Last occurred", value: row.lastOccurred },
    { label: "Updated", value: row.updatedAt },
    { label: "Resolved", value: row.resolvedAt },
  ].filter((item) => item.value && item.value !== "—");

  const status = row.status !== "—" ? row.status : row.resolved === "Yes" ? "Resolved" : "Open";
  const facts: KeyValueItem[] = [
    { label: "Status", value: status },
    { label: "Resolved", value: row.autoResolved ? "Automatically" : row.resolved },
    { label: "Acknowledged", value: row.acknowledged },
  ];
  if (row.impactType !== "—") facts.push({ label: "Impact", value: row.impactType.replace(/_/g, " ") });
  if (row.entityName && row.entityName !== "—") {
    facts.push({ label: "Affects", value: row.entityType !== "—" ? `${row.entityName} (${row.entityType})` : row.entityName });
  }
  facts.push({
    label: "Cluster",
    value: (
      <Link to={clusterPath(row.cluster)} className="drawer-cluster-link">
        {row.clusterName}
        {clusterSubtitle ? <span className="mono drawer-cluster-address">{clusterSubtitle}</span> : null}
      </Link>
    ),
    title: row.clusterName,
  });
  if (row.clusterUUID) facts.push({ label: "Cluster UUID", value: row.clusterUUID, mono: true, copy: row.clusterUUID });
  timeline.forEach((item) => facts.push({ label: item.label, value: formatStamp(item.value) }));
  if (row.alertType !== "—") facts.push({ label: "Alert type", value: row.alertType, mono: true });
  if (row.serviceName) facts.push({ label: "Service", value: row.serviceName });
  if (row.extId) facts.push({ label: "Alert ID", value: row.extId, mono: true, copy: row.extId });

  return (
    <div className="finding-inspector">
      <Panel title="What Prism Central reports" icon={<MessageOutlined />}>
        <Typography.Paragraph className="finding-message">{row.detail || "Prism Central sent no message."}</Typography.Paragraph>
        {showRootCause ? (
          <>
            <Typography.Text type="secondary" className="finding-subhead">
              <AimOutlined /> Root cause
            </Typography.Text>
            <Typography.Paragraph className="finding-message">{row.rootCause}</Typography.Paragraph>
          </>
        ) : null}
        <FindingActions row={row} clusterNameMap={clusterNameMap} />
      </Panel>

      <Panel title="Details" icon={<ClockCircleOutlined />}>
        <KeyValueList items={facts} />
      </Panel>
    </div>
  );
}

function displayPCStatus(raw: Record<string, unknown>): string {
  const status = String(raw.status ?? "").toUpperCase();
  if (status === "AUTO_RESOLVED" || raw.isAutoResolved === true || raw.auto_resolved === true) {
    const resolved = String(raw.resolvedTime ?? raw.resolved_at ?? "").trim();
    return resolved ? `Resolved automatically (${new Date(resolved).toLocaleString()})` : "Resolved automatically";
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
      <ClusterNameLine row={row} url={url} />
      {sub ? (
        <span className="alerts-cluster-meta" title={sub}>
          {sub}
        </span>
      ) : null}
    </div>
  );
}

function ClusterNameLine({ row, url }: { row: RowRecord; url?: string }) {
  return (
    <span className="alerts-cluster-name-line">
      <Link
        to={clusterPath(row.cluster)}
        className="alerts-cluster-link"
        title={`Open the ${row.clusterName} cluster page`}
        onClick={(e) => e.stopPropagation()}
      >
        {row.clusterName}
      </Link>
      {url ? (
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          className="alerts-cluster-prism"
          aria-label={`Open ${row.clusterName} in Prism`}
          title="Open in Prism"
          onClick={(e) => e.stopPropagation()}
        >
          <ExportOutlined />
        </a>
      ) : null}
    </span>
  );
}

/** A dot and a label, so open and handled rows read as one column of states. */
function HandlingStatus({ tone, label }: { tone: "open" | "ack" | "resolved" | "returned"; label: string }) {
  return (
    <span className={`alerts-status alerts-status-${tone}`}>
      <span className="alerts-status-dot" aria-hidden />
      {label}
    </span>
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

/** How the NCC findings are laid out; PC alerts always use the table. */
type FindingsView = "table" | "focused" | "grouped" | "list";

const VIEW_OPTIONS: Array<{ value: FindingsView; label: string; icon: ReactNode; hint: string }> = [
  { value: "table", label: "Table", icon: <TableOutlined />, hint: "Every column, including versions and detail." },
  { value: "focused", label: "Focused", icon: <ProfileOutlined />, hint: "Fewer columns: the finding with its detail, cluster, KB and status." },
  { value: "grouped", label: "By check", icon: <AppstoreOutlined />, hint: "One row per check, with the clusters it fails on." },
  { value: "list", label: "List", icon: <BarsOutlined />, hint: "Each finding as a short sentence, easiest to scan." },
];
const VIEW_KEYS = new Set<string>(VIEW_OPTIONS.map((v) => v.value));

const SEVERITY_RANK: Record<Severity, number> = { FAIL: 5, WARN: 4, ERR: 3, INFO: 2, UNKNOWN: 1 };

function severityOf(row: RowRecord): Severity {
  return row.isUnknownSeverity ? "UNKNOWN" : row.severity;
}

function SeverityIcon({ severity }: { severity: Severity }) {
  const icon =
    severity === "FAIL" ? <CloseCircleOutlined /> :
    severity === "WARN" ? <WarningOutlined /> :
    severity === "ERR" ? <ExclamationCircleOutlined /> :
    severity === "INFO" ? <InfoCircleOutlined /> :
    <QuestionCircleOutlined />;
  return (
    <span className={`findings-sev-icon sev-${severity.toLowerCase()}`} role="img" aria-label={severity}>
      {icon}
    </span>
  );
}

const COLUMN_MIN_WIDTH = 64;
const COLUMN_MAX_WIDTH = 900;

type ResizableHeaderProps = ThHTMLAttributes<HTMLTableCellElement> & {
  onResize?: (width: number) => void;
  onResetWidth?: () => void;
  resizeLabel?: string;
};

/**
 * Table header cell with a drag handle on its right edge. Drag (or focus it
 * and use the arrow keys) to resize; double-click puts the default back.
 */
function ResizableHeaderCell({ onResize, onResetWidth, resizeLabel, children, className, ...rest }: ResizableHeaderProps) {
  if (!onResize) {
    return (
      <th className={className} {...rest}>
        {children}
      </th>
    );
  }
  const clamp = (w: number) => Math.min(COLUMN_MAX_WIDTH, Math.max(COLUMN_MIN_WIDTH, Math.round(w)));
  const startDrag = (event: ReactPointerEvent<HTMLSpanElement>) => {
    event.preventDefault();
    event.stopPropagation();
    const th = event.currentTarget.parentElement;
    if (!th) return;
    const startX = event.clientX;
    const startWidth = th.getBoundingClientRect().width;
    const move = (e: PointerEvent) => onResize(clamp(startWidth + e.clientX - startX));
    const stop = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
      document.body.classList.remove("is-col-resizing");
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop);
    document.body.classList.add("is-col-resizing");
  };
  const onKey = (event: ReactKeyboardEvent<HTMLSpanElement>) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const th = event.currentTarget.parentElement;
    const current = th ? th.getBoundingClientRect().width : 120;
    onResize(clamp(current + (event.key === "ArrowRight" ? 16 : -16)));
  };
  return (
    <th className={`${className ?? ""} is-resizable`} {...rest}>
      {children}
      <span
        role="separator"
        aria-orientation="vertical"
        aria-label={resizeLabel}
        tabIndex={0}
        title="Drag to resize · double-click to reset"
        className="alerts-col-resizer"
        onPointerDown={startDrag}
        onDoubleClick={(e) => {
          e.stopPropagation();
          onResetWidth?.();
        }}
        onClick={(e) => e.stopPropagation()}
        onKeyDown={onKey}
      />
    </th>
  );
}

const TABLE_COMPONENTS = { header: { cell: ResizableHeaderCell } };

/** Distinct checks among rows (the By check view pages by check). */
function groupCountFor(rows: RowRecord[]): number {
  return new Set(rows.map((r) => r.alert)).size;
}

const SELECTION_COLUMN_WIDTH = 42;
const DETAIL_MIN_WIDTH = 220;
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
  clusterNameMap,
  onShowCheck,
  mark,
  note,
  onNote,
  pending,
  onAction,
  readOnlyReason,
}: {
  row: RowRecord;
  clusterNameMap: Record<string, string>;
  onShowCheck?: (check: string) => void;
  mark?: NCCDisposition;
  note: string;
  onNote: (value: string) => void;
  pending?: boolean;
  /** Absent when marks are read-only (handling off, or a viewer). */
  onAction?: (action: "acknowledge" | "resolve" | "reopen") => void;
  readOnlyReason?: string;
}) {
  const current = handlingLabel(mark?.status || "");
  const history = [...(mark?.history || [])].reverse();
  const hasMark = mark?.status === "acknowledged" || mark?.status === "resolved" || mark?.status === "reopened";
  const address = row.clusterIP || (row.cluster !== row.clusterName ? row.cluster : "");
  const facts: KeyValueItem[] = [
    {
      label: "Cluster",
      value: (
        <Link to={clusterPath(row.cluster)} className="drawer-cluster-link">
          {row.clusterName}
          {address && address !== row.clusterName ? <span className="mono drawer-cluster-address">{address}</span> : null}
        </Link>
      ),
      title: row.clusterName,
    },
    { label: "AOS", value: row.clusterVersion || "Not reported" },
    { label: "NCC", value: row.nccVersion || "Not reported" },
  ];
  if (row.logName) facts.push({ label: "Run log", value: row.logName, mono: true, title: row.logPath || row.logName });
  return (
    <div className="finding-inspector">
      {mark?.status === "reopened" ? (
        <Alert
          type="warning"
          showIcon
          title="This finding came back"
          description={mark.reason || "It was resolved, then found again on a later run."}
        />
      ) : null}

      <Panel
        title="What it found"
        icon={<FileTextOutlined />}
        extra={
          row.isChanged || row.isFlaky ? (
            <Space size={4}>
              {row.isChanged ? <StatusBadge tone="warn">Changed since last run</StatusBadge> : null}
              {row.isFlaky ? <StatusBadge tone="info">Flaky</StatusBadge> : null}
            </Space>
          ) : undefined
        }
      >
        <Typography.Paragraph className="finding-message">{row.detail || "The check reported no detail."}</Typography.Paragraph>
        <FindingActions row={row} clusterNameMap={clusterNameMap} onShowCheck={onShowCheck} />
        <KeyValueList items={facts} />
      </Panel>

      <Panel title="Handling" icon={<ToolOutlined />} extra={<Tag color={current.color}>{current.text}</Tag>}>
        {mark?.by && mark.status ? (
          <Typography.Paragraph className="ncc-handling-who">
            {current.text} by <Typography.Text strong>{mark.by}</Typography.Text>
            {mark.at ? ` · ${formatDateTime(mark.at)}` : ""}
            {mark.run_at ? ` · run ${formatDateTime(mark.run_at)}` : ""}
          </Typography.Paragraph>
        ) : (
          <Typography.Paragraph type="secondary" className="ncc-handling-who">
            Nobody has acknowledged or resolved this finding yet.
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
              Acknowledge notes that you have seen it. Resolve marks it done until a later run finds it again.
            </Typography.Paragraph>
          </>
        ) : (
          <Typography.Paragraph type="secondary" className="ncc-handling-hint">
            {readOnlyReason || "Finding handling is turned off under Settings → Features, so marks are read-only."}
          </Typography.Paragraph>
        )}
      </Panel>

      <NCCLifecyclePanel
        cluster={row.cluster}
        check={row.alert}
        pending={pending}
        onResolve={onAction ? () => onAction("resolve") : undefined}
      />

      {history.length > 0 ? (
        <Panel title="Handling history" icon={<HistoryOutlined />}>
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
        </Panel>
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
  onShowCheck,
  selectedClusters,
  environmentClusters = null,
  clusterNameMap,
  severityFilters,
  pcAlerts,
  alertSource,
  pcResolvedFilter,
  compareMode,
  dispositions,
  onDisposition,
  dispositionPending,
  readOnlyReason,
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
  const [view, setView] = useLocalStorageState<FindingsView>("dashboard.alerts.view", "table");
  const [columnWidths, setColumnWidths] = useLocalStorageState<Record<string, number>>("dashboard.alerts.columnWidths", {});
  const [openGroups, setOpenGroups] = useState<string[]>([]);
  const [handlingNote, setHandlingNote] = useState("");
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [copyState, setCopyState] = useState<"idle" | "copied">("idle");
  const screens = Grid.useBreakpoint();
  // NCC rows can be marked unless alert handling is off (no onDisposition).
  const canHandle = alertSource !== "PC" && Boolean(onDisposition);
  const activeView: FindingsView = alertSource === "PC" || !VIEW_KEYS.has(view) ? "table" : view;

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
        // Names and addresses, so filters given either (environment files list addresses) match.
        clusterKeys: [clusterName, nameOf(cluster), cluster, String(raw.cluster_ip ?? raw.address ?? "")]
          .map((k) => k.trim().toLowerCase())
          .filter(Boolean),
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
    const envSet = environmentClusters ? new Set(environmentClusters.map((v) => v.toLowerCase())) : null;
    return baseRows.filter((r) => {
      if (envSet && !r.clusterKeys.some((k) => envSet.has(k))) return false;
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
  }, [baseRows, deferredFilterText, selectedClusters, environmentClusters, severityFilters, alertSource, pcResolvedFilter, compareMode]);

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
  const pageCount = Math.max(1, Math.ceil((activeView === "grouped" ? groupCountFor(shownRows) : shownRows.length) / pageSize) || 1);
  const currentPage = Math.min(page, pageCount);
  // The list and grouped views have no column sorters: worst first.
  const orderedRows = useMemo(() => {
    if (activeView === "table" || activeView === "focused") return shownRows;
    return [...shownRows].sort(
      (a, b) =>
        SEVERITY_RANK[severityOf(b)] - SEVERITY_RANK[severityOf(a)] ||
        a.alert.localeCompare(b.alert) ||
        a.clusterName.localeCompare(b.clusterName),
    );
  }, [shownRows, activeView]);
  // By check: one group per check, worst and most widespread first.
  const groups = useMemo(() => {
    if (activeView !== "grouped") return [];
    const byCheck = new Map<string, RowRecord[]>();
    for (const row of orderedRows) {
      const list = byCheck.get(row.alert);
      if (list) list.push(row);
      else byCheck.set(row.alert, [row]);
    }
    return Array.from(byCheck, ([check, rows]) => {
      const severity = rows.reduce<Severity>((worst, r) => (SEVERITY_RANK[severityOf(r)] > SEVERITY_RANK[worst] ? severityOf(r) : worst), "UNKNOWN");
      const open = rows.filter((r) => {
        const st = handlingStatus(r);
        return st !== "acknowledged" && st !== "resolved";
      }).length;
      return { check, rows, severity, open };
    }).sort((a, b) => SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity] || b.rows.length - a.rows.length || a.check.localeCompare(b.check));
  }, [orderedRows, activeView, dispositionByKey]);
  const pagerTotal = activeView === "grouped" ? groups.length : shownRows.length;
  const pagedRows = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return orderedRows.slice(start, start + pageSize);
  }, [orderedRows, currentPage, pageSize]);
  const pagedGroups = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return groups.slice(start, start + pageSize);
  }, [groups, currentPage, pageSize]);

  useEffect(() => {
    if (page > pageCount) setPage(pageCount);
  }, [page, pageCount]);

  useEffect(() => {
    const body = hostRef.current?.querySelector(".ant-table-body") ?? hostRef.current;
    if (body) body.scrollTop = 0;
  }, [currentPage, pageSize, hostRef, activeView]);

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
  const severityRank = SEVERITY_RANK;

  const openDetails = (row: RowRecord) => setDrawerRow(row);
  // The finding name opens its details (row clicks do too); it is a real
  // button so keyboard users get there without a separate icon column.
  const findingButton = (row: RowRecord, className = "alerts-title") => (
    <button
      type="button"
      className={`${className} alerts-title-button`}
      title={row.alert}
      aria-label={`Open details for ${row.alert} on ${row.clusterName || row.cluster}`}
      onClick={(e) => {
        e.stopPropagation();
        openDetails(row);
      }}
    >
      {row.alert}
    </button>
  );
  const flagsFor = (row: RowRecord) =>
    row.isChanged || row.isFlaky ? (
      <span className="alerts-title-flags">
        {row.isChanged ? <Tag color="gold" bordered={false}>changed</Tag> : null}
        {row.isFlaky ? <Tag color="purple" bordered={false}>flaky</Tag> : null}
      </span>
    ) : null;
  const kbLink = (row: RowRecord) => {
    const kb = kbOf(row);
    return kb ? (
      <a
        className="alerts-kb-link"
        href={kb}
        target="_blank"
        rel="noreferrer"
        title="Open the Nutanix KB article"
        onClick={(e) => e.stopPropagation()}
      >
        <ReadOutlined aria-hidden />
        {kbLabel(kb)}
      </a>
    ) : null;
  };
  const statusFor = (row: RowRecord) => {
    const mark = markFor(row);
    if (mark?.status === "acknowledged") return <HandlingStatus tone="ack" label="Acknowledged" />;
    if (mark?.status === "resolved") return <HandlingStatus tone="resolved" label="Resolved" />;
    if (mark?.status === "reopened") {
      return (
        <Tooltip title={mark.reason || "This finding was resolved, then found again on a later run."}>
          <HandlingStatus tone="returned" label="Returned" />
        </Tooltip>
      );
    }
    return <HandlingStatus tone="open" label="Open" />;
  };
  const isSelected = (key: string) => selectedKeys.includes(key);
  const setSelected = (keys: string[], on: boolean) =>
    setSelectedKeys((prev) => (on ? Array.from(new Set([...prev, ...keys])) : prev.filter((k) => !keys.includes(k))));

  const clusterColumn = {
    title: "Cluster",
    dataIndex: "clusterName",
    key: "clusterName",
    width: 170,
    sorter: (a: RowRecord, b: RowRecord) => a.clusterName.localeCompare(b.clusterName),
    render: (_: unknown, row: RowRecord) => (
      <ClusterCell row={row} url={clusterPrismURL(row.cluster, clusterNameMap)} subtitle={row.cluster} />
    ),
  };
  const kbColumn = {
    title: "KB",
    key: "kb",
    width: 92,
    render: (_: unknown, row: RowRecord) => kbLink(row) ?? <span className="alerts-empty-cell">—</span>,
  };
  const handlingColumn = {
    title: "Handling",
    key: "handling",
    width: 120,
    render: (_: unknown, row: RowRecord) => statusFor(row),
  };

  const titleColumn = {
    title: "Finding",
    dataIndex: "alert",
    key: "alert",
    width: 210,
    sorter: (a: RowRecord, b: RowRecord) => a.alert.localeCompare(b.alert),
    render: (_: string, row: RowRecord) => (
      <div className="alerts-title-cell">
        {findingButton(row)}
        {flagsFor(row)}
      </div>
    ),
  };
  const severityColumn = {
    title: "Severity",
    dataIndex: "severity",
    key: "severity",
    width: 92,
    sorter: (a: RowRecord, b: RowRecord) => severityRank[a.severity] - severityRank[b.severity],
    defaultSortOrder: "descend" as const,
    render: (value: Severity, row: RowRecord) => {
      const sev = row.isUnknownSeverity ? "UNKNOWN" : value;
      const className = row.isUnknownSeverity ? "severity-pill severity-pill-unknown" : "severity-pill";
      return <Tag className={className} color={SEVERITY_TAG_COLOR[sev]}>{sev}</Tag>;
    },
  };
  const nccColumns: ColumnsType<RowRecord> = [
    severityColumn,
    clusterColumn,
    {
      title: "Versions",
      key: "versions",
      width: 116,
      sorter: (a, b) =>
        a.clusterVersion.localeCompare(b.clusterVersion, undefined, { numeric: true }) ||
        a.nccVersion.localeCompare(b.nccVersion, undefined, { numeric: true }),
      render: (_, row) => <VersionsCell row={row} />,
    },
    titleColumn,
    kbColumn,
    {
      title: "Detail",
      dataIndex: "detail",
      key: "detail",
      render: (value: string) => {
        const text = previewDetail(value);
        return (
          <span className={`alerts-detail alerts-detail-${density === "compact" ? "1" : "2"}`} title={text === "—" ? undefined : text}>
            {text}
          </span>
        );
      },
    },
    handlingColumn,
  ];

  // Focused: the finding and its detail in one cell, a severity edge
  // instead of a severity column, and no versions (they are in the drawer).
  const focusedColumns: ColumnsType<RowRecord> = [
    {
      title: <span className="sr-only">Severity</span>,
      key: "severityEdge",
      width: 28,
      sorter: (a, b) => SEVERITY_RANK[severityOf(a)] - SEVERITY_RANK[severityOf(b)],
      defaultSortOrder: "descend",
      render: (_, row) => (
        <span className={`alerts-sev-edge sev-${severityOf(row).toLowerCase()}`} title={severityOf(row)} role="img" aria-label={severityOf(row)} />
      ),
    },
    {
      title: "Finding",
      key: "finding",
      sorter: (a, b) => a.alert.localeCompare(b.alert),
      render: (_, row) => {
        const text = previewDetail(row.detail);
        return (
          <div className="alerts-focus-cell">
            <span className="alerts-focus-title">
              {findingButton(row, "alerts-focus-name")}
              {flagsFor(row)}
            </span>
            {text !== "—" ? (
              <span className="alerts-focus-detail" title={text}>
                {text}
              </span>
            ) : null}
          </div>
        );
      },
    },
    { ...clusterColumn, width: 190 },
    kbColumn,
    { ...handlingColumn, title: "Status" },
  ];

  const pcColumns: ColumnsType<RowRecord> = [
    {
      title: "Title",
      dataIndex: "alert",
      key: "title",
      width: 250,
      sorter: (a, b) => a.alert.localeCompare(b.alert),
      render: (_, row) => findingButton(row),
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
      render: (value: string) => <span className="alerts-time">{value === "—" ? value : formatDateTime(value)}</span>,
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
      render: (value: string) => <HandlingStatus tone={value === "Yes" ? "resolved" : "open"} label={value} />,
    },
    {
      title: "Impact type",
      dataIndex: "impactType",
      key: "impactType",
      width: 130,
    },
  ];
  // Below 1600px (a 14" laptop) the PC table drops the two least-used
  // columns rather than scroll sideways; both stay in the details drawer.
  const baseColumns =
    alertSource === "PC"
      ? pcColumns.filter((c) => screens.xxl || !PC_WIDE_ONLY_COLUMNS.has(String(c.key)))
      : activeView === "focused"
        ? focusedColumns
        : nccColumns;
  // Widths the user dragged, per source and view; the rest keep defaults.
  const widthKey = (key: string) => `${alertSource}.${activeView}.${key}`;
  const hasCustomWidths = Object.keys(columnWidths).some((k) => k.startsWith(`${alertSource}.${activeView}.`));
  const columns: ColumnsType<RowRecord> = baseColumns.map((c) => {
    const key = String(c.key);
    const width = columnWidths[widthKey(key)] ?? c.width;
    const label = typeof c.title === "string" ? c.title : key === "severityEdge" ? "Severity" : key;
    return {
      ...c,
      width,
      onHeaderCell: () =>
        ({
          onResize: (w: number) => setColumnWidths((prev) => ({ ...prev, [widthKey(key)]: w })),
          onResetWidth: () =>
            setColumnWidths((prev) => {
              const next = { ...prev };
              delete next[widthKey(key)];
              return next;
            }),
          resizeLabel: `Resize the ${label} column`,
        }) as ResizableHeaderProps,
    };
  });
  const resetColumnWidths = () =>
    setColumnWidths((prev) => Object.fromEntries(Object.entries(prev).filter(([k]) => !k.startsWith(`${alertSource}.${activeView}.`))));
  // Fixed columns plus room for the flexible Detail column (and checkboxes).
  const scrollX =
    columns.reduce((sum, c) => sum + (typeof c.width === "number" ? c.width : DETAIL_MIN_WIDTH), 0) +
    (canHandle ? SELECTION_COLUMN_WIDTH : 0);

  const copyDetail = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopyState("copied");
      notify.success("Copied to clipboard.");
      setTimeout(() => setCopyState("idle"), 1600);
    } catch {
      notify.warning("Couldn't copy to the clipboard.");
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
            Findings
          </Typography.Title>
          <Typography.Text type="secondary" className="tile-subtitle">
            {shownRows.length.toLocaleString()} {shownRows.length === 1 ? "finding" : "findings"} shown
            {shownRows.length !== totalRows ? ` · ${totalRows.toLocaleString()} match the dashboard filters` : ""}
          </Typography.Text>
        </div>
        {/* Severity totals sit above the filters; no second copy here. */}
        <Space size={[8, 8]} wrap className="alerts-summary-pills">
          {alertSource !== "PC" ? (
            <Segmented<FindingsView>
              size="small"
              aria-label="Findings view"
              className="alerts-view-switch"
              value={activeView}
              onChange={(v) => setView(v)}
              options={VIEW_OPTIONS.map((o) => ({
                value: o.value,
                label: (
                  <Tooltip title={o.hint}>
                    <span className="alerts-view-option">
                      {o.icon}
                      <span>{o.label}</span>
                    </span>
                  </Tooltip>
                ),
              }))}
            />
          ) : null}
          {hasCustomWidths && (activeView === "table" || activeView === "focused") ? (
            <Tooltip title="Put every column back to its default width">
              <Button size="small" icon={<ColumnWidthOutlined />} onClick={resetColumnWidths}>
                Reset columns
              </Button>
            </Tooltip>
          ) : null}
          <Tooltip title={density === "compact" ? "Switch to comfortable rows" : "Switch to compact rows"}>
            <Button
              size="small"
              onClick={() => setDensity(density === "compact" ? "comfortable" : "compact")}
            >
              {density === "compact" ? "Compact" : "Comfortable"}
            </Button>
          </Tooltip>
          <Tooltip title={expanded ? "Leave full page (Esc)" : "Show the findings across the page. Filters and details stay available."}>
            <Button
              size="small"
              aria-label={expanded ? "Exit full page" : "Expand findings"}
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
                    aria-label={`${item.label}, ${count} ${count === 1 ? "finding" : "findings"}`}
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
              ? `${handlingCounts.returned} resolved ${handlingCounts.returned === 1 ? "finding was" : "findings were"} found again`
              : "Resolved findings return here if a later run finds them again"}
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
                {totalRows > 0
                  ? "No findings in this handling view"
                  : baseRows.length === 0
                    ? alertSource === "PC"
                      ? "Prism Central reports no alerts"
                      : "No findings in the latest run"
                    : "No findings match the current filters"}
              </Typography.Text>
              <Typography.Text type="secondary">
                {totalRows > 0
                  ? "Choose All to see acknowledged and resolved rows again."
                  : baseRows.length === 0
                    ? alertSource === "PC"
                      ? "Nothing is open on the Prism Central instances in this report."
                      : "Every check passed on the clusters in this report."
                    : "Try clearing severity chips or the cluster filter."}
              </Typography.Text>
            </Space>
          }
        />
      ) : (
        <>
          {activeView === "list" ? (
            <div className="alerts-table-host alerts-view-host" ref={hostRef}>
              <ul className="findings-list" aria-label="Findings">
                {pagedRows.map((row) => {
                  const sev = severityOf(row);
                  const text = previewDetail(row.detail);
                  return (
                    <li
                      key={row.key}
                      className={`findings-list-item sev-${sev.toLowerCase()}${isSelected(row.key) ? " is-selected" : ""}`}
                      onClick={(e) => {
                        if ((e.target as HTMLElement).closest("a, button, .ant-checkbox-wrapper")) return;
                        openDetails(row);
                      }}
                    >
                      {canHandle ? (
                        <Checkbox
                          checked={isSelected(row.key)}
                          onChange={(e) => setSelected([row.key], e.target.checked)}
                          aria-label={`Select ${row.alert} on ${row.clusterName}`}
                        />
                      ) : null}
                      <SeverityIcon severity={sev} />
                      <div className="findings-list-main">
                        <div className="findings-list-title">
                          {findingButton(row, "findings-list-name")}
                          <span className="findings-list-on">on</span>
                          <ClusterNameLine row={row} url={clusterPrismURL(row.cluster, clusterNameMap)} />
                          {flagsFor(row)}
                        </div>
                        {text !== "—" ? (
                          <div className="findings-list-detail" title={text}>
                            {text}
                          </div>
                        ) : null}
                      </div>
                      <div className="findings-list-side">
                        {kbLink(row)}
                        {statusFor(row)}
                      </div>
                    </li>
                  );
                })}
              </ul>
            </div>
          ) : activeView === "grouped" ? (
            <div className="alerts-table-host alerts-view-host" ref={hostRef}>
              <div className="findings-groups">
                {pagedGroups.map((g) => {
                  const open = openGroups.includes(g.check);
                  const keys = g.rows.map((r) => r.key);
                  const chosen = keys.filter((k) => isSelected(k)).length;
                  const toggle = () => setOpenGroups((prev) => (open ? prev.filter((c) => c !== g.check) : [...prev, g.check]));
                  const kbRow = g.rows.find((r) => kbOf(r));
                  const detail = previewDetail(g.rows[0].detail);
                  const shownClusters = g.rows.slice(0, 3);
                  return (
                    <section key={g.check} className={`findings-group sev-${g.severity.toLowerCase()}${open ? " is-open" : ""}`}>
                      <div className="findings-group-head" onClick={(e) => {
                        if ((e.target as HTMLElement).closest("a, button, .ant-checkbox-wrapper")) return;
                        toggle();
                      }}>
                        <button
                          type="button"
                          className="findings-group-toggle"
                          aria-expanded={open}
                          aria-label={`${open ? "Hide" : "Show"} the clusters for ${g.check}`}
                          onClick={toggle}
                        >
                          {open ? <DownOutlined /> : <RightOutlined />}
                        </button>
                        {canHandle ? (
                          <Checkbox
                            checked={chosen === keys.length}
                            indeterminate={chosen > 0 && chosen < keys.length}
                            onChange={(e) => setSelected(keys, e.target.checked)}
                            aria-label={`Select every cluster for ${g.check}`}
                          />
                        ) : null}
                        <Tag className="severity-pill" color={SEVERITY_TAG_COLOR[g.severity]}>{g.severity}</Tag>
                        <div className="findings-group-main">
                          <span className="findings-group-name" title={g.check}>{g.check}</span>
                          {detail !== "—" ? <span className="findings-group-detail" title={detail}>{detail}</span> : null}
                        </div>
                        <div className="findings-group-clusters" aria-label="Clusters">
                          {shownClusters.map((r) => (
                            <Link key={r.key} to={clusterPath(r.cluster)} className="findings-group-chip" title={`Open the ${r.clusterName} cluster page`}>
                              {r.clusterName}
                            </Link>
                          ))}
                          {g.rows.length > shownClusters.length ? (
                            <span className="findings-group-chip is-more">+{g.rows.length - shownClusters.length}</span>
                          ) : null}
                        </div>
                        <span className="findings-group-count" title="Clusters where this check still needs attention">
                          <b>{g.open}</b> of {g.rows.length} open
                        </span>
                        {kbRow ? kbLink(kbRow) : <span className="findings-group-kb-empty" />}
                      </div>
                      {open ? (
                        <ul className="findings-group-rows">
                          {g.rows.map((row) => {
                            const text = previewDetail(row.detail);
                            return (
                              <li
                                key={row.key}
                                className={isSelected(row.key) ? "is-selected" : undefined}
                                onClick={(e) => {
                                  if ((e.target as HTMLElement).closest("a, button, .ant-checkbox-wrapper")) return;
                                  openDetails(row);
                                }}
                              >
                                {canHandle ? (
                                  <Checkbox
                                    checked={isSelected(row.key)}
                                    onChange={(e) => setSelected([row.key], e.target.checked)}
                                    aria-label={`Select ${row.alert} on ${row.clusterName}`}
                                  />
                                ) : null}
                                <SeverityIcon severity={severityOf(row)} />
                                <ClusterCell row={row} url={clusterPrismURL(row.cluster, clusterNameMap)} subtitle={row.cluster} />
                                <span className="findings-group-versions">
                                  {[row.clusterVersion && `AOS ${row.clusterVersion}`, row.nccVersion && `NCC ${row.nccVersion}`].filter(Boolean).join(" · ") || "—"}
                                </span>
                                <span className="findings-group-row-detail" title={text}>{text}</span>
                                {statusFor(row)}
                                <button
                                  type="button"
                                  className="findings-group-open"
                                  aria-label={`Open details for ${row.alert} on ${row.clusterName || row.cluster}`}
                                  onClick={() => openDetails(row)}
                                >
                                  Details
                                </button>
                              </li>
                            );
                          })}
                        </ul>
                      ) : null}
                    </section>
                  );
                })}
              </div>
            </div>
          ) : (
          <div className="alerts-table-host" ref={hostRef} data-scroll-y={tableY}>
            <Table<RowRecord>
              key={`${alertSource}-${activeView}`}
              components={TABLE_COMPONENTS}
              virtual
              className="alerts-table"
              tableLayout="fixed"
              rowKey="key"
              columns={columns}
              dataSource={pagedRows}
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
          )}
          <Pagination
            className="alerts-pagination"
            current={currentPage}
            pageSize={pageSize}
            total={pagerTotal}
            showSizeChanger
            pageSizeOptions={[50, 100, 200, 500]}
            showTotal={(total, range) =>
              `${range[0].toLocaleString()}–${range[1].toLocaleString()} of ${total.toLocaleString()}${activeView === "grouped" ? (total === 1 ? " check" : " checks") : ""}`
            }
            onChange={(nextPage, nextPageSize) => {
              setPage(nextPage);
              if (nextPageSize && nextPageSize !== rowsPerPage) setRowsPerPage(nextPageSize);
            }}
          />
        </>
      )}

      <Drawer
        open={Boolean(drawerRow)}
        title={
          drawerRow ? (
            <Space orientation="vertical" size={4} className="finding-drawer-head">
              <Typography.Text type="secondary" className="finding-drawer-eyebrow">
                {drawerRow.source === "PC" ? "Prism Central alert" : "NCC finding"} · {drawerRow.clusterName}
              </Typography.Text>
              <Typography.Title level={4} className="finding-drawer-title">
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
        size={drawerRow?.source === "PC" ? 840 : 720}
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
              clusterNameMap={clusterNameMap}
              onShowCheck={
                onShowCheck
                  ? (check) => {
                      setDrawerRow(null);
                      onShowCheck(check);
                    }
                  : undefined
              }
              mark={markFor(drawerRow)}
              note={handlingNote}
              onNote={setHandlingNote}
              pending={dispositionPending}
              readOnlyReason={readOnlyReason}
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
