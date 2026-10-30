import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Drawer, Skeleton, Table, Tabs, Tooltip } from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  BarChartOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  InfoCircleOutlined,
  LinkOutlined,
  MinusOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import {
  asArray,
  asRecord,
  buildClusterNameMap,
  nccDispositionKey,
  resolveClusterName,
  toNumber,
  weightedHealthScore,
} from "../utils/report";
import { api } from "../api/client";
import { FeatureDisabled } from "../features/features/FeatureDisabled";
import { useFeatureFlags } from "../features/features/useFeatureFlags";
import { DrilldownDiffPanel } from "../features/report/DrilldownDiffPanel";
import { FlakyChecksPanel } from "../features/report/FlakyChecksPanel";
import { LifecycleInsightsCard } from "../features/report/LifecycleInsightsCard";
import { SloPanel } from "../features/report/SloPanel";
import { sectionHref } from "../navigation";
import { notifyError } from "../notify";
import { EmptyState, KeyValueList, Panel, StatTile, StatusBadge, toneFor, type Tone } from "../ui";
import { formatDateTime, relativeTime } from "../utils/datetime";
import { formatDuration, freshness } from "../utils/freshness";
import { nutanixKbUrl, safeExternalUrl } from "../utils/url";

// ---------- helpers ----------

function normalizeCheckTitle(raw: string): string {
  return raw.replace(/^detailed information for\s*/i, "").replace(/:$/, "").trim();
}

function compactReason(raw: string): string {
  const cleaned = raw.replace(/https?:\/\/\S+/gi, "").replace(/\s+/g, " ").trim();
  if (!cleaned) return "";
  if (/^detailed information for\b/i.test(cleaned)) return "";
  if (/^cluster health is impacted/i.test(cleaned)) return "";
  return cleaned.slice(0, 240);
}

function deriveReason(row: Record<string, unknown>, severity: string): string {
  const candidates = [
    String(row.detail || ""),
    String(row.details || ""),
    String(row.message || ""),
    String(row.reason || ""),
    String(row.hint || ""),
  ];
  for (const c of candidates) {
    const reason = compactReason(c);
    if (reason) return reason;
  }
  if (severity === "ERR") return "Error-level check failed; investigate service/runtime/API conditions for this cluster.";
  return "Failing policy/check indicates concrete remediation is required before accepting this cluster as healthy.";
}

function extractKbId(row: Record<string, unknown>): string {
  const direct = String(row.kb || row.kb_link || row.kb_url || "");
  const m = direct.match(/kb\/(\d+)/i);
  if (m) return m[1];
  const fromDetail = String(row.detail || row.details || "").match(/kb[\s\-/]?(\d{2,6})/i);
  return fromDetail ? fromDetail[1] : "";
}

function kbUrl(row: Record<string, unknown>): string {
  // Report data: only web links become an href.
  const direct = safeExternalUrl(row.kb || row.kb_link || row.kb_url);
  if (direct) return direct;
  const id = extractKbId(row);
  return id ? nutanixKbUrl(id) : "";
}

function scoreTone(score: number | null): "ok" | "warn" | "fail" | undefined {
  if (score == null) return undefined;
  if (score >= 90) return "ok";
  if (score >= 60) return "warn";
  return "fail";
}

function healthGrade(score: number): string {
  if (score >= 90) return "Excellent";
  if (score >= 75) return "Good";
  if (score >= 60) return "Fair";
  if (score >= 40) return "Poor";
  return "Critical";
}

// ---------- component ----------

type DrillRow = {
  key: string;
  name: string;
  severity: string;
  count: number;
  clusterList: string[];
  kb: string;
  kbId: string;
  sample: Record<string, unknown>;
};

type TabKey = "summary" | "clusters" | "trends" | "triage";
const TABS: TabKey[] = ["summary", "clusters", "trends", "triage"];

// Module-level fallback so each render doesn't allocate a fresh empty object —
// otherwise downstream `useMemo` hooks that depend on `data` re-execute on
// every render, defeating their memoization.
const EMPTY_REPORT_DATA = Object.freeze({
  run_summary: {},
  ncc_summary_counts: {},
  ncc_cluster_summary: [],
  checks_snapshot: [],
  agg_rows: [],
  drilldown_diff: {},
  flaky_checks: {},
  regression_summary: {},
  slo_dashboard: {},
  report_meta: {},
  artifact_links: {},
}) as Record<string, unknown>;

const BRIEF_ICON: Record<Tone, React.ReactNode> = {
  fail: <ExclamationCircleOutlined />,
  warn: <WarningOutlined />,
  info: <InfoCircleOutlined />,
  ok: <CheckCircleOutlined />,
  neutral: <InfoCircleOutlined />,
};

/**
 * Insights: what the latest run says, per cluster, over time, and how
 * findings are being handled — one tab each (kept in the URL as ?tab=).
 */
export function InsightsPage() {
  const features = useFeatureFlags();
  const insightsOff = features.data?.insights === false;
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData, enabled: features.isFetched && !insightsOff });
  const trends = useQuery({ queryKey: ["report-trends"], queryFn: () => api.reportTrends(24), enabled: features.isFetched && !insightsOff });
  const nccMarks = useQuery({ queryKey: ["ncc-dispositions"], queryFn: api.nccDispositions, staleTime: 15_000, enabled: features.isFetched && !insightsOff });
  const [drillCheck, setDrillCheck] = useState<DrillRow | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const requested = searchParams.get("tab") as TabKey | null;
  const tab: TabKey = requested && TABS.includes(requested) ? requested : "summary";

  useEffect(() => {
    if (report.error) notifyError(report.error, "Could not load insights");
  }, [report.error]);
  useEffect(() => {
    if (trends.error) notifyError(trends.error, "Could not load trends");
  }, [trends.error]);

  const data = report.data ?? EMPTY_REPORT_DATA;

  const meta = asRecord(data.report_meta);
  const summaryCounts = asRecord(data.ncc_summary_counts);
  const clusterSummary = asArray(data.ncc_cluster_summary).map((c) => asRecord(c));
  const runSummary = asRecord(data.run_summary);
  const aggRows = asArray(data.agg_rows).map((r) => asRecord(r));
  const regression = asRecord(data.regression_summary);

  const totalPlugins = toNumber(summaryCounts.total_plugins);
  const passCount = toNumber(summaryCounts.pass);
  const failCount = toNumber(summaryCounts.fail);
  const warnCount = toNumber(summaryCounts.warn);
  const errorCount = toNumber(summaryCounts.error);
  const infoCount = toNumber(summaryCounts.info);
  const unknownCount = toNumber(summaryCounts.unknown);
  const rawPassRate = totalPlugins > 0 ? (passCount / totalPlugins) * 100 : 0;
  // Null with no checks: a fresh install is "no data", not "0% Critical".
  const weightedHealth = weightedHealthScore(summaryCounts);
  const consistencyOk =
    passCount + failCount + warnCount + errorCount + infoCount + unknownCount === totalPlugins;
  const affectedClusters = clusterSummary.filter((c) => toNumber(c.fail) > 0 || toNumber(c.error) > 0);

  const clusterNameMap = useMemo(
    () =>
      buildClusterNameMap({
        runSummary: data.run_summary,
        checksSnapshot: data.checks_snapshot,
        aggRows: Array.isArray(data.agg_rows) ? data.agg_rows : [],
        drilldownDiff: data.drilldown_diff,
        flakyChecks: data.flaky_checks,
        sloDashboard: data.slo_dashboard,
        regressionSummary: data.regression_summary,
      }),
    [data],
  );

  const trendPoints = asArray(asRecord(trends.data || {}).points).map((p) => asRecord(p));
  const recentTrends = trendPoints.slice(-12);
  const runTimestamp = String(runSummary.timestamp || "");
  const fresh = freshness(runTimestamp);
  const runDuration = toNumber(runSummary.duration_s);
  const failureClasses = asRecord(runSummary.failure_classes);
  const failureClassEntries = Object.entries(failureClasses)
    .map(([k, v]) => ({ name: k, count: toNumber(v) }))
    .filter((e) => e.count > 0)
    .sort((a, b) => b.count - a.count);

  // Top failing checks aggregated across clusters
  const checkAgg = useMemo(() => {
    const map = new Map<string, { name: string; severity: string; count: number; clusters: Set<string>; kb: string; sample: Record<string, unknown> }>();
    for (const r of aggRows) {
      const sev = String(r.severity || "").toUpperCase();
      if (sev !== "FAIL" && sev !== "ERR" && sev !== "WARN") continue;
      const name = normalizeCheckTitle(String(r.check || r.check_name || "Unnamed check"));
      const key = `${name}|${sev}`;
      const cluster = resolveClusterName(String(r.cluster || ""), clusterNameMap);
      const existing = map.get(key);
      if (existing) {
        existing.count += 1;
        if (cluster) existing.clusters.add(cluster);
        if (!existing.kb) existing.kb = kbUrl(r);
      } else {
        map.set(key, {
          name,
          severity: sev,
          count: 1,
          clusters: new Set(cluster ? [cluster] : []),
          kb: kbUrl(r),
          sample: r,
        });
      }
    }
    const arr = Array.from(map.values()).map((entry) => ({
      ...entry,
      clusterList: Array.from(entry.clusters),
      kbId: extractKbId(entry.sample),
    }));
    return arr.sort((a, b) => {
      const sevWeight = (s: string) => (s === "FAIL" ? 3 : s === "ERR" ? 2 : 1);
      return sevWeight(b.severity) - sevWeight(a.severity) || b.count - a.count;
    });
  }, [aggRows, clusterNameMap]);

  const actionableFindings = useMemo(() => {
    const items = aggRows.map((r) => {
      const severity = String(r.severity || "").toUpperCase();
      const checkName = normalizeCheckTitle(String(r.check || r.check_name || "Unnamed check"));
      const detail = String(r.detail || r.details || r.message || "").trim();
      const kb = kbUrl(r);
      const score = (severity === "ERR" ? 100 : severity === "FAIL" ? 80 : 0) + (kb ? 10 : 0) + (detail ? 5 : 0);
      return { row: r, severity, checkName, kb, score };
    });
    return items.filter((x) => x.severity === "FAIL" || x.severity === "ERR").sort((a, b) => b.score - a.score).slice(0, 5);
  }, [aggRows]);

  const triage = useMemo(() => {
    const marks = new Map<string, { status: string; by: string }>();
    for (const item of nccMarks.data?.items ?? []) {
      if (!item.cluster || !item.check) continue;
      marks.set(nccDispositionKey(item.cluster, item.check), item);
    }
    let needsAttention = 0;
    let acknowledged = 0;
    let resolved = 0;
    let returned = 0;
    let missingKb = 0;
    const owners = new Map<string, number>();
    const byCluster = new Map<string, { name: string; needs: number; acknowledged: number; resolved: number }>();
    const nccVersions = new Map<string, Set<string>>();
    const aosVersions = new Map<string, Set<string>>();
    const nccSeen = new Set<string>();
    const aosSeen = new Set<string>();
    for (const r of aggRows) {
      const sev = String(r.severity || "").toUpperCase();
      const cluster = String(r.cluster || "");
      const named = resolveClusterName(cluster, clusterNameMap);
      const label = named && named !== "-" ? named : cluster || "Unknown";
      const ncc = String(r.nccVersion || r.ncc_version || "").trim();
      const aos = String(r.clusterVersion || r.cluster_version || "").trim();
      if (ncc && !nccSeen.has(label)) {
        nccSeen.add(label);
        const set = nccVersions.get(ncc) || new Set<string>();
        set.add(label);
        nccVersions.set(ncc, set);
      }
      if (aos && !aosSeen.has(label)) {
        aosSeen.add(label);
        const set = aosVersions.get(aos) || new Set<string>();
        set.add(label);
        aosVersions.set(aos, set);
      }
      if (sev !== "FAIL" && sev !== "ERR") continue;
      if (!kbUrl(r)) missingKb += 1;
      const rawCheck = String(r.check || r.check_name || "");
      const check = normalizeCheckTitle(rawCheck);
      const mark =
        marks.get(nccDispositionKey(cluster, check)) ||
        marks.get(nccDispositionKey(cluster, rawCheck)) ||
        marks.get(nccDispositionKey(named, check)) ||
        marks.get(nccDispositionKey(named, rawCheck));
      const slot = byCluster.get(label) || { name: label, needs: 0, acknowledged: 0, resolved: 0 };
      if (mark?.status === "resolved") {
        resolved += 1;
        slot.resolved += 1;
      } else if (mark?.status === "acknowledged") {
        acknowledged += 1;
        slot.acknowledged += 1;
        const who = mark.by.trim();
        if (who && who !== "unknown") owners.set(who, (owners.get(who) || 0) + 1);
      } else {
        if (mark?.status === "reopened") returned += 1;
        needsAttention += 1;
        slot.needs += 1;
      }
      byCluster.set(label, slot);
    }
    const top = [...owners.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0];
    const clusters = [...byCluster.values()].sort((a, b) => b.needs - a.needs || b.acknowledged - a.acknowledged || a.name.localeCompare(b.name));
    return {
      needsAttention,
      acknowledged,
      resolved,
      returned,
      missingKb,
      topOwner: top?.[0] ?? "",
      topCount: top?.[1] ?? 0,
      clusters,
      nccVersions: [...nccVersions.entries()].map(([version, clusters]) => ({ version, clusters: [...clusters] })).sort((a, b) => b.clusters.length - a.clusters.length),
      aosVersions: [...aosVersions.entries()].map(([version, clusters]) => ({ version, clusters: [...clusters] })).sort((a, b) => b.clusters.length - a.clusters.length),
    };
  }, [aggRows, clusterNameMap, nccMarks.data]);

  // KB Index — unique KBs across findings
  const kbIndex = useMemo(() => {
    const map = new Map<string, { id: string; url: string; titles: Set<string>; clusters: Set<string>; count: number }>();
    for (const r of aggRows) {
      const id = extractKbId(r);
      if (!id) continue;
      const url = kbUrl(r);
      const title = normalizeCheckTitle(String(r.check || ""));
      const cluster = resolveClusterName(String(r.cluster || ""), clusterNameMap);
      const cur = map.get(id);
      if (cur) {
        cur.count += 1;
        if (title) cur.titles.add(title);
        if (cluster) cur.clusters.add(cluster);
      } else {
        map.set(id, {
          id,
          url,
          titles: new Set(title ? [title] : []),
          clusters: new Set(cluster ? [cluster] : []),
          count: 1,
        });
      }
    }
    return Array.from(map.values())
      .map((e) => ({ ...e, titlesArr: Array.from(e.titles), clustersArr: Array.from(e.clusters) }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 12);
  }, [aggRows, clusterNameMap]);

  const clusterRanking = useMemo(() => {
    return clusterSummary
      .map((c) => {
        const total = toNumber(c.total_plugins);
        const fail = toNumber(c.fail);
        const warn = toNumber(c.warn);
        const err = toNumber(c.error);
        const pass = toNumber(c.pass);
        const info = toNumber(c.info);
        const unknown = toNumber(c.unknown);
        return {
          address: String(c.address || ""),
          name: resolveClusterName(String(c.address || ""), clusterNameMap),
          total,
          fail,
          warn,
          err,
          pass,
          info,
          unknown,
          health: weightedHealthScore(c) ?? 0,
          riskCount: fail + err,
        };
      })
      .filter((x) => x.total > 0)
      .sort((a, b) => a.health - b.health);
  }, [clusterSummary, clusterNameMap]);

  // Per-cluster occurrences for the currently drilled check. MUST be declared
  // before any conditional early-return below — otherwise the loading branch
  // renders 5 hooks and the data branch renders 6, which React rejects with
  // error #310 ("Rendered more hooks than during the previous render").
  const drillOccurrences = useMemo(() => {
    if (!drillCheck) return [] as Record<string, unknown>[];
    return aggRows
      .filter((r) => {
        const sev = String(r.severity || "").toUpperCase();
        const nm = normalizeCheckTitle(String(r.check || r.check_name || "Unnamed check"));
        return sev === drillCheck.severity && nm === drillCheck.name;
      })
      .slice(0, 50);
  }, [aggRows, drillCheck]);

  // ---------- render ----------

  if (insightsOff) {
    return <FeatureDisabled feature="insights" />;
  }

  if ((features.isLoading && !features.data) || (report.isLoading && !report.data)) {
    return (
      <div className="insights-page">
        <div className="overview-stats">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="ui-stat">
              <Skeleton active paragraph={{ rows: 1 }} title={false} />
            </div>
          ))}
        </div>
        <Panel>
          <Skeleton active paragraph={{ rows: 6 }} />
        </Panel>
      </div>
    );
  }

  if (totalPlugins === 0 && trendPoints.length === 0) {
    return (
      <div className="insights-page">
        <Panel>
          <EmptyState
            icon={<BarChartOutlined />}
            title="Nothing to analyse yet"
            description="Insights appear after the first run that records check results: health per cluster, trends across runs, and how findings are being handled."
            actions={
              <Link to={sectionHref("runs")}>
                <Button type="primary">Go to Runs</Button>
              </Link>
            }
          />
        </Panel>
      </div>
    );
  }

  const deltaFail = toNumber(regression.delta_fail_total);
  const hasRegression = Boolean(regression.has_regression);
  const previousTs = String(regression.previous_timestamp || "");
  const diff = asRecord(data.drilldown_diff);
  const newFailCount = toNumber(diff.new_fail_count);
  const resolvedFailCount = toNumber(diff.resolved_fail_count);
  const clustersFailed = toNumber(runSummary.clusters_failed);
  const clustersOk = toNumber(runSummary.clusters_ok);
  const sharedChecks = checkAgg
    .filter((row) => row.clusterList.length >= 2 && (row.severity === "FAIL" || row.severity === "ERR"))
    .slice()
    .sort((a, b) => b.clusterList.length - a.clusterList.length || b.count - a.count);
  const lastTrend = recentTrends.length > 0 ? recentTrends[recentTrends.length - 1] : null;
  const prevTrend = recentTrends.length > 1 ? recentTrends[recentTrends.length - 2] : null;
  const trendFailDelta = lastTrend && prevTrend ? toNumber(lastTrend.fail_total) - toNumber(prevTrend.fail_total) : null;
  const trendHealthDelta = lastTrend && prevTrend ? toNumber(lastTrend.avg_health_score) - toNumber(prevTrend.avg_health_score) : null;

  const briefing: Array<{ tone: Tone; text: string }> = [];
  if (clustersFailed > 0) {
    const classes = failureClassEntries.map((e) => `${e.count} ${e.name.replace(/_/g, " ")}`).join(", ");
    briefing.push({
      tone: "fail",
      text: `${clustersFailed} of ${clustersOk + clustersFailed} clusters did not complete this run.${classes ? ` Reasons: ${classes}.` : ""}`,
    });
  }
  if (fresh.label === "Stale" || fresh.label === "Aging") {
    briefing.push({
      tone: fresh.label === "Stale" ? "fail" : "warn",
      text: `These results are ${fresh.label.toLowerCase()} (${relativeTime(runTimestamp)}). They describe that run.`,
    });
  }
  if (newFailCount > 0 || resolvedFailCount > 0) {
    briefing.push({
      tone: newFailCount > resolvedFailCount ? "warn" : "ok",
      text: `${newFailCount} FAIL ${newFailCount === 1 ? "finding is" : "findings are"} new since the previous run, and ${resolvedFailCount} ${resolvedFailCount === 1 ? "was" : "were"} resolved.`,
    });
  }
  if (triage.returned > 0) {
    briefing.push({
      tone: "warn",
      text: `${triage.returned} resolved ${triage.returned === 1 ? "alert was" : "alerts were"} found again on this run.`,
    });
  }
  if (triage.needsAttention > 0) {
    const hottest = triage.clusters.find((c) => c.needs > 0);
    briefing.push({
      tone: "warn",
      text: `${triage.needsAttention} critical ${triage.needsAttention === 1 ? "alert has" : "alerts have"} not been acknowledged or resolved.${hottest ? ` ${hottest.name} still has ${hottest.needs}.` : ""}`,
    });
  }
  if (sharedChecks.length > 0) {
    const top = sharedChecks[0];
    briefing.push({
      tone: "info",
      text: `${sharedChecks.length} FAIL/ERR ${sharedChecks.length === 1 ? "check appears" : "checks appear"} on more than one cluster. “${top.name}” is on ${top.clusterList.length}.`,
    });
  }
  if (triage.missingKb > 0) {
    briefing.push({
      tone: "info",
      text: `${triage.missingKb} critical ${triage.missingKb === 1 ? "alert has" : "alerts have"} no knowledge-base article.`,
    });
  }
  if (triage.nccVersions.length > 1) {
    briefing.push({
      tone: "warn",
      text: `NCC versions differ across clusters: ${triage.nccVersions.map((v) => `${v.version} (${v.clusters.length})`).join(", ")}.`,
    });
  }
  if (briefing.length === 0 && totalPlugins > 0) {
    briefing.push({
      tone: "ok",
      text: previousTs
        ? "Every cluster completed, there are no new failures since the previous run, and no critical alerts are waiting."
        : "Every cluster completed, and no critical alerts are waiting. There is no earlier run to compare.",
    });
  }

  const severityRows: Array<{ label: string; count: number; tone: Tone }> = [
    { label: "Pass", count: passCount, tone: "ok" },
    { label: "Fail", count: failCount, tone: "fail" },
    { label: "Error", count: errorCount, tone: "fail" },
    { label: "Warning", count: warnCount, tone: "warn" },
    { label: "Info", count: infoCount, tone: "info" },
    { label: "Unknown", count: unknownCount, tone: "neutral" },
  ];

  const openDrill = (record: (typeof checkAgg)[number]) =>
    setDrillCheck({
      key: `${record.name}|${record.severity}`,
      name: record.name,
      severity: record.severity,
      count: record.count,
      clusterList: record.clusterList,
      kb: record.kb,
      kbId: record.kbId,
      sample: record.sample,
    });
  const drillRow = (record: (typeof checkAgg)[number]) => ({
    onClick: () => openDrill(record),
    onKeyDown: (e: React.KeyboardEvent) => {
      if (e.key === "Enter") openDrill(record);
    },
    tabIndex: 0,
    className: "is-clickable",
  });

  const kbCell = (row: { kb: string; kbId: string }) =>
    row.kbId ? (
      <a href={row.kb} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
        <LinkOutlined /> KB {row.kbId}
      </a>
    ) : (
      "—"
    );

  const checkAggColumns: ColumnsType<(typeof checkAgg)[number]> = [
    {
      title: "Check",
      key: "name",
      render: (_, row) => (
        <span className="insights-check">
          <strong>{row.name}</strong>
          {row.clusterList.length > 0 ? (
            <span className="ui-field-help">
              {row.clusterList.slice(0, 4).join(", ")}
              {row.clusterList.length > 4 ? ` +${row.clusterList.length - 4} more` : ""}
            </span>
          ) : null}
        </span>
      ),
    },
    { title: "Severity", dataIndex: "severity", key: "severity", width: 110, render: (v: string) => <StatusBadge tone={toneFor(v)}>{v}</StatusBadge> },
    { title: "Occurrences", dataIndex: "count", key: "count", width: 120, align: "right" },
    { title: "Clusters", key: "clusters", width: 100, align: "right", render: (_, row) => row.clusterList.length },
    { title: "KB", key: "kb", width: 100, render: (_, row) => kbCell(row) },
  ];

  const trendMax = Math.max(1, ...recentTrends.map((p) => toNumber(p.fail_total) + toNumber(p.err_total)));

  const summaryTab = (
    <div className="insights-tab">
      <Panel title="What this run is saying" description="Highlights from this run and the one before it.">
        <ul className="insights-brief">
          {briefing.map((item) => (
            <li key={item.text} className={`tone-${item.tone}`}>
              <span className="insights-brief-icon" aria-hidden="true">
                {BRIEF_ICON[item.tone]}
              </span>
              <span>{item.text}</span>
            </li>
          ))}
        </ul>
      </Panel>

      <Panel
        title="Results"
        description={totalPlugins > 0 ? `${totalPlugins.toLocaleString()} checks in this run.` : "No check totals for this run."}
      >
        {totalPlugins > 0 ? (
          <>
            <div className="insights-stack" role="img" aria-label={severityRows.map((s) => `${s.label} ${s.count}`).join(", ")}>
              {severityRows.map((s) =>
                s.count > 0 ? (
                  <Tooltip key={s.label} title={`${s.label}: ${s.count} (${((s.count / totalPlugins) * 100).toFixed(1)}%)`}>
                    <span className={`overview-bar-fill tone-${s.tone}`} style={{ width: `${(s.count / totalPlugins) * 100}%` }} />
                  </Tooltip>
                ) : null,
              )}
            </div>
            <ul className="insights-legend">
              {severityRows.map((s) => (
                <li key={s.label}>
                  <StatusBadge tone={s.tone}>{s.label}</StatusBadge>
                  <strong>{s.count.toLocaleString()}</strong>
                  <span className="ui-field-help">{((s.count / totalPlugins) * 100).toFixed(1)}%</span>
                </li>
              ))}
            </ul>
            {!consistencyOk ? (
              <Alert
                type="warning"
                showIcon
                title="Check totals do not match"
                description="The severity counts do not add up to the total number of checks. Start the run again if this looks wrong."
              />
            ) : null}
          </>
        ) : null}
      </Panel>

      <div className="insights-grid">
        <Panel title="Top failing checks" description="Most frequent problems. Select one for its clusters and output." flush>
          <Table
            size="middle"
            rowKey={(r) => `${r.name}|${r.severity}`}
            columns={checkAggColumns}
            dataSource={checkAgg.slice(0, 10)}
            pagination={false}
            scroll={{ x: 560 }}
            onRow={drillRow}
            locale={{ emptyText: <EmptyState title="No failures, errors or warnings" /> }}
          />
        </Panel>
        <Panel title="Review first" description="The critical findings most worth a look." flush>
          {actionableFindings.length === 0 ? (
            <EmptyState title="No critical findings in this run" />
          ) : (
            <ul className="insights-findings">
              {actionableFindings.map((item, idx) => {
                const cluster = resolveClusterName(String(item.row.cluster || "-"), clusterNameMap);
                const kbId = extractKbId(item.row);
                return (
                  <li key={`${cluster}-${item.checkName}-${idx}`}>
                    <div className="insights-finding-head">
                      <StatusBadge tone={toneFor(item.severity)}>{item.severity}</StatusBadge>
                      <strong>{item.checkName}</strong>
                    </div>
                    <span className="ui-field-help">{cluster}</span>
                    <p>{deriveReason(item.row, item.severity)}</p>
                    {kbId ? (
                      <a href={item.kb} target="_blank" rel="noreferrer">
                        <LinkOutlined /> KB {kbId}
                      </a>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          )}
        </Panel>
      </div>

      {Object.keys(meta).length > 0 ? (
        <Panel title="About this report">
          <KeyValueList
            items={[
              { label: "Orchestrator version", value: String(meta.version || "—") },
              { label: "Build", value: `${String(meta.build_date || "—")}${meta.stream ? ` · ${String(meta.stream)}` : ""}` },
              { label: "Revision", value: String(meta.git_revision || "—"), mono: true },
              { label: "Host", value: String(meta.hostname || "—") },
              { label: "Started by", value: String(meta.scheduler_source || "—") },
            ]}
          />
        </Panel>
      ) : null}
    </div>
  );

  const clustersTab = (
    <div className="insights-tab">
      <div className="insights-grid">
        <Panel title="Cluster health" description="Worst first, by weighted health." flush>
          {clusterRanking.length === 0 ? (
            <EmptyState title="No per-cluster results" />
          ) : (
            <ul className="insights-ranking">
              {clusterRanking.map((c) => {
                const open = triage.clusters.find((item) => item.name === c.name);
                const t = scoreTone(c.health) ?? "neutral";
                return (
                  <li key={c.address}>
                    <div className="insights-ranking-head">
                      <strong>{c.name}</strong>
                      <span className={`insights-score tone-${t}`}>
                        {c.health.toFixed(1)}% · {healthGrade(c.health)}
                      </span>
                    </div>
                    <span className="overview-bar" aria-hidden="true">
                      <span className={`overview-bar-fill tone-${t}`} style={{ width: `${Math.max(1, c.health)}%` }} />
                    </span>
                    <span className="ui-field-help">
                      {[c.fail && `${c.fail} fail`, c.err && `${c.err} error`, c.warn && `${c.warn} warn`, c.info && `${c.info} info`]
                        .filter(Boolean)
                        .join(" · ") || "No findings"}
                      {open && open.needs > 0 ? ` · ${open.needs} not yet handled` : ""}
                    </span>
                  </li>
                );
              })}
            </ul>
          )}
        </Panel>
        <Panel title="Run reliability" description="Clusters that could not be checked, and versions seen.">
          {failureClassEntries.length === 0 ? (
            <p className="insights-ok">
              <CheckCircleOutlined /> Every cluster completed this run.
            </p>
          ) : (
            <KeyValueList items={failureClassEntries.map((e) => ({ label: e.name.replace(/_/g, " "), value: `${e.count} cluster(s)` }))} />
          )}
          <KeyValueList
            items={[
              { label: "Clusters completed", value: String(clustersOk) },
              { label: "Clusters not completed", value: String(clustersFailed) },
              { label: "Run result", value: toNumber(runSummary.exit_code) === 0 ? "Completed" : `Failed (exit ${toNumber(runSummary.exit_code)})` },
              ...(triage.nccVersions.length
                ? [{ label: "NCC versions", value: triage.nccVersions.map((v) => `${v.version} (${v.clusters.length})`).join(", ") }]
                : []),
              ...(triage.aosVersions.length
                ? [{ label: "AOS versions", value: triage.aosVersions.map((v) => `${v.version} (${v.clusters.length})`).join(", ") }]
                : []),
            ]}
          />
        </Panel>
      </div>
      <SloPanel
        sloDashboard={data.slo_dashboard}
        nccClusterSummary={Array.isArray(data.ncc_cluster_summary) ? data.ncc_cluster_summary : []}
        regressionSummary={data.regression_summary}
        clusterNameMap={clusterNameMap}
      />
    </div>
  );

  const trendsTab = (
    <div className="insights-tab">
      <Panel
        title="Failures over recent runs"
        description={
          recentTrends.length
            ? `Failing and error checks in the last ${recentTrends.length} runs.${
                trendFailDelta !== null ? ` Versus the previous run: ${trendFailDelta > 0 ? "+" : ""}${trendFailDelta}.` : ""
              }${trendHealthDelta !== null ? ` Average health ${trendHealthDelta > 0 ? "+" : ""}${trendHealthDelta.toFixed(0)}.` : ""}`
            : undefined
        }
      >
        {recentTrends.length === 0 ? (
          <EmptyState title="Not enough runs for a trend" description="A trend appears after a few runs have finished." />
        ) : (
          <div className="insights-trend" role="img" aria-label={`Failures across ${recentTrends.length} runs`}>
            {recentTrends.map((p, idx) => {
              const value = toNumber(p.fail_total) + toNumber(p.err_total);
              const ts = String(p.timestamp || "");
              return (
                <Tooltip
                  key={`${ts}-${idx}`}
                  title={`${formatDateTime(ts)} · ${toNumber(p.fail_total)} fail · ${toNumber(p.err_total)} error`}
                >
                  <div className="insights-trend-col">
                    <span className="insights-trend-value">{value}</span>
                    <span className="insights-trend-track">
                      <span
                        className={`insights-trend-bar tone-${value > 0 ? "fail" : "ok"}`}
                        style={{ height: `${Math.max(4, (value / trendMax) * 100)}%` }}
                      />
                    </span>
                    <span className="insights-trend-label">{relativeTime(ts)}</span>
                  </div>
                </Tooltip>
              );
            })}
          </div>
        )}
      </Panel>

      <Panel title="Compared with the previous run" description={previousTs ? `Previous run ${relativeTime(previousTs)}.` : "No earlier run to compare."}>
        {previousTs ? (
          <div className="overview-stats">
            <StatTile label="Failing before" value={toNumber(regression.previous_fail_total)} />
            <StatTile label="Failing now" value={toNumber(regression.current_fail_total)} />
            <StatTile
              label="Change"
              value={
                <span className="insights-delta">
                  {deltaFail > 0 ? <ArrowUpOutlined /> : deltaFail < 0 ? <ArrowDownOutlined /> : <MinusOutlined />}
                  {deltaFail > 0 ? `+${deltaFail}` : deltaFail}
                </span>
              }
              tone={deltaFail > 0 ? "fail" : deltaFail < 0 ? "ok" : undefined}
            />
            <StatTile
              label="Outcome"
              value={hasRegression ? "Regression" : deltaFail < 0 ? "Improved" : "Stable"}
              tone={hasRegression ? "fail" : deltaFail < 0 ? "ok" : undefined}
            />
          </div>
        ) : null}
      </Panel>

      <DrilldownDiffPanel drilldownDiff={data.drilldown_diff} clusterNameMap={clusterNameMap} />
      <FlakyChecksPanel flakyChecks={data.flaky_checks} clusterNameMap={clusterNameMap} />
    </div>
  );

  const triageTab = (
    <div className="insights-tab">
      <Panel title="Triage queue" description="Critical alerts and how they have been handled. Resolved alerts come back here if a later run finds them again.">
        <div className="overview-stats">
          <StatTile label="Need attention" value={triage.needsAttention} tone={triage.needsAttention > 0 ? "fail" : "ok"} />
          <StatTile label="Acknowledged, still open" value={triage.acknowledged} />
          <StatTile label="Resolved" value={triage.resolved} />
          <StatTile label="Most acknowledgements" value={triage.topOwner || "—"} hint={triage.topCount > 0 ? `${triage.topCount} alerts` : undefined} />
        </div>
        {triage.returned > 0 ? (
          <p className="ui-field-help">
            {triage.returned} resolved {triage.returned === 1 ? "alert was" : "alerts were"} found again on this run.
          </p>
        ) : null}
        {triage.clusters.some((c) => c.needs > 0) ? (
          <Table
            className="insights-table"
            size="middle"
            pagination={false}
            rowKey="name"
            dataSource={triage.clusters.filter((c) => c.needs > 0).slice(0, 8)}
            columns={[
              { title: "Cluster", dataIndex: "name", key: "name" },
              { title: "Need attention", dataIndex: "needs", key: "needs", width: 150, align: "right" },
              { title: "Acknowledged", dataIndex: "acknowledged", key: "acknowledged", width: 140, align: "right" },
              { title: "Resolved", dataIndex: "resolved", key: "resolved", width: 120, align: "right" },
            ]}
          />
        ) : null}
        {triage.missingKb > 0 ? <p className="ui-field-help">{triage.missingKb} of these alerts have no knowledge-base article.</p> : null}
        <Link to="/findings">
          <Button>Work through them in Findings</Button>
        </Link>
      </Panel>

      <LifecycleInsightsCard enabled={features.isFetched && !insightsOff} />

      {sharedChecks.length > 0 ? (
        <Panel title="Same check, several clusters" description="The same critical check failing on more than one cluster." flush>
          <Table
            size="middle"
            pagination={false}
            rowKey={(row) => `${row.name}|${row.severity}`}
            dataSource={sharedChecks.slice(0, 8)}
            scroll={{ x: 520 }}
            onRow={drillRow}
            columns={[
              { title: "Check", dataIndex: "name", key: "name" },
              { title: "Severity", dataIndex: "severity", key: "severity", width: 110, render: (v: string) => <StatusBadge tone={toneFor(v)}>{v}</StatusBadge> },
              { title: "Clusters", key: "clusters", width: 100, align: "right", render: (_, row) => row.clusterList.length },
              { title: "Rows", dataIndex: "count", key: "count", width: 80, align: "right" },
              { title: "KB", key: "kb", width: 100, render: (_, row) => kbCell(row) },
            ]}
          />
        </Panel>
      ) : null}

      <Panel title="Knowledge base articles" description="Articles referenced by failing checks.">
        {kbIndex.length === 0 ? (
          <EmptyState title="No articles referenced in this run" />
        ) : (
          <ul className="insights-kb">
            {kbIndex.map((kb) => (
              <li key={kb.id}>
                <a href={kb.url} target="_blank" rel="noreferrer">
                  <LinkOutlined /> KB {kb.id}
                </a>
                <span className="ui-field-help">
                  {kb.count}× · {kb.titlesArr.slice(0, 2).join(", ")}
                  {kb.clustersArr.length ? ` · ${kb.clustersArr.slice(0, 3).join(", ")}${kb.clustersArr.length > 3 ? ` +${kb.clustersArr.length - 3}` : ""}` : ""}
                </span>
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </div>
  );

  return (
    <div className="insights-page">
      <div className="overview-runline">
        <div className="overview-runline-text">
          {runTimestamp ? (
            <>
              <StatusBadge tone={fresh.tone}>{fresh.label}</StatusBadge>
              <Tooltip title={formatDateTime(runTimestamp)}>
                <span>Latest run {relativeTime(runTimestamp)}</span>
              </Tooltip>
              <StatusBadge tone={hasRegression ? "fail" : deltaFail < 0 ? "ok" : "neutral"}>
                {hasRegression ? "Regression" : deltaFail < 0 ? "Improving" : "Stable"}
              </StatusBadge>
            </>
          ) : (
            <span>No completed run yet</span>
          )}
        </div>
      </div>

      <div className="overview-stats">
        <StatTile
          label="Health score"
          value={weightedHealth == null ? "—" : `${weightedHealth.toFixed(1)}%`}
          tone={scoreTone(weightedHealth)}
          hint={weightedHealth == null ? "No completed checks" : `${healthGrade(weightedHealth)} · pass rate ${rawPassRate.toFixed(1)}%`}
        />
        <StatTile label="Checks" value={totalPlugins.toLocaleString()} hint={`${clusterSummary.length} cluster(s)`} />
        <StatTile
          label="Clusters at risk"
          value={`${affectedClusters.length}/${clusterSummary.length}`}
          tone={affectedClusters.length > 0 ? "fail" : clusterSummary.length ? "ok" : undefined}
          hint="With failing or error checks"
        />
        <StatTile label="Run duration" value={runDuration > 0 ? formatDuration(runDuration) : "—"} />
      </div>

      <Tabs
        className="insights-tabs"
        activeKey={tab}
        onChange={(k) => {
          const next = new URLSearchParams(searchParams);
          if (k === "summary") next.delete("tab");
          else next.set("tab", k);
          setSearchParams(next, { replace: true });
        }}
        items={[
          { key: "summary", label: "Summary", children: summaryTab },
          { key: "clusters", label: "Clusters", children: clustersTab },
          { key: "trends", label: "Trends", children: trendsTab },
          { key: "triage", label: `Triage${triage.needsAttention ? ` (${triage.needsAttention})` : ""}`, children: triageTab },
        ]}
      />

      <Drawer
        open={Boolean(drillCheck)}
        onClose={() => setDrillCheck(null)}
        size={640}
        destroyOnHidden
        title={
          drillCheck ? (
            <span className="insights-finding-head">
              <StatusBadge tone={toneFor(drillCheck.severity)}>{drillCheck.severity}</StatusBadge>
              <span>{drillCheck.name}</span>
            </span>
          ) : null
        }
        extra={
          drillCheck?.kbId ? (
            <a href={drillCheck.kb} target="_blank" rel="noreferrer">
              <Button type="primary" icon={<LinkOutlined />} size="small">
                Open KB {drillCheck.kbId}
              </Button>
            </a>
          ) : null
        }
      >
        {drillCheck ? (
          <div className="insights-tab">
            <KeyValueList
              items={[
                { label: "Occurrences", value: String(drillCheck.count) },
                { label: "Clusters", value: drillCheck.clusterList.join(", ") || "—" },
              ]}
            />
            <div>
              <h4 className="ui-panel-title">Check output</h4>
              {drillOccurrences.length === 0 ? (
                <EmptyState title="No output captured for this check" />
              ) : (
                <ul className="insights-output">
                  {drillOccurrences.map((row, i) => {
                    const cluster = resolveClusterName(String(row.cluster || "-"), clusterNameMap);
                    const detail = String(row.detail || row.details || row.message || "").trim();
                    return (
                      <li key={i}>
                        <strong>{cluster}</strong>
                        {row.host ? <span className="ui-field-help"> · {String(row.host)}</span> : null}
                        {detail ? <pre>{detail.length > 400 ? `${detail.slice(0, 400)}…` : detail}</pre> : <span className="ui-field-help">No detail captured.</span>}
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          </div>
        ) : null}
      </Drawer>
    </div>
  );
}
