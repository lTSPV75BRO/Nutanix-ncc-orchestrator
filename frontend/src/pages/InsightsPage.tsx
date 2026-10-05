import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Alert,
  Badge,
  Button,
  Card,
  Col,
  Descriptions,
  Drawer,
  Empty,
  List,
  Progress,
  Row,
  Skeleton,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  ClockCircleOutlined,
  ExclamationCircleOutlined,
  LinkOutlined,
  MinusOutlined,
  SafetyCertificateOutlined,
} from "@ant-design/icons";
import { asArray, asRecord, buildClusterNameMap, nccDispositionKey, resolveClusterName, toNumber } from "../utils/report";
import { api } from "../api/client";
import { FeatureDisabled } from "../features/features/FeatureDisabled";
import { useFeatureFlags } from "../features/features/useFeatureFlags";
import { DrilldownDiffPanel } from "../features/report/DrilldownDiffPanel";
import { FlakyChecksPanel } from "../features/report/FlakyChecksPanel";
import { SloPanel } from "../features/report/SloPanel";
import { notifyError } from "../notify";
import { ageMs, relativeTime } from "../utils/datetime";

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
  const direct = String(row.kb || row.kb_link || row.kb_url || "").trim();
  if (direct) return direct;
  const id = extractKbId(row);
  return id ? `http://portal.nutanix.com/kb/${id}` : "";
}

function freshnessTag(iso: string): { color: string; label: string } {
  if (!iso) return { color: "default", label: "Unknown" };
  const age = ageMs(iso);
  if (!Number.isFinite(age)) return { color: "default", label: "Unknown" };
  if (age <= 6 * 3600_000) return { color: "success", label: "Fresh" };
  if (age <= 24 * 3600_000) return { color: "processing", label: "Recent" };
  if (age <= 72 * 3600_000) return { color: "warning", label: "Aging" };
  return { color: "error", label: "Stale" };
}

function healthGradeColor(score: number): string {
  if (score >= 90) return "#22c55e";
  if (score >= 75) return "#84cc16";
  if (score >= 60) return "#f59e0b";
  if (score >= 40) return "#f97316";
  return "#f43f5e";
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

export function InsightsPage() {
  const features = useFeatureFlags();
  const insightsOff = features.data?.insights === false;
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData, enabled: features.isFetched && !insightsOff });
  const trends = useQuery({ queryKey: ["report-trends"], queryFn: () => api.reportTrends(24), enabled: features.isFetched && !insightsOff });
  const nccMarks = useQuery({ queryKey: ["ncc-dispositions"], queryFn: api.nccDispositions, staleTime: 15_000, enabled: features.isFetched && !insightsOff });
  const [drillCheck, setDrillCheck] = useState<DrillRow | null>(null);

  useEffect(() => {
    if (report.error) notifyError(report.error, "Failed to load insights");
  }, [report.error]);
  useEffect(() => {
    if (trends.error) notifyError(trends.error, "Failed to load trends");
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
  const weightedPenalty =
    (totalPlugins > 0 ? (failCount / totalPlugins) * 100 : 0) * 8.0 +
    (totalPlugins > 0 ? (errorCount / totalPlugins) * 100 : 0) * 5.5 +
    (totalPlugins > 0 ? (warnCount / totalPlugins) * 100 : 0) * 3.5 +
    (totalPlugins > 0 ? (infoCount / totalPlugins) * 100 : 0) * 2.2 +
    (totalPlugins > 0 ? (unknownCount / totalPlugins) * 100 : 0) * 3.0;
  const weightedHealth = Math.max(0, Math.min(100, rawPassRate - weightedPenalty));
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
  const fresh = freshnessTag(runTimestamp);
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
        const passRate = total > 0 ? (pass / total) * 100 : 0;
        const penalty =
          (total > 0 ? (fail / total) * 100 : 0) * 8.0 +
          (total > 0 ? (err / total) * 100 : 0) * 5.5 +
          (total > 0 ? (warn / total) * 100 : 0) * 3.5 +
          (total > 0 ? (info / total) * 100 : 0) * 2.2 +
          (total > 0 ? (unknown / total) * 100 : 0) * 3.0;
        const health = Math.max(0, Math.min(100, passRate - penalty));
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
          health,
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

  // ---------- render skeleton ----------

  if (insightsOff) {
    return <FeatureDisabled feature="insights" />;
  }

  if ((features.isLoading && !features.data) || (report.isLoading && !report.data)) {
    return (
      <Card className="page-card">
        <Skeleton active paragraph={{ rows: 6 }} />
      </Card>
    );
  }

  // ---------- main render ----------

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

  const briefing: Array<{ tone: "error" | "warning" | "info" | "success"; text: string }> = [];
  if (clustersFailed > 0) {
    const classes = failureClassEntries.map((e) => `${e.count} ${e.name.replace(/_/g, " ")}`).join(", ");
    briefing.push({
      tone: "error",
      text: `${clustersFailed} of ${clustersOk + clustersFailed} clusters did not complete this run.${classes ? ` Reasons: ${classes}.` : ""}`,
    });
  }
  if (fresh.label === "Stale" || fresh.label === "Aging") {
    briefing.push({
      tone: fresh.label === "Stale" ? "error" : "warning",
      text: `These results are ${fresh.label.toLowerCase()} (${relativeTime(runTimestamp)}). They describe that run.`,
    });
  }
  if (newFailCount > 0 || resolvedFailCount > 0) {
    briefing.push({
      tone: newFailCount > resolvedFailCount ? "warning" : "success",
      text: `${newFailCount} FAIL ${newFailCount === 1 ? "finding is" : "findings are"} new since the previous run, and ${resolvedFailCount} ${resolvedFailCount === 1 ? "was" : "were"} resolved.`,
    });
  }
  if (triage.returned > 0) {
    briefing.push({
      tone: "warning",
      text: `${triage.returned} resolved ${triage.returned === 1 ? "alert was" : "alerts were"} found again on this run.`,
    });
  }
  if (triage.needsAttention > 0) {
    const hottest = triage.clusters.find((c) => c.needs > 0);
    briefing.push({
      tone: "warning",
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
      tone: "warning",
      text: `NCC versions differ across clusters: ${triage.nccVersions.map((v) => `${v.version} (${v.clusters.length})`).join(", ")}.`,
    });
  }
  if (briefing.length === 0 && totalPlugins > 0) {
    briefing.push({
      tone: "success",
      text: previousTs
        ? "Every cluster completed, there are no new failures since the previous run, and no critical alerts are waiting."
        : "Every cluster completed, and no critical alerts are waiting. There is no earlier run to compare.",
    });
  }

  const severityRows = [
    { label: "PASS", count: passCount, color: "#22c55e" },
    { label: "FAIL", count: failCount, color: "#f43f5e" },
    { label: "WARN", count: warnCount, color: "#f59e0b" },
    { label: "ERR", count: errorCount, color: "#f97316" },
    { label: "INFO", count: infoCount, color: "#38bdf8" },
    { label: "UNKNOWN", count: unknownCount, color: "#94a3b8" },
  ];

  const checkAggColumns: ColumnsType<(typeof checkAgg)[number]> = [
    {
      title: "Check",
      key: "name",
      render: (_, row) => (
        <Space orientation="vertical" size={0}>
          <Typography.Text strong>{row.name}</Typography.Text>
          {row.clusterList.length > 0 ? (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {row.clusterList.slice(0, 4).join(", ")}
              {row.clusterList.length > 4 ? ` +${row.clusterList.length - 4} more` : ""}
            </Typography.Text>
          ) : null}
        </Space>
      ),
    },
    {
      title: "Severity",
      dataIndex: "severity",
      key: "severity",
      width: 100,
      render: (v: string) => (
        <Tag color={v === "FAIL" ? "error" : v === "ERR" ? "volcano" : "warning"}>{v}</Tag>
      ),
    },
    { title: "Occurrences", dataIndex: "count", key: "count", width: 110, align: "right" },
    {
      title: "Affected Clusters",
      key: "clusters",
      width: 130,
      align: "right",
      render: (_, row) => row.clusterList.length,
    },
    {
      title: "KB",
      key: "kb",
      width: 90,
      render: (_, row) =>
        row.kbId ? (
          <a href={row.kb} target="_blank" rel="noreferrer">
            <Tag color="processing" icon={<LinkOutlined />}>
              KB {row.kbId}
            </Tag>
          </a>
        ) : (
          <Typography.Text type="secondary">—</Typography.Text>
        ),
    },
  ];

  return (
    <>
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {/* HERO HEADER */}
      <Card className="page-card insights-hero">
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} md={8}>
            <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
              <Tooltip
                title={`Pass rate ${rawPassRate.toFixed(1)}% (${passCount.toLocaleString()} of ${totalPlugins.toLocaleString()} checks). Health also weighs warnings and errors.`}
              >
                <Progress
                  type="dashboard"
                  percent={Number(weightedHealth.toFixed(1))}
                  strokeColor={healthGradeColor(weightedHealth)}
                  size={140}
                  format={(percent) => (
                    <div style={{ textAlign: "center" }}>
                      <div style={{ fontSize: 24, fontWeight: 700 }}>{percent}%</div>
                      <div style={{ fontSize: 12, color: healthGradeColor(weightedHealth), fontWeight: 600 }}>
                        {healthGrade(weightedHealth)}
                      </div>
                    </div>
                  )}
                />
              </Tooltip>
              <div>
                <Typography.Text type="secondary" style={{ fontSize: 12, letterSpacing: 1, textTransform: "uppercase" }}>
                  Weighted Health
                </Typography.Text>
                <div>
                  <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                    Pass rate {totalPlugins > 0 ? `${rawPassRate.toFixed(1)}%` : "—"}
                  </Typography.Text>
                </div>
                <Typography.Title level={4} style={{ margin: "4px 0 6px" }}>
                  Cluster Insights
                </Typography.Title>
                <Space size={6} wrap>
                  <Tooltip title={runTimestamp || "Run time unavailable"}>
                    <Tag icon={<ClockCircleOutlined />} color={fresh.color}>
                      {fresh.label} · {relativeTime(runTimestamp)}
                    </Tag>
                  </Tooltip>
                  {hasRegression ? (
                    <Tag icon={<ArrowUpOutlined />} color="error">
                      Regression
                    </Tag>
                  ) : deltaFail < 0 ? (
                    <Tag icon={<ArrowDownOutlined />} color="success">
                      Improving
                    </Tag>
                  ) : (
                    <Tag icon={<MinusOutlined />} color="default">
                      Stable
                    </Tag>
                  )}
                </Space>
              </div>
            </div>
          </Col>
          <Col xs={24} md={16}>
            <Row gutter={[12, 12]}>
              <Col xs={12} md={6}>
                <Statistic title="Checks" value={totalPlugins} />
              </Col>
              <Col xs={12} md={6}>
                <Statistic title="Clusters" value={clusterSummary.length} />
              </Col>
              <Col xs={12} md={6}>
                <Statistic
                  title="At-risk Clusters"
                  value={affectedClusters.length}
                  valueStyle={{ color: affectedClusters.length > 0 ? "#f43f5e" : "#22c55e" }}
                />
              </Col>
              <Col xs={12} md={6}>
                <Statistic
                  title="Run Duration"
                  value={runDuration > 0 ? runDuration.toFixed(1) : "—"}
                  suffix={runDuration > 0 ? "s" : ""}
                />
              </Col>
              <Col xs={12} md={4}>
                <Statistic
                  title="FAIL"
                  value={failCount}
                  valueStyle={{ color: failCount > 0 ? "#f43f5e" : undefined }}
                  prefix={<ExclamationCircleOutlined />}
                />
              </Col>
              <Col xs={12} md={4}>
                <Statistic
                  title="WARN"
                  value={warnCount}
                  valueStyle={{ color: warnCount > 0 ? "#f59e0b" : undefined }}
                />
              </Col>
              <Col xs={12} md={4}>
                <Statistic
                  title="ERR"
                  value={errorCount}
                  valueStyle={{ color: errorCount > 0 ? "#f97316" : undefined }}
                />
              </Col>
              <Col xs={12} md={4}>
                <Statistic
                  title="INFO"
                  value={infoCount}
                  valueStyle={{ color: infoCount > 0 ? "#38bdf8" : undefined }}
                />
              </Col>
              <Col xs={12} md={8}>
                <Statistic
                  title="PASS"
                  value={passCount}
                  valueStyle={{ color: "#22c55e" }}
                  prefix={<SafetyCertificateOutlined />}
                />
              </Col>
            </Row>
          </Col>
        </Row>
        {!consistencyOk ? (
          <Alert
            type="warning"
            showIcon
            style={{ marginTop: 16 }}
            title="Check totals do not match"
            description="The severity counts do not add up to the total number of checks. Start the run again if this looks wrong."
          />
        ) : null}
      </Card>

      {briefing.length > 0 ? (
        <Card className="page-card">
          <Typography.Title level={4} className="section-title">
            What this run is saying
          </Typography.Title>
          <Typography.Text type="secondary" className="section-subtitle">
            Highlights from this run and the one before it.
          </Typography.Text>
          <Space orientation="vertical" size={8} style={{ width: "100%", marginTop: 12 }}>
            {briefing.map((item) => (
              <Alert key={item.text} type={item.tone} showIcon title={item.text} />
            ))}
          </Space>
        </Card>
      ) : null}

      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          NCC triage queue
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Critical alerts and how they have been handled. Resolved alerts return to Needs attention if a later run finds them again.
        </Typography.Text>
        <Row gutter={[12, 12]} style={{ marginTop: 16 }}>
          <Col xs={12} md={6}>
            <Statistic title="Needs attention" value={triage.needsAttention} valueStyle={{ color: triage.needsAttention > 0 ? "#f43f5e" : "#22c55e" }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="Acknowledged, still open" value={triage.acknowledged} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="Resolved" value={triage.resolved} />
          </Col>
          <Col xs={24} md={6}>
            <Statistic
              title="Most acknowledgements"
              value={triage.topOwner || "—"}
              suffix={triage.topCount > 0 ? `(${triage.topCount})` : ""}
            />
          </Col>
        </Row>
        {triage.returned > 0 ? (
          <Typography.Paragraph type="secondary" style={{ margin: "12px 0 0" }}>
            {triage.returned} resolved {triage.returned === 1 ? "alert was" : "alerts were"} found again on this run.
          </Typography.Paragraph>
        ) : null}
        {triage.clusters.some((c) => c.needs > 0) ? (
          <Table
            style={{ marginTop: 16 }}
            size="small"
            pagination={false}
            rowKey="name"
            dataSource={triage.clusters.filter((c) => c.needs > 0).slice(0, 8)}
            columns={[
              { title: "Cluster", dataIndex: "name", key: "name" },
              { title: "Needs attention", dataIndex: "needs", key: "needs", width: 160, align: "right" },
              { title: "Acknowledged", dataIndex: "acknowledged", key: "acknowledged", width: 140, align: "right" },
              { title: "Resolved", dataIndex: "resolved", key: "resolved", width: 120, align: "right" },
            ]}
          />
        ) : null}
        {triage.missingKb > 0 ? (
          <Typography.Text type="secondary" style={{ display: "block", marginTop: 8 }}>
            {triage.missingKb} of these alerts have no knowledge-base article.
          </Typography.Text>
        ) : null}
      </Card>

      {sharedChecks.length > 0 ? (
        <Card className="page-card">
          <Typography.Title level={4} className="section-title">
            Same check, several clusters
          </Typography.Title>
          <Typography.Text type="secondary" className="section-subtitle">
            The same critical check on more than one cluster.
          </Typography.Text>
          <Table
            style={{ marginTop: 12 }}
            size="small"
            pagination={false}
            rowKey={(row) => `${row.name}|${row.severity}`}
            dataSource={sharedChecks.slice(0, 8)}
            onRow={(record) => ({
              onClick: () =>
                setDrillCheck({
                  key: `${record.name}|${record.severity}`,
                  name: record.name,
                  severity: record.severity,
                  count: record.count,
                  clusterList: record.clusterList,
                  kb: record.kb,
                  kbId: record.kbId,
                  sample: record.sample,
                }),
              style: { cursor: "pointer" },
            })}
            columns={[
              { title: "Check", dataIndex: "name", key: "name" },
              {
                title: "Severity",
                dataIndex: "severity",
                key: "severity",
                width: 110,
                render: (v: string) => <Tag color={v === "FAIL" ? "error" : "volcano"}>{v}</Tag>,
              },
              { title: "Clusters", key: "clusters", width: 110, align: "right", render: (_, row) => row.clusterList.length },
              { title: "Rows", dataIndex: "count", key: "count", width: 90, align: "right" },
              {
                title: "KB",
                key: "kb",
                width: 110,
                render: (_, row) => (row.kbId ? <a href={row.kb} target="_blank" rel="noreferrer">KB {row.kbId}</a> : "—"),
              },
            ]}
          />
        </Card>
      ) : null}

      {/* SEVERITY DISTRIBUTION */}
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Severity Distribution
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          How {totalPlugins.toLocaleString()} plugin checks are distributed across severities for the current run.
        </Typography.Text>
        {totalPlugins === 0 ? (
          <Empty description="No check totals for this run." />
        ) : (
          <>
            <div
              style={{
                display: "flex",
                height: 14,
                borderRadius: 999,
                overflow: "hidden",
                marginTop: 12,
                marginBottom: 12,
                background: "rgba(148,163,184,0.18)",
              }}
            >
              {severityRows.map((s) =>
                s.count > 0 ? (
                  <Tooltip key={s.label} title={`${s.label}: ${s.count} (${((s.count / totalPlugins) * 100).toFixed(1)}%)`}>
                    <div style={{ width: `${(s.count / totalPlugins) * 100}%`, background: s.color }} />
                  </Tooltip>
                ) : null,
              )}
            </div>
            <Row gutter={[12, 12]}>
              {severityRows.map((s) => (
                <Col key={s.label} xs={12} md={Math.floor(24 / severityRows.length)}>
                  <Space size={8} align="center">
                    <Badge color={s.color} />
                    <Typography.Text strong style={{ minWidth: 70, display: "inline-block" }}>
                      {s.label}
                    </Typography.Text>
                    <Typography.Text>{s.count.toLocaleString()}</Typography.Text>
                    <Typography.Text type="secondary">
                      ({totalPlugins > 0 ? ((s.count / totalPlugins) * 100).toFixed(1) : "0.0"}%)
                    </Typography.Text>
                  </Space>
                </Col>
              ))}
            </Row>
          </>
        )}
      </Card>

      {/* TOP FAILING CHECKS + ACTIONABLE FINDINGS */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card className="page-card">
            <Typography.Title level={4} className="section-title">
              Top Failing Checks
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Most frequent failed checks.
            </Typography.Text>
            {checkAgg.length === 0 ? (
              <Empty description="No FAIL/ERR/WARN findings." />
            ) : (
              <Table
                size="small"
                rowKey={(r) => `${r.name}|${r.severity}`}
                columns={checkAggColumns}
                dataSource={checkAgg.slice(0, 10)}
                pagination={false}
                onRow={(record) => ({
                  onClick: () =>
                    setDrillCheck({
                      key: `${record.name}|${record.severity}`,
                      name: record.name,
                      severity: record.severity,
                      count: record.count,
                      clusterList: record.clusterList,
                      kb: record.kb,
                      kbId: record.kbId,
                      sample: record.sample,
                    }),
                  style: { cursor: "pointer" },
                })}
              />
            )}
            <Typography.Text type="secondary" style={{ fontSize: 12, display: "block", marginTop: 8 }}>
              Select a row to see the clusters and the original check output.
            </Typography.Text>
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card className="page-card" style={{ height: "100%" }}>
            <Typography.Title level={4} className="section-title">
              Top Actionable Findings
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Critical findings to review first.
            </Typography.Text>
            {actionableFindings.length === 0 ? (
              <Empty description="No critical findings in this run." />
            ) : (
              <Space orientation="vertical" size={10} style={{ width: "100%" }}>
                {actionableFindings.map((item, idx) => {
                  const cluster = resolveClusterName(String(item.row.cluster || "-"), clusterNameMap);
                  const reason = deriveReason(item.row, item.severity);
                  const kbId = extractKbId(item.row);
                  return (
                    <Card key={`${cluster}-${item.checkName}-${idx}`} size="small" className="finding-card">
                      <Space size={6} wrap style={{ marginBottom: 4 }}>
                        <Tag color={item.severity === "ERR" ? "volcano" : "error"}>{item.severity}</Tag>
                        <Typography.Text strong>{cluster}</Typography.Text>
                        <Typography.Text type="secondary">·</Typography.Text>
                        <Typography.Text>{item.checkName}</Typography.Text>
                      </Space>
                      <Typography.Paragraph type="secondary" style={{ marginBottom: 6 }}>
                        {reason}
                      </Typography.Paragraph>
                      {kbId ? (
                        <a href={item.kb} target="_blank" rel="noreferrer">
                          <Tag color="processing" icon={<LinkOutlined />}>
                            Open KB {kbId}
                          </Tag>
                        </a>
                      ) : null}
                    </Card>
                  );
                })}
              </Space>
            )}
          </Card>
        </Col>
      </Row>

      {/* CLUSTER RANKING + SEVERITY TREND */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card className="page-card">
            <Typography.Title level={4} className="section-title">
              Cluster Health Ranking
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Clusters ordered from worst to best weighted health.
            </Typography.Text>
            {clusterRanking.length === 0 ? (
              <Empty description="No cluster summaries available." />
            ) : (
              <Space orientation="vertical" size={10} style={{ width: "100%", marginTop: 8 }}>
                {clusterRanking.map((c) => (
                  <div key={c.address}>
                    <Space size={8} style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                      <Space size={6}>
                        <Typography.Text strong>{c.name}</Typography.Text>
                        {(() => {
                          const open = triage.clusters.find((item) => item.name === c.name);
                          return open && open.needs > 0 ? <Tag color="gold">{open.needs} unmarked</Tag> : null;
                        })()}
                        {c.fail > 0 ? <Tag color="error">FAIL {c.fail}</Tag> : null}
                        {c.err > 0 ? <Tag color="volcano">ERR {c.err}</Tag> : null}
                        {c.warn > 0 ? <Tag color="warning">WARN {c.warn}</Tag> : null}
                        {c.info > 0 ? <Tag color="processing">INFO {c.info}</Tag> : null}
                      </Space>
                      <Typography.Text strong style={{ color: healthGradeColor(c.health) }}>
                        {c.health.toFixed(1)}% · {healthGrade(c.health)}
                      </Typography.Text>
                    </Space>
                    <Progress
                      percent={Number(c.health.toFixed(1))}
                      strokeColor={healthGradeColor(c.health)}
                      showInfo={false}
                      size="small"
                    />
                  </div>
                ))}
              </Space>
            )}
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card className="page-card" style={{ height: "100%" }}>
            <Typography.Title level={4} className="section-title">
              Severity Trend
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Failed checks across the last {recentTrends.length || 0} runs.
              {trendFailDelta !== null ? ` Failures versus the previous run: ${trendFailDelta > 0 ? "+" : ""}${trendFailDelta}.` : ""}
              {trendHealthDelta !== null ? ` Average health ${trendHealthDelta > 0 ? "+" : ""}${trendHealthDelta.toFixed(0)}.` : ""}
            </Typography.Text>
            {recentTrends.length === 0 ? (
              <Empty description="Not enough runs to show a trend." />
            ) : (
              <Space orientation="vertical" size={6} style={{ width: "100%", marginTop: 8 }}>
                {recentTrends.map((p, idx) => {
                  const total = Math.max(1, toNumber(p.total_checks, 1));
                  const riskPct = Math.min(100, ((toNumber(p.fail_total) + toNumber(p.err_total)) / total) * 100);
                  const ts = String(p.timestamp || "-");
                  return (
                    <div key={`${ts}-${idx}`}>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }}>
                        <Typography.Text type="secondary">{relativeTime(ts)}</Typography.Text>
                        <Typography.Text type="secondary">
                          FAIL {toNumber(p.fail_total)} · ERR {toNumber(p.err_total)}
                        </Typography.Text>
                      </div>
                      <Progress
                        percent={Number(riskPct.toFixed(2))}
                        size="small"
                        showInfo={false}
                        status={riskPct >= 3 ? "exception" : riskPct >= 1 ? "active" : "success"}
                      />
                    </div>
                  );
                })}
              </Space>
            )}
          </Card>
        </Col>
      </Row>

      {/* KB INDEX + RUN RELIABILITY */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card className="page-card">
            <Typography.Title level={4} className="section-title">
              Knowledge Base References
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Articles referenced by failed checks. Select one to open it.
            </Typography.Text>
            {kbIndex.length === 0 ? (
              <Empty description="No knowledge-base articles in this run." />
            ) : (
              <Space size={[8, 8]} wrap style={{ marginTop: 8 }}>
                {kbIndex.map((kb) => (
                  <Tooltip
                    key={kb.id}
                    title={
                      <>
                        <div>{kb.titlesArr.slice(0, 3).join(", ")}</div>
                        <div style={{ marginTop: 4, opacity: 0.85 }}>
                          {kb.clustersArr.slice(0, 4).join(", ")}
                          {kb.clustersArr.length > 4 ? ` +${kb.clustersArr.length - 4}` : ""}
                        </div>
                      </>
                    }
                  >
                    <a href={kb.url} target="_blank" rel="noreferrer">
                      <Tag color="processing" icon={<LinkOutlined />} style={{ fontSize: 13, padding: "2px 10px" }}>
                        KB {kb.id} <span style={{ opacity: 0.7, marginLeft: 4 }}>×{kb.count}</span>
                      </Tag>
                    </a>
                  </Tooltip>
                ))}
              </Space>
            )}
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card className="page-card" style={{ height: "100%" }}>
            <Typography.Title level={4} className="section-title">
              Run Reliability
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Clusters that could not be reached.
            </Typography.Text>
            {failureClassEntries.length === 0 ? (
              <Alert type="success" showIcon style={{ marginTop: 8 }} title="Every cluster completed this run." />
            ) : (
              <Space orientation="vertical" size={8} style={{ width: "100%", marginTop: 8 }}>
                {failureClassEntries.map((e) => (
                  <div key={e.name} style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <Tag color={e.name === "rate_limit" || e.name === "timeout" ? "warning" : "error"}>{e.name.replace(/_/g, " ")}</Tag>
                    <Typography.Text strong>{e.count}</Typography.Text>
                  </div>
                ))}
              </Space>
            )}
            {triage.nccVersions.length > 0 || triage.aosVersions.length > 0 ? (
              <div style={{ marginTop: 16 }}>
                <Typography.Text strong>Versions in this run</Typography.Text>
                <div style={{ marginTop: 8 }}>
                  <Space size={[6, 6]} wrap>
                    {triage.nccVersions.map((v) => (
                      <Tooltip key={`ncc-${v.version}`} title={v.clusters.slice(0, 6).join(", ")}>
                        <Tag>NCC {v.version} · {v.clusters.length}</Tag>
                      </Tooltip>
                    ))}
                    {triage.aosVersions.map((v) => (
                      <Tooltip key={`aos-${v.version}`} title={v.clusters.slice(0, 6).join(", ")}>
                        <Tag>{v.version} · {v.clusters.length}</Tag>
                      </Tooltip>
                    ))}
                  </Space>
                </div>
              </div>
            ) : null}
            <div style={{ marginTop: 16 }}>
              <Descriptions size="small" column={1} bordered>
                <Descriptions.Item label="Clusters completed">{toNumber(runSummary.clusters_ok)}</Descriptions.Item>
                <Descriptions.Item label="Clusters not completed">
                  <Typography.Text type={toNumber(runSummary.clusters_failed) > 0 ? "danger" : undefined}>
                    {toNumber(runSummary.clusters_failed)}
                  </Typography.Text>
                </Descriptions.Item>
                <Descriptions.Item label="Run result">{toNumber(runSummary.exit_code) === 0 ? "Completed" : `Failed (${toNumber(runSummary.exit_code)})`}</Descriptions.Item>
              </Descriptions>
            </div>
          </Card>
        </Col>
      </Row>

      {/* REGRESSION SUMMARY */}
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Run-over-Run Comparison
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          {previousTs
            ? <>Compared with previous run from {relativeTime(previousTs)}.</>
            : "No previous run found for comparison."}
        </Typography.Text>
        <Row gutter={[12, 12]} style={{ marginTop: 8 }}>
          <Col xs={12} md={6}>
            <Statistic title="Previous FAILs" value={toNumber(regression.previous_fail_total)} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="Current FAILs" value={toNumber(regression.current_fail_total)} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic
              title="Change"
              value={deltaFail}
              prefix={deltaFail > 0 ? <ArrowUpOutlined /> : deltaFail < 0 ? <ArrowDownOutlined /> : <MinusOutlined />}
              valueStyle={{ color: deltaFail > 0 ? "#f43f5e" : deltaFail < 0 ? "#22c55e" : undefined }}
            />
          </Col>
          <Col xs={12} md={6}>
            <Statistic
              title="Outcome"
              value={hasRegression ? "Regression" : deltaFail < 0 ? "Improvement" : "Stable"}
              valueStyle={{ color: hasRegression ? "#f43f5e" : deltaFail < 0 ? "#22c55e" : undefined }}
            />
          </Col>
        </Row>
      </Card>

      {/* EXISTING DETAIL PANELS */}
      <DrilldownDiffPanel drilldownDiff={data.drilldown_diff} clusterNameMap={clusterNameMap} />
      <FlakyChecksPanel flakyChecks={data.flaky_checks} clusterNameMap={clusterNameMap} />
      <SloPanel
        sloDashboard={data.slo_dashboard}
        nccClusterSummary={Array.isArray(data.ncc_cluster_summary) ? data.ncc_cluster_summary : []}
        regressionSummary={data.regression_summary}
        clusterNameMap={clusterNameMap}
      />

      {/* REPORT META — moved to bottom as small footer */}
      <Card className="page-card">
        <Typography.Title level={5} className="section-title">
          Report Metadata
        </Typography.Title>
        {Object.keys(meta).length === 0 ? (
          <Empty description="No report metadata available." />
        ) : (
          <Descriptions size="small" column={2} bordered>
            <Descriptions.Item label="Version">{String(meta.version || "-")}</Descriptions.Item>
            <Descriptions.Item label="Stream">{String(meta.stream || "-")}</Descriptions.Item>
            <Descriptions.Item label="Build Date">{String(meta.build_date || "-")}</Descriptions.Item>
            <Descriptions.Item label="Git Revision">{String(meta.git_revision || "-")}</Descriptions.Item>
            <Descriptions.Item label="Hostname">{String(meta.hostname || "-")}</Descriptions.Item>
            <Descriptions.Item label="Source">{String(meta.scheduler_source || "-")}</Descriptions.Item>
          </Descriptions>
        )}
      </Card>
    </Space>

    <Drawer
      open={Boolean(drillCheck)}
      onClose={() => setDrillCheck(null)}
      width={Math.min(720, typeof window !== "undefined" ? window.innerWidth - 64 : 720)}
      destroyOnHidden
      title={
        drillCheck ? (
          <Space size={8} wrap>
            <Tag color={drillCheck.severity === "FAIL" ? "error" : drillCheck.severity === "ERR" ? "volcano" : "warning"}>
              {drillCheck.severity}
            </Tag>
            <span>{drillCheck.name}</span>
          </Space>
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
        <Space orientation="vertical" size={16} style={{ width: "100%" }}>
          <Descriptions size="small" column={2} bordered>
            <Descriptions.Item label="Occurrences">{drillCheck.count}</Descriptions.Item>
            <Descriptions.Item label="Affected clusters">{drillCheck.clusterList.length}</Descriptions.Item>
          </Descriptions>

          <div>
            <Typography.Title level={5} style={{ marginBottom: 8 }}>
              Clusters where this check fired
            </Typography.Title>
            {drillCheck.clusterList.length === 0 ? (
              <Empty description="No clusters listed." />
            ) : (
              <Space size={[6, 6]} wrap>
                {drillCheck.clusterList.map((c) => (
                  <Tag key={c} color="default">{c}</Tag>
                ))}
              </Space>
            )}
          </div>

          <div>
            <Typography.Title level={5} style={{ marginBottom: 8 }}>
              Check output
            </Typography.Title>
            {drillOccurrences.length === 0 ? (
              <Empty description="No details for this check." />
            ) : (
              <List
                size="small"
                bordered
                dataSource={drillOccurrences}
                renderItem={(row) => {
                  const cluster = resolveClusterName(String(row.cluster || "-"), clusterNameMap);
                  const detail = String(row.detail || row.details || row.message || "").trim();
                  return (
                    <List.Item style={{ display: "block" }}>
                      <Space size={6} wrap style={{ marginBottom: 4 }}>
                        <Typography.Text strong>{cluster}</Typography.Text>
                        {row.host ? <Typography.Text type="secondary">· {String(row.host)}</Typography.Text> : null}
                      </Space>
                      {detail ? (
                        <Typography.Paragraph
                          style={{
                            margin: 0,
                            whiteSpace: "pre-wrap",
                            fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                            fontSize: 12,
                            background: "var(--surface-muted, rgba(0,0,0,0.04))",
                            padding: 8,
                            borderRadius: 4,
                          }}
                        >
                          {detail.length > 400 ? `${detail.slice(0, 400)}…` : detail}
                        </Typography.Paragraph>
                      ) : (
                        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                          (no detail captured)
                        </Typography.Text>
                      )}
                    </List.Item>
                  );
                }}
              />
            )}
          </div>
        </Space>
      ) : null}
    </Drawer>
    </>
  );
}
