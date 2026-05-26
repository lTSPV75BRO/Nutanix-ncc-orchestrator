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
  ReloadOutlined,
  SafetyCertificateOutlined,
} from "@ant-design/icons";
import { asArray, asRecord, buildClusterNameMap, resolveClusterName, toNumber } from "../utils/report";
import { api } from "../api/client";
import { DrilldownDiffPanel } from "../features/report/DrilldownDiffPanel";
import { FlakyChecksPanel } from "../features/report/FlakyChecksPanel";
import { SloPanel } from "../features/report/SloPanel";
import { notifyError } from "../notify";

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

function freshnessTag(iso: string): { color: string; label: string } {
  if (!iso) return { color: "default", label: "Unknown" };
  const ageMs = Date.now() - new Date(iso).getTime();
  if (Number.isNaN(ageMs)) return { color: "default", label: "Unknown" };
  if (ageMs <= 6 * 3600_000) return { color: "success", label: "Fresh" };
  if (ageMs <= 24 * 3600_000) return { color: "processing", label: "Recent" };
  if (ageMs <= 72 * 3600_000) return { color: "warning", label: "Aging" };
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

export function InsightsPage() {
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData });
  const trends = useQuery({ queryKey: ["report-trends"], queryFn: () => api.reportTrends(24) });
  const [drillCheck, setDrillCheck] = useState<DrillRow | null>(null);

  useEffect(() => {
    if (report.error) notifyError(report.error, "Failed to load insights");
  }, [report.error]);
  useEffect(() => {
    if (trends.error) notifyError(trends.error, "Failed to load trends");
  }, [trends.error]);

  const data = report.data ?? {
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
  };

  const meta = asRecord(data.report_meta);
  const summaryCounts = asRecord(data.ncc_summary_counts);
  const clusterSummary = asArray(data.ncc_cluster_summary).map((c) => asRecord(c));
  const runSummary = asRecord(data.run_summary);
  const aggRows = asArray(data.agg_rows).map((r) => asRecord(r));
  const regression = asRecord(data.regression_summary);

  const totalPlugins = toNumber(summaryCounts.total_plugins);
  const passCount = toNumber(summaryCounts.pass);
  const failCount = toNumber(summaryCounts.fail);
  const errorCount = toNumber(summaryCounts.error);
  const infoCount = toNumber(summaryCounts.info);
  const unknownCount = toNumber(summaryCounts.unknown);
  const rawPassRate = totalPlugins > 0 ? (passCount / totalPlugins) * 100 : 0;
  const weightedPenalty =
    (totalPlugins > 0 ? (failCount / totalPlugins) * 100 : 0) * 8.0 +
    (totalPlugins > 0 ? (errorCount / totalPlugins) * 100 : 0) * 5.5 +
    (totalPlugins > 0 ? (infoCount / totalPlugins) * 100 : 0) * 2.2 +
    (totalPlugins > 0 ? (unknownCount / totalPlugins) * 100 : 0) * 3.0;
  const weightedHealth = Math.max(0, Math.min(100, rawPassRate - weightedPenalty));
  const consistencyOk = passCount + failCount + errorCount + infoCount + unknownCount === totalPlugins;
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
        const err = toNumber(c.error);
        const pass = toNumber(c.pass);
        const info = toNumber(c.info);
        const unknown = toNumber(c.unknown);
        const passRate = total > 0 ? (pass / total) * 100 : 0;
        const penalty =
          (total > 0 ? (fail / total) * 100 : 0) * 8.0 +
          (total > 0 ? (err / total) * 100 : 0) * 5.5 +
          (total > 0 ? (info / total) * 100 : 0) * 2.2 +
          (total > 0 ? (unknown / total) * 100 : 0) * 3.0;
        const health = Math.max(0, Math.min(100, passRate - penalty));
        return {
          address: String(c.address || ""),
          name: resolveClusterName(String(c.address || ""), clusterNameMap),
          total,
          fail,
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

  // ---------- render skeleton ----------

  if (report.isLoading && !report.data) {
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

  const severityRows = [
    { label: "PASS", count: passCount, color: "#22c55e" },
    { label: "FAIL", count: failCount, color: "#f43f5e" },
    { label: "ERR", count: errorCount, color: "#f97316" },
    { label: "INFO", count: infoCount, color: "#38bdf8" },
    { label: "UNKNOWN", count: unknownCount, color: "#94a3b8" },
  ];

  const checkAggColumns: ColumnsType<(typeof checkAgg)[number]> = [
    {
      title: "Check",
      key: "name",
      render: (_, row) => (
        <Space direction="vertical" size={0}>
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

  // Build a list of per-cluster occurrences for the currently drilled check.
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

  return (
    <>
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      {/* HERO HEADER */}
      <Card className="page-card insights-hero">
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} md={8}>
            <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
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
              <div>
                <Typography.Text type="secondary" style={{ fontSize: 12, letterSpacing: 1, textTransform: "uppercase" }}>
                  Weighted Health
                </Typography.Text>
                <Typography.Title level={4} style={{ margin: "4px 0 6px" }}>
                  Cluster Insights
                </Typography.Title>
                <Space size={6} wrap>
                  <Tooltip title={runTimestamp || "no timestamp"}>
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
                <Statistic title="Total Plugins" value={totalPlugins} />
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
              <Col xs={12} md={6}>
                <Statistic
                  title="FAIL"
                  value={failCount}
                  valueStyle={{ color: failCount > 0 ? "#f43f5e" : undefined }}
                  prefix={<ExclamationCircleOutlined />}
                />
              </Col>
              <Col xs={12} md={6}>
                <Statistic
                  title="ERR"
                  value={errorCount}
                  valueStyle={{ color: errorCount > 0 ? "#f97316" : undefined }}
                />
              </Col>
              <Col xs={12} md={6}>
                <Statistic
                  title="INFO"
                  value={infoCount}
                  valueStyle={{ color: infoCount > 0 ? "#38bdf8" : undefined }}
                />
              </Col>
              <Col xs={12} md={6}>
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
            message="Summary count mismatch"
            description="The sum of PASS/FAIL/ERR/INFO/UNKNOWN does not equal total plugins. Re-run NCC if the numbers look off."
          />
        ) : null}
      </Card>

      {/* SEVERITY DISTRIBUTION */}
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Severity Distribution
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          How {totalPlugins.toLocaleString()} plugin checks are distributed across severities for the current run.
        </Typography.Text>
        {totalPlugins === 0 ? (
          <Empty description="No NCC summary counts available." />
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
              Aggregated FAIL/ERR/WARN occurrences ranked by severity then frequency.
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
              Tip: click any row to see clusters where it failed and the original NCC plugin output.
            </Typography.Text>
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card className="page-card" style={{ height: "100%" }}>
            <Typography.Title level={4} className="section-title">
              Top Actionable Findings
            </Typography.Title>
            <Typography.Text type="secondary" className="section-subtitle">
              Highest-priority FAIL/ERR items with context and KB pointers.
            </Typography.Text>
            {actionableFindings.length === 0 ? (
              <Empty description="No FAIL/ERR findings in the current snapshot." />
            ) : (
              <Space direction="vertical" size={10} style={{ width: "100%" }}>
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
              <Space direction="vertical" size={10} style={{ width: "100%", marginTop: 8 }}>
                {clusterRanking.map((c) => (
                  <div key={c.address}>
                    <Space size={8} style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                      <Space size={6}>
                        <Typography.Text strong>{c.name}</Typography.Text>
                        {c.fail > 0 ? <Tag color="error">FAIL {c.fail}</Tag> : null}
                        {c.err > 0 ? <Tag color="volcano">ERR {c.err}</Tag> : null}
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
              FAIL+ERR rate over the last {recentTrends.length || 0} runs.
            </Typography.Text>
            {recentTrends.length === 0 ? (
              <Empty description="No trend points available yet." />
            ) : (
              <Space direction="vertical" size={6} style={{ width: "100%", marginTop: 8 }}>
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
              KB articles cited by failing checks in this run. Click to open.
            </Typography.Text>
            {kbIndex.length === 0 ? (
              <Empty description="No KB references in current findings." />
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
              Failure classes encountered during cluster polling.
            </Typography.Text>
            {failureClassEntries.length === 0 ? (
              <Alert type="success" showIcon style={{ marginTop: 8 }} message="All clusters polled cleanly. No transport, auth, or rate-limit errors." />
            ) : (
              <Space direction="vertical" size={8} style={{ width: "100%", marginTop: 8 }}>
                {failureClassEntries.map((e) => (
                  <div key={e.name} style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <Tag color={e.name === "rate_limit" || e.name === "timeout" ? "warning" : "error"}>{e.name.replace(/_/g, " ")}</Tag>
                    <Typography.Text strong>{e.count}</Typography.Text>
                  </div>
                ))}
              </Space>
            )}
            <div style={{ marginTop: 16 }}>
              <Descriptions size="small" column={1} bordered>
                <Descriptions.Item label="Clusters OK">{toNumber(runSummary.clusters_ok)}</Descriptions.Item>
                <Descriptions.Item label="Clusters Failed">
                  <Typography.Text type={toNumber(runSummary.clusters_failed) > 0 ? "danger" : undefined}>
                    {toNumber(runSummary.clusters_failed)}
                  </Typography.Text>
                </Descriptions.Item>
                <Descriptions.Item label="Exit Code">{toNumber(runSummary.exit_code)}</Descriptions.Item>
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
              title="Delta"
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
        {Object.keys((data.artifact_links || {}) as Record<string, unknown>).length > 0 ? (
          <div style={{ marginTop: 12 }}>
            <Typography.Text type="secondary">Artifacts: </Typography.Text>
            <Space size={[6, 6]} wrap>
              {Object.entries((data.artifact_links || {}) as Record<string, string>).map(([name, href]) => (
                <a key={name} href={href} target="_blank" rel="noreferrer">
                  <Tag icon={<ReloadOutlined />}>{name}</Tag>
                </a>
              ))}
            </Space>
          </div>
        ) : null}
      </Card>
    </Space>

    <Drawer
      open={Boolean(drillCheck)}
      onClose={() => setDrillCheck(null)}
      width={Math.min(720, typeof window !== "undefined" ? window.innerWidth - 64 : 720)}
      destroyOnClose
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
        <Space direction="vertical" size={16} style={{ width: "100%" }}>
          <Descriptions size="small" column={2} bordered>
            <Descriptions.Item label="Occurrences">{drillCheck.count}</Descriptions.Item>
            <Descriptions.Item label="Affected clusters">{drillCheck.clusterList.length}</Descriptions.Item>
          </Descriptions>

          <div>
            <Typography.Title level={5} style={{ marginBottom: 8 }}>
              Clusters where this check fired
            </Typography.Title>
            {drillCheck.clusterList.length === 0 ? (
              <Empty description="No clusters resolved." />
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
              Sample plugin output
            </Typography.Title>
            {drillOccurrences.length === 0 ? (
              <Empty description="No per-row detail available." />
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
