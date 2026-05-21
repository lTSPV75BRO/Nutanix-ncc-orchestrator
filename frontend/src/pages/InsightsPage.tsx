import { useQuery } from "@tanstack/react-query";
import { Alert, Card, Col, Descriptions, Empty, Progress, Row, Space, Statistic, Typography } from "antd";
import { asArray, asRecord, buildClusterNameMap, resolveClusterName, toNumber } from "../utils/report";
import { api } from "../api/client";
import { DrilldownDiffPanel } from "../features/report/DrilldownDiffPanel";
import { FlakyChecksPanel } from "../features/report/FlakyChecksPanel";
import { SloPanel } from "../features/report/SloPanel";
import { useMemo } from "react";

export function InsightsPage() {
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData });
  const trends = useQuery({ queryKey: ["report-trends"], queryFn: () => api.reportTrends(24) });
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
  const weightedHealthRate = Math.max(0, rawPassRate - weightedPenalty);
  const countConsistencyOk = passCount + failCount + errorCount + infoCount + unknownCount === totalPlugins;
  const affectedClusters = clusterSummary.filter((c) => toNumber(c.fail) > 0 || toNumber(c.error) > 0);
  const worstCluster = clusterSummary
    .slice()
    .sort((a, b) => {
      const aTotal = toNumber(a.total_plugins, 0);
      const bTotal = toNumber(b.total_plugins, 0);
      const aRiskRate = aTotal > 0 ? ((toNumber(a.fail) + toNumber(a.error)) / aTotal) * 100 : 0;
      const bRiskRate = bTotal > 0 ? ((toNumber(b.fail) + toNumber(b.error)) / bTotal) * 100 : 0;
      return bRiskRate - aRiskRate;
    })[0];
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
  const recentTrends = trendPoints.slice(-8);
  const actionableFindings = asArray(data.agg_rows)
    .map((r) => asRecord(r))
    .filter((r) => {
      const severity = String(r.severity || "").toUpperCase();
      return severity === "FAIL" || severity === "ERR";
    })
    .slice(0, 3);
  return (
    <>
      <Card className="page-card">
          <Typography.Title level={4} className="section-title">
            Insights
          </Typography.Title>
          <Typography.Text type="secondary" className="section-subtitle">
            Advanced JSON artifacts and analysis details moved out of dashboard.
          </Typography.Text>
          <Space size={[8, 8]} wrap style={{ marginTop: 8, display: "flex" }}>
            {Object.entries((data.artifact_links || {}) as Record<string, string>).map(([name, href]) => (
              <a key={name} href={href} target="_blank" rel="noreferrer">{name}</a>
            ))}
          </Space>
          <Descriptions
            size="small"
            column={2}
            style={{ marginTop: 10 }}
            items={[
              { key: "version", label: "Version", children: String(meta.version || "-") },
              { key: "stream", label: "Stream", children: String(meta.stream || "-") },
              { key: "build", label: "Build Date", children: String(meta.build_date || "-") },
              { key: "git", label: "Git Revision", children: String(meta.git_revision || "-") },
            ]}
          />
          {Object.keys(meta).length === 0 ? <Empty description="No report metadata available." /> : null}
      </Card>
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Operational Snapshot
        </Typography.Title>
        <Typography.Text type="secondary" className="section-subtitle">
          Plugin-level health and risk summary derived from NCC per-cluster summary logs.
        </Typography.Text>
        <Row gutter={[12, 12]} style={{ marginTop: 8, marginBottom: 8 }}>
          <Col xs={24} md={6}>
            <Card size="small">
              <Statistic title="Total Plugins" value={totalPlugins} />
            </Card>
          </Col>
          <Col xs={24} md={6}>
            <Card size="small">
              <Statistic title="Raw Pass Rate" value={Number(rawPassRate.toFixed(2))} suffix="%" />
            </Card>
          </Col>
          <Col xs={24} md={6}>
            <Card size="small">
              <Statistic title="Weighted Health Rate" value={Number(weightedHealthRate.toFixed(2))} suffix="%" />
            </Card>
          </Col>
          <Col xs={24} md={6}>
            <Card size="small">
              <Statistic title="Affected Clusters (FAIL/ERR)" value={affectedClusters.length} />
            </Card>
          </Col>
        </Row>
        {worstCluster ? (
          <Typography.Text type="secondary" className="section-subtitle">
            Highest-risk cluster: {resolveClusterName(String(worstCluster.address || "-"), clusterNameMap)} (
            {(() => {
              const total = toNumber(worstCluster.total_plugins, 0);
              const riskRate = total > 0 ? ((toNumber(worstCluster.fail) + toNumber(worstCluster.error)) / total) * 100 : 0;
              return `${riskRate.toFixed(2)}% FAIL+ERR`;
            })()}
            )
          </Typography.Text>
        ) : null}
        <Alert
          type={countConsistencyOk ? "success" : "warning"}
          showIcon
          style={{ marginTop: 8 }}
          message={
            countConsistencyOk
              ? "Summary consistency check passed."
              : "Summary consistency mismatch: FAIL/ERR/INFO/PASS/UNKNOWN does not match total plugins."
          }
        />
      </Card>
      <Row gutter={[12, 12]}>
        <Col xs={24} md={12}>
          <Card className="page-card" title="Top 3 Actionable Findings">
            {actionableFindings.length === 0 ? (
              <Empty description="No FAIL/ERR findings in current snapshot." />
            ) : (
              <Space direction="vertical" size={10} style={{ width: "100%" }}>
                {actionableFindings.map((item, idx) => {
                  const cluster = resolveClusterName(String(item.cluster || "-"), clusterNameMap);
                  const checkName = String(item.check || item.check_name || item.title || "Unnamed check");
                  const kbUrl = String(item.kb || item.kb_link || item.kb_url || "").trim();
                  return (
                    <Card key={`${cluster}-${checkName}-${idx}`} size="small">
                      <Typography.Text strong>{cluster}</Typography.Text>
                      <div>{checkName}</div>
                      <Typography.Text type="secondary">
                        Why it matters: {String(item.details || item.message || "Cluster health is impacted by this result.").slice(0, 180)}
                      </Typography.Text>
                      {kbUrl ? (
                        <div style={{ marginTop: 6 }}>
                          <a href={kbUrl} target="_blank" rel="noreferrer">
                            KB reference
                          </a>
                        </div>
                      ) : null}
                    </Card>
                  );
                })}
              </Space>
            )}
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card className="page-card" title="Severity Trend (last runs)">
            {recentTrends.length === 0 ? (
              <Empty description="No trend points available yet." />
            ) : (
              <Space direction="vertical" size={8} style={{ width: "100%" }}>
                {recentTrends.map((p, idx) => {
                  const total = Math.max(1, toNumber(p.total_checks, 1));
                  const riskPct = Math.min(100, ((toNumber(p.fail_total) + toNumber(p.err_total)) / total) * 100);
                  return (
                    <div key={`${p.timestamp || "point"}-${idx}`}>
                      <Typography.Text type="secondary">
                        {String(p.timestamp || "-")} | FAIL {toNumber(p.fail_total)} | ERR {toNumber(p.err_total)}
                      </Typography.Text>
                      <Progress percent={Number(riskPct.toFixed(2))} size="small" status={riskPct >= 3 ? "exception" : riskPct >= 1 ? "active" : "success"} />
                    </div>
                  );
                })}
              </Space>
            )}
          </Card>
        </Col>
      </Row>
      <DrilldownDiffPanel drilldownDiff={data.drilldown_diff} clusterNameMap={clusterNameMap} />
      <FlakyChecksPanel flakyChecks={data.flaky_checks} clusterNameMap={clusterNameMap} />
      <SloPanel
        sloDashboard={data.slo_dashboard}
        nccClusterSummary={Array.isArray(data.ncc_cluster_summary) ? data.ncc_cluster_summary : []}
        regressionSummary={data.regression_summary}
        clusterNameMap={clusterNameMap}
      />
    </>
  );
}
