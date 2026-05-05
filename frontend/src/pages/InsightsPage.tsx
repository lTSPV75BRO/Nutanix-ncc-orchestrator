import { useQuery } from "@tanstack/react-query";
import { Card, Descriptions, Empty, Space, Typography } from "antd";
import { asRecord, buildClusterNameMap } from "../utils/report";
import { api } from "../api/client";
import { DrilldownDiffPanel } from "../features/report/DrilldownDiffPanel";
import { FlakyChecksPanel } from "../features/report/FlakyChecksPanel";
import { SloPanel } from "../features/report/SloPanel";
import { useMemo } from "react";

export function InsightsPage() {
  const report = useQuery({ queryKey: ["report-data"], queryFn: api.reportData });
  const data = report.data ?? {
    run_summary: {},
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
      <DrilldownDiffPanel drilldownDiff={data.drilldown_diff} clusterNameMap={clusterNameMap} />
      <FlakyChecksPanel flakyChecks={data.flaky_checks} clusterNameMap={clusterNameMap} />
      <SloPanel sloDashboard={data.slo_dashboard} regressionSummary={data.regression_summary} clusterNameMap={clusterNameMap} />
    </>
  );
}
