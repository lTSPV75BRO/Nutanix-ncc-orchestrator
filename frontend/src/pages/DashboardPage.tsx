import { useEffect, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Card, Input, Select, Space, Tag, Typography } from "antd";
import { api } from "../api/client";
import { InsightsCards } from "../features/report/InsightsCards";
import { PolicyStatus } from "../features/report/PolicyStatus";
import { ClusterHealthWidget } from "../features/report/ClusterHealthWidget";
import { ClusterTable } from "../features/report/ClusterTable";
import { asArray, asRecord, buildClusterNameMap, displayClusterName, resolveClusterName } from "../utils/report";
import { useLocalStorageState } from "../hooks/useLocalStorageState";

type Severity = "FAIL" | "WARN" | "ERR" | "INFO";

export function DashboardPage() {
  const PREVIEW_LIMIT = 1500;
  const queryClient = useQueryClient();
  const [filterText, setFilterText] = useLocalStorageState("dashboard.filterText", "");
  const [severityFilters, setSeverityFilters] = useLocalStorageState<Severity[]>("dashboard.severityFilters", []);
  const [compareMode, setCompareMode] = useLocalStorageState<"all" | "changed" | "flaky">("dashboard.compareMode", "all");
  const [selectedClusters, setSelectedClusters] = useLocalStorageState<string[]>("dashboard.selectedClusters", []);
  const [loadFullReport, setLoadFullReport] = useState(false);

  const previewReport = useQuery({
    queryKey: ["report-data", "preview", PREVIEW_LIMIT],
    queryFn: () => api.reportDataWithPagination({ limit: PREVIEW_LIMIT }),
    staleTime: 30_000,
  });

  const fullReport = useQuery({
    queryKey: ["report-data", "full"],
    queryFn: () => api.reportData(),
    enabled: loadFullReport,
    staleTime: 30_000,
  });

  useEffect(() => {
    if (!previewReport.data) return;
    const hasMoreChecks = Boolean(previewReport.data.pagination?.checks_snapshot?.has_more);
    const hasMoreAgg = Boolean(previewReport.data.pagination?.agg_rows?.has_more);
    if (hasMoreChecks || hasMoreAgg) {
      const timer = setTimeout(() => setLoadFullReport(true), 100);
      return () => clearTimeout(timer);
    }
  }, [previewReport.data]);

  useEffect(() => {
    if (!fullReport.data) return;
    // Release preview cache once full payload is ready.
    queryClient.removeQueries({ queryKey: ["report-data", "preview", PREVIEW_LIMIT], exact: true });
  }, [fullReport.data, queryClient]);

  const reportData = fullReport.data ?? previewReport.data ?? {
    run_summary: {},
    checks_snapshot: [],
    drilldown_diff: {},
    flaky_checks: {},
    regression_summary: {},
    slo_dashboard: {},
    policy_violations: [],
  };

  const clusterNameMap = useMemo(
    () =>
      buildClusterNameMap({
        runSummary: reportData.run_summary,
        checksSnapshot: reportData.checks_snapshot,
        aggRows: Array.isArray(reportData.agg_rows) ? reportData.agg_rows : [],
        drilldownDiff: reportData.drilldown_diff,
        flakyChecks: reportData.flaky_checks,
        sloDashboard: reportData.slo_dashboard,
        regressionSummary: reportData.regression_summary,
      }),
    [
      reportData.run_summary,
      reportData.checks_snapshot,
      reportData.agg_rows,
      reportData.drilldown_diff,
      reportData.flaky_checks,
      reportData.slo_dashboard,
      reportData.regression_summary,
    ],
  );

  const clusterOptions = useMemo(() => {
    const names = new Set<string>();
    const add = (value: unknown) => {
      const label = resolveClusterName(value, clusterNameMap).trim();
      if (!label || label === "-") return;
      names.add(label);
    };
    asArray(asRecord(reportData.run_summary).clusters)
      .map((c) => asRecord(c))
      .forEach((c) => add(displayClusterName(c)));
    asArray(reportData.agg_rows)
      .map((r) => asRecord(r))
      .forEach((r) => {
        add(displayClusterName(r));
        add(r.cluster);
      });
    asArray(reportData.checks_snapshot)
      .map((r) => asRecord(r))
      .forEach((r) => {
        add(displayClusterName(r));
        add(r.cluster);
      });
    return Array.from(names)
      .sort((a, b) => a.localeCompare(b))
      .map((name) => ({ label: name, value: name }));
  }, [reportData.run_summary, reportData.agg_rows, reportData.checks_snapshot, clusterNameMap]);

  const filteredSummary = useMemo(() => {
    const summary = asRecord(reportData.run_summary);
    const clusters = asArray(summary.clusters).map((c) => asRecord(c));
    const selectedSet = new Set(selectedClusters.map((s) => s.toLowerCase()));
    const filtered = clusters.filter((c) => {
      const displayName = resolveClusterName(displayClusterName(c), clusterNameMap).toLowerCase();
      if (!displayName.includes(filterText.toLowerCase())) return false;
      if (selectedSet.size === 0) return true;
      const clusterAddress = String(c.cluster || c.address || "").toLowerCase();
      return selectedSet.has(displayName) || selectedSet.has(resolveClusterName(clusterAddress, clusterNameMap).toLowerCase());
    });
    return { ...summary, clusters: filtered };
  }, [reportData.run_summary, filterText, selectedClusters, clusterNameMap]);

  const toggleSeverity = (severity: Severity) => {
    setSeverityFilters((prev) => (prev.includes(severity) ? prev.filter((s) => s !== severity) : [...prev, severity]));
  };

  return (
    <>
      <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Dashboard
        </Typography.Title>
        <Space.Compact style={{ width: "100%" }}>
          <Button disabled type="default">
            Search / tokens
          </Button>
          <Input
            value={filterText}
            onChange={(e) => setFilterText(e.target.value)}
            placeholder="Type to filter... tokens: sev:FAIL cluster:10.1 changed:true flaky:true"
          />
        </Space.Compact>
        <Space size={[8, 8]} wrap style={{ marginTop: 12, marginBottom: 8 }}>
          <Tag.CheckableTag checked={severityFilters.length === 0} onChange={(checked) => checked && setSeverityFilters([])}>
            ALL
          </Tag.CheckableTag>
          {(["FAIL", "WARN", "ERR", "INFO"] as const).map((s) => (
            <Tag.CheckableTag key={s} checked={severityFilters.includes(s)} onChange={() => toggleSeverity(s)}>
              {s}
            </Tag.CheckableTag>
          ))}
          <Select
            mode="multiple"
            allowClear
            placeholder="Filter clusters (v1 style)"
            maxTagCount="responsive"
            style={{ minWidth: 300 }}
            value={selectedClusters}
            onChange={(values) => setSelectedClusters(values)}
            options={clusterOptions}
          />
          <Select
            style={{ minWidth: 180 }}
            value={compareMode}
            onChange={(value) => setCompareMode(value)}
            options={[
              { value: "all", label: "All rows" },
              { value: "changed", label: "Changed only" },
              { value: "flaky", label: "Flaky only" },
            ]}
          />
        </Space>
        <Typography.Text type="secondary" className="section-subtitle">
          Search tokens: <code>sev:FAIL</code> <code>cluster:10.1.1.5</code> <code>changed:true</code> <code>flaky:true</code>
        </Typography.Text>
        <Typography.Text type="secondary">Severity filters: {severityFilters.length === 0 ? "ALL" : severityFilters.join(", ")}</Typography.Text>
      </Card>

      {previewReport.data && !fullReport.data && loadFullReport && (
        <Alert
          type="info"
          showIcon
          title="Loading complete dataset in background"
          description="A fast preview is shown first for responsiveness. Full report rows are being fetched."
          style={{ marginBottom: 12 }}
        />
      )}

      <InsightsCards
        runSummary={reportData.run_summary}
        aggRows={Array.isArray(reportData.agg_rows) ? reportData.agg_rows : []}
      />
      <PolicyStatus violations={Array.isArray(reportData.policy_violations) ? reportData.policy_violations : []} />
      <ClusterHealthWidget runSummary={filteredSummary} filterText={filterText} selectedClusters={selectedClusters} clusterNameMap={clusterNameMap} />
      <ClusterTable
        checksSnapshot={reportData.checks_snapshot}
        aggRows={Array.isArray(reportData.agg_rows) ? reportData.agg_rows : []}
        diffFlags={(reportData.diff_flags || {}) as Record<string, unknown>}
        flakyKeys={(reportData.flaky_keys || {}) as Record<string, unknown>}
        nccLogs={Array.isArray(reportData.ncc_logs) ? reportData.ncc_logs : []}
        filterText={filterText}
        selectedClusters={selectedClusters}
        clusterNameMap={clusterNameMap}
        severityFilters={severityFilters}
        compareMode={compareMode}
      />
    </>
  );
}
