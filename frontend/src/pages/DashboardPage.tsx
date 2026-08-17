import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  Badge,
  Button,
  Card,
  Col,
  Empty,
  Input,
  Row,
  Select,
  Skeleton,
  Space,
  Statistic,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  ArrowRightOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  ClearOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  InfoCircleOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  SearchOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import type { RunActiveData, MeData } from "../api/types";
import { api } from "../api/client";
import { notify, notifyError } from "../notify";
import { ClusterTable } from "../features/report/ClusterTable";
import {
  asArray,
  asRecord,
  buildClusterNameMap,
  displayClusterName,
  resolveClusterName,
  toNumber,
} from "../utils/report";
import { useLocalStorageState } from "../hooks/useLocalStorageState";
import { ageMs, formatDateTime, formatTime, relativeTime } from "../utils/datetime";

type Severity = "FAIL" | "WARN" | "ERR" | "INFO";
type CompareMode = "all" | "changed" | "flaky";

// Ordered by severity priority: FAIL, then WARN (ahead of ERR — a warning on
// a known check is generally more actionable than an unclassified runtime
// error), then ERR, then INFO. Drives both display order (hero pills, filter
// chips) and the Alerts table's severity sort (see severityRank in
// ClusterTable.tsx, which must be kept in sync with this ordering).
const SEVERITY_META: Array<{ key: Severity; color: string; label: string; icon: ReactNode }> = [
  { key: "FAIL", color: "#f43f5e", label: "FAIL", icon: <ExclamationCircleOutlined /> },
  { key: "WARN", color: "#f59e0b", label: "WARN", icon: <WarningOutlined /> },
  { key: "ERR", color: "#f97316", label: "ERR", icon: <CloseCircleOutlined /> },
  { key: "INFO", color: "#38bdf8", label: "INFO", icon: <InfoCircleOutlined /> },
];

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

export function DashboardPage() {
  const PREVIEW_LIMIT = 1500;
  const queryClient = useQueryClient();
  const [filterText, setFilterText] = useLocalStorageState("dashboard.filterText", "");
  const [severityFilters, setSeverityFilters] = useLocalStorageState<Severity[]>("dashboard.severityFilters", []);
  const [compareMode, setCompareMode] = useLocalStorageState<CompareMode>("dashboard.compareMode", "all");
  const [selectedClusters, setSelectedClusters] = useLocalStorageState<string[]>("dashboard.selectedClusters", []);
  const [loadFullReport, setLoadFullReport] = useState(false);
  const [searchParams, setSearchParams] = useSearchParams();
  const [tableSummary, setTableSummary] = useState<{
    total: number;
    fail: number;
    err: number;
    warn: number;
    info: number;
    unknown: number;
  } | null>(null);

  useEffect(() => {
    const q = (searchParams.get("q") || "").trim();
    const sev = (searchParams.get("sev") || "")
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter((s): s is Severity => ["FAIL", "WARN", "ERR", "INFO"].includes(s));
    const mode = (searchParams.get("mode") || "").trim();
    const clusters = (searchParams.get("clusters") || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (q && q !== filterText) setFilterText(q);
    if (sev.length > 0 && sev.join(",") !== severityFilters.join(",")) setSeverityFilters(sev);
    if (clusters.length > 0 && clusters.join(",") !== selectedClusters.join(",")) setSelectedClusters(clusters);
    if ((mode === "all" || mode === "changed" || mode === "flaky") && mode !== compareMode) {
      setCompareMode(mode);
    }
    // Mount/query driven sync.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  useEffect(() => {
    const next = new URLSearchParams(searchParams);
    if (filterText.trim()) next.set("q", filterText.trim());
    else next.delete("q");
    if (severityFilters.length > 0) next.set("sev", severityFilters.join(","));
    else next.delete("sev");
    if (selectedClusters.length > 0) next.set("clusters", selectedClusters.join(","));
    else next.delete("clusters");
    if (compareMode !== "all") next.set("mode", compareMode);
    else next.delete("mode");
    if (next.toString() !== searchParams.toString()) {
      setSearchParams(next, { replace: true });
    }
  }, [filterText, severityFilters, selectedClusters, compareMode, searchParams, setSearchParams]);

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

  // Reuse the same query key the header trigger button uses so react-query
  // dedupes and we don't double-poll the orchestrator. Lets the empty-state
  // card distinguish "no data yet, but a run is in flight" from "no run has
  // ever produced data".
  const runActiveQuery = useQuery({
    queryKey: ["runs-active"],
    queryFn: api.runActive,
    refetchInterval: (q) => ((q.state.data as RunActiveData | undefined)?.active ? 3000 : 30000),
    staleTime: 1500,
  });
  const meQuery = useQuery({
    queryKey: ["auth", "me"],
    queryFn: api.me,
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
    queryClient.removeQueries({ queryKey: ["report-data", "preview", PREVIEW_LIMIT], exact: true });
  }, [fullReport.data, queryClient]);

  useEffect(() => {
    if (previewReport.error) notifyError(previewReport.error, "Failed to load report preview");
  }, [previewReport.error]);

  useEffect(() => {
    if (fullReport.error) notifyError(fullReport.error, "Failed to load full report");
  }, [fullReport.error]);

  const reportData = fullReport.data ?? previewReport.data ?? {
    run_summary: {},
    checks_snapshot: [],
    drilldown_diff: {},
    flaky_checks: {},
    regression_summary: {},
    slo_dashboard: {},
    policy_violations: [],
  };

  const me = meQuery.data as MeData | undefined;
  const clusterRestricted = me?.cluster_access_unrestricted === false;
  const allowedClustersSet = useMemo(
    () =>
      new Set(
        (me?.allowed_clusters ?? [])
          .map((c) => c.trim().toLowerCase())
          .filter(Boolean),
      ),
    [me?.allowed_clusters],
  );

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

  const isClusterAllowed = (rawCluster: unknown): boolean => {
    if (!clusterRestricted) return true;
    const raw = String(rawCluster ?? "").trim();
    if (!raw) return false;
    const resolved = resolveClusterName(raw, clusterNameMap).trim();
    return allowedClustersSet.has(raw.toLowerCase()) || allowedClustersSet.has(resolved.toLowerCase());
  };

  const filteredAggRows = useMemo(
    () =>
      asArray(reportData.agg_rows)
        .map((r) => asRecord(r))
        .filter((r) => isClusterAllowed(displayClusterName(r))),
    [reportData.agg_rows, clusterRestricted, allowedClustersSet, clusterNameMap],
  );

  const filteredChecksSnapshot = useMemo(() => {
    if (!clusterRestricted) return reportData.checks_snapshot;
    const snapshot = asRecord(reportData.checks_snapshot);
    const legacyClusters = asArray(snapshot.clusters).map((c) => asRecord(c));
    if (legacyClusters.length > 0) {
      return {
        ...snapshot,
        clusters: legacyClusters.filter((c) => isClusterAllowed(displayClusterName(c))),
      };
    }
    return asArray(reportData.checks_snapshot)
      .map((r) => asRecord(r))
      .filter((r) => isClusterAllowed(displayClusterName(r)));
  }, [reportData.checks_snapshot, clusterRestricted, allowedClustersSet, clusterNameMap]);

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
    filteredAggRows
      .map((r) => asRecord(r))
      .forEach((r) => {
        add(displayClusterName(r));
        add(r.cluster);
      });
    asArray(filteredChecksSnapshot)
      .map((r) => asRecord(r))
      .forEach((r) => {
        add(displayClusterName(r));
        add(r.cluster);
      });
    return Array.from(names)
      .sort((a, b) => a.localeCompare(b))
      .map((name) => ({ label: name, value: name }));
  }, [reportData.run_summary, filteredAggRows, filteredChecksSnapshot, clusterNameMap]);

  // ----------------- HERO METRICS -----------------

  const summaryCounts = asRecord(reportData.ncc_summary_counts);
  const totalPlugins = toNumber(summaryCounts.total_plugins);
  const passCount = toNumber(summaryCounts.pass);
  const failCount = toNumber(summaryCounts.fail);
  const errorCount = toNumber(summaryCounts.error);
  const infoCount = toNumber(summaryCounts.info);
  const unknownCount = toNumber(summaryCounts.unknown);
  const warnCount = toNumber(summaryCounts.warn);
  const rawPassRate = totalPlugins > 0 ? (passCount / totalPlugins) * 100 : 0;
  const weightedPenalty =
    (totalPlugins > 0 ? (failCount / totalPlugins) * 100 : 0) * 8.0 +
    (totalPlugins > 0 ? (errorCount / totalPlugins) * 100 : 0) * 5.5 +
    (totalPlugins > 0 ? (warnCount / totalPlugins) * 100 : 0) * 3.5 +
    (totalPlugins > 0 ? (infoCount / totalPlugins) * 100 : 0) * 2.2 +
    (totalPlugins > 0 ? (unknownCount / totalPlugins) * 100 : 0) * 3.0;
  const weightedHealth = Math.max(0, Math.min(100, rawPassRate - weightedPenalty));

  const runSummary = asRecord(reportData.run_summary);
  const runTimestamp = String(runSummary.timestamp || "");
  const runExitCode = toNumber(runSummary.exit_code);
  const runTotalChecks = toNumber(runSummary.total_checks);
  const runSource = String(runSummary.source || runSummary.run_source || "").trim();
  const latestRunError = String(runActiveQuery.data?.last_error || "").trim();
  const fresh = freshnessTag(runTimestamp);
  const clustersOk = toNumber(runSummary.clusters_ok);
  const clustersFailed = toNumber(runSummary.clusters_failed);

  const regression = asRecord(reportData.regression_summary);
  const hasRegression = Boolean(regression.has_regression);
  const deltaFail = toNumber(regression.delta_fail_total);
  const policyViolations = Array.isArray(reportData.policy_violations) ? reportData.policy_violations : [];

  // Severity totals fall back to agg_rows if NCC summary missing (e.g. some setups).
  const aggRows = filteredAggRows.map((r) => asRecord(r));
  // checks_snapshot can be either:
  // 1) a flat row array, or
  // 2) legacy/object form {clusters:[{checks:[...]}]}.
  // Treat either shape as "has alert data" so we don't show a false empty state.
  const hasChecksSnapshotData = useMemo(() => {
    const flat = asArray(filteredChecksSnapshot);
    if (flat.length > 0) return true;
    const snap = asRecord(filteredChecksSnapshot);
    const clusters = asArray(snap.clusters).map((c) => asRecord(c));
    for (const c of clusters) {
      if (asArray(c.checks).length > 0) return true;
    }
    return false;
  }, [filteredChecksSnapshot]);
  const aggFail = aggRows.filter((r) => String(r.severity || "").toUpperCase() === "FAIL").length;
  const aggErr = aggRows.filter((r) => String(r.severity || "").toUpperCase() === "ERR").length;
  const aggWarn = aggRows.filter((r) => String(r.severity || "").toUpperCase() === "WARN").length;
  const aggInfo = aggRows.filter((r) => String(r.severity || "").toUpperCase() === "INFO").length;
  const displayedSevTotals: Record<Severity, number> = {
    FAIL: tableSummary?.fail ?? aggFail,
    ERR: tableSummary?.err ?? aggErr,
    WARN: tableSummary?.warn ?? aggWarn,
    INFO: tableSummary?.info ?? aggInfo,
  };

  const refreshAll = async () => {
    await Promise.all([previewReport.refetch(), loadFullReport ? fullReport.refetch() : Promise.resolve()]);
    notify.success("Dashboard refreshed.");
  };

  const toggleSeverity = (severity: Severity) => {
    setSeverityFilters((prev) => (prev.includes(severity) ? prev.filter((s) => s !== severity) : [...prev, severity]));
  };

  const clearFilters = () => {
    setFilterText("");
    setSeverityFilters([]);
    setSelectedClusters([]);
    setCompareMode("all");
  };

  const filtersActive =
    filterText.trim().length > 0 ||
    severityFilters.length > 0 ||
    selectedClusters.length > 0 ||
    compareMode !== "all";

  const initialLoading = previewReport.isLoading && !previewReport.data && !fullReport.data;

  if (initialLoading) {
    // The loading state mirrors the *exact* structure of the loaded dashboard
    // (hero card with the four stats + severity strip, then the filter-toolbar
    // card, then the table) rather than a generic stack of skeletons. This
    // keeps the LCP element (the "Operations Dashboard" <h4>) painting on first
    // render and — critically — holds the above-the-fold layout at a stable
    // height so the loading→loaded transition causes almost no layout shift
    // (lower CLS). Numbers render as "—" placeholders that swap to real values
    // in place without resizing their rows.
    return (
      <Space orientation="vertical" size={16} style={{ width: "100%" }}>
        {/* HERO STRIP */}
        <Card className="page-card dashboard-hero">
          <Row gutter={[12, 12]} align="middle">
            <Col xs={24} md={6}>
              <Space size={12} align="center">
                <div
                  className="health-pill"
                  style={{ background: "linear-gradient(135deg, #3f3f46, #27272ab0)" }}
                >
                  <div style={{ fontSize: 10, opacity: 0.9, letterSpacing: 1 }}>HEALTH</div>
                  <div style={{ fontSize: 22, fontWeight: 700, lineHeight: 1 }}>—</div>
                </div>
                <div>
                  <Typography.Title level={4} style={{ margin: 0 }}>
                    Operations Dashboard
                  </Typography.Title>
                  <Tag icon={<ClockCircleOutlined />} style={{ marginTop: 6 }}>
                    Loading…
                  </Tag>
                </div>
              </Space>
            </Col>
            <Col xs={24} md={12}>
              <Row gutter={[8, 8]}>
                {["Clusters OK", "Failed", "Total Plugins", "Pass"].map((t) => (
                  <Col xs={12} md={6} key={t}>
                    <Statistic title={t} value="—" />
                  </Col>
                ))}
              </Row>
            </Col>
            <Col xs={24} md={6}>
              <Space size={8} style={{ display: "flex", justifyContent: "flex-end" }} wrap>
                <Button icon={<ReloadOutlined />} disabled>
                  Refresh
                </Button>
                <Button type="primary" icon={<ArrowRightOutlined />} iconPosition="end" disabled>
                  Insights
                </Button>
              </Space>
            </Col>
          </Row>
          <div className="severity-totals-row">
            {SEVERITY_META.map((sm) => (
              <div key={sm.key} className="severity-total-pill" style={{ borderColor: sm.color }}>
                <Badge color={sm.color} />
                <Typography.Text strong style={{ color: sm.color }}>
                  {sm.label}
                </Typography.Text>
                <Typography.Text strong>—</Typography.Text>
              </div>
            ))}
            <div className="severity-total-pill subtle">
              <Typography.Text type="secondary">UNKNOWN</Typography.Text>
              <Typography.Text strong>—</Typography.Text>
            </div>
          </div>
        </Card>

        {/* FILTER TOOLBAR */}
        <Card className="page-card filter-toolbar-card">
          <Typography.Title level={5} className="section-title" style={{ marginBottom: 8 }}>
            Alert Filters
          </Typography.Title>
          <Skeleton.Input active block style={{ height: 32 }} />
          <Space size={[6, 6]} wrap style={{ marginTop: 12 }}>
            <Skeleton.Button active size="small" />
            <Skeleton.Button active size="small" />
            <Skeleton.Button active size="small" />
            <Skeleton.Button active size="small" />
          </Space>
        </Card>

        {/* MAIN TABLE */}
        <Card className="page-card">
          <Skeleton active paragraph={{ rows: 8 }} />
        </Card>
      </Space>
    );
  }

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {/* HERO STRIP */}
      <Card className="page-card dashboard-hero">
        <Row gutter={[12, 12]} align="middle">
          <Col xs={24} md={6}>
            <Space size={12} align="center">
              <div
                className="health-pill"
                style={{
                  background: `linear-gradient(135deg, ${healthGradeColor(weightedHealth)}, ${healthGradeColor(weightedHealth)}b0)`,
                }}
              >
                <div style={{ fontSize: 10, opacity: 0.9, letterSpacing: 1 }}>HEALTH</div>
                <div style={{ fontSize: 22, fontWeight: 700, lineHeight: 1 }}>{weightedHealth.toFixed(1)}%</div>
              </div>
              <div>
                <Typography.Title level={4} style={{ margin: 0 }}>
                  Operations Dashboard
                </Typography.Title>
                <Tooltip title={runTimestamp || "no timestamp"}>
                  <Tag icon={<ClockCircleOutlined />} color={fresh.color} style={{ marginTop: 6 }}>
                    {fresh.label} · {relativeTime(runTimestamp)}
                  </Tag>
                </Tooltip>
              </div>
            </Space>
          </Col>
          <Col xs={24} md={12}>
            <Row gutter={[8, 8]}>
              <Col xs={12} md={6}>
                <Statistic
                  title="Clusters OK"
                  value={clustersOk}
                  prefix={<CheckCircleOutlined style={{ color: "#22c55e" }} />}
                />
              </Col>
              <Col xs={12} md={6}>
                <Statistic
                  title="Failed"
                  value={clustersFailed}
                  valueStyle={{ color: clustersFailed > 0 ? "#f43f5e" : undefined }}
                />
              </Col>
              <Col xs={12} md={6}>
                <Statistic title="Total Plugins" value={totalPlugins} />
              </Col>
              <Col xs={12} md={6}>
                <Statistic title="Pass" value={passCount} valueStyle={{ color: "#22c55e" }} />
              </Col>
            </Row>
          </Col>
          <Col xs={24} md={6}>
            <Space size={8} style={{ display: "flex", justifyContent: "flex-end" }} wrap>
              <Button
                icon={<ReloadOutlined />}
                onClick={refreshAll}
                loading={previewReport.isFetching || fullReport.isFetching}
              >
                Refresh
              </Button>
              <Link to="/insights">
                <Button type="primary" icon={<ArrowRightOutlined />} iconPosition="end">
                  Insights
                </Button>
              </Link>
            </Space>
          </Col>
        </Row>

        {/* Severity totals strip */}
        {clusterRestricted ? (
          <Alert
            type="info"
            showIcon
            style={{ marginTop: 12 }}
            title="Cluster-group view active"
            description="Severity totals and alert table are scoped to your allowed cluster groups."
          />
        ) : null}
        <div className="severity-totals-row">
          {SEVERITY_META.map((sm) => (
            <div key={sm.key} className="severity-total-pill" style={{ borderColor: sm.color }}>
              <Badge color={sm.color} />
              <Typography.Text strong style={{ color: sm.color }}>
                {sm.label}
              </Typography.Text>
              <Typography.Text strong>{displayedSevTotals[sm.key].toLocaleString()}</Typography.Text>
            </div>
          ))}
          <div className="severity-total-pill subtle">
            <Typography.Text type="secondary">UNKNOWN</Typography.Text>
            <Typography.Text strong>{(tableSummary?.unknown ?? unknownCount).toLocaleString()}</Typography.Text>
          </div>
        </div>
      </Card>

      {/* CONTEXTUAL BANNERS */}
      {hasRegression ? (
        <Alert
          type="error"
          showIcon
          title="Regression detected vs previous run"
          description={`FAIL count increased by ${deltaFail}. Check Insights → Run-over-Run Comparison for details.`}
          action={
            <Link to="/insights">
              <Button size="small" type="primary">
                Open Insights
              </Button>
            </Link>
          }
        />
      ) : null}

      {policyViolations.length > 0 ? (
        <Alert
          type="warning"
          showIcon
          title={`${policyViolations.length} policy gate violation${policyViolations.length > 1 ? "s" : ""}`}
          description={
            <ul style={{ margin: "4px 0 0 18px", padding: 0 }}>
              {policyViolations.slice(0, 5).map((v, i) => (
                <li key={i}>{v}</li>
              ))}
              {policyViolations.length > 5 ? <li>+{policyViolations.length - 5} more…</li> : null}
            </ul>
          }
        />
      ) : null}

      {previewReport.data && !fullReport.data && loadFullReport && (
        <Alert
          type="info"
          showIcon
          title="Loading complete dataset in background"
          description="A fast preview is shown first. Full report rows are being fetched."
        />
      )}

      {/* FILTER TOOLBAR */}
      <Card className="page-card filter-toolbar-card">
        <Typography.Title level={5} className="section-title" style={{ marginBottom: 8 }}>
          Alert Filters
        </Typography.Title>
        <Row gutter={[12, 12]} align="middle">
          <Col xs={24} md={12} lg={10}>
            <Tooltip
              title={
                <div style={{ fontSize: 12 }}>
                  Tokens: <code>sev:FAIL</code> <code>cluster:10.1</code> <code>changed:true</code>{" "}
                  <code>flaky:true</code>
                </div>
              }
            >
              <Input
                id="alerts-search"
                name="alerts-search"
                aria-label="Search alerts"
                allowClear
                prefix={<SearchOutlined />}
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                placeholder="Search alerts… try sev:FAIL, cluster:10.1, changed:true"
                autoComplete="off"
              />
            </Tooltip>
          </Col>
          <Col xs={24} md={12} lg={8}>
            <Select
              id="dashboard-cluster-filter"
              aria-label="Filter alerts by cluster"
              mode="multiple"
              allowClear
              placeholder="Filter by cluster"
              maxTagCount="responsive"
              style={{ width: "100%" }}
              value={selectedClusters}
              onChange={(values) => setSelectedClusters(values)}
              options={clusterOptions}
            />
          </Col>
          <Col xs={24} md={12} lg={4}>
            <Select
              id="dashboard-compare-mode"
              aria-label="Compare mode"
              style={{ width: "100%" }}
              value={compareMode}
              onChange={(value) => setCompareMode(value as CompareMode)}
              options={[
                { value: "all", label: "All rows" },
                { value: "changed", label: "Changed only" },
                { value: "flaky", label: "Flaky only" },
              ]}
            />
          </Col>
          <Col xs={24} md={12} lg={2}>
            <Button
              icon={<ClearOutlined />}
              disabled={!filtersActive}
              onClick={clearFilters}
              style={{ width: "100%" }}
            >
              Clear
            </Button>
          </Col>
        </Row>
        <Space size={[6, 6]} wrap style={{ marginTop: 12 }}>
          <Tag.CheckableTag
            checked={severityFilters.length === 0}
            onChange={(checked) => checked && setSeverityFilters([])}
          >
            ALL
          </Tag.CheckableTag>
          {SEVERITY_META.map((sm) => (
            <Tag.CheckableTag
              key={sm.key}
              checked={severityFilters.includes(sm.key)}
              onChange={() => toggleSeverity(sm.key)}
            >
              <Space size={4}>
                {sm.icon}
                {sm.label}
              </Space>
            </Tag.CheckableTag>
          ))}
        </Space>
      </Card>

      {/* MAIN ALERTS TABLE */}
      {aggRows.length === 0 && !hasChecksSnapshotData ? (
        <Card className="page-card">
          {(() => {
            // Empty-state copy is contextual:
            //   1) A run is currently in progress → tell the user to wait,
            //      not "trigger another run". Show a spinning indicator that
            //      mirrors the header pill.
            //   2) A previous run completed cleanly → no alerts is good news.
            //   3) No prior run on disk → onboarding hint.
            const runActive = runActiveQuery.data?.active === true;
            const hasPriorRun = Boolean(runTimestamp);
            const priorRunWasSuccessful =
              hasPriorRun && (runSummary.success === true || (failCount === 0 && errorCount === 0 && warnCount === 0));

            if (runActive) {
              const startedAt = runActiveQuery.data?.started_at
                ? formatTime(runActiveQuery.data.started_at)
                : "moments ago";
              return (
                <Empty
                  image={<PlayCircleOutlined spin style={{ fontSize: 48, color: "var(--ant-color-primary, #1677ff)" }} />}
                  imageStyle={{ height: 56 }}
                  description={
                    <Space orientation="vertical" size={4} align="center">
                      <Typography.Text strong>Run in progress · alerts will populate when it completes</Typography.Text>
                      <Typography.Text type="secondary">
                        Started {startedAt}. The dashboard will refresh automatically — there's nothing to do.
                      </Typography.Text>
                      <Link to="/settings?tab=runs">
                        <Button size="small" type="link">
                          View live output →
                        </Button>
                      </Link>
                    </Space>
                  }
                />
              );
            }

            if (priorRunWasSuccessful) {
              return (
                <Empty
                  image={<CheckCircleOutlined style={{ fontSize: 48, color: "#22c55e" }} />}
                  imageStyle={{ height: 56 }}
                  description={
                    <Space orientation="vertical" size={4} align="center">
                      <Typography.Text strong>All clusters clean — no alerts in the latest run</Typography.Text>
                      <Typography.Text type="secondary">
                        {hasPriorRun
                          ? `Last run finished ${formatDateTime(runTimestamp)} with no findings.`
                          : "The most recent run found no failures, errors, or warnings."}
                      </Typography.Text>
                    </Space>
                  }
                />
              );
            }

            // Either no run yet, or last run produced an error before any
            // alerts were collected (e.g. orchestrator failed pre-flight).
            const sourceLabel =
              runSource === "scheduled" ? "scheduled run" : runSource === "manual" ? "manual run" : "latest run";
            const likelyEarlyFailure =
              runSummary.success === false ||
              runExitCode > 0 ||
              clustersFailed > 0 ||
              runTotalChecks === 0;
            const artifactGap = runTotalChecks > 0 && clustersOk + clustersFailed > 0;
            const summaryHint = runExitCode > 0 ? `Exit code ${runExitCode}.` : "";
            const errorHint = latestRunError ? ` Last runtime error: ${latestRunError}.` : "";
            return (
              <Empty
                description={
                  <Space orientation="vertical" size={4} align="center">
                    <Typography.Text strong>
                      {hasPriorRun ? "Latest run produced no alerts data" : "No alerts yet"}
                    </Typography.Text>
                    <Typography.Text type="secondary">
                      {hasPriorRun
                        ? likelyEarlyFailure
                          ? `${sourceLabel[0].toUpperCase()}${sourceLabel.slice(1)} appears to have finished before per-check findings were generated. ${summaryHint}${errorHint} Re-run from Settings → Runs and check live output for the first failing stage.`
                          : artifactGap
                            ? `The ${sourceLabel} summary exists, but per-check artifacts are missing. This usually indicates incomplete report artifact generation or cleanup. Open Settings → Runs, inspect the latest run artifacts/logs, then re-run.`
                            : `The ${sourceLabel} completed but did not emit per-check findings. Re-run from Settings → Runs with full output enabled.`
                        : "Trigger a run from Settings → Runs to populate this view."}
                    </Typography.Text>
                    {hasPriorRun ? (
                      <Link to="/settings?tab=runs">
                        <Button size="small" type="link">
                          Open Runs →
                        </Button>
                      </Link>
                    ) : null}
                  </Space>
                }
              />
            );
          })()}
        </Card>
      ) : (
        <ClusterTable
          checksSnapshot={filteredChecksSnapshot}
          aggRows={filteredAggRows}
          diffFlags={(reportData.diff_flags || {}) as Record<string, unknown>}
          flakyKeys={(reportData.flaky_keys || {}) as Record<string, unknown>}
          nccLogs={Array.isArray(reportData.ncc_logs) ? reportData.ncc_logs : []}
          filterText={filterText}
          selectedClusters={selectedClusters}
          clusterNameMap={clusterNameMap}
          severityFilters={severityFilters}
          compareMode={compareMode}
          onSummaryChange={setTableSummary}
        />
      )}
    </Space>
  );
}
