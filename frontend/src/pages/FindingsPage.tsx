import type { ReactNode } from "react";
import { useRunsActive } from "../hooks/useRunsActive";
import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { sectionHref } from "../navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  Badge,
  Button,
  Card,
  Empty,
  Input,
  Select,
  Segmented,
  Skeleton,
  Space,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  ClearOutlined,
  CloseCircleOutlined,
  CopyOutlined,
  ExclamationCircleOutlined,
  InfoCircleOutlined,
  LoadingOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  SearchOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import type { AlertSource, MeData } from "../api/types";
import { api } from "../api/client";
import { NCC_ALERTS_QUERY_KEY } from "../utils/alertLifecycle";
import { notify, notifyError } from "../notify";
import { FeatureDisabled } from "../features/features/FeatureDisabled";
import { useFeatureFlags } from "../features/features/useFeatureFlags";
import { ClusterTable } from "../features/report/ClusterTable";
import {
  asArray,
  asRecord,
  buildClusterNameMap,
  displayClusterName,
  mergePCClusterIdentityMap,
  resolveClusterName,
  toNumber,
} from "../utils/report";
import { useLocalStorageState } from "../hooks/useLocalStorageState";
import { formatDateTime, formatTime, relativeTime } from "../utils/datetime";
import { freshness } from "../utils/freshness";

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


/**
 * Findings: every NCC (and, in beta, Prism Central) alert from the latest run,
 * with filters kept in the URL, acknowledge/resolve, and per-row detail. The
 * summary of the run itself lives on the Overview.
 */
export function FindingsPage() {
  const PREVIEW_LIMIT = 1500;
  const queryClient = useQueryClient();
  const [filterText, setFilterText] = useLocalStorageState("dashboard.filterText", "");
  const [severityFilters, setSeverityFilters] = useLocalStorageState<Severity[]>("dashboard.severityFilters", []);
  const [compareMode, setCompareMode] = useLocalStorageState<CompareMode>("dashboard.compareMode", "all");
  const [selectedClusters, setSelectedClusters] = useLocalStorageState<string[]>("dashboard.selectedClusters", []);
  const [alertSource, setAlertSource] = useState<AlertSource>("NCC");
  const [pcResolvedFilter, setPcResolvedFilter] = useState<"all" | "No" | "Yes">("No");
  const [loadFullReport, setLoadFullReport] = useState(false);
  const [searchParams, setSearchParams] = useSearchParams();
  const [filtersReady, setFiltersReady] = useState(false);
  const [tableSummary, setTableSummary] = useState<{
    total: number;
    fail: number;
    err: number;
    warn: number;
    info: number;
    unknown: number;
  } | null>(null);

  useEffect(() => {
    const hasShare = ["q", "sev", "clusters", "mode", "source", "resolved"].some((k) => searchParams.has(k));
    if (hasShare) {
      setFilterText(searchParams.get("q") || "");
      const sev = (searchParams.get("sev") || "")
        .split(",")
        .map((s) => s.trim().toUpperCase())
        .filter((s): s is Severity => ["FAIL", "WARN", "ERR", "INFO"].includes(s));
      setSeverityFilters(sev);
      const clusters = (searchParams.get("clusters") || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      setSelectedClusters(clusters);
      const mode = (searchParams.get("mode") || "").trim();
      if (mode === "all" || mode === "changed" || mode === "flaky") setCompareMode(mode);
      const source = (searchParams.get("source") || "").trim().toUpperCase();
      if (source === "NCC" || source === "PC") setAlertSource(source);
      const resolved = (searchParams.get("resolved") || "").trim();
      if (resolved === "No" || resolved === "Yes" || resolved === "all") setPcResolvedFilter(resolved);
    }
    setFiltersReady(true);
    // URL is the source of truth for a shared/bookmarked dashboard.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!filtersReady) return;
    const next = new URLSearchParams(searchParams);
    next.set("source", alertSource.toLowerCase());
    if (filterText.trim()) next.set("q", filterText.trim());
    else next.delete("q");
    if (severityFilters.length > 0) next.set("sev", severityFilters.join(","));
    else next.delete("sev");
    if (selectedClusters.length > 0) next.set("clusters", selectedClusters.join(","));
    else next.delete("clusters");
    if (compareMode !== "all") next.set("mode", compareMode);
    else next.delete("mode");
    if (alertSource === "PC" && pcResolvedFilter !== "No") next.set("resolved", pcResolvedFilter);
    else next.delete("resolved");
    if (next.toString() !== searchParams.toString()) {
      setSearchParams(next, { replace: true });
    }
  }, [
    filtersReady,
    filterText,
    severityFilters,
    selectedClusters,
    compareMode,
    alertSource,
    pcResolvedFilter,
    searchParams,
    setSearchParams,
  ]);

  const copyDashboardLink = async () => {
    const u = new URL(window.location.href);
    u.pathname = "/findings";
    u.hash = "";
    const next = new URLSearchParams();
    next.set("source", alertSource.toLowerCase());
    if (filterText.trim()) next.set("q", filterText.trim());
    if (severityFilters.length > 0) next.set("sev", severityFilters.join(","));
    if (selectedClusters.length > 0) next.set("clusters", selectedClusters.join(","));
    if (compareMode !== "all") next.set("mode", compareMode);
    if (alertSource === "PC" && pcResolvedFilter !== "No") next.set("resolved", pcResolvedFilter);
    u.search = next.toString();
    const link = u.toString();
    try {
      await navigator.clipboard.writeText(link);
      notify.success("Link to these findings copied");
    } catch {
      notify.info({ message: "Copy this link", description: link, duration: 8 });
    }
  };

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

  const nccDispositionsQuery = useQuery({
    queryKey: ["ncc-dispositions"],
    queryFn: api.nccDispositions,
    staleTime: 15_000,
  });
  const setNCCDisposition = useMutation({
    mutationFn: api.setNCCDisposition,
    onSuccess: async (_data, variables) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["ncc-dispositions"] }),
        queryClient.invalidateQueries({ queryKey: [NCC_ALERTS_QUERY_KEY] }),
      ]);
      const count = variables.items?.length ?? 1;
      const noun = count === 1 ? "alert" : `${count} alerts`;
      if (variables.action === "resolve") {
        notify.success(`Resolved ${noun}. A later run that generates the same check clears the mark.`);
      } else if (variables.action === "acknowledge") {
        notify.success(`Acknowledged ${noun}. It stays open until you resolve it.`);
      } else {
        notify.success(`Reopened ${noun}.`);
      }
    },
    onError: (err) => notifyError(err, "Could not update the NCC alert"),
  });

  const features = useFeatureFlags();
  const pcAlertsOn = !features.isSuccess || features.data.pc_alerts !== false;
  const pcBlocked = alertSource === "PC" && features.data?.pc_alerts === false;
  const pcUnresolvedAlertsQuery = useQuery({
    queryKey: ["pc-alerts", "No"],
    queryFn: () => api.alerts(false, "No"),
    enabled: alertSource === "PC" && features.isFetched && pcAlertsOn,
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
  const pcAllAlertsQuery = useQuery({
    queryKey: ["pc-alerts", "all"],
    queryFn: () => api.alerts(false, "all"),
    enabled: alertSource === "PC" && features.isFetched && pcAlertsOn,
    staleTime: 5 * 60_000,
    refetchInterval: false,
  });

  // Reuse the same query key the header trigger button uses so react-query
  // dedupes and we don't double-poll the orchestrator. Lets the empty-state
  // card distinguish "no data yet, but a run is in flight" from "no run has
  // ever produced data".
  const runActiveQuery = useRunsActive();
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
    if (previewReport.error) notifyError(previewReport.error, "Could not load the latest findings");
  }, [previewReport.error]);

  useEffect(() => {
    if (fullReport.error) notifyError(fullReport.error, "Could not load all findings");
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
      mergePCClusterIdentityMap(
        buildClusterNameMap({
          runSummary: reportData.run_summary,
          checksSnapshot: reportData.checks_snapshot,
          aggRows: Array.isArray(reportData.agg_rows) ? reportData.agg_rows : [],
          drilldownDiff: reportData.drilldown_diff,
          flakyChecks: reportData.flaky_checks,
          sloDashboard: reportData.slo_dashboard,
          regressionSummary: reportData.regression_summary,
        }),
        (pcResolvedFilter === "No"
          ? pcUnresolvedAlertsQuery.data?.cluster_map
          : pcAllAlertsQuery.data?.cluster_map) as
          | Record<string, { name?: string; address?: string; ext_id?: string }>
          | undefined,
      ),
    [
      reportData.run_summary,
      reportData.checks_snapshot,
      reportData.agg_rows,
      reportData.drilldown_diff,
      reportData.flaky_checks,
      reportData.slo_dashboard,
      reportData.regression_summary,
      pcResolvedFilter,
      pcUnresolvedAlertsQuery.data?.cluster_map,
      pcAllAlertsQuery.data?.cluster_map,
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

  // ----------------- RUN SUMMARY -----------------

  const summaryCounts = asRecord(reportData.ncc_summary_counts);
  const failCount = toNumber(summaryCounts.fail);
  const errorCount = toNumber(summaryCounts.error);
  const unknownCount = toNumber(summaryCounts.unknown);
  const warnCount = toNumber(summaryCounts.warn);

  const runSummary = asRecord(reportData.run_summary);
  const runTimestamp = String(runSummary.timestamp || "");
  const runExitCode = toNumber(runSummary.exit_code);
  const runTotalChecks = toNumber(runSummary.total_checks);
  const runSource = String(runSummary.source || runSummary.run_source || "").trim();
  const latestRunError = String(runActiveQuery.data?.last_error || "").trim();
  const fresh = freshness(runTimestamp);
  const clustersOk = toNumber(runSummary.clusters_ok);
  const clustersFailed = toNumber(runSummary.clusters_failed);

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
  // The pills double as severity filters, so they count the whole run, not
  // the filtered table (which would show 0 for every severity not selected).
  // The table's own counts are the fallback for PC alerts and older runs
  // without agg rows.
  const useRunTotals = alertSource === "NCC" && aggRows.length > 0;
  const displayedSevTotals: Record<Severity, number> = useRunTotals
    ? { FAIL: aggFail, ERR: aggErr, WARN: aggWarn, INFO: aggInfo }
    : {
        FAIL: tableSummary?.fail ?? aggFail,
        ERR: tableSummary?.err ?? aggErr,
        WARN: tableSummary?.warn ?? aggWarn,
        INFO: tableSummary?.info ?? aggInfo,
      };

  const refreshAll = async () => {
    await Promise.all([
      previewReport.refetch(),
      loadFullReport ? fullReport.refetch() : Promise.resolve(),
      queryClient.invalidateQueries({ queryKey: ["ncc-dispositions"] }),
      queryClient.invalidateQueries({ queryKey: [NCC_ALERTS_QUERY_KEY] }),
      alertSource === "PC"
        ? Promise.all([
            api.alerts(true, "No").then((data) => {
              queryClient.setQueryData(["pc-alerts", "No"], data);
            }),
            api.alerts(true, "all").then((data) => {
              queryClient.setQueryData(["pc-alerts", "all"], data);
            }),
          ])
        : Promise.resolve(),
    ]);
    notify.success("Findings refreshed.");
  };

  const toggleSeverity = (severity: Severity) => {
    setSeverityFilters((prev) => (prev.includes(severity) ? prev.filter((s) => s !== severity) : [...prev, severity]));
  };

  const clearFilters = () => {
    setFilterText("");
    setSeverityFilters([]);
    setSelectedClusters([]);
    setCompareMode("all");
    setAlertSource("NCC");
    setPcResolvedFilter("No");
  };

  const filtersActive =
    filterText.trim().length > 0 ||
    severityFilters.length > 0 ||
    selectedClusters.length > 0 ||
    compareMode !== "all" ||
    alertSource !== "NCC";

  const initialLoading = previewReport.isLoading && !previewReport.data && !fullReport.data;

  if (initialLoading) {
    // Mirrors the loaded layout (summary bar, filters, table) so the
    // loading→loaded swap does not shift the page.
    return (
      <div className="dashboard-page">
        <div className="findings-summary">
          <Tag icon={<ClockCircleOutlined />}>Loading…</Tag>
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
          </div>
          <Button icon={<ReloadOutlined />} disabled className="findings-summary-refresh">
            Refresh
          </Button>
        </div>

        {/* FILTER TOOLBAR */}
        <Card className="page-card filter-toolbar-card">
          <Skeleton.Input active block />
        </Card>

        {/* MAIN TABLE */}
        <Card className="page-card alerts-card">
          <Skeleton active paragraph={{ rows: 8 }} />
        </Card>
      </div>
    );
  }

  return (
    <div className="dashboard-page">
      {/* Which run these findings come from, and how many of each severity. */}
      <div className="findings-summary">
        <Tooltip title={runTimestamp ? formatDateTime(runTimestamp) : "No completed run yet"}>
          <Tag icon={<ClockCircleOutlined />} color={fresh.color}>
            {runTimestamp ? `${fresh.label} · run ${relativeTime(runTimestamp)}` : "No run yet"}
          </Tag>
        </Tooltip>
        <div className="severity-totals-row">
          {SEVERITY_META.map((sm) => (
            <button
              type="button"
              key={sm.key}
              className={`severity-total-pill${severityFilters.includes(sm.key) ? " is-active" : ""}`}
              style={{ borderColor: sm.color }}
              aria-pressed={severityFilters.includes(sm.key)}
              aria-label={`${sm.label}: ${displayedSevTotals[sm.key]}. Filter by ${sm.label}.`}
              onClick={() => toggleSeverity(sm.key)}
            >
              <Badge color={sm.color} />
              <Typography.Text strong style={{ color: sm.color }}>
                {sm.label}
              </Typography.Text>
              <Typography.Text strong>{displayedSevTotals[sm.key].toLocaleString()}</Typography.Text>
            </button>
          ))}
          <div className="severity-total-pill subtle">
            <Typography.Text type="secondary">UNKNOWN</Typography.Text>
            <Typography.Text strong>{(tableSummary?.unknown ?? unknownCount).toLocaleString()}</Typography.Text>
          </div>
        </div>
        <Button
          icon={<ReloadOutlined />}
          className="findings-summary-refresh"
          onClick={refreshAll}
          loading={previewReport.isFetching || fullReport.isFetching}
        >
          Refresh
        </Button>
      </div>

      {clusterRestricted ? (
        <Alert
          type="info"
          showIcon
          title="Cluster-group view active"
          description="Severity totals and findings are scoped to your allowed cluster groups."
        />
      ) : null}

      {previewReport.data && !fullReport.data && loadFullReport && (
        <Alert
          type="info"
          showIcon
          title="Loading the rest of this run"
          description="A preview is shown while the full results load."
        />
      )}

      {/* FILTER TOOLBAR */}
      {/* One row of filters; severity is filtered with the totals above. */}
      <Card className="page-card filter-toolbar-card">
        <div className="findings-toolbar">
          <Tooltip title="Prism Central alerts are in beta and can be turned off in Settings → Features.">
            <Segmented
              aria-label="Alert source"
              value={alertSource}
              onChange={(value) => setAlertSource(value as AlertSource)}
              options={[
                { label: "NCC", value: "NCC" },
                { label: "PC · Beta", value: "PC" },
              ]}
            />
          </Tooltip>
          <div className="findings-toolbar-search">
            <Input
              id="alerts-search"
              name="alerts-search"
              aria-label="Search alerts"
              allowClear
              prefix={<SearchOutlined className="filter-control-icon" />}
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              placeholder="Search alerts (e.g. cluster:10.1)"
              autoComplete="off"
              suffix={
                <Tooltip title={<div style={{ fontSize: 12 }}><strong>Supported tokens:</strong><br /><code>sev:FAIL</code> <code>cluster:10.1</code> <code>changed:true</code> <code>flaky:true</code></div>}>
                  <InfoCircleOutlined className="filter-control-icon" style={{ cursor: "pointer" }} />
                </Tooltip>
              }
            />
          </div>
          <div className="findings-toolbar-clusters">
            <Select
              id="dashboard-cluster-filter"
              aria-label="Filter alerts by cluster"
              mode="multiple"
              allowClear
              placeholder="All Clusters"
              maxTagCount="responsive"
              style={{ width: "100%" }}
              value={selectedClusters}
              onChange={(values) => setSelectedClusters(values)}
              options={clusterOptions}
            />
          </div>
          {alertSource === "PC" && !pcBlocked ? (
            <Select
              aria-label="PC resolved filter"
              className="findings-toolbar-select"
              value={pcResolvedFilter}
              onChange={(value) => setPcResolvedFilter(value as "all" | "No" | "Yes")}
              options={[
                { value: "No", label: "Unresolved" },
                { value: "Yes", label: "Resolved" },
                { value: "all", label: "All statuses" },
              ]}
            />
          ) : (
            <Select
              id="dashboard-compare-mode"
              aria-label="Compare mode"
              className="findings-toolbar-select"
              value={compareMode}
              onChange={(value) => setCompareMode(value as CompareMode)}
              options={[
                { value: "all", label: "All rows" },
                { value: "changed", label: "Changed only" },
                { value: "flaky", label: "Flaky only" },
              ]}
            />
          )}
          <div className="findings-toolbar-actions">
            {filtersActive ? (
              <Button type="text" icon={<ClearOutlined />} onClick={clearFilters}>
                Reset
              </Button>
            ) : null}
            <Button type="text" icon={<CopyOutlined />} onClick={() => void copyDashboardLink()}>
              Copy link
            </Button>
          </div>
        </div>
      </Card>

      {alertSource === "PC" && !pcBlocked && pcAllAlertsQuery.isFetching ? (
        <Alert
          type="info"
          showIcon
          icon={<LoadingOutlined spin />}
          message="Loading Prism Central alerts…"
          description="Unresolved alerts are available now. Resolved and all-status results are loading in the background."
        />
      ) : null}

      {alertSource === "PC" && !pcBlocked && (pcUnresolvedAlertsQuery.error || pcAllAlertsQuery.error) ? (
        <Alert
          type="warning"
          showIcon
          message="Prism Central alerts unavailable"
          description="NCC alerts remain available. Check the configured Prism Central URL, credentials, and TLS settings."
        />
      ) : null}

      {/* MAIN ALERTS TABLE */}
      {pcBlocked ? (
        <FeatureDisabled feature="pc_alerts" />
      ) : aggRows.length === 0 &&
      !hasChecksSnapshotData &&
      ((pcResolvedFilter === "No"
        ? pcUnresolvedAlertsQuery.data?.alerts?.length
        : pcAllAlertsQuery.data?.alerts?.length) ?? 0) === 0 ? (
        <Card className="page-card alerts-card">
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
                      <Link to={sectionHref("runs")}>
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
                          ? `The ${sourceLabel} finished before any check results were saved. ${summaryHint}${errorHint} Start it again from Runs and review the live output.`
                          : artifactGap
                            ? `The ${sourceLabel} summary is available, but the check results are missing. Open Runs, review the latest run, then start it again.`
                            : `The ${sourceLabel} completed without check results. Start it again from Runs.`
                        : "Start a run from Runs. Add clusters or Prism Central under Clusters & config first."}
                    </Typography.Text>
                    {hasPriorRun ? (
                      <Link to={sectionHref("runs")}>
                        <Button size="small" type="link">
                          Open Runs →
                        </Button>
                      </Link>
                    ) : (
                      <Space>
                        <Link to={sectionHref("config")}>
                          <Button size="small" type="primary">
                            Open config
                          </Button>
                        </Link>
                        <Link to={sectionHref("runs")}>
                          <Button size="small">
                            Open Runs
                          </Button>
                        </Link>
                      </Space>
                    )}
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
          pcAlerts={
            (pcResolvedFilter === "No"
              ? pcUnresolvedAlertsQuery.data?.alerts
              : pcAllAlertsQuery.data?.alerts) ?? []
          }
          diffFlags={(reportData.diff_flags || {}) as Record<string, unknown>}
          flakyKeys={(reportData.flaky_keys || {}) as Record<string, unknown>}
          nccLogs={Array.isArray(reportData.ncc_logs) ? reportData.ncc_logs : []}
          filterText={filterText}
          selectedClusters={selectedClusters}
          clusterNameMap={clusterNameMap}
          severityFilters={severityFilters}
          alertSource={alertSource}
          pcResolvedFilter={pcResolvedFilter}
          compareMode={alertSource === "PC" ? "all" : compareMode}
          onSummaryChange={setTableSummary}
          dispositions={nccDispositionsQuery.data?.items}
          dispositionPending={setNCCDisposition.isPending}
          runAt={runTimestamp}
          onDisposition={
            features.data?.alert_handling === false ? undefined : (input) => setNCCDisposition.mutate(input)
          }
        />
      )}
    </div>
  );
}
