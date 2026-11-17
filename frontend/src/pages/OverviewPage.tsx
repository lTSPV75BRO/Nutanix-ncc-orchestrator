import { useMemo, useState } from "react";
import { Link, useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Select, Skeleton, Tooltip } from "antd";
import { ArrowRightOutlined, ClusterOutlined, HistoryOutlined, PieChartOutlined, ReloadOutlined } from "@ant-design/icons";
import { api } from "../api/client";
import { useAuth } from "../auth/AuthContext";
import { SetupBanner, useOnboarding } from "../features/onboarding/SetupChecklist";
import { SetupEmptyState } from "../features/onboarding/SetupEmptyState";
import { RunsTable } from "../features/runs/RunsTable";
import { useRunsActive } from "../hooks/useRunsActive";
import { clusterPath, sectionHref } from "../navigation";
import { EmptyState, ListToolbar, matchesSearch, Panel, StatTile, StatusBadge, VirtualList, type Tone } from "../ui";
import { ageMs, formatDateTime, relativeTime } from "../utils/datetime";
import { formatDuration, freshness, mergeFreshness, overdueScheduleText, type OverviewClusterRow as ClusterRow } from "../utils/freshness";
import { asArray, asRecord, buildClusterNameMap, resolveClusterName, toNumber, weightedHealthScore } from "../utils/report";

// Same query as the Findings page, so moving between them reuses the cache.
const PREVIEW_LIMIT = 1500;

const SEVERITIES: Array<{ key: string; label: string; count: (c: Record<string, unknown>) => number; tone: Tone }> = [
  { key: "FAIL", label: "Fail", count: (c) => toNumber(c.fail), tone: "fail" },
  { key: "ERR", label: "Error", count: (c) => toNumber(c.error), tone: "fail" },
  { key: "WARN", label: "Warning", count: (c) => toNumber(c.warn), tone: "warn" },
  { key: "INFO", label: "Info", count: (c) => toNumber(c.info), tone: "info" },
];

const DAY_MS = 24 * 3600_000;

/** Text after the cluster name: counts, or why it has no current data. */
function clusterDetail(c: ClusterRow): string {
  if (c.ok) return clusterCounts(c);
  const why = c.error || "Unreachable";
  if (!c.checkedAt) return why;
  if (!c.lastDataAt) return `${why} · no data yet`;
  return `${why} · data from ${relativeTime(c.lastDataAt)}`;
}

/** Tooltip for the age column. */
function clusterAgeTitle(c: ClusterRow): string {
  if (!c.checkedAt) return "";
  const parts = [`Last checked ${formatDateTime(c.checkedAt)}.`];
  if (!c.ok) {
    parts.push(c.lastDataAt ? `Last data ${formatDateTime(c.lastDataAt)}.` : "No run has returned data for it yet.");
    if ((c.failedRuns ?? 0) > 1) parts.push(`The last ${c.failedRuns} runs could not check it.`);
  }
  return parts.join(" ");
}

function sortClusters(rows: ClusterRow[]): ClusterRow[] {
  return [...rows].sort((a, b) => Number(a.ok) - Number(b.ok) || b.fail + b.err - (a.fail + a.err) || b.warn - a.warn || a.name.localeCompare(b.name));
}

function clusterStatus(c: ClusterRow): { tone: Tone; label: string } {
  if (!c.ok) return { tone: "fail", label: "Not checked" };
  if (c.fail > 0 || c.err > 0) return { tone: "fail", label: "Failing" };
  if (c.warn > 0) return { tone: "warn", label: "Warnings" };
  return { tone: "ok", label: "Healthy" };
}

/** "2 fail · 1 error · 3 warn", leaving out zeros; "No findings" when clean. */
function clusterCounts(c: ClusterRow): string {
  const parts = [
    c.fail ? `${c.fail} fail` : "",
    c.err ? `${c.err} error` : "",
    c.warn ? `${c.warn} warn` : "",
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : "No findings";
}

type ClusterFilter = "all" | "fail" | "warn" | "down" | "ok";

function clusterBucket(c: ClusterRow): Exclude<ClusterFilter, "all"> {
  if (!c.ok) return "down";
  if (c.fail > 0 || c.err > 0) return "fail";
  if (c.warn > 0) return "warn";
  return "ok";
}

/** Searchable, scrolling cluster list; renders only the rows in view. */
function ClusterHealthList({ clusters }: { clusters: ClusterRow[] }) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<ClusterFilter>("all");
  const counts = useMemo(() => {
    const k = { fail: 0, warn: 0, down: 0, ok: 0 };
    for (const c of clusters) k[clusterBucket(c)]++;
    return k;
  }, [clusters]);
  const shown = useMemo(
    () => clusters.filter((c) => (filter === "all" || clusterBucket(c) === filter) && matchesSearch(search, c.name, c.address, c.error)),
    [clusters, filter, search],
  );
  const filters = [
    { value: "all", label: "All" },
    { value: "fail", label: "Failing", count: counts.fail },
    { value: "warn", label: "Warnings", count: counts.warn },
    ...(counts.down ? [{ value: "down", label: "Not checked", count: counts.down }] : []),
    { value: "ok", label: "Healthy", count: counts.ok },
  ];
  return (
    <>
      {clusters.length > 8 ? (
        <ListToolbar
          search={search}
          onSearch={setSearch}
          filters={filters}
          filter={filter}
          onFilter={(v) => setFilter(v as ClusterFilter)}
          shown={shown.length}
          total={clusters.length}
        />
      ) : null}
      {shown.length === 0 ? (
        <EmptyState title="No matching clusters" description="Try another search or filter." />
      ) : (
        <VirtualList
          className="overview-clusters"
          aria-label="Clusters"
          items={shown}
          rowHeight={44}
          maxHeight={396}
          itemKey={(c) => c.name}
          renderRow={(c) => {
            const st = clusterStatus(c);
            const old = c.checkedAt ? ageMs(c.checkedAt) > DAY_MS : false;
            return (
              <Link to={clusterPath(c.address || c.name)} className="overview-cluster-row">
                <span className="overview-cluster-name" title={c.error || c.name}>
                  {c.name}
                </span>
                <span className="overview-cluster-counts">{clusterDetail(c)}</span>
                <span className={`overview-cluster-age${old ? " is-old" : ""}`} title={clusterAgeTitle(c)}>
                  {c.checkedAt ? relativeTime(c.checkedAt) : ""}
                </span>
                <StatusBadge tone={st.tone}>{st.label}</StatusBadge>
              </Link>
            );
          }}
        />
      )}
    </>
  );
}

function scoreTone(score: number | null): "ok" | "warn" | "fail" | undefined {
  if (score == null) return undefined;
  if (score >= 90) return "ok";
  if (score >= 60) return "warn";
  return "fail";
}

/**
 * Overview: is everything healthy, what needs attention, and what ran
 * recently. Each block links to where the detail lives (Findings, Runs).
 */
export function OverviewPage() {
  const navigate = useNavigate();
  const { canOperate, isAdmin } = useAuth();
  const report = useQuery({
    queryKey: ["report-data", "preview", PREVIEW_LIMIT],
    queryFn: () => api.reportDataWithPagination({ limit: PREVIEW_LIMIT }),
    staleTime: 30_000,
  });
  const runs = useQuery({
    queryKey: ["runs", "recent"],
    queryFn: () => api.runs({ limit: 6 }),
    staleTime: 30_000,
  });
  const freshnessQuery = useQuery({
    queryKey: ["freshness"],
    queryFn: api.freshness,
    staleTime: 30_000,
    refetchInterval: 5 * 60_000,
  });
  const active = useRunsActive();
  const running = active.data?.active === true;
  const onboarding = useOnboarding(isAdmin || canOperate);
  // No schedule, and the setup banner (which would say so) is hidden.
  const scheduleStep = onboarding.data?.steps.find((s) => s.id === "schedule");
  const scheduleMissing =
    canOperate && Boolean(onboarding.data?.completed || onboarding.data?.dismissed) && Boolean(scheduleStep && !scheduleStep.done && !scheduleStep.skipped);

  const data = report.data;
  const counts = asRecord(data?.ncc_summary_counts);
  const runSummary = asRecord(data?.run_summary);
  const timestamp = String(runSummary.timestamp || "");
  const totalChecks = toNumber(counts.total_plugins);
  const score = weightedHealthScore(counts);
  const fresh = freshness(timestamp);
  const clustersOk = toNumber(runSummary.clusters_ok);
  const clustersFailed = toNumber(runSummary.clusters_failed);
  const attention = toNumber(counts.fail) + toNumber(counts.error) + toNumber(counts.warn);
  const regression = asRecord(data?.regression_summary);
  const policyViolations = Array.isArray(data?.policy_violations) ? (data?.policy_violations as string[]) : [];

  const clusters: ClusterRow[] = useMemo(() => {
    if (!data) return [];
    const nameMap = buildClusterNameMap({
      runSummary: data.run_summary,
      checksSnapshot: data.checks_snapshot,
      aggRows: Array.isArray(data.agg_rows) ? data.agg_rows : [],
      drilldownDiff: data.drilldown_diff,
      flakyChecks: data.flaky_checks,
      sloDashboard: data.slo_dashboard,
      regressionSummary: data.regression_summary,
    });
    // Per-cluster results come from the run summary; older runs only have
    // them in the SLO export.
    const fromRun = asArray(asRecord(data.run_summary).clusters).map((c) => asRecord(c));
    const source = fromRun.length > 0 ? fromRun : asArray(asRecord(data.slo_dashboard).clusters).map((c) => asRecord(c));
    const rows = source.map((c) => ({
      name: resolveClusterName(c.address, nameMap) || String(c.address || "—"),
      address: String(c.address || ""),
      ok: c.ok === undefined ? String(c.status || "").toLowerCase() !== "error" : Boolean(c.ok),
      error: String(c.error || ""),
      fail: toNumber(c.fail_count),
      warn: toNumber(c.warn_count),
      err: toNumber(c.err_count),
      score: c.health_score === undefined ? null : toNumber(c.health_score),
    }));
    return sortClusters(mergeFreshness(rows, freshnessQuery.data?.clusters, (a) => resolveClusterName(a, nameMap)));
  }, [data, freshnessQuery.data]);
  const fromEarlierRuns = clusters.length - asArray(asRecord(data?.run_summary).clusters).length;
  const overdueSchedules = (freshnessQuery.data?.schedules ?? []).filter((s) => s.overdue);

  // Environment filter for the cluster list ("" = every cluster).
  const environmentsQuery = useQuery({ queryKey: ["environments"], queryFn: api.listEnvironments, staleTime: 60_000 });
  const environments = useMemo(
    () => (environmentsQuery.data?.items ?? []).filter((e) => !e.error && (e.clusters?.length ?? 0) > 0),
    [environmentsQuery.data],
  );
  const [environment, setEnvironment] = useState("");
  const shownClusters = useMemo(() => {
    const env = environments.find((e) => e.name === environment);
    if (!env) return clusters;
    const keys = new Set((env.clusters ?? []).map((v) => v.toLowerCase()));
    return clusters.filter((c) => keys.has(c.address.toLowerCase()) || keys.has(c.name.toLowerCase()));
  }, [clusters, environments, environment]);

  const header = (
    <div className="overview-runline">
      <div className="overview-runline-text">
        {timestamp ? (
          <>
            <StatusBadge tone={fresh.tone}>{fresh.label}</StatusBadge>
            <Tooltip title={formatDateTime(timestamp)}>
              <span>Latest run {relativeTime(timestamp)}</span>
            </Tooltip>
            {toNumber(runSummary.duration_s) > 0 ? <span>· took {formatDuration(toNumber(runSummary.duration_s))}</span> : null}
          </>
        ) : (
          <span>No completed run yet</span>
        )}
      </div>
      <div className="overview-runline-actions">
        <Button icon={<ReloadOutlined />} onClick={() => void Promise.all([report.refetch(), runs.refetch()])} loading={report.isFetching}>
          Refresh
        </Button>
        <Link to="/findings">
          <Button type="primary" icon={<ArrowRightOutlined />} iconPlacement="end">
            View findings
          </Button>
        </Link>
      </div>
    </div>
  );

  if (report.isLoading && !data) {
    return (
      <div className="overview">
        <div className="overview-stats">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="ui-stat">
              <Skeleton active paragraph={{ rows: 1 }} title={false} />
            </div>
          ))}
        </div>
        <Panel>
          <Skeleton active />
        </Panel>
      </div>
    );
  }

  const noRunYet = !timestamp && totalChecks === 0;

  return (
    <div className="overview">
      <SetupBanner />
      {header}

      {report.isError && !data ? (
        <Alert type="error" showIcon title="Couldn't load the latest results" description="Refresh to try again." />
      ) : null}

      {overdueSchedules.length > 0 ? (
        <Alert
          type="warning"
          showIcon
          title="Scheduled runs have stopped"
          description={
            <>
              {overdueSchedules.length > 1 ? (
                <ul className="overview-violations">
                  {overdueSchedules.map((s) => (
                    <li key={`${s.kind}-${s.id ?? ""}`}>{overdueScheduleText(s)}</li>
                  ))}
                </ul>
              ) : (
                overdueScheduleText(overdueSchedules[0])
              )}{" "}
              Results here may be out of date.
            </>
          }
          action={
            canOperate ? (
              <Link to={sectionHref("schedule")}>
                <Button size="small">Check the schedule</Button>
              </Link>
            ) : undefined
          }
        />
      ) : null}
      {scheduleMissing && !noRunYet && overdueSchedules.length === 0 ? (
        <Alert
          type="info"
          showIcon
          title="No schedule: NCC runs only when someone starts it"
          action={
            <Link to={sectionHref("schedule")}>
              <Button size="small">Set a schedule</Button>
            </Link>
          }
        />
      ) : null}
      {regression.has_regression ? (
        <Alert
          type="error"
          showIcon
          title="Regression since the previous run"
          description={`${toNumber(regression.delta_fail_total)} more failing checks than the run before.`}
          action={
            <Link to="/insights">
              <Button size="small">Compare runs</Button>
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
            <ul className="overview-violations">
              {policyViolations.slice(0, 5).map((v, i) => (
                <li key={i}>{v}</li>
              ))}
              {policyViolations.length > 5 ? <li>+{policyViolations.length - 5} more</li> : null}
            </ul>
          }
        />
      ) : null}

      {noRunYet ? (
        <Panel>
          <SetupEmptyState />
        </Panel>
      ) : (
        <>
          <div className="overview-stats">
            <StatTile
              label="Health score"
              value={score == null ? "—" : `${score.toFixed(1)}%`}
              tone={scoreTone(score)}
              hint={score == null ? "No completed checks" : `${totalChecks.toLocaleString()} checks in the latest run`}
            />
            <StatTile
              label="Need attention"
              value={attention.toLocaleString()}
              tone={toNumber(counts.fail) + toNumber(counts.error) > 0 ? "fail" : attention > 0 ? "warn" : "ok"}
              hint={`${toNumber(counts.fail)} fail · ${toNumber(counts.error)} error · ${toNumber(counts.warn)} warning`}
            />
            <StatTile
              label="Clusters checked"
              value={`${clustersOk}/${clustersOk + clustersFailed}`}
              tone={clustersFailed > 0 ? "fail" : clustersOk > 0 ? "ok" : undefined}
              hint={clustersFailed > 0 ? `${clustersFailed} could not be checked` : "All reachable"}
            />
            <StatTile
              label="Last run"
              value={running ? "Running" : timestamp ? relativeTime(timestamp) : "—"}
              hint={running ? "Results update when it finishes" : `${fresh.label} results`}
            />
          </div>

          <div className="overview-grid">
            <Panel
              title="Checks by result" icon={<PieChartOutlined />}
              description="From the latest run. Select one to see those findings."
              flush
            >
              <ul className="overview-severity">
                {SEVERITIES.map((s) => {
                  const n = s.count(counts);
                  const pct = totalChecks > 0 ? (n / totalChecks) * 100 : 0;
                  return (
                    <li key={s.key}>
                      <Link to={`/findings?sev=${s.key}`} className="overview-severity-row">
                        <StatusBadge tone={s.tone}>{s.label}</StatusBadge>
                        <span className="overview-bar" aria-hidden="true">
                          <span className={`overview-bar-fill tone-${s.tone}`} style={{ width: `${Math.min(100, pct)}%` }} />
                        </span>
                        <span className="overview-count">{n.toLocaleString()}</span>
                      </Link>
                    </li>
                  );
                })}
                <li>
                  <div className="overview-severity-row is-static">
                    <StatusBadge tone="ok">Pass</StatusBadge>
                    <span className="overview-bar" aria-hidden="true">
                      <span
                        className="overview-bar-fill tone-ok"
                        style={{ width: `${totalChecks > 0 ? Math.min(100, (toNumber(counts.pass) / totalChecks) * 100) : 0}%` }}
                      />
                    </span>
                    <span className="overview-count">{toNumber(counts.pass).toLocaleString()}</span>
                  </div>
                </li>
              </ul>
            </Panel>

            <Panel
              title="Clusters" icon={<ClusterOutlined />}
              description={
                clusters.length
                  ? fromEarlierRuns > 0
                    ? `${clusters.length} clusters, each from its newest run`
                    : `${clusters.length} in the latest run`
                  : undefined
              }
              flush
              extra={
                environments.length > 0 && clusters.length > 0 ? (
                  <Select
                    aria-label="Environment"
                    size="small"
                    value={environment}
                    onChange={setEnvironment}
                    popupMatchSelectWidth={false}
                    options={[{ value: "", label: "All environments" }, ...environments.map((e) => ({ value: e.name, label: e.name }))]}
                  />
                ) : null
              }
            >
              {clusters.length === 0 ? (
                <EmptyState icon={<ClusterOutlined />} title="No per-cluster results" description="The latest run did not record results per cluster." />
              ) : shownClusters.length === 0 ? (
                <EmptyState icon={<ClusterOutlined />} title={`No ${environment} clusters in the latest run`} description="Run this environment to see its clusters here." />
              ) : (
                <ClusterHealthList key={environment} clusters={shownClusters} />
              )}
            </Panel>
          </div>
        </>
      )}

      <Panel
        title="Recent runs" icon={<HistoryOutlined />}
        flush
        extra={
          <Link to={sectionHref("runs")}>
            <Button size="small" type="text" icon={<ArrowRightOutlined />} iconPlacement="end">
              All runs
            </Button>
          </Link>
        }
      >
        <RunsTable
          runs={(runs.data ?? []).slice(0, 5)}
          loading={runs.isLoading}
          onOpen={(r) => navigate(`/runs/${encodeURIComponent(r.id)}`)}
          empty={<EmptyState title="No runs yet" description="Runs you start, and scheduled runs, appear here." />}
        />
      </Panel>
    </div>
  );
}
