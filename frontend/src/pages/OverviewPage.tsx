import { useMemo } from "react";
import { Link, useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Skeleton, Table, Tooltip } from "antd";
import { ArrowRightOutlined, ClusterOutlined, ReloadOutlined, ThunderboltOutlined } from "@ant-design/icons";
import { api } from "../api/client";
import type { RunInfo } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { SetupBanner } from "../features/onboarding/SetupChecklist";
import { useRunsActive } from "../hooks/useRunsActive";
import { sectionHref } from "../navigation";
import { EmptyState, Panel, StatTile, StatusBadge, type Tone } from "../ui";
import { formatDateTime, relativeTime } from "../utils/datetime";
import { formatDuration, freshness } from "../utils/freshness";
import { asArray, asRecord, buildClusterNameMap, resolveClusterName, toNumber, weightedHealthScore } from "../utils/report";

// Same query as the Findings page, so moving between them reuses the cache.
const PREVIEW_LIMIT = 1500;

const SEVERITIES: Array<{ key: string; label: string; count: (c: Record<string, unknown>) => number; tone: Tone }> = [
  { key: "FAIL", label: "Fail", count: (c) => toNumber(c.fail), tone: "fail" },
  { key: "ERR", label: "Error", count: (c) => toNumber(c.error), tone: "fail" },
  { key: "WARN", label: "Warning", count: (c) => toNumber(c.warn), tone: "warn" },
  { key: "INFO", label: "Info", count: (c) => toNumber(c.info), tone: "info" },
];

type ClusterRow = { name: string; ok: boolean; error: string; fail: number; warn: number; err: number; score: number | null };

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

function runStatus(r: RunInfo): { tone: Tone; label: string } {
  switch (r.status) {
    case "success":
      return { tone: "ok", label: "Succeeded" };
    case "partial":
      return { tone: "warn", label: "Partial" };
    case "failed":
    case "trigger_failed":
      return { tone: "fail", label: "Failed" };
    case "running":
    case "triggered":
      return { tone: "info", label: "Running" };
    case "queued":
    case "pending":
      return { tone: "info", label: "Queued" };
    case "cancelled":
      return { tone: "neutral", label: "Cancelled" };
    default:
      if (r.success === true) return { tone: "ok", label: "Succeeded" };
      if (r.success === false) return { tone: "fail", label: "Failed" };
      return { tone: "neutral", label: "Unknown" };
  }
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
  const active = useRunsActive();
  const running = active.data?.active === true;

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
    return source
      .map((c) => ({
        name: resolveClusterName(c.address, nameMap) || String(c.address || "—"),
        ok: c.ok === undefined ? String(c.status || "").toLowerCase() !== "error" : Boolean(c.ok),
        error: String(c.error || ""),
        fail: toNumber(c.fail_count),
        warn: toNumber(c.warn_count),
        err: toNumber(c.err_count),
        score: c.health_score === undefined ? null : toNumber(c.health_score),
      }))
      .sort((a, b) => Number(a.ok) - Number(b.ok) || b.fail + b.err - (a.fail + a.err) || b.warn - a.warn || a.name.localeCompare(b.name));
  }, [data]);

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
          <Button type="primary" icon={<ArrowRightOutlined />} iconPosition="end">
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
        <Alert type="error" showIcon title="Could not load the latest results" description="Refresh to try again." />
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
          <EmptyState
            icon={<ThunderboltOutlined />}
            title={running ? "The first run is in progress" : "No results yet"}
            description={
              running
                ? "Results appear here as soon as it finishes."
                : "Run NCC across your clusters to see their health, findings and trends here."
            }
            actions={
              running ? (
                <Button onClick={() => navigate(sectionHref("runs"))}>Watch the run</Button>
              ) : (
                <>
                  {canOperate ? (
                    <Button type="primary" onClick={() => navigate(sectionHref("runs"))}>
                      Start a run
                    </Button>
                  ) : null}
                  {isAdmin ? <Button onClick={() => navigate(sectionHref("config"))}>Configure clusters</Button> : null}
                </>
              )
            }
          />
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
              title="Checks by result"
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
              title="Clusters"
              description={clusters.length ? `${clusters.length} in the latest run` : undefined}
              flush
            >
              {clusters.length === 0 ? (
                <EmptyState icon={<ClusterOutlined />} title="No per-cluster results" description="The latest run did not record results per cluster." />
              ) : (
                <ul className="overview-clusters">
                  {clusters.slice(0, 8).map((c) => {
                    const st = clusterStatus(c);
                    return (
                      <li key={c.name}>
                        <Link to={`/findings?clusters=${encodeURIComponent(c.name)}`} className="overview-cluster-row">
                          <span className="overview-cluster-name" title={c.error || c.name}>
                            {c.name}
                          </span>
                          <span className="overview-cluster-counts">{c.ok ? clusterCounts(c) : c.error || "Unreachable"}</span>
                          <StatusBadge tone={st.tone}>{st.label}</StatusBadge>
                        </Link>
                      </li>
                    );
                  })}
                  {clusters.length > 8 ? (
                    <li className="overview-more">
                      <Link to="/findings">+{clusters.length - 8} more clusters in Findings</Link>
                    </li>
                  ) : null}
                </ul>
              )}
            </Panel>
          </div>
        </>
      )}

      <Panel
        title="Recent runs"
        flush
        extra={
          canOperate ? (
            <Link to={sectionHref("runs")}>
              <Button size="small" type="text" icon={<ArrowRightOutlined />} iconPosition="end">
                All runs
              </Button>
            </Link>
          ) : null
        }
      >
        <Table<RunInfo>
          className="overview-runs"
          size="middle"
          rowKey="id"
          loading={runs.isLoading}
          pagination={false}
          dataSource={(runs.data ?? []).slice(0, 5)}
          locale={{ emptyText: <EmptyState title="No runs yet" description="Runs you start, and scheduled runs, appear here." /> }}
          scroll={{ x: 640 }}
          columns={[
            {
              title: "Started",
              key: "started",
              render: (_: unknown, r) => {
                const at = r.timestamp || r.mod_time;
                return <Tooltip title={formatDateTime(at)}>{relativeTime(at)}</Tooltip>;
              },
            },
            {
              title: "Status",
              key: "status",
              render: (_: unknown, r) => {
                const st = runStatus(r);
                return <StatusBadge tone={st.tone}>{st.label}</StatusBadge>;
              },
            },
            {
              title: "Trigger",
              key: "trigger",
              render: (_: unknown, r) => {
                const src = (r.run_source || "").toLowerCase();
                return src === "scheduled" ? "Scheduled" : src === "manual" ? "Manual" : src === "external" ? "External" : "—";
              },
            },
            { title: "Duration", key: "duration", render: (_: unknown, r) => formatDuration(r.duration_s) },
            {
              title: "Clusters",
              key: "clusters",
              render: (_: unknown, r) =>
                r.clusters_ok == null && r.clusters_failed == null ? "—" : `${r.clusters_ok ?? 0}/${(r.clusters_ok ?? 0) + (r.clusters_failed ?? 0)}`,
            },
            {
              title: "Findings",
              key: "findings",
              render: (_: unknown, r) =>
                r.fail_total == null && r.warn_total == null ? "—" : `${(r.fail_total ?? 0) + (r.err_total ?? 0)} fail · ${r.warn_total ?? 0} warn`,
            },
          ]}
        />
      </Panel>
    </div>
  );
}
