import { useMemo, useState } from "react";
import { Link, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Segmented, Skeleton, Table, Tabs, Tooltip } from "antd";
import { ArrowLeftOutlined, DownloadOutlined, FileTextOutlined } from "@ant-design/icons";
import { api, ApiError } from "../api/client";
import { sectionHref } from "../navigation";
import { EmptyState, KeyValueList, Panel, StatTile, StatusBadge, toneFor, type Tone } from "../ui";
import { formatDateTime, relativeTime } from "../utils/datetime";
import { formatDuration } from "../utils/freshness";
import { asArray, asRecord, toNumber } from "../utils/report";
import { runStatusTone } from "../utils/runStatus";

type ClusterResult = {
  address: string;
  ok: boolean;
  error: string;
  errorClass: string;
  fail: number;
  err: number;
  warn: number;
  info: number;
  total: number;
  score: number | null;
};

type CheckRow = { key: string; cluster: string; severity: string; check: string; detail: string };
type SevFilter = "all" | "FAIL" | "ERR" | "WARN" | "INFO";

const SEV_ORDER: Record<string, number> = { FAIL: 0, ERR: 1, WARN: 2, INFO: 3 };

function clusterTone(c: ClusterResult): { tone: Tone; label: string } {
  if (!c.ok) return { tone: "fail", label: "Not checked" };
  if (c.fail + c.err > 0) return { tone: "fail", label: "Failing" };
  if (c.warn > 0) return { tone: "warn", label: "Warnings" };
  return { tone: "ok", label: "Healthy" };
}

function downloadJSON(name: string, value: unknown) {
  const blob = new Blob([typeof value === "string" ? value : JSON.stringify(value, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

const FILE_LABELS: Record<string, string> = {
  "run-summary.json": "Run summary",
  "checks-snapshot.json": "Check results per cluster",
  "regression-summary.json": "Changes since the previous run",
  "ncc-run-record.json": "Run record (for automation)",
  "run-meta.json": "Run metadata",
};

/**
 * One archived run: outcome, per-cluster results, every non-passing check,
 * and the run's saved files. Logs are not stored per run; System → Logs has
 * the runner log.
 */
export function RunDetailPage() {
  const { id = "" } = useParams();
  const [sev, setSev] = useState<SevFilter>("all");
  const q = useQuery({ queryKey: ["runs", "detail", id], queryFn: () => api.runById(id), enabled: Boolean(id), retry: false });

  const run = q.data?.run;
  const files = asRecord(q.data?.artifacts);
  const summary = asRecord(files["run-summary.json"]);
  const snapshot = asRecord(files["checks-snapshot.json"]);
  const regression = asRecord(files["regression-summary.json"]);

  const clusters: ClusterResult[] = useMemo(() => {
    // run-summary.json has reachability and errors; checks-snapshot.json has
    // counts for runs whose summary predates per-cluster results.
    const fromSummary = asArray(summary.clusters).map((c) => asRecord(c));
    const fromSnapshot = asArray(snapshot.clusters).map((c) => asRecord(c));
    const source = fromSummary.length > 0 ? fromSummary : fromSnapshot;
    const failed = new Set(asArray(summary.failed_clusters).map(String));
    return source
      .map((c) => ({
        address: String(c.address || "—"),
        ok: c.ok === undefined ? !failed.has(String(c.address)) : Boolean(c.ok),
        error: String(c.error || ""),
        errorClass: String(c.error_class || ""),
        fail: toNumber(c.fail_count),
        err: toNumber(c.err_count),
        warn: toNumber(c.warn_count),
        info: toNumber(c.info_count),
        total: toNumber(c.checks_total),
        score: c.health_score === undefined ? null : toNumber(c.health_score),
      }))
      .sort((a, b) => Number(a.ok) - Number(b.ok) || b.fail + b.err - (a.fail + a.err) || b.warn - a.warn || a.address.localeCompare(b.address));
  }, [summary.clusters, summary.failed_clusters, snapshot.clusters]);

  const checks: CheckRow[] = useMemo(() => {
    const rows: CheckRow[] = [];
    for (const c of asArray(snapshot.clusters).map((x) => asRecord(x))) {
      asArray(c.checks).forEach((raw, i) => {
        const k = asRecord(raw);
        const severity = String(k.severity || "").toUpperCase();
        if (severity === "PASS") return;
        rows.push({ key: `${c.address}:${i}`, cluster: String(c.address || "—"), severity, check: String(k.check_name || "—"), detail: String(k.detail || "") });
      });
    }
    return rows.sort((a, b) => (SEV_ORDER[a.severity] ?? 9) - (SEV_ORDER[b.severity] ?? 9) || a.cluster.localeCompare(b.cluster));
  }, [snapshot.clusters]);

  const shownChecks = sev === "all" ? checks : checks.filter((c) => c.severity === sev);

  if (q.isLoading) {
    return (
      <div className="runs-page">
        <Skeleton active paragraph={{ rows: 6 }} />
      </div>
    );
  }

  if (q.isError || !run) {
    const notFound = q.error instanceof ApiError && q.error.status === 404;
    return (
      <div className="runs-page">
        <Link to={sectionHref("runs")} className="run-back">
          <ArrowLeftOutlined /> Runs
        </Link>
        <Panel>
          <EmptyState
            title={notFound ? "Run not found" : "Could not load this run"}
            description={notFound ? `There is no archived run "${id}". It may have been pruned by retention.` : String((q.error as Error)?.message || "")}
            actions={<Button onClick={() => void q.refetch()}>Try again</Button>}
          />
        </Panel>
      </div>
    );
  }

  const st = runStatusTone(run);
  const at = run.timestamp || run.mod_time;
  const clustersTotal = (run.clusters_ok ?? 0) + (run.clusters_failed ?? 0);
  const failTotal = (run.fail_total ?? 0) + (run.err_total ?? 0);
  const failureClasses = Object.entries(asRecord(summary.failure_classes)).filter(([, n]) => toNumber(n) > 0);

  return (
    <div className="runs-page">
      <Link to={sectionHref("runs")} className="run-back">
        <ArrowLeftOutlined /> Runs
      </Link>
      <div className="run-head">
        <div>
          <h2 className="ui-page-header-title">
            <Tooltip title={formatDateTime(at)}>Run {relativeTime(at)}</Tooltip>
          </h2>
          <p className="ui-page-header-desc">
            <span className="mono">{run.id}</span> · {formatDateTime(at)}
            {run.run_source ? ` · ${run.run_source === "scheduled" ? "Scheduled" : run.run_source === "manual" ? "Manual" : run.run_source}` : ""}
          </p>
        </div>
        <StatusBadge tone={st.tone}>{st.label}</StatusBadge>
      </div>

      <div className="overview-stats">
        <StatTile label="Duration" value={formatDuration(run.duration_s)} />
        <StatTile
          label="Clusters checked"
          value={clustersTotal ? `${run.clusters_ok ?? 0}/${clustersTotal}` : "—"}
          tone={(run.clusters_failed ?? 0) > 0 ? "fail" : clustersTotal ? "ok" : undefined}
          hint={(run.clusters_failed ?? 0) > 0 ? `${run.clusters_failed} could not be checked` : undefined}
        />
        <StatTile
          label="Failing checks"
          value={run.fail_total == null && run.err_total == null ? "—" : failTotal.toLocaleString()}
          tone={failTotal > 0 ? "fail" : run.fail_total != null ? "ok" : undefined}
          hint={run.warn_total != null ? `${run.warn_total} warnings` : undefined}
        />
        <StatTile
          label="Average health"
          value={run.avg_health_score ? `${run.avg_health_score}` : "—"}
          hint={run.min_health_score ? `Lowest cluster: ${run.min_health_score}` : undefined}
        />
      </div>

      {st.hint ? <Alert type="info" showIcon title={st.hint} /> : null}

      <Tabs
        className="run-tabs"
        items={[
          {
            key: "clusters",
            label: `Clusters${clusters.length ? ` (${clusters.length})` : ""}`,
            children: (
              <Panel flush>
                {clusters.length === 0 ? (
                  <EmptyState title="No per-cluster results" description="This run did not record results per cluster." />
                ) : (
                  <Table<ClusterResult>
                    size="middle"
                    rowKey="address"
                    dataSource={clusters}
                    pagination={false}
                    scroll={{ x: 640 }}
                    columns={[
                      { title: "Cluster", dataIndex: "address", key: "address", render: (v: string) => <span className="mono">{v}</span> },
                      {
                        title: "Status",
                        key: "status",
                        render: (_: unknown, c) => {
                          const t = clusterTone(c);
                          return <StatusBadge tone={t.tone}>{t.label}</StatusBadge>;
                        },
                      },
                      {
                        title: "Result",
                        key: "result",
                        render: (_: unknown, c) =>
                          c.ok
                            ? [c.fail && `${c.fail} fail`, c.err && `${c.err} error`, c.warn && `${c.warn} warn`, c.info && `${c.info} info`]
                                .filter(Boolean)
                                .join(" · ") || "No findings"
                            : `${c.error || "Unreachable"}${c.errorClass ? ` (${c.errorClass})` : ""}`,
                      },
                      { title: "Checks", key: "total", render: (_: unknown, c) => (c.total ? c.total.toLocaleString() : "—") },
                      { title: "Health", key: "score", render: (_: unknown, c) => (c.score == null ? "—" : c.score) },
                    ]}
                  />
                )}
              </Panel>
            ),
          },
          {
            key: "checks",
            label: `Checks${checks.length ? ` (${checks.length})` : ""}`,
            children: (
              <Panel
                flush
                title="Checks that did not pass"
                extra={
                  <Segmented<SevFilter>
                    size="small"
                    value={sev}
                    onChange={setSev}
                    aria-label="Filter by severity"
                    options={(["all", "FAIL", "ERR", "WARN", "INFO"] as SevFilter[]).map((s) => ({
                      value: s,
                      label: s === "all" ? `All (${checks.length})` : `${s} (${checks.filter((c) => c.severity === s).length})`,
                    }))}
                  />
                }
              >
                <Table<CheckRow>
                  size="middle"
                  rowKey="key"
                  dataSource={shownChecks}
                  pagination={shownChecks.length > 25 ? { pageSize: 25, showSizeChanger: false } : false}
                  scroll={{ x: 720 }}
                  locale={{
                    emptyText: (
                      <EmptyState
                        title={checks.length ? "None at this severity" : "No check results saved"}
                        description={checks.length ? undefined : "This run has no check snapshot, or every check passed."}
                      />
                    ),
                  }}
                  columns={[
                    { title: "Severity", dataIndex: "severity", key: "severity", width: 110, render: (v: string) => <StatusBadge tone={toneFor(v)}>{v}</StatusBadge> },
                    { title: "Cluster", dataIndex: "cluster", key: "cluster", width: 160, render: (v: string) => <span className="mono">{v}</span> },
                    { title: "Check", dataIndex: "check", key: "check", width: 260 },
                    { title: "Detail", dataIndex: "detail", key: "detail", render: (v: string) => v || "—" },
                  ]}
                />
              </Panel>
            ),
          },
          {
            key: "details",
            label: "Details",
            children: (
              <div className="run-details">
                <Panel title="Run">
                  <KeyValueList
                    items={[
                      { label: "Run ID", value: run.id, mono: true, copy: run.id },
                      { label: "Started", value: formatDateTime(at) },
                      { label: "Exit code", value: run.exit_code == null ? "—" : String(run.exit_code) },
                      { label: "Checks run", value: run.total_checks ? run.total_checks.toLocaleString() : "—" },
                      { label: "Outcome source", value: run.status_source || "—" },
                    ]}
                  />
                </Panel>
                {failureClasses.length > 0 ? (
                  <Panel title="Why clusters could not be checked">
                    <KeyValueList items={failureClasses.map(([k, n]) => ({ label: k, value: `${toNumber(n)} cluster(s)` }))} />
                  </Panel>
                ) : null}
                {regression.previous_timestamp ? (
                  <Panel title="Compared with the previous run">
                    <KeyValueList
                      items={[
                        { label: "Previous run", value: formatDateTime(String(regression.previous_timestamp)) },
                        {
                          label: "Failing checks",
                          value: `${toNumber(regression.previous_fail_total)} → ${toNumber(regression.current_fail_total)} (${
                            toNumber(regression.delta_fail_total) > 0 ? "+" : ""
                          }${toNumber(regression.delta_fail_total)})`,
                        },
                        { label: "Got worse", value: asArray(regression.increased_clusters).map(String).join(", ") || "—" },
                        { label: "Improved", value: asArray(regression.decreased_clusters).map(String).join(", ") || "—" },
                      ]}
                    />
                  </Panel>
                ) : null}
                <Panel title="Saved files" description="JSON documents archived with this run." flush>
                  {Object.keys(files).length === 0 ? (
                    <EmptyState title="No files saved" />
                  ) : (
                    <ul className="run-files">
                      {Object.entries(files).map(([name, value]) => (
                        <li key={name}>
                          <FileTextOutlined aria-hidden="true" />
                          <span>
                            <strong>{FILE_LABELS[name] ?? name}</strong>
                            <span className="ui-field-help mono"> {name}</span>
                          </span>
                          <Button size="small" icon={<DownloadOutlined />} onClick={() => downloadJSON(`${run.id}-${name}`, value)}>
                            Download
                          </Button>
                        </li>
                      ))}
                    </ul>
                  )}
                </Panel>
                <p className="ui-field-help">
                  Runner output is not stored per run. The latest runner log is under{" "}
                  <Link to={sectionHref("logs")}>System → Logs</Link>.
                </p>
              </div>
            ),
          },
        ]}
      />
    </div>
  );
}
