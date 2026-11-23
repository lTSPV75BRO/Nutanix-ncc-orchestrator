import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Skeleton, Table, Tooltip, Typography } from "antd";
import {
  AlertOutlined,
  ArrowLeftOutlined,
  ExportOutlined,
  HistoryOutlined,
  InfoCircleOutlined,
  PlayCircleOutlined,
} from "@ant-design/icons";
import { api } from "../api/client";
import type { NCCAlert } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { SetupEmptyState, seesNoClusters } from "../features/onboarding/SetupEmptyState";
import type { RunPreset } from "../features/runs/NewRunDrawer";
import { useClusterDirectory } from "../hooks/useClusterDirectory";
import { OVERVIEW_PATH, sectionHref } from "../navigation";
import { EmptyState, KeyValueList, ListToolbar, matchesSearch, Panel, StatTile, StatusBadge, toneFor, type KeyValueItem } from "../ui";
import { formatDateTime, relativeTime } from "../utils/datetime";
import { freshness as dataAge } from "../utils/freshness";
import { NCC_ALERTS_QUERY_KEY } from "../utils/alertLifecycle";
import { asRecord } from "../utils/report";
import { plural } from "../utils/text";
import { prismURLFor } from "../utils/prism";

const SEV_ORDER: Record<string, number> = { FAIL: 0, ERR: 1, WARN: 2, INFO: 3 };
type StateFilter = "all" | "open" | "acknowledged";

/** "Resolved, not reported since" / "Came back" for a closed finding. */
function closeLabel(a: NCCAlert): string {
  if (a.close_reason === "recurred") return "Came back (new instance)";
  if (a.state === "resolved" && a.resolved_by) return `Resolved by ${a.resolved_by}`;
  if (a.close_reason === "resolved_not_detected") return "No longer reported";
  return "Closed";
}

/**
 * One cluster: its latest result, how current its data is, its open NCC
 * findings and what was recently closed, with links to Findings, Prism and a
 * run of just this cluster.
 */
export function ClusterPage() {
  const { key = "" } = useParams<{ key: string }>();
  const navigate = useNavigate();
  const { canOperate, isAdmin, me } = useAuth();
  const dir = useClusterDirectory();
  const entry = dir.find(key);
  const address = entry?.address ?? key;
  const name = entry?.name ?? dir.nameOf(key);

  const alerts = useQuery({
    queryKey: [NCC_ALERTS_QUERY_KEY, "cluster", address],
    queryFn: () => api.nccAlertsAll({ cluster: address, include_closed: true }, 5),
    staleTime: 15_000,
    enabled: Boolean(address),
  });

  const [search, setSearch] = useState("");
  const [state, setState] = useState<StateFilter>("all");

  const { open, closed } = useMemo(() => {
    const items = alerts.data ?? [];
    const openItems = items
      .filter((a) => !a.closed && a.state !== "resolved")
      .sort(
        (a, b) =>
          (SEV_ORDER[(a.severity || "").toUpperCase()] ?? 9) - (SEV_ORDER[(b.severity || "").toUpperCase()] ?? 9) ||
          a.check.localeCompare(b.check),
      );
    const closedItems = items
      .filter((a) => a.closed || a.state === "resolved")
      .sort((a, b) => Date.parse(b.closed_at || b.resolved_at || "") - Date.parse(a.closed_at || a.resolved_at || ""))
      .slice(0, 20);
    return { open: openItems, closed: closedItems };
  }, [alerts.data]);

  const shownOpen = useMemo(
    () => open.filter((a) => (state === "all" || a.state === state) && matchesSearch(search, a.check, a.severity ?? "")),
    [open, state, search],
  );
  const ackCount = open.filter((a) => a.state === "acknowledged").length;

  if (dir.isLoading) {
    return (
      <div className="runs-page">
        <Skeleton active paragraph={{ rows: 6 }} />
      </div>
    );
  }

  const back = (
    <Link to={OVERVIEW_PATH} className="run-back">
      <ArrowLeftOutlined /> Overview
    </Link>
  );

  if (!entry && !alerts.isLoading && (alerts.data?.length ?? 0) === 0) {
    return (
      <div className="runs-page">
        {back}
        <Panel>
          {seesNoClusters(me, isAdmin) ? (
            <SetupEmptyState gap={{ kind: "restricted" }} />
          ) : (
            <EmptyState
              title="Cluster not found"
              description={
                isAdmin
                  ? `No run has reported a cluster called "${key}". Check the name, or add it under Clusters & config.`
                  : me?.cluster_access_unrestricted === false
                    ? `No run has reported a cluster called "${key}" in your cluster groups. Ask an administrator if you need it.`
                    : `No run has reported a cluster called "${key}". Check the name, or ask an administrator to add it.`
              }
              actions={
                <>
                  {isAdmin ? (
                    <Button type="primary" onClick={() => navigate(sectionHref("config"))}>
                      Add clusters
                    </Button>
                  ) : null}
                  <Button onClick={() => navigate(OVERVIEW_PATH)}>Go to the Overview</Button>
                </>
              }
            />
          )}
        </Panel>
      </div>
    );
  }

  const fresh = entry?.freshness;
  const checkedAt = fresh?.last_checked_at || "";
  // Without the freshness endpoint, a cluster the newest run checked has that run's data.
  const latestRunAt = String(asRecord(dir.report.data?.run_summary).timestamp || "");
  const lastDataAt = fresh ? fresh.last_success_at || "" : entry?.inLatestRun && entry.ok ? latestRunAt : "";
  const reachable = entry ? entry.ok : true;
  const age = dataAge(lastDataAt);
  const failing = (entry?.fail ?? 0) + (entry?.err ?? 0);
  const prismUrl = prismURLFor(address);
  const findingsHref = `/findings?clusters=${encodeURIComponent(name)}`;

  const about: KeyValueItem[] = [
    { label: "Name", value: name },
    { label: "Address", value: address, mono: true, copy: address },
    { label: "AOS", value: entry?.aosVersion || "Not reported" },
    { label: "NCC", value: entry?.nccVersion || "Not reported" },
    { label: "Environment", value: fresh ? fresh.environment || "Main config" : "—" },
    {
      label: "Last checked",
      value: checkedAt ? `${relativeTime(checkedAt)} · ${formatDateTime(checkedAt)}` : "—",
    },
    {
      label: "Newest data",
      value: lastDataAt ? `${relativeTime(lastDataAt)} · ${formatDateTime(lastDataAt)}` : "No run has returned data yet",
    },
  ];
  if (entry?.checks) about.push({ label: "Checks in the last run", value: entry.checks.toLocaleString() });

  return (
    <div className="runs-page cluster-page">
      {back}
      <div className="run-head">
        <div>
          <h2 className="ui-page-header-title">{name}</h2>
          <p className="ui-page-header-desc">
            {name !== address ? <span className="mono">{address}</span> : null}
            {name !== address && (entry?.aosVersion || entry?.nccVersion) ? " · " : ""}
            {[entry?.aosVersion ? `AOS ${entry.aosVersion}` : "", entry?.nccVersion ? `NCC ${entry.nccVersion}` : ""].filter(Boolean).join(" · ")}
          </p>
        </div>
        <div className="run-head-actions">
          <StatusBadge tone={!reachable ? "fail" : failing > 0 ? "fail" : (entry?.warn ?? 0) > 0 ? "warn" : "ok"}>
            {!reachable ? "Not reachable" : failing > 0 ? "Failing checks" : (entry?.warn ?? 0) > 0 ? "Warnings" : "Healthy"}
          </StatusBadge>
          {prismUrl ? (
            <Button icon={<ExportOutlined />} href={prismUrl} target="_blank" rel="noopener noreferrer">
              Open in Prism
            </Button>
          ) : null}
          <Button icon={<AlertOutlined />} onClick={() => navigate(findingsHref)}>
            Findings
          </Button>
          {canOperate ? (
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={() => {
                const rerun: RunPreset = {
                  key: `cluster:${address}:${Date.now()}`,
                  environment: fresh?.environment ?? "",
                  clusters: [address],
                  note: `Checking only ${name}. Change the selection below if needed.`,
                };
                navigate(sectionHref("runs"), { state: { rerun } });
              }}
            >
              Run this cluster
            </Button>
          ) : null}
        </div>
      </div>

      <div className="overview-stats">
        <StatTile
          label="Health"
          value={entry?.score != null ? `${entry.score}` : "—"}
          tone={entry?.score == null ? undefined : entry.score >= 75 ? "ok" : entry.score >= 60 ? "warn" : "fail"}
          hint={entry?.score != null ? "Weighted score from the last run" : "No score yet"}
        />
        <StatTile
          label="Failing checks"
          value={entry ? failing.toLocaleString() : "—"}
          tone={failing > 0 ? "fail" : entry ? "ok" : undefined}
          hint={entry?.err ? `${plural(entry.err, "error")} included` : undefined}
        />
        <StatTile label="Warnings" value={entry ? (entry.warn ?? 0).toLocaleString() : "—"} tone={(entry?.warn ?? 0) > 0 ? "warn" : undefined} />
        <StatTile
          label="Data age"
          value={lastDataAt ? relativeTime(lastDataAt) : "—"}
          tone={age.tone === "fail" ? "fail" : age.tone === "warn" ? "warn" : lastDataAt ? "ok" : undefined}
          hint={checkedAt && checkedAt !== lastDataAt ? `Last tried ${relativeTime(checkedAt)}` : undefined}
        />
      </div>

      {!reachable ? (
        <Alert
          type="error"
          showIcon
          title={`The last run could not check this cluster${entry?.errorClass ? ` (${entry.errorClass.replace(/_/g, " ")})` : ""}`}
          description={
            <>
              {entry?.error || "No reason was recorded."}
              {fresh?.failed_runs && fresh.failed_runs > 1 ? ` It has failed ${fresh.failed_runs} runs in a row.` : ""}
              {lastDataAt ? ` The numbers below are from ${formatDateTime(lastDataAt)}.` : ""}
            </>
          }
        />
      ) : null}

      <div className="cluster-page-grid">
        <Panel
          flush
          title="Open findings"
          icon={<AlertOutlined />}
          description="NCC results that still need a decision on this cluster. Open one in Findings to acknowledge or resolve it."
        >
          {alerts.isLoading ? (
            <div className="cluster-page-pad">
              <Skeleton active paragraph={{ rows: 4 }} title={false} />
            </div>
          ) : alerts.isError ? (
            <EmptyState title="Finding history is not available" description="This API server does not track NCC findings over time." />
          ) : open.length === 0 ? (
            <EmptyState title="No open findings" description="Every NCC result on this cluster is resolved or passing." />
          ) : (
            <>
              <ListToolbar
                search={search}
                onSearch={setSearch}
                placeholder="Search checks"
                filter={state}
                onFilter={(v) => setState(v as StateFilter)}
                filters={[
                  { value: "all", label: "All" },
                  { value: "open", label: "Needs attention", count: open.length - ackCount },
                  { value: "acknowledged", label: "Acknowledged", count: ackCount },
                ]}
                shown={shownOpen.length}
                total={open.length}
                noun="findings"
              />
              <Table<NCCAlert>
                size="middle"
                rowKey="id"
                dataSource={shownOpen}
                pagination={shownOpen.length > 25 ? { pageSize: 25, showSizeChanger: false } : false}
                scroll={{ x: 560 }}
                locale={{ emptyText: <EmptyState title="No matching findings" description="Try another search or filter." /> }}
                onRow={(a) => ({
                  onClick: () => navigate(`${findingsHref}&q=${encodeURIComponent(a.check)}`),
                  className: "cluster-page-row",
                })}
                columns={[
                  {
                    title: "Severity",
                    key: "severity",
                    width: 110,
                    render: (_, a) => <StatusBadge tone={toneFor(a.severity)}>{(a.severity || "—").toUpperCase()}</StatusBadge>,
                  },
                  { title: "Check", dataIndex: "check", key: "check", render: (v: string) => <span className="mono">{v}</span> },
                  {
                    title: "Open since",
                    key: "since",
                    width: 150,
                    render: (_, a) => {
                      const since = a.first_seen_at || a.created_at;
                      return since ? <Tooltip title={formatDateTime(since)}>{relativeTime(since)}</Tooltip> : "—";
                    },
                  },
                  {
                    title: "State",
                    key: "state",
                    width: 150,
                    render: (_, a) =>
                      a.state === "acknowledged" ? (
                        <Tooltip title={a.acknowledged_by ? `By ${a.acknowledged_by}` : undefined}>
                          <span>Acknowledged</span>
                        </Tooltip>
                      ) : a.reopened ? (
                        "Came back"
                      ) : (
                        "Needs attention"
                      ),
                  },
                ]}
              />
            </>
          )}
        </Panel>

        <div className="cluster-page-side">
          <Panel title="About this cluster" icon={<InfoCircleOutlined />}>
            <KeyValueList items={about} />
          </Panel>

          <Panel title="Recently closed" icon={<HistoryOutlined />} description="Findings that were resolved or stopped being reported.">
            {closed.length === 0 ? (
              <Typography.Text type="secondary">Nothing closed yet.</Typography.Text>
            ) : (
              <ol className="cluster-page-closed">
                {closed.map((a) => {
                  const at = a.closed_at || a.resolved_at || "";
                  return (
                    <li key={a.id}>
                      <span className="mono">{a.check}</span>
                      <Typography.Text type="secondary">
                        {closeLabel(a)}
                        {at ? ` · ${relativeTime(at)}` : ""}
                      </Typography.Text>
                    </li>
                  );
                })}
              </ol>
            )}
          </Panel>
        </div>
      </div>
    </div>
  );
}
