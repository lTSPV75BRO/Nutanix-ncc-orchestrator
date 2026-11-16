import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Skeleton, Space, Tag, Typography } from "antd";
import { NodeIndexOutlined } from "@ant-design/icons";
import { KeyValueList, Panel } from "../../ui";
import { api } from "../../api/client";
import { formatDateTime, relativeTime } from "../../utils/datetime";
import { formatSpan, NCC_ALERTS_QUERY_KEY, severityChanges, summarizeFinding } from "../../utils/alertLifecycle";

/**
 * Lifecycle facts for one NCC finding in the alert drawer: how long it has been
 * open, whether it keeps coming back, whether the latest run still reports it,
 * and how its severity has moved. Looked up by cluster + check so it also works
 * for alerts nobody has acknowledged yet.
 */
export function NCCLifecyclePanel({
  cluster,
  check,
  pending,
  onResolve,
}: {
  cluster: string;
  check: string;
  pending?: boolean;
  /** Absent when alert handling is turned off: the hint shows without a button. */
  onResolve?: () => void;
}) {
  const instances = useQuery({
    queryKey: [NCC_ALERTS_QUERY_KEY, "finding", cluster, check],
    queryFn: () => api.nccAlerts({ cluster, check, include_closed: true, limit: 1000 }),
    staleTime: 15_000,
    enabled: Boolean(cluster && check),
  });
  const summary = summarizeFinding(instances.data?.items ?? []);
  const activeId = summary.active?.id;
  const detail = useQuery({
    queryKey: [NCC_ALERTS_QUERY_KEY, "detail", activeId],
    queryFn: () => api.nccAlertHistory(activeId as string),
    staleTime: 15_000,
    enabled: Boolean(activeId),
  });

  if (instances.isLoading) {
    return (
      <Panel title="Lifecycle" icon={<NodeIndexOutlined />}>
        <Skeleton active paragraph={{ rows: 2 }} title={false} />
      </Panel>
    );
  }
  if (instances.isError) {
    return (
      <Panel title="Lifecycle" icon={<NodeIndexOutlined />}>
        <Typography.Text type="secondary">Lifecycle history is not available from this server.</Typography.Text>
      </Panel>
    );
  }
  const active = summary.active;
  if (!active) {
    return (
      <Panel title="Lifecycle" icon={<NodeIndexOutlined />}>
        <Typography.Text type="secondary">No lifecycle history yet. It is recorded within a minute of a run finishing.</Typography.Text>
      </Panel>
    );
  }
  const changes = severityChanges(detail.data?.events ?? []);
  const stateLabel = active.state === "acknowledged" ? "acknowledged" : active.state === "resolved" ? "resolved" : "open";

  return (
    <Panel title="Lifecycle" icon={<NodeIndexOutlined />}>
      {summary.looksFixed ? (
        <Alert
          type="info"
          showIcon
          className="lifecycle-fixed-hint"
          title={
            active.not_detected_since_run
              ? `Not reported since the ${formatDateTime(active.not_detected_since_run)} run`
              : "The latest run no longer reports this check"
          }
          description={onResolve ? `It is still ${stateLabel}. Resolve it if the issue is fixed.` : `It is still ${stateLabel}.`}
          action={
            onResolve ? (
              <Button size="small" type="primary" disabled={pending} onClick={onResolve}>
                Resolve
              </Button>
            ) : undefined
          }
        />
      ) : null}

      <KeyValueList
        items={[
          summary.openForMs != null
            ? { label: "Open for", value: `${formatSpan(summary.openForMs)} · since ${formatDateTime(active.first_seen_at || active.created_at)}` }
            : { label: "State", value: `Resolved · first seen ${formatDateTime(active.first_seen_at || active.created_at)}` },
          {
            label: "Last reported",
            value: active.last_seen_at ? `${relativeTime(active.last_seen_at)} · ${formatDateTime(active.last_seen_at)}` : "—",
          },
          {
            label: "Came back",
            value: `${summary.recurrences === 1 ? "once" : `${summary.recurrences} times`} · ${summary.occurrences} ${summary.occurrences === 1 ? "occurrence" : "occurrences"} in all`,
          },
          { label: "First ever seen", value: summary.firstEverMs != null ? formatDateTime(summary.firstEverMs) : "—" },
        ]}
      />

      {summary.recurrences > 0 && summary.lastResolution ? (
        <Typography.Paragraph className="lifecycle-note">
          Last resolved by <Typography.Text strong>{summary.lastResolution.by}</Typography.Text> on{" "}
          {formatDateTime(summary.lastResolution.at)}
          {summary.lastResolution.note ? ` with the note "${summary.lastResolution.note}"` : ""}, then found again. A
          longer-term fix may be needed.
        </Typography.Paragraph>
      ) : null}

      {changes.length > 0 ? (
        <div className="lifecycle-note">
          <Typography.Text type="secondary">Severity changes</Typography.Text>
          <ol className="ncc-handling-history">
            {changes.map((c) => (
              <li key={`${c.at}-${c.from}-${c.to}`}>
                <Space size={4} wrap>
                  <Tag>{c.from}</Tag>→<Tag color={c.escalated ? "red" : "green"}>{c.to}</Tag>
                  <Typography.Text type="secondary">{formatDateTime(c.at)}</Typography.Text>
                </Space>
              </li>
            ))}
          </ol>
        </div>
      ) : null}
    </Panel>
  );
}
