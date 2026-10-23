import { useQuery } from "@tanstack/react-query";
import { Alert, Button, Card, Skeleton, Space, Tag, Typography } from "antd";
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
  onResolve: () => void;
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
      <Card size="small" title="Lifecycle">
        <Skeleton active paragraph={{ rows: 2 }} title={false} />
      </Card>
    );
  }
  if (instances.isError) {
    return (
      <Card size="small" title="Lifecycle">
        <Typography.Text type="secondary">Lifecycle history is not available from this server.</Typography.Text>
      </Card>
    );
  }
  const active = summary.active;
  if (!active) {
    return (
      <Card size="small" title="Lifecycle">
        <Typography.Text type="secondary">No lifecycle history yet. It is recorded within a minute of a run finishing.</Typography.Text>
      </Card>
    );
  }
  const changes = severityChanges(detail.data?.events ?? []);
  const stateLabel = active.state === "acknowledged" ? "acknowledged" : active.state === "resolved" ? "resolved" : "open";

  return (
    <Card size="small" title="Lifecycle" className="ncc-lifecycle-card">
      {summary.looksFixed ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title={
            active.not_detected_since_run
              ? `Not reported since the ${formatDateTime(active.not_detected_since_run)} run`
              : "The latest run no longer reports this check"
          }
          description={`It is still ${stateLabel}. Resolve it if the issue is fixed.`}
          action={
            <Button size="small" type="primary" disabled={pending} onClick={onResolve}>
              Resolve
            </Button>
          }
        />
      ) : null}

      <div className="pc-alert-metric-grid">
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">{summary.openForMs != null ? "Open for" : "State"}</div>
          <div className="pc-alert-metric-value">{summary.openForMs != null ? formatSpan(summary.openForMs) : "Resolved"}</div>
          <Typography.Text type="secondary">
            since {formatDateTime(active.first_seen_at || active.created_at)}
          </Typography.Text>
        </div>
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">Last reported</div>
          <div className="pc-alert-metric-value">{active.last_seen_at ? relativeTime(active.last_seen_at) : "—"}</div>
          {active.last_seen_at ? <Typography.Text type="secondary">{formatDateTime(active.last_seen_at)}</Typography.Text> : null}
        </div>
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">Times it came back</div>
          <div className="pc-alert-metric-value">{summary.recurrences}</div>
          <Typography.Text type="secondary">
            {summary.occurrences} {summary.occurrences === 1 ? "occurrence" : "occurrences"}
          </Typography.Text>
        </div>
        <div className="pc-alert-metric">
          <div className="pc-alert-metric-label">First seen</div>
          <div className="pc-alert-metric-value">{summary.firstEverMs != null ? formatDateTime(summary.firstEverMs) : "—"}</div>
        </div>
      </div>

      {summary.recurrences > 0 && summary.lastResolution ? (
        <Typography.Paragraph style={{ margin: "12px 0 0" }}>
          Last resolved by <Typography.Text strong>{summary.lastResolution.by}</Typography.Text> on{" "}
          {formatDateTime(summary.lastResolution.at)}
          {summary.lastResolution.note ? ` with the note "${summary.lastResolution.note}"` : ""}, then found again. A
          longer-term fix may be needed.
        </Typography.Paragraph>
      ) : null}

      {changes.length > 0 ? (
        <div style={{ marginTop: 12 }}>
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
    </Card>
  );
}
