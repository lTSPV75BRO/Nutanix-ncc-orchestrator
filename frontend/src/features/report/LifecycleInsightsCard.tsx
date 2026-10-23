import { useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Col, Empty, Row, Skeleton, Statistic, Table, Tag, Typography } from "antd";
import { api } from "../../api/client";
import { formatDateTime } from "../../utils/datetime";
import { notify, notifyError } from "../../notify";
import { AGE_BUCKETS, buildLifecycleInsights, formatSpan, NCC_ALERTS_QUERY_KEY } from "../../utils/alertLifecycle";

/**
 * Insights card built from the NCC alert lifecycle store: how long open alerts
 * have been waiting, how quickly they get handled, which findings keep coming
 * back, and which open alerts the latest runs no longer report.
 */
export function LifecycleInsightsCard({ enabled }: { enabled: boolean }) {
  const alerts = useQuery({
    queryKey: [NCC_ALERTS_QUERY_KEY, "all"],
    queryFn: () => api.nccAlertsAll({ include_closed: true }),
    staleTime: 30_000,
    enabled,
  });
  const insights = useMemo(() => buildLifecycleInsights(alerts.data ?? []), [alerts.data]);
  const queryClient = useQueryClient();
  // These alerts are not in the latest run, so the Dashboard table cannot show
  // them; resolve them here instead.
  const resolve = useMutation({
    mutationFn: (id: string) => api.nccAlertAction(id, "resolve"),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: [NCC_ALERTS_QUERY_KEY] }),
        queryClient.invalidateQueries({ queryKey: ["ncc-dispositions"] }),
      ]);
      notify.success("Resolved. A later run that finds the same check opens it again.");
    },
    onError: (error) => notifyError(error, "Could not resolve the alert"),
  });

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Alert aging and recurrence
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        How long open NCC alerts have been waiting, how fast they get handled, and which ones keep coming back.
      </Typography.Text>

      {alerts.isLoading ? (
        <Skeleton active style={{ marginTop: 16 }} />
      ) : alerts.isError ? (
        <Typography.Paragraph type="secondary" style={{ marginTop: 16 }}>
          Alert history is not available from this server.
        </Typography.Paragraph>
      ) : (alerts.data ?? []).length === 0 ? (
        <Empty style={{ marginTop: 16 }} description="No NCC alert history yet." />
      ) : (
        <>
          <Row gutter={[12, 12]} style={{ marginTop: 16 }}>
            {AGE_BUCKETS.map((b) => (
              <Col xs={12} md={6} key={b.key}>
                <Statistic
                  title={`Open ${b.label.toLowerCase()}`}
                  value={insights.buckets[b.key]}
                  styles={{ content: b.key === "older" && insights.buckets.older > 0 ? { color: "#f43f5e" } : undefined }}
                />
              </Col>
            ))}
            <Col xs={12} md={6}>
              <Statistic
                title="Typical time to acknowledge"
                value={formatSpan(insights.medianAckMs)}
                suffix={insights.ackSamples > 0 ? <Typography.Text type="secondary">({insights.ackSamples})</Typography.Text> : null}
              />
            </Col>
            <Col xs={12} md={6}>
              <Statistic
                title="Typical time to resolve"
                value={formatSpan(insights.medianResolveMs)}
                suffix={insights.resolveSamples > 0 ? <Typography.Text type="secondary">({insights.resolveSamples})</Typography.Text> : null}
              />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title="Came back after resolve" value={insights.recurring.length} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic
                title="Open but no longer reported"
                value={insights.looksFixed.length}
                styles={{ content: insights.looksFixed.length > 0 ? { color: "#f59e0b" } : undefined }}
              />
            </Col>
          </Row>
          <Typography.Text type="secondary" style={{ display: "block", marginTop: 4 }}>
            Typical times are medians, measured from the first run that reported each alert; the count of alerts measured is in brackets.
          </Typography.Text>

          {insights.clusters.length > 0 ? (
            <Table
              style={{ marginTop: 16 }}
              size="small"
              pagination={false}
              rowKey="cluster"
              dataSource={insights.clusters.slice(0, 8)}
              columns={[
                { title: "Cluster", dataIndex: "name", key: "name" },
                ...AGE_BUCKETS.map((b) => ({ title: b.label, dataIndex: b.key, key: b.key, width: 120, align: "right" as const })),
                { title: "Oldest", key: "oldest", width: 100, align: "right" as const, render: (_: unknown, r: { oldestMs: number }) => formatSpan(r.oldestMs) },
              ]}
            />
          ) : null}

          {insights.recurring.length > 0 ? (
            <>
              <Typography.Title level={5} style={{ marginTop: 20 }}>
                Keeps coming back
              </Typography.Title>
              <Typography.Text type="secondary">
                Resolved, then found again by a later run. These usually need a lasting fix or a support case.
              </Typography.Text>
              <Table
                style={{ marginTop: 8 }}
                size="small"
                pagination={false}
                rowKey="findingId"
                dataSource={insights.recurring.slice(0, 8)}
                columns={[
                  { title: "Cluster", dataIndex: "clusterName", key: "cluster", width: 180 },
                  { title: "Check", dataIndex: "check", key: "check" },
                  { title: "Came back", dataIndex: "recurrences", key: "recurrences", width: 110, align: "right" },
                  {
                    title: "Last resolved",
                    key: "last",
                    width: 220,
                    render: (_: unknown, r) =>
                      r.lastResolution ? `${r.lastResolution.by} · ${formatDateTime(r.lastResolution.at)}` : "—",
                  },
                  {
                    title: "Now",
                    key: "now",
                    width: 90,
                    render: (_: unknown, r) => (r.openNow ? <Tag color="red">Open</Tag> : <Tag>Handled</Tag>),
                  },
                ]}
              />
            </>
          ) : null}

          {insights.looksFixed.length > 0 ? (
            <>
              <Typography.Title level={5} style={{ marginTop: 20 }}>
                Open but no longer reported
              </Typography.Title>
              <Typography.Text type="secondary">
                The latest runs no longer find these checks. Resolve them if the issue is fixed.
              </Typography.Text>
              <Table
                style={{ marginTop: 8 }}
                size="small"
                pagination={false}
                rowKey="id"
                dataSource={insights.looksFixed.slice(0, 8)}
                columns={[
                  { title: "Cluster", key: "cluster", width: 180, render: (_: unknown, a) => a.cluster_name || a.cluster },
                  { title: "Check", dataIndex: "check", key: "check" },
                  { title: "State", dataIndex: "state", key: "state", width: 120, render: (s: string) => <Tag>{s}</Tag> },
                  {
                    title: "Not reported since",
                    key: "since",
                    width: 200,
                    render: (_: unknown, a) => (a.not_detected_since_run ? formatDateTime(a.not_detected_since_run) : "—"),
                  },
                  {
                    title: "",
                    key: "action",
                    width: 100,
                    render: (_: unknown, a) => (
                      <Button
                        size="small"
                        loading={resolve.isPending && resolve.variables === a.id}
                        disabled={resolve.isPending}
                        onClick={() => resolve.mutate(a.id)}
                      >
                        Resolve
                      </Button>
                    ),
                  },
                ]}
              />
            </>
          ) : null}
        </>
      )}
    </Card>
  );
}
