import { Card, Col, Empty, List, Progress, Row, Statistic, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { asArray, asRecord, resolveClusterName, toNumber } from "../../utils/report";

type Props = {
  sloDashboard: unknown;
  nccClusterSummary?: Array<Record<string, unknown>>;
  regressionSummary: unknown;
  clusterNameMap: Record<string, string>;
};

export function SloPanel({ sloDashboard, nccClusterSummary, regressionSummary, clusterNameMap }: Props) {
  const slo = asRecord(sloDashboard);
  const useNCCSummary = Array.isArray(nccClusterSummary) && nccClusterSummary.length > 0;
  const clusters = (useNCCSummary ? nccClusterSummary || [] : asArray(slo.clusters)).map((c, idx) => {
    const cc = asRecord(c);
    const checks = useNCCSummary ? toNumber(cc.total_plugins) : toNumber(cc.checks_total);
    const failCount = useNCCSummary ? toNumber(cc.fail) + toNumber(cc.error) : toNumber(cc.fail_count);
    const failRate = checks > 0 ? (failCount / checks) * 100 : 0;
    const healthRaw = useNCCSummary ? toNumber(cc.health_rate) : toNumber(cc.health_score);
    const infoRate = checks > 0 ? (useNCCSummary ? (toNumber(cc.info) / checks) * 100 : 0) : 0;
    const unknownRate = checks > 0 ? (useNCCSummary ? (toNumber(cc.unknown) / checks) * 100 : 0) : 0;
    const health = useNCCSummary ? Math.max(0, healthRaw - failRate * 8.0 - infoRate * 2.2 - unknownRate * 3.0) : healthRaw;
    const status = failRate >= 2 ? "critical" : failRate >= 1 ? "at-risk" : String(cc.status || "ok");
    return {
      key: `${String(cc.address || "cluster")}-${idx}`,
      cluster: resolveClusterName(String(cc.address || "-"), clusterNameMap),
      health,
      failRate,
      status,
      failCount,
      checks,
    };
  });
  const avgHealth = clusters.length ? clusters.reduce((acc, c) => acc + c.health, 0) / clusters.length : 0;
  const avgFailRate = clusters.length ? clusters.reduce((acc, c) => acc + c.failRate, 0) / clusters.length : 0;

  const reg = asRecord(regressionSummary);
  const increased = asArray(reg.increased_clusters).map((v) => resolveClusterName(String(v), clusterNameMap));
  const decreased = asArray(reg.decreased_clusters).map((v) => resolveClusterName(String(v), clusterNameMap));
  const unchanged = asArray(reg.unchanged_clusters).map((v) => resolveClusterName(String(v), clusterNameMap));

  const columns: ColumnsType<(typeof clusters)[number]> = [
    { title: "Cluster", dataIndex: "cluster", key: "cluster" },
    {
      title: "Health Score",
      key: "health",
      width: 220,
      render: (_, row) => <Progress percent={Number(row.health.toFixed(1))} size="small" status={row.health < 75 ? "exception" : row.health < 90 ? "active" : "success"} />,
    },
    {
      title: "Fail Rate",
      key: "failRate",
      width: 130,
      render: (_, row) => `${row.failRate.toFixed(1)}%`,
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
      render: (v: string) => <Tag color={v.toLowerCase().includes("at-risk") ? "warning" : v.toLowerCase().includes("critical") ? "error" : "success"}>{v || "-"}</Tag>,
    },
    { title: "FAIL / Total", key: "failTotal", width: 110, render: (_, row) => `${row.failCount}/${row.checks}` },
  ];

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        SLO + Regression
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        SLO summarizes cluster reliability posture. Regression highlights FAIL changes from previous run.
      </Typography.Text>
      <Row gutter={[12, 12]} style={{ marginTop: 8, marginBottom: 8 }}>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="Clusters" value={clusters.length} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="Avg Health Score" value={Number(avgHealth.toFixed(1))} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="Avg Fail Rate" value={Number(avgFailRate.toFixed(2))} suffix="%" />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic
              title="Delta FAIL Total"
              value={toNumber(reg.delta_fail_total)}
              styles={{
                content: {
                  color: toNumber(reg.delta_fail_total) > 0 ? "#f43f5e" : toNumber(reg.delta_fail_total) < 0 ? "#22c55e" : undefined,
                },
              }}
            />
          </Card>
        </Col>
      </Row>

      <Typography.Title level={5}>SLO Dashboard</Typography.Title>
      {clusters.length === 0 ? (
        <Empty description="No SLO cluster data found." />
      ) : (
        <Table size="small" columns={columns} dataSource={clusters} pagination={{ pageSize: 8, showSizeChanger: false }} />
      )}

      <Typography.Title level={5}>Regression Summary</Typography.Title>
      <Row gutter={[12, 12]}>
        <Col xs={24} md={8}>
          <Card size="small" title="Increased Clusters">
            {increased.length ? <List size="small" dataSource={increased} renderItem={(item) => <List.Item>{item}</List.Item>} /> : <Typography.Text type="secondary">None</Typography.Text>}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title="Decreased Clusters">
            {decreased.length ? <List size="small" dataSource={decreased} renderItem={(item) => <List.Item>{item}</List.Item>} /> : <Typography.Text type="secondary">None</Typography.Text>}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title="Unchanged Clusters">
            {unchanged.length ? <List size="small" dataSource={unchanged} renderItem={(item) => <List.Item>{item}</List.Item>} /> : <Typography.Text type="secondary">None</Typography.Text>}
          </Card>
        </Col>
      </Row>
    </Card>
  );
}
