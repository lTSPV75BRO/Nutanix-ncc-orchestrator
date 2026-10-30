import { Card, Col, Empty, List, Progress, Row, Statistic, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { asArray, asRecord, resolveClusterName, toNumber, weightedHealthScore } from "../../utils/report";

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
    const failOnlyCount = useNCCSummary ? toNumber(cc.fail) : toNumber(cc.fail_count);
    const errCount = useNCCSummary ? toNumber(cc.error) : toNumber(cc.error_count);
    const failCount = failOnlyCount + errCount;
    const failRate = checks > 0 ? (failCount / checks) * 100 : 0;
    // Same weighted score as the Overview and the cluster ranking; the SLO
    // export (no NCC summary) carries the runner's own health_score.
    const health = useNCCSummary ? (weightedHealthScore(cc) ?? 0) : toNumber(cc.health_score);
    const status = failRate >= 2 ? "critical" : failRate >= 1 ? "at-risk" : String(cc.status || "ok");
    return {
      key: `${String(cc.address || "cluster")}-${idx}`,
      cluster: resolveClusterName(String(cc.address || "-"), clusterNameMap),
      health,
      failRate,
      status,
      failCount,
      failOnlyCount,
      errCount,
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
      title: "Health score",
      key: "health",
      width: 220,
      render: (_, row) => <Progress percent={Number(row.health.toFixed(1))} size="small" status={row.health < 75 ? "exception" : row.health < 90 ? "active" : "success"} />,
    },
    {
      title: "Fail rate",
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
    { title: "FAIL", key: "failOnly", width: 90, render: (_, row) => row.failOnlyCount },
    { title: "ERR", key: "errCount", width: 90, render: (_, row) => row.errCount },
    { title: "FAIL+ERR / Total", key: "failTotal", width: 140, render: (_, row) => `${row.failCount}/${row.checks}` },
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
            <Statistic title="Average health score" value={Number(avgHealth.toFixed(1))} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="Average fail rate" value={Number(avgFailRate.toFixed(2))} suffix="%" />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic
              title="Change in FAIL total"
              value={toNumber(reg.delta_fail_total)}
              styles={{
                content: {
                  color: toNumber(reg.delta_fail_total) > 0 ? "var(--ui-fail-text)" : toNumber(reg.delta_fail_total) < 0 ? "var(--ui-ok-text)" : undefined,
                },
              }}
            />
          </Card>
        </Col>
      </Row>

      <Typography.Title level={5}>SLO by cluster</Typography.Title>
      {clusters.length === 0 ? (
        <Empty description="No SLO cluster data found." />
      ) : (
        <Table size="small" columns={columns} dataSource={clusters} pagination={{ pageSize: 8, showSizeChanger: false }} />
      )}

      <Typography.Title level={5}>Regression summary</Typography.Title>
      <Row gutter={[12, 12]}>
        <Col xs={24} md={8}>
          <Card size="small" title="More failures">
            {increased.length ? <List size="small" dataSource={increased} renderItem={(item) => <List.Item>{item}</List.Item>} /> : <Typography.Text type="secondary">None</Typography.Text>}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title="Fewer failures">
            {decreased.length ? <List size="small" dataSource={decreased} renderItem={(item) => <List.Item>{item}</List.Item>} /> : <Typography.Text type="secondary">None</Typography.Text>}
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small" title="Unchanged">
            {unchanged.length ? <List size="small" dataSource={unchanged} renderItem={(item) => <List.Item>{item}</List.Item>} /> : <Typography.Text type="secondary">None</Typography.Text>}
          </Card>
        </Col>
      </Row>
    </Card>
  );
}
