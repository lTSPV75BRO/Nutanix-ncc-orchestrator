import { Card, Col, Empty, Progress, Row, Statistic, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { asArray, asRecord, resolveClusterName, toNumber } from "../../utils/report";

type Props = {
  flakyChecks: unknown;
  clusterNameMap: Record<string, string>;
};

export function FlakyChecksPanel({ flakyChecks, clusterNameMap }: Props) {
  const data = asRecord(flakyChecks);
  const checks = asArray(data.checks)
    .map((c, idx) => {
      const cc = asRecord(c);
      const transitions = toNumber(cc.transitions);
      const observations = toNumber(cc.observations);
      const instability = observations > 0 ? Math.min(100, (transitions / observations) * 100) : 0;
      return {
        key: `${String(cc.cluster || "cluster")}-${String(cc.check_name || "check")}-${idx}`,
        cluster: resolveClusterName(String(cc.cluster || "-"), clusterNameMap),
        check: String(cc.check_name || "-"),
        transitions,
        observations,
        current: String(cc.current || "-"),
        instability,
      };
    })
    .sort((a, b) => b.transitions - a.transitions);

  const columns: ColumnsType<(typeof checks)[number]> = [
    { title: "Cluster", dataIndex: "cluster", key: "cluster", width: 180 },
    { title: "Check", dataIndex: "check", key: "check" },
    { title: "Transitions", dataIndex: "transitions", key: "transitions", width: 110 },
    { title: "Observations", dataIndex: "observations", key: "observations", width: 120 },
    {
      title: "Instability",
      key: "instability",
      width: 190,
      render: (_, row) => <Progress percent={Number(row.instability.toFixed(1))} size="small" status={row.instability >= 30 ? "exception" : row.instability >= 15 ? "active" : "normal"} />,
    },
    {
      title: "Current",
      dataIndex: "current",
      key: "current",
      width: 90,
      render: (v: string) => <Tag color={v.toUpperCase() === "FAIL" ? "error" : v.toUpperCase() === "WARN" ? "warning" : "default"}>{v}</Tag>,
    },
  ];

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Flaky Checks
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Highlights checks that frequently change state across recent runs.
      </Typography.Text>
      <Row gutter={[12, 12]} style={{ marginTop: 8, marginBottom: 8 }}>
        <Col xs={24} md={8}>
          <Card size="small">
            <Statistic title="Total Flaky Checks" value={toNumber(data.total_flaky_checks, checks.length)} />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small">
            <Statistic title="Lookback Runs" value={toNumber(data.lookback_runs)} />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card size="small">
            <Statistic title="Min Transitions Threshold" value={toNumber(data.min_transitions)} />
          </Card>
        </Col>
      </Row>
      {checks.length === 0 ? (
        <Empty description="No flaky checks identified in this run history window." />
      ) : (
        <Table size="small" columns={columns} dataSource={checks} pagination={{ pageSize: 8, showSizeChanger: false }} />
      )}
    </Card>
  );
}
