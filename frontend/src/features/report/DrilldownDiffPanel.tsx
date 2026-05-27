import { Card, Col, Empty, Row, Statistic, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { asArray, asRecord, resolveClusterName, toNumber } from "../../utils/report";

type Props = {
  drilldownDiff: unknown;
  clusterNameMap: Record<string, string>;
};

export function DrilldownDiffPanel({ drilldownDiff, clusterNameMap }: Props) {
  const diff = asRecord(drilldownDiff);
  const clusters = asArray(diff.clusters).map((c, idx) => {
    const cc = asRecord(c);
    const newFailures = asArray(cc.new_failures);
    const resolvedFailures = asArray(cc.resolved_failures);
    const severityChanges = asArray(cc.severity_changes);
    return {
      key: `${String(cc.address || "cluster")}-${idx}`,
      cluster: resolveClusterName(String(cc.address || "-"), clusterNameMap),
      newFail: newFailures.length,
      resolvedFail: resolvedFailures.length,
      severityChanges: severityChanges.length,
      net: newFailures.length - resolvedFailures.length,
    };
  });

  const columns: ColumnsType<(typeof clusters)[number]> = [
    { title: "Cluster", dataIndex: "cluster", key: "cluster" },
    { title: "New FAIL", dataIndex: "newFail", key: "newFail", width: 110 },
    { title: "Resolved FAIL", dataIndex: "resolvedFail", key: "resolvedFail", width: 130 },
    { title: "Severity Changes", dataIndex: "severityChanges", key: "severityChanges", width: 140 },
    {
      title: "Net",
      dataIndex: "net",
      key: "net",
      width: 90,
      render: (v: number) => (v > 0 ? <Tag color="error">{`+${v}`}</Tag> : v < 0 ? <Tag color="success">{v}</Tag> : <Tag>0</Tag>),
    },
  ];

  const newFailCount = toNumber(diff.new_fail_count, clusters.reduce((acc, c) => acc + c.newFail, 0));
  const resolvedFailCount = toNumber(diff.resolved_fail_count, clusters.reduce((acc, c) => acc + c.resolvedFail, 0));
  const netFail = newFailCount - resolvedFailCount;

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Drilldown Diff
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Compare signal between current and previous run to spot newly introduced failures quickly.
      </Typography.Text>
      <Row gutter={[12, 12]} style={{ marginTop: 8, marginBottom: 8 }}>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="New FAIL" value={newFailCount} styles={{ content: { color: "#f43f5e" } }} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="Resolved FAIL" value={resolvedFailCount} styles={{ content: { color: "#22c55e" } }} />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic
              title="Net FAIL Change"
              value={netFail}
              styles={{ content: { color: netFail > 0 ? "#f43f5e" : netFail < 0 ? "#22c55e" : undefined } }}
            />
          </Card>
        </Col>
        <Col xs={24} md={6}>
          <Card size="small">
            <Statistic title="Impacted Clusters" value={clusters.length} />
          </Card>
        </Col>
      </Row>
      {clusters.length === 0 ? (
        <Empty description="No drilldown diff clusters found." />
      ) : (
        <Table size="small" columns={columns} dataSource={clusters} pagination={{ pageSize: 8, showSizeChanger: false }} />
      )}
    </Card>
  );
}
