import { asArray, asRecord, displayClusterName, resolveClusterName, toNumber } from "../../utils/report";
import { Card, Col, Row, Tag, Typography } from "antd";

type Props = {
  runSummary: unknown;
  filterText: string;
  selectedClusters: string[];
  clusterNameMap: Record<string, string>;
};

export function ClusterHealthWidget({ runSummary, filterText, selectedClusters, clusterNameMap }: Props) {
  const selectedSet = new Set((selectedClusters || []).map((c) => c.toLowerCase()));
  const clusters = asArray(asRecord(runSummary).clusters)
    .map((c) => asRecord(c))
    .filter((c) => {
      const name = resolveClusterName(displayClusterName(c), clusterNameMap).toLowerCase();
      const cluster = String(c.cluster || c.address || "").toLowerCase();
      if (!name.includes(filterText.toLowerCase())) return false;
      if (selectedSet.size === 0) return true;
      return selectedSet.has(name) || selectedSet.has(resolveClusterName(cluster, clusterNameMap).toLowerCase());
    });

  const sorted = clusters
    .slice()
    .sort((a, b) => toNumber(a.health_score, 0) - toNumber(b.health_score, 0))
    .slice(0, 10);

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Cluster Health Top 10 Worst
      </Typography.Title>
      {sorted.length === 0 ? (
        <Typography.Text type="secondary">No clusters available.</Typography.Text>
      ) : (
        <Row gutter={[8, 8]}>
          {sorted.map((c, idx) => (
            <Col key={`${displayClusterName(c)}-${idx}`} xs={24} sm={12} md={8} lg={6}>
              <Card size="small">
                <Typography.Text className="mono">{resolveClusterName(displayClusterName(c), clusterNameMap)}</Typography.Text>
                <div style={{ marginTop: 8 }}>
                  <Tag color={toNumber(c.health_score, 0) < 75 ? "error" : toNumber(c.health_score, 0) < 90 ? "warning" : "success"}>
                    {`${toNumber(c.health_score, 0).toFixed(1)}/100`}
                  </Tag>
                </div>
              </Card>
            </Col>
          ))}
        </Row>
      )}
    </Card>
  );
}
