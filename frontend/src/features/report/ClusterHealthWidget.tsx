import { asArray, asRecord, displayClusterName, resolveClusterName, toNumber } from "../../utils/report";
import { Card, Col, Progress, Row, Tag, Typography } from "antd";

type Props = {
  runSummary: unknown;
  nccClusterSummary?: Array<Record<string, unknown>>;
  filterText: string;
  selectedClusters: string[];
  clusterNameMap: Record<string, string>;
};

export function ClusterHealthWidget({ runSummary, nccClusterSummary, filterText, selectedClusters, clusterNameMap }: Props) {
  const selectedSet = new Set((selectedClusters || []).map((c) => c.toLowerCase()));
  const useNCCSummary = Array.isArray(nccClusterSummary) && nccClusterSummary.length > 0;
  const clusters = (useNCCSummary ? nccClusterSummary || [] : asArray(asRecord(runSummary).clusters))
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
    .sort((a, b) => {
      const aScore = useNCCSummary ? toNumber(a.health_rate, 0) : toNumber(a.health_score, 0);
      const bScore = useNCCSummary ? toNumber(b.health_rate, 0) : toNumber(b.health_score, 0);
      return aScore - bScore;
    })
    .slice(0, 10);

  const penalizedHealth = (row: Record<string, unknown>) => {
    const passRate = toNumber(row.health_rate, 0);
    const total = toNumber(row.total_plugins, 0);
    if (total <= 0) return passRate;
    const failRate = (toNumber(row.fail, 0) / total) * 100;
    const errorRate = (toNumber(row.error, 0) / total) * 100;
    const infoRate = (toNumber(row.info, 0) / total) * 100;
    const penalty = failRate * 6.0 + errorRate * 4.0 + infoRate * 0.8;
    return Math.max(0, passRate - penalty);
  };

  const trendText = (failRate: number, adjustedScore: number, failCount: number, errorCount: number) => {
    if (failCount > 0 || adjustedScore < 90) return { label: "Trend: worsening", color: "error" as const };
    if (errorCount > 0 || failRate >= 1 || adjustedScore < 95) return { label: "Trend: watchlist", color: "warning" as const };
    return { label: "Trend: stable", color: "success" as const };
  };

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Lowest Health Clusters (Top 10)
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Clusters ranked by penalized health score (weighted Fail/Error/Info impact) with risk trend hints.
      </Typography.Text>
      {sorted.length === 0 ? (
        <Typography.Text type="secondary">No clusters available.</Typography.Text>
      ) : (
        <Row gutter={[8, 8]}>
          {sorted.map((c, idx) => (
            <Col key={`${displayClusterName(c)}-${idx}`} xs={24} sm={12} md={8} lg={6}>
              <Card size="small">
                <Typography.Text className="mono">{resolveClusterName(displayClusterName(c), clusterNameMap)}</Typography.Text>
                <div style={{ marginTop: 8 }}>
                  {(() => {
                    const score = useNCCSummary ? penalizedHealth(c) : toNumber(c.health_score, 0);
                    return (
                  <Progress
                    percent={Number(score.toFixed(1))}
                    size="small"
                    status={score < 75 ? "exception" : score < 90 ? "active" : "success"}
                    format={(percent) => `${percent ?? 0}/100`}
                  />
                    );
                  })()}
                </div>
                <div style={{ marginTop: 8 }}>
                  {(() => {
                    const failRate = useNCCSummary
                      ? (toNumber(c.total_plugins, 0) > 0 ? ((toNumber(c.fail, 0) + toNumber(c.error, 0)) / toNumber(c.total_plugins, 1)) * 100 : 0)
                      : toNumber(c.fail_rate_percent, 0);
                    const adjustedScore = useNCCSummary ? penalizedHealth(c) : toNumber(c.health_score, 0);
                    const trend = trendText(failRate, adjustedScore, toNumber(c.fail, 0), toNumber(c.error, 0));
                    return (
                      <Tag color={trend.color}>
                        {trend.label}
                      </Tag>
                    );
                  })()}
                  {(() => {
                    const failRate = useNCCSummary
                      ? (toNumber(c.total_plugins, 0) > 0 ? ((toNumber(c.fail, 0) + toNumber(c.error, 0)) / toNumber(c.total_plugins, 1)) * 100 : 0)
                      : toNumber(c.fail_rate_percent, 0);
                    return (
                      <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
                        Fail rate: {failRate.toFixed(1)}%
                      </Typography.Text>
                    );
                  })()}
                </div>
              </Card>
            </Col>
          ))}
        </Row>
      )}
    </Card>
  );
}
