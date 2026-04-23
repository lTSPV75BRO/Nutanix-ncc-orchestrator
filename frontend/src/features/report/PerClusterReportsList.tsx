import type { RunInfo } from "../../api/types";
import { Card, List, Typography } from "antd";

type Props = {
  runs: RunInfo[];
};

export function PerClusterReportsList({ runs }: Props) {
  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Per-cluster Reports
      </Typography.Title>
      {runs.length === 0 ? (
        <Typography.Text type="secondary">No run history directories found.</Typography.Text>
      ) : (
        <List
          size="small"
          dataSource={runs}
          renderItem={(run) => (
            <List.Item key={run.id}>
              <span>
                <span className="mono">{run.id}</span> {run.mod_time} {run.has_index ? "index" : ""}
              </span>
            </List.Item>
          )}
        />
      )}
    </Card>
  );
}
