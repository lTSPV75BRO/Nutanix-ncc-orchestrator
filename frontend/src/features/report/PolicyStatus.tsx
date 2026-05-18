import { Alert, Card, List, Typography } from "antd";

type Props = {
  violations: string[];
};

export function PolicyStatus({ violations }: Props) {
  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Policy Gates
      </Typography.Title>
      <Alert
        type={violations.length > 0 ? "error" : "success"}
        style={{ marginBottom: 8 }}
        title={violations.length > 0 ? "FAILED" : "PASS/NOT_CONFIGURED"}
      />
      {violations.length > 0 ? (
        <List
          size="small"
          bordered
          dataSource={violations}
          renderItem={(item, idx) => <List.Item key={`${item}-${idx}`}>{item}</List.Item>}
        />
      ) : (
        <Typography.Text type="secondary">No policy-gates violations for this run.</Typography.Text>
      )}
    </Card>
  );
}
