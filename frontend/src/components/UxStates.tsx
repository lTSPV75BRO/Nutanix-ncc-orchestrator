import type { ReactNode } from "react";
import { Alert, Button, Card, Empty, Skeleton, Space, Typography } from "antd";
import { ReloadOutlined } from "@ant-design/icons";

export function LoadingStateCard({ rows = 4, title }: { rows?: number; title?: string }) {
  return (
    <Card className="page-card">
      {title ? (
        <Typography.Title level={5} className="section-title" style={{ marginBottom: 8 }}>
          {title}
        </Typography.Title>
      ) : null}
      <Skeleton active paragraph={{ rows }} />
    </Card>
  );
}

export function EmptyStateCard({
  title,
  description,
  cta,
}: {
  title: string;
  description?: ReactNode;
  cta?: ReactNode;
}) {
  return (
    <Card className="page-card">
      <Empty description={title}>
        {description ? (
          <Typography.Paragraph type="secondary" style={{ marginTop: 8 }}>
            {description}
          </Typography.Paragraph>
        ) : null}
        {cta ? <div style={{ marginTop: 6 }}>{cta}</div> : null}
      </Empty>
    </Card>
  );
}

export function ErrorStateCard({
  title,
  error,
  onRetry,
  retryLabel = "Retry",
}: {
  title: string;
  error?: ReactNode;
  onRetry?: () => void;
  retryLabel?: string;
}) {
  return (
    <Card className="page-card">
      <Space direction="vertical" size={12} style={{ width: "100%" }}>
        <Alert
          type="error"
          showIcon
          message={title}
          description={error ? <Typography.Text type="secondary">{error}</Typography.Text> : undefined}
        />
        {onRetry ? (
          <Button icon={<ReloadOutlined />} onClick={onRetry}>
            {retryLabel}
          </Button>
        ) : null}
      </Space>
    </Card>
  );
}

