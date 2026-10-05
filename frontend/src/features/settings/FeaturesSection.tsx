import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Alert, Card, Space, Switch, Tag, Typography } from "antd";
import { api } from "../../api/client";
import { notifyError } from "../../notify";
import { FEATURE_CATALOG, type FeatureFlags, type FeatureKey } from "../features/featureCatalog";
import { useFeatureFlags } from "../features/useFeatureFlags";

export function FeaturesSection() {
  const queryClient = useQueryClient();
  const flags = useFeatureFlags();
  const update = useMutation({
    mutationFn: (patch: Partial<FeatureFlags>) => api.updateFeatureFlags(patch),
    onSuccess: async (next) => {
      queryClient.setQueryData(["feature-flags"], next);
    },
    onError: (err) => notifyError(err, "Could not save feature settings"),
  });

  const current = flags.data;
  const setFlag = (key: FeatureKey, enabled: boolean) => {
    update.mutate({ [key]: enabled });
  };

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="tile-title">
        Features
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        Choose which optional views are available. Changes apply for everyone right away. Health checks keep running.
      </Typography.Paragraph>
      {flags.isError ? (
        <Alert type="error" showIcon title="Could not load feature settings" description={String((flags.error as Error)?.message || "Unknown error")} />
      ) : null}
      <Space orientation="vertical" size={12} style={{ width: "100%" }}>
        {FEATURE_CATALOG.map((spec) => {
          const on = current ? current[spec.key] !== false : true;
          return (
            <div key={spec.key} className="feature-flag-row">
              <div>
                <Space size={8} wrap>
                  <Typography.Text strong>{spec.title}</Typography.Text>
                  {spec.beta ? <Tag color="gold">Beta</Tag> : null}
                  <Tag color={on ? "green" : "default"}>{on ? "On" : "Off"}</Tag>
                </Space>
                <Typography.Paragraph type="secondary" style={{ margin: "4px 0 0" }}>
                  {spec.saves}
                </Typography.Paragraph>
              </div>
              <Switch
                aria-label={spec.title}
                checked={on}
                loading={flags.isLoading || update.isPending}
                onChange={(checked) => setFlag(spec.key, checked)}
              />
            </div>
          );
        })}
      </Space>
    </Card>
  );
}
