import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Alert, Space, Switch, Tag, Typography } from "antd";
import { AppstoreOutlined, LoginOutlined, ThunderboltOutlined } from "@ant-design/icons";
import type { ReactNode } from "react";
import { api } from "../../api/client";
import { notifyError } from "../../notify";
import { ItemList, Panel } from "../../ui";
import { FEATURE_CATALOG, FEATURE_GROUPS, type FeatureFlags, type FeatureKey } from "../features/featureCatalog";
import { useFeatureFlags } from "../features/useFeatureFlags";

const GROUP_ICONS: Record<string, ReactNode> = {
  views: <AppstoreOutlined />,
  actions: <ThunderboltOutlined />,
  signin: <LoginOutlined />,
};

/** Settings → Features: one panel per group, one switch per feature. */
export function FeaturesSection() {
  const queryClient = useQueryClient();
  const flags = useFeatureFlags();
  const update = useMutation({
    mutationFn: (patch: Partial<FeatureFlags>) => api.updateFeatureFlags(patch),
    onSuccess: async (next) => {
      queryClient.setQueryData(["feature-flags"], next);
    },
    onError: (err) => notifyError(err, "Couldn't save the change"),
  });

  const current = flags.data;
  const setFlag = (key: FeatureKey, enabled: boolean) => {
    update.mutate({ [key]: enabled });
  };

  return (
    <div className="ui-stack">
      {flags.isError ? (
        <Alert type="error" showIcon title="Couldn't load the features" description={String((flags.error as Error)?.message || "")} />
      ) : null}
      {FEATURE_GROUPS.map((group) => (
        <Panel key={group.key} title={group.title} icon={GROUP_ICONS[group.key]} description={group.description} flush>
          <ItemList
            items={FEATURE_CATALOG.filter((spec) => (spec.group ?? "views") === group.key)}
            itemKey={(spec) => spec.key}
            render={(spec) => (
              <div>
                <Space size={8} wrap>
                  <Typography.Text strong>{spec.title}</Typography.Text>
                  {spec.beta ? <Tag color="gold">Beta</Tag> : null}
                </Space>
                <Typography.Paragraph type="secondary" className="feature-flag-text">
                  Off: {spec.saves.charAt(0).toLowerCase() + spec.saves.slice(1)}
                </Typography.Paragraph>
              </div>
            )}
            actions={(spec) => (
              <Switch
                aria-label={spec.title}
                checked={current ? current[spec.key] !== false : true}
                loading={flags.isLoading || update.isPending}
                onChange={(checked) => setFlag(spec.key, checked)}
              />
            )}
          />
        </Panel>
      ))}
    </div>
  );
}
