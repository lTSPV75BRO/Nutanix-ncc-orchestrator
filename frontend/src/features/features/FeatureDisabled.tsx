import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Card, Typography } from "antd";
import { api } from "../../api/client";
import { useAuth } from "../../auth/AuthContext";
import { notify, notifyError } from "../../notify";
import { featureSpec, type FeatureKey } from "./featureCatalog";

export function FeatureDisabled({ feature }: { feature: FeatureKey }) {
  const spec = featureSpec(feature);
  const { isAdmin } = useAuth();
  const queryClient = useQueryClient();
  const enable = useMutation({
    mutationFn: () => api.updateFeatureFlags({ [spec.key]: true }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["feature-flags"] });
      notify.success(`${spec.title} is on.`);
    },
    onError: (err) => notifyError(err, `Could not enable ${spec.title}`),
  });

  return (
    <Card className="page-card">
      <Alert
        type="info"
        showIcon
        title={`${spec.title} is turned off`}
        description={
          <div>
            <Typography.Paragraph style={{ marginBottom: 8 }}>
              An administrator turned this off. {spec.saves}
            </Typography.Paragraph>
            {isAdmin ? (
              <Button type="primary" loading={enable.isPending} onClick={() => enable.mutate()}>
                Enable {spec.title}
              </Button>
            ) : (
              <Typography.Text type="secondary">Ask an administrator to turn it on from Settings → Features.</Typography.Text>
            )}
          </div>
        }
      />
    </Card>
  );
}
