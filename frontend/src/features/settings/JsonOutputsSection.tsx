import { useEffect, useState } from "react";
import { Card, Col, Row, Select, Typography } from "antd";
import { api } from "../../api/client";
import type { ReportData } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

type Props = {
  onError: (e: unknown) => void;
};

const OUTPUT_KEYS = [
  "run_summary",
  "checks_snapshot",
  "drilldown_diff",
  "flaky_checks",
  "regression_summary",
  "slo_dashboard",
] as const;

export function JsonOutputsSection({ onError }: Props) {
  const [data, setData] = useState<ReportData | null>(null);
  const [selected, setSelected] = useLocalStorageState<(typeof OUTPUT_KEYS)[number]>("settings.jsonOutputs.selected", "run_summary");

  useEffect(() => {
    api.reportData().then(setData).catch(onError);
  }, []);

  const selectedData = data ? data[selected] : {};

  return (
    <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          JSON Outputs
        </Typography.Title>
        <Row gutter={8}>
          <Col xs={24} md={8}>
            <Typography.Text type="secondary">Artifact JSON</Typography.Text>
            <Select
              value={selected}
              onChange={(v) => setSelected(v as (typeof OUTPUT_KEYS)[number])}
              options={OUTPUT_KEYS.map((k) => ({ value: k, label: k }))}
            />
          </Col>
        </Row>
        <pre>{JSON.stringify(selectedData ?? {}, null, 2)}</pre>
    </Card>
  );
}
