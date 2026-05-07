import { useMemo, useState } from "react";
import { Button, Card, InputNumber, List, Select, Space, Tag, Typography } from "antd";

type Props = {
  configContent: string;
  onApplyPolicyGates: (csv: string) => void;
};

type Rule = {
  metric: string;
  op: string;
  value: number;
};

const METRICS = [
  "new-fails",
  "resolved-fails",
  "fail-rate",
  "clusters-failed",
  "max-cluster-failures",
  "regressions",
  "flaky-checks",
  "min-health-score",
  "avg-health-score",
  "timeout-clusters",
  "auth-failures",
  "network-failures",
  "api-failures",
  "parser-failures",
  "rate-limit-failures",
  "unknown-failures",
];

const OPERATORS = [">", ">=", "<", "<=", "="];

function parsePolicyGatesLine(content: string): string {
  const match = content.match(/^\s*policy-gates:\s*(.+?)\s*$/m);
  if (!match) return "";
  let raw = match[1].trim();
  raw = raw.replace(/^['"]|['"]$/g, "").trim();
  return raw;
}

export function PolicyGateBuilderSection({ configContent, onApplyPolicyGates }: Props) {
  const [metric, setMetric] = useState<string>(METRICS[0]);
  const [op, setOp] = useState<string>(OPERATORS[0]);
  const [value, setValue] = useState<number>(1);
  const [rules, setRules] = useState<Rule[]>([]);

  const csv = useMemo(() => rules.map((r) => `${r.metric}${r.op}${r.value}`).join(","), [rules]);

  const addRule = () => {
    setRules((prev) => [...prev, { metric, op, value }]);
  };

  const loadFromConfig = () => {
    const raw = parsePolicyGatesLine(configContent);
    if (!raw) {
      setRules([]);
      return;
    }
    const parsed: Rule[] = [];
    for (const token of raw.split(",")) {
      const v = token.trim();
      if (!v) continue;
      const match = v.match(/^([a-z-]+)\s*(>=|<=|=|>|<)\s*(-?\d+(?:\.\d+)?)$/i);
      if (!match) continue;
      parsed.push({
        metric: match[1],
        op: match[2],
        value: Number(match[3]),
      });
    }
    setRules(parsed);
  };

  return (
    <Card className="page-card" style={{ marginTop: 16 }}>
      <Typography.Title level={5} className="section-title">
        Policy Gate Builder
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Build policy gates visually, then apply the generated CSV into config.
      </Typography.Text>
      <Space size={8} wrap style={{ marginTop: 8, marginBottom: 8 }}>
        <Select value={metric} style={{ minWidth: 220 }} options={METRICS.map((m) => ({ value: m, label: m }))} onChange={setMetric} />
        <Select value={op} style={{ width: 90 }} options={OPERATORS.map((x) => ({ value: x, label: x }))} onChange={setOp} />
        <InputNumber value={value} onChange={(v) => setValue(Number(v ?? 0))} />
        <Button onClick={addRule}>Add Rule</Button>
        <Button onClick={loadFromConfig}>Load from Config</Button>
        <Button onClick={() => setRules([])}>Clear</Button>
      </Space>
      <List
        bordered
        size="small"
        locale={{ emptyText: "No policy gate rules added yet" }}
        dataSource={rules}
        renderItem={(r, idx) => (
          <List.Item
            actions={[
              <Button key="remove" size="small" onClick={() => setRules((prev) => prev.filter((_, i) => i !== idx))}>
                Remove
              </Button>,
            ]}
          >
            <Tag>{`${r.metric}${r.op}${r.value}`}</Tag>
          </List.Item>
        )}
      />
      <div style={{ marginTop: 8 }}>
        <Typography.Text type="secondary">Generated policy-gates</Typography.Text>
        <pre style={{ marginTop: 4 }}>{csv || "(empty)"}</pre>
        <Button type="primary" onClick={() => onApplyPolicyGates(csv)}>
          Apply to Config Editor
        </Button>
      </div>
    </Card>
  );
}
