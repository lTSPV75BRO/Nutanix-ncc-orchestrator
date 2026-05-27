import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Empty, Form, InputNumber, Select, Space, Tag, Tooltip, Typography } from "antd";
import {
  ClearOutlined,
  DeleteOutlined,
  ImportOutlined,
  PlusOutlined,
  SafetyCertificateOutlined,
  SendOutlined,
} from "@ant-design/icons";
import { notify } from "../../notify";

type Props = {
  configContent: string;
  onApplyPolicyGates: (csv: string) => void;
};

type Rule = {
  metric: string;
  op: string;
  value: number;
};

type MetricMeta = {
  key: string;
  label: string;
  hint: string;
  unit?: string;
};

const METRICS: MetricMeta[] = [
  { key: "new-fails", label: "New fails", hint: "Number of checks newly failing vs the previous run." },
  { key: "resolved-fails", label: "Resolved fails", hint: "Number of previously failing checks now passing." },
  { key: "fail-rate", label: "Fail rate", hint: "Percentage of checks failing across all clusters.", unit: "%" },
  { key: "clusters-failed", label: "Failed clusters", hint: "Clusters with at least one failed check." },
  { key: "max-cluster-failures", label: "Max cluster failures", hint: "Highest fail count from a single cluster." },
  { key: "regressions", label: "Regressions", hint: "Checks that worsened in severity vs the previous run." },
  { key: "flaky-checks", label: "Flaky checks", hint: "Checks toggling between PASS/FAIL across recent runs." },
  { key: "min-health-score", label: "Min health score", hint: "Lowest cluster health score across all clusters." },
  { key: "avg-health-score", label: "Avg health score", hint: "Mean cluster health score across all clusters." },
  { key: "timeout-clusters", label: "Timeout clusters", hint: "Clusters that timed out during the run." },
  { key: "auth-failures", label: "Auth failures", hint: "Authentication errors observed during the run." },
  { key: "network-failures", label: "Network failures", hint: "Network-class errors during the run." },
  { key: "api-failures", label: "API failures", hint: "Generic API errors observed during the run." },
  { key: "parser-failures", label: "Parser failures", hint: "NCC log parsing errors during the run." },
  { key: "rate-limit-failures", label: "Rate-limit failures", hint: "HTTP 429s encountered during the run." },
  { key: "unknown-failures", label: "Unknown failures", hint: "Failures that don't fit other classes." },
];

const OPERATORS = [
  { value: ">", label: ">" },
  { value: ">=", label: "≥" },
  { value: "<", label: "<" },
  { value: "<=", label: "≤" },
  { value: "=", label: "=" },
];

function metricMeta(key: string): MetricMeta | undefined {
  return METRICS.find((m) => m.key === key);
}

function parsePolicyGatesLine(content: string): string {
  const match = content.match(/^\s*policy-gates:\s*(.+?)\s*$/m);
  if (!match) return "";
  let raw = match[1].trim();
  raw = raw.replace(/^['"]|['"]$/g, "").trim();
  return raw;
}

function ruleToToken(rule: Rule): string {
  return `${rule.metric}${rule.op}${rule.value}`;
}

function describeRule(rule: Rule): string {
  const m = metricMeta(rule.metric);
  const opLabel = OPERATORS.find((o) => o.value === rule.op)?.label ?? rule.op;
  const unit = m?.unit ?? "";
  return `${m?.label ?? rule.metric} ${opLabel} ${rule.value}${unit}`;
}

export function PolicyGateBuilderSection({ configContent, onApplyPolicyGates }: Props) {
  const [metric, setMetric] = useState<string>(METRICS[0].key);
  const [op, setOp] = useState<string>(OPERATORS[0].value);
  const [value, setValue] = useState<number>(1);
  const [rules, setRules] = useState<Rule[]>([]);
  const [loadedFromConfig, setLoadedFromConfig] = useState(false);

  // Auto-load from config the first time we have content.
  useEffect(() => {
    if (loadedFromConfig || !configContent) return;
    loadFromConfigSilent();
    setLoadedFromConfig(true);
  }, [configContent, loadedFromConfig]);

  const csv = useMemo(() => rules.map(ruleToToken).join(","), [rules]);
  const configCsv = useMemo(() => parsePolicyGatesLine(configContent), [configContent]);
  const dirty = csv !== configCsv;

  const addRule = () => {
    if (rules.some((r) => r.metric === metric && r.op === op && r.value === value)) {
      notify.warning("Duplicate rule.");
      return;
    }
    setRules((prev) => [...prev, { metric, op, value }]);
  };

  const loadFromConfigSilent = () => {
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
      parsed.push({ metric: match[1], op: match[2], value: Number(match[3]) });
    }
    setRules(parsed);
  };

  const loadFromConfig = () => {
    loadFromConfigSilent();
    notify.info("Loaded rules from current config.");
  };

  const apply = () => {
    onApplyPolicyGates(csv);
    notify.success({
      message: "Applied to config",
      description: csv ? `policy-gates: "${csv}"` : "Cleared policy-gates value.",
    });
  };

  return (
    <div>
      <Space size={10} align="center" style={{ marginBottom: 6 }}>
        <span className="config-section-icon">
          <SafetyCertificateOutlined />
        </span>
        <div>
          <Typography.Title level={4} style={{ margin: 0 }}>
            Policy Gates Builder
          </Typography.Title>
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            Compose conditions that fail the run when thresholds are exceeded. The rules are serialized into the{" "}
            <Typography.Text code>policy-gates</Typography.Text> config key.
          </Typography.Text>
        </div>
      </Space>

      <Form layout="vertical" style={{ marginTop: 16 }} onSubmitCapture={(e) => e.preventDefault()}>
        <Space size={8} wrap align="end" style={{ width: "100%" }}>
          <Form.Item label="Metric" htmlFor="policy-metric" style={{ marginBottom: 0, minWidth: 240 }}>
            <Select
              id="policy-metric"
              value={metric}
              onChange={setMetric}
              showSearch
              optionFilterProp="label"
              style={{ width: 240 }}
              options={METRICS.map((m) => ({
                value: m.key,
                label: (
                  <Tooltip placement="right" title={m.hint}>
                    <span>{m.label}</span>
                  </Tooltip>
                ),
              }))}
            />
          </Form.Item>
          <Form.Item label="Operator" htmlFor="policy-op" style={{ marginBottom: 0 }}>
            <Select id="policy-op" value={op} onChange={setOp} options={OPERATORS} style={{ width: 88 }} />
          </Form.Item>
          <Form.Item label="Value" htmlFor="policy-value" style={{ marginBottom: 0 }}>
            <InputNumber
              id="policy-value"
              name="policy-value"
              value={value}
              onChange={(v) => setValue(Number(v ?? 0))}
              style={{ width: 120 }}
              addonAfter={metricMeta(metric)?.unit}
            />
          </Form.Item>
          <Form.Item label=" " style={{ marginBottom: 0 }}>
            <Button type="primary" icon={<PlusOutlined />} onClick={addRule}>
              Add rule
            </Button>
          </Form.Item>
        </Space>
      </Form>

      <div style={{ marginTop: 16 }}>
        {rules.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No rules yet — add one above" />
        ) : (
          <Space size={[8, 8]} wrap>
            {rules.map((rule, idx) => (
              <Tooltip key={`${rule.metric}-${idx}`} title={metricMeta(rule.metric)?.hint}>
                <Tag
                  closable
                  closeIcon={<DeleteOutlined />}
                  onClose={(e) => {
                    e.preventDefault();
                    setRules((prev) => prev.filter((_, i) => i !== idx));
                  }}
                  style={{ padding: "5px 10px", borderRadius: 999, fontSize: 13 }}
                  color="processing"
                >
                  {describeRule(rule)}
                </Tag>
              </Tooltip>
            ))}
          </Space>
        )}
      </div>

      <Alert
        type="info"
        showIcon
        style={{ marginTop: 16 }}
        message={
          <Typography.Text>
            Generated value:&nbsp;
            <Typography.Text code copyable={csv ? { text: csv } : false}>
              {csv || "(empty — clears the gate)"}
            </Typography.Text>
          </Typography.Text>
        }
      />

      <Space size={8} wrap style={{ marginTop: 12 }}>
        <Button icon={<ImportOutlined />} onClick={loadFromConfig}>
          Load from config
        </Button>
        <Button icon={<ClearOutlined />} onClick={() => setRules([])} disabled={rules.length === 0}>
          Clear all
        </Button>
        <Tooltip title={dirty ? "Apply rules to the config editor" : "Already in sync with config"}>
          <Button type="primary" icon={<SendOutlined />} onClick={apply} disabled={!dirty}>
            Apply to config
          </Button>
        </Tooltip>
      </Space>
    </div>
  );
}
