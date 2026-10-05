import { useEffect, useMemo, useState } from "react";
import { Button, Card, Col, Empty, Input, Row, Segmented, Space, Tag, Tooltip, Typography } from "antd";
import {
  CodeOutlined,
  CopyOutlined,
  DownloadOutlined,
  FileTextOutlined,
  ReloadOutlined,
  SearchOutlined,
} from "@ant-design/icons";
import { api } from "../../api/client";
import type { ReportData } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";
import { notify } from "../../notify";

type Props = {
  onError: (e: unknown) => void;
};

const SLICES: { key: keyof ReportData; label: string; description: string }[] = [
  { key: "run_summary", label: "Run summary", description: "Totals for the latest run." },
  { key: "checks_snapshot", label: "Check results", description: "Check results across clusters." },
  { key: "drilldown_diff", label: "Run comparison", description: "How this run differs from the previous one." },
  { key: "flaky_checks", label: "Flaky checks", description: "Checks that pass and fail across recent runs." },
  { key: "regression_summary", label: "Change summary", description: "What got better or worse since the previous run." },
  { key: "slo_dashboard", label: "Service health", description: "Health and failure rate for each cluster." },
];

function describe(value: unknown): string {
  if (value === null || value === undefined) return "No data";
  if (Array.isArray(value)) return `${value.length} ${value.length === 1 ? "item" : "items"}`;
  if (typeof value === "object") {
    const keys = Object.keys(value as Record<string, unknown>);
    return `${keys.length} ${keys.length === 1 ? "field" : "fields"}`;
  }
  return "Value";
}

function approximateBytes(text: string): number {
  return new TextEncoder().encode(text).length;
}

function formatBytes(bytes: number): string {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const v = bytes / Math.pow(1024, i);
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

export function JsonOutputsSection({ onError }: Props) {
  const [data, setData] = useState<ReportData | null>(null);
  const [selected, setSelected] = useLocalStorageState<keyof ReportData>(
    "settings.jsonOutputs.selected",
    "run_summary",
  );
  const [filter, setFilter] = useState("");
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const out = await api.reportData();
      setData(out);
    } catch (e) {
      onError(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  const selectedValue = data ? data[selected] : undefined;
  const fullJson = useMemo(() => JSON.stringify(selectedValue ?? {}, null, 2), [selectedValue]);

  // Light-weight client-side JSON line filter so users can quickly grep keys.
  const displayJson = useMemo(() => {
    const q = filter.trim();
    if (!q) return fullJson;
    const needle = q.toLowerCase();
    const matched = fullJson
      .split("\n")
      .filter((line) => line.toLowerCase().includes(needle))
      .join("\n");
    return matched || "(no lines match)";
  }, [fullJson, filter]);

  const sizeBytes = approximateBytes(fullJson);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(fullJson);
      notify.success("JSON copied to clipboard.");
    } catch {
      notify.warning("Clipboard unavailable.");
    }
  };

  const download = () => {
    const blob = new Blob([fullJson], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${String(selected)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const currentMeta = SLICES.find((s) => s.key === selected);

  return (
    <Card
      className="page-card"
      title={
        <Space size={10} align="center">
          <CodeOutlined className="section-header-icon" />
          <Typography.Text strong>Saved reports</Typography.Text>
        </Space>
      }
      extra={
        <Tooltip title="Reload report data">
          <Button icon={<ReloadOutlined />} loading={loading} onClick={load}>
            Refresh
          </Button>
        </Tooltip>
      }
    >
      <Row gutter={[16, 12]}>
        <Col xs={24}>
          <Segmented
            block
            value={selected}
            onChange={(v) => setSelected(v as keyof ReportData)}
            options={SLICES.map((s) => ({
              label: (
                <span style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "4px 0" }}>
                  <FileTextOutlined />
                  {s.label}
                </span>
              ),
              value: s.key,
            }))}
          />
        </Col>
      </Row>

      <Space size={8} wrap style={{ margin: "12px 0", display: "flex", justifyContent: "space-between" }}>
        <Space size={6} wrap>
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            {currentMeta?.description ?? ""}
          </Typography.Text>
          <Tag color="default">{describe(selectedValue)}</Tag>
          <Tag color="default">{formatBytes(sizeBytes)}</Tag>
        </Space>
        <Space size={6}>
          <Input
            id="json-outputs-filter"
            name="json-filter"
            aria-label="Filter JSON output lines"
            allowClear
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter lines…"
            prefix={<SearchOutlined />}
            style={{ width: 220 }}
            autoComplete="off"
          />
          <Tooltip title="Copy JSON">
            <Button icon={<CopyOutlined />} onClick={copy} />
          </Tooltip>
          <Tooltip title="Download JSON">
            <Button icon={<DownloadOutlined />} onClick={download} />
          </Tooltip>
        </Space>
      </Space>

      {fullJson === "{}" && !loading ? (
        <Empty description="No data yet. Start a run to see results here." />
      ) : (
        <CodeEditor value={displayJson} language="json" readOnly height={460} />
      )}
    </Card>
  );
}
