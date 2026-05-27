import { useEffect, useMemo, useState } from "react";
import { Button, Card, Col, Empty, Input, List, Row, Space, Tag, Tooltip, Typography } from "antd";
import {
  CopyOutlined,
  DownloadOutlined,
  FileOutlined,
  FileTextOutlined,
  FileZipOutlined,
  ReloadOutlined,
  SearchOutlined,
} from "@ant-design/icons";
import { api } from "../../api/client";
import type { ArtifactInfo } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor, inferEditorLanguage } from "../../components/CodeEditor";
import { notify } from "../../notify";

type Props = {
  onError: (e: unknown) => void;
};

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const v = bytes / Math.pow(1024, i);
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function relativeTime(iso: string): string {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return "—";
  const diff = Date.now() - t;
  if (diff < 0) return "in the future";
  if (diff < 60_000) return "just now";
  const m = Math.floor(diff / 60_000);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function iconForName(name: string) {
  const ext = name.split(".").pop()?.toLowerCase() ?? "";
  if (["zip", "gz", "tar", "tgz"].includes(ext)) return <FileZipOutlined />;
  if (["json", "yaml", "yml", "log", "txt", "csv", "md", "html"].includes(ext)) return <FileTextOutlined />;
  return <FileOutlined />;
}

export function RawOutputsSection({ onError }: Props) {
  const [artifacts, setArtifacts] = useState<ArtifactInfo[]>([]);
  const [selected, setSelected] = useLocalStorageState("settings.rawOutputs.selectedArtifact", "");
  const [raw, setRaw] = useState("");
  const [filter, setFilter] = useLocalStorageState("settings.rawOutputs.filter", "");
  const [loading, setLoading] = useState(false);

  const loadArtifacts = async () => {
    try {
      const out = await api.artifacts();
      setArtifacts(out);
      if (out.length === 0) {
        setSelected("");
        setRaw("");
        return;
      }
      const hasSelected = out.some((a) => a.name === selected);
      if (!hasSelected) {
        setSelected(out[0].name);
        setRaw("");
      }
    } catch (e) {
      onError(e);
    }
  };

  const loadRaw = async (name: string) => {
    if (!name) return;
    setLoading(true);
    try {
      const out = await api.artifactByName(name);
      setRaw(out.content || "");
    } catch (e) {
      onError(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadArtifacts();
  }, []);

  useEffect(() => {
    if (!selected) return;
    if (!artifacts.some((a) => a.name === selected)) return;
    void loadRaw(selected);
  }, [selected, artifacts]);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return artifacts;
    return artifacts.filter((a) => a.name.toLowerCase().includes(q));
  }, [filter, artifacts]);

  const totalSize = useMemo(() => artifacts.reduce((acc, a) => acc + (a.size || 0), 0), [artifacts]);

  const copyContent = async () => {
    try {
      await navigator.clipboard.writeText(raw);
      notify.success("Copied to clipboard.");
    } catch (e) {
      notify.warning("Clipboard unavailable.");
      void e;
    }
  };

  return (
    <Card
      className="page-card"
      title={
        <Space size={10} align="center">
          <FileTextOutlined className="section-header-icon" />
          <Typography.Text strong>Artifacts</Typography.Text>
          <Tag color="default">{artifacts.length} files</Tag>
          <Tag color="default">{formatBytes(totalSize)}</Tag>
        </Space>
      }
      extra={
        <Tooltip title="Refresh artifact list">
          <Button icon={<ReloadOutlined />} onClick={loadArtifacts}>
            Refresh
          </Button>
        </Tooltip>
      }
    >
      <Row gutter={[16, 16]}>
        <Col xs={24} md={9}>
          <Input
            id="raw-outputs-filter"
            name="raw-filter"
            aria-label="Search raw output artifacts"
            allowClear
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Search artifacts…"
            prefix={<SearchOutlined />}
            style={{ marginBottom: 10 }}
            autoComplete="off"
          />
          {filtered.length === 0 ? (
            <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No artifacts" />
          ) : (
            <List
              size="small"
              bordered
              style={{ maxHeight: 460, overflow: "auto" }}
              dataSource={filtered}
              renderItem={(a) => {
                const isSelected = selected === a.name;
                return (
                  <List.Item
                    key={a.name}
                    className={`artifact-row${isSelected ? " selected" : ""}`}
                    onClick={() => setSelected(a.name)}
                  >
                    <Space direction="vertical" size={2} style={{ width: "100%" }}>
                      <Space size={6} align="center">
                        <span className="artifact-row-icon">{iconForName(a.name)}</span>
                        <Typography.Text strong ellipsis style={{ maxWidth: 240 }}>
                          {a.name}
                        </Typography.Text>
                      </Space>
                      <Typography.Text type="secondary" style={{ fontSize: 11 }}>
                        {formatBytes(a.size)} · modified {relativeTime(a.mod_time)}
                      </Typography.Text>
                    </Space>
                  </List.Item>
                );
              }}
            />
          )}
        </Col>
        <Col xs={24} md={15}>
          {selected ? (
            <>
              <Space size={8} style={{ marginBottom: 10, display: "flex", justifyContent: "space-between" }}>
                <Space size={6} align="center">
                  <span className="artifact-row-icon">{iconForName(selected)}</span>
                  <Typography.Text strong copyable={{ text: selected }}>
                    {selected}
                  </Typography.Text>
                </Space>
                <Space size={6}>
                  <Tooltip title="Copy content">
                    <Button icon={<CopyOutlined />} onClick={copyContent} />
                  </Tooltip>
                  <Tooltip title="Download">
                    <Button
                      icon={<DownloadOutlined />}
                      href={`/api/v1/artifacts/${encodeURIComponent(selected)}?download=1`}
                    />
                  </Tooltip>
                  <Tooltip title="Reload">
                    <Button icon={<ReloadOutlined />} loading={loading} onClick={() => loadRaw(selected)} />
                  </Tooltip>
                </Space>
              </Space>
              <CodeEditor value={raw} language={inferEditorLanguage(selected)} readOnly height={460} />
            </>
          ) : (
            <Empty description="Select an artifact to view its contents" />
          )}
        </Col>
      </Row>
    </Card>
  );
}
