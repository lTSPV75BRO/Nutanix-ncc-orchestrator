import { useCallback, useEffect, useState } from "react";
import { Button, Card, Empty, List, Space, Tag, Typography } from "antd";
import { api } from "../../api/client";
import { CodeEditor, inferEditorLanguage } from "../../components/CodeEditor";
import type { ConfigRelatedFileInfo } from "../../api/types";
import { PolicyGateBuilderSection } from "./PolicyGateBuilderSection";

type Props = {
  onError: (e: unknown) => void;
};

function applyPolicyGatesToConfigContent(content: string, gatesCsv: string): string {
  const line = `policy-gates: "${gatesCsv}"`;
  const pattern = /^\s*policy-gates:\s*.*$/m;
  if (pattern.test(content)) {
    return content.replace(pattern, line);
  }
  const trimmed = content.trimEnd();
  if (!trimmed) return `${line}\n`;
  return `${trimmed}\n${line}\n`;
}

export function ConfigSection({ onError }: Props) {
  const [content, setContent] = useState("");
  const [out, setOut] = useState("");
  const [configPath, setConfigPath] = useState("");
  const [files, setFiles] = useState<ConfigRelatedFileInfo[]>([]);
  const [activeFile, setActiveFile] = useState<string>("");
  const [activeFileContent, setActiveFileContent] = useState("");

  const refreshConfigFiles = async (preserveActivePath?: string) => {
    const resp = await api.listConfigFiles();
    const next = resp.items ?? [];
    setFiles(next);
    const selectedPath = preserveActivePath || activeFile;
    const exists = next.some((it) => it.path === selectedPath);
    if (selectedPath && exists) {
      setActiveFile(selectedPath);
      return next;
    }
    if (!selectedPath && next.length > 0) {
      setActiveFile(next[0].path);
      return next;
    }
    if (!exists) {
      setActiveFile("");
      setActiveFileContent("");
    }
    return next;
  };

  const load = useCallback(async (suppressError = false) => {
    try {
      const cfg = await api.loadConfig();
      setContent(cfg.content ?? "");
      setConfigPath(cfg.path ?? "");
      const related = await refreshConfigFiles();
      setOut(
        JSON.stringify(
          {
            config: cfg,
            related_files_count: related.length,
          },
          null,
          2,
        ),
      );
      return true;
    } catch (e) {
      if (!suppressError) {
        onError(e);
      }
      return false;
    }
  }, [activeFile, onError]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      // Retry briefly to handle API startup race right after v2-start.
      for (let attempt = 0; attempt < 3; attempt += 1) {
        if (cancelled) {
          return;
        }
        const ok = await load(attempt < 2);
        if (ok) {
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [load]);

  const save = async () => {
    try {
      const resp = await api.saveConfig(content);
      await refreshConfigFiles(activeFile);
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const loadRelatedFile = async (path: string) => {
    try {
      const resp = await api.loadConfigFile(path);
      setActiveFile(path);
      setActiveFileContent(resp.content ?? "");
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const saveRelatedFile = async () => {
    if (!activeFile) {
      return;
    }
    try {
      const resp = await api.saveConfigFile(activeFile, activeFileContent);
      await refreshConfigFiles(activeFile);
      setOut(JSON.stringify(resp, null, 2));
    } catch (e) {
      onError(e);
    }
  };

  const applyPolicyGates = (csv: string) => {
    setContent((prev) => applyPolicyGatesToConfigContent(prev, csv));
  };

  return (
    <Card className="page-card">
      <Typography.Title level={4} className="section-title">
        Application Config
      </Typography.Title>
      <Space size={8} style={{ marginBottom: 12 }}>
        <Button onClick={() => void load()}>Load Config</Button>
        <Button type="primary" onClick={save}>
          Save Config
        </Button>
      </Space>
      {configPath ? (
        <Typography.Text type="secondary" style={{ display: "block", marginBottom: 8 }}>
          Loaded: {configPath} ({content.length} chars)
        </Typography.Text>
      ) : null}
      <CodeEditor value={content} onChange={setContent} language="yaml" height={320} />
      <PolicyGateBuilderSection configContent={content} onApplyPolicyGates={applyPolicyGates} />

      <div style={{ marginTop: 16 }}>
        <Space size={8} style={{ marginBottom: 8 }}>
          <Typography.Text strong>Config-Referenced Files</Typography.Text>
          <Button size="small" onClick={() => refreshConfigFiles(activeFile)}>
            Refresh
          </Button>
        </Space>
        {files.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No related files detected in config yet" />
        ) : (
          <List
            bordered
            size="small"
            dataSource={files}
            renderItem={(item) => (
              <List.Item
                actions={[
                  <Button key="use" size="small" onClick={() => loadRelatedFile(item.path)}>
                    Load
                  </Button>,
                ]}
              >
                <Space direction="vertical" size={0}>
                  <Typography.Text code>{item.key}</Typography.Text>
                  <Typography.Text>{item.path}</Typography.Text>
                  {item.resolved_path ? (
                    <Typography.Text type="secondary">{item.resolved_path}</Typography.Text>
                  ) : (
                    <Typography.Text type="warning">Path escapes repo root</Typography.Text>
                  )}
                </Space>
                <Tag color={item.exists ? "green" : "default"}>{item.exists ? "exists" : "missing"}</Tag>
              </List.Item>
            )}
          />
        )}
      </div>

      {activeFile ? (
        <div style={{ marginTop: 16 }}>
          <Space size={8} style={{ marginBottom: 8 }}>
            <Typography.Text strong>Editing: {activeFile}</Typography.Text>
            <Button onClick={() => loadRelatedFile(activeFile)}>Reload</Button>
            <Button type="primary" onClick={saveRelatedFile}>
              Save File
            </Button>
          </Space>
          <CodeEditor
            value={activeFileContent}
            onChange={setActiveFileContent}
            language={inferEditorLanguage(activeFile)}
            height={260}
          />
        </div>
      ) : null}

      <div style={{ marginTop: 12 }}>
        <Typography.Text type="secondary">Response</Typography.Text>
        <CodeEditor value={out} language="json" height={220} readOnly />
      </div>
    </Card>
  );
}
