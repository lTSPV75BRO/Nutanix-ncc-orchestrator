import { useEffect, useMemo, useState } from "react";
import { Button, Card, Col, Input, Row, Space, Switch, Tag, Tooltip, Typography } from "antd";
import {
  ClearOutlined,
  DownloadOutlined,
  FileTextOutlined,
  FilterOutlined,
  ReloadOutlined,
  ToTopOutlined,
} from "@ant-design/icons";
import { api } from "../../api/client";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";

type Props = {
  onError: (e: unknown) => void;
};

function relativeTime(ms: number): string {
  if (!ms) return "—";
  const diff = Date.now() - ms;
  if (diff < 1000) return "just now";
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  return `${h}h ago`;
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const v = bytes / Math.pow(1024, i);
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

export function LogsSection({ onError }: Props) {
  const [content, setContent] = useState("");
  const [path, setPath] = useState("");
  const [auto, setAuto] = useLocalStorageState("settings.logs.autoRefresh", true);
  const [followTail, setFollowTail] = useLocalStorageState("settings.logs.followTail", true);
  const [filter, setFilter] = useLocalStorageState("settings.logs.filter", "");
  const [caseSensitive, setCaseSensitive] = useLocalStorageState("settings.logs.caseSensitive", false);
  const [lastRefreshMs, setLastRefreshMs] = useState<number>(0);
  const [, forceTick] = useState(0);
  const [jumpToLastSignal, setJumpToLastSignal] = useState(0);

  const load = async () => {
    try {
      const out = await api.runnerLogs();
      setContent(out.content || "");
      setPath(out.path || "");
      setLastRefreshMs(Date.now());
    } catch (e) {
      onError(e);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  useEffect(() => {
    if (!auto) return;
    const timer = setInterval(() => void load(), 3000);
    return () => clearInterval(timer);
  }, [auto]);

  // Re-render so the relative-time tag stays current.
  useEffect(() => {
    if (!lastRefreshMs) return;
    const id = window.setInterval(() => forceTick((n) => n + 1), 5000);
    return () => window.clearInterval(id);
  }, [lastRefreshMs]);

  const lines = useMemo(() => content.split(/\r?\n/), [content]);

  /**
   * When filtering, prefix each matching line with its original line number so
   * users can correlate matches back to the unfiltered log. The width of the
   * line-number gutter is adapted to the largest displayed number.
   */
  const filteredContent = useMemo(() => {
    const q = filter.trim();
    if (!q) return content;
    let predicate: (line: string) => boolean;
    try {
      const re = new RegExp(q, caseSensitive ? "" : "i");
      predicate = (l) => re.test(l);
    } catch {
      const needle = caseSensitive ? q : q.toLowerCase();
      predicate = (l) => (caseSensitive ? l : l.toLowerCase()).includes(needle);
    }
    const matches: { ln: number; text: string }[] = [];
    for (let i = 0; i < lines.length; i += 1) {
      if (predicate(lines[i])) matches.push({ ln: i + 1, text: lines[i] });
    }
    if (matches.length === 0) return "";
    const maxLn = matches[matches.length - 1].ln;
    const width = String(maxLn).length;
    return matches.map(({ ln, text }) => `${String(ln).padStart(width, " ")}│ ${text}`).join("\n");
  }, [content, lines, filter, caseSensitive]);

  const filteredLineCount = filter.trim()
    ? filteredContent
        .split(/\r?\n/)
        .filter((l) => l.trim().length > 0).length
    : lines.length;
  const fileSizeBytes = new TextEncoder().encode(content).length;

  return (
    <Card
      className="page-card"
      title={
        <Space size={10} align="center">
          <FileTextOutlined className="section-header-icon" />
          <div style={{ display: "inline-flex", flexDirection: "column", lineHeight: 1.15 }}>
            <Typography.Text strong>Runner Logs</Typography.Text>
            <Typography.Text type="secondary" style={{ fontSize: 12 }} copyable={path ? { text: path } : false}>
              {path || "logs/ncc-runner.log"}
            </Typography.Text>
          </div>
        </Space>
      }
      extra={
        <Space size={6} wrap>
          <Tag color="default">{filteredLineCount.toLocaleString()} {filter.trim() ? "matches" : "lines"}</Tag>
          <Tag color="default">{formatBytes(fileSizeBytes)}</Tag>
          <Tooltip title={lastRefreshMs ? new Date(lastRefreshMs).toLocaleString() : ""}>
            <Tag color={auto ? "processing" : "default"}>
              {auto ? "Auto-refresh on" : "Auto-refresh off"} · {relativeTime(lastRefreshMs)}
            </Tag>
          </Tooltip>
        </Space>
      }
    >
      <Row gutter={[12, 12]} style={{ marginBottom: 12 }}>
        <Col xs={24} md={14}>
          <Input
            allowClear
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter lines (regex or substring)…"
            prefix={<FilterOutlined />}
            suffix={
              <Tooltip title="Case sensitive">
                <Tag.CheckableTag
                  checked={caseSensitive}
                  onChange={setCaseSensitive}
                  style={{ marginInlineEnd: 0, fontSize: 11 }}
                >
                  Aa
                </Tag.CheckableTag>
              </Tooltip>
            }
          />
        </Col>
        <Col xs={24} md={10}>
          <Space size={8} wrap style={{ display: "flex", justifyContent: "flex-end" }}>
            <Space size={6}>
              <Switch checked={auto} onChange={setAuto} size="small" />
              <Typography.Text type="secondary">Auto-refresh</Typography.Text>
            </Space>
            <Space size={6}>
              <Switch checked={followTail} onChange={setFollowTail} size="small" />
              <Typography.Text type="secondary">Follow tail</Typography.Text>
            </Space>
            <Tooltip title="Jump to latest line">
              <Button icon={<ToTopOutlined rotate={180} />} onClick={() => setJumpToLastSignal((n) => n + 1)} />
            </Tooltip>
            <Tooltip title="Refresh now">
              <Button icon={<ReloadOutlined />} onClick={load} />
            </Tooltip>
            <Tooltip title="Download log">
              <Button
                icon={<DownloadOutlined />}
                onClick={() => {
                  const blob = new Blob([content], { type: "text/plain" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a");
                  a.href = url;
                  a.download = path.split("/").pop() || "ncc-runner.log";
                  a.click();
                  URL.revokeObjectURL(url);
                }}
              />
            </Tooltip>
            {filter ? (
              <Tooltip title="Clear filter">
                <Button icon={<ClearOutlined />} onClick={() => setFilter("")} />
              </Tooltip>
            ) : null}
          </Space>
        </Col>
      </Row>

      <CodeEditor
        value={filteredContent || (filter.trim() ? "(no lines match the current filter)" : "(log file is empty)")}
        language="plaintext"
        readOnly
        height={460}
        autoRevealLastLine={followTail && !filter.trim()}
        jumpToLastSignal={jumpToLastSignal}
      />
    </Card>
  );
}
