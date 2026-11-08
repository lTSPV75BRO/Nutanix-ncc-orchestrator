import { useCallback, useEffect, useMemo, useState } from "react";
import { Alert, Button, Drawer, Segmented, Select, Switch, Tooltip, Typography } from "antd";
import {
  ApiOutlined,
  CalendarOutlined,
  CloudServerOutlined,
  CopyOutlined,
  DesktopOutlined,
  DownloadOutlined,
  FileTextOutlined,
  FilterOutlined,
  ReloadOutlined,
  SafetyOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { LogLevelName, LogSource } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor } from "../../components/CodeEditor";
import { notify } from "../../notify";
import { EmptyState, ListToolbar, matchesSearch, middleEllipsis, Panel, StatusBadge, VirtualList, type Tone } from "../../ui";

type Props = {
  onError: (e: unknown) => void;
};

type Level = "error" | "warn" | "info" | "debug";

/** One parsed log line. */
type Entry = {
  n: number;
  time: string;
  level: Level;
  message: string;
  /** Short "key=value" summary of the most useful fields. */
  summary: string;
  fields: Record<string, unknown> | null;
  raw: string;
};

const SOURCE_ICONS: Record<string, React.ReactNode> = {
  runner: <ThunderboltOutlined />,
  api: <ApiOutlined />,
  "api-console": <CloudServerOutlined />,
  ui: <DesktopOutlined />,
  supervisor: <SafetyOutlined />,
  scheduler: <CalendarOutlined />,
};

const LEVEL_TONE: Record<Level, Tone> = { error: "fail", warn: "warn", info: "info", debug: "neutral" };
const LEVEL_LABEL: Record<Level, string> = { error: "Error", warn: "Warning", info: "Info", debug: "Debug" };

/** Fields shown on the row, in this order, when a line has them. */
const SUMMARY_KEYS = [
  "cluster",
  "run_id",
  "environment",
  "method",
  "path",
  "status",
  "duration_ms",
  "duration_s",
  "user",
  "action",
  "channel",
  "phase",
  "error",
  "error_code",
  "request_id",
];

const HIDDEN_KEYS = new Set(["time", "level", "msg", "message", "source", "pid", "host", "component", "git_revision", "go_version", "Version", "stream"]);

function normLevel(v: unknown): Level {
  const s = String(v ?? "").toLowerCase();
  if (s.startsWith("err") || s === "fatal" || s === "panic") return "error";
  if (s.startsWith("warn")) return "warn";
  if (s.startsWith("debug") || s === "trace") return "debug";
  return "info";
}

const TEXT_LEVEL = /\blevel=(\w+)|\b(ERROR|ERR|WARN|WARNING|INFO|DEBUG|FATAL)\b/;
const TEXT_TIME = /^(\d{4}-\d{2}-\d{2}[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)/;

function parseLine(raw: string, n: number): Entry {
  const t = raw.trim();
  if (t.startsWith("{")) {
    try {
      const f = JSON.parse(t) as Record<string, unknown>;
      const message = String(f.msg ?? f.message ?? "");
      const parts: string[] = [];
      for (const k of SUMMARY_KEYS) {
        const v = f[k];
        if (v === undefined || v === null || v === "") continue;
        parts.push(`${k}=${typeof v === "object" ? JSON.stringify(v) : String(v)}`);
      }
      if (f.err !== undefined || f.error !== undefined) {
        if (f.err !== undefined && f.error === undefined) parts.push(`error=${String(f.err)}`);
      }
      return { n, time: String(f.time ?? f.ts ?? ""), level: normLevel(f.level), message, summary: parts.join("  "), fields: f, raw };
    } catch {
      // not JSON after all; fall through to text
    }
  }
  const lv = TEXT_LEVEL.exec(raw);
  let level: Level = lv ? normLevel(lv[1] ?? lv[2]) : "info";
  if (!lv && /\b(error|failed|panic)\b/i.test(raw)) level = "error";
  const tm = TEXT_TIME.exec(t);
  return { n, time: tm ? tm[1] : "", level, message: tm ? t.slice(tm[1].length).trim() : t, summary: "", fields: null, raw };
}

function shortTime(t: string): string {
  if (!t) return "";
  const d = new Date(t);
  if (Number.isNaN(d.getTime())) return t.slice(0, 19);
  const p = (x: number) => String(x).padStart(2, "0");
  const today = new Date();
  const time = `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  return d.toDateString() === today.toDateString() ? time : `${p(d.getMonth() + 1)}-${p(d.getDate())} ${time}`;
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const v = bytes / Math.pow(1024, i);
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function download(name: string, content: string) {
  const blob = new Blob([content], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

/**
 * Settings → Logs: every process log under logs/, newest line first.
 * Structured (JSON) logs show as rows with level, time, message and the
 * fields that matter (cluster, run, request, user, error); click one for
 * the whole entry. Admins also see the API server, UI server, supervisor and
 * scheduler logs and can change the API server's log level.
 */
export function LogsSection({ onError }: Props) {
  const { isAdmin } = useAuth();
  const qc = useQueryClient();
  const [sourceId, setSourceId] = useLocalStorageState("settings.logs.source", "runner");
  const [file, setFile] = useState<string | undefined>(undefined);
  const [auto, setAuto] = useLocalStorageState("settings.logs.autoRefresh", true);
  const [search, setSearch] = useLocalStorageState("settings.logs.filter", "");
  const [level, setLevel] = useLocalStorageState<string>("settings.logs.level", "all");
  const [raw, setRaw] = useLocalStorageState("settings.logs.raw", false);
  const [open, setOpen] = useState<Entry | null>(null);

  const sourcesQuery = useQuery({ queryKey: ["log-sources"], queryFn: api.logSources, staleTime: 30_000 });
  const sources: LogSource[] = sourcesQuery.data?.sources ?? [];
  const source = sources.find((s) => s.id === sourceId) ?? sources[0];

  useEffect(() => {
    if (sources.length && !sources.some((s) => s.id === sourceId)) setSourceId(sources[0].id);
  }, [sources, sourceId, setSourceId]);
  useEffect(() => setFile(undefined), [sourceId]);

  const logQuery = useQuery({
    queryKey: ["log-file", source?.id, file],
    queryFn: () => api.logFile(source!.id, file),
    enabled: Boolean(source),
    refetchInterval: auto ? () => (document.hidden ? false : 4000) : false,
  });
  useEffect(() => {
    if (logQuery.error) onError(logQuery.error);
  }, [logQuery.error, onError]);

  const content = logQuery.data?.content ?? "";
  const entries = useMemo(() => {
    const lines = content ? content.split(/\r?\n/) : [];
    const out: Entry[] = [];
    // A truncated tail may start mid-line; drop the partial first line.
    const start = logQuery.data?.truncated ? 1 : 0;
    for (let i = start; i < lines.length; i += 1) {
      if (lines[i].trim()) out.push(parseLine(lines[i], i + 1));
    }
    return out.reverse();
  }, [content, logQuery.data?.truncated]);

  const counts = useMemo(() => {
    const c: Record<Level, number> = { error: 0, warn: 0, info: 0, debug: 0 };
    for (const e of entries) c[e.level] += 1;
    return c;
  }, [entries]);

  const shown = useMemo(
    () =>
      entries.filter(
        (e) =>
          (level === "all" || (level === "problems" ? e.level === "error" || e.level === "warn" : e.level === level)) &&
          matchesSearch(search, e.raw),
      ),
    [entries, level, search],
  );

  const levelQuery = useQuery({ queryKey: ["logging-settings"], queryFn: api.loggingSettings, enabled: isAdmin });
  const setServerLevel = useCallback(
    async (v: string) => {
      try {
        await api.setLogLevel(v);
        await qc.invalidateQueries({ queryKey: ["logging-settings"] });
        notify.success(`API server now logs at ${v} level until it restarts.`);
      } catch (e) {
        onError(e);
      }
    },
    [qc, onError],
  );

  const showAllFor = (value: unknown) => {
    setSearch(String(value));
    setLevel("all");
    setOpen(null);
  };

  const fileName = logQuery.data?.path || source?.files?.[0] || "";
  const files = source?.files ?? [];

  return (
    <div className="logs-page">
      <Panel
        title="Sources"
        icon={<FileTextOutlined />}
        description="Everything the orchestrator's processes write under logs/, newest first. Passwords, tokens and webhook secrets are removed before lines are written."
        extra={
          isAdmin && levelQuery.data ? (
            <span className="logs-level-control">
              <Typography.Text type="secondary">API server log level</Typography.Text>
              <Tooltip title={levelQuery.data.persistence}>
                <Select
                  size="small"
                  aria-label="API server log level"
                  value={levelQuery.data.level}
                  onChange={(v) => void setServerLevel(v)}
                  options={(["debug", "info", "warn", "error"] as LogLevelName[]).map((l) => ({ value: l, label: l === "debug" ? "Debug (every request)" : l[0].toUpperCase() + l.slice(1) }))}
                  popupMatchSelectWidth={false}
                />
              </Tooltip>
            </span>
          ) : null
        }
      >
        {sources.length > 1 ? (
          <Segmented
            className="logs-sources"
            value={source?.id}
            onChange={(v) => setSourceId(String(v))}
            options={sources.map((s) => ({
              value: s.id,
              label: (
                <span className="logs-source-option">
                  <span aria-hidden="true">{SOURCE_ICONS[s.id] ?? <FileTextOutlined />}</span>
                  {s.label}
                </span>
              ),
            }))}
          />
        ) : null}
        {source ? (
          <div className="logs-source-meta">
            <span>{source.description}</span>
            <span className="logs-source-file">
              {files.length > 1 ? (
                <Select
                  size="small"
                  aria-label="Log file"
                  value={file ?? files[0]}
                  onChange={setFile}
                  options={files.map((f) => ({ value: f, label: f }))}
                  popupMatchSelectWidth={false}
                />
              ) : (
                <Typography.Text code copyable={fileName ? { text: fileName } : false} title={fileName}>
                  {middleEllipsis(fileName || "—", 60)}
                </Typography.Text>
              )}
              {logQuery.data?.size ? <span>{formatBytes(logQuery.data.size)}</span> : null}
            </span>
          </div>
        ) : null}
      </Panel>

      <Panel
        flush
        title={source ? source.label : "Log"}
        icon={source ? SOURCE_ICONS[source.id] : <FileTextOutlined />}
        description={logQuery.data?.truncated ? "Showing the most recent part of the file. Download for the rest, or open the file on the server." : undefined}
        extra={
          <span className="logs-actions">
            <span className="logs-switch">
              <Switch size="small" id="logs-auto" checked={auto} onChange={setAuto} aria-label="Auto-refresh" />
              <label htmlFor="logs-auto">Live</label>
            </span>
            <span className="logs-switch">
              <Switch size="small" id="logs-raw" checked={raw} onChange={setRaw} aria-label="Raw lines" />
              <label htmlFor="logs-raw">Raw</label>
            </span>
            <Tooltip title="Refresh now">
              <Button size="small" icon={<ReloadOutlined />} aria-label="Refresh now" onClick={() => void logQuery.refetch()} loading={logQuery.isFetching && !auto} />
            </Tooltip>
            <Tooltip title="Download what's shown">
              <Button size="small" icon={<DownloadOutlined />} aria-label="Download" onClick={() => download(fileName.split("/").pop() || "ncc.log", content)} disabled={!content} />
            </Tooltip>
          </span>
        }
      >
        <ListToolbar
          search={search}
          onSearch={setSearch}
          placeholder="Search: cluster, run ID, request ID, user, error…"
          filters={[
            { value: "all", label: "All" },
            { value: "problems", label: "Problems", count: counts.error + counts.warn },
            { value: "error", label: "Errors", count: counts.error },
            { value: "warn", label: "Warnings", count: counts.warn },
            { value: "info", label: "Info", count: counts.info },
            ...(counts.debug ? [{ value: "debug", label: "Debug", count: counts.debug }] : []),
          ]}
          filter={level}
          onFilter={setLevel}
          shown={shown.length}
          total={entries.length}
          noun="lines"
        />
        {sourcesQuery.isError ? (
          <Alert type="error" showIcon title="Couldn't list the logs" />
        ) : !logQuery.data && logQuery.isLoading ? (
          <EmptyState title="Loading…" />
        ) : logQuery.data && !logQuery.data.exists ? (
          <EmptyState icon={<FileTextOutlined />} title="This log hasn't been written yet" description={`${fileName || "The file"} appears once the process writes to it.`} />
        ) : shown.length === 0 ? (
          <EmptyState icon={<FilterOutlined />} title={entries.length ? "No lines match" : "The log is empty"} description={entries.length ? "Try another search or level." : undefined} />
        ) : raw ? (
          <CodeEditor value={shown.map((e) => e.raw).join("\n")} language="plaintext" readOnly height={520} />
        ) : (
          <VirtualList
            className="logs-list"
            aria-label="Log lines, newest first"
            items={shown}
            rowHeight={52}
            maxHeight={560}
            itemKey={(e) => e.n}
            renderRow={(e) => (
              <button type="button" className={`logs-row level-${e.level}`} onClick={() => setOpen(e)}>
                <span className="logs-row-time" title={e.time}>
                  {shortTime(e.time)}
                </span>
                <StatusBadge tone={LEVEL_TONE[e.level]}>{LEVEL_LABEL[e.level]}</StatusBadge>
                <span className="logs-row-text">
                  <span className="logs-row-msg">{e.message || e.raw}</span>
                  {e.summary ? <span className="logs-row-fields">{e.summary}</span> : null}
                </span>
              </button>
            )}
          />
        )}
      </Panel>

      <Drawer
        open={open !== null}
        onClose={() => setOpen(null)}
        title={open ? `${LEVEL_LABEL[open.level]} · ${shortTime(open.time) || `line ${open.n}`}` : ""}
        size={560}
        extra={
          open ? (
            <Button
              size="small"
              icon={<CopyOutlined />}
              onClick={() => {
                void navigator.clipboard?.writeText(open.raw);
                notify.success("Copied the log line.");
              }}
            >
              Copy
            </Button>
          ) : null
        }
      >
        {open ? (
          <div className="logs-detail">
            <p className="logs-detail-msg">{open.message || open.raw}</p>
            {open.fields ? (
              <dl className="logs-detail-fields">
                {Object.entries(open.fields)
                  .filter(([k]) => !HIDDEN_KEYS.has(k))
                  .map(([k, v]) => (
                    <div key={k} className="logs-detail-field">
                      <dt>{k}</dt>
                      <dd>{typeof v === "object" ? JSON.stringify(v, null, 2) : String(v)}</dd>
                    </div>
                  ))}
              </dl>
            ) : null}
            <div className="logs-detail-actions">
              {(["request_id", "run_id", "cluster"] as const).map((k) =>
                open.fields?.[k] ? (
                  <Button key={k} size="small" onClick={() => showAllFor(open.fields?.[k])}>
                    All lines for this {k === "request_id" ? "request" : k === "run_id" ? "run" : "cluster"}
                  </Button>
                ) : null,
              )}
            </div>
            <Typography.Text type="secondary" className="logs-detail-raw-label">
              Line {open.n} as written
            </Typography.Text>
            <pre className="logs-detail-raw">{open.fields ? JSON.stringify(open.fields, null, 2) : open.raw}</pre>
          </div>
        ) : null}
      </Drawer>
    </div>
  );
}
