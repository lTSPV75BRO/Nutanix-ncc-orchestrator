import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Card,
  Col,
  Drawer,
  Empty,
  Input,
  List,
  Row,
  Segmented,
  Select,
  Space,
  Statistic,
  Switch,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  ApiOutlined,
  CopyOutlined,
  ExperimentOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
  SearchOutlined,
  SendOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import { CodeEditor } from "../../components/CodeEditor";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { notify } from "../../notify";

type HttpMethod = "GET" | "POST" | "PUT" | "DELETE";

type EndpointPreset = {
  label: string;
  method: HttpMethod;
  path: string;
  body?: string;
};

type RouteMeta = {
  path: string;
  methods: string[];
  description?: string;
  sample_body?: string;
};

type OpenAPISpec = {
  paths?: Record<
    string,
    Record<
      string,
      { summary?: string; requestBody?: { content?: Record<string, { example?: unknown }> } }
    >
  >;
};

const PRESETS: EndpointPreset[] = [
  { label: "Health", method: "GET", path: "/api/v1/health" },
  { label: "Report Data", method: "GET", path: "/api/v1/report/data" },
  { label: "Report Trends", method: "GET", path: "/api/v1/report/trends?limit=30" },
  { label: "Runs Summary", method: "GET", path: "/api/v1/runs/summary" },
  { label: "Artifacts", method: "GET", path: "/api/v1/artifacts" },
  { label: "Schedule Health", method: "GET", path: "/api/v1/schedule/health" },
  { label: "Diagnostics (System Health)", method: "GET", path: "/api/v1/health/diagnostics" },
  { label: "List Backups", method: "GET", path: "/api/v1/settings/backups" },
  {
    label: "Create Snapshot (optionally encrypted)",
    method: "POST",
    path: "/api/v1/settings/backups",
    body: JSON.stringify({ passphrase: "" }, null, 2),
  },
  {
    label: "Trigger Run (sample)",
    method: "POST",
    path: "/api/v1/runs/trigger",
    body: JSON.stringify(
      {
        config_path: "config.yaml",
        password: "",
        extra_args: ["--no-html"],
      },
      null,
      2,
    ),
  },
];

type Props = {
  onError: (message: string) => void;
};

type ResponseData = {
  status: number;
  statusText: string;
  elapsedMs: number;
  headers: Record<string, string>;
  body: string;
};

const METHOD_COLOR: Record<HttpMethod, string> = {
  GET: "blue",
  POST: "green",
  PUT: "orange",
  DELETE: "red",
};

function parseHeaderLines(raw: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const idx = trimmed.indexOf(":");
    if (idx <= 0) {
      throw new Error(`invalid header format: ${trimmed}`);
    }
    const key = trimmed.slice(0, idx).trim();
    const value = trimmed.slice(idx + 1).trim();
    if (!key) {
      throw new Error(`invalid header key in line: ${trimmed}`);
    }
    out[key] = value;
  }
  return out;
}

function prettyJsonIfPossible(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "";
  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2);
  } catch {
    return raw;
  }
}

function stringifySampleBody(value: unknown): string {
  if (value === undefined) return "";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function parseRoutesFromOpenAPI(spec: OpenAPISpec): RouteMeta[] {
  const out: RouteMeta[] = [];
  const paths = spec.paths || {};
  for (const [path, operations] of Object.entries(paths)) {
    const methods = Object.keys(operations || {})
      .map((m) => m.toUpperCase())
      .filter((m) => ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"].includes(m));
    if (methods.length === 0) continue;
    const firstOperation = operations?.[methods[0].toLowerCase()];
    const sample = firstOperation?.requestBody?.content?.["application/json"]?.example;
    out.push({
      path,
      methods,
      description: firstOperation?.summary || "",
      sample_body: stringifySampleBody(sample),
    });
  }
  return out.sort((a, b) => a.path.localeCompare(b.path));
}

function buildCurl(method: HttpMethod, url: string, headers: Record<string, string>, body?: string): string {
  const parts: string[] = ["curl"];
  if (method !== "GET") parts.push(`-X ${method}`);
  parts.push(`'${url}'`);
  for (const [k, v] of Object.entries(headers)) {
    parts.push(`-H '${k}: ${v}'`);
  }
  if (body && body.trim()) {
    const escaped = body.replace(/'/g, "'\\''");
    parts.push(`--data '${escaped}'`);
  }
  return parts.join(" \\\n  ");
}

function statusToBadgeStatus(status: number): "success" | "warning" | "error" | "default" {
  if (status >= 200 && status < 300) return "success";
  if (status >= 300 && status < 400) return "default";
  if (status >= 400 && status < 500) return "warning";
  return "error";
}

export function ApiExplorerSection({ onError }: Props) {
  const [method, setMethod] = useLocalStorageState<HttpMethod>("apiExplorer.method", "GET");
  const [path, setPath] = useLocalStorageState("apiExplorer.path", "/api/v1/health");
  const [headerLines, setHeaderLines] = useState("");
  const [body, setBody] = useState("");
  const [allowExternalURL, setAllowExternalURL] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [response, setResponse] = useState<ResponseData | null>(null);
  const [routes, setRoutes] = useState<RouteMeta[]>([]);
  const [routesError, setRoutesError] = useState("");
  const [routesOpen, setRoutesOpen] = useState(false);
  const [routeFilter, setRouteFilter] = useState("");
  const [responseTab, setResponseTab] = useState<"body" | "headers">("body");

  const effectiveUrl = useMemo(() => {
    const trimmed = path.trim();
    if (!trimmed) return "";
    if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) return trimmed;
    if (trimmed.startsWith("/")) return trimmed;
    return `/${trimmed}`;
  }, [path]);

  const filteredRoutes = useMemo(() => {
    const q = routeFilter.trim().toLowerCase();
    if (!q) return routes;
    return routes.filter(
      (r) =>
        r.path.toLowerCase().includes(q) ||
        (r.description || "").toLowerCase().includes(q) ||
        r.methods.some((m) => m.toLowerCase() === q),
    );
  }, [routes, routeFilter]);

  const applyPreset = (label: string) => {
    const preset = PRESETS.find((p) => p.label === label);
    if (!preset) return;
    setMethod(preset.method);
    setPath(preset.path);
    setBody(preset.body ?? "");
  };

  const applyRouteMeta = (route: RouteMeta) => {
    const firstMethod = route.methods[0] || "GET";
    if (firstMethod === "GET" || firstMethod === "POST" || firstMethod === "PUT" || firstMethod === "DELETE") {
      setMethod(firstMethod);
    }
    setPath(route.path);
    setBody(route.sample_body ?? "");
    setRoutesOpen(false);
  };

  // Merge both route sources by path so the explorer is never missing an
  // endpoint: the OpenAPI spec carries richer per-path detail (query params,
  // request examples) for some routes, while /api/v1/meta/routes is the
  // comprehensive catalog of every registered route. We union them — preferring
  // the OpenAPI entry where present and filling any gaps (and empty
  // descriptions / sample bodies) from the catalog.
  const loadRoutes = async () => {
    const headers = { "X-Requested-With": "ncc-ui" };
    const byPath = new Map<string, RouteMeta>();
    let loadedAny = false;
    let lastError = "";

    try {
      const openAPIRes = await fetch("/api/v1/openapi.json", {
        method: "GET",
        credentials: "same-origin",
        headers,
      });
      const openAPI = (await openAPIRes.json().catch(() => ({}))) as OpenAPISpec;
      if (openAPIRes.ok) {
        for (const r of parseRoutesFromOpenAPI(openAPI)) byPath.set(r.path, r);
        loadedAny = true;
      }
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }

    try {
      const res = await fetch("/api/v1/meta/routes", {
        method: "GET",
        credentials: "same-origin",
        headers,
      });
      const payload = (await res.json().catch(() => ({}))) as {
        success?: boolean;
        error?: string;
        data?: { routes?: RouteMeta[] };
      };
      if (res.ok && payload.success) {
        for (const r of payload.data?.routes ?? []) {
          const existing = byPath.get(r.path);
          if (!existing) {
            byPath.set(r.path, r);
          } else {
            byPath.set(r.path, {
              path: r.path,
              methods: existing.methods.length ? existing.methods : r.methods,
              description: existing.description || r.description,
              sample_body: existing.sample_body || r.sample_body,
            });
          }
        }
        loadedAny = true;
      } else if (!loadedAny) {
        lastError = payload.error || res.statusText || "failed to load routes";
      }
    } catch (error) {
      if (!loadedAny) lastError = error instanceof Error ? error.message : String(error);
    }

    if (!loadedAny) {
      setRoutesError(lastError || "failed to load routes");
      return;
    }
    setRoutesError("");
    setRoutes(Array.from(byPath.values()).sort((a, b) => a.path.localeCompare(b.path)));
  };

  useEffect(() => {
    void loadRoutes();
  }, []);

  const sendRequest = async () => {
    try {
      if (!effectiveUrl) throw new Error("path is required");
      if (!allowExternalURL && /^https?:\/\//i.test(effectiveUrl)) {
        throw new Error("external URLs are blocked. Enable 'Allow External URL' to send to remote endpoints.");
      }
      setIsLoading(true);
      const extraHeaders = parseHeaderLines(headerLines);
      const headers = new Headers({ "X-Requested-With": "ncc-ui", ...extraHeaders });
      const init: RequestInit = { method, credentials: "same-origin", headers };
      if (method === "POST" || method === "PUT" || method === "DELETE") {
        const payload = body.trim();
        if (payload) {
          if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
          try {
            init.body = JSON.stringify(JSON.parse(payload));
          } catch {
            init.body = payload;
          }
        }
      }
      const startedAt = performance.now();
      const res = await fetch(effectiveUrl, init);
      const elapsedMs = Math.round(performance.now() - startedAt);
      const text = await res.text();
      const headerObj: Record<string, string> = {};
      res.headers.forEach((value, key) => {
        headerObj[key] = value;
      });
      setResponse({
        status: res.status,
        statusText: res.statusText,
        elapsedMs,
        headers: headerObj,
        body: prettyJsonIfPossible(text),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      onError(message);
    } finally {
      setIsLoading(false);
    }
  };

  const copyAsCurl = async () => {
    try {
      const headers = parseHeaderLines(headerLines);
      const text = buildCurl(method, effectiveUrl, headers, body || undefined);
      await navigator.clipboard.writeText(text);
      notify.success("cURL command copied.");
    } catch (e) {
      notify.warning(e instanceof Error ? e.message : "Failed to build cURL.");
    }
  };

  return (
    <Card
      className="page-card"
      title={
        <Space size={10} align="center">
          <ApiOutlined className="section-header-icon" />
          <Typography.Text strong>REST API Explorer</Typography.Text>
        </Space>
      }
      extra={
        <Space size={6}>
          <Tooltip title="Discovered routes">
            <Badge count={routes.length} size="small" offset={[-4, 4]}>
              <Button icon={<ExperimentOutlined />} onClick={() => setRoutesOpen(true)}>
                Routes
              </Button>
            </Badge>
          </Tooltip>
          <Tooltip title="Refresh route list">
            <Button icon={<ReloadOutlined />} onClick={() => void loadRoutes()} />
          </Tooltip>
        </Space>
      }
    >
      <Typography.Text type="secondary" className="section-subtitle">
        Send requests to the orchestrator API directly from the UI. Same-origin paths use the proxy; cross-origin
        URLs require explicit opt-in.
      </Typography.Text>

      <Space size={8} wrap style={{ marginTop: 12, marginBottom: 12 }}>
        <Select
          id="api-explorer-preset"
          aria-label="Quick presets"
          style={{ minWidth: 220 }}
          placeholder="Quick presets…"
          onChange={applyPreset}
          options={PRESETS.map((p) => ({
            label: (
              <Space size={6}>
                <Tag color={METHOD_COLOR[p.method]} style={{ marginInlineEnd: 0 }}>
                  {p.method}
                </Tag>
                <span>{p.label}</span>
              </Space>
            ),
            value: p.label,
          }))}
        />
        <Tooltip title="Allow sending requests to absolute http(s) URLs (off by default)">
          <Space size={6}>
            <Switch id="api-explorer-allow-external" aria-label="Allow external URLs" checked={allowExternalURL} onChange={setAllowExternalURL} size="small" />
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              <label htmlFor="api-explorer-allow-external">Allow external URLs</label>
            </Typography.Text>
            {allowExternalURL ? <WarningOutlined style={{ color: "#f59e0b" }} /> : null}
          </Space>
        </Tooltip>
      </Space>

      <Row gutter={[8, 8]} align="middle">
        <Col xs={8} sm={5}>
          <Select<HttpMethod>
            id="api-explorer-method"
            aria-label="HTTP method"
            value={method}
            onChange={setMethod}
            style={{ width: "100%" }}
            options={(["GET", "POST", "PUT", "DELETE"] as HttpMethod[]).map((m) => ({
              value: m,
              label: <Tag color={METHOD_COLOR[m]} style={{ marginInlineEnd: 0 }}>{m}</Tag>,
            }))}
          />
        </Col>
        <Col xs={16} sm={13}>
          <Input
            id="api-explorer-path"
            name="api-path"
            aria-label="API request path"
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="/api/v1/health"
            prefix={<ApiOutlined style={{ color: "rgba(226,232,240,0.6)" }} />}
            autoComplete="off"
          />
        </Col>
        <Col xs={24} sm={6}>
          <Space size={6} style={{ display: "flex" }}>
            <Tooltip title="Copy as cURL">
              <Button icon={<CopyOutlined />} onClick={copyAsCurl} />
            </Tooltip>
            <Button type="primary" loading={isLoading} icon={<SendOutlined />} onClick={() => void sendRequest()} block>
              Send
            </Button>
          </Space>
        </Col>
      </Row>

      <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
        <Col xs={24} md={12}>
          <Typography.Text strong style={{ fontSize: 13 }}>Headers</Typography.Text>
          <Typography.Text type="secondary" style={{ fontSize: 12, marginLeft: 6 }}>
            (one per line, <code>Key: Value</code>)
          </Typography.Text>
          <Input.TextArea
            id="api-explorer-headers"
            name="headers"
            aria-label="Request headers"
            value={headerLines}
            onChange={(e) => setHeaderLines(e.target.value)}
            rows={5}
            placeholder={"Authorization: Bearer <token>\nX-Custom-Header: value"}
            style={{ marginTop: 6, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 12 }}
            autoComplete="off"
          />
        </Col>
        <Col xs={24} md={12}>
          <Typography.Text strong style={{ fontSize: 13 }}>Body (JSON)</Typography.Text>
          <Input.TextArea
            id="api-explorer-body"
            name="body"
            aria-label="Request body (JSON)"
            value={body}
            onChange={(e) => setBody(e.target.value)}
            rows={5}
            placeholder='{"example":"value"}'
            style={{ marginTop: 6, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", fontSize: 12 }}
            autoComplete="off"
          />
        </Col>
      </Row>

      <Alert
        type="info"
        showIcon
        icon={<SafetyCertificateOutlined />}
        style={{ marginTop: 12 }}
        message="Sensitive request headers/body are not persisted in browser storage."
      />

      {response ? (
        <Card size="small" style={{ marginTop: 12 }} className="api-response-card">
          <Space size={20} wrap style={{ marginBottom: 12 }} align="center">
            <Statistic
              title="Status"
              value={`${response.status} ${response.statusText}`.trim()}
              valueStyle={{ fontSize: 18 }}
              prefix={<Badge status={statusToBadgeStatus(response.status)} />}
            />
            <Statistic title="Duration" value={response.elapsedMs} suffix="ms" valueStyle={{ fontSize: 18 }} />
            <Statistic
              title="Size"
              value={new TextEncoder().encode(response.body).length}
              suffix="bytes"
              valueStyle={{ fontSize: 18 }}
            />
            <Tooltip title="Copy body">
              <Button
                icon={<CopyOutlined />}
                size="small"
                onClick={() => {
                  void navigator.clipboard.writeText(response.body);
                  notify.success("Response body copied.");
                }}
              />
            </Tooltip>
          </Space>
          <Segmented
            value={responseTab}
            onChange={(v) => setResponseTab(v as "body" | "headers")}
            options={[
              { value: "body", label: "Body" },
              { value: "headers", label: `Headers (${Object.keys(response.headers).length})` },
            ]}
            style={{ marginBottom: 8 }}
          />
          {responseTab === "body" ? (
            <CodeEditor value={response.body || "(empty response body)"} language="json" readOnly height={320} />
          ) : (
            <CodeEditor
              value={JSON.stringify(response.headers, null, 2)}
              language="json"
              readOnly
              height={320}
            />
          )}
        </Card>
      ) : null}

      <Drawer
        open={routesOpen}
        onClose={() => setRoutesOpen(false)}
        title="Discovered backend routes"
        width={560}
        styles={{ body: { padding: 16 } }}
      >
        <Input
          id="api-routes-filter"
          name="route-filter"
          aria-label="Filter routes"
          allowClear
          value={routeFilter}
          onChange={(e) => setRouteFilter(e.target.value)}
          placeholder="Filter by path, method, or description…"
          prefix={<SearchOutlined />}
          style={{ marginBottom: 12 }}
          autoComplete="off"
        />
        {routesError ? (
          <Alert type="warning" showIcon style={{ marginBottom: 12 }} message={routesError} />
        ) : null}
        {filteredRoutes.length === 0 ? (
          <Empty description="No routes match" />
        ) : (
          <List
            size="small"
            dataSource={filteredRoutes}
            renderItem={(route) => (
              <List.Item
                actions={[
                  <Button key="use" size="small" type="link" onClick={() => applyRouteMeta(route)}>
                    Use
                  </Button>,
                ]}
              >
                <Space direction="vertical" size={2} style={{ width: "100%" }}>
                  <Space size={6} wrap>
                    {route.methods.map((m) => (
                      <Tag key={`${route.path}-${m}`} color={METHOD_COLOR[m as HttpMethod] || "default"}>
                        {m}
                      </Tag>
                    ))}
                    <Typography.Text code style={{ fontSize: 12 }}>{route.path}</Typography.Text>
                  </Space>
                  {route.description ? (
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                      {route.description}
                    </Typography.Text>
                  ) : null}
                </Space>
              </List.Item>
            )}
          />
        )}
      </Drawer>
    </Card>
  );
}
