import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Card, Col, Input, List, Row, Select, Space, Statistic, Switch, Tag, Typography } from "antd";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";

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
  paths?: Record<string, Record<string, { summary?: string; requestBody?: { content?: Record<string, { example?: unknown }> } }>>;
};

const presets: EndpointPreset[] = [
  { label: "Health", method: "GET", path: "/api/v1/health" },
  { label: "Report Data", method: "GET", path: "/api/v1/report/data" },
  { label: "Report Trends", method: "GET", path: "/api/v1/report/trends?limit=30" },
  { label: "Runs Summary", method: "GET", path: "/api/v1/runs/summary" },
  { label: "Artifacts", method: "GET", path: "/api/v1/artifacts" },
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

export function ApiExplorerSection({ onError }: Props) {
  const [method, setMethod] = useLocalStorageState<HttpMethod>("apiExplorer.method", "GET");
  const [path, setPath] = useLocalStorageState("apiExplorer.path", "/api/v1/health");
  const [headerLines, setHeaderLines] = useState("");
  const [body, setBody] = useState("");
  const [allowExternalURL, setAllowExternalURL] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [response, setResponse] = useState<ResponseData | null>(null);
  const [lastError, setLastError] = useState("");
  const [routes, setRoutes] = useState<RouteMeta[]>([]);
  const [routesError, setRoutesError] = useState("");
  const [selectedRoute, setSelectedRoute] = useState<string>("");

  const effectiveUrl = useMemo(() => {
    const trimmed = path.trim();
    if (!trimmed) return "";
    if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) return trimmed;
    if (trimmed.startsWith("/")) return trimmed;
    return `/${trimmed}`;
  }, [path]);

  const applyPreset = (label: string) => {
    const preset = presets.find((p) => p.label === label);
    if (!preset) return;
    setMethod(preset.method);
    setPath(preset.path);
    setBody(preset.body ?? "");
  };

  const applyRouteMeta = (routePath: string) => {
    setSelectedRoute(routePath);
    const route = routes.find((r) => r.path === routePath);
    if (!route) return;
    const firstMethod = route.methods[0] || "GET";
    if (firstMethod === "GET" || firstMethod === "POST" || firstMethod === "PUT" || firstMethod === "DELETE") {
      setMethod(firstMethod);
    }
    setPath(route.path);
    setBody(route.sample_body ?? "");
  };

  const loadRoutes = async () => {
    try {
      setRoutesError("");
      const openAPIRes = await fetch("/api/v1/openapi.json", {
        method: "GET",
        credentials: "same-origin",
        headers: {
          "X-Requested-With": "ncc-ui",
        },
      });
      const openAPI = (await openAPIRes.json().catch(() => ({}))) as OpenAPISpec;
      if (openAPIRes.ok) {
        const parsed = parseRoutesFromOpenAPI(openAPI);
        if (parsed.length > 0) {
          setRoutes(parsed);
          return;
        }
      }

      const res = await fetch("/api/v1/meta/routes", {
        method: "GET",
        credentials: "same-origin",
        headers: {
          "X-Requested-With": "ncc-ui",
        },
      });
      const payload = (await res.json().catch(() => ({}))) as {
        success?: boolean;
        error?: string;
        data?: { routes?: RouteMeta[] };
      };
      if (!res.ok || !payload.success) {
        throw new Error(payload.error || res.statusText || "failed to load routes from OpenAPI or meta endpoint");
      }
      const apiRoutes = Array.isArray(payload.data?.routes) ? payload.data?.routes : [];
      setRoutes(apiRoutes);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setRoutesError(message);
    }
  };

  useEffect(() => {
    void loadRoutes();
  }, []);

  const sendRequest = async () => {
    try {
      if (!effectiveUrl) {
        throw new Error("path is required");
      }
      if (!allowExternalURL && /^https?:\/\//i.test(effectiveUrl)) {
        throw new Error("external URLs are blocked by default. Enable 'Allow External URL' only for trusted endpoints.");
      }
      setIsLoading(true);
      setLastError("");
      const extraHeaders = parseHeaderLines(headerLines);
      const headers = new Headers({
        "X-Requested-With": "ncc-ui",
        ...extraHeaders,
      });
      const init: RequestInit = {
        method,
        credentials: "same-origin",
        headers,
      };
      if (method === "POST" || method === "PUT" || method === "DELETE") {
        const payload = body.trim();
        if (payload) {
          if (!headers.has("Content-Type")) {
            headers.set("Content-Type", "application/json");
          }
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
      setLastError(message);
      onError(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Card>
      <Typography.Title level={4} className="section-title">
        REST API Explorer
      </Typography.Title>
      <Typography.Text type="secondary" className="section-subtitle">
        Build and send requests against backend endpoints from the UI. Relative paths use the same origin (for example `/api/v1/...`).
      </Typography.Text>
      <Space wrap style={{ marginTop: 12, marginBottom: 12 }}>
        <Select
          style={{ minWidth: 220 }}
          placeholder="Load endpoint preset"
          onChange={applyPreset}
          options={presets.map((p) => ({ label: p.label, value: p.label }))}
        />
        <Select
          style={{ minWidth: 280 }}
          placeholder="Load backend route (auto)"
          value={selectedRoute || undefined}
          onChange={applyRouteMeta}
          options={routes.map((r) => ({ label: `${r.methods.join("|")} ${r.path}`, value: r.path }))}
        />
        <Button onClick={() => void loadRoutes()}>Refresh Routes</Button>
        <Space>
          <Typography.Text>Allow External URL</Typography.Text>
          <Switch checked={allowExternalURL} onChange={setAllowExternalURL} />
        </Space>
        <Button type="primary" loading={isLoading} onClick={() => void sendRequest()}>
          Send Request
        </Button>
      </Space>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        title="Sensitive request headers/body are not persisted in browser storage."
      />
      {routesError ? <Alert type="warning" showIcon style={{ marginBottom: 12 }} title={`Route discovery failed: ${routesError}`} /> : null}

      <Row gutter={[12, 12]}>
        <Col xs={24} md={6}>
          <Typography.Text strong>Method</Typography.Text>
          <Select<HttpMethod>
            value={method}
            style={{ width: "100%", marginTop: 6 }}
            onChange={setMethod}
            options={[
              { label: "GET", value: "GET" },
              { label: "POST", value: "POST" },
              { label: "PUT", value: "PUT" },
              { label: "DELETE", value: "DELETE" },
            ]}
          />
        </Col>
        <Col xs={24} md={18}>
          <Typography.Text strong>Path or URL</Typography.Text>
          <Input
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="/api/v1/health"
            style={{ marginTop: 6 }}
          />
        </Col>
        <Col xs={24}>
          <Typography.Text strong>Headers (optional)</Typography.Text>
          <Input.TextArea
            value={headerLines}
            onChange={(e) => setHeaderLines(e.target.value)}
            rows={4}
            placeholder={"Authorization: Bearer <token>\nX-Custom-Header: value"}
            style={{ marginTop: 6 }}
          />
        </Col>
        <Col xs={24}>
          <Typography.Text strong>Body (optional)</Typography.Text>
          <Input.TextArea
            value={body}
            onChange={(e) => setBody(e.target.value)}
            rows={10}
            placeholder='{"example":"value"}'
            style={{ marginTop: 6, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" }}
          />
        </Col>
      </Row>

      <Card size="small" style={{ marginTop: 12 }}>
        <Typography.Text strong>Discovered Backend Routes</Typography.Text>
        <List
          size="small"
          dataSource={routes}
          locale={{ emptyText: "No discovered routes yet." }}
          renderItem={(route) => (
            <List.Item
              actions={[
                <Button key="use" size="small" onClick={() => applyRouteMeta(route.path)}>
                  Use
                </Button>,
              ]}
            >
              <Space direction="vertical" size={2} style={{ width: "100%" }}>
                <Space size={6} wrap>
                  {route.methods.map((m) => (
                    <Tag key={`${route.path}-${m}`} color="blue">
                      {m}
                    </Tag>
                  ))}
                  <Typography.Text code>{route.path}</Typography.Text>
                </Space>
                {route.description ? <Typography.Text type="secondary">{route.description}</Typography.Text> : null}
              </Space>
            </List.Item>
          )}
        />
      </Card>

      {lastError ? <Alert type="error" title={lastError} style={{ marginTop: 12 }} /> : null}

      {response ? (
        <Card size="small" style={{ marginTop: 12 }}>
          <Space size={24} wrap style={{ marginBottom: 12 }}>
            <Statistic title="Status" value={`${response.status} ${response.statusText}`.trim()} />
            <Statistic title="Duration" value={response.elapsedMs} suffix="ms" />
          </Space>
          <Typography.Text strong>Response Headers</Typography.Text>
          <pre>{JSON.stringify(response.headers, null, 2)}</pre>
          <Typography.Text strong>Response Body</Typography.Text>
          <pre>{response.body || "(empty response body)"}</pre>
        </Card>
      ) : null}
    </Card>
  );
}
