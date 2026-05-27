import type {
  ArtifactInfo,
  AuditLogData,
  ConfigData,
  ConfigRelatedFileData,
  ConfigRelatedFilesData,
  Envelope,
  HealthData,
  ReportData,
  ReportTrendsData,
  RunByIdData,
  RunCancelData,
  RunPreflightData,
  RunActiveData,
  RunnerLogData,
  RunInfo,
  ScheduleState,
  ScheduleHealthData,
  TriggerRunData,
} from "./types";

// ApiError preserves both the human-readable message and the structured `data`
// payload from a failed API response. Callers that want to react to specific
// failure modes (e.g. 409 "run in progress") can read `status` and `data`
// directly instead of regex-parsing a flattened message.
export class ApiError extends Error {
  status: number;
  data: unknown;
  constructor(message: string, status: number, data: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.data = data;
  }
}

async function callApi<T>(path: string, init?: RequestInit): Promise<T> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), 30000);
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    signal: ctl.signal,
    headers: {
      "Content-Type": "application/json",
      "X-Requested-With": "ncc-ui",
      ...(init?.headers ?? {}),
    },
  }).finally(() => clearTimeout(timer));
  const contentType = (response.headers.get("content-type") || "").toLowerCase();
  if (!contentType.includes("application/json")) {
    const textBody = (await response.text().catch(() => "")).trim();
    const snippet = textBody ? `\n${textBody.slice(0, 600)}` : "";
    throw new ApiError(
      `unexpected response content-type: ${contentType || "unknown"}${snippet}`,
      response.status,
      undefined,
    );
  }
  const payload = (await response.json().catch(() => ({}))) as Envelope<T>;
  if (!response.ok || !payload.success) {
    throw new ApiError(payload.error ?? response.statusText, response.status, payload.data);
  }
  return (payload.data ?? ({} as T)) as T;
}

function tryParseJSON(raw: string, fallback: unknown): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function loadReportDataFallback(): Promise<ReportData> {
  const runSummary = await api.runSummary().catch(() => ({}));
  const readArtifactJSON = async (name: string, fallback: unknown) => {
    try {
      const a = await api.artifactByName(name);
      return tryParseJSON(a.content, fallback);
    } catch {
      return fallback;
    }
  };
  const policyViolations = await (async () => {
    try {
      const p = await api.artifactByName("policy-gates.txt");
      return String(p.content || "")
        .split("\n")
        .map((s) => s.trim())
        .filter(Boolean);
    } catch {
      return [];
    }
  })();
  return {
    run_summary: runSummary ?? {},
    checks_snapshot: await readArtifactJSON("checks-snapshot.json", []),
    drilldown_diff: await readArtifactJSON("drilldown-diff.json", {}),
    flaky_checks: await readArtifactJSON("flaky-checks.json", {}),
    regression_summary: await readArtifactJSON("regression-summary.json", {}),
    slo_dashboard: await readArtifactJSON("slo-dashboard.json", {}),
    policy_violations: policyViolations,
  };
}

export const api = {
  health: () => callApi<HealthData>("/api/v1/health"),
  loadConfig: () => callApi<ConfigData>("/api/v1/settings/config"),
  saveConfig: (content: string) =>
    callApi<ConfigData>("/api/v1/settings/config", {
      method: "PUT",
      body: JSON.stringify({ content }),
    }),
  listConfigFiles: () => callApi<ConfigRelatedFilesData>("/api/v1/settings/config-files"),
  loadConfigFile: (path: string) =>
    callApi<ConfigRelatedFileData>(`/api/v1/settings/config-file?path=${encodeURIComponent(path)}`),
  saveConfigFile: (path: string, content: string) =>
    callApi<ConfigRelatedFileData>("/api/v1/settings/config-file", {
      method: "PUT",
      body: JSON.stringify({ path, content }),
    }),
  loadSchedule: () => callApi<ScheduleState>("/api/v1/schedule"),
  saveSchedule: (payload: Partial<ScheduleState> & { apply: boolean }) =>
    callApi("/api/v1/schedule", {
      method: "PUT",
      body: JSON.stringify(payload),
    }),
  scheduleHealth: () => callApi<ScheduleHealthData>("/api/v1/schedule/health"),
  runs: (opts?: { limit?: number; source?: "history" | "summary" | "trigger"; since?: string }) => {
    const params = new URLSearchParams();
    if (typeof opts?.limit === "number" && opts.limit > 0) params.set("limit", String(opts.limit));
    if (opts?.source) params.set("source", opts.source);
    if (opts?.since) params.set("since", opts.since);
    const path = params.size > 0 ? `/api/v1/runs?${params.toString()}` : "/api/v1/runs";
    return callApi<RunInfo[]>(path);
  },
  runById: (id: string) => callApi<RunByIdData>(`/api/v1/runs/${encodeURIComponent(id)}`),
  runSummary: () => callApi<unknown>("/api/v1/runs/summary"),
  runActive: () => callApi<RunActiveData>("/api/v1/runs/active"),
  runCancel: () =>
    callApi<RunCancelData>("/api/v1/runs/active", {
      method: "DELETE",
    }),
  runPreflight: (payload: { config_path?: string }) =>
    callApi<RunPreflightData>("/api/v1/runs/preflight", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runTrigger: (payload: { config_path?: string; password?: string; extra_args: string[] }) =>
    callApi<TriggerRunData>("/api/v1/runs/trigger", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  artifacts: () => callApi<ArtifactInfo[]>("/api/v1/artifacts"),
  artifactByName: (name: string) => callApi<{ name: string; content: string }>(`/api/v1/artifacts/${encodeURIComponent(name)}`),
  reportDataWithPagination: async (opts?: { limit?: number; offset?: number }) => {
    const params = new URLSearchParams();
    if (typeof opts?.limit === "number" && opts.limit >= 0) params.set("limit", String(opts.limit));
    if (typeof opts?.offset === "number" && opts.offset >= 0) params.set("offset", String(opts.offset));
    const path = params.size > 0 ? `/api/v1/report/data?${params.toString()}` : "/api/v1/report/data";
    try {
      return await callApi<ReportData>(path);
    } catch {
      return loadReportDataFallback();
    }
  },
  reportData: async () => {
    try {
      return await callApi<ReportData>("/api/v1/report/data");
    } catch {
      return loadReportDataFallback();
    }
  },
  reportTrends: (limit = 30) => callApi<ReportTrendsData>(`/api/v1/report/trends?limit=${encodeURIComponent(String(limit))}`),
  runnerLogs: () => callApi<RunnerLogData>("/api/v1/logs/runner"),
  audit: (opts?: { limit?: number; action?: string; failures?: boolean }) => {
    const params = new URLSearchParams();
    if (typeof opts?.limit === "number" && opts.limit > 0) params.set("limit", String(opts.limit));
    if (opts?.action) params.set("action", opts.action);
    if (opts?.failures) params.set("failures", "1");
    const path = params.size > 0 ? `/api/v1/audit?${params.toString()}` : "/api/v1/audit";
    return callApi<AuditLogData>(path);
  },
};
