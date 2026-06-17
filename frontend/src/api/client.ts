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
  DiagnosticsData,
  TriggerRunData,
  MeData,
  LoginData,
  UserAccount,
  UserRole,
  SSOConfig,
  SSOConfigInput,
  ClusterGroup,
  DirectorySearchResult,
  PCDiscoverResult,
  LDAPConfig,
  LDAPConfigInput,
  LDAPTestInput,
  LDAPTestResult,
  SessionPolicy,
  PasswordResetRequest,
  PersonalToken,
  CreatedToken,
  TLSPolicy,
  TLSApplyResult,
  UpdateStatus,
  UpdateJob,
  BackupEntry,
  BackupScheduleResponse,
  BackupScheduleState,
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

// readCookie returns the value of a document cookie by name, or "" if absent.
function readCookie(name: string): string {
  const prefix = `${name}=`;
  for (const part of document.cookie.split(";")) {
    const c = part.trim();
    if (c.startsWith(prefix)) return decodeURIComponent(c.slice(prefix.length));
  }
  return "";
}

const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

export type AuditQuery = {
  limit?: number;
  action?: string;
  failures?: boolean;
  user?: string;
  since?: string;
  until?: string;
  format?: "csv";
};

function buildAuditPath(opts?: AuditQuery): string {
  const params = new URLSearchParams();
  if (typeof opts?.limit === "number" && opts.limit > 0) params.set("limit", String(opts.limit));
  if (opts?.action) params.set("action", opts.action);
  if (opts?.failures) params.set("failures", "1");
  if (opts?.user) params.set("user", opts.user);
  if (opts?.since) params.set("since", opts.since);
  if (opts?.until) params.set("until", opts.until);
  if (opts?.format) params.set("format", opts.format);
  return params.size > 0 ? `/api/v1/audit?${params.toString()}` : "/api/v1/audit";
}

async function callApi<T>(path: string, init?: RequestInit): Promise<T> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), 30000);
  const method = (init?.method ?? "GET").toUpperCase();
  // Cookie-based sessions require the CSRF double-submit token on mutations.
  // The token is harmless to send otherwise (static-token automation ignores
  // it), so attach it whenever it's present and the method is state-changing.
  const csrfHeader: Record<string, string> = {};
  if (MUTATING_METHODS.has(method)) {
    const csrf = readCookie("ncc_csrf");
    if (csrf) csrfHeader["X-CSRF-Token"] = csrf;
  }
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    signal: ctl.signal,
    headers: {
      "Content-Type": "application/json",
      "X-Requested-With": "ncc-ui",
      ...csrfHeader,
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

// callApiEnvelope is like callApi but returns the full envelope so callers can
// read the server-provided `message` (e.g. the admin self-reset guidance).
async function callApiEnvelope<T>(path: string, init?: RequestInit): Promise<Envelope<T>> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), 30000);
  const method = (init?.method ?? "GET").toUpperCase();
  const csrfHeader: Record<string, string> = {};
  if (MUTATING_METHODS.has(method)) {
    const csrf = readCookie("ncc_csrf");
    if (csrf) csrfHeader["X-CSRF-Token"] = csrf;
  }
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    signal: ctl.signal,
    headers: {
      "Content-Type": "application/json",
      "X-Requested-With": "ncc-ui",
      ...csrfHeader,
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
  return payload;
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
  diagnostics: () => callApi<DiagnosticsData>("/api/v1/health/diagnostics"),
  healDiagnostics: () => callApi<DiagnosticsData>("/api/v1/health/diagnostics", { method: "POST" }),
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
  runCancel: (id?: string) =>
    callApi<RunCancelData>(id ? `/api/v1/runs/active?id=${encodeURIComponent(id)}` : "/api/v1/runs/active", {
      method: "DELETE",
    }),
  runPreflight: (payload: { config_path?: string }) =>
    callApi<RunPreflightData>("/api/v1/runs/preflight", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runTrigger: (payload: {
    config_path?: string;
    password?: string;
    extra_args: string[];
    group?: string;
    clusters?: string[];
  }) =>
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
  audit: (opts?: AuditQuery) => {
    const path = buildAuditPath(opts);
    return callApi<AuditLogData>(path);
  },
  // auditExportCSV fetches the filtered audit log as CSV text (the backend
  // streams a downloadable file). Returns the raw CSV so the caller can build a
  // Blob download; uses a raw fetch because the response is not JSON.
  auditExportCSV: async (opts?: AuditQuery): Promise<string> => {
    const path = buildAuditPath({ ...opts, format: "csv" });
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: { "X-Requested-With": "ncc-ui" },
    });
    if (!response.ok) {
      throw new ApiError(`audit export failed (${response.status})`, response.status, undefined);
    }
    return response.text();
  },
  me: () => callApi<MeData>("/api/v1/auth/me"),
  login: (username: string, password: string) =>
    callApi<LoginData>("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  logout: () => callApi<unknown>("/api/v1/auth/logout", { method: "POST" }),
  logoutAll: () => callApi<unknown>("/api/v1/auth/logout-all", { method: "POST" }),
  forgotPassword: (username: string) =>
    callApiEnvelope<{ admin_reset?: boolean }>("/api/v1/auth/forgot-password", {
      method: "POST",
      body: JSON.stringify({ username }),
    }),
  listPasswordResets: () =>
    callApi<{ requests: PasswordResetRequest[] }>("/api/v1/settings/password-resets"),
  dismissPasswordReset: (username: string) =>
    callApi<unknown>(`/api/v1/settings/password-resets/${encodeURIComponent(username)}`, {
      method: "DELETE",
    }),
  refreshSession: () =>
    callApi<{ expires_at: string; ttl_sec: number }>("/api/v1/auth/refresh", { method: "POST" }),
  changePassword: (currentPassword: string, newPassword: string) =>
    callApi<unknown>("/api/v1/auth/change-password", {
      method: "POST",
      body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
    }),
  listUsers: () => callApi<{ users: UserAccount[] }>("/api/v1/settings/users"),
  createUser: (payload: { username: string; password: string; role: UserRole; must_change_password?: boolean }) =>
    callApi<unknown>("/api/v1/settings/users", { method: "POST", body: JSON.stringify(payload) }),
  updateUser: (
    username: string,
    payload: {
      role?: UserRole;
      password?: string;
      must_change_password?: boolean;
      generate_password?: boolean;
      revoke_sessions?: boolean;
    },
  ) =>
    callApi<{ temporary_password?: string; bootstrap_file?: string }>(
      `/api/v1/settings/users/${encodeURIComponent(username)}`,
      {
        method: "PUT",
        body: JSON.stringify(payload),
      },
    ),
  deleteUser: (username: string) =>
    callApi<unknown>(`/api/v1/settings/users/${encodeURIComponent(username)}`, { method: "DELETE" }),
  getSSO: () => callApi<SSOConfig>("/api/v1/settings/sso"),
  updateSSO: (payload: SSOConfigInput) =>
    callApi<unknown>("/api/v1/settings/sso", { method: "PUT", body: JSON.stringify(payload) }),
  getLDAP: () => callApi<LDAPConfig>("/api/v1/settings/ldap"),
  updateLDAP: (payload: LDAPConfigInput) =>
    callApi<unknown>("/api/v1/settings/ldap", { method: "PUT", body: JSON.stringify(payload) }),
  testLDAP: (payload: LDAPTestInput) =>
    callApi<LDAPTestResult>("/api/v1/settings/ldap/test", { method: "POST", body: JSON.stringify(payload) }),
  getClusterGroups: () => callApi<{ groups: ClusterGroup[] }>("/api/v1/settings/cluster-groups"),
  updateClusterGroups: (groups: ClusterGroup[]) =>
    callApi<unknown>("/api/v1/settings/cluster-groups", { method: "PUT", body: JSON.stringify({ groups }) }),
  getClusterInventory: () => callApi<{ clusters: string[] }>("/api/v1/settings/clusters"),
  discoverPCClusters: (pc: string) =>
    callApi<PCDiscoverResult>(`/api/v1/settings/pc-clusters?pc=${encodeURIComponent(pc)}`),
  searchDirectory: (q: string, type: "group" | "user" | "all" = "all", limit = 25) =>
    callApi<DirectorySearchResult>(
      `/api/v1/settings/ldap/search?q=${encodeURIComponent(q)}&type=${type}&limit=${limit}`,
    ),
  downloadBackup: async (
    passphrase?: string,
  ): Promise<{ blob: Blob; filename: string }> => {
    const csrf = readCookie("ncc_csrf");
    const pass = (passphrase ?? "").trim();
    const response = await fetch("/api/v1/settings/backup", {
      method: "GET",
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "ncc-ui",
        ...(csrf ? { "X-CSRF-Token": csrf } : {}),
        ...(pass ? { "X-NCC-Backup-Passphrase": pass } : {}),
      },
    });
    const contentType = (response.headers.get("content-type") || "").toLowerCase();
    if (!response.ok || contentType.includes("application/json")) {
      const payload = (await response.json().catch(() => ({}))) as Envelope<unknown>;
      throw new ApiError(payload.error ?? response.statusText, response.status, payload.data);
    }
    const disposition = response.headers.get("content-disposition") || "";
    const match = /filename="?([^"]+)"?/.exec(disposition);
    const filename = match?.[1] ?? `ncc-backup-${new Date().toISOString().replace(/[:.]/g, "-")}.tar.gz`;
    return { blob: await response.blob(), filename };
  },
  restoreBackup: async (file: File, passphrase?: string): Promise<Envelope<unknown>> => {
    const csrf = readCookie("ncc_csrf");
    const form = new FormData();
    form.append("archive", file);
    const pass = (passphrase ?? "").trim();
    if (pass) form.append("passphrase", pass);
    const response = await fetch("/api/v1/settings/restore", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "ncc-ui",
        ...(csrf ? { "X-CSRF-Token": csrf } : {}),
      },
      body: form,
    });
    const payload = (await response.json().catch(() => ({}))) as Envelope<unknown>;
    if (!response.ok || !payload.success) {
      throw new ApiError(payload.error ?? response.statusText, response.status, payload.data);
    }
    return payload;
  },
  getSessionPolicy: () => callApi<SessionPolicy>("/api/v1/settings/session"),
  updateSessionPolicy: (payload: { ttl_min?: number; ttl_sec?: number }) =>
    callApi<{ ttl_sec: number }>("/api/v1/settings/session", {
      method: "PUT",
      body: JSON.stringify(payload),
    }),
  listTokens: () => callApi<{ tokens: PersonalToken[] }>("/api/v1/auth/tokens"),
  createToken: (payload: { name: string; expires_in_days?: number }) =>
    callApi<CreatedToken>("/api/v1/auth/tokens", { method: "POST", body: JSON.stringify(payload) }),
  revokeToken: (id: string) =>
    callApi<unknown>(`/api/v1/auth/tokens/${encodeURIComponent(id)}`, { method: "DELETE" }),
  listAllTokens: () => callApi<{ tokens: PersonalToken[] }>("/api/v1/settings/tokens"),
  adminRevokeToken: (id: string) =>
    callApi<unknown>(`/api/v1/settings/tokens/${encodeURIComponent(id)}`, { method: "DELETE" }),
  getTLS: () => callApi<TLSPolicy>("/api/v1/settings/tls"),
  installTLS: (payload: { cert: string; key: string }) =>
    callApi<TLSApplyResult>("/api/v1/settings/tls", { method: "PUT", body: JSON.stringify(payload) }),
  generateTLS: (payload?: { hosts?: string[]; valid_days?: number }) =>
    callApi<TLSApplyResult>("/api/v1/settings/tls/generate", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  disableTLS: () => callApi<TLSApplyResult>("/api/v1/settings/tls", { method: "DELETE" }),
  updateStatus: () => callApi<UpdateStatus>("/api/v1/settings/update"),
  checkUpdate: () => callApi<UpdateStatus>("/api/v1/settings/update?check=1"),
  applyUpdate: (payload?: { target_version?: string }) =>
    callApiEnvelope<UpdateJob>("/api/v1/settings/update/apply", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  listBackups: () => callApi<{ backups: BackupEntry[] }>("/api/v1/settings/backups"),
  createBackup: (passphrase?: string) => {
    const pass = (passphrase ?? "").trim();
    return callApiEnvelope<{ backup: BackupEntry }>("/api/v1/settings/backups", {
      method: "POST",
      ...(pass ? { body: JSON.stringify({ passphrase: pass }) } : {}),
    });
  },
  restoreNamedBackup: (name: string, passphrase?: string) => {
    const pass = (passphrase ?? "").trim();
    return callApiEnvelope<{ restarting?: boolean }>("/api/v1/settings/backups/restore", {
      method: "POST",
      body: JSON.stringify(pass ? { name, passphrase: pass } : { name }),
    });
  },
  getBackupSchedule: () => callApi<BackupScheduleResponse>("/api/v1/settings/backups/schedule"),
  updateBackupSchedule: (payload: {
    enabled: boolean;
    every: string;
    encrypt: boolean;
    retain: number;
    run_now?: boolean;
  }) =>
    callApiEnvelope<{ schedule: BackupScheduleState }>("/api/v1/settings/backups/schedule", {
      method: "PUT",
      body: JSON.stringify(payload),
    }),
  verifyNamedBackup: (name: string, passphrase?: string) => {
    const pass = (passphrase ?? "").trim();
    return callApiEnvelope<{ output?: string }>("/api/v1/settings/backups/verify", {
      method: "POST",
      body: JSON.stringify(pass ? { name, passphrase: pass } : { name }),
    });
  },
  deleteBackup: (name: string) =>
    callApi<unknown>("/api/v1/settings/backups/delete", {
      method: "POST",
      body: JSON.stringify({ name }),
    }),
  downloadNamedBackup: async (name: string): Promise<{ blob: Blob; filename: string }> => {
    const csrf = readCookie("ncc_csrf");
    const response = await fetch(
      `/api/v1/settings/backups/download?name=${encodeURIComponent(name)}`,
      {
        method: "GET",
        credentials: "same-origin",
        headers: {
          "X-Requested-With": "ncc-ui",
          ...(csrf ? { "X-CSRF-Token": csrf } : {}),
        },
      },
    );
    const contentType = (response.headers.get("content-type") || "").toLowerCase();
    if (!response.ok || contentType.includes("application/json")) {
      const payload = (await response.json().catch(() => ({}))) as Envelope<unknown>;
      throw new ApiError(payload.error ?? response.statusText, response.status, payload.data);
    }
    return { blob: await response.blob(), filename: name };
  },
};
