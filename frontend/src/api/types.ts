export type Envelope<T> = {
  success: boolean;
  message?: string;
  data?: T;
  error?: string;
};

export type HealthData = {
  status: string;
  time: string;
  config_path: string;
  output_dir: string;
  log_dir?: string;
  token_file?: string;
  token_source?: string;
  auth_mode?: string;
  repo_root?: string;
  schedule_state?: string;
  orchestrator_bin?: string;
  orchestrator_cmd?: string;
  /** API server version, e.g. "2.0.0" or "2.0.0-<sha>". */
  version?: string;
  /** Build timestamp (RFC3339) injected via -ldflags at build time. */
  build_date?: string;
  /** Release stream, e.g. "Release", "Beta", "dev". */
  stream?: string;
  /** Go toolchain that built the binary. */
  go_version?: string;
  os?: string;
  arch?: string;
};

export type AuditLogEntry = {
  ts: string;
  action: string;
  success: boolean;
  path?: string;
  method?: string;
  client?: string;
  auth_mode?: string;
  user_agent?: string;
  /** All other audit fields land here. */
  [key: string]: unknown;
};

export type AuditLogData = {
  path: string;
  size: number;
  mod_time: string;
  limit: number;
  count: number;
  max_bytes: number;
  entries: AuditLogEntry[];
  filters: {
    action: string;
    failures: boolean;
  };
};

export type ConfigData = {
  path: string;
  content: string;
};

export type ConfigRelatedFileInfo = {
  key: string;
  path: string;
  resolved_path: string;
  exists: boolean;
  size?: number;
};

export type ConfigRelatedFilesData = {
  items: ConfigRelatedFileInfo[];
};

export type ConfigRelatedFileData = {
  key: string;
  path: string;
  resolved: string;
  exists: boolean;
  content: string;
};

export type ScheduleState = {
  type: string;
  action: string;
  cron?: string;
  every?: string;
  config: string;
  log_path?: string;
  with_lock?: boolean;
  task_name?: string;
  print_only: boolean;
  updated_at: string;
};

export type ScheduleHealthData = {
  configured: boolean;
  saved?: boolean;
  installed?: boolean;
  state_file_exists?: boolean;
  task_name?: string;
  type?: string;
  action?: string;
  with_lock?: boolean;
  log_path?: string;
  lock_path?: string;
  last_updated_at?: string;
  last_run?: string;
  last_success?: string;
  last_error?: string;
  log_exists?: boolean;
  log_size?: number;
  log_mod_time?: string;
  detector?: string;
  install_check_error?: string;
  error?: string;
};

export type ArtifactInfo = {
  name: string;
  size: number;
  mod_time: string;
};

export type RunInfo = {
  id: string;
  path: string;
  mod_time: string;
  has_index: boolean;
};

export type RunActiveData = {
  active: boolean;
  started_at: string;
  last_error: string;
  last_output?: string;
  live_output?: string;
  runner_log?: string;
  output_dir: string;
  config_path: string;
  command?: string[];
  work_dir?: string;
  env?: Record<string, string>;
};

export type TriggerRunData = {
  pid: number;
  command: string[];
  started_at: string;
  config_path?: string;
  used_password?: boolean;
};

export type PreflightCheck = {
  id: string;
  status: "pass" | "fail" | "warn";
  title: string;
  message: string;
  remediation_code?: string;
  hint?: string;
  output?: string;
};

export type RunPreflightData = {
  ok: boolean;
  failed: number;
  warn: number;
  config_path: string;
  checks: PreflightCheck[];
  actionableHints: string[];
};

export type RunnerLogData = {
  path: string;
  content: string;
  exists: boolean;
};

export type ReportData = {
  run_summary: unknown;
  ncc_summary_counts?: Record<string, number>;
  ncc_cluster_summary?: Array<Record<string, unknown>>;
  checks_snapshot: unknown;
  drilldown_diff: unknown;
  flaky_checks: unknown;
  regression_summary: unknown;
  slo_dashboard: unknown;
  policy_violations: string[];
  agg_rows?: unknown[];
  diff_flags?: Record<string, unknown>;
  flaky_keys?: Record<string, unknown>;
  cluster_links?: unknown[];
  artifact_links?: Record<string, string>;
  report_meta?: Record<string, unknown>;
  ncc_logs?: Array<{ name: string; path: string }>;
  pagination?: Record<
    string,
    {
      total: number;
      offset: number;
      limit: number;
      count: number;
      has_more: boolean;
    }
  >;
};

export type TrendPoint = {
  timestamp: string;
  duration_s: number;
  clusters_ok: number;
  clusters_failed: number;
  total_checks: number;
  avg_health_score: number;
  min_health_score: number;
  fail_total: number;
  warn_total: number;
  err_total: number;
  info_total: number;
};

export type ReportTrendsData = {
  points: TrendPoint[];
  count: number;
  limit: number;
  report_source_dir: string;
};
