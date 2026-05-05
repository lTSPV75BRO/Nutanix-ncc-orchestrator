export type Envelope<T> = {
  success: boolean;
  message?: string;
  data?: T;
  error?: string;
};

export type HealthData = {
  status: string;
  time: string;
  repo_root: string;
  config_path: string;
  output_dir: string;
  schedule_state: string;
  orchestrator_bin: string;
  orchestrator_cmd: string;
  token_file?: string;
  token_source?: string;
};

export type ConfigData = {
  path: string;
  content: string;
};

export type ScheduleState = {
  type: string;
  action: string;
  cron?: string;
  every?: string;
  config: string;
  task_name?: string;
  print_only: boolean;
  updated_at: string;
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

export type RunnerLogData = {
  path: string;
  content: string;
  exists: boolean;
};

export type ReportData = {
  run_summary: unknown;
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
};
