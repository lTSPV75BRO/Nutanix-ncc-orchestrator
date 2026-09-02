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
  /** Whether interactive login (local accounts or SAML) is enabled. */
  login_enabled?: boolean;
  local_login?: boolean;
  saml_enabled?: boolean;
  ldap_enabled?: boolean;
};

export type UserRole = "admin" | "operator" | "viewer" | "";

export type MeData = {
  authenticated: boolean;
  login_enabled: boolean;
  local_enabled: boolean;
  saml_enabled: boolean;
  ldap_enabled?: boolean;
  auth_mode?: string;
  /** True while the built-in admin still has its initial bootstrap password (unchanged). */
  bootstrap_pending?: boolean;
  username?: string;
  role?: UserRole;
  is_admin?: boolean;
  can_operate?: boolean;
  must_change_password?: boolean;
  /** True when the session is backed by a local password account (eligible for self-service password change). */
  is_local?: boolean;
  /** Effective session lifetime (seconds) configured for new logins. */
  session_ttl_sec?: number;
  /** When the current session expires (RFC3339), if authenticated via session. */
  expires_at?: string;
  /** Seconds remaining before the current session expires. */
  expires_in_sec?: number;
  /** False when the caller is confined to cluster groups (non-admin); true for admins/static tokens. */
  cluster_access_unrestricted?: boolean;
  /** When restricted, the clusters the caller may see/act on. */
  allowed_clusters?: string[];
  /** User's preferred run config path. */
  run_config_path?: string;
};

export type SessionPolicy = {
  ttl_sec: number;
  ttl_min: number;
  default_ttl_sec: number;
  min_sec: number;
  max_sec: number;
  source: "runtime" | "default";
};

export type LoginData = {
  username: string;
  role: UserRole;
  must_change_password?: boolean;
  expires_at: string;
  ttl_sec: number;
};

export type UserAccount = {
  username: string;
  role: UserRole;
  must_change_password?: boolean;
  created_at?: string;
  updated_at?: string;
};

export type PasswordResetRequest = {
  username: string;
  requested_at: string;
  client_ip?: string;
};

/** A personal access token's metadata (the secret is never returned after creation). */
export type PersonalToken = {
  id: string;
  name: string;
  owner: string;
  owner_local?: boolean;
  role: string;
  created_at?: string;
  expires_at?: string;
  last_used_at?: string;
  created_ip?: string;
};

/** Response from creating a personal access token; `token` is shown only once. */
export type CreatedToken = {
  id: string;
  name: string;
  role: string;
  token: string;
  created_at?: string;
  expires_at?: string;
};

/** The admin-managed HTTPS/TLS policy for the UI server (no private key is ever returned). */
export type TLSPolicy = {
  https_enabled: boolean;
  subject?: string;
  issuer?: string;
  not_before?: string;
  not_after?: string;
  dns_names?: string[];
  updated_at?: string;
};

/** Response from enabling/disabling HTTPS — the stack restarts to apply it. */
export type TLSApplyResult = {
  tls: TLSPolicy;
  restarting: boolean;
  restart_required: boolean;
};

export type UpdatePhase =
  | "idle"
  | "backing_up"
  | "updating"
  | "restarting"
  | "done"
  | "error";

/** Live status of an in-app software update job. */
export type UpdateJob = {
  in_progress: boolean;
  phase: UpdatePhase;
  message?: string;
  error?: string;
  target_version?: string;
  backup_path?: string;
  started_at?: string;
  finished_at?: string;
};

/** Result of GET /api/v1/settings/update (version check + current job). */
export type UpdateStatus = {
  current_version: string;
  latest_version?: string;
  latest_overall?: string;
  has_package?: boolean;
  update_available?: boolean;
  supported?: boolean;
  check_error?: string;
  check_skipped?: string;
  raw?: string;
  job: UpdateJob;
};

/** One archive in the server-side backups directory. */
export type BackupEntry = {
  name: string;
  kind: "pre-update" | "manual" | "other";
  size: number;
  mod_time: string;
  rollback_candidate?: boolean;
  encrypted?: boolean;
};

export type BackupScheduleState = {
  enabled: boolean;
  every: string;
  encrypt: boolean;
  retain: number;
  last_run_at?: string;
  last_status?: "ok" | "error";
  last_error?: string;
  last_file?: string;
  updated_at?: string;
};

export type BackupScheduleResponse = {
  schedule: BackupScheduleState;
  key_configured: boolean;
  next_run?: string;
};

export type NotificationEvents = {
  run_success?: boolean;
  run_failure?: boolean;
  policy_violations?: boolean;
};

export type SlackNotificationConfig = {
  enabled?: boolean;
  webhook_url?: string;
  channel?: string;
  username?: string;
};

export type WebhookNotificationConfig = {
  enabled?: boolean;
  url?: string;
};

export type EmailNotificationConfig = {
  enabled?: boolean;
  smtp_host?: string;
  smtp_port?: number;
  username?: string;
  password?: string;
  from?: string;
  to?: string;
};

export type QuietHoursConfig = {
  enabled?: boolean;
  start?: string;
  end?: string;
  timezone?: string;
  allow_failures?: boolean;
};

export type MaintenanceWindow = {
  start: string;
  end: string;
  note?: string;
};

export type NotificationThrottle = {
  dedup_window_sec?: number;
  min_interval_sec?: number;
};

export type DigestConfig = {
  enabled?: boolean;
  every?: string;
  last_sent_at?: string;
};

export type NotificationDeliveryStatus = {
  last_attempt_at?: string;
  last_success_at?: string;
  last_event?: string;
  last_error?: string;
  success?: boolean;
  total_success?: number;
  total_failure?: number;
};

export type NotificationState = {
  enabled?: boolean;
  events?: NotificationEvents;
  slack?: SlackNotificationConfig;
  webhook?: WebhookNotificationConfig;
  email?: EmailNotificationConfig;
  quiet?: QuietHoursConfig;
  maintenance?: MaintenanceWindow[];
  throttle?: NotificationThrottle;
  digest?: DigestConfig;
  last_delivery?: Record<string, NotificationDeliveryStatus>;
  updated_at?: string;
};

export type SSOConfig = {
  enabled: boolean;
  managed_by: "flags" | "runtime";
  sp_metadata_url?: string;
  root_url?: string;
  entity_id?: string;
  idp_metadata_url?: string;
  has_idp_metadata_xml?: boolean;
  username_attribute?: string;
  role_attribute?: string;
  role_map?: string;
  default_role?: string;
  allow_idp_initiated?: boolean;
  sp_cert_pem?: string;
};

export type SSOConfigInput = {
  enabled: boolean;
  root_url: string;
  entity_id?: string;
  idp_metadata_xml?: string;
  idp_metadata_url?: string;
  username_attribute?: string;
  role_attribute?: string;
  role_map?: string;
  default_role?: string;
  allow_idp_initiated?: boolean;
};

export type LDAPConfig = {
  enabled: boolean;
  managed_by: "flags" | "runtime";
  url?: string;
  start_tls?: boolean;
  insecure_skip_verify?: boolean;
  has_ca_cert?: boolean;
  bind_dn?: string;
  has_bind_password?: boolean;
  base_dn?: string;
  user_filter?: string;
  username_attribute?: string;
  group_attribute?: string;
  role_map?: string;
  default_role?: string;
};

export type LDAPConfigInput = {
  enabled: boolean;
  url: string;
  start_tls?: boolean;
  insecure_skip_verify?: boolean;
  ca_cert_pem?: string;
  bind_dn?: string;
  /** Write-only. Omit (undefined) to keep the stored secret; empty string clears it. */
  bind_password?: string;
  base_dn: string;
  user_filter?: string;
  username_attribute?: string;
  group_attribute?: string;
  role_map?: string;
  default_role?: string;
};

export type LDAPTestInput = LDAPConfigInput & {
  test_username: string;
  test_password: string;
};

export type LDAPTestResult = {
  username: string;
  role: UserRole;
  groups?: string[];
};

export type ClusterGroup = {
  name: string;
  clusters?: string[];
  prism_centrals?: string[];
  local_users?: string[];
  ad_groups?: string[];
  ad_users?: string[];
};

export type PCCluster = {
  name: string;
  address: string;
};

export type PCDiscoverResult = {
  prism_central: string;
  count: number;
  clusters: PCCluster[];
};

export type DirectoryEntry = {
  type: "group" | "user";
  value: string;
  name: string;
  dn: string;
  upn?: string;
};

export type DirectorySearchResult = {
  ldap_enabled: boolean;
  results: DirectoryEntry[];
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
  /** Acting principal (set by withAuth-attributed entries). */
  user?: string;
  role?: string;
  /** Some auth-flow entries (login/password change) carry an explicit username. */
  username?: string;
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

export type ConfigListItem = {
  path: string;
  resolved: string;
  exists: boolean;
  is_active?: boolean;
};

export type ConfigListData = {
  items: ConfigListItem[];
  default_path?: string;
};

export type ConfigBatchOperation = {
  action: "add" | "update" | "remove" | "delete";
  path: string;
  content?: string;
};

export type ConfigBatchResult = {
  action: string;
  path: string;
  resolved?: string;
  exists?: boolean;
  ok: boolean;
  error?: string;
  validate_output?: string;
};

export type ConfigBatchData = {
  total: number;
  ok: number;
  failed: number;
  results: ConfigBatchResult[];
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

export type ConfigRelatedFileBatchOperation = {
  action: "add" | "update" | "remove" | "delete";
  path: string;
  content?: string;
};

export type ConfigRelatedFileBatchResult = {
  action: string;
  path: string;
  key?: string;
  resolved?: string;
  exists?: boolean;
  ok: boolean;
  error?: string;
};

export type ConfigRelatedFilesBatchData = {
  total: number;
  ok: number;
  failed: number;
  results: ConfigRelatedFileBatchResult[];
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

export type DiagnosticCheck = {
  id: string;
  title: string;
  category: string;
  status: "ok" | "warn" | "fail";
  message: string;
  hint?: string;
  fixed?: boolean;
  fix_message?: string;
  source: "orchestrator" | "api";
  disruptive?: boolean;
};

export type DiagnosticsData = {
  generated_at: string;
  fix_applied: boolean;
  overall: "ok" | "warn" | "fail";
  summary: { ok: number; warn: number; fail: number };
  checks: DiagnosticCheck[];
  auto_fix_loop?: boolean;
  orchestrator_error?: string;
  fix_history?: {
    fixed_ids?: string[];
    fixed_titles?: string[];
    count?: number;
  };
  guardrails?: {
    no_disruptive?: boolean;
    active_run_guard?: boolean;
    allow_disruptive_requested?: boolean;
    allow_disruptive_applied?: boolean;
  };
  verification_runs?: number;
  verified_stable?: boolean;
  actionable?: {
    count?: number;
    auto_fixable?: number;
    manual_action?: number;
    disruptive_skipped?: number;
  };
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
  path?: string;
  mod_time: string;
  has_index: boolean;
  source?: "history" | "summary" | "trigger";
  timestamp?: string;
  duration_s?: number;
  clusters_ok?: number;
  clusters_failed?: number;
  total_checks?: number;
  avg_health_score?: number;
  min_health_score?: number;
  fail_total?: number;
  warn_total?: number;
  err_total?: number;
  info_total?: number;
  exit_code?: number;
  success?: boolean;
  run_source?: string;
  client?: string;
  user_agent?: string;
  auth_mode?: string;
};

// ActiveRunEntry is one queued or running entry in the concurrent run engine.
export type ActiveRunEntry = {
  id: string;
  status: "queued" | "running" | "done";
  group?: string;
  started_at: string;
  queued_at?: string;
  elapsed_sec?: number;
  pid?: number;
  clusters?: string[];
  skipped?: string[];
  skipped_owner?: Record<string, string>;
  all_clusters?: boolean;
  live_output?: string;
  /** 1-based position in the FIFO queue (queued runs only). */
  queue_position?: number;
  /** How the run was launched ("scheduled" | "manual" | "external"). */
  source?: string;
  /** True for runs not launched by the api-server (e.g. systemd-timer scheduled runs). */
  external?: boolean;
};

export type RunActiveData = {
  active: boolean;
  started_at: string;
  elapsed_seconds?: number;
  elapsed_human?: string;
  expected_deadline?: string;
  overdue?: boolean;
  pid?: number;
  last_error: string;
  last_output?: string;
  live_output?: string;
  runner_log?: string;
  output_dir: string;
  config_path: string;
  command?: string[];
  work_dir?: string;
  env?: Record<string, string>;
  // Concurrent run engine.
  runs?: ActiveRunEntry[];
  running_count?: number;
  queued_count?: number;
  max_concurrent?: number;
  /** Mean wall-clock duration of completed runs this process has seen (queue ETA basis). */
  avg_run_duration_sec?: number;
};

export type RunConflictData = {
  active: boolean;
  started_at: string;
  elapsed_seconds: number;
  elapsed_human: string;
  pid: number;
  config_path: string;
  runner_log: string;
  run_timeout: string;
  expected_deadline: string;
  overdue: boolean;
  hint: string;
  poll_endpoint: string;
};

export type RunCancelData = {
  run_id?: string;
  pid?: number;
  cancelled?: number;
  started_at?: string;
  elapsed_seconds?: number;
  elapsed_human?: string;
  poll_endpoint: string;
};

export type RunByIdData = {
  run: RunInfo;
  artifacts: Record<string, unknown>;
};

export type TriggerRunData = {
  run_id?: string;
  started?: boolean;
  queued?: boolean;
  queue_position?: number;
  started_at?: string;
  config_path?: string;
  used_password?: boolean;
  clusters?: string[];
  skipped_clusters?: string[];
  skipped_owner?: Record<string, string>;
  running_count?: number;
  // Legacy field (no longer populated; kept for backward compatibility).
  pid?: number;
  command?: string[];
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

export type AlertSource = "NCC" | "PC";

export type ComponentStatus = {
  version?: string;
  status: string;
};

export type ComponentsData = {
  components: {
    orchestrator: ComponentStatus;
    "api-server": ComponentStatus;
    "ui-server": ComponentStatus;
    consistent: boolean;
  };
};

export type PCAlertsData = {
  alerts: Array<Record<string, unknown>>;
  source: "PC";
  fetched_at?: string;
  cache_hit?: boolean;
  cache_ttl_s?: number;
  errors?: string[];
  configured?: boolean;
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
