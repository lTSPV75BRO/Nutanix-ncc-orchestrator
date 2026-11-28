package main

import (
	"strings"
	"sync"
)

type envelope struct {
	SchemaVersion string      `json:"schema_version,omitempty"`
	Success       bool        `json:"success"`
	Message       string      `json:"message,omitempty"`
	Data          interface{} `json:"data,omitempty"`
	Error         string      `json:"error,omitempty"`
	ErrorCode     string      `json:"error_code,omitempty"`
	// RequestID is set on errors so a user can quote it and an operator can
	// find the matching audit/log line.
	RequestID   string                 `json:"request_id,omitempty"`
	Retryable   bool                   `json:"retryable,omitempty"`
	Remediation string                 `json:"remediation,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
	// RetryAfterSeconds mirrors the Retry-After header on 429 responses
	// (sign-in lockout, per-client rate limit) so the UI can count down.
	RetryAfterSeconds int `json:"retry_after_seconds,omitempty"`
}

type routeMeta struct {
	Path        string   `json:"path"`
	Methods     []string `json:"methods"`
	Description string   `json:"description,omitempty"`
	SampleBody  string   `json:"sample_body,omitempty"`
	// MinRole is the least-privileged role that may call the route (the most
	// restrictive across its methods), surfaced so the API explorer can show
	// what access each endpoint needs. Populated by apiRouteCatalog.
	MinRole string `json:"min_role,omitempty"`
}

type configUpdateRequest struct {
	Path    string `json:"path,omitempty"`
	Content string `json:"content"`
}

type runConfigPreferenceRequest struct {
	Path string `json:"path,omitempty"`
	// Environment, when set, makes the default an environment instead of a path.
	Environment string `json:"environment,omitempty"`
}

type availableConfigFile struct {
	Path     string `json:"path"`
	Resolved string `json:"resolved"`
	Exists   bool   `json:"exists"`
	IsActive bool   `json:"is_active,omitempty"`
}

type configBatchOperation struct {
	Action  string `json:"action"`
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
}

type configBatchRequest struct {
	Operations []configBatchOperation `json:"operations"`
}

type configRelatedFile struct {
	Key          string `json:"key"`
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path"`
	Exists       bool   `json:"exists"`
	Size         int64  `json:"size,omitempty"`
}

type configRelatedFileUpdateRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type configRelatedFileBatchOperation struct {
	Action  string `json:"action"`
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
}

type configRelatedFileBatchRequest struct {
	Operations []configRelatedFileBatchOperation `json:"operations"`
}

type scheduleState struct {
	Type      string `json:"type"`
	Action    string `json:"action"`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	Config    string `json:"config"`
	LogPath   string `json:"log_path,omitempty"`
	WithLock  bool   `json:"with_lock,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly bool   `json:"print_only"`
	UpdatedAt string `json:"updated_at"`
	// ResourceVersion is the Kubernetes CronJob's resourceVersion (or, on a
	// host, a hash of the saved schedule file) when the schedule was read.
	// Sent back on save, it makes the update conditional so two admins can't
	// silently overwrite each other's change.
	ResourceVersion string `json:"resource_version,omitempty"`
}

type scheduleUpdateRequest struct {
	Type      string `json:"type"`
	Action    string `json:"action"`
	Cron      string `json:"cron,omitempty"`
	Every     string `json:"every,omitempty"`
	Config    string `json:"config,omitempty"`
	LogPath   string `json:"log_path,omitempty"`
	WithLock  *bool  `json:"with_lock,omitempty"`
	TaskName  string `json:"task_name,omitempty"`
	PrintOnly *bool  `json:"print_only,omitempty"`
	Apply     bool   `json:"apply"`
	// ResourceVersion: the version the form was loaded from (the CronJob's
	// resourceVersion on Kubernetes, a hash of the saved file on a host).
	// Optional; when set, a save over a newer schedule answers 409.
	ResourceVersion string `json:"resource_version,omitempty"`
}

type runTriggerRequest struct {
	// Environment names configs/environments/<name>.yaml; its runner settings
	// are laid over the main config. Preferred over ConfigPath.
	Environment string   `json:"environment,omitempty"`
	ConfigPath  string   `json:"config_path,omitempty"`
	Password    string   `json:"password,omitempty"`
	ExtraArgs   []string `json:"extra_args,omitempty"`
	// Group optionally scopes the run to a single cluster group (by name).
	Group string `json:"group,omitempty"`
	// Clusters optionally scopes the run to an explicit cluster subset. Both are
	// further intersected with the caller's allowed clusters for non-admins.
	Clusters []string `json:"clusters,omitempty"`
}

type runPreflightRequest struct {
	ConfigPath  string `json:"config_path,omitempty"`
	Environment string `json:"environment,omitempty"`
}

type artifactInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

// runInfo is a single entry in the /api/v1/runs feed. The list combines:
//
//   - "history" entries archived under outputDir/runs/<id>/
//   - the current outputDir/run-summary.json + ncc-run-record.json ("summary")
//   - audit-log `runs.trigger` events that have no artifacts yet ("trigger")
//
// All numeric/metric fields are omitted from JSON when zero so a "trigger"-only
// record stays small and clients can detect the absence of full data.
type runInfo struct {
	ID       string `json:"id"`
	Path     string `json:"path,omitempty"`
	ModTime  string `json:"mod_time"`
	HasIndex bool   `json:"has_index"`

	// Where this row came from. One of: "history" | "summary" | "trigger".
	Source string `json:"source,omitempty"`
	// How the run was launched ("scheduled" | "manual"), from run-summary.json.
	RunSource string `json:"run_source,omitempty"`
	// Environment the run used (configs/environments/<name>.yaml); empty for
	// the main config. From run-meta.json.
	Environment string `json:"environment,omitempty"`

	// Enrichment fields (populated from run-summary.json when available).
	Timestamp      string  `json:"timestamp,omitempty"`
	DurationS      float64 `json:"duration_s,omitempty"`
	ClustersOK     int     `json:"clusters_ok,omitempty"`
	ClustersFailed int     `json:"clusters_failed,omitempty"`
	TotalChecks    int     `json:"total_checks,omitempty"`
	AvgHealthScore int     `json:"avg_health_score,omitempty"`
	MinHealthScore int     `json:"min_health_score,omitempty"`
	FailTotal      int     `json:"fail_total,omitempty"`
	WarnTotal      int     `json:"warn_total,omitempty"`
	ErrTotal       int     `json:"err_total,omitempty"`
	InfoTotal      int     `json:"info_total,omitempty"`
	ExitCode       *int    `json:"exit_code,omitempty"`
	Success        *bool   `json:"success,omitempty"`
	// Status is the explicit run outcome: "success", "failed", "partial"
	// (some clusters failed), "triggered", "trigger_failed", or "unknown"
	// when the outcome genuinely cannot be determined. Clients should prefer
	// it over interpreting Success/ExitCode themselves.
	Status string `json:"status"`
	// StatusSource explains how Status was derived: "exit_code" (recorded by
	// the runner), "legacy_summary" (older runners omitted exit_code when it
	// was 0), "audit" (trigger rows), or "none".
	StatusSource string `json:"status_source,omitempty"`

	// Optional provenance for "trigger"-only rows.
	Client    string `json:"client,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	AuthMode  string `json:"auth_mode,omitempty"`
}

func isInternalArtifactName(name string) bool {
	clean := strings.TrimSpace(name)
	switch clean {
	case ".ncc-preflight-check", ".ncc-prefight-check":
		return true
	default:
		return false
	}
}

type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newTailBuffer(max int) *tailBuffer {
	return &tailBuffer{max: max}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if t.max > 0 && len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}
