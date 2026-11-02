// Package ai contains stable, machine-readable contracts shared by the
// orchestrator, API server, and MCP server.
package ai

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the current AI-facing contract version.
const SchemaVersion = "3.0"

// Result is the common envelope returned by AI-facing operations.
type Result[T any] struct {
	SchemaVersion string       `json:"schema_version"`
	Status        string       `json:"status"`
	Summary       string       `json:"summary,omitempty"`
	Data          T            `json:"data,omitempty"`
	Warnings      []Notice     `json:"warnings,omitempty"`
	NextActions   []NextAction `json:"next_actions,omitempty"`
	RequestID     string       `json:"request_id,omitempty"`
	RunID         string       `json:"run_id,omitempty"`
	JobID         string       `json:"job_id,omitempty"`
	Truncated     bool         `json:"truncated,omitempty"`
}

// Notice is a non-fatal condition that an AI client should be able to explain.
type Notice struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	Retryable   bool   `json:"retryable,omitempty"`
	Remediation string `json:"remediation,omitempty"`
}

// NextAction describes a safe, suggested follow-up operation.
type NextAction struct {
	Tool        string `json:"tool,omitempty"`
	Method      string `json:"method,omitempty"`
	Description string `json:"description"`
	Confirm     bool   `json:"confirm,omitempty"`
}

// Error is the stable machine-readable error envelope.
type Error struct {
	SchemaVersion string         `json:"schema_version"`
	Status        string         `json:"status"`
	Code          string         `json:"code"`
	Category      string         `json:"category"`
	Message       string         `json:"message"`
	Retryable     bool           `json:"retryable,omitempty"`
	Remediation   string         `json:"remediation,omitempty"`
	Fields        []FieldError   `json:"fields,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
	RunID         string         `json:"run_id,omitempty"`
	JobID         string         `json:"job_id,omitempty"`
}

// FieldError identifies an invalid input field.
type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Page is a cursor-paginated collection.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more,omitempty"`
	Total      *int   `json:"total,omitempty"`
}

// Finding is the normalized representation of a check result exposed to AI
// clients. Source-specific evidence remains available in Evidence.
type Finding struct {
	ID          string         `json:"id"`
	RunID       string         `json:"run_id,omitempty"`
	Cluster     string         `json:"cluster"`
	Check       string         `json:"check"`
	Severity    string         `json:"severity"`
	Title       string         `json:"title,omitempty"`
	Detail      string         `json:"detail,omitempty"`
	Remediation string         `json:"remediation,omitempty"`
	Evidence    map[string]any `json:"evidence,omitempty"`
	ReportRef   string         `json:"report_ref,omitempty"`
	ObservedAt  time.Time      `json:"observed_at,omitempty"`
}

// Job is the common lifecycle shape for long-running operations.
type Job struct {
	ID         string     `json:"job_id"`
	Kind       string     `json:"kind"`
	State      string     `json:"state"`
	Phase      string     `json:"phase,omitempty"`
	Progress   *float64   `json:"progress,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	ResultURL  string     `json:"result_url,omitempty"`
	RequestID  string     `json:"request_id,omitempty"`
	Error      *Error     `json:"error,omitempty"`
}

// ErrorCode constants are intentionally stable. Add new codes; do not reuse a
// code for a different remediation contract.
const (
	CodeInvalidArgument     = "INVALID_ARGUMENT"
	CodeConfigInvalid       = "CONFIG_INVALID"
	CodeSecretUnavailable   = "SECRET_UNAVAILABLE"
	CodeClusterUnreachable  = "CLUSTER_UNREACHABLE"
	CodePermissionDenied    = "PERMISSION_DENIED"
	CodeNotFound            = "NOT_FOUND"
	CodeConflict            = "CONFLICT"
	CodeRunInProgress       = "RUN_IN_PROGRESS"
	CodeJobInProgress       = "JOB_IN_PROGRESS"
	CodeTimeout             = "TIMEOUT"
	CodeCancelled           = "CANCELLED"
	CodeUpstreamUnavailable = "UPSTREAM_UNAVAILABLE"
	CodeInternal            = "INTERNAL"
)

// NewError creates a contract error with the supplied stable code.
func NewError(code, category, message string) *Error {
	if strings.TrimSpace(code) == "" {
		code = CodeInternal
	}
	if strings.TrimSpace(category) == "" {
		category = "internal"
	}
	return &Error{
		SchemaVersion: SchemaVersion,
		Status:        "error",
		Code:          code,
		Category:      category,
		Message:       message,
	}
}

// WithRemediation adds actionable guidance and returns the same error.
func (e *Error) WithRemediation(remediation string) *Error {
	e.Remediation = strings.TrimSpace(remediation)
	return e
}

// WithRetryable marks whether retrying the operation is appropriate.
func (e *Error) WithRetryable(retryable bool) *Error {
	e.Retryable = retryable
	return e
}

// MarshalResult is useful when a caller must embed a typed result in an
// existing JSON response without losing the contract version.
func MarshalResult[T any](status, summary string, data T) ([]byte, error) {
	if strings.TrimSpace(status) == "" {
		status = "success"
	}
	return json.Marshal(Result[T]{
		SchemaVersion: SchemaVersion,
		Status:        status,
		Summary:       summary,
		Data:          data,
	})
}

// Validate checks the invariants common to all contract errors.
func (e Error) Validate() error {
	if e.SchemaVersion == "" {
		return fmt.Errorf("schema_version is required")
	}
	if e.Status != "error" {
		return fmt.Errorf("status must be error")
	}
	if e.Code == "" || e.Category == "" || e.Message == "" {
		return fmt.Errorf("code, category, and message are required")
	}
	return nil
}
