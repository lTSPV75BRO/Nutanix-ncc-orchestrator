package types

import (
	"html/template"
	"net/http"
	"os"
)

// TaskStatus represents the status of an NCC task
type TaskStatus struct {
	PercentageComplete int    `json:"percentage_complete"`
	ProgressStatus     string `json:"progress_status"`
}

// NCCSummary represents the summary of an NCC run
type NCCSummary struct {
	RunSummary string `json:"runSummary"`
}

// ParsedBlock represents a parsed block from NCC output
type ParsedBlock struct {
	Severity  string
	CheckName string
	DetailRaw string
}

// Row represents a row in the HTML report
type Row struct {
	Severity  string
	CheckName string
	Detail    template.HTML
}

// AggBlock represents an aggregated block for the combined report
type AggBlock struct {
	Cluster  string
	Severity string
	Check    string
	Detail   string
}

// ClusterResult represents the result of processing a cluster
type ClusterResult struct {
	Cluster string
	Blocks  []ParsedBlock
	Err     error
}

// HTTPClient interface for HTTP operations
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// FS interface for file system operations
type FS interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(path string, data []byte, perm os.FileMode) error
	ReadFile(path string) ([]byte, error)
	ReadDir(path string) ([]os.DirEntry, error)
	Create(path string) (*os.File, error)
}
