package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// ==================== Utility Function Tests ====================

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Normal CSV",
			input:    "FAIL,WARN,ERR",
			expected: []string{"FAIL", "WARN", "ERR"},
		},
		{
			name:     "With spaces",
			input:    "FAIL, WARN , ERR",
			expected: []string{"FAIL", "WARN", "ERR"},
		},
		{
			name:     "Empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "Single value",
			input:    "FAIL",
			expected: []string{"FAIL"},
		},
		{
			name:     "Empty values filtered",
			input:    "FAIL,,WARN,  ,ERR",
			expected: []string{"FAIL", "WARN", "ERR"},
		},
		{
			name:     "Only spaces",
			input:    "   ,  ,  ",
			expected: nil,
		},
		{
			name:     "Mixed case with spaces",
			input:    "  FAIL  ,  WARN  ,  ERR  ",
			expected: []string{"FAIL", "WARN", "ERR"},
		},
		{
			name:     "Single comma",
			input:    ",",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitCSV(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d items, got %d", len(tt.expected), len(result))
				return
			}
			for i, exp := range tt.expected {
				if result[i] != exp {
					t.Errorf("Expected %s at index %d, got %s", exp, i, result[i])
				}
			}
		})
	}
}

func TestValidateSecretsFileHardening(t *testing.T) {
	tmpDir := t.TempDir()
	okPath := filepath.Join(tmpDir, "secrets.yaml")
	if err := os.WriteFile(okPath, []byte("password: secret\n"), 0o600); err != nil {
		t.Fatalf("write secrets file: %v", err)
	}
	if err := validateSecretsFileHardening(okPath); err != nil {
		t.Fatalf("expected secure file to pass, got: %v", err)
	}
	loosePath := filepath.Join(tmpDir, "secrets-loose.yaml")
	if err := os.WriteFile(loosePath, []byte("password: secret\n"), 0o644); err != nil {
		t.Fatalf("write loose secrets file: %v", err)
	}
	if err := validateSecretsFileHardening(loosePath); err == nil {
		t.Fatal("expected loose permissions to fail hardening check")
	}
}

func TestApplyArtifactRetentionPolicies(t *testing.T) {
	tmpDir := t.TempDir()
	protected := filepath.Join(tmpDir, "index.html")
	if err := os.WriteFile(protected, []byte("index"), 0o644); err != nil {
		t.Fatalf("write protected file: %v", err)
	}
	oldFile := filepath.Join(tmpDir, "old.log")
	if err := os.WriteFile(oldFile, []byte("old"), 0o644); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	newFile := filepath.Join(tmpDir, "new.log")
	if err := os.WriteFile(newFile, []byte("new"), 0o644); err != nil {
		t.Fatalf("write new file: %v", err)
	}
	now := time.Now()
	oldTime := now.Add(-72 * time.Hour)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes old file: %v", err)
	}
	if _, err := applyArtifactRetentionPolicies(tmpDir, 2, 1, now); err != nil {
		t.Fatalf("applyArtifactRetentionPolicies failed: %v", err)
	}
	if _, err := os.Stat(protected); err != nil {
		t.Fatalf("protected file should remain: %v", err)
	}
	if _, err := os.Stat(oldFile); err == nil {
		t.Fatal("old file should have been deleted by retention")
	}
}

func TestClassifyClusterError(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"context deadline exceeded", "timeout"},
		{"start checks failed: get summary failed: HTTP 401", "auth"},
		{"retry circuit breaker opened after 3 consecutive retryable failures (last HTTP 429)", "rate_limit"},
		{"parse filtered failed", "parser"},
		{"dial tcp 10.0.0.1:9440: connect: connection refused", "network"},
		{"start checks failed: get summary failed: HTTP 500", "api"},
	}
	for _, tt := range tests {
		got := classifyClusterError(errors.New(tt.msg))
		if got != tt.want {
			t.Fatalf("classifyClusterError(%q)=%q want %q", tt.msg, got, tt.want)
		}
	}
}

func TestDoWithRetryCircuitBreaker(t *testing.T) {
	attempts := 0
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{
				StatusCode: 503,
				Body:       io.NopCloser(strings.NewReader("service unavailable")),
				Header:     make(http.Header),
			}, nil
		},
	}
	cfg := Config{
		RetryMaxAttempts:    6,
		RetryBaseDelay:      1 * time.Millisecond,
		RetryMaxDelay:       2 * time.Millisecond,
		RetryCircuitBreaker: 2,
		RequestTimeout:      2 * time.Second,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	_, _, err = doWithRetry(context.Background(), client, req, cfg, "test-op")
	if err == nil {
		t.Fatal("expected circuit breaker error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "circuit breaker") {
		t.Fatalf("expected circuit breaker error, got %v", err)
	}
	if attempts != cfg.RetryCircuitBreaker {
		t.Fatalf("expected %d attempts before breaker, got %d", cfg.RetryCircuitBreaker, attempts)
	}
}

func TestMustParseDur(t *testing.T) {
	defaultDur := 5 * time.Second

	tests := []struct {
		name     string
		input    string
		expected time.Duration
	}{
		{
			name:     "Valid duration",
			input:    "10s",
			expected: 10 * time.Second,
		},
		{
			name:     "Valid minutes",
			input:    "5m",
			expected: 5 * time.Minute,
		},
		{
			name:     "Valid hours",
			input:    "2h",
			expected: 2 * time.Hour,
		},
		{
			name:     "Empty string returns default",
			input:    "",
			expected: defaultDur,
		},
		{
			name:     "Invalid duration returns default",
			input:    "invalid",
			expected: defaultDur,
		},
		{
			name:     "Milliseconds",
			input:    "500ms",
			expected: 500 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mustParseDur(tt.input, defaultDur)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestSanitizeLabel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normal string",
			input:    "test-label",
			expected: "test-label",
		},
		{
			name:     "With quotes",
			input:    `test"label`,
			expected: `test\"label`,
		},
		{
			name:     "With backslash",
			input:    `test\label`,
			expected: `test\\label`,
		},
		{
			name:     "With newline",
			input:    "test\nlabel",
			expected: "test label",
		},
		{
			name:     "With spaces",
			input:    "  test label  ",
			expected: "test label",
		},
		{
			name:     "Multiple backslashes",
			input:    `test\\label`,
			expected: `test\\\\label`,
		},
		{
			name:     "Multiple quotes",
			input:    `test""label`,
			expected: `test\"\"label`,
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "Only spaces",
			input:    "   ",
			expected: "",
		},
		{
			name:     "Mixed special chars",
			input:    `test\label"value`,
			expected: `test\\label\"value`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeLabel(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Trace by name", "trace", "trace"},
		{"Trace by number", "0", "trace"},
		{"Debug by name", "debug", "debug"},
		{"Debug by number", "1", "debug"},
		{"Info by name", "info", "info"},
		{"Info by number", "2", "info"},
		{"Warn by name", "warn", "warn"},
		{"Warn by number", "3", "warn"},
		{"Error by name", "error", "error"},
		{"Error by number", "4", "error"},
		{"Case insensitive", "INFO", "info"},
		{"With spaces", "  info  ", "info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseLogLevel(tt.input)
			// Just verify it doesn't panic and returns a valid level
			if result.String() == "" {
				t.Error("Expected valid log level")
			}
		})
	}
}

// ==================== Severity Filtering Tests ====================

func TestFilterBlocksBySeverity(t *testing.T) {
	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "Check1", DetailRaw: "Detail1"},
		{Severity: "WARN", CheckName: "Check2", DetailRaw: "Detail2"},
		{Severity: "INFO", CheckName: "Check3", DetailRaw: "Detail3"},
		{Severity: "ERR", CheckName: "Check4", DetailRaw: "Detail4"},
		{Severity: "", CheckName: "Check5", DetailRaw: "Detail5"}, // Empty severity defaults to INFO
		{Severity: "FAIL", CheckName: "Check6", DetailRaw: "Detail6"},
	}

	tests := []struct {
		name          string
		allowed       []string
		expectedCount int
		expectedSevs  []string
	}{
		{
			name:          "Filter FAIL only",
			allowed:       []string{"FAIL"},
			expectedCount: 2,
			expectedSevs:  []string{"FAIL"},
		},
		{
			name:          "Filter FAIL and WARN",
			allowed:       []string{"FAIL", "WARN"},
			expectedCount: 3,
			expectedSevs:  []string{"FAIL", "WARN"},
		},
		{
			name:          "No filter (empty)",
			allowed:       []string{},
			expectedCount: 6,
			expectedSevs:  []string{"FAIL", "WARN", "INFO", "ERR", "INFO", "FAIL"},
		},
		{
			name:          "Filter all severities",
			allowed:       []string{"FAIL", "WARN", "ERR", "INFO"},
			expectedCount: 6,
			expectedSevs:  []string{"FAIL", "WARN", "INFO", "ERR", "INFO", "FAIL"},
		},
		{
			name:          "Filter single severity (ERR)",
			allowed:       []string{"ERR"},
			expectedCount: 1,
			expectedSevs:  []string{"ERR"},
		},
		{
			name:          "Filter with case variations (case-insensitive)",
			allowed:       []string{"fail", "WARN"},
			expectedCount: 3, // Case-insensitive, so "fail" matches "FAIL" and "WARN" matches "WARN"
			expectedSevs:  []string{"FAIL", "WARN", "FAIL"},
		},
		{
			name:          "Filter non-existent severity",
			allowed:       []string{"UNKNOWN"},
			expectedCount: 0,
			expectedSevs:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterBlocksBySeverity(blocks, tt.allowed)
			if len(result) != tt.expectedCount {
				t.Errorf("Expected %d blocks, got %d", tt.expectedCount, len(result))
			}
			for i, block := range result {
				sev := block.Severity
				if sev == "" {
					sev = "INFO"
				}
				found := false
				for _, expectedSev := range tt.expectedSevs {
					if sev == expectedSev {
						found = true
						break
					}
				}
				if !found && len(tt.allowed) > 0 {
					t.Errorf("Block %d has severity %s which is not in allowed list", i, sev)
				}
			}
		})
	}
}

func TestFilterBlocksByTitle(t *testing.T) {
	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "AOS service health", DetailRaw: "Detail1"},
		{Severity: "WARN", CheckName: "Disk health", DetailRaw: "Detail2"},
		{Severity: "INFO", CheckName: "Prism connectivity", DetailRaw: "Detail3"},
		{Severity: "ERR", CheckName: "AOS service health degraded", DetailRaw: "Detail4"},
	}

	tests := []struct {
		name          string
		excluded      []string
		mode          string
		expectedCount int
		excludedCount int
		expectTitles  []string
		wantErr       bool
	}{
		{
			name:          "Exclude single title",
			excluded:      []string{"AOS service health"},
			mode:          "exact",
			expectedCount: 3,
			excludedCount: 1,
			expectTitles:  []string{"Disk health", "Prism connectivity", "AOS service health degraded"},
		},
		{
			name:          "Exclude contains mode",
			excluded:      []string{"service health"},
			mode:          "contains",
			expectedCount: 2,
			excludedCount: 2,
			expectTitles:  []string{"Disk health", "Prism connectivity"},
		},
		{
			name:          "Exclude regex mode",
			excluded:      []string{"AOS\\s+service\\s+health.*"},
			mode:          "regex",
			expectedCount: 2,
			excludedCount: 2,
			expectTitles:  []string{"Disk health", "Prism connectivity"},
		},
		{
			name:          "Exclude multiple titles exact",
			excluded:      []string{"AOS service health", "Disk health"},
			mode:          "exact",
			expectedCount: 2,
			excludedCount: 2,
			expectTitles:  []string{"Prism connectivity", "AOS service health degraded"},
		},
		{
			name:          "Exclude with case-insensitive exact match",
			excluded:      []string{"aos SERVICE health"},
			mode:          "exact",
			expectedCount: 3,
			excludedCount: 1,
			expectTitles:  []string{"Disk health", "Prism connectivity", "AOS service health degraded"},
		},
		{
			name:          "No exclusions",
			excluded:      []string{},
			mode:          "exact",
			expectedCount: 4,
			excludedCount: 0,
			expectTitles:  []string{"AOS service health", "Disk health", "Prism connectivity", "AOS service health degraded"},
		},
		{
			name:          "Invalid regex",
			excluded:      []string{"[unclosed"},
			mode:          "regex",
			expectedCount: 4,
			excludedCount: 0,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, excluded, err := filterBlocksByTitle(blocks, tt.excluded, tt.mode)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != tt.expectedCount {
				t.Fatalf("expected %d blocks, got %d", tt.expectedCount, len(result))
			}
			if len(excluded) != tt.excludedCount {
				t.Fatalf("expected %d excluded blocks, got %d", tt.excludedCount, len(excluded))
			}
			for i, b := range result {
				if !contains(tt.expectTitles, b.CheckName) {
					t.Errorf("row %d has unexpected title %q", i, b.CheckName)
				}
			}
		})
	}
}

// ==================== JSON Generation Tests ====================

func TestGenerateJSON(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		blocks  []ParsedBlock
		meta    HTMLMeta
		wantErr bool
	}{
		{
			name: "Normal blocks",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "Test Check 1", DetailRaw: "Test detail 1"},
				{Severity: "WARN", CheckName: "Test Check 2", DetailRaw: "Test detail 2"},
				{Severity: "INFO", CheckName: "Test Check 3", DetailRaw: "Test detail 3"},
			},
			meta: HTMLMeta{
				ClusterName:    "Test Cluster",
				ClusterVersion: "6.0.0",
				NCCVersion:     "4.0.0",
			},
			wantErr: false,
		},
		{
			name:    "Empty blocks",
			blocks:  []ParsedBlock{},
			meta:    HTMLMeta{},
			wantErr: false,
		},
		{
			name: "Blocks with empty severity",
			blocks: []ParsedBlock{
				{Severity: "", CheckName: "Check", DetailRaw: "Detail"},
			},
			meta:    HTMLMeta{},
			wantErr: false,
		},
		{
			name: "All severity types",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "F1", DetailRaw: "D1"},
				{Severity: "WARN", CheckName: "W1", DetailRaw: "D2"},
				{Severity: "ERR", CheckName: "E1", DetailRaw: "D3"},
				{Severity: "INFO", CheckName: "I1", DetailRaw: "D4"},
			},
			meta:    HTMLMeta{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := filepath.Join(tmpDir, fmt.Sprintf("test-%s.json", tt.name))
			fs := OSFS{}

			err := generateJSON(fs, tt.blocks, filename, tt.meta)
			if (err != nil) != tt.wantErr {
				t.Errorf("generateJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Read and validate JSON
				data, err := os.ReadFile(filename)
				if err != nil {
					t.Fatalf("Failed to read JSON file: %v", err)
				}

				var output JSONOutput
				if err := json.Unmarshal(data, &output); err != nil {
					t.Fatalf("Failed to unmarshal JSON: %v", err)
				}

				// Validate structure
				if output.GeneratedAt == "" {
					t.Error("GeneratedAt should not be empty")
				}

				if len(output.Checks) != len(tt.blocks) {
					t.Errorf("Expected %d checks, got %d", len(tt.blocks), len(output.Checks))
				}

				if output.Summary.Total != len(tt.blocks) {
					t.Errorf("Expected total %d, got %d", len(tt.blocks), output.Summary.Total)
				}

				// Validate counts match
				expectedCounts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
				for _, b := range tt.blocks {
					sev := b.Severity
					if sev == "" {
						sev = "INFO"
					}
					expectedCounts[sev]++
				}

				for sev, expected := range expectedCounts {
					if output.Summary.Count[sev] != expected {
						t.Errorf("Expected %d %s, got %d", expected, sev, output.Summary.Count[sev])
					}
				}
			}
		})
	}
}

func TestJSONOutputStructure(t *testing.T) {
	output := JSONOutput{
		GeneratedAt: "2024-01-01T00:00:00Z",
		Checks: []JSONCheck{
			{Severity: "FAIL", CheckName: "Test", Detail: "Detail"},
		},
		Summary: JSONSummary{
			Total: 1,
			Count: map[string]int{"FAIL": 1},
		},
	}

	// Marshal to ensure it's valid JSON
	data, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("Failed to marshal JSONOutput: %v", err)
	}

	// Unmarshal to ensure it's valid
	var unmarshaled JSONOutput
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal JSONOutput: %v", err)
	}

	if unmarshaled.GeneratedAt != output.GeneratedAt {
		t.Error("GeneratedAt mismatch")
	}
	if len(unmarshaled.Checks) != len(output.Checks) {
		t.Error("Checks length mismatch")
	}
	if unmarshaled.Summary.Total != output.Summary.Total {
		t.Error("Summary total mismatch")
	}
}

// ==================== CSV Generation Tests ====================

func TestGenerateCSV(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		blocks  []ParsedBlock
		wantErr bool
	}{
		{
			name: "Normal blocks",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "Check1", DetailRaw: "Detail1"},
				{Severity: "WARN", CheckName: "Check2", DetailRaw: "Detail2"},
			},
			wantErr: false,
		},
		{
			name:    "Empty blocks",
			blocks:  []ParsedBlock{},
			wantErr: false,
		},
		{
			name: "Blocks with special characters",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "Check,with,commas", DetailRaw: "Detail\nwith\nnewlines"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := filepath.Join(tmpDir, fmt.Sprintf("test-%s.csv", tt.name))
			fs := OSFS{}

			err := generateCSV(fs, tt.blocks, filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("generateCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Read and validate CSV
				data, err := os.ReadFile(filename)
				if err != nil {
					t.Fatalf("Failed to read CSV file: %v", err)
				}

				reader := csv.NewReader(bytes.NewReader(data))
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				// Check header
				if len(records) == 0 {
					t.Error("CSV should have at least header row")
					return
				}

				expectedHeader := []string{"Severity", "CheckName", "Detail"}
				if len(records[0]) != len(expectedHeader) {
					t.Errorf("Header length mismatch: expected %d, got %d", len(expectedHeader), len(records[0]))
				}

				// Check data rows
				expectedRows := len(tt.blocks) + 1 // +1 for header
				if len(records) != expectedRows {
					t.Errorf("Expected %d rows (including header), got %d", expectedRows, len(records))
				}

				// Validate data
				for i, block := range tt.blocks {
					rowIdx := i + 1 // +1 for header
					if rowIdx < len(records) {
						if records[rowIdx][0] != block.Severity {
							t.Errorf("Row %d: Expected severity %s, got %s", i, block.Severity, records[rowIdx][0])
						}
						if records[rowIdx][1] != block.CheckName {
							t.Errorf("Row %d: Expected check name %s, got %s", i, block.CheckName, records[rowIdx][1])
						}
						if records[rowIdx][2] != block.DetailRaw {
							t.Errorf("Row %d: Expected detail %s, got %s", i, block.DetailRaw, records[rowIdx][2])
						}
					}
				}
			}
		})
	}
}

// ==================== Parsing Tests ====================

func TestDetectSeverity(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"FAIL with colon", "FAIL: something", "FAIL"},
		{"WARN with colon", "WARN: something", "WARN"},
		{"ERR with colon", "ERR: something", "ERR"},
		{"INFO with colon", "INFO: something", "INFO"},
		{"FAIL without colon", "This is a FAIL message", "INFO"}, // detectSeverity requires colon
		{"WARN without colon", "This is a WARN message", "INFO"}, // detectSeverity requires colon
		{"No severity", "This is a normal message", "INFO"},
		{"Empty string", "", "INFO"},
		{"Multiple severities", "FAIL: but also WARN:", "FAIL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectSeverity(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestSplitLines(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Normal lines",
			input:    "line1\nline2\nline3",
			expected: []string{"line1", "line2", "line3"},
		},
		{
			name:     "Empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "Single line",
			input:    "single line",
			expected: []string{"single line"},
		},
		{
			name:     "Ends with newline",
			input:    "line1\nline2\n",
			expected: []string{"line1", "line2", ""},
		},
		{
			name:     "Windows line endings",
			input:    "line1\r\nline2\r\n",
			expected: []string{"line1", "line2", ""},
		},
		{
			name:     "Multiple empty lines",
			input:    "line1\n\n\nline2",
			expected: []string{"line1", "", "", "line2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitLines(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d lines, got %d", len(tt.expected), len(result))
				return
			}
			for i, exp := range tt.expected {
				if i < len(result) && result[i] != exp {
					t.Errorf("Line %d: Expected %q, got %q", i, exp, result[i])
				}
			}
		})
	}
}

func TestParseSummary(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantErr    bool
		minBlocks  int
		checkNames []string // optional: exact check names for first blocks
	}{
		{
			name: "Valid summary",
			input: `Detailed information for Check1
Some detail here
Refer to something

Detailed information for Check2
More details
Refer to something else`,
			wantErr:    false,
			minBlocks:  2,
			checkNames: []string{"Detailed information for Check1", "Detailed information for Check2"},
		},
		{
			name:      "Empty summary",
			input:     "",
			wantErr:   false,
			minBlocks: 0,
		},
		{
			name: "Single block",
			input: `Detailed information for Check1
Detail
Refer to something`,
			wantErr:    false,
			minBlocks:  1,
			checkNames: []string{"Detailed information for Check1"},
		},
		{
			name: "No blocks (no block start line)",
			input: `Refer to documentation
Some other line`,
			wantErr:   false,
			minBlocks: 0,
		},
		{
			name: "Invalid / no Refer to line still captures block",
			input: `Detailed information for MyCheck
FAIL: something wrong
No Refer line`,
			wantErr:    false,
			minBlocks:  1,
			checkNames: []string{"Detailed information for MyCheck"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocks, err := ParseSummary(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSummary() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(blocks) < tt.minBlocks {
				t.Errorf("Expected at least %d blocks, got %d", tt.minBlocks, len(blocks))
			}
			for i, wantName := range tt.checkNames {
				if i >= len(blocks) {
					break
				}
				if blocks[i].CheckName != wantName {
					t.Errorf("Block %d CheckName = %q, want %q", i, blocks[i].CheckName, wantName)
				}
			}
		})
	}
}

func TestDigestOverviewFormat(t *testing.T) {
	// Digest overview format: "Run completed in %s. Clusters OK: %d, Failed: %d."
	runDuration := 2*time.Minute + 30*time.Second
	clustersOK, clustersFailed := 3, 1
	overview := fmt.Sprintf("Run completed in %s. Clusters OK: %d, Failed: %d.",
		runDuration.Round(time.Second), clustersOK, clustersFailed)
	if !strings.Contains(overview, "Run completed") {
		t.Error("overview should contain 'Run completed'")
	}
	if !strings.Contains(overview, "Clusters OK") {
		t.Error("overview should contain 'Clusters OK'")
	}
	if !strings.Contains(overview, "3") || !strings.Contains(overview, "1") {
		t.Error("overview should contain cluster counts")
	}
}

// ==================== Retry Helper Tests ====================

func TestJitteredBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 1 * time.Second

	tests := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{"First attempt", 1, base},
		{"Second attempt", 2, 2 * base},
		{"Third attempt", 3, 4 * base},
		{"Fourth attempt", 4, 8 * base},
		{"Exceeds max", 20, maxDelay}, // Should cap at maxDelay
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := jitteredBackoff(base, maxDelay, tt.attempt)
			// Result should be between 0 and expected (due to jitter)
			if result < 0 {
				t.Errorf("Backoff should not be negative, got %v", result)
			}
			if tt.attempt < 20 && result > tt.expected {
				t.Errorf("Backoff should not exceed %v, got %v", tt.expected, result)
			}
			if tt.attempt >= 20 && result > maxDelay {
				t.Errorf("Backoff should not exceed maxDelay %v, got %v", maxDelay, result)
			}
		})
	}
}

func TestIsRetryableStatus(t *testing.T) {
	tests := []struct {
		code     int
		expected bool
	}{
		{200, false},
		{201, false},
		{299, false},
		{400, false},
		{401, false},
		{403, false},
		{404, false},
		{429, true},
		{500, true},
		{502, true},
		{503, true},
		{504, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Status%d", tt.code), func(t *testing.T) {
			result := isRetryableStatus(tt.code)
			if result != tt.expected {
				t.Errorf("Expected %v for status %d, got %v", tt.expected, tt.code, result)
			}
		})
	}
}

// ==================== File System Tests ====================

func TestOSFS(t *testing.T) {
	tmpDir := t.TempDir()
	fs := OSFS{}

	// Test MkdirAll
	dir := filepath.Join(tmpDir, "test", "nested", "dir")
	err := fs.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Test WriteFile
	testFile := filepath.Join(dir, "test.txt")
	testData := []byte("test data")
	err = fs.WriteFile(testFile, testData, 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Test ReadFile
	readData, err := fs.ReadFile(testFile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(readData) != string(testData) {
		t.Errorf("Read data mismatch: expected %q, got %q", testData, readData)
	}

	// Test Create
	createFile := filepath.Join(dir, "create.txt")
	file, err := fs.Create(createFile)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	file.Write([]byte("created"))
	file.Close()

	// Test ReadDir
	entries, err := fs.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) < 2 {
		t.Errorf("Expected at least 2 entries, got %d", len(entries))
	}
}

// ==================== Integration Tests ====================

func TestFilterBlocksToFileIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	fs := OSFS{}

	inputPath := filepath.Join(tmpDir, "input.log")
	outputPath := filepath.Join(tmpDir, "output.log")

	// Create test input
	inputData := `Detailed information for Test Check
FAIL: This is a failure
Some detail here
Refer to something`
	err := fs.WriteFile(inputPath, []byte(inputData), 0644)
	if err != nil {
		t.Fatalf("Failed to create input file: %v", err)
	}

	// Test filterBlocksToFile
	blocks, err := filterBlocksToFile(fs, inputPath, outputPath)
	if err != nil {
		t.Fatalf("filterBlocksToFile failed: %v", err)
	}

	if len(blocks) == 0 {
		t.Error("Expected at least one block")
	}

	// Verify output file exists
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("Output file was not created")
	}
}

// ==================== Edge Case Tests ====================

func TestGenerateJSONEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	fs := OSFS{}

	tests := []struct {
		name    string
		blocks  []ParsedBlock
		setup   func() error
		wantErr bool
	}{
		{
			name: "Nested directory creation",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "Test", DetailRaw: "Detail"},
			},
			setup: func() error {
				// Test that generateJSON creates nested directories
				return nil // generateJSON will create the directory via fs.Create
			},
			wantErr: false, // Should create directory automatically
		},
		{
			name: "Very long detail",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "Test", DetailRaw: strings.Repeat("x", 100000)},
			},
			setup:   func() error { return nil },
			wantErr: false,
		},
		{
			name: "Special characters in check name",
			blocks: []ParsedBlock{
				{Severity: "FAIL", CheckName: "Test\nCheck\"Name", DetailRaw: "Detail"},
			},
			setup:   func() error { return nil },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				if err := tt.setup(); err != nil {
					t.Fatalf("Setup failed: %v", err)
				}
			}

			var filename string
			if tt.name == "Nested directory creation" {
				// Use nested path to test directory creation
				filename = filepath.Join(tmpDir, "nested", "deep", "path", fmt.Sprintf("test-%s.json", strings.ReplaceAll(tt.name, " ", "_")))
			} else {
				filename = filepath.Join(tmpDir, fmt.Sprintf("test-%s.json", strings.ReplaceAll(tt.name, " ", "_")))
			}
			err := generateJSON(fs, tt.blocks, filename, HTMLMeta{})
			if (err != nil) != tt.wantErr {
				t.Errorf("generateJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				// Verify file was created
				if _, err := os.Stat(filename); os.IsNotExist(err) {
					t.Errorf("Expected file %s to be created", filename)
				}
			}
		})
	}
}

// ==================== Mock HTTP Client for Testing ====================

type mockHTTPClient struct {
	doFunc func(*http.Request) (*http.Response, error)
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

func TestSendSlackDisabled(t *testing.T) {
	cfg := Config{
		SlackEnabled:    false,
		SlackWebhookURL: "",
	}

	ctx := context.Background()
	client := &mockHTTPClient{}

	err := sendSlack(ctx, client, cfg, NotificationSummary{})
	if err != nil {
		t.Errorf("sendSlack should return nil when disabled, got %v", err)
	}
}

func TestSendWebhookDisabled(t *testing.T) {
	cfg := Config{
		WebhookEnabled: false,
		WebhookURL:     "",
	}

	ctx := context.Background()
	client := &mockHTTPClient{}

	err := sendWebhook(ctx, client, cfg, NotificationSummary{})
	if err != nil {
		t.Errorf("sendWebhook should return nil when disabled, got %v", err)
	}
}

// ==================== Configuration Tests ====================

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}

	// Test that zero values are reasonable
	if cfg.MaxParallel <= 0 {
		// This is fine, will be set by bindConfig
	}

	// Test that empty slices are nil or empty
	if len(cfg.Clusters) != 0 {
		t.Error("Empty Clusters should be empty")
	}
	if len(cfg.OutputFormats) != 0 {
		t.Error("Empty OutputFormats should be empty")
	}
}

// ==================== Comprehensive JSON Tests ====================

func TestGenerateJSONWithAllSeverities(t *testing.T) {
	tmpDir := t.TempDir()
	fs := OSFS{}

	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "F1", DetailRaw: "D1"},
		{Severity: "FAIL", CheckName: "F2", DetailRaw: "D2"},
		{Severity: "WARN", CheckName: "W1", DetailRaw: "D3"},
		{Severity: "WARN", CheckName: "W2", DetailRaw: "D4"},
		{Severity: "ERR", CheckName: "E1", DetailRaw: "D5"},
		{Severity: "INFO", CheckName: "I1", DetailRaw: "D6"},
		{Severity: "", CheckName: "I2", DetailRaw: "D7"}, // Empty = INFO
	}

	filename := filepath.Join(tmpDir, "all-severities.json")
	err := generateJSON(fs, blocks, filename, HTMLMeta{})
	if err != nil {
		t.Fatalf("generateJSON failed: %v", err)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var output JSONOutput
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// Verify counts
	if output.Summary.Count["FAIL"] != 2 {
		t.Errorf("Expected 2 FAIL, got %d", output.Summary.Count["FAIL"])
	}
	if output.Summary.Count["WARN"] != 2 {
		t.Errorf("Expected 2 WARN, got %d", output.Summary.Count["WARN"])
	}
	if output.Summary.Count["ERR"] != 1 {
		t.Errorf("Expected 1 ERR, got %d", output.Summary.Count["ERR"])
	}
	if output.Summary.Count["INFO"] != 2 { // I1 + I2 (empty severity)
		t.Errorf("Expected 2 INFO, got %d", output.Summary.Count["INFO"])
	}
	if output.Summary.Total != 7 {
		t.Errorf("Expected total 7, got %d", output.Summary.Total)
	}
}

// ==================== Benchmark Tests ====================

func BenchmarkFilterBlocksBySeverity(b *testing.B) {
	blocks := make([]ParsedBlock, 1000)
	for i := 0; i < 1000; i++ {
		severities := []string{"FAIL", "WARN", "ERR", "INFO"}
		blocks[i] = ParsedBlock{
			Severity:  severities[i%4],
			CheckName: fmt.Sprintf("Check%d", i),
			DetailRaw: fmt.Sprintf("Detail%d", i),
		}
	}

	allowed := []string{"FAIL", "WARN"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filterBlocksBySeverity(blocks, allowed)
	}
}

func BenchmarkGenerateJSON(b *testing.B) {
	blocks := make([]ParsedBlock, 100)
	for i := 0; i < 100; i++ {
		blocks[i] = ParsedBlock{
			Severity:  "FAIL",
			CheckName: fmt.Sprintf("Check%d", i),
			DetailRaw: fmt.Sprintf("Detail%d", i),
		}
	}

	tmpDir := b.TempDir()
	fs := OSFS{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filename := filepath.Join(tmpDir, fmt.Sprintf("bench-%d.json", i))
		_ = generateJSON(fs, blocks, filename, HTMLMeta{})
	}
}

func BenchmarkSanitizeLabel(b *testing.B) {
	testString := `test\label"with"special\nchars`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sanitizeLabel(testString)
	}
}

// ==================== CLI and Configuration Tests ====================

// setMinimalValidConfig sets clusters and username so bindConfig() validation passes.
func setMinimalValidConfig() {
	viper.Set("clusters", "10.0.0.1")
	viper.Set("username", "admin")
}

func TestBindConfigDefaults(t *testing.T) {
	// Reset viper to clean state
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	// Test defaults
	if cfg.OutputDirLogs != "nccfiles" {
		t.Errorf("Expected default OutputDirLogs 'nccfiles', got %s", cfg.OutputDirLogs)
	}
	if cfg.OutputDirFiltered != "outputfiles" {
		t.Errorf("Expected default OutputDirFiltered 'outputfiles', got %s", cfg.OutputDirFiltered)
	}
	if len(cfg.OutputFormats) == 0 {
		t.Error("Expected default OutputFormats to have at least one format")
	}
	if cfg.Timeout != 15*time.Minute {
		t.Errorf("Expected default Timeout 15m, got %v", cfg.Timeout)
	}
	if cfg.RequestTimeout != 20*time.Second {
		t.Errorf("Expected default RequestTimeout 20s, got %v", cfg.RequestTimeout)
	}
	if cfg.MaxParallel != 4 {
		t.Errorf("Expected default MaxParallel 4, got %d", cfg.MaxParallel)
	}
	if cfg.RetryMaxAttempts != 6 {
		t.Errorf("Expected default RetryMaxAttempts 6, got %d", cfg.RetryMaxAttempts)
	}
}

func TestBindConfigWithFlags(t *testing.T) {
	// Reset viper
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Set flag values via viper (simulating command line flags)
	viper.Set("clusters", "10.0.1.1,10.0.2.1")
	viper.Set("username", "testuser")
	viper.Set("password", "testpass")
	viper.Set("insecure-skip-verify", true)
	viper.Set("timeout", "30m")
	viper.Set("max-parallel", "8")
	viper.Set("outputs", "html,json")
	viper.Set("severity-filter", "FAIL,WARN")
	viper.Set("dry-run", true)
	viper.Set("log-level", "debug")
	viper.Set("log-http", true)

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	// Verify values
	if len(cfg.Clusters) != 2 {
		t.Errorf("Expected 2 clusters, got %d", len(cfg.Clusters))
	}
	if cfg.Clusters[0] != "10.0.1.1" || cfg.Clusters[1] != "10.0.2.1" {
		t.Errorf("Expected clusters [10.0.1.1, 10.0.2.1], got %v", cfg.Clusters)
	}
	if cfg.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got %s", cfg.Username)
	}
	if cfg.Password != "testpass" {
		t.Errorf("Expected password 'testpass', got %s", cfg.Password)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify to be true")
	}
	if cfg.Timeout != 30*time.Minute {
		t.Errorf("Expected timeout 30m, got %v", cfg.Timeout)
	}
	if cfg.MaxParallel != 8 {
		t.Errorf("Expected MaxParallel 8, got %d", cfg.MaxParallel)
	}
	if len(cfg.OutputFormats) != 2 {
		t.Errorf("Expected 2 output formats, got %d", len(cfg.OutputFormats))
	}
	if !contains(cfg.OutputFormats, "html") || !contains(cfg.OutputFormats, "json") {
		t.Errorf("Expected outputs [html, json], got %v", cfg.OutputFormats)
	}
	if len(cfg.SeverityFilter) != 2 {
		t.Errorf("Expected 2 severity filters, got %d", len(cfg.SeverityFilter))
	}
	if !cfg.DryRun {
		t.Error("Expected DryRun to be true")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug', got %s", cfg.LogLevel)
	}
	if !cfg.LogHTTP {
		t.Error("Expected LogHTTP to be true")
	}
}

func TestBindConfigPCModeWithPcsFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmpDir := t.TempDir()
	pcsFile := filepath.Join(tmpDir, "pcs.txt")
	content := "# pcs list\n10.10.10.10\npc-lab.local\n"
	if err := os.WriteFile(pcsFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write pcs file: %v", err)
	}

	viper.Set("cluster-source-mode", "pc")
	viper.Set("pcs-file", pcsFile)
	viper.Set("username", "admin")

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}
	if cfg.ClusterSourceMode != "pc" {
		t.Fatalf("expected cluster-source-mode pc, got %q", cfg.ClusterSourceMode)
	}
	if len(cfg.PCs) != 2 {
		t.Fatalf("expected 2 pc targets, got %d (%v)", len(cfg.PCs), cfg.PCs)
	}
	if cfg.PCs[0] != "10.10.10.10" || cfg.PCs[1] != "pc-lab.local" {
		t.Fatalf("unexpected pcs list: %v", cfg.PCs)
	}
}

func TestDiscoverClustersFromPCTargetsV3(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/nutanix/v3/clusters/list" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entities":[{"spec":{"resources":{"network":{"external_ip":"10.20.30.40"}}}},{"spec":{"resources":{"network":{"external_ip":"10.20.30.41"}}}}]}`))
	}))
	defer srv.Close()

	cfg := Config{
		ClusterSourceMode:   "pc",
		PCs:                 []string{srv.URL},
		Username:            "admin",
		Password:            "secret",
		DiscoverAPIVersion:  "v3",
		NutanixV4APIVersion: defaultNutanixV4APIVersion,
	}
	clusters, err := discoverClustersFromPCTargets(cfg)
	if err != nil {
		t.Fatalf("discoverClustersFromPCTargets() failed: %v", err)
	}
	if len(clusters) != 2 {
		t.Fatalf("expected 2 discovered clusters, got %d (%v)", len(clusters), clusters)
	}
	if clusters[0] != "10.20.30.40" || clusters[1] != "10.20.30.41" {
		t.Fatalf("unexpected discovered clusters: %v", clusters)
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func TestBindConfigOutputFormats(t *testing.T) {
	tests := []struct {
		name            string
		outputs         string
		expectedCount   int
		expectedFormats []string
	}{
		{"Single format", "html", 1, []string{"html"}},
		{"Multiple formats", "html,csv,json", 3, []string{"html", "csv", "json"}},
		{"With spaces", "html, csv , json", 3, []string{"html", "csv", "json"}},
		{"Empty string", "", 1, []string{"html"}}, // Defaults to html
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			viper.SetEnvPrefix("ncc")
			viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			viper.AutomaticEnv()

			if tt.outputs != "" {
				viper.Set("outputs", tt.outputs)
			}
			setMinimalValidConfig()

			cfg, err := bindConfig()
			if err != nil {
				t.Fatalf("bindConfig() failed: %v", err)
			}

			if len(cfg.OutputFormats) != tt.expectedCount {
				t.Errorf("Expected %d formats, got %d", tt.expectedCount, len(cfg.OutputFormats))
			}

			for _, expected := range tt.expectedFormats {
				if !contains(cfg.OutputFormats, expected) {
					t.Errorf("Expected format %s not found in %v", expected, cfg.OutputFormats)
				}
			}
		})
	}
}

func TestBindConfigDurationParsing(t *testing.T) {
	tests := []struct {
		name         string
		timeout      string
		expected     time.Duration
		defaultValue time.Duration
	}{
		{"Valid minutes", "30m", 30 * time.Minute, 15 * time.Minute},
		{"Valid seconds", "45s", 45 * time.Second, 20 * time.Second},
		{"Valid hours", "2h", 2 * time.Hour, 15 * time.Minute},
		{"Invalid format", "invalid", 15 * time.Minute, 15 * time.Minute},
		{"Empty string", "", 15 * time.Minute, 15 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			viper.SetEnvPrefix("ncc")
			viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			viper.AutomaticEnv()

			viper.Set("timeout", tt.timeout)
			setMinimalValidConfig()

			cfg, err := bindConfig()
			if err != nil {
				t.Fatalf("bindConfig() failed: %v", err)
			}

			if cfg.Timeout != tt.expected {
				t.Errorf("Expected timeout %v, got %v", tt.expected, cfg.Timeout)
			}
		})
	}
}

func TestBindConfigSeverityFilter(t *testing.T) {
	tests := []struct {
		name          string
		filter        string
		expectedCount int
		expected      []string
	}{
		{"Single severity", "FAIL", 1, []string{"FAIL"}},
		{"Multiple severities", "FAIL,WARN", 2, []string{"FAIL", "WARN"}},
		{"All severities", "FAIL,WARN,ERR,INFO", 4, []string{"FAIL", "WARN", "ERR", "INFO"}},
		{"With spaces", "FAIL, WARN , ERR", 3, []string{"FAIL", "WARN", "ERR"}},
		{"Empty filter", "", 0, []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			viper.SetEnvPrefix("ncc")
			viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			viper.AutomaticEnv()

			if tt.filter != "" {
				viper.Set("severity-filter", tt.filter)
			}
			setMinimalValidConfig()

			cfg, err := bindConfig()
			if err != nil {
				t.Fatalf("bindConfig() failed: %v", err)
			}

			if len(cfg.SeverityFilter) != tt.expectedCount {
				t.Errorf("Expected %d filters, got %d", tt.expectedCount, len(cfg.SeverityFilter))
			}

			for _, expected := range tt.expected {
				if !contains(cfg.SeverityFilter, expected) {
					t.Errorf("Expected filter %s not found in %v", expected, cfg.SeverityFilter)
				}
			}
		})
	}
}

func TestBindConfigExcludeAlertTitles(t *testing.T) {
	tests := []struct {
		name          string
		excludes      string
		expectedCount int
		expected      []string
	}{
		{"Single title", "AOS service health", 1, []string{"AOS service health"}},
		{"Multiple titles", "AOS service health,Disk health", 2, []string{"AOS service health", "Disk health"}},
		{"With spaces", "AOS service health, Disk health , Prism connectivity", 3, []string{"AOS service health", "Disk health", "Prism connectivity"}},
		{"Empty excludes", "", 0, []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			viper.SetEnvPrefix("ncc")
			viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			viper.AutomaticEnv()

			if tt.excludes != "" {
				viper.Set("exclude-alert-titles", tt.excludes)
			}
			setMinimalValidConfig()

			cfg, err := bindConfig()
			if err != nil {
				t.Fatalf("bindConfig() failed: %v", err)
			}

			if len(cfg.ExcludeAlertTitles) != tt.expectedCount {
				t.Errorf("Expected %d excluded titles, got %d", tt.expectedCount, len(cfg.ExcludeAlertTitles))
			}

			for _, expected := range tt.expected {
				if !contains(cfg.ExcludeAlertTitles, expected) {
					t.Errorf("Expected excluded title %q not found in %v", expected, cfg.ExcludeAlertTitles)
				}
			}
		})
	}
}

func TestBindConfigExcludeAlertTitlesFileMergeAndMode(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "exclude-titles.txt")
	content := "# comment line\nAOS service health\nDisk health\n"
	if err := os.WriteFile(filePath, []byte(content), 0o600); err != nil {
		t.Fatalf("write exclude titles file: %v", err)
	}

	viper.Set("exclude-alert-titles", "Prism connectivity,Disk health")
	viper.Set("exclude-alert-titles-file", filePath)
	viper.Set("exclude-alert-match-mode", "contains")
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}
	if cfg.ExcludeAlertMatchMode != "contains" {
		t.Fatalf("expected contains mode, got %q", cfg.ExcludeAlertMatchMode)
	}
	for _, want := range []string{"AOS service health", "Disk health", "Prism connectivity"} {
		if !contains(cfg.ExcludeAlertTitles, want) {
			t.Errorf("expected merged title %q in %v", want, cfg.ExcludeAlertTitles)
		}
	}
}

func TestBindConfigExcludeAlertMatchModeInvalid(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("exclude-alert-match-mode", "wildcard")
	setMinimalValidConfig()

	_, err := bindConfig()
	if err == nil {
		t.Fatal("expected bindConfig to fail for invalid exclude-alert-match-mode")
	}
	if !strings.Contains(err.Error(), "exclude-alert-match-mode") {
		t.Fatalf("expected exclude-alert-match-mode error, got: %v", err)
	}
}

func TestWriteExcludedAlertsAuditJSONSchemaVersion(t *testing.T) {
	tmpDir := t.TempDir()
	perCluster := map[string][]ExcludedAlert{
		"10.0.0.1": {
			{
				Severity:   "FAIL",
				CheckName:  "AOS service health",
				Detail:     "detail",
				MatchMode:  "exact",
				MatchValue: "AOS service health",
			},
		},
	}
	if err := writeExcludedAlertsAuditJSON(OSFS{}, tmpDir, "exact", []string{"AOS service health"}, perCluster); err != nil {
		t.Fatalf("writeExcludedAlertsAuditJSON failed: %v", err)
	}
	path := filepath.Join(tmpDir, "excluded-alerts.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read excluded-alerts.json failed: %v", err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(b, &payload); err != nil {
		t.Fatalf("unmarshal excluded-alerts.json failed: %v", err)
	}
	if got, _ := payload["schema_version"].(string); got != "1.0" {
		t.Fatalf("expected schema_version 1.0, got %q", got)
	}
}

func TestBindConfigEmailSettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("email-enabled", true)
	viper.Set("smtp-server", "smtp.example.com")
	viper.Set("smtp-port", "587")
	viper.Set("smtp-user", "user@example.com")
	viper.Set("smtp-password", "password123")
	viper.Set("email-from", "ncc@example.com")
	viper.Set("email-to", "admin@example.com,ops@example.com")
	viper.Set("email-use-tls", true)
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if !cfg.EmailEnabled {
		t.Error("Expected EmailEnabled to be true")
	}
	if cfg.SMTPServer != "smtp.example.com" {
		t.Errorf("Expected SMTPServer 'smtp.example.com', got %s", cfg.SMTPServer)
	}
	if cfg.SMTPPort != 587 {
		t.Errorf("Expected SMTPPort 587, got %d", cfg.SMTPPort)
	}
	if cfg.SMTPUser != "user@example.com" {
		t.Errorf("Expected SMTPUser 'user@example.com', got %s", cfg.SMTPUser)
	}
	if cfg.SMTPPassword != "password123" {
		t.Errorf("Expected SMTPPassword 'password123', got %s", cfg.SMTPPassword)
	}
	if cfg.EmailFrom != "ncc@example.com" {
		t.Errorf("Expected EmailFrom 'ncc@example.com', got %s", cfg.EmailFrom)
	}
	if len(cfg.EmailTo) != 2 {
		t.Errorf("Expected 2 email recipients, got %d", len(cfg.EmailTo))
	}
	if !cfg.EmailUseTLS {
		t.Error("Expected EmailUseTLS to be true")
	}
}

func TestBindConfigWebhookSettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("webhook-enabled", true)
	viper.Set("webhook-url", "https://hooks.example.com/ncc")
	viper.Set("webhook-headers", map[string]string{
		"X-Auth-Token": "token123",
		"X-Custom":     "value",
	})
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if !cfg.WebhookEnabled {
		t.Error("Expected WebhookEnabled to be true")
	}
	if cfg.WebhookURL != "https://hooks.example.com/ncc" {
		t.Errorf("Expected WebhookURL 'https://hooks.example.com/ncc', got %s", cfg.WebhookURL)
	}
	if len(cfg.WebhookHeaders) != 2 {
		t.Errorf("Expected 2 webhook headers, got %d", len(cfg.WebhookHeaders))
	}
	if cfg.WebhookHeaders["X-Auth-Token"] != "token123" {
		t.Errorf("Expected header X-Auth-Token 'token123', got %s", cfg.WebhookHeaders["X-Auth-Token"])
	}
}

func TestBindConfigSlackSettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("slack-enabled", true)
	viper.Set("slack-webhook-url", "https://hooks.slack.com/services/XXX/YYY/ZZZ")
	viper.Set("slack-channel", "#ncc-alerts")
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if !cfg.SlackEnabled {
		t.Error("Expected SlackEnabled to be true")
	}
	if cfg.SlackWebhookURL != "https://hooks.slack.com/services/XXX/YYY/ZZZ" {
		t.Errorf("Expected SlackWebhookURL, got %s", cfg.SlackWebhookURL)
	}
	if cfg.SlackChannel != "#ncc-alerts" {
		t.Errorf("Expected SlackChannel '#ncc-alerts', got %s", cfg.SlackChannel)
	}
}

func TestBindConfigRetrySettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("retry-max-attempts", "10")
	viper.Set("retry-base-delay", "1s")
	viper.Set("retry-max-delay", "30s")
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if cfg.RetryMaxAttempts != 10 {
		t.Errorf("Expected RetryMaxAttempts 10, got %d", cfg.RetryMaxAttempts)
	}
	if cfg.RetryBaseDelay != 1*time.Second {
		t.Errorf("Expected RetryBaseDelay 1s, got %v", cfg.RetryBaseDelay)
	}
	if cfg.RetryMaxDelay != 30*time.Second {
		t.Errorf("Expected RetryMaxDelay 30s, got %v", cfg.RetryMaxDelay)
	}
}

func TestBindConfigPollingSettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("poll-interval", "30s")
	viper.Set("poll-jitter", "5s")
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if cfg.PollInterval != 30*time.Second {
		t.Errorf("Expected PollInterval 30s, got %v", cfg.PollInterval)
	}
	if cfg.PollJitter != 5*time.Second {
		t.Errorf("Expected PollJitter 5s, got %v", cfg.PollJitter)
	}
}

func TestBindConfigLoggingSettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("log-file", "custom.log")
	viper.Set("log-level", "debug")
	viper.Set("log-http", true)
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if cfg.LogFile != "custom.log" {
		t.Errorf("Expected LogFile 'custom.log', got %s", cfg.LogFile)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected LogLevel 'debug', got %s", cfg.LogLevel)
	}
	if !cfg.LogHTTP {
		t.Error("Expected LogHTTP to be true")
	}
}

func TestBindConfigPrometheusSettings(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("prom-dir", "custom-prom")
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if cfg.PromDir != "custom-prom" {
		t.Errorf("Expected PromDir 'custom-prom', got %s", cfg.PromDir)
	}
}

func TestBindConfigDryRun(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("dry-run", true)
	viper.Set("clusters", "10.0.1.1")
	viper.Set("username", "admin")

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if !cfg.DryRun {
		t.Error("Expected DryRun to be true")
	}
}

func TestBindConfigEmptyClusters(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Don't set clusters - validation should fail
	_, err := bindConfig()
	if err == nil {
		t.Fatal("bindConfig() expected to fail when no clusters provided")
	}
	if !strings.Contains(err.Error(), "at least one cluster") {
		t.Errorf("Expected error about clusters, got: %v", err)
	}
}

func TestBindConfigMultipleClusters(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.Set("clusters", "10.0.1.1,10.0.2.1,10.0.3.1,10.0.4.1")
	viper.Set("username", "testuser")

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	if len(cfg.Clusters) != 4 {
		t.Errorf("Expected 4 clusters, got %d", len(cfg.Clusters))
	}

	expected := []string{"10.0.1.1", "10.0.2.1", "10.0.3.1", "10.0.4.1"}
	for i, exp := range expected {
		if i < len(cfg.Clusters) && cfg.Clusters[i] != exp {
			t.Errorf("Expected cluster %d to be %s, got %s", i, exp, cfg.Clusters[i])
		}
	}
}

func TestBindConfigInvalidDuration(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Set invalid duration - should use default
	viper.Set("timeout", "not-a-duration")
	setMinimalValidConfig()

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	// Should fall back to default
	if cfg.Timeout != 15*time.Minute {
		t.Errorf("Expected default timeout 15m for invalid input, got %v", cfg.Timeout)
	}
}

func TestBindConfigInvalidBoolInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "bad-bool.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
insecure-skip-verify: flse
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	_, err := bindConfig()
	if err == nil {
		t.Fatal("bindConfig() expected to fail for invalid bool in config file")
	}
	if !strings.Contains(err.Error(), "insecure-skip-verify") {
		t.Fatalf("expected insecure-skip-verify error, got: %v", err)
	}
}

func TestBindConfigInvalidDurationInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "bad-duration.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
request-timeout: "30seconds"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	_, err := bindConfig()
	if err == nil {
		t.Fatal("bindConfig() expected to fail for invalid duration in config file")
	}
	if !strings.Contains(err.Error(), "request-timeout") {
		t.Fatalf("expected request-timeout error, got: %v", err)
	}
}

func TestBindConfigUnknownKeyInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "unknown-key.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
strict-mode: true
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	_, err := bindConfig()
	if err == nil {
		t.Fatal("bindConfig() expected to fail for unknown config key")
	}
	if !strings.Contains(err.Error(), "unknown config key") {
		t.Fatalf("expected unknown config key error, got: %v", err)
	}
}

func TestBindConfigInvalidLogLevelInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "bad-log-level.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
log-level: "verbose"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	_, err := bindConfig()
	if err == nil {
		t.Fatal("bindConfig() expected to fail for invalid log-level in config file")
	}
	if !strings.Contains(err.Error(), "invalid log-level") {
		t.Fatalf("expected invalid log-level error, got: %v", err)
	}
}

func TestBindConfigInvalidWebhookHeadersTypeInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "bad-webhook-headers.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
webhook-headers:
  Authorization: 42
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	_, err := bindConfig()
	if err == nil {
		t.Fatal("bindConfig() expected to fail for invalid webhook-headers value type")
	}
	if !strings.Contains(err.Error(), "webhook-headers") {
		t.Fatalf("expected webhook-headers error, got: %v", err)
	}
}

func TestBindConfigAllowsGenTestAggInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "gen-test-agg.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
gen-test-agg: 10
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	if _, err := bindConfig(); err != nil {
		t.Fatalf("bindConfig() expected to accept gen-test-agg key, got: %v", err)
	}
}

func TestBindConfigAllowsUpdateInConfigFile(t *testing.T) {
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "update.yaml")
	content := `clusters: "10.0.1.1"
username: "admin"
update: false
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	viper.Set("config", cfgPath)

	if _, err := bindConfig(); err != nil {
		t.Fatalf("bindConfig() expected to accept update key, got: %v", err)
	}
}

func TestBindConfigMaxParallel(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected int
	}{
		{"Valid value", "8", 8},
		{"Single cluster", "1", 1},
		{"Large value", "100", 100},
		{"Invalid string", "invalid", 4}, // viper returns 0; app defaults to 4 when <= 0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			viper.SetEnvPrefix("ncc")
			viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			viper.AutomaticEnv()

			viper.Set("max-parallel", tt.value)
			setMinimalValidConfig()

			cfg, err := bindConfig()
			if err != nil {
				t.Fatalf("bindConfig() failed: %v", err)
			}

			if cfg.MaxParallel != tt.expected {
				t.Errorf("Expected MaxParallel %d, got %d", tt.expected, cfg.MaxParallel)
			}
		})
	}
}

func TestBindConfigSMTPPort(t *testing.T) {
	tests := []struct {
		name     string
		port     string
		expected int
	}{
		{"Standard STARTTLS", "587", 587},
		{"SSL port", "465", 465},
		{"Invalid port", "invalid", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			viper.SetEnvPrefix("ncc")
			viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
			viper.AutomaticEnv()

			viper.Set("smtp-port", tt.port)
			setMinimalValidConfig()

			cfg, err := bindConfig()
			if err != nil {
				t.Fatalf("bindConfig() failed: %v", err)
			}

			if cfg.SMTPPort != tt.expected {
				t.Errorf("Expected SMTPPort %d, got %d", tt.expected, cfg.SMTPPort)
			}
		})
	}
}

func TestBindConfigAllFlags(t *testing.T) {
	// Test that all flags can be set simultaneously
	viper.Reset()
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Set all flags
	viper.Set("clusters", "10.0.1.1,10.0.2.1")
	viper.Set("username", "admin")
	viper.Set("password", "pass")
	viper.Set("insecure-skip-verify", true)
	viper.Set("timeout", "30m")
	viper.Set("request-timeout", "60s")
	viper.Set("poll-interval", "20s")
	viper.Set("poll-jitter", "3s")
	viper.Set("max-parallel", "10")
	viper.Set("outputs", "html,csv,json")
	viper.Set("output-dir-logs", "custom-logs")
	viper.Set("output-dir-filtered", "custom-output")
	viper.Set("severity-filter", "FAIL,WARN")
	viper.Set("dry-run", true)
	viper.Set("log-file", "test.log")
	viper.Set("log-level", "trace")
	viper.Set("log-http", true)
	viper.Set("retry-max-attempts", "10")
	viper.Set("retry-base-delay", "1s")
	viper.Set("retry-max-delay", "30s")
	viper.Set("prom-dir", "custom-prom")
	viper.Set("email-enabled", true)
	viper.Set("smtp-server", "smtp.test.com")
	viper.Set("smtp-port", "587")
	viper.Set("smtp-user", "user")
	viper.Set("smtp-password", "pass")
	viper.Set("email-from", "from@test.com")
	viper.Set("email-to", "to@test.com")
	viper.Set("email-use-tls", true)
	viper.Set("webhook-enabled", true)
	viper.Set("webhook-url", "https://webhook.test.com")
	viper.Set("slack-enabled", true)
	viper.Set("slack-webhook-url", "https://slack.test.com")
	viper.Set("slack-channel", "#test")

	cfg, err := bindConfig()
	if err != nil {
		t.Fatalf("bindConfig() failed: %v", err)
	}

	// Verify all settings
	if len(cfg.Clusters) != 2 {
		t.Error("Clusters not set correctly")
	}
	if cfg.Username != "admin" {
		t.Error("Username not set correctly")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify not set correctly")
	}
	if cfg.Timeout != 30*time.Minute {
		t.Error("Timeout not set correctly")
	}
	if len(cfg.OutputFormats) != 3 {
		t.Error("OutputFormats not set correctly")
	}
	if len(cfg.SeverityFilter) != 2 {
		t.Error("SeverityFilter not set correctly")
	}
	if !cfg.DryRun {
		t.Error("DryRun not set correctly")
	}
	if !cfg.EmailEnabled {
		t.Error("EmailEnabled not set correctly")
	}
	if !cfg.WebhookEnabled {
		t.Error("WebhookEnabled not set correctly")
	}
	if !cfg.SlackEnabled {
		t.Error("SlackEnabled not set correctly")
	}
}

// ==================== Validation and Helpers ====================

func TestValidateClusterAddress(t *testing.T) {
	tests := []struct {
		name    string
		cluster string
		wantErr bool
	}{
		{"Valid IPv4", "10.0.1.1", false},
		{"Valid IPv4 another", "192.168.0.1", false},
		{"Valid hostname", "prism.example.com", false},
		{"Valid hostname with hyphen", "prism-element-01", false},
		{"Empty", "", true},
		{"Double dot", "10.0..1", true},
		{"Leading dot", ".host", true},
		{"Trailing dot", "host.", true},
		{"Invalid character space", "10.0.1 1", true},
		{"Invalid character at", "host@name", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClusterAddress(tt.cluster)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateClusterAddress(%q) err = %v, wantErr %v", tt.cluster, err, tt.wantErr)
			}
		})
	}
}

func TestValidateClusters(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		err := validateClusters(nil)
		if err == nil {
			t.Error("expected error for nil clusters")
		}
		if err := validateClusters([]string{}); err == nil {
			t.Error("expected error for empty clusters")
		}
	})
	t.Run("Duplicate", func(t *testing.T) {
		err := validateClusters([]string{"10.0.1.1", "10.0.1.1"})
		if err == nil {
			t.Error("expected error for duplicate cluster")
		}
		if err != nil && !strings.Contains(err.Error(), "duplicate") {
			t.Errorf("expected duplicate message, got %v", err)
		}
	})
	t.Run("Valid single", func(t *testing.T) {
		if err := validateClusters([]string{"10.0.1.1"}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("Invalid address", func(t *testing.T) {
		err := validateClusters([]string{"bad..host"})
		if err == nil {
			t.Error("expected error for invalid address")
		}
	})
}

func TestReadClusterFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "clusters.txt")
	content := `# comment
10.0.1.1
10.0.1.2,admin
10.0.1.3,svc-user,pass123
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write clusters file: %v", err)
	}
	clusters, creds, err := readClusterFile(path)
	if err != nil {
		t.Fatalf("readClusterFile: %v", err)
	}
	if len(clusters) != 3 {
		t.Fatalf("len(clusters)=%d, want 3", len(clusters))
	}
	if clusters[0] != "10.0.1.1" || clusters[1] != "10.0.1.2" || clusters[2] != "10.0.1.3" {
		t.Fatalf("unexpected clusters: %#v", clusters)
	}
	if creds["10.0.1.2"].Username != "admin" || creds["10.0.1.2"].Password != "" {
		t.Fatalf("unexpected creds for 10.0.1.2: %#v", creds["10.0.1.2"])
	}
	if creds["10.0.1.3"].Username != "svc-user" || creds["10.0.1.3"].Password != "pass123" {
		t.Fatalf("unexpected creds for 10.0.1.3: %#v", creds["10.0.1.3"])
	}
}

func TestReadClusterFileInvalidLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "clusters.txt")
	content := `10.0.1.1,admin,pass,extra`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write clusters file: %v", err)
	}
	if _, _, err := readClusterFile(path); err == nil {
		t.Fatal("expected error for invalid cluster line")
	}
}

func TestValidateURL(t *testing.T) {
	tests := []struct {
		name   string
		urlStr string
		wantOk bool
	}{
		{"Valid https", "https://hooks.example.com/ncc", true},
		{"Valid http", "http://localhost:8080", true},
		{"Empty", "", false},
		{"No scheme", "hooks.example.com", false},
		{"Invalid scheme", "ftp://example.com", false},
		{"No host", "https://", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateURL(tt.urlStr)
			if (err == nil) != tt.wantOk {
				t.Errorf("validateURL(%q) err = %v, wantOk %v", tt.urlStr, err, tt.wantOk)
			}
		})
	}
}

func TestValidateEmailAddress(t *testing.T) {
	tests := []struct {
		name   string
		email  string
		wantOk bool
	}{
		{"Valid", "user@example.com", true},
		{"Valid with subdomain", "ncc@mail.example.com", true},
		{"Empty", "", false},
		{"No at", "userexample.com", false},
		{"Two at", "user@name@example.com", false},
		{"No domain dot", "user@nodot", false},
		{"Empty local", "@example.com", false},
		{"Empty domain", "user@", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEmailAddress(tt.email)
			if (err == nil) != tt.wantOk {
				t.Errorf("validateEmailAddress(%q) err = %v, wantOk %v", tt.email, err, tt.wantOk)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	validPaths := func() Config {
		return Config{
			Clusters:          []string{"10.0.1.1"},
			Username:          "admin",
			Timeout:           15 * time.Minute,
			RequestTimeout:    20 * time.Second,
			MaxParallel:       4,
			RetryMaxAttempts:  6,
			RetryBaseDelay:    400 * time.Millisecond,
			RetryMaxDelay:     8 * time.Second,
			OutputFormats:     []string{"html"},
			OutputDirLogs:     "nccfiles",
			OutputDirFiltered: "outputfiles",
			LogFile:           "logs/ncc-runner.log",
			PromDir:           "promfiles",
		}
	}
	t.Run("Valid minimal", func(t *testing.T) {
		cfg := validPaths()
		if err := validateConfig(cfg); err != nil {
			t.Errorf("validateConfig: %v", err)
		}
	})
	t.Run("Empty clusters", func(t *testing.T) {
		cfg := validPaths()
		cfg.Clusters = nil
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for empty clusters")
		}
	})
	t.Run("Empty username", func(t *testing.T) {
		cfg := validPaths()
		cfg.Username = ""
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for empty username")
		}
	})
	t.Run("PC mode valid", func(t *testing.T) {
		cfg := validPaths()
		cfg.ClusterSourceMode = "pc"
		cfg.Clusters = nil
		cfg.PCs = []string{"10.10.10.10"}
		cfg.DiscoverAPIVersion = "v4"
		if err := validateConfig(cfg); err != nil {
			t.Errorf("validateConfig should accept pc mode: %v", err)
		}
	})
	t.Run("PC mode requires targets", func(t *testing.T) {
		cfg := validPaths()
		cfg.ClusterSourceMode = "pc"
		cfg.Clusters = nil
		cfg.PCs = nil
		cfg.PrismCentralURL = ""
		cfg.DiscoverAPIVersion = "v4"
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for pc mode without targets")
		}
	})
	t.Run("Per-cluster username in clusters-file map", func(t *testing.T) {
		cfg := validPaths()
		cfg.Username = ""
		cfg.ClusterCredentials = map[string]ClusterCredential{
			"10.0.1.1": {Username: "admin"},
		}
		if err := validateConfig(cfg); err != nil {
			t.Errorf("validateConfig should accept per-cluster username: %v", err)
		}
	})
	t.Run("Zero timeout", func(t *testing.T) {
		cfg := validPaths()
		cfg.Timeout = 0
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for zero timeout")
		}
	})
	t.Run("Empty output-dir-logs", func(t *testing.T) {
		cfg := validPaths()
		cfg.OutputDirLogs = ""
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for empty output-dir-logs")
		}
		if err := validateConfig(cfg); err != nil && !strings.Contains(err.Error(), "output-dir-logs") {
			t.Errorf("expected output-dir-logs in error, got %v", err)
		}
	})
	t.Run("Whitespace output-dir-filtered", func(t *testing.T) {
		cfg := validPaths()
		cfg.OutputDirFiltered = "   "
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for whitespace output-dir-filtered")
		}
	})
	t.Run("Empty log-file", func(t *testing.T) {
		cfg := validPaths()
		cfg.LogFile = ""
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for empty log-file")
		}
	})
	t.Run("Empty prom-dir", func(t *testing.T) {
		cfg := validPaths()
		cfg.PromDir = ""
		if err := validateConfig(cfg); err == nil {
			t.Error("expected error for empty prom-dir")
		}
	})
}

func TestCheckOutputPermissions(t *testing.T) {
	t.Run("Success with temp dir", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &Config{
			LogFile:           filepath.Join(dir, "ncc-runner.log"),
			OutputDirLogs:     filepath.Join(dir, "nccfiles"),
			OutputDirFiltered: filepath.Join(dir, "outputfiles"),
			PromDir:           filepath.Join(dir, "promfiles"),
		}
		if err := checkOutputPermissions(cfg); err != nil {
			t.Errorf("checkOutputPermissions: %v", err)
		}
	})
}

func TestMaskPassword(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"Empty", "", "(empty)"},
		{"Short 1", "a", "****"},
		{"Short 4", "abcd", "****"},
		{"Long", "password123", "pa****23"},
		{"Exact 5", "abcde", "ab****de"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskPassword(tt.in)
			if got != tt.want {
				t.Errorf("maskPassword(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseNCCHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "header.log")
	content := `Cluster Name: my-cluster
Cluster Version: 6.5.2
NCC Version: 4.2.0
Some other line
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	meta, err := parseNCCHeader(path)
	if err != nil {
		t.Fatalf("parseNCCHeader: %v", err)
	}
	if meta.ClusterName != "my-cluster" {
		t.Errorf("ClusterName = %q, want my-cluster", meta.ClusterName)
	}
	if meta.ClusterVersion != "6.5.2" {
		t.Errorf("ClusterVersion = %q, want 6.5.2", meta.ClusterVersion)
	}
	if meta.NCCVersion != "4.2.0" {
		t.Errorf("NCCVersion = %q, want 4.2.0", meta.NCCVersion)
	}
}

func TestParseNCCHeaderMissing(t *testing.T) {
	_, err := parseNCCHeader(filepath.Join(t.TempDir(), "nonexistent.log"))
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestRowsFromBlocks(t *testing.T) {
	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "Check One", DetailRaw: "Detail line 1\nLine 2"},
		{Severity: "WARN", CheckName: "Check Two", DetailRaw: "Detail"},
	}
	rows := rowsFromBlocks(blocks)
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	if rows[0].Severity != "FAIL" || rows[0].CheckName != "Check One" {
		t.Errorf("row[0]: severity=%q checkName=%q", rows[0].Severity, rows[0].CheckName)
	}
	if rows[1].Severity != "WARN" || rows[1].CheckName != "Check Two" {
		t.Errorf("row[1]: severity=%q checkName=%q", rows[1].Severity, rows[1].CheckName)
	}
	if !strings.Contains(string(rows[0].Detail), "Detail line 1") {
		t.Errorf("row[0].Detail should contain escaped content")
	}
}

func TestRowsFromBlocksEmpty(t *testing.T) {
	rows := rowsFromBlocks(nil)
	if len(rows) != 0 {
		t.Errorf("len(rows) = %d, want 0", len(rows))
	}
}

func TestSanitizeSummary(t *testing.T) {
	got := sanitizeSummary("line1\\nline2")
	if got != "line1\nline2" {
		t.Errorf("sanitizeSummary = %q, want line1\\nline2 with real newline", got)
	}
	got = sanitizeSummary("no backslash n")
	if got != "no backslash n" {
		t.Errorf("sanitizeSummary = %q", got)
	}
}

func TestGenerateTestAgg(t *testing.T) {
	dir := t.TempDir()
	if err := generateTestAgg(5, dir); err != nil {
		t.Fatalf("generateTestAgg(5): %v", err)
	}
	indexPath := filepath.Join(dir, "index.html")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile index.html: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "const AGG = ") {
		t.Error("index.html should contain AGG data")
	}
	if !strings.Contains(content, "cluster-001") {
		t.Error("index.html should contain cluster-001")
	}
	if !strings.Contains(content, "CLUSTER_LINKS") {
		t.Error("index.html should contain CLUSTER_LINKS")
	}
}

func TestGenerateTestAggZeroClusters(t *testing.T) {
	dir := t.TempDir()
	if err := generateTestAgg(0, dir); err != nil {
		t.Fatalf("generateTestAgg(0) should not error: %v", err)
	}
	indexPath := filepath.Join(dir, "index.html")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "AGG = []") {
		t.Error("expected AGG = [] for 0 clusters")
	}
}

func TestRetryAfterDelay(t *testing.T) {
	dur, ok := retryAfterDelay(nil)
	if ok || dur != 0 {
		t.Errorf("retryAfterDelay(nil) = %v, %v; want 0, false", dur, ok)
	}
	resp := &http.Response{Header: http.Header{}}
	dur, ok = retryAfterDelay(resp)
	if ok || dur != 0 {
		t.Errorf("no Retry-After: want 0, false; got %v, %v", dur, ok)
	}
	resp.Header.Set("Retry-After", "30")
	dur, ok = retryAfterDelay(resp)
	if !ok || dur != 30*time.Second {
		t.Errorf("Retry-After 30: want 30s, true; got %v, %v", dur, ok)
	}
}

func TestNormalizeNCCAPIVersion(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "v4", false},
		{"v4", "v4", false},
		{"Legacy", "v1", false},
		{"legacy", "v1", false},
		{"v1", "v1", false},
		{"bogus", "", true},
	} {
		got, err := normalizeNCCAPIVersion(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: want error", tc.in)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q: got %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestMapPrismTaskJSONToTaskStatus(t *testing.T) {
	raw := []byte(`{
  "data": {
    "status": "QUEUED",
    "progressPercentage": 36
  }
}`)
	st, err := mapPrismTaskJSONToTaskStatus(raw)
	if err != nil {
		t.Fatal(err)
	}
	if st.PercentageComplete != 36 || st.ProgressStatus != "Running" {
		t.Fatalf("QUEUED: got pct=%d ps=%q", st.PercentageComplete, st.ProgressStatus)
	}
	raw2 := []byte(`{"data":{"status":"SUCCEEDED","progressPercentage":100}}`)
	st2, err := mapPrismTaskJSONToTaskStatus(raw2)
	if err != nil {
		t.Fatal(err)
	}
	if st2.PercentageComplete != 100 || st2.ProgressStatus != "Succeeded" {
		t.Fatalf("SUCCEEDED: got pct=%d ps=%q", st2.PercentageComplete, st2.ProgressStatus)
	}
	raw3 := []byte(`{"data":{"status":"FAILED","progressPercentage":50}}`)
	st3, err := mapPrismTaskJSONToTaskStatus(raw3)
	if err != nil {
		t.Fatal(err)
	}
	if st3.ProgressStatus != "Failed" {
		t.Fatalf("FAILED: got ps=%q", st3.ProgressStatus)
	}
}

func TestValidateNutanixV4APIVersion(t *testing.T) {
	for _, tc := range []struct {
		in string
		ok bool
	}{
		{"v4.2", true},
		{"v4.0.a1", true},
		{"v4.1", true},
		{"v4", true},
		{"bad/path", false},
		{"v4..2", false},
	} {
		err := validateNutanixV4APIVersion(tc.in)
		if tc.ok && err != nil {
			t.Errorf("%q: want ok, got %v", tc.in, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%q: want error, got nil", tc.in)
		}
	}
}

func TestExtractClusterAddressV4(t *testing.T) {
	const sample = `{
  "name": "test_cluster_name",
  "network": {
    "externalAddress": {
      "ipv4": { "value": "10.0.0.50", "prefixLength": 32 }
    }
  }
}`
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(sample), &m); err != nil {
		t.Fatal(err)
	}
	if got := extractClusterAddressV4(m); got != "10.0.0.50" {
		t.Errorf("extractClusterAddressV4 = %q, want 10.0.0.50", got)
	}
}

func TestClusterEntityMatchesUserRef(t *testing.T) {
	pcJSON := `{
  "extId": "pc-uuid",
  "name": "Tiamut-PC",
  "nodes": {
    "nodeList": [
      {
        "controllerVmIp": {
          "ipv4": { "value": "10.48.52.75", "prefixLength": 32 }
        }
      }
    ]
  }
}`
	var pc map[string]interface{}
	if err := json.Unmarshal([]byte(pcJSON), &pc); err != nil {
		t.Fatal(err)
	}
	if !clusterEntityMatchesUserRef("10.48.52.75", pc) {
		t.Error("expected match on PC CVM IP")
	}
	if !clusterEntityMatchesUserRef("Tiamut-PC", pc) {
		t.Error("expected match on name")
	}
	if !clusterEntityMatchesUserRef("pc-uuid", pc) {
		t.Error("expected match on extId")
	}

	aosJSON := `{
  "extId": "aos-uuid",
  "name": "Tiamut",
  "network": {
    "externalAddress": {
      "ipv4": { "value": "10.48.52.74", "prefixLength": 32 }
    }
  },
  "nodes": {
    "nodeList": [
      {
        "controllerVmIp": {
          "ipv4": { "value": "10.48.52.65", "prefixLength": 32 }
        }
      }
    ]
  }
}`
	var aos map[string]interface{}
	if err := json.Unmarshal([]byte(aosJSON), &aos); err != nil {
		t.Fatal(err)
	}
	if clusterEntityMatchesUserRef("10.48.52.75", aos) {
		t.Error("did not expect AOS cluster to match PC IP")
	}
	if !clusterEntityMatchesUserRef("10.48.52.74", aos) {
		t.Error("expected match on external address")
	}
	if !clusterEntityMatchesUserRef("10.48.52.65", aos) {
		t.Error("expected match on CVM IP")
	}
}

func TestVersionLessSemver(t *testing.T) {
	tests := []struct {
		a, b string
		want bool // a < b
	}{
		{"0.1.12", "0.1.13", true},
		{"0.9.0", "1.0.0", true},
		{"1.0.0", "1.0.0", false},
		{"1.0.0-rc1", "1.0.0", true},
		{"v1.0.0", "v1.0.1", true},
	}
	for _, tc := range tests {
		got := versionLess(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("versionLess(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestBuildRunClusterSummary(t *testing.T) {
	ok := ClusterResult{
		Cluster: "10.0.0.1",
		Blocks: []ParsedBlock{
			{Severity: "FAIL", CheckName: "c1"},
			{Severity: "WARN", CheckName: "c2"},
			{Severity: "", CheckName: "c3"},
		},
	}
	s := buildRunClusterSummary(ok)
	if !s.OK || s.FailCount != 1 || s.WarnCount != 1 || s.InfoCount != 1 || s.ChecksTotal != 3 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	fail := ClusterResult{Cluster: "10.0.0.2", Err: fmt.Errorf("boom")}
	s2 := buildRunClusterSummary(fail)
	if s2.OK || s2.Error != "boom" {
		t.Fatalf("unexpected failed summary: %+v", s2)
	}
}

func TestExtractCVMIPv4sFromClusterEntity(t *testing.T) {
	const sample = `{
  "nodes": {
    "nodeList": [
      {
        "controllerVmIp": {
          "ipv4": { "value": "10.0.0.1", "prefixLength": 32 }
        }
      },
      {
        "controllerVmIp": {
          "ipv4": { "value": "10.0.0.2", "prefixLength": 32 }
        }
      }
    ]
  }
}`
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(sample), &m); err != nil {
		t.Fatal(err)
	}
	got := extractCVMIPv4sFromClusterEntity(m)
	if len(got) != 2 || got[0] != "10.0.0.1" || got[1] != "10.0.0.2" {
		t.Fatalf("extractCVMIPv4sFromClusterEntity = %#v", got)
	}
}
