package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
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
	"runtime"
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

func TestQuickstartPrompt_EmptyThenDefault(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("\n\n"))
	got, err := quickstartPrompt(reader, "Cluster targets", "10.0.0.1")
	if err != nil {
		t.Fatalf("quickstartPrompt returned error: %v", err)
	}
	if got != "10.0.0.1" {
		t.Fatalf("expected default value, got %q", got)
	}
}

func TestQuickstartConfirm_EmptyThenDefault(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("\n\n"))
	got, err := quickstartConfirm(reader, "Apply?", true)
	if err != nil {
		t.Fatalf("quickstartConfirm returned error: %v", err)
	}
	if !got {
		t.Fatal("expected default yes on double empty input")
	}
}

func TestQuickstartPromptChoice_InvalidThenDefault(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("wrong\n\n\n"))
	got, err := quickstartPromptChoice(reader, "Mode", "clusters", []string{"clusters", "pc"})
	if err != nil {
		t.Fatalf("quickstartPromptChoice returned error: %v", err)
	}
	if got != "clusters" {
		t.Fatalf("expected fallback to default, got %q", got)
	}
}

func TestRepairConfigInlineCommentValues(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	bad := []byte("timeout: \"15m\\\"                            # Per-cluster overall timeout\"\nrequest-timeout: \"20s\"\n")
	if err := os.WriteFile(cfgPath, bad, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	changed, err := repairConfigInlineCommentValues(cfgPath)
	if err != nil {
		t.Fatalf("repairConfigInlineCommentValues error: %v", err)
	}
	if !changed {
		t.Fatal("expected repair to report changes")
	}
	gotRaw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	got := string(gotRaw)
	if !strings.Contains(got, "timeout: \"15m\"") {
		t.Fatalf("expected repaired timeout value, got: %s", got)
	}
	if strings.Contains(got, "\\\"                            #") {
		t.Fatalf("expected legacy malformed suffix to be removed, got: %s", got)
	}
}

func TestRepairConfigInlineCommentValues_TrailingBackslashes(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	bad := []byte("timeout: \"15m\\\\\"\nrequest-timeout: \"20s\\\\\"\n")
	if err := os.WriteFile(cfgPath, bad, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	changed, err := repairConfigInlineCommentValues(cfgPath)
	if err != nil {
		t.Fatalf("repairConfigInlineCommentValues error: %v", err)
	}
	if !changed {
		t.Fatal("expected trailing backslash repair to report changes")
	}
	gotRaw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	got := string(gotRaw)
	if !strings.Contains(got, "timeout: \"15m\"") || !strings.Contains(got, "request-timeout: \"20s\"") {
		t.Fatalf("expected cleaned duration values, got: %s", got)
	}
}

func TestRepairConfigInlineCommentValues_StripsInlineCommentTailForKnownKeys(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	bad := []byte("nutanix-v4-api-version: \"v4.2              # v4 path revision\"\n")
	if err := os.WriteFile(cfgPath, bad, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	changed, err := repairConfigInlineCommentValues(cfgPath)
	if err != nil {
		t.Fatalf("repairConfigInlineCommentValues error: %v", err)
	}
	if !changed {
		t.Fatal("expected inline comment tail repair to report changes")
	}
	gotRaw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	got := string(gotRaw)
	if !strings.Contains(got, "nutanix-v4-api-version: \"v4.2\"") {
		t.Fatalf("expected cleaned v4 api version value, got: %s", got)
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
		// Regression: a DNS-failure-driven circuit-breaker must classify as
		// `network`, not `rate_limit`. The breaker opens on transport errors
		// for unresolved hosts, but the underlying problem is DNS.
		{"start checks failed: get cluster uuid v4 retry circuit breaker opened after 3 consecutive transport failures: dial tcp: lookup PC-Rushmore: no such host", "network"},
		{"retry circuit breaker opened after 3 consecutive transport failures: dial tcp 10.0.0.1:9440: connect: no route to host", "network"},
		{"tls: handshake failure", "network"},
		{"x509: certificate signed by unknown authority", "network"},
		{"HTTP 429 Too Many Requests; Retry-After 5", "rate_limit"},
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
		InsecureSkipVerify:  true,
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
		{"Valid URL target", "https://prism.example.com:9440/api", false},
		{"Valid host with port", "prism.example.com:9440", false},
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

func TestNormalizeClusterAddress(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "ip", raw: "10.0.1.1", want: "10.0.1.1"},
		{name: "hostname", raw: "prism.example.com", want: "prism.example.com"},
		{name: "url", raw: "https://prism.example.com:9440/api/v1", want: "prism.example.com"},
		{name: "host and port", raw: "prism.example.com:9440", want: "prism.example.com"},
		{name: "ipv4 and port", raw: "10.0.1.1:9440", want: "10.0.1.1"},
		{name: "empty", raw: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeClusterAddress(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("normalizeClusterAddress(%q) err=%v wantErr=%v", tt.raw, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("normalizeClusterAddress(%q)=%q want=%q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestPreflightResolveClusterTarget(t *testing.T) {
	if err := preflightResolveClusterTarget("127.0.0.1"); err != nil {
		t.Fatalf("expected IP target to pass preflight resolution, got %v", err)
	}
	if err := preflightResolveClusterTarget("nonexistent-preflight-target.invalid"); err == nil {
		t.Fatal("expected invalid FQDN to fail preflight resolution")
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

func TestClosestToken(t *testing.T) {
	candidates := []string{"discover-clusters", "preflight-check", "validate-config"}
	if got := closestToken("discovr-clusters", candidates); got != "discover-clusters" {
		t.Fatalf("closestToken mismatch: got=%q want=%q", got, "discover-clusters")
	}
	if got := closestToken("zzzz", candidates); got != "" {
		t.Fatalf("expected no suggestion for distant token, got=%q", got)
	}
}

func TestHumanizeCLIError(t *testing.T) {
	root := newRootCmd()
	msg := humanizeCLIError(root, []string{"discovr-clusters"}, errors.New(`unknown command "discovr-clusters" for "ncc-orchestrator"`))
	if !strings.Contains(msg, "Did you mean `discover-clusters`?") {
		t.Fatalf("expected command suggestion, got: %s", msg)
	}
	flagMsg := humanizeCLIError(root, []string{"--max-paralel"}, errors.New(`unknown flag: --max-paralel`))
	if !strings.Contains(flagMsg, "Did you mean `--max-parallel`?") {
		t.Fatalf("expected flag suggestion, got: %s", flagMsg)
	}
	subcmdFlagMsg := humanizeCLIError(root, []string{"gen-test-agg", "--autr"}, errors.New(`unknown flag: --autr`))
	if strings.Contains(subcmdFlagMsg, "`--auto`") {
		t.Fatalf("did not expect root-only flag suggestion for subcommand, got: %s", subcmdFlagMsg)
	}
	if !strings.Contains(subcmdFlagMsg, "ncc-orchestrator gen-test-agg --help") {
		t.Fatalf("expected subcommand help hint, got: %s", subcmdFlagMsg)
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
			PromEnabled:       true,
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
	t.Run("Empty prom-dir allowed when prom disabled", func(t *testing.T) {
		cfg := validPaths()
		cfg.PromEnabled = false
		cfg.PromDir = ""
		if err := validateConfig(cfg); err != nil {
			t.Errorf("expected no error when prom is disabled, got %v", err)
		}
	})
}

func getPreflightCheckByID(t *testing.T, checks []preflightCheck, id string) preflightCheck {
	t.Helper()
	for _, c := range checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("preflight check %q not found in %+v", id, checks)
	return preflightCheck{}
}

func TestBuildPreflightReportWithoutConfigPath(t *testing.T) {
	viper.Reset()
	report := buildPreflightReport("")
	if report.Failed != 0 {
		t.Fatalf("expected no failures without config path, got %d", report.Failed)
	}
	c := getPreflightCheckByID(t, report.Checks, "validate-config")
	if c.Status != "warn" {
		t.Fatalf("expected validate-config warn, got %q", c.Status)
	}
}

func TestBuildPreflightReportPathPermissionFailure(t *testing.T) {
	viper.Reset()
	tmpDir := t.TempDir()
	blockPath := filepath.Join(tmpDir, "not-a-dir")
	if err := os.WriteFile(blockPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker file: %v", err)
	}
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	cfg := fmt.Sprintf(`
clusters: "10.0.0.1"
username: "admin"
output-dir-logs: "%s"
output-dir-filtered: "%s"
log-file: "%s"
prom-dir: "%s"
`, blockPath, filepath.Join(tmpDir, "out"), filepath.Join(tmpDir, "logs", "ncc-runner.log"), filepath.Join(tmpDir, "prom"))
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	report := buildPreflightReport(cfgPath)
	c := getPreflightCheckByID(t, report.Checks, "path.output-dir-logs")
	if c.Status != "fail" {
		t.Fatalf("expected output-dir-logs permission check to fail, got %q", c.Status)
	}
	if report.Failed == 0 {
		t.Fatal("expected failures in preflight report")
	}
}

func TestBuildPreflightReportIncludesSecurityWarnings(t *testing.T) {
	viper.Reset()
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	cfg := fmt.Sprintf(`
clusters: "10.0.0.1"
username: "admin"
insecure-skip-verify: true
log-http: true
max-parallel: 25
output-dir-logs: "%s"
output-dir-filtered: "%s"
log-file: "%s"
prom-dir: "%s"
`, filepath.Join(tmpDir, "raw"), filepath.Join(tmpDir, "out"), filepath.Join(tmpDir, "logs", "ncc-runner.log"), filepath.Join(tmpDir, "prom"))
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	report := buildPreflightReport(cfgPath)
	if getPreflightCheckByID(t, report.Checks, "safety.insecure-skip-verify").Status != "warn" {
		t.Fatal("expected safety.insecure-skip-verify warning")
	}
	if getPreflightCheckByID(t, report.Checks, "safety.log-http").Status != "warn" {
		t.Fatal("expected safety.log-http warning")
	}
	if getPreflightCheckByID(t, report.Checks, "safety.max-parallel").Status != "warn" {
		t.Fatal("expected safety.max-parallel warning")
	}
}

func TestBuildPreflightReportSecretsFailure(t *testing.T) {
	viper.Reset()
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	cfg := fmt.Sprintf(`
clusters: "10.0.0.1"
username: "admin"
password: "secret://MISSING_PASSWORD"
output-dir-logs: "%s"
output-dir-filtered: "%s"
log-file: "%s"
prom-dir: "%s"
`, filepath.Join(tmpDir, "raw"), filepath.Join(tmpDir, "out"), filepath.Join(tmpDir, "logs", "ncc-runner.log"), filepath.Join(tmpDir, "prom"))
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	report := buildPreflightReport(cfgPath)
	sec := getPreflightCheckByID(t, report.Checks, "validate-secrets")
	if sec.Status != "fail" {
		t.Fatalf("expected validate-secrets fail, got %q", sec.Status)
	}
	if report.Failed == 0 {
		t.Fatal("expected failures in report")
	}
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

func TestNormalizeGitHubRepo(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "owner/repo", want: "owner/repo"},
		{in: "https://github.com/owner/repo", want: "owner/repo"},
		{in: "https://github.com/owner/repo.git", want: "owner/repo"},
		{in: "https://example.com/owner/repo", wantErr: true},
		{in: "owner-only", wantErr: true},
	}
	for _, tc := range tests {
		got, err := normalizeGitHubRepo(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("normalizeGitHubRepo(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("normalizeGitHubRepo(%q) unexpected err: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("normalizeGitHubRepo(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEnforceMajorUpgradePolicy(t *testing.T) {
	if err := enforceMajorUpgradePolicy("1.1.0", "1.9.0", false); err != nil {
		t.Fatalf("same-major upgrade should be allowed, got err: %v", err)
	}
	if err := enforceMajorUpgradePolicy("1.1.0", "2.0.0", false); err == nil {
		t.Fatalf("expected major-upgrade block error")
	}
	if err := enforceMajorUpgradePolicy("1.1.0", "2.0.0", true); err != nil {
		t.Fatalf("major-upgrade should pass with explicit opt-in, got err: %v", err)
	}
}

func TestPickLatestSemverRelease(t *testing.T) {
	releases := []githubRelease{
		{TagName: "v2.0.0"},
		{TagName: "v1.3.0"},
		{TagName: "v1.2.9"},
		{TagName: "v2.1.0-rc1", Prerelease: true},
	}
	gotV1 := pickLatestSemverRelease(releases, 1)
	if gotV1 == nil || gotV1.TagName != "v1.3.0" {
		t.Fatalf("pickLatestSemverRelease(v1) got %+v", gotV1)
	}
	gotAny := pickLatestSemverRelease(releases, 0)
	if gotAny == nil || gotAny.TagName != "v2.0.0" {
		t.Fatalf("pickLatestSemverRelease(any) got %+v", gotAny)
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

// TestPickAssetForPlatform_PrefersExeBasenamePrefix locks in the fix for the
// v1.x→v2.0.0 self-updater regression where a release that shipped multiple
// binaries per platform (e.g. ncc-orchestrator-*, ncc-api-server-*,
// ncc-ui-server-*) caused the first-match selector to pick the wrong asset
// (alphabetically: api-server) and silently overwrite the orchestrator
// binary with the api-server binary. See RELEASE_NOTES_v2.0.0 known-issues
// section.
func TestPickAssetForPlatform_PrefersExeBasenamePrefix(t *testing.T) {
	// Asset list mirroring the original (pre-hotfix) v2.0.0 release order,
	// which is the order GitHub's API returns assets (alphabetical by name).
	v200Layout := githubRelease{
		TagName: "v2.0.0",
		Assets: []githubAsset{
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
			{Name: "example_config.yaml", BrowserDownloadURL: "https://example.com/example_config.yaml"},
			{Name: "ncc-api-server-darwin-amd64", BrowserDownloadURL: "https://example.com/ncc-api-server-darwin-amd64"},
			{Name: "ncc-api-server-darwin-arm64", BrowserDownloadURL: "https://example.com/ncc-api-server-darwin-arm64"},
			{Name: "ncc-api-server-linux-amd64", BrowserDownloadURL: "https://example.com/ncc-api-server-linux-amd64"},
			{Name: "ncc-api-server-linux-arm64", BrowserDownloadURL: "https://example.com/ncc-api-server-linux-arm64"},
			{Name: "ncc-api-server-windows-amd64.exe", BrowserDownloadURL: "https://example.com/ncc-api-server-windows-amd64.exe"},
			{Name: "ncc-api-server-windows-arm64.exe", BrowserDownloadURL: "https://example.com/ncc-api-server-windows-arm64.exe"},
			{Name: "ncc-orchestrator-darwin-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-amd64"},
			{Name: "ncc-orchestrator-darwin-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-arm64"},
			{Name: "ncc-orchestrator-linux-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-amd64"},
			{Name: "ncc-orchestrator-linux-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-arm64"},
			{Name: "ncc-orchestrator-windows-amd64.exe", BrowserDownloadURL: "https://example.com/ncc-orchestrator-windows-amd64.exe"},
			{Name: "ncc-orchestrator-windows-arm64.exe", BrowserDownloadURL: "https://example.com/ncc-orchestrator-windows-arm64.exe"},
			{Name: "ncc-ui-server-darwin-amd64", BrowserDownloadURL: "https://example.com/ncc-ui-server-darwin-amd64"},
			{Name: "ncc-ui-server-darwin-arm64", BrowserDownloadURL: "https://example.com/ncc-ui-server-darwin-arm64"},
			{Name: "ncc-ui-server-linux-amd64", BrowserDownloadURL: "https://example.com/ncc-ui-server-linux-amd64"},
			{Name: "ncc-ui-server-linux-arm64", BrowserDownloadURL: "https://example.com/ncc-ui-server-linux-arm64"},
			{Name: "ncc-ui-server-windows-amd64.exe", BrowserDownloadURL: "https://example.com/ncc-ui-server-windows-amd64.exe"},
			{Name: "ncc-ui-server-windows-arm64.exe", BrowserDownloadURL: "https://example.com/ncc-ui-server-windows-arm64.exe"},
			{Name: "ncc-v2-stack-darwin-amd64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-amd64.tar.gz"},
			{Name: "ncc-v2-stack-darwin-arm64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-arm64.tar.gz"},
			{Name: "ncc-v2-stack-linux-amd64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-linux-amd64.tar.gz"},
			{Name: "ncc-v2-stack-linux-arm64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-linux-arm64.tar.gz"},
			{Name: "ncc-v2-stack-windows-amd64.zip", BrowserDownloadURL: "https://example.com/ncc-v2-stack-windows-amd64.zip"},
			{Name: "ncc-v2-stack-windows-arm64.zip", BrowserDownloadURL: "https://example.com/ncc-v2-stack-windows-arm64.zip"},
		},
	}

	cases := []struct {
		name        string
		goos        string
		goarch      string
		exeBase     string
		wantName    string
		description string
	}{
		{
			name:        "orchestrator on darwin/arm64 must pick orchestrator binary",
			goos:        "darwin",
			goarch:      "arm64",
			exeBase:     "ncc-orchestrator",
			wantName:    "ncc-orchestrator-darwin-arm64",
			description: "regression guard: pre-fix selector wrongly picked ncc-api-server-darwin-arm64",
		},
		{
			name:     "orchestrator on linux/amd64",
			goos:     "linux",
			goarch:   "amd64",
			exeBase:  "ncc-orchestrator",
			wantName: "ncc-orchestrator-linux-amd64",
		},
		{
			name:     "orchestrator on windows/arm64 (exe suffix tolerated)",
			goos:     "windows",
			goarch:   "arm64",
			exeBase:  "ncc-orchestrator.exe",
			wantName: "ncc-orchestrator-windows-arm64.exe",
		},
		{
			name:     "api-server invoked directly picks api-server",
			goos:     "linux",
			goarch:   "amd64",
			exeBase:  "ncc-api-server",
			wantName: "ncc-api-server-linux-amd64",
		},
		{
			name:     "ui-server invoked directly picks ui-server",
			goos:     "darwin",
			goarch:   "arm64",
			exeBase:  "ncc-ui-server",
			wantName: "ncc-ui-server-darwin-arm64",
		},
		{
			name:        "renamed binary falls back to first non-archive match (legacy behavior)",
			goos:        "darwin",
			goarch:      "arm64",
			exeBase:     "ncc-fork",
			wantName:    "ncc-api-server-darwin-arm64", // first non-archive that matches; legacy semantics
			description: "preserves v1.x behavior for forks/renamed binaries",
		},
		{
			name:     "empty exeBase falls back to first non-archive match",
			goos:     "linux",
			goarch:   "arm64",
			exeBase:  "",
			wantName: "ncc-api-server-linux-arm64",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, gotName := pickAssetForPlatform(v200Layout, tc.goos, tc.goarch, tc.exeBase)
			if gotName != tc.wantName {
				t.Fatalf("pickAssetForPlatform(%s/%s, exeBase=%q) = %q; want %q (%s)",
					tc.goos, tc.goarch, tc.exeBase, gotName, tc.wantName, tc.description)
			}
		})
	}
}

// TestPickAssetForPlatform_ArchiveOnlyRelease verifies that when only archive
// assets exist for a platform, the function returns the archive (so the
// caller can emit a "download and extract" hint rather than overwriting the
// binary in place).
func TestPickAssetForPlatform_ArchiveOnlyRelease(t *testing.T) {
	rel := githubRelease{
		TagName: "v2.0.0-archives-only",
		Assets: []githubAsset{
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
			{Name: "ncc-v2-stack-darwin-arm64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-arm64.tar.gz"},
			{Name: "ncc-v2-stack-linux-amd64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-linux-amd64.tar.gz"},
		},
	}
	url, name := pickAssetForPlatform(rel, "darwin", "arm64", "ncc-orchestrator")
	if name != "ncc-v2-stack-darwin-arm64.tar.gz" {
		t.Fatalf("archive-only release should surface .tar.gz, got name=%q url=%q", name, url)
	}
	if !isArchiveAssetURL(url) {
		t.Fatalf("expected isArchiveAssetURL(%q) = true", url)
	}
}

// TestPickAssetForPlatform_NoMatch ensures empty strings are returned when no
// asset matches the requested platform.
func TestPickAssetForPlatform_NoMatch(t *testing.T) {
	rel := githubRelease{
		TagName: "v2.0.0-darwin-only",
		Assets: []githubAsset{
			{Name: "ncc-orchestrator-darwin-arm64", BrowserDownloadURL: "https://example.com/x"},
		},
	}
	url, name := pickAssetForPlatform(rel, "linux", "amd64", "ncc-orchestrator")
	if url != "" || name != "" {
		t.Fatalf("expected empty results for unsupported platform; got url=%q name=%q", url, name)
	}
}

// TestDefaultV2InstallDir_AutoDetectsStackLayout pins the v2.0.2 UX fix
// where v2-check / v2-start / v2-stop / uninstall, when run from inside a
// bootstrapped or extracted stack layout (`<X>/bin/<exe>`), default the
// install dir to <X> instead of `.ncc-v2` relative to CWD. Without this,
// `cd <X>/bin && ./ncc-orchestrator v2-check` reports false-positive
// "binary not executable under install dir / frontend-dist missing"
// failures even though everything is sitting one level up.
//
// We can't redirect os.Executable() from a test, but we can exercise the
// helper via a thin wrapper that takes an injected exe path. The behavior
// asserted here is the same logic the real helper applies, gated by the
// presence of the v2 layout markers.
func TestDefaultV2InstallDir_AutoDetectsStackLayout(t *testing.T) {
	stack := t.TempDir()
	mustMkdir(t, filepath.Join(stack, "bin"))
	mustMkdir(t, filepath.Join(stack, "frontend-dist"))
	binSubdir := filepath.Join(stack, "bin")

	cases := []struct {
		name   string
		exeDir string
		want   string
	}{
		{
			name:   "running from <X>/bin in a complete stack returns <X>",
			exeDir: binSubdir,
			want:   stack,
		},
		{
			name:   "running from somewhere not under bin/ falls back to .ncc-v2",
			exeDir: t.TempDir(),
			want:   ".ncc-v2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultV2InstallDirForExeDir(tc.exeDir)
			if got != tc.want {
				t.Fatalf("defaultV2InstallDirForExeDir(%q) = %q; want %q", tc.exeDir, got, tc.want)
			}
		})
	}
}

// TestDefaultV2InstallDir_AcceptsSuffixedBinaryNaming pins that the helper
// recognizes legacy v2.0.0-style stack layouts where binaries under bin/
// have platform-suffixed names (bin/ncc-api-server-<os>-<arch>). Required so
// that v2-check works against archives extracted from the original v2.0.0
// stack without a pre-hotfix orchestrator.
func TestDefaultV2InstallDir_AcceptsSuffixedBinaryNaming(t *testing.T) {
	stack := t.TempDir()
	mustMkdir(t, filepath.Join(stack, "bin"))
	binName := fmt.Sprintf("ncc-api-server-%s-%s", runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	mustTouch(t, filepath.Join(stack, "bin", binName))
	got := defaultV2InstallDirForExeDir(filepath.Join(stack, "bin"))
	if got != stack {
		t.Fatalf("expected suffix-named bin layout to resolve to %q; got %q", stack, got)
	}
}

// TestDefaultV2InstallDir_BinDirWithoutLayoutMarkers pins that the helper
// does NOT match a directory just because it has a `bin/` parent — there
// must also be a v2 layout marker (frontend-dist/ or bin/ncc-api-server*).
// This prevents auto-detect from "swallowing" arbitrary <X>/bin/foo
// invocations as v2 stacks.
func TestDefaultV2InstallDir_BinDirWithoutLayoutMarkers(t *testing.T) {
	stack := t.TempDir()
	mustMkdir(t, filepath.Join(stack, "bin"))
	got := defaultV2InstallDirForExeDir(filepath.Join(stack, "bin"))
	if got != ".ncc-v2" {
		t.Fatalf("bare bin/ without layout markers should fall back to .ncc-v2; got %q", got)
	}
}

// TestExtractTarGzArchive_PreservesExecutableBit pins the v2.0.1 fix to
// extractTarGzArchive (and by symmetry extractZipArchive). Previously the
// extractor wrote every file with a hardcoded 0644 mode, which dropped the
// executable bit from binaries shipped inside the v2 stack archives. The
// post-extract isExecutableFile checks in v2-check / v2-start then failed
// with "binary not executable under install dir: ...". The fix honors
// hdr.Mode (or zip's FileInfo().Mode()) so extracted binaries are 0755 as
// packaged. This test builds a synthetic tar.gz with one 0755 binary-like
// entry and one 0644 plain file, extracts it, and asserts the on-disk modes
// match the archive metadata.
func TestExtractTarGzArchive_PreservesExecutableBit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix mode bits not meaningful on windows")
	}
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)
	if err := tw.WriteHeader(&tar.Header{Name: "bin/", Mode: 0o755, Typeflag: tar.TypeDir}); err != nil {
		t.Fatalf("write dir hdr: %v", err)
	}
	binBody := []byte("#!/bin/sh\necho hi\n")
	if err := tw.WriteHeader(&tar.Header{Name: "bin/ncc-stub", Mode: 0o755, Size: int64(len(binBody)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatalf("write exec hdr: %v", err)
	}
	if _, err := tw.Write(binBody); err != nil {
		t.Fatalf("write exec body: %v", err)
	}
	cfgBody := []byte("key: value\n")
	if err := tw.WriteHeader(&tar.Header{Name: "example.yaml", Mode: 0o644, Size: int64(len(cfgBody)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatalf("write cfg hdr: %v", err)
	}
	if _, err := tw.Write(cfgBody); err != nil {
		t.Fatalf("write cfg body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gzw.Close(); err != nil {
		t.Fatalf("gz close: %v", err)
	}
	dest := t.TempDir()
	if err := extractTarGzArchive(buf.Bytes(), dest); err != nil {
		t.Fatalf("extract: %v", err)
	}
	binPath := filepath.Join(dest, "bin", "ncc-stub")
	cfgPath := filepath.Join(dest, "example.yaml")
	binInfo, err := os.Stat(binPath)
	if err != nil {
		t.Fatalf("stat bin: %v", err)
	}
	if binInfo.Mode().Perm() != 0o755 {
		t.Fatalf("bin/ncc-stub mode = %#o; want 0755 (executable bit dropped during extraction)", binInfo.Mode().Perm())
	}
	if !isExecutableFile(binPath) {
		t.Fatalf("isExecutableFile(bin/ncc-stub) = false; extractor must preserve the +x bit so v2-check passes")
	}
	cfgInfo, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("stat cfg: %v", err)
	}
	if cfgInfo.Mode().Perm() != 0o644 {
		t.Fatalf("example.yaml mode = %#o; want 0644 (non-exec entries should not gain +x)", cfgInfo.Mode().Perm())
	}
}

// TestPickStackAssetForPlatform locks in the v2.0.1 behavior that `update`
// upgrades the whole v2 stack package irrespective of which binary
// (orchestrator, api-server, ui-server, or any renamed variant) was
// invoked. The selector must return the ncc-v2-stack-<os>-<arch> archive
// when present, and empty strings for legacy releases that ship only
// individual binaries.
func TestPickStackAssetForPlatform(t *testing.T) {
	v201Layout := githubRelease{
		TagName: "v2.0.1",
		Assets: []githubAsset{
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
			{Name: "example_config.yaml", BrowserDownloadURL: "https://example.com/example_config.yaml"},
			{Name: "ncc-orchestrator-darwin-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-amd64"},
			{Name: "ncc-orchestrator-darwin-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-arm64"},
			{Name: "ncc-orchestrator-linux-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-amd64"},
			{Name: "ncc-orchestrator-linux-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-arm64"},
			{Name: "ncc-orchestrator-windows-amd64.exe", BrowserDownloadURL: "https://example.com/ncc-orchestrator-windows-amd64.exe"},
			{Name: "ncc-orchestrator-windows-arm64.exe", BrowserDownloadURL: "https://example.com/ncc-orchestrator-windows-arm64.exe"},
			{Name: "ncc-v2-stack-darwin-amd64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-amd64.tar.gz"},
			{Name: "ncc-v2-stack-darwin-arm64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-arm64.tar.gz"},
			{Name: "ncc-v2-stack-linux-amd64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-linux-amd64.tar.gz"},
			{Name: "ncc-v2-stack-linux-arm64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-linux-arm64.tar.gz"},
			{Name: "ncc-v2-stack-windows-amd64.zip", BrowserDownloadURL: "https://example.com/ncc-v2-stack-windows-amd64.zip"},
			{Name: "ncc-v2-stack-windows-arm64.zip", BrowserDownloadURL: "https://example.com/ncc-v2-stack-windows-arm64.zip"},
		},
	}
	cases := []struct {
		name     string
		goos     string
		goarch   string
		wantName string
	}{
		{"darwin/arm64 selects darwin-arm64 stack", "darwin", "arm64", "ncc-v2-stack-darwin-arm64.tar.gz"},
		{"darwin/amd64 selects darwin-amd64 stack", "darwin", "amd64", "ncc-v2-stack-darwin-amd64.tar.gz"},
		{"linux/arm64 selects linux-arm64 stack", "linux", "arm64", "ncc-v2-stack-linux-arm64.tar.gz"},
		{"linux/amd64 selects linux-amd64 stack", "linux", "amd64", "ncc-v2-stack-linux-amd64.tar.gz"},
		{"windows/arm64 selects windows-arm64 zip", "windows", "arm64", "ncc-v2-stack-windows-arm64.zip"},
		{"windows/amd64 selects windows-amd64 zip", "windows", "amd64", "ncc-v2-stack-windows-amd64.zip"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := pickStackAssetForPlatform(v201Layout, tc.goos, tc.goarch)
			if got != tc.wantName {
				t.Fatalf("pickStackAssetForPlatform(%s/%s) = %q; want %q", tc.goos, tc.goarch, got, tc.wantName)
			}
		})
	}
}

// TestPickStackAssetForPlatform_NoStackInRelease confirms that legacy v1.x
// releases (no ncc-v2-stack-* assets) return empty strings so the caller can
// fall back to the single-binary update path.
func TestPickStackAssetForPlatform_NoStackInRelease(t *testing.T) {
	v1Layout := githubRelease{
		TagName: "v1.1.0",
		Assets: []githubAsset{
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
			{Name: "ncc-orchestrator-darwin-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-arm64"},
			{Name: "ncc-orchestrator-linux-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-amd64"},
		},
	}
	url, name := pickStackAssetForPlatform(v1Layout, "darwin", "arm64")
	if url != "" || name != "" {
		t.Fatalf("legacy release should return empty stack asset; got url=%q name=%q", url, name)
	}
}

// TestResolvePackageInstallDir locks the install-dir resolution invariants:
//
//  1. running binary inside <X>/bin/ → install-dir is <X>
//  2. running binary anywhere else → install-dir is the binary's directory
//
// This is what makes `update` invariant to the running binary's name (the
// caller just needs the install-dir, and the running binary self-replaces
// via os.Rename regardless of its filename).
func TestResolvePackageInstallDir(t *testing.T) {
	cases := []struct {
		name     string
		selfPath string
		want     string
	}{
		{
			name:     "inside bootstrapped stack",
			selfPath: filepath.Join("/opt", ".ncc-v2", "bin", "ncc-orchestrator"),
			want:     filepath.Join("/opt", ".ncc-v2"),
		},
		{
			name:     "inside bootstrapped stack, renamed binary",
			selfPath: filepath.Join("/opt", "ncc-v2", "bin", "my-fork"),
			want:     filepath.Join("/opt", "ncc-v2"),
		},
		{
			name:     "flat layout, alongside other binaries",
			selfPath: filepath.Join("/home", "user", "dist", "ncc-orchestrator-darwin-arm64"),
			want:     filepath.Join("/home", "user", "dist"),
		},
		{
			name:     "system path",
			selfPath: filepath.Join("/usr", "local", "bin", "ncc-orchestrator"),
			want:     filepath.Join("/usr", "local"), // bin parent
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolvePackageInstallDir(tc.selfPath)
			if got != tc.want {
				t.Fatalf("resolvePackageInstallDir(%q) = %q; want %q", tc.selfPath, got, tc.want)
			}
		})
	}
}

// TestHasBootstrappedV2Layout_AcceptsBothNamingConventions locks in the fix
// for the v2.0.0 stack-archive bootstrap regression where `v2-bootstrap`
// downloaded the stack tarball, extracted it, then failed the post-extract
// layout check because the binaries were named with platform suffixes
// (`bin/ncc-api-server-darwin-arm64`) but the layout check looked up the
// canonical name only (`bin/ncc-api-server`). The fix accepts both forms.
func TestHasBootstrappedV2Layout_AcceptsBothNamingConventions(t *testing.T) {
	t.Run("canonical names (post-fix packaging)", func(t *testing.T) {
		dir := t.TempDir()
		mustMkdir(t, filepath.Join(dir, "bin"))
		mustMkdir(t, filepath.Join(dir, "frontend-dist"))
		apiBin := "ncc-api-server"
		uiBin := "ncc-ui-server"
		if runtime.GOOS == "windows" {
			apiBin += ".exe"
			uiBin += ".exe"
		}
		mustTouch(t, filepath.Join(dir, "bin", apiBin))
		mustTouch(t, filepath.Join(dir, "bin", uiBin))
		if !hasBootstrappedV2Layout(dir) {
			t.Fatalf("canonical names should be accepted: %s", dir)
		}
	})
	t.Run("platform-suffixed names (legacy v2.0.0 packaging)", func(t *testing.T) {
		dir := t.TempDir()
		mustMkdir(t, filepath.Join(dir, "bin"))
		mustMkdir(t, filepath.Join(dir, "frontend-dist"))
		apiBin := fmt.Sprintf("ncc-api-server-%s-%s", runtime.GOOS, runtime.GOARCH)
		uiBin := fmt.Sprintf("ncc-ui-server-%s-%s", runtime.GOOS, runtime.GOARCH)
		if runtime.GOOS == "windows" {
			apiBin += ".exe"
			uiBin += ".exe"
		}
		mustTouch(t, filepath.Join(dir, "bin", apiBin))
		mustTouch(t, filepath.Join(dir, "bin", uiBin))
		if !hasBootstrappedV2Layout(dir) {
			t.Fatalf("platform-suffixed names should be accepted (legacy v2.0.0 stack layout): %s", dir)
		}
	})
	t.Run("missing frontend-dist fails", func(t *testing.T) {
		dir := t.TempDir()
		mustMkdir(t, filepath.Join(dir, "bin"))
		apiBin := "ncc-api-server"
		uiBin := "ncc-ui-server"
		if runtime.GOOS == "windows" {
			apiBin += ".exe"
			uiBin += ".exe"
		}
		mustTouch(t, filepath.Join(dir, "bin", apiBin))
		mustTouch(t, filepath.Join(dir, "bin", uiBin))
		if hasBootstrappedV2Layout(dir) {
			t.Fatalf("missing frontend-dist must fail layout check: %s", dir)
		}
	})
	t.Run("missing api binary fails", func(t *testing.T) {
		dir := t.TempDir()
		mustMkdir(t, filepath.Join(dir, "bin"))
		mustMkdir(t, filepath.Join(dir, "frontend-dist"))
		uiBin := "ncc-ui-server"
		if runtime.GOOS == "windows" {
			uiBin += ".exe"
		}
		mustTouch(t, filepath.Join(dir, "bin", uiBin))
		if hasBootstrappedV2Layout(dir) {
			t.Fatalf("missing api binary must fail layout check: %s", dir)
		}
	})
}

func mustMkdir(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", p, err)
	}
}

func mustTouch(t *testing.T, p string) {
	t.Helper()
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create %s: %v", p, err)
	}
	_ = f.Close()
}

// TestPickAssetForPlatform_TrimmedV200Release simulates the post-hotfix v2.0.0
// release (12 standalone api-server/ui-server assets removed). The v1.x
// selector must now find ncc-orchestrator-* as the first match purely by
// alphabetical order — the test pins this asset-layout policy.
func TestPickAssetForPlatform_TrimmedV200Release(t *testing.T) {
	rel := githubRelease{
		TagName: "v2.0.0-trimmed",
		Assets: []githubAsset{
			{Name: "checksums.txt", BrowserDownloadURL: "https://example.com/checksums.txt"},
			{Name: "example_config.yaml", BrowserDownloadURL: "https://example.com/example_config.yaml"},
			{Name: "ncc-orchestrator-darwin-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-amd64"},
			{Name: "ncc-orchestrator-darwin-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-darwin-arm64"},
			{Name: "ncc-orchestrator-linux-amd64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-amd64"},
			{Name: "ncc-orchestrator-linux-arm64", BrowserDownloadURL: "https://example.com/ncc-orchestrator-linux-arm64"},
			{Name: "ncc-orchestrator-windows-amd64.exe", BrowserDownloadURL: "https://example.com/ncc-orchestrator-windows-amd64.exe"},
			{Name: "ncc-orchestrator-windows-arm64.exe", BrowserDownloadURL: "https://example.com/ncc-orchestrator-windows-arm64.exe"},
			{Name: "ncc-v2-stack-darwin-amd64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-amd64.tar.gz"},
			{Name: "ncc-v2-stack-darwin-arm64.tar.gz", BrowserDownloadURL: "https://example.com/ncc-v2-stack-darwin-arm64.tar.gz"},
		},
	}
	// Even with the legacy empty-exeBase code path, the trimmed layout
	// resolves correctly because no other binary collides alphabetically.
	_, name := pickAssetForPlatform(rel, "darwin", "arm64", "")
	if name != "ncc-orchestrator-darwin-arm64" {
		t.Fatalf("trimmed layout should yield orchestrator binary even with empty exeBase; got %q", name)
	}
	// And with the fix's prefix-match path:
	_, name2 := pickAssetForPlatform(rel, "darwin", "arm64", "ncc-orchestrator")
	if name2 != "ncc-orchestrator-darwin-arm64" {
		t.Fatalf("trimmed layout + exeBase should yield orchestrator binary; got %q", name2)
	}
}
