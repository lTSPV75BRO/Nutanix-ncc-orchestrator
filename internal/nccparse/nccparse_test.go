package nccparse

import (
	"strings"
	"testing"

	"goncc/internal/model"
)

// FuzzParseSummary ensures ParseSummary never panics on arbitrary input and
// returns blocks whose severities are always from the known set.
func FuzzParseSummary(f *testing.F) {
	f.Add("")
	f.Add("Detailed information for health_checks:\nPASS\n")
	f.Add("Node check: FAIL\nDetail: something broke\n")
	f.Add("\x00\x01\xffrandom\nWARN: weird\n")
	f.Add(strings.Repeat("INFO line\n", 100))
	f.Fuzz(func(t *testing.T, in string) {
		blocks, err := ParseSummary(in)
		if err != nil {
			return
		}
		allowed := map[string]bool{"FAIL": true, "WARN": true, "ERR": true, "INFO": true, "PASS": true, "": true}
		for _, b := range blocks {
			if !allowed[b.Severity] {
				t.Fatalf("unexpected severity %q for check %q", b.Severity, b.CheckName)
			}
		}
	})
}

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
	if got := SplitLines(""); len(got) != 0 {
		t.Errorf("empty input: got %v, want []", got)
	}
	got := SplitLines("a\nb\n")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "" {
		t.Errorf("trailing newline should yield empty final line, got %v", got)
	}
	if got := SplitLines("x\ny"); len(got) != 2 {
		t.Errorf("no trailing newline: got %v, want 2 lines", got)
	}
}

func TestParseSummary(t *testing.T) {
	input := strings.Join([]string{
		"Detailed information for check_a:",
		"Node 1: FAIL: disk full",
		"Refer to KB 123 for details.",
		"Detailed information for check_b:",
		"Node 2: WARN: high latency",
		"Refer to KB 456 for details.",
	}, "\n")

	blocks, err := ParseSummary(input)
	if err != nil {
		t.Fatalf("ParseSummary: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("got %d blocks, want 2", len(blocks))
	}
	if blocks[0].Severity != "FAIL" {
		t.Errorf("block 0 severity: got %q, want FAIL", blocks[0].Severity)
	}
	if blocks[1].Severity != "WARN" {
		t.Errorf("block 1 severity: got %q, want WARN", blocks[1].Severity)
	}
	if !strings.Contains(blocks[0].DetailRaw, "disk full") {
		t.Errorf("block 0 detail missing content: %q", blocks[0].DetailRaw)
	}
}

func TestValidateParsedAlertsAgainstPluginResults(t *testing.T) {
	// No plugin-result lines -> validation is a no-op (passes).
	if err := ValidateParsedAlertsAgainstPluginResults("no plugin markers here", nil); err != nil {
		t.Errorf("expected nil when no plugin results present, got %v", err)
	}

	raw := "plugin_results:\n[ FAIL ] something\n[ WARN ] other"
	matching := []model.ParsedBlock{{Severity: "FAIL"}, {Severity: "WARN"}}
	if err := ValidateParsedAlertsAgainstPluginResults(raw, matching); err != nil {
		t.Errorf("matching severities should validate, got %v", err)
	}

	mismatch := []model.ParsedBlock{{Severity: "FAIL"}} // missing the WARN
	if err := ValidateParsedAlertsAgainstPluginResults(raw, mismatch); err == nil {
		t.Errorf("expected mismatch error, got nil")
	}
}
