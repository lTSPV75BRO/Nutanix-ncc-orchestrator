package main

import (
	"testing"
	"time"
)

func TestClusterHealthScore(t *testing.T) {
	if got := clusterHealthScore(0, 0, 0, 10); got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
	if got := clusterHealthScore(5, 0, 0, 5); got != 0 {
		t.Fatalf("expected 0 for all FAIL, got %d", got)
	}
}

func TestComputeDrillDownDiff(t *testing.T) {
	prev := ChecksSnapshotJSON{
		Timestamp: "2026-04-20T10:00:00Z",
		Clusters: []ClusterChecksSnapshot{
			{
				Address: "10.0.0.1",
				Checks: []CheckSnapshotEntry{
					{CheckName: "check-a", Severity: "WARN"},
					{CheckName: "check-b", Severity: "FAIL"},
				},
			},
		},
	}
	curr := ChecksSnapshotJSON{
		Timestamp: "2026-04-21T10:00:00Z",
		Clusters: []ClusterChecksSnapshot{
			{
				Address: "10.0.0.1",
				Checks: []CheckSnapshotEntry{
					{CheckName: "check-a", Severity: "FAIL"},
					{CheckName: "check-c", Severity: "INFO"},
				},
			},
		},
	}
	diff := computeDrillDownDiff(prev, true, curr)
	if diff.NewFailCount != 1 {
		t.Fatalf("expected new fail count=1, got %d", diff.NewFailCount)
	}
	if diff.ResolvedFailCount != 1 {
		t.Fatalf("expected resolved fail count=1, got %d", diff.ResolvedFailCount)
	}
	if len(diff.Clusters) != 1 {
		t.Fatalf("expected one cluster diff, got %d", len(diff.Clusters))
	}
}

func TestEvaluatePolicyGates(t *testing.T) {
	metrics := map[string]float64{
		"new-fails":        2,
		"fail-rate":        3.5,
		"min-health-score": 88,
	}
	violations, err := evaluatePolicyGates([]string{"new-fails>0", "fail-rate>2", "min-health-score<90"}, metrics)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(violations) != 3 {
		t.Fatalf("expected 3 violations, got %d", len(violations))
	}
}

func TestInQuietHoursOvernight(t *testing.T) {
	now := time.Date(2026, 4, 21, 1, 30, 0, 0, time.Local)
	ok, err := inQuietHours(now, "22:00-06:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected in quiet-hours for overnight range")
	}
}

func TestValidateParsedAlertsAgainstPluginResults_Match(t *testing.T) {
	raw := `
Detailed information for foo_check:
Node x:
FAIL: something failed
Refer to KB ...

Detailed information for bar_check:
Node x:
WARN: something warning
Refer to KB ...

PLUGIN RESULTS
/health/foo [ FAIL ]
/health/bar [ WARN ]
`
	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "foo_check"},
		{Severity: "WARN", CheckName: "bar_check"},
	}
	if err := validateParsedAlertsAgainstPluginResults(raw, blocks); err != nil {
		t.Fatalf("expected no mismatch, got error: %v", err)
	}
}

func TestValidateParsedAlertsAgainstPluginResults_Mismatch(t *testing.T) {
	raw := `
PLUGIN RESULTS
/health/foo [ FAIL ]
/health/bar [ WARN ]
`
	blocks := []ParsedBlock{
		{Severity: "FAIL", CheckName: "foo_check"},
	}
	if err := validateParsedAlertsAgainstPluginResults(raw, blocks); err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
}
