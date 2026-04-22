package main

import "testing"

func TestComputeRegressionSummary(t *testing.T) {
	previous := RunSummaryJSON{
		Timestamp: "2026-04-01T10:00:00Z",
		Clusters: []RunClusterSummary{
			{Address: "10.0.0.1", FailCount: 1},
			{Address: "10.0.0.2", FailCount: 0},
		},
	}
	current := RunSummaryJSON{
		Timestamp: "2026-04-02T10:00:00Z",
		Clusters: []RunClusterSummary{
			{Address: "10.0.0.1", FailCount: 3}, // increased
			{Address: "10.0.0.2", FailCount: 0}, // unchanged
		},
	}

	reg := computeRegressionSummary(previous, true, current)
	if !reg.HasRegression {
		t.Fatal("expected regression when a cluster fail count increases")
	}
	if reg.DeltaFailTotal != 2 {
		t.Fatalf("unexpected delta fail total: got %d want 2", reg.DeltaFailTotal)
	}
	if len(reg.IncreasedClusters) != 1 || reg.IncreasedClusters[0] != "10.0.0.1" {
		t.Fatalf("unexpected increased clusters: %#v", reg.IncreasedClusters)
	}
}

func TestComputeRegressionSummary_NoPrevious(t *testing.T) {
	current := RunSummaryJSON{
		Clusters: []RunClusterSummary{
			{Address: "10.0.0.1", FailCount: 0},
		},
	}
	reg := computeRegressionSummary(RunSummaryJSON{}, false, current)
	if reg.HasRegression {
		t.Fatal("expected no regression when there is no previous baseline and no FAIL increase")
	}
	if reg.PreviousFailTotal != 0 || reg.CurrentFailTotal != 0 {
		t.Fatalf("unexpected totals: prev=%d curr=%d", reg.PreviousFailTotal, reg.CurrentFailTotal)
	}
}
