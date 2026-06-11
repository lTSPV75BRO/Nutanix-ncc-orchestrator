package main

import (
	"reflect"
	"testing"
)

func TestClassifyRunOutput(t *testing.T) {
	cases := map[string]runFailureClass{
		"cluster x: HTTP 401 Unauthorized":              runFailAuth,
		"dial tcp 10.0.0.1:9440: connection refused":    runFailNetwork,
		"x509: certificate signed by unknown authority": runFailNetwork,
		"HTTP 429 Too Many Requests (Retry-After: 5)":   runFailRateLimit,
		"context deadline exceeded while polling task":  runFailTimeout,
		"failed to parse filtered NCC output":           runFailParser,
		"some entirely unrecognized failure":            runFailUnknown,
	}
	for text, want := range cases {
		if got := classifyRunOutput(text); got != want {
			t.Errorf("classifyRunOutput(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestMitigationArgsForClass(t *testing.T) {
	// Rate-limit: back off parallelism + add retry headroom.
	args, ok := mitigationArgsForClass(runFailRateLimit, nil)
	if !ok || !reflect.DeepEqual(args, []string{"--max-parallel", "2", "--retry-max-attempts", "8"}) {
		t.Fatalf("rate_limit mitigation = %v ok=%v", args, ok)
	}

	// Timeout: bump request-timeout and reduce parallelism.
	args, ok = mitigationArgsForClass(runFailTimeout, nil)
	if !ok || !reflect.DeepEqual(args, []string{"--request-timeout", "40s", "--max-parallel", "2"}) {
		t.Fatalf("timeout mitigation = %v ok=%v", args, ok)
	}

	// Operator-set flags must not be overridden; if all would-be additions are
	// already present, there's nothing safe to change → not retryable.
	if _, ok := mitigationArgsForClass(runFailRateLimit, []string{"--max-parallel", "8", "--retry-max-attempts", "3"}); ok {
		t.Fatal("expected no mitigation when all flags already set by operator")
	}

	// Non-recoverable classes never auto-retry.
	for _, c := range []runFailureClass{runFailAuth, runFailParser, runFailUnknown} {
		if _, ok := mitigationArgsForClass(c, nil); ok {
			t.Errorf("class %q must not be auto-recoverable", c)
		}
	}
}

func TestDecideRunHeal(t *testing.T) {
	// Happy path: a recoverable failure on the first attempt retries with a mitigation.
	d := decideRunHeal(true, false, false, 0, "HTTP 429 Too Many Requests", nil)
	if !d.Retry || d.Class != runFailRateLimit || len(d.Mitigation) == 0 {
		t.Fatalf("expected retry on rate-limit: %+v", d)
	}

	// Guards: no error, cancelled, disabled, or already retried → never retry.
	if decideRunHeal(false, false, false, 0, "HTTP 429", nil).Retry {
		t.Error("no error must not retry")
	}
	if decideRunHeal(true, true, false, 0, "HTTP 429", nil).Retry {
		t.Error("cancelled run must not retry")
	}
	if decideRunHeal(true, false, true, 0, "HTTP 429", nil).Retry {
		t.Error("disabled auto-retry must not retry")
	}
	if decideRunHeal(true, false, false, 1, "HTTP 429", nil).Retry {
		t.Error("already-retried run must not retry again (bounded to 1)")
	}

	// Auth failure is recognized but not auto-retried.
	d = decideRunHeal(true, false, false, 0, "HTTP 401 Unauthorized", nil)
	if d.Retry || d.Class != runFailAuth {
		t.Fatalf("auth failure must classify but not retry: %+v", d)
	}
}
