package main

import "strings"

// This file implements run self-healing for the api-server's run engine: it
// classifies a failed run's output into a root cause and, for the
// auto-recoverable classes, derives a *safe* mitigation (a conservative flag
// override) that a single bounded auto-retry applies. The goal is to turn a
// transient class of failure (a rate-limit burst, a slow Prism, a saturated
// link) into an automatic recovery instead of an operator ticket.

// runFailureClass is a coarse root-cause bucket for a failed orchestrator run.
// It mirrors the orchestrator's per-cluster classifier but operates on the
// captured run output text (the two binaries don't share a package).
type runFailureClass string

const (
	runFailAuth      runFailureClass = "auth"
	runFailRateLimit runFailureClass = "rate_limit"
	runFailTimeout   runFailureClass = "timeout"
	runFailNetwork   runFailureClass = "network"
	runFailParser    runFailureClass = "parser"
	runFailUnknown   runFailureClass = "unknown"
)

// classifyRunOutput buckets run output/error text into a failure class. Order
// matters: network/auth signals are matched before the generic rate-limit and
// transport phrases so a connection failure is not mislabeled.
func classifyRunOutput(text string) runFailureClass {
	m := strings.ToLower(text)
	switch {
	case containsAny(m, "http 401", "http 403", "unauthorized", "forbidden", "authentication failed", "invalid credentials"):
		return runFailAuth
	case containsAny(m, "no such host", "connection refused", "no route to host", "network is unreachable", "host is down", "x509", "tls handshake", "certificate"):
		return runFailNetwork
	case containsAny(m, "http 429", "too many requests", "rate limit", "rate-limit", "retry-after"):
		return runFailRateLimit
	case containsAny(m, "context deadline exceeded", "timed out", "timeout", "i/o timeout"):
		return runFailTimeout
	case containsAny(m, "parse filtered", "parse summary", "parser"):
		return runFailParser
	default:
		return runFailUnknown
	}
}

func containsAny(haystack string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(haystack, n) {
			return true
		}
	}
	return false
}

// mitigationArgsForClass returns extra orchestrator flags that conservatively
// counter the given failure class, skipping any flag the caller already set
// (so an explicit operator choice is never overridden). ok is false when the
// class is not safely auto-recoverable (auth, parser, unknown) — retrying those
// unchanged would just fail again or mask a real misconfiguration.
func mitigationArgsForClass(class runFailureClass, existingArgs []string) (args []string, ok bool) {
	has := func(flag string) bool { return extraArgsHaveFlag(existingArgs, strings.TrimPrefix(flag, "--")) }
	add := func(flag, val string) {
		if !has(flag) {
			args = append(args, flag, val)
		}
	}
	switch class {
	case runFailRateLimit:
		// Back off: fewer parallel clusters and more retry headroom.
		add("--max-parallel", "2")
		add("--retry-max-attempts", "8")
	case runFailTimeout, runFailNetwork:
		// Give slow/lossy links more time and reduce concurrency pressure.
		add("--request-timeout", "40s")
		add("--max-parallel", "2")
	default:
		return nil, false
	}
	if len(args) == 0 {
		// Everything we'd add was already set by the operator; a retry would be
		// identical, so don't bother.
		return nil, false
	}
	return args, true
}

// runHealDecision is the outcome of evaluating whether to auto-retry a run.
type runHealDecision struct {
	Retry      bool
	Class      runFailureClass
	Mitigation []string
}

// decideRunHeal evaluates a finished run for a single bounded auto-retry. It is
// pure so the policy is unit-testable independent of the live engine.
func decideRunHeal(runErrPresent, cancelled, autoRetryDisabled bool, retryCount int, output string, existingArgs []string) runHealDecision {
	if !runErrPresent || cancelled || autoRetryDisabled || retryCount >= 1 {
		return runHealDecision{}
	}
	class := classifyRunOutput(output)
	mit, ok := mitigationArgsForClass(class, existingArgs)
	if !ok {
		return runHealDecision{Class: class}
	}
	return runHealDecision{Retry: true, Class: class, Mitigation: mit}
}
