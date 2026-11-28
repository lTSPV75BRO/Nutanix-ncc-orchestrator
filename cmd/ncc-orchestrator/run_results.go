package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type parsedPolicyGate struct {
	Raw       string
	Metric    string
	Operator  string
	Threshold float64
}

var policyGateRe = regexp.MustCompile(`^\s*([a-zA-Z0-9\-_]+)\s*(>=|<=|==|!=|>|<)\s*([0-9]+(?:\.[0-9]+)?)\s*$`)

func parsePolicyGate(expr string) (parsedPolicyGate, error) {
	expr = strings.TrimSpace(expr)
	m := policyGateRe.FindStringSubmatch(expr)
	if len(m) != 4 {
		return parsedPolicyGate{}, fmt.Errorf("invalid policy gate %q (expected metric<op>number)", expr)
	}
	th, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return parsedPolicyGate{}, err
	}
	return parsedPolicyGate{
		Raw:       expr,
		Metric:    strings.ToLower(strings.TrimSpace(m[1])),
		Operator:  m[2],
		Threshold: th,
	}, nil
}

func compareFloat(lhs float64, op string, rhs float64) bool {
	switch op {
	case ">":
		return lhs > rhs
	case ">=":
		return lhs >= rhs
	case "<":
		return lhs < rhs
	case "<=":
		return lhs <= rhs
	case "==":
		return lhs == rhs
	case "!=":
		return lhs != rhs
	default:
		return false
	}
}

func evaluatePolicyGates(gates []string, metrics map[string]float64) ([]string, error) {
	var violations []string
	for _, g := range gates {
		if strings.TrimSpace(g) == "" {
			continue
		}
		p, err := parsePolicyGate(g)
		if err != nil {
			return nil, err
		}
		val, ok := metrics[p.Metric]
		if !ok {
			return nil, fmt.Errorf("unsupported policy gate metric %q", p.Metric)
		}
		if compareFloat(val, p.Operator, p.Threshold) {
			violations = append(violations, fmt.Sprintf("%s violated (actual=%.2f)", p.Raw, val))
		}
	}
	return violations, nil
}

// classifyClusterError maps a failed cluster's error chain into one of the
// canonical buckets surfaced in run-summary.json's `failure_classes`.
//
// IMPORTANT: the order matters. Network signals (no-such-host, dial tcp,
// connection refused) are evaluated BEFORE the "retry circuit breaker opened"
// match, because the retry circuit breaker opens on *any* repeated transport
// failure — including DNS resolution failures. If we matched the breaker first
// we would misclassify a DNS lookup failure as `rate_limit`, falsely
// implicating max-parallel/throttling when the real fix is "configure DNS or
// use an IP address".
func classifyClusterError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "timed out"), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "http 401"), strings.Contains(msg, "http 403"), strings.Contains(msg, "unauthorized"), strings.Contains(msg, "forbidden"), strings.Contains(msg, "authentication"):
		return "auth"
	// Network-layer signals first so DNS/refused/unreachable errors are not
	// swallowed by a downstream circuit-breaker match below.
	case strings.Contains(msg, "no such host"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "host is down"),
		strings.Contains(msg, "tls"),
		strings.Contains(msg, "x509"):
		return "network"
	// Only classify as rate-limit when there's a *real* rate-limit signal —
	// HTTP 429, an explicit "rate limit" phrase, "too many requests", or a
	// Retry-After header. A circuit breaker that opened because the TCP dial
	// kept failing is NOT a rate-limit problem.
	case strings.Contains(msg, "http 429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "rate limit"),
		strings.Contains(msg, "rate-limit"),
		strings.Contains(msg, "retry-after"):
		return "rate_limit"
	case strings.Contains(msg, "parse filtered"), strings.Contains(msg, "parse summary"), strings.Contains(msg, "parser"):
		return "parser"
	// Generic transport bucket: circuit breakers triggered by repeated
	// transport failures (without an explicit rate-limit signal) land here.
	case strings.Contains(msg, "retry circuit breaker opened"),
		strings.Contains(msg, "transport error"),
		strings.Contains(msg, "dial tcp"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection reset"):
		return "network"
	case strings.Contains(msg, "http 4"), strings.Contains(msg, "http 5"), strings.Contains(msg, "start checks failed"), strings.Contains(msg, "get summary failed"), strings.Contains(msg, "poll failed"):
		return "api"
	default:
		return "unknown"
	}
}

// runFailedRemediation returns an actionable, single-line hint for the
// synthetic clusterRunFailedCheckName alert's Detail field, based on the
// cluster's classified error bucket (classifyClusterError). Deliberately
// plain text with no URL — the Alerts table's KB column derives from a
// portal.nutanix.com link in Detail, and there's no real NCC KB article for
// "the run couldn't reach this cluster", so KB is left empty on purpose.
func runFailedRemediation(err error, errorClass string) string {
	if err == nil {
		return ""
	}
	class := strings.TrimSpace(errorClass)
	if class == "" {
		class = classifyClusterError(err)
	}
	var hint string
	switch class {
	case "auth":
		hint = "Verify the username/password (or secret:// source) and check for an account lockout in Prism. Try: ncc-orchestrator preflight-check --config <config.yaml>"
	case "timeout":
		hint = "The cluster didn't respond in time. Increase --timeout/--request-timeout, reduce --max-parallel, and verify routing to the cluster."
	case "network":
		hint = "Could not reach the cluster (DNS/connection failure). Verify the address resolves and is routable, and check firewall/VPN. Try: ncc-orchestrator discover-clusters --prism-central-url <pc-url>"
	case "rate_limit":
		hint = "The cluster is rate-limiting requests. Lower --max-parallel and increase --retry-max-attempts/--retry-base-delay."
	case "api":
		hint = "The Prism API rejected the request. Verify --ncc-api-version/--nutanix-v4-api-version and that Prism services are healthy."
	case "parser":
		hint = "NCC output could not be parsed. Inspect the raw log under output-dir-logs for an unexpected payload format."
	default:
		hint = "Investigate connectivity/credentials for this cluster, then re-run. Try: ncc-orchestrator preflight-check --config <config.yaml>"
	}
	return hint
}

func buildRunClusterSummary(r ClusterResult) RunClusterSummary {
	s := RunClusterSummary{Address: r.Cluster, OK: r.Err == nil}
	if r.Err != nil {
		s.Error = r.Err.Error()
		if strings.TrimSpace(r.ErrorClass) != "" {
			s.ErrorClass = r.ErrorClass
		} else {
			s.ErrorClass = classifyClusterError(r.Err)
		}
		return s
	}
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range r.Blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		if _, ok := counts[sev]; ok {
			counts[sev]++
		} else {
			counts["INFO"]++
		}
	}
	s.FailCount = counts["FAIL"]
	s.WarnCount = counts["WARN"]
	s.ErrCount = counts["ERR"]
	s.InfoCount = counts["INFO"]
	s.ChecksTotal = len(r.Blocks)
	s.HealthScore = clusterHealthScore(s.FailCount, s.WarnCount, s.ErrCount, s.ChecksTotal)
	return s
}

func newFailureClassCounts() map[string]int {
	return map[string]int{
		"timeout":    0,
		"auth":       0,
		"network":    0,
		"api":        0,
		"parser":     0,
		"rate_limit": 0,
		"unknown":    0,
	}
}

func incrementFailureClassCount(counts map[string]int, r ClusterResult) {
	if r.Err == nil {
		return
	}
	class := strings.TrimSpace(r.ErrorClass)
	if class == "" {
		class = classifyClusterError(r.Err)
	}
	if _, ok := counts[class]; !ok {
		class = "unknown"
	}
	counts[class]++
}

func printFailureResolutionHints(counts map[string]int) {
	if counts == nil {
		return
	}
	hints := []string{}
	if counts["auth"] > 0 {
		hints = append(hints, "Auth failures detected: verify username/password or secret:// source and Prism account lock state. Command: ncc-orchestrator preflight-check --config <config.yaml>")
	}
	if counts["timeout"] > 0 || counts["network"] > 0 {
		hints = append(hints, "Timeout/network failures detected: reduce --max-parallel, increase --timeout/--request-timeout, verify routing/firewall. Command: ncc-orchestrator --auto --automation-level full-auto --config <config.yaml>")
	}
	if counts["rate_limit"] > 0 {
		hints = append(hints, "Rate-limit failures detected: lower --max-parallel and tune retry/backoff settings. Command: ncc-orchestrator --max-parallel 4 --retry-max-attempts 8 --config <config.yaml>")
	}
	if counts["api"] > 0 {
		hints = append(hints, "API failures detected: verify API version flags and Prism service health. Command: ncc-orchestrator discover-clusters --discover-api-version v4 --prism-central-url <pc-url>")
	}
	if counts["parser"] > 0 {
		hints = append(hints, "Parser failures detected: inspect raw NCC logs under output-dir-logs for unexpected payload formats. Command: ncc-orchestrator preflight-check --config <config.yaml>")
	}
	if len(hints) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "Failure resolution recommendations:")
	for _, h := range hints {
		fmt.Fprintf(os.Stderr, "- %s\n", h)
	}
}
