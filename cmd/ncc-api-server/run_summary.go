package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// classifyClusterErrorMessage mirrors goncc.classifyClusterError so the
// API server can heal stale `error_class` values written by older binaries.
// Keep this in sync with the orchestrator implementation in cmd/ncc-orchestrator/run_results.go.
func classifyClusterErrorMessage(raw string) string {
	msg := strings.ToLower(strings.TrimSpace(raw))
	if msg == "" {
		return ""
	}
	switch {
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "timed out"), strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "http 401"), strings.Contains(msg, "http 403"), strings.Contains(msg, "unauthorized"), strings.Contains(msg, "forbidden"), strings.Contains(msg, "authentication"):
		return "auth"
	case strings.Contains(msg, "no such host"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "host is down"),
		strings.Contains(msg, "tls"),
		strings.Contains(msg, "x509"):
		return "network"
	case strings.Contains(msg, "http 429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "rate limit"),
		strings.Contains(msg, "rate-limit"),
		strings.Contains(msg, "retry-after"):
		return "rate_limit"
	case strings.Contains(msg, "parse filtered"), strings.Contains(msg, "parse summary"), strings.Contains(msg, "parser"):
		return "parser"
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

// reclassifyRunSummaryInPlace walks the parsed run-summary map and rewrites
// each failed cluster's `error_class` using the current classifier, then
// rebuilds the top-level `failure_classes` histogram so the Insights "Run
// Reliability" panel reflects the corrected buckets even for legacy runs.
func reclassifyRunSummaryInPlace(m map[string]interface{}) {
	clusters, ok := m["clusters"].([]interface{})
	if !ok {
		return
	}
	counts := map[string]int{
		"timeout": 0, "auth": 0, "network": 0, "parser": 0,
		"rate_limit": 0, "api": 0, "unknown": 0,
	}
	for _, raw := range clusters {
		c, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if okFlag, _ := c["ok"].(bool); okFlag {
			continue
		}
		errMsg, _ := c["error"].(string)
		if strings.TrimSpace(errMsg) == "" {
			continue
		}
		class := classifyClusterErrorMessage(errMsg)
		if class == "" {
			class = "unknown"
		}
		c["error_class"] = class
		counts[class]++
	}
	m["failure_classes"] = map[string]interface{}{
		"timeout":    counts["timeout"],
		"auth":       counts["auth"],
		"network":    counts["network"],
		"parser":     counts["parser"],
		"rate_limit": counts["rate_limit"],
		"api":        counts["api"],
		"unknown":    counts["unknown"],
	}
}

// parseNCCSummaryCounts aggregates per-state counts from every *.log file in
// logDir, recognising the NCC summary table. The "Warning" row was previously
// omitted, which caused the UI to show a spurious "summary count mismatch"
// banner whenever NCC reported any WARN rows.
// nccPluginCountRes holds one compiled pattern per NCC "Plugin results" label
// (`| Fail | 3 |`). Both log parsers used to compile them per label per file —
// ~50k compilations per pass over a 7,000-cluster log directory.
var (
	nccPluginCountMu  sync.Mutex
	nccPluginCountRes = map[string]*regexp.Regexp{}
)

// nccPluginCount returns the count on the `| <label> | <n> |` line, or 0.
func nccPluginCount(content, label string) int {
	nccPluginCountMu.Lock()
	re, ok := nccPluginCountRes[label]
	if !ok {
		re = regexp.MustCompile(`(?im)\|\s*` + regexp.QuoteMeta(label) + `\s*\|\s*(\d+)\s*\|`)
		nccPluginCountRes[label] = re
	}
	nccPluginCountMu.Unlock()
	m := re.FindStringSubmatch(content)
	if len(m) < 2 {
		return 0
	}
	v, _ := strconv.Atoi(m[1])
	return v
}

func parseNCCSummaryCounts(logDir string) map[string]int {
	totals := map[string]int{
		"fail":          0,
		"warn":          0,
		"pass":          0,
		"info":          0,
		"error":         0,
		"unknown":       0,
		"total_plugins": 0,
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return totals
	}
	parseCount := nccPluginCount
	// Some NCC variants emit "Warning", others "Warn". Try both and take the
	// first that produced a non-zero value (or the last one, to be safe).
	parseWarn := func(content string) int {
		if v := parseCount(content, "Warning"); v > 0 {
			return v
		}
		return parseCount(content, "Warn")
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(logDir, e.Name()))
		if err != nil {
			continue
		}
		content := string(b)
		totals["fail"] += parseCount(content, "Fail")
		totals["warn"] += parseWarn(content)
		totals["pass"] += parseCount(content, "Pass")
		totals["info"] += parseCount(content, "Info")
		totals["error"] += parseCount(content, "Error")
		totals["unknown"] += parseCount(content, "Unknown")
		totals["total_plugins"] += parseCount(content, "Total Plugins")
	}
	return totals
}

func parseNCCClusterSummary(logDir string) []map[string]interface{} {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return []map[string]interface{}{}
	}
	out := make([]map[string]interface{}, 0, len(entries))
	parseCount := nccPluginCount
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".log") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(logDir, name))
		if err != nil {
			continue
		}
		content := string(b)
		fail := parseCount(content, "Fail")
		warn := parseCount(content, "Warning")
		if warn == 0 {
			warn = parseCount(content, "Warn")
		}
		pass := parseCount(content, "Pass")
		info := parseCount(content, "Info")
		errCount := parseCount(content, "Error")
		unknown := parseCount(content, "Unknown")
		total := parseCount(content, "Total Plugins")
		healthRate := 0.0
		if total > 0 {
			healthRate = (float64(pass) / float64(total)) * 100.0
		}
		addr := strings.TrimSuffix(name, ".log")
		out = append(out, map[string]interface{}{
			"address":       addr,
			"log_name":      name,
			"fail":          fail,
			"warn":          warn,
			"pass":          pass,
			"info":          info,
			"error":         errCount,
			"unknown":       unknown,
			"total_plugins": total,
			"health_rate":   healthRate,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["address"]) < fmt.Sprint(out[j]["address"])
	})
	return out
}
