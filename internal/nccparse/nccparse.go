// Package nccparse parses raw NCC health-check summary text into structured
// findings (model.ParsedBlock) and cross-checks parsed severities against the
// raw plugin-result lines. It depends only on goncc/internal/model.
//
// Package main re-exports SplitLines, ParseSummary, and
// ValidateParsedAlertsAgainstPluginResults via aliases so existing call sites
// are unchanged.
package nccparse

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"goncc/internal/model"
)

var (
	reBlockStart = regexp.MustCompile(`^Detailed information for .*`)
	reBlockEnd   = regexp.MustCompile(`^Refer to.*`)
	reSeverity   = regexp.MustCompile(`\b(FAIL|WARN|INFO|ERR)\s*:`)
	rePluginSev  = regexp.MustCompile(`\[\s*(FAIL|WARN|ERR|INFO)\s*\]`)
)

// SplitLines splits text into lines using a large scanner buffer, preserving a
// trailing empty line when the input ends with a newline.
func SplitLines(s string) []string {
	sc := bufio.NewScanner(strings.NewReader(s))
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lines := []string{}
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(s) > 0 && strings.HasSuffix(s, "\n") {
		lines = append(lines, "")
	}
	return lines
}

func detectSeverity(s string) string {
	loc := reSeverity.FindStringSubmatch(s)
	if len(loc) > 1 {
		return loc[1]
	}
	switch {
	case strings.Contains(s, "FAIL:"):
		return "FAIL"
	case strings.Contains(s, "WARN:"):
		return "WARN"
	case strings.Contains(s, "ERR:"):
		return "ERR"
	case strings.Contains(s, "INFO:"):
		return "INFO"
	default:
		return "INFO"
	}
}

// ParseSummary extracts the per-check detail blocks from an NCC run summary.
func ParseSummary(text string) ([]model.ParsedBlock, error) {
	lines := SplitLines(text)
	var blocks []model.ParsedBlock
	for i := 0; i < len(lines); i++ {
		if reBlockStart.MatchString(lines[i]) {
			checkName := lines[i]
			i++
			var buf []string
			for i < len(lines) && !reBlockEnd.MatchString(lines[i]) {
				buf = append(buf, lines[i])
				i++
			}
			if i < len(lines) {
				buf = append(buf, lines[i])
			}
			joined := strings.Join(buf, "\n")
			blocks = append(blocks, model.ParsedBlock{
				Severity:  detectSeverity(joined),
				CheckName: checkName,
				DetailRaw: joined,
			})
		}
	}
	return blocks, nil
}

func countParsedSeverities(blocks []model.ParsedBlock) map[string]int {
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range blocks {
		sev := strings.ToUpper(strings.TrimSpace(b.Severity))
		if _, ok := counts[sev]; !ok {
			continue
		}
		counts[sev]++
	}
	return counts
}

func parsePluginResultsSeverities(raw string) map[string]int {
	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, ln := range SplitLines(raw) {
		m := rePluginSev.FindStringSubmatch(strings.ToUpper(ln))
		if len(m) != 2 {
			continue
		}
		sev := m[1]
		if _, ok := counts[sev]; ok {
			counts[sev]++
		}
	}
	return counts
}

// ValidateParsedAlertsAgainstPluginResults ensures parser output remains
// aligned with NCC plugin-result severities in raw logs.
func ValidateParsedAlertsAgainstPluginResults(raw string, blocks []model.ParsedBlock) error {
	plugin := parsePluginResultsSeverities(raw)
	totalPlugin := plugin["FAIL"] + plugin["WARN"] + plugin["ERR"] + plugin["INFO"]
	if totalPlugin == 0 {
		return nil
	}
	parsed := countParsedSeverities(blocks)
	if parsed["FAIL"] != plugin["FAIL"] ||
		parsed["WARN"] != plugin["WARN"] ||
		parsed["ERR"] != plugin["ERR"] ||
		parsed["INFO"] != plugin["INFO"] {
		return fmt.Errorf("parsed alerts mismatch plugin results: parsed={FAIL:%d WARN:%d ERR:%d INFO:%d} plugin_results={FAIL:%d WARN:%d ERR:%d INFO:%d}",
			parsed["FAIL"], parsed["WARN"], parsed["ERR"], parsed["INFO"],
			plugin["FAIL"], plugin["WARN"], plugin["ERR"], plugin["INFO"])
	}
	return nil
}
