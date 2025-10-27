package parser

import (
	"bufio"
	"html"
	"html/template"
	"regexp"
	"strings"

	"goncc/pkg/errors"
	"goncc/pkg/types"
)

var (
	reBlockStart = regexp.MustCompile(`^Detailed information for .*`)
	reBlockEnd   = regexp.MustCompile(`^Refer to.*`)
	reSeverity   = regexp.MustCompile(`\b(FAIL|WARN|INFO|ERR):`)
)

// splitLines splits a string into lines with proper handling of large content
func splitLines(s string) []string {
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

// detectSeverity detects the severity level from a text block
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

// ParseSummary parses NCC summary text into structured blocks
func ParseSummary(text string) ([]types.ParsedBlock, error) {
	if text == "" {
		return nil, errors.ParseError("empty summary text")
	}

	lines := splitLines(text)
	var blocks []types.ParsedBlock

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
			blocks = append(blocks, types.ParsedBlock{
				Severity:  detectSeverity(joined),
				CheckName: checkName,
				DetailRaw: joined,
			})
		}
	}

	if len(blocks) == 0 {
		return nil, errors.ParseError("no valid blocks found in summary")
	}

	return blocks, nil
}

// RowsFromBlocks converts parsed blocks to HTML rows
func RowsFromBlocks(blocks []types.ParsedBlock) []types.Row {
	rows := make([]types.Row, 0, len(blocks))
	for _, b := range blocks {
		detail := template.HTML(strings.ReplaceAll(html.EscapeString(b.DetailRaw), "\n", "<br>"))
		rows = append(rows, types.Row{
			Severity:  b.Severity,
			CheckName: html.EscapeString(strings.ReplaceAll(b.CheckName, "\n", " ")),
			Detail:    detail,
		})
	}
	return rows
}

// SanitizeSummary cleans up summary text
func SanitizeSummary(s string) string {
	return strings.ReplaceAll(s, "\\n", "\n")
}
