package main

import (
	"fmt"
	"html"
	"html/template"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

func filterBlocksBySeverity(blocks []ParsedBlock, allowedSeverities []string) []ParsedBlock {
	if len(allowedSeverities) == 0 {
		return blocks
	}
	allowed := make(map[string]bool)
	for _, s := range allowedSeverities {
		allowed[strings.ToUpper(strings.TrimSpace(s))] = true
	}
	filtered := make([]ParsedBlock, 0, len(blocks))
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		if allowed[sev] {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

type ExcludedAlert struct {
	Severity   string `json:"severity"`
	CheckName  string `json:"check_name"`
	Detail     string `json:"detail,omitempty"`
	MatchMode  string `json:"match_mode"`
	MatchValue string `json:"match_value"`
}

type excludeTitleMatcher struct {
	mode          string
	cleanedTitles []string
	lowerTitles   []string
	regexPatterns []*regexp.Regexp
}

var excludeTitleMatcherCache sync.Map

func buildExcludeTitleMatcher(excludedTitles []string, matchMode string) (*excludeTitleMatcher, error) {
	mode, err := normalizeExcludeAlertMatchMode(matchMode)
	if err != nil {
		return nil, err
	}
	cleanedTitles := make([]string, 0, len(excludedTitles))
	for _, title := range excludedTitles {
		t := strings.TrimSpace(title)
		if t == "" {
			continue
		}
		cleanedTitles = append(cleanedTitles, t)
	}
	if len(cleanedTitles) == 0 {
		return nil, nil
	}
	cacheKey := mode + "\x00" + strings.Join(cleanedTitles, "\x00")
	if cached, ok := excludeTitleMatcherCache.Load(cacheKey); ok {
		if m, ok := cached.(*excludeTitleMatcher); ok {
			return m, nil
		}
	}
	m := &excludeTitleMatcher{
		mode:          mode,
		cleanedTitles: cleanedTitles,
		lowerTitles:   make([]string, 0, len(cleanedTitles)),
	}
	for _, title := range cleanedTitles {
		m.lowerTitles = append(m.lowerTitles, strings.ToLower(title))
	}
	if mode == "regex" {
		m.regexPatterns = make([]*regexp.Regexp, 0, len(cleanedTitles))
		for _, pattern := range cleanedTitles {
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("exclude-alert-titles regex %q: %w", pattern, err)
			}
			m.regexPatterns = append(m.regexPatterns, re)
		}
	}
	excludeTitleMatcherCache.Store(cacheKey, m)
	return m, nil
}

func (m *excludeTitleMatcher) match(checkName string) (bool, string) {
	if m == nil {
		return false, ""
	}
	checkLower := strings.ToLower(strings.TrimSpace(checkName))
	switch m.mode {
	case "exact":
		for i, titleLower := range m.lowerTitles {
			if checkLower == titleLower {
				return true, m.cleanedTitles[i]
			}
		}
	case "contains":
		for i, titleLower := range m.lowerTitles {
			if strings.Contains(checkLower, titleLower) {
				return true, m.cleanedTitles[i]
			}
		}
	case "regex":
		for i, re := range m.regexPatterns {
			if re.MatchString(checkName) {
				return true, m.cleanedTitles[i]
			}
		}
	}
	return false, ""
}

func filterBlocksByTitle(blocks []ParsedBlock, excludedTitles []string, matchMode string) ([]ParsedBlock, []ExcludedAlert, error) {
	matcher, err := buildExcludeTitleMatcher(excludedTitles, matchMode)
	if err != nil {
		return blocks, nil, err
	}
	if matcher == nil {
		return blocks, nil, nil
	}

	filtered := make([]ParsedBlock, 0, len(blocks))
	excluded := make([]ExcludedAlert, 0)
	for _, b := range blocks {
		if ok, matchedBy := matcher.match(b.CheckName); ok {
			excluded = append(excluded, ExcludedAlert{
				Severity:   b.Severity,
				CheckName:  b.CheckName,
				Detail:     b.DetailRaw,
				MatchMode:  matcher.mode,
				MatchValue: matchedBy,
			})
			continue
		}
		filtered = append(filtered, b)
	}
	return filtered, excluded, nil
}

func rowsFromBlocks(blocks []ParsedBlock) []Row {
	rows := make([]Row, 0, len(blocks))
	for _, b := range blocks {
		detail := template.HTML(strings.ReplaceAll(html.EscapeString(b.DetailRaw), "\n", "<br>"))
		rows = append(rows, Row{
			Severity:  b.Severity,
			CheckName: html.EscapeString(strings.ReplaceAll(b.CheckName, "\n", " ")),
			Detail:    detail,
		})
	}
	return rows
}

func filterBlocksToFile(fs FS, inputPath, outputPath string) ([]ParsedBlock, error) {
	data, err := fs.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	log.Debug().Str("path", inputPath).Int("bytes", len(data)).Msg("read raw log")
	return filterBlocksFromText(fs, string(data), inputPath, outputPath)
}

// filterBlocksFromText parses an NCC summary already in memory and writes the
// filtered view. A live run uses this with the text it just saved, rather than
// reading the raw log back from disk (at 7,000 clusters that re-read was ~40%
// of the run's CPU/IO time, mostly waiting on a freshly written file).
func filterBlocksFromText(fs FS, text, sourceLabel, outputPath string) ([]ParsedBlock, error) {
	blocks, err := ParseSummary(text)
	if err != nil {
		return nil, err
	}
	if err := validateParsedAlertsAgainstPluginResults(text, blocks); err != nil {
		log.Warn().Err(err).Str("path", sourceLabel).Msg("parser validation: mismatch between parsed alerts and NCC plugin results")
	}
	if err := fs.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, err
	}
	var b strings.Builder
	for _, pb := range blocks {
		b.WriteString(pb.CheckName)
		b.WriteString("\n")
		b.WriteString(pb.DetailRaw)
		b.WriteString("\n\n---------------------------------------\n")
	}
	if err := fs.WriteFile(outputPath, []byte(b.String()), 0644); err != nil {
		return nil, err
	}
	log.Debug().Str("path", outputPath).Int("bytes", len(b.String())).Msg("wrote filtered")
	return blocks, nil
}
