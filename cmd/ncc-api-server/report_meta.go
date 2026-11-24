package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func readTailFile(path string, maxBytes int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	var offset int64 = 0
	if st.Size() > maxBytes {
		offset = st.Size() - maxBytes
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	b, err := io.ReadAll(io.LimitReader(f, maxBytes)) // the file can grow after Stat
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// loadReportMeta reads REPORT_META embedded in index.html and enriches it with
// fields from ncc-run-record.json (hostname, scheduler_source, git_revision,
// orchestrator_version) when they are missing or empty. Older reports — produced
// before reportMeta carried these fields — are upgraded transparently.
func loadReportMeta(outDir string) map[string]interface{} {
	out := map[string]interface{}{}
	if v, ok := readInlineJSONVarCached(filepath.Join(outDir, "index.html"), "REPORT_META", map[string]interface{}{}).(map[string]interface{}); ok {
		for k, val := range v {
			out[k] = val
		}
	}
	// ncc-run-record.json lists every cluster (1.5 MB at 7,000), so it is
	// parsed once per run rather than on every report request.
	if rec, ok := readJSONArtifactCached(filepath.Join(outDir, "ncc-run-record.json"), nil).(map[string]interface{}); ok {
		fill := func(key string, recKey string) {
			if cur, ok := out[key].(string); ok && strings.TrimSpace(cur) != "" {
				return
			}
			if v, ok := rec[recKey].(string); ok && strings.TrimSpace(v) != "" {
				out[key] = v
			}
		}
		fill("hostname", "hostname")
		fill("scheduler_source", "scheduler_source")
		fill("git_revision", "git_revision")
		fill("stream", "stream")
		if cur, ok := out["version"].(string); !ok || strings.TrimSpace(cur) == "" {
			if v, ok := rec["orchestrator_version"].(string); ok && strings.TrimSpace(v) != "" {
				out["version"] = v
			}
		}
	}
	return out
}

// inlineVarPrefixPattern builds a regex matching only the `const NAME =` /
// `var NAME =` declaration prefix (never the value itself). The value's end
// is located separately via findJSONValueEnd, which is JSON-aware — see its
// doc comment for why a naive `(.*?);` regex is unsafe here.
func inlineVarPrefixPattern(varName string) *regexp.Regexp {
	return regexp.MustCompile(`(?:const|var)\s+` + regexp.QuoteMeta(varName) + `\s*=\s*`)
}

// findJSONValueEnd scans the JSON value that starts at (or after) index i in
// data — skipping leading whitespace — and returns the index just past its
// final character.
//
// This exists because index.html embeds report data as
// `const AGG = <json>;` and a naive `(.*?);` regex (the previous approach)
// stops at the FIRST semicolon it finds, including one that appears inside a
// quoted JSON string. NCC check titles/details very commonly contain literal
// semicolons (e.g. "Description: X; Recommendation: Y"), so that regex would
// silently truncate the embedded JSON mid-string, corrupting the value on
// read AND, more seriously, on write: replaceInlineJSONVar used the same
// pattern to locate the OLD value to overwrite during report merges, so a
// semicolon anywhere in the previous AGG payload could cause a merge to
// splice fresh JSON together with a dangling fragment of the old JSON,
// permanently corrupting the canonical index.html until the next full
// (non-merged) report write. This scanner instead tracks bracket depth and
// string-escaping so it finds the true end of the array/object/string/
// primitive, regardless of any semicolons embedded inside it.
func findJSONValueEnd(data []byte, i int) (int, bool) {
	n := len(data)
	for i < n {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		}
		break
	}
	if i >= n {
		return 0, false
	}
	start := i
	switch data[i] {
	case '[', '{':
		depth := 0
		inString := false
		escaped := false
		for ; i < n; i++ {
			c := data[i]
			if inString {
				switch {
				case escaped:
					escaped = false
				case c == '\\':
					escaped = true
				case c == '"':
					inString = false
				}
				continue
			}
			switch c {
			case '"':
				inString = true
			case '[', '{':
				depth++
			case ']', '}':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
		}
		return 0, false
	case '"':
		escaped := false
		for i++; i < n; i++ {
			c := data[i]
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				return i + 1, true
			}
		}
		return 0, false
	default:
		// A bare primitive (number/true/false/null): ends at the next
		// statement-terminating character.
		for ; i < n; i++ {
			switch data[i] {
			case ';', ',', '\n', '\r', ' ', '\t':
				if i == start {
					return 0, false
				}
				return i, true
			}
		}
		if i == start {
			return 0, false
		}
		return i, true
	}
}

func readInlineJSONVar(path, varName string, fallback interface{}) interface{} {
	b, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	if out, ok := parseInlineJSONVar(b, varName); ok {
		return out
	}
	return fallback
}

// parseInlineJSONVar extracts and decodes `const NAME = <json>` from an HTML
// report's bytes.
func parseInlineJSONVar(b []byte, varName string) (interface{}, bool) {
	loc := inlineVarPrefixPattern(varName).FindIndex(b)
	if loc == nil {
		return nil, false
	}
	end, ok := findJSONValueEnd(b, loc[1])
	if !ok {
		return nil, false
	}
	raw := bytes.TrimSpace(b[loc[1]:end])
	if len(raw) == 0 {
		return nil, false
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, false
	}
	return out, true
}

func (s *apiServer) selectBestReportOutDir() string {
	seen := map[string]bool{}
	candidates := []string{}
	add := func(p string) {
		if strings.TrimSpace(p) == "" {
			return
		}
		abs := s.absPath(p)
		if seen[abs] {
			return
		}
		seen[abs] = true
		candidates = append(candidates, abs)
	}
	add(s.outputDir)
	add(filepath.Join(filepath.Dir(s.absPath(s.configPath)), "outputfiles"))
	if strings.TrimSpace(s.lastCfg) != "" {
		add(filepath.Join(filepath.Dir(s.lastCfg), "outputfiles"))
	}
	best := s.absPath(s.outputDir)
	bestScore := -1
	for _, c := range candidates {
		// Cached, and counts sidecar rows too: large runs keep them in
		// aggregated-data.json, which scored 0 here before.
		score := len(readAggRowsCached(c))
		if score > bestScore {
			best = c
			bestScore = score
		}
	}
	return best
}

func listNCCLogs(logDir string) []map[string]string {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return []map[string]string{}
	}
	out := make([]map[string]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".log") {
			continue
		}
		out = append(out, map[string]string{
			"name": name,
			"path": filepath.Join(logDir, name),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"] < out[j]["name"] })
	return out
}
