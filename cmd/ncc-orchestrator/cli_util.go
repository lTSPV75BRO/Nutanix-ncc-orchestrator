package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// ==================== Utility Functions ====================

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func splitCSVValue(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch values := v.(type) {
	case []string:
		out := make([]string, 0, len(values))
		for _, value := range values {
			out = append(out, splitCSV(value)...)
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(values))
		for _, value := range values {
			out = append(out, splitCSV(fmt.Sprint(value))...)
		}
		return out
	default:
		return splitCSV(fmt.Sprint(v))
	}
}

func normalizeClusterSourceMode(s string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(s))
	if mode == "" {
		return defaultClusterSourceMode, nil
	}
	switch mode {
	case "clusters", "pc":
		return mode, nil
	default:
		return "", fmt.Errorf("cluster-source-mode must be one of clusters or pc")
	}
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr := make([]int, len(b)+1)
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = minInt(del, minInt(ins, sub))
		}
		prev = curr
	}
	return prev[len(b)]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func closestToken(input string, candidates []string) string {
	in := strings.ToLower(strings.TrimSpace(input))
	if in == "" || len(candidates) == 0 {
		return ""
	}
	best := ""
	bestDist := 1 << 30
	for _, c := range candidates {
		clean := strings.ToLower(strings.TrimSpace(c))
		if clean == "" {
			continue
		}
		if clean == in {
			return c
		}
		d := levenshteinDistance(in, clean)
		if d < bestDist {
			bestDist = d
			best = c
		}
	}
	// Keep suggestions conservative to avoid noisy/wrong hints.
	if bestDist > 3 && !strings.HasPrefix(strings.ToLower(best), in) {
		return ""
	}
	return best
}

func knownCommandNames(cmd *cobra.Command) []string {
	names := []string{}
	for _, c := range cmd.Commands() {
		if !c.IsAvailableCommand() || c.Hidden {
			continue
		}
		names = append(names, c.Name())
	}
	return names
}

func knownFlagNames(cmd *cobra.Command) []string {
	set := map[string]struct{}{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		set["--"+f.Name] = struct{}{}
	})
	cmd.InheritedFlags().VisitAll(func(f *pflag.Flag) {
		set["--"+f.Name] = struct{}{}
	})
	cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		set["--"+f.Name] = struct{}{}
	})
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func extractQuotedValue(msg string) string {
	start := strings.Index(msg, "\"")
	if start < 0 {
		return ""
	}
	rest := msg[start+1:]
	end := strings.Index(rest, "\"")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func humanizeCLIError(root *cobra.Command, argv []string, err error) string {
	msg := strings.TrimSpace(err.Error())
	lower := strings.ToLower(msg)
	active := root
	if len(argv) > 0 {
		if found, _, findErr := root.Find(argv); findErr == nil && found != nil {
			active = found
		}
	}
	helpCmd := "ncc-orchestrator --help"
	if active != nil && active != root {
		helpCmd = active.CommandPath() + " --help"
	}
	if strings.Contains(lower, "unknown command") {
		bad := extractQuotedValue(msg)
		suggestion := closestToken(bad, knownCommandNames(root))
		if suggestion != "" {
			return fmt.Sprintf("unknown command %q. Did you mean `%s`?\nHint: run `%s` for available commands.", bad, suggestion, helpCmd)
		}
		return fmt.Sprintf("%s\nHint: run `%s` for available commands.", msg, helpCmd)
	}
	if strings.Contains(lower, "unknown flag") {
		bad := extractQuotedValue(msg)
		if bad == "" {
			if idx := strings.Index(lower, "unknown flag:"); idx >= 0 {
				raw := strings.TrimSpace(msg[idx+len("unknown flag:"):])
				if raw != "" {
					bad = strings.Fields(raw)[0]
				}
			}
		}
		suggestion := closestToken(bad, knownFlagNames(active))
		if strings.EqualFold(strings.TrimSpace(suggestion), strings.TrimSpace(bad)) {
			suggestion = ""
		}
		if suggestion != "" {
			return fmt.Sprintf("%s\nDid you mean `%s`?\nHint: run `%s` to view supported flags.", msg, suggestion, helpCmd)
		}
		return fmt.Sprintf("%s\nHint: run `%s` to view supported flags.", msg, helpCmd)
	}
	return msg
}

func readLineValuesFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	seen := make(map[string]bool)
	for i, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if err := validateClusterAddress(trimmed); err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out, nil
}

func loadExcludeAlertTitlesFromFile(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(b), "\n")
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		v := strings.TrimSpace(ln)
		if v == "" || strings.HasPrefix(v, "#") {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

func mergeUniqueStrings(items ...[]string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0)
	for _, arr := range items {
		for _, raw := range arr {
			v := strings.TrimSpace(raw)
			if v == "" {
				continue
			}
			k := strings.ToLower(v)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, v)
		}
	}
	return out
}
