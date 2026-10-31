package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// featureFlags turns off optional work. Missing keys stay enabled so an older
// file does not silently disable something the operator never chose.
type featureFlags struct {
	Insights      bool `json:"insights"`
	PCAlerts      bool `json:"pc_alerts"`
	RunComparison bool `json:"run_comparison"`
	FlakyChecks   bool `json:"flaky_checks"`
	SLO           bool `json:"slo"`
	NCCLogIndex   bool `json:"ncc_log_index"`
	PCDiscovery   bool `json:"pc_discovery"`
	// AlertHandling allows acknowledging, resolving and reopening NCC alerts.
	// Off makes findings read-only; existing marks and history stay visible.
	AlertHandling bool `json:"alert_handling"`
	// SelfHeal allows applying remediations from System → Health (POST to
	// /health/diagnostics). Off leaves the checks report-only.
	SelfHeal bool `json:"self_heal"`
	// UpdateChecks allows looking up and installing new releases. Off makes
	// no outbound release lookup, for air-gapped or change-controlled sites.
	UpdateChecks bool `json:"update_checks"`
	// IntroTitle shows the opening title before sign-in. It is exposed on the
	// public /health payload because the page needs it before anyone signs in.
	IntroTitle bool `json:"intro_title"`
}

// featureFlagKeys lists every switch, in the order the API documents them.
var featureFlagKeys = []string{
	"insights", "pc_alerts", "run_comparison", "flaky_checks", "slo", "ncc_log_index", "pc_discovery",
	"alert_handling", "self_heal", "update_checks", "intro_title",
}

func defaultFeatureFlags() featureFlags {
	return featureFlags{
		Insights:      true,
		PCAlerts:      true,
		RunComparison: true,
		FlakyChecks:   true,
		SLO:           true,
		NCCLogIndex:   true,
		PCDiscovery:   true,
		AlertHandling: true,
		SelfHeal:      true,
		UpdateChecks:  true,
		IntroTitle:    true,
	}
}

func knownFeatureFlag(key string) bool {
	for _, k := range featureFlagKeys {
		if k == key {
			return true
		}
	}
	return false
}

// writeFeatureDisabled answers a request for something an administrator
// turned off under Settings → Features.
func writeFeatureDisabled(w http.ResponseWriter, what string) {
	writeJSON(w, http.StatusForbidden, envelope{
		Success:   false,
		Error:     what + " is turned off. An administrator can turn it on under Settings → Features.",
		ErrorCode: "NCC_API_FEATURE_DISABLED",
	})
}

func (f *featureFlags) apply(raw map[string]bool) {
	set := func(dst *bool, key string) {
		if v, ok := raw[key]; ok {
			*dst = v
		}
	}
	set(&f.Insights, "insights")
	set(&f.PCAlerts, "pc_alerts")
	set(&f.RunComparison, "run_comparison")
	set(&f.FlakyChecks, "flaky_checks")
	set(&f.SLO, "slo")
	set(&f.NCCLogIndex, "ncc_log_index")
	set(&f.PCDiscovery, "pc_discovery")
	set(&f.AlertHandling, "alert_handling")
	set(&f.SelfHeal, "self_heal")
	set(&f.UpdateChecks, "update_checks")
	set(&f.IntroTitle, "intro_title")
}

func (s *apiServer) featureFlagsPath() string {
	root := "."
	if s != nil && strings.TrimSpace(s.repoRoot) != "" {
		root = s.repoRoot
	}
	return filepath.Join(root, "outputfiles", "feature-flags.json")
}

func (s *apiServer) loadFeatureFlags() featureFlags {
	flags := defaultFeatureFlags()
	b, err := os.ReadFile(s.featureFlagsPath())
	if err != nil || len(strings.TrimSpace(string(b))) == 0 {
		return flags
	}
	var raw map[string]bool
	if err := json.Unmarshal(b, &raw); err != nil {
		return flags
	}
	flags.apply(raw)
	return flags
}

func (s *apiServer) saveFeatureFlags(flags featureFlags) error {
	path := s.featureFlagsPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(flags, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *apiServer) handleFeatureFlags(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: s.loadFeatureFlags()})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var raw map[string]bool
		if err := decodeJSON(http.MaxBytesReader(w, r.Body, 1<<16), &raw); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if len(raw) == 0 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Send at least one feature to turn on or off."})
			return
		}
		for key := range raw {
			if !knownFeatureFlag(key) {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Unknown feature " + key + ". Use one of: " + strings.Join(featureFlagKeys, ", ") + "."})
				return
			}
		}
		flags := s.loadFeatureFlags()
		flags.apply(raw)
		if err := s.saveFeatureFlags(flags); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not save feature settings: " + err.Error()})
			return
		}
		s.audit(r, "features.update", true, map[string]interface{}{
			"insights":       flags.Insights,
			"pc_alerts":      flags.PCAlerts,
			"run_comparison": flags.RunComparison,
			"flaky_checks":   flags.FlakyChecks,
			"slo":            flags.SLO,
			"ncc_log_index":  flags.NCCLogIndex,
			"pc_discovery":   flags.PCDiscovery,
			"alert_handling": flags.AlertHandling,
			"self_heal":      flags.SelfHeal,
			"update_checks":  flags.UpdateChecks,
			"intro_title":    flags.IntroTitle,
		})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Feature settings saved", Data: flags})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// The three NCC-log views below read every raw log in the log directory
// (7,000 files at 7,000 clusters, twice per report request before caching).
// They are cached per run via cachedLogDirValue; results are shared and
// read-only.

func nccLogsForReport(s *apiServer, flags featureFlags, outDir string) []map[string]string {
	if s == nil || !flags.NCCLogIndex {
		return []map[string]string{}
	}
	logDir := s.absPath(s.logDir)
	v := cachedLogDirValue("list", logDir, outDir, func() interface{} {
		logs := listNCCLogs(logDir)
		// Report data is viewer-readable; don't disclose the absolute log dir.
		// Resolve the directory once: resolving each of 7,000 files (symlink
		// evaluation per path element) took ~30 s on Windows.
		// Same result as displayRelPath per file: the path under the stack
		// root, or just the file name when the log dir is outside it.
		prefix := ""
		root, dir := s.absPath(s.repoRoot), s.absPath(logDir)
		if rel, err := filepath.Rel(root, dir); err == nil && rel != "." && rel != ".." &&
			!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
			prefix = filepath.ToSlash(rel)
		}
		for _, l := range logs {
			name := l["name"]
			if prefix != "" {
				l["path"] = prefix + "/" + name
			} else {
				l["path"] = name
			}
		}
		return logs
	})
	logs, _ := v.([]map[string]string)
	return logs
}

func nccSummaryForReport(s *apiServer, flags featureFlags, outDir string) map[string]int {
	if s == nil || !flags.NCCLogIndex {
		return map[string]int{}
	}
	logDir := s.absPath(s.logDir)
	v := cachedLogDirValue("counts", logDir, outDir, func() interface{} { return parseNCCSummaryCounts(logDir) })
	counts, _ := v.(map[string]int)
	return counts
}

func nccClusterSummaryForReport(s *apiServer, flags featureFlags, access clusterAccess, outDir string) interface{} {
	if s == nil || !flags.NCCLogIndex {
		return []interface{}{}
	}
	logDir := s.absPath(s.logDir)
	v := cachedLogDirValue("clusters", logDir, outDir, func() interface{} { return parseNCCClusterSummary(logDir) })
	return deepFilterClusters(v, access)
}

func trendsForReport(flags featureFlags, outDir string) []trendPoint {
	if !flags.Insights {
		return []trendPoint{}
	}
	return collectTrendPoints(outDir, 30)
}
