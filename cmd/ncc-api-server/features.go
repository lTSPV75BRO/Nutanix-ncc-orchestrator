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
	}
}

func knownFeatureFlag(key string) bool {
	switch key {
	case "insights", "pc_alerts", "run_comparison", "flaky_checks", "slo", "ncc_log_index", "pc_discovery":
		return true
	default:
		return false
	}
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
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Unknown feature " + key + ". Use insights, pc_alerts, run_comparison, flaky_checks, slo, ncc_log_index, or pc_discovery."})
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
		})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "Feature settings saved", Data: flags})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func nccLogsForReport(s *apiServer, flags featureFlags) []map[string]string {
	if s == nil || !flags.NCCLogIndex {
		return []map[string]string{}
	}
	return listNCCLogs(s.absPath(s.logDir))
}

func nccSummaryForReport(s *apiServer, flags featureFlags) map[string]int {
	if s == nil || !flags.NCCLogIndex {
		return map[string]int{}
	}
	return parseNCCSummaryCounts(s.absPath(s.logDir))
}

func nccClusterSummaryForReport(s *apiServer, flags featureFlags, access clusterAccess) interface{} {
	if s == nil || !flags.NCCLogIndex {
		return []interface{}{}
	}
	return deepFilterClusters(parseNCCClusterSummary(s.absPath(s.logDir)), access)
}

func trendsForReport(flags featureFlags, outDir string) []trendPoint {
	if !flags.Insights {
		return []trendPoint{}
	}
	return collectTrendPoints(outDir, 30)
}
