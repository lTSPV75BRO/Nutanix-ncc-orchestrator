package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Health & repair: the endpoints behind the umbrella page that groups the
// doctor checks (diagnostics.go), automatic healing, migrations, updates and
// backups. Everything here is admin-only (routeMinRoleFor).

// healHistoryEntry mirrors the runner's selfheal-history.jsonl lines.
type healHistoryEntry struct {
	At       string `json:"at"`
	ID       string `json:"id"`
	Title    string `json:"title"`
	Category string `json:"category"`
	FixMsg   string `json:"fix_message"`
	Verified *bool  `json:"verified,omitempty"`
	Status   string `json:"status"`
}

// recurringFixThreshold matches the runner: this many fixes of one check in
// 24 hours means something keeps undoing the repair.
const recurringFixThreshold = 3

func (s *apiServer) healHistoryPath() string {
	return filepath.Join(s.maintenanceInstallDir(), "logs", "selfheal-history.jsonl")
}

func readHealHistoryFile(path string) []healHistoryEntry {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []healHistoryEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var e healHistoryEntry
		if json.Unmarshal(sc.Bytes(), &e) == nil && e.ID != "" {
			out = append(out, e)
		}
	}
	return out
}

// GET /api/v1/health/history?limit=N — fixes applied by any doctor run (the
// periodic loop, Heal now, or the CLI), newest first, with per-check counts.
func (s *apiServer) handleHealHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	limit := 100
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 1000 {
		limit = n
	}
	all := readHealHistoryFile(s.healHistoryPath())
	now := time.Now()
	type checkStats struct {
		ID        string `json:"id"`
		Title     string `json:"title"`
		Fixes24h  int    `json:"fixes_24h"`
		Fixes7d   int    `json:"fixes_7d"`
		Last      string `json:"last"`
		Recurring bool   `json:"recurring"`
	}
	stats := map[string]*checkStats{}
	unverified := 0
	for _, e := range all {
		at, err := time.Parse(time.RFC3339, e.At)
		if err != nil {
			continue
		}
		st := stats[e.ID]
		if st == nil {
			st = &checkStats{ID: e.ID, Title: e.Title}
			stats[e.ID] = st
		}
		if now.Sub(at) <= 7*24*time.Hour {
			st.Fixes7d++
			if e.Verified != nil && !*e.Verified {
				unverified++
			}
		}
		if now.Sub(at) <= 24*time.Hour {
			st.Fixes24h++
		}
		st.Last = e.At
	}
	byCheck := make([]checkStats, 0, len(stats))
	for _, st := range stats {
		st.Recurring = st.Fixes24h >= recurringFixThreshold
		byCheck = append(byCheck, *st)
	}
	sort.Slice(byCheck, func(i, j int) bool {
		if byCheck[i].Fixes7d != byCheck[j].Fixes7d {
			return byCheck[i].Fixes7d > byCheck[j].Fixes7d
		}
		return byCheck[i].ID < byCheck[j].ID
	})
	entries := make([]healHistoryEntry, 0, limit)
	for i := len(all) - 1; i >= 0 && len(entries) < limit; i-- {
		entries = append(entries, all[i])
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"entries":         entries,
		"total":           len(all),
		"by_check":        byCheck,
		"unverified_7d":   unverified,
		"recurring_after": recurringFixThreshold,
	}})
}

// autoHealer is one automatic healing mechanism and whether it is active.
type autoHealer struct {
	ID          string                 `json:"id"`
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	State       string                 `json:"state"` // on | off | partial
	Detail      string                 `json:"detail,omitempty"`
	HowToChange string                 `json:"how_to_change,omitempty"`
	Stats       map[string]interface{} `json:"stats,omitempty"`
}

// runnerAutomation reads auto / automation-level from the active config in
// either layout (the flat key wins, as in the runner).
func (s *apiServer) runnerAutomation() (bool, string) {
	raw, err := os.ReadFile(s.absPath(s.configPath))
	if err != nil {
		return false, ""
	}
	var doc map[string]interface{}
	if yaml.Unmarshal(raw, &doc) != nil {
		return false, ""
	}
	nested := func(path ...string) (interface{}, bool) {
		var cur interface{} = doc
		for _, p := range path {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return nil, false
			}
			if cur, ok = m[p]; !ok {
				return nil, false
			}
		}
		return cur, true
	}
	enabled, ok := doc["auto"]
	if !ok {
		enabled, _ = nested("runner", "automation", "enabled")
	}
	level, ok := doc["automation-level"]
	if !ok {
		level, _ = nested("runner", "automation", "level")
	}
	on := strings.EqualFold(fmt.Sprint(enabled), "true")
	lv := strings.TrimSpace(fmt.Sprint(level))
	if level == nil || lv == "" {
		lv = "safe-fix"
	}
	return on, lv
}

// supervisorPID is the live `v2-start --self-heal` supervisor, from its PID
// file under the install dir.
func (s *apiServer) supervisorPID() (int, bool) {
	b, err := os.ReadFile(filepath.Join(s.maintenanceInstallDir(), "run", "v2-supervisor.pid"))
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 || !processAlive(pid) {
		return 0, false
	}
	return pid, true
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

// GET /api/v1/health/autoheal — every automatic healing mechanism, whether it
// is on, and what it has done since the server started.
func (s *apiServer) handleAutoHeal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	flags := s.loadFeatureFlags()
	loop := autoHealer{
		ID:          "selfheal-loop",
		Title:       "Background health checks",
		Description: "Runs every doctor check on a timer, alerts when one starts failing, and (with auto-fix) applies the safe fixes without anyone clicking Heal.",
		State:       onOff(s.selfHealInterval > 0),
		HowToChange: "Start the API server with --self-heal-interval (e.g. 1h) and --self-heal-auto-fix.",
		Stats: map[string]interface{}{
			"interval": s.selfHealInterval.String(),
			"auto_fix": s.selfHealAutoFix,
			"runs":     s.selfHealRunsTotal.Load(),
			"fixes":    s.selfHealFixesTotal.Load(),
		},
	}
	switch {
	case s.selfHealInterval <= 0:
		loop.Detail = "Off: checks run only when this page is opened or Heal is clicked."
	case s.selfHealAutoFix:
		loop.Detail = fmt.Sprintf("Every %s, applying safe fixes.", s.selfHealInterval)
	default:
		loop.State = "partial"
		loop.Detail = fmt.Sprintf("Every %s, report only: fixes wait for Heal.", s.selfHealInterval)
	}
	if rep := s.cachedSelfHeal(); rep != nil {
		loop.Stats["last_run"] = rep.RanAt.Format(time.RFC3339)
		loop.Stats["last_summary"] = rep.Summary
	}

	retry := autoHealer{
		ID:          "run-auto-retry",
		Title:       "Run auto-retry",
		Description: "When a run fails for a recoverable reason (rate limit, timeout, network), retries it once with a safer setting such as lower parallelism or a longer timeout.",
		State:       onOff(!s.runAutoRetryDisabled),
		HowToChange: "Turned off with the API server flag --disable-run-auto-retry.",
		Stats:       map[string]interface{}{"retries": s.runAutoRetriesTotal.Load()},
	}
	auto, level := s.runnerAutomation()
	runner := autoHealer{
		ID:          "runner-automation",
		Title:       "Connection self-healing in runs",
		Description: "When a run's connection check fails, the runner applies fixes for its automation level (advisory, safe-fix or full-auto) and tries again.",
		State:       onOff(auto),
		Detail:      "Level: " + level,
		HowToChange: "Clusters & config → Run behavior → Self-healing.",
	}
	actions := autoHealer{
		ID:          "heal-actions",
		Title:       "Heal buttons",
		Description: "Lets admins apply fixes from this page. Off leaves every check report-only.",
		State:       onOff(flags.SelfHeal),
		HowToChange: "Settings → Features → Self-heal actions.",
	}
	restore := autoHealer{
		ID:          "post-restore",
		Title:       "Repair after restore",
		Description: "After a backup is restored, re-anchors output paths, rebuilds missing schedule files and recreates a missing HTTPS certificate before the stack restarts.",
		State:       "on",
	}
	supervisor := autoHealer{
		ID:          "supervisor",
		Title:       "Service supervisor",
		Description: "Restarts the API and UI servers when they crash or stop answering health probes, with backoff and a restart budget.",
		HowToChange: "Start the stack with ncc-orchestrator v2-start --detach --self-heal.",
	}
	if pid, ok := s.supervisorPID(); ok {
		supervisor.State = "on"
		supervisor.Detail = fmt.Sprintf("Running (pid %d).", pid)
	} else {
		supervisor.State = "off"
		supervisor.Detail = "Not running: a crashed or hung server stays down until someone restarts it."
	}
	if s.capabilities.Kubernetes {
		restore.State = "off"
		restore.Detail = "Kubernetes controllers own restarts and certificates."
		supervisor.State = "on"
		supervisor.Detail = "Kubernetes restarts failed pods."
		supervisor.HowToChange = ""
	}
	healers := []autoHealer{loop, supervisor, retry, runner, actions, restore}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"healers": healers}})
}

// configMigrationResult mirrors `ncc-orchestrator migrate-config --json`.
type configMigrationResult struct {
	ConfigPath string `json:"config_path"`
	Layout     string `json:"layout"`
	Changed    bool   `json:"changed"`
	Applied    bool   `json:"applied"`
	Moved      []struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"moved,omitempty"`
	Duplicates []map[string]interface{} `json:"duplicates,omitempty"`
	Removed    []string                 `json:"removed,omitempty"`
	KeptFlat   []string                 `json:"kept_flat,omitempty"`
	Backup     string                   `json:"backup,omitempty"`
	Content    string                   `json:"content,omitempty"`
	Message    string                   `json:"message"`
}

func (s *apiServer) runConfigMigration(dryRun bool) (configMigrationResult, error) {
	args := []string{"migrate-config", "--json", "--config", s.absPath(s.configPath)}
	if dryRun {
		args = append(args, "--dry-run")
	}
	out, runErr := s.runOrchestrator(args, 30*time.Second)
	// The JSON is on stdout; an error line from cobra may follow it.
	var payload struct {
		Result configMigrationResult `json:"result"`
		Error  string                `json:"error"`
	}
	start, end := strings.Index(out, "{"), strings.LastIndex(out, "}")
	if start < 0 || end < start || json.Unmarshal([]byte(out[start:end+1]), &payload) != nil {
		if runErr != nil {
			return configMigrationResult{}, fmt.Errorf("migrate-config failed: %s", strings.TrimSpace(out))
		}
		return configMigrationResult{}, fmt.Errorf("could not read migrate-config output")
	}
	if payload.Error != "" {
		return payload.Result, fmt.Errorf("%s", payload.Error)
	}
	payload.Result.ConfigPath = s.displayRelPath(s.configPath)
	if payload.Result.Backup != "" {
		payload.Result.Backup = s.displayRelPath(payload.Result.Backup)
	}
	return payload.Result, nil
}

// GET  /api/v1/health/migrations/config — preview converting the active config
// to the nested layout (nothing is written).
// POST /api/v1/health/migrations/config — apply it; the original is kept as a
// .bak next to the file.
func (s *apiServer) handleConfigMigration(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		res, err := s.runConfigMigration(true)
		if err != nil {
			writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"result": res, "error": err.Error()}})
			return
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"result": res}})
	case http.MethodPost:
		if s.hasInFlightRuns() {
			writeJSON(w, http.StatusConflict, envelope{Success: false, Error: "a run is in progress; migrate the config when it finishes"})
			return
		}
		res, err := s.runConfigMigration(false)
		s.audit(r, "health.migrations.config", err == nil, map[string]interface{}{
			"changed": res.Changed, "moved": len(res.Moved), "removed": len(res.Removed), "backup": res.Backup,
		})
		if err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, envelope{Success: false, Error: err.Error()})
			return
		}
		res.Content = ""
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: res.Message, Data: map[string]interface{}{"result": res}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}
