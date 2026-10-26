package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Setup checklist ("onboarding").
//
// The old first-time setup was a browser-local modal: its progress lived in
// localStorage, so every new browser (or user) saw "New install detected" on
// long-running installs, viewers were sent to Settings tabs they cannot open,
// its steps were manual "mark as done" boxes unrelated to the real state, and
// its restore detection looked for the wrong audit action. The checklist is
// now derived on the server from the install's actual state, so it is the same
// for everyone, completes itself as the work gets done, and an admin's "hide"
// applies to the whole install.

// onboardingStep is one checklist item. Action names the Settings tab where
// the step is done; AdminOnly marks steps operators cannot complete themselves.
type onboardingStep struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Done        bool   `json:"done"`
	Optional    bool   `json:"optional,omitempty"`
	Detail      string `json:"detail,omitempty"`
	Tab         string `json:"tab"`
	ActionLabel string `json:"action_label"`
	AdminOnly   bool   `json:"admin_only,omitempty"`
}

type onboardingStatus struct {
	Steps         []onboardingStep `json:"steps"`
	RequiredDone  int              `json:"required_done"`
	RequiredTotal int              `json:"required_total"`
	Completed     bool             `json:"completed"`
	Dismissed     bool             `json:"dismissed"`
	DismissedAt   string           `json:"dismissed_at,omitempty"`
	DismissedBy   string           `json:"dismissed_by,omitempty"`
}

// onboardingState is the persisted part: only the admin's dismissal.
type onboardingState struct {
	Dismissed   bool   `json:"dismissed"`
	DismissedAt string `json:"dismissed_at,omitempty"`
	DismissedBy string `json:"dismissed_by,omitempty"`
}

func (s *apiServer) onboardingStatePath() string {
	return s.absPath(".ncc-api-onboarding.json")
}

func (s *apiServer) loadOnboardingState() onboardingState {
	var st onboardingState
	b, err := os.ReadFile(s.onboardingStatePath())
	if err != nil {
		return st
	}
	_ = json.Unmarshal(b, &st)
	return st
}

func (s *apiServer) saveOnboardingState(st onboardingState) error {
	path := s.onboardingStatePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(b, '\n'), 0o644)
}

// isPlaceholderTarget reports sample values shipped in example/dummy configs
// (REPLACE_WITH_CLUSTER_IP and friends), which must not count as configured.
func isPlaceholderTarget(v string) bool {
	v = strings.ToUpper(strings.TrimSpace(v))
	return v == "" || strings.Contains(v, "REPLACE_") || strings.Contains(v, "<") || v == "CHANGEME"
}

func realTargets(values []string) []string {
	var out []string
	for _, v := range values {
		if !isPlaceholderTarget(v) {
			out = append(out, strings.TrimSpace(v))
		}
	}
	return out
}

// onboardingClusterStep inspects the configured cluster source.
func (s *apiServer) onboardingClusterStep() onboardingStep {
	step := onboardingStep{
		ID:          "clusters",
		Title:       "Connect your clusters",
		Description: "Add the Prism Element clusters to check, or a Prism Central to discover them, and the credentials to use.",
		Tab:         "config",
		ActionLabel: "Open configuration",
		AdminOnly:   true,
	}
	cfg, err := s.loadRawConfigMap()
	if err != nil {
		step.Detail = "The configuration file could not be read: " + redactPaths(err.Error())
		return step
	}
	clusters := realTargets(readMapCSV(cfg, "clusters"))
	pcs := realTargets(append(readMapCSV(cfg, "pcs"), readMapString(cfg, "prism-central-url", "")))
	files := realTargets([]string{readMapString(cfg, "clusters-file", ""), readMapString(cfg, "pcs-file", "")})
	switch {
	case len(clusters) > 0:
		step.Done = true
		step.Detail = fmt.Sprintf("%d cluster(s) configured.", len(clusters))
	case len(pcs) > 0:
		step.Done = true
		step.Detail = fmt.Sprintf("%d Prism Central source(s) configured.", len(pcs))
	case len(files) > 0:
		step.Done = true
		step.Detail = "Clusters are read from a clusters file."
	default:
		step.Detail = "No clusters yet — the configuration still has the sample value."
	}
	if step.Done && strings.TrimSpace(readMapString(cfg, "username", "")) == "" {
		step.Done = false
		step.Detail += " Add the Prism username (and password or secret) to use."
	}
	return step
}

// onboardingRunStep reports whether a run has produced results.
func (s *apiServer) onboardingRunStep() onboardingStep {
	step := onboardingStep{
		ID:          "first_run",
		Title:       "Run NCC for the first time",
		Description: "Run a preflight check, then trigger a run and review the results on the Dashboard.",
		Tab:         "runs",
		ActionLabel: "Open runs",
	}
	v, ok := s.latestRunSummaryView()
	switch {
	case !ok:
		step.Detail = "No completed run yet."
	case v.ClustersOK > 0:
		step.Done = true
		step.Detail = fmt.Sprintf("Last run checked %d cluster(s)", v.ClustersOK)
		if v.ClustersFailed > 0 {
			step.Detail += fmt.Sprintf("; %d failed", v.ClustersFailed)
		}
		step.Detail += "."
	default:
		// A run that reached no cluster usually means wrong credentials or
		// an unreachable address; it should not tick the step.
		step.Detail = fmt.Sprintf("The last run did not complete on any cluster (%d failed). Check addresses and credentials with a preflight check.", v.ClustersFailed)
	}
	return step
}

func (s *apiServer) onboardingScheduleStep() onboardingStep {
	step := onboardingStep{
		ID:          "schedule",
		Title:       "Schedule recurring runs",
		Description: "Run NCC automatically so findings stay current.",
		Tab:         "schedule",
		ActionLabel: "Open schedule",
	}
	st, err := s.loadSchedule()
	if err == nil && st.Action == "create" && (strings.TrimSpace(st.Cron) != "" || strings.TrimSpace(st.Every) != "") {
		step.Done = true
		when := strings.TrimSpace(st.Every)
		if when != "" {
			when = "every " + when
		} else {
			when = "cron " + strings.TrimSpace(st.Cron)
		}
		step.Detail = "Runs are scheduled " + when + "."
	} else {
		step.Detail = "No recurring schedule yet."
	}
	return step
}

func (s *apiServer) onboardingNotificationStep() onboardingStep {
	step := onboardingStep{
		ID:          "notifications",
		Title:       "Get notified",
		Description: "Send run results and failures to email, Slack, or a webhook.",
		Tab:         "notifications",
		ActionLabel: "Open notifications",
		Optional:    true,
		AdminOnly:   true,
	}
	st, err := s.loadNotifications()
	if err != nil {
		step.Detail = "Notification settings could not be read."
		return step
	}
	var channels []string
	if st.Email.Enabled {
		channels = append(channels, "email")
	}
	if st.Slack.Enabled {
		channels = append(channels, "Slack")
	}
	if st.Webhook.Enabled {
		channels = append(channels, "webhook")
	}
	if !st.Enabled || len(channels) == 0 {
		step.Detail = "No notification channel is on."
		return step
	}
	step.Done = true
	step.Detail = "On: " + strings.Join(channels, ", ") + "."
	delivered := false
	for _, d := range st.LastDelivery {
		if d.TotalSuccess > 0 {
			delivered = true
		}
	}
	if !delivered {
		step.Detail += " Send a test to confirm delivery."
	}
	return step
}

func (s *apiServer) onboardingBackupStep() onboardingStep {
	step := onboardingStep{
		ID:          "backups",
		Title:       "Protect your setup with backups",
		Description: "Turn on scheduled, encrypted backups of configuration, accounts, and history.",
		Tab:         "maintenance",
		ActionLabel: "Open backups",
		AdminOnly:   true,
	}
	sched, err := s.loadBackupSchedule()
	entries, _ := s.listBackupEntries()
	switch {
	case err == nil && sched.Enabled:
		step.Done = true
		step.Detail = fmt.Sprintf("Scheduled every %s; %d backup(s) on disk.", strings.TrimSpace(sched.Every), len(entries))
	case len(entries) > 0:
		step.Detail = fmt.Sprintf("%d backup(s) on disk, but no schedule. Turn on scheduled backups.", len(entries))
	default:
		step.Detail = "No backups yet."
	}
	return step
}

func (s *apiServer) onboardingAccessStep() onboardingStep {
	step := onboardingStep{
		ID:          "access",
		Title:       "Invite your team",
		Description: "Add accounts, or connect Active Directory or SSO, so people sign in as themselves.",
		Tab:         "access",
		ActionLabel: "Open access",
		Optional:    true,
		AdminOnly:   true,
	}
	switch {
	case s.samlIsEnabled() || s.ldapIsEnabled():
		step.Done = true
		step.Detail = "Directory or SSO sign-in is configured."
	case s.users != nil && s.users.count() > 1:
		step.Done = true
		step.Detail = fmt.Sprintf("%d local accounts.", s.users.count())
	case s.users == nil:
		step.Detail = "Local accounts are disabled on this server."
	default:
		step.Detail = "Only the built-in admin account exists."
	}
	return step
}

func (s *apiServer) onboardingStatus() onboardingStatus {
	steps := []onboardingStep{
		s.onboardingClusterStep(),
		s.onboardingRunStep(),
		s.onboardingScheduleStep(),
		s.onboardingBackupStep(),
		s.onboardingNotificationStep(),
		s.onboardingAccessStep(),
	}
	out := onboardingStatus{Steps: steps}
	for _, st := range steps {
		if st.Optional {
			continue
		}
		out.RequiredTotal++
		if st.Done {
			out.RequiredDone++
		}
	}
	out.Completed = out.RequiredDone == out.RequiredTotal
	persisted := s.loadOnboardingState()
	out.Dismissed = persisted.Dismissed
	out.DismissedAt = persisted.DismissedAt
	out.DismissedBy = persisted.DismissedBy
	return out
}

// handleOnboarding serves GET (operator+) and PUT {"dismissed": bool} (admin).
func (s *apiServer) handleOnboarding(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: s.onboardingStatus()})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var body struct {
			Dismissed *bool `json:"dismissed"`
		}
		if err := decodeJSON(http.MaxBytesReader(w, r.Body, 4096), &body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if body.Dismissed == nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: errors.New(`send {"dismissed": true} or {"dismissed": false}`).Error()})
			return
		}
		st := onboardingState{Dismissed: *body.Dismissed}
		if st.Dismissed {
			st.DismissedAt = time.Now().UTC().Format(time.RFC3339)
			if p, ok := principalFromContext(r.Context()); ok {
				st.DismissedBy = p.subject
			}
		}
		if err := s.saveOnboardingState(st); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not save the setup checklist state: " + redactPaths(err.Error())})
			return
		}
		s.audit(r, "onboarding.update", true, map[string]interface{}{"dismissed": st.Dismissed})
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: s.onboardingStatus()})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}
