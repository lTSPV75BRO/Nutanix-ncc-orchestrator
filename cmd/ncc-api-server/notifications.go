package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// urlRedactPattern matches http(s) URLs so they can be stripped from
// client-facing error strings (a Slack/webhook URL is a bearer secret).
var urlRedactPattern = regexp.MustCompile(`https?://[^\s"']+`)

// redactURLs replaces any embedded http(s) URL with a placeholder so a
// transport error surfaced to a non-admin caller cannot leak a secret endpoint.
func redactURLs(s string) string {
	return urlRedactPattern.ReplaceAllString(s, "[redacted-url]")
}

type notificationEvents struct {
	RunSuccess       bool `json:"run_success"`
	RunFailure       bool `json:"run_failure"`
	PolicyViolations bool `json:"policy_violations"`
}

type slackNotificationConfig struct {
	Enabled    bool   `json:"enabled"`
	WebhookURL string `json:"webhook_url"`
	Channel    string `json:"channel,omitempty"`
	Username   string `json:"username,omitempty"`
}

type webhookNotificationConfig struct {
	Enabled bool   `json:"enabled"`
	URL     string `json:"url"`
}

type emailNotificationConfig struct {
	Enabled  bool   `json:"enabled"`
	SMTPHost string `json:"smtp_host"`
	SMTPPort int    `json:"smtp_port"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	From     string `json:"from"`
	To       string `json:"to"`
}

type notificationState struct {
	Enabled      bool                                  `json:"enabled"`
	Events       notificationEvents                    `json:"events"`
	Slack        slackNotificationConfig               `json:"slack"`
	Webhook      webhookNotificationConfig             `json:"webhook"`
	Email        emailNotificationConfig               `json:"email"`
	Quiet        quietHoursConfig                      `json:"quiet,omitempty"`
	Maintenance  []maintenanceWindow                   `json:"maintenance,omitempty"`
	Throttle     notificationThrottle                  `json:"throttle,omitempty"`
	Digest       digestConfig                          `json:"digest,omitempty"`
	LastDelivery map[string]notificationDeliveryStatus `json:"last_delivery,omitempty"`
	UpdatedAt    string                                `json:"updated_at"`
}

// quietHoursConfig suppresses non-urgent notifications during a recurring daily
// window (e.g. overnight). Failures can be exempted so pages still get through.
type quietHoursConfig struct {
	Enabled       bool   `json:"enabled"`
	Start         string `json:"start"`          // "HH:MM" (24h, local to Timezone)
	End           string `json:"end"`            // "HH:MM"; may wrap past midnight
	Timezone      string `json:"timezone"`       // IANA name; empty = UTC
	AllowFailures bool   `json:"allow_failures"` // failures bypass quiet hours when true
}

// maintenanceWindow suppresses ALL notifications (and in-process scheduled
// backups) during an explicit absolute time range.
type maintenanceWindow struct {
	Start string `json:"start"` // RFC3339
	End   string `json:"end"`   // RFC3339
	Note  string `json:"note,omitempty"`
}

// notificationThrottle prevents alert storms: DedupWindowSec collapses repeats
// of the same event; MinIntervalSec enforces a global floor between any sends.
type notificationThrottle struct {
	DedupWindowSec int `json:"dedup_window_sec"`
	MinIntervalSec int `json:"min_interval_sec"`
}

// digestConfig drives the scheduled summary email of the latest run's health.
type digestConfig struct {
	Enabled    bool   `json:"enabled"`
	Every      string `json:"every"` // 6h / 24h / 7d (see parseEveryDuration)
	LastSentAt string `json:"last_sent_at,omitempty"`
}

type notificationDeliveryStatus struct {
	LastAttemptAt string `json:"last_attempt_at,omitempty"`
	LastSuccessAt string `json:"last_success_at,omitempty"`
	LastEvent     string `json:"last_event,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	Success       bool   `json:"success"`
	TotalSuccess  int    `json:"total_success"`
	TotalFailure  int    `json:"total_failure"`
}

type notificationTestRequest struct {
	Channel string `json:"channel,omitempty"`
}

func defaultNotificationState() notificationState {
	return notificationState{
		Enabled: true,
		Events: notificationEvents{
			RunSuccess:       false,
			RunFailure:       true,
			PolicyViolations: true,
		},
		Slack:        slackNotificationConfig{},
		Webhook:      webhookNotificationConfig{},
		Email:        emailNotificationConfig{SMTPPort: 587},
		LastDelivery: map[string]notificationDeliveryStatus{},
	}
}

func (s *apiServer) handleNotifications(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		state, err := s.loadNotifications()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		state.Email.Password = ""
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: state})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req notificationState
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		existing, _ := s.loadNotifications()
		if strings.TrimSpace(req.Email.Password) == "" {
			req.Email.Password = existing.Email.Password
		}
		if len(req.LastDelivery) == 0 {
			req.LastDelivery = existing.LastDelivery
		}
		if len(req.Digest.LastSentAt) == 0 {
			req.Digest.LastSentAt = existing.Digest.LastSentAt
		}
		req.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if err := validateNotificationState(req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := validateNotificationControls(req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := s.saveNotifications(req); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		req.Email.Password = ""
		s.audit(r, "settings.notifications.update", true, map[string]interface{}{"enabled": req.Enabled})
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "notifications updated", Data: req})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleNotificationsTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
		return
	}
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req notificationTestRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	channel := strings.ToLower(strings.TrimSpace(req.Channel))
	st, err := s.loadNotifications()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
		return
	}
	target, err := requestedChannels(channel)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	if err := validateNotificationStateForChannels(st, target); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	details := map[string]interface{}{
		"manual_test": true,
		"channel":     defaultIfEmpty(channel, "all"),
	}
	if err := s.dispatchNotifications(&st, "manual_test", "NCC notifications test", details, target); err != nil {
		// Operators (not just admins) may send a test notification, but the
		// underlying transport error can embed the configured webhook/SMTP URL
		// (a bearer secret for Slack/webhook). Redact URLs from the response;
		// the full error is already logged server-side in dispatchNotifications.
		writeJSON(w, http.StatusBadGateway, envelope{Success: false, Error: redactURLs(err.Error()), Data: map[string]interface{}{
			"last_delivery": st.LastDelivery,
		}})
		return
	}
	s.audit(r, "settings.notifications.test", true, map[string]interface{}{"channel": defaultIfEmpty(channel, "all")})
	st.Email.Password = ""
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "test notification sent", Data: map[string]interface{}{
		"channel":       defaultIfEmpty(channel, "all"),
		"last_delivery": st.LastDelivery,
	}})
}

func validateNotificationState(st notificationState) error {
	if st.Slack.Enabled {
		if strings.TrimSpace(st.Slack.WebhookURL) == "" {
			return errors.New("slack webhook_url is required when slack is enabled")
		}
		if _, err := url.ParseRequestURI(strings.TrimSpace(st.Slack.WebhookURL)); err != nil {
			return errors.New("invalid slack webhook_url")
		}
	}
	if st.Webhook.Enabled {
		if strings.TrimSpace(st.Webhook.URL) == "" {
			return errors.New("webhook url is required when webhook is enabled")
		}
		if _, err := url.ParseRequestURI(strings.TrimSpace(st.Webhook.URL)); err != nil {
			return errors.New("invalid webhook url")
		}
	}
	if st.Email.Enabled {
		if strings.TrimSpace(st.Email.SMTPHost) == "" || st.Email.SMTPPort <= 0 {
			return errors.New("email smtp_host and smtp_port are required")
		}
		if strings.TrimSpace(st.Email.From) == "" || strings.TrimSpace(st.Email.To) == "" {
			return errors.New("email from and to are required")
		}
	}
	return nil
}

func validateNotificationStateForChannels(st notificationState, channels map[string]bool) error {
	if channels["slack"] && !st.Slack.Enabled {
		return errors.New("slack is not enabled")
	}
	if channels["slack"] && st.Slack.Enabled {
		if strings.TrimSpace(st.Slack.WebhookURL) == "" {
			return errors.New("slack webhook_url is required when slack is enabled")
		}
		if _, err := url.ParseRequestURI(strings.TrimSpace(st.Slack.WebhookURL)); err != nil {
			return errors.New("invalid slack webhook_url")
		}
	}
	if channels["webhook"] && !st.Webhook.Enabled {
		return errors.New("webhook is not enabled")
	}
	if channels["webhook"] && st.Webhook.Enabled {
		if strings.TrimSpace(st.Webhook.URL) == "" {
			return errors.New("webhook url is required when webhook is enabled")
		}
		if _, err := url.ParseRequestURI(strings.TrimSpace(st.Webhook.URL)); err != nil {
			return errors.New("invalid webhook url")
		}
	}
	if channels["email"] && !st.Email.Enabled {
		return errors.New("email is not enabled")
	}
	if channels["email"] && st.Email.Enabled {
		if strings.TrimSpace(st.Email.SMTPHost) == "" || st.Email.SMTPPort <= 0 {
			return errors.New("email smtp_host and smtp_port are required")
		}
		if strings.TrimSpace(st.Email.From) == "" || strings.TrimSpace(st.Email.To) == "" {
			return errors.New("email from and to are required")
		}
	}
	return nil
}

func (s *apiServer) loadNotifications() (notificationState, error) {
	st, err := s.loadNotificationsFromConfig()
	if err != nil {
		return notificationState{}, err
	}
	legacy, err := s.loadLegacyNotificationState()
	if err != nil {
		return notificationState{}, err
	}
	if migrated, migrateErr := s.migrateLegacyNotificationRuntimeFields(&st, legacy); migrateErr != nil {
		log.Printf("notifications migration warning: unable to migrate legacy runtime settings to config.yaml: %v", migrateErr)
	} else if migrated {
		log.Printf("notifications migration: copied legacy runtime settings into config.yaml")
	}
	// Legacy state keeps API-local metadata and controls that do not have
	// direct config.yaml equivalents yet.
	if strings.TrimSpace(st.Email.Password) == "" {
		st.Email.Password = legacy.Email.Password
	}
	st.Enabled = legacy.Enabled
	st.Events = legacy.Events
	st.Throttle = legacy.Throttle
	if legacy.Quiet.Timezone != "" {
		st.Quiet.Timezone = legacy.Quiet.Timezone
	}
	st.Quiet.AllowFailures = legacy.Quiet.AllowFailures
	st.Digest.LastSentAt = legacy.Digest.LastSentAt
	if legacy.LastDelivery != nil {
		st.LastDelivery = legacy.LastDelivery
	}
	if legacy.UpdatedAt != "" {
		st.UpdatedAt = legacy.UpdatedAt
	}
	def := defaultNotificationState()
	if st.Email.SMTPPort == 0 {
		st.Email.SMTPPort = def.Email.SMTPPort
	}
	if st.LastDelivery == nil {
		st.LastDelivery = map[string]notificationDeliveryStatus{}
	}
	return st, nil
}

func (s *apiServer) saveNotifications(st notificationState) error {
	if err := s.saveNotificationsToConfig(st); err != nil {
		return err
	}
	// Keep API-local metadata in the legacy sidecar file so delivery/test status
	// and non-runtime controls survive restarts without polluting config.yaml.
	path := s.absPath(s.notificationStatePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func (s *apiServer) loadLegacyNotificationState() (notificationState, error) {
	path := s.absPath(s.notificationStatePath)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return defaultNotificationState(), nil
		}
		return notificationState{}, err
	}
	var st notificationState
	if err := json.Unmarshal(b, &st); err != nil {
		return notificationState{}, err
	}
	if st.LastDelivery == nil {
		st.LastDelivery = map[string]notificationDeliveryStatus{}
	}
	return st, nil
}

func (s *apiServer) loadNotificationsFromConfig() (notificationState, error) {
	st := defaultNotificationState()
	cfg, err := s.loadRawConfigMap()
	if err != nil {
		return st, err
	}
	st.Email.Enabled = readMapBool(cfg, "email-enabled", st.Email.Enabled)
	st.Email.SMTPHost = readMapString(cfg, "smtp-server", st.Email.SMTPHost)
	st.Email.SMTPPort = readMapInt(cfg, "smtp-port", st.Email.SMTPPort)
	st.Email.Username = readMapString(cfg, "smtp-user", st.Email.Username)
	st.Email.Password = readMapString(cfg, "smtp-password", st.Email.Password)
	st.Email.From = readMapString(cfg, "email-from", st.Email.From)
	st.Email.To = strings.Join(readMapCSV(cfg, "email-to"), ",")

	st.Webhook.Enabled = readMapBool(cfg, "webhook-enabled", st.Webhook.Enabled)
	st.Webhook.URL = readMapString(cfg, "webhook-url", st.Webhook.URL)

	st.Slack.Enabled = readMapBool(cfg, "slack-enabled", st.Slack.Enabled)
	st.Slack.WebhookURL = readMapString(cfg, "slack-webhook-url", st.Slack.WebhookURL)
	st.Slack.Channel = readMapString(cfg, "slack-channel", st.Slack.Channel)

	st.Digest.Enabled = readMapBool(cfg, "notify-digest", st.Digest.Enabled)
	st.Quiet = quietHoursFromConfig(cfg, st.Quiet)
	st.Maintenance = maintenanceFromConfig(cfg)
	return st, nil
}

func (s *apiServer) saveNotificationsToConfig(st notificationState) error {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o700); err != nil {
		return err
	}
	cfg, err := s.loadRawConfigMap()
	if err != nil {
		return err
	}
	cfg["email-enabled"] = st.Email.Enabled
	cfg["smtp-server"] = strings.TrimSpace(st.Email.SMTPHost)
	cfg["smtp-port"] = st.Email.SMTPPort
	cfg["smtp-user"] = strings.TrimSpace(st.Email.Username)
	cfg["smtp-password"] = st.Email.Password
	cfg["email-from"] = strings.TrimSpace(st.Email.From)
	cfg["email-to"] = strings.TrimSpace(st.Email.To)

	cfg["webhook-enabled"] = st.Webhook.Enabled
	cfg["webhook-url"] = strings.TrimSpace(st.Webhook.URL)

	cfg["slack-enabled"] = st.Slack.Enabled
	cfg["slack-webhook-url"] = strings.TrimSpace(st.Slack.WebhookURL)
	cfg["slack-channel"] = strings.TrimSpace(st.Slack.Channel)

	cfg["notify-digest"] = st.Digest.Enabled
	cfg["quiet-hours"] = quietHoursToConfig(st.Quiet)
	cfg["maintenance-windows"] = maintenanceToConfig(st.Maintenance)

	out, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, out, 0o600)
}

func (s *apiServer) loadRawConfigMap() (map[string]interface{}, error) {
	cfgPath, err := s.validateConfigPath(s.configPath)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]interface{}{}, nil
		}
		return nil, err
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return map[string]interface{}{}, nil
	}
	out := map[string]interface{}{}
	if err := yaml.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *apiServer) migrateLegacyNotificationRuntimeFields(st *notificationState, legacy notificationState) (bool, error) {
	cfg, err := s.loadRawConfigMap()
	if err != nil {
		return false, err
	}
	changed := false
	adoptBool := func(key string, src bool, dst *bool) {
		if _, ok := cfg[key]; ok {
			return
		}
		*dst = src
		changed = true
	}
	adoptString := func(key, src string, dst *string) {
		if _, ok := cfg[key]; ok {
			return
		}
		src = strings.TrimSpace(src)
		if src == "" {
			return
		}
		*dst = src
		changed = true
	}
	adoptInt := func(key string, src int, dst *int) {
		if _, ok := cfg[key]; ok {
			return
		}
		if src <= 0 {
			return
		}
		*dst = src
		changed = true
	}

	adoptBool("email-enabled", legacy.Email.Enabled, &st.Email.Enabled)
	adoptString("smtp-server", legacy.Email.SMTPHost, &st.Email.SMTPHost)
	adoptInt("smtp-port", legacy.Email.SMTPPort, &st.Email.SMTPPort)
	adoptString("smtp-user", legacy.Email.Username, &st.Email.Username)
	adoptString("smtp-password", legacy.Email.Password, &st.Email.Password)
	adoptString("email-from", legacy.Email.From, &st.Email.From)
	adoptString("email-to", legacy.Email.To, &st.Email.To)

	adoptBool("webhook-enabled", legacy.Webhook.Enabled, &st.Webhook.Enabled)
	adoptString("webhook-url", legacy.Webhook.URL, &st.Webhook.URL)

	adoptBool("slack-enabled", legacy.Slack.Enabled, &st.Slack.Enabled)
	adoptString("slack-webhook-url", legacy.Slack.WebhookURL, &st.Slack.WebhookURL)
	adoptString("slack-channel", legacy.Slack.Channel, &st.Slack.Channel)

	adoptBool("notify-digest", legacy.Digest.Enabled, &st.Digest.Enabled)
	if _, ok := cfg["quiet-hours"]; !ok {
		qh := quietHoursToConfig(legacy.Quiet)
		if qh != "" {
			st.Quiet.Enabled = legacy.Quiet.Enabled
			st.Quiet.Start = legacy.Quiet.Start
			st.Quiet.End = legacy.Quiet.End
			changed = true
		}
	}
	if _, ok := cfg["maintenance-windows"]; !ok {
		mw := maintenanceToConfig(legacy.Maintenance)
		if mw != "" {
			st.Maintenance = legacy.Maintenance
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	if err := s.saveNotificationsToConfig(*st); err != nil {
		return false, err
	}
	return true, nil
}

func quietHoursFromConfig(cfg map[string]interface{}, def quietHoursConfig) quietHoursConfig {
	spec := strings.TrimSpace(readMapString(cfg, "quiet-hours", ""))
	if spec == "" {
		return def
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return def
	}
	def.Enabled = true
	def.Start = strings.TrimSpace(parts[0])
	def.End = strings.TrimSpace(parts[1])
	return def
}

func quietHoursToConfig(q quietHoursConfig) string {
	if !q.Enabled {
		return ""
	}
	start := strings.TrimSpace(q.Start)
	end := strings.TrimSpace(q.End)
	if start == "" || end == "" {
		return ""
	}
	return start + "-" + end
}

func maintenanceFromConfig(cfg map[string]interface{}) []maintenanceWindow {
	spec := strings.TrimSpace(readMapString(cfg, "maintenance-windows", ""))
	if spec == "" {
		return nil
	}
	var windows []maintenanceWindow
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "/", 2)
		if len(parts) != 2 {
			continue
		}
		windows = append(windows, maintenanceWindow{
			Start: strings.TrimSpace(parts[0]),
			End:   strings.TrimSpace(parts[1]),
		})
	}
	return windows
}

func maintenanceToConfig(windows []maintenanceWindow) string {
	if len(windows) == 0 {
		return ""
	}
	var pairs []string
	for _, w := range windows {
		start := strings.TrimSpace(w.Start)
		end := strings.TrimSpace(w.End)
		if start == "" || end == "" {
			continue
		}
		pairs = append(pairs, start+"/"+end)
	}
	return strings.Join(pairs, ",")
}

func readMapString(m map[string]interface{}, key, def string) string {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case []interface{}:
		var parts []string
		for _, it := range t {
			parts = append(parts, strings.TrimSpace(fmt.Sprintf("%v", it)))
		}
		return strings.Join(parts, ",")
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func readMapBool(m map[string]interface{}, key string, def bool) bool {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(t))
		if err == nil {
			return b
		}
	}
	return def
}

func readMapInt(m map[string]interface{}, key string, def int) int {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err == nil {
			return n
		}
	}
	return def
}

func readMapCSV(m map[string]interface{}, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case string:
		return splitCSVValues(t)
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, it := range t {
			val := strings.TrimSpace(fmt.Sprintf("%v", it))
			if val != "" {
				out = append(out, val)
			}
		}
		return out
	default:
		return splitCSVValues(fmt.Sprintf("%v", v))
	}
}

func splitCSVValues(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func (s *apiServer) notifyRunFinished(runErr error) {
	st, err := s.loadNotifications()
	if err != nil || !st.Enabled {
		return
	}
	s.mu.Lock()
	startedAt := s.started
	lastOut := s.lastOut
	s.mu.Unlock()

	event := ""
	title := ""
	details := map[string]interface{}{
		"started_at": startedAt.Format(time.RFC3339),
		"output_dir": s.absPath(s.outputDir),
	}
	if runErr != nil {
		if !st.Events.RunFailure {
			return
		}
		event = "run_failure"
		title = "NCC run failed"
		details["error"] = runErr.Error()
		details["last_output"] = lastOut
	} else {
		if st.Events.RunSuccess {
			event = "run_success"
			title = "NCC run completed"
			details["last_output"] = lastOut
			s.emit(&st, event, title, details)
		}
		if st.Events.PolicyViolations {
			violations := s.readPolicyViolations()
			if len(violations) > 0 {
				event = "policy_violations"
				title = fmt.Sprintf("NCC policy violations detected (%d)", len(violations))
				details["violations"] = violations
				s.emit(&st, event, title, details)
			}
		}
		return
	}
	s.emit(&st, event, title, details)
}

// notifyOperationalFailure sends an alert for a non-run operational failure
// (a failed backup snapshot or a self-heal cycle that found failing checks).
// It piggybacks on the existing "run failure" toggle — an operator who asked to
// be told about failures wants to hear about these too — so no new config knob
// or UI is required, and existing notification states keep working unchanged.
// Best-effort and non-blocking for callers: delivery errors are only logged.
func (s *apiServer) notifyOperationalFailure(event, title string, details map[string]interface{}) {
	st, err := s.loadNotifications()
	if err != nil || !st.Enabled || !st.Events.RunFailure {
		return
	}
	s.emit(&st, event, title, details)
}

func (s *apiServer) readPolicyViolations() []string {
	outDir := s.selectBestReportOutDir()
	b, err := os.ReadFile(filepath.Join(outDir, "policy-gates.txt"))
	if err != nil {
		return nil
	}
	out := []string{}
	for _, ln := range strings.Split(string(b), "\n") {
		ln = strings.TrimSpace(ln)
		if ln != "" {
			out = append(out, ln)
		}
	}
	return out
}

func (s *apiServer) dispatchNotifications(st *notificationState, event, title string, details map[string]interface{}, channels map[string]bool) error {
	payload := map[string]interface{}{
		"event":     event,
		"title":     title,
		"time":      time.Now().UTC().Format(time.RFC3339),
		"repo_root": s.absPath(s.repoRoot),
		"details":   details,
	}
	b, _ := json.Marshal(payload)
	msgBody := string(b)
	if channels == nil {
		channels = map[string]bool{"slack": true, "webhook": true, "email": true}
	}
	var failed []string
	if channels["webhook"] && st.Webhook.Enabled {
		err := sendWebhookJSON(st.Webhook.URL, payload)
		s.recordDelivery(st, "webhook", event, err)
		if err != nil {
			log.Printf("notifications webhook error: %v", err)
			failed = append(failed, "webhook: "+err.Error())
		}
	}
	if channels["slack"] && st.Slack.Enabled {
		err := sendSlackWebhook(st.Slack, title, msgBody)
		s.recordDelivery(st, "slack", event, err)
		if err != nil {
			log.Printf("notifications slack error: %v", err)
			failed = append(failed, "slack: "+err.Error())
		}
	}
	if channels["email"] && st.Email.Enabled {
		err := sendSMTPMail(st.Email, title, msgBody)
		s.recordDelivery(st, "email", event, err)
		if err != nil {
			log.Printf("notifications email error: %v", err)
			failed = append(failed, "email: "+err.Error())
		}
	}
	if err := s.saveNotifications(*st); err != nil {
		log.Printf("notifications status save error: %v", err)
	}
	if len(failed) > 0 {
		return errors.New(strings.Join(failed, "; "))
	}
	return nil
}

func (s *apiServer) recordDelivery(st *notificationState, channel, event string, err error) {
	if st.LastDelivery == nil {
		st.LastDelivery = map[string]notificationDeliveryStatus{}
	}
	item := st.LastDelivery[channel]
	now := time.Now().UTC().Format(time.RFC3339)
	item.LastAttemptAt = now
	item.LastEvent = event
	if err != nil {
		item.LastError = err.Error()
		item.Success = false
		item.TotalFailure++
	} else {
		item.LastSuccessAt = now
		item.LastError = ""
		item.Success = true
		item.TotalSuccess++
	}
	st.LastDelivery[channel] = item
}

func requestedChannels(channel string) (map[string]bool, error) {
	switch channel {
	case "", "all":
		return map[string]bool{"slack": true, "webhook": true, "email": true}, nil
	case "slack":
		return map[string]bool{"slack": true}, nil
	case "webhook":
		return map[string]bool{"webhook": true}, nil
	case "email":
		return map[string]bool{"email": true}, nil
	default:
		return nil, errors.New("channel must be one of: all, slack, webhook, email")
	}
}

func sendWebhookJSON(endpoint string, payload map[string]interface{}) error {
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	cli := &http.Client{Timeout: 10 * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook status: %d", resp.StatusCode)
	}
	return nil
}

func sendSlackWebhook(cfg slackNotificationConfig, title, text string) error {
	payload := map[string]interface{}{
		"text": fmt.Sprintf("*%s*\n%s", title, text),
	}
	if strings.TrimSpace(cfg.Channel) != "" {
		payload["channel"] = cfg.Channel
	}
	if strings.TrimSpace(cfg.Username) != "" {
		payload["username"] = cfg.Username
	}
	return sendWebhookJSON(cfg.WebhookURL, payload)
}

func sendSMTPMail(cfg emailNotificationConfig, subject, body string) error {
	addr := fmt.Sprintf("%s:%d", strings.TrimSpace(cfg.SMTPHost), cfg.SMTPPort)
	host := strings.TrimSpace(cfg.SMTPHost)
	var auth smtp.Auth
	if strings.TrimSpace(cfg.Username) != "" {
		auth = smtp.PlainAuth("", strings.TrimSpace(cfg.Username), cfg.Password, host)
	}
	msg := strings.Join([]string{
		fmt.Sprintf("From: %s", strings.TrimSpace(cfg.From)),
		fmt.Sprintf("To: %s", strings.TrimSpace(cfg.To)),
		fmt.Sprintf("Subject: %s", subject),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
		"",
		body,
	}, "\r\n")
	recipients := []string{}
	for _, v := range strings.Split(cfg.To, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			recipients = append(recipients, v)
		}
	}
	if len(recipients) == 0 {
		return errors.New("no email recipients configured")
	}
	return smtp.SendMail(addr, auth, strings.TrimSpace(cfg.From), recipients, []byte(msg))
}
