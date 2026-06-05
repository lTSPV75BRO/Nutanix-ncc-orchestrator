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
	"strings"
	"time"
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
	LastDelivery map[string]notificationDeliveryStatus `json:"last_delivery,omitempty"`
	UpdatedAt    string                                `json:"updated_at"`
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
		req.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if err := validateNotificationState(req); err != nil {
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
			_ = s.dispatchNotifications(&st, event, title, details, nil)
		}
		if st.Events.PolicyViolations {
			violations := s.readPolicyViolations()
			if len(violations) > 0 {
				event = "policy_violations"
				title = fmt.Sprintf("NCC policy violations detected (%d)", len(violations))
				details["violations"] = violations
				_ = s.dispatchNotifications(&st, event, title, details, nil)
			}
		}
		return
	}
	_ = s.dispatchNotifications(&st, event, title, details, nil)
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
