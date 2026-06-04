// Package notify owns the orchestrator's outbound notifications: email
// (SMTP), generic webhook, and Slack, including the retry wrappers, the
// optional Go text/template overrides, and the per-channel delivery-metrics
// accumulator. It depends only on goncc/internal/model and
// goncc/internal/retryutil so it can be reused without importing package main.
//
// Package main re-exports the senders and ApplyEmailTemplates via aliases, and
// reads the run's delivery counters through ResetMetrics/SnapshotMetrics.
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/smtp"
	"os"
	"path/filepath"
	"strings"
	"sync"
	texttemplate "text/template"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"goncc/internal/model"
	"goncc/internal/retryutil"
)

// signWebhookBody returns the HMAC-SHA256 signature header value ("sha256=<hex>")
// for body using secret.
func signWebhookBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// deadLetterRecord is the JSON written for a notification that failed to
// deliver after retries.
type deadLetterRecord struct {
	Channel string `json:"channel"`
	Time    string `json:"time"`
	Error   string `json:"error"`
	Cluster string `json:"cluster,omitempty"`
	Subject string `json:"subject,omitempty"`
	Payload string `json:"payload"`
}

// writeDeadLetter persists a failed notification to dir (best-effort; never
// returns an error to the caller). A nanosecond suffix avoids collisions
// between clusters running in parallel.
func writeDeadLetter(dir, channel, cluster, subject string, deliverErr error, payload []byte) {
	if strings.TrimSpace(dir) == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("notification dead-letter: mkdir failed")
		return
	}
	rec := deadLetterRecord{
		Channel: channel,
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
		Cluster: cluster,
		Subject: subject,
		Payload: string(payload),
	}
	if deliverErr != nil {
		rec.Error = deliverErr.Error()
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return
	}
	name := fmt.Sprintf("%s-%s-%d.json", channel, time.Now().UTC().Format("20060102T150405Z"), time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("notification dead-letter: write failed")
	}
}

// RetryAttempts is the number of times each notification is retried by the
// *WithRetry wrappers before a failure is recorded.
const RetryAttempts = 3

// Channels is the fixed set of channels reported, so the exported metric
// always emits a line per channel (0 when unused) and alerting rules don't
// break on absent series.
var Channels = []string{"email", "webhook", "slack"}

// Metrics accumulates per-channel notification delivery outcomes across a run.
// Clusters run in parallel, so access is mutex-guarded. One outcome is
// recorded per notification (after retries are exhausted), not per attempt.
type Metrics struct {
	mu       sync.Mutex
	attempts map[string]int
	failures map[string]int
}

// NewMetrics returns an empty Metrics accumulator.
func NewMetrics() *Metrics {
	return &Metrics{attempts: map[string]int{}, failures: map[string]int{}}
}

// Record increments the attempt counter for channel, and the failure counter
// when err != nil. A nil receiver is a no-op.
func (m *Metrics) Record(channel string, err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts[channel]++
	if err != nil {
		m.failures[channel]++
	}
}

// Reset clears all counts.
func (m *Metrics) Reset() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts = map[string]int{}
	m.failures = map[string]int{}
}

// Snapshot returns copies of the attempt/failure counts under the lock.
func (m *Metrics) Snapshot() (attempts, failures map[string]int) {
	attempts = map[string]int{}
	failures = map[string]int{}
	if m == nil {
		return attempts, failures
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.attempts {
		attempts[k] = v
	}
	for k, v := range m.failures {
		failures[k] = v
	}
	return attempts, failures
}

// defaultMetrics accumulates notification outcomes for the current run. The
// orchestrator runs as a one-shot process per run; ResetMetrics is called at
// the start of a run so in-process tests don't accumulate across cases.
var defaultMetrics = NewMetrics()

// ResetMetrics clears the run-level accumulator.
func ResetMetrics() { defaultMetrics.Reset() }

// SnapshotMetrics returns the run-level per-channel attempt/failure counts.
func SnapshotMetrics() (attempts, failures map[string]int) { return defaultMetrics.Snapshot() }

// RenderTemplate renders an operator-supplied Go text/template against the
// notification summary. Used for the optional email subject/body and webhook
// body overrides. missingkey=error makes typos in field names fail loudly
// (caller falls back to the built-in default) instead of emitting "<no
// value>". Uses text/template (not html/template) so plain-text email and JSON
// bodies are not HTML-escaped.
func RenderTemplate(name, tmplStr string, data model.NotificationSummary) (string, error) {
	t, err := texttemplate.New(name).Option("missingkey=error").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("parse %s template: %w", name, err)
	}
	var b strings.Builder
	if err := t.Execute(&b, data); err != nil {
		return "", fmt.Errorf("execute %s template: %w", name, err)
	}
	return b.String(), nil
}

// ApplyEmailTemplates returns the email subject and body, applying the
// operator's text/template overrides when configured and falling back to the
// provided defaults on any template error (logged, never fatal).
func ApplyEmailTemplates(cfg model.Config, defSubj, defBody string, data model.NotificationSummary, l zerolog.Logger) (string, string) {
	subj, body := defSubj, defBody
	if strings.TrimSpace(cfg.EmailSubjectTemplate) != "" {
		if s, err := RenderTemplate("email-subject", cfg.EmailSubjectTemplate, data); err != nil {
			l.Error().Err(err).Msg("email subject template failed; using default subject")
		} else {
			subj = s
		}
	}
	if strings.TrimSpace(cfg.EmailBodyTemplate) != "" {
		if bdy, err := RenderTemplate("email-body", cfg.EmailBodyTemplate, data); err != nil {
			l.Error().Err(err).Msg("email body template failed; using default body")
		} else {
			body = bdy
		}
	}
	return subj, body
}

// ==================== Email ====================

// SendEmail sends a single email (no retry). It no-ops when email is disabled.
func SendEmail(cfg model.Config, subj string, body string, attachPath string) error {
	if !cfg.EmailEnabled || cfg.SMTPServer == "" || len(cfg.EmailTo) == 0 {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", cfg.SMTPServer, cfg.SMTPPort)
	auth := smtp.PlainAuth("", cfg.SMTPUser, cfg.SMTPPassword, cfg.SMTPServer)

	var msg bytes.Buffer
	attachHTML := cfg.EmailAttachHTML && attachPath != ""
	if attachHTML {
		attachBody, err := os.ReadFile(attachPath)
		if err != nil {
			return fmt.Errorf("read attachment %s: %w", attachPath, err)
		}
		boundary := "ncc-report-boundary"
		msg.WriteString(fmt.Sprintf("From: %s\r\n", cfg.EmailFrom))
		msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(cfg.EmailTo, ",")))
		msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subj))
		msg.WriteString("MIME-Version: 1.0\r\n")
		msg.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=%s\r\n", boundary))
		msg.WriteString("\r\n")
		msg.WriteString(fmt.Sprintf("--%s\r\n", boundary))
		msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		msg.WriteString("\r\n")
		msg.WriteString(body)
		msg.WriteString("\r\n")
		msg.WriteString(fmt.Sprintf("--%s\r\n", boundary))
		msg.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
		msg.WriteString("Content-Disposition: attachment; filename=\"" + filepath.Base(attachPath) + "\"\r\n")
		msg.WriteString("\r\n")
		msg.Write(attachBody)
		msg.WriteString("\r\n")
		msg.WriteString(fmt.Sprintf("--%s--\r\n", boundary))
	} else {
		msg.WriteString(fmt.Sprintf("From: %s\r\n", cfg.EmailFrom))
		msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(cfg.EmailTo, ",")))
		msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subj))
		msg.WriteString("MIME-Version: 1.0\r\n")
		msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		msg.WriteString("\r\n")
		msg.WriteString(body)
	}

	if cfg.EmailUseTLS {
		// STARTTLS-style connection:
		c, err := smtp.Dial(addr)
		if err != nil {
			return err
		}
		defer c.Close()

		if err := c.StartTLS(&tls.Config{ServerName: cfg.SMTPServer, InsecureSkipVerify: cfg.SMTPInsecureSkipVerify}); err != nil {
			return err
		}
		if err := c.Auth(auth); err != nil {
			return err
		}
		if err := c.Mail(cfg.EmailFrom); err != nil {
			return err
		}
		for _, rcpt := range cfg.EmailTo {
			if err := c.Rcpt(rcpt); err != nil {
				return err
			}
		}
		w, err := c.Data()
		if err != nil {
			return err
		}
		if _, err := w.Write(msg.Bytes()); err != nil {
			return err
		}
		return w.Close()
	}

	return smtp.SendMail(addr, auth, cfg.EmailFrom, cfg.EmailTo, msg.Bytes())
}

// SendEmailWithRetry sends an email with retry/backoff and records the outcome.
func SendEmailWithRetry(cfg model.Config, subj string, body string, attachPath string) error {
	// Don't count a disabled channel as an attempt: SendEmail returns nil
	// early when email is off, which would otherwise inflate the metric.
	if !cfg.EmailEnabled || cfg.SMTPServer == "" || len(cfg.EmailTo) == 0 {
		return nil
	}
	var lastErr error
	for attempt := 1; attempt <= RetryAttempts; attempt++ {
		lastErr = SendEmail(cfg, subj, body, attachPath)
		if lastErr == nil {
			defaultMetrics.Record("email", nil)
			return nil
		}
		if attempt < RetryAttempts {
			backoff := retryutil.JitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
			time.Sleep(backoff)
		}
	}
	defaultMetrics.Record("email", lastErr)
	writeDeadLetter(cfg.NotificationDeadLetterDir, "email", "", subj, lastErr, []byte(body))
	return lastErr
}

// ==================== Webhook ====================

// SendWebhook posts the summary to the configured webhook (no outer retry
// wrapper). It no-ops when webhook is disabled.
func SendWebhook(ctx context.Context, client model.HTTPClient, cfg model.Config, summary model.NotificationSummary) error {
	if !cfg.WebhookEnabled || cfg.WebhookURL == "" {
		return nil
	}

	var payload []byte
	if strings.TrimSpace(cfg.WebhookTemplate) != "" {
		rendered, terr := RenderTemplate("webhook", cfg.WebhookTemplate, summary)
		if terr != nil {
			// Fall back to the default JSON encoding so a bad template never
			// drops the notification; surface the misconfig in the log.
			log.Error().Err(terr).Msg("webhook template failed; using default JSON payload")
		} else {
			payload = []byte(rendered)
		}
	}
	if payload == nil {
		p, err := json.Marshal(summary)
		if err != nil {
			return err
		}
		payload = p
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s := strings.TrimSpace(cfg.WebhookSecret); s != "" {
		req.Header.Set("X-NCC-Signature", signWebhookBody(s, payload))
	}
	for k, v := range cfg.WebhookHeaders {
		req.Header.Set(k, v)
	}

	// simple retry loop using shared helpers
	for attempt := 1; attempt <= cfg.RetryMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("webhook request failed: %w", err)
			}
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if !retryutil.IsRetryableStatus(resp.StatusCode) || attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("webhook status %d", resp.StatusCode)
			}
			if d, ok := retryutil.RetryAfterDelay(resp); ok {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(d):
					continue
				}
			}
		}
		backoff := retryutil.JitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("webhook exhausted retries")
}

// SendWebhookWithRetry wraps SendWebhook with the notification retry policy and
// records the outcome.
func SendWebhookWithRetry(ctx context.Context, client model.HTTPClient, cfg model.Config, summary model.NotificationSummary) error {
	if !cfg.WebhookEnabled || cfg.WebhookURL == "" {
		return nil
	}
	var lastErr error
	for attempt := 1; attempt <= RetryAttempts; attempt++ {
		lastErr = SendWebhook(ctx, client, cfg, summary)
		if lastErr == nil {
			defaultMetrics.Record("webhook", nil)
			return nil
		}
		if attempt < RetryAttempts {
			backoff := retryutil.JitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	defaultMetrics.Record("webhook", lastErr)
	if body, mErr := json.Marshal(summary); mErr == nil {
		writeDeadLetter(cfg.NotificationDeadLetterDir, "webhook", summary.Cluster, "", lastErr, body)
	}
	return lastErr
}

// ==================== Slack ====================

// SendSlack posts the summary to the configured Slack webhook (no outer retry
// wrapper). It no-ops when slack is disabled.
func SendSlack(ctx context.Context, client model.HTTPClient, cfg model.Config, summary model.NotificationSummary) error {
	if !cfg.SlackEnabled || cfg.SlackWebhookURL == "" {
		return nil
	}

	// Determine color based on severity
	color := "#36a64f" // green
	if summary.FailCount > 0 {
		color = "#ff0000" // red
	} else if summary.WarnCount > 0 {
		color = "#ffaa00" // orange
	}

	// Build Slack message
	attachment := map[string]interface{}{
		"color": color,
		"title": fmt.Sprintf("NCC Report: %s", summary.Cluster),
		"fields": []map[string]string{
			{"title": "FAIL", "value": fmt.Sprintf("%d", summary.FailCount), "short": "true"},
			{"title": "WARN", "value": fmt.Sprintf("%d", summary.WarnCount), "short": "true"},
			{"title": "ERR", "value": fmt.Sprintf("%d", summary.ErrCount), "short": "true"},
			{"title": "INFO", "value": fmt.Sprintf("%d", summary.InfoCount), "short": "true"},
			{"title": "Total Checks", "value": fmt.Sprintf("%d", summary.TotalChecks), "short": "false"},
		},
		"footer": "NCC Orchestrator",
		"ts":     summary.FinishedAt.Unix(),
	}

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{attachment},
	}

	if cfg.SlackChannel != "" {
		payload["channel"] = cfg.SlackChannel
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.SlackWebhookURL, bytes.NewReader(jsonPayload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Simple retry loop
	for attempt := 1; attempt <= cfg.RetryMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("slack request failed: %w", err)
			}
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if !retryutil.IsRetryableStatus(resp.StatusCode) || attempt == cfg.RetryMaxAttempts {
				return fmt.Errorf("slack status %d", resp.StatusCode)
			}
		}
		backoff := retryutil.JitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("slack exhausted retries")
}

// SendSlackWithRetry wraps SendSlack with the notification retry policy and
// records the outcome.
func SendSlackWithRetry(ctx context.Context, client model.HTTPClient, cfg model.Config, summary model.NotificationSummary) error {
	if !cfg.SlackEnabled || cfg.SlackWebhookURL == "" {
		return nil
	}
	var lastErr error
	for attempt := 1; attempt <= RetryAttempts; attempt++ {
		lastErr = SendSlack(ctx, client, cfg, summary)
		if lastErr == nil {
			defaultMetrics.Record("slack", nil)
			return nil
		}
		if attempt < RetryAttempts {
			backoff := retryutil.JitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	defaultMetrics.Record("slack", lastErr)
	if body, mErr := json.Marshal(summary); mErr == nil {
		writeDeadLetter(cfg.NotificationDeadLetterDir, "slack", summary.Cluster, "", lastErr, body)
	}
	return lastErr
}
