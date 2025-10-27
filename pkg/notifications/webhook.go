package notifications

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"goncc/pkg/errors"
	"goncc/pkg/types"
)

// WebhookConfig holds webhook notification configuration
type WebhookConfig struct {
	URL     string
	Method  string
	Headers map[string]string
	Timeout time.Duration
}

// WebhookNotifier handles webhook notifications
type WebhookNotifier struct {
	config WebhookConfig
	client *http.Client
}

// NewWebhookNotifier creates a new webhook notifier
func NewWebhookNotifier(config WebhookConfig) *WebhookNotifier {
	return &WebhookNotifier{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// SendReport sends a webhook with NCC report
func (w *WebhookNotifier) SendReport(results []types.AggBlock, failedClusters []string) error {
	if w.config.URL == "" {
		return errors.New(errors.ErrorTypeConfig, "webhook URL not configured")
	}

	payload, err := w.buildPayload(results, failedClusters)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to build webhook payload")
	}

	req, err := http.NewRequest(w.config.Method, w.config.URL, bytes.NewBuffer(payload))
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeNetwork, "failed to create webhook request")
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for key, value := range w.config.Headers {
		req.Header.Set(key, value)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeNetwork, "failed to send webhook")
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return errors.New(errors.ErrorTypeNetwork, fmt.Sprintf("webhook returned status %d", resp.StatusCode))
	}

	return nil
}

// buildPayload builds the webhook payload
func (w *WebhookNotifier) buildPayload(results []types.AggBlock, failedClusters []string) ([]byte, error) {
	// Count severities
	summary := struct {
		Total int `json:"total"`
		Fail  int `json:"fail"`
		Warn  int `json:"warn"`
		Info  int `json:"info"`
		Err   int `json:"err"`
	}{}

	for _, result := range results {
		summary.Total++
		switch result.Severity {
		case "FAIL":
			summary.Fail++
		case "WARN":
			summary.Warn++
		case "INFO":
			summary.Info++
		case "ERR":
			summary.Err++
		}
	}

	payload := struct {
		Timestamp      string            `json:"timestamp"`
		EventType      string            `json:"event_type"`
		Summary        interface{}       `json:"summary"`
		Results        []types.AggBlock  `json:"results"`
		FailedClusters []string          `json:"failed_clusters"`
		Metadata       map[string]string `json:"metadata"`
	}{
		Timestamp:      time.Now().Format(time.RFC3339),
		EventType:      "ncc_report",
		Summary:        summary,
		Results:        results,
		FailedClusters: failedClusters,
		Metadata: map[string]string{
			"source":  "ncc-orchestrator",
			"version": "1.0.0", // This could be made configurable
		},
	}

	return json.Marshal(payload)
}
