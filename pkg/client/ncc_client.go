package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"goncc/pkg/config"
	"goncc/pkg/errors"
	"goncc/pkg/types"
)

// NCCClient handles communication with Nutanix NCC APIs
type NCCClient struct {
	baseURL string
	user    string
	pass    string
	http    types.HTTPClient
	cfg     *config.Config
}

// NewNCCClient creates a new NCC client
func NewNCCClient(cluster, user, pass string, httpc types.HTTPClient, cfg *config.Config) *NCCClient {
	return &NCCClient{
		baseURL: fmt.Sprintf("https://%s:9440/PrismGateway/services/rest", cluster),
		user:    user,
		pass:    pass,
		http:    httpc,
		cfg:     cfg,
	}
}

// StartChecks initiates NCC checks on the cluster
func (c *NCCClient) StartChecks(ctx context.Context) (string, []byte, error) {
	url := c.baseURL + "/v1/ncc/checks"
	payload := []byte(`{"sendEmail":false}`)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return "", nil, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to create request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := DoWithRetry(ctx, c.http, req, c.cfg, "start checks")
	if err != nil {
		return "", body, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to start checks")
	}
	_ = resp

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", body, errors.Wrap(err, errors.ErrorTypeParse, "failed to parse start checks response")
	}
	uuid, _ := data["taskUuid"].(string)
	if uuid == "" {
		if alt, ok := data["task_uuid"].(string); ok && alt != "" {
			uuid = alt
		}
	}
	if uuid == "" {
		return "", body, errors.New(errors.ErrorTypeParse, "missing taskUuid in response")
	}
	return uuid, body, nil
}

// GetTask retrieves the status of an NCC task
func (c *NCCClient) GetTask(ctx context.Context, taskID string) (types.TaskStatus, []byte, error) {
	url := c.baseURL + "/v2.0/tasks/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return types.TaskStatus{}, nil, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to create request")
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := DoWithRetry(ctx, c.http, req, c.cfg, "get task")
	if err != nil {
		return types.TaskStatus{}, body, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to get task status")
	}
	_ = resp

	var status types.TaskStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return types.TaskStatus{}, body, errors.Wrap(err, errors.ErrorTypeParse, "failed to parse task status response")
	}
	return status, body, nil
}

// GetRunSummary retrieves the summary of a completed NCC run
func (c *NCCClient) GetRunSummary(ctx context.Context, taskID string) (types.NCCSummary, []byte, error) {
	url := c.baseURL + "/v1/ncc/" + taskID
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return types.NCCSummary{}, nil, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to create request")
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	resp, body, err := DoWithRetry(ctx, c.http, req, c.cfg, "get summary")
	if err != nil {
		return types.NCCSummary{}, body, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to get run summary")
	}
	_ = resp

	var summary types.NCCSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return types.NCCSummary{}, body, errors.Wrap(err, errors.ErrorTypeParse, "failed to parse run summary response")
	}
	return summary, body, nil
}

// HealthCheck performs a simple connectivity and authentication check
func (c *NCCClient) HealthCheck(ctx context.Context) error {
	// Try to access a simple Prism Gateway endpoint that doesn't require NCC
	// Use the cluster endpoint which is more reliable
	url := c.baseURL + "/v1/cluster"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeNetwork, "failed to create health check request")
	}
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(c.user, c.pass)

	// Use a shorter timeout for health checks to avoid long waits
	healthCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Don't use retries for health checks - we want fast feedback
	resp, err := c.http.Do(req.WithContext(healthCtx))
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeNetwork, "health check failed")
	}
	defer resp.Body.Close()

	// Read response body for better error messages
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		body = []byte("unable to read response body")
	}

	// Check for authentication errors first
	if resp.StatusCode == 401 {
		return errors.New(errors.ErrorTypeAuth, "authentication failed - check username and password")
	}

	// Check if we got a valid response (200-299)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(errors.ErrorTypeNetwork, fmt.Sprintf("health check returned status %d: %s", resp.StatusCode, string(body)))
	}

	// Basic validation that we got JSON response
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return errors.Wrap(err, errors.ErrorTypeParse, "health check response is not valid JSON")
	}

	return nil
}
