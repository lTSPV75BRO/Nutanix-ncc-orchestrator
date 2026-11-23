package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
	"goncc/internal/mcpclient"
)

const maxAPIResponseBytes = 1_000_000

type apiEnvelope struct {
	Success     bool            `json:"success"`
	Error       string          `json:"error"`
	ErrorCode   string          `json:"error_code"`
	Message     string          `json:"message"`
	Data        json.RawMessage `json:"data"`
	RequestID   string          `json:"request_id"`
	Remediation string          `json:"remediation"`
}

// apiTransport is replaced in tests.
var apiTransport http.RoundTripper = &http.Transport{
	Proxy:           nil,
	IdleConnTimeout: 30 * time.Second,
}

func apiClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: apiTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func callAPI(ctx context.Context, tool, method, path string, query url.Values, body any, timeout time.Duration) (apiEnvelope, []byte, error) {
	tool = mcpclient.SanitizeToolName(tool)
	token, err := configuredMCPToken()
	if err != nil {
		return apiEnvelope{}, nil, err
	}
	ver := mcpclient.HeaderSafeVersion(productVersion)
	proof, err := mcpclient.SignProof(token, ver, time.Now().UTC())
	if err != nil {
		return apiEnvelope{}, nil, err
	}
	base, err := mcpclient.ParseAPIURL(mcpAPIBase(), mcpAuthAPIURLEnv)
	if err != nil {
		return apiEnvelope{}, nil, err
	}
	u, err := joinAPIURL(base, path)
	if err != nil {
		return apiEnvelope{}, nil, err
	}
	if query != nil {
		u.RawQuery = query.Encode()
	}
	var rdr io.Reader
	if body != nil {
		raw, mErr := json.Marshal(body)
		if mErr != nil {
			return apiEnvelope{}, nil, mErr
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
	if err != nil {
		return apiEnvelope{}, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", mcpclient.UserAgent(ver))
	req.Header.Set(mcpclient.VersionHeader, ver)
	req.Header.Set(mcpclient.ProofHeader, proof)
	if tool != "" {
		req.Header.Set(mcpclient.ToolHeader, tool)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := apiClient(timeout).Do(req)
	if err != nil {
		return apiEnvelope{}, nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponseBytes+1))
	truncated := len(raw) > maxAPIResponseBytes
	if truncated {
		raw = raw[:maxAPIResponseBytes]
	}
	var env apiEnvelope
	parseErr := json.Unmarshal(raw, &env)
	if truncated && parseErr != nil && resp.StatusCode < 400 {
		// A cut-off JSON document cannot be read; answering {"ok":true} (as
		// happened before) would tell the model nothing was wrong.
		return env, raw, fmt.Errorf("the answer is larger than %d bytes and was cut off; narrow the request (cluster, check, severity, limit)", maxAPIResponseBytes)
	}
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(env.Error)
		if msg == "" {
			msg = strings.TrimSpace(env.Message)
		}
		if msg == "" {
			msg = fmt.Sprintf("API %s %s returned HTTP %d", method, path, resp.StatusCode)
		}
		if env.ErrorCode == mcpclient.ErrorCode || env.ErrorCode == mcpclient.ProofErrorCode {
			return env, raw, fmt.Errorf("%s", msg)
		}
		return env, raw, fmt.Errorf("%s", msg)
	}
	if truncated {
		env.Message = strings.TrimSpace(env.Message + " [truncated]")
	}
	return env, raw, nil
}

func mcpAPIToolResult(env apiEnvelope, data any, err error) (*mcp.CallToolResult, any, error) {
	if err != nil {
		code := ai.CodeUpstreamUnavailable
		if env.ErrorCode == mcpclient.ErrorCode || env.ErrorCode == mcpclient.ProofErrorCode {
			code = ai.CodePermissionDenied
		} else if strings.Contains(strings.ToLower(err.Error()), "unauthorized") || strings.Contains(strings.ToLower(err.Error()), "authentication") {
			code = ai.CodePermissionDenied
		}
		msg := err.Error()
		e := ai.NewError(code, "authorization", msg)
		if env.Remediation != "" {
			e = e.WithRemediation(env.Remediation)
		}
		payload, _ := json.Marshal(e)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(payload)}},
			IsError: true,
		}, e, nil
	}
	if data == nil && len(env.Data) > 0 {
		var parsed any
		if json.Unmarshal(env.Data, &parsed) == nil {
			data = parsed
		} else {
			data = json.RawMessage(env.Data)
		}
	}
	// Whatever the endpoint returned, the model only gets redacted values.
	data = scrubForModel(data)
	// Compact JSON: the indentation is whitespace the model would be billed for.
	text, _ := json.Marshal(data)
	if len(text) == 0 || string(text) == "null" {
		text = []byte(env.Message)
		if len(text) == 0 {
			text = []byte(`{"ok":true}`)
		}
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(text)}},
	}, data, nil
}

func refusePasswordArg(password string) (*mcp.CallToolResult, any, error) {
	if strings.TrimSpace(password) == "" {
		return nil, nil, nil
	}
	return mcpContractError(ai.CodePermissionDenied, "authorization", "MCP tools must not send passwords; store credentials with secret:// on the API server", false)
}
