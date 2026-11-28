package mcpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const AuthMePath = "/api/v1/auth/me"

// ParseAPIURL accepts https:// anywhere and http:// only on loopback.
func ParseAPIURL(raw, envName string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("%s must be an absolute URL", envName)
	}
	if u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("%s must not include credentials, query, or fragment", envName)
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return u, nil
	case "http":
		host := u.Hostname()
		if host == "localhost" || host == "127.0.0.1" || host == "::1" {
			return u, nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return u, nil
		}
		return nil, fmt.Errorf("%s must use https:// (http:// is allowed only on loopback)", envName)
	default:
		return nil, fmt.Errorf("%s must use https://", envName)
	}
}

// LookupIdentity GET /api/v1/auth/me with PAT + MCP proof headers.
// Fail closed unless the API reports authenticated=true.
func LookupIdentity(apiURL, token, version, proof string) error {
	base, err := ParseAPIURL(apiURL, "NCC_MCP_API_URL")
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(base.String(), "/") + AuthMePath
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	ver := HeaderSafeVersion(version)
	req.Header.Set("User-Agent", UserAgent(ver))
	req.Header.Set(VersionHeader, ver)
	if strings.TrimSpace(proof) != "" {
		req.Header.Set(ProofHeader, proof)
	}
	req.Header.Set(ToolHeader, "auth_me")
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			Proxy:           nil,
			IdleConnTimeout: 5 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("could not verify MCP token with the API: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusForbidden {
		var denied struct {
			Error     string `json:"error"`
			ErrorCode string `json:"error_code"`
		}
		if json.Unmarshal(body, &denied) == nil && denied.Error != "" {
			return fmt.Errorf("%s", denied.Error)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authentication required")
	}
	var envelope struct {
		Success bool `json:"success"`
		Data    struct {
			Authenticated bool `json:"authenticated"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &envelope) != nil || !envelope.Success || !envelope.Data.Authenticated {
		return fmt.Errorf("authentication required")
	}
	return nil
}
