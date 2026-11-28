package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
	"goncc/internal/mcpclient"
)

const (
	mcpAuthTokenEnv      = mcpclient.TokenEnv
	mcpAuthAPIURLEnv     = "NCC_MCP_API_URL"
	mcpAuthMinTokenBytes = mcpclient.MinTokenBytes
	mcpAuthMePath        = mcpclient.AuthMePath
)

// mcpAuthReady fails closed when the process has no usable MCP credential.
// Stdio hosts put the token in the server environment; HTTP clients must also
// send it on each request (see requireMCPAuth).
func mcpAuthReady() error {
	if _, err := configuredMCPToken(); err != nil {
		return err
	}
	if _, err := mcpclient.ParseAPIURL(mcpAPIBase(), mcpAuthAPIURLEnv); err != nil {
		return err
	}
	return nil
}

func mcpAPIBase() string {
	raw := strings.TrimSpace(os.Getenv(mcpAuthAPIURLEnv))
	if raw == "" {
		return "http://127.0.0.1:8081"
	}
	return raw
}

func configuredMCPToken() (string, error) {
	tok := strings.TrimSpace(os.Getenv(mcpAuthTokenEnv))
	if len(tok) < mcpAuthMinTokenBytes {
		return "", fmt.Errorf("%s must be set to a personal access token (at least %d characters) before the MCP server will run tools", mcpAuthTokenEnv, mcpAuthMinTokenBytes)
	}
	return tok, nil
}

func extractPresentedMCPToken(req mcp.Request) string {
	extra := req.GetExtra()
	if extra == nil {
		return ""
	}
	if extra.Header == nil {
		return ""
	}
	if t := strings.TrimSpace(extra.Header.Get("X-API-Token")); t != "" {
		return t
	}
	authz := strings.TrimSpace(extra.Header.Get("Authorization"))
	if len(authz) >= 7 && strings.EqualFold(authz[:7], "bearer ") {
		return strings.TrimSpace(authz[7:])
	}
	return ""
}

func tokenEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// authorizeMCPRequest requires a configured token for tools/call and
// resources/read. HTTP transports must present the same token; stdio is
// authenticated by the host having placed it in the process environment.
func authorizeMCPRequest(req mcp.Request) error {
	configured, err := configuredMCPToken()
	if err != nil {
		return err
	}
	if extra := req.GetExtra(); extra != nil && extra.Header != nil {
		presented := extractPresentedMCPToken(req)
		if presented == "" || !tokenEqual(presented, configured) {
			return fmt.Errorf("authentication required")
		}
		return verifyMCPTokenWithAPI(presented)
	}
	return verifyMCPTokenWithAPI(configured)
}

func verifyMCPTokenWithAPI(token string) error {
	return mcpVerifyAgainstAPI(mcpAPIBase(), token)
}

// mcpVerifyAgainstAPI is replaced in tests.
var mcpVerifyAgainstAPI = lookupMCPIdentity

func lookupMCPIdentity(apiURL, token string) error {
	proof, err := mcpclient.SignProof(token, productVersion, time.Now().UTC())
	if err != nil {
		return err
	}
	return mcpclient.LookupIdentity(apiURL, token, productVersion, proof)
}

func requireMCPAuth(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		switch method {
		case "tools/call", "resources/read":
			if err := authorizeMCPRequest(req); err != nil {
				if method == "tools/call" {
					msg := err.Error()
					e := ai.NewError(ai.CodePermissionDenied, "authorization", msg)
					data, _ := json.Marshal(e)
					return &mcp.CallToolResult{
						Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
						IsError: true,
					}, nil
				}
				return nil, err
			}
		}
		return next(ctx, method, req)
	}
}
