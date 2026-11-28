package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

func mcpTextError(message string) (*mcp.CallToolResult, any, error) {
	return mcpContractError(ai.CodePermissionDenied, "authorization", message, false)
}

// requireMCPConfirmation is the single gate every mutating tool passes through.
// Order matters: the cheap server-side switches run first so a call that would
// be refused anyway never bothers the human, then (when the connected client
// can show one) a human is asked to approve this exact call.
//
// tool is the registered tool name and args the typed tool input; both are only
// used to describe the call in the confirmation dialog.
func requireMCPConfirmation(ctx context.Context, req *mcp.CallToolRequest, tool, confirm string, args any) (*mcp.CallToolResult, any, error) {
	if !mcpMutationsAllowed() {
		return mcpTextError("mutating MCP tools are disabled; set NCC_MCP_ALLOW_MUTATIONS=true in the MCP server environment")
	}
	if confirm != "APPLY" {
		return mcpTextError("mutating MCP tools require confirm=\"APPLY\"")
	}
	return requireHumanConfirmation(ctx, req, tool, args)
}

func mcpMutationsAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("NCC_MCP_ALLOW_MUTATIONS"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// NCC_MCP_CONFIRM selects how a human is involved in mutating tool calls.
const mcpConfirmEnv = "NCC_MCP_CONFIRM"

type mcpConfirmMode string

const (
	// confirmAuto asks the human through MCP elicitation when the client
	// supports it and otherwise relies on confirm=APPLY alone.
	confirmAuto mcpConfirmMode = "auto"
	// confirmRequire refuses mutating calls from clients that cannot elicit.
	confirmRequire mcpConfirmMode = "require"
	// confirmOff never asks.
	confirmOff mcpConfirmMode = "off"
)

// mcpConfirmTimeout bounds how long a call waits for the human. Replaced in tests.
var mcpConfirmTimeout = 2 * time.Minute

// mcpElicitUnsupportedFrom is the first protocol revision in which a server may
// no longer send elicitation/create while serving a request (it must use the
// multi round-trip input mechanism, which this server does not implement).
// Revisions are ISO dates, so string comparison orders them.
const mcpElicitUnsupportedFrom = "2026-07-28"

// elicitationProtocolVersions lists the protocol revisions the server offers:
// every revision the SDK supports that still allows server-initiated requests.
func elicitationProtocolVersions() []string {
	var out []string
	for _, v := range mcp.SupportedProtocolVersions() {
		if v < mcpElicitUnsupportedFrom {
			out = append(out, v)
		}
	}
	return out
}

func mcpConfirmModeFromEnv() (mcpConfirmMode, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(mcpConfirmEnv))) {
	case "", "auto":
		return confirmAuto, nil
	case "require":
		return confirmRequire, nil
	case "off":
		return confirmOff, nil
	}
	return "", fmt.Errorf("%s=%q is not valid; use auto (default), require or off", mcpConfirmEnv, os.Getenv(mcpConfirmEnv))
}

// clientCanElicit reports whether the connected client declared form
// elicitation and the session can still use server-initiated requests.
func clientCanElicit(ss *mcp.ServerSession) bool {
	if ss == nil {
		return false
	}
	p := ss.InitializeParams()
	if p == nil || p.Capabilities == nil || p.Capabilities.Elicitation == nil {
		return false
	}
	if p.ProtocolVersion >= mcpElicitUnsupportedFrom {
		return false
	}
	e := p.Capabilities.Elicitation
	// An empty capability object means form mode, as in the SDK.
	return e.Form != nil || e.URL == nil
}

func requireHumanConfirmation(ctx context.Context, req *mcp.CallToolRequest, tool string, args any) (*mcp.CallToolResult, any, error) {
	mode, err := mcpConfirmModeFromEnv()
	if err != nil {
		return mcpTextError(err.Error())
	}
	if mode == confirmOff {
		return nil, nil, nil
	}
	var ss *mcp.ServerSession
	if req != nil {
		ss = req.Session
	}
	if !clientCanElicit(ss) {
		if mode == confirmRequire {
			return mcpTextError(mcpConfirmEnv + "=require: this MCP client does not support elicitation, so a human cannot confirm " + sanitizeForDialog(tool, 64) + "; use a client that supports it or change " + mcpConfirmEnv)
		}
		return nil, nil, nil
	}

	ectx, cancel := context.WithTimeout(ctx, mcpConfirmTimeout)
	defer cancel()
	res, err := ss.Elicit(ectx, &mcp.ElicitParams{
		Mode:    "form",
		Message: confirmationMessage(tool, args),
		RequestedSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"confirm": map[string]any{
					"type":        "boolean",
					"title":       "Allow this change",
					"description": "Tick to allow exactly the call shown above.",
					"default":     false,
				},
			},
			"required": []string{"confirm"},
		},
	})
	switch {
	case err != nil && ectx.Err() != nil:
		return mcpTextError("the change was not applied: no answer from the human within " + mcpConfirmTimeout.String() + ", or the request was cancelled")
	case err != nil:
		return mcpTextError("the change was not applied: the human could not be asked (" + sanitizeForDialog(err.Error(), 160) + ")")
	case res == nil || res.Action != "accept":
		return mcpTextError("the change was not applied: the human did not approve it")
	}
	if ok, _ := res.Content["confirm"].(bool); !ok {
		return mcpTextError("the change was not applied: the human did not approve it")
	}
	return nil, nil, nil
}

const (
	dialogValueMax   = 120
	dialogMessageMax = 1200
	dialogMaxArgs    = 16
)

// confirmationMessage describes the call for the human. Argument values come
// from the model, so they are scrubbed of secrets, stripped of control and
// formatting characters (no forged lines or bidi tricks), quoted, and capped.
func confirmationMessage(tool string, args any) string {
	var b strings.Builder
	b.WriteString("An AI assistant asks the NCC Orchestrator to make a change.\nTool: ")
	b.WriteString(sanitizeForDialog(tool, 64))
	b.WriteString("\nArguments:")
	n := 0
	if raw, err := json.Marshal(args); err == nil {
		var m map[string]any
		if json.Unmarshal(raw, &m) == nil {
			scrubbed, _ := scrubForModel(m).(map[string]any)
			keys := make([]string, 0, len(scrubbed))
			for k := range scrubbed {
				if k != "confirm" {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			for _, k := range keys {
				if n == dialogMaxArgs {
					b.WriteString("\n- ... more arguments not shown")
					break
				}
				var v string
				if s, ok := scrubbed[k].(string); ok {
					v = s
				} else {
					enc, _ := json.Marshal(scrubbed[k])
					v = string(enc)
				}
				b.WriteString("\n- " + sanitizeForDialog(k, 40) + ": " + strconv.Quote(sanitizeForDialog(v, dialogValueMax)))
				n++
			}
		}
	}
	if n == 0 {
		b.WriteString(" none")
	}
	b.WriteString("\nOnly allow this if you asked for it.")
	msg := b.String()
	if r := []rune(msg); len(r) > dialogMessageMax {
		msg = string(r[:dialogMessageMax]) + "..."
	}
	return msg
}

// sanitizeForDialog drops control, format (bidi overrides, zero-width) and
// line/paragraph separator characters, then caps the length in runes.
func sanitizeForDialog(s string, max int) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) || r == utf8.RuneError {
			continue
		}
		out = append(out, r)
	}
	if len(out) > max {
		// Say how much is hidden: the person approves the whole call.
		return string(out[:max]) + fmt.Sprintf("... (%d more characters not shown)", len(out)-max)
	}
	return string(out)
}
