package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"goncc/internal/ai"
)

// mcpV3Output is the single output shape advertised by the v3 MCP server.
// Data is intentionally flexible because the orchestrator has several legacy
// report formats; each tool documents the concrete data it returns.
type mcpV3Output struct {
	SchemaVersion string          `json:"schema_version"`
	Status        string          `json:"status"`
	Summary       string          `json:"summary,omitempty"`
	Data          any             `json:"data,omitempty"`
	Error         *ai.Error       `json:"error,omitempty"`
	Warnings      []ai.Notice     `json:"warnings,omitempty"`
	NextActions   []ai.NextAction `json:"next_actions,omitempty"`
	RequestID     string          `json:"request_id,omitempty"`
	RunID         string          `json:"run_id,omitempty"`
	JobID         string          `json:"job_id,omitempty"`
	Truncated     bool            `json:"truncated,omitempty"`
}

// mcpV3OutputSchema is deliberately explicit so clients can discover the
// contract from tools/list without having to infer it from handler types.
var mcpV3OutputSchema = map[string]any{
	"type":     "object",
	"required": []string{"schema_version", "status"},
	"properties": map[string]any{
		"schema_version": map[string]any{"type": "string", "const": ai.SchemaVersion},
		"status":         map[string]any{"type": "string", "enum": []string{"success", "partial", "error"}},
		"summary":        map[string]any{"type": "string"},
		"data":           map[string]any{},
		"error":          map[string]any{"$ref": "#/$defs/AIError"},
		"warnings": map[string]any{
			"type":  "array",
			"items": map[string]any{"type": "object"},
		},
		"next_actions": map[string]any{
			"type":  "array",
			"items": map[string]any{"type": "object"},
		},
		"request_id": map[string]any{"type": "string"},
		"run_id":     map[string]any{"type": "string"},
		"job_id":     map[string]any{"type": "string"},
		"truncated":  map[string]any{"type": "boolean"},
	},
	"$defs": map[string]any{
		"AIError": map[string]any{
			"type":     "object",
			"required": []string{"schema_version", "status", "code", "category", "message"},
			"properties": map[string]any{
				"schema_version": map[string]any{"type": "string"},
				"status":         map[string]any{"type": "string", "const": "error"},
				"code":           map[string]any{"type": "string"},
				"category":       map[string]any{"type": "string"},
				"message":        map[string]any{"type": "string"},
				"retryable":      map[string]any{"type": "boolean"},
				"remediation":    map[string]any{"type": "string"},
			},
		},
	},
}

func addAITool[In any](server *mcp.Server, tool *mcp.Tool, handler func(context.Context, *mcp.CallToolRequest, In) (*mcp.CallToolResult, any, error)) {
	if !mcpToolAllowed(tool.Name) {
		return
	}
	tool.OutputSchema = mcpV3OutputSchema
	mcp.AddTool[In, mcpV3Output](server, tool, func(ctx context.Context, req *mcp.CallToolRequest, input In) (*mcp.CallToolResult, mcpV3Output, error) {
		notifyMCPProgress(ctx, req, 0, "starting "+tool.Name)
		result, output, err := handler(ctx, req, input)
		notifyMCPProgress(ctx, req, 1, "completed "+tool.Name)
		return result, normalizeMCPOutput(result, output, err), err
	})
}

// NCC_MCP_PROFILE controls which capabilities are advertised:
// readonly, operator (default), or admin. This is registration-time policy,
// so a client cannot discover or invoke excluded tools.
func mcpToolAllowed(name string) bool {
	profile := strings.ToLower(strings.TrimSpace(os.Getenv("NCC_MCP_PROFILE")))
	if profile == "" {
		profile = "operator"
	}
	if profile == "admin" {
		return true
	}
	adminOnly := map[string]bool{
		"apply_upgrade": true, "bootstrap_v2_stack": true, "install_service": true,
		"uninstall_service": true, "start_v2_stack": true, "stop_v2_stack": true,
		"restart_v2_stack": true, "repair_v2_stack": true, "restore_v2_backup": true,
		"backup_v2_stack": true, "orchestrator_action": true,
	}
	if adminOnly[name] {
		return false
	}
	if profile == "readonly" {
		switch name {
		case "run_ncc", "replay_reports", "create_schedule", "delete_schedule":
			return false
		}
	}
	return true
}

func notifyMCPProgress(ctx context.Context, req *mcp.CallToolRequest, progress float64, message string) {
	if req == nil || req.Session == nil {
		return
	}
	_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
		Progress: progress,
		Total:    1,
		Message:  message,
	})
}

func normalizeMCPOutput(result *mcp.CallToolResult, output any, handlerErr error) mcpV3Output {
	out := mcpV3Output{SchemaVersion: ai.SchemaVersion, Status: "success", Data: output}
	if handlerErr != nil {
		out.Status = "error"
		out.Summary = handlerErr.Error()
		out.Data = nil
		out.Error = contractErrorFromProcess(handlerErr, "")
		return out
	}
	if result == nil {
		return out
	}
	if result.IsError {
		out.Status = "error"
		out.Data = nil
		out.Summary = textFromMCPResult(result)
		if e, ok := output.(*ai.Error); ok {
			out.Error = e
		} else if e, ok := output.(ai.Error); ok {
			out.Error = &e
		} else {
			out.Error = ai.NewError(ai.CodeInternal, "tool", out.Summary)
		}
		return out
	}
	if out.Data == nil {
		text := textFromMCPResult(result)
		if text != "" {
			if parsed, ok := parseJSONValue(text); ok {
				out.Data = parsed
			} else {
				out.Summary = text
			}
		}
	}
	return out
}

func textFromMCPResult(result *mcp.CallToolResult) string {
	if result == nil {
		return ""
	}
	var parts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func parseJSONValue(text string) (any, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, false
	}
	var value any
	if json.Unmarshal([]byte(text), &value) == nil {
		return value, true
	}
	// Some existing commands prefix JSON with a short label. Preserve the
	// complete text when it cannot be parsed rather than guessing at fields.
	return nil, false
}

func mcpContractError(code, category, message string, retryable bool) (*mcp.CallToolResult, any, error) {
	e := ai.NewError(code, category, message).WithRetryable(retryable)
	data, _ := json.Marshal(e)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		IsError: true,
	}, e, nil
}

func mcpInvalidArgument(field, message string) (*mcp.CallToolResult, any, error) {
	e := ai.NewError(ai.CodeInvalidArgument, "validation", message)
	e.Fields = []ai.FieldError{{Field: field, Code: ai.CodeInvalidArgument, Message: message}}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
		IsError: true,
	}, e, nil
}

func isContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func contractErrorFromProcess(err error, output string) *ai.Error {
	if isContextCancellation(err) {
		code := ai.CodeCancelled
		if errors.Is(err, context.DeadlineExceeded) {
			code = ai.CodeTimeout
		}
		return ai.NewError(code, "execution", err.Error()).WithRetryable(true)
	}
	return ai.NewError(ai.CodeInternal, "execution", fmt.Sprintf("%v", err)).
		WithRemediation(strings.TrimSpace(output))
}
