package main

import (
	"context"
	"log"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// logMCPCalls logs each request the MCP client sends (to stderr; stdout carries
// the protocol): the method, the tool name for tool calls, how long it took and
// whether it failed. Tool arguments are not logged; they may carry passwords.
func logMCPCalls(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		start := time.Now()
		tool := ""
		if p, ok := req.GetParams().(*mcp.CallToolParamsRaw); ok && p != nil {
			tool = p.Name
		}
		res, err := next(ctx, method, req)
		d := time.Since(start)
		switch {
		case err != nil:
			log.Printf("level=ERROR msg=\"mcp request failed\" method=%s tool=%s duration_ms=%d error=%q", method, tool, d.Milliseconds(), err.Error())
		case tool != "":
			level, outcome := "INFO", "ok"
			if r, ok := res.(*mcp.CallToolResult); ok && r != nil && r.IsError {
				level, outcome = "WARN", "tool error"
			}
			log.Printf("level=%s msg=\"mcp tool call\" tool=%s result=%q duration_ms=%d", level, tool, outcome, d.Milliseconds())
		case method != "notifications/initialized" && method != "ping":
			log.Printf("level=DEBUG msg=\"mcp request\" method=%s duration_ms=%d", method, d.Milliseconds())
		}
		return res, err
	}
}
