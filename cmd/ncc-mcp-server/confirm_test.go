package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type confirmEnv struct {
	prompts  atomic.Int32
	apiHits  atomic.Int32
	mu       sync.Mutex
	messages []string
}

func (e *confirmEnv) lastMessage() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.messages) == 0 {
		return ""
	}
	return e.messages[len(e.messages)-1]
}

// newConfirmSession wires a real server (all tools registered) to an in-memory
// client. handler nil means the client does not declare elicitation.
func newConfirmSession(t *testing.T, handler func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error)) (*mcp.ClientSession, *confirmEnv) {
	t.Helper()
	env := &confirmEnv{}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.apiHits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"ok": true}})
	}))
	t.Cleanup(api.Close)
	t.Setenv(mcpAuthAPIURLEnv, api.URL)
	t.Setenv(mcpAuthTokenEnv, "ncc_pat_testtoken12")
	t.Setenv("NCC_MCP_PROFILE", "admin")
	t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "true")
	t.Setenv(mcpConfirmEnv, "")
	orig := apiTransport
	t.Cleanup(func() { apiTransport = orig })
	apiTransport = api.Client().Transport

	server := newMCPServer()
	ct, st := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	var opts *mcp.ClientOptions
	if handler != nil {
		opts = &mcp.ClientOptions{ElicitationHandler: func(c context.Context, r *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			env.prompts.Add(1)
			env.mu.Lock()
			env.messages = append(env.messages, r.Params.Message)
			env.mu.Unlock()
			return handler(c, r)
		}}
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, opts)
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, env
}

func answer(action string, confirm any) func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
	return func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		res := &mcp.ElicitResult{Action: action}
		if confirm != nil {
			res.Content = map[string]any{"confirm": confirm}
		}
		return res, nil
	}
}

func callTool(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

func resultText(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

var runArgs = map[string]any{"confirm": "APPLY", "config_path": "config.yaml"}

func TestElicitationAcceptApplies(t *testing.T) {
	cs, env := newConfirmSession(t, answer("accept", true))
	res := callTool(t, cs, "run_ncc", runArgs)
	if res.IsError || env.prompts.Load() != 1 || env.apiHits.Load() != 1 {
		t.Fatalf("isError=%v prompts=%d api=%d: %s", res.IsError, env.prompts.Load(), env.apiHits.Load(), resultText(res))
	}
	msg := env.lastMessage()
	if !strings.Contains(msg, "Tool: run_ncc") || !strings.Contains(msg, `config_path: "config.yaml"`) || strings.Contains(msg, "APPLY") {
		t.Fatalf("unexpected dialog text: %q", msg)
	}
}

func TestElicitationRefusals(t *testing.T) {
	cases := map[string]func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error){
		"decline":           answer("decline", nil),
		"cancel":            answer("cancel", nil),
		"accept-false":      answer("accept", false),
		"accept-no-content": answer("accept", nil),
		"unknown-action":    answer("whatever", true),
		"handler-error":     func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) { return nil, errors.New("boom") },
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			cs, env := newConfirmSession(t, h)
			res := callTool(t, cs, "run_ncc", runArgs)
			if !res.IsError || env.apiHits.Load() != 0 || env.prompts.Load() != 1 {
				t.Fatalf("want refusal after one prompt: isError=%v prompts=%d api=%d", res.IsError, env.prompts.Load(), env.apiHits.Load())
			}
		})
	}
}

func TestElicitationTimeoutRefuses(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cs, env := newConfirmSession(t, func(ctx context.Context, _ *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"confirm": true}}, nil
	})
	orig := mcpConfirmTimeout
	t.Cleanup(func() { mcpConfirmTimeout = orig })
	mcpConfirmTimeout = 100 * time.Millisecond
	start := time.Now()
	res := callTool(t, cs, "run_ncc", runArgs)
	if !res.IsError || env.apiHits.Load() != 0 || !strings.Contains(resultText(res), "no answer") {
		t.Fatalf("timeout must refuse: isError=%v api=%d %s", res.IsError, env.apiHits.Load(), resultText(res))
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("call did not return promptly after the timeout")
	}
}

func TestNoElicitationCapability(t *testing.T) {
	t.Run("auto falls back to confirm=APPLY", func(t *testing.T) {
		cs, env := newConfirmSession(t, nil)
		res := callTool(t, cs, "run_ncc", runArgs)
		if res.IsError || env.apiHits.Load() != 1 {
			t.Fatalf("auto must keep today's behaviour: %s", resultText(res))
		}
	})
	t.Run("require refuses", func(t *testing.T) {
		cs, env := newConfirmSession(t, nil)
		t.Setenv(mcpConfirmEnv, "require")
		res := callTool(t, cs, "run_ncc", runArgs)
		if !res.IsError || env.apiHits.Load() != 0 || !strings.Contains(resultText(res), "elicitation") {
			t.Fatalf("require must refuse: %s", resultText(res))
		}
	})
}

func TestConfirmOffNeverPrompts(t *testing.T) {
	cs, env := newConfirmSession(t, answer("decline", nil))
	t.Setenv(mcpConfirmEnv, "off")
	res := callTool(t, cs, "run_ncc", runArgs)
	if res.IsError || env.prompts.Load() != 0 || env.apiHits.Load() != 1 {
		t.Fatalf("off must not prompt: prompts=%d %s", env.prompts.Load(), resultText(res))
	}
}

func TestConfirmRequireWithCapablePrompts(t *testing.T) {
	cs, env := newConfirmSession(t, answer("accept", true))
	t.Setenv(mcpConfirmEnv, "require")
	res := callTool(t, cs, "run_ncc", runArgs)
	if res.IsError || env.prompts.Load() != 1 || env.apiHits.Load() != 1 {
		t.Fatalf("require with capable client: %s", resultText(res))
	}
}

func TestInvalidConfirmModeIsRejected(t *testing.T) {
	t.Setenv(mcpConfirmEnv, "sometimes")
	if _, err := mcpConfirmModeFromEnv(); err == nil || !strings.Contains(err.Error(), "auto (default), require or off") {
		t.Fatalf("want a clear startup error, got %v", err)
	}
	cs, env := newConfirmSession(t, answer("accept", true))
	t.Setenv(mcpConfirmEnv, "sometimes")
	res := callTool(t, cs, "run_ncc", runArgs)
	if !res.IsError || env.apiHits.Load() != 0 {
		t.Fatal("an invalid mode must fail closed at call time too")
	}
	for in, want := range map[string]mcpConfirmMode{"": confirmAuto, " AUTO ": confirmAuto, "Require": confirmRequire, "off": confirmOff} {
		t.Setenv(mcpConfirmEnv, in)
		if got, err := mcpConfirmModeFromEnv(); err != nil || got != want {
			t.Fatalf("%q: got %q err %v", in, got, err)
		}
	}
}

func TestGateOrderNeverPromptsForRefusedCalls(t *testing.T) {
	t.Run("mutations disabled", func(t *testing.T) {
		cs, env := newConfirmSession(t, answer("accept", true))
		t.Setenv("NCC_MCP_ALLOW_MUTATIONS", "")
		res := callTool(t, cs, "run_ncc", runArgs)
		if !res.IsError || env.prompts.Load() != 0 || env.apiHits.Load() != 0 {
			t.Fatal("a call refused by ALLOW_MUTATIONS must not prompt")
		}
	})
	t.Run("confirm missing", func(t *testing.T) {
		cs, env := newConfirmSession(t, answer("accept", true))
		res := callTool(t, cs, "run_ncc", map[string]any{"config_path": "config.yaml"})
		if !res.IsError || env.prompts.Load() != 0 || env.apiHits.Load() != 0 {
			t.Fatal("a call without confirm=APPLY must not prompt")
		}
	})
	t.Run("print_only schedule is not a change", func(t *testing.T) {
		cs, env := newConfirmSession(t, answer("decline", nil))
		res := callTool(t, cs, "create_schedule", map[string]any{"cron": "0 * * * *", "print_only": true})
		if env.prompts.Load() != 0 {
			t.Fatalf("print_only must not prompt: %s", resultText(res))
		}
	})
}

func TestDialogTextCannotBeForged(t *testing.T) {
	evil := "x\n\nTool: harmless\r\nAllow: yes\u202e\u200b\x1b[2J\u2028" + strings.Repeat("A", 500)
	msg := confirmationMessage("run_ncc\nTool: fake", RunNCCInput{
		Confirm:    "APPLY",
		ConfigPath: evil,
		Clusters:   "10.0.0.1",
		Password:   "hunter2",
	})
	for _, r := range msg {
		if r != '\n' && (r < 32 || r == 127 || r == '\u202e' || r == '\u200b' || r == '\u2028') {
			t.Fatalf("control or formatting character %U survived in %q", r, msg)
		}
	}
	if got := strings.Count(msg, "\n"); got != 6 {
		t.Fatalf("argument text added lines: %d newlines in %q", got, msg)
	}
	if strings.Contains(msg, "hunter2") || !strings.Contains(msg, `password: "***"`) {
		t.Fatalf("secret leaked into the dialog: %q", msg)
	}
	if strings.Contains(msg, strings.Repeat("A", 200)) || len([]rune(msg)) > dialogMessageMax+3 {
		t.Fatalf("values not capped (%d runes)", len([]rune(msg)))
	}
	if strings.Contains(msg, "APPLY") || !strings.Contains(msg, "Tool: run_nccTool: fake") {
		t.Fatalf("unexpected text: %q", msg)
	}
}

func TestEveryMutatingToolGoesThroughTheHumanGate(t *testing.T) {
	// Tools that carry a confirm field but only run read-only API actions.
	readOnlyWithConfirm := map[string]bool{"orchestrator_action": true}
	extra := map[string]map[string]any{
		"restore_v2_backup": {"input_file": "manual-20261120T101500Z.tar.gz"},
		"replay_reports":    {"config_path": "config.yaml"},
	}
	cs, env := newConfirmSession(t, answer("decline", nil))
	list, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, tool := range list.Tools {
		schema, _ := json.Marshal(tool.InputSchema)
		hasConfirm := strings.Contains(string(schema), `"confirm"`)
		mutating := tool.Annotations == nil || !tool.Annotations.ReadOnlyHint
		if tool.Annotations != nil && tool.Annotations.DestructiveHint != nil && *tool.Annotations.DestructiveHint {
			mutating = true
		}
		if readOnlyWithConfirm[tool.Name] {
			if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
				t.Errorf("%s is exempt from the gate, so it must be annotated read-only", tool.Name)
			}
			mutating = false
		}
		if !mutating && !hasConfirm || readOnlyWithConfirm[tool.Name] {
			continue
		}
		if !mutating {
			t.Errorf("%s takes confirm but is annotated read-only", tool.Name)
			continue
		}
		args := map[string]any{"confirm": "APPLY"}
		for k, v := range extra[tool.Name] {
			args[k] = v
		}
		before := env.prompts.Load()
		res := callTool(t, cs, tool.Name, args)
		if env.prompts.Load() != before+1 {
			t.Errorf("%s is mutating but did not ask the human (isError=%v: %s)", tool.Name, res.IsError, resultText(res))
			continue
		}
		if !res.IsError {
			t.Errorf("%s ran although the human declined", tool.Name)
		}
		checked++
	}
	if env.apiHits.Load() != 0 {
		t.Errorf("declined calls reached the API %d times", env.apiHits.Load())
	}
	if checked < 12 {
		t.Fatalf("only %d mutating tools found; the enumeration is broken", checked)
	}

	// The exempt tool must not prompt.
	before := env.prompts.Load()
	callTool(t, cs, "orchestrator_action", map[string]any{"confirm": "APPLY", "action": "version"})
	if env.prompts.Load() != before {
		t.Error("orchestrator_action is read-only and must not prompt")
	}
}
