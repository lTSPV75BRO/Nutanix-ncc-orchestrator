package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSecureCompare(t *testing.T) {
	if !secureCompare("abc", "abc") {
		t.Fatal("expected equal tokens")
	}
	if secureCompare("abc", "abcd") {
		t.Fatal("expected different tokens to fail")
	}
}

func TestScrubEnvDropsMasterKey(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"NCC_MASTER_KEY=supersecret",
		"NCC_MASTER_KEY_FILE=/tmp/k", // a path, not the key itself: must be kept
		"HOME=/root",
	}
	got := scrubEnv(env, "NCC_MASTER_KEY")
	for _, kv := range got {
		if kv == "NCC_MASTER_KEY=supersecret" {
			t.Fatal("scrubEnv must drop NCC_MASTER_KEY")
		}
	}
	// Prefix-only drop: NCC_MASTER_KEY_FILE shares a prefix but is a distinct
	// var and must survive.
	var sawFile, sawPath bool
	for _, kv := range got {
		if kv == "NCC_MASTER_KEY_FILE=/tmp/k" {
			sawFile = true
		}
		if kv == "PATH=/usr/bin" {
			sawPath = true
		}
	}
	if !sawFile {
		t.Fatal("scrubEnv must not drop NCC_MASTER_KEY_FILE (only the exact NCC_MASTER_KEY var)")
	}
	if !sawPath {
		t.Fatal("scrubEnv dropped an unrelated var")
	}
	// Input slice must not be mutated.
	if len(env) != 4 || env[1] != "NCC_MASTER_KEY=supersecret" {
		t.Fatal("scrubEnv must not mutate its input")
	}
}

func TestValidateConfigPath(t *testing.T) {
	repo := t.TempDir()
	s := &apiServer{repoRoot: repo}
	got, err := s.validateConfigPath("configs/test.yaml")
	if err != nil {
		t.Fatalf("validateConfigPath failed: %v", err)
	}
	repoReal, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatalf("resolve temp dir symlink: %v", err)
	}
	if want := filepath.Join(repoReal, "configs", "test.yaml"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if _, err := s.validateConfigPath("../outside.yaml"); err == nil {
		t.Fatal("expected repo escape to be rejected")
	}
	if _, err := s.validateConfigPath("bad.txt"); err == nil {
		t.Fatal("expected non-yaml config to be rejected")
	}
}

func TestValidateScheduleInput(t *testing.T) {
	cases := []struct {
		name    string
		state   scheduleState
		wantErr bool
	}{
		{name: "empty action=create rejects", state: scheduleState{Type: "auto", Action: "create"}, wantErr: true},
		{name: "create with cron ok", state: scheduleState{Type: "cron", Action: "create", Cron: "*/5 * * * *"}, wantErr: false},
		{name: "create with every ok", state: scheduleState{Type: "auto", Action: "create", Every: "15m"}, wantErr: false},
		{name: "list without cron ok", state: scheduleState{Type: "auto", Action: "list"}, wantErr: false},
		{name: "remove without cron ok", state: scheduleState{Type: "auto", Action: "remove"}, wantErr: false},
		{name: "bad type", state: scheduleState{Type: "garbage", Action: "create", Cron: "*/5 * * * *"}, wantErr: true},
		{name: "bad action", state: scheduleState{Type: "cron", Action: "exec", Cron: "*/5 * * * *"}, wantErr: true},
		{name: "cron with semicolon", state: scheduleState{Type: "cron", Action: "create", Cron: "*/5 * * * *;rm -rf /"}, wantErr: true},
		{name: "every bad pattern", state: scheduleState{Type: "auto", Action: "create", Every: "fast"}, wantErr: true},
		{name: "task name unsafe", state: scheduleState{Type: "auto", Action: "create", Every: "15m", TaskName: "ncc;evil"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateScheduleInput(tc.state)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestSanitizeExtraArgs(t *testing.T) {
	got, err := sanitizeExtraArgs([]string{"--outputs", "csv,json", "--dry-run", "--quiet-hours", "22:00-06:00"})
	if err != nil {
		t.Fatalf("sanitizeExtraArgs returned error for safe args: %v", err)
	}
	if want := []string{"--outputs", "csv,json", "--dry-run", "--quiet-hours", "22:00-06:00"}; strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("sanitizeExtraArgs = %q, want %q", got, want)
	}
	if _, err := sanitizeExtraArgs([]string{"--unknown", "x"}); err == nil {
		t.Fatal("expected unknown arg to fail")
	}
	if _, err := sanitizeExtraArgs([]string{"--prom-dir", "x;rm -rf /"}); err == nil {
		t.Fatal("expected unsafe value to fail")
	}
	// --quiet-hours takes a value on the runner; a bare flag must be rejected
	// here rather than failing the run with "flag needs an argument".
	if _, err := sanitizeExtraArgs([]string{"--quiet-hours"}); err == nil {
		t.Fatal("expected --quiet-hours without a value to fail")
	}
	// Formerly allowlisted names the runner never had now fail validation with
	// a hint instead of starting a run that exits with "unknown flag".
	for _, retired := range []string{"--no-html", "--output-dir", "--log-dir", "--cluster-file", "--compare-with", "--maintenance-window", "--gen-test-agg"} {
		_, err := sanitizeExtraArgs([]string{retired, "x"})
		if err == nil || !strings.Contains(err.Error(), "not a runner flag") {
			t.Fatalf("%s: want a 'not a runner flag' error, got %v", retired, err)
		}
	}
}

// Every allowlisted extra arg must be a flag the runner's root command
// actually defines; this is the drift that let --no-html et al. through.
func TestExtraArgsAllowlistMatchesRunnerFlags(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "goNCC.go"))
	if err != nil {
		t.Skipf("runner source not available: %v", err)
	}
	for flagName := range extraArgsAllowed {
		name := strings.TrimPrefix(flagName, "--")
		if !regexp.MustCompile(`cmd\.Flags\(\)\.\w+\("` + regexp.QuoteMeta(name) + `"`).Match(src) {
			t.Errorf("allowlisted %s is not defined on the runner command", flagName)
		}
	}
}

func TestConfineExtraArgPaths(t *testing.T) {
	root := t.TempDir()
	s := &apiServer{repoRoot: root}
	out, err := s.confineExtraArgPaths([]string{"--prom-dir", "outputfiles/prom", "--outputs", "csv"})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(out[1]) || !isWithinBase(mustEvalSymlinks(t, root), out[1]) {
		t.Fatalf("prom-dir not confined under the repo root: %q", out[1])
	}
	if out[3] != "csv" {
		t.Fatalf("non-path value changed: %q", out[3])
	}
	if _, err := s.confineExtraArgPaths([]string{"--prom-dir", "../../etc"}); err == nil {
		t.Fatal("expected a prom-dir outside the repo root to be rejected")
	}
}

func mustEvalSymlinks(t *testing.T, p string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSessionTokenRoundTrip(t *testing.T) {
	s := &apiServer{
		sessionSecret: "supersecret",
		sessionTTL:    2 * time.Minute,
		sessionIssuer: "ncc-api-server",
	}
	tok, _, err := s.issueSessionToken("127.0.0.1")
	if err != nil {
		t.Fatalf("issueSessionToken failed: %v", err)
	}
	if err := s.verifySessionToken(tok, "127.0.0.1"); err != nil {
		t.Fatalf("verifySessionToken failed: %v", err)
	}
	if err := s.verifySessionToken(tok, "10.1.1.1"); err == nil {
		t.Fatal("expected client IP mismatch to fail")
	}
}

func TestFixedWindowRateLimiterEvictsExpiredBuckets(t *testing.T) {
	l := newFixedWindowRateLimiter(1, time.Second)
	base := time.Unix(100, 0).UTC()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("client-%d", i)
		ok, _ := l.allow(key, base)
		if !ok {
			t.Fatalf("expected key %q to pass on first request", key)
		}
	}
	if got := len(l.buckets); got != 100 {
		t.Fatalf("expected 100 buckets before cleanup, got %d", got)
	}

	ok, _ := l.allow("fresh-client", base.Add(2*time.Second))
	if !ok {
		t.Fatal("expected fresh client to pass after window roll")
	}
	if got := len(l.buckets); got != 1 {
		t.Fatalf("expected expired buckets to be evicted, got %d buckets", got)
	}
}

func TestRateLimiterStatsCounters(t *testing.T) {
	l := newFixedWindowRateLimiter(1, time.Minute)
	now := time.Unix(200, 0).UTC()

	ok, _ := l.allow("127.0.0.1", now)
	if !ok {
		t.Fatal("expected first request to be allowed")
	}
	ok, _ = l.allow("127.0.0.1", now.Add(1*time.Second))
	if ok {
		t.Fatal("expected second request to be blocked")
	}

	st := l.stats(now.Add(2 * time.Second))
	if st.AllowedTotal != 1 {
		t.Fatalf("expected allowed_total=1, got %d", st.AllowedTotal)
	}
	if st.BlockedTotal != 1 {
		t.Fatalf("expected blocked_total=1, got %d", st.BlockedTotal)
	}
	if st.ActiveBuckets != 1 {
		t.Fatalf("expected active_buckets=1, got %d", st.ActiveBuckets)
	}
}

func TestSanitizeExtraArgsChecksChoices(t *testing.T) {
	if _, err := sanitizeExtraArgs([]string{"--outputs", "csv,pdf"}); err == nil || !strings.Contains(err.Error(), `"pdf"`) {
		t.Fatalf("want a choices error naming pdf, got %v", err)
	}
	if _, err := sanitizeExtraArgs([]string{"--severity-filter", "fail,warn"}); err != nil {
		t.Fatalf("severities are case-insensitive: %v", err)
	}
}

// The published run options must be exactly the allowlist, so the UI can
// never offer a flag the server rejects (or the runner lacks).
func TestRunExtraArgsEndpointMatchesAllowlist(t *testing.T) {
	s := &apiServer{}
	rr := httptest.NewRecorder()
	s.handleRunExtraArgs(rr, httptest.NewRequest(http.MethodGet, "/api/v1/runs/extra-args", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	var env struct {
		Data struct {
			Options []extraArgSpec `json:"options"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Data.Options) != len(extraArgsAllowed) {
		t.Fatalf("published %d options, allowlist has %d", len(env.Data.Options), len(extraArgsAllowed))
	}
	for _, o := range env.Data.Options {
		takes, ok := extraArgsAllowed[o.Flag]
		if !ok || takes != o.TakesValue || o.Label == "" {
			t.Fatalf("option %+v does not match the allowlist", o)
		}
		if o.Example != "" && o.TakesValue {
			if _, err := sanitizeExtraArgs([]string{o.Flag, o.Example}); err != nil {
				t.Fatalf("published example for %s is rejected: %v", o.Flag, err)
			}
		}
	}
}
