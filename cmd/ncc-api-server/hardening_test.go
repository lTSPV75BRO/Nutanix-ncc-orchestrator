package main

import (
	"fmt"
	"path/filepath"
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
	_, err := sanitizeExtraArgs([]string{"--output-dir", "outputfiles", "--gen-test-agg"})
	if err != nil {
		t.Fatalf("sanitizeExtraArgs returned error for safe args: %v", err)
	}
	if _, err := sanitizeExtraArgs([]string{"--unknown", "x"}); err == nil {
		t.Fatal("expected unknown arg to fail")
	}
	if _, err := sanitizeExtraArgs([]string{"--output-dir", "x;rm -rf /"}); err == nil {
		t.Fatal("expected unsafe value to fail")
	}
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
