package main

import (
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
	if want := filepath.Join(repo, "configs", "test.yaml"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if _, err := s.validateConfigPath("../outside.yaml"); err == nil {
		t.Fatal("expected repo escape to be rejected")
	}
	if _, err := s.validateConfigPath("bad.txt"); err == nil {
		t.Fatal("expected non-yaml config to be rejected")
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
