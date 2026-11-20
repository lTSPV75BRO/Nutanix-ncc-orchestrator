package main

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"goncc/internal/mcpclient"
)

func TestDenyLegacyMCPClientAllowsNormalCLI(t *testing.T) {
	t.Setenv(mcpclient.VersionEnv, "")
	t.Setenv(mcpclient.ProofEnv, "")
	t.Setenv(mcpclient.TokenEnv, "")
	if parentIsMCPServer() {
		t.Fatalf("test process parent %q must not look like ncc-mcp-server", parentProcessBaseName(os.Getppid()))
	}
	if err := denyLegacyMCPClient(); err != nil {
		t.Fatal(err)
	}
}

func TestDenyLegacyMCPClientRejectsOldVersion(t *testing.T) {
	t.Setenv(mcpclient.VersionEnv, "2.3.0")
	t.Setenv(mcpclient.ProofEnv, "")
	t.Setenv(mcpclient.TokenEnv, "ncc_pat_testtoken12")
	err := denyLegacyMCPClient()
	if err == nil || !errors.Is(err, mcpclient.ErrUpgrade) {
		t.Fatalf("got %v", err)
	}
}

func TestDenyLegacyMCPClientRejectsStampedVersionWithoutProof(t *testing.T) {
	t.Setenv(mcpclient.VersionEnv, "2.4.0")
	t.Setenv(mcpclient.ProofEnv, "")
	t.Setenv(mcpclient.TokenEnv, "ncc_pat_testtoken12")
	if err := denyLegacyMCPClient(); err == nil {
		t.Fatal("ldflags/env version stamp without HMAC must be refused")
	}
}

func TestDenyLegacyMCPClientRejectsForgedProof(t *testing.T) {
	t.Setenv(mcpclient.VersionEnv, "2.4.0")
	t.Setenv(mcpclient.ProofEnv, "v1.1.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	t.Setenv(mcpclient.TokenEnv, "ncc_pat_testtoken12")
	if err := denyLegacyMCPClient(); err == nil || !errors.Is(err, mcpclient.ErrInvalidProof) {
		t.Fatalf("got %v", err)
	}
}

func TestDenyLegacyMCPClientAcceptsSignedProofWhenAPIOK(t *testing.T) {
	token := "ncc_pat_testtoken12"
	proof, err := mcpclient.SignProof(token, "2.4.0", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(mcpclient.VersionEnv, "2.4.0")
	t.Setenv(mcpclient.ProofEnv, proof)
	t.Setenv(mcpclient.TokenEnv, token)
	orig := mcpVerifyAPI
	t.Cleanup(func() { mcpVerifyAPI = orig })
	mcpVerifyAPI = func(string) error { return nil }
	if err := denyLegacyMCPClient(); err != nil {
		t.Fatal(err)
	}
}

func TestDenyLegacyMCPClientFailsClosedWhenAPIRejects(t *testing.T) {
	token := "ncc_pat_testtoken12"
	proof, err := mcpclient.SignProof(token, "2.4.0", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(mcpclient.VersionEnv, "2.4.0")
	t.Setenv(mcpclient.ProofEnv, proof)
	t.Setenv(mcpclient.TokenEnv, token)
	orig := mcpVerifyAPI
	t.Cleanup(func() { mcpVerifyAPI = orig })
	mcpVerifyAPI = func(string) error { return errors.New("authentication required") }
	if err := denyLegacyMCPClient(); err == nil || !strings.Contains(err.Error(), "authentication required") {
		t.Fatalf("got %v", err)
	}
}
