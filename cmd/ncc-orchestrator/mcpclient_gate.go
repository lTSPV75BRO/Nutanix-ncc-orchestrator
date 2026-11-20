package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"goncc/internal/mcpclient"
	"goncc/internal/procutil"
)

// mcpVerifyAPI is replaced in tests. Production verifies the PAT with the API
// so a spoofed version string and a dummy token cannot open the MCP path.
var mcpVerifyAPI = verifyMCPTokenAgainstAPI

// denyLegacyMCPClient refuses MCP-originated work unless the caller presents a
// 2.4.0+ HMAC proof keyed by NCC_MCP_TOKEN and that token authenticates to the
// API. Self-reported versions are not enough.
func denyLegacyMCPClient() error {
	version := strings.TrimSpace(os.Getenv(mcpclient.VersionEnv))
	proof := strings.TrimSpace(os.Getenv(mcpclient.ProofEnv))
	fromParent := parentIsMCPServer()
	if version == "" && proof == "" && !fromParent {
		return nil
	}
	token := strings.TrimSpace(os.Getenv(mcpclient.TokenEnv))
	if err := mcpclient.VerifyProof(token, version, proof, time.Now().UTC()); err != nil {
		return err
	}
	if err := mcpVerifyAPI(token); err != nil {
		return err
	}
	return nil
}

func verifyMCPTokenAgainstAPI(token string) error {
	raw := strings.TrimSpace(os.Getenv("NCC_MCP_API_URL"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("NCC_API_URL"))
	}
	if raw == "" {
		raw = "http://127.0.0.1:8081"
	}
	return mcpclient.LookupIdentity(raw, token, strings.TrimSpace(os.Getenv(mcpclient.VersionEnv)), strings.TrimSpace(os.Getenv(mcpclient.ProofEnv)))
}

func parentIsMCPServer() bool {
	name := parentProcessBaseName(os.Getppid())
	name = strings.ToLower(strings.TrimSuffix(filepath.Base(name), ".exe"))
	return name == "ncc-mcp-server"
}

func parentProcessBaseName(pid int) string {
	if pid <= 0 {
		return ""
	}
	if runtime.GOOS == "windows" {
		// There is no ps on Windows, so without this the parent check never
		// matched and a legacy MCP server was not recognised there.
		image, err := procutil.ImagePath(pid)
		if err != nil {
			return ""
		}
		return filepath.Base(image)
	}
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile(filepath.Join("/proc", fmt.Sprintf("%d", pid), "comm"))
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(data))
	}
	out, err := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "comm=").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
