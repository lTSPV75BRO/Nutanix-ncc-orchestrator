package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func readyServer(t *testing.T) *apiServer {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "ncc-orchestrator")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &apiServer{repoRoot: root, outputDir: "outputfiles", orchestratorBin: bin}
}

func getReady(t *testing.T, s *apiServer) (int, readinessResult, string) {
	t.Helper()
	rr := httptest.NewRecorder()
	s.withAuth(http.HandlerFunc(s.handleReady)).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/health/ready", nil))
	var env struct {
		Data readinessResult `json:"data"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	return rr.Code, env.Data, rr.Body.String()
}

func TestReadinessOKWithoutCredentials(t *testing.T) {
	s := readyServer(t)
	code, res, _ := getReady(t, s)
	if code != http.StatusOK || !res.Ready {
		t.Fatalf("want 200 ready, got %d %+v", code, res)
	}
}

func TestReadinessFailsWithoutRunnerAndHidesPaths(t *testing.T) {
	s := readyServer(t)
	s.orchestratorBin = filepath.Join(s.repoRoot, "missing", "ncc-orchestrator")
	code, res, body := getReady(t, s)
	if code != http.StatusServiceUnavailable || res.Ready {
		t.Fatalf("want 503 not ready, got %d %+v", code, res)
	}
	failed := ""
	for _, c := range res.Checks {
		if !c.OK {
			failed = c.Name
		}
	}
	if failed != "runner_binary" {
		t.Fatalf("failing check = %q, want runner_binary", failed)
	}
	if strings.Contains(body, s.repoRoot) || strings.Contains(body, "missing") {
		t.Fatalf("unauthenticated readiness must not disclose paths: %s", body)
	}
}
