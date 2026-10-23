package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"goncc/internal/selfsigned"
)

func TestBuildUIHealthProbeArgsPassesRealTLSPaths(t *testing.T) {
	if got := buildUIHealthProbeArgs("", ":8080", "", ""); got != nil {
		t.Fatalf("empty bin must yield nil, got %v", got)
	}
	plain := strings.Join(buildUIHealthProbeArgs("/opt/ncc/ncc-ui-server", ":8080", "", ""), " ")
	if !strings.Contains(plain, "--health-check") || strings.Contains(plain, "--tls-cert-file") {
		t.Fatalf("http probe: %q", plain)
	}
	if got := strings.Join(buildUIHealthProbeArgs("/opt/ncc/ncc-ui-server", ":8443", "/opt/ncc/tls/ui.crt", ""), " "); strings.Contains(got, "--tls-cert-file") {
		t.Fatalf("cert without key does not enable TLS on the server, so the probe must stay http: %q", got)
	}
	tls := buildUIHealthProbeArgs("/opt/ncc/ncc-ui-server", ":8443", "/opt/ncc/tls/ui.crt", "/opt/ncc/tls/ui.key")
	want := []string{"--tls-cert-file", "/opt/ncc/tls/ui.crt", "--tls-key-file", "/opt/ncc/tls/ui.key"}
	if strings.Join(tls[len(tls)-4:], " ") != strings.Join(want, " ") {
		t.Fatalf("https probe must carry the served cert/key paths, got %v", tls)
	}
}

// Regression: the supervisor's UI probe passed placeholder cert paths
// ("tls.crt") while --health-check verifies the server against the given
// certificate, so every probe failed and the supervisor kept killing a healthy
// TLS UI until its restart budget ran out. This runs the real supervisor over
// a real ncc-ui-server serving TLS and checks the UI is never restarted.
func TestSuperviseKeepsHealthyTLSUIRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs ncc-ui-server")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not on PATH")
	}
	dir := t.TempDir()
	uiBin := filepath.Join(dir, "ncc-ui-server")
	if runtime.GOOS == "windows" {
		uiBin += ".exe"
	}
	if out, err := exec.Command(goBin, "build", "-buildvcs=false", "-o", uiBin, "./cmd/ncc-ui-server").CombinedOutput(); err != nil {
		t.Fatalf("build ncc-ui-server: %v\n%s", err, out)
	}
	certPEM, keyPEM, err := selfsigned.Generate([]string{"localhost", "127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, "ui.crt"), filepath.Join(dir, "ui.key")
	static := filepath.Join(dir, "dist")
	for path, data := range map[string][]byte{certFile: certPEM, keyFile: keyPEM, filepath.Join(static, "index.html"): []byte("<!doctype html><title>ok</title>")} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen := ln.Addr().String()
	_ = ln.Close()

	child := &superviseChild{
		name: "ui",
		bin:  uiBin,
		args: []string{"--listen", listen, "--dir", static, "--tls-cert-file", certFile, "--tls-key-file", keyFile,
			"--backend-url", "http://127.0.0.1:1", "--api-token", "supervise-test-token",
			// Skip backend login detection: against an unreachable backend it
			// delays the first listen by ~10s on Windows, which this test's
			// short probe timing would read as a hang.
			"--login-mode", "off"},
		pidPath:    filepath.Join(dir, "ui.pid"),
		logPath:    filepath.Join(dir, "ui.log"),
		healthArgs: buildUIHealthProbeArgs(uiBin, listen, certFile, keyFile),
	}
	cfg := superviseConfig{
		installDir:  dir,
		children:    []*superviseChild{child},
		maxRestarts: 3,
		window:      time.Minute,
		// runHealthProbe also uses the interval as the probe timeout, so keep
		// it long enough for a probe process to start and finish a TLS check.
		probeInterval:      2 * time.Second,
		unhealthyThreshold: 2,
		initialBackoff:     50 * time.Millisecond,
	}
	cfg.applyDefaults()

	var mu sync.Mutex
	var events []string
	logf := func(format string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, strings.TrimSpace(fmt.Sprintf(format, a...)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { superviseRun(ctx, &cfg, logf); close(done) }()
	<-done

	mu.Lock()
	defer mu.Unlock()
	starts := 0
	for _, e := range events {
		if strings.Contains(e, "unhealthy threshold") || strings.Contains(e, "exhausted restarts") {
			t.Fatalf("supervisor restarted a healthy TLS UI; events:\n%s", strings.Join(events, "\n"))
		}
		if strings.Contains(e, "ui started pid=") {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("UI should start once and stay up, started %d times; events:\n%s", starts, strings.Join(events, "\n"))
	}
}
