package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goncc/internal/runtimecaps"
)

// buildAPIHealthProbeCmd returns a shell command that runs the api-server's
// built-in `--health-check` self-probe (GET /api/v1/health over loopback,
// exit 0 healthy / 1 unhealthy). The supervisor runs this against a still-alive
// api-server to detect hangs the liveness check cannot. Returns "" if the
// binary path is empty.
func buildAPIHealthProbeCmd(apiBin, listen, tokenFile, repoRoot string) string {
	if strings.TrimSpace(apiBin) == "" {
		return ""
	}
	parts := []string{shellQuote(apiBin), "--health-check"}
	if strings.TrimSpace(listen) != "" {
		parts = append(parts, "--listen", shellQuote(listen))
	}
	if strings.TrimSpace(tokenFile) != "" {
		parts = append(parts, "--token-file-path", shellQuote(tokenFile))
	}
	if strings.TrimSpace(repoRoot) != "" {
		parts = append(parts, "--repo-root", shellQuote(repoRoot))
	}
	return strings.Join(parts, " ")
}

// buildUIHealthProbeCmd returns a shell command that runs the ui-server's
// built-in `--health-check` self-probe (GET / over loopback, exit 0 healthy / 1
// unhealthy) so the supervisor can detect a hung UI process, not just a crashed
// one. tls toggles the https scheme. Returns "" if the binary path is empty.
func buildUIHealthProbeCmd(uiBin, listen, tlsCertFile, tlsKeyFile string) string {
	if strings.TrimSpace(uiBin) == "" {
		return ""
	}
	parts := []string{shellQuote(uiBin), "--health-check"}
	if strings.TrimSpace(listen) != "" {
		parts = append(parts, "--listen", shellQuote(listen))
	}
	if strings.TrimSpace(tlsCertFile) != "" && strings.TrimSpace(tlsKeyFile) != "" {
		// The cert/key flags switch the probe to https, and the probe verifies
		// the server against that certificate, so pass the real paths the UI
		// serves. A placeholder fails every probe and the supervisor would
		// keep restarting a healthy UI.
		parts = append(parts, "--tls-cert-file", shellQuote(tlsCertFile), "--tls-key-file", shellQuote(tlsKeyFile))
	}
	return strings.Join(parts, " ")
}

// selfHealSupervisorScript builds the POSIX-sh supervisor loop. It is split out
// from process launch so its behavior can be unit-tested (the generated script
// is asserted by TestSelfHealSupervisorScript). healthCmd is an optional
// shell command run against a *still-alive* process to detect hangs/deadlocks
// (empty = liveness-only). probeIntervalSec/unhealthyThreshold gate that probe.
func selfHealSupervisorScript(serviceName, cmdLine, pidPath, logPath, healthCmd string, maxRestarts, windowSeconds, probeIntervalSec, unhealthyThreshold int) string {
	return strings.Join([]string{
		"set -eu",
		"MAX_RESTARTS=" + strconv.Itoa(maxRestarts),
		"WINDOW_SECONDS=" + strconv.Itoa(windowSeconds),
		"PROBE_INTERVAL=" + strconv.Itoa(probeIntervalSec),
		"UNHEALTHY_MAX=" + strconv.Itoa(unhealthyThreshold),
		// Cap exponential restart backoff and cool down (then resume) after the
		// restart budget is exhausted instead of giving up permanently, so a
		// prolonged-but-transient fault (disk full, downstream outage) still
		// self-heals once it clears.
		"BACKOFF_CAP=30",
		"COOLDOWN_SECONDS=$WINDOW_SECONDS",
		"PID_FILE=" + shellQuote(pidPath),
		"LOG_FILE=" + shellQuote(logPath),
		"CMD=" + shellQuote(cmdLine),
		"HEALTH_CMD=" + shellQuote(healthCmd),
		// SERVICE_NAME is passed as a shell-quoted variable (not concatenated
		// into the echo lines) so a name containing quotes or $(...) cannot
		// break out of the generated script.
		"SERVICE_NAME=" + shellQuote(serviceName),
		"log() { echo \"$(date -u +%Y-%m-%dT%H:%M:%SZ) $SERVICE_NAME $1\" >> \"$LOG_FILE\"; }",
		"restarts=0",
		"window_start=$(date +%s)",
		"unhealthy=0",
		"backoff=1",
		"last_probe=0",
		"while true; do",
		"  pid=\"\"",
		"  if [ -f \"$PID_FILE\" ]; then",
		"    pid=$(sed -n '1p' \"$PID_FILE\" | tr -d '\\r\\n ' || true)",
		"  fi",
		"  if [ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null; then",
		// Process is alive. If a health probe is configured, run it on the
		// configured interval; consecutive failures mean the process is hung
		// (alive but not serving) so kill it and let the restart path fire.
		"    if [ -n \"$HEALTH_CMD\" ]; then",
		"      now=$(date +%s)",
		"      if [ $((now-last_probe)) -ge $PROBE_INTERVAL ]; then",
		"        last_probe=$now",
		"        if sh -c \"$HEALTH_CMD\" >/dev/null 2>&1; then",
		"          unhealthy=0",
		"        else",
		"          unhealthy=$((unhealthy+1))",
		"          log \"health probe failed ($unhealthy/$UNHEALTHY_MAX) pid=$pid\"",
		"          if [ $unhealthy -ge $UNHEALTHY_MAX ]; then",
		"            log \"unhealthy threshold reached; restarting pid=$pid\"",
		"            kill \"$pid\" 2>/dev/null || true",
		"            sleep 2",
		"            kill -0 \"$pid\" 2>/dev/null && kill -9 \"$pid\" 2>/dev/null || true",
		"            unhealthy=0",
		"            continue",
		"          fi",
		"        fi",
		"      fi",
		"    fi",
		"    sleep 2",
		"    continue",
		"  fi",
		"  now=$(date +%s)",
		"  if [ $((now-window_start)) -gt $WINDOW_SECONDS ]; then",
		"    window_start=$now",
		"    restarts=0",
		"    backoff=1",
		"  fi",
		"  restarts=$((restarts+1))",
		"  if [ $restarts -gt $MAX_RESTARTS ]; then",
		"    log \"self-heal exhausted restarts ($MAX_RESTARTS within ${WINDOW_SECONDS}s); cooling down ${COOLDOWN_SECONDS}s before resuming\"",
		"    sleep \"$COOLDOWN_SECONDS\"",
		"    window_start=$(date +%s)",
		"    restarts=0",
		"    backoff=1",
		"    unhealthy=0",
		"    continue",
		"  fi",
		"  sh -c \"$CMD\" >> \"$LOG_FILE\" 2>&1 &",
		"  newpid=$!",
		"  echo \"$newpid\" > \"$PID_FILE\"",
		"  log \"self-heal restart #$restarts pid=$newpid (backoff ${backoff}s)\"",
		"  unhealthy=0",
		"  last_probe=$(date +%s)",
		"  sleep \"$backoff\"",
		"  backoff=$((backoff*2))",
		"  if [ $backoff -gt $BACKOFF_CAP ]; then backoff=$BACKOFF_CAP; fi",
		"done",
	}, "\n")
}

func startSelfHealSupervisor(serviceName string, bin string, args []string, pidPath string, logPath string, maxRestarts int, window time.Duration, healthCmd string, probeInterval time.Duration, unhealthyThreshold int) (int, error) {
	if runtime.GOOS == "windows" {
		return 0, fmt.Errorf("self-heal supervisor is currently unsupported on windows")
	}
	if maxRestarts <= 0 {
		maxRestarts = 3
	}
	if window <= 0 {
		window = 5 * time.Minute
	}
	windowSeconds := int(window.Seconds())
	if windowSeconds < 30 {
		windowSeconds = 30
	}
	probeIntervalSec := int(probeInterval.Seconds())
	if probeIntervalSec < 1 {
		probeIntervalSec = 10
	}
	if unhealthyThreshold < 1 {
		unhealthyThreshold = 3
	}
	quotedArgs := make([]string, 0, len(args))
	for _, a := range args {
		quotedArgs = append(quotedArgs, shellQuote(a))
	}
	cmdLine := shellQuote(bin)
	if len(quotedArgs) > 0 {
		cmdLine += " " + strings.Join(quotedArgs, " ")
	}
	script := selfHealSupervisorScript(serviceName, cmdLine, pidPath, logPath, healthCmd, maxRestarts, windowSeconds, probeIntervalSec, unhealthyThreshold)
	monitorCmd := exec.Command("sh", "-c", script)
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer devNull.Close()
	monitorCmd.Stdin = devNull
	monitorCmd.Stdout = devNull
	monitorCmd.Stderr = devNull
	if err := monitorCmd.Start(); err != nil {
		return 0, err
	}
	pid := monitorCmd.Process.Pid
	_ = monitorCmd.Process.Release()
	return pid, nil
}

// systemdStackIsActive reports whether the installed stack service is
// currently active. v2-start/v2-restart must not launch a second detached
// runtime beside the native systemd supervisor: both copies would race for
// :8080/:8081 and the supervisor would only report opaque exit status 1s.
func systemdStackIsActive() bool {
	if runtime.GOOS == "windows" {
		return false
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	return exec.Command("systemctl", "is-active", "--quiet", "ncc-orchestrator.service").Run() == nil
}

func restartSystemdStack() error {
	out, err := exec.Command("systemctl", "restart", "ncc-orchestrator.service").CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("restart ncc-orchestrator.service: %s", msg)
	}
	return nil
}

// ensureV2StartSlotsAvailable prevents a second detached runtime from being
// created after a stale/partial stop. Stale pid files are safe to remove; a
// live PID or an occupied listen address is never killed automatically because
// it may belong to an unrelated service.
func ensureV2StartSlotsAvailable(installDir string, opts v2StartOptions) error {
	runDir := filepath.Join(installDir, "run")
	pids := []struct {
		name     string
		path     string
		identity []string
	}{
		{"supervisor", filepath.Join(runDir, "v2-supervisor.pid"), []string{"ncc-orchestrator", "v2-supervise"}},
		{"api", filepath.Join(runDir, "v2-api.pid"), []string{"ncc-api-server"}},
	}
	if !opts.APIOnly {
		pids = append(pids, struct {
			name     string
			path     string
			identity []string
		}{"ui", filepath.Join(runDir, "v2-ui.pid"), []string{"ncc-ui-server"}})
	}
	for _, item := range pids {
		pid, err := readPIDFromFile(item.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot inspect %s pid file: %w", item.name, err)
		}
		if processIsExpected(pid, item.identity...) {
			return fmt.Errorf("%s is already running (pid %d); stop it with `ncc-orchestrator v2-stop --install-dir %s` before starting another stack", item.name, pid, installDir)
		}
		// A live PID with a known non-NCC command is a reused PID. Do not
		// signal it; remove only the stale metadata and continue.
		_ = os.Remove(item.path)
	}
	if err := canBindListenAddress(opts.APIListen); err != nil {
		return fmt.Errorf("api listen address %s is unavailable: %w (another process may already own the port)", opts.APIListen, err)
	}
	if !opts.APIOnly {
		if err := canBindListenAddress(opts.UIListen); err != nil {
			return fmt.Errorf("ui listen address %s is unavailable: %w (another process may already own the port)", opts.UIListen, err)
		}
	}
	return nil
}

func runV2Start(opts v2StartOptions) error {
	if err := runtimecaps.Detect().RejectHostOperation("local stack start/restart"); err != nil {
		return err
	}
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if absInstallDir, err := filepath.Abs(installDir); err == nil {
		installDir = absInstallDir
	}
	if opts.Detach && !opts.Supervise && systemdStackIsActive() {
		return fmt.Errorf("ncc-orchestrator.service is already active; use `systemctl restart ncc-orchestrator.service` or `ncc-orchestrator v2-restart` instead of starting a detached duplicate")
	}
	if !opts.Supervise && resumeInterruptedUpgrade(installDir) {
		fmt.Println("An interrupted upgrade is being finished by the update helper, which also starts the stack. Follow it with: ncc-orchestrator upgrade --status")
		return nil
	}
	// Record the portable start settings so backups carry them and an
	// orchestrator-managed restart (after a restore, or via v2-restart) reuses
	// the same CORS/listen/session configuration. Best-effort; never block the
	// start on a state-write failure.
	if err := writeV2StartState(installDir, opts); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not persist v2-start settings: %v\n", err)
	}
	recordTrustedStackQuiet(installDir, "start")
	apiOnly := opts.APIOnly
	var (
		apiBin      string
		uiBin       string
		frontendDir string
		layout      string
	)
	if apiOnly {
		apiBin, layout = resolveV2APIBinary(installDir)
		if apiBin == "" {
			return fmt.Errorf("could not locate API runtime binary. expected either %s (from v2-bootstrap), or local release asset ncc-api-server-%s-%s (run `ncc-orchestrator v2-bootstrap` first)",
				binaryPathInInstallDir(installDir, "ncc-api-server"), runtime.GOOS, runtime.GOARCH,
			)
		}
	} else {
		apiBin, uiBin, frontendDir, layout = resolveV2RuntimeLayout(installDir)
		if apiBin == "" || uiBin == "" || frontendDir == "" {
			return fmt.Errorf("could not locate v2 runtime assets. expected either %s + %s + %s (from v2-bootstrap), or local release assets like ncc-api-server-%s-%s, ncc-ui-server-%s-%s, and frontend-dist/ (run `ncc-orchestrator v2-bootstrap` first)",
				binaryPathInInstallDir(installDir, "ncc-api-server"),
				binaryPathInInstallDir(installDir, "ncc-ui-server"),
				filepath.Join(installDir, "frontend-dist"),
				runtime.GOOS, runtime.GOARCH, runtime.GOOS, runtime.GOARCH,
			)
		}
	}

	// Default the runtime paths relative to the resolved install-dir (so
	// `bin/ncc-orchestrator v2-start` from inside a stack picks colocated
	// config/outputs/logs instead of paths under bin/).
	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = filepath.Join(installDir, "config.yaml")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		opts.OutputDir = filepath.Join(installDir, "outputfiles")
	}
	if strings.TrimSpace(opts.LogDir) == "" {
		opts.LogDir = filepath.Join(installDir, "nccfiles")
	}
	if strings.TrimSpace(opts.OrchestratorBin) == "" {
		opts.OrchestratorBin = "./ncc-orchestrator"
	}
	opts.OrchestratorBin = resolveV2OrchestratorBin(opts.OrchestratorBin)
	if strings.TrimSpace(opts.APIListen) == "" {
		opts.APIListen = ":8081"
	}
	if strings.TrimSpace(opts.UIListen) == "" {
		opts.UIListen = ":8080"
	}
	if strings.TrimSpace(opts.TokenFile) == "" {
		opts.TokenFile = filepath.Join(installDir, ".ncc-api-token")
	}
	if absConfigPath, err := filepath.Abs(opts.ConfigPath); err == nil {
		opts.ConfigPath = absConfigPath
	}
	if absOutputDir, err := filepath.Abs(opts.OutputDir); err == nil {
		opts.OutputDir = absOutputDir
	}
	if absLogDir, err := filepath.Abs(opts.LogDir); err == nil {
		opts.LogDir = absLogDir
	}
	if absTokenPath, err := filepath.Abs(opts.TokenFile); err == nil {
		opts.TokenFile = absTokenPath
	}
	// Pin the writable user database to the install dir so local accounts,
	// roles, and SSO config survive v2-stop / v2-start regardless of the
	// process's working directory (without this the api-server falls back to a
	// CWD-relative default that can differ between launches).
	if strings.TrimSpace(opts.UsersDB) == "" {
		opts.UsersDB = filepath.Join(installDir, ".ncc-api-users.json")
	}
	if absUsersDB, err := filepath.Abs(opts.UsersDB); err == nil {
		opts.UsersDB = absUsersDB
	}
	// Same example_config.yaml fallback as v2-check: if config.yaml is
	// missing but the install ships an example, prefer that with a
	// stderr warning rather than failing on a fresh extraction.
	if _, err := os.Stat(opts.ConfigPath); err != nil {
		exampleCfg := filepath.Join(installDir, "example_config.yaml")
		if _, exErr := os.Stat(exampleCfg); exErr == nil {
			fmt.Fprintf(os.Stderr, "warning: config-path %s not found; falling back to %s (replace with your own config before production use)\n", opts.ConfigPath, exampleCfg)
			opts.ConfigPath = exampleCfg
		}
	}
	if cfg, err := loadConfigForValidation(opts.ConfigPath); err == nil {
		pwd := strings.TrimSpace(cfg.Password)
		if pwd != "" && !strings.HasPrefix(strings.ToLower(pwd), "secret://") {
			fmt.Fprintln(os.Stderr, "warning: config contains plaintext password; prefer NCC_PASSWORD env or secret:// source")
		}
	}
	if strings.TrimSpace(opts.APIAuthMode) == "" {
		opts.APIAuthMode = "token"
	}
	if opts.APISessionTTL <= 0 {
		opts.APISessionTTL = 10 * time.Minute
	}
	if opts.APIRunTimeout <= 0 {
		opts.APIRunTimeout = 90 * time.Minute
	}
	if opts.APIRateLimitPerMinute < 0 {
		return fmt.Errorf("api-rate-limit-per-minute must be >= 0")
	}
	if opts.APIReadTimeout <= 0 {
		opts.APIReadTimeout = 15 * time.Second
	}
	if opts.APIWriteTimeout <= 0 {
		opts.APIWriteTimeout = 60 * time.Second
	}
	if opts.APIIdleTimeout <= 0 {
		opts.APIIdleTimeout = 60 * time.Second
	}
	if opts.ReadyTimeout <= 0 {
		opts.ReadyTimeout = 20 * time.Second
	}
	if opts.SelfHealMaxRestarts <= 0 {
		opts.SelfHealMaxRestarts = 3
	}
	if opts.SelfHealWindow <= 0 {
		opts.SelfHealWindow = 10 * time.Minute
	}
	if opts.SelfHealProbeInterval <= 0 {
		opts.SelfHealProbeInterval = 10 * time.Second
	}
	if opts.SelfHealUnhealthyThreshold <= 0 {
		opts.SelfHealUnhealthyThreshold = 3
	}
	if opts.Detach && !opts.Supervise {
		if err := ensureV2StartSlotsAvailable(installDir, opts); err != nil {
			return err
		}
	}
	if opts.SelfHeal && !opts.Detach {
		fmt.Fprintln(os.Stderr, "warning: --self-heal is effective only with --detach; continuing without detached self-heal monitor")
	}
	if strings.TrimSpace(opts.APISessionSecret) != "" && strings.TrimSpace(opts.APISessionSecretFile) != "" {
		return fmt.Errorf("only one of api-session-secret or api-session-secret-file may be set")
	}
	if strings.TrimSpace(opts.APISessionSecretFile) != "" {
		secret, err := readTrimmedFile(opts.APISessionSecretFile)
		if err != nil {
			return fmt.Errorf("read api-session-secret-file: %w", err)
		}
		if secret == "" {
			return fmt.Errorf("api-session-secret-file is empty")
		}
		opts.APISessionSecret = secret
	}
	if opts.APIAuthMode != "token" && opts.APIAuthMode != "session" && opts.APIAuthMode != "hybrid" {
		return fmt.Errorf("api-auth-mode must be one of token, session, hybrid")
	}
	cwd, _ := os.Getwd()
	// repoRoot must contain config-path, output-dir, log-dir, token-file
	// (api-server enforces this as a path-traversal sandbox). Use the
	// install-dir-or-CWD ancestor and pre-resolve symlinks so /tmp vs
	// /private/tmp on macOS doesn't break the check.
	repoRoot := resolveV2RepoRoot(installDir, cwd)
	// Also EvalSymlinks the secondary paths so they share the prefix the
	// api-server compares against (it EvalSymlinks rootAbs but not abs).
	if real := resolveV2PathToReal(opts.ConfigPath); real != "" {
		opts.ConfigPath = real
	}
	if real := resolveV2PathToReal(opts.OutputDir); real != "" {
		opts.OutputDir = real
	}
	if real := resolveV2PathToReal(opts.LogDir); real != "" {
		opts.LogDir = real
	}
	if real := resolveV2PathToReal(opts.TokenFile); real != "" {
		opts.TokenFile = real
	}
	if real := resolveV2PathToReal(installDir); real != "" {
		installDir = real
	}
	secureInstallACLs(installDir, opts)

	// Secure-by-default: when serving the UI and the operator did not bring
	// their own certificate (or explicitly opt into plain HTTP with
	// --ui-insecure-http), mint/reuse a self-signed certificate and bind the
	// UI server to TLS. The ui-server additionally 308-redirects any plain-HTTP
	// client on the same port, so "http://…" still lands the user on HTTPS.
	// Browsers show a one-time self-signed warning, which is expected for an
	// internal IP-addressed tool. Operators can install a real cert any time
	// from Settings → Access (or with --ui-tls-cert-file/--ui-tls-key-file).
	if !opts.APIOnly &&
		strings.TrimSpace(opts.UITLSCertFile) == "" &&
		strings.TrimSpace(opts.UITLSKeyFile) == "" &&
		!opts.UIInsecureHTTP {
		certPath, keyPath, gerr := ensureDefaultUISelfSignedCert(installDir, opts)
		if gerr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not generate the default self-signed UI certificate (%v); falling back to plain HTTP. Pass --ui-tls-cert-file/--ui-tls-key-file to provide your own, or --ui-insecure-http to silence this.\n", gerr)
		} else {
			opts.UITLSCertFile = certPath
			opts.UITLSKeyFile = keyPath
			fmt.Fprintf(os.Stderr, "[tls] no UI certificate provided; serving self-signed HTTPS by default (cert: %s). Use --ui-insecure-http for plain HTTP.\n", certPath)
		}
	}
	uiTLSActive := strings.TrimSpace(opts.UITLSCertFile) != "" && strings.TrimSpace(opts.UITLSKeyFile) != ""

	backendURL := strings.TrimSpace(opts.UIBackendURL)
	if backendURL == "" {
		if strings.TrimSpace(opts.APITLSCertFile) != "" && strings.TrimSpace(opts.APITLSKeyFile) != "" {
			backendURL = strings.Replace(localHTTPURLFromListen(opts.APIListen, "8081"), "http://", "https://", 1)
		} else {
			backendURL = localHTTPURLFromListen(opts.APIListen, "8081")
		}
	}
	uiOrigin := localHTTPURLFromListen(opts.UIListen, "8080")
	// When the UI listens on a loopback IP, browsers may reach it via
	// either form (http://localhost:port OR http://127.0.0.1:port). Add
	// the alternate form so CORS doesn't reject whichever the user types.
	corsBase := uiOrigin
	if alt := loopbackAltOriginFromListen(opts.UIListen, "8080"); alt != "" && alt != uiOrigin {
		corsBase = uiOrigin + "," + alt
	}
	// When the UI serves TLS, the browser's origin is https://; reflect that in
	// the derived origins so CORS matches. (Same-origin requests are accepted
	// regardless, but keep the advertised/printed URL on the right scheme.)
	if uiTLSActive {
		uiOrigin = strings.Replace(uiOrigin, "http://", "https://", 1)
		corsBase = strings.ReplaceAll(corsBase, "http://", "https://")
	}
	allowedOrigins := mergeAllowedOriginsCSV(corsBase, opts.UIAllowedOrigins)
	apiCORSOrigins := strings.TrimSpace(opts.APICORSOrigins)
	if apiCORSOrigins == "" {
		apiCORSOrigins = allowedOrigins
	}
	displayAPIURL := strings.TrimSpace(opts.APIAdvertiseURL)
	if displayAPIURL == "" {
		displayAPIURL = backendURL
	}
	displayUIURL := strings.TrimSpace(opts.UIAdvertiseURL)
	if displayUIURL == "" {
		displayUIURL = uiOrigin
	}

	apiArgs := []string{
		"--listen", opts.APIListen,
		"--repo-root", repoRoot,
		"--config-path", opts.ConfigPath,
		"--output-dir", opts.OutputDir,
		"--log-dir", opts.LogDir,
		"--orchestrator-bin", opts.OrchestratorBin,
		"--token-file-path", opts.TokenFile,
		"--users-db", opts.UsersDB,
		"--cors-origin", apiCORSOrigins,
		"--auth-mode", opts.APIAuthMode,
		"--session-ttl", opts.APISessionTTL.String(),
		"--run-timeout", opts.APIRunTimeout.String(),
		"--rate-limit-per-minute", strconv.Itoa(opts.APIRateLimitPerMinute),
		"--read-timeout", opts.APIReadTimeout.String(),
		"--write-timeout", opts.APIWriteTimeout.String(),
		"--idle-timeout", opts.APIIdleTimeout.String(),
	}
	if strings.TrimSpace(opts.APISessionSecret) != "" {
		apiArgs = append(apiArgs, "--session-secret", opts.APISessionSecret)
	}
	if strings.TrimSpace(opts.APITLSCertFile) != "" || strings.TrimSpace(opts.APITLSKeyFile) != "" {
		apiArgs = append(apiArgs, "--tls-cert-file", opts.APITLSCertFile, "--tls-key-file", opts.APITLSKeyFile)
	}
	if strings.TrimSpace(opts.APITLSClientCAFile) != "" {
		apiArgs = append(apiArgs, "--tls-client-ca-file", opts.APITLSClientCAFile)
	}
	// Drop the Secure attribute on session cookies when serving the stack over
	// plain HTTP from a non-localhost address; otherwise browsers silently
	// refuse to store the session cookie and every login bounces back to the
	// login screen. TLS is still the right answer for anything exposed.
	if opts.APICookieInsecure {
		apiArgs = append(apiArgs, "--cookie-insecure")
	} else if uiTLSActive {
		// The browser reaches the stack over HTTPS, so session cookies can (and
		// should) carry the Secure attribute. The api-server sits on loopback
		// behind the UI server and cannot infer this itself, so tell it here.
		apiArgs = append(apiArgs, "--cookie-secure")
	}

	apiCmd := exec.Command(apiBin, apiArgs...)
	var uiCmd *exec.Cmd
	var uiArgs []string
	if !apiOnly {
		uiAuthMode := "token"
		if opts.APIAuthMode == "session" {
			uiAuthMode = "session"
		}
		uiArgs = []string{
			"--listen", opts.UIListen,
			"--dir", frontendDir,
			"--backend-url", backendURL,
			"--api-token-file", opts.TokenFile,
			"--api-auth-mode", uiAuthMode,
			"--allowed-origins", allowedOrigins,
		}
		if strings.TrimSpace(opts.UITLSCertFile) != "" || strings.TrimSpace(opts.UITLSKeyFile) != "" {
			uiArgs = append(uiArgs, "--tls-cert-file", opts.UITLSCertFile, "--tls-key-file", opts.UITLSKeyFile)
		}
		if strings.TrimSpace(opts.UIBackendCAFile) != "" {
			uiArgs = append(uiArgs, "--backend-ca-file", opts.UIBackendCAFile)
		}
		if strings.TrimSpace(opts.UIBackendClientCertFile) != "" {
			uiArgs = append(uiArgs, "--backend-client-cert-file", opts.UIBackendClientCertFile)
		}
		if strings.TrimSpace(opts.UIBackendClientKeyFile) != "" {
			uiArgs = append(uiArgs, "--backend-client-key-file", opts.UIBackendClientKeyFile)
		}
		if opts.UIBackendInsecureSkipVerify {
			uiArgs = append(uiArgs, "--backend-insecure-skip-verify")
		}
		uiCmd = exec.Command(uiBin, uiArgs...)
	}
	if opts.Supervise {
		// Native foreground supervisor: this process owns the children and
		// keeps them alive (crash + hang recovery) for as long as it runs.
		// Run it as a Type=simple systemd service so the OS keeps the
		// supervisor alive across reboots. apiCmd/uiCmd built above are not
		// used here; the supervisor builds its own commands on every restart.
		runDir := filepath.Join(installDir, "run")
		logDir := filepath.Join(installDir, "logs")
		apiLogPath := strings.TrimSpace(opts.APILogFile)
		if apiLogPath == "" {
			apiLogPath = filepath.Join(logDir, "v2-api.log")
		}
		apiPIDPath := strings.TrimSpace(opts.APIPIDFile)
		if apiPIDPath == "" {
			apiPIDPath = filepath.Join(runDir, "v2-api.pid")
		}
		// Health-probe restarts catch a hung-but-alive api-server. The probe
		// reuses the api-server's built-in --health-check (loopback HTTP), so
		// it only applies when the api-server is not serving its own TLS.
		var apiHealthArgs []string
		if strings.TrimSpace(opts.APITLSCertFile) == "" {
			apiHealthArgs = buildAPIHealthProbeArgs(apiBin, opts.APIListen, opts.TokenFile, repoRoot)
		}
		children := []*superviseChild{{
			name:       "api",
			bin:        apiBin,
			args:       apiArgs,
			listen:     opts.APIListen,
			pidPath:    apiPIDPath,
			logPath:    apiLogPath,
			healthArgs: apiHealthArgs,
		}}
		if uiCmd != nil {
			uiLogPath := strings.TrimSpace(opts.UILogFile)
			if uiLogPath == "" {
				uiLogPath = filepath.Join(logDir, "v2-ui.log")
			}
			uiPIDPath := strings.TrimSpace(opts.UIPIDFile)
			if uiPIDPath == "" {
				uiPIDPath = filepath.Join(runDir, "v2-ui.pid")
			}
			children = append(children, &superviseChild{
				name:       "ui",
				bin:        uiBin,
				args:       uiArgs,
				listen:     opts.UIListen,
				pidPath:    uiPIDPath,
				logPath:    uiLogPath,
				healthArgs: buildUIHealthProbeArgs(uiBin, opts.UIListen, opts.UITLSCertFile, opts.UITLSKeyFile),
				// Gate the UI's first launch on the API having written the
				// shared token file (same ordering as the detached path).
				waitToken: opts.TokenFile,
			})
		}
		fmt.Fprintf(os.Stderr, "v2 stack supervisor starting (foreground)\n")
		if strings.TrimSpace(layout) != "" {
			fmt.Fprintf(os.Stderr, "Asset layout: %s\n", layout)
		}
		fmt.Fprintf(os.Stderr, "API binary: %s\n", apiBin)
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "UI binary : %s\n", uiBin)
			fmt.Fprintf(os.Stderr, "Frontend  : %s\n", frontendDir)
		}
		fmt.Fprintf(os.Stderr, "Config path: %s\n", opts.ConfigPath)
		fmt.Fprintf(os.Stderr, "Token file: %s\n", opts.TokenFile)
		fmt.Fprintf(os.Stderr, "API: %s\n", displayAPIURL)
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "UI : %s\n", displayUIURL)
			fmt.Fprintf(os.Stderr, "UI allowed origins: %s\n", allowedOrigins)
		}
		fmt.Fprintf(os.Stderr, "Supervisor: max_restarts=%d window=%s, health-probe every %s after %d failures, cooldown-and-resume\n",
			opts.SelfHealMaxRestarts, opts.SelfHealWindow, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
		fmt.Fprintf(os.Stderr, "Stop with SIGTERM/SIGINT (or `systemctl stop ncc-orchestrator`).\n")
		return runV2Supervise(superviseConfig{
			installDir:         installDir,
			children:           children,
			maxRestarts:        opts.SelfHealMaxRestarts,
			window:             opts.SelfHealWindow,
			probeInterval:      opts.SelfHealProbeInterval,
			unhealthyThreshold: opts.SelfHealUnhealthyThreshold,
		})
	}
	if opts.Detach {
		runDir := filepath.Join(installDir, "run")
		detachedLogDir := filepath.Join(installDir, "logs")
		if err := os.MkdirAll(runDir, 0755); err != nil {
			return fmt.Errorf("prepare detached run dir: %w", err)
		}
		if err := os.MkdirAll(detachedLogDir, 0755); err != nil {
			return fmt.Errorf("prepare detached log dir: %w", err)
		}
		apiLogPath := strings.TrimSpace(opts.APILogFile)
		if apiLogPath == "" {
			apiLogPath = filepath.Join(detachedLogDir, "v2-api.log")
		}
		if absPath, err := filepath.Abs(apiLogPath); err == nil {
			apiLogPath = absPath
		}
		opts.APILogFile = apiLogPath
		if err := os.MkdirAll(filepath.Dir(apiLogPath), 0755); err != nil {
			return fmt.Errorf("prepare api detached log parent dir: %w", err)
		}
		apiLogFile, err := os.OpenFile(apiLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open api detached log: %w", err)
		}
		defer apiLogFile.Close()
		devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open devnull: %w", err)
		}
		defer devNull.Close()
		apiCmd.Stdin = devNull
		apiCmd.Stdout = apiLogFile
		apiCmd.Stderr = apiLogFile
		if uiCmd != nil {
			uiLogPath := strings.TrimSpace(opts.UILogFile)
			if uiLogPath == "" {
				uiLogPath = filepath.Join(detachedLogDir, "v2-ui.log")
			}
			if absPath, err := filepath.Abs(uiLogPath); err == nil {
				uiLogPath = absPath
			}
			opts.UILogFile = uiLogPath
			if err := os.MkdirAll(filepath.Dir(uiLogPath), 0755); err != nil {
				return fmt.Errorf("prepare ui detached log parent dir: %w", err)
			}
			uiLogFile, err := os.OpenFile(uiLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("open ui detached log: %w", err)
			}
			defer uiLogFile.Close()
			uiCmd.Stdin = devNull
			uiCmd.Stdout = uiLogFile
			uiCmd.Stderr = uiLogFile
		}
	} else {
		apiCmd.Stdout = os.Stdout
		apiCmd.Stderr = os.Stderr
		if uiCmd != nil {
			uiCmd.Stdout = os.Stdout
			uiCmd.Stderr = os.Stderr
		}
	}
	if err := apiCmd.Start(); err != nil {
		return fmt.Errorf("start api server: %w", err)
	}

	if strings.TrimSpace(opts.TokenFile) != "" {
		waitForFile(opts.TokenFile, 5*time.Second)
	}

	if uiCmd != nil {
		if err := uiCmd.Start(); err != nil {
			signalProcessStop(apiCmd)
			return fmt.Errorf("start ui server: %w", err)
		}
	}

	if uiCmd != nil {
		fmt.Fprintf(os.Stderr, "v2 services started (api pid=%d, ui pid=%d)\n", apiCmd.Process.Pid, uiCmd.Process.Pid)
	} else {
		fmt.Fprintf(os.Stderr, "v2 api service started (api pid=%d)\n", apiCmd.Process.Pid)
	}
	if strings.TrimSpace(layout) != "" {
		fmt.Fprintf(os.Stderr, "Asset layout: %s\n", layout)
	}
	fmt.Fprintf(os.Stderr, "API binary: %s\n", apiBin)
	if uiCmd != nil {
		fmt.Fprintf(os.Stderr, "UI binary : %s\n", uiBin)
		fmt.Fprintf(os.Stderr, "Frontend  : %s\n", frontendDir)
	}
	fmt.Fprintf(os.Stderr, "Orchestrator binary for API runs: %s\n", opts.OrchestratorBin)
	fmt.Fprintf(os.Stderr, "Config path: %s\n", opts.ConfigPath)
	fmt.Fprintf(os.Stderr, "Token file: %s\n", opts.TokenFile)
	fmt.Fprintf(os.Stderr, "API: %s\n", displayAPIURL)
	if uiCmd != nil {
		fmt.Fprintf(os.Stderr, "UI : %s\n", displayUIURL)
		fmt.Fprintf(os.Stderr, "UI allowed origins: %s\n", allowedOrigins)
	} else {
		fmt.Fprintf(os.Stderr, "API docs: %s/\n", displayAPIURL)
	}
	if opts.WaitReady {
		healthURL := strings.TrimRight(backendURL, "/") + "/api/v1/health"
		if err := waitForHTTPReady(healthURL, opts.ReadyTimeout); err != nil {
			signalProcessStop(apiCmd)
			if uiCmd != nil {
				signalProcessStop(uiCmd)
			}
			return fmt.Errorf("wait-ready api health check failed: %w", err)
		}
		if uiCmd != nil {
			if err := waitForHTTPReady(uiOrigin, opts.ReadyTimeout); err != nil {
				signalProcessStop(apiCmd)
				signalProcessStop(uiCmd)
				return fmt.Errorf("wait-ready ui check failed: %w", err)
			}
		}
		fmt.Fprintf(os.Stderr, "Readiness checks passed (timeout=%s).\n", opts.ReadyTimeout)
	}
	if opts.Detach {
		runDir := filepath.Join(installDir, "run")
		apiPIDPath := strings.TrimSpace(opts.APIPIDFile)
		if apiPIDPath == "" {
			apiPIDPath = filepath.Join(runDir, "v2-api.pid")
		}
		if absPath, err := filepath.Abs(apiPIDPath); err == nil {
			apiPIDPath = absPath
		}
		if err := os.MkdirAll(filepath.Dir(apiPIDPath), 0755); err != nil {
			signalProcessStop(apiCmd)
			if uiCmd != nil {
				signalProcessStop(uiCmd)
			}
			return fmt.Errorf("prepare api pid parent dir: %w", err)
		}
		if err := os.WriteFile(apiPIDPath, []byte(fmt.Sprintf("%d\n", apiCmd.Process.Pid)), 0644); err != nil {
			signalProcessStop(apiCmd)
			return fmt.Errorf("write api pid file: %w", err)
		}
		uiPIDPath := ""
		if uiCmd != nil {
			uiPIDPath = strings.TrimSpace(opts.UIPIDFile)
			if uiPIDPath == "" {
				uiPIDPath = filepath.Join(runDir, "v2-ui.pid")
			}
			if absPath, err := filepath.Abs(uiPIDPath); err == nil {
				uiPIDPath = absPath
			}
			if err := os.MkdirAll(filepath.Dir(uiPIDPath), 0755); err != nil {
				signalProcessStop(apiCmd)
				signalProcessStop(uiCmd)
				return fmt.Errorf("prepare ui pid parent dir: %w", err)
			}
			if err := os.WriteFile(uiPIDPath, []byte(fmt.Sprintf("%d\n", uiCmd.Process.Pid)), 0644); err != nil {
				signalProcessStop(apiCmd)
				signalProcessStop(uiCmd)
				return fmt.Errorf("write ui pid file: %w", err)
			}
		}
		var apiSupervisorPID int
		var uiSupervisorPID int
		if opts.SelfHeal {
			if runtime.GOOS == "windows" {
				return fmt.Errorf("self-heal is currently unsupported on windows detached mode")
			}
			apiLogPath := strings.TrimSpace(opts.APILogFile)
			if apiLogPath == "" {
				apiLogPath = filepath.Join(installDir, "logs", "v2-api.log")
			}
			// Health-probe restarts catch a hung-but-alive api-server (deadlock,
			// stuck handler) that a liveness-only check would miss. The probe
			// reuses the api-server's built-in `--health-check` mode (no curl
			// dependency), which hits /api/v1/health over loopback HTTP — so it
			// only applies when the api-server is not serving its own TLS.
			apiHealthCmd := ""
			if strings.TrimSpace(opts.APITLSCertFile) == "" {
				apiHealthCmd = buildAPIHealthProbeCmd(apiBin, opts.APIListen, opts.TokenFile, repoRoot)
			}
			pid, err := startSelfHealSupervisor("api", apiBin, apiArgs, apiPIDPath, apiLogPath, opts.SelfHealMaxRestarts, opts.SelfHealWindow, apiHealthCmd, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
			if err != nil {
				return fmt.Errorf("start api self-heal supervisor: %w", err)
			}
			apiSupervisorPID = pid
			apiSupPIDPath := filepath.Join(runDir, "v2-api-supervisor.pid")
			_ = os.WriteFile(apiSupPIDPath, []byte(fmt.Sprintf("%d\n", apiSupervisorPID)), 0644)
			if uiCmd != nil {
				uiLogPath := strings.TrimSpace(opts.UILogFile)
				if uiLogPath == "" {
					uiLogPath = filepath.Join(installDir, "logs", "v2-ui.log")
				}
				// The UI server's built-in --health-check probe lets the
				// supervisor catch a hung (alive-but-unresponsive) UI process,
				// not just a crashed one.
				uiHealthCmd := buildUIHealthProbeCmd(uiBin, opts.UIListen, opts.UITLSCertFile, opts.UITLSKeyFile)
				pid, err := startSelfHealSupervisor("ui", uiBin, uiArgs, uiPIDPath, uiLogPath, opts.SelfHealMaxRestarts, opts.SelfHealWindow, uiHealthCmd, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
				if err != nil {
					return fmt.Errorf("start ui self-heal supervisor: %w", err)
				}
				uiSupervisorPID = pid
				uiSupPIDPath := filepath.Join(runDir, "v2-ui-supervisor.pid")
				_ = os.WriteFile(uiSupPIDPath, []byte(fmt.Sprintf("%d\n", uiSupervisorPID)), 0644)
			}
		}
		_ = apiCmd.Process.Release()
		if uiCmd != nil {
			_ = uiCmd.Process.Release()
		}
		fmt.Fprintf(os.Stderr, "Detached mode enabled.\n")
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "PID files: %s, %s\n", apiPIDPath, uiPIDPath)
			apiLogPath := strings.TrimSpace(opts.APILogFile)
			if apiLogPath == "" {
				apiLogPath = filepath.Join(installDir, "logs", "v2-api.log")
			}
			uiLogPath := strings.TrimSpace(opts.UILogFile)
			if uiLogPath == "" {
				uiLogPath = filepath.Join(installDir, "logs", "v2-ui.log")
			}
			fmt.Fprintf(os.Stderr, "Logs: %s, %s\n", apiLogPath, uiLogPath)
			if runtime.GOOS == "windows" {
				fmt.Fprintf(os.Stderr, "Kill cmd: taskkill /PID %d /PID %d /T\n", apiCmd.Process.Pid, uiCmd.Process.Pid)
			} else {
				fmt.Fprintf(os.Stderr, "Kill cmd: kill \"$(cat %s)\" \"$(cat %s)\"\n", shellQuote(apiPIDPath), shellQuote(uiPIDPath))
			}
			if opts.SelfHeal {
				fmt.Fprintf(os.Stderr, "Self-heal: enabled (max_restarts=%d window=%s, health-probe every %s after %d failures, cooldown-and-resume)\n", opts.SelfHealMaxRestarts, opts.SelfHealWindow, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
				fmt.Fprintf(os.Stderr, "Self-heal supervisor pids: api=%d ui=%d\n", apiSupervisorPID, uiSupervisorPID)
			}
		} else {
			fmt.Fprintf(os.Stderr, "PID file: %s\n", apiPIDPath)
			apiLogPath := strings.TrimSpace(opts.APILogFile)
			if apiLogPath == "" {
				apiLogPath = filepath.Join(installDir, "logs", "v2-api.log")
			}
			fmt.Fprintf(os.Stderr, "Log: %s\n", apiLogPath)
			if runtime.GOOS == "windows" {
				fmt.Fprintf(os.Stderr, "Kill cmd: taskkill /PID %d /T\n", apiCmd.Process.Pid)
			} else {
				fmt.Fprintf(os.Stderr, "Kill cmd: kill \"$(cat %s)\"\n", shellQuote(apiPIDPath))
			}
			if opts.SelfHeal {
				fmt.Fprintf(os.Stderr, "Self-heal: enabled (max_restarts=%d window=%s, health-probe every %s after %d failures, cooldown-and-resume)\n", opts.SelfHealMaxRestarts, opts.SelfHealWindow, opts.SelfHealProbeInterval, opts.SelfHealUnhealthyThreshold)
				fmt.Fprintf(os.Stderr, "Self-heal supervisor pid: api=%d\n", apiSupervisorPID)
			}
		}
		fmt.Fprintf(os.Stderr, "Recommended stop cmd: ncc-orchestrator v2-stop --install-dir %s\n", shellQuote(installDir))
		return nil
	}
	if uiCmd != nil {
		fmt.Fprintln(os.Stderr, "Press Ctrl+C to stop both services.")
	} else {
		fmt.Fprintln(os.Stderr, "Press Ctrl+C to stop API service.")
	}

	type procExit struct {
		name string
		err  error
	}
	exitCh := make(chan procExit, 2)
	go func() { exitCh <- procExit{name: "api", err: apiCmd.Wait()} }()
	if uiCmd != nil {
		go func() { exitCh <- procExit{name: "ui", err: uiCmd.Wait()} }()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		if uiCmd != nil {
			fmt.Fprintf(os.Stderr, "received %s, stopping services...\n", sig.String())
		} else {
			fmt.Fprintf(os.Stderr, "received %s, stopping API service...\n", sig.String())
		}
		signalProcessStop(apiCmd)
		if uiCmd != nil {
			signalProcessStop(uiCmd)
		}
		<-exitCh
		if uiCmd != nil {
			<-exitCh
		}
		return nil
	case first := <-exitCh:
		signalProcessStop(apiCmd)
		if uiCmd != nil {
			signalProcessStop(uiCmd)
		}
		if first.err != nil {
			return fmt.Errorf("%s server exited with error: %w", first.name, first.err)
		}
		if uiCmd != nil {
			second := <-exitCh
			if second.err != nil {
				return fmt.Errorf("%s server exited with error: %w", second.name, second.err)
			}
		}
		return fmt.Errorf("%s server exited", first.name)
	}
}
