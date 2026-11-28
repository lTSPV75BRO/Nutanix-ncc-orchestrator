package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// restartV2Stack stops and restarts the v2 stack by re-invoking THIS
// orchestrator binary (v2-stop then v2-start --detach). The restart is
// performed entirely by the binary — no external script — so a CLI restore can
// bring the stack back automatically. v2-start runs detached so the restarted
// stack outlives this short-lived restore process.
// v2StartStateFile records the portable v2-start settings of the most recent
// start at the install-dir root, so a backup can carry them and an
// orchestrator-managed restart (after a restore, or via v2-restart) relaunches
// with the same CORS/listen/session/etc. configuration instead of falling back
// to defaults.
const v2StartStateFile = ".ncc-v2-start.json"

// v2StartState is the curated, portable subset of v2StartOptions persisted to
// v2StartStateFile. Path-type flags (config-path, output/log dirs, token/users
// DB, TLS material, pid/log files) are intentionally omitted: they default
// under the install dir and an absolute path captured on one host/OS is
// meaningless after a cross-OS restore, so a restart re-derives them.
type v2StartState struct {
	APIListen                   string        `json:"api_listen,omitempty"`
	UIListen                    string        `json:"ui_listen,omitempty"`
	APIAdvertiseURL             string        `json:"api_advertise_url,omitempty"`
	UIAdvertiseURL              string        `json:"ui_advertise_url,omitempty"`
	UIBackendURL                string        `json:"ui_backend_url,omitempty"`
	APICORSOrigins              string        `json:"api_cors_origins,omitempty"`
	UIAllowedOrigins            string        `json:"ui_allowed_origins,omitempty"`
	APIAuthMode                 string        `json:"api_auth_mode,omitempty"`
	APISessionTTL               time.Duration `json:"api_session_ttl,omitempty"`
	APISessionSecret            string        `json:"api_session_secret,omitempty"`
	APIRunTimeout               time.Duration `json:"api_run_timeout,omitempty"`
	APIRateLimitPerMinute       int           `json:"api_rate_limit_per_minute"` // 0 = disabled is meaningful, so no omitempty
	APIReadTimeout              time.Duration `json:"api_read_timeout,omitempty"`
	APIWriteTimeout             time.Duration `json:"api_write_timeout,omitempty"`
	APIIdleTimeout              time.Duration `json:"api_idle_timeout,omitempty"`
	UIBackendInsecureSkipVerify bool          `json:"ui_backend_insecure_skip_verify,omitempty"`
	APICookieInsecure           bool          `json:"api_cookie_insecure,omitempty"`
	// UI TLS material is persisted (unlike the api-server's, which is treated
	// as environment-specific) because it is managed at runtime from
	// Settings → Access (TLS): enabling HTTPS writes the cert/key here so the
	// next start/restart binds the browser-facing UI server to TLS.
	UITLSCertFile string `json:"ui_tls_cert_file,omitempty"`
	UITLSKeyFile  string `json:"ui_tls_key_file,omitempty"`
	// UIInsecureHTTP opts out of the default self-signed HTTPS and serves the
	// UI over plain HTTP. Persisted so a restart keeps the operator's choice.
	UIInsecureHTTP             bool          `json:"ui_insecure_http,omitempty"`
	APIOnly                    bool          `json:"api_only,omitempty"`
	SelfHeal                   bool          `json:"self_heal,omitempty"`
	SelfHealMaxRestarts        int           `json:"self_heal_max_restarts,omitempty"`
	SelfHealWindow             time.Duration `json:"self_heal_window,omitempty"`
	SelfHealProbeInterval      time.Duration `json:"self_heal_probe_interval,omitempty"`
	SelfHealUnhealthyThreshold int           `json:"self_heal_unhealthy_threshold,omitempty"`
}

// writeV2StartState records the portable start settings of opts under
// installDir. Best-effort: a write failure must not abort the start.
func writeV2StartState(installDir string, opts v2StartOptions) error {
	st := v2StartState{
		APIListen:                   opts.APIListen,
		UIListen:                    opts.UIListen,
		APIAdvertiseURL:             opts.APIAdvertiseURL,
		UIAdvertiseURL:              opts.UIAdvertiseURL,
		UIBackendURL:                opts.UIBackendURL,
		APICORSOrigins:              opts.APICORSOrigins,
		UIAllowedOrigins:            opts.UIAllowedOrigins,
		APIAuthMode:                 opts.APIAuthMode,
		APISessionTTL:               opts.APISessionTTL,
		APISessionSecret:            opts.APISessionSecret,
		APIRunTimeout:               opts.APIRunTimeout,
		APIRateLimitPerMinute:       opts.APIRateLimitPerMinute,
		APIReadTimeout:              opts.APIReadTimeout,
		APIWriteTimeout:             opts.APIWriteTimeout,
		APIIdleTimeout:              opts.APIIdleTimeout,
		UIBackendInsecureSkipVerify: opts.UIBackendInsecureSkipVerify,
		APICookieInsecure:           opts.APICookieInsecure,
		UITLSCertFile:               opts.UITLSCertFile,
		UITLSKeyFile:                opts.UITLSKeyFile,
		UIInsecureHTTP:              opts.UIInsecureHTTP,
		APIOnly:                     opts.APIOnly,
		SelfHeal:                    opts.SelfHeal,
		SelfHealMaxRestarts:         opts.SelfHealMaxRestarts,
		SelfHealWindow:              opts.SelfHealWindow,
		SelfHealProbeInterval:       opts.SelfHealProbeInterval,
		SelfHealUnhealthyThreshold:  opts.SelfHealUnhealthyThreshold,
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeSecretFile(filepath.Join(installDir, v2StartStateFile), append(data, '\n'))
}

// saveListenAddresses changes the addresses of the saved start settings; an
// empty one stays as it is.
func saveListenAddresses(installDir, api, ui string) error {
	if api == "" && ui == "" {
		return nil
	}
	for _, a := range []string{api, ui} {
		if a == "" {
			continue
		}
		if _, port, err := net.SplitHostPort(a); err != nil || !validPort(port) {
			return fmt.Errorf("%q is not a listen address (use HOST:PORT or :PORT)", a)
		}
	}
	st, ok := loadV2StartState(installDir)
	if !ok {
		st = v2StartState{APIRateLimitPerMinute: defaultAPIRateLimitPerMinute}
	}
	if api != "" {
		st.APIListen = api
	}
	if ui != "" {
		st.UIListen = ui
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return writeSecretFile(filepath.Join(installDir, v2StartStateFile), append(data, '\n'))
}

func validPort(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && n > 0 && n < 65536
}

// defaultAPIRateLimitPerMinute is the default of --api-rate-limit-per-minute. A
// saved 0 turns the limit off, so new settings need it.
const defaultAPIRateLimitPerMinute = 60

// preserveHostStartStateNetworking re-applies the current host's
// environment-specific v2-start networking settings (listen addresses,
// advertise URLs, backend URL, CORS/allowed origins, and UI TLS material) on
// top of a start-state that a restore just overwrote. Backups are portable
// across hosts/OSes, so those fields describe the *source* host; keeping them
// would point the restored stack at the wrong addresses and make the UI reject
// this host's browser origin. A setting this host left empty stays empty too,
// or a crafted backup could point the UI at another backend or turn HTTPS off
// (SEC-19). Non-network settings (auth mode, session TTL,
// timeouts, rate limit, self-heal, …) are intentionally taken from the backup.
// Returns the names of the fields it kept (for logging). Best-effort: it never
// fails the restore.
func preserveHostStartStateNetworking(installDir string, pre v2StartState, hadPre bool) []string {
	if !hadPre {
		return nil
	}
	cur, ok := loadV2StartState(installDir)
	if !ok {
		return nil
	}
	var kept []string
	keep := func(name string, dst *string, src string) {
		if keepHostValue(dst, src) {
			kept = append(kept, name)
		}
	}
	keepBool := func(name string, dst *bool, src bool) {
		if keepHostValue(dst, src) {
			kept = append(kept, name)
		}
	}
	keep("api-listen", &cur.APIListen, pre.APIListen)
	keep("ui-listen", &cur.UIListen, pre.UIListen)
	keep("api-advertise-url", &cur.APIAdvertiseURL, pre.APIAdvertiseURL)
	keep("ui-advertise-url", &cur.UIAdvertiseURL, pre.UIAdvertiseURL)
	keep("ui-backend-url", &cur.UIBackendURL, pre.UIBackendURL)
	keep("api-cors-origins", &cur.APICORSOrigins, pre.APICORSOrigins)
	keep("ui-allowed-origins", &cur.UIAllowedOrigins, pre.UIAllowedOrigins)
	keep("ui-tls-cert-file", &cur.UITLSCertFile, pre.UITLSCertFile)
	keep("ui-tls-key-file", &cur.UITLSKeyFile, pre.UITLSKeyFile)
	keepBool("ui-insecure-http", &cur.UIInsecureHTTP, pre.UIInsecureHTTP)
	keepBool("ui-backend-insecure-skip-verify", &cur.UIBackendInsecureSkipVerify, pre.UIBackendInsecureSkipVerify)
	keepBool("api-cookie-insecure", &cur.APICookieInsecure, pre.APICookieInsecure)
	keepBool("api-only", &cur.APIOnly, pre.APIOnly)
	if len(kept) == 0 {
		return nil
	}
	data, err := json.MarshalIndent(cur, "", "  ")
	if err != nil {
		return nil
	}
	if err := writeSecretFile(filepath.Join(installDir, v2StartStateFile), append(data, '\n')); err != nil {
		return nil
	}
	return kept
}

// loadV2StartState reads the persisted start settings; ok is false when the
// file is absent or unparseable (a fresh install, or a pre-feature backup).
func loadV2StartState(installDir string) (v2StartState, bool) {
	data, err := os.ReadFile(filepath.Join(installDir, v2StartStateFile))
	if err != nil {
		return v2StartState{}, false
	}
	var st v2StartState
	if json.Unmarshal(data, &st) != nil {
		return v2StartState{}, false
	}
	return st, true
}

// v2StartArgsFromState builds the `v2-start` argument list (always detached)
// from the persisted settings. Returns ok=false when no state is available so
// the caller can fall back to a bare default start.
func v2StartArgsFromState(installDir string) (args []string, ok bool) {
	st, found := loadV2StartState(installDir)
	if !found {
		return nil, false
	}
	args = []string{"v2-start", "--install-dir", installDir, "--detach"}
	addStr := func(flag, v string) {
		if strings.TrimSpace(v) != "" {
			args = append(args, flag, v)
		}
	}
	addDur := func(flag string, d time.Duration) {
		if d > 0 {
			args = append(args, flag, d.String())
		}
	}
	addStr("--api-listen", st.APIListen)
	addStr("--ui-listen", st.UIListen)
	addStr("--api-advertise-url", st.APIAdvertiseURL)
	addStr("--ui-advertise-url", st.UIAdvertiseURL)
	addStr("--ui-backend-url", st.UIBackendURL)
	addStr("--api-cors-origins", st.APICORSOrigins)
	addStr("--ui-allowed-origins", st.UIAllowedOrigins)
	addStr("--api-auth-mode", st.APIAuthMode)
	addStr("--api-session-secret", st.APISessionSecret)
	addDur("--api-session-ttl", st.APISessionTTL)
	addDur("--api-run-timeout", st.APIRunTimeout)
	addDur("--api-read-timeout", st.APIReadTimeout)
	addDur("--api-write-timeout", st.APIWriteTimeout)
	addDur("--api-idle-timeout", st.APIIdleTimeout)
	args = append(args, "--api-rate-limit-per-minute", strconv.Itoa(st.APIRateLimitPerMinute))
	if st.UIBackendInsecureSkipVerify {
		args = append(args, "--ui-backend-insecure-skip-verify")
	}
	if st.APICookieInsecure {
		args = append(args, "--api-cookie-insecure")
	}
	addStr("--ui-tls-cert-file", st.UITLSCertFile)
	addStr("--ui-tls-key-file", st.UITLSKeyFile)
	if st.UIInsecureHTTP {
		args = append(args, "--ui-insecure-http")
	}
	if st.APIOnly {
		args = append(args, "--api-only")
	}
	if st.SelfHeal {
		args = append(args, "--self-heal")
		if st.SelfHealMaxRestarts > 0 {
			args = append(args, "--self-heal-max-restarts", strconv.Itoa(st.SelfHealMaxRestarts))
		}
		addDur("--self-heal-window", st.SelfHealWindow)
		addDur("--self-heal-probe-interval", st.SelfHealProbeInterval)
		if st.SelfHealUnhealthyThreshold > 0 {
			args = append(args, "--self-heal-unhealthy-threshold", strconv.Itoa(st.SelfHealUnhealthyThreshold))
		}
	}
	return args, true
}

// runsAsSystem: Windows only; false elsewhere.
func runsAsSystem() bool {
	u, err := user.Current()
	return err == nil && u.Uid == "S-1-5-18"
}

func restartV2Stack(installDir string) error {
	if _, ok := opsAvailable(installDir); ok {
		// The service runs unprivileged: its service helper restarts it.
		fmt.Println("\nRestarting stack (systemd-managed, by the service helper)...")
		_, err := submitOps(installDir, opsRequest{Op: opRestart}, 30*time.Second, 0)
		return err
	}
	if systemdStackIsActive() {
		fmt.Println("\nRestarting stack (systemd-managed)...")
		return restartSystemdStack()
	}
	// An administrator restarts the Windows service as a service, not as a stack
	// of their own account. A restart from inside it (SYSTEM) must not end the
	// task, because that ends this process too.
	if runtime.GOOS == "windows" && !runsAsSystem() {
		if svc := findWindowsService(installDir); svc != nil && svc.running() {
			if err := svc.privileged(); err != nil {
				return err
			}
			fmt.Printf("\nRestarting stack (%s)...\n", svc.title())
			if err := svc.stop(); err != nil {
				return err
			}
			return svc.start()
		}
	}
	self, err := os.Executable()
	if err != nil || strings.TrimSpace(self) == "" {
		return fmt.Errorf("locate orchestrator binary: %w", err)
	}
	fmt.Println("\nRestarting stack (binary-managed)...")
	// Stop first; ignore the error since the stack may already be down.
	stop := exec.Command(self, "v2-stop", "--install-dir", installDir) // #nosec G702 -- runs this same binary (os.Executable), no shell
	stop.Stdout, stop.Stderr = os.Stdout, os.Stderr
	_ = stop.Run()
	// Start detached so the relaunched API/UI survive this process exiting,
	// reusing the persisted start settings (CORS, listen, session TTL, …) so a
	// restore or v2-restart does not silently drop the operator's flags.
	startArgs, ok := v2StartArgsFromState(installDir)
	if !ok {
		startArgs = []string{"v2-start", "--install-dir", installDir, "--detach"}
	}
	start := exec.Command(self, startArgs...)
	start.Stdout, start.Stderr = os.Stdout, os.Stderr
	if err := start.Run(); err != nil {
		return fmt.Errorf("v2-start: %w", err)
	}
	return nil
}
