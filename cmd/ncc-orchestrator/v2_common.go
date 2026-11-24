package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goncc/internal/selfsigned"
	"goncc/internal/v2layout"
)

type v2BootstrapOptions struct {
	Repo               string
	Version            string
	InstallDir         string
	ConfigPath         string
	OutputDir          string
	LogDir             string
	OrchestratorBin    string
	APIListen          string
	UIListen           string
	TokenFile          string
	CheckOnly          bool
	SkipChecksumVerify bool
}

type v2StartOptions struct {
	InstallDir                  string
	ConfigPath                  string
	OutputDir                   string
	LogDir                      string
	OrchestratorBin             string
	APIListen                   string
	UIListen                    string
	APIAdvertiseURL             string
	UIAdvertiseURL              string
	UIBackendURL                string
	APICORSOrigins              string
	UIAllowedOrigins            string
	TokenFile                   string
	UsersDB                     string
	APIAuthMode                 string
	APISessionTTL               time.Duration
	APISessionSecret            string
	APISessionSecretFile        string
	APIRunTimeout               time.Duration
	APIRateLimitPerMinute       int
	APIReadTimeout              time.Duration
	APIWriteTimeout             time.Duration
	APIIdleTimeout              time.Duration
	APITLSCertFile              string
	APITLSKeyFile               string
	APITLSClientCAFile          string
	APICookieInsecure           bool
	UITLSCertFile               string
	UITLSKeyFile                string
	UIInsecureHTTP              bool
	UIBackendCAFile             string
	UIBackendClientCertFile     string
	UIBackendClientKeyFile      string
	UIBackendInsecureSkipVerify bool
	WaitReady                   bool
	ReadyTimeout                time.Duration
	Detach                      bool
	APIOnly                     bool
	APILogFile                  string
	UILogFile                   string
	APIPIDFile                  string
	UIPIDFile                   string
	SelfHeal                    bool
	SelfHealMaxRestarts         int
	SelfHealWindow              time.Duration
	// SelfHealProbeInterval is how often the supervisor runs an HTTP health
	// probe against a still-alive process to catch hangs/deadlocks (not just
	// crashes). SelfHealUnhealthyThreshold is the number of consecutive failed
	// probes that triggers a restart. Zero values fall back to defaults.
	SelfHealProbeInterval      time.Duration
	SelfHealUnhealthyThreshold int
	// Supervise runs the stack under the native foreground supervisor instead
	// of detaching: a single long-lived process launches and keeps API/UI
	// alive (liveness + health-probe restarts, backoff, cooldown-and-resume).
	// Intended to be run as a Type=simple systemd service for reboot
	// persistence. Mutually preferred over Detach when both are set.
	Supervise bool
}

type v2StopOptions struct {
	InstallDir  string
	APIPIDFile  string
	UIPIDFile   string
	Force       bool
	StopTimeout time.Duration
}

type uninstallOptions struct {
	ConfigPath      string
	InstallDir      string
	TaskName        string
	Force           bool
	DryRun          bool
	RemoveLocal     bool
	RemoveSchedule  bool
	RemoveV2Runtime bool
}

func findAsset(rel githubRelease, pred func(name string) bool) (githubAsset, bool) {
	for _, a := range rel.Assets {
		if pred(strings.ToLower(strings.TrimSpace(a.Name))) {
			return a, true
		}
	}
	return githubAsset{}, false
}

func writeExecutable(path string, body []byte) error {
	return os.WriteFile(path, body, 0755)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func binaryPathInInstallDir(installDir, base string) string {
	path := filepath.Join(installDir, "bin", base)
	if runtime.GOOS == "windows" {
		return path + ".exe"
	}
	return path
}

// existingBinaryInInstallDir returns the path to a v2 runtime binary inside
// <installDir>/bin/, accepting either the canonical name (`ncc-api-server`,
// preferred for newly-packaged stack archives) or the platform-suffixed name
// (`ncc-api-server-<os>-<arch>`, used by legacy v2.0.0 stack archives).
// Returns "" if no candidate exists.
func existingBinaryInInstallDir(installDir, base string) string {
	binDir := filepath.Join(installDir, "bin")
	for _, name := range v2BinaryNameCandidates(base) {
		p := filepath.Join(binDir, name)
		if existingFile(p) {
			return p
		}
	}
	return ""
}

func existingFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func existingDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

func v2BinaryNameCandidates(base string) []string {
	names := []string{
		base,
		fmt.Sprintf("%s-%s-%s", base, runtime.GOOS, runtime.GOARCH),
	}
	if runtime.GOOS == "windows" {
		withExe := make([]string, 0, len(names)*2)
		for _, n := range names {
			withExe = append(withExe, n, n+".exe")
		}
		return withExe
	}
	return names
}

func firstExistingBinary(searchDirs []string, base string) string {
	candidates := v2BinaryNameCandidates(base)
	for _, dir := range searchDirs {
		d := strings.TrimSpace(dir)
		if d == "" {
			continue
		}
		for _, name := range candidates {
			p := filepath.Join(d, name)
			if existingFile(p) {
				return p
			}
		}
	}
	return ""
}

// resolveV2RepoRoot picks the api-server's --repo-root value. The api-server
// uses repo-root as the path-traversal sandbox boundary for every file it
// touches (config, outputs, logs, token), so it must contain ALL of those
// paths. We pick the directory that contains both the install-dir and the
// CWD when one is an ancestor of the other; otherwise prefer install-dir
// (since v2.0.2 defaults all secondary paths relative to install-dir).
//
// We also EvalSymlinks the chosen root so the api-server's
// normalizeAndConfinePath comparison is consistent on platforms where /tmp
// is a symlink to /private/tmp (macOS): the api-server EvalSymlinks the
// rootAbs but compares against the user-supplied (unresolved) absolute
// path. Without orchestrator-side resolution, /tmp/.../config.yaml would
// fail the prefix check against /private/tmp/.../<repo-root>.
func resolveV2RepoRoot(installDir, cwd string) string {
	installAbs, err := filepath.Abs(installDir)
	if err != nil {
		installAbs = installDir
	}
	cwdAbs, err := filepath.Abs(cwd)
	if err != nil {
		cwdAbs = cwd
	}
	installAbs = filepath.Clean(installAbs)
	cwdAbs = filepath.Clean(cwdAbs)

	chosen := installAbs
	switch {
	case installAbs == cwdAbs:
		chosen = installAbs
	case isPathAncestor(installAbs, cwdAbs):
		chosen = installAbs
	case isPathAncestor(cwdAbs, installAbs):
		chosen = cwdAbs
	}
	if real, err := filepath.EvalSymlinks(chosen); err == nil {
		return filepath.Clean(real)
	}
	return chosen
}

// isPathAncestor reports whether parent is an ancestor of (or equal to)
// child, using lexical path comparison after Clean. Caller is expected to
// supply absolute paths. Handles the filesystem root ("/" on unix, "C:\"
// on windows) where Clean(root) already ends in the separator and we must
// not append a second separator before the prefix check.
func isPathAncestor(parent, child string) bool {
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)
	if parent == child {
		return true
	}
	sep := string(os.PathSeparator)
	if strings.HasSuffix(parent, sep) {
		return strings.HasPrefix(child, parent)
	}
	return strings.HasPrefix(child, parent+sep)
}

// resolveV2PathToReal returns p as an absolute, symlink-resolved path. If
// the path doesn't exist (e.g. output-dir before mkdir, log-dir before
// mkdir, token-file before write), it walks up to the first existing
// ancestor, EvalSymlinks that, then re-attaches the non-existing suffix.
// This is critical for the api-server's normalizeAndConfinePath sandbox
// check on platforms where /tmp is a symlink (macOS): without consistent
// resolution, repo-root would be /private/tmp/X but output-dir would
// remain /tmp/X/outputfiles — failing the prefix check.
func resolveV2PathToReal(p string) string {
	if strings.TrimSpace(p) == "" {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	abs = filepath.Clean(abs)
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(real)
	}
	// Walk up until we find an existing ancestor, then re-attach the
	// non-existing suffix. Bounded by the filesystem root.
	parent := filepath.Dir(abs)
	rest := filepath.Base(abs)
	for {
		if parent == filepath.Dir(parent) {
			return abs // hit the root without finding an existing ancestor
		}
		if real, err := filepath.EvalSymlinks(parent); err == nil {
			return filepath.Clean(filepath.Join(real, rest))
		}
		rest = filepath.Join(filepath.Base(parent), rest)
		parent = filepath.Dir(parent)
	}
}

// defaultV2InstallDir returns the install directory used by run-time consumer
// commands (v2-check, v2-start, v2-stop, uninstall) when the user does not
// pass an explicit --install-dir. The detection mirrors
// resolvePackageInstallDir so that running from inside an extracted/
// bootstrapped stack layout (`<X>/bin/<exe>`) "just works" without forcing
// the user to type `--install-dir <X>` every time.
//
// Resolution:
//
//  1. If os.Executable() is at <X>/bin/<exe> AND <X> contains the v2 layout
//     (frontend-dist/ present, or bin/ncc-api-server present under either
//     canonical or platform-suffixed naming) → return <X>.
//  2. Otherwise → return ".ncc-v2" (the historic default; preserves backward
//     compatibility with existing scripts, docs, and CI invocations that
//     bootstrap into ./.ncc-v2 from a project root).
func defaultV2InstallDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ".ncc-v2"
	}
	return defaultV2InstallDirForExeDir(filepath.Dir(exe))
}

// defaultV2InstallDirForExeDir is the testable inner of defaultV2InstallDir.
// Split out so unit tests can drive the helper without having to redirect
// os.Executable() at runtime.
func defaultV2InstallDirForExeDir(exeDir string) string {
	if exeDir == "" || filepath.Base(exeDir) != "bin" {
		return ".ncc-v2"
	}
	parent := filepath.Dir(exeDir)
	if existingDir(filepath.Join(parent, "frontend-dist")) {
		return parent
	}
	if existingBinaryInInstallDir(parent, "ncc-api-server") != "" {
		return parent
	}
	return ".ncc-v2"
}

func resolveV2RuntimeLayout(installDir string) (string, string, string, string) {
	installDir = strings.TrimSpace(installDir)
	if installDir == "" {
		installDir = ".ncc-v2"
	}
	cwd, _ := os.Getwd()
	exeDir := ""
	if exe, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exe)
	}

	// Preferred layout from v2-bootstrap: <install>/bin/* + <install>/frontend-dist
	// Accepts either canonical (ncc-api-server) or platform-suffixed
	// (ncc-api-server-<os>-<arch>) binary names; legacy v2.0.0 stack archives
	// shipped the suffixed form, current/future archives ship canonical.
	apiBoot := existingBinaryInInstallDir(installDir, "ncc-api-server")
	uiBoot := existingBinaryInInstallDir(installDir, "ncc-ui-server")
	frontBoot := filepath.Join(installDir, "frontend-dist")
	if apiBoot != "" && uiBoot != "" && existingDir(frontBoot) {
		return apiBoot, uiBoot, frontBoot, "install-dir"
	}

	// Fallback for release asset folders (flat binaries + frontend-dist).
	searchDirs := []string{installDir, cwd, exeDir}
	apiFlat := firstExistingBinary(searchDirs, "ncc-api-server")
	uiFlat := firstExistingBinary(searchDirs, "ncc-ui-server")
	if apiFlat != "" && uiFlat != "" {
		for _, d := range []string{
			filepath.Join(installDir, "frontend-dist"),
			filepath.Join(cwd, "frontend-dist"),
			filepath.Join(exeDir, "frontend-dist"),
			filepath.Join(cwd, "frontend", "dist"),
			filepath.Join(exeDir, "frontend", "dist"),
		} {
			if existingDir(d) {
				return apiFlat, uiFlat, d, "local-release-assets"
			}
		}
	}
	return "", "", "", ""
}

func resolveV2APIBinary(installDir string) (string, string) {
	installDir = strings.TrimSpace(installDir)
	if installDir == "" {
		installDir = ".ncc-v2"
	}
	if apiBoot := existingBinaryInInstallDir(installDir, "ncc-api-server"); apiBoot != "" {
		return apiBoot, "install-dir"
	}
	cwd, _ := os.Getwd()
	exeDir := ""
	if exe, err := os.Executable(); err == nil {
		exeDir = filepath.Dir(exe)
	}
	apiFlat := firstExistingBinary([]string{installDir, cwd, exeDir}, "ncc-api-server")
	if apiFlat != "" {
		return apiFlat, "local-release-assets"
	}
	return "", ""
}

func resolveV2OrchestratorBin(preferred string) string {
	// Return an absolute (and symlink-resolved when possible) path so the
	// api-server doesn't need to interpret it relative to its CWD
	// (repo-root). Without this, "./ncc-orchestrator" would be valid for
	// the orchestrator's own CWD but not for the api-server's CWD when
	// they differ (e.g. running v2-start from <stack>/bin/ resolves
	// repo-root to <stack>, where ncc-orchestrator is at bin/, not the
	// repo-root itself).
	asAbs := func(p string) string {
		if abs, err := filepath.Abs(p); err == nil {
			if real, err := filepath.EvalSymlinks(abs); err == nil {
				return filepath.Clean(real)
			}
			return filepath.Clean(abs)
		}
		return p
	}
	clean := strings.TrimSpace(preferred)
	if clean != "" && existingFile(clean) {
		return asAbs(clean)
	}
	cwd, _ := os.Getwd()
	exePath, _ := os.Executable()
	exeDir := filepath.Dir(exePath)
	for _, p := range []string{
		clean,
		filepath.Join(cwd, "ncc-orchestrator"),
		filepath.Join(cwd, fmt.Sprintf("ncc-orchestrator-%s-%s", runtime.GOOS, runtime.GOARCH)),
		filepath.Join(exeDir, "ncc-orchestrator"),
		filepath.Join(exeDir, fmt.Sprintf("ncc-orchestrator-%s-%s", runtime.GOOS, runtime.GOARCH)),
		exePath,
	} {
		if strings.TrimSpace(p) != "" && existingFile(p) {
			return asAbs(p)
		}
	}
	return clean
}

// loopbackAltOriginFromListen returns the "http://localhost:port" variant
// for a listen address that's bound to an explicit loopback IP
// (127.0.0.1 or ::1). Used to ensure CORS allow-list contains both forms
// so browser clients that type either work, even though the connection
// URL produced by localHTTPURLFromListen prefers the actual IP for
// IPv4/IPv6 disambiguation.
//
// Returns empty string when the listen address is not loopback (e.g.
// 0.0.0.0, ::, hostnames, or external IPs) — in those cases the
// localhost-form would be misleading.
func loopbackAltOriginFromListen(listenAddr, defaultPort string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" || strings.HasPrefix(addr, ":") {
		return ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	switch host {
	case "127.0.0.1", "::1", "[::1]":
		return "http://localhost:" + port
	}
	return ""
}

func localHTTPURLFromListen(listenAddr, defaultPort string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		addr = ":" + defaultPort
	}
	if strings.HasPrefix(addr, ":") {
		// Server binds to all interfaces; "localhost" works for client.
		return "http://localhost" + addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	// Preserve the user-supplied host so the derived URL targets the same
	// address family the server bound to. Critical on macOS where the
	// server binds 127.0.0.1 (IPv4) but `localhost` resolves to ::1 (IPv6)
	// first, causing wait-ready / UI backend connections to fail with
	// "connection refused" even though the server is healthy.
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return "http://localhost:" + port
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "http://[" + host + "]:" + port
	}
	return "http://" + host + ":" + port
}

// listenHostForCert extracts a concrete host from a listen address for use as a
// certificate SAN. A wildcard bind (":8080", "0.0.0.0:8080", "[::]:8080")
// returns "" since there is no specific host to certify.
func listenHostForCert(listenAddr string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" || strings.HasPrefix(addr, ":") {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return ""
	}
	return host
}

// ensureDefaultUISelfSignedCert returns the path to a self-signed UI cert/key,
// generating it under <installDir>/tls on first use. An existing pair is reused
// across restarts so the browser's trust decision sticks. The certificate
// covers localhost/loopback plus the UI advertise host and any concrete listen
// host, so https://<that-host> validates the name (self-signed trust aside).
func ensureDefaultUISelfSignedCert(installDir string, opts v2StartOptions) (certPath, keyPath string, err error) {
	var hosts []string
	if u := strings.TrimSpace(opts.UIAdvertiseURL); u != "" {
		if parsed, perr := url.Parse(u); perr == nil && parsed.Hostname() != "" {
			hosts = append(hosts, parsed.Hostname())
		}
	}
	if h := listenHostForCert(opts.UIListen); h != "" {
		hosts = append(hosts, h)
	}
	return selfsigned.Ensure(filepath.Join(installDir, "tls"), hosts)
}

func mergeAllowedOriginsCSV(baseOrigin string, extraCSV string) string {
	seen := map[string]bool{}
	out := make([]string, 0, 4)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if seen[v] {
			return
		}
		seen[v] = true
		out = append(out, v)
	}
	add(baseOrigin)
	for _, part := range strings.Split(extraCSV, ",") {
		add(part)
	}
	return strings.Join(out, ",")
}

func waitForFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func signalProcessStop(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
}

func readPIDFromFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return 0, fmt.Errorf("pid file %s is empty", path)
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid pid %q in %s", raw, path)
	}
	return pid, nil
}

func signalPIDStop(pid int, force bool) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" || force {
		return proc.Kill()
	}
	return proc.Signal(syscall.SIGTERM)
}

func readTrimmedFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func waitForHTTPReady(url string, timeout time.Duration) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 500 {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			// The server answered with a certificate this host does not trust,
			// the default self-signed one for example: it is up, and nothing
			// is sent to it here.
			var untrusted *tls.CertificateVerificationError
			if errors.As(err, &untrusted) {
				return nil
			}
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = errors.New("timed out")
	}
	return fmt.Errorf("endpoint %s not ready: %w", url, lastErr)
}

// isExecutableFile reports whether path is a runnable binary on the
// current OS. Delegates to v2layout.IsExecutable so the Unix
// executable-bit check and the Windows PATHEXT-extension check stay in
// one place (shared with the api-server's startup guard). On Windows
// the Unix mode bits are meaningless, so a bit-only test wrongly
// rejected every shipped ncc-*.exe with "not executable".
func isExecutableFile(path string) bool {
	return v2layout.IsExecutable(path)
}

func canBindListenAddress(listenAddr string) error {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		return errors.New("empty listen address")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	_ = ln.Close()
	return nil
}
