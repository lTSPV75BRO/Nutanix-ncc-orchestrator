package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"goncc/internal/plural"
)

// v2DoctorOptions wires the `doctor` subcommand's flags.
type v2DoctorOptions struct {
	InstallDir   string
	APIListen    string
	UIListen     string
	OutputFile   string // when set, writes a redacted support tarball
	NoBundle     bool
	ConfigPath   string // override for the self-heal config checks
	Fix          bool   // apply safe self-heal remediations
	JSON         bool   // emit the self-heal report as JSON (skips the human report + bundle)
	OnlyChecks   string // optional comma-separated check ids
	NoDisruptive bool   // skip disruptive checks/fixes (service restarts, etc.)
}

// runV2Doctor is the "something's broken, give me everything"
// diagnostic. Runs every existing read-only check, prints a unified
// report, and (by default) writes a redacted support tarball under
// <cwd>/ncc-support-<timestamp>.tar.gz that the operator can attach
// to a support ticket.
//
// Sections:
//
//  1. version + verify (embedded buildinfo + self SHA-256)
//  2. v2-check (install-dir layout + path readability)
//  3. v2-status (PIDs alive? api healthy?)
//  4. environment summary (GOOS/GOARCH, NCC_* env var NAMES only)
//  5. recent log tails (last 200 lines of each v2-*.log)
//  6. (optional) tar.gz bundle with the above + redacted config
//
// Always exits 0 unless the bundle write itself fails. Non-zero exit
// on missing services would defeat the "I'll run doctor when things
// look broken" use case.
func runV2Doctor(opts v2DoctorOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	apiListen := strings.TrimSpace(opts.APIListen)
	if apiListen == "" {
		apiListen = ":8081"
	}
	uiListen := strings.TrimSpace(opts.UIListen)
	if uiListen == "" {
		uiListen = ":8080"
	}

	// JSON mode is the machine-readable self-heal report only: no human report,
	// no support bundle. Used by automation and the api-server diagnostics view.
	if opts.JSON {
		hr := runSelfHealWithOptions(installDir, opts.ConfigPath, healRunOptions{
			Fix:          opts.Fix,
			OnlyChecks:   parseDoctorOnlyChecks(opts.OnlyChecks),
			NoDisruptive: opts.NoDisruptive,
		})
		out, err := json.MarshalIndent(hr, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		if hr.Worst() == healFail {
			return fmt.Errorf("self-heal found %s", plural.Count(hr.Summary["fail"], "failing check"))
		}
		return nil
	}

	var report bytes.Buffer
	w := io.MultiWriter(os.Stdout, &report)

	fmt.Fprintln(w, "========================================")
	fmt.Fprintln(w, "ncc-orchestrator doctor")
	fmt.Fprintln(w, "========================================")
	fmt.Fprintf(w, "generated_at:  %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "install_dir:   %s\n", installDir)
	fmt.Fprintf(w, "cwd:           %s\n", strDefaultStr(getwdSafe(), "(unknown)"))
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 1. verify (build provenance) --")
	_ = runVerifyCommand(w, verifyOptions{})
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 2. v2-check (install-dir layout) --")
	checkErr := runV2Check(v2StartOptions{
		InstallDir: installDir,
		APIListen:  apiListen,
		UIListen:   uiListen,
	})
	if checkErr != nil {
		fmt.Fprintf(w, "v2-check exit: %v\n", checkErr)
	} else {
		fmt.Fprintln(w, "v2-check: ok")
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 3. v2-status (running services) --")
	_ = runV2Status(v2StatusOptions{
		InstallDir: installDir,
		APIListen:  apiListen,
		UIListen:   uiListen,
	})
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 4. environment summary --")
	fmt.Fprintf(w, "go_version:    %s\n", runtime.Version())
	fmt.Fprintf(w, "os/arch:       %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(w, "num_cpu:       %d\n", runtime.NumCPU())
	fmt.Fprintln(w)
	fmt.Fprintln(w, "NCC_* env var names (values REDACTED for support-ticket safety):")
	doctorPrintRedactedEnv(w)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "-- 5. recent log tails (last 200 lines each) --")
	logDir := filepath.Join(installDir, "logs")
	for _, name := range []string{"v2-supervisor.log", "v2-api.log", "v2-ui.log", "v2-api-supervisor.log", "v2-ui-supervisor.log"} {
		p := filepath.Join(logDir, name)
		fmt.Fprintf(w, "\n--- %s ---\n", p)
		doctorTailFile(w, p, 200)
	}
	fmt.Fprintln(w)

	heading := "-- 6. self-heal checks --"
	if opts.Fix {
		heading = "-- 6. self-heal checks (--fix: applying safe remediations) --"
	}
	fmt.Fprintln(w, heading)
	hr := runSelfHealWithOptions(installDir, opts.ConfigPath, healRunOptions{
		Fix:          opts.Fix,
		OnlyChecks:   parseDoctorOnlyChecks(opts.OnlyChecks),
		NoDisruptive: opts.NoDisruptive,
	})
	doctorPrintSelfHeal(w, hr)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "========================================")
	fmt.Fprintln(w, "doctor: report complete")
	fmt.Fprintln(w, "========================================")

	if opts.NoBundle {
		return nil
	}

	bundlePath := strings.TrimSpace(opts.OutputFile)
	if bundlePath == "" {
		bundlePath = filepath.Join(".",
			fmt.Sprintf("ncc-support-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z")))
	}
	if err := writeDoctorBundle(bundlePath, report.String(), installDir); err != nil {
		fmt.Fprintf(os.Stderr, "doctor: failed to write bundle %s: %v\n", bundlePath, err)
		return err
	}
	fmt.Fprintf(os.Stdout, "\nsupport bundle: %s\n", bundlePath)
	fmt.Fprintln(os.Stdout, "  attach this file to your support ticket; secrets and tokens have been redacted.")
	return nil
}

// doctorPrintSelfHeal renders a self-heal report as aligned, human-readable
// lines: a status glyph, the check title, its message, and any remediation
// applied or hint to follow up.
func doctorPrintSelfHeal(w io.Writer, hr healReport) {
	glyph := func(s healStatus) string {
		switch s {
		case healOK:
			return "[ ok ]"
		case healWarn:
			return "[warn]"
		case healFail:
			return "[FAIL]"
		default:
			return "[ ?? ]"
		}
	}
	fmt.Fprintf(w, "config: %s\n", hr.ConfigPath)
	for _, res := range hr.Results {
		fmt.Fprintf(w, "  %s %-28s %s\n", glyph(res.Status), res.Title, res.Message)
		if res.Fixed && strings.TrimSpace(res.FixMsg) != "" {
			fmt.Fprintf(w, "         fixed: %s\n", res.FixMsg)
		}
		if !res.Fixed && strings.TrimSpace(res.Hint) != "" {
			fmt.Fprintf(w, "         hint:  %s\n", res.Hint)
		}
	}
	fmt.Fprintf(w, "summary: %d ok, %d warn, %d fail\n", hr.Summary["ok"], hr.Summary["warn"], hr.Summary["fail"])
}

func parseDoctorOnlyChecks(raw string) map[string]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		out[id] = true
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// doctorPrintRedactedEnv prints NCC_* env var NAMES only — values
// are redacted. Operators copy/paste support output straight into
// tickets, so values (which often contain Prism passwords or
// secret://refs) must never appear.
func doctorPrintRedactedEnv(w io.Writer) {
	keys := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "NCC_") {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		keys = append(keys, kv[:eq])
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		fmt.Fprintln(w, "  (none set)")
		return
	}
	for _, k := range keys {
		fmt.Fprintf(w, "  %s=***REDACTED***\n", k)
	}
}

// doctorTailFile prints up to maxLines trailing lines of path. If
// the file is missing or unreadable, prints a single-line marker.
// Tail rather than read-all so the doctor output (and the bundle)
// stays bounded even when a log file has rotated to gigabytes.
func doctorTailFile(w io.Writer, path string, maxLines int) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(w, "  (not present)")
			return
		}
		fmt.Fprintf(w, "  (read error: %v)\n", err)
		return
	}
	defer f.Close()
	st, _ := f.Stat()
	if st != nil {
		const window = int64(64 * 1024)
		if st.Size() > window {
			_, _ = f.Seek(-window, io.SeekEnd)
		}
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<16), 1<<20)
	lines := make([]string, 0, maxLines)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > maxLines {
			lines = lines[len(lines)-maxLines:]
		}
	}
	if len(lines) == 0 {
		fmt.Fprintln(w, "  (empty)")
		return
	}
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

// writeDoctorBundle emits a redacted support tarball:
//
//   - report.txt: same banner the operator just saw on stdout
//   - logs/*.log: last 1000 lines of each v2-{api,ui,*-supervisor}.log
//   - config.redacted.yaml: copy of <install-dir>/config.yaml or
//     example_config.yaml with values for any key matching
//     /password|secret|token|key|credential|cert/i replaced by
//     "***REDACTED***"
//
// gzip-compressed; safe to attach to a support ticket.
func writeDoctorBundle(path, report, installDir string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	addFile := func(name string, data []byte, mode int64) error {
		hdr := &tar.Header{
			Name:    name,
			Size:    int64(len(data)),
			Mode:    mode,
			ModTime: time.Now(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err := tw.Write(data)
		return err
	}

	if err := addFile("report.txt", []byte(report), 0o644); err != nil {
		return err
	}

	// Every process log under logs/: the stack's console logs plus the
	// structured API server, runner and scheduler logs (secrets are redacted
	// when those lines are written). Rotated .gz backups are left out.
	logDir := filepath.Join(installDir, "logs")
	for _, name := range doctorBundleLogNames(logDir) {
		lines := 1000
		if !strings.HasPrefix(name, "v2-") {
			lines = 5000 // structured logs: one line per event, worth more history
		}
		var buf bytes.Buffer
		doctorTailFile(&buf, filepath.Join(logDir, name), lines)
		if err := addFile("logs/"+name, buf.Bytes(), 0o644); err != nil {
			return err
		}
	}

	// Environment runner files (dev / staging / prod), redacted like config.
	envDir := filepath.Join(installDir, "configs", "environments")
	if entries, err := os.ReadDir(envDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
				continue
			}
			if data, err := os.ReadFile(filepath.Join(envDir, e.Name())); err == nil {
				if err := addFile("environments/"+strings.TrimSuffix(e.Name(), ".yaml")+".redacted.yaml", redactConfigYAML(data), 0o644); err != nil {
					return err
				}
			}
		}
	}

	cfgPath := filepath.Join(installDir, "config.yaml")
	if _, err := os.Stat(cfgPath); err != nil {
		cfgPath = filepath.Join(installDir, "example_config.yaml")
	}
	if data, err := os.ReadFile(cfgPath); err == nil {
		redacted := redactConfigYAML(data)
		if err := addFile("config.redacted.yaml", redacted, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// doctorBundleLogNames lists the logs a support bundle carries: the stack's
// fixed console logs (always, so a missing one shows as empty) plus every
// other *.log in logDir, at most 40 files.
func doctorBundleLogNames(logDir string) []string {
	names := []string{"v2-supervisor.log", "v2-api.log", "v2-ui.log", "v2-api-supervisor.log", "v2-ui-supervisor.log"}
	seen := map[string]bool{}
	for _, n := range names {
		seen[n] = true
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return names
	}
	var extra []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || seen[n] || !strings.HasSuffix(n, ".log") {
			continue
		}
		extra = append(extra, n)
	}
	sort.Strings(extra)
	names = append(names, extra...)
	if len(names) > 40 {
		names = names[:40]
	}
	return names
}

// redactConfigYAML masks values for keys whose name contains
// password / secret / token / credential / api-key / client-id (case
// insensitive). Conservative: only handles the canonical "key: value"
// line form. Good enough for the "don't leak the Prism password"
// support-bundle use case.
func redactConfigYAML(in []byte) []byte {
	rx := regexp.MustCompile(`(?i)^(\s*[-]?\s*(?:[a-z0-9_-]*(?:password|secret|token|credential|api[_-]?key|client[_-]?id)[a-z0-9_-]*))\s*:\s*(.*)$`)
	out := bytes.Buffer{}
	scanner := bufio.NewScanner(bytes.NewReader(in))
	scanner.Buffer(make([]byte, 1<<16), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if m := rx.FindStringSubmatch(line); m != nil && strings.TrimSpace(m[2]) != "" {
			out.WriteString(m[1])
			out.WriteString(": ***REDACTED***\n")
			continue
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

func getwdSafe() string {
	d, err := os.Getwd()
	if err != nil {
		return ""
	}
	return d
}

func strDefaultStr(s, dflt string) string {
	if strings.TrimSpace(s) == "" {
		return dflt
	}
	return s
}

func runV2Check(opts v2StartOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if absInstallDir, err := filepath.Abs(installDir); err == nil {
		installDir = absInstallDir
	}
	// Secondary paths default to <install-dir>/<conventional-name> when
	// the user did not pass an explicit value. This makes `v2-check` (and
	// `v2-start`) "just work" when run from inside a bootstrapped stack
	// layout: ./bin/ncc-orchestrator v2-check now finds config, output,
	// log, and token paths colocated with the install instead of relative
	// to bin/.
	failures := make([]string, 0)
	warnings := make([]string, 0)
	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = filepath.Join(installDir, "config.yaml")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		opts.OutputDir = filepath.Join(installDir, "outputfiles")
	}
	if strings.TrimSpace(opts.LogDir) == "" {
		opts.LogDir = filepath.Join(installDir, "nccfiles")
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
	// If config.yaml is missing but the install ships an example_config.yaml
	// (every published v2 stack does), fall back to it with a warning rather
	// than failing v2-check outright. This is the common "user just extracted
	// the stack and ran v2-check" path.
	if _, err := os.Stat(opts.ConfigPath); err != nil {
		exampleCfg := filepath.Join(installDir, "example_config.yaml")
		if _, exErr := os.Stat(exampleCfg); exErr == nil {
			warnings = append(warnings, fmt.Sprintf("config-path %s not found; falling back to %s for the v2-check (replace with your own config before running v2-start in production)", opts.ConfigPath, exampleCfg))
			opts.ConfigPath = exampleCfg
		}
	}
	opts.OrchestratorBin = resolveV2OrchestratorBin(opts.OrchestratorBin)
	apiBin, uiBin, frontDir, _ := resolveV2RuntimeLayout(installDir)
	if !isExecutableFile(opts.OrchestratorBin) {
		failures = append(failures, fmt.Sprintf("orchestrator-bin not executable: %s", opts.OrchestratorBin))
	}
	if apiBin == "" || !isExecutableFile(apiBin) {
		failures = append(failures, fmt.Sprintf("api-server binary not executable under install dir: %s", installDir))
	}
	if !opts.APIOnly {
		if uiBin == "" || !isExecutableFile(uiBin) {
			failures = append(failures, fmt.Sprintf("ui-server binary not executable under install dir: %s", installDir))
		}
		if strings.TrimSpace(frontDir) == "" || !existingDir(frontDir) {
			failures = append(failures, fmt.Sprintf("frontend-dist missing under install dir: %s", installDir))
		}
	}
	if _, err := os.Stat(opts.ConfigPath); err != nil {
		failures = append(failures, fmt.Sprintf("config-path not readable: %s (%v)", opts.ConfigPath, err))
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		failures = append(failures, fmt.Sprintf("output-dir not writable: %s (%v)", opts.OutputDir, err))
	}
	if err := os.MkdirAll(opts.LogDir, 0o755); err != nil {
		failures = append(failures, fmt.Sprintf("log-dir not writable: %s (%v)", opts.LogDir, err))
	}
	if err := os.MkdirAll(filepath.Dir(opts.TokenFile), 0o755); err != nil {
		failures = append(failures, fmt.Sprintf("token-file parent dir not writable: %s (%v)", filepath.Dir(opts.TokenFile), err))
	}
	if err := canBindListenAddress(opts.APIListen); err != nil {
		failures = append(failures, fmt.Sprintf("api-listen bind failed (%s): %v", opts.APIListen, err))
	}
	if !opts.APIOnly {
		if err := canBindListenAddress(opts.UIListen); err != nil {
			failures = append(failures, fmt.Sprintf("ui-listen bind failed (%s): %v", opts.UIListen, err))
		}
	}
	if cfg, err := loadConfigForValidation(opts.ConfigPath); err == nil {
		pwd := strings.TrimSpace(cfg.Password)
		if pwd != "" && !strings.HasPrefix(strings.ToLower(pwd), "secret://") {
			warnings = append(warnings, "config contains plaintext password; prefer NCC_PASSWORD env or secret:// source")
		}
	}
	fmt.Println("v2-check results")
	fmt.Println("---------------")
	fmt.Printf("install-dir: %s\n", installDir)
	fmt.Printf("config-path: %s\n", opts.ConfigPath)
	fmt.Printf("output-dir:  %s\n", opts.OutputDir)
	fmt.Printf("log-dir:     %s\n", opts.LogDir)
	fmt.Printf("token-file:  %s\n", opts.TokenFile)
	if len(warnings) > 0 {
		fmt.Println("warnings:")
		for _, w := range warnings {
			fmt.Printf("- %s\n", w)
		}
	}
	if len(failures) > 0 {
		fmt.Println("failures:")
		for _, f := range failures {
			fmt.Printf("- %s\n", f)
		}
		return fmt.Errorf("v2-check failed (%d issues)", len(failures))
	}
	fmt.Println("status: ok (all checks passed)")
	return nil
}
