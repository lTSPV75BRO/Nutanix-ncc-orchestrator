package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func quickstartPrompt(reader *bufio.Reader, prompt string, defaultValue string) (string, error) {
	renderPrompt := func() {
		if strings.TrimSpace(defaultValue) != "" {
			fmt.Printf("%s [%s]: ", prompt, defaultValue)
		} else {
			fmt.Printf("%s: ", prompt)
		}
	}
	renderPrompt()
	raw, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(raw)
	if v != "" {
		return v, nil
	}
	fmt.Println("Input is empty. Press Enter again to continue, or type a value.")
	renderPrompt()
	raw, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	v = strings.TrimSpace(raw)
	if v == "" {
		return strings.TrimSpace(defaultValue), nil
	}
	return v, nil
}

func quickstartWarnOnEmptyTargets(mode, clusters, pcs string) {
	if mode == "clusters" && strings.TrimSpace(clusters) == "" {
		fmt.Println("Warning: cluster targets are empty; preflight will fail until clusters (or clusters-file) is set.")
	}
	if mode == "pc" && strings.TrimSpace(pcs) == "" {
		fmt.Println("Warning: Prism Central targets are empty; discovery will rely on prism-central-url.")
	}
}

func quickstartPromptChoice(reader *bufio.Reader, prompt string, defaultValue string, allowed []string) (string, error) {
	allowSet := map[string]bool{}
	for _, a := range allowed {
		allowSet[strings.ToLower(strings.TrimSpace(a))] = true
	}
	v, err := quickstartPrompt(reader, prompt, defaultValue)
	if err != nil {
		return "", err
	}
	normalized := strings.ToLower(strings.TrimSpace(v))
	if allowSet[normalized] {
		return normalized, nil
	}
	fmt.Printf("Invalid value %q. Allowed values: %s\n", v, strings.Join(allowed, ", "))
	v, err = quickstartPrompt(reader, prompt, defaultValue)
	if err != nil {
		return "", err
	}
	normalized = strings.ToLower(strings.TrimSpace(v))
	if !allowSet[normalized] {
		return strings.ToLower(strings.TrimSpace(defaultValue)), nil
	}
	return normalized, nil
}

func upsertYAMLScalar(content string, key string, value string) string {
	quoted := fmt.Sprintf("%s: %q", key, value)
	// Keep the line's own indentation: the canonical config is nested
	// (schema version 1), and dropping it moved keys such as
	// storage.run-history-dir to the top level. Horizontal whitespace only, so
	// the match cannot reach back over blank lines either.
	pattern := fmt.Sprintf(`(?m)^([ \t]*)%s:[ \t]*.*$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	if re.MatchString(content) {
		return re.ReplaceAllString(content, "${1}"+strings.ReplaceAll(quoted, "$", "$$"))
	}
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return content + quoted + "\n"
}

func yamlScalarValue(content, key string) string {
	pattern := fmt.Sprintf(`(?m)^\s*%s:\s*(.*)$`, regexp.QuoteMeta(key))
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(content)
	if len(m) != 2 {
		return ""
	}
	v := strings.TrimSpace(m[1])
	if i := strings.Index(v, "#"); i >= 0 {
		v = strings.TrimSpace(v[:i])
	}
	v = strings.TrimSpace(strings.Trim(v, `"'`))
	return v
}

func repairConfigInlineCommentValues(cfgPath string) (bool, error) {
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return false, err
	}
	lines := strings.Split(string(raw), "\n")
	changed := false
	repairableKey := func(k string) bool {
		switch strings.TrimSpace(k) {
		case "timeout", "request-timeout", "poll-interval", "poll-jitter", "retry-base-delay", "retry-max-delay", "idle-conn-timeout":
			return true
		default:
			return false
		}
	}
	stripCommentTailForKey := func(k string) bool {
		switch strings.TrimSpace(k) {
		case "username", "cluster-source-mode", "discover-api-version", "nutanix-v4-api-version", "insecure-skip-verify", "timeout", "request-timeout":
			return true
		default:
			return false
		}
	}
	for i, ln := range lines {
		trim := strings.TrimSpace(ln)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		colon := strings.Index(ln, ":")
		if colon <= 0 {
			continue
		}
		key := strings.TrimSpace(ln[:colon])
		prefix := ln[:colon+1]
		rest := strings.TrimSpace(ln[colon+1:])
		if len(rest) < 2 || rest[0] != '"' || rest[len(rest)-1] != '"' {
			continue
		}
		if stripCommentTailForKey(key) {
			body := strings.TrimPrefix(rest, `"`)
			body = strings.TrimSuffix(body, `"`)
			if hash := strings.Index(body, "#"); hash >= 0 {
				body = body[:hash]
			}
			body = strings.TrimSpace(body)
			for strings.HasSuffix(body, `\"`) {
				body = strings.TrimSpace(strings.TrimSuffix(body, `\"`))
			}
			body = strings.TrimRight(body, `\`)
			fixedLine := prefix + " " + strconv.Quote(body)
			if strings.TrimSpace(fixedLine) != strings.TrimSpace(ln) {
				lines[i] = fixedLine
				changed = true
			}
			continue
		}
		inner := rest[1 : len(rest)-1]
		orig := inner
		legacyIdx := strings.Index(inner, `\"`)
		if legacyIdx >= 0 {
			legacyTail := inner[legacyIdx+2:]
			if strings.Contains(legacyTail, "#") {
				inner = strings.TrimSpace(inner[:legacyIdx])
			}
		}
		if repairableKey(key) {
			inner = strings.TrimRight(inner, `\`)
		}
		inner = strings.TrimSpace(inner)
		if inner != strings.TrimSpace(orig) {
			lines[i] = prefix + " " + strconv.Quote(inner)
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	out := strings.Join(lines, "\n")
	if err := os.WriteFile(cfgPath, []byte(out), 0644); err != nil {
		return false, err
	}
	return true, nil
}

func maskQuickstartValue(key, value string) string {
	if strings.EqualFold(strings.TrimSpace(key), "password") {
		if strings.TrimSpace(value) == "" {
			return `""`
		}
		return `"********"`
	}
	return strconv.Quote(value)
}

func runQuickstartInteractive(cfgPath string) error {
	reader := bufio.NewReader(os.Stdin)
	contentBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	content := string(contentBytes)
	original := content
	updates := map[string]string{}
	defaultMode := yamlScalarValue(original, "cluster-source-mode")
	if defaultMode == "" {
		defaultMode = "clusters"
	}
	mode, err := quickstartPromptChoice(reader, "Cluster source mode (clusters|pc)", defaultMode, []string{"clusters", "pc"})
	if err != nil {
		return err
	}
	updates["cluster-source-mode"] = mode
	content = upsertYAMLScalar(content, "cluster-source-mode", mode)
	if mode == "pc" {
		pcURLDefault := yamlScalarValue(original, "prism-central-url")
		pcURL, err := quickstartPrompt(reader, "Prism Central URL (optional, e.g. https://pc:9440)", pcURLDefault)
		if err != nil {
			return err
		}
		updates["prism-central-url"] = pcURL
		content = upsertYAMLScalar(content, "prism-central-url", pcURL)
		pcs := ""
		if strings.TrimSpace(pcURL) == "" {
			pcsDefault := yamlScalarValue(original, "pcs")
			pcs, err = quickstartPrompt(reader, "Prism Central targets (comma-separated)", pcsDefault)
			if err != nil {
				return err
			}
			updates["pcs"] = pcs
			content = upsertYAMLScalar(content, "pcs", pcs)
			quickstartWarnOnEmptyTargets(mode, "", pcs)
		} else {
			updates["pcs"] = yamlScalarValue(original, "pcs")
		}
		updates["clusters"] = ""
		content = upsertYAMLScalar(content, "clusters", "")
	} else {
		clusterDefault := yamlScalarValue(original, "clusters")
		clusters, err := quickstartPrompt(reader, "Cluster targets (comma-separated)", clusterDefault)
		if err != nil {
			return err
		}
		updates["clusters"] = clusters
		updates["pcs"] = ""
		content = upsertYAMLScalar(content, "clusters", clusters)
		content = upsertYAMLScalar(content, "pcs", "")
		quickstartWarnOnEmptyTargets(mode, clusters, "")
	}
	defaultUser := yamlScalarValue(original, "username")
	if strings.TrimSpace(defaultUser) == "" {
		defaultUser = "admin"
	}
	username, err := quickstartPrompt(reader, "Prism username", defaultUser)
	if err != nil {
		return err
	}
	updates["username"] = username
	content = upsertYAMLScalar(content, "username", username)
	insecureDefault := strings.ToLower(strings.TrimSpace(yamlScalarValue(original, "insecure-skip-verify")))
	if insecureDefault != "true" && insecureDefault != "false" {
		insecureDefault = "false"
	}
	insecureValue, err := quickstartPromptChoice(reader, "Skip TLS certificate verification? (true|false)", insecureDefault, []string{"true", "false"})
	if err != nil {
		return err
	}
	updates["insecure-skip-verify"] = insecureValue
	content = upsertYAMLScalar(content, "insecure-skip-verify", insecureValue)
	advanced, err := quickstartConfirm(reader, "Configure advanced network/runtime options (timeouts/api version)?", false)
	if err != nil {
		return err
	}
	if advanced {
		timeoutDefault := yamlScalarValue(original, "timeout")
		if timeoutDefault == "" {
			timeoutDefault = "15m"
		}
		timeoutValue, err := quickstartPrompt(reader, "Overall cluster timeout (e.g. 15m)", timeoutDefault)
		if err != nil {
			return err
		}
		updates["timeout"] = timeoutValue
		content = upsertYAMLScalar(content, "timeout", timeoutValue)
		reqTimeoutDefault := yamlScalarValue(original, "request-timeout")
		if reqTimeoutDefault == "" {
			reqTimeoutDefault = "20s"
		}
		reqTimeoutValue, err := quickstartPrompt(reader, "Per-request timeout (e.g. 20s)", reqTimeoutDefault)
		if err != nil {
			return err
		}
		updates["request-timeout"] = reqTimeoutValue
		content = upsertYAMLScalar(content, "request-timeout", reqTimeoutValue)
		if mode == "pc" {
			discoverDefault := yamlScalarValue(original, "discover-api-version")
			if discoverDefault == "" {
				discoverDefault = defaultDiscoverAPIVersion
			}
			discoverVer, err := quickstartPromptChoice(reader, "Discover API version (v4|v3)", discoverDefault, []string{"v4", "v3"})
			if err != nil {
				return err
			}
			updates["discover-api-version"] = discoverVer
			content = upsertYAMLScalar(content, "discover-api-version", discoverVer)

			v4PathDefault := yamlScalarValue(original, "nutanix-v4-api-version")
			if v4PathDefault == "" {
				v4PathDefault = defaultNutanixV4APIVersion
			}
			v4PathValue, err := quickstartPrompt(reader, "Nutanix v4 API path version (e.g. v4.2)", v4PathDefault)
			if err != nil {
				return err
			}
			updates["nutanix-v4-api-version"] = v4PathValue
			content = upsertYAMLScalar(content, "nutanix-v4-api-version", v4PathValue)
		}
	}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		setPasswordNow, err := quickstartConfirm(reader, "Set Prism password now?", false)
		if err != nil {
			return err
		}
		if setPasswordNow {
			fmt.Print("Prism password (input hidden): ")
			bytePw, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				return fmt.Errorf("read quickstart password: %w", err)
			}
			pw := strings.TrimSpace(string(bytePw))
			updates["password"] = pw
			content = upsertYAMLScalar(content, "password", pw)
		}
	}
	fmt.Println("Planned config updates:")
	orderedKeys := []string{
		"cluster-source-mode", "clusters", "pcs", "prism-central-url",
		"username", "password", "insecure-skip-verify",
		"timeout", "request-timeout", "discover-api-version", "nutanix-v4-api-version",
	}
	changed := 0
	for _, key := range orderedKeys {
		newVal, ok := updates[key]
		if !ok {
			continue
		}
		oldVal := yamlScalarValue(original, key)
		if oldVal == newVal {
			continue
		}
		changed++
		fmt.Printf("- %s: %s -> %s\n", key, maskQuickstartValue(key, oldVal), maskQuickstartValue(key, newVal))
	}
	if changed == 0 {
		fmt.Println("- No effective value changes detected.")
	}
	applyChanges, err := quickstartConfirm(reader, "Apply these changes to config?", true)
	if err != nil {
		return err
	}
	if !applyChanges {
		fmt.Printf("Interactive quickstart did not modify %s\n", cfgPath)
		return nil
	}
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		return err
	}
	fmt.Printf("Interactive quickstart saved updates to %s\n", cfgPath)
	return nil
}

func quickstartConfirm(reader *bufio.Reader, prompt string, defaultYes bool) (bool, error) {
	suffix := "[y/N]"
	if defaultYes {
		suffix = "[Y/n]"
	}
	renderPrompt := func() {
		fmt.Printf("%s %s ", prompt, suffix)
	}
	renderPrompt()
	raw, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		fmt.Println("Input is empty. Press Enter again to continue, or type y/n.")
		renderPrompt()
		raw, err = reader.ReadString('\n')
		if err != nil {
			return false, err
		}
		v = strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			return defaultYes, nil
		}
	}
	if v == "y" || v == "yes" {
		return true, nil
	}
	if v == "n" || v == "no" {
		return false, nil
	}
	return defaultYes, nil
}

func quickstartV2Status(installDir string) (bool, string, string, string, string) {
	apiBin, uiBin, frontendDir, layout := resolveV2RuntimeLayout(installDir)
	if strings.TrimSpace(apiBin) == "" || strings.TrimSpace(uiBin) == "" || strings.TrimSpace(frontendDir) == "" {
		return false, "", "", "", ""
	}
	return true, apiBin, uiBin, frontendDir, layout
}

func quickstartStatusSummary(report preflightReport) (pass, warn, fail int) {
	for _, c := range report.Checks {
		switch strings.ToLower(strings.TrimSpace(c.Status)) {
		case "pass":
			pass++
		case "warn":
			warn++
		case "fail":
			fail++
		}
	}
	return pass, warn, fail
}

func printQuickstartWarningHighlights(report preflightReport, limit int) {
	if limit <= 0 {
		limit = 3
	}
	printed := 0
	for _, c := range report.Checks {
		if strings.ToLower(strings.TrimSpace(c.Status)) != "warn" {
			continue
		}
		msg := strings.TrimSpace(c.Message)
		title := strings.TrimSpace(c.Title)
		switch strings.ToLower(msg) {
		case "", "not set":
			if title != "" {
				msg = fmt.Sprintf("%s: %s", title, strings.TrimSpace(c.Message))
			}
		}
		if strings.TrimSpace(msg) == "" {
			msg = c.ID
		}
		fmt.Printf("- [warn] %s", strings.TrimSpace(msg))
		if h := strings.TrimSpace(c.Hint); h != "" {
			fmt.Printf(" | hint: %s", h)
		}
		fmt.Println()
		printed++
		if printed >= limit {
			break
		}
	}
	if report.Warn > printed {
		fmt.Printf("- ... %d more warnings (run `ncc-orchestrator preflight-check --config %s` for full details)\n", report.Warn-printed, report.ConfigPath)
	}
}

func runQuickstartCommand(cmd *cobra.Command, args []string) error {
	cfgPath, _ := cmd.Flags().GetString("config")
	if strings.TrimSpace(cfgPath) == "" {
		cfgPath = "config.yaml"
	}
	autoFix, _ := cmd.Flags().GetBool("auto-fix")
	interactive, _ := cmd.Flags().GetBool("interactive")
	assumeYes, _ := cmd.Flags().GetBool("assume-yes")
	setupV2, _ := cmd.Flags().GetString("setup-v2")
	installDir, _ := cmd.Flags().GetString("install-dir")
	repo, _ := cmd.Flags().GetString("repo")
	levelRaw, _ := cmd.Flags().GetString("automation-level")
	level, err := parseAutomationLevel(levelRaw)
	if err != nil {
		return err
	}
	setupV2 = strings.ToLower(strings.TrimSpace(setupV2))
	if setupV2 == "" {
		setupV2 = "ask"
	}
	switch setupV2 {
	case "ask", "download", "skip":
	default:
		return fmt.Errorf("invalid --setup-v2 %q (valid: ask, download, skip)", setupV2)
	}
	if strings.TrimSpace(installDir) == "" {
		installDir = ".ncc-v2"
	}
	if strings.TrimSpace(repo) == "" {
		repo = defaultGitHubRepo
	}
	cfgPath = strings.TrimSpace(cfgPath)
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}
	fmt.Println("Quickstart mode: beginner-friendly setup")
	fmt.Println("Step 1/3: checking config")
	if _, err := os.Stat(cfgPath); errors.Is(err, os.ErrNotExist) {
		if err := writeDummyConfig(cfgPath); err != nil {
			return fmt.Errorf("quickstart: create dummy config: %w", err)
		}
		fmt.Printf("Created starter config: %s\n", cfgPath)
	}
	if autoFix {
		repairedAny := false
		for i := 0; i < 3; i++ {
			fixed, err := repairConfigInlineCommentValues(cfgPath)
			if err != nil {
				break
			}
			if !fixed {
				break
			}
			repairedAny = true
		}
		if repairedAny {
			fmt.Println("Auto-fix: repaired malformed quoted config values.")
		}
	}
	if interactive {
		if err := runQuickstartInteractive(cfgPath); err != nil {
			return fmt.Errorf("quickstart interactive: %w", err)
		}
	}
	fmt.Println("Step 2/3: running safety checks")
	report := buildPreflightReport(cfgPath)
	pass, warn, fail := quickstartStatusSummary(report)
	fmt.Printf("Quickstart preflight: pass=%d warn=%d failed=%d\n", pass, warn, fail)
	if warn > 0 {
		fmt.Println("Preflight warnings:")
		printQuickstartWarningHighlights(report, 3)
	}
	if report.Failed > 0 && autoFix {
		cfg, cfgErr := loadConfigForValidation(cfgPath)
		if cfgErr == nil {
			fixed, actions := applySafePreflightFixes(cfg, cfgPath, report, level)
			for _, a := range actions {
				fmt.Printf("Auto-fix: %s\n", a)
			}
			if fixed > 0 {
				report = buildPreflightReport(cfgPath)
				pass, warn, fail = quickstartStatusSummary(report)
				fmt.Printf("After auto-fix: pass=%d warn=%d failed=%d\n", pass, warn, fail)
				if warn > 0 {
					fmt.Println("Remaining warnings:")
					printQuickstartWarningHighlights(report, 3)
				}
			}
		}
	}
	if report.Failed > 0 {
		printAutomationRunbook(report, level)
		return fmt.Errorf("quickstart preflight failed (%d failures)", report.Failed)
	}
	fmt.Println("Step 3/3: checking optional v2 API/UI components")
	v2Ready, apiBin, uiBin, frontendDir, layout := quickstartV2Status(installDir)
	webComponentsInstalled := v2Ready
	if v2Ready {
		fmt.Println("v2 components are ready.")
		fmt.Printf("- API binary : %s\n", apiBin)
		fmt.Printf("- UI binary  : %s\n", uiBin)
		fmt.Printf("- Frontend   : %s\n", frontendDir)
		fmt.Printf("- Layout     : %s\n", layout)
	} else {
		downloadV2 := false
		switch setupV2 {
		case "download":
			downloadV2 = true
		case "ask":
			if assumeYes {
				downloadV2 = true
			} else if term.IsTerminal(int(os.Stdin.Fd())) {
				reader := bufio.NewReader(os.Stdin)
				ok, err := quickstartConfirm(reader, "Install optional API/UI components now?", true)
				if err == nil {
					downloadV2 = ok
				}
			}
		case "skip":
			downloadV2 = false
		}
		if downloadV2 {
			fmt.Println("Downloading v2 components...")
			err := runV2Bootstrap(v2BootstrapOptions{
				Repo:       repo,
				InstallDir: installDir,
			})
			if err != nil {
				fmt.Printf("Could not auto-download v2 components: %v\n", err)
				fmt.Printf("You can download manually from: https://github.com/%s/releases/latest\n", repo)
				fmt.Printf("Command: ncc-orchestrator v2-bootstrap --repo %s --install-dir %s\n", repo, installDir)
			} else {
				fmt.Println("v2 components downloaded successfully.")
				webComponentsInstalled = true
			}
		} else {
			fmt.Println("v2 components are not installed yet (this is okay for CLI-only use).")
			fmt.Printf("Download link: https://github.com/%s/releases/latest\n", repo)
			fmt.Printf("Command: ncc-orchestrator v2-bootstrap --repo %s --install-dir %s\n", repo, installDir)
		}
	}
	fmt.Println("Quickstart complete.")
	fmt.Println("Next:")
	fmt.Printf("1) Edit config if needed: %s\n", cfgPath)
	fmt.Printf("2) Run checks: ncc-orchestrator --auto --config %s\n", cfgPath)
	if webComponentsInstalled {
		fmt.Println("3) (Optional) Start web UI: ncc-orchestrator v2-start")
	} else {
		fmt.Printf("3) (Optional) Install API/UI components: ncc-orchestrator v2-bootstrap --repo %s --install-dir %s\n", repo, installDir)
	}
	return nil
}
