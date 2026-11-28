package main

import (
	"fmt"
	"os"
	"strings"

	"goncc/internal/v2layout"
)

func tokenSource(activeToken, envToken string) string {
	if strings.TrimSpace(envToken) != "" && strings.TrimSpace(activeToken) == strings.TrimSpace(envToken) {
		return "env"
	}
	return "generated"
}

// defaultUsersDBPath returns the writable user-database path used when no
// backend is configured. It lives alongside the other server state in the
// repo root so the default-on login flow "just works" for a bare run.
func defaultUsersDBPath(repoRoot string) string {
	root := strings.TrimSpace(repoRoot)
	if root == "" {
		root = "."
	}
	return v2layout.UsersDB(root)
}

func applyStackAwareDefaults(s *apiServer, argv []string) {
	root, ok := v2layout.DetectStackRootFromExe()
	if !ok {
		return
	}
	explicit := explicitFlagSet(argv)

	// Only auto-redirect a flag that's still at its compile-time
	// default. The flag package's default values match what the
	// CWD-relative invocation would have produced.
	type pathFlag struct {
		flagName  string
		current   *string
		zeroValue string
		stackPath string
	}
	// Pre-resolve every path through symlinks (matches the
	// orchestrator v2-start fix). Without this, /var on macOS
	// resolves to /private/var inside the api-server's path
	// sandbox, but the un-resolved /var paths we pass for
	// config-path/output-dir/log-dir get rejected as "path escapes
	// repo root".
	resolvedRoot := v2layout.ResolveToReal(root)
	cfg := v2layout.ConfigPath(root)
	if cfg == "" {
		// no config.yaml AND no example_config.yaml in stack — leave
		// the api-server's --config-path default alone so its existing
		// "config not found" error message kicks in.
		cfg = s.configPath
	} else {
		cfg = v2layout.ResolveToReal(cfg)
	}
	// Resolve the orchestrator binary inside <root>/bin (canonical
	// or platform-suffixed). FindBinary returns "" when the
	// orchestrator wasn't shipped (e.g. someone copied just the
	// api-server binary into <root>/bin/) — in that case we leave
	// --orchestrator-bin alone and the user gets the existing
	// "orchestrator binary not found" error path when triggering a
	// run.
	orchBin := v2layout.FindBinary(root, "ncc-orchestrator")
	if orchBin != "" {
		orchBin = v2layout.ResolveToReal(orchBin)
	}
	flags := []pathFlag{
		{"repo-root", &s.repoRoot, ".", resolvedRoot},
		{"config-path", &s.configPath, "config.yaml", cfg},
		{"output-dir", &s.outputDir, "outputfiles", v2layout.ResolveToReal(v2layout.OutputDir(root))},
		{"log-dir", &s.logDir, "nccfiles", v2layout.ResolveToReal(v2layout.LogDir(root))},
		{"token-file-path", &s.tokenFilePath, ".ncc-api-token", v2layout.ResolveToReal(v2layout.TokenFile(root))},
		{"users-db", &s.usersDBPath, "", v2layout.ResolveToReal(v2layout.UsersDB(root))},
		{"orchestrator-bin", &s.orchestratorBin, "./ncc-orchestrator", orchBin},
	}
	rewrote := []string{}
	skipUsersDB := stackAwareKeepsUsersDBUnset(s)
	for _, f := range flags {
		if explicit[f.flagName] {
			continue
		}
		if f.flagName == "users-db" && skipUsersDB {
			continue
		}
		if *f.current != f.zeroValue {
			continue
		}
		if f.stackPath == "" || f.stackPath == *f.current {
			continue
		}
		*f.current = f.stackPath
		rewrote = append(rewrote, f.flagName)
	}
	if len(rewrote) > 0 {
		fmt.Fprintf(os.Stderr,
			"[stack-aware] detected v2 stack at %s; auto-resolved %s\n",
			root, strings.Join(rewrote, ", "))
		fmt.Fprintln(os.Stderr,
			"             pass any of those flags explicitly to override.")
	}
}

// stackAwareKeepsUsersDBUnset reports whether the stack-aware default must
// leave --users-db alone because the operator already chose a user-database
// backend: a Kubernetes Secret (--users-db-secret / NCC_USERS_DB_SECRET) or a
// file path from NCC_USERS_DB. Both are read after the defaults are applied,
// so filling --users-db first made the Secret backend fail with "mutually
// exclusive" and made NCC_USERS_DB silently lose. This is what crash-looped
// the API pod in the Helm and k8s/ manifests: the container image keeps both
// binaries in /usr/local/bin, which looks like a v2 stack.
func stackAwareKeepsUsersDBUnset(s *apiServer) bool {
	return strings.TrimSpace(s.usersDBSecret) != "" ||
		strings.TrimSpace(os.Getenv("NCC_USERS_DB_SECRET")) != "" ||
		strings.TrimSpace(os.Getenv("NCC_USERS_DB")) != ""
}

// explicitFlagSet returns the set of flag names the user passed on
// the command line. We reparse argv ourselves rather than using
// flag.Visit because Visit reports flags whose values match the
// default when set from environment variables in some workflows;
// here we want strict "did the user type --foo on the command line".
func explicitFlagSet(argv []string) map[string]bool {
	out := map[string]bool{}
	for _, a := range argv {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		a = strings.TrimLeft(a, "-")
		if i := strings.IndexByte(a, '='); i >= 0 {
			a = a[:i]
		}
		if a == "" {
			continue
		}
		out[a] = true
	}
	return out
}
