// Package v2layout centralizes the "is the running binary inside an
// extracted ncc-v2-stack archive?" detection used by ncc-orchestrator,
// ncc-api-server, and ncc-ui-server.
//
// Background: the v2 stack archive ships as
//
//	<root>/
//	  bin/
//	    ncc-orchestrator
//	    ncc-api-server
//	    ncc-ui-server
//	  frontend-dist/
//	  example_config.yaml
//
// In v2.0.2 the orchestrator's v2-check / v2-start / v2-stop / uninstall
// commands learned to auto-detect <root> when invoked from <root>/bin/<self>.
// This package extends the same detection to the api-server and ui-server
// so that a user who copies just one of those binaries out of the archive
// and runs it directly (or who runs it from inside the unpacked stack)
// still gets sensible defaults for config-path, output-dir, log-dir,
// frontend-dist, and the API token file — without having to pass any
// flags. This eliminates a class of "5 issues" v2-check failures that
// surfaced for users following the documented "extract, cd bin, run"
// flow.
//
// Detection is purely structural — no environment variables, no special
// markers — so it works the same on every platform and inside Docker
// images that just copy bin/* into /usr/local/bin (where the function
// will simply return ok=false and the caller falls back to its
// pre-existing CWD-relative defaults).
package v2layout

import (
	"os"
	"path/filepath"
	"runtime"
)

// Marker file/dir names inside the stack root that identify a valid v2
// stack layout. We accept either the frontend-dist directory or a
// sibling api-server binary as proof — the first because every stack
// archive ships it, the second because some operators delete
// frontend-dist when they only run the API in headless mode.
const (
	stackBinDir       = "bin"
	stackFrontendDir  = "frontend-dist"
	apiServerBaseName = "ncc-api-server"
	uiServerBaseName  = "ncc-ui-server"
	tokenFileName     = ".ncc-api-token"
	configFileName    = "config.yaml"
	exampleConfigName = "example_config.yaml"
	outputDirName     = "outputfiles"
	logDirName        = "nccfiles"
)

// DetectStackRoot returns the absolute install-dir for the v2 stack the
// current executable belongs to, or ok=false if the binary is not
// running from inside a recognizable stack layout. The root is the
// PARENT of the directory the binary lives in (i.e. <root>/bin/<self>
// → <root>).
//
// The caller passes its own exe directory rather than this package
// calling os.Executable() so unit tests can drive the helper without
// having to redirect os.Executable at runtime.
func DetectStackRoot(exeDir string) (string, bool) {
	if exeDir == "" {
		return "", false
	}
	if filepath.Base(exeDir) != stackBinDir {
		return "", false
	}
	parent := filepath.Dir(exeDir)
	if parent == exeDir || parent == "" {
		return "", false
	}
	if isDir(filepath.Join(parent, stackFrontendDir)) {
		return parent, true
	}
	if findBinary(parent, apiServerBaseName) != "" {
		return parent, true
	}
	return "", false
}

// DetectStackRootFromExe is a convenience wrapper that calls
// os.Executable() and feeds the directory into DetectStackRoot. Returns
// ok=false if os.Executable fails.
func DetectStackRootFromExe() (string, bool) {
	exe, err := os.Executable()
	if err != nil {
		return "", false
	}
	return DetectStackRoot(filepath.Dir(exe))
}

// FindBinary returns the absolute path of an executable named
// `<base>` or `<base>-<goos>-<goarch>[.exe]` inside <root>/bin, or ""
// when no candidate exists. Mirrors the orchestrator's
// existingBinaryInInstallDir and is exported here so the api-server
// and ui-server can locate the orchestrator binary for help-text or
// recovery hints.
func FindBinary(installDir, base string) string {
	return findBinary(installDir, base)
}

// ConfigPath returns the recommended config path for an install dir:
// <root>/config.yaml when present, otherwise <root>/example_config.yaml
// when present, otherwise an empty string. Callers typically prefer the
// first non-empty result.
func ConfigPath(installDir string) string {
	root := filepath.Clean(installDir)
	if cfg := filepath.Join(root, configFileName); isFile(cfg) {
		return cfg
	}
	if cfg := filepath.Join(root, exampleConfigName); isFile(cfg) {
		return cfg
	}
	return ""
}

// OutputDir / LogDir / TokenFile / FrontendDir return the canonical
// install-dir-relative path for the given resource. The returned path
// may not yet exist; callers are expected to MkdirAll/touch as
// needed.
func OutputDir(installDir string) string   { return filepath.Join(installDir, outputDirName) }
func LogDir(installDir string) string      { return filepath.Join(installDir, logDirName) }
func TokenFile(installDir string) string   { return filepath.Join(installDir, tokenFileName) }
func FrontendDir(installDir string) string { return filepath.Join(installDir, stackFrontendDir) }

// PlatformSuffix returns "<goos>-<goarch>" (used by both stack archive
// asset names and platform-suffixed binary names inside legacy v2.0.0
// stacks).
func PlatformSuffix() string { return runtime.GOOS + "-" + runtime.GOARCH }

// ResolveToReal returns the absolute, symlink-resolved form of p.
// For paths that don't yet exist (e.g. an output-dir we'll MkdirAll
// in a moment) it walks up to the first existing ancestor, resolves
// THAT, then reattaches the non-existing suffix. This is essential
// on macOS where /tmp and /var are symlinks to /private/tmp and
// /private/var: the api-server's path-traversal sandbox (--repo-root)
// uses filepath.EvalSymlinks under the hood, so any path passed
// alongside it must be pre-resolved to the same canonical form or
// validation rejects it as "path escapes repo root".
func ResolveToReal(p string) string {
	if p == "" {
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
	parent := filepath.Dir(abs)
	rest := filepath.Base(abs)
	for {
		if parent == filepath.Dir(parent) {
			return abs
		}
		if real, err := filepath.EvalSymlinks(parent); err == nil {
			return filepath.Clean(filepath.Join(real, rest))
		}
		rest = filepath.Join(filepath.Base(parent), rest)
		parent = filepath.Dir(parent)
	}
}

func findBinary(installDir, base string) string {
	if installDir == "" || base == "" {
		return ""
	}
	binDir := filepath.Join(installDir, stackBinDir)
	candidates := []string{
		base,
		base + ".exe",
		base + "-" + PlatformSuffix(),
		base + "-" + PlatformSuffix() + ".exe",
	}
	for _, name := range candidates {
		p := filepath.Join(binDir, name)
		if isFile(p) {
			return p
		}
	}
	return ""
}

func isDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

func isFile(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}
