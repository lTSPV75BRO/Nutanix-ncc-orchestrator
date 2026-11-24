package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// runVerifyCommand backs the `ncc-orchestrator verify` subcommand.
// Renders the trust-relevant metadata in a stable, scriptable format
// (single-key-per-line) so support tickets and CI checks can pin
// fields with simple grep. The hash is computed over the running
// executable's bytes on disk; if the file is unreadable (chroot,
// procfs-only systems, etc.) we degrade gracefully rather than
// failing the entire command.
//
// Wraps an io.Writer rather than printing directly so unit tests can
// drive the function with a bytes.Buffer.
//
// Note on attribution: this is an independent open-source project
// (MIT licensed) and is not affiliated with or endorsed by Nutanix,
// Inc. The "project_url" field below is the source of truth for
// where the binary came from.
// verifyOptions controls the `verify` subcommand. The default (zero value) is
// the historical offline behavior — print the self-hash and a compare URL. With
// Online set, verify fetches the matching release's checksums.txt from GitHub
// and reports MATCH / MISMATCH / NOT FOUND, returning a non-zero error on
// mismatch so it is usable in CI / health checks.
type verifyOptions struct {
	Online           bool
	ReleaseTag       string // override the release tag to check against ("" = stamped version)
	Repo             string // override the GitHub repo ("" = default)
	RequireSignature bool   // fail unless the Ed25519 signature over checksums.txt verifies
}

func runVerifyCommand(out io.Writer, opts verifyOptions) error {
	exe, err := os.Executable()
	if err != nil {
		exe = "(unknown)"
	}
	exeReal := exe
	if r, errReal := filepath.EvalSymlinks(exe); errReal == nil {
		exeReal = r
	}
	hash, hashErr := sha256OfFile(exeReal)
	gitRev, gitDirty, _ := goBuildInfoVCS()
	fmt.Fprintln(out, "ncc-orchestrator verify")
	fmt.Fprintln(out, "-----------------------")
	fmt.Fprintf(out, "version:           %s\n", Version)
	fmt.Fprintf(out, "stream:            %s\n", Stream)
	fmt.Fprintf(out, "build_date:        %s\n", BuildDate)
	fmt.Fprintf(out, "go_version:        %s\n", GoVersion)
	fmt.Fprintf(out, "os_arch:           %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(out, "git_revision:      %s\n", strDefault(gitRev, "(unknown)"))
	fmt.Fprintf(out, "git_dirty:         %t\n", gitDirty)
	fmt.Fprintf(out, "executable_path:   %s\n", exeReal)
	if hashErr != nil {
		fmt.Fprintf(out, "executable_sha256: (unavailable: %v)\n", hashErr)
	} else {
		fmt.Fprintf(out, "executable_sha256: %s\n", hash)
	}
	fmt.Fprintf(out, "license:           MIT\n")
	fmt.Fprintf(out, "project_url:       https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator\n")
	fmt.Fprintf(out, "affiliation:       independent open-source project; not affiliated with or endorsed by Nutanix, Inc.\n")

	// Strip git-rev / dirty suffixes (e.g. "2.0.2-<sha>" or "2.0.2-dirty")
	// so the URL / release lookup points at the canonical release tag.
	tagVersion := Version
	if i := strings.IndexAny(tagVersion, "-+"); i >= 0 {
		tagVersion = tagVersion[:i]
	}
	releaseTag := tagVersion
	if t := strings.TrimSpace(opts.ReleaseTag); t != "" {
		releaseTag = strings.TrimPrefix(t, "v")
	}

	if !opts.Online {
		fmt.Fprintf(out, "verify:            compare executable_sha256 against checksums.txt at\n")
		fmt.Fprintf(out, "                   https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/tag/v%s\n", releaseTag)
		fmt.Fprintf(out, "                   (run `verify --online` to fetch and compare automatically)\n")
		return nil
	}

	return runVerifyOnline(out, exeReal, hash, hashErr, gitDirty, releaseTag, opts.Repo, opts.RequireSignature)
}

// runVerifyOnline fetches the matching release's checksums.txt from GitHub and
// compares the running executable's SHA-256 against the published value for the
// platform's ncc-orchestrator-<os>-<arch> asset. It prints a verify_result line
// and returns a non-zero error on MISMATCH / NOT FOUND so callers (and CI) can
// branch on the exit code.
//
// Trust note: the checksum is fetched from the same repository that serves the
// binary, so this authenticates an *accidentally corrupted, truncated, or
// MITM'd* download and pins the *version* — it is not proof against a
// compromised release. Cryptographic provenance (signed checksums / GitHub
// artifact attestations) is tracked as future work.
func runVerifyOnline(out io.Writer, exePath, localHash string, hashErr error, gitDirty bool, releaseTag, repoOverride string, requireSignature bool) error {
	if hashErr != nil {
		return fmt.Errorf("verify --online: cannot hash executable: %w", hashErr)
	}
	if gitDirty {
		fmt.Fprintf(out, "verify_result:     SKIPPED — locally-modified build (git_dirty=true) will not match a published release\n")
		return nil
	}
	repo := strings.TrimSpace(repoOverride)
	if repo == "" {
		repo = defaultGitHubRepo
	}
	repo, err := normalizeGitHubRepo(repo)
	if err != nil {
		return fmt.Errorf("verify --online: %w", err)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	fmt.Fprintf(out, "verify:            checking %s against %s release v%s checksums.txt …\n", filepath.Base(exePath), repo, releaseTag)
	releases, err := fetchGitHubReleases(repo, client)
	if err != nil {
		return fmt.Errorf("verify --online: fetch releases: %w", err)
	}
	var rel *githubRelease
	for i := range releases {
		if strings.EqualFold(strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v"), releaseTag) {
			rel = &releases[i]
			break
		}
	}
	if rel == nil {
		fmt.Fprintf(out, "verify_result:     NOT FOUND — no release tagged v%s in %s (dev build or unpublished version?)\n", releaseTag, repo)
		return fmt.Errorf("release v%s not found in %s", releaseTag, repo)
	}
	_, assetName := pickAssetForCurrentPlatform(*rel)
	if assetName == "" {
		fmt.Fprintf(out, "verify_result:     NOT FOUND — release v%s has no asset for %s/%s\n", releaseTag, runtime.GOOS, runtime.GOARCH)
		return fmt.Errorf("no asset for %s/%s in release v%s", runtime.GOOS, runtime.GOARCH, releaseTag)
	}
	csBody, csAsset, err := fetchReleaseChecksumBody(rel, client)
	if err != nil {
		return fmt.Errorf("verify --online: %w", err)
	}
	expected := parseChecksumFile(csBody, assetName)
	if expected == "" {
		return fmt.Errorf("verify --online: checksum entry for %s not found in %s", assetName, csAsset)
	}
	fmt.Fprintf(out, "release_asset:     %s\n", assetName)
	fmt.Fprintf(out, "checksum_source:   %s (%s)\n", csAsset, rel.TagName)
	fmt.Fprintf(out, "expected_sha256:   %s\n", expected)

	// Verify the Ed25519 signature over checksums.txt before trusting the hash.
	// INVALID always fails; MISSING/SKIPPED fail only with --require-signature.
	sigStatus, sigErr := verifyChecksumSignature(rel, csBody, client, requireSignature)
	fmt.Fprintf(out, "signature_result:  %s\n", sigStatus)
	if sigErr != nil {
		fmt.Fprintf(out, "verify_result:     FAILED — checksum signature %s: %v\n", sigStatus, sigErr)
		return fmt.Errorf("checksum signature %s: %w", sigStatus, sigErr)
	}
	if strings.EqualFold(expected, localHash) {
		fmt.Fprintf(out, "verify_result:     MATCH — executable matches the published checksum for v%s\n", releaseTag)
		return nil
	}
	fmt.Fprintf(out, "verify_result:     MISMATCH — executable does NOT match the published checksum for v%s\n", releaseTag)
	return fmt.Errorf("checksum mismatch for v%s: expected %s, got %s", releaseTag, expected, localHash)
}

// fetchReleaseChecksum finds the release's checksums.txt (or sha256/.sha256)
// asset, downloads it, and returns the published SHA-256 for assetName plus the
// checksum asset's name. Mirrors the asset selection in
// verifyAssetAgainstReleaseChecksum but returns the hash for display/comparison
// rather than verifying a downloaded body.
func fetchReleaseChecksum(rel *githubRelease, assetName string, client *http.Client) (expected, checksumAsset string, err error) {
	csBody, csAsset, err := fetchReleaseChecksumBody(rel, client)
	if err != nil {
		return "", csAsset, err
	}
	h := parseChecksumFile(csBody, assetName)
	if h == "" {
		return "", csAsset, fmt.Errorf("checksum entry for %s not found in %s", assetName, csAsset)
	}
	return h, csAsset, nil
}

// fetchReleaseChecksumBody downloads the release's checksums.txt (or
// sha256/.sha256) asset and returns its raw bytes plus the asset name. The raw
// body is what release-signature verification authenticates, so callers that
// verify signatures must use the exact bytes returned here.
func fetchReleaseChecksumBody(rel *githubRelease, client *http.Client) (csBody []byte, checksumAsset string, err error) {
	if rel == nil {
		return nil, "", errors.New("nil release passed to checksum fetcher")
	}
	for _, a := range rel.Assets {
		an := strings.ToLower(a.Name)
		// The signature asset (checksums.txt.sig) also contains "checksum"; skip it.
		if strings.HasSuffix(an, ".sig") {
			continue
		}
		if !(strings.Contains(an, "checksum") || strings.Contains(an, "sha256") || strings.HasSuffix(an, ".sha256")) {
			continue
		}
		body, ferr := fetchURL(a.BrowserDownloadURL, client)
		if ferr != nil {
			return nil, a.Name, fmt.Errorf("fetch checksum asset %s: %w", a.Name, ferr)
		}
		return body, a.Name, nil
	}
	return nil, "", fmt.Errorf("no checksum asset found for release %s", rel.TagName)
}

// sha256OfFile streams a file through SHA-256. Streamed (rather than
// reading into memory) so the verify command stays fast even on a
// 50MB binary built with -trimpath (no debug info).
func sha256OfFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// goBuildInfoVCS pulls the (commit, dirty, build_time) triple Go's
// linker embeds via -buildvcs=true. Returns ("", false, false) on
// binaries built with -buildvcs=false. Stable across Go releases —
// the keys "vcs.revision", "vcs.modified", "vcs.time" have been
// part of debug.ReadBuildInfo since Go 1.18.
func goBuildInfoVCS() (revision string, dirty bool, ok bool) {
	info, infoOK := debug.ReadBuildInfo()
	if !infoOK {
		return "", false, false
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	return revision, dirty, revision != ""
}

func strDefault(s, dflt string) string {
	if strings.TrimSpace(s) == "" {
		return dflt
	}
	return s
}

// runnerEnvKeys are the NCC_ suffixes for runner config. Hyphens in the
// config key become underscores. webhook-secret is env/config only.
var runnerEnvKeys = []string{
	"CONFIG",
	"SCHEMA_VERSION",
	"SKIP_PREFLIGHT_CHECK",
	"AUTO",
	"AUTOMATION_LEVEL",
	"CLUSTER_SOURCE_MODE",
	"CLUSTERS",
	"CLUSTERS_FILE",
	"PCS",
	"PCS_FILE",
	"PRISM_CENTRAL_URL",
	"DISCOVER_API_VERSION",
	"USERNAME",
	"PASSWORD",
	"NCC_API_VERSION",
	"NUTANIX_V4_API_VERSION",
	"PC_ALERTS_CACHE_TTL",
	"INSECURE_SKIP_VERIFY",
	"CA_BUNDLE",
	"PIN_SHA256",
	"TIMEOUT",
	"REQUEST_TIMEOUT",
	"POLL_INTERVAL",
	"POLL_JITTER",
	"MAX_PARALLEL",
	"OUTPUTS",
	"OUTPUT_DIR_LOGS",
	"OUTPUT_DIR_FILTERED",
	"SINGLE_REPORT",
	"LOG_FILE",
	"LOG_LEVEL",
	"LOG_HTTP",
	"RETRY_MAX_ATTEMPTS",
	"RETRY_BASE_DELAY",
	"RETRY_MAX_DELAY",
	"RETRY_CIRCUIT_BREAKER",
	"GEN_TEST_AGG",
	"PROM_ENABLED",
	"PROM_DIR",
	"RUN_HISTORY",
	"RUN_HISTORY_DIR",
	"RETAIN_LAST",
	"RETAIN_DAYS",
	"ARTIFACT_RETAIN_DAYS",
	"ARTIFACT_RETAIN_MAX_FILES",
	"NOTIFY_ON_REGRESSION",
	"ADAPTIVE_PARALLELISM",
	"POLICY_GATES",
	"QUIET_HOURS",
	"MAINTENANCE_WINDOWS",
	"FLAKY_LOOKBACK_RUNS",
	"FLAKY_MIN_TRANSITIONS",
	"SEVERITY_FILTER",
	"EXCLUDE_ALERT_TITLES",
	"EXCLUDE_ALERT_TITLES_FILE",
	"EXCLUDE_ALERT_MATCH_MODE",
	"DRY_RUN",
	"REPLAY",
	"MAX_IDLE_CONNS",
	"MAX_IDLE_CONNS_PER_HOST",
	"MAX_CONNS_PER_HOST",
	"IDLE_CONN_TIMEOUT",
	"EMAIL_ENABLED",
	"EMAIL_ATTACH_HTML",
	"NOTIFY_DIGEST",
	"SMTP_SERVER",
	"SMTP_PORT",
	"SMTP_USER",
	"SMTP_PASSWORD",
	"EMAIL_FROM",
	"EMAIL_TO",
	"EMAIL_USE_TLS",
	"SMTP_INSECURE_SKIP_VERIFY",
	"EMAIL_SUBJECT_TEMPLATE",
	"EMAIL_BODY_TEMPLATE",
	"WEBHOOK_ENABLED",
	"WEBHOOK_INCLUDE_HTML",
	"WEBHOOK_URL",
	"WEBHOOK_HEADERS",
	"WEBHOOK_TEMPLATE",
	"WEBHOOK_SECRET",
	"NOTIFICATION_DEADLETTER_DIR",
	"SLACK_ENABLED",
	"SLACK_WEBHOOK_URL",
	"SLACK_CHANNEL",
	"SECRETS_PROVIDER",
	"SECRETS_FILE",
}

// serverEnvKeys are process settings for the API and UI. env-info reports
// only whether each one is set, never the value.
var serverEnvKeys = []string{
	"API_TOKEN",
	"API_STATIC_TOKEN",
	"API_VIEWER_TOKEN",
	"JWT_SECRET",
	"TOKEN_EXPIRY",
	"CORS_ORIGIN",
	"UI_ORIGIN",
	"USERS_DB",
	"USERS_DB_SECRET",
	"USERS_DB_SECRET_NAMESPACE",
	"USERS_FILE",
	"MASTER_KEY",
	"MASTER_KEY_FILE",
	"DISABLE_LOCAL_ACCOUNTS",
	"RUNTIME_MODE",
	"IMAGE_TAG",
	"ORCHESTRATOR_IMAGE_TAG",
	"UI_IMAGE_TAG",
	"OTEL_ENABLED",
	"BACKUP_PASSPHRASE",
	"BACKUP_KEY",
	"BACKUP_KEY_FILE",
}

var sensitiveEnvKeys = map[string]bool{
	"PASSWORD":          true,
	"SMTP_PASSWORD":     true,
	"WEBHOOK_URL":       true,
	"WEBHOOK_HEADERS":   true,
	"WEBHOOK_SECRET":    true,
	"SLACK_WEBHOOK_URL": true,
}

func printEnvInfo() {
	fmt.Println("Runner environment variables (prefix NCC_). Secrets are masked.")
	fmt.Println("Unset a variable to use the config file or the built-in default.")
	fmt.Println("NCC_WEBHOOK_SECRET has no CLI flag.")
	for _, key := range runnerEnvKeys {
		envVar := "NCC_" + key
		val := os.Getenv(envVar)
		if val == "" {
			fmt.Printf("%s = (not set)\n", envVar)
			continue
		}
		if sensitiveEnvKeys[key] {
			fmt.Printf("%s = %s\n", envVar, maskPassword(val))
			continue
		}
		fmt.Printf("%s = %s\n", envVar, val)
	}
	fmt.Println()
	fmt.Println("API and UI environment variables (values are not printed):")
	for _, key := range serverEnvKeys {
		envVar := "NCC_" + key
		if os.Getenv(envVar) == "" {
			fmt.Printf("%s = (not set)\n", envVar)
			continue
		}
		fmt.Printf("%s = (set)\n", envVar)
	}
}
