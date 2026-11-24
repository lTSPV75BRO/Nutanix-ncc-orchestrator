package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"goncc/internal/plural"

	"github.com/Masterminds/semver/v3"
)

// defaultGitHubRepo is used by update to fetch releases (format: owner/repo).
const defaultGitHubRepo = "lTSPV75BRO/Nutanix-ncc-orchestrator"

const v1ToV2MigrationDocURL = "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator#upgrading"

// stripGoBuildGitSuffix removes a trailing -<hex> from ldflags/git injection (e.g. 1.0.0-deadbeef...).
// Does not strip semver prereleases like 1.0.0-rc1 (suffix is not all hex).
func stripGoBuildGitSuffix(s string) string {
	i := strings.LastIndex(s, "-")
	if i <= 0 {
		return s
	}
	tail := s[i+1:]
	if len(tail) < 7 || len(tail) > 64 {
		return s
	}
	for _, c := range tail {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			continue
		}
		return s
	}
	return s[:i]
}

// versionLess returns true if a is semantically less than b (e.g. "0.9.0" < "1.0.0").
// Uses semver when both strings parse; otherwise falls back to simple numeric parsing.
func versionLess(a, b string) bool {
	a = strings.TrimPrefix(strings.TrimSpace(a), "v")
	b = strings.TrimPrefix(strings.TrimSpace(b), "v")
	if va, ea := semver.NewVersion(a); ea == nil {
		if vb, eb := semver.NewVersion(b); eb == nil {
			return va.LessThan(vb)
		}
	}
	a2 := stripGoBuildGitSuffix(a)
	b2 := stripGoBuildGitSuffix(b)
	if va, ea := semver.NewVersion(a2); ea == nil {
		if vb, eb := semver.NewVersion(b2); eb == nil {
			return va.LessThan(vb)
		}
	}
	return versionLessLegacy(a2, b2)
}

func versionLessLegacy(a, b string) bool {
	parse := func(s string) (major, minor, patch int) {
		parts := strings.SplitN(s, ".", 4)
		if len(parts) >= 1 {
			major, _ = strconv.Atoi(parts[0])
		}
		if len(parts) >= 2 {
			minor, _ = strconv.Atoi(parts[1])
		}
		if len(parts) >= 3 {
			patch, _ = strconv.Atoi(strings.SplitN(parts[2], "-", 2)[0])
		}
		return major, minor, patch
	}
	ma, mia, pa := parse(a)
	mb, mib, pb := parse(b)
	if ma != mb {
		return ma < mb
	}
	if mia != mib {
		return mia < mib
	}
	return pa < pb
}

// githubRelease represents the minimal GitHub releases/latest API response.
type githubRelease struct {
	TagName    string        `json:"tag_name"`
	Assets     []githubAsset `json:"assets"`
	HTMLURL    string        `json:"html_url"`
	Draft      bool          `json:"draft"`
	Prerelease bool          `json:"prerelease"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// These bases are variables so update-check fallback behavior can be tested
// without contacting GitHub. The API endpoint provides release assets; the
// browser endpoint is a rate-limit-resistant source for the latest tag only.
var (
	githubAPIBaseURL = "https://api.github.com"
	githubWebBaseURL = "https://github.com"
)

type updateOptions struct {
	CheckOnly          bool
	AllowMajorUpgrade  bool
	Repo               string
	BinaryURL          string
	BinarySHA256       string
	TargetVersion      string
	SkipChecksumVerify bool
	IncludePrerelease  bool // also consider release candidates
	JSONOut            bool // emit a machine-readable result line for --check (consumed by the api-server)
}

// updateCheckJSONPrefix is the sentinel that precedes the machine-readable
// JSON object printed by `update --check --json`. The orchestrator mixes
// human-readable progress on stderr with this single stdout line, and callers
// (the api-server) scan combined output for this prefix to parse the result
// without depending on the wording of the human-readable lines.
const updateCheckJSONPrefix = "NCC_UPDATE_JSON "

func parseVersionMajor(raw string) (int64, error) {
	clean := strings.TrimPrefix(stripGoBuildGitSuffix(strings.TrimSpace(raw)), "v")
	if v, err := semver.NewVersion(clean); err == nil {
		return int64(v.Major()), nil
	}
	parts := strings.SplitN(clean, ".", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return 0, fmt.Errorf("invalid version %q", raw)
	}
	major, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid version %q", raw)
	}
	return major, nil
}

func normalizeGitHubRepo(repo string) (string, error) {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return "", errors.New("repo cannot be empty")
	}
	if strings.HasPrefix(repo, "http://") || strings.HasPrefix(repo, "https://") {
		u, err := url.Parse(repo)
		if err != nil {
			return "", fmt.Errorf("parse repo URL: %w", err)
		}
		if !strings.Contains(strings.ToLower(u.Host), "github.com") {
			return "", fmt.Errorf("repo URL host %q is not github.com", u.Host)
		}
		p := strings.Trim(strings.TrimSuffix(u.Path, ".git"), "/")
		parts := strings.Split(p, "/")
		if len(parts) < 2 {
			return "", fmt.Errorf("repo URL must include owner/repo path, got %q", repo)
		}
		return parts[0] + "/" + parts[1], nil
	}
	parts := strings.Split(strings.Trim(repo, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("repo must be in owner/repo format, got %q", repo)
	}
	return parts[0] + "/" + parts[1], nil
}

func isArchiveAssetURL(raw string) bool {
	u := strings.ToLower(strings.TrimSpace(raw))
	return strings.HasSuffix(u, ".tar.gz") || strings.HasSuffix(u, ".zip")
}

func normalizeSHA256Hex(raw string) (string, error) {
	sum := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "sha256:")))
	if len(sum) != 64 {
		return "", fmt.Errorf("sha256 must be 64 hex characters, got %d", len(sum))
	}
	for _, r := range sum {
		if (r >= 'a' && r <= 'f') || (r >= '0' && r <= '9') {
			continue
		}
		return "", fmt.Errorf("sha256 contains invalid character %q", r)
	}
	return sum, nil
}

func printV1ToV2MigrationSteps() {
	fmt.Fprintln(os.Stderr, "To upgrade from v1.x to v2.x, review migration steps first:")
	fmt.Fprintln(os.Stderr, "1) Deploy/enable v2 services (ncc-api-server and ncc-ui-server).")
	fmt.Fprintln(os.Stderr, "2) Update automation to use API/UI endpoints where needed.")
	fmt.Fprintln(os.Stderr, "3) Validate config compatibility and run preflight checks.")
	fmt.Fprintf(os.Stderr, "Migration guide: %s\n", v1ToV2MigrationDocURL)
}

func enforceMajorUpgradePolicy(currentVer, targetVer string, allowMajorUpgrade bool) error {
	currentMajor, currentErr := parseVersionMajor(currentVer)
	targetMajor, targetErr := parseVersionMajor(targetVer)
	if currentErr != nil || targetErr != nil {
		return nil
	}
	if currentMajor < targetMajor && !allowMajorUpgrade {
		if currentMajor == 1 && targetMajor >= 2 {
			fmt.Fprintln(os.Stderr, "Major upgrade blocked: current binary is v1.x and target release is v2.x.")
			fmt.Fprintln(os.Stderr, "Use --allow-major-upgrade to proceed after migration review.")
			printV1ToV2MigrationSteps()
			return errors.New("major upgrade requires explicit opt-in")
		}
		return fmt.Errorf("major upgrade from v%d to v%d requires --allow-major-upgrade", currentMajor, targetMajor)
	}
	return nil
}

// isPrereleaseVersion reports whether v carries a pre-release suffix
// (2.4.0-rc.1); the git suffix of development builds does not count.
func isPrereleaseVersion(v string) bool {
	parsed, err := semver.NewVersion(strings.TrimPrefix(stripGoBuildGitSuffix(strings.TrimSpace(v)), "v"))
	return err == nil && parsed.Prerelease() != ""
}

// pickLatestSemverRelease returns the newest published release, of one major
// version when majorFilter is set. Release candidates count only with
// includePrerelease, whether GitHub marks them or only their tag says so.
func pickLatestSemverRelease(releases []githubRelease, majorFilter int64, includePrerelease bool) *githubRelease {
	var best *githubRelease
	for i := range releases {
		rel := &releases[i]
		if rel.Draft {
			continue
		}
		if !includePrerelease && (rel.Prerelease || isPrereleaseVersion(rel.TagName)) {
			continue
		}
		tag := strings.TrimSpace(rel.TagName)
		if tag == "" {
			continue
		}
		if majorFilter > 0 {
			maj, err := parseVersionMajor(tag)
			if err != nil || maj != majorFilter {
				continue
			}
		}
		if best == nil || versionLess(best.TagName, rel.TagName) {
			best = rel
		}
	}
	return best
}

func fetchGitHubReleases(repo string, client *http.Client) ([]githubRelease, error) {
	apiURL := strings.TrimRight(githubAPIBaseURL, "/") + "/repos/" + repo + "/releases?per_page=100"
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "ncc-orchestrator/"+stripGoBuildGitSuffix(Version))
	sentToken := false
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && githubTokenAllowed(apiURL) {
		req.Header.Set("Authorization", "Bearer "+token)
		sentToken = true
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch releases: %w", err)
	}
	defer resp.Body.Close()
	if sentToken {
		warnGitHubTokenScopes(resp.Header)
	}
	if resp.StatusCode == 403 && resp.Header.Get("X-RateLimit-Remaining") == "0" {
		return nil, fmt.Errorf("GitHub API rate limited (resets %s): set GITHUB_TOKEN for higher limits or try again later", githubRateLimitReset(resp))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, string(body))
	}
	var releases []githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return nil, fmt.Errorf("parse releases: %w", err)
	}
	return releases, nil
}

var githubScopeWarnOnce sync.Once

// githubTokenScopeWarning returns a warning when GitHub reports that
// GITHUB_TOKEN carries scopes. Update checks read public releases only, so a
// classic token needs no scope and a fine-grained token needs only public
// read access. GitHub sends X-OAuth-Scopes for classic tokens (empty when
// the token has none) and leaves it out for fine-grained tokens.
func githubTokenScopeWarning(h http.Header) string {
	var scopes []string
	for _, s := range strings.Split(h.Get("X-OAuth-Scopes"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			scopes = append(scopes, s)
		}
	}
	if len(scopes) == 0 {
		return ""
	}
	return fmt.Sprintf("warning: GITHUB_TOKEN has %s (%s) but update checks only read public releases. "+
		"Use a fine-grained token with public repository read-only access, or a classic token with no scopes.",
		plural.Count(len(scopes), "scope"), strings.Join(scopes, ", "))
}

func warnGitHubTokenScopes(h http.Header) {
	if msg := githubTokenScopeWarning(h); msg != "" {
		githubScopeWarnOnce.Do(func() { fmt.Fprintln(os.Stderr, msg) })
	}
}

func githubRateLimitReset(resp *http.Response) string {
	raw := strings.TrimSpace(resp.Header.Get("X-RateLimit-Reset"))
	if unix, err := strconv.ParseInt(raw, 10, 64); err == nil && unix > 0 {
		return time.Unix(unix, 0).UTC().Format(time.RFC3339)
	}
	return "later"
}

// fetchLatestGitHubTag follows GitHub's public /releases/latest redirect. It
// does not require the REST API quota and is therefore useful when the API is
// rate limited. It intentionally returns only the tag: assets still require
// authenticated API metadata before an update can be applied.
func fetchLatestGitHubTag(repo string, client *http.Client) (string, error) {
	webURL := strings.TrimRight(githubWebBaseURL, "/") + "/" + repo + "/releases/latest"
	req, err := http.NewRequest(http.MethodGet, webURL, nil)
	if err != nil {
		return "", fmt.Errorf("create latest-release request: %w", err)
	}
	req.Header.Set("User-Agent", "ncc-orchestrator/"+stripGoBuildGitSuffix(Version))
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch latest release page: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("latest release page returned %d", resp.StatusCode)
	}
	path := resp.Request.URL.Path
	marker := "/releases/tag/"
	idx := strings.Index(path, marker)
	if idx == -1 {
		return "", errors.New("latest release page did not identify a release tag")
	}
	tag := strings.Trim(strings.TrimPrefix(path[idx+len(marker):], "/"), " ")
	if tag == "" {
		return "", errors.New("latest release page returned an empty release tag")
	}
	if _, err := semver.NewVersion(strings.TrimPrefix(tag, "v")); err != nil {
		return "", fmt.Errorf("latest release page returned invalid tag %q", tag)
	}
	return tag, nil
}

func pickAssetForCurrentPlatform(rel githubRelease) (downloadURL string, assetName string) {
	return pickAssetForPlatform(rel, runtime.GOOS, runtime.GOARCH, currentExecutableBasename())
}

// pickStackAssetForCurrentPlatform returns the v2 stack archive URL+name for
// this platform, or empty strings if no stack archive is published in the
// release.
//
// The stack archive is the canonical "update-the-whole-package" distribution
// path. It is independent of which binary (orchestrator, api-server,
// ui-server) was invoked, independent of how that binary was renamed, and
// upgrades all v2 components atomically (orchestrator + api-server +
// ui-server + frontend-dist + example_config.yaml). For releases that do not
// publish a stack archive (e.g. legacy v1.x releases), this returns empty
// strings and callers fall back to the single-binary update path via
// pickAssetForCurrentPlatform.
func pickStackAssetForCurrentPlatform(rel githubRelease) (downloadURL string, assetName string) {
	return pickStackAssetForPlatform(rel, runtime.GOOS, runtime.GOARCH)
}

// pickStackAssetForPlatform is the testable inner function of
// pickStackAssetForCurrentPlatform. Returns the first archive asset whose
// name contains "ncc-v2-stack", goos, and goarch.
func pickStackAssetForPlatform(rel githubRelease, goos, goarch string) (downloadURL string, assetName string) {
	goos = strings.ToLower(strings.TrimSpace(goos))
	goarch = strings.ToLower(strings.TrimSpace(goarch))
	for _, a := range rel.Assets {
		name := strings.ToLower(a.Name)
		if !strings.Contains(name, "ncc-v2-stack") {
			continue
		}
		if !strings.Contains(name, goos) || !strings.Contains(name, goarch) {
			continue
		}
		if !isArchiveAssetURL(a.BrowserDownloadURL) {
			continue
		}
		return a.BrowserDownloadURL, a.Name
	}
	return "", ""
}

// currentExecutableBasename returns the basename of the running executable
// with any ".exe" suffix stripped, lowercased. Empty string if it cannot be
// determined (callers fall back to the legacy first-match behavior).
func currentExecutableBasename() string {
	p, err := os.Executable()
	if err != nil || p == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSuffix(filepath.Base(p), ".exe"))
}

// pickAssetForPlatform selects the most appropriate release asset for the
// given GOOS/GOARCH and the running executable's basename (e.g.
// "ncc-orchestrator"). Preference order:
//
//  1. Non-archive asset whose name starts with "<exeBase>-" AND contains both
//     goos and goarch. This guards against silent corruption when a release
//     ships multiple binaries per platform (regression that affected the
//     v1.x→v2.0.0 self-updater, see RELEASE_NOTES_v2.0.0 known-issues
//     section).
//  2. Any other non-archive asset whose name contains both goos and goarch
//     (legacy v1.x behavior, preserved for binaries renamed by the user or
//     custom forks).
//  3. Archive asset (.tar.gz / .zip) whose name contains both goos and
//     goarch, returned for inspection by the caller — the install path then
//     emits a "download and extract" hint rather than overwriting.
//
// Returns empty strings when no asset matches.
func pickAssetForPlatform(rel githubRelease, goos, goarch, exeBase string) (downloadURL string, assetName string) {
	goos = strings.ToLower(strings.TrimSpace(goos))
	goarch = strings.ToLower(strings.TrimSpace(goarch))
	exeBase = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(exeBase), ".exe"))

	var (
		prefixedURL, prefixedName string
		fallbackURL, fallbackName string
		archiveURL, archiveName   string
	)
	for _, a := range rel.Assets {
		name := strings.ToLower(a.Name)
		if !strings.Contains(name, goos) || !strings.Contains(name, goarch) {
			continue
		}
		isArchive := isArchiveAssetURL(a.BrowserDownloadURL)

		if exeBase != "" && strings.HasPrefix(name, exeBase+"-") {
			if isArchive {
				if archiveURL == "" {
					archiveURL = a.BrowserDownloadURL
					archiveName = a.Name
				}
				continue
			}
			if prefixedURL == "" {
				prefixedURL = a.BrowserDownloadURL
				prefixedName = a.Name
			}
			continue
		}

		if isArchive {
			if archiveURL == "" {
				archiveURL = a.BrowserDownloadURL
				archiveName = a.Name
			}
			continue
		}

		if fallbackURL == "" {
			fallbackURL = a.BrowserDownloadURL
			fallbackName = a.Name
		}
	}

	if prefixedURL != "" {
		return prefixedURL, prefixedName
	}
	if fallbackURL != "" {
		return fallbackURL, fallbackName
	}
	return archiveURL, archiveName
}
