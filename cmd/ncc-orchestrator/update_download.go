package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"goncc/internal/runtimecaps"
)

func downloadBinaryURL(client *http.Client, downloadURL string) ([]byte, error) {
	dlReq, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create download request: %w", err)
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && githubTokenAllowed(downloadURL) {
		dlReq.Header.Set("Authorization", "Bearer "+token)
	}
	dlResp, err := client.Do(dlReq)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer dlResp.Body.Close()
	if dlResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download returned %d", dlResp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(dlResp.Body, 200*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read download: %w", err)
	}
	return body, nil
}

func installDownloadedBinary(body []byte, targetVer string) error {
	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}
	if resolved, rerr := filepath.EvalSymlinks(selfPath); rerr == nil {
		selfPath = resolved
	}
	// Move the running image aside and activate the new one (works on
	// Windows, where a running .exe cannot be overwritten but can be renamed).
	if err := replaceRunningExecutable(selfPath, body); err == nil {
		fmt.Fprintln(os.Stderr, "Update complete. Run the binary again to use the new version.")
		return nil
	} else if runtime.GOOS != "windows" {
		fallback := filepath.Join(".", "ncc-orchestrator-"+strings.TrimPrefix(strings.TrimSpace(targetVer), "v"))
		if wErr := os.WriteFile(fallback, body, 0755); wErr != nil {
			return fmt.Errorf("replace binary failed (%v); write to %s failed: %w", err, fallback, wErr)
		}
		return fmt.Errorf("%w: could not replace %s (%v); the new binary was saved as %s — move it into place and run again", errManualSwapRequired, selfPath, err, fallback)
	} else {
		newPath := selfPath + ".new.exe"
		if wErr := os.WriteFile(newPath, body, 0755); wErr != nil {
			return fmt.Errorf("replace binary failed (%v); write %s failed: %w", err, newPath, wErr)
		}
		// Last resort: a helper that waits for this process to exit and then
		// swaps the file. Report it as an error so no caller (in particular
		// the in-app update job) mistakes it for a completed update.
		if helperPath, herr := writeWindowsUpdateSwapHelper(selfPath, newPath); herr == nil {
			return fmt.Errorf("%w: could not replace %s (%v); exit this program, then run %s to finish", errManualSwapRequired, selfPath, err, helperPath)
		}
		return fmt.Errorf("%w: could not replace %s (%v); exit this program, then replace it with %s", errManualSwapRequired, selfPath, err, newPath)
	}
}

// writeWindowsUpdateSwapHelper writes an apply-ncc-update.cmd next to the
// running executable. The script polls `move` until the old binary's lock is
// released (i.e. this process has exited), swaps the freshly downloaded
// `.new.exe` over it, and then deletes itself. This replaces the previous
// "manually copy the file yourself" instruction with a single command and is
// safe to re-run. The generator is pure (no Windows-only APIs) so it can be
// unit-tested on any platform.
func writeWindowsUpdateSwapHelper(selfPath, newPath string) (string, error) {
	dir := filepath.Dir(selfPath)
	base := filepath.Base(selfPath)
	helperPath := filepath.Join(dir, "apply-ncc-update.cmd")
	var b strings.Builder
	b.WriteString("@echo off\r\n")
	b.WriteString("setlocal\r\n")
	b.WriteString("echo Waiting for " + base + " to close...\r\n")
	b.WriteString(":retry\r\n")
	b.WriteString("move /y \"" + newPath + "\" \"" + selfPath + "\" >nul 2>&1\r\n")
	b.WriteString("if errorlevel 1 (\r\n")
	b.WriteString("  timeout /t 1 /nobreak >nul\r\n")
	b.WriteString("  goto retry\r\n")
	b.WriteString(")\r\n")
	b.WriteString("echo Update applied to " + selfPath + "\r\n")
	b.WriteString("del \"%~f0\"\r\n")
	if err := os.WriteFile(helperPath, []byte(b.String()), 0o755); err != nil {
		return "", err
	}
	return helperPath, nil
}

// resolvePackageInstallDir returns the directory that should receive the
// extracted v2 stack components when the user runs `update`. It is invariant
// to which binary (orchestrator, api-server, ui-server) was invoked and to
// any user-side renames — only the running executable's location matters.
//
// Resolution order:
//
//  1. If the running binary lives inside <X>/bin/, treat <X> as the install
//     dir (the binary is already inside a bootstrapped stack layout, so
//     refresh that layout in place).
//  2. Otherwise, treat the running binary's directory as the install dir
//     (flat layout: <install-dir>/<self>, <install-dir>/bin/*,
//     <install-dir>/frontend-dist/, <install-dir>/example_config.yaml).
//
// Callers may still self-replace the running binary atomically via
// installDownloadedBinary, regardless of the install-dir result.
func resolvePackageInstallDir(selfPath string) string {
	dir := filepath.Dir(selfPath)
	if filepath.Base(dir) == "bin" {
		return filepath.Dir(dir)
	}
	return dir
}

// installPackageUpdate downloads the stack archive, verifies the checksum
// against the release's checksums.txt asset, extracts to a temp dir, copies
// the v2 stack components (bin/*, frontend-dist/, example_config.yaml) into
// the resolved install dir, and atomically self-replaces the running binary
// with the canonically-named match from the extracted bin/.
//
// This is the canonical "upgrade-the-whole-package" code path that handles
// the running binary being orchestrator OR api-server OR ui-server (or any
// renamed variant of those). The selection is based on the running
// executable's basename; if no match is found in the archive, the function
// falls back to bin/ncc-orchestrator and emits a guidance message.
func installPackageUpdate(stackURL, stackName string, rel *githubRelease, targetVer string, client *http.Client, skipChecksumVerify bool) error {
	fmt.Fprintf(os.Stderr, "Downloading package %s ...\n", stackURL)
	body, err := downloadBinaryURL(client, stackURL)
	if err != nil {
		return err
	}
	if err := verifyDownloadedAsset(rel, stackName, body, client, skipChecksumVerify); err != nil {
		return err
	}

	tmpRoot, err := os.MkdirTemp("", "ncc-update-package-")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpRoot)

	if err := extractArchiveByAssetName(body, stackName, tmpRoot); err != nil {
		return fmt.Errorf("extract stack archive %s: %w", stackName, err)
	}
	if !hasBootstrappedV2Layout(tmpRoot) {
		return fmt.Errorf("stack archive %s extracted but required layout was not found (expected bin/ncc-api-server, bin/ncc-ui-server, frontend-dist/)", stackName)
	}

	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}
	installDir := resolvePackageInstallDir(selfPath)
	fmt.Fprintf(os.Stderr, "Installing package into %s ...\n", installDir)

	if err := installStackComponents(tmpRoot, installDir); err != nil {
		return fmt.Errorf("install stack components: %w", err)
	}

	// Locate the new binary that matches the currently-running one so we
	// can self-replace. We honor the user's renamed binaries by matching
	// the running exe's basename first, then fall back to the canonical
	// ncc-orchestrator name.
	exeBase := currentExecutableBasename()
	newSelfPath := ""
	if exeBase != "" {
		newSelfPath = existingBinaryInInstallDir(tmpRoot, exeBase)
	}
	if newSelfPath == "" {
		newSelfPath = existingBinaryInInstallDir(tmpRoot, "ncc-orchestrator")
	}
	if newSelfPath == "" {
		fmt.Fprintf(os.Stderr, "Package update complete. The running binary was not replaced because no matching binary was found in the stack archive. Re-launch from %s.\n", filepath.Join(installDir, "bin"))
		return nil
	}
	newBody, err := os.ReadFile(newSelfPath)
	if err != nil {
		return fmt.Errorf("read new binary %s: %w", newSelfPath, err)
	}
	return installDownloadedBinary(newBody, targetVer)
}

// verifyDownloadedAsset verifies `body` against the release's published
// checksum for `assetName` unless skipVerify is set. When verification is
// skipped it prints an explicit, support-friendly warning so the operator
// knows the bytes were NOT authenticated (intended only for air-gapped or
// internally-mirrored installs). When not skipped it delegates to
// verifyAssetAgainstReleaseChecksum, which hard-fails on a missing checksum
// asset or a hash mismatch.
func verifyDownloadedAsset(rel *githubRelease, assetName string, body []byte, client *http.Client, skipVerify bool) error {
	if skipVerify {
		fmt.Fprintf(os.Stderr, "warning: --skip-checksum-verify set; NOT verifying %s against release checksums.txt\n", assetName)
		return nil
	}
	return verifyAssetAgainstReleaseChecksum(rel, assetName, body, client)
}

// verifyAssetAgainstReleaseChecksum looks up `assetName` in the release's
// checksums.txt asset (or sha256/.sha256 asset) and compares its hash to a
// fresh sha256 of `body`. Returns nil on match, an error otherwise. If no
// checksum asset is present on the release the function returns an error
// (package updates must be authenticated).
func verifyAssetAgainstReleaseChecksum(rel *githubRelease, assetName string, body []byte, client *http.Client) error {
	if rel == nil {
		return errors.New("nil release passed to checksum verifier")
	}
	for _, a := range rel.Assets {
		an := strings.ToLower(a.Name)
		if strings.HasSuffix(an, ".sig") {
			continue // the detached signature asset, not the checksum list
		}
		if !(strings.Contains(an, "checksum") || strings.Contains(an, "sha256") || strings.HasSuffix(an, ".sha256")) {
			continue
		}
		csBody, err := fetchURL(a.BrowserDownloadURL, client)
		if err != nil {
			return fmt.Errorf("fetch checksum asset %s: %w", a.Name, err)
		}
		// When this build embeds a release public key, the checksums.txt must
		// carry a valid Ed25519 signature before we trust any hash in it —
		// otherwise an attacker who can swap the asset could swap its checksum
		// too. Unsigned/dev builds (no embedded key) keep the prior behavior.
		// A signed build also refuses a release that carries no signature at
		// all: otherwise removing the .sig asset turned the check off.
		_, signedBuild := embeddedReleasePublicKey()
		if sigStatus, sigErr := verifyChecksumSignature(rel, csBody, client, signedBuild); sigErr != nil {
			return fmt.Errorf("refusing to install %s: checksums.txt signature %s: %w", assetName, sigStatus, sigErr)
		} else if sigStatus == sigValid {
			fmt.Fprintln(os.Stderr, "Checksum signature verified (Ed25519).")
		}
		expectedHash := parseChecksumFile(csBody, assetName)
		if expectedHash == "" {
			return fmt.Errorf("checksum entry for %s not found in %s", assetName, a.Name)
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if !strings.EqualFold(got, expectedHash) {
			return fmt.Errorf("checksum mismatch for %s: expected %s, got %s", assetName, expectedHash, got)
		}
		fmt.Fprintln(os.Stderr, "Checksum verified.")
		return nil
	}
	return fmt.Errorf("no checksum asset found for release %s; refusing to install %s without authentication", rel.TagName, assetName)
}

// installStackComponents copies bin/*, frontend-dist/, and example_config.yaml
// from the extracted stack root (`src`) to the target install directory
// (`dst`) as one transaction: every file is staged next to its destination,
// existing files (including running executables on Windows) are moved aside
// rather than overwritten, and any failure restores the previous files so the
// install is never left half-updated. See update_install.go.
func installStackComponents(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dst, err)
	}
	cleanupStaleUpdateArtifacts(dst)
	txn := newInstallTxn()
	var staged []string
	fail := func(err error) error {
		for _, p := range staged {
			_ = os.RemoveAll(p)
		}
		if notRestored := txn.rollback(); len(notRestored) > 0 {
			return fmt.Errorf("%w; rollback could not restore: %s", err, strings.Join(notRestored, ", "))
		}
		return fmt.Errorf("%w (previous files restored)", err)
	}
	srcBin := filepath.Join(src, "bin")
	dstBin := filepath.Join(dst, "bin")
	if existingDir(srcBin) {
		if err := os.MkdirAll(dstBin, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dstBin, err)
		}
		entries, err := os.ReadDir(srcBin)
		if err != nil {
			return fmt.Errorf("read %s: %w", srcBin, err)
		}
		// Stage everything first so a disk-full or permission problem is
		// detected before anything in the live install changes.
		type pending struct{ staged, to string }
		var plan []pending
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			to := filepath.Join(dstBin, ent.Name())
			sp, err := stageFileCopy(filepath.Join(srcBin, ent.Name()), to, 0o755)
			if err != nil {
				return fail(fmt.Errorf("stage binary %s: %w", ent.Name(), err))
			}
			staged = append(staged, sp)
			plan = append(plan, pending{sp, to})
		}
		for _, p := range plan {
			if err := txn.replace(p.staged, p.to); err != nil {
				return fail(fmt.Errorf("install binary %s: %w", filepath.Base(p.to), err))
			}
		}
	}
	srcFront := filepath.Join(src, "frontend-dist")
	dstFront := filepath.Join(dst, "frontend-dist")
	if existingDir(srcFront) {
		stagedFront := filepath.Join(dst, ".ncc-stage-frontend-"+txn.stamp)
		staged = append(staged, stagedFront)
		if err := copyDir(srcFront, stagedFront); err != nil {
			return fail(fmt.Errorf("stage frontend-dist: %w", err))
		}
		if err := txn.replace(stagedFront, dstFront); err != nil {
			return fail(fmt.Errorf("install frontend-dist: %w", err))
		}
	}
	srcCfg := filepath.Join(src, "example_config.yaml")
	dstCfg := filepath.Join(dst, "example_config.yaml")
	if existingFile(srcCfg) {
		sp, err := stageFileCopy(srcCfg, dstCfg, 0o644)
		if err != nil {
			return fail(fmt.Errorf("stage example_config.yaml: %w", err))
		}
		staged = append(staged, sp)
		if err := txn.replace(sp, dstCfg); err != nil {
			return fail(fmt.Errorf("install example_config.yaml: %w", err))
		}
	}
	txn.commit()
	refreshInstallACLs(dst)
	return nil
}

// copyFileAtomic writes the source file to dst via a sibling temp file and
// rename, so that the destination is never observed half-written. Existing
// destination files are replaced.
func copyFileAtomic(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".ncc-update-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		_ = os.Remove(tmpName)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return closeErr
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// copyDir recursively copies the contents of src into dst. Destination must
// not exist (caller is responsible for cleaning up beforehand if needed).
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		return copyFileAtomic(path, target, info.Mode().Perm())
	})
}

// runUpdate fetches release metadata and updates or checks binary availability.
func runUpdate(opts updateOptions) error {
	if err := runtimecaps.Detect().RejectHostOperation("in-place software updates"); err != nil {
		return fmt.Errorf("%w; update the container image and roll out the Deployment", err)
	}
	currentVer := stripGoBuildGitSuffix(Version)
	client := &http.Client{Timeout: 20 * time.Second}
	providedSHA256 := strings.TrimSpace(opts.BinarySHA256)
	if providedSHA256 != "" {
		normalized, err := normalizeSHA256Hex(providedSHA256)
		if err != nil {
			return fmt.Errorf("invalid --binary-sha256: %w", err)
		}
		providedSHA256 = normalized
	}

	if strings.TrimSpace(opts.BinaryURL) != "" {
		if isArchiveAssetURL(opts.BinaryURL) {
			return fmt.Errorf("binary URL points to archive; provide direct binary URL: %s", opts.BinaryURL)
		}
		if strings.TrimSpace(opts.TargetVersion) != "" {
			if err := enforceMajorUpgradePolicy(currentVer, opts.TargetVersion, opts.AllowMajorUpgrade); err != nil {
				return err
			}
		}
		if opts.CheckOnly {
			req, err := http.NewRequest(http.MethodHead, opts.BinaryURL, nil)
			if err != nil {
				return fmt.Errorf("create HEAD request: %w", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("check binary URL: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return fmt.Errorf("binary URL check failed: HTTP %d", resp.StatusCode)
			}
			fmt.Fprintf(os.Stderr, "Binary URL is reachable: %s (status %d)\n", opts.BinaryURL, resp.StatusCode)
			if strings.TrimSpace(opts.TargetVersion) != "" {
				if versionLess(currentVer, opts.TargetVersion) {
					fmt.Fprintf(os.Stderr, "Update available: current=%s target=%s\n", currentVer, opts.TargetVersion)
				} else {
					fmt.Fprintf(os.Stderr, "No upgrade needed: current=%s target=%s\n", currentVer, opts.TargetVersion)
				}
			}
			return nil
		}
		if providedSHA256 == "" {
			return errors.New("--binary-sha256 is required when using --binary-url")
		}
		fmt.Fprintf(os.Stderr, "Downloading %s ...\n", opts.BinaryURL)
		body, err := downloadBinaryURL(client, opts.BinaryURL)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if !strings.EqualFold(got, providedSHA256) {
			return fmt.Errorf("checksum mismatch: expected %s, got %s", providedSHA256, got)
		}
		fmt.Fprintln(os.Stderr, "Checksum verified.")
		targetVer := strings.TrimSpace(opts.TargetVersion)
		if targetVer == "" {
			targetVer = "custom"
		}
		return installDownloadedBinary(body, targetVer)
	}

	repo := strings.TrimSpace(opts.Repo)
	if repo == "" {
		repo = defaultGitHubRepo
	}
	repo, err := normalizeGitHubRepo(repo)
	if err != nil {
		return err
	}
	releases, err := fetchGitHubReleases(repo, client)
	if err != nil {
		// GitHub's release-page redirect is not subject to the REST API
		// rate-limit bucket. Preserve an accurate latest version for the UI,
		// but mark the result degraded because assets cannot be selected safely
		// without the API response. Never reuse an older/stale release value.
		if opts.CheckOnly && strings.Contains(err.Error(), "GitHub API rate limited") {
			if latestTag, pageErr := fetchLatestGitHubTag(repo, client); pageErr == nil {
				latestOverall := strings.TrimPrefix(strings.TrimSpace(latestTag), "v")
				targetVer := latestOverall
				currentMajor, _ := parseVersionMajor(currentVer)
				targetMajor, _ := parseVersionMajor(targetVer)
				if !opts.AllowMajorUpgrade && currentMajor > 0 && currentMajor != targetMajor {
					targetVer = currentVer
				}
				checkError := "GitHub API rate limited; latest release tag is " + latestTag + ", but release assets are unavailable. Set GITHUB_TOKEN for a complete update check."
				if opts.JSONOut {
					result := map[string]interface{}{
						"current_version":  currentVer,
						"latest_version":   targetVer,
						"latest_overall":   latestOverall,
						"update_available": versionLess(currentVer, targetVer),
						"has_package":      false,
						"check_degraded":   true,
						"check_error":      checkError,
					}
					if b, marshalErr := json.Marshal(result); marshalErr == nil {
						fmt.Fprintf(os.Stdout, "%s%s\n", updateCheckJSONPrefix, string(b))
					}
				}
				fmt.Fprintln(os.Stderr, checkError)
				return nil
			}
		}
		return err
	}
	if len(releases) == 0 {
		return fmt.Errorf("no releases found for %s", repo)
	}

	currentMajor, _ := parseVersionMajor(currentVer)
	// An install that runs a release candidate follows candidates until the
	// release itself, which is newer than any of them, is out.
	includePre := opts.IncludePrerelease || isPrereleaseVersion(currentVer)
	latestOverall := pickLatestSemverRelease(releases, 0, includePre)
	if latestOverall == nil {
		return fmt.Errorf("no stable releases found for %s", repo)
	}
	targetRelease := latestOverall
	if !opts.AllowMajorUpgrade && currentMajor > 0 {
		if sameMajor := pickLatestSemverRelease(releases, currentMajor, includePre); sameMajor != nil {
			targetRelease = sameMajor
		}
	}
	if strings.TrimSpace(opts.TargetVersion) != "" {
		want := strings.TrimPrefix(strings.TrimSpace(opts.TargetVersion), "v")
		var match *githubRelease
		for i := range releases {
			tag := strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v")
			if strings.EqualFold(tag, want) {
				match = &releases[i]
				break
			}
		}
		if match == nil {
			return fmt.Errorf("target version %q not found in repo %s", opts.TargetVersion, repo)
		}
		targetRelease = match
	}
	targetVer := strings.TrimPrefix(strings.TrimSpace(targetRelease.TagName), "v")
	if err := enforceMajorUpgradePolicy(currentVer, targetVer, opts.AllowMajorUpgrade); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Current version: %s\n", currentVer)
	fmt.Fprintf(os.Stderr, "Selected release: %s\n", targetRelease.TagName)
	if targetRelease.TagName != latestOverall.TagName {
		fmt.Fprintf(os.Stderr, "Latest overall release: %s (major upgrade not auto-applied)\n", latestOverall.TagName)
		if currentMajor == 1 {
			fmt.Fprintln(os.Stderr, "Use --allow-major-upgrade to move from v1.x to v2.x after migration review.")
		}
	}
	// Prefer the stack archive when available: this is the canonical
	// "update the whole package" path. Falls back to single-binary update
	// for legacy releases (v1.x) that don't ship a stack archive.
	stackURL, stackName := pickStackAssetForCurrentPlatform(*targetRelease)

	if opts.CheckOnly {
		if opts.JSONOut {
			result := map[string]interface{}{
				"current_version":  currentVer,
				"latest_version":   targetVer,
				"latest_overall":   strings.TrimPrefix(strings.TrimSpace(latestOverall.TagName), "v"),
				"update_available": versionLess(currentVer, targetVer),
				"has_package":      stackURL != "",
			}
			if b, err := json.Marshal(result); err == nil {
				fmt.Fprintf(os.Stdout, "%s%s\n", updateCheckJSONPrefix, string(b))
			}
		}
		if versionLess(currentVer, targetVer) {
			fmt.Fprintf(os.Stderr, "Update available in track: %s -> %s\n", currentVer, targetVer)
		} else if versionLess(targetVer, currentVer) {
			fmt.Fprintln(os.Stderr, "You have a newer version than the selected release track (dev build).")
		} else {
			fmt.Fprintln(os.Stderr, "You are already on the latest version for the selected track.")
		}
		if stackURL != "" {
			fmt.Fprintf(os.Stderr, "Package candidate for %s/%s: %s (%s)\n", runtime.GOOS, runtime.GOARCH, stackName, stackURL)
		} else {
			downloadURL, assetName := pickAssetForCurrentPlatform(*targetRelease)
			if downloadURL != "" {
				fmt.Fprintf(os.Stderr, "Binary candidate for %s/%s: %s (%s)\n", runtime.GOOS, runtime.GOARCH, assetName, downloadURL)
			}
		}
		return nil
	}
	if !versionLess(currentVer, targetVer) {
		if versionLess(targetVer, currentVer) {
			fmt.Fprintln(os.Stderr, "You have a newer version than the selected release (dev build).")
		} else {
			fmt.Fprintln(os.Stderr, "You are already on the latest version for the selected track.")
		}
		return nil
	}

	if stackURL != "" {
		return installPackageUpdate(stackURL, stackName, targetRelease, targetVer, client, opts.SkipChecksumVerify)
	}

	// Legacy single-binary update path (v1.x releases without a stack archive).
	downloadURL, chosenAssetName := pickAssetForCurrentPlatform(*targetRelease)
	if downloadURL == "" {
		// Returning nil here used to make the in-app update report success
		// (and restart the stack) although nothing was installed.
		return fmt.Errorf("no update package or binary for %s/%s in release %s; download it manually from %s", runtime.GOOS, runtime.GOARCH, targetRelease.TagName, targetRelease.HTMLURL)
	}
	if isArchiveAssetURL(downloadURL) {
		return fmt.Errorf("release %s only ships an archive for %s/%s; download and extract it manually: %s", targetRelease.TagName, runtime.GOOS, runtime.GOARCH, downloadURL)
	}

	fmt.Fprintf(os.Stderr, "Downloading %s ...\n", downloadURL)
	body, err := downloadBinaryURL(client, downloadURL)
	if err != nil {
		return err
	}
	if err := verifyDownloadedAsset(targetRelease, chosenAssetName, body, client, opts.SkipChecksumVerify); err != nil {
		return err
	}
	return installDownloadedBinary(body, targetVer)
}

// maxDiscoveryResponseBytes caps one Prism Central discovery page. A page of
// a few thousand clusters is a few MiB; a broken or hostile endpoint must not
// be able to exhaust memory.
const maxDiscoveryResponseBytes = 64 << 20

// readBounded reads r up to limit bytes and fails, rather than truncating,
// when there is more.
func readBounded(r io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("response is larger than %d bytes", limit)
	}
	return data, nil
}

// githubTokenAllowed reports whether GITHUB_TOKEN may be sent to rawURL: only
// over HTTPS to github.com, githubusercontent.com or their subdomains. A
// substring test would also match hosts such as github.com.example.net.
func githubTokenAllowed(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || !strings.EqualFold(u.Scheme, "https") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, d := range []string{"github.com", "githubusercontent.com"} {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

func fetchURL(url string, client *http.Client) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && githubTokenAllowed(url) {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
}

// parseChecksumFile finds a line in checksum body that matches filename (or *filename) and returns the hex hash.
func parseChecksumFile(body []byte, filename string) string {
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Format: "hash  filename" or "hash *filename"
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		hashStr := fields[0]
		name := fields[1]
		if strings.TrimPrefix(name, "*") == filename || name == filename {
			hashStr = strings.TrimPrefix(hashStr, "0x")
			if len(hashStr) == 64 {
				return hashStr
			}
			return ""
		}
	}
	return ""
}
