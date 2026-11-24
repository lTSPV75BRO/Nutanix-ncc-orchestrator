package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func extractZipArchive(archive []byte, destDir string) error {
	r, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	destClean := filepath.Clean(destDir) + string(os.PathSeparator)
	for _, f := range r.File {
		name := filepath.Clean(f.Name)
		target := filepath.Join(destDir, name)
		if !strings.HasPrefix(target, destClean) {
			return fmt.Errorf("zip contains unsafe path %q", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(rc, 200*1024*1024))
		_ = rc.Close()
		if err != nil {
			return err
		}
		// Preserve archive entry mode bits (esp. the executable bit). The
		// stack archives we ship contain executable binaries under bin/; if
		// we wrote them with a hardcoded 0644 mode the post-extract
		// isExecutableFile checks (and any attempt to exec the binary) would
		// fail. Defaults to 0644 only if the archive entry has no mode set.
		mode := f.FileInfo().Mode().Perm()
		if mode == 0 {
			mode = 0o644
		}
		if err := os.WriteFile(target, data, mode); err != nil {
			return err
		}
	}
	return nil
}

func extractTarGzArchive(archive []byte, destDir string) error {
	gzr, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return fmt.Errorf("open gzip: %w", err)
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	destClean := filepath.Clean(destDir) + string(os.PathSeparator)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(hdr.Name)
		target := filepath.Join(destDir, name)
		if !strings.HasPrefix(target, destClean) {
			return fmt.Errorf("tar contains unsafe path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg, '\x00': // '\x00' = legacy regular file (formerly tar.TypeRegA)
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			data, err := io.ReadAll(io.LimitReader(tr, 200*1024*1024))
			if err != nil {
				return err
			}
			// Preserve the tar entry mode (esp. the +x bit for binaries
			// under bin/). Without this, v2-bootstrap drops the executable
			// bit during extraction, causing v2-check / v2-start failures
			// with "binary not executable under install dir" errors.
			mode := os.FileMode(hdr.Mode).Perm()
			if mode == 0 {
				mode = 0o644
			}
			if err := os.WriteFile(target, data, mode); err != nil {
				return err
			}
		}
	}
	return nil
}

func listAssetNames(rel githubRelease) []string {
	out := make([]string, 0, len(rel.Assets))
	for _, a := range rel.Assets {
		out = append(out, a.Name)
	}
	sort.Strings(out)
	return out
}

func extractArchiveByAssetName(archive []byte, assetName string, destDir string) error {
	lower := strings.ToLower(strings.TrimSpace(assetName))
	switch {
	case strings.HasSuffix(lower, ".zip"):
		return extractZipArchive(archive, destDir)
	case strings.HasSuffix(lower, ".tar.gz"):
		return extractTarGzArchive(archive, destDir)
	default:
		return fmt.Errorf("unsupported archive format for %s", assetName)
	}
}

func hasBootstrappedV2Layout(installDir string) bool {
	if existingBinaryInInstallDir(installDir, "ncc-api-server") == "" {
		return false
	}
	if existingBinaryInInstallDir(installDir, "ncc-ui-server") == "" {
		return false
	}
	return existingDir(filepath.Join(installDir, "frontend-dist"))
}

func releaseHasRequiredV2Assets(rel githubRelease, goos, goarch string) bool {
	_, okStack := findAsset(rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "ncc-v2-stack") && strings.Contains(n, goos) && strings.Contains(n, goarch)
	})
	if okStack {
		return true
	}
	_, okAPI := findAsset(rel, func(n string) bool {
		return strings.Contains(n, "ncc-api-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	_, okUI := findAsset(rel, func(n string) bool {
		return strings.Contains(n, "ncc-ui-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	_, okFrontend := findAsset(rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "frontend") || strings.Contains(n, "ui-dist")
	})
	return okAPI && okUI && okFrontend
}

func pickBestReleaseWithV2Assets(releases []githubRelease, goos, goarch string, majorFilter int64) *githubRelease {
	var best *githubRelease
	for i := range releases {
		rel := &releases[i]
		if rel.Draft || rel.Prerelease || isPrereleaseVersion(rel.TagName) {
			continue
		}
		if !releaseHasRequiredV2Assets(*rel, goos, goarch) {
			continue
		}
		if majorFilter > 0 {
			maj, err := parseVersionMajor(rel.TagName)
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

func runV2Bootstrap(opts v2BootstrapOptions) error {
	repo := strings.TrimSpace(opts.Repo)
	if repo == "" {
		repo = defaultGitHubRepo
	}
	repo, err := normalizeGitHubRepo(repo)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	releases, err := fetchGitHubReleases(repo, client)
	if err != nil {
		return err
	}
	if len(releases) == 0 {
		return fmt.Errorf("no releases found for %s", repo)
	}

	goos := runtime.GOOS
	goarch := runtime.GOARCH

	var rel *githubRelease
	if strings.TrimSpace(opts.Version) != "" {
		want := strings.TrimPrefix(strings.TrimSpace(opts.Version), "v")
		for i := range releases {
			tag := strings.TrimPrefix(strings.TrimSpace(releases[i].TagName), "v")
			if strings.EqualFold(tag, want) {
				rel = &releases[i]
				break
			}
		}
		if rel == nil {
			return fmt.Errorf("release version %q not found", opts.Version)
		}
		if !releaseHasRequiredV2Assets(*rel, goos, goarch) {
			return fmt.Errorf("release %s does not contain required v2 assets for %s/%s. available assets: %s", rel.TagName, goos, goarch, strings.Join(listAssetNames(*rel), ", "))
		}
	} else {
		rel = pickBestReleaseWithV2Assets(releases, goos, goarch, 2)
		if rel == nil {
			rel = pickBestReleaseWithV2Assets(releases, goos, goarch, 0)
		}
		if rel == nil {
			latestOverall := pickLatestSemverRelease(releases, 0, false)
			if latestOverall != nil {
				return fmt.Errorf("no stable release in %s contains required v2 assets for %s/%s. latest release is %s with assets: %s", repo, goos, goarch, latestOverall.TagName, strings.Join(listAssetNames(*latestOverall), ", "))
			}
			return errors.New("no stable releases available")
		}
	}
	stackAsset, okStack := findAsset(*rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "ncc-v2-stack") && strings.Contains(n, goos) && strings.Contains(n, goarch)
	})
	apiAsset, okAPI := findAsset(*rel, func(n string) bool {
		return strings.Contains(n, "ncc-api-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	uiAsset, okUI := findAsset(*rel, func(n string) bool {
		return strings.Contains(n, "ncc-ui-server") && strings.Contains(n, goos) && strings.Contains(n, goarch) && !strings.HasSuffix(n, ".zip") && !strings.HasSuffix(n, ".tar.gz")
	})
	frontendAsset, okFrontend := findAsset(*rel, func(n string) bool {
		if !(strings.HasSuffix(n, ".zip") || strings.HasSuffix(n, ".tar.gz")) {
			return false
		}
		return strings.Contains(n, "frontend") || strings.Contains(n, "ui-dist")
	})
	if !okStack && (!okAPI || !okUI || !okFrontend) {
		return fmt.Errorf("release %s does not contain all required v2 assets for %s/%s (need api binary, ui binary, frontend archive). available assets: %s",
			rel.TagName, goos, goarch, strings.Join(listAssetNames(*rel), ", "))
	}

	fmt.Fprintf(os.Stderr, "Selected release: %s\n", rel.TagName)
	if okStack {
		fmt.Fprintf(os.Stderr, "v2 stack bundle: %s\n", stackAsset.Name)
	} else {
		fmt.Fprintf(os.Stderr, "API binary: %s\n", apiAsset.Name)
		fmt.Fprintf(os.Stderr, "UI binary: %s\n", uiAsset.Name)
		fmt.Fprintf(os.Stderr, "Frontend bundle: %s\n", frontendAsset.Name)
	}
	if opts.CheckOnly {
		fmt.Fprintln(os.Stderr, "Check-only mode: no files downloaded.")
		return nil
	}

	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = ".ncc-v2"
	}
	binDir := filepath.Join(installDir, "bin")
	frontendDir := filepath.Join(installDir, "frontend-dist")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(frontendDir, 0755); err != nil {
		return err
	}

	apiBin := filepath.Join(binDir, "ncc-api-server")
	uiBin := filepath.Join(binDir, "ncc-ui-server")
	if runtime.GOOS == "windows" {
		apiBin += ".exe"
		uiBin += ".exe"
	}

	if okStack {
		fmt.Fprintf(os.Stderr, "Downloading %s\n", stackAsset.BrowserDownloadURL)
		stackArchive, err := downloadBinaryURL(client, stackAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download v2 stack bundle: %w", err)
		}
		if err := verifyDownloadedAsset(rel, stackAsset.Name, stackArchive, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify v2 stack bundle: %w", err)
		}
		if err := extractArchiveByAssetName(stackArchive, stackAsset.Name, installDir); err != nil {
			return fmt.Errorf("extract v2 stack bundle: %w", err)
		}
		if !hasBootstrappedV2Layout(installDir) {
			return fmt.Errorf("v2 stack bundle %s extracted but required layout was not found in %s (expected bin/ncc-api-server, bin/ncc-ui-server, frontend-dist/)", stackAsset.Name, installDir)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Downloading %s\n", apiAsset.BrowserDownloadURL)
		apiBody, err := downloadBinaryURL(client, apiAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download api binary: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Downloading %s\n", uiAsset.BrowserDownloadURL)
		uiBody, err := downloadBinaryURL(client, uiAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download ui binary: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Downloading %s\n", frontendAsset.BrowserDownloadURL)
		frontendArchive, err := downloadBinaryURL(client, frontendAsset.BrowserDownloadURL)
		if err != nil {
			return fmt.Errorf("download frontend archive: %w", err)
		}
		if err := verifyDownloadedAsset(rel, apiAsset.Name, apiBody, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify api binary: %w", err)
		}
		if err := verifyDownloadedAsset(rel, uiAsset.Name, uiBody, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify ui binary: %w", err)
		}
		if err := verifyDownloadedAsset(rel, frontendAsset.Name, frontendArchive, client, opts.SkipChecksumVerify); err != nil {
			return fmt.Errorf("verify frontend archive: %w", err)
		}

		if err := writeExecutable(apiBin, apiBody); err != nil {
			return fmt.Errorf("write api binary: %w", err)
		}
		if err := writeExecutable(uiBin, uiBody); err != nil {
			return fmt.Errorf("write ui binary: %w", err)
		}
		if err := extractArchiveByAssetName(frontendArchive, frontendAsset.Name, frontendDir); err != nil {
			return fmt.Errorf("extract frontend bundle: %w", err)
		}
	}

	cwd, _ := os.Getwd()
	// Same repo-root resolution as runV2Start: must contain all the paths
	// the api-server might touch, and must be EvalSymlinks-resolved so the
	// api-server's normalize check is consistent on /tmp-vs-/private/tmp.
	repoRoot := resolveV2RepoRoot(installDir, cwd)
	if strings.TrimSpace(opts.OrchestratorBin) == "" {
		opts.OrchestratorBin = "./ncc-orchestrator"
	}
	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = "config.yaml"
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		opts.OutputDir = "outputfiles"
	}
	if strings.TrimSpace(opts.LogDir) == "" {
		opts.LogDir = "nccfiles"
	}
	if strings.TrimSpace(opts.APIListen) == "" {
		opts.APIListen = ":8081"
	}
	if strings.TrimSpace(opts.UIListen) == "" {
		opts.UIListen = ":8080"
	}
	if strings.TrimSpace(opts.TokenFile) == "" {
		opts.TokenFile = ".ncc-api-token"
	}
	backendURL := "http://localhost:8081"
	if strings.HasPrefix(opts.APIListen, ":") {
		backendURL = "http://localhost" + opts.APIListen
	}
	uiOrigin := "http://localhost:8080"
	if strings.HasPrefix(opts.UIListen, ":") {
		uiOrigin = "http://localhost" + opts.UIListen
	}

	apiScript := fmt.Sprintf(`#!/usr/bin/env sh
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
"%s" --listen %s --repo-root %s --config-path %s --output-dir %s --log-dir %s --orchestrator-bin %s
`, apiBin, shellQuote(opts.APIListen), shellQuote(repoRoot), shellQuote(opts.ConfigPath), shellQuote(opts.OutputDir), shellQuote(opts.LogDir), shellQuote(opts.OrchestratorBin))
	uiScript := fmt.Sprintf(`#!/usr/bin/env sh
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
"%s" --listen %s --dir %s --backend-url %s --api-token-file %s --api-auth-mode token --allowed-origins %s
`, uiBin, shellQuote(opts.UIListen), shellQuote(frontendDir), shellQuote(backendURL), shellQuote(opts.TokenFile), shellQuote(uiOrigin))

	apiScriptPath := filepath.Join(installDir, "start-api.sh")
	uiScriptPath := filepath.Join(installDir, "start-ui.sh")
	if runtime.GOOS == "windows" {
		apiScriptPath = filepath.Join(installDir, "start-api.cmd")
		uiScriptPath = filepath.Join(installDir, "start-ui.cmd")
		apiScript = fmt.Sprintf("@echo off\r\n\"%s\" --listen %s --repo-root %s --config-path %s --output-dir %s --log-dir %s --orchestrator-bin %s\r\n",
			apiBin, opts.APIListen, repoRoot, opts.ConfigPath, opts.OutputDir, opts.LogDir, opts.OrchestratorBin)
		uiScript = fmt.Sprintf("@echo off\r\n\"%s\" --listen %s --dir %s --backend-url %s --api-token-file %s --api-auth-mode token --allowed-origins %s\r\n",
			uiBin, opts.UIListen, frontendDir, backendURL, opts.TokenFile, uiOrigin)
	}
	if err := os.WriteFile(apiScriptPath, []byte(apiScript), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(uiScriptPath, []byte(uiScript), 0755); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "v2 bootstrap completed in %s\n", installDir)
	fmt.Fprintf(os.Stderr, "Start API: %s\n", apiScriptPath)
	fmt.Fprintf(os.Stderr, "Start UI : %s\n", uiScriptPath)
	fmt.Fprintf(os.Stderr, "Open UI  : %s\n", uiOrigin)
	return nil
}
