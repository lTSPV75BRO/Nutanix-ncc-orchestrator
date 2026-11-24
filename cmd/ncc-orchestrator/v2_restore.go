package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"goncc/internal/plural"
)

func runV2Restore(opts v2RestoreOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	in := strings.TrimSpace(opts.InputFile)
	if in == "" {
		return fmt.Errorf("--input-file is required (path to a backup archive)")
	}
	if abs, err := filepath.Abs(in); err == nil {
		in = abs
	}
	// --verify-only: isolated check of the archive (decrypt, entries, paths,
	// manifest, config, secrets, report paths, version). It reads the archive
	// in memory and never writes into the install dir.
	if opts.VerifyOnly {
		out := opts.Out
		if out == nil {
			out = os.Stdout
		}
		return runBackupVerify(out, in, opts.KeyFile, opts.Passphrase, opts.JSON, stackTarget{installDir: installDir, trustStack: opts.TrustStack})
	}
	// Transparently decrypt an encrypted backup (v2-backup --encrypt) into a
	// temp .tar.gz before the rest of the flow runs. An unencrypted archive is
	// untouched. The decrypted temp file is removed when we return.
	if enc, derr := backupArchiveIsEncrypted(in); derr == nil && enc {
		dec, derr := decryptBackupArchive(in, opts.KeyFile, opts.Passphrase)
		if derr != nil {
			return fmt.Errorf("decrypt backup archive: %w", derr)
		}
		defer os.Remove(dec)
		in = dec
	}
	// Restore preflight: validate the archive before touching the install dir.
	if _, verr := verifyBackupArchive(in); verr != nil {
		return fmt.Errorf("backup archive failed verification: %w", verr)
	}
	// Capture this host's environment-specific start settings BEFORE the archive
	// overwrites the start-state file. A backup is portable across hosts, so its
	// origins/advertise URLs/listen addresses belong to the source host;
	// adopting them here would make the UI reject this host's browser origin
	// ("origin not allowed"). We re-apply the captured values after extraction.
	preStartState, hadPreStartState := loadV2StartState(installDir)
	// Refuse to clobber a running stack unless forced.
	if !opts.Force {
		if running, which := v2StackRunning(installDir); running {
			return fmt.Errorf("stack appears to be running (%s alive); stop it with 'v2-stop' first, or pass --force", which)
		}
	}

	// Restore in two passes over the on-disk archive so memory stays bounded no
	// matter how large it decompresses to: pass 1 reads only headers (and the
	// small manifest) to validate confinement, enforce caps, and detect
	// overwrites; pass 2 streams each file body straight to its target on disk.
	// Neither pass buffers a whole file in RAM, so a decompression bomb cannot
	// exhaust memory, and the per-file/total caps bound disk use as well.
	type pendingFile struct {
		rel  string
		mode int64
	}
	var manifest *backupManifest
	var files []pendingFile

	// Pass 1: headers + manifest only (no file bodies read into memory).
	if err := func() error {
		f, err := os.Open(in)
		if err != nil {
			return err
		}
		defer f.Close()
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("open archive %s: %w", in, err)
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read archive: %w", err)
			}
			if hdr.FileInfo().IsDir() {
				continue
			}
			switch {
			case hdr.Name == "manifest.json":
				data, err := io.ReadAll(io.LimitReader(tr, 1<<20))
				if err != nil {
					return err
				}
				var m backupManifest
				if err := json.Unmarshal(data, &m); err != nil {
					return fmt.Errorf("invalid manifest.json in archive: %w", err)
				}
				manifest = &m
			case strings.HasPrefix(hdr.Name, "data/"):
				rel := strings.TrimPrefix(hdr.Name, "data/")
				clean := filepath.Clean(filepath.FromSlash(rel))
				if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.IsAbs(clean) {
					return fmt.Errorf("archive contains an unsafe path: %q", hdr.Name)
				}
				dst := filepath.Join(installDir, clean)
				if rel, err := filepath.Rel(installDir, dst); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					return fmt.Errorf("refusing to write outside install dir: %s", dst)
				}
				if len(files) >= maxBackupFileCount {
					return fmt.Errorf("archive contains too many files (>%d)", maxBackupFileCount)
				}
				files = append(files, pendingFile{rel: clean, mode: hdr.Mode})
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	if manifest == nil {
		return fmt.Errorf("archive is missing manifest.json (not an ncc backup?)")
	}
	if len(files) == 0 {
		return fmt.Errorf("archive contains no files to restore")
	}

	// Files that are never taken from an archive (SEC-19, restorepolicy.go).
	{
		kept := files[:0]
		for _, p := range files {
			if why := restoreSkipReasonAt(installDir, p.rel); why != "" {
				fmt.Printf("Skipped %s from the archive (%s).\n", p.rel, why)
				continue
			}
			kept = append(kept, p)
		}
		files = kept
	}
	// Program files are restored only as a whole set this host has run
	// (SEC-07, trustedstack.go): otherwise a crafted backup could run its own
	// code, or mix files of different versions.
	var stack stackVerdict
	if !opts.TrustStack {
		var stackRels []string
		for _, p := range files {
			if backupRelIsStack(p.rel) {
				stackRels = append(stackRels, p.rel)
			}
		}
		if len(stackRels) > 0 {
			var err error
			if stack, err = judgeArchiveStack(installDir, in, stackRels); err != nil {
				return fmt.Errorf("check the program files in the backup: %w", err)
			}
			if !stack.trusted() {
				kept := files[:0]
				for _, p := range files {
					if !backupRelIsStack(p.rel) {
						kept = append(kept, p)
					}
				}
				files = kept
			}
		}
	}
	if len(files) == 0 {
		return onlySkippedStackError(stack, manifest.Version)
	}

	// Detect overwrites up front so a non-forced restore fails before writing
	// anything.
	var existing []string
	for _, p := range files {
		if isRegularFile(filepath.Join(installDir, p.rel)) {
			existing = append(existing, p.rel)
		}
	}
	if len(existing) > 0 && !opts.Force {
		fmt.Println("The following files already exist in the install dir and would be overwritten:")
		for _, e := range existing {
			fmt.Printf("  - %s\n", e)
		}
		return fmt.Errorf("refusing to overwrite %s; re-run with --force", plural.Count(len(existing), "existing file"))
	}

	if err := os.MkdirAll(installDir, 0o755); err != nil {
		return err
	}

	// Decided by the files, not by the manifest: one that claims a stack the
	// archive does not hold must not clear the live frontend.
	includesStack := false
	for _, p := range files {
		if backupRelIsStack(p.rel) {
			includesStack = true
			break
		}
	}
	// Pass 2 is transactional: each file is written to a staged sibling and
	// swapped in through an installTxn, which moves the previous file aside.
	// Any failure (size cap on a later file, disk full, a locked file) rolls
	// every replaced file back, so a failed restore leaves the install exactly
	// as it was instead of a mix of old and restored files. Before this, files
	// were overwritten in place one by one and frontend-dist was deleted up
	// front, with no way back.
	txn := newInstallTxn()
	committed := false
	defer func() {
		if !committed {
			if notRestored := txn.rollback(); len(notRestored) > 0 {
				fmt.Fprintf(os.Stderr, "restore rollback could not restore: %s\n", strings.Join(notRestored, ", "))
			}
		}
	}()

	// A versioned snapshot must replace the live frontend, not merge with it:
	// leftover chunks from a newer UI would mix with the restored index. The
	// old directory is moved aside (not deleted) so a rollback can bring it back.
	if includesStack {
		frontDir := filepath.Join(installDir, "frontend-dist")
		if _, err := os.Lstat(frontDir); err == nil {
			empty := frontDir + ".ncc-restore-stage-" + txn.stamp
			if err := os.MkdirAll(empty, 0o755); err != nil {
				return fmt.Errorf("replace frontend-dist: %w", err)
			}
			if err := txn.replace(empty, frontDir); err != nil {
				_ = os.RemoveAll(empty)
				return fmt.Errorf("replace frontend-dist: %w", err)
			}
		}
	}

	// Stream each data/ file body to its staged path, enforcing a per-file
	// and a cumulative size cap.
	modeByRel := make(map[string]int64, len(files))
	for _, p := range files {
		modeByRel[p.rel] = p.mode
	}
	if err := func() error {
		f, err := os.Open(in)
		if err != nil {
			return err
		}
		defer f.Close()
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("open archive %s: %w", in, err)
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		var total int64
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read archive: %w", err)
			}
			if hdr.FileInfo().IsDir() || !strings.HasPrefix(hdr.Name, "data/") {
				continue
			}
			clean := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(hdr.Name, "data/")))
			storedMode, ok := modeByRel[clean]
			if !ok {
				continue // not validated in pass 1 (only happens if the archive mutated underneath us)
			}
			dst := filepath.Join(installDir, clean)
			if rel, err := filepath.Rel(installDir, dst); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("refusing to write outside install dir: %s", dst)
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			mode := restoreFileMode(clean, storedMode)
			staged := dst + ".ncc-restore-stage-" + txn.stamp
			out, err := os.OpenFile(staged, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return fmt.Errorf("write %s: %w", dst, err)
			}
			if !backupRelIsStack(clean) {
				restrictNewSecret(staged)
			}
			// +1 so an oversize file is detected (n > cap) rather than silently
			// truncated to the cap.
			n, copyErr := io.Copy(out, io.LimitReader(tr, maxBackupFileBytes+1))
			closeErr := out.Close()
			var fail error
			switch {
			case copyErr != nil:
				fail = fmt.Errorf("write %s: %w", dst, copyErr)
			case closeErr != nil:
				fail = fmt.Errorf("write %s: %w", dst, closeErr)
			case n > maxBackupFileBytes:
				fail = fmt.Errorf("archive file %q exceeds the per-file limit (%d bytes)", clean, maxBackupFileBytes)
			case total+n > maxBackupTotalBytes:
				fail = fmt.Errorf("archive exceeds the total restore size limit (%d bytes)", maxBackupTotalBytes)
			}
			if fail == nil {
				if err := os.Chmod(staged, mode); err != nil {
					fail = fmt.Errorf("chmod %s: %w", dst, err)
				}
			}
			if fail != nil {
				_ = os.Remove(staged)
				return fail
			}
			total += n
			if err := txn.replace(staged, dst); err != nil {
				_ = os.Remove(staged)
				return err
			}
		}
		return nil
	}(); err != nil {
		return fmt.Errorf("%w (restore rolled back; the install is unchanged)", err)
	}
	txn.commit()
	committed = true
	refreshInstallACLs(installDir)

	fmt.Printf("Restored %s into %s\n", plural.Count(len(files), "file"), installDir)
	for _, p := range files {
		fmt.Printf("  - %s\n", p.rel)
	}
	switch {
	case includesStack:
		fmt.Printf("stack restored: yes (ncc-orchestrator %s)\n", defaultStr(manifest.Version, "?"))
	case !stack.trusted():
		printSkippedStack(os.Stdout, stack, manifest.Version)
	default:
		fmt.Printf("stack restored: no\n")
	}
	fmt.Printf("\nSource backup: tool=%s version=%s stream=%s built=%s created_at=%s install_dir=%s\n",
		manifest.Tool, manifest.Version, defaultStr(manifest.Stream, "?"), defaultStr(manifest.BuildDate, "?"),
		manifest.CreatedAt, manifest.InstallDir)
	// Warn when this orchestrator is OLDER than the one that produced the
	// backup: a newer backup may carry config/state this binary can't read.
	if v := strings.TrimSpace(manifest.Version); v != "" && versionLess(Version, v) {
		fmt.Printf("WARNING: this ncc-orchestrator is %s but the backup was created by %s. "+
			"Restoring a newer backup with an older binary may not fully load; upgrade the orchestrator if you hit issues.\n", Version, v)
	}

	// Surface auth-provider coverage from the file that actually landed on
	// disk, so the operator can confirm SAML/LDAP config and their secrets
	// were restored (they live inside .ncc-api-users.json). Fall back to the
	// manifest's recorded summary if the restored DB can't be re-read.
	restoredAuth := summarizeAuthProviders(filepath.Join(installDir, ".ncc-api-users.json"))
	if restoredAuth == nil {
		restoredAuth = manifest.Auth
	}
	if restoredAuth != nil {
		fmt.Println()
		warns := printAuthSummary(os.Stdout, restoredAuth)
		for _, wmsg := range warns {
			fmt.Printf("  ! %s\n", wmsg)
		}
	}

	// Heal cross-OS paths in the restored config so a backup taken on one OS
	// (e.g. Windows, with drive letters / backslashes) restores cleanly onto
	// another (Linux/macOS): rewrite file-reference paths that pointed inside
	// the backup's install dir to this install dir and normalize separators.
	// Best-effort — it never fails the restore.
	if healed, warns := healRestoredConfigPaths(installDir, defaultStr(manifest.InstallDir, "")); len(healed) > 0 || len(warns) > 0 {
		if len(healed) > 0 {
			fmt.Printf("\nAdjusted %s for this host: %s\n", plural.Count(len(healed), "config path"), strings.Join(healed, ", "))
		}
		for _, wmsg := range warns {
			fmt.Printf("  ! %s\n", wmsg)
		}
	}

	// Re-apply this host's networking settings on top of the start-state the
	// archive just wrote, so a backup taken on another host (or with different
	// listen/origin flags) does not lock this host's browser out with
	// "origin not allowed" after the restart replays the restored state.
	if preserved := preserveHostStartStateNetworking(installDir, preStartState, hadPreStartState); len(preserved) > 0 {
		fmt.Printf("\nKept this host's start settings (overriding the backup's): %s\n", strings.Join(preserved, ", "))
	}

	// Decide whether to restart automatically — a restart is what makes the
	// restored config/accounts/token take effect. Default: restart when the
	// stack is currently running (the common "restore into a live install"
	// case). --restart forces a restart/start even if it looks stopped;
	// --no-restart suppresses it (e.g. staging a box for later).
	stackRunning, _ := v2StackRunning(installDir)
	if !opts.NoRestart && (stackRunning || opts.Restart) {
		if err := restartV2Stack(installDir); err != nil {
			fmt.Printf("\nRestore succeeded, but the automatic restart failed: %v\n", err)
			fmt.Println("Restart the stack manually with 'v2-stop' then 'v2-start'.")
			return nil
		}
		fmt.Println("\nStack restarted; the restored config, accounts, and token are now loaded.")
		return nil
	}
	if opts.NoRestart {
		fmt.Println("Restore complete (--no-restart); start or restart the stack to load the restored config and accounts.")
	} else {
		fmt.Println("Start the stack with 'v2-start' to load the restored config and accounts.")
	}
	return nil
}

// windowsAbsPath reports whether p looks like a Windows absolute path: a
// drive-letter root (C:\ or C:/) or a UNC share (\\server\share). Used during
// restore-path healing so a backup taken on Windows can be re-pathed onto a
// Unix host.
func windowsAbsPath(p string) bool {
	p = strings.TrimSpace(p)
	if len(p) >= 2 && p[1] == ':' {
		c := p[0]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			return true
		}
	}
	return strings.HasPrefix(p, `\\`)
}

// healRestoredConfigPaths rewrites file-reference path values in the restored
// config.yaml so a backup is portable across operating systems. For each known
// path key it: (1) normalizes backslashes to forward slashes; and (2) if the
// value was an absolute path under the backup's original install dir
// (oldInstallDir), rebases it to a path relative to this install dir. Absolute
// paths outside the old install dir (e.g. C:\secrets\creds.yaml) can't be
// re-homed automatically and are reported as warnings. Returns the list of
// keys that were changed plus any warnings. Never errors — healing is
// best-effort and the file is left untouched on any read/write failure.
func healRestoredConfigPaths(installDir, oldInstallDir string) (changed []string, warnings []string) {
	cfgPath := filepath.Join(installDir, "config.yaml")
	content, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, nil
	}
	text := string(content)
	pathKeys := []string{
		"clusters-file", "exclude-alert-titles-file", "secrets-file", "pcs-file",
		"log-file", "output-dir-logs", "output-dir-filtered", "run-history-dir",
		"prom-dir", "ca-bundle", "notification-deadletter-dir",
	}
	// Normalize the old install dir for prefix comparison (slashes, no trailing).
	oldNorm := strings.TrimRight(strings.ReplaceAll(strings.TrimSpace(oldInstallDir), `\`, "/"), "/")
	for _, key := range pathKeys {
		raw := extractConfigRefPath(text, key)
		if raw == "" {
			continue
		}
		orig := raw
		slashed := strings.ReplaceAll(raw, `\`, "/")
		newVal := slashed
		if windowsAbsPath(slashed) || filepath.IsAbs(slashed) {
			matched := false
			if oldNorm != "" {
				lc := strings.ToLower(slashed)
				lo := strings.ToLower(oldNorm)
				if lc == lo {
					newVal = "."
					matched = true
				} else if strings.HasPrefix(lc, lo+"/") {
					newVal = strings.TrimPrefix(slashed[len(oldNorm):], "/")
					matched = true
				}
			}
			if !matched {
				// Can't rebase. Only warn when the path doesn't resolve on
				// this host (a same-host/same-path restore is still valid).
				probe := slashed
				if !filepath.IsAbs(probe) {
					probe = filepath.Join(installDir, probe)
				}
				if _, statErr := os.Stat(probe); statErr != nil {
					warnings = append(warnings, fmt.Sprintf("config key %q points at an absolute path from another host (%s); review it before starting the stack", key, orig))
				}
				// Still normalize separators below even if we can't rebase.
			}
		}
		if newVal != orig {
			text = upsertYAMLScalar(text, key, newVal)
			changed = append(changed, key)
		}
	}
	if len(changed) > 0 {
		if err := os.WriteFile(cfgPath, []byte(text), 0o600); err != nil {
			// Roll back our reported changes — we couldn't persist them.
			return nil, append(warnings, fmt.Sprintf("could not write healed config.yaml: %v", err))
		}
	}
	return changed, warnings
}

// defaultStr returns v when non-empty (trimmed), else def.
func defaultStr(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
