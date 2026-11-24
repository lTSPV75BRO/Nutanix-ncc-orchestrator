package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"goncc/internal/plural"
)

// ---- v2 backup / restore --------------------------------------------------
//
// backup/restore capture the *stateful* contents of a v2 install dir — the
// config and its referenced files (clusters / alert-exclusions / secrets), the
// local user database (accounts, roles, SAML config, session policy), the API
// token, the first-run admin password (if still present), and the scheduler /
// notifications state — into a single tar.gz. Regenerable artifacts (binaries,
// frontend bundle, logs, run/ pid files, output/ncc files) are intentionally
// excluded. The archive contains secrets, so it is written 0600 and restore is
// confined strictly to the install dir.

// backupManifest is the JSON index written at the root of a backup archive so
// restore can validate the archive and report its provenance. Tool/Version
// (plus Stream/BuildDate/GoVersion) record the exact ncc-orchestrator binary
// that produced the backup, so restore can report it and warn on a version
// mismatch.
type backupManifest struct {
	Tool       string       `json:"tool"`
	Version    string       `json:"version"`              // ncc-orchestrator version that created the backup
	Stream     string       `json:"stream,omitempty"`     // release stream (prod/dev/beta)
	BuildDate  string       `json:"build_date,omitempty"` // build timestamp of the creating binary
	GoVersion  string       `json:"go_version,omitempty"` // Go toolchain of the creating binary
	CreatedAt  string       `json:"created_at"`
	InstallDir string       `json:"install_dir"`
	Files      []string     `json:"files"`
	Auth       *authSummary `json:"auth,omitempty"` // which auth providers/secrets the archive carries
	// IncludesStack is true when the archive also holds stack binaries and/or
	// frontend-dist, so restore can roll the software version back.
	IncludesStack bool `json:"includes_stack,omitempty"`
	// Checksums maps each data/ file (archive-relative) to its SHA-256, so
	// `v2-restore --verify-only` can tell a file changed after the backup.
	Checksums map[string]string `json:"sha256,omitempty"`
}

// authSummary records which authentication providers a backed-up user database
// holds and whether their server-side secrets came along. SAML and LDAP config
// (including the SP signing key and the LDAP bind password) live inside
// .ncc-api-users.json rather than in their own files, so this summary makes that
// coverage explicit in the manifest and in backup/restore output instead of
// leaving it implicit.
type authSummary struct {
	LocalAccounts       int  `json:"local_accounts"`
	SAMLPresent         bool `json:"saml_present"`
	SAMLEnabled         bool `json:"saml_enabled"`
	SAMLHasSPKey        bool `json:"saml_has_sp_key"`
	LDAPPresent         bool `json:"ldap_present"`
	LDAPEnabled         bool `json:"ldap_enabled"`
	LDAPHasBindPassword bool `json:"ldap_has_bind_password"`
}

// backupEntry pairs an on-disk absolute path with the archive-relative path it
// is stored under (always relative to the install dir).
type backupEntry struct {
	Rel string
	Abs string
}

type v2BackupOptions struct {
	InstallDir string
	OutputFile string
	// OutputDir, when set and OutputFile is empty, writes the default
	// timestamped ncc-backup-<UTC>.tar.gz into this directory instead of the
	// current working directory. Lets a scheduled backup land in a fixed
	// location (e.g. <install>/backups) without the caller computing the stamp.
	OutputDir string
	// Retain, when > 0, prunes older ncc-backup-*.tar.gz siblings of the output
	// file so at most Retain backups are kept (newest wins).
	Retain int
	// Encrypt seals the finished archive with AES-256-GCM. Key material comes
	// from Passphrase/NCC_BACKUP_PASSPHRASE (scrypt) or KeyFile/NCC_BACKUP_KEY_*
	// (raw 32-byte key).
	Encrypt    bool
	KeyFile    string
	Passphrase string
	// IncludeStack also archives the install's stack binaries (bin/ncc-*) and
	// frontend-dist so a restore can roll the software version back, not just
	// config and accounts. Used by the in-app updater's pre-update snapshot.
	IncludeStack bool
}

type v2RestoreOptions struct {
	InstallDir string
	InputFile  string
	Force      bool
	Restart    bool
	NoRestart  bool
	// VerifyOnly runs the isolated backup check (backupverify.go) and reports
	// pass/warn/fail per check, without extracting anything.
	VerifyOnly bool
	// JSON prints the VerifyOnly report as JSON (used by the API server).
	JSON bool
	// Out receives the VerifyOnly report (default os.Stdout).
	Out io.Writer
	// KeyFile/Passphrase decrypt an encrypted backup; an unencrypted archive
	// ignores them.
	KeyFile    string
	Passphrase string
	// TrustStack restores the backup's program files (bin/ncc-*,
	// frontend-dist/) even when this host has never installed them. Command
	// line only: the API never passes it (see trustedstack.go).
	TrustStack bool
}

// Backup archive limits, shared by the verifier and the restorer so the two
// agree on what is acceptable (a file that verifies must also restore without
// silent truncation). They also bound the work a malicious or corrupt archive
// can force: a decompression bomb (a tiny gzip that expands to gigabytes)
// cannot exhaust memory or disk during restore.
const (
	maxBackupFileBytes  = 64 << 20  // per-file uncompressed cap (64 MiB)
	maxBackupTotalBytes = 512 << 20 // total uncompressed cap across all files (512 MiB)
	maxBackupFileCount  = 10000     // sanity cap on the number of entries
)

// backupVerifyResult summarizes a validated backup archive.
type backupVerifyResult struct {
	Manifest  backupManifest
	DataFiles int
	Bytes     int64
}

// verifyBackupArchive reads an archive end-to-end and validates it is a
// restorable ncc backup: it must decompress cleanly (gzip CRC), untar without
// error, carry a parseable manifest.json, contain at least one data/ file, and
// hold no path-traversal/absolute entries. It writes nothing. A "backup" that
// cannot be restored is worse than no backup, so create/restore and the doctor
// all gate on this.
func verifyBackupArchive(path string) (backupVerifyResult, error) {
	var res backupVerifyResult
	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return res, fmt.Errorf("not a gzip archive: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	sawManifest := false
	sep := string(filepath.Separator)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return res, fmt.Errorf("corrupt tar stream: %w", err)
		}
		if hdr.FileInfo().IsDir() {
			continue
		}
		switch {
		case hdr.Name == "manifest.json":
			data, err := io.ReadAll(io.LimitReader(tr, 1<<20))
			if err != nil {
				return res, err
			}
			if err := json.Unmarshal(data, &res.Manifest); err != nil {
				return res, fmt.Errorf("invalid manifest.json: %w", err)
			}
			sawManifest = true
		case strings.HasPrefix(hdr.Name, "data/"):
			rel := strings.TrimPrefix(hdr.Name, "data/")
			clean := filepath.Clean(filepath.FromSlash(rel))
			if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+sep) || filepath.IsAbs(clean) {
				return res, fmt.Errorf("unsafe path in archive: %q", hdr.Name)
			}
			if hdr.Typeflag != tar.TypeReg {
				return res, fmt.Errorf("%q in archive is not a regular file", hdr.Name)
			}
			// +1 so we can tell "exactly at the cap" from "over the cap" and
			// reject an oversize file rather than let it through (the restorer
			// would otherwise truncate it).
			n, err := io.Copy(io.Discard, io.LimitReader(tr, maxBackupFileBytes+1))
			if err != nil {
				return res, fmt.Errorf("read %s: %w", hdr.Name, err)
			}
			if n > maxBackupFileBytes {
				return res, fmt.Errorf("archive file %q exceeds the per-file limit (%d bytes)", hdr.Name, maxBackupFileBytes)
			}
			res.DataFiles++
			if res.DataFiles > maxBackupFileCount {
				return res, fmt.Errorf("archive contains too many files (>%d)", maxBackupFileCount)
			}
			res.Bytes += n
			if res.Bytes > maxBackupTotalBytes {
				return res, fmt.Errorf("archive exceeds the total size limit (%d bytes)", maxBackupTotalBytes)
			}
		}
	}
	if !sawManifest {
		return res, fmt.Errorf("archive missing manifest.json (not an ncc backup?)")
	}
	if res.DataFiles == 0 {
		return res, fmt.Errorf("archive contains no data files to restore")
	}
	return res, nil
}

// pruneOldBackups keeps at most retain newest ncc-backup-*.tar.gz files in dir,
// deleting the rest. Returns the names pruned.
func pruneOldBackups(dir string, retain int) ([]string, error) {
	if retain <= 0 {
		return nil, nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, "ncc-backup-*.tar.gz"))
	if err != nil {
		return nil, err
	}
	if len(matches) <= retain {
		return nil, nil
	}
	type bk struct {
		path string
		mod  time.Time
	}
	var list []bk
	for _, m := range matches {
		if st, err := os.Stat(m); err == nil && !st.IsDir() {
			list = append(list, bk{m, st.ModTime()})
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].mod.After(list[j].mod) })
	var pruned []string
	for _, b := range list[retain:] {
		if err := os.Remove(b.path); err == nil {
			pruned = append(pruned, filepath.Base(b.path))
		}
	}
	return pruned, nil
}

// isRegularFile reports whether path exists and is a regular (non-dir) file.
func isRegularFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// sensitiveBackupName reports whether a backed-up file holds secrets and must be
// (re)written with 0600 perms: the auth token, user DB, bootstrap admin
// password, and any config-referenced secrets file.
func sensitiveBackupName(rel string) bool {
	base := strings.ToLower(filepath.Base(rel))
	switch {
	case strings.HasPrefix(base, ".ncc-api-"),
		base == ".ncc-initial-admin-password",
		strings.Contains(base, "secret"):
		return true
	}
	return false
}

// extractConfigRefPath pulls a `key: value` file path out of raw config.yaml
// content for the small set of file-reference keys. Kept local so the
// orchestrator binary needs no api-server internals. Returns "" when absent or
// explicitly unset.
func extractConfigRefPath(content, key string) string {
	rx := regexp.MustCompile(`(?im)^\s*` + regexp.QuoteMeta(key) + `\s*:\s*(.+?)\s*$`)
	m := rx.FindStringSubmatch(content)
	if m == nil {
		return ""
	}
	v := strings.TrimSpace(m[1])
	// Strip a trailing inline comment (best-effort; paths rarely contain " #").
	if i := strings.Index(v, " #"); i >= 0 {
		v = strings.TrimSpace(v[:i])
	}
	v = strings.Trim(v, `"'`)
	if v == "" || v == "~" || strings.EqualFold(v, "null") {
		return ""
	}
	return v
}

func stackBinaryRel(rel string) bool {
	rel = filepath.ToSlash(rel)
	if strings.HasPrefix(rel, "frontend-dist/") || rel == "frontend-dist" {
		return false
	}
	return backupRelIsStack(rel)
}

func backupRelIsStack(rel string) bool {
	rel = filepath.ToSlash(rel)
	if rel == "frontend-dist" || strings.HasPrefix(rel, "frontend-dist/") {
		return true
	}
	base := filepath.Base(rel)
	switch base {
	case "ncc-orchestrator", "ncc-orchestrator.exe", "ncc-api-server", "ncc-api-server.exe", "ncc-ui-server", "ncc-ui-server.exe":
		dir := filepath.ToSlash(filepath.Dir(rel))
		if dir == "." || dir == "bin" {
			return true
		}
	}
	return isProgramFileAlias(rel)
}

var stackBinaryNames = []string{
	"ncc-orchestrator", "ncc-orchestrator.exe",
	"ncc-api-server", "ncc-api-server.exe",
	"ncc-ui-server", "ncc-ui-server.exe",
}

// collectStackBackupEntries captures the running software version: the three
// stack binaries and the frontend bundle. Manual/scheduled backups omit these
// (they are regenerable); pre-update rollback points include them.
func collectStackBackupEntries(installDir string) (entries []backupEntry, skipped []string) {
	seen := map[string]bool{}
	add := func(abs string) {
		abs = filepath.Clean(abs)
		if !isRegularFile(abs) {
			return
		}
		rel, err := filepath.Rel(installDir, abs)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			skipped = append(skipped, abs)
			return
		}
		if seen[rel] {
			return
		}
		seen[rel] = true
		entries = append(entries, backupEntry{Rel: filepath.ToSlash(rel), Abs: abs})
	}
	for _, name := range stackBinaryNames {
		add(filepath.Join(installDir, "bin", name))
		add(filepath.Join(installDir, name))
	}
	front := filepath.Join(installDir, "frontend-dist")
	_ = filepath.Walk(front, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		add(path)
		return nil
	})
	return entries, skipped
}

// collectBackupEntries returns the config/auth/state files to capture from an
// install dir. Files that resolve outside the install dir (e.g. a
// config-referenced file placed elsewhere) are reported via skipped so the
// caller can warn; the archive is always confined to the install dir so restore
// can never write outside it. includeStack also captures bin/ncc-* and
// frontend-dist for versioned rollback.
func collectBackupEntries(installDir string, includeStack bool) (entries []backupEntry, skipped []string) {
	seen := map[string]bool{}
	add := func(abs string) {
		abs = filepath.Clean(abs)
		if !isRegularFile(abs) {
			return
		}
		rel, err := filepath.Rel(installDir, abs)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			skipped = append(skipped, abs)
			return
		}
		if seen[rel] {
			return
		}
		seen[rel] = true
		entries = append(entries, backupEntry{Rel: filepath.ToSlash(rel), Abs: abs})
	}

	// Core auth/state files, by convention at the install-dir root.
	for _, name := range []string{
		"config.yaml",
		".ncc-api-users.json",
		".ncc-api-token",
		".ncc-initial-admin-password",
		".ncc-api-schedule.json",
		".ncc-api-notifications.json",
		".ncc-api-backup-schedule.json",
		v2StartStateFile, // persisted v2-start flags (CORS, listen, session TTL, …)
	} {
		add(filepath.Join(installDir, name))
	}

	// Sweep any other api-server state the install may hold at its root
	// (e.g. future ".ncc-api-*" files), so backups stay complete as new
	// runtime state is added without having to revisit this list. seen[]
	// dedupes against the explicit names above.
	if matches, err := filepath.Glob(filepath.Join(installDir, ".ncc-api-*")); err == nil {
		for _, m := range matches {
			add(m)
		}
	}

	// Kubernetes PVC layout stores config/auth/logs under subdirectories of
	// the shared volume root (typically /data). Include those files when the
	// backup is taken from the volume root rather than the config/ folder.
	for _, rel := range []string{
		filepath.Join("config", "config.yaml"),
		filepath.Join("auth", ".ncc-api-token"),
		filepath.Join("auth", ".ncc-initial-admin-password"),
		filepath.Join("logs", "ncc-audit.log"),
		filepath.Join("logs", ".ncc-api-notifications.json"),
		filepath.Join("config", ".ncc-api-schedule.json"),
		filepath.Join("config", ".ncc-api-backup-schedule.json"),
	} {
		add(filepath.Join(installDir, rel))
	}
	if matches, err := filepath.Glob(filepath.Join(installDir, "config", ".ncc-api-*")); err == nil {
		for _, m := range matches {
			add(m)
		}
	}

	// JSONL audit log (security/action history). In a standard v2 install the
	// api-server writes it under <install-dir>/logs/ncc-audit.log; add() is a
	// no-op when it is absent or rolled elsewhere.
	add(filepath.Join(installDir, "logs", "ncc-audit.log"))

	// Config-referenced files (clusters list / alert-exclusions / secrets) plus
	// the latest run's report artifacts.
	cfgPath := filepath.Join(installDir, "config.yaml")
	if !isRegularFile(cfgPath) {
		cfgPath = filepath.Join(installDir, "config", "config.yaml")
	}
	if content, err := os.ReadFile(cfgPath); err == nil {
		cfg := string(content)
		for _, key := range []string{"clusters-file", "exclude-alert-titles-file", "secrets-file"} {
			raw := extractConfigRefPath(cfg, key)
			if raw == "" {
				continue
			}
			p := raw
			if !filepath.IsAbs(p) {
				p = filepath.Join(installDir, p)
			}
			add(p)
		}

		// Latest run data: the dashboard reads run-summary.json plus the HTML /
		// CSV / JSON report artifacts that live at the top level of
		// output-dir-filtered. Capture those regular files (non-recursive) so a
		// restored stack shows the most recent run immediately. The potentially
		// large run-history under <output-dir-filtered>/runs/ is intentionally
		// left out — add() skips directories — keeping the archive to a single
		// run's worth of artifacts rather than the full history.
		outDir := strings.TrimSpace(extractConfigRefPath(cfg, "output-dir-filtered"))
		if outDir == "" {
			outDir = defaultOutputDirFiltered
		}
		if !filepath.IsAbs(outDir) {
			outDir = filepath.Join(installDir, outDir)
		}
		if matches, err := filepath.Glob(filepath.Join(outDir, "*")); err == nil {
			for _, m := range matches {
				add(m) // add() ignores directories (e.g. the runs/ history) and non-regular files
			}
		}
	}
	if includeStack {
		more, moreSkip := collectStackBackupEntries(installDir)
		entries = append(entries, more...)
		skipped = append(skipped, moreSkip...)
	}
	return entries, skipped
}

// summarizeAuthProviders parses a backed-up .ncc-api-users.json (best-effort,
// using a local shape so the orchestrator needs no api-server internals) to
// report which auth providers and secrets it carries. SAML/LDAP config — and
// their secrets (the SAML SP signing key, the LDAP bind password) — are stored
// inside this single file, so reading it is how backup/restore verifies that
// SSO/directory config actually travelled with the archive. Returns nil when
// the file is absent or unparseable.
func summarizeAuthProviders(usersDBPath string) *authSummary {
	data, err := os.ReadFile(usersDBPath)
	if err != nil {
		return nil
	}
	return summarizeAuthProvidersBytes(data)
}

// summarizeAuthProvidersBytes is summarizeAuthProviders for in-memory content
// (the backup verifier reads the user database straight from the archive).
func summarizeAuthProvidersBytes(data []byte) *authSummary {
	var db struct {
		Users []json.RawMessage `json:"users"`
		SAML  *struct {
			Enabled  bool   `json:"enabled"`
			SPKeyPEM string `json:"sp_key_pem"`
		} `json:"saml"`
		LDAP *struct {
			Enabled      bool   `json:"enabled"`
			BindPassword string `json:"bind_password"`
		} `json:"ldap"`
	}
	if json.Unmarshal(data, &db) != nil {
		return nil
	}
	s := &authSummary{LocalAccounts: len(db.Users)}
	if db.SAML != nil {
		s.SAMLPresent = true
		s.SAMLEnabled = db.SAML.Enabled
		s.SAMLHasSPKey = strings.TrimSpace(db.SAML.SPKeyPEM) != ""
	}
	if db.LDAP != nil {
		s.LDAPPresent = true
		s.LDAPEnabled = db.LDAP.Enabled
		s.LDAPHasBindPassword = strings.TrimSpace(db.LDAP.BindPassword) != ""
	}
	return s
}

func yesNoStr(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func enabledWord(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

// printAuthSummary writes a human-readable auth-provider summary to w and
// returns any warnings (a provider is enabled but its server-side secret is
// missing). A nil summary prints nothing.
func printAuthSummary(w io.Writer, s *authSummary) []string {
	if s == nil {
		return nil
	}
	fmt.Fprintf(w, "auth providers:\n")
	fmt.Fprintf(w, "  - local accounts: %d\n", s.LocalAccounts)
	if s.SAMLPresent {
		fmt.Fprintf(w, "  - SAML SSO:       %s (SP signing key: %s)\n", enabledWord(s.SAMLEnabled), yesNoStr(s.SAMLHasSPKey))
	} else {
		fmt.Fprintf(w, "  - SAML SSO:       not configured\n")
	}
	if s.LDAPPresent {
		fmt.Fprintf(w, "  - LDAP / AD:      %s (bind password: %s)\n", enabledWord(s.LDAPEnabled), yesNoStr(s.LDAPHasBindPassword))
	} else {
		fmt.Fprintf(w, "  - LDAP / AD:      not configured\n")
	}
	var warns []string
	if s.SAMLEnabled && !s.SAMLHasSPKey {
		warns = append(warns, "SAML is enabled but no SP signing key is stored; the server may regenerate one and you may need to re-publish SP metadata to the IdP.")
	}
	if s.LDAPEnabled && !s.LDAPHasBindPassword {
		warns = append(warns, "LDAP is enabled with no bind password stored (anonymous bind); confirm this is intended for your directory.")
	}
	return warns
}

// tarWriteBytes writes one in-memory file into a tar stream.
func tarWriteBytes(tw *tar.Writer, name string, data []byte, mode int64) error {
	hdr := &tar.Header{Name: name, Size: int64(len(data)), Mode: mode, ModTime: time.Now()}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// writeBackupArchive serializes the manifest + entries into a tar.gz at out
// (created 0600 because it contains secrets).
func writeBackupArchive(out string, manifest backupManifest, entries []backupEntry) error {
	f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	restrictNewSecret(out)
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	if manifest.Checksums == nil {
		manifest.Checksums = make(map[string]string, len(entries))
		for _, e := range entries {
			sum, err := sha256File(e.Abs)
			if err != nil {
				return fmt.Errorf("read %s: %w", e.Abs, err)
			}
			manifest.Checksums[e.Rel] = sum
		}
	}
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := tarWriteBytes(tw, "manifest.json", mb, 0o600); err != nil {
		return err
	}
	for _, e := range entries {
		data, err := os.ReadFile(e.Abs)
		if err != nil {
			return fmt.Errorf("read %s: %w", e.Abs, err)
		}
		mode := int64(0o644)
		if info, err := os.Stat(e.Abs); err == nil {
			if perm := info.Mode().Perm(); perm != 0 {
				mode = int64(perm)
			}
		}
		if sensitiveBackupName(e.Rel) {
			mode = 0o600
		}
		if stackBinaryRel(e.Rel) {
			mode |= 0o111
		}
		if err := tarWriteBytes(tw, "data/"+e.Rel, data, mode); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

func runV2Backup(opts v2BackupOptions) error {
	installDir := strings.TrimSpace(opts.InstallDir)
	if installDir == "" {
		installDir = defaultV2InstallDir()
	}
	if abs, err := filepath.Abs(installDir); err == nil {
		installDir = abs
	}
	if st, err := os.Stat(installDir); err != nil || !st.IsDir() {
		return fmt.Errorf("install dir not found: %s", installDir)
	}
	// Record the installed program files as trusted, so restoring this
	// backup (or a pre-update rollback point) may put them back.
	recordTrustedStackQuiet(installDir, "backup")
	entries, skipped := collectBackupEntries(installDir, opts.IncludeStack)
	if len(entries) == 0 {
		return fmt.Errorf("nothing to back up under %s (no config.yaml, user database, or state files found)", installDir)
	}
	out := strings.TrimSpace(opts.OutputFile)
	if out == "" {
		name := fmt.Sprintf("ncc-backup-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z"))
		if dir := strings.TrimSpace(opts.OutputDir); dir != "" {
			out = filepath.Join(dir, name)
		} else {
			out = name
		}
	}
	if abs, err := filepath.Abs(out); err == nil {
		out = abs
	}
	// Disk guard: refuse to write a backup we don't have room for (a truncated
	// archive is an unrestorable backup). Estimate from the uncompressed source
	// size plus headroom; gzip only makes the real archive smaller.
	var estBytes uint64
	for _, e := range entries {
		if st, err := os.Stat(e.Abs); err == nil {
			estBytes += uint64(st.Size())
		}
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("create backup output dir: %w", err)
	}
	if free, ok := diskFreeBytes(filepath.Dir(out)); ok && free < estBytes+(64<<20) {
		return fmt.Errorf("insufficient disk space for backup: need ~%s, only %s free on %s; prune old backups or free space", humanBytes(estBytes), humanBytes(free), filepath.Dir(out))
	}
	rels := make([]string, 0, len(entries))
	for _, e := range entries {
		rels = append(rels, e.Rel)
	}
	auth := summarizeAuthProviders(filepath.Join(installDir, ".ncc-api-users.json"))
	includesStack := false
	for _, e := range entries {
		if backupRelIsStack(e.Rel) {
			includesStack = true
			break
		}
	}
	manifest := backupManifest{
		Tool:          "ncc-orchestrator",
		Version:       Version,
		Stream:        Stream,
		BuildDate:     BuildDate,
		GoVersion:     GoVersion,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		InstallDir:    installDir,
		Files:         rels,
		Auth:          auth,
		IncludesStack: includesStack,
	}
	if err := writeBackupArchive(out, manifest, entries); err != nil {
		return err
	}
	// Verify-after-create: immediately read the archive back and validate it is
	// restorable. If it isn't, delete the bad file so it can't be mistaken for a
	// usable backup, and fail loudly.
	vr, verr := verifyBackupArchive(out)
	if verr != nil {
		_ = os.Remove(out)
		return fmt.Errorf("backup verification failed (removed %s): %w", out, verr)
	}
	// Encrypt-at-rest: seal the verified archive after verification (the sealed
	// file is no longer a readable tar.gz, so it must be sealed last).
	encrypted := false
	if opts.Encrypt {
		key, mode, salt, kerr := resolveBackupEncKey(opts.KeyFile, opts.Passphrase)
		if kerr != nil {
			_ = os.Remove(out)
			return fmt.Errorf("encrypt backup: %w", kerr)
		}
		if eerr := encryptBackupFile(out, key, mode, salt); eerr != nil {
			_ = os.Remove(out)
			return fmt.Errorf("encrypt backup: %w", eerr)
		}
		encrypted = true
	}
	fmt.Printf("Backup written: %s\n", out)
	fmt.Printf("verified:       %s readable from archive (%s uncompressed)\n", plural.Count(vr.DataFiles, "file"), humanBytes(uint64(vr.Bytes)))
	if encrypted {
		fmt.Printf("encrypted:      yes (AES-256-GCM) — restore needs the same passphrase/key\n")
	}
	fmt.Printf("created by:     ncc-orchestrator %s (%s, built %s)\n", Version, Stream, BuildDate)
	fmt.Printf("install-dir:    %s\n", installDir)
	fmt.Printf("files (%d):\n", len(entries))
	for _, e := range entries {
		fmt.Printf("  - %s\n", e.Rel)
	}
	for _, s := range skipped {
		fmt.Printf("  ! skipped (resolves outside install dir, not archived): %s\n", s)
	}
	if auth != nil {
		fmt.Println()
		warns := printAuthSummary(os.Stdout, auth)
		for _, wmsg := range warns {
			fmt.Printf("  ! %s\n", wmsg)
		}
	}
	if opts.Retain > 0 {
		if pruned, perr := pruneOldBackups(filepath.Dir(out), opts.Retain); perr == nil && len(pruned) > 0 {
			fmt.Printf("retention:      kept newest %d, pruned %s: %s\n", opts.Retain, plural.Count(len(pruned), "older backup"), strings.Join(pruned, ", "))
		}
	}
	if encrypted {
		fmt.Println("\nThis archive contains secrets (API token, password hashes, SAML SP key, LDAP bind password) but is encrypted at rest (AES-256-GCM). Keep the passphrase/key separate from the archive.")
	} else {
		fmt.Println("\nThis archive contains secrets (API token, password hashes, SAML SP key, LDAP bind password). It was created with 0600 permissions; store it securely, or use --encrypt to seal it.")
	}
	return nil
}

// v2StackRunning reports whether any of the stack's pid files point at a live
// process, so restore can refuse to overwrite a running install dir.
func v2StackRunning(installDir string) (bool, string) {
	runDir := filepath.Join(installDir, "run")
	for _, name := range []string{"v2-supervisor.pid", "v2-api.pid", "v2-ui.pid", "v2-api-supervisor.pid", "v2-ui-supervisor.pid"} {
		if pid, err := readPIDFromFile(filepath.Join(runDir, name)); err == nil && processIsAlive(pid) {
			return true, name
		}
	}
	return false, ""
}
