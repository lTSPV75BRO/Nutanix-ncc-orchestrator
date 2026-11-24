package main

// Restore policy (SEC-19): which data paths of a backup may be written.
//
// The trusted-stack ledger (trustedstack.go) decides about program files, but
// other files in the install dir also end up being run: the supervisor copies
// run/upgrade-rollback/ over bin/ when run/v2-upgrade-state.json says a
// rollback was interrupted, systemd and cron run the generated scheduler
// scripts in logs/, and so on. No backup this tool makes holds such files, so
// a restore leaves them out whoever made the archive, --trust-stack included.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"goncc/internal/plural"
)

// restoreProtectedDirs are top-level folders a backup never holds and whose
// content is run, or decides what is run or served.
var restoreProtectedDirs = map[string]string{
	"run":      "the supervisor's working files",
	"bin":      "the programs folder",
	"helper":   "the update helper's program",
	"tls":      "this server's certificates and keys",
	"backups":  "this server's backups",
	"frontend": "a fallback UI folder",
	"cmd":      "a fallback source folder",
}

// runnableExts are files the OS, a service manager or a scheduler runs or
// loads; keyExts are certificates and private keys.
var (
	runnableExts = map[string]bool{
		".sh": true, ".bash": true, ".cmd": true, ".bat": true, ".ps1": true, ".psm1": true, ".psd1": true,
		".vbs": true, ".vbe": true, ".exe": true, ".com": true, ".scr": true, ".msi": true, ".lnk": true,
		".dll": true, ".so": true, ".dylib": true,
		".service": true, ".timer": true, ".socket": true, ".plist": true,
	}
	keyExts = map[string]bool{".pem": true, ".key": true, ".crt": true, ".cer": true, ".pfx": true, ".p12": true}
)

// isRunnableFile reports whether path names a script, program or service file.
func isRunnableFile(path string) bool {
	return runnableExts[strings.ToLower(filepath.Ext(strings.TrimRight(path, ". ")))]
}

// restoreSkipReason says why the archive path rel (clean OS path) must not be
// restored, or "" when it may be. Program files are left to judgeStack.
func restoreSkipReason(rel string) string {
	if isTrustedStackLedger(rel) {
		return "this host's own record"
	}
	elems := archivePathElems(rel)
	if runtime.GOOS == "windows" {
		if !filepath.IsLocal(rel) {
			return "not a valid file name here"
		}
		for i, e := range strings.Split(filepath.ToSlash(rel), "/") {
			if e != elems[i] {
				return "not a valid file name here"
			}
		}
	}
	if backupRelIsStack(rel) {
		return ""
	}
	for dir, what := range restoreProtectedDirs {
		if strings.EqualFold(elems[0], dir) {
			return what
		}
	}
	last := elems[len(elems)-1]
	if isProgramFileAlias(last) {
		return "a program file name"
	}
	for _, e := range elems {
		if strings.EqualFold(e, "frontend-dist") {
			return "a UI folder"
		}
	}
	ext := strings.ToLower(filepath.Ext(last))
	switch {
	case runnableExts[ext]:
		return "a script, program or service file"
	case keyExts[ext] && !strings.HasPrefix(last, ".ncc-api-"): // the app's own state, which backups hold
		return "a certificate or key"
	}
	return ""
}

// restoreSkipReasonAt is restoreSkipReason for a restore into installDir: it
// also refuses to replace a folder with a file, which would delete the folder.
func restoreSkipReasonAt(installDir, rel string) string {
	if why := restoreSkipReason(rel); why != "" {
		return why
	}
	if st, err := os.Lstat(filepath.Join(installDir, rel)); err == nil && st.IsDir() && !backupRelIsStack(rel) {
		return "a folder here"
	}
	return ""
}

// restoreFileMode is the mode a restored file gets. Data is never executable,
// and no program file is writable by other accounts.
func restoreFileMode(rel string, stored int64) os.FileMode {
	mode := os.FileMode(stored).Perm()
	if mode == 0 {
		mode = 0o644
	}
	if sensitiveBackupName(rel) {
		mode = 0o600
	}
	switch {
	case stackBinaryRel(rel):
		return mode&^0o022 | 0o111
	case backupRelIsStack(rel):
		return mode &^ 0o133
	}
	return mode &^ 0o111
}

// addProtectedPathsCheck warns about files a restore would leave out.
func (r *backupInspectReport) addProtectedPathsCheck(sc backupTarScan) {
	var bad []string
	for _, rel := range sc.dataOrder {
		if why := restoreSkipReason(filepath.FromSlash(rel)); why != "" {
			bad = append(bad, fmt.Sprintf("%s (%s)", rel, why))
		}
	}
	if len(bad) == 0 {
		return
	}
	sort.Strings(bad)
	r.add("Protected files", checkWarn, "%s that a backup never holds would be left out: %s. The archive was changed after it was made, or was not made by ncc-orchestrator.",
		plural.Count(len(bad), "file"), summarizeIssues(bad))
}

// validateSchedulePaths refuses scheduler paths that would inject lines into
// the generated systemd unit or crontab, or append the run's output to a file
// that is run. They can come from a restored schedule.
func validateSchedulePaths(logPath, configPath string) error {
	for _, ch := range logPath + configPath {
		if ch < 0x20 || ch == 0x7f {
			return fmt.Errorf("log-path and config must not contain control characters")
		}
	}
	return validateLogFilePath("log-path", logPath)
}

// validateCronSpec accepts an @-macro or exactly five fields. Anything more
// would become part of the command in the crontab, and a restored schedule can
// carry any text.
func validateCronSpec(spec string) error {
	spec = strings.TrimSpace(spec)
	if strings.HasPrefix(spec, "@") {
		switch spec {
		case "@reboot", "@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly":
			return nil
		}
		return fmt.Errorf("cron: unknown macro %q", spec)
	}
	for _, ch := range spec {
		if !strings.ContainsRune("*,/-~ 	", ch) && (ch < '0' || ch > '9') && (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') {
			return fmt.Errorf("cron contains invalid characters")
		}
	}
	if n := len(strings.Fields(spec)); n != 5 {
		return fmt.Errorf("cron must have five fields (minute hour day-of-month month day-of-week), got %d", n)
	}
	return nil
}

// validateLogFilePath refuses to append log lines to a script or program.
func validateLogFilePath(what, p string) error {
	base := filepath.Base(p)
	if isRunnableFile(base) || isProgramFileAlias(base) {
		return fmt.Errorf("%s must not be a script or program file (%s)", what, base)
	}
	return nil
}

// keepHostValue makes *dst the host's value, "" included, and reports whether
// that changed it.
func keepHostValue[T comparable](dst *T, host T) bool {
	if *dst == host {
		return false
	}
	*dst = host
	return true
}

// snapshotHashes hashes every file of the supervisor's rollback folder (slash
// rel path -> sha256 hex).
func snapshotHashes(snap string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.Walk(snap, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(snap, path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", filepath.ToSlash(rel))
		}
		sum, err := sha256File(path)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = sum
		return nil
	})
	return out, err
}

// recordUpgradeSnapshot notes in the ledger what the supervisor saved before
// an upgrade, so a rollback resumed by another supervisor process can tell its
// own copy from a planted one. nil clears the note.
func recordUpgradeSnapshot(installDir string, files map[string]string) error {
	l := loadTrustedStack(installDir)
	if len(l.UpgradeSnapshot) == 0 && len(files) == 0 {
		return nil
	}
	l.UpgradeSnapshot = files
	return writeTrustedStack(installDir, l)
}

// checkUpgradeSnapshot reports why the rollback folder must not be put back:
// its files are not the ones this supervisor saved (known, nil after a
// restart), nor the ones noted in the ledger, nor one set recorded there (an
// older version rewrites the ledger without the note).
func checkUpgradeSnapshot(installDir, snap string, known map[string]string) error {
	got, err := snapshotHashes(snap)
	if err != nil {
		return err
	}
	if len(got) == 0 {
		return fmt.Errorf("it is empty")
	}
	ledger := loadTrustedStack(installDir)
	for _, want := range []map[string]string{known, ledger.UpgradeSnapshot} {
		if len(want) > 0 && sameFiles(got, want) {
			return nil
		}
	}
	sets := []map[string]string{installedStackHashes(installDir)}
	for _, s := range ledger.Sets {
		sets = append(sets, s.Files)
	}
	for _, s := range sets {
		if snapshotMatchesSet(got, s) {
			return nil
		}
	}
	return fmt.Errorf("its files are not a version this server ran")
}

// snapshotMatchesSet reports whether a rollback folder holds exactly the
// bin/ and frontend-dist/ files of set. The supervisor's own binary (self/)
// must be one of the set's, and leftovers of an update are ignored.
func snapshotMatchesSet(snap, set map[string]string) bool {
	bins := 0
	for rel, sum := range snap {
		switch {
		case strings.HasPrefix(rel, "self/"):
			found := false
			for r, s := range set {
				found = found || (filepath.Base(r) == filepath.Base(rel) && s == sum)
			}
			if !found {
				return false
			}
		case strings.HasPrefix(rel, "bin/") && strings.Contains(rel, updateAsideMarker):
		case set[rel] != sum:
			return false
		case strings.HasPrefix(rel, "bin/"):
			bins++
		}
	}
	for rel := range set {
		if strings.HasPrefix(rel, "bin/") || strings.HasPrefix(rel, "frontend-dist/") {
			if _, ok := snap[rel]; !ok {
				return false
			}
		}
	}
	return bins > 0
}
