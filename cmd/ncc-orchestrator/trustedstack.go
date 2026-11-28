package main

// Trusted stack (SEC-07): which program files a restore may put back.
//
// A backup that holds the stack (bin/ncc-* and frontend-dist/) used to have
// those files written and made executable on restore, so anyone able to
// restore a crafted archive (an admin in the UI, or anyone holding the backup
// key) could run code on the host as the service account after the restart.
//
// The fix keeps a ledger, in the install dir, of the SHA-256 of every stack
// file set this host has actually installed or run: it is recorded when the
// stack starts, when the supervisor starts, after a supervised upgrade, and
// whenever v2-backup runs (so a pre-update rollback point is always covered).
// Trusting files this host already ran adds no new code.
//
// On restore the stack is put back only when the program files on disk
// afterwards would be exactly one recorded set, or exactly what is installed
// now (SEC-07b). Judging each file on its own would let a crafted backup
// combine files of different versions, for example an old, vulnerable api
// server with a newer UI, which never ran together here. "On disk afterwards"
// is what matters: a restore replaces frontend-dist as a whole (no installed
// asset survives) but leaves installed binaries the backup does not hold, so
// those count too. Otherwise only data is restored, and the operator installs
// that version separately. The ledger is never backed up and never restored
// from an archive. `v2-restore --trust-stack`, only available on the server's
// command line (the API never passes it), skips the check.

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"goncc/internal/plural"
)

const (
	trustedStackFile = ".ncc-trusted-stack.json"
	// trustedStackMaxSets bounds the ledger: about the last 20 versions.
	trustedStackMaxSets = 20
	// unreadableSum stands for a program file that could not be hashed. It is
	// not a SHA-256, so it never matches a recorded or archived file.
	unreadableSum = "unreadable"
)

type trustedStackSet struct {
	Version    string            `json:"version,omitempty"`
	RecordedAt string            `json:"recorded_at"`
	Source     string            `json:"source,omitempty"`
	Files      map[string]string `json:"files"` // slash rel path → sha256 hex
}

type trustedStackLedger struct {
	Sets []trustedStackSet `json:"sets"`
	// UpgradeSnapshot: the rollback folder of the upgrade in progress
	// (recordUpgradeSnapshot, restorepolicy.go).
	UpgradeSnapshot map[string]string `json:"upgrade_snapshot,omitempty"`
}

// archivePathElems splits an archive path into elements, each reduced to the
// name the file system may really use: Windows drops trailing dots and spaces
// and reads "name:stream" as name. Compare the elements with EqualFold, since
// Windows and macOS ignore case.
func archivePathElems(rel string) []string {
	elems := strings.Split(filepath.ToSlash(filepath.Clean(rel)), "/")
	for i, e := range elems {
		if j := strings.IndexByte(e, ':'); j >= 0 {
			e = e[:j]
		}
		elems[i] = strings.TrimRight(e, ". ")
	}
	return elems
}

// isTrustedStackLedger reports whether rel names the ledger itself.
func isTrustedStackLedger(rel string) bool {
	e := archivePathElems(rel)
	return len(e) == 1 && strings.EqualFold(e[0], trustedStackFile)
}

// isProgramFileAlias reports whether rel reaches a program file under another
// spelling (case, trailing dot, stream, 8.3 short name) or is a binary name
// the server also starts (platform-suffixed). Such a path is never recorded,
// so counting it as a program file means it is never restored unchecked.
func isProgramFileAlias(rel string) bool {
	e := archivePathElems(rel)
	if strings.EqualFold(e[0], "frontend-dist") {
		return true
	}
	if runtime.GOOS == "windows" && len(e) > 1 && strings.Contains(e[0], "~") {
		return true // may be the short name of frontend-dist
	}
	if len(e) == 2 && strings.EqualFold(e[0], "bin") {
		e = e[1:]
	}
	if len(e) != 1 {
		return false
	}
	for _, base := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		for _, name := range []string{base, base + "-" + runtime.GOOS + "-" + runtime.GOARCH} {
			if strings.EqualFold(e[0], name) || strings.EqualFold(e[0], name+".exe") {
				return true
			}
		}
	}
	return false
}

func sha256File(path string) (string, error) {
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

// installedStackHashes hashes the stack files installed now; a file that
// can't be read is listed as unreadableSum.
func installedStackHashes(installDir string) map[string]string {
	entries, _ := collectStackBackupEntries(installDir)
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		sum, err := sha256File(e.Abs)
		if err != nil {
			sum = unreadableSum
		}
		out[filepath.ToSlash(e.Rel)] = sum
	}
	return out
}

func loadTrustedStack(installDir string) trustedStackLedger {
	var l trustedStackLedger
	b, err := os.ReadFile(filepath.Join(installDir, trustedStackFile))
	if err != nil {
		return l
	}
	if json.Unmarshal(b, &l) != nil {
		return trustedStackLedger{}
	}
	return l
}

func sameFiles(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	return containsFiles(b, a)
}

// containsFiles reports whether every file of sub is in set, unchanged.
func containsFiles(set, sub map[string]string) bool {
	for k, v := range sub {
		if got, ok := set[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// recordTrustedStack adds the installed stack to the ledger. Best-effort:
// a failure only means a later restore of this version restores data only.
func recordTrustedStack(installDir, source string) error {
	if unprivilegedInstall(installDir) {
		// Root keeps the ledger of an install run by the service user (the
		// install and the update helper record it); the service cannot.
		return nil
	}
	files := installedStackHashes(installDir)
	if len(files) == 0 {
		return nil
	}
	for rel, sum := range files {
		if sum == unreadableSum {
			// An incomplete set would never match a restore.
			return fmt.Errorf("cannot read %s", rel)
		}
	}
	l := loadTrustedStack(installDir)
	now := time.Now().UTC().Format(time.RFC3339)
	kept := l.Sets[:0]
	for _, s := range l.Sets {
		if !sameFiles(s.Files, files) {
			kept = append(kept, s)
		}
	}
	kept = append(kept, trustedStackSet{Version: Version, RecordedAt: now, Source: source, Files: files})
	if len(kept) > trustedStackMaxSets {
		kept = kept[len(kept)-trustedStackMaxSets:]
	}
	l.Sets = kept
	return writeTrustedStack(installDir, l)
}

func writeTrustedStack(installDir string, l trustedStackLedger) error {
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(installDir, trustedStackFile)
	if runtime.GOOS != "windows" {
		// A fresh, randomly named file: never one the service user planted
		// in the install (a fixed .tmp name could be a symbolic link).
		tmp, err := stageBytes(b, path, 0o600)
		if err != nil {
			return err
		}
		if err := os.Rename(tmp, path); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		return nil
	}
	tmp := path + ".tmp"
	if err := writeSecretFile(tmp, b); err != nil {
		return err
	}
	return renameWithRetry(tmp, path)
}

// recordTrustedStackQuiet is recordTrustedStack for start/backup paths that
// must never fail because of it.
func recordTrustedStackQuiet(installDir, source string) {
	if err := recordTrustedStack(installDir, source); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not record the installed program files as trusted: %v\n", err)
	}
}

// hashArchiveEntries returns the SHA-256 of the data/ entries named in rels
// (clean OS paths), streaming the archive so memory stays bounded.
func hashArchiveEntries(archive string, rels map[string]bool) (map[string]string, error) {
	f, err := os.Open(archive)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.FileInfo().IsDir() || !strings.HasPrefix(hdr.Name, "data/") {
			continue
		}
		clean := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(hdr.Name, "data/")))
		if !rels[clean] {
			continue
		}
		h := sha256.New()
		n, err := io.Copy(h, io.LimitReader(tr, maxBackupFileBytes+1))
		if err != nil {
			return nil, err
		}
		if n > maxBackupFileBytes {
			return nil, fmt.Errorf("archive file %q exceeds the per-file limit (%d bytes)", clean, maxBackupFileBytes)
		}
		out[clean] = hex.EncodeToString(h.Sum(nil))
	}
	return out, nil
}

// Why a backup's program files are left out. Each is printed as
// "reason: <value>", which the API server reads (restoreStackNote).
const (
	stackUnknownVersion    = "unknown version"
	stackMixedVersions     = "mixed versions"
	stackIncompleteVersion = "incomplete version"
)

// stackVerdict says whether a backup's program files may be restored.
type stackVerdict struct {
	reason string // "" = trusted
	total  int    // program files in the backup
	detail string // what is wrong, without a final full stop
}

func (v stackVerdict) trusted() bool { return v.reason == "" }

// judgeStack applies the trusted-stack rule to the program files of a backup
// (slash rel path -> sha256 hex).
func judgeStack(installDir string, archive map[string]string) stackVerdict {
	if len(archive) == 0 {
		return stackVerdict{}
	}
	installed := installedStackHashes(installDir)
	result := make(map[string]string, len(archive))
	for rel, sum := range archive {
		result[rel] = sum
	}
	var kept []string // installed files that would stay next to the restored ones
	for rel, sum := range installed {
		if strings.HasPrefix(rel, "frontend-dist/") {
			continue // replaced as a whole
		}
		if _, ok := archive[rel]; !ok {
			result[rel] = sum
			kept = append(kept, rel)
		}
	}
	sort.Strings(kept)
	sets := []map[string]string{installed}
	ledger := loadTrustedStack(installDir)
	for i := len(ledger.Sets) - 1; i >= 0; i-- { // newest first
		sets = append(sets, ledger.Sets[i].Files)
	}
	for _, s := range sets {
		if sameFiles(result, s) {
			return stackVerdict{total: len(archive)}
		}
	}
	return explainRefusal(archive, result, kept, sets)
}

// explainRefusal says why no set matched the program files on disk after the
// restore (result); kept are the installed ones among them.
func explainRefusal(archive, result map[string]string, kept []string, sets []map[string]string) stackVerdict {
	v := stackVerdict{total: len(archive)}
	var unknown []string
	for rel, sum := range archive {
		known := false
		for _, s := range sets {
			if got, ok := s[rel]; ok && got == sum {
				known = true
				break
			}
		}
		if !known {
			unknown = append(unknown, rel)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		v.reason = stackUnknownVersion
		v.detail = fmt.Sprintf("this server has never installed %d of them (%s), so they could be anything", len(unknown), summarizeIssues(unknown))
		return v
	}
	for _, s := range sets {
		if !containsFiles(s, archive) {
			continue
		}
		var missing []string
		for rel := range s {
			if _, ok := archive[rel]; !ok && result[rel] != s[rel] {
				missing = append(missing, rel)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			v.reason = stackIncompleteVersion
			v.detail = fmt.Sprintf("they are only part of a version this server ran (missing %s), so the result would mix versions or lack files", summarizeIssues(missing))
			return v
		}
		// The version is complete, but other installed binaries would stay.
		var extra []string
		for _, rel := range kept {
			if _, ok := s[rel]; !ok {
				extra = append(extra, rel)
			}
		}
		v.reason = stackMixedVersions
		v.detail = fmt.Sprintf("the installed %s is not part of that version and would stay next to it, which mixes versions", summarizeIssues(extra))
		return v
	}
	v.reason = stackMixedVersions
	v.detail = "the backup mixes program files from different versions; each one ran on this server, but never together"
	return v
}

// judgeArchiveStack is judgeStack for the program files stackRels (clean OS
// paths) of a tar.gz backup.
func judgeArchiveStack(installDir, archive string, stackRels []string) (stackVerdict, error) {
	want := make(map[string]bool, len(stackRels))
	for _, r := range stackRels {
		want[r] = true
	}
	sums, err := hashArchiveEntries(archive, want)
	if err != nil {
		return stackVerdict{}, err
	}
	files := make(map[string]string, len(stackRels))
	for _, r := range stackRels {
		files[filepath.ToSlash(r)] = sums[r]
	}
	return judgeStack(installDir, files), nil
}

// printSkippedStack reports, after a restore, that the program files were
// left out and why. The first two lines are read by the API server.
func printSkippedStack(w io.Writer, v stackVerdict, version string) {
	fmt.Fprintf(w, "stack restored: no (program files not trusted)\nreason: %s\n", v.reason)
	fmt.Fprintf(w, "\nThe backup's program files (ncc-orchestrator %s, %s) were NOT restored: %s. Only data was restored, and the installed version keeps running.\n",
		defaultStr(version, "?"), plural.Count(v.total, "file"), v.detail)
	fmt.Fprintln(w, "To run that version, install it from its release (the in-app update, or the download page). If you trust this backup, re-run v2-restore on the server with --trust-stack.")
}

// onlySkippedStackError is returned when a backup holds nothing else.
func onlySkippedStackError(v stackVerdict, version string) error {
	if v.trusted() { // only the ledger, which is never restored
		return fmt.Errorf("the backup holds nothing to restore")
	}
	return fmt.Errorf("the backup holds only program files (ncc-orchestrator %s) that are not trusted (%s: %s), so nothing was restored; install that version from its release instead, or re-run on the server with --trust-stack", defaultStr(version, "?"), v.reason, v.detail)
}

// stackTarget is the install a backup is verified against (--verify-only).
type stackTarget struct {
	installDir string
	trustStack bool
}

// addProgramFilesCheck reports whether a restore into target would put the
// backup's program files back, with the same rule as the restore. Left-out
// program files are a warning (the data still restores) unless the backup
// holds nothing else.
func (r *backupInspectReport) addProgramFilesCheck(sc backupTarScan, target stackTarget, version string) {
	if target.installDir == "" {
		return
	}
	files := map[string]string{}
	data := 0
	for _, rel := range sc.dataOrder {
		switch {
		case restoreSkipReason(filepath.FromSlash(rel)) != "":
		case backupRelIsStack(rel):
			files[rel] = sc.sums[rel]
		default:
			data++
		}
	}
	if len(files) == 0 {
		return
	}
	const name = "Program files"
	count := plural.Count(len(files), "program file")
	if target.trustStack {
		r.add(name, checkWarn, "%s (ncc-orchestrator %s) not checked because of --trust-stack; a restore with it installs them.", count, defaultStr(version, "?"))
		return
	}
	v := judgeStack(target.installDir, files)
	switch {
	case v.trusted():
		r.add(name, checkPass, "%s (ncc-orchestrator %s) match a version this server ran, so a restore puts them back.", count, defaultStr(version, "?"))
	case data == 0:
		r.add(name, checkFail, "The backup holds only program files and a restore would refuse them (%s): %s.", v.reason, v.detail)
	default:
		r.add(name, checkWarn, "A restore would leave out %s (%s): %s. Data still restores.", count, v.reason, v.detail)
	}
}
