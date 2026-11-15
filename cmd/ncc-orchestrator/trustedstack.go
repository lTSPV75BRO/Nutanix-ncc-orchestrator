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
// Trusting files this host already ran adds no new code. On restore, the
// stack is put back only when every one of its files matches the ledger or
// the file installed now; otherwise only data is restored, and the operator
// installs that version separately. The ledger is never backed up and never
// restored from an archive. `v2-restore --trust-stack`, only available on the
// server's command line (the API never passes it), skips the check.

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
	"sort"
	"strings"
	"time"
)

const (
	trustedStackFile = ".ncc-trusted-stack.json"
	// trustedStackMaxSets bounds the ledger: about the last 20 versions.
	trustedStackMaxSets = 20
)

type trustedStackSet struct {
	Version    string            `json:"version,omitempty"`
	RecordedAt string            `json:"recorded_at"`
	Source     string            `json:"source,omitempty"`
	Files      map[string]string `json:"files"` // slash rel path → sha256 hex
}

type trustedStackLedger struct {
	Sets []trustedStackSet `json:"sets"`
}

// isTrustedStackLedger reports whether rel names the ledger itself.
func isTrustedStackLedger(rel string) bool {
	return strings.EqualFold(filepath.ToSlash(filepath.Clean(rel)), trustedStackFile)
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

// installedStackHashes hashes the stack files installed now.
func installedStackHashes(installDir string) map[string]string {
	entries, _ := collectStackBackupEntries(installDir)
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		if sum, err := sha256File(e.Abs); err == nil {
			out[filepath.ToSlash(e.Rel)] = sum
		}
	}
	return out
}

func loadTrustedStack(installDir string) trustedStackLedger {
	var l trustedStackLedger
	b, err := os.ReadFile(filepath.Join(installDir, trustedStackFile))
	if err != nil {
		return l
	}
	_ = json.Unmarshal(b, &l)
	return l
}

func sameFiles(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// recordTrustedStack adds the installed stack to the ledger. Best-effort:
// a failure only means a later restore of this version restores data only.
func recordTrustedStack(installDir, source string) error {
	files := installedStackHashes(installDir)
	if len(files) == 0 {
		return nil
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
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(installDir, trustedStackFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// recordTrustedStackQuiet is recordTrustedStack for start/backup paths that
// must never fail because of it.
func recordTrustedStackQuiet(installDir, source string) {
	if err := recordTrustedStack(installDir, source); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not record the installed program files as trusted: %v\n", err)
	}
}

// trusts reports whether rel with this hash was recorded, or is the file
// installed now.
func (l trustedStackLedger) trusts(rel, sum string, installed map[string]string) bool {
	rel = filepath.ToSlash(rel)
	if installed[rel] == sum {
		return true
	}
	for _, s := range l.Sets {
		if s.Files[rel] == sum {
			return true
		}
	}
	return false
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

// untrustedStackFiles returns the stack files in the archive that match
// neither the ledger nor the installed files, sorted. Every rel must be a
// stack file.
func untrustedStackFiles(installDir, archive string, stackRels []string) ([]string, error) {
	want := make(map[string]bool, len(stackRels))
	for _, r := range stackRels {
		want[r] = true
	}
	sums, err := hashArchiveEntries(archive, want)
	if err != nil {
		return nil, err
	}
	ledger := loadTrustedStack(installDir)
	installed := installedStackHashes(installDir)
	var bad []string
	for _, r := range stackRels {
		sum, ok := sums[r]
		if !ok || !ledger.trusts(r, sum, installed) {
			bad = append(bad, filepath.ToSlash(r))
		}
	}
	sort.Strings(bad)
	return bad, nil
}
