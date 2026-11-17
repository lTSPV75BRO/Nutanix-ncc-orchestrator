package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const verifySecretValue = "hunter2-not-for-output"

// newVerifySource builds a small install with config, a file-provider secrets
// file, a clusters file, auth state and one report, and backs it up.
func newVerifyBackup(t *testing.T, opts v2BackupOptions) string {
	t.Helper()
	src := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(src, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("config.yaml", "prismCentral: pc\npassword: secret://pc-password\nsecrets-provider: file\nsecrets-file: secrets.json\nclusters-file: clusters.yaml\n")
	write("secrets.json", `{"pc-password":"`+verifySecretValue+`"}`)
	write("clusters.yaml", "- name: c1\n")
	write(".ncc-api-users.json", `{"users":[{"username":"admin"}]}`)
	write(".ncc-api-token", "tok")
	write("outputfiles/run-summary.json", `{"ok":true}`)
	opts.InstallDir = src
	if opts.OutputFile == "" {
		opts.OutputFile = filepath.Join(t.TempDir(), "b.tar.gz")
	}
	if err := runV2Backup(opts); err != nil {
		t.Fatalf("v2-backup: %v", err)
	}
	return opts.OutputFile
}

// rewriteArchive copies a plain backup, letting edit change, drop (return
// false) or add entries.
func rewriteArchive(t *testing.T, in string, edit func(h *tar.Header, body []byte) ([]byte, bool), extra map[string]string) string {
	t.Helper()
	f, err := os.Open(in)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	out := filepath.Join(t.TempDir(), "edited.tar.gz")
	of, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	ogz := gzip.NewWriter(of)
	tw := tar.NewWriter(ogz)
	put := func(name string, body []byte) {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tr)
		if edit != nil {
			var keep bool
			body, keep = edit(h, body)
			if !keep {
				continue
			}
		}
		put(h.Name, body)
	}
	for name, body := range extra {
		put(name, []byte(body))
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ogz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := of.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func checkStatus(t *testing.T, rep backupInspectReport, name string) backupCheck {
	t.Helper()
	for _, c := range rep.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %q check in %+v", name, rep.Checks)
	return backupCheck{}
}

func wantFail(t *testing.T, rep backupInspectReport, name, msgPart string) {
	t.Helper()
	if rep.OK {
		t.Fatalf("report OK, want %s to fail: %+v", name, rep.Checks)
	}
	c := checkStatus(t, rep, name)
	if c.Status != checkFail || !strings.Contains(c.Message, msgPart) {
		t.Fatalf("%s = %s %q, want fail containing %q", name, c.Status, c.Message, msgPart)
	}
}

func TestBackupVerifyGoodArchivePasses(t *testing.T) {
	arch := newVerifyBackup(t, v2BackupOptions{})
	rep := inspectBackupArchive(arch, "", "", Version)
	if !rep.OK {
		t.Fatalf("good archive failed: %+v", rep.Checks)
	}
	for _, c := range rep.Checks {
		if c.Status != checkPass {
			t.Errorf("check %s = %s (%s), want pass", c.Name, c.Status, c.Message)
		}
	}
	want := map[string]bool{".ncc-api-token": false, ".ncc-api-users.json": false, "secret pc-password": false}
	for _, s := range rep.Secrets {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("secret %q not reported; got %v", k, rep.Secrets)
		}
	}
	js, _ := json.Marshal(rep)
	if bytes.Contains(js, []byte(verifySecretValue)) {
		t.Fatal("report leaks a secret value")
	}
	var buf bytes.Buffer
	if err := runBackupVerify(&buf, arch, "", "", false); err != nil {
		t.Fatalf("runBackupVerify: %v\n%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "Result: OK") || strings.Contains(buf.String(), verifySecretValue) {
		t.Fatalf("unexpected text report:\n%s", buf.String())
	}
}

func TestBackupVerifyTamperedEntryFails(t *testing.T) {
	arch := newVerifyBackup(t, v2BackupOptions{})
	bad := rewriteArchive(t, arch, func(h *tar.Header, body []byte) ([]byte, bool) {
		if h.Name == "data/clusters.yaml" {
			return []byte("- name: evil\n"), true
		}
		return body, true
	}, nil)
	wantFail(t, inspectBackupArchive(bad, "", "", Version), "Contents", "checksum mismatch")

	// An extra file the manifest does not list is also caught.
	extra := rewriteArchive(t, arch, nil, map[string]string{"data/planted.sh": "echo hi"})
	wantFail(t, inspectBackupArchive(extra, "", "", Version), "Contents", "not listed in the manifest")

	// A truncated archive (gzip CRC/tar stream) fails the Archive check.
	raw, _ := os.ReadFile(arch)
	trunc := filepath.Join(t.TempDir(), "trunc.tar.gz")
	if err := os.WriteFile(trunc, raw[:len(raw)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	wantFail(t, inspectBackupArchive(trunc, "", "", Version), "Archive", "damaged")
}

func TestBackupVerifyPathTraversalFails(t *testing.T) {
	arch := newVerifyBackup(t, v2BackupOptions{})
	bad := rewriteArchive(t, arch, nil, map[string]string{"data/../../etc/evil": "x"})
	wantFail(t, inspectBackupArchive(bad, "", "", Version), "Paths", "unsafe path")
	odd := rewriteArchive(t, arch, nil, map[string]string{"bin/payload": "x"})
	wantFail(t, inspectBackupArchive(odd, "", "", Version), "Paths", "outside data/")
}

func TestBackupVerifyMissingManifestFails(t *testing.T) {
	arch := newVerifyBackup(t, v2BackupOptions{})
	bad := rewriteArchive(t, arch, func(h *tar.Header, body []byte) ([]byte, bool) {
		return body, h.Name != "manifest.json"
	}, nil)
	wantFail(t, inspectBackupArchive(bad, "", "", Version), "Manifest", "no manifest.json")
}

func TestBackupVerifyWrongKeyFails(t *testing.T) {
	t.Setenv("NCC_BACKUP_PASSPHRASE", "")
	arch := newVerifyBackup(t, v2BackupOptions{Encrypt: true, Passphrase: "right passphrase"})
	wantFail(t, inspectBackupArchive(arch, "", "wrong passphrase", Version), "Decryption", "wrong key")
	wantFail(t, inspectBackupArchive(arch, "", "", Version), "Decryption", "provide --passphrase")
	rep := inspectBackupArchive(arch, "", "right passphrase", Version)
	if !rep.OK || !rep.Encrypted {
		t.Fatalf("right passphrase: ok=%v encrypted=%v %+v", rep.OK, rep.Encrypted, rep.Checks)
	}
}

func TestBackupVerifyVersionCompatibility(t *testing.T) {
	var r backupInspectReport
	r.addVersionCheck("2.4.0", "3.0.0")
	r.addVersionCheck("2.5.1", "2.4.0")
	r.addVersionCheck("2.3.9", "2.4.0")
	r.addVersionCheck("", "2.4.0")
	got := []string{}
	for _, c := range r.Checks {
		got = append(got, c.Status)
	}
	if strings.Join(got, ",") != "fail,warn,pass,warn" {
		t.Fatalf("statuses = %v (%+v)", got, r.Checks)
	}
	if !strings.Contains(r.Checks[0].Message, "Different major versions") {
		t.Fatalf("major mismatch message: %q", r.Checks[0].Message)
	}

	arch := newVerifyBackup(t, v2BackupOptions{})
	wantFail(t, inspectBackupArchive(arch, "", "", "9.0.0"), "Version", "not compatible")
}

func TestBackupVerifyMissingReferencedFileFails(t *testing.T) {
	arch := newVerifyBackup(t, v2BackupOptions{})
	// Drop the clusters file and its manifest entry so only the reference check trips.
	bad := rewriteArchive(t, arch, func(h *tar.Header, body []byte) ([]byte, bool) {
		if h.Name == "manifest.json" {
			var m backupManifest
			if err := json.Unmarshal(body, &m); err != nil {
				t.Fatal(err)
			}
			files := m.Files[:0]
			for _, f := range m.Files {
				if f != "clusters.yaml" {
					files = append(files, f)
				}
			}
			m.Files = files
			delete(m.Checksums, "clusters.yaml")
			nb, _ := json.Marshal(m)
			return nb, true
		}
		return body, h.Name != "data/clusters.yaml"
	}, nil)
	wantFail(t, inspectBackupArchive(bad, "", "", Version), "Report paths", "clusters-file points at clusters.yaml")
}

// snapshotTree records every path with its size and mod time.
func snapshotTree(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		out = append(out, rel+"|"+info.ModTime().UTC().Format(time.RFC3339Nano)+"|"+info.Mode().String()+"|"+strconv.FormatInt(info.Size(), 10))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func TestBackupVerifyLeavesInstallDirUntouched(t *testing.T) {
	arch := newVerifyBackup(t, v2BackupOptions{})
	install := t.TempDir()
	if err := os.WriteFile(filepath.Join(install, "config.yaml"), []byte("prismCentral: live\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := snapshotTree(t, install)

	var buf bytes.Buffer
	if err := runV2Restore(v2RestoreOptions{InstallDir: install, InputFile: arch, VerifyOnly: true, JSON: true, Out: &buf}); err != nil {
		t.Fatalf("verify-only: %v\n%s", err, buf.String())
	}
	var rep backupInspectReport
	if err := json.Unmarshal(buf.Bytes(), &rep); err != nil || !rep.OK || len(rep.Checks) == 0 {
		t.Fatalf("JSON report: err=%v ok=%v\n%s", err, rep.OK, buf.String())
	}
	// A failing archive also leaves it alone and returns an error.
	bad := rewriteArchive(t, arch, nil, map[string]string{"data/../escape": "x"})
	buf.Reset()
	if err := runV2Restore(v2RestoreOptions{InstallDir: install, InputFile: bad, VerifyOnly: true, Out: &buf}); err == nil {
		t.Fatalf("verify-only of a bad archive returned nil:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "FAIL") {
		t.Fatalf("text report missing FAIL:\n%s", buf.String())
	}
	after := snapshotTree(t, install)
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("install dir changed:\nbefore %v\nafter  %v", before, after)
	}
	if b, _ := os.ReadFile(filepath.Join(install, "config.yaml")); string(b) != "prismCentral: live\n" {
		t.Fatalf("config.yaml changed: %q", b)
	}
}
