package main

import (
	"archive/tar"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// stackVersion is the program files of one version; each has its own content
// and its own hashed asset name, as a real build does.
func stackVersion(v string) map[string]string {
	return map[string]string{
		"bin/ncc-orchestrator":                  "orch " + v,
		"bin/ncc-api-server":                    "api " + v,
		"bin/ncc-ui-server":                     "ui " + v,
		"frontend-dist/index.html":              "index " + v,
		"frontend-dist/assets/app-" + v + ".js": "js " + v,
		"frontend-dist/favicon.ico":             "same in every version",
	}
}

// installStackVersion replaces the installed program files, as an update does.
func installStackVersion(t *testing.T, install string, files map[string]string) {
	t.Helper()
	for _, d := range []string{"bin", "frontend-dist"} {
		if err := os.RemoveAll(filepath.Join(install, d)); err != nil {
			t.Fatal(err)
		}
	}
	for rel, body := range files {
		writeInstallFile(t, install, rel, body)
	}
}

// readStack returns the installed program files and their content.
func readStack(install string) map[string]string {
	entries, _ := collectStackBackupEntries(install)
	out := map[string]string{}
	for _, e := range entries {
		b, _ := os.ReadFile(e.Abs)
		out[e.Rel] = string(b)
	}
	return out
}

// mergeFiles copies base, then applies over; an empty body removes the file.
func mergeFiles(base, over map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		if v == "" {
			delete(out, k)
		} else {
			out[k] = v
		}
	}
	return out
}

func stackArchive(t *testing.T, files map[string]string) string {
	t.Helper()
	entries := [][2]string{{"config.yaml", "x: restored\n"}}
	for rel, body := range files {
		entries = append(entries, [2]string{rel, body})
	}
	return writeRestoreTestArchive(t, entries)
}

// captureRestoreOutput returns what fn prints to stdout.
func captureRestoreOutput(t *testing.T, fn func()) string {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = f
	defer func() { os.Stdout = old }()
	fn()
	_ = f.Close()
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// SEC-07b: program files are restored only when the files on disk afterwards
// are exactly one recorded set, or the installed set. v1 and v2 ran here and
// are recorded; v2 is installed unless the case says otherwise.
func TestRestoreRequiresOneRecordedSet(t *testing.T) {
	v1, v2 := stackVersion("v1"), stackVersion("v2")
	apiV1 := map[string]string{"bin/ncc-api-server": v1["bin/ncc-api-server"]}
	noAPI := map[string]string{"bin/ncc-api-server": ""}
	cases := []struct {
		name      string
		installed map[string]string // default v2
		noLedger  bool
		archive   map[string]string
		trust     bool
		reason    string // "" = restored
	}{
		{name: "rollback to the recorded v1", archive: v1},
		{name: "the installed version", archive: v2},
		{name: "the installed version without a ledger", archive: v2, noLedger: true},
		{name: "an older version without a ledger", archive: v1, noLedger: true, reason: stackUnknownVersion},
		{name: "old api server, rest of the installed version", archive: mergeFiles(v2, apiV1), reason: stackMixedVersions},
		{name: "old UI, servers of the installed version", reason: stackMixedVersions,
			archive: mergeFiles(v2, map[string]string{
				"frontend-dist/index.html":       v1["frontend-dist/index.html"],
				"frontend-dist/assets/app-v1.js": v1["frontend-dist/assets/app-v1.js"],
				"frontend-dist/assets/app-v2.js": ""})},
		{name: "an extra asset of another version", reason: stackMixedVersions,
			archive: mergeFiles(v1, map[string]string{"frontend-dist/assets/app-v2.js": v2["frontend-dist/assets/app-v2.js"]})},
		{name: "one crafted file", archive: mergeFiles(v1, map[string]string{"bin/ncc-ui-server": "crafted"}), reason: stackUnknownVersion},
		{name: "one extra crafted asset", archive: mergeFiles(v1, map[string]string{"frontend-dist/x.js": "crafted"}), reason: stackUnknownVersion},
		{name: "crafted, with --trust-stack", archive: mergeFiles(v2, map[string]string{"bin/ncc-ui-server": "crafted"}), trust: true},
		{name: "mixed, with --trust-stack", archive: mergeFiles(v2, apiV1), trust: true},

		// Subsets: what counts is the set on disk afterwards.
		{name: "only the old api server", archive: apiV1, reason: stackIncompleteVersion},
		{name: "only the installed api server, so the frontend would be lost", reason: stackIncompleteVersion,
			archive: map[string]string{"bin/ncc-api-server": v2["bin/ncc-api-server"]}},
		{name: "only the old frontend", reason: stackIncompleteVersion,
			archive: mergeFiles(v1, map[string]string{"bin/ncc-orchestrator": "", "bin/ncc-api-server": "", "bin/ncc-ui-server": ""})},
		{name: "v1 without its api server, v2 installed", archive: mergeFiles(v1, noAPI), reason: stackIncompleteVersion},
		{name: "v1 without its api server, which is the installed one", installed: mergeFiles(v2, apiV1), archive: mergeFiles(v1, noAPI)},
		{name: "v1 without an asset", archive: mergeFiles(v1, map[string]string{"frontend-dist/favicon.ico": ""}), reason: stackIncompleteVersion},

		// What stays on disk.
		{name: "an installed binary v1 did not have would stay", archive: v1, reason: stackMixedVersions,
			installed: mergeFiles(v2, map[string]string{"ncc-orchestrator": "root orch v2"})},
		{name: "stale installed assets go with the folder", archive: v1,
			installed: mergeFiles(v2, map[string]string{"frontend-dist/assets/app-v0.js": "stale"})},
		{name: "nothing installed, recorded version", installed: map[string]string{}, archive: v1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			install := t.TempDir()
			writeInstallFile(t, install, "config.yaml", "x: before\n")
			if !c.noLedger {
				for _, v := range []map[string]string{v1, v2} {
					installStackVersion(t, install, v)
					if err := recordTrustedStack(install, "test"); err != nil {
						t.Fatal(err)
					}
				}
			}
			installed := c.installed
			if installed == nil {
				installed = v2
			}
			installStackVersion(t, install, installed)
			archive := stackArchive(t, c.archive)

			// --verify-only says beforehand what the restore will do.
			rep := inspectBackupArchiveFor(archive, "", "", "2.2.1", stackTarget{installDir: install, trustStack: c.trust})
			check := checkStatus(t, rep, "Program files")
			wantStatus := checkPass
			if c.reason != "" || c.trust {
				wantStatus = checkWarn
			}
			if check.Status != wantStatus || !strings.Contains(check.Message, c.reason) {
				t.Errorf("verify: %+v, want %s %q", check, wantStatus, c.reason)
			}

			before := readStack(install)
			out := captureRestoreOutput(t, func() {
				if err := restoreForTest(t, install, archive, c.trust); err != nil {
					t.Error(err)
				}
			})
			if got := readInstallFile(t, install, "config.yaml"); got != "x: restored\n" {
				t.Errorf("data must restore in every case: %q", got)
			}
			after := readStack(install)
			if c.reason != "" {
				if !sameFiles(before, after) {
					t.Errorf("program files changed: %v", after)
				}
				if !strings.Contains(out, "stack restored: no (program files not trusted)\nreason: "+c.reason+"\n") {
					t.Errorf("output lacks the reason %q:\n%s", c.reason, out)
				}
				return
			}
			if !strings.Contains(out, "stack restored: yes") {
				t.Errorf("not restored:\n%s", out)
			}
			want := mergeFiles(c.archive, nil)
			for rel, body := range installed {
				if _, ok := want[rel]; !ok && !strings.HasPrefix(rel, "frontend-dist/") {
					want[rel] = body
				}
			}
			if !sameFiles(after, want) {
				t.Errorf("on disk: %v\nwant: %v", after, want)
			}
		})
	}
}

// The whole flow: v1 runs, the pre-update backup is taken, v2 is installed and
// started; the rollback point puts v1 back, and v2's own backup v2.
func TestRestoreRollbackFlow(t *testing.T) {
	install := t.TempDir()
	v1, v2 := stackVersion("v1"), stackVersion("v2")
	writeInstallFile(t, install, "config.yaml", "x: 1\n")
	backup := func() string {
		out := filepath.Join(t.TempDir(), "b.tar.gz")
		if err := runV2Backup(v2BackupOptions{InstallDir: install, OutputFile: out, IncludeStack: true}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	installStackVersion(t, install, v1)
	b1 := backup()
	installStackVersion(t, install, v2)
	recordTrustedStackQuiet(install, "upgrade")
	b2 := backup()
	for i, step := range []struct {
		archive string
		want    map[string]string
	}{{b1, v1}, {b2, v2}, {b2, v2}, {b1, v1}} {
		if err := restoreForTest(t, install, step.archive, false); err != nil {
			t.Fatal(err)
		}
		if got := readStack(install); !sameFiles(got, step.want) {
			t.Fatalf("step %d: on disk %v, want %v", i, got, step.want)
		}
	}
}

// A ledger written by an earlier 2.4.0 build (the same format, one set per
// version) keeps working; a damaged one trusts only the installed files.
func TestRestoreLegacyLedger(t *testing.T) {
	v1, v2 := stackVersion("v1"), stackVersion("v2")
	for _, damaged := range []bool{false, true} {
		install := t.TempDir()
		installStackVersion(t, install, v1)
		var files []string
		for rel, sum := range installedStackHashes(install) {
			files = append(files, fmt.Sprintf("%q: %q", rel, sum))
		}
		ledger := `{"sets":[{"recorded_at":"2026-10-01T00:00:00Z","source":"start","files":{` + strings.Join(files, ",") + `}}]}`
		if damaged {
			ledger = ledger[:len(ledger)-1]
		}
		writeInstallFile(t, install, trustedStackFile, ledger)
		installStackVersion(t, install, v2)
		if err := restoreForTest(t, install, stackArchive(t, v1), false); err != nil {
			t.Fatal(err)
		}
		want := v1
		if damaged {
			want = v2
		}
		if got := readStack(install); !sameFiles(got, want) {
			t.Fatalf("damaged=%v: on disk %v, want %v", damaged, got, want)
		}
	}
}

// A backup that holds only refused program files fails, in the check too.
func TestRestoreOnlyMixedStackFails(t *testing.T) {
	install := t.TempDir()
	v1, v2 := stackVersion("v1"), stackVersion("v2")
	for _, v := range []map[string]string{v1, v2} {
		installStackVersion(t, install, v)
		if err := recordTrustedStack(install, "test"); err != nil {
			t.Fatal(err)
		}
	}
	var entries [][2]string
	for rel, body := range mergeFiles(v2, map[string]string{"bin/ncc-api-server": v1["bin/ncc-api-server"]}) {
		entries = append(entries, [2]string{rel, body})
	}
	archive := writeRestoreTestArchive(t, entries)
	err := restoreForTest(t, install, archive, false)
	if err == nil || !strings.Contains(err.Error(), stackMixedVersions) {
		t.Fatalf("err = %v", err)
	}
	rep := inspectBackupArchiveFor(archive, "", "", "2.2.1", stackTarget{installDir: install})
	if c := checkStatus(t, rep, "Program files"); c.Status != checkFail || rep.OK {
		t.Fatalf("verify: %+v", c)
	}
	if got := readStack(install); !sameFiles(got, v2) {
		t.Fatalf("program files changed: %v", got)
	}
}

// Another spelling of a program file's path (which Windows and macOS open as
// the same file) must not slip past the check as data.
func TestRestoreRefusesProgramFileAliases(t *testing.T) {
	v1 := stackVersion("v1")
	aliases := []string{
		"bin/NCC-API-SERVER", "BIN/ncc-api-server", "bin/ncc-api-server.", "bin /ncc-ui-server", "Frontend-dist/index.html",
		"frontend-dist./evil.js", "bin/ncc-api-server::$DATA", "bin/ncc-api-server-" + runtime.GOOS + "-" + runtime.GOARCH,
	}
	if runtime.GOOS == "windows" {
		aliases = append(aliases, "FRONTE~1/index.html")
	}
	for _, alias := range aliases {
		install := t.TempDir()
		writeInstallFile(t, install, "config.yaml", "x: 1\n")
		installStackVersion(t, install, v1)
		archive := stackArchive(t, mergeFiles(v1, map[string]string{alias: "crafted"}))
		if err := restoreForTest(t, install, archive, false); err != nil {
			t.Fatalf("%q: %v", alias, err)
		}
		if got := readStack(install); !sameFiles(got, v1) {
			t.Errorf("%q changed the program files: %v", alias, got)
		}
		if got := readInstallFile(t, install, alias); got == "crafted" {
			t.Errorf("%q was restored as data", alias)
		}
	}
	for _, rel := range []string{"config.yaml", "bin/readme.txt", "ncc-orchestrator-clusters.yaml", "logs/ncc-api-server", "outputfiles/frontend-dist.html", "PROD~1.yaml"} {
		if backupRelIsStack(filepath.FromSlash(rel)) {
			t.Errorf("%q is data", rel)
		}
	}
	for _, rel := range []string{trustedStackFile + ".", strings.ToUpper(trustedStackFile), trustedStackFile + "::$DATA"} {
		if !isTrustedStackLedger(rel) {
			t.Errorf("%q can name the ledger", rel)
		}
	}
}

// A manifest that claims a stack the archive does not hold must not clear the
// live frontend.
func TestRestoreManifestClaimKeepsFrontend(t *testing.T) {
	install := t.TempDir()
	writeInstallFile(t, install, "config.yaml", "x: 1\n")
	installStackVersion(t, install, stackVersion("v1"))
	archive := rewriteArchive(t, writeRestoreTestArchive(t, [][2]string{{"config.yaml", "x: 2\n"}}), func(h *tar.Header, body []byte) ([]byte, bool) {
		if h.Name == "manifest.json" {
			body = []byte(`{"tool":"ncc-orchestrator","version":"2.2.1","includes_stack":true}`)
		}
		return body, true
	}, nil)
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	if got := readStack(install); !sameFiles(got, stackVersion("v1")) {
		t.Fatalf("program files changed: %v", got)
	}
}

// The rollback folder of an upgrade in progress is noted in the ledger, so a
// restarted supervisor puts back exactly that, whatever the binaries are named
// (here a legacy platform-suffixed one that no recorded set holds).
func TestUpgradeSnapshotSurvivesSupervisorRestart(t *testing.T) {
	f := newFakeStack(t)
	suffixed := "bin/ncc-ui-server-" + runtime.GOOS + "-" + runtime.GOARCH
	writeInstallFile(t, f.dir, suffixed, "old suffixed")
	u := f.upgrader()
	if err := u.snapshot(); err != nil {
		t.Fatal(err)
	}
	if len(loadTrustedStack(f.dir).UpgradeSnapshot) == 0 {
		t.Fatal("snapshot not noted")
	}
	recordTrustedStackQuiet(f.dir, "backup") // keeps the note
	writeInstallFile(t, f.dir, "bin/ncc-api-server", "new")
	writeInstallFile(t, f.dir, suffixed, "new suffixed")

	restarted := f.upgrader()
	restarted.job = &upgradeJob{ID: "r", Phase: upgRollingBack, Error: "unhealthy", StartedAt: nowRFC3339()}
	restarted.resume(context.Background())
	if restarted.job.Phase != upgRolledBack {
		t.Fatalf("job = %+v", restarted.job)
	}
	if a, b := readInstallFile(t, f.dir, "bin/ncc-api-server"), readInstallFile(t, f.dir, suffixed); a != "old ncc-api-server" || b != "old suffixed" {
		t.Fatalf("not put back: %q %q", a, b)
	}
	if len(loadTrustedStack(f.dir).UpgradeSnapshot) != 0 {
		t.Fatal("note left behind")
	}
}

// The same process goes by what it saved, even when an older version rewrote
// the ledger without the note; a changed copy is refused.
func TestUpgradeSnapshotInMemory(t *testing.T) {
	for _, tampered := range []bool{false, true} {
		f := newFakeStack(t)
		u := f.upgrader()
		if err := u.snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(f.dir, trustedStackFile)); err != nil {
			t.Fatal(err)
		}
		writeInstallFile(t, f.dir, "bin/ncc-api-server", "new")
		if tampered {
			writeInstallFile(t, f.dir, "run/"+upgradeSnapshotDir+"/bin/ncc-api-server", "crafted")
		}
		u.job = &upgradeJob{ID: "m", StartedAt: nowRFC3339()}
		u.rollback(context.Background(), "unhealthy")
		wantPhase, wantAPI := upgRolledBack, "old ncc-api-server"
		if tampered {
			wantPhase, wantAPI = upgFailed, "new"
		}
		if got := readInstallFile(t, f.dir, "bin/ncc-api-server"); u.job.Phase != wantPhase || got != wantAPI {
			t.Fatalf("tampered=%v: phase %s, api %q", tampered, u.job.Phase, got)
		}
	}
}

func TestRestoreFileMode(t *testing.T) {
	for _, c := range []struct {
		rel    string
		stored int64
		want   os.FileMode
	}{
		{"outputfiles/report.html", 0o777, 0o666}, {"config.yaml", 0o600, 0o600}, {"config.yaml", 0, 0o644},
		{".ncc-api-token", 0o777, 0o600}, {"bin/ncc-api-server", 0o777, 0o755}, {"bin/ncc-api-server", 0o600, 0o711},
		{"frontend-dist/index.html", 0o777, 0o644},
	} {
		if got := restoreFileMode(filepath.FromSlash(c.rel), c.stored); got != c.want {
			t.Errorf("%s %o: got %o, want %o", c.rel, c.stored, got, c.want)
		}
	}
}

func TestLogFileMustNotBeRunnable(t *testing.T) {
	for p, bad := range map[string]bool{"logs/ncc-runner.log": false, "logs/ncc-sched-x.sh": true, "bin/ncc-api-server.exe": true, "logs/x.SH": true, "runner": false} {
		if err := validateLogFilePath("log-file", p); (err != nil) != bad {
			t.Errorf("%s: %v", p, err)
		}
	}
}

func TestRestoreKeepsAppStateKeys(t *testing.T) {
	for rel, skipped := range map[string]bool{
		".ncc-api-saml.key": false, "config/.ncc-api-saml.pem": false,
		"ui.key": true, "tls/.ncc-api-x.key": true, ".ncc-api-x.sh": true,
	} {
		if got := restoreSkipReason(filepath.FromSlash(rel)); (got != "") != skipped {
			t.Errorf("%s: reason %q", rel, got)
		}
	}
}
