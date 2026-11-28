package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// SEC-19: files that are run, or decide what is run, never come from a backup.
func TestRestoreSkipsProtectedPaths(t *testing.T) {
	bad := []string{
		"run/upgrade-rollback/bin/ncc-api-server", "run/v2-upgrade-state.json", "run/v2-supervisor-upgrade.request", "run/v2-api.pid",
		"RUN/v2-upgrade-state.json",
		"bin/other-tool", "bin/version.dll", "start-api.sh", "start-ui.cmd", "apply-ncc-update.cmd",
		"logs/ncc-sched-ncc-orchestrator.sh", "logs/ncc-scheduler.sh", "Logs/NCC-SCHEDULER.SH", "logs/units/ncc.service",
		"tls/ui.crt", "tls/ui.key", "certs/ui.key", "backups/pre-update-x.tar.gz",
		"cmd/ncc-orchestrator/main.go", "frontend/dist/index.html",
		".ncc-v2/bin/ncc-api-server", ".ncc-v2/frontend-dist/index.html",
	}
	if runtime.GOOS == "windows" {
		bad = append(bad, "run./v2-upgrade-state.json", "start-api.cmd.", "logs/x.sh::$DATA", "outputfiles/COM1")
	}
	for _, rel := range bad {
		install := t.TempDir()
		writeInstallFile(t, install, "config.yaml", "x: 1\n")
		archive := writeRestoreTestArchive(t, [][2]string{{"config.yaml", "x: 2\n"}, {rel, "crafted"}})
		if err := restoreForTest(t, install, archive, true); err != nil { // --trust-stack does not open these
			t.Errorf("%s: %v", rel, err)
		}
		if got := readInstallFile(t, install, rel); got != "<missing>" {
			t.Errorf("%s was restored: %q", rel, got)
		}
		if got := readInstallFile(t, install, "config.yaml"); got != "x: 2\n" {
			t.Errorf("%s: data should still restore, config is %q", rel, got)
		}
	}
}

// Everything a real backup holds still restores, host layout and Kubernetes
// volume layout alike.
func TestRestoreKeepsEverythingABackupHolds(t *testing.T) {
	src := t.TempDir()
	files := map[string]string{
		"config.yaml":                                  "clusters-file: lists/clusters.txt\nexclude-alert-titles-file: ncc-orchestrator-exclude\nsecrets-file: secrets/ncc.json\noutput-dir-filtered: outputfiles\n",
		"lists/clusters.txt":                           "10.0.0.1\n",
		"ncc-orchestrator-exclude":                     "title\n",
		"secrets/ncc.json":                             "{}",
		"outputfiles/run-summary.json":                 "{}",
		"outputfiles/10.0.0.1.html":                    "<html></html>",
		"outputfiles/10.0.0.1.csv":                     "a,b\n",
		"outputfiles/10.0.0.1.sarif":                   "{}",
		"outputfiles/report.md":                        "# r\n",
		"outputfiles/ncc.prom":                         "x 1\n",
		".ncc-api-users.json":                          `{"users":[]}`,
		".ncc-api-token":                               "tok",
		".ncc-initial-admin-password":                  "pw",
		".ncc-api-schedule.json":                       "{}",
		".ncc-api-notifications.json":                  "{}",
		".ncc-api-backup-schedule.json":                "{}",
		".ncc-api-alert-lifecycle.json":                "{}",
		".ncc-v2-start.json":                           `{"api_auth_mode":"hybrid"}`,
		"config/config.yaml":                           "x: 1\n",
		"config/.ncc-api-schedule.json":                "{}",
		"auth/.ncc-api-token":                          "tok",
		"auth/.ncc-initial-admin-password":             "pw",
		"logs/ncc-audit.log":                           "{}\n",
		"logs/.ncc-api-notifications.json":             "{}",
		"bin/ncc-api-server":                           "api",
		"bin/ncc-orchestrator":                         "orch",
		"bin/ncc-ui-server":                            "ui",
		"frontend-dist/assets/run/vendor-abc.mjs":      "js",
		"frontend-dist/index.html":                     "index",
		"frontend-dist/.well-known/appspecific/a.json": "{}",
	}
	for rel, body := range files {
		writeInstallFile(t, src, rel, body)
	}
	out := filepath.Join(t.TempDir(), "b.tar.gz")
	if err := runV2Backup(v2BackupOptions{InstallDir: src, OutputFile: out, IncludeStack: true}); err != nil {
		t.Fatal(err)
	}
	sc := scanBackupTar(out)
	if len(sc.dataOrder) != len(files) {
		t.Fatalf("the backup holds %d of %d files: %v", len(sc.dataOrder), len(files), sc.dataOrder)
	}
	// Into the same install, emptied of everything but the ledger.
	ledger := readInstallFile(t, src, trustedStackFile)
	entries, _ := os.ReadDir(src)
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(src, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	writeInstallFile(t, src, trustedStackFile, ledger)
	if err := restoreForTest(t, src, out, false); err != nil {
		t.Fatal(err)
	}
	for rel := range files {
		if readInstallFile(t, src, rel) == "<missing>" {
			t.Errorf("%s was not restored", rel)
		}
	}
}

// A file entry named like a folder must not replace (and delete) the folder.
func TestRestoreKeepsFolders(t *testing.T) {
	for _, rel := range []string{"bin", "outputfiles", "logs"} {
		install := t.TempDir()
		writeInstallFile(t, install, "config.yaml", "x: 1\n")
		writeInstallFile(t, install, rel+"/keep", "kept")
		archive := writeRestoreTestArchive(t, [][2]string{{"config.yaml", "x: 2\n"}, {rel, "not a folder"}})
		if err := restoreForTest(t, install, archive, false); err != nil {
			t.Fatal(err)
		}
		if got := readInstallFile(t, install, rel+"/keep"); got != "kept" {
			t.Errorf("%s/ was replaced: %q", rel, got)
		}
	}
}

// Links, devices and other special members are refused, not written as empty
// files.
func TestRestoreRefusesSpecialMembers(t *testing.T) {
	for _, typ := range []byte{tar.TypeSymlink, tar.TypeLink, tar.TypeFifo, tar.TypeChar} {
		path := filepath.Join(t.TempDir(), "b.tar.gz")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		gz := gzip.NewWriter(f)
		tw := tar.NewWriter(gz)
		m := `{"tool":"ncc-orchestrator","version":"2.2.1"}`
		for _, h := range []*tar.Header{
			{Name: "manifest.json", Mode: 0o600, Size: int64(len(m)), Typeflag: tar.TypeReg},
			{Name: "data/config.yaml", Mode: 0o600, Typeflag: typ, Linkname: "/etc/passwd"},
		} {
			if err := tw.WriteHeader(h); err != nil {
				t.Fatal(err)
			}
			if h.Size > 0 {
				_, _ = io.WriteString(tw, m)
			}
		}
		_, _, _ = tw.Close(), gz.Close(), f.Close()
		install := t.TempDir()
		writeInstallFile(t, install, "config.yaml", "x: 1\n")
		if err := restoreForTest(t, install, path, false); err == nil {
			t.Errorf("type %q accepted", typ)
		}
		if got := readInstallFile(t, install, "config.yaml"); got != "x: 1\n" {
			t.Errorf("type %q changed config.yaml: %q", typ, got)
		}
	}
}

// A setting this host left empty is not filled from a backup either: that
// would point the UI at another backend, allow another origin or turn HTTPS
// off at the next start.
func TestRestoreKeepsHostListenerSettings(t *testing.T) {
	install := t.TempDir()
	write := func(rel string, st v2StartState) string {
		b, _ := json.Marshal(st)
		writeInstallFile(t, install, rel, string(b))
		return string(b)
	}
	host := v2StartState{APIListen: ":8081", UIListen: ":8080", APIAuthMode: "token"}
	write(v2StartStateFile, host)
	foreign := v2StartState{APIListen: ":8081", UIListen: ":8080", UIBackendURL: "https://evil.example", UIAllowedOrigins: "https://evil.example",
		APICORSOrigins: "https://evil.example", APIAdvertiseURL: "https://evil.example", UITLSCertFile: "x/ui.crt", UITLSKeyFile: "x/ui.key",
		UIInsecureHTTP: true, UIBackendInsecureSkipVerify: true, APICookieInsecure: true, APIOnly: true, APIAuthMode: "hybrid", APIRateLimitPerMinute: 7}
	b, _ := json.Marshal(foreign)
	archive := writeRestoreTestArchive(t, [][2]string{{v2StartStateFile, string(b)}})
	if err := restoreForTest(t, install, archive, false); err != nil {
		t.Fatal(err)
	}
	got, ok := loadV2StartState(install)
	want := host
	want.APIAuthMode, want.APIRateLimitPerMinute = "hybrid", 7 // not listener settings: from the backup
	if !ok || got != want {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}

	// Migration to a host that never started: the backup's settings apply.
	fresh := t.TempDir()
	writeInstallFile(t, fresh, "config.yaml", "x: 1\n")
	if err := restoreForTest(t, fresh, archive, false); err != nil {
		t.Fatal(err)
	}
	if got, ok := loadV2StartState(fresh); !ok || got != foreign {
		t.Fatalf("fresh host: %+v", got)
	}
}

// The scheduler's log path (which a restored schedule carries) must not add
// lines to the unit or crontab, or append to a script.
func TestCreateScheduleRefusesUnsafeLogPath(t *testing.T) {
	for _, p := range []string{"logs/a\nExecStartPre=/bin/touch /tmp/x\n/b.log", "logs/a\r.log", "logs/ncc-sched-other.sh", "bin/ncc-api-server", "x/evil.service"} {
		cmd := newRootCmd()
		cmd.SetArgs([]string{"create-schedule", "--type", "cron", "--every", "4h", "--log-path", p})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "log-path") {
			t.Errorf("%q: err = %v", p, err)
		}
	}
}

// A planted rollback folder plus a "rolling_back" state must not be put back.
func TestSupervisorRefusesPlantedRollback(t *testing.T) {
	plant := func(f *fakeStack, api string) {
		for rel, body := range map[string]string{
			"bin/ncc-orchestrator": "old ncc-orchestrator", "bin/ncc-ui-server": "old ncc-ui-server", "bin/ncc-api-server": api,
			"frontend-dist/index.html": "old ui",
		} {
			p := filepath.Join(f.dir, "run", upgradeSnapshotDir, filepath.FromSlash(rel))
			_ = os.MkdirAll(filepath.Dir(p), 0o755)
			_ = os.WriteFile(p, []byte(body), 0o755)
		}
	}
	for _, c := range []struct {
		name, api, extra, installed string
		wantPhase, wantAPI          string
	}{
		{"foreign api server", "crafted", "", "new", upgFailed, "new"},
		{"foreign extra binary", "old ncc-api-server", "bin/evil.dll", "new", upgFailed, "new"},
		{"foreign UI file", "old ncc-api-server", "frontend-dist/evil.js", "new", upgFailed, "new"},
		{"foreign supervisor binary", "old ncc-api-server", "self/ncc-orchestrator", "new", upgFailed, "new"},
		// Recorded when v1 started, and the supervisor restarted mid-rollback.
		{"the recorded version", "old ncc-api-server", "", "new", upgRolledBack, "old ncc-api-server"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeStack(t)
			if err := recordTrustedStack(f.dir, "start"); err != nil {
				t.Fatal(err)
			}
			writeInstallFile(t, f.dir, "bin/ncc-api-server", c.installed)
			plant(f, c.api)
			if c.extra != "" {
				writeInstallFile(t, f.dir, "run/"+upgradeSnapshotDir+"/"+c.extra, "crafted")
			}
			b, _ := json.Marshal(upgradeJob{ID: "p", Phase: upgRollingBack, Error: "planted", StartedAt: nowRFC3339()})
			if err := os.WriteFile(upgradeStatePath(f.dir), b, 0o644); err != nil {
				t.Fatal(err)
			}
			u := f.upgrader()
			j := readJob(t, f.dir)
			u.job = j
			u.resume(context.Background())
			if j.Phase != c.wantPhase {
				t.Errorf("phase = %s (%s)", j.Phase, j.Message)
			}
			if got := readInstallFile(t, f.dir, "bin/ncc-api-server"); got != c.wantAPI {
				t.Errorf("api server = %q, want %q", got, c.wantAPI)
			}
			if c.extra != "" && readInstallFile(t, f.dir, c.extra) != "<missing>" {
				t.Errorf("%s was installed", c.extra)
			}
		})
	}
}

// A cron spec ends up at the start of a crontab line, so a sixth field would be
// the command that runs (SEC-19: a restored schedule can carry any text).
func TestValidateCronSpec(t *testing.T) {
	for _, ok := range []string{"15 2 * * *", "*/5 0-23/2 1,15 JAN mon-fri", "@daily", "@reboot", " 0 4 * * 0 "} {
		if err := validateCronSpec(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"* * * * * /tmp/x", "* * * * *\n* * * * * x", "* * * * * reboot", "@daily x", "@weird", "* * * *", "* * * * $(id)", "0 4 * * 0%"} {
		if validateCronSpec(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestCreateScheduleRefusesCronWithCommand(t *testing.T) {
	cmd := newRootCmd()
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"create-schedule", "--type", "cron", "--print-only", "--cron", "* * * * * reboot"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "five fields") {
		t.Fatalf("err = %v", err)
	}
}
