package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckVersionSkew(t *testing.T) {
	cases := []struct {
		cur, tgt, minFrom string
		down              bool
		wantErr           string
	}{
		{"2.3.0", "2.4.0", "", false, ""},
		{"2.4.0", "2.4.0", "", false, ""},
		{"2.4.0-rc.2", "2.4.0-rc.3", "", false, ""},
		{"2.4.1", "2.4.0", "", false, "downgrade is refused"},
		{"2.4.1", "2.4.0", "", true, ""},
		{"2.2.1", "2.5.0", "2.4.0", false, "upgrade to v2.4.0 first"},
		{"2.4.0", "2.5.0", "2.4.0", false, ""},
		{"v2.3.0-0123456789abcdef", "2.4.0", "2.3.0", false, ""},
		{"smoke", "2.4.0", "2.4.0", false, ""}, // not a version: not compared
	}
	for _, c := range cases {
		_, err := checkVersionSkew(c.cur, c.tgt, c.minFrom, c.down)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%+v: unexpected %v", c, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%+v: got %v, want %q", c, err, c.wantErr)
		}
	}
}

func TestParseByteQuantity(t *testing.T) {
	for in, want := range map[string]int64{"": 0, "0": 0, "512Mi": 512 << 20, "2Gi": 2 << 30, "1.5Gi": 3 << 29, "500M": 500e6, "1024": 1024, "1Ki": 1024} {
		got, err := parseByteQuantity(in)
		if err != nil || got != want {
			t.Errorf("%q: %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"lots", "-1Gi", "12Xi", "99999999999Pi"} {
		if _, err := parseByteQuantity(in); err == nil {
			t.Errorf("%q was accepted", in)
		}
	}
}

func TestCheckFreeSpace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("free space is read on Linux and macOS")
	}
	dir := t.TempDir()
	if _, err := checkFreeSpace(dir, 1); err != nil {
		t.Fatal(err)
	}
	_, err := checkFreeSpace(dir, 1<<62)
	if err == nil || !strings.Contains(err.Error(), "minFreeSpace") {
		t.Fatalf("a threshold above the volume was accepted: %v", err)
	}
}

func TestCheckDataWritable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := checkDataWritable(dir); err != nil {
		t.Fatal(err)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*", ".ncc-upgrade-probe-*")); len(left) > 0 {
		t.Fatalf("probe files left behind: %v", left)
	}
	if _, err := checkDataWritable(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a missing volume passed")
	}
	if runtime.GOOS != "windows" && os.Getuid() != 0 {
		if err := os.Chmod(filepath.Join(dir, "config"), 0o555); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chmod(filepath.Join(dir, "config"), 0o755) }()
		if _, err := checkDataWritable(dir); err == nil || !strings.Contains(err.Error(), "cannot write") {
			t.Fatalf("a read-only folder passed: %v", err)
		}
	}
}

// fakePreflightAPI answers the endpoints the pre-upgrade checks use.
func fakePreflightAPI(t *testing.T, version string, backupStatus int, verifyOK bool, calls *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls = append(*calls, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/health":
			fmt.Fprintf(w, `{"success":true,"data":{"version":%q}}`, version)
		case "/api/v1/settings/backups":
			if r.Header.Get("X-API-Token") != "tok" {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"success":false,"error":"unauthorized"}`)
				return
			}
			if backupStatus != http.StatusOK {
				w.WriteHeader(backupStatus)
				fmt.Fprint(w, `{"success":false,"error":"snapshot failed: no space left"}`)
				return
			}
			fmt.Fprint(w, `{"success":true,"data":{"backup":{"name":"manual-1.tar.gz"}}}`)
		case "/api/v1/settings/backups/verify":
			if !verifyOK {
				w.WriteHeader(http.StatusUnprocessableEntity)
				fmt.Fprint(w, `{"success":false,"error":"checksum mismatch"}`)
				return
			}
			fmt.Fprint(w, `{"success":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
}

func preflightBase(api string) preflightOptions {
	return preflightOptions{API: api, Token: "tok", Target: "2.4.1", Backup: true, Skew: true, Permissions: true, Timeout: time.Second}
}

func TestPreflightPasses(t *testing.T) {
	var calls []string
	srv := fakePreflightAPI(t, "2.4.0", http.StatusOK, true, &calls)
	defer srv.Close()
	o := preflightBase(srv.URL)
	o.DataDir = t.TempDir()
	var out strings.Builder
	if err := runPreflight(context.Background(), o, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	for _, want := range []string{"version skew", "permissions", "backup manual-1.tar.gz taken and verified", "all pre-upgrade checks passed"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}

func TestPreflightStopsBeforeTheBackup(t *testing.T) {
	var calls []string
	srv := fakePreflightAPI(t, "2.5.0", http.StatusOK, true, &calls)
	defer srv.Close()
	o := preflightBase(srv.URL) // a downgrade 2.5.0 -> 2.4.1
	o.DataDir = t.TempDir()
	err := runPreflight(context.Background(), o, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "version skew") {
		t.Fatalf("a downgrade passed: %v", err)
	}
	for _, c := range calls {
		if strings.Contains(c, "backups") {
			t.Fatalf("a backup was taken although a check had failed: %v", calls)
		}
	}
}

func TestPreflightSpaceThreshold(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("free space is read on Linux and macOS")
	}
	var calls []string
	srv := fakePreflightAPI(t, "2.4.0", http.StatusOK, true, &calls)
	defer srv.Close()
	o := preflightBase(srv.URL)
	o.DataDir = t.TempDir()
	o.MinFreeBytes = 1 << 62
	var out strings.Builder
	err := runPreflight(context.Background(), o, &out)
	if err == nil || !strings.Contains(out.String(), "FAIL  free space") || !strings.Contains(out.String(), "nothing was changed") {
		t.Fatalf("err=%v\n%s", err, out.String())
	}
}

func TestPreflightBackupFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		verify bool
		token  string
		want   string
	}{
		"the backup fails":     {http.StatusInternalServerError, true, "tok", "no space left"},
		"it does not verify":   {http.StatusOK, false, "tok", "does not verify"},
		"the token is refused": {http.StatusOK, true, "bad", "unauthorized"},
		"no token":             {http.StatusOK, true, "", "NCC_API_TOKEN"},
	} {
		var calls []string
		srv := fakePreflightAPI(t, "2.4.0", tc.status, tc.verify, &calls)
		o := preflightBase(srv.URL)
		o.DataDir, o.Token = t.TempDir(), tc.token
		var out strings.Builder
		err := runPreflight(context.Background(), o, &out)
		srv.Close()
		if err == nil || !strings.Contains(out.String(), tc.want) {
			t.Errorf("%s: err=%v\n%s", name, err, out.String())
		}
	}
}

func TestPreflightBackupOffNeedsNoAPI(t *testing.T) {
	o := preflightOptions{API: "http://127.0.0.1:1", DataDir: t.TempDir(), Skew: true, Permissions: true, Timeout: 10 * time.Millisecond, Target: "2.4.1"}
	var out strings.Builder
	if err := runPreflight(context.Background(), o, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	o.Backup = true
	if err := runPreflight(context.Background(), o, io.Discard); err == nil {
		t.Fatal("a backup without an API passed")
	}
}

func TestPreflightExpectMustMatchTarget(t *testing.T) {
	o := preflightOptions{DataDir: t.TempDir(), Permissions: true, Target: "2.4.1", Expect: "v2.5.0"}
	if err := runPreflight(context.Background(), o, io.Discard); err == nil {
		t.Fatal("images and expected version disagree but passed")
	}
}

func TestUpgradeVerifyChecksAPIUIAndRunner(t *testing.T) {
	every := upgradeCheckEvery
	upgradeCheckEvery = time.Millisecond
	defer func() { upgradeCheckEvery = every }()
	var uiVersion atomic.Value
	uiVersion.Store("2.4.1")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"success":true,"data":{"version":"2.4.1"}}`)
	}))
	defer api.Close()
	ui := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := uiVersion.Load().(string); v != "" {
			w.Header().Set("X-NCC-UI-Version", v)
		}
		if r.URL.Path == "/api/v1/health" {
			fmt.Fprint(w, `{"success":true,"data":{"version":"2.4.1"}}`)
		}
	}))
	defer ui.Close()
	run := func(expect, runner string) error {
		return runUpgradeVerify(context.Background(), rolloutOptions{API: api.URL, UI: ui.URL, Expect: expect, Runner: runner, Timeout: 20 * time.Millisecond}, io.Discard)
	}
	if err := run("2.4.1", "2.4.1"); err != nil {
		t.Fatal(err)
	}
	if err := run("", "2.4.1"); err != nil {
		t.Fatalf("no expected version: %v", err)
	}
	if err := run("2.4.1", "2.4.0"); err == nil || !strings.Contains(err.Error(), "runner image is v2.4.0") {
		t.Fatalf("an old runner image passed: %v", err)
	}
	uiVersion.Store("2.4.0")
	if err := run("2.4.1", "2.4.1"); err == nil || !strings.Contains(err.Error(), "UI server runs v2.4.0") {
		t.Fatalf("an old UI passed: %v", err)
	}
	uiVersion.Store("")
	if err := run("2.4.1", "2.4.1"); err == nil || !strings.Contains(err.Error(), "older UI image") {
		t.Fatalf("a UI without a version passed: %v", err)
	}
}
