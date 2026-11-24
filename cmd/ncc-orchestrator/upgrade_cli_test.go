package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpgradeCheckWaitsForTheWholeRollout(t *testing.T) {
	every := upgradeCheckEvery
	upgradeCheckEvery = time.Millisecond
	defer func() { upgradeCheckEvery = every }()
	var n atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := "2.4.1"
		if i := n.Add(1); i < 12 && i%3 == 0 { // an old replica answers now and then
			v = "2.4.0"
		}
		fmt.Fprintf(w, `{"success":true,"data":{"version":%q}}`, v)
	})
	api, ui := httptest.NewServer(h), httptest.NewTLSServer(h)
	defer api.Close()
	defer ui.Close()
	if err := runUpgradeCheck(context.Background(), api.URL, "", "v2.4.1", 10*time.Second, io.Discard); err != nil {
		t.Fatal(err)
	}
	if n.Load() < 9+upgradeCheckInARow {
		t.Fatalf("stopped after %d answers", n.Load())
	}
	// The UI has a self-signed certificate.
	if err := runUpgradeCheck(context.Background(), api.URL, ui.URL, "2.4.1", 10*time.Second, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := runUpgradeCheck(context.Background(), api.URL, "", "2.5.0", 10*time.Millisecond, io.Discard); err == nil || !strings.Contains(err.Error(), "not v2.5.0") {
		t.Fatalf("err = %v", err)
	}
}

// The release script writes the manifest from helperProtocol with sed.
func TestReleaseScriptFindsTheHelperProtocol(t *testing.T) {
	src, err := os.ReadFile("upgrade_stage.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^const helperProtocol = ([0-9]+)\b`).FindSubmatch(src)
	if m == nil || string(m[1]) != fmt.Sprint(helperProtocol) {
		t.Fatalf("helperProtocol line = %q", m)
	}
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "binaryGO.txt"))
	if err != nil {
		t.Skip("no scripts folder")
	}
	format := regexp.MustCompile(`printf '([{]"schema".*[}]).n' "\$VERSION" "\$HELPER_PROTOCOL" > dist/` + regexp.QuoteMeta(upgradeManifestName)).FindSubmatch(script)
	if format == nil {
		t.Fatal("binaryGO.txt does not write the manifest")
	}
	var man upgradeManifest
	if err := json.Unmarshal([]byte(strings.NewReplacer(`"%s"`, `"2.4.0"`, "%s", string(m[1])).Replace(string(format[1]))), &man); err != nil {
		t.Fatal(err)
	}
	if man.Schema != 1 || man.Version != "2.4.0" || man.allows("2.3.0") != nil {
		t.Fatalf("%+v", man)
	}
}

func TestInterruptedUpgradeResumesAtV2Start(t *testing.T) {
	dir := t.TempDir()
	if resumeInterruptedUpgrade(dir) {
		t.Fatal("no job")
	}
	j := &helperJournal{helperRequest: helperRequest{ID: "20261124T100000Z", Action: helperActionUpgrade, Kind: kindSystemd, Target: "2.4.1", From: "2.4.0"}}
	if err := writeHelperJSON(helperJournalPath(dir), j); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(helperBinaryPath(dir)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(helperBinaryPath(dir), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if resumeInterruptedUpgrade(dir) {
		t.Fatal("systemd starts the helper itself")
	}
	t.Setenv(helperChildEnv, "1")
	if resumeInterruptedUpgrade(dir) {
		t.Fatal("the helper's own v2-start")
	}
}

func TestManualStackThatStoppedIsNotWaitedFor(t *testing.T) {
	dir := t.TempDir()
	c := &stackCheck{installDir: dir, svc: newManualService(dir), poll: time.Millisecond, window: 20 * time.Millisecond, timeout: time.Minute}
	c.manualProbes = func() error { return fmt.Errorf("the api-server is not running: %w", errServerStopped) }
	start := time.Now()
	if err := c.wait(context.Background(), time.Now(), "2.4.1"); err == nil || time.Since(start) > 10*time.Second {
		t.Fatalf("err = %v after %s", err, time.Since(start))
	}
	// A server that is still starting is waited for.
	n := 0
	c.manualProbes = func() error {
		if n++; n < 10 {
			return fmt.Errorf("it does not answer")
		}
		return nil
	}
	if err := c.wait(context.Background(), time.Now(), "2.4.1"); err != nil {
		t.Fatal(err)
	}
}
