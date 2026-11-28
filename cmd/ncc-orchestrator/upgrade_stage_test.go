package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// playHelper makes the service's wake-up act as the installed helper would.
func (h *testHost) playHelper(probeErr string) {
	h.svc.wake = func() {
		var p helperProbe
		if readHelperJSON(helperProbePath(h.dir), &p) == nil {
			_ = os.Remove(helperProbePath(h.dir))
			_ = writeHelperJSON(helperResultPath(h.dir), helperProbeResult{Nonce: p.Nonce, OK: probeErr == "", Error: probeErr,
				Version: fakeVersion(helperBinaryPath(h.dir))})
			return
		}
		if j, ok, err := takeHelperJob(h.dir); ok && err == nil {
			e := h.resume(j)
			e.version = fakeVersion(helperBinaryPath(h.dir))
			e.run(context.Background())
		}
	}
}

func (h *testHost) stageOne() *stageOne {
	logf := func(f string, a ...any) { h.t.Logf(f, a...) }
	return &stageOne{installDir: h.dir, svc: h.svc, logf: logf, from: "2.3.0", runCmd: fakeRun,
		log:          newJobLog(h.dir, logf, newJob("job1", helperActionUpgrade, "", "", "test")),
		probeTimeout: 200 * time.Millisecond, takeTimeout: 200 * time.Millisecond, poll: 5 * time.Millisecond}
}

// bundle writes a release archive and its checksums to a folder.
func writeBundle(t *testing.T, version string) string {
	t.Helper()
	name, body, sums := releaseArchive(t, version, nil)
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, name), string(body))
	writeTestFile(t, filepath.Join(dir, stagedChecksums), string(sums))
	return filepath.Join(dir, name)
}

func TestUpgradeFromBundle(t *testing.T) {
	h := newTestHost(t)
	h.playHelper("")
	s := h.stageOne()
	s.bundle = writeBundle(t, "2.4.0")
	if err := s.run(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	job := h.wantJob(upgDone)
	h.wantVersion("2.4.0", newUnit)
	if fakeVersion(helperBinaryPath(h.dir)) != "2.4.0" || h.svc.helperDef != helperBinaryPath(h.dir) {
		t.Error("the helper was not installed")
	}
	// One list of steps from both stages.
	var phases []string
	for _, st := range job.Steps {
		phases = append(phases, st.Phase)
	}
	got := strings.Join(phases, " ")
	want := "preflight downloading helper_install helper_check handover staging saving stopping swapping migrating migrating starting checking done"
	if got != want {
		t.Errorf("steps\n got %s\nwant %s", got, want)
	}
}

// Until the hand-over nothing of the stack may change, whatever goes wrong.
func TestUpgradePreparationLeavesTheStackAlone(t *testing.T) {
	cases := map[string]struct {
		spoil func(h *testHost, s *stageOne)
		want  string
	}{
		"helper cannot control the service": {func(h *testHost, _ *stageOne) { h.playHelper("no right to write units") }, "no right to write units"},
		"helper does not answer":            {func(h *testHost, _ *stageOne) { h.svc.wake = nil }, "did not answer"},
		"helper does not take the job": {func(h *testHost, _ *stageOne) {
			h.playHelper("")
			wake := h.svc.wake
			h.svc.wake = func() {
				if fileExists(helperProbePath(h.dir)) {
					wake()
				}
			}
		}, "did not start"},
		"checksum": {func(h *testHost, s *stageOne) {
			h.playHelper("")
			writeTestFile(h.t, filepath.Join(filepath.Dir(s.bundle), stagedChecksums), strings.Repeat("0", 64)+"  "+filepath.Base(s.bundle)+"\n")
		}, "checksum"},
		"no checksums": {func(h *testHost, s *stageOne) { _ = os.Remove(filepath.Join(filepath.Dir(s.bundle), stagedChecksums)) }, "checksums.txt"},
		"older":        {func(h *testHost, s *stageOne) { s.bundle = writeBundle(h.t, "2.2.0") }, "older"},
		"major":        {func(h *testHost, s *stageOne) { s.bundle = writeBundle(h.t, "3.0.0") }, "major"},
		"not root":     {func(h *testHost, s *stageOne) { h.svc.privErr = errors.New("needs root") }, "needs root"},
		"job in progress": {func(h *testHost, s *stageOne) {
			_ = writeHelperJSON(helperJournalPath(h.dir), helperJournal{helperRequest: helperRequest{ID: "other", Action: helperActionAdopt}})
			h.t.Cleanup(func() { _ = os.Remove(helperJournalPath(h.dir)) })
		}, "in progress"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newTestHost(t)
			s := h.stageOne()
			s.bundle = writeBundle(t, "2.4.0")
			tc.spoil(h, s)
			err := s.run(context.Background(), "")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			h.wantVersion("2.3.0", oldUnit)
			job, _ := readUpgradeJob(h.dir)
			if job == nil || job.Phase != upgFailed || !strings.Contains(job.Message, "Nothing was changed") {
				t.Errorf("job = %+v", job)
			}
			if len(h.svc.runs) != 0 || fileExists(helperRequestPath(h.dir)) {
				t.Error("the stack was touched, or a request was left behind")
			}
		})
	}

	// The previous helper is put back when the new one does not work.
	h := newTestHost(t)
	writeTestFile(t, helperBinaryPath(h.dir), "version 2.3.0 helper")
	h.playHelper("broken")
	s := h.stageOne()
	s.bundle = writeBundle(t, "2.4.0")
	if s.run(context.Background(), "") == nil {
		t.Fatal("no error")
	}
	if v := fakeVersion(helperBinaryPath(h.dir)); v != "2.3.0" {
		t.Errorf("the helper is %s", v)
	}

	h = newTestHost(t)
	s = h.stageOne()
	s.bundle = writeBundle(t, "2.3.0")
	if err := s.run(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if job, _ := readUpgradeJob(h.dir); job.Phase != upgDone || !strings.Contains(job.Message, "Already") {
		t.Errorf("job = %+v", job)
	}
}

// releaseServer serves GitHub's release API with one release.
func releaseServer(t *testing.T, version string, files map[string][]byte, edit func(rel *githubRelease)) {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	rel := githubRelease{TagName: "v" + version}
	for name, body := range files {
		body := body
		mux.HandleFunc("/dl/"+name, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) })
		rel.Assets = append(rel.Assets, githubAsset{Name: name, BrowserDownloadURL: srv.URL + "/dl/" + name})
	}
	if edit != nil {
		edit(&rel)
	}
	mux.HandleFunc("/repos/", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode([]githubRelease{rel}) })
	old := githubAPIBaseURL
	githubAPIBaseURL = srv.URL
	t.Cleanup(func() { githubAPIBaseURL = old })
}

func TestUpgradeFromRelease(t *testing.T) {
	_, body, _ := releaseArchive(t, "2.4.0", nil)
	asset := fmt.Sprintf("ncc-v2-stack-%s-%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	line := func(name string, b []byte) string {
		s := sha256.Sum256(b)
		return hex.EncodeToString(s[:]) + "  " + name + "\n"
	}
	manifest := func(m string) map[string][]byte {
		return map[string][]byte{asset: body, upgradeManifestName: []byte(m),
			stagedChecksums: []byte(line(asset, body) + line(upgradeManifestName, []byte(m)))}
	}
	ok := `{"schema":1,"version":"2.4.0","helper_protocol":1,"new_field":true}`
	for _, tc := range []struct {
		name  string
		files map[string][]byte
		edit  func(*githubRelease)
		want  string
	}{
		{"with a manifest", manifest(ok), nil, ""},
		{"without one", map[string][]byte{asset: body, stagedChecksums: []byte(line(asset, body))}, nil, ""},
		{"newer helper protocol", manifest(`{"schema":1,"version":"2.4.0","helper_protocol":2}`), nil, "directly"},
		{"from too old", manifest(`{"schema":1,"version":"2.4.0","helper_protocol":1,"min_from":"2.3.5"}`), nil, "needs v2.3.5"},
		{"manifest removed", manifest(ok), func(r *githubRelease) {
			for i, a := range r.Assets {
				if a.Name == upgradeManifestName {
					r.Assets = append(r.Assets[:i], r.Assets[i+1:]...)
					return
				}
			}
		}, "does not have it"},
		{"no package", map[string][]byte{stagedChecksums: []byte("x")}, nil, "no package"},
		{"no checksums", map[string][]byte{asset: body}, nil, "no checksums.txt"},
		{"archive not listed", map[string][]byte{asset: body, stagedChecksums: []byte("x\n")}, nil, "not listed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releaseServer(t, "2.4.0", tc.files, tc.edit)
			h := newTestHost(t)
			h.playHelper("")
			err := h.stageOne().run(context.Background(), "")
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				h.wantJob(upgDone)
				h.wantVersion("2.4.0", newUnit)
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			h.wantVersion("2.3.0", oldUnit)
		})
	}
}

// The supervisor gives the upgrade to the helper when a service runs it, and
// ends a preparation that a restart interrupted.
func TestSupervisorUsesTheHelper(t *testing.T) {
	h := newTestHost(t)
	u := &upgrader{installDir: h.dir, logf: func(string, ...any) {}, findService: func(string) stackService { return h.svc }}
	if u.helperService() == nil {
		t.Error("the helper is not used")
	}
	t.Setenv("NCC_UPGRADE_HELPER", "0")
	if u.helperService() != nil {
		t.Error("NCC_UPGRADE_HELPER=0 is ignored")
	}
	t.Setenv("NCC_UPGRADE_HELPER", "")
	h.svc.privErr = errors.New("no")
	if u.helperService() != nil {
		t.Error("used without the rights for it")
	}

	for _, tc := range []struct {
		phase, runner, want string
		journal             bool
	}{
		{upgDownloading, "stage", upgFailed, false},
		{upgPreflight, "stage", upgFailed, false},
		{upgHandover, "stage", upgHandover, true},
		{upgSwapping, "helper", upgSwapping, true},
		{upgDone, "stage", upgDone, false},
	} {
		h := newTestHost(t)
		newJobLog(h.dir, func(string, ...any) {}, upgradeJob{ID: "x", Runner: tc.runner}).step(tc.phase, "working")
		if tc.journal {
			_ = writeHelperJSON(helperJournalPath(h.dir), helperJournal{})
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		(&upgrader{installDir: h.dir, logf: func(string, ...any) {}, poll: time.Millisecond}).loop(ctx)
		if job, _ := readUpgradeJob(h.dir); job.Phase != tc.want {
			t.Errorf("%s/%s became %s, want %s", tc.runner, tc.phase, job.Phase, tc.want)
		}
	}
}
