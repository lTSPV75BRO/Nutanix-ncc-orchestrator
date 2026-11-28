package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// fakeService is a service manager in memory: its definition is a file, and
// starting it "runs" whatever programs are installed.
type fakeService struct {
	dir     string
	def     string // the definition's file
	desired string
	kindV   string

	up         bool
	runs       []string // version of each start
	badVersion string   // this version does not stay up
	stopErr    error
	migrateErr error
	privErr    error
	wake       func()
	held       int
	helperDef  string
	// outside: a service program outside bin/.
	outside string
	// activeRun: a scheduled run that is going on.
	activeRun string
}

func (s *fakeService) kind() string { return s.kindV }
func (s *fakeService) title() string {
	return "ncc-orchestrator.service"
}
func (s *fakeService) orchestratorBin() string {
	if s.outside != "" {
		return s.outside
	}
	return filepath.Join(s.dir, "bin", "ncc-orchestrator")
}
func (s *fakeService) helperCurrent(bin string) bool { return s.helperDef == bin }
func (s *fakeService) installHelper(bin string) error {
	s.helperDef = bin
	return nil
}
func (s *fakeService) removeHelper() { s.helperDef = "" }
func (s *fakeService) wakeHelper() error {
	if s.wake != nil {
		s.wake()
	}
	return nil
}
func (s *fakeService) probe() error      { return nil }
func (s *fakeService) privileged() error { return s.privErr }
func (s *fakeService) running() bool     { return s.up }
func (s *fakeService) start() error {
	s.runs = append(s.runs, fakeVersion(s.orchestratorBin()))
	s.up = true
	return nil
}
func (s *fakeService) stop() error {
	if s.stopErr != nil {
		return s.stopErr
	}
	s.up = false
	return nil
}
func (s *fakeService) plan(replace bool) []defChange {
	b, _ := os.ReadFile(s.def)
	out := []defChange{planUnit("Service", s.def, string(b), s.desired)}
	if replace {
		out = replaceEdited(out)
	}
	return out
}
func (s *fakeService) migrate(replace bool) ([]defChange, error) {
	changes := s.plan(replace)
	for _, c := range changes {
		if err := applyUnit(c); err != nil {
			return changes, err
		}
	}
	return changes, s.migrateErr
}
func (s *fakeService) save() ([]savedDefinition, error) {
	a, err := saveFileDefinition(s.def)
	if err != nil {
		return nil, err
	}
	b, err := saveFileDefinition(unitDropInPath(s.def))
	return []savedDefinition{a, b}, err
}
func (s *fakeService) restore(defs []savedDefinition) error {
	for _, d := range defs {
		if err := restoreFileDefinition(d); err != nil {
			return err
		}
	}
	return nil
}
func (s *fakeService) holdRestarts() func() { s.held++; return func() { s.held-- } }
func (s *fakeService) runActive() string    { return s.activeRun }

// fakeVersion reads the version out of a fake program ("version 2.4.0 ...").
func fakeVersion(path string) string {
	b, _ := os.ReadFile(path)
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return ""
	}
	return f[1]
}

func fakeRun(_ time.Duration, name string, _ ...string) (string, error) {
	if v := fakeVersion(name); v != "" {
		return "ncc\n  version: " + v + "\n", nil
	}
	return "", errors.New("does not run")
}

const (
	oldUnit = "[Unit]\nDescription=ncc\n\n[Service]\nExecStart=/opt/ncc/bin/ncc-orchestrator v2-supervise --install-dir /opt/ncc\n\n[Install]\nWantedBy=multi-user.target\n"
	newUnit = "[Unit]\nDescription=ncc\n\n[Service]\nExecStart=/opt/ncc/bin/ncc-orchestrator v2-supervise --install-dir /opt/ncc\nNoNewPrivileges=yes\n\n[Install]\nWantedBy=multi-user.target\n"
)

// testHost is an installed stack of version 2.3.0 with a service.
type testHost struct {
	t   *testing.T
	dir string
	svc *fakeService
}

func writeTestFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func newTestHost(t *testing.T) *testHost {
	dir := t.TempDir()
	for _, p := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		writeTestFile(t, filepath.Join(dir, "bin", p), "version 2.3.0 "+p)
	}
	writeTestFile(t, filepath.Join(dir, "frontend-dist", "index.html"), "old ui")
	writeTestFile(t, filepath.Join(dir, "frontend-dist", "old.js"), "old asset")
	writeTestFile(t, filepath.Join(dir, "config.yaml"), "a: 1\n")
	writeTestFile(t, filepath.Join(dir, "etc", "ncc.service"), oldUnit)
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := prepareHelperDir(dir); err != nil {
		t.Fatal(err)
	}
	return &testHost{t: t, dir: dir, svc: &fakeService{dir: dir, kindV: kindSystemd, up: true,
		def: filepath.Join(dir, "etc", "ncc.service"), desired: newUnit}}
}

// archive builds a release archive and its checksums.
func releaseArchive(t *testing.T, version string, files map[string]string) (name string, body, sums []byte) {
	t.Helper()
	all := map[string]string{
		"bin/ncc-orchestrator": "version " + version + " ncc-orchestrator", "bin/ncc-api-server": "version " + version + " ncc-api-server",
		"bin/ncc-ui-server": "version " + version + " ncc-ui-server", "frontend-dist/index.html": "new ui", "example_config.yaml": "example",
	}
	for k, v := range files {
		all[k] = v
	}
	var buf bytes.Buffer
	name = "ncc-v2-stack-test.tar.gz"
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, k := range []string{"bin/ncc-orchestrator", "bin/ncc-api-server", "bin/ncc-ui-server", "frontend-dist/index.html", "example_config.yaml", "bin/ncc-mcp-server"} {
		v, ok := all[k]
		if !ok {
			continue
		}
		if err := tw.WriteHeader(&tar.Header{Name: k, Mode: 0o755, Size: int64(len(v)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(tw.Close(), gz.Close()); err != nil {
		t.Fatal(err)
	}
	body = buf.Bytes()
	sum := sha256.Sum256(body)
	return name, body, []byte(hex.EncodeToString(sum[:]) + "  " + name + "\n")
}

// stageRelease puts a verified release where stage 1 would.
func (h *testHost) stageRelease(version string, files map[string]string) {
	h.t.Helper()
	name, body, sums := releaseArchive(h.t, version, files)
	if err := saveStage(helperStageDir(h.dir), name, body, sums, nil); err != nil {
		h.t.Fatal(err)
	}
	if err := writeHelperJSON(stageInfoPath(h.dir), stagedRelease{Version: version, Archive: name}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *testHost) engine(action string) *helperEngine {
	j := &helperJournal{helperRequest: helperRequest{ID: "job1", Action: action, Kind: kindSystemd, From: "2.3.0", Target: "2.4.0"}}
	if action == helperActionAdopt {
		j.From = "2.4.0"
	}
	return h.resume(j)
}

// resume is a fresh helper process working on j.
func (h *testHost) resume(j *helperJournal) *helperEngine {
	logf := func(f string, a ...any) { h.t.Logf(f, a...) }
	e := &helperEngine{
		installDir: h.dir, version: "2.4.0", svc: h.svc, logf: logf, j: j, runCmd: fakeRun,
		log:    newJobLog(h.dir, logf, helperJob(h.dir, j)),
		doctor: func() *upgradeDoctorNote { return &upgradeDoctorNote{} },
	}
	e.check = func(_ context.Context, _ time.Time, version string) error {
		got := fakeVersion(h.svc.orchestratorBin())
		switch {
		case !h.svc.up:
			return errors.New("not running")
		case got == h.svc.badVersion:
			return fmt.Errorf("v%s keeps crashing", got)
		case version != "" && got != version:
			return fmt.Errorf("the service started v%s instead of v%s", got, version)
		}
		return nil
	}
	return e
}

func (h *testHost) read(rel string) string {
	b, err := os.ReadFile(filepath.Join(h.dir, filepath.FromSlash(rel)))
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(b)
}

func (h *testHost) wantVersion(version, unit string) {
	h.t.Helper()
	for _, p := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		if got := fakeVersion(filepath.Join(h.dir, "bin", p)); got != version {
			h.t.Errorf("%s is version %q, want %q", p, got, version)
		}
	}
	if got := h.read("etc/ncc.service"); got != unit {
		h.t.Errorf("the definition is\n%s", got)
	}
	ui := "old ui"
	if version == "2.4.0" {
		ui = "new ui"
	}
	if got := h.read("frontend-dist/index.html"); got != ui {
		h.t.Errorf("frontend = %q, want %q", got, ui)
	}
	if !h.svc.up {
		h.t.Error("the service is not running")
	}
	if n := len(h.svc.runs); n > 0 && h.svc.runs[n-1] != version {
		h.t.Errorf("the service was last started on %s", h.svc.runs[n-1])
	}
	if h.svc.held != 0 {
		h.t.Error("the update watcher is still paused")
	}
}

func (h *testHost) wantJob(phase string) *upgradeJob {
	h.t.Helper()
	job, err := readUpgradeJob(h.dir)
	if err != nil {
		h.t.Fatal(err)
	}
	if job.Phase != phase {
		h.t.Fatalf("phase = %s (%s), want %s", job.Phase, job.Message, phase)
	}
	if job.FinishedAt == "" {
		h.t.Error("no finish time")
	}
	if helperUnfinished(h.dir) {
		h.t.Error("the journal is still there")
	}
	if existingDir(helperStageDir(h.dir)) {
		h.t.Error("the staged release is still there")
	}
	return job
}

func TestHelperUpgrade(t *testing.T) {
	h := newTestHost(t)
	h.stageRelease("2.4.0", map[string]string{"bin/ncc-mcp-server": "version 2.4.0 mcp"})
	h.engine(helperActionUpgrade).run(context.Background())
	job := h.wantJob(upgDone)
	h.wantVersion("2.4.0", newUnit)
	if job.ToVersion != "2.4.0" || len(job.Migrations) != 1 || job.RollbackUntil == "" {
		t.Errorf("job = %+v", job)
	}
	if h.read("frontend-dist/old.js")[0] != '<' {
		t.Error("an old asset was left behind")
	}
	info, ok := rollbackAvailable(h.dir)
	if !ok || info.From != "2.3.0" || len(info.NewFiles) != 1 || info.NewFiles[0] != "ncc-mcp-server" {
		t.Errorf("rollback info = %+v, %v", info, ok)
	}

	// And back by hand, with one file edited since and one changed by the upgrade.
	writeTestFile(t, filepath.Join(h.dir, "config.yaml"), "a: 2\n")
	e := h.resume(&helperJournal{helperRequest: helperRequest{ID: "job2", Action: helperActionRollback, Kind: kindSystemd, From: "2.3.0", Target: "2.4.0"}})
	e.run(context.Background())
	h.wantJob(upgRolledBack)
	h.wantVersion("2.3.0", oldUnit)
	if fileExists(filepath.Join(h.dir, "bin", "ncc-mcp-server")) {
		t.Error("a program of the new version was left behind")
	}
	if got := h.read("config.yaml"); got != "a: 2\n" {
		t.Errorf("an edited config.yaml was overwritten: %q", got)
	}
	if h.read("frontend-dist/old.js") != "old asset" {
		t.Error("the old frontend is incomplete")
	}
	if _, ok := rollbackAvailable(h.dir); ok {
		t.Error("the snapshot was not removed")
	}
}

func TestHelperRollsBackByItself(t *testing.T) {
	cases := map[string]func(h *testHost, e *helperEngine){
		"new version crashes":    func(h *testHost, _ *helperEngine) { h.svc.badVersion = "2.4.0" },
		"definitions fail":       func(h *testHost, _ *helperEngine) { h.svc.migrateErr = errors.New("rejected") },
		"config changed, crash":  func(h *testHost, e *helperEngine) { h.svc.badVersion = "2.4.0"; e.entered = h.spoilConfig },
		"service does not stop":  func(h *testHost, _ *helperEngine) { h.svc.stopErr = errors.New("busy") },
		"wrong version comes up": func(h *testHost, e *helperEngine) { e.entered = h.oldAgain },
	}
	for name, spoil := range cases {
		t.Run(name, func(t *testing.T) {
			h := newTestHost(t)
			h.stageRelease("2.4.0", map[string]string{"bin/ncc-mcp-server": "version 2.4.0 mcp"})
			e := h.engine(helperActionUpgrade)
			spoil(h, e)
			e.run(context.Background())
			job := h.wantJob(upgRolledBack)
			h.wantVersion("2.3.0", oldUnit)
			if job.Error == "" || h.read("config.yaml") != "a: 1\n" {
				t.Errorf("error %q, config %q", job.Error, h.read("config.yaml"))
			}
			if fileExists(filepath.Join(h.dir, "bin", "ncc-mcp-server")) {
				t.Error("a program of the new version was left behind")
			}
			if existingDir(helperSnapshotDir(h.dir)) {
				t.Error("the snapshot was not removed")
			}
		})
	}
}

// spoilConfig: the new version rewrites the configuration when it starts.
func (h *testHost) spoilConfig(step string) {
	if step == upgChecking {
		writeTestFile(h.t, filepath.Join(h.dir, "config.yaml"), "new: format\n")
	}
}

// oldAgain: something else puts an old program back.
func (h *testHost) oldAgain(step string) {
	if step == upgChecking {
		writeTestFile(h.t, filepath.Join(h.dir, "bin", "ncc-orchestrator"), "version 2.2.0 other")
	}
}

// A helper that dies at any step is started again (same journal on disk), and
// the host ends up entirely on one version.
func TestHelperCrashAtEveryStep(t *testing.T) {
	ends := map[string]string{
		upgStaging: upgDone, upgSaving: upgDone,
		upgStopping: upgRolledBack, upgSwapping: upgRolledBack, upgMigrating: upgRolledBack,
		upgStarting: upgDone, upgChecking: upgDone,
	}
	for _, bad := range []string{"", "2.4.0"} {
		for step, want := range ends {
			t.Run(step+"/bad="+bad, func(t *testing.T) {
				h := newTestHost(t)
				h.svc.badVersion = bad
				h.stageRelease("2.4.0", nil)
				crash := func(e *helperEngine, at string) {
					defer func() { _ = recover() }()
					e.entered = func(s string) {
						if s == at {
							panic("crash")
						}
					}
					e.run(context.Background())
					t.Fatal("did not crash")
				}
				crash(h.engine(helperActionUpgrade), step)
				h.svc.held = 0 // the process that held the watcher is gone
				if bad != "" {
					want = upgRolledBack
				}
				again := func() *helperEngine {
					var j helperJournal
					if err := readHelperJSON(helperJournalPath(h.dir), &j); err != nil {
						t.Fatal(err)
					}
					return h.resume(&j)
				}
				if want == upgRolledBack {
					crash(again(), upgRestoring) // dies a second time, mid-restore
					h.svc.held = 0
				}
				again().run(context.Background())
				h.wantJob(want)
				if want == upgDone {
					h.wantVersion("2.4.0", newUnit)
				} else {
					h.wantVersion("2.3.0", oldUnit)
				}
			})
		}
	}
}

func TestHelperGivesUpCrashingNewVersion(t *testing.T) {
	h := newTestHost(t)
	h.stageRelease("2.4.0", nil)
	e := h.engine(helperActionUpgrade)
	for i := 0; ; i++ {
		if i > 10 {
			t.Fatal("never gave up")
		}
		func() {
			defer func() { _ = recover() }()
			e.entered = func(s string) {
				if s == upgChecking {
					panic("the host goes down")
				}
			}
			e.run(context.Background())
		}()
		h.svc.held = 0
		if !helperUnfinished(h.dir) {
			break
		}
		var j helperJournal
		if err := readHelperJSON(helperJournalPath(h.dir), &j); err != nil {
			t.Fatal(err)
		}
		e = h.resume(&j)
	}
	h.wantJob(upgRolledBack)
	h.wantVersion("2.3.0", oldUnit)
}

func TestHelperRefusesTamperedRelease(t *testing.T) {
	cases := map[string]func(h *testHost){
		"archive changed": func(h *testHost) {
			p := filepath.Join(helperStageDir(h.dir), "ncc-v2-stack-test.tar.gz")
			b, _ := os.ReadFile(p)
			_ = os.WriteFile(p, append(b, 0), 0o600)
		},
		"checksums gone": func(h *testHost) { _ = os.Remove(filepath.Join(helperStageDir(h.dir), stagedChecksums)) },
		"other version":  func(h *testHost) { h.stageRelease("2.5.0", nil) },
		"mixed versions": func(h *testHost) {
			h.stageRelease("2.4.0", map[string]string{"bin/ncc-api-server": "version 2.1.0 old"})
		},
		"program does not run": func(h *testHost) { h.stageRelease("2.4.0", map[string]string{"bin/ncc-ui-server": "x"}) },
		"path in the name": func(h *testHost) {
			_ = writeHelperJSON(stageInfoPath(h.dir), stagedRelease{Version: "2.4.0", Archive: "../../evil.tar.gz"})
		},
		"unknown field": func(h *testHost) {
			_ = writeHelperFile(stageInfoPath(h.dir), []byte(`{"version":"2.4.0","archive":"ncc-v2-stack-test.tar.gz","run":"x"}`))
		},
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			h := newTestHost(t)
			h.stageRelease("2.4.0", nil)
			tamper(h)
			h.engine(helperActionUpgrade).run(context.Background())
			job := h.wantJob(upgFailed)
			h.wantVersion("2.3.0", oldUnit)
			if !strings.Contains(job.Message, "Nothing was changed") || len(h.svc.runs) != 0 {
				t.Errorf("message %q, starts %v", job.Message, h.svc.runs)
			}
		})
	}
}

func TestHelperChecksSignature(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	old := releaseSigningPublicKeyB64
	releaseSigningPublicKeyB64 = base64.StdEncoding.EncodeToString(pub)
	t.Cleanup(func() { releaseSigningPublicKeyB64 = old })
	name, body, sums := releaseArchive(t, "2.4.0", nil)
	good := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(priv, sums)) + "\n")
	for _, tc := range []struct {
		name string
		sig  []byte
		want string
	}{{"signed", good, upgDone}, {"unsigned", nil, upgFailed}, {"wrong", []byte(base64.StdEncoding.EncodeToString(make([]byte, 64))), upgFailed}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHost(t)
			if err := saveStage(helperStageDir(h.dir), name, body, sums, tc.sig); err != nil {
				t.Fatal(err)
			}
			if err := writeHelperJSON(stageInfoPath(h.dir), stagedRelease{Version: "2.4.0", Archive: name}); err != nil {
				t.Fatal(err)
			}
			h.engine(helperActionUpgrade).run(context.Background())
			h.wantJob(tc.want)
		})
	}
}

func TestHelperDoesNotRestoreTamperedSnapshot(t *testing.T) {
	h := newTestHost(t)
	h.svc.badVersion = "2.4.0"
	h.stageRelease("2.4.0", nil)
	e := h.engine(helperActionUpgrade)
	e.entered = func(s string) {
		if s == upgChecking {
			writeTestFile(t, filepath.Join(helperSnapshotDir(h.dir), "bin", "ncc-api-server"), "version 2.3.0 evil")
		}
	}
	e.j.BackupPath = filepath.Join(h.dir, "backups", "pre.tar.gz")
	e.run(context.Background())
	job := h.wantJob(upgFailed)
	if strings.Contains(h.read("bin/ncc-api-server"), "evil") || !strings.Contains(job.Message, "pre.tar.gz") {
		t.Errorf("message %q", job.Message)
	}
}

func TestHelperAdopt(t *testing.T) {
	h := newTestHost(t)
	for _, p := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		writeTestFile(t, filepath.Join(h.dir, "bin", p), "version 2.4.0 "+p)
	}
	h.engine(helperActionAdopt).run(context.Background())
	h.wantJob(upgDone)
	if h.read("etc/ncc.service") != newUnit || len(h.svc.runs) != 1 {
		t.Errorf("definition %q, starts %v", h.read("etc/ncc.service"), h.svc.runs)
	}

	// The new definition does not work on this host: put back.
	h = newTestHost(t)
	for _, p := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		writeTestFile(t, filepath.Join(h.dir, "bin", p), "version 2.4.0 "+p)
	}
	e := h.engine(helperActionAdopt)
	check := e.check
	e.check = func(ctx context.Context, since time.Time, v string) error {
		if h.read("etc/ncc.service") == newUnit {
			return errors.New("the service does not start")
		}
		return check(ctx, since, v)
	}
	e.run(context.Background())
	h.wantJob(upgRolledBack)
	if h.read("etc/ncc.service") != oldUnit {
		t.Error("the definition was not put back")
	}
}

func TestHelperServiceProgramOutsideBin(t *testing.T) {
	for _, bad := range []string{"", "2.4.0"} {
		h := newTestHost(t)
		h.svc.outside = filepath.Join(h.dir, "usr", "ncc")
		h.svc.badVersion = bad
		writeTestFile(t, h.svc.outside, "version 2.3.0 outside")
		h.stageRelease("2.4.0", nil)
		h.engine(helperActionUpgrade).run(context.Background())
		want := "2.4.0"
		if bad != "" {
			want = "2.3.0"
		}
		if got := fakeVersion(h.svc.outside); got != want {
			t.Errorf("bad=%q: the service program is %s, want %s", bad, got, want)
		}
	}
}

func TestTakeHelperJob(t *testing.T) {
	h := newTestHost(t)
	if _, ok, err := takeHelperJob(h.dir); ok || err != nil {
		t.Fatalf("idle: %v %v", ok, err)
	}
	req := helperRequest{ID: "j", Action: helperActionUpgrade, Kind: kindSystemd, Target: "2.4.0"}
	if err := writeHelperJSON(helperRequestPath(h.dir), req); err != nil {
		t.Fatal(err)
	}
	j, ok, err := takeHelperJob(h.dir)
	if !ok || err != nil || j.Target != "2.4.0" {
		t.Fatalf("%+v %v %v", j, ok, err)
	}
	if fileExists(helperRequestPath(h.dir)) || fileExists(helperAcceptedPath(h.dir)) || !helperUnfinished(h.dir) {
		t.Error("the request was not turned into the journal")
	}
	// Died between the rename and the journal.
	_ = os.Remove(helperJournalPath(h.dir))
	_ = writeHelperJSON(helperAcceptedPath(h.dir), req)
	if j, ok, err = takeHelperJob(h.dir); !ok || err != nil || j.ID != "j" {
		t.Errorf("accepted: %+v %v %v", j, ok, err)
	}
	for _, bad := range []string{`{"id":"x","action":"run","kind":"systemd"}`, `{"id":"../x","action":"adopt","kind":"systemd"}`, `{"id":"x","action":"upgrade","kind":"systemd"}`,
		`{"id":"x","action":"adopt","kind":"systemd","exec":"/bin/sh"}`, `{"id":"x","action":"upgrade","kind":"systemd","target":"2.4.0; reboot"}`, `[]`, ``} {
		_ = os.Remove(helperJournalPath(h.dir))
		_ = writeHelperFile(helperRequestPath(h.dir), []byte(bad))
		if _, ok, err := takeHelperJob(h.dir); ok || err == nil {
			t.Errorf("accepted %s", bad)
		}
		if fileExists(helperRequestPath(h.dir)) || fileExists(helperAcceptedPath(h.dir)) {
			t.Errorf("a bad request stays and is tried again: %s", bad)
		}
	}
	_ = writeHelperFile(helperJournalPath(h.dir), []byte("{"))
	if _, ok, err := takeHelperJob(h.dir); ok || err == nil {
		t.Error("a broken journal was accepted")
	}
}

func TestStackCheckSupervised(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	child := func(state string) *superviseChildState {
		return &superviseChildState{State: state, StartedAt: now.Add(-time.Minute).Format(time.RFC3339)}
	}
	base := func() superviseState {
		return superviseState{PID: os.Getpid(), Version: "2.4.0", StartedAt: now.Add(-time.Minute).Format(time.RFC3339),
			Children: map[string]*superviseChildState{"api": child("running"), "ui": child("running")}}
	}
	for _, tc := range []struct {
		name string
		edit func(*superviseState)
		want string // part of the error, "" = healthy
	}{
		{"healthy", func(*superviseState) {}, ""},
		{"old process", func(s *superviseState) { s.StartedAt = now.Add(-time.Hour).Format(time.RFC3339) }, "has not started yet"},
		{"other version", func(s *superviseState) { s.Version = "2.3.0" }, "v2.3.0 instead of v2.4.0"},
		{"dead", func(s *superviseState) { s.PID = 0 }, "not running"},
		{"cooldown", func(s *superviseState) { s.Children["ui"] = child("cooldown") }, "kept stopping"},
		{"starting", func(s *superviseState) { s.Children["api"] = child("starting") }, "the API server is starting"},
		{"not probed yet", func(s *superviseState) { s.Children["api"].Probed = true }, "the API server is running"},
		{"no servers", func(s *superviseState) { s.Children = nil }, "no server"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := base()
			tc.edit(&st)
			b, _ := json.Marshal(st)
			if err := os.WriteFile(supervisorStatePath(dir), b, 0o644); err != nil {
				t.Fatal(err)
			}
			c := &stackCheck{installDir: dir, svc: &fakeService{kindV: kindSystemd}, poll: time.Millisecond, window: time.Second, timeout: 20 * time.Millisecond}
			err := c.wait(context.Background(), now.Add(-2*time.Minute), "2.4.0")
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestReadHelperJSONIsStrict(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.json")
	for _, bad := range []string{`{"id":"a"} {}`, `{"nope":1}`, ``, strings.Repeat(" ", 2<<20)} {
		_ = os.WriteFile(p, []byte(bad), 0o600)
		var r helperRequest
		if readHelperJSON(p, &r) == nil {
			t.Errorf("accepted %.40q", bad)
		}
	}
	if runtime.GOOS != "windows" {
		_ = os.WriteFile(filepath.Join(dir, "real.json"), []byte(`{"id":"a"}`), 0o600)
		_ = os.Remove(p)
		_ = os.Symlink(filepath.Join(dir, "real.json"), p)
		var r helperRequest
		if readHelperJSON(p, &r) == nil {
			t.Error("followed a link")
		}
	}
}
