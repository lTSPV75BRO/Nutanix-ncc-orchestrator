package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Tests for the parts of docs/UPGRADE_HELPER_PLAN.md built after 2.4.0-rc.3:
// replacing an edited unit, rollback after adoption and `update`, Windows tasks
// of another name, the bundle manifest, and the fault-injection list.

// editedUnit runs another command: an upgrade keeps it unless asked.
const editedUnit = "[Unit]\nDescription=ncc\n\n[Service]\nExecStart=/opt/ncc/bin/ncc-orchestrator v2-supervise --install-dir /opt/ncc --verbose\n\n[Install]\nWantedBy=multi-user.target\n"

func TestLineDiff(t *testing.T) {
	got := lineDiff("a\nb\nc\nd\ne\nf\n", "a\nb\nX\nd\ne\nf\ng\n", 100)
	want := " b\n-c\n+X\n d\n...\n f\n+g\n"
	if got != want {
		t.Errorf("diff\n%s\nwant\n%s", got, want)
	}
	if lineDiff("same\r\n", "same\n", 100) != "" {
		t.Error("line endings are not a change")
	}
	if d := lineDiff("", strings.Repeat("x\n", 50), 10); !strings.Contains(d, "(40 more lines)") {
		t.Errorf("not cut: %q", d)
	}
}

func TestPlanShowsTheDiffOfAnEditedUnit(t *testing.T) {
	c := planUnit("Service", "/etc/x.service", editedUnit, newUnit)
	if c.Action != defKeep || !strings.Contains(c.Diff(), "-ExecStart=") || !strings.Contains(c.Diff(), "+NoNewPrivileges=yes") {
		t.Fatalf("%+v\n%s", c, c.Diff())
	}
	r := replaceEdited([]defChange{c})[0]
	if r.Action != defReplace || !r.pending() || !strings.Contains(r.Detail, "replaced on request") {
		t.Errorf("%+v", r)
	}
	if planUnit("Service", "/etc/x.service", newUnit, newUnit).Diff() != "" {
		t.Error("an unchanged unit has a diff")
	}
}

func TestHelperReplacesAnEditedUnitOnRequest(t *testing.T) {
	// Default (Decision 3): kept.
	h := newTestHost(t)
	writeTestFile(t, h.svc.def, editedUnit)
	h.stageRelease("2.4.0", nil)
	h.engine(helperActionUpgrade).run(context.Background())
	job := h.wantJob(upgDone)
	if h.read("etc/ncc.service") != editedUnit || !strings.Contains(strings.Join(job.Migrations, " "), "keep") {
		t.Fatalf("kept? %q %v", h.read("etc/ncc.service"), job.Migrations)
	}

	// Asked for: replaced, and put back by a rollback.
	h = newTestHost(t)
	writeTestFile(t, h.svc.def, editedUnit)
	h.stageRelease("2.4.0", nil)
	e := h.engine(helperActionUpgrade)
	e.j.ReplaceEdited = true
	e.run(context.Background())
	job = h.wantJob(upgDone)
	h.wantVersion("2.4.0", newUnit)
	if !strings.Contains(strings.Join(job.Migrations, " "), "replaced on request") {
		t.Errorf("migrations = %v", job.Migrations)
	}
	h.resume(&helperJournal{helperRequest: helperRequest{ID: "job2", Action: helperActionRollback, Kind: kindSystemd}}).run(context.Background())
	h.wantJob(upgRolledBack)
	h.wantVersion("2.3.0", editedUnit)

	// A failed start after the replacement puts the edited unit back too.
	h = newTestHost(t)
	writeTestFile(t, h.svc.def, editedUnit)
	h.svc.badVersion = "2.4.0"
	h.stageRelease("2.4.0", nil)
	e = h.engine(helperActionUpgrade)
	e.j.ReplaceEdited = true
	e.run(context.Background())
	h.wantJob(upgRolledBack)
	h.wantVersion("2.3.0", editedUnit)
}

func TestRollbackAfterAdoption(t *testing.T) {
	h := newTestHost(t)
	for _, p := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server"} {
		writeTestFile(t, filepath.Join(h.dir, "bin", p), "version 2.4.0 "+p)
	}
	h.engine(helperActionAdopt).run(context.Background())
	job := h.wantJob(upgDone)
	info, ok := rollbackAvailable(h.dir)
	if !ok || info.Action != helperActionAdopt || job.RollbackUntil == "" {
		t.Fatalf("no rollback after adoption: %+v %v", info, ok)
	}
	if existingDir(filepath.Join(helperSnapshotDir(h.dir), "bin")) {
		t.Error("an adoption saved programs it does not change")
	}
	h.resume(&helperJournal{helperRequest: helperRequest{ID: "job2", Action: helperActionRollback, Kind: kindSystemd}}).run(context.Background())
	job = h.wantJob(upgRolledBack)
	if h.read("etc/ncc.service") != oldUnit || !strings.Contains(job.Message, "service definitions from before v2.4.0") {
		t.Errorf("unit %q, message %q", h.read("etc/ncc.service"), job.Message)
	}
	if fakeVersion(filepath.Join(h.dir, "bin", "ncc-api-server")) != "2.4.0" {
		t.Error("the programs changed")
	}
	if _, ok := rollbackAvailable(h.dir); ok {
		t.Error("the snapshot is still offered")
	}
}

func TestRollbackAfterUpdate(t *testing.T) {
	h := newTestHost(t)
	old := findStackService
	findStackService = func(string) stackService { return h.svc }
	t.Cleanup(func() { findStackService = old })
	snap, err := saveUpdateSnapshot(h.dir, fakeRun)
	if err != nil {
		t.Fatal(err)
	}
	// What `update` does: new programs, one of them new.
	for _, p := range []string{"ncc-orchestrator", "ncc-api-server", "ncc-ui-server", "ncc-mcp-server"} {
		writeTestFile(t, filepath.Join(h.dir, "bin", p), "version 2.4.0 "+p)
	}
	writeTestFile(t, filepath.Join(h.dir, "frontend-dist", "index.html"), "new ui")
	if err := snap.keep("v2.4.0"); err != nil {
		t.Fatal(err)
	}
	info, ok := rollbackAvailable(h.dir)
	if !ok || info.Action != helperActionUpdate || info.From != "2.3.0" || info.To != "2.4.0" || len(info.NewFiles) != 1 {
		t.Fatalf("%+v %v", info, ok)
	}
	h.resume(&helperJournal{helperRequest: helperRequest{ID: "job2", Action: helperActionRollback, Kind: kindSystemd}}).run(context.Background())
	h.wantJob(upgRolledBack)
	h.wantVersion("2.3.0", oldUnit)
	if fileExists(filepath.Join(h.dir, "bin", "ncc-mcp-server")) {
		t.Error("a program of the update was left behind")
	}

	// The supervisor's own rollback (programs-only update) drops it.
	snap, err = saveUpdateSnapshot(h.dir, fakeRun)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.keep("2.4.0"); err != nil {
		t.Fatal(err)
	}
	dropUpdateSnapshot(h.dir)
	if _, ok := rollbackAvailable(h.dir); ok {
		t.Error("still offered after the supervisor put the programs back")
	}
}

func TestUpdateHandsOverOnlyWhatTheHelperCanDo(t *testing.T) {
	base := updateOptions{Repo: defaultGitHubRepo}
	if !updateHelperCanTakeOver(base, false) {
		t.Error("a plain update is not handed over")
	}
	for name, o := range map[string]updateOptions{
		"check":      {CheckOnly: true},
		"binary url": {BinaryURL: "https://example.com/x"},
		"no verify":  {SkipChecksumVerify: true},
		"major":      {AllowMajorUpgrade: true},
		"other repo": {Repo: "someone/fork"},
	} {
		if updateHelperCanTakeOver(o, false) {
			t.Errorf("%s: handed over", name)
		}
	}
	if updateHelperCanTakeOver(base, true) {
		t.Error("--programs-only: handed over")
	}
	t.Setenv(updateProgramsOnlyEnv, "1")
	if updateHelperCanTakeOver(base, false) {
		t.Error("NCC_UPDATE_PROGRAMS_ONLY: handed over")
	}
}

func TestWindowsTaskOfAnotherName(t *testing.T) {
	dir := `C:\ncc`
	o := installServiceOptions{InstallDir: dir, ServiceName: "ncc-prod", OrchestratorBin: dir + `\bin\ncc-orchestrator.exe`}
	ours := windowsSupervisorTaskXML(o)
	scan := windowsTaskXML(windowsTaskSpec{Description: "scan", Command: o.OrchestratorBin, Arguments: "run --config x.yaml", WorkingDir: dir})
	all := "<Tasks>\r\n<!-- \\Other -->\r\n" + scan + "\r\n<!-- \\ncc-prod -->\r\n" + ours + "\r\n<!-- \\Folder\\ncc-copy -->\r\n" + ours + "</Tasks>"
	docs := splitWindowsTasks(all)
	if len(docs) != 3 || docs[0].name != "Other" || docs[1].name != "ncc-prod" || docs[2].name != `\Folder\ncc-copy` {
		t.Fatalf("%+v", docs)
	}
	oldOne, oldAll := queryWindowsTaskXML, queryAllWindowsTasksXML
	t.Cleanup(func() { queryWindowsTaskXML, queryAllWindowsTasksXML = oldOne, oldAll })
	queryWindowsTaskXML = func(string) (string, bool) { return "", false }
	queryAllWindowsTasksXML = func() (string, bool) { return all, true }
	svc, ok := findWindowsService(dir).(*windowsService)
	if !ok || svc.o.ServiceName != "ncc-prod" || svc.helperName() != "ncc-prod-upgrade" || svc.title() != "scheduled task ncc-prod" {
		t.Fatalf("found %+v", svc)
	}
	if findWindowsService(`C:\other`) != nil {
		t.Error("the task of another folder was taken")
	}
}

func TestBundleManifestMinFrom(t *testing.T) {
	h := newTestHost(t)
	h.playHelper("")
	s := h.stageOne()
	s.bundle = writeBundle(t, "2.4.0")
	folder := filepath.Dir(s.bundle)
	man := []byte(`{"schema":1,"version":"2.4.0","helper_protocol":1,"min_from":"2.3.5","downtime_hint":"2 minutes"}`)
	writeTestFile(t, filepath.Join(folder, upgradeManifestName), string(man))
	sum := sha256.Sum256(man)
	sums, _ := os.ReadFile(filepath.Join(folder, stagedChecksums))
	writeTestFile(t, filepath.Join(folder, stagedChecksums), string(sums)+hex.EncodeToString(sum[:])+"  "+upgradeManifestName+"\n")
	err := s.run(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "upgrade to v2.3.5 first") {
		t.Fatalf("err = %v", err)
	}
	h.wantVersion("2.3.0", oldUnit)
	var m upgradeManifest
	if decodeManifest(man, &m) != nil || m.DowntimeHint != "2 minutes" {
		t.Errorf("%+v", m)
	}

	// Listed but missing.
	_ = os.Remove(filepath.Join(folder, upgradeManifestName))
	if err := checkBundleManifest(folder, []byte(string(sums)+hex.EncodeToString(sum[:])+"  "+upgradeManifestName+"\n"), "2.3.0"); err == nil {
		t.Error("a removed manifest was not noticed")
	}
}

// ---- fault injection: each ends Upgraded, Rolled back or Nothing was changed ----

var errDiskFull = errors.New("no space left on device")

func diskFullUnder(t *testing.T, dir string, until func() bool) {
	t.Helper()
	writeFaultHook = func(path string) error {
		if pathWithin(path, dir) && (until == nil || !until()) {
			return errDiskFull
		}
		return nil
	}
	t.Cleanup(func() { writeFaultHook = nil })
}

func (h *testHost) wantNothingChanged() {
	h.t.Helper()
	job := h.wantJob(upgFailed)
	if !strings.Contains(job.Message, "Nothing was changed") {
		h.t.Errorf("message %q", job.Message)
	}
	h.wantVersion("2.3.0", oldUnit)
	if len(h.svc.runs) != 0 {
		h.t.Errorf("the stack was restarted: %v", h.svc.runs)
	}
	if existingDir(helperSnapshotDir(h.dir)) {
		h.t.Error("a half snapshot was left behind")
	}
}

func TestFaultFullDiskWhileStaging(t *testing.T) {
	h := newTestHost(t)
	h.stageRelease("2.4.0", nil)
	diskFullUnder(t, filepath.Join(helperStageDir(h.dir), "root"), nil)
	h.engine(helperActionUpgrade).run(context.Background())
	h.wantNothingChanged()
}

func TestFaultFullDiskAtSnapshot(t *testing.T) {
	h := newTestHost(t)
	h.stageRelease("2.4.0", nil)
	diskFullUnder(t, helperSnapshotDir(h.dir), nil)
	h.engine(helperActionUpgrade).run(context.Background())
	h.wantNothingChanged()
}

func TestFaultFullDiskAtSwap(t *testing.T) {
	h := newTestHost(t)
	h.stageRelease("2.4.0", nil)
	e := h.engine(helperActionUpgrade)
	restoring := false
	e.entered = func(step string) { restoring = restoring || step == upgRestoring }
	// Full while the new programs go in; space is back for the restore.
	diskFullUnder(t, filepath.Join(h.dir, "bin"), func() bool { return restoring })
	e.run(context.Background())
	job := h.wantJob(upgRolledBack)
	h.wantVersion("2.3.0", oldUnit)
	if !strings.Contains(job.Error, "no space left") {
		t.Errorf("error %q", job.Error)
	}
}

func TestFaultTruncatedArchive(t *testing.T) {
	// A truncated download whose checksum matches (a broken release): refused
	// while unpacking, in stage 1 ...
	h := newTestHost(t)
	h.playHelper("")
	s := h.stageOne()
	s.bundle = writeBundle(t, "2.4.0")
	body, _ := os.ReadFile(s.bundle)
	cut := body[:len(body)/2]
	writeTestFile(t, s.bundle, string(cut))
	sum := sha256.Sum256(cut)
	writeTestFile(t, filepath.Join(filepath.Dir(s.bundle), stagedChecksums), hex.EncodeToString(sum[:])+"  "+filepath.Base(s.bundle)+"\n")
	if err := s.run(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "unpack") {
		t.Fatalf("err = %v", err)
	}
	h.wantVersion("2.3.0", oldUnit)

	// ... and by the helper, should it get one.
	h = newTestHost(t)
	name, full, _ := releaseArchive(t, "2.4.0", nil)
	cut = full[:len(full)/2]
	sum = sha256.Sum256(cut)
	if err := saveStage(helperStageDir(h.dir), name, cut, []byte(hex.EncodeToString(sum[:])+"  "+name+"\n"), nil); err != nil {
		t.Fatal(err)
	}
	if err := writeHelperJSON(stageInfoPath(h.dir), stagedRelease{Version: "2.4.0", Archive: name}); err != nil {
		t.Fatal(err)
	}
	h.engine(helperActionUpgrade).run(context.Background())
	h.wantNothingChanged()
}

func TestFaultTwoUpgradeRequestsAtOnce(t *testing.T) {
	h := newTestHost(t)
	h.playHelper("")
	play := h.svc.wake
	inside, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.svc.wake = func() {
		once.Do(func() { close(inside); <-release }) // the first request is being prepared
		play()
	}
	first := h.stageOne()
	first.bundle = writeBundle(t, "2.4.0")
	done := make(chan error, 1)
	go func() { done <- first.run(context.Background(), "") }()
	<-inside

	second := h.stageOne()
	second.log = newJobLog(h.dir, second.logf, newJob("job2", helperActionUpgrade, "", "", "test"))
	second.bundle = writeBundle(t, "2.4.0")
	err := second.run(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "another upgrade") {
		t.Fatalf("second: %v", err)
	}
	if job, _ := readUpgradeJob(h.dir); job == nil || job.ID != "job1" || !helperPhaseActive(job.Phase) {
		t.Fatalf("the second request overwrote the progress: %+v", job)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if job := h.wantJob(upgDone); job.ID != "job1" {
		t.Errorf("job %s", job.ID)
	}
	h.wantVersion("2.4.0", newUnit)
}

func TestFaultRunStartedDuringStaging(t *testing.T) {
	h := newTestHost(t)
	h.stageRelease("2.4.0", nil)
	e := h.engine(helperActionUpgrade)
	e.entered = func(step string) {
		if step == upgStaging {
			h.svc.activeRun = "the scheduled run nightly" // its timer fired
		}
	}
	e.run(context.Background())
	h.wantNothingChanged()
	if job, _ := readUpgradeJob(h.dir); !strings.Contains(job.Message, "nightly is in progress") {
		t.Errorf("message %q", job.Message)
	}
}

func TestFaultClockJump(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The clock is set back an hour right after the check begins; the
	// supervisor and its servers stamp their start with the new time.
	now := time.Now()
	var mu sync.Mutex
	calls := 0
	wallClock = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if calls++; calls == 1 {
			return time.Now()
		}
		return time.Now().Add(-time.Hour)
	}
	t.Cleanup(func() { wallClock = time.Now })
	stamp := now.Add(-time.Hour + 2*time.Second).UTC().Format(time.RFC3339)
	child := func() *superviseChildState { return &superviseChildState{State: "running", StartedAt: stamp} }
	b, _ := json.Marshal(superviseState{PID: os.Getpid(), Version: "2.4.0", StartedAt: stamp,
		Children: map[string]*superviseChildState{"api": child(), "ui": child()}})
	if err := os.WriteFile(supervisorStatePath(dir), b, 0o644); err != nil {
		t.Fatal(err)
	}
	c := &stackCheck{installDir: dir, svc: &fakeService{kindV: kindSystemd}, poll: time.Millisecond, window: time.Second, timeout: 200 * time.Millisecond}
	if err := c.wait(context.Background(), now, "2.4.0"); err != nil {
		t.Fatalf("a clock set back made a healthy start look old: %v", err)
	}

	// Jumps measured from a start (the engine's switch time).
	wallClock = func() time.Time { return time.Now().Add(-time.Hour) }
	start := time.Now()
	if j := clockJump(start, start); j > -59*time.Minute || j < -61*time.Minute {
		t.Errorf("jump = %s", j)
	}
	wallClock = time.Now
	if j := clockJump(start, start); j < -time.Second || j > time.Second {
		t.Errorf("no jump = %s", j)
	}
}
