package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSystemdUnitGapsAndPatch(t *testing.T) {
	current := systemdSupervisorUnit(installServiceOptions{InstallDir: "/opt/ncc", OrchestratorBin: "/opt/ncc/bin/ncc-orchestrator"})
	if gaps := systemdUnitGaps(current); len(gaps) != 0 {
		t.Fatalf("current unit reports gaps %v", gaps)
	}
	old := strings.Join([]string{
		"[Unit]",
		"Description=NCC",
		"",
		"[Service]",
		"ExecStart=/opt/ncc/bin/ncc-orchestrator v2-supervise --install-dir /opt/ncc",
		"Restart=on-failure",
		"Environment=HTTPS_PROXY=http://proxy:3128",
		"",
		"[Install]",
		"WantedBy=multi-user.target",
	}, "\n")
	gaps := systemdUnitGaps(old)
	if len(gaps) != 3 {
		t.Fatalf("gaps = %v", gaps)
	}
	patched := patchSystemdUnit(old, gaps)
	if g := systemdUnitGaps(patched); len(g) != 0 {
		t.Fatalf("patched unit still lacks %v:\n%s", g, patched)
	}
	if !strings.Contains(patched, "Environment=HTTPS_PROXY=http://proxy:3128") || strings.Contains(patched, "Restart=on-failure") {
		t.Fatalf("local edits not kept or Restart not replaced:\n%s", patched)
	}
	if !strings.Contains(patched, "[Unit]\nStartLimitIntervalSec=0") {
		t.Fatalf("StartLimitIntervalSec not in [Unit]:\n%s", patched)
	}
}

func TestCheckSystemdUnitFixesOnlyThisInstall(t *testing.T) {
	prev := systemdDaemonReload
	reloads := 0
	systemdDaemonReload = func() (string, error) { reloads++; return "", nil }
	t.Cleanup(func() { systemdDaemonReload = prev })

	install := t.TempDir()
	bin := filepath.Join(install, "ncc-orchestrator")
	if err := os.WriteFile(bin, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	unitPath := filepath.Join(t.TempDir(), "ncc-orchestrator.service")
	unit := "[Unit]\nDescription=NCC\n\n[Service]\nExecStart=" + bin + " v2-supervise --install-dir " + install + "\nRestart=always\n"
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		t.Fatal(err)
	}
	base := healResult{ID: "service-config"}

	other := checkSystemdUnit(&healContext{InstallDir: t.TempDir(), Fix: true}, base, unitPath)
	if other.Status != healOK || other.Fixed || !strings.Contains(other.Message, "another install") {
		t.Fatalf("other install: %+v", other)
	}
	warn := checkSystemdUnit(&healContext{InstallDir: install}, base, unitPath)
	if warn.Status != healWarn || !strings.Contains(warn.Message, "StartLimitIntervalSec=0") {
		t.Fatalf("read-only: %+v", warn)
	}
	fixed := checkSystemdUnit(&healContext{InstallDir: install, Fix: true}, base, unitPath)
	if !fixed.Fixed || fixed.Status != healOK || reloads != 1 {
		t.Fatalf("fix: %+v reloads=%d", fixed, reloads)
	}
	if again := checkSystemdUnit(&healContext{InstallDir: install}, base, unitPath); again.Status != healOK {
		t.Fatalf("after fix: %+v", again)
	}

	_ = os.Remove(bin)
	if missing := checkSystemdUnit(&healContext{InstallDir: install}, base, unitPath); missing.Status != healFail {
		t.Fatalf("missing binary: %+v", missing)
	}
}

func TestWindowsTaskGaps(t *testing.T) {
	x := windowsSupervisorTaskXML(installServiceOptions{InstallDir: `C:\NCC & Co`, ServiceName: "ncc-orchestrator", OrchestratorBin: `C:\NCC & Co\bin\ncc-orchestrator.exe`})
	if gaps := windowsTaskGaps(x); len(gaps) != 0 {
		t.Fatalf("current definition reports gaps %v", gaps)
	}
	if got := xmlElement(x, "Command"); got != `C:\NCC & Co\bin\ncc-orchestrator.exe` {
		t.Fatalf("Command round trip = %q", got)
	}
	if got := installDirFromCommand(xmlElement(x, "Arguments")); got != `C:\NCC & Co` {
		t.Fatalf("install dir round trip = %q", got)
	}
	legacy := `<Task><Settings><StopIfGoingOnBatteries>true</StopIfGoingOnBatteries><ExecutionTimeLimit>PT72H</ExecutionTimeLimit></Settings></Task>`
	if gaps := windowsTaskGaps(legacy); len(gaps) != 3 {
		t.Fatalf("legacy gaps = %v", gaps)
	}
	if b := utf16LEWithBOM("<a/>"); len(b) != 10 || b[0] != 0xFF || b[1] != 0xFE || b[2] != '<' || b[3] != 0 {
		t.Fatalf("utf16 = %v", b)
	}
}

func TestExitSummaryLine(t *testing.T) {
	tail := []string{"listening on :8080", "panic: runtime error: invalid memory address", "goroutine 1 [running]:", "main.main()"}
	if got := exitSummaryLine(tail); got != "panic: runtime error: invalid memory address" {
		t.Fatalf("panic line = %q", got)
	}
	if got := exitSummaryLine([]string{"open db: error: locked", "shutting down"}); got != "open db: error: locked" {
		t.Fatalf("error line = %q", got)
	}
	if got := exitSummaryLine([]string{"a", "b"}); got != "b" {
		t.Fatalf("fallback = %q", got)
	}
	if exitSummaryLine(nil) != "" {
		t.Fatal("empty tail")
	}
}

func TestInstallDirFromCommand(t *testing.T) {
	for in, want := range map[string]string{
		`/opt/ncc/bin/ncc-orchestrator v2-supervise --install-dir /opt/ncc`: "/opt/ncc",
		`v2-supervise --install-dir "C:\Program Files\NCC"`:                 `C:\Program Files\NCC`,
		`--config "C:\x\config.yaml" --run-history --retain-last 500`:       "",
	} {
		if got := installDirFromCommand(in); got != want {
			t.Errorf("installDirFromCommand(%q) = %q, want %q", in, got, want)
		}
	}
}

func writeSupervisorState(t *testing.T, install string, st superviseState) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(install, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(st)
	if err := os.WriteFile(supervisorStatePath(install), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCheckSupervisorHealth(t *testing.T) {
	prev := supervisorRequestWait
	supervisorRequestWait = 0
	t.Cleanup(func() { supervisorRequestWait = prev })

	install := t.TempDir()
	if r := checkSupervisorHealth(&healContext{InstallDir: install}); r.Status != healOK {
		t.Fatalf("no state: %+v", r)
	}

	started := time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)
	writeSupervisorState(t, install, superviseState{PID: os.Getpid(), Children: map[string]*superviseChildState{
		"api": {Name: "api", State: "running", StartedAt: started},
		"ui":  {Name: "ui", State: "cooldown", RestartsInWindow: 6, NextStartAt: "soon", LastExitReason: "crashed", LastExitDetail: "exit status 1", LastExitLogTail: []string{"panic: boom", "goroutine 1 [running]:"}},
	}})
	r := checkSupervisorHealth(&healContext{InstallDir: install})
	if r.Status != healFail || !strings.Contains(r.Message, "api up 3h") || !strings.Contains(r.Message, "panic: boom") {
		t.Fatalf("cooldown: %+v", r)
	}
	r = checkSupervisorHealth(&healContext{InstallDir: install, Fix: true})
	if !r.Fixed || !fileExists(supervisorRestartRequestPath(install, "ui")) || fileExists(supervisorRestartRequestPath(install, "api")) {
		t.Fatalf("fix should request a ui restart only: %+v", r)
	}

	// A state file whose supervisor is gone is stale.
	writeSupervisorState(t, install, superviseState{PID: 99999999, Children: map[string]*superviseChildState{}})
	if r := checkSupervisorHealth(&healContext{InstallDir: install}); r.Status != healWarn {
		t.Fatalf("stale: %+v", r)
	}
	if r := checkSupervisorHealth(&healContext{InstallDir: install, Fix: true}); !r.Fixed || fileExists(supervisorStatePath(install)) {
		t.Fatalf("stale fix: %+v", r)
	}
}

func TestWaitOrRestartRequestEndsEarly(t *testing.T) {
	install := t.TempDir()
	if err := os.MkdirAll(filepath.Join(install, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &superviseConfig{installDir: install, requestPoll: 20 * time.Millisecond}
	c := &superviseChild{name: "api"}
	go func() {
		time.Sleep(60 * time.Millisecond)
		_ = os.WriteFile(supervisorRestartRequestPath(install, "api"), []byte("x"), 0o644)
	}()
	start := time.Now()
	if !waitOrRestartRequest(context.Background(), cfg, c, time.Minute) {
		t.Fatal("wait reported shutdown")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("waited %s; the request should end the wait", time.Since(start))
	}
	if fileExists(supervisorRestartRequestPath(install, "api")) {
		t.Fatal("request file not consumed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitOrRestartRequest(ctx, cfg, c, time.Minute) {
		t.Fatal("cancelled context should end the wait with false")
	}
}

func TestSupervisorStateWriter(t *testing.T) {
	install := t.TempDir()
	if err := os.MkdirAll(filepath.Join(install, "run"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &superviseConfig{children: []*superviseChild{{name: "api", healthArgs: []string{"x"}}, {name: "ui"}}}
	w := newSuperviseStateWriter(supervisorStatePath(install), cfg)
	w.update("api", func(s *superviseChildState) { s.State = "running"; s.PID = 42 })
	for i := 0; i < supervisorEventLimit+5; i++ {
		w.event("api", "started", "")
	}
	st, err := readSupervisorState(install)
	if err != nil {
		t.Fatal(err)
	}
	if st.PID != os.Getpid() || st.Children["api"].PID != 42 || !st.Children["api"].Probed || st.Children["ui"].Probed || len(st.Events) != supervisorEventLimit {
		t.Fatalf("state = %+v", st)
	}
	w.remove()
	if fileExists(supervisorStatePath(install)) {
		t.Fatal("remove left the file")
	}
	var nilWriter *superviseStateWriter
	nilWriter.update("api", func(*superviseChildState) {})
	nilWriter.event("api", "x", "")
}

func TestTailLines(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.log")
	if err := os.WriteFile(p, []byte("a\r\n\r\nb\nc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := tailLines(p, 2); strings.Join(got, ",") != "b,c" {
		t.Fatalf("tail = %v", got)
	}
	if tailLines(filepath.Join(t.TempDir(), "missing"), 3) != nil {
		t.Fatal("missing file should give nil")
	}
}

func TestPlatformChecksNeverFailOnThisHost(t *testing.T) {
	hc := &healContext{InstallDir: t.TempDir()}
	for _, fn := range []func(*healContext) healResult{checkPlatformArch, checkOSLimits, checkInodes, checkMemoryPressure, checkBinaryQuarantine, checkCgroupLimits} {
		r := fn(hc)
		if r.ID == "" || r.Message == "" || r.Status == healFail {
			t.Errorf("%s: %+v", r.ID, r)
		}
	}
}
