package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// LIN-02 (U5): the stack's unit runs as the service user, with no
// capabilities and the programs read-only.
func TestUnprivilegedSupervisorUnit(t *testing.T) {
	install := "/opt/ncc" // not under /tmp, which keeps the shared /tmp instead
	o := installServiceOptions{InstallDir: install, ServiceName: "ncc-orchestrator", OrchestratorBin: install + "/bin/ncc-orchestrator", ServiceUser: "ncc"}
	got := serviceSection(t, systemdSupervisorUnit(o))
	want := map[string]string{
		"User":                  "ncc",
		"Group":                 "ncc",
		"NoNewPrivileges":       "yes",
		"ProtectSystem":         "strict",
		"ProtectHome":           "read-only",
		"CapabilityBoundingSet": "",
		"AmbientCapabilities":   "",
		"SystemCallFilter":      "@system-service",
		"PrivateDevices":        "yes",
		"ProtectProc":           "invisible",
		"RemoveIPC":             "yes",
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Errorf("%s = %q (set %v), want %q", k, g, ok, v)
		}
	}
	if !strings.Contains(" "+got["ReadWritePaths"]+" ", " -"+install+" ") {
		t.Errorf("ReadWritePaths = %q, want the install", got["ReadWritePaths"])
	}
	for _, tree := range serviceRootTrees {
		if !strings.Contains(got["ReadOnlyPaths"], "-"+install+"/"+tree) {
			t.Errorf("ReadOnlyPaths = %q, want %s read-only", got["ReadOnlyPaths"], tree)
		}
	}
	for _, k := range []string{"DynamicUser", "PrivateNetwork", "InaccessiblePaths"} {
		if v, ok := got[k]; ok {
			t.Errorf("%s=%s must not be set", k, v)
		}
	}
}

// A port below 1024 gets CAP_NET_BIND_SERVICE and nothing else.
func TestUnprivilegedUnitLowPort(t *testing.T) {
	dir := t.TempDir()
	b, _ := json.Marshal(map[string]string{"api_listen": "0.0.0.0:443", "ui_listen": "0.0.0.0:8080"})
	if err := os.WriteFile(filepath.Join(dir, ".ncc-v2-start.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if st, ok := loadV2StartState(dir); !ok || st.APIListen != "0.0.0.0:443" {
		t.Skipf("start state format changed: %+v", st)
	}
	if !listenNeedsBind(dir) {
		t.Fatal("port 443 needs CAP_NET_BIND_SERVICE")
	}
	got := serviceSection(t, strings.Join(append([]string{"[Service]"}, systemdUnprivilegedHardening("/opt/ncc", "ncc", true)...), "\n"))
	if got["CapabilityBoundingSet"] != "CAP_NET_BIND_SERVICE" || got["AmbientCapabilities"] != "CAP_NET_BIND_SERVICE" {
		t.Errorf("capabilities %q / %q", got["CapabilityBoundingSet"], got["AmbientCapabilities"])
	}
	if listenNeedsBind(t.TempDir()) {
		t.Error("no saved state needs no capability")
	}
}

// The root opt-out writes the unit of 2.4 plus User=root: nothing else changes.
func TestRootServiceUnitIsUnchanged(t *testing.T) {
	o := installServiceOptions{InstallDir: "/opt/ncc", ServiceName: "ncc-orchestrator", OrchestratorBin: "/opt/ncc/bin/ncc-orchestrator"}
	legacy := systemdSupervisorUnit(o)
	if strings.Contains(legacy, "User=") || !strings.Contains(legacy, "ProtectSystem=full") {
		t.Fatalf("the unit without a user is not the root unit:\n%s", legacy)
	}
	o.ServiceUser = "root"
	optOut := systemdSupervisorUnit(o)
	if serviceSection(t, optOut)["User"] != "root" {
		t.Errorf("the opt-out does not say User=root:\n%s", optOut)
	}
	var rest []string
	for _, l := range strings.Split(optOut, "\n") {
		if l != "User=root" && !strings.HasPrefix(l, "#") {
			rest = append(rest, l)
		}
	}
	var legacyLines []string
	for _, l := range strings.Split(legacy, "\n") {
		if !strings.HasPrefix(l, "#") {
			legacyLines = append(legacyLines, l)
		}
	}
	if strings.Join(rest, "\n") != strings.Join(legacyLines, "\n") {
		t.Errorf("the opt-out differs from the root unit:\n%s\nvs\n%s", optOut, legacy)
	}
}

// Units of 2.3.x, 2.4.0-rc and 2.4.0 (root) are replaced by the unprivileged
// unit without asking; a line the administrator added is still kept.
func TestPlanUnitToServiceUser(t *testing.T) {
	root := installServiceOptions{InstallDir: "/opt/ncc", ServiceName: "ncc-orchestrator", OrchestratorBin: "/opt/ncc/bin/ncc-orchestrator"}
	ncc := root
	ncc.ServiceUser = "ncc"
	desired := systemdSupervisorUnit(ncc)
	head := systemdSupervisorUnit(root)
	rc := strings.Replace(head, "ReadWritePaths=-/opt/ncc", "ReadWritePaths=-/opt/ncc -/data/ncc-reports", 1)
	for _, tc := range []struct{ name, installed, action string }{
		{"2.4.0 root unit", head, defReplace},
		{"2.4.0-rc unit with configured folders", rc, defReplace},
		{"2.3.0", unitOf("2.3.0", root), defReplace},
		{"2.2.1", unitOf("2.2.1", root), defReplace},
		{"already ncc", desired, defUnchanged},
		{"own environment", strings.Replace(head, "[Service]", "[Service]\nEnvironment=HTTPS_PROXY=http://p:3128", 1), defMerge},
		{"other user", strings.Replace(head, "[Service]", "[Service]\nUser=svcacct", 1), defKeep},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := planUnit("Service", filepath.Join(t.TempDir(), "ncc.service"), tc.installed, desired, "/opt/ncc")
			if c.Action != tc.action {
				t.Errorf("%s (%s), want %s", c.Action, c.Detail, tc.action)
			}
		})
	}
}

func TestRetiredUnitLine(t *testing.T) {
	for text, want := range map[string]bool{
		systemdCapabilityLine:                    true,
		"ProtectSystem=full":                     true,
		"User=root":                              true,
		"User=ncc":                               true,
		"Group=ncc":                              true,
		"ReadWritePaths=-/opt/ncc":               true,
		"ReadWritePaths=-/opt/ncc -/data/out":    true,
		"ReadWritePaths=/opt/ncc":                false, // not ours: no "-"
		"ReadWritePaths=-/data/out":              false, // not the unit's folder
		"ReadWritePaths=-/opt/ncc /etc/shadow":   false,
		"Environment=HTTPS_PROXY=http://p:3128":  false,
		"User=svcacct":                           false,
		"CapabilityBoundingSet=CAP_NET_RAW":      false,
		"ProtectSystem=strict":                   false,
		"ReadWritePaths=-/opt/ncc-other -/x/out": false,
	} {
		if got := retiredUnitLine(unitLine{section: "[Service]", text: text}, "/opt/ncc"); got != want {
			t.Errorf("%q: %v, want %v", text, got, want)
		}
	}
}

func TestServiceUserChoice(t *testing.T) {
	setting := filepath.Join(t.TempDir(), "service-user")
	old := serviceUserSetting
	serviceUserSetting = setting
	t.Cleanup(func() { serviceUserSetting = old })
	t.Setenv(serviceUserEnv, "")

	dir := t.TempDir()
	if u, _ := desiredServiceUser(dir, "[Service]\nUser=svcacct\n"); u != "svcacct" {
		t.Errorf("installed unit's user: %q", u)
	}
	if err := os.WriteFile(setting, []byte("root\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if u, _ := desiredServiceUser(dir, "[Service]\nUser=ncc\n"); u != "root" {
		t.Errorf("setting file: %q", u)
	}
	t.Setenv(serviceUserEnv, "ncc")
	if u, _ := desiredServiceUser(dir, ""); u != "ncc" {
		t.Errorf("environment: %q", u)
	}
	t.Setenv(serviceUserEnv, "bad user;")
	if u := serviceUserOverride(); u != "root" {
		t.Errorf("an invalid environment value falls back to the file: %q", u)
	}
	for _, bad := range []string{"Root", "a b", "../x", "x\ny", strings.Repeat("a", 40)} {
		if _, err := normalizeServiceUser(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func goodOps(op string) opsRequest {
	r := opsRequest{ID: strings.Repeat("ab", 16), Op: op}
	switch op {
	case opScheduleInstall:
		r.Task, r.OnCalendar = "prod", "*-*-* *:0/30:00"
		r.Script, r.WorkDir = "/opt/ncc/logs/ncc-sched-prod.sh", "/opt/ncc"
	case opScheduleRemove:
		r.Task = "prod"
	case opUpgrade:
		r.Target, r.JobID = "2.5.0", strings.Repeat("cd", 16)
		r.BackupPath = "/opt/ncc/backups/pre-update-20261127T120000Z.tar.gz"
	}
	return r
}

func TestOpsRequestValidation(t *testing.T) {
	for _, op := range []string{opScheduleInstall, opScheduleRemove, opRestart, opAdopt, opUpgrade, opRollback} {
		if err := goodOps(op).validate("/opt/ncc"); err != nil {
			t.Errorf("%s: %v", op, err)
		}
	}
	bad := map[string]func(*opsRequest){
		"id":                 func(r *opsRequest) { r.ID = "../../etc/x" },
		"op":                 func(r *opsRequest) { r.Op = "shell" },
		"task":               func(r *opsRequest) { r.Task = "prod; rm -rf /" },
		"calendar newline":   func(r *opsRequest) { r.OnCalendar = "daily\nExecStartPre=/bin/sh" },
		"calendar specifier": func(r *opsRequest) { r.OnCalendar = "%h" },
		"script elsewhere":   func(r *opsRequest) { r.Script = "/tmp/evil.sh" },
		"script dot dot":     func(r *opsRequest) { r.Script = "/opt/ncc/../root/ncc-sched-prod.sh" },
		"script quote":       func(r *opsRequest) { r.Script = "/opt/ncc/logs/x\"/ncc-sched-prod.sh" },
		"workdir relative":   func(r *opsRequest) { r.WorkDir = "opt/ncc" },
		"extra field":        func(r *opsRequest) { r.Target = "2.5.0" },
	}
	for name, mutate := range bad {
		r := goodOps(opScheduleInstall)
		mutate(&r)
		if err := r.validate("/opt/ncc"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	up := map[string]func(*opsRequest){
		"backup elsewhere": func(r *opsRequest) { r.BackupPath = "/etc/pre-update-20261127T120000Z.tar.gz" },
		"backup name":      func(r *opsRequest) { r.BackupPath = "/opt/ncc/backups/x.tar.gz" },
		"target":           func(r *opsRequest) { r.Target = "2.5.0 --bundle /tmp/x" },
		"job id":           func(r *opsRequest) { r.JobID = "../x" },
		"requested_by":     func(r *opsRequest) { r.RequestedBy = "a\nb" },
		"schedule field":   func(r *opsRequest) { r.Script = "/opt/ncc/x.sh" },
	}
	for name, mutate := range up {
		r := goodOps(opUpgrade)
		mutate(&r)
		if err := r.validate("/opt/ncc"); err == nil {
			t.Errorf("upgrade %s accepted", name)
		}
	}
	r := goodOps(opRestart)
	r.Task = "prod"
	if r.validate("/opt/ncc") == nil {
		t.Error("restart with a task accepted")
	}
}

func TestDecodeOpsRequest(t *testing.T) {
	good, _ := json.Marshal(goodOps(opRestart))
	if _, err := decodeOpsRequest(good); err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string]string{
		"unknown field": `{"id":"` + strings.Repeat("ab", 16) + `","op":"restart","exec":"/bin/sh"}`,
		"trailing":      string(good) + `{}`,
		"not json":      `restart`,
		"too large":     `{"id":"` + strings.Repeat("a", maxOpsRequest) + `"}`,
	} {
		if _, err := decodeOpsRequest([]byte(b)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestOwnershipRecordRoundTrip(t *testing.T) {
	rec := ownershipRecord{DefaultUID: 0, DefaultGID: 0, Entries: []ownEntry{{Path: ".", UID: 0, GID: 0, Mode: 0o755}, {Path: "logs", UID: 0, GID: 0, Mode: 0o755, Tree: true}}}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var back ownershipRecord
	if err := json.Unmarshal(b, &back); err != nil || len(back.Entries) != 2 || !back.Entries[1].Tree {
		t.Fatalf("%+v %v", back, err)
	}
	for p, want := range map[string]bool{".": true, "logs": true, "run/v2.pid": true, "../etc": false, "/etc": false, "a/../../b": false, "": false} {
		if got := ownRelPathOK(p); got != want {
			t.Errorf("ownRelPathOK(%q) = %v, want %v", p, got, want)
		}
	}
}
