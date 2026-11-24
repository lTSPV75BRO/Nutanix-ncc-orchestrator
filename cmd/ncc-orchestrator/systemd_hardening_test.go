package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// serviceSection returns the directives of a unit's [Service] section.
func serviceSection(t *testing.T, unit string) map[string]string {
	t.Helper()
	out := map[string]string{}
	in := false
	for _, l := range strings.Split(unit, "\n") {
		l = strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(l, "["):
			in = l == "[Service]"
		case in && l != "" && !strings.HasPrefix(l, "#"):
			k, v, ok := strings.Cut(l, "=")
			if !ok {
				t.Fatalf("malformed line %q in:\n%s", l, unit)
			}
			if _, dup := out[k]; dup {
				t.Fatalf("%s set twice in:\n%s", k, unit)
			}
			out[k] = v
		}
	}
	return out
}

// LIN-01: the generated services are sandboxed.
func TestSystemdUnitsAreHardened(t *testing.T) {
	o := installServiceOptions{InstallDir: "/opt/ncc", ServiceName: "ncc-orchestrator", OrchestratorBin: "/opt/ncc/bin/ncc-orchestrator"}
	schedule, _ := buildSystemdScheduleUnits("prod", "*-*-* 0/4:00:00", "/opt/ncc/logs/s.sh", "/opt/ncc", scheduleMarker("prod"))
	process := map[string]string{
		"NoNewPrivileges":         "yes",
		"RestrictSUIDSGID":        "yes",
		"LockPersonality":         "yes",
		"RestrictRealtime":        "yes",
		"RestrictNamespaces":      "yes",
		"RestrictAddressFamilies": "AF_UNIX AF_INET AF_INET6 AF_NETLINK",
		"SystemCallArchitectures": "native",
	}
	kernel := map[string]string{
		"ProtectKernelTunables": "yes",
		"ProtectKernelModules":  "yes",
		"ProtectKernelLogs":     "yes",
		"ProtectControlGroups":  "yes",
		"ProtectClock":          "yes",
	}
	for name, tc := range map[string]struct {
		unit   string
		kernel bool
	}{
		"supervisor":      {systemdSupervisorUnit(o), true},
		"log maintenance": {systemdLogMaintenanceServiceUnit(o), true},
		"schedule":        {schedule, true},
		"apply-update":    {systemdApplyUpdateOneshotUnit(o), false},
	} {
		t.Run(name, func(t *testing.T) {
			got := serviceSection(t, tc.unit)
			for k, v := range process {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
			for k, v := range kernel {
				if !tc.kernel {
					v = ""
				}
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
			// Each of these breaks a supported setup (see systemd_hardening.go).
			for _, k := range []string{"ProtectSystem", "ProtectHome", "PrivateTmp", "ProtectHostname", "MemoryDenyWriteExecute", "User", "DynamicUser", "UMask", "PrivateNetwork", "ReadOnlyPaths", "InaccessiblePaths"} {
				if v, ok := got[k]; ok {
					t.Errorf("%s=%s must not be set", k, v)
				}
			}
		})
	}
}

// The capability list removes by name; it must not become an allow list or
// drop what writes outside the install dir, ports below 1024, limits,
// systemctl, or signalling a run need.
func TestSystemdCapabilityBoundingSet(t *testing.T) {
	set := serviceSection(t, systemdSupervisorUnit(installServiceOptions{InstallDir: "/opt/ncc", OrchestratorBin: "/opt/ncc/bin/ncc-orchestrator"}))["CapabilityBoundingSet"]
	if !strings.HasPrefix(set, "~") {
		t.Fatalf("CapabilityBoundingSet is not a deny list: %q", set)
	}
	dropped := map[string]bool{}
	for _, c := range strings.Fields(strings.TrimPrefix(set, "~")) {
		dropped[c] = true
	}
	for _, c := range []string{"CAP_SYS_MODULE", "CAP_SYS_BOOT", "CAP_SYS_TIME", "CAP_SYS_RAWIO", "CAP_MAC_ADMIN", "CAP_NET_RAW"} {
		if !dropped[c] {
			t.Errorf("%s is not dropped", c)
		}
	}
	for _, c := range []string{"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_FSETID", "CAP_KILL", "CAP_SETUID", "CAP_SETGID", "CAP_NET_BIND_SERVICE", "CAP_SYS_ADMIN", "CAP_SYS_RESOURCE", "CAP_SYS_PTRACE"} {
		if dropped[c] {
			t.Errorf("%s must be kept", c)
		}
	}
}

// Existing installs keep their unit: doctor --fix must not add the sandbox.
func TestServiceConfigDoesNotAddHardening(t *testing.T) {
	old := "[Unit]\nDescription=NCC\n\n[Service]\nExecStart=/opt/ncc/bin/ncc-orchestrator v2-supervise --install-dir /opt/ncc\n"
	patched := patchSystemdUnit(old, systemdUnitGaps(old))
	if strings.Contains(patched, "NoNewPrivileges") || strings.Contains(patched, "Protect") {
		t.Fatalf("doctor rewrote the sandbox of an existing unit:\n%s", patched)
	}
}

// The VM templates called v2-install-service with flags it does not have, so
// the first boot failed (INS-02). They leave units to the service installer.
func TestVMTemplatesUseKnownFlags(t *testing.T) {
	flag := regexp.MustCompile(`--[a-z][a-z-]*`)
	for _, file := range []string{"../../deploy/cloud-init/ncc-v2.yaml", "../../deploy/windows/setup-ncc.ps1"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
		calls := 0
		for n, l := range lines {
			at := strings.Index(l, "v2-install-service\"")
			if at < 0 {
				at = strings.Index(l, "v2-install-service --")
			}
			if at < 0 {
				continue
			}
			calls++
			cmd, _, err := newRootCmd().Find([]string{"v2-install-service"})
			if err != nil {
				t.Fatal(err)
			}
			// The optional arguments are in the lines before, or the line after.
			text := strings.Join(lines[max(0, n-2):n], "\n") + l[at:] + lines[n+1]
			for _, f := range flag.FindAllString(text, -1) {
				if cmd.Flags().Lookup(strings.TrimPrefix(f, "--")) == nil {
					t.Errorf("%s: v2-install-service has no flag %s", file, f)
				}
			}
		}
		if calls != 1 {
			t.Errorf("%s: %d calls of v2-install-service", file, calls)
		}
		for _, bad := range []string{"--self-heal", "/etc/systemd/system", "secrets-provider: env"} {
			if strings.Contains(string(raw), bad) {
				t.Errorf("%s: %q", file, bad)
			}
		}
	}
}
