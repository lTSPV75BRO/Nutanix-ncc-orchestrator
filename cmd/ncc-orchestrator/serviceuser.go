package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// LIN-02 (phase U5 of docs/UPGRADE_HELPER_PLAN.md): on Linux with systemd the
// supervisor, API, UI, NCC runs, schedules and log maintenance run as a system
// user of their own, ncc. They cannot change their own programs (bin/,
// frontend-dist/, helper/ stay root's) or anything outside the folders they
// write. What needs root is done by root units that take no instructions from
// the service other than validated request files: the update helper
// (run/upgrade) and the service helper (run/ops, serviceops.go).
//
// `v2-install-service --service-user root`, NCC_SERVICE_USER=root or the file
// /etc/ncc-orchestrator/service-user keep the service as root, as before LIN-02.

const (
	defaultServiceUser = "ncc"
	serviceUserEnv     = "NCC_SERVICE_USER"
)

// serviceUserSetting is root's choice for upgrades and adoption. It lives
// outside the install folder, which the service can write.
var serviceUserSetting = "/etc/ncc-orchestrator/service-user"

var serviceUserPattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,30}$`)

// normalizeServiceUser checks a --service-user value ("" stays "").
func normalizeServiceUser(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	if !serviceUserPattern.MatchString(s) {
		return "", fmt.Errorf("invalid service user %q (a system user name such as ncc, or root)", s)
	}
	return s, nil
}

// serviceUserOverride is the operator's choice: the environment, else the
// setting file ("" when neither says anything usable).
func serviceUserOverride() string {
	if u, err := normalizeServiceUser(os.Getenv(serviceUserEnv)); err == nil && u != "" {
		return u
	}
	b, err := os.ReadFile(serviceUserSetting)
	if err != nil || len(b) > 256 {
		return ""
	}
	line, _, _ := strings.Cut(string(b), "\n")
	if u, err := normalizeServiceUser(line); err == nil {
		return u
	}
	return ""
}

// unitServiceUser is the User= of a unit ("" when it has none).
func unitServiceUser(unit string) string {
	user := ""
	for _, l := range unitLines(unit) {
		if l.section == "[Service]" && l.key() == "User" {
			_, v, _ := strings.Cut(l.text, "=")
			user = strings.TrimSpace(v)
		}
	}
	return user
}

// rootService: the stack runs as root ("" is a unit from before LIN-02).
func rootService(user string) bool { return user == "" || user == "root" }

// errInstallUnreachable: the service user cannot even reach the install folder.
var errInstallUnreachable = errors.New("unreachable")

// installReachable reports whether an unprivileged user can get to installDir:
// every folder above it must let others pass (an install under /root does not).
func installReachable(installDir string) error {
	dir := filepath.Clean(installDir)
	for {
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		st, err := os.Stat(parent)
		if err != nil {
			return err
		}
		if st.Mode().Perm()&0o001 == 0 {
			return fmt.Errorf("%w: %s (mode %04o) does not let other users through", errInstallUnreachable, parent, st.Mode().Perm())
		}
		dir = parent
	}
}

// desiredServiceUser decides as whom the stack runs: the operator's choice,
// else the installed unit's, else ncc when it can reach the install, else root.
func desiredServiceUser(installDir, installedUnit string) (user, note string) {
	if u := serviceUserOverride(); u != "" {
		return u, ""
	}
	if u := unitServiceUser(installedUnit); u != "" {
		return u, ""
	}
	if err := installReachable(installDir); err != nil {
		return "root", fmt.Sprintf("the service stays root: %v; move the install, or set %s", err, serviceUserSetting)
	}
	return defaultServiceUser, ""
}

// serviceGroup is the primary group of user (its own name when it does not
// exist yet: it is created with a group of its own).
func serviceGroup(user string) string {
	if g := lookupPrimaryGroup(user); g != "" {
		return g
	}
	return user
}

// listenNeedsBind reports whether the saved API or UI address is a port below
// 1024, which an unprivileged process can only open with CAP_NET_BIND_SERVICE.
func listenNeedsBind(installDir string) bool {
	st, ok := loadV2StartState(installDir)
	if !ok {
		return false
	}
	for _, a := range []string{st.APIListen, st.UIListen} {
		if i := strings.LastIndex(a, ":"); i >= 0 {
			var n int
			if _, err := fmt.Sscanf(a[i+1:], "%d", &n); err == nil && n > 0 && n < 1024 {
				return true
			}
		}
	}
	return false
}

// Trees an unprivileged unit never writes, even when the configuration names
// a folder in them.
var systemdStrictDenied = []string{"/usr", "/boot", "/efi", "/etc", "/proc", "/sys", "/dev", "/run", "/bin", "/sbin", "/lib", "/lib64", "/lib32", "/libx32"}

// Folders of the install that stay root's: the service runs them, so it must
// not be able to change them.
var serviceRootTrees = []string{"bin", "frontend-dist", "helper"}

// systemdUnprivilegedHardening is the sandbox of a unit that runs the stack, or
// a part of it, as user: no capabilities (CAP_NET_BIND_SERVICE for a port
// below 1024), the whole file system read-only except the install folder and
// the folders its configuration names, and the programs read-only in it.
func systemdUnprivilegedHardening(installDir, user string, bindLow bool, writable ...string) []string {
	lines := []string{
		"# Sandboxing. Older systemd ignores settings it does not know. To relax one,",
		"# use `systemctl edit` (the drop-in survives a reinstall), not this file.",
		"User=" + user,
		"Group=" + serviceGroup(user),
		"NoNewPrivileges=yes",
		"RestrictSUIDSGID=yes",
		"LockPersonality=yes",
		"RestrictRealtime=yes",
		"RestrictNamespaces=yes",
		"RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK",
		"SystemCallArchitectures=native",
		"SystemCallFilter=@system-service",
		"SystemCallErrorNumber=EPERM",
		"MemoryDenyWriteExecute=yes",
		"PrivateDevices=yes",
		"ProtectProc=invisible",
		"RemoveIPC=yes",
	}
	if bindLow {
		lines = append(lines, "CapabilityBoundingSet=CAP_NET_BIND_SERVICE", "AmbientCapabilities=CAP_NET_BIND_SERVICE")
	} else {
		lines = append(lines, "CapabilityBoundingSet=", "AmbientCapabilities=")
	}
	lines = append(lines, systemdKernelHardening()...)
	return append(lines, systemdStrictFilesystem(installDir, writable...)...)
}

// systemdStrictFilesystem: ProtectSystem=strict with ReadWritePaths for the
// install, the given folders and the configured ones, and ReadOnlyPaths for
// the programs inside the install.
func systemdStrictFilesystem(installDir string, writable ...string) []string {
	var rw []string
	sharedTmp := false
	add := func(p string, configured bool) {
		p = strings.TrimSpace(filepath.ToSlash(p))
		if !strings.HasPrefix(p, "/") || strings.ContainsAny(p, "\"\\\n\r\t") {
			return
		}
		p = path.Clean(p)
		if withinAny(p, systemdTmpTrees) {
			sharedTmp = true
			return
		}
		if configured && !existingDir(p) {
			p = path.Dir(p)
		}
		if p == "/" || (configured && (path.Dir(p) == "/" || withinAny(p, systemdStrictDenied))) {
			return
		}
		rw = append(rw, p)
	}
	add(installDir, false)
	for _, w := range writable {
		add(w, false)
	}
	for _, p := range configuredPaths(installDir) {
		add(p, true)
	}
	lines := []string{"ProtectSystem=strict", "ProtectHome=read-only"}
	if !sharedTmp {
		lines = append(lines, "PrivateTmp=yes")
	}
	if rw = outermostPaths(rw); len(rw) > 0 {
		words := make([]string, 0, len(rw))
		for _, p := range rw {
			words = append(words, systemdPathWord("-"+p))
		}
		lines = append(lines, "ReadWritePaths="+strings.Join(words, " "))
	}
	base := path.Clean(filepath.ToSlash(installDir))
	if strings.HasPrefix(base, "/") {
		words := make([]string, 0, len(serviceRootTrees))
		for _, t := range serviceRootTrees {
			words = append(words, systemdPathWord("-"+path.Join(base, t)))
		}
		lines = append(lines, "ReadOnlyPaths="+strings.Join(words, " "))
	}
	return lines
}

// serviceHardening is the sandbox of a unit that runs a part of the stack (a
// schedule, log maintenance) for the service user; it opens no ports.
func serviceHardening(installDir, user string, writable ...string) []string {
	if rootService(user) {
		return systemdHardening(installDir, writable...)
	}
	return systemdUnprivilegedHardening(installDir, user, false, writable...)
}

// retiredUnitLine reports a line that an earlier release wrote and that a
// migration drops without asking: what 2.4 wrote for the root service, which
// the unprivileged unit replaces. dirs are the folders the unit was written
// for (install, working directory).
func retiredUnitLine(l unitLine, dirs ...string) bool {
	if retiredUnitLines[l.text] {
		return true
	}
	switch l.text {
	case systemdCapabilityLine, "ProtectSystem=full", "User=root", "User=" + defaultServiceUser, "Group=" + defaultServiceUser:
		return true
	}
	// 2.4.0-rc.3 listed the unit's folders and those of the configuration,
	// which differ from host to host, each with a leading "-".
	if l.key() != "ReadWritePaths" || len(dirs) == 0 {
		return false
	}
	ours := false
	for _, w := range strings.Fields(strings.TrimPrefix(l.text, "ReadWritePaths=")) {
		w = strings.Trim(w, `"`)
		if !strings.HasPrefix(w, "-/") {
			return false
		}
		for _, d := range dirs {
			if d != "" && w == "-"+path.Clean(filepath.ToSlash(d)) {
				ours = true
			}
		}
	}
	return ours
}
