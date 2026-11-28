package main

import (
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Sandboxing for the generated systemd units. The service stays root: it has to
// write units in /etc/systemd/system, call systemctl and relabel binaries. Every
// child (API, UI, NCC runs, chcon, crontab, systemctl) inherits these settings.
//
// Deliberately not set, because a supported setup would break:
//   - ProtectSystem=strict: output, log and backup folders may be anywhere.
//     "full" is set instead, with the folders the stack writes in
//     ReadWritePaths (see systemdFilesystemHardening).
//   - ProtectHome=yes: installs under /root and /home are supported, and
//     ReadWritePaths cannot reopen a folder that is inaccessible. "read-only"
//     is set instead.
//   - ProtectHostname: freezes the host name used in alerts and the audit log.
//   - MemoryDenyWriteExecute: breaks the PCRE JIT that the SELinux tools use.
//   - UMask: changes the permissions of reports and logs that others read.
//   - CAP_SYS_RESOURCE and CAP_SYS_PTRACE are kept: raising limits needs the
//     first, reading another process under /proc mounted with hidepid the
//     second. CAP_SYS_ADMIN is dropped (LIN-02): systemctl run as root is
//     authorized by its user id over the private socket or D-Bus, not by a
//     capability, and chcon to an existing type needs neither it nor
//     CAP_MAC_ADMIN.

// systemdProcessHardening uses seccomp filters and capabilities only, so it
// also works where systemd cannot create a mount namespace.
func systemdProcessHardening() []string {
	return []string{
		"NoNewPrivileges=yes",
		"RestrictSUIDSGID=yes",
		"LockPersonality=yes",
		"RestrictRealtime=yes",
		"RestrictNamespaces=yes",
		// Go and glibc list network interfaces and routes over netlink.
		"RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK",
		"SystemCallArchitectures=native",
		systemdCapabilityLine,
	}
}

const systemdCapabilityLine = "CapabilityBoundingSet=~CAP_SYS_ADMIN CAP_SYS_MODULE CAP_SYS_BOOT CAP_SYS_TIME CAP_SYS_RAWIO CAP_SYS_PACCT CAP_SYS_TTY_CONFIG CAP_SYSLOG CAP_WAKE_ALARM CAP_BLOCK_SUSPEND CAP_MAC_ADMIN CAP_NET_ADMIN CAP_NET_RAW"

// systemdCapabilityLine240rc is the line 2.4.0-rc.1 and rc.2 wrote, which kept
// CAP_SYS_ADMIN. The update helper replaces it without asking.
const systemdCapabilityLine240rc = "CapabilityBoundingSet=~CAP_SYS_MODULE CAP_SYS_BOOT CAP_SYS_TIME CAP_SYS_RAWIO CAP_SYS_PACCT CAP_SYS_TTY_CONFIG CAP_SYSLOG CAP_WAKE_ALARM CAP_BLOCK_SUSPEND CAP_MAC_ADMIN CAP_NET_ADMIN CAP_NET_RAW"

// systemdKernelHardening needs a mount namespace.
func systemdKernelHardening() []string {
	return []string{
		"ProtectKernelTunables=yes",
		"ProtectKernelModules=yes",
		"ProtectKernelLogs=yes",
		"ProtectControlGroups=yes",
		"ProtectClock=yes",
	}
}

// Trees that ProtectSystem=full and ProtectHome=read-only make read-only, and
// the ones that PrivateTmp replaces.
var (
	systemdReadOnlyTrees = []string{"/usr", "/boot", "/efi", "/etc", "/home", "/root", "/run/user"}
	systemdTmpTrees      = []string{"/tmp", "/var/tmp"}
)

// unixPathWithin reports whether p is dir or below it (both slash paths).
func unixPathWithin(p, dir string) bool {
	return p == dir || strings.HasPrefix(p, strings.TrimSuffix(dir, "/")+"/")
}

func withinAny(p string, dirs []string) bool {
	for _, d := range dirs {
		if unixPathWithin(p, d) {
			return true
		}
	}
	return false
}

// systemdFilesystemHardening makes /usr, /boot, /efi, /etc and the home folders
// read-only and gives the unit its own /tmp (it needs a mount namespace, like
// the kernel settings). The unit can still write its install folder, the given
// folders, and the folders that the install's configuration names (output,
// logs, token file, ...). An install, or a configured folder, under /tmp or
// /var/tmp keeps the shared /tmp. A folder configured later is only added when
// the unit is written again: by the update helper, v2-install-service, or when
// a schedule is saved; until then it can be allowed with a drop-in.
func systemdFilesystemHardening(installDir string, writable ...string) []string {
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
		if !configured {
			rw = append(rw, p)
			return
		}
		// A configured value may be a file: allow its folder. Never a whole
		// protected tree, or anything at the top level.
		if !existingDir(p) {
			p = path.Dir(p)
		}
		for _, root := range systemdReadOnlyTrees {
			if p == root {
				return
			}
		}
		if withinAny(p, systemdReadOnlyTrees) && path.Dir(p) != "/" {
			rw = append(rw, p)
		}
	}
	add(installDir, false)
	for _, w := range writable {
		add(w, false)
	}
	for _, p := range configuredPaths(installDir) {
		add(p, true)
	}
	lines := []string{"ProtectSystem=full", "ProtectHome=read-only"}
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
	return lines
}

// outermostPaths sorts the paths and drops those inside another one.
func outermostPaths(paths []string) []string {
	sort.Strings(paths)
	var out []string
	for _, p := range paths {
		if len(out) > 0 && unixPathWithin(p, out[len(out)-1]) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// systemdPathWord quotes one entry of a path list such as ReadWritePaths.
func systemdPathWord(p string) string {
	p = strings.ReplaceAll(p, "%", "%%")
	if strings.ContainsAny(p, " '") {
		return `"` + p + `"`
	}
	return p
}

// configuredPaths lists the absolute paths that the install's configuration
// names: config files, environments and the saved start settings. Values of
// keys that hold passwords are skipped.
func configuredPaths(installDir string) []string {
	if strings.TrimSpace(installDir) == "" {
		return nil
	}
	files := configFiles(installDir)
	if more, err := filepath.Glob(filepath.Join(installDir, "configs", "environments", "*.yaml")); err == nil {
		files = append(files, more...)
	}
	var out []string
	var walk func(key string, v any)
	walk = func(key string, v any) {
		low := strings.ToLower(key)
		if strings.Contains(low, "password") || strings.Contains(low, "passphrase") {
			return
		}
		switch t := v.(type) {
		case string:
			s := strings.TrimSpace(t)
			if strings.HasPrefix(s, "--") { // "--output-dir=/x" in saved arguments
				_, s, _ = strings.Cut(s, "=")
			}
			if strings.HasPrefix(s, "/") && len(s) < 4096 {
				out = append(out, s)
			}
		case map[string]any:
			for k, x := range t {
				walk(k, x)
			}
		case []any:
			for _, x := range t {
				walk(key, x)
			}
		}
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil || len(b) > 8<<20 {
			continue
		}
		var doc any
		if strings.HasSuffix(f, ".json") {
			err = json.Unmarshal(b, &doc)
		} else {
			err = yaml.Unmarshal(b, &doc)
		}
		if err == nil {
			walk("", doc)
		}
	}
	sort.Strings(out)
	return out
}

// systemdHardening is the sandbox block of the services that run the stack or
// a part of it: the supervisor, log maintenance and schedules.
func systemdHardening(installDir string, writable ...string) []string {
	lines := []string{
		"# Sandboxing. Older systemd ignores settings it does not know. To relax one,",
		"# use `systemctl edit` (the drop-in survives a reinstall), not this file.",
	}
	lines = append(lines, systemdProcessHardening()...)
	lines = append(lines, systemdKernelHardening()...)
	return append(lines, systemdFilesystemHardening(installDir, writable...)...)
}
