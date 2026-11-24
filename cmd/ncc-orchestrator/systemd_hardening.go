package main

// Sandboxing for the generated systemd units. The service stays root: it has to
// write units in /etc/systemd/system, call systemctl and relabel binaries. Every
// child (API, UI, NCC runs, chcon, crontab, systemctl) inherits these settings.
//
// Deliberately not set, because a supported setup would break:
//   - PrivateTmp: the install, output or backup dir may be under /tmp or
//     /var/tmp, and doctor cleans leftovers of CLI backups there.
//   - ProtectHome: installs under /root and /home are supported.
//   - ProtectSystem: "full" blocks writing units to /etc/systemd/system; "true"
//     blocks in-app updates of an install under /usr/local, and output dirs
//     such as /usr/share/nginx/html.
//   - ProtectHostname: freezes the host name used in alerts and the audit log.
//   - MemoryDenyWriteExecute: breaks the PCRE JIT that the SELinux tools use.
//   - UMask: changes the permissions of reports and logs that others read.
//   - CAP_SYS_ADMIN, CAP_SYS_RESOURCE and CAP_SYS_PTRACE are kept: peers of
//     systemctl may check the first, raising limits needs the second, and
//     reading another process under /proc mounted with hidepid the third.

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
		// chcon to an existing type needs no CAP_MAC_ADMIN.
		"CapabilityBoundingSet=~CAP_SYS_MODULE CAP_SYS_BOOT CAP_SYS_TIME CAP_SYS_RAWIO CAP_SYS_PACCT CAP_SYS_TTY_CONFIG CAP_SYSLOG CAP_WAKE_ALARM CAP_BLOCK_SUSPEND CAP_MAC_ADMIN CAP_NET_ADMIN CAP_NET_RAW",
	}
}

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

// systemdHardening is the sandbox block shared by the generated services.
func systemdHardening() []string {
	lines := []string{
		"# Sandboxing. Older systemd ignores settings it does not know. To relax one,",
		"# use `systemctl edit` (the drop-in survives a reinstall), not this file.",
	}
	lines = append(lines, systemdProcessHardening()...)
	return append(lines, systemdKernelHardening()...)
}
