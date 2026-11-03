// Package hostinfo answers platform questions the health checks need and the
// standard library does not: the machine's native CPU architecture (to spot
// a binary running under emulation), memory, open-file limits, inode usage
// and download quarantine flags. Each answer says whether it is known, so a
// platform without the facility reports "unknown" rather than a false alarm.
package hostinfo

import "runtime"

// Arch describes the CPU architecture the process runs on.
type Arch struct {
	Binary   string `json:"binary"`           // runtime.GOARCH of this build
	Native   string `json:"native,omitempty"` // the machine's own architecture, GOARCH spelling
	Emulated bool   `json:"emulated"`         // Rosetta 2, Windows x64-on-ARM, WOW64, ...
	Known    bool   `json:"known"`
}

// CPUArch reports the build and native architectures.
func CPUArch() Arch {
	a := Arch{Binary: runtime.GOARCH}
	if native, emulated, ok := nativeArch(); ok {
		a.Native, a.Emulated, a.Known = native, emulated, true
	}
	return a
}

// Memory is system memory in bytes; zero fields are unknown.
type Memory struct {
	Total     uint64 `json:"total_bytes,omitempty"`
	Available uint64 `json:"available_bytes,omitempty"`
}

// SystemMemory reports total and available memory where the OS exposes them.
func SystemMemory() Memory { return systemMemory() }

// FileLimit returns the process's open-file limits (RLIMIT_NOFILE); ok is
// false on Windows, which has no such per-process limit.
func FileLimit() (soft, hard uint64, ok bool) { return fileLimit() }

// Inodes returns free and total inodes of the filesystem holding path; ok is
// false where the filesystem has no fixed inode table (Windows, btrfs, ZFS).
func Inodes(path string) (free, total uint64, ok bool) {
	free, total, ok = inodes(path)
	if total == 0 {
		return 0, 0, false
	}
	return free, total, ok
}

// Quarantined reports whether a downloaded-file mark is set on path: the
// com.apple.quarantine attribute on macOS, the Zone.Identifier stream on
// Windows. Gatekeeper or SmartScreen can then block or prompt when it runs.
func Quarantined(path string) bool { return quarantined(path) }

// ClearQuarantine removes that mark.
func ClearQuarantine(path string) error { return clearQuarantine(path) }

// goarchFor maps machine names (uname, Windows image types) to GOARCH.
func goarchFor(machine string) string {
	switch machine {
	case "x86_64", "amd64", "AMD64", "x64":
		return "amd64"
	case "aarch64", "arm64", "ARM64", "aarch64_be":
		return "arm64"
	case "i386", "i486", "i586", "i686", "x86", "386":
		return "386"
	case "armv7l", "armv6l", "arm":
		return "arm"
	case "ppc64le":
		return "ppc64le"
	case "s390x":
		return "s390x"
	case "riscv64":
		return "riscv64"
	}
	return machine
}
