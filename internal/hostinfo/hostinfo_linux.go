package hostinfo

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// nativeArch compares the kernel's machine type with the build. It catches a
// 32-bit build on a 64-bit kernel; qemu-user emulation reports the emulated
// machine and is not detectable this way.
func nativeArch() (string, bool, bool) {
	var u unix.Utsname
	if err := unix.Uname(&u); err != nil {
		return "", false, false
	}
	native := goarchFor(unix.ByteSliceToString(u.Machine[:]))
	return native, native != runtime.GOARCH, true
}

func systemMemory() Memory {
	return parseMeminfo("/proc/meminfo")
}

func parseMeminfo(path string) Memory {
	var m Memory
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			m.Total = kb << 10
		case "MemAvailable:":
			m.Available = kb << 10
		}
	}
	return m
}

func quarantined(string) bool      { return false }
func clearQuarantine(string) error { return nil }
