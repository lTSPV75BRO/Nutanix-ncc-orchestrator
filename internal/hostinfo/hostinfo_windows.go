package hostinfo

import (
	"errors"
	"io/fs"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Image file machine types returned by IsWow64Process2.
const (
	machineI386  = 0x014c
	machineAMD64 = 0x8664
	machineARM64 = 0xAA64
)

// nativeArch asks Windows for the machine's native architecture. An amd64
// build on an ARM64 PC runs under x64 emulation; a 386 build under WOW64.
func nativeArch() (string, bool, bool) {
	var process, native uint16
	if err := windows.IsWow64Process2(windows.CurrentProcess(), &process, &native); err != nil {
		return "", false, false
	}
	var arch string
	switch native {
	case machineAMD64:
		arch = "amd64"
	case machineARM64:
		arch = "arm64"
	case machineI386:
		arch = "386"
	default:
		return "", false, false
	}
	return arch, arch != runtime.GOARCH, true
}

// memoryStatusEx is MEMORYSTATUSEX.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

var procGlobalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

func systemMemory() Memory {
	st := memoryStatusEx{}
	st.Length = uint32(unsafe.Sizeof(st))
	if r, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&st))); r == 0 {
		return Memory{}
	}
	return Memory{Total: st.TotalPhys, Available: st.AvailPhys}
}

func fileLimit() (uint64, uint64, bool)    { return 0, 0, false }
func inodes(string) (uint64, uint64, bool) { return 0, 0, false }
func zoneStream(path string) string        { return path + ":Zone.Identifier" }

func quarantined(path string) bool {
	_, err := os.Stat(zoneStream(path))
	return err == nil
}

func clearQuarantine(path string) error {
	err := os.Remove(zoneStream(path))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
