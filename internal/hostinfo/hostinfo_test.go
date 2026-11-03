package hostinfo

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCPUArchMatchesBuild(t *testing.T) {
	a := CPUArch()
	if a.Binary != runtime.GOARCH {
		t.Fatalf("binary arch %q", a.Binary)
	}
	if a.Known && a.Native == "" {
		t.Fatalf("known arch without a native name: %+v", a)
	}
}

func TestGoarchFor(t *testing.T) {
	for in, want := range map[string]string{"x86_64": "amd64", "aarch64": "arm64", "i686": "386", "AMD64": "amd64"} {
		if got := goarchFor(in); got != want {
			t.Errorf("goarchFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSystemMemory(t *testing.T) {
	m := SystemMemory()
	if runtime.GOOS == "windows" || runtime.GOOS == "linux" {
		if m.Total == 0 || m.Available == 0 || m.Available > m.Total {
			t.Fatalf("memory %+v", m)
		}
	}
}

func TestQuarantineRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bin.exe")
	if err := os.WriteFile(p, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if Quarantined(p) {
		t.Fatal("fresh file reported quarantined")
	}
	if err := ClearQuarantine(p); err != nil {
		t.Fatalf("clearing an unmarked file: %v", err)
	}
	if runtime.GOOS != "windows" {
		return
	}
	if err := os.WriteFile(p+":Zone.Identifier", []byte("[ZoneTransfer]\r\nZoneId=3\r\n"), 0o644); err != nil {
		t.Skipf("no alternate data streams here: %v", err)
	}
	if !Quarantined(p) {
		t.Fatal("Zone.Identifier not detected")
	}
	if err := ClearQuarantine(p); err != nil || Quarantined(p) {
		t.Fatalf("clear: %v", err)
	}
}
