package runtimetune

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadLimitsV2(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "cgroup.controllers"), "cpu memory\n")
	write(t, filepath.Join(root, "memory.max"), "1073741824\n")
	write(t, filepath.Join(root, "memory.current"), "52428800\n")
	write(t, filepath.Join(root, "cpu.max"), "150000 100000\n")
	write(t, filepath.Join(root, "memory.events"), "low 0\nhigh 0\nmax 4\noom 2\noom_kill 2\n")
	l := readLimits(root)
	if l.Version != 2 || l.MemoryLimit != 1<<30 || l.MemoryUsage != 50<<20 || l.CPULimit != 1.5 || l.OOMKills != 2 {
		t.Fatalf("got %+v", l)
	}
}

func TestReadLimitsV2Unlimited(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "cgroup.controllers"), "cpu memory\n")
	write(t, filepath.Join(root, "memory.max"), "max\n")
	write(t, filepath.Join(root, "cpu.max"), "max 100000\n")
	if l := readLimits(root); l.MemoryLimit != 0 || l.CPULimit != 0 {
		t.Fatalf("got %+v", l)
	}
}

func TestReadLimitsV1(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "536870912\n")
	write(t, filepath.Join(root, "cpu", "cpu.cfs_quota_us"), "200000\n")
	write(t, filepath.Join(root, "cpu", "cpu.cfs_period_us"), "100000\n")
	l := readLimits(root)
	if l.Version != 1 || l.MemoryLimit != 512<<20 || l.CPULimit != 2 {
		t.Fatalf("got %+v", l)
	}
	write(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "9223372036854771712\n")
	if l := readLimits(root); l.MemoryLimit != 0 {
		t.Fatalf("v1 no-limit sentinel read as %d", l.MemoryLimit)
	}
}

func TestApply(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	if r := apply("", Limits{}); r.Source != "none" || r.MemoryLimit != 0 {
		t.Fatalf("no limit: %+v", r)
	}
	r := apply("", Limits{MemoryLimit: 1000 << 20})
	if r.Source != "cgroup" || r.MemoryLimit != 900<<20 || debug.SetMemoryLimit(-1) != 900<<20 {
		t.Fatalf("cgroup: %+v", r)
	}
	if r := apply("2GiB", Limits{MemoryLimit: 1000 << 20}); r.Source != "GOMEMLIMIT" {
		t.Fatalf("env wins: %+v", r)
	}
}
