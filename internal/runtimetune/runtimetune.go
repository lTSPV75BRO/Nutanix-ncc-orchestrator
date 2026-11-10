// Package runtimetune reads the limits the process actually runs under
// (container cgroups) and tunes the Go runtime to them.
//
// Go already sizes GOMAXPROCS from the cgroup CPU quota, but it does not know
// a container's memory limit: the garbage collector lets the heap grow until
// the kernel OOM-kills the process. Apply sets a soft memory limit just below
// the container's, so the collector works harder near the limit instead.
package runtimetune

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
)

// memoryLimitShare is the part of the container memory limit given to the Go
// heap; the rest covers stacks, cgo and the page cache of open files.
const memoryLimitShare = 0.9

// cgroupRoot is a variable so tests can point it at a fake hierarchy.
var cgroupRoot = "/sys/fs/cgroup"

// Limits are the cgroup limits that apply to this process. Zero means none.
type Limits struct {
	Version     int     `json:"cgroup_version,omitempty"` // 1 or 2; 0 when not in a cgroup with limits
	MemoryLimit uint64  `json:"memory_limit_bytes,omitempty"`
	MemoryUsage uint64  `json:"memory_usage_bytes,omitempty"`
	CPULimit    float64 `json:"cpu_limit,omitempty"` // CPUs, e.g. 1.5
	OOMKills    int     `json:"oom_kills,omitempty"`
}

// ReadLimits returns the cgroup limits on Linux; elsewhere it is empty.
func ReadLimits() Limits {
	if runtime.GOOS != "linux" {
		return Limits{}
	}
	return readLimits(cgroupRoot)
}

func readLimits(root string) Limits {
	var l Limits
	// cgroup v2: one unified hierarchy.
	if b, err := os.ReadFile(filepath.Join(root, "cgroup.controllers")); err == nil && len(b) > 0 {
		l.Version = 2
		l.MemoryLimit = readUintFile(filepath.Join(root, "memory.max"))
		l.MemoryUsage = readUintFile(filepath.Join(root, "memory.current"))
		if f := strings.Fields(readString(filepath.Join(root, "cpu.max"))); len(f) == 2 && f[0] != "max" {
			quota, e1 := strconv.ParseFloat(f[0], 64)
			period, e2 := strconv.ParseFloat(f[1], 64)
			if e1 == nil && e2 == nil && period > 0 {
				l.CPULimit = quota / period
			}
		}
		l.OOMKills = uint64ToInt(readKeyed(filepath.Join(root, "memory.events"), "oom_kill"))
		return l
	}
	// cgroup v1: per-controller hierarchies.
	mem := filepath.Join(root, "memory")
	if _, err := os.Stat(filepath.Join(mem, "memory.limit_in_bytes")); err == nil {
		l.Version = 1
		lim := readUintFile(filepath.Join(mem, "memory.limit_in_bytes"))
		// "No limit" is a huge page-aligned number in v1.
		if lim < 1<<62 {
			l.MemoryLimit = lim
		}
		l.MemoryUsage = readUintFile(filepath.Join(mem, "memory.usage_in_bytes"))
		l.OOMKills = uint64ToInt(readKeyed(filepath.Join(mem, "memory.oom_control"), "oom_kill"))
		cpu := filepath.Join(root, "cpu")
		quota, e1 := strconv.ParseFloat(readString(filepath.Join(cpu, "cpu.cfs_quota_us")), 64)
		period, e2 := strconv.ParseFloat(readString(filepath.Join(cpu, "cpu.cfs_period_us")), 64)
		if e1 == nil && e2 == nil && quota > 0 && period > 0 {
			l.CPULimit = quota / period
		}
	}
	return l
}

// Result says what Apply did.
type Result struct {
	MemoryLimit int64  `json:"memory_limit_bytes,omitempty"` // the soft limit in effect; 0 = none
	Source      string `json:"memory_limit_source"`          // GOMEMLIMIT | cgroup | none
	GOMAXPROCS  int    `json:"gomaxprocs"`
}

var (
	applyOnce   sync.Once
	applyResult Result
)

// Apply sets the soft memory limit from the container's limit unless the
// operator set GOMEMLIMIT. It runs once per process; later calls return the
// first result.
func Apply() Result {
	applyOnce.Do(func() { applyResult = apply(os.Getenv("GOMEMLIMIT"), ReadLimits()) })
	return applyResult
}

func apply(envLimit string, l Limits) Result {
	r := Result{Source: "none", GOMAXPROCS: runtime.GOMAXPROCS(0)}
	if strings.TrimSpace(envLimit) != "" {
		// The runtime already parsed it; read it back without changing it.
		r.MemoryLimit = debug.SetMemoryLimit(-1)
		r.Source = "GOMEMLIMIT"
		return r
	}
	if l.MemoryLimit == 0 {
		return r
	}
	r.MemoryLimit = int64(float64(l.MemoryLimit) * memoryLimitShare)
	debug.SetMemoryLimit(r.MemoryLimit)
	r.Source = "cgroup"
	return r
}

func readString(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// readUintFile reads a single number; "max" and errors read as 0.
func readUintFile(path string) uint64 {
	n, err := strconv.ParseUint(readString(path), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func uint64ToInt(n uint64) int {
	maxInt := uint64(^uint(0) >> 1)
	if n > maxInt {
		return int(maxInt)
	}
	return int(n)
}

// readKeyed reads "key value" lines (memory.events, oom_control).
func readKeyed(path, key string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 && fields[0] == key {
			n, _ := strconv.ParseUint(fields[1], 10, 64)
			return n
		}
	}
	return 0
}
