package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
)

// startRunProfiling starts a CPU profile in dir and returns a function that
// stops it and writes a heap profile next to it. It returns nil (profiling
// off) when dir is empty or cannot be used, so a typo never stops a run.
// Used through NCC_PROFILE_DIR when tuning runs across thousands of clusters.
func startRunProfiling(dir string) func() {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "NCC_PROFILE_DIR: %v (profiling off)\n", err)
		return nil
	}
	cpu, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "NCC_PROFILE_DIR: %v (profiling off)\n", err)
		return nil
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		cpu.Close()
		fmt.Fprintf(os.Stderr, "NCC_PROFILE_DIR: %v (profiling off)\n", err)
		return nil
	}
	return func() {
		pprof.StopCPUProfile()
		cpu.Close()
		heap, err := os.Create(filepath.Join(dir, "heap.pprof"))
		if err != nil {
			return
		}
		defer heap.Close()
		runtime.GC()
		_ = pprof.WriteHeapProfile(heap)
	}
}
