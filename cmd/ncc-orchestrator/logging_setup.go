package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// checkOutputPermissions verifies the process can create/open files for write in the log path
// and in each output directory. Returns a clear error on first failure so permission issues
// are reported early instead of during normal writes.
//
// The probe never truncates an existing target file (it opens with O_APPEND
// only) and, when it has to create the file itself, removes it again once the
// check passes. This matters most for "aggregated index.html": that path is
// also the run's real report output, and in the scoped/per-run execution
// model used by the API server it lives in a fresh, run-specific directory.
// If a run aborts early (e.g. cluster discovery failure) after this probe
// ran but before the real report was ever written, a truncated 0-byte
// index.html left behind here would later be read by mergeIndexHTML (see
// cmd/ncc-api-server/runmanager.go) as "this run legitimately produced zero
// rows", wiping every owned cluster's entries from the canonical aggregated
// report. Cleaning up the probe artifact (and never truncating real content)
// avoids that.
func checkOutputPermissions(cfg *Config) error {
	probeName := ".ncc-writecheck"
	indexHTML := filepath.Join(cfg.OutputDirFiltered, "index.html")
	checks := []struct {
		label  string
		path   string
		remove bool
	}{
		{"log file", cfg.LogFile, false},
		{"output dir (raw logs)", filepath.Join(cfg.OutputDirLogs, probeName), true},
		{"output dir (filtered)", filepath.Join(cfg.OutputDirFiltered, probeName), true},
		{"aggregated index.html", indexHTML, true},
	}
	if cfg.PromEnabled {
		checks = append(checks, struct {
			label  string
			path   string
			remove bool
		}{"prom dir", filepath.Join(cfg.PromDir, probeName), true})
	}
	for _, c := range checks {
		dir := filepath.Dir(c.path)
		if dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("cannot create directory %s: %w", dir, err)
			}
		}
		existedBefore := false
		if _, statErr := os.Stat(c.path); statErr == nil {
			existedBefore = true
		}
		f, err := os.OpenFile(c.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("cannot open/create file for write (%s): %w", c.label, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close probe file %s: %w", c.path, err)
		}
		if c.remove && !existedBefore {
			_ = os.Remove(c.path)
		}
	}
	return nil
}

// ==================== Logging ====================

// In setupFileLogger, add the new version fields to the global logger context
func setupFileLogger(logPath string, lvl zerolog.Level) error {
	dir := filepath.Dir(logPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	fileWriter := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    20, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	var gitRevision string
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				gitRevision = s.Value
				break
			}
		}
		log.Logger = runLogContext(zerolog.New(fileWriter).Level(lvl).With().
			Timestamp().
			Str("git_revision", gitRevision).
			Str("go_version", bi.GoVersion).
			Str("Version", Version).
			Str("stream", Stream)).
			Logger()
	} else {
		log.Logger = runLogContext(zerolog.New(fileWriter).Level(lvl).With().Timestamp()).Logger()
	}
	return nil
}

// runLogContext tags every runner log line with the run it belongs to (set by
// the API server or scheduler that started the run) and the process, so lines
// from runs that overlap in the shared log can be told apart.
func runLogContext(c zerolog.Context) zerolog.Context {
	c = c.Str("component", "runner").Int("pid", os.Getpid())
	if host, err := os.Hostname(); err == nil && host != "" {
		c = c.Str("host", host)
	}
	for _, kv := range [][2]string{{"NCC_RUN_ID", "run_id"}, {"NCC_RUN_SOURCE", "run_source"}, {"NCC_RUN_ENVIRONMENT", "environment"}} {
		if v := strings.TrimSpace(os.Getenv(kv[0])); v != "" {
			c = c.Str(kv[1], v)
		}
	}
	return c
}
