package main

// Run pipeline helpers for large fleets: processing-pool sizing, automatic
// max-parallel, the retry pass for transient failures, and the run journal
// behind --resume. The dispatch loop itself is in the root command (goNCC.go).

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// autoMaxParallelCap bounds max-parallel: 0 ("auto"). Each in-flight
	// cluster is mostly an idle wait on its NCC task, so a few hundred cost
	// little here; adaptive parallelism backs off if Prism or the network
	// pushes back.
	autoMaxParallelCap = 500
	// resumeMaxAge is how old an interrupted run's journal may be for
	// --resume to reuse its finished clusters.
	resumeMaxAge   = 24 * time.Hour
	runJournalName = ".ncc-run-journal.jsonl"
)

// autoMaxParallel resolves max-parallel: 0 for a fleet of n clusters.
func autoMaxParallel(n int) int {
	if n < 1 {
		return 1
	}
	if n > autoMaxParallelCap {
		return autoMaxParallelCap
	}
	return n
}

// processingWorkers sizes the pool that writes reports and sends per-cluster
// notifications. The work is mostly waiting (disk, SMTP, webhooks), so the
// pool matches the cluster slots feeding it: a pool sized to the CPUs made a
// run with 3-second webhooks slower than before, because fetched clusters
// queued behind a few busy notifiers while still holding their slots. Go
// schedules the CPU-bound parts (parsing, HTML) onto the available cores.
func processingWorkers(maxParallel int) int {
	n := maxParallel
	if min := runtime.NumCPU() * 2; n < min {
		n = min
	}
	if n < 1 {
		n = 1
	}
	return n
}

// clusterProcessJob is a fetched cluster waiting for the processing pool.
type clusterProcessJob struct {
	cluster  string
	fetched  clusterFetch
	setPhase func(string)
	finish   func(ok bool) // completes the cluster's progress bars
}

// retryableClusterError reports whether a failed cluster is worth a second
// attempt in the same run: the cluster or network was briefly unavailable or
// pushed back. Authentication, parsing and NCC's own failures repeat the
// same way, so they are not retried.
func retryableClusterError(r ClusterResult) bool {
	if r.Err == nil || errors.Is(r.Err, errRunCancelled) {
		return false
	}
	switch r.ErrorClass {
	case "timeout", "network", "rate_limit":
		return true
	}
	return false
}

// errRunCancelled marks results of clusters stopped by a shutdown signal.
var errRunCancelled = errors.New("run cancelled")

// applyBlockFilters applies severity-filter and exclude-alert-titles to a
// cluster's parsed findings (live runs and resumed clusters alike).
func applyBlockFilters(cfg Config, blocks []ParsedBlock, l zerolog.Logger) ([]ParsedBlock, []ExcludedAlert, error) {
	if len(cfg.SeverityFilter) > 0 {
		originalCount := len(blocks)
		blocks = filterBlocksBySeverity(blocks, cfg.SeverityFilter)
		l.Info().Int("original", originalCount).Int("filtered", len(blocks)).Strs("severities", cfg.SeverityFilter).Msg("applied severity filter")
	}
	excludedByTitle := make([]ExcludedAlert, 0)
	if len(cfg.ExcludeAlertTitles) > 0 {
		originalCount := len(blocks)
		filteredBlocks, excludedAlerts, err := filterBlocksByTitle(blocks, cfg.ExcludeAlertTitles, cfg.ExcludeAlertMatchMode)
		if err != nil {
			return nil, nil, err
		}
		blocks = filteredBlocks
		excludedByTitle = excludedAlerts
		l.Info().
			Int("original", originalCount).
			Int("filtered", len(blocks)).
			Int("excluded", len(excludedByTitle)).
			Str("mode", cfg.ExcludeAlertMatchMode).
			Strs("titles", cfg.ExcludeAlertTitles).
			Msg("applied alert title exclusion filter")
	}
	return blocks, excludedByTitle, nil
}

// ---- Run journal (--resume) ----
//
// While a run is in progress, each cluster that finishes is appended to
// <output-dir-filtered>/.ncc-run-journal.jsonl. A run that completes removes
// the journal. If a long run is interrupted (crash, reboot, Ctrl-C), starting
// it again with --resume reuses the clusters already finished — their raw
// NCC summaries are on disk — and only contacts the rest. The journal is only
// trusted for the same cluster list and for resumeMaxAge.

type runJournalHeader struct {
	StartedAt   time.Time `json:"started_at"`
	Fingerprint string    `json:"fingerprint"`
	Clusters    int       `json:"clusters"`
}

type runJournalEntry struct {
	Cluster string    `json:"cluster"`
	At      time.Time `json:"at"`
}

type runJournal struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

func runJournalPath(cfg Config) string {
	return filepath.Join(cfg.OutputDirFiltered, runJournalName)
}

// clusterListFingerprint identifies the set of clusters a run covers.
func clusterListFingerprint(clusters []string) string {
	sorted := append([]string(nil), clusters...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:16])
}

// readRunJournal returns the clusters an interrupted run finished, or why it
// cannot be used.
func readRunJournal(path string, clusters []string, now time.Time) (map[string]bool, string) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "no interrupted run to resume"
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	if !sc.Scan() {
		return nil, "the run journal is empty"
	}
	var head runJournalHeader
	if err := json.Unmarshal(sc.Bytes(), &head); err != nil || head.Fingerprint == "" {
		return nil, "the run journal is unreadable"
	}
	if head.Fingerprint != clusterListFingerprint(clusters) {
		return nil, "the cluster list changed since the interrupted run"
	}
	if now.Sub(head.StartedAt) > resumeMaxAge {
		return nil, fmt.Sprintf("the interrupted run started more than %s ago", resumeMaxAge)
	}
	done := map[string]bool{}
	for sc.Scan() {
		var e runJournalEntry
		if json.Unmarshal(sc.Bytes(), &e) == nil && e.Cluster != "" {
			done[e.Cluster] = true
		}
	}
	return done, ""
}

// openRunJournal starts a journal for this run, or continues the one being
// resumed (keeping its header, so a second interruption still resumes).
func openRunJournal(cfg Config, resuming bool, now time.Time) (*runJournal, error) {
	path := runJournalPath(cfg)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if resuming {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			return &runJournal{path: path, f: f}, nil
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	head, _ := json.Marshal(runJournalHeader{StartedAt: now.UTC(), Fingerprint: clusterListFingerprint(cfg.Clusters), Clusters: len(cfg.Clusters)})
	if _, err := f.Write(append(head, '\n')); err != nil {
		f.Close()
		return nil, err
	}
	return &runJournal{path: path, f: f}, nil
}

// markDone records a finished cluster. Errors are not fatal: the journal
// only saves time on a resume.
func (j *runJournal) markDone(cluster string) {
	if j == nil {
		return
	}
	line, _ := json.Marshal(runJournalEntry{Cluster: cluster, At: time.Now().UTC()})
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		_, _ = j.f.Write(append(line, '\n'))
	}
}

// close stops recording; complete also removes the journal (the run finished,
// so there is nothing to resume).
func (j *runJournal) close(complete bool) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		_ = j.f.Close()
		j.f = nil
	}
	if complete {
		_ = os.Remove(j.path)
	}
}

// resumedClusterResult rebuilds a finished cluster's result from the raw NCC
// summary it saved, without contacting the cluster again.
func resumedClusterResult(cfg Config, cluster string) (ClusterResult, bool) {
	b, err := os.ReadFile(filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster)))
	if err != nil || len(b) == 0 {
		return ClusterResult{}, false
	}
	text := string(b)
	blocks, err := ParseSummary(text)
	if err != nil {
		return ClusterResult{}, false
	}
	l := zerolog.Nop()
	blocks, excluded, err := applyBlockFilters(cfg, blocks, l)
	if err != nil {
		return ClusterResult{}, false
	}
	return ClusterResult{Cluster: cluster, Blocks: blocks, ExcludedByTitle: excluded, Meta: parseNCCHeaderText(text)}, true
}
