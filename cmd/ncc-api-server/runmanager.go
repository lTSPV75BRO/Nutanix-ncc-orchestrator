package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// defaultMaxConcurrentRuns bounds how many orchestrator processes execute at
// once. Beyond this, triggers queue and start automatically as slots free.
const defaultMaxConcurrentRuns = 4

// runRecord tracks a single queued or in-flight orchestrator run. The concurrent
// run engine lets two cluster groups run at the same time against disjoint
// cluster sets; overlapping clusters are executed by whichever run claimed them
// first and the result is shared (each group only ever sees its own clusters via
// cluster-group filtering on the dashboard).
type runRecord struct {
	id        string
	group     string // optional cluster-group label that triggered the run
	status    string // "queued" | "running" | "done"
	queuedAt  time.Time
	startedAt time.Time
	finished  time.Time

	pid       int
	cancel    context.CancelFunc
	cancelled bool
	liveOut   *tailBuffer

	// clustersNorm are the normalized cluster names this run owns/executes
	// (after overlap subtraction). Empty for an unrestricted "run everything".
	clustersNorm []string
	// clusters are the display names actually pinned via --clusters. Empty for
	// an unrestricted run-all.
	clusters []string
	// skipped lists requested clusters that were NOT run because another active
	// run already owns them; skippedOwner maps each to that run's id.
	skipped      []string
	skippedOwner map[string]string

	// wildcard marks an unrestricted "run everything" with no known inventory,
	// which implicitly owns every cluster and must run exclusively.
	wildcard bool

	outDir  string // per-run filtered output dir (absolute)
	logDir  string // per-run logs dir (absolute)
	cfgPath string

	cmd []string
	cwd string
	env map[string]string

	errStr  string
	outTail string

	// launch parameters (retained so a queued run can start later).
	password  string
	extraArgs []string

	// retryCount is the self-heal auto-retry depth (0 = original submission).
	// Bounded to a single retry so a persistently-failing run cannot loop.
	retryCount int

	// audit provenance captured at submission time.
	auditSubject string
	auditRole    string
	auditClient  string
}

// runStartParams is the validated input to submitRun, produced by
// handleRunTrigger after config/scope checks.
type runStartParams struct {
	cfgPath   string
	password  string
	extraArgs []string
	group     string
	// requested are the display-name clusters this run wants (post group/scope
	// resolution, pre overlap subtraction). Empty means "run everything".
	requested    []string
	unrestricted bool // caller is admin/static token (eligible for run-all)

	auditSubject string
	auditRole    string
	auditClient  string

	// retryCount carries the self-heal auto-retry depth into a resubmission.
	retryCount int
}

// runSubmitResult describes the outcome of submitRun so the handler can shape
// its HTTP response.
type runSubmitResult struct {
	started      bool
	queued       bool
	noop         bool // nothing to run (everything already in flight)
	rec          *runRecord
	queuePos     int
	runningCount int
	skipped      []string
	skippedOwner map[string]string
}

func (s *apiServer) ensureRunManager() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runs == nil {
		s.runs = map[string]*runRecord{}
	}
	if s.clusterOwners == nil {
		s.clusterOwners = map[string]string{}
	}
	if s.maxConcurrentRuns <= 0 {
		s.maxConcurrentRuns = defaultMaxConcurrentRuns
	}
}

// runningCountLocked counts runs currently executing (caller holds s.mu).
func (s *apiServer) runningCountLocked() int {
	n := 0
	for _, rec := range s.runs {
		if rec.status == "running" {
			n++
		}
	}
	return n
}

// nextRunIDLocked allocates a unique, sortable run id (caller holds s.mu).
func (s *apiServer) nextRunIDLocked() string {
	s.runSeq++
	return fmt.Sprintf("%s-%03d", time.Now().UTC().Format("20060102T150405Z"), s.runSeq)
}

// submitRun applies the concurrency/overlap policy and either starts a run
// immediately, queues it for a free slot, or returns a no-op when all requested
// clusters are already covered by other active runs.
func (s *apiServer) submitRun(p runStartParams) runSubmitResult {
	s.ensureRunManager()

	// Resolve the desired cluster set. For an unrestricted run with no explicit
	// subset, expand to the known inventory so overlap dedup is precise; fall
	// back to a wildcard (exclusive) run when no inventory is discoverable.
	desired := append([]string{}, p.requested...)
	wildcard := false
	if len(desired) == 0 && p.unrestricted {
		desired = s.knownClusters()
		if len(desired) == 0 {
			wildcard = true
		}
	}

	s.mu.Lock()
	id := s.nextRunIDLocked()

	rec := &runRecord{
		id:           id,
		group:        strings.TrimSpace(p.group),
		status:       "queued",
		queuedAt:     time.Now().UTC(),
		liveOut:      newTailBuffer(64000),
		cfgPath:      p.cfgPath,
		password:     p.password,
		extraArgs:    p.extraArgs,
		wildcard:     wildcard,
		skippedOwner: map[string]string{},
		auditSubject: p.auditSubject,
		auditRole:    p.auditRole,
		auditClient:  p.auditClient,
		retryCount:   p.retryCount,
	}

	skipped := []string{}
	if wildcard {
		// A wildcard run-all owns everything; it can only run when the registry
		// is otherwise empty, so it always begins queued and is released by the
		// dequeue path.
		rec.clustersNorm = nil
		rec.clusters = nil
	} else {
		var remainder []string
		remainder, skipped, rec.skippedOwner = computeRunRemainder(desired, s.clusterOwners, id)
		rec.clusters = remainder
		for _, c := range remainder {
			rec.clustersNorm = append(rec.clustersNorm, normClusterName(c))
		}
		rec.skipped = skipped

		if len(remainder) == 0 {
			// Everything requested is already being refreshed by other runs.
			s.mu.Unlock()
			return runSubmitResult{noop: true, skipped: skipped, skippedOwner: rec.skippedOwner}
		}
		// Reserve ownership of this run's clusters (covers queued + running) so
		// concurrent triggers de-dupe against it.
		for _, n := range rec.clustersNorm {
			s.clusterOwners[n] = id
		}
	}

	s.runs[id] = rec
	s.runOrder = append(s.runOrder, id)
	s.runsTriggeredTotal.Add(1)

	canStart := s.runningCountLocked() < s.maxConcurrentRuns
	if wildcard {
		// Exclusive: only start when nothing else is queued or running.
		canStart = len(s.runs) == 1
	} else if s.wildcardRunID != "" {
		// A wildcard run-all is active/queued; defer everything behind it.
		canStart = false
	}

	res := runSubmitResult{rec: rec, runningCount: s.runningCountLocked(), skipped: skipped, skippedOwner: rec.skippedOwner}
	if canStart {
		rec.status = "running"
		rec.startedAt = time.Now().UTC()
		if wildcard {
			s.wildcardRunID = id
		}
		s.refreshLegacyRunStateLocked()
		res.started = true
		s.mu.Unlock()
		go s.launchRun(rec)
		return res
	}

	s.runQueue = append(s.runQueue, id)
	res.queued = true
	res.queuePos = len(s.runQueue)
	s.mu.Unlock()
	return res
}

// refreshLegacyRunStateLocked mirrors the most relevant run into the legacy
// single-run fields so existing read paths (/metrics, the legacy /runs/active
// top-level fields) keep working (caller holds s.mu).
func (s *apiServer) refreshLegacyRunStateLocked() {
	var latest *runRecord
	for _, rec := range s.runs {
		if rec.status != "running" {
			continue
		}
		if latest == nil || rec.startedAt.After(latest.startedAt) {
			latest = rec
		}
	}
	if latest == nil {
		s.active = false
		return
	}
	s.active = true
	s.started = latest.startedAt
	s.lastPID = latest.pid
	s.liveOut = latest.liveOut
	s.cancelRun = latest.cancel
	s.lastCfg = latest.cfgPath
	s.lastCmd = latest.cmd
	s.lastCwd = latest.cwd
	s.lastEnv = latest.env
}

// launchRun starts the orchestrator process for rec (already marked running) and
// spawns the waiter goroutine that finalizes it.
func (s *apiServer) launchRun(rec *runRecord) {
	canonicalOut := s.absPath(s.outputDir)
	perRunFiltered := filepath.Join(canonicalOut, ".runs-live", rec.id, "filtered")
	perRunLogs := filepath.Join(canonicalOut, ".runs-live", rec.id, "logs")
	if err := os.MkdirAll(perRunFiltered, 0o755); err != nil {
		s.finishRun(rec, fmt.Errorf("create run output dir: %w", err), "")
		return
	}
	_ = os.MkdirAll(perRunLogs, 0o755)
	rec.outDir = perRunFiltered
	rec.logDir = perRunLogs

	// Seed the regression baseline so a scoped run can still diff against the
	// last canonical result for its clusters.
	seedBaseline(canonicalOut, perRunFiltered)

	args := []string{"--config", rec.cfgPath}
	if !extraArgsHaveFlag(rec.extraArgs, "output-dir-filtered") {
		args = append(args, "--output-dir-filtered", perRunFiltered)
	}
	if !extraArgsHaveFlag(rec.extraArgs, "output-dir-logs") {
		args = append(args, "--output-dir-logs", perRunLogs)
	}
	if len(rec.clusters) > 0 {
		args = append(args, "--clusters", strings.Join(rec.clusters, ","))
	}
	args = append(args, rec.extraArgs...)

	ctx, cancel := context.WithTimeout(context.Background(), s.runTimeout)
	cmd := s.makeOrchestratorCommand(ctx, args...)
	cmd.Dir = s.absPath(s.repoRoot)
	injectedEnv := map[string]string{}
	if strings.TrimSpace(rec.password) != "" {
		cmd.Env = append(os.Environ(), "NCC_PASSWORD="+rec.password)
		injectedEnv["NCC_PASSWORD"] = "***"
	}
	fullCmd := append(s.orchestratorBaseCommand(), redactedArgs(args)...)
	var runOut bytes.Buffer
	mw := io.MultiWriter(&runOut, rec.liveOut)
	cmd.Stdout = mw
	cmd.Stderr = mw

	s.mu.Lock()
	rec.cmd = fullCmd
	rec.cwd = cmd.Dir
	rec.env = injectedEnv
	rec.cancel = cancel
	s.mu.Unlock()

	if err := cmd.Start(); err != nil {
		cancel()
		s.finishRun(rec, err, "")
		return
	}
	pid := cmd.Process.Pid
	s.mu.Lock()
	rec.pid = pid
	s.refreshLegacyRunStateLocked()
	s.mu.Unlock()

	go func() {
		defer cancel()
		err := cmd.Wait()
		s.mu.Lock()
		cancelled := rec.cancelled
		s.mu.Unlock()
		switch {
		case cancelled:
			err = errors.New("run cancelled by user")
		case ctx.Err() == context.DeadlineExceeded:
			err = fmt.Errorf("run timed out after %s", s.runTimeout)
		}
		s.finishRun(rec, err, runOut.String())
	}()
}

// finishRun merges a completed run's artifacts into the canonical output dir,
// archives the run, releases its cluster ownership, and starts any queued run
// that can now proceed.
func (s *apiServer) finishRun(rec *runRecord, runErr error, output string) {
	// Merge per-cluster artifacts into the canonical dir so every group's
	// dashboard reflects the latest result for its clusters regardless of which
	// run produced them. Serialized to avoid concurrent canonical writes.
	if rec.outDir != "" {
		s.mergeMu.Lock()
		canonicalOut := s.absPath(s.outputDir)
		owned := ownedClusterSet(rec, rec.outDir)
		if err := mergeRunIntoCanonical(canonicalOut, rec.outDir, owned); err != nil {
			log.Printf("run %s: merge artifacts: %v", rec.id, err)
		}
		mergeRunLogs(rec.logDir, s.absPath(s.logDir))
		s.mergeMu.Unlock()
		s.archiveRunDir(rec.outDir, rec.startedAt, runErr)
		_ = os.RemoveAll(filepath.Dir(rec.outDir)) // .runs-live/<id>
	}

	s.runsCompletedTotal.Add(1)
	if runErr != nil {
		s.runsFailedTotal.Add(1)
	}
	// Accumulate run duration for /metrics (ncc_run_duration_seconds_{sum,count})
	// and the UI queue-ETA estimate. Only count runs that actually started.
	if !rec.startedAt.IsZero() {
		ms := time.Since(rec.startedAt).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		s.runDurationMillisSum.Add(ms)
		s.runDurationCount.Add(1)
		s.lastRunDurationMs.Store(ms)
	}

	var toStart []*runRecord
	s.mu.Lock()
	rec.status = "done"
	rec.finished = time.Now().UTC()
	rec.outTail = tailString(strings.TrimSpace(output), 4000)
	if runErr != nil {
		if rec.outTail != "" {
			rec.errStr = fmt.Sprintf("%s\n%s", runErr.Error(), rec.outTail)
		} else {
			rec.errStr = runErr.Error()
		}
	}
	// Release cluster ownership held by this run.
	for n, owner := range s.clusterOwners {
		if owner == rec.id {
			delete(s.clusterOwners, n)
		}
	}
	if s.wildcardRunID == rec.id {
		s.wildcardRunID = ""
	}
	// Remove from the active registry; keep a snapshot in legacy fields.
	delete(s.runs, rec.id)
	s.runOrder = removeString(s.runOrder, rec.id)
	s.lastErr = rec.errStr
	s.lastOut = rec.outTail
	s.lastPID = rec.pid

	// Capture what a possible self-heal auto-retry needs while we hold the lock.
	healDecision := decideRunHeal(runErr != nil, rec.cancelled, s.runAutoRetryDisabled, rec.retryCount, output, rec.extraArgs)
	var retryParams runStartParams
	if healDecision.Retry {
		retryParams = runStartParams{
			cfgPath:      rec.cfgPath,
			password:     rec.password,
			extraArgs:    append(append([]string{}, rec.extraArgs...), healDecision.Mitigation...),
			group:        rec.group,
			requested:    append([]string{}, rec.clusters...),
			unrestricted: rec.wildcard,
			auditSubject: rec.auditSubject,
			auditRole:    rec.auditRole,
			auditClient:  rec.auditClient,
			retryCount:   rec.retryCount + 1,
		}
	}

	// Promote queued runs into freed slots (respecting the wildcard barrier).
	toStart = s.dequeueRunnableLocked()
	s.refreshLegacyRunStateLocked()
	s.mu.Unlock()

	for _, q := range toStart {
		go s.launchRun(q)
	}

	if healDecision.Retry {
		s.runAutoRetriesTotal.Add(1)
		log.Printf("run %s failed (%s); self-heal auto-retrying once with mitigation %v",
			rec.id, healDecision.Class, healDecision.Mitigation)
		s.auditEvent("run.auto_retry", true, map[string]interface{}{
			"failed_run":  rec.id,
			"class":       string(healDecision.Class),
			"mitigation":  strings.Join(healDecision.Mitigation, " "),
			"retry_count": retryParams.retryCount,
		})
		go s.submitRun(retryParams)
	}

	go s.notifyRunFinished(runErr)
}

// dequeueRunnableLocked promotes queued runs that can now start (caller holds
// s.mu). It honors the concurrency cap and the wildcard exclusivity rule.
func (s *apiServer) dequeueRunnableLocked() []*runRecord {
	var started []*runRecord
	for len(s.runQueue) > 0 {
		if s.wildcardRunID != "" {
			return started
		}
		nextID := s.runQueue[0]
		rec := s.runs[nextID]
		if rec == nil || rec.status != "queued" {
			s.runQueue = s.runQueue[1:]
			continue
		}
		if rec.wildcard {
			// Exclusive: needs an otherwise-empty registry.
			if len(s.runs) != 1 || s.runningCountLocked() != 0 {
				return started
			}
		} else if s.runningCountLocked() >= s.maxConcurrentRuns {
			return started
		}
		s.runQueue = s.runQueue[1:]
		rec.status = "running"
		rec.startedAt = time.Now().UTC()
		if rec.wildcard {
			s.wildcardRunID = rec.id
		}
		started = append(started, rec)
	}
	return started
}

// cancelRunByID cancels a single run (running or queued). Returns the affected
// record and whether it was found.
func (s *apiServer) cancelRunByID(id string) (*runRecord, bool) {
	s.ensureRunManager()
	var toStart []*runRecord
	s.mu.Lock()
	rec, ok := s.runs[id]
	if !ok {
		s.mu.Unlock()
		return nil, false
	}
	if rec.status == "running" {
		rec.cancelled = true
		cancel := rec.cancel
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return rec, true
	}
	// Queued: drop it and release its reserved clusters.
	rec.status = "done"
	rec.finished = time.Now().UTC()
	rec.errStr = "run cancelled before start"
	for n, owner := range s.clusterOwners {
		if owner == id {
			delete(s.clusterOwners, n)
		}
	}
	if s.wildcardRunID == id {
		s.wildcardRunID = ""
	}
	delete(s.runs, id)
	s.runOrder = removeString(s.runOrder, id)
	s.runQueue = removeString(s.runQueue, id)
	toStart = s.dequeueRunnableLocked()
	s.refreshLegacyRunStateLocked()
	s.mu.Unlock()
	for _, q := range toStart {
		go s.launchRun(q)
	}
	return rec, true
}

// cancelAllRuns cancels every running and queued run. Returns the count.
func (s *apiServer) cancelAllRuns() int {
	s.ensureRunManager()
	s.mu.Lock()
	ids := append([]string{}, s.runOrder...)
	s.mu.Unlock()
	n := 0
	for _, id := range ids {
		if _, ok := s.cancelRunByID(id); ok {
			n++
		}
	}
	return n
}

// activeRunsSnapshot returns a JSON-serializable view of all queued/running
// runs, newest first.
func (s *apiServer) activeRunsSnapshot() []map[string]interface{} {
	s.ensureRunManager()
	s.mu.Lock()
	defer s.mu.Unlock()
	// Map each queued run id to its 1-based position in the FIFO queue so the UI
	// can show "position N in line".
	queuePos := make(map[string]int, len(s.runQueue))
	for i, id := range s.runQueue {
		queuePos[id] = i + 1
	}
	out := make([]map[string]interface{}, 0, len(s.runOrder))
	for _, id := range s.runOrder {
		rec := s.runs[id]
		if rec == nil {
			continue
		}
		var elapsed int
		if !rec.startedAt.IsZero() {
			elapsed = int(time.Since(rec.startedAt).Round(time.Second).Seconds())
		}
		entry := map[string]interface{}{
			"id":          rec.id,
			"status":      rec.status,
			"group":       rec.group,
			"started_at":  rec.startedAt.UTC().Format(time.RFC3339),
			"queued_at":   rec.queuedAt.UTC().Format(time.RFC3339),
			"elapsed_sec": elapsed,
			"pid":         rec.pid,
			"clusters":    rec.clusters,
			"skipped":     rec.skipped,
			"live_output": strings.TrimSpace(rec.liveOut.String()),
		}
		if pos, ok := queuePos[id]; ok {
			entry["queue_position"] = pos
		}
		if len(rec.skippedOwner) > 0 {
			entry["skipped_owner"] = rec.skippedOwner
		}
		if rec.wildcard {
			entry["all_clusters"] = true
		}
		out = append(out, entry)
	}
	// Newest first.
	sort.SliceStable(out, func(i, j int) bool {
		return out[i]["id"].(string) > out[j]["id"].(string)
	})
	return out
}

// queuedCount returns the number of runs waiting for a slot.
func (s *apiServer) queuedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runQueue)
}

// avgRunDurationSeconds returns the mean wall-clock duration of completed runs
// this process has observed, or 0 when none have finished yet. Used as a rough
// queue-ETA basis in the UI.
func (s *apiServer) avgRunDurationSeconds() float64 {
	count := s.runDurationCount.Load()
	if count <= 0 {
		return 0
	}
	return (float64(s.runDurationMillisSum.Load()) / 1000.0) / float64(count)
}

// computeRunRemainder splits desired clusters into the subset this run should
// actually execute (remainder — those not already owned by another active run)
// and the subset skipped because another run already owns them (mapped to that
// owning run id). This is the core of the overlap de-duplication: when two
// cluster groups trigger runs with shared clusters, the second run only
// executes the clusters the first did not already claim.
func computeRunRemainder(desired []string, owners map[string]string, selfID string) (remainder, skipped []string, skippedOwner map[string]string) {
	skippedOwner = map[string]string{}
	seen := map[string]bool{}
	for _, c := range desired {
		n := normClusterName(c)
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		if owner, taken := owners[n]; taken && owner != selfID {
			skipped = append(skipped, strings.TrimSpace(c))
			skippedOwner[strings.TrimSpace(c)] = owner
			continue
		}
		remainder = append(remainder, strings.TrimSpace(c))
	}
	return remainder, skipped, skippedOwner
}

// removeString returns in with the first occurrence of v removed.
func removeString(in []string, v string) []string {
	for i, x := range in {
		if x == v {
			return append(in[:i], in[i+1:]...)
		}
	}
	return in
}

// ---------------------------------------------------------------------------
// Per-run artifact merge into the canonical output dir.
// ---------------------------------------------------------------------------

// seedBaseline copies the canonical regression-baseline artifacts into a fresh
// per-run dir so a scoped run can diff against the last known result.
func seedBaseline(canonicalOut, perRunFiltered string) {
	for _, name := range []string{"run-summary.json", "checks-snapshot.json"} {
		b, err := os.ReadFile(filepath.Join(canonicalOut, name))
		if err != nil {
			continue
		}
		_ = os.WriteFile(filepath.Join(perRunFiltered, name), b, 0o644)
	}
}

// ownedClusterSet computes the set of normalized cluster names a finished run is
// responsible for: the clusters it was pinned to, unioned with every cluster
// present in its own run-summary.json (covers run-all / discovery cases).
func ownedClusterSet(rec *runRecord, perRunDir string) map[string]bool {
	owned := map[string]bool{}
	for _, n := range rec.clustersNorm {
		owned[n] = true
	}
	if b, err := os.ReadFile(filepath.Join(perRunDir, "run-summary.json")); err == nil {
		var sum map[string]interface{}
		if json.Unmarshal(b, &sum) == nil {
			if arr, ok := sum["clusters"].([]interface{}); ok {
				for _, it := range arr {
					if n, has := jsonObjClusterNorm(it); has {
						owned[n] = true
					}
				}
			}
		}
	}
	return owned
}

// jsonObjClusterNorm returns the normalized cluster identity carried by a JSON
// object, if any.
func jsonObjClusterNorm(item interface{}) (string, bool) {
	m, ok := item.(map[string]interface{})
	if !ok {
		return "", false
	}
	if name, ok := objClusterName(m); ok {
		return normClusterName(name), true
	}
	return "", false
}

// mergeRunIntoCanonical merges a per-run output dir into the canonical dir,
// per-cluster latest-wins for the run's owned clusters.
func mergeRunIntoCanonical(canonicalOut, perRunDir string, owned map[string]bool) error {
	if _, err := os.Stat(perRunDir); err != nil {
		return nil // run produced nothing (e.g. failed to start)
	}
	if err := os.MkdirAll(canonicalOut, 0o755); err != nil {
		return err
	}

	mergeRunSummary(canonicalOut, perRunDir, owned)
	mergeClusterArrayArtifact(filepath.Join(canonicalOut, "checks-snapshot.json"), filepath.Join(perRunDir, "checks-snapshot.json"), owned)
	mergeClusterArrayArtifact(filepath.Join(canonicalOut, "slo-dashboard.json"), filepath.Join(perRunDir, "slo-dashboard.json"), owned)
	mergeClusterArrayArtifact(filepath.Join(canonicalOut, "drilldown-diff.json"), filepath.Join(perRunDir, "drilldown-diff.json"), owned)
	mergeClusterArrayArtifact(filepath.Join(canonicalOut, "flaky-checks.json"), filepath.Join(perRunDir, "flaky-checks.json"), owned)
	mergeIndexHTML(canonicalOut, perRunDir, owned)

	// Best-effort copies for whole-run records (last writer wins).
	copyIfExistsFile(filepath.Join(perRunDir, "ncc-run-record.json"), filepath.Join(canonicalOut, "ncc-run-record.json"))
	return nil
}

// mergeRunSummary merges the per-run run-summary.json into the canonical one,
// replacing the owned clusters and recomputing the top-level aggregates.
func mergeRunSummary(canonicalOut, perRunDir string, owned map[string]bool) {
	perRunPath := filepath.Join(perRunDir, "run-summary.json")
	perRunBytes, err := os.ReadFile(perRunPath)
	if err != nil {
		return
	}
	canonicalPath := filepath.Join(canonicalOut, "run-summary.json")
	canonicalBytes, cErr := os.ReadFile(canonicalPath)
	if cErr != nil {
		// No baseline yet — adopt the run's summary verbatim.
		_ = os.WriteFile(canonicalPath, perRunBytes, 0o644)
		return
	}

	var canonical, perRun map[string]interface{}
	if json.Unmarshal(canonicalBytes, &canonical) != nil || json.Unmarshal(perRunBytes, &perRun) != nil {
		_ = os.WriteFile(canonicalPath, perRunBytes, 0o644)
		return
	}

	canonClusters, _ := canonical["clusters"].([]interface{})
	perRunClusters, _ := perRun["clusters"].([]interface{})
	merged := []interface{}{}
	for _, it := range canonClusters {
		if n, has := jsonObjClusterNorm(it); has && owned[n] {
			continue
		}
		merged = append(merged, it)
	}
	merged = append(merged, perRunClusters...)
	canonical["clusters"] = merged

	// Recompute top-level aggregates from the merged cluster list.
	ok, failed, totalChecks := 0, 0, 0
	failedClusters := []string{}
	healthSum, healthCount, minHealth := 0, 0, math.MaxInt32
	for _, it := range merged {
		m, isObj := it.(map[string]interface{})
		if !isObj {
			continue
		}
		if b, _ := m["ok"].(bool); b {
			ok++
		} else {
			failed++
			if addr, _ := m["address"].(string); strings.TrimSpace(addr) != "" {
				failedClusters = append(failedClusters, addr)
			}
		}
		totalChecks += jsonInt(m["checks_total"])
		if hv, present := m["health_score"]; present {
			h := jsonInt(hv)
			healthSum += h
			healthCount++
			if h < minHealth {
				minHealth = h
			}
		}
	}
	canonical["clusters_ok"] = ok
	canonical["clusters_failed"] = failed
	if len(failedClusters) > 0 {
		canonical["failed_clusters"] = failedClusters
	} else {
		delete(canonical, "failed_clusters")
	}
	if totalChecks > 0 {
		canonical["total_checks"] = totalChecks
	}
	if healthCount > 0 {
		canonical["avg_health_score"] = int(math.Round(float64(healthSum) / float64(healthCount)))
		canonical["min_health_score"] = minHealth
	}
	// Adopt the freshest run's scalar provenance.
	if ts := newestTimestamp(canonical["timestamp"], perRun["timestamp"]); ts != "" {
		canonical["timestamp"] = ts
	}
	if v, ok := perRun["duration_s"]; ok {
		canonical["duration_s"] = v
	}
	if v, ok := perRun["exit_code"]; ok {
		canonical["exit_code"] = v
	}
	// Merge failure_classes (overlay the run's buckets onto the baseline).
	if fc, ok := perRun["failure_classes"].(map[string]interface{}); ok {
		base, _ := canonical["failure_classes"].(map[string]interface{})
		if base == nil {
			base = map[string]interface{}{}
		}
		for k, v := range fc {
			base[k] = v
		}
		canonical["failure_classes"] = base
	}

	if out, err := json.MarshalIndent(canonical, "", "  "); err == nil {
		_ = os.WriteFile(canonicalPath, out, 0o644)
	}
}

// mergeClusterArrayArtifact merges a JSON artifact shaped as {..., "<arrayKey>":
// [ {cluster...}, ... ]} (e.g. checks-snapshot.json, slo-dashboard.json) by
// dropping the canonical entries for owned clusters and appending the run's.
func mergeClusterArrayArtifact(canonicalPath, perRunPath string, owned map[string]bool) {
	perRunBytes, err := os.ReadFile(perRunPath)
	if err != nil {
		return
	}
	canonicalBytes, cErr := os.ReadFile(canonicalPath)
	if cErr != nil {
		_ = os.WriteFile(canonicalPath, perRunBytes, 0o644)
		return
	}
	var canonical, perRun map[string]interface{}
	if json.Unmarshal(canonicalBytes, &canonical) != nil || json.Unmarshal(perRunBytes, &perRun) != nil {
		_ = os.WriteFile(canonicalPath, perRunBytes, 0o644)
		return
	}
	for key, perRunVal := range perRun {
		perRunArr, isArr := perRunVal.([]interface{})
		if !isArr {
			// Adopt the run's freshest scalar (timestamp etc.) without clobbering
			// canonical aggregate counts blindly: only overwrite timestamp.
			if key == "timestamp" {
				if ts := newestTimestamp(canonical["timestamp"], perRunVal); ts != "" {
					canonical["timestamp"] = ts
				}
			}
			continue
		}
		canonArr, _ := canonical[key].([]interface{})
		merged := []interface{}{}
		for _, it := range canonArr {
			if n, has := jsonObjClusterNorm(it); has && owned[n] {
				continue
			}
			merged = append(merged, it)
		}
		merged = append(merged, perRunArr...)
		canonical[key] = merged
	}
	if out, err := json.MarshalIndent(canonical, "", "  "); err == nil {
		_ = os.WriteFile(canonicalPath, out, 0o644)
	}
}

// mergeIndexHTML merges the per-cluster inline JSON vars (AGG, CLUSTER_LINKS)
// from a run's index.html into the canonical index.html. When the canonical
// report does not exist yet, the run's index.html is adopted wholesale.
func mergeIndexHTML(canonicalOut, perRunDir string, owned map[string]bool) {
	canonicalPath := filepath.Join(canonicalOut, "index.html")
	perRunPath := filepath.Join(perRunDir, "index.html")
	perRunBytes, err := os.ReadFile(perRunPath)
	if err != nil {
		return
	}
	if _, cErr := os.Stat(canonicalPath); cErr != nil {
		_ = os.WriteFile(canonicalPath, perRunBytes, 0o644)
		return
	}
	for _, varName := range []string{"AGG", "CLUSTER_LINKS"} {
		canonVal := readInlineJSONVar(canonicalPath, varName, []interface{}{})
		perRunVal := readInlineJSONVar(perRunPath, varName, []interface{}{})
		merged := []interface{}{}
		if arr, ok := canonVal.([]interface{}); ok {
			for _, it := range arr {
				if n, has := jsonObjClusterNorm(it); has && owned[n] {
					continue
				}
				merged = append(merged, it)
			}
		}
		if arr, ok := perRunVal.([]interface{}); ok {
			merged = append(merged, arr...)
		}
		if err := replaceInlineJSONVar(canonicalPath, varName, merged); err != nil {
			log.Printf("merge index.html %s: %v", varName, err)
		}
	}
}

// mergeRunLogs copies a run's raw NCC logs into the canonical log dir so the
// dashboard's per-cluster NCC summary reflects the latest run for each cluster.
func mergeRunLogs(perRunLogs, canonicalLogs string) {
	if perRunLogs == "" || canonicalLogs == "" {
		return
	}
	entries, err := os.ReadDir(perRunLogs)
	if err != nil {
		return
	}
	_ = os.MkdirAll(canonicalLogs, 0o755)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		copyIfExistsFile(filepath.Join(perRunLogs, e.Name()), filepath.Join(canonicalLogs, e.Name()))
	}
}

// archiveRunDir archives a single run's per-run artifacts under
// outputDir/runs/<id> so the Runs history lists each concurrent run separately.
func (s *apiServer) archiveRunDir(perRunDir string, startedAt time.Time, runErr error) {
	canonicalOut := s.absPath(s.outputDir)
	id := startedAt.UTC().Format("20060102T150405Z")
	exitCode := 0
	if runErr != nil {
		exitCode = -1
	}
	sumBytes, sumErr := os.ReadFile(filepath.Join(perRunDir, "run-summary.json"))
	if sumErr == nil {
		var probe struct {
			Timestamp string `json:"timestamp"`
			ExitCode  *int   `json:"exit_code"`
		}
		if json.Unmarshal(sumBytes, &probe) == nil {
			if strings.TrimSpace(probe.Timestamp) != "" {
				if t, err := time.Parse(time.RFC3339, probe.Timestamp); err == nil {
					id = t.UTC().Format("20060102T150405Z")
				}
			}
			if probe.ExitCode != nil {
				exitCode = *probe.ExitCode
			}
		}
	}
	target := filepath.Join(canonicalOut, "runs", id)
	if err := os.MkdirAll(target, 0o755); err != nil {
		log.Printf("archive run: mkdir %s: %v", target, err)
		return
	}
	for _, name := range []string{"run-summary.json", "ncc-run-record.json", "regression-summary.json", "checks-snapshot.json"} {
		copyIfExistsFile(filepath.Join(perRunDir, name), filepath.Join(target, name))
	}
	meta := map[string]interface{}{
		"started_at":  startedAt.UTC().Format(time.RFC3339),
		"finished_at": time.Now().UTC().Format(time.RFC3339),
		"duration_s":  time.Since(startedAt).Seconds(),
		"success":     runErr == nil,
		"exit_code":   exitCode,
	}
	if runErr != nil {
		meta["error"] = runErr.Error()
	}
	if b, err := json.MarshalIndent(meta, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(target, "run-meta.json"), b, 0o644)
	}
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

func copyIfExistsFile(src, dst string) {
	b, err := os.ReadFile(src)
	if err != nil {
		return
	}
	_ = os.WriteFile(dst, b, 0o644)
}

func jsonInt(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

func newestTimestamp(a, b interface{}) string {
	as, _ := a.(string)
	bs, _ := b.(string)
	at, aerr := time.Parse(time.RFC3339, strings.TrimSpace(as))
	bt, berr := time.Parse(time.RFC3339, strings.TrimSpace(bs))
	switch {
	case aerr != nil && berr != nil:
		if strings.TrimSpace(bs) != "" {
			return bs
		}
		return as
	case aerr != nil:
		return bs
	case berr != nil:
		return as
	case bt.After(at):
		return bs
	default:
		return as
	}
}

// replaceInlineJSONVar rewrites the value of an inline `var NAME = <json>;` (or
// `const NAME = ...`) declaration in an HTML/JS file, preserving the rest of the
// document byte-for-byte.
func replaceInlineJSONVar(path, varName string, value interface{}) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	nb, err := json.Marshal(value)
	if err != nil {
		return err
	}
	pattern := `(?s)((?:const|var)\s+` + regexp.QuoteMeta(varName) + `\s*=\s*)(.*?)(;)`
	re := regexp.MustCompile(pattern)
	loc := re.FindSubmatchIndex(b)
	if loc == nil {
		return fmt.Errorf("inline var %s not found in %s", varName, path)
	}
	// loc[4]:loc[5] spans the old value (capture group 2).
	var buf bytes.Buffer
	buf.Write(b[:loc[4]])
	buf.Write(nb)
	buf.Write(b[loc[5]:])
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
