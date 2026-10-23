package main

// NCC alert lifecycle store.
//
// Before this store existed, NCC acknowledge/resolve marks lived in
// outputfiles/ncc-alert-dispositions.json. That design had three failure modes
// that made marks disappear "when a new report was generated":
//
//  1. The file lived in the runner's output directory, and the runner's
//     artifact retention (artifact-retain-days / artifact-retain-max-files)
//     deletes every non-protected file there after each run.
//  2. Read-modify-write cycles were serialized only by an in-process mutex.
//     With two API replicas on a shared RWX volume (the Helm default), the
//     auto-reopen performed on GET right after a new run could overwrite a
//     mark saved concurrently by the other replica, and both replicas used the
//     same "<file>.tmp" name for their atomic rename.
//  3. "Reopen" deleted the record, and history was capped at 20 entries, so the
//     audit trail was lost.
//
// The lifecycle store separates the lifecycle of a finding from the lifecycle
// of a run report:
//
//   - A finding's logical identity is its correlation key (cluster + normalized
//     check title). It is stable across runs.
//   - Each alert *instance* (an episode of that finding) gets a server-side,
//     cryptographically random UUIDv4. Instances are never overwritten; a
//     resolved finding that is detected again by a newer run closes the
//     resolved instance and opens a new one that links back to it.
//   - Every state transition is an append-only event with actor, timestamp and
//     the run that caused it.
//   - Detection ("detected" / "not_detected") is tracked separately from the
//     operator state ("open" / "acknowledged" / "resolved"), so an issue that
//     is no longer reported is never confused with one an operator resolved.
//
// The store is a JSON document next to the other api-server state
// (<repo-root>/.ncc-api-alert-lifecycle.json), so it is outside the runner's
// pruned output directory, survives restarts, is on the shared PVC in
// Kubernetes, and is captured by v2-backup (which sweeps .ncc-api-* files).
// Writes take a cross-process file lock and use unique temp files.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	alertLifecycleSchemaVersion = 1
	alertLifecycleFileName      = ".ncc-api-alert-lifecycle.json"
	alertLifecycleLockTimeout   = 10 * time.Second

	alertStateOpen         = "open"
	alertStateAcknowledged = "acknowledged"
	alertStateResolved     = "resolved"

	alertDetected    = "detected"
	alertNotDetected = "not_detected"
	alertUnknown     = "unknown"

	// Event types (append-only).
	alertEvtDetected        = "detected"
	alertEvtRedetected      = "redetected"
	alertEvtNotDetected     = "not_detected"
	alertEvtSeverityChanged = "severity_changed"
	alertEvtAcknowledged    = "acknowledged"
	alertEvtResolved        = "resolved"
	alertEvtReopened        = "reopened"
	alertEvtRecurred        = "recurred"
	alertEvtClosed          = "closed"
	alertEvtMigrated        = "migrated"
	alertEvtNoteUpdated     = "note_updated"

	alertMaxEventsPerAlert      = 1000
	alertIngestedRunsMax        = 500
	alertDefaultHistoryRetainDy = 365
)

// nccAlert is one alert instance (an episode of a logical NCC finding).
type nccAlert struct {
	ID             string `json:"id"`
	FindingID      string `json:"finding_id"`
	CorrelationKey string `json:"correlation_key"`
	Source         string `json:"source"`
	Type           string `json:"type"`
	Cluster        string `json:"cluster"`
	ClusterName    string `json:"cluster_name,omitempty"`
	Check          string `json:"check"`
	Severity       string `json:"severity,omitempty"`

	State     string `json:"state"`
	Detection string `json:"detection"`
	Closed    bool   `json:"closed"`
	ClosedAt  string `json:"closed_at,omitempty"`
	// CloseReason is "resolved_not_detected" (an operator resolved it and a
	// later run confirmed it is gone) or "recurred" (a newer run detected the
	// resolved finding again; see NextID).
	CloseReason string `json:"close_reason,omitempty"`

	FirstSeenAt     string `json:"first_seen_at,omitempty"`
	FirstSeenRun    string `json:"first_seen_run,omitempty"`
	LastSeenAt      string `json:"last_seen_at,omitempty"`
	LastSeenRun     string `json:"last_seen_run,omitempty"`
	NotDetectedRun  string `json:"not_detected_since_run,omitempty"`
	AcknowledgedAt  string `json:"acknowledged_at,omitempty"`
	AcknowledgedBy  string `json:"acknowledged_by,omitempty"`
	ResolvedAt      string `json:"resolved_at,omitempty"`
	ResolvedBy      string `json:"resolved_by,omitempty"`
	ResolvedRun     string `json:"resolved_run,omitempty"`
	Note            string `json:"note,omitempty"`
	Reopened        bool   `json:"reopened,omitempty"`
	ReopenReason    string `json:"reopen_reason,omitempty"`
	PreviousID      string `json:"previous_id,omitempty"`
	NextID          string `json:"next_id,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
	Revision        int    `json:"revision"`
	LegacyMigrated  bool   `json:"legacy_migrated,omitempty"`
	LastActor       string `json:"last_actor,omitempty"`
	LastStateChange string `json:"last_state_change_at,omitempty"`
}

// nccAlertEvent is one immutable lifecycle event.
type nccAlertEvent struct {
	ID             string `json:"id"`
	AlertID        string `json:"alert_id"`
	CorrelationKey string `json:"correlation_key"`
	Type           string `json:"type"`
	At             string `json:"at"`
	Actor          string `json:"actor"`
	RunAt          string `json:"run_at,omitempty"`
	FromState      string `json:"from_state,omitempty"`
	ToState        string `json:"to_state,omitempty"`
	FromSeverity   string `json:"from_severity,omitempty"`
	ToSeverity     string `json:"to_severity,omitempty"`
	Note           string `json:"note,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Seq            int64  `json:"seq"`
}

type alertLifecycleDoc struct {
	SchemaVersion  int             `json:"schema_version"`
	Seq            int64           `json:"seq"`
	Alerts         []nccAlert      `json:"alerts"`
	Events         []nccAlertEvent `json:"events"`
	IngestedRuns   []string        `json:"ingested_runs"`
	LastIngestRun  string          `json:"last_ingest_run,omitempty"`
	LegacyImported bool            `json:"legacy_imported,omitempty"`
}

// nccFinding is one finding observed in a run report.
type nccFinding struct {
	Cluster     string
	ClusterName string
	Check       string
	Severity    string
}

// nccRunSnapshot is the set of findings in one completed run.
type nccRunSnapshot struct {
	RunAt    string
	Findings []nccFinding
	// Covered is the set of normalized cluster keys that the run actually
	// checked successfully. Findings for clusters outside this set are never
	// marked "not detected" (a partial run must not hide other clusters).
	Covered map[string]struct{}
}

type alertLifecycleStore struct {
	path       string
	legacyPath string
	mu         sync.Mutex
	now        func() time.Time
	newID      func() (string, error)
	retainDays int
}

func newAlertLifecycleStore(path, legacyPath string) *alertLifecycleStore {
	return &alertLifecycleStore{
		path:       path,
		legacyPath: legacyPath,
		now:        time.Now,
		newID:      newUUIDv4,
		retainDays: alertDefaultHistoryRetainDy,
	}
}

// newUUIDv4 returns an RFC 4122 version-4 UUID drawn from crypto/rand. It fails
// closed: if the system CSPRNG is unavailable no ID is produced.
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate alert id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

// alertCorrelationKey is the stable logical identity of an NCC finding.
func alertCorrelationKey(cluster, check string) string {
	return "ncc|" + strings.ToLower(strings.TrimSpace(cluster)) + "|" + strings.ToLower(normalizeNCCCheckTitle(check))
}

// alertFindingID is a short, URL-safe, non-reversible digest of the correlation
// key, so integrations can deduplicate on a stable value without embedding
// cluster names in URLs.
func alertFindingID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "f_" + hex.EncodeToString(sum[:12])
}

func severityRank(sev string) int {
	switch strings.ToUpper(strings.TrimSpace(sev)) {
	case "FAIL", "CRITICAL":
		return 4
	case "ERR", "ERROR":
		return 3
	case "WARN", "WARNING":
		return 2
	case "INFO":
		return 1
	}
	return 0
}

func (st *alertLifecycleStore) lockPath() string { return st.path + ".lock" }

// withLock runs fn with the in-process mutex and the cross-process file lock
// held, against a freshly loaded document. When fn returns changed=true the
// document is written back atomically (plus the legacy compatibility mirror).
func (st *alertLifecycleStore) withLock(fn func(doc *alertLifecycleDoc) (bool, error)) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(st.path), 0o700); err != nil {
		return err
	}
	release, err := lockStateFile(st.lockPath(), alertLifecycleLockTimeout)
	if err != nil {
		return err
	}
	defer release()
	doc, err := st.loadLocked()
	if err != nil {
		return err
	}
	migrated := false
	if !doc.LegacyImported {
		migrated, err = st.importLegacyLocked(doc)
		if err != nil {
			return err
		}
	}
	changed, err := fn(doc)
	if err != nil {
		return err
	}
	if !changed && !migrated {
		return nil
	}
	st.pruneLocked(doc)
	return st.saveLocked(doc)
}

// read returns a consistent snapshot without writing (except a one-time legacy
// import, which is persisted so the import is not repeated).
func (st *alertLifecycleStore) read() (*alertLifecycleDoc, error) {
	var out *alertLifecycleDoc
	err := st.withLock(func(doc *alertLifecycleDoc) (bool, error) {
		cp := *doc
		cp.Alerts = append([]nccAlert(nil), doc.Alerts...)
		cp.Events = append([]nccAlertEvent(nil), doc.Events...)
		out = &cp
		return false, nil
	})
	return out, err
}

func (st *alertLifecycleStore) loadLocked() (*alertLifecycleDoc, error) {
	doc := &alertLifecycleDoc{SchemaVersion: alertLifecycleSchemaVersion}
	b, err := os.ReadFile(st.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return doc, nil
		}
		return nil, err
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return doc, nil
	}
	if err := json.Unmarshal(b, doc); err != nil {
		// Never silently replace a corrupt history file: keep it for
		// investigation and refuse to write over it.
		return nil, fmt.Errorf("alert lifecycle store %s is not valid JSON (left untouched for recovery): %w", filepath.Base(st.path), err)
	}
	if doc.SchemaVersion > alertLifecycleSchemaVersion {
		return nil, fmt.Errorf("alert lifecycle store schema %d is newer than this server supports (%d); upgrade the api-server", doc.SchemaVersion, alertLifecycleSchemaVersion)
	}
	if doc.SchemaVersion == 0 {
		doc.SchemaVersion = alertLifecycleSchemaVersion
	}
	return doc, nil
}

func (st *alertLifecycleStore) saveLocked(doc *alertLifecycleDoc) error {
	doc.SchemaVersion = alertLifecycleSchemaVersion
	if doc.Alerts == nil {
		doc.Alerts = []nccAlert{}
	}
	if doc.Events == nil {
		doc.Events = []nccAlertEvent{}
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(st.path, append(b, '\n'), 0o600); err != nil {
		return err
	}
	// Compatibility mirror: keep the legacy dispositions file current so a
	// rollback to an older api-server still sees today's marks.
	if strings.TrimSpace(st.legacyPath) != "" {
		items := legacyDispositionsFromDoc(doc)
		if lb, err := json.MarshalIndent(nccDispositionFile{Items: items}, "", "  "); err == nil {
			_ = writeFileAtomic(st.legacyPath, append(lb, '\n'), 0o644)
		}
	}
	return nil
}

func (st *alertLifecycleStore) stamp() string { return st.now().UTC().Format(time.RFC3339Nano) }

func (st *alertLifecycleStore) appendEvent(doc *alertLifecycleDoc, a *nccAlert, evt nccAlertEvent) error {
	id, err := st.newID()
	if err != nil {
		return err
	}
	doc.Seq++
	evt.ID = id
	evt.Seq = doc.Seq
	evt.AlertID = a.ID
	evt.CorrelationKey = a.CorrelationKey
	if evt.At == "" {
		evt.At = st.stamp()
	}
	doc.Events = append(doc.Events, evt)
	a.UpdatedAt = evt.At
	a.Revision++
	return nil
}

// newAlert creates a new instance with a fresh, unique UUID. Uniqueness is
// enforced against every instance already stored (the persistence-layer
// equivalent of a unique constraint), so a CSPRNG collision can never alias
// two alerts.
func (st *alertLifecycleStore) newAlert(doc *alertLifecycleDoc, cluster, clusterName, check, severity string) (*nccAlert, error) {
	var id string
	for attempt := 0; attempt < 5; attempt++ {
		cand, err := st.newID()
		if err != nil {
			return nil, err
		}
		if findAlertIndex(doc, cand) < 0 {
			id = cand
			break
		}
	}
	if id == "" {
		return nil, errors.New("could not allocate a unique alert id")
	}
	key := alertCorrelationKey(cluster, check)
	now := st.stamp()
	doc.Alerts = append(doc.Alerts, nccAlert{
		ID:             id,
		FindingID:      alertFindingID(key),
		CorrelationKey: key,
		Source:         "NCC",
		Type:           "ncc_check",
		Cluster:        strings.TrimSpace(cluster),
		ClusterName:    strings.TrimSpace(clusterName),
		Check:          normalizeNCCCheckTitle(check),
		Severity:       strings.ToUpper(strings.TrimSpace(severity)),
		State:          alertStateOpen,
		Detection:      alertUnknown,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	return &doc.Alerts[len(doc.Alerts)-1], nil
}

func findAlertIndex(doc *alertLifecycleDoc, id string) int {
	for i := range doc.Alerts {
		if doc.Alerts[i].ID == id {
			return i
		}
	}
	return -1
}

// activeIndexByKey maps correlation key -> index of the single open instance.
func activeIndexByKey(doc *alertLifecycleDoc) map[string]int {
	out := make(map[string]int, len(doc.Alerts))
	for i := range doc.Alerts {
		if doc.Alerts[i].Closed {
			continue
		}
		out[doc.Alerts[i].CorrelationKey] = i
	}
	return out
}

func clusterCoverageKey(cluster string) string { return strings.ToLower(strings.TrimSpace(cluster)) }

// ingestRun applies one completed run's findings to the lifecycle. It is
// idempotent: re-ingesting the same run (same run timestamp) is a no-op, and
// an older run than the last ingested one is ignored, so duplicate or
// out-of-order report delivery cannot rewind state.
//
// Rules (also documented in docs/integrations/third-party-alert-integrations.md):
//   - New finding                      -> new instance, state open, event detected.
//   - Still detected                   -> last_seen updated; acknowledged and
//     resolved states are preserved.
//   - Severity changed                 -> severity_changed event; an escalation
//     (e.g. WARN -> FAIL) reopens an acknowledged instance because the issue
//     materially changed.
//   - Resolved but detected by a newer run -> resolved instance closed
//     (close_reason=recurred) and a new open instance created that links back
//     via previous_id (event recurred). History is never overwritten.
//   - Not in a run that covered its cluster -> detection=not_detected (event
//     not_detected). Operator state is unchanged: "no longer detected" is not
//     "resolved". A resolved instance that is confirmed gone is closed
//     (close_reason=resolved_not_detected).
//   - Detected again after not_detected -> redetected event, same instance,
//     acknowledgement preserved.
func (st *alertLifecycleStore) ingestRun(snap nccRunSnapshot) (int, error) {
	runAt := strings.TrimSpace(snap.RunAt)
	if runAt == "" {
		return 0, nil
	}
	changes := 0
	err := st.withLock(func(doc *alertLifecycleDoc) (bool, error) {
		for _, r := range doc.IngestedRuns {
			if r == runAt {
				return false, nil
			}
		}
		if doc.LastIngestRun != "" && !runIsNewer(runAt, doc.LastIngestRun) {
			return false, nil
		}
		// Collapse duplicate rows for the same cluster+check (keep the worst severity).
		type agg struct {
			f nccFinding
		}
		byKey := map[string]agg{}
		order := []string{}
		for _, f := range snap.Findings {
			if strings.TrimSpace(f.Cluster) == "" || strings.TrimSpace(f.Check) == "" {
				continue
			}
			k := alertCorrelationKey(f.Cluster, f.Check)
			cur, ok := byKey[k]
			if !ok {
				order = append(order, k)
				byKey[k] = agg{f: f}
				continue
			}
			if severityRank(f.Severity) > severityRank(cur.f.Severity) {
				cur.f.Severity = f.Severity
			}
			if cur.f.ClusterName == "" {
				cur.f.ClusterName = f.ClusterName
			}
			byKey[k] = cur
		}
		active := activeIndexByKey(doc)
		seen := map[string]struct{}{}
		for _, k := range order {
			f := byKey[k].f
			seen[k] = struct{}{}
			idx, ok := active[k]
			if !ok {
				a, err := st.newAlert(doc, f.Cluster, f.ClusterName, f.Check, f.Severity)
				if err != nil {
					return false, err
				}
				a.Detection = alertDetected
				a.FirstSeenAt, a.FirstSeenRun = runAt, runAt
				a.LastSeenAt, a.LastSeenRun = runAt, runAt
				if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtDetected, Actor: nccAutoActor, RunAt: runAt, ToState: alertStateOpen, ToSeverity: a.Severity}); err != nil {
					return false, err
				}
				active[k] = len(doc.Alerts) - 1
				changes++
				continue
			}
			a := &doc.Alerts[idx]
			if f.ClusterName != "" && a.ClusterName == "" {
				a.ClusterName = f.ClusterName
			}
			// Recurrence of a resolved finding in a run newer than the resolution.
			if a.State == alertStateResolved {
				basis := a.ResolvedRun
				if basis == "" {
					basis = a.ResolvedAt
				}
				if runIsNewer(runAt, basis) {
					prevID := a.ID
					reason := "This check was generated again on the run at " + runAt + " after it was resolved."
					a.Closed = true
					a.ClosedAt = st.stamp()
					a.CloseReason = "recurred"
					if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtClosed, Actor: nccAutoActor, RunAt: runAt, FromState: alertStateResolved, ToState: alertStateResolved, Reason: reason}); err != nil {
						return false, err
					}
					na, err := st.newAlert(doc, f.Cluster, f.ClusterName, f.Check, f.Severity)
					if err != nil {
						return false, err
					}
					// newAlert may have reallocated the slice; re-resolve the old instance.
					if oi := findAlertIndex(doc, prevID); oi >= 0 {
						doc.Alerts[oi].NextID = na.ID
					}
					na = &doc.Alerts[len(doc.Alerts)-1]
					na.PreviousID = prevID
					na.Reopened = true
					na.ReopenReason = reason
					na.Detection = alertDetected
					na.FirstSeenAt, na.FirstSeenRun = runAt, runAt
					na.LastSeenAt, na.LastSeenRun = runAt, runAt
					na.LastActor = nccAutoActor
					na.LastStateChange = st.stamp()
					if err := st.appendEvent(doc, na, nccAlertEvent{Type: alertEvtRecurred, Actor: nccAutoActor, RunAt: runAt, FromState: alertStateResolved, ToState: alertStateOpen, ToSeverity: na.Severity, Reason: reason}); err != nil {
						return false, err
					}
					active[k] = len(doc.Alerts) - 1
					changes++
					continue
				}
			}
			touched := false
			if a.Detection == alertNotDetected {
				if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtRedetected, Actor: nccAutoActor, RunAt: runAt, FromState: a.State, ToState: a.State}); err != nil {
					return false, err
				}
				a.NotDetectedRun = ""
				touched = true
			}
			a.Detection = alertDetected
			if a.FirstSeenAt == "" {
				a.FirstSeenAt, a.FirstSeenRun = runAt, runAt
			}
			if a.LastSeenRun != runAt {
				a.LastSeenAt, a.LastSeenRun = runAt, runAt
				touched = true
			}
			newSev := strings.ToUpper(strings.TrimSpace(f.Severity))
			if newSev != "" && newSev != a.Severity {
				oldSev := a.Severity
				escalated := oldSev != "" && severityRank(newSev) > severityRank(oldSev)
				a.Severity = newSev
				evt := nccAlertEvent{Type: alertEvtSeverityChanged, Actor: nccAutoActor, RunAt: runAt, FromSeverity: oldSev, ToSeverity: newSev, FromState: a.State, ToState: a.State}
				if escalated && a.State == alertStateAcknowledged {
					evt.ToState = alertStateOpen
					evt.Reason = fmt.Sprintf("Severity escalated from %s to %s, so the acknowledgement was reopened for review.", oldSev, newSev)
					a.State = alertStateOpen
					a.Reopened = true
					a.ReopenReason = evt.Reason
					a.LastActor = nccAutoActor
					a.LastStateChange = st.stamp()
				}
				if err := st.appendEvent(doc, a, evt); err != nil {
					return false, err
				}
				touched = true
			}
			if touched {
				a.UpdatedAt = st.stamp()
				changes++
			}
		}
		// Findings absent from a run that covered their cluster.
		for i := range doc.Alerts {
			a := &doc.Alerts[i]
			if a.Closed {
				continue
			}
			if _, ok := seen[a.CorrelationKey]; ok {
				continue
			}
			if !clusterCovered(snap.Covered, a) {
				continue
			}
			if a.State == alertStateResolved {
				a.Detection = alertNotDetected
				a.Closed = true
				a.ClosedAt = st.stamp()
				a.CloseReason = "resolved_not_detected"
				if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtClosed, Actor: nccAutoActor, RunAt: runAt, FromState: alertStateResolved, ToState: alertStateResolved, Reason: "The run at " + runAt + " no longer reports this check, confirming the resolution."}); err != nil {
					return false, err
				}
				changes++
				continue
			}
			if a.Detection == alertNotDetected {
				continue
			}
			if a.Detection == alertUnknown && a.LastSeenRun == "" {
				// Operator-created mark for a finding we never observed in an
				// ingested run (e.g. created before the first ingestion).
				a.Detection = alertNotDetected
				a.NotDetectedRun = runAt
				a.UpdatedAt = st.stamp()
				changes++
				continue
			}
			a.Detection = alertNotDetected
			a.NotDetectedRun = runAt
			if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtNotDetected, Actor: nccAutoActor, RunAt: runAt, FromState: a.State, ToState: a.State, Reason: "Not reported by the run at " + runAt + ". The operator state is unchanged."}); err != nil {
				return false, err
			}
			changes++
		}
		doc.IngestedRuns = append(doc.IngestedRuns, runAt)
		if len(doc.IngestedRuns) > alertIngestedRunsMax {
			doc.IngestedRuns = append([]string(nil), doc.IngestedRuns[len(doc.IngestedRuns)-alertIngestedRunsMax:]...)
		}
		doc.LastIngestRun = runAt
		return true, nil
	})
	return changes, err
}

func clusterCovered(covered map[string]struct{}, a *nccAlert) bool {
	if len(covered) == 0 {
		return false
	}
	if _, ok := covered[clusterCoverageKey(a.Cluster)]; ok {
		return true
	}
	if a.ClusterName != "" {
		if _, ok := covered[clusterCoverageKey(a.ClusterName)]; ok {
			return true
		}
	}
	return false
}

// alertActionTarget identifies an alert either by instance id or by cluster+check.
type alertActionTarget struct {
	ID      string
	Cluster string
	Check   string
}

var errAlertNotFound = errors.New("alert not found")

// applyAction records an operator acknowledge/resolve/reopen. It is idempotent:
// repeating the current state with the same note does not add an event.
func (st *alertLifecycleStore) applyAction(targets []alertActionTarget, action, actor, note, runAt string, permits func(cluster, clusterName string) bool) ([]nccAlert, error) {
	var toState string
	switch action {
	case "acknowledge":
		toState = alertStateAcknowledged
	case "resolve":
		toState = alertStateResolved
	case "reopen":
		toState = alertStateOpen
	default:
		return nil, fmt.Errorf("action must be acknowledge, resolve, or reopen")
	}
	var out []nccAlert
	err := st.withLock(func(doc *alertLifecycleDoc) (bool, error) {
		out = out[:0]
		changed := false
		active := activeIndexByKey(doc)
		// Validate every target first so a forbidden or unknown target fails
		// the whole request without partial writes.
		idxs := make([]int, len(targets))
		for i, t := range targets {
			idx := -1
			if strings.TrimSpace(t.ID) != "" {
				idx = findAlertIndex(doc, strings.TrimSpace(t.ID))
				if idx < 0 {
					return false, fmt.Errorf("%w: %s", errAlertNotFound, t.ID)
				}
				if doc.Alerts[idx].Closed {
					return false, fmt.Errorf("alert %s is closed; act on its newer instance %s", t.ID, doc.Alerts[idx].NextID)
				}
			} else if j, ok := active[alertCorrelationKey(t.Cluster, t.Check)]; ok {
				idx = j
			}
			cluster, name := t.Cluster, ""
			if idx >= 0 {
				cluster, name = doc.Alerts[idx].Cluster, doc.Alerts[idx].ClusterName
			}
			if permits != nil && !permits(cluster, name) {
				return false, errAlertForbidden
			}
			idxs[i] = idx
		}
		for i, t := range targets {
			idx := idxs[i]
			if idx < 0 {
				// Operator marked a finding that has not been ingested yet;
				// create the instance so the mark persists.
				a, err := st.newAlert(doc, t.Cluster, "", t.Check, "")
				if err != nil {
					return false, err
				}
				idx = len(doc.Alerts) - 1
				active[a.CorrelationKey] = idx
				if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtDetected, Actor: actor, RunAt: runAt, ToState: alertStateOpen, Reason: "Created by an operator action before the finding was ingested from a run."}); err != nil {
					return false, err
				}
				changed = true
			}
			a := &doc.Alerts[idx]
			if a.State == toState && a.Note == note && !(toState == alertStateOpen && a.Reopened) {
				out = append(out, *a)
				continue
			}
			from := a.State
			evtType := map[string]string{alertStateAcknowledged: alertEvtAcknowledged, alertStateResolved: alertEvtResolved, alertStateOpen: alertEvtReopened}[toState]
			if from == toState {
				evtType = alertEvtNoteUpdated
			}
			now := st.stamp()
			a.State = toState
			a.Note = note
			a.LastActor = actor
			a.LastStateChange = now
			switch toState {
			case alertStateAcknowledged:
				a.AcknowledgedAt, a.AcknowledgedBy = now, actor
				a.Reopened, a.ReopenReason = false, ""
			case alertStateResolved:
				a.ResolvedAt, a.ResolvedBy = now, actor
				a.ResolvedRun = runAt
				a.Reopened, a.ReopenReason = false, ""
			case alertStateOpen:
				a.Reopened, a.ReopenReason = false, ""
			}
			if err := st.appendEvent(doc, a, nccAlertEvent{Type: evtType, Actor: actor, RunAt: runAt, FromState: from, ToState: toState, Note: note}); err != nil {
				return false, err
			}
			changed = true
			out = append(out, *a)
		}
		return changed, nil
	})
	return out, err
}

var errAlertForbidden = errors.New("forbidden: alert belongs to a cluster outside your cluster groups")

// importLegacyLocked migrates outputfiles/ncc-alert-dispositions.json into the
// lifecycle store once. The legacy file is left in place (and kept current as
// a mirror) so a rollback to an older api-server keeps working.
func (st *alertLifecycleStore) importLegacyLocked(doc *alertLifecycleDoc) (bool, error) {
	doc.LegacyImported = true
	if strings.TrimSpace(st.legacyPath) == "" {
		return true, nil
	}
	b, err := os.ReadFile(st.legacyPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	var file nccDispositionFile
	if len(strings.TrimSpace(string(b))) > 0 {
		if err := json.Unmarshal(b, &file); err != nil {
			return false, fmt.Errorf("legacy NCC dispositions file is not valid JSON (left untouched): %w", err)
		}
	}
	active := activeIndexByKey(doc)
	for _, item := range file.Items {
		if strings.TrimSpace(item.Cluster) == "" || strings.TrimSpace(item.Check) == "" {
			continue
		}
		key := alertCorrelationKey(item.Cluster, item.Check)
		if _, ok := active[key]; ok {
			continue
		}
		a, err := st.newAlert(doc, item.Cluster, "", item.Check, "")
		if err != nil {
			return false, err
		}
		a.LegacyMigrated = true
		a.Note = item.Note
		a.LastActor = item.By
		a.LastStateChange = item.At
		switch item.Status {
		case "acknowledged":
			a.State = alertStateAcknowledged
			a.AcknowledgedAt, a.AcknowledgedBy = item.At, item.By
		case "resolved":
			a.State = alertStateResolved
			a.ResolvedAt, a.ResolvedBy, a.ResolvedRun = item.At, item.By, item.RunAt
		case "reopened":
			a.State = alertStateOpen
			a.Reopened = true
			a.ReopenReason = item.Reason
		}
		for _, h := range item.History {
			if err := st.appendEvent(doc, a, nccAlertEvent{Type: legacyEventType(h.Status), At: h.At, Actor: h.By, RunAt: h.RunAt, ToState: legacyStateOf(h.Status), Note: h.Note, Reason: h.Reason}); err != nil {
				return false, err
			}
		}
		// The current legacy mark becomes an event carrying its original
		// actor and time, followed by a marker recording the migration.
		if item.Status != "" {
			if err := st.appendEvent(doc, a, nccAlertEvent{Type: legacyEventType(item.Status), At: item.At, Actor: item.By, RunAt: item.RunAt, ToState: a.State, Note: item.Note, Reason: item.Reason}); err != nil {
				return false, err
			}
		}
		if err := st.appendEvent(doc, a, nccAlertEvent{Type: alertEvtMigrated, Actor: nccAutoActor, ToState: a.State,
			Reason: "Imported from the legacy NCC dispositions file (outputfiles/ncc-alert-dispositions.json)."}); err != nil {
			return false, err
		}
		a.UpdatedAt = st.stamp()
		active[key] = len(doc.Alerts) - 1
	}
	return true, nil
}

func legacyEventType(status string) string {
	switch status {
	case "acknowledged":
		return alertEvtAcknowledged
	case "resolved":
		return alertEvtResolved
	case "reopened":
		return alertEvtReopened
	}
	return alertEvtMigrated
}

func legacyStateOf(status string) string {
	switch status {
	case "acknowledged":
		return alertStateAcknowledged
	case "resolved":
		return alertStateResolved
	}
	return alertStateOpen
}

// legacyStatus maps an instance onto the v2.2.0 disposition status vocabulary.
func legacyStatus(a nccAlert) string {
	switch a.State {
	case alertStateAcknowledged:
		return "acknowledged"
	case alertStateResolved:
		return "resolved"
	}
	if a.Reopened {
		return "reopened"
	}
	return ""
}

// legacyDispositionsFromDoc renders the active instances that carry an
// operator mark in the original /alerts/ncc-dispositions shape.
func legacyDispositionsFromDoc(doc *alertLifecycleDoc) []nccDisposition {
	// History spans every instance of the finding (earlier, closed episodes
	// included) so a recurrence still shows who resolved it before.
	eventsByKey := map[string][]nccAlertEvent{}
	for _, e := range doc.Events {
		eventsByKey[e.CorrelationKey] = append(eventsByKey[e.CorrelationKey], e)
	}
	items := []nccDisposition{}
	for _, a := range doc.Alerts {
		if a.Closed {
			continue
		}
		status := legacyStatus(a)
		if status == "" {
			continue
		}
		item := nccDisposition{
			Cluster: a.Cluster,
			Check:   a.Check,
			Status:  status,
			By:      a.LastActor,
			At:      a.LastStateChange,
			Note:    a.Note,
			Reason:  a.ReopenReason,
		}
		switch a.State {
		case alertStateResolved:
			item.RunAt = a.ResolvedRun
		default:
			item.RunAt = a.LastSeenRun
		}
		for _, e := range eventsByKey[a.CorrelationKey] {
			switch e.Type {
			case alertEvtAcknowledged, alertEvtResolved, alertEvtReopened, alertEvtRecurred:
				item.History = append(item.History, nccDispositionEvent{Status: legacyEventStatus(e.Type), By: e.Actor, At: e.At, RunAt: e.RunAt, Note: e.Note, Reason: e.Reason})
			}
		}
		items = append(items, item)
	}
	return items
}

func legacyEventStatus(t string) string {
	switch t {
	case alertEvtAcknowledged:
		return "acknowledged"
	case alertEvtResolved:
		return "resolved"
	}
	return "reopened"
}

// pruneLocked applies history retention: closed instances (and their events)
// older than retainDays are removed; active instances are never pruned, and
// per-instance event lists are capped (keeping the first event so the origin
// of every alert stays visible).
func (st *alertLifecycleStore) pruneLocked(doc *alertLifecycleDoc) {
	if st.retainDays > 0 {
		cutoff := st.now().UTC().Add(-time.Duration(st.retainDays) * 24 * time.Hour)
		drop := map[string]struct{}{}
		kept := doc.Alerts[:0]
		for _, a := range doc.Alerts {
			if a.Closed {
				if t, err := time.Parse(time.RFC3339Nano, a.ClosedAt); err == nil && t.Before(cutoff) {
					drop[a.ID] = struct{}{}
					continue
				}
			}
			kept = append(kept, a)
		}
		doc.Alerts = kept
		if len(drop) > 0 {
			ev := doc.Events[:0]
			for _, e := range doc.Events {
				if _, ok := drop[e.AlertID]; ok {
					continue
				}
				ev = append(ev, e)
			}
			doc.Events = ev
		}
	}
	counts := map[string]int{}
	for _, e := range doc.Events {
		counts[e.AlertID]++
	}
	over := false
	for _, n := range counts {
		if n > alertMaxEventsPerAlert {
			over = true
			break
		}
	}
	if !over {
		return
	}
	seenFirst := map[string]bool{}
	remaining := map[string]int{}
	for id, n := range counts {
		remaining[id] = n
	}
	ev := doc.Events[:0]
	for _, e := range doc.Events {
		if !seenFirst[e.AlertID] {
			seenFirst[e.AlertID] = true
			ev = append(ev, e)
			remaining[e.AlertID]--
			continue
		}
		if remaining[e.AlertID] >= alertMaxEventsPerAlert {
			remaining[e.AlertID]--
			continue
		}
		remaining[e.AlertID]--
		ev = append(ev, e)
	}
	doc.Events = ev
}

// ---- query helpers -------------------------------------------------------

type alertQuery struct {
	State         string
	Detection     string
	Cluster       string
	Check         string
	FindingID     string
	IncludeClosed bool
	UpdatedSince  time.Time
	Cursor        alertCursor
	Limit         int
}

type alertCursor struct {
	At string
	ID string
}

func encodeAlertCursor(at, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(at + "|" + id))
}

func decodeAlertCursor(raw string) (alertCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return alertCursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return alertCursor{}, errors.New("invalid cursor")
	}
	at, id, ok := strings.Cut(string(b), "|")
	if !ok {
		return alertCursor{}, errors.New("invalid cursor")
	}
	if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		return alertCursor{}, errors.New("invalid cursor")
	}
	return alertCursor{At: at, ID: id}, nil
}

func cmpUpdated(a, b string) int {
	ta, ea := time.Parse(time.RFC3339Nano, a)
	tb, eb := time.Parse(time.RFC3339Nano, b)
	if ea == nil && eb == nil {
		return ta.Compare(tb)
	}
	return strings.Compare(a, b)
}

// filterAlerts returns alerts ordered by (updated_at, id) ascending — a stable
// order suitable for cursor-based incremental synchronization — plus the next
// cursor when more results remain.
func filterAlerts(alerts []nccAlert, q alertQuery, permits func(cluster, clusterName string) bool) ([]nccAlert, string) {
	sorted := append([]nccAlert(nil), alerts...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if c := cmpUpdated(sorted[i].UpdatedAt, sorted[j].UpdatedAt); c != 0 {
			return c < 0
		}
		return sorted[i].ID < sorted[j].ID
	})
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	out := make([]nccAlert, 0, limit)
	for _, a := range sorted {
		if a.Closed && !q.IncludeClosed {
			continue
		}
		if q.State != "" && a.State != q.State {
			continue
		}
		if q.Detection != "" && a.Detection != q.Detection {
			continue
		}
		if q.Cluster != "" && !strings.EqualFold(a.Cluster, q.Cluster) && !strings.EqualFold(a.ClusterName, q.Cluster) {
			continue
		}
		if q.Check != "" && !strings.EqualFold(a.Check, normalizeNCCCheckTitle(q.Check)) {
			continue
		}
		if q.FindingID != "" && a.FindingID != q.FindingID {
			continue
		}
		if !q.UpdatedSince.IsZero() {
			if t, err := time.Parse(time.RFC3339Nano, a.UpdatedAt); err == nil && t.Before(q.UpdatedSince) {
				continue
			}
		}
		if q.Cursor.At != "" {
			c := cmpUpdated(a.UpdatedAt, q.Cursor.At)
			if c < 0 || (c == 0 && a.ID <= q.Cursor.ID) {
				continue
			}
		}
		if permits != nil && !permits(a.Cluster, a.ClusterName) {
			continue
		}
		if len(out) == limit {
			last := out[len(out)-1]
			return out, encodeAlertCursor(last.UpdatedAt, last.ID)
		}
		out = append(out, a)
	}
	return out, ""
}

// writeAlertLifecycleMetrics renders aggregate alert lifecycle gauges for
// Prometheus. Labels are bounded (state x detection x severity); per-alert
// series are intentionally not exported to keep cardinality low — use the
// /api/v1/alerts/ncc API for per-alert data.
func (s *apiServer) writeAlertLifecycleMetrics(w io.Writer) {
	doc, err := s.alerts().read()
	if err != nil {
		fmt.Fprintf(w, "# HELP ncc_alerts_store_up 1 if the NCC alert lifecycle store is readable.\n# TYPE ncc_alerts_store_up gauge\nncc_alerts_store_up 0\n")
		return
	}
	type key struct{ state, detection, severity string }
	counts := map[key]int{}
	for _, a := range doc.Alerts {
		if a.Closed {
			continue
		}
		sev := a.Severity
		if sev == "" {
			sev = "UNKNOWN"
		}
		counts[key{a.State, a.Detection, sev}]++
	}
	keys := make([]key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].state != keys[j].state {
			return keys[i].state < keys[j].state
		}
		if keys[i].detection != keys[j].detection {
			return keys[i].detection < keys[j].detection
		}
		return keys[i].severity < keys[j].severity
	})
	fmt.Fprintf(w, "# HELP ncc_alerts_store_up 1 if the NCC alert lifecycle store is readable.\n# TYPE ncc_alerts_store_up gauge\nncc_alerts_store_up 1\n")
	fmt.Fprintf(w, "# HELP ncc_alerts_active Active (not closed) NCC alert instances by operator state, detection and severity.\n# TYPE ncc_alerts_active gauge\n")
	for _, k := range keys {
		fmt.Fprintf(w, "ncc_alerts_active{state=%q,detection=%q,severity=%q} %d\n", k.state, k.detection, k.severity, counts[k])
	}
	var lastRun float64
	if t, err := time.Parse(time.RFC3339Nano, doc.LastIngestRun); err == nil {
		lastRun = float64(t.Unix())
	}
	fmt.Fprintf(w, "# HELP ncc_alerts_last_ingested_run_timestamp_seconds Run timestamp last applied to the alert lifecycle.\n# TYPE ncc_alerts_last_ingested_run_timestamp_seconds gauge\nncc_alerts_last_ingested_run_timestamp_seconds %g\n", lastRun)
	fmt.Fprintf(w, "# HELP ncc_alerts_events_total Lifecycle events retained in the alert history store.\n# TYPE ncc_alerts_events_total gauge\nncc_alerts_events_total %d\n", len(doc.Events))
}
