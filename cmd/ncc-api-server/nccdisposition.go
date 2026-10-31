package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// nccDisposition is the v2.2.0 wire shape of an operator mark on one NCC
// finding (GET/POST /api/v1/alerts/ncc-dispositions). Since v2.2.1 the marks
// are backed by the persistent alert lifecycle store (alertlifecycle.go); this
// type remains the backward-compatible view. Fields after History are
// additive.
type nccDisposition struct {
	Cluster string                `json:"cluster"`
	Check   string                `json:"check"`
	Status  string                `json:"status"` // acknowledged, resolved, reopened, or empty
	By      string                `json:"by"`
	At      string                `json:"at"`
	Note    string                `json:"note,omitempty"`
	RunAt   string                `json:"run_at,omitempty"`
	Reason  string                `json:"reason,omitempty"`
	History []nccDispositionEvent `json:"history,omitempty"`

	ID          string `json:"id,omitempty"`
	FindingID   string `json:"finding_id,omitempty"`
	Detection   string `json:"detection,omitempty"`
	Severity    string `json:"severity,omitempty"`
	FirstSeenAt string `json:"first_seen_at,omitempty"`
	LastSeenAt  string `json:"last_seen_at,omitempty"`
}

type nccDispositionEvent struct {
	Status string `json:"status"`
	By     string `json:"by"`
	At     string `json:"at"`
	RunAt  string `json:"run_at,omitempty"`
	Note   string `json:"note,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type nccDispositionFile struct {
	Items []nccDisposition `json:"items"`
}

var nccDetailPrefix = regexp.MustCompile(`(?i)^detailed information for\s*`)

const (
	nccDispositionNoteMax = 500
	nccDispositionBulkMax = 200
	nccAutoActor          = "NCC Orchestrator"
	alertSyncInterval     = time.Minute
)

func normalizeNCCCheckTitle(check string) string {
	s := strings.TrimSpace(nccDetailPrefix.ReplaceAllString(check, ""))
	s = strings.TrimRight(s, ":")
	return strings.TrimSpace(s)
}

func (s *apiServer) stateRoot() string {
	root := "."
	if s != nil && strings.TrimSpace(s.repoRoot) != "" {
		root = s.repoRoot
	}
	if abs, err := filepath.Abs(root); err == nil {
		return abs
	}
	return root
}

// nccDispositionPath is the legacy (v2.2.0) marks file. It is now only an
// import source and a compatibility mirror for rollbacks; the authoritative
// state is the lifecycle store.
func (s *apiServer) nccDispositionPath() string {
	return filepath.Join(s.stateRoot(), "outputfiles", "ncc-alert-dispositions.json")
}

func (s *apiServer) alertLifecyclePath() string {
	return filepath.Join(s.stateRoot(), alertLifecycleFileName)
}

func (s *apiServer) alerts() *alertLifecycleStore {
	s.alertStoreMu.Lock()
	defer s.alertStoreMu.Unlock()
	if s.alertStore == nil {
		s.alertStore = newAlertLifecycleStore(s.alertLifecyclePath(), s.nccDispositionPath())
		if s.alertHistoryRetainDay >= 0 {
			s.alertStore.retainDays = s.alertHistoryRetainDay
		}
	}
	return s.alertStore
}

// loadNCCDispositions returns the legacy view of the current marks.
func (s *apiServer) loadNCCDispositions() ([]nccDisposition, error) {
	doc, err := s.alerts().read()
	if err != nil {
		return nil, err
	}
	return legacyDispositionsFromDoc(doc), nil
}

func runIsNewer(current, basis string) bool {
	current = strings.TrimSpace(current)
	basis = strings.TrimSpace(basis)
	if current == "" || basis == "" {
		return false
	}
	c, err1 := time.Parse(time.RFC3339Nano, current)
	b, err2 := time.Parse(time.RFC3339Nano, basis)
	if err1 == nil && err2 == nil {
		return c.After(b)
	}
	return current > basis
}

func jsonFieldString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		v, ok := m[key]
		if !ok || v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			s = strings.TrimSpace(fmt.Sprint(v))
		}
		s = strings.TrimSpace(s)
		if s != "" && s != "<nil>" {
			return s
		}
	}
	return ""
}

// readAggRows returns the aggregated NCC rows of a report directory: the AGG
// array embedded in index.html, or — for very large runs where the runner
// moves the rows out of the HTML — the aggregated-data.json sidecar.
// The rows are cached per run and shared: do not modify them.
func readAggRows(outDir string) []interface{} {
	return readAggRowsCached(outDir)
}

// currentNCCRunSnapshot builds the lifecycle input for the latest completed
// run: its timestamp, findings, and the clusters it covered successfully.
func (s *apiServer) currentNCCRunSnapshot() nccRunSnapshot {
	snap := nccRunSnapshot{Covered: map[string]struct{}{}}
	if s == nil {
		return snap
	}
	outDir := s.selectBestReportOutDir()
	summary, _ := readRunSummaryCached(outDir).(map[string]interface{})
	if summary != nil {
		snap.RunAt = jsonFieldString(summary, "timestamp")
		if clusters, ok := summary["clusters"].([]interface{}); ok {
			for _, raw := range clusters {
				c, ok := raw.(map[string]interface{})
				if !ok {
					continue
				}
				if okv, _ := c["ok"].(bool); !okv {
					continue
				}
				for _, k := range []string{"address", "name", "cluster", "cluster_name"} {
					if v := jsonFieldString(c, k); v != "" {
						snap.Covered[clusterCoverageKey(v)] = struct{}{}
					}
				}
			}
		}
	}
	for _, raw := range readAggRows(outDir) {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		check := jsonFieldString(m, "check", "check_name", "title", "alert")
		cluster := jsonFieldString(m, "cluster", "address", "cluster_name")
		if check == "" || cluster == "" {
			continue
		}
		snap.Findings = append(snap.Findings, nccFinding{
			Cluster:     cluster,
			ClusterName: jsonFieldString(m, "clusterName", "cluster_name", "name"),
			Check:       check,
			Severity:    jsonFieldString(m, "severity"),
		})
	}
	return snap
}

// syncNCCAlertLifecycle ingests the latest completed run into the lifecycle
// store. Safe to call often: ingestion is idempotent per run timestamp.
func (s *apiServer) syncNCCAlertLifecycle() (int, error) {
	// Every alerts request calls this. Check the run timestamp first: building
	// the snapshot walks every finding (200k at 7,000 clusters).
	if summary, ok := readRunSummaryCached(s.selectBestReportOutDir()).(map[string]interface{}); ok {
		if s.alerts().hasIngested(jsonFieldString(summary, "timestamp")) {
			return 0, nil
		}
	}
	snap := s.currentNCCRunSnapshot()
	if snap.RunAt == "" {
		return 0, nil
	}
	return s.alerts().ingestRun(snap)
}

// startAlertLifecycleSyncLoop ingests new runs in the background so history
// and lifecycle events are recorded even when nobody has the dashboard open
// (e.g. Kubernetes CronJob runs).
func (s *apiServer) startAlertLifecycleSyncLoop(ctx context.Context) {
	go func() {
		t := time.NewTicker(alertSyncInterval)
		defer t.Stop()
		for {
			// Warm the report first: pages need it, and ingesting a new run's
			// alerts can take longer at fleet scale.
			s.warmReportCache()
			if n, err := s.syncNCCAlertLifecycle(); err != nil {
				log.Printf("alert lifecycle sync: %v", err)
			} else if n > 0 {
				s.auditEvent("alerts.ncc.sync", true, map[string]interface{}{"changes": n})
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// alertPermits returns a predicate enforcing cluster-group scoping for the
// caller. Unrestricted callers get nil (no filtering).
func (s *apiServer) alertPermits(r *http.Request) func(cluster, clusterName string) bool {
	p, ok := principalFromContext(r.Context())
	if !ok {
		return nil
	}
	access := s.allowedClusters(p)
	if access.unrestricted {
		return nil
	}
	return func(cluster, clusterName string) bool {
		return access.permits(cluster) || (clusterName != "" && access.permits(clusterName))
	}
}

func dispositionFromAlert(a nccAlert) nccDisposition {
	d := nccDisposition{
		Cluster:     a.Cluster,
		Check:       a.Check,
		Status:      legacyStatus(a),
		By:          a.LastActor,
		At:          a.LastStateChange,
		Note:        a.Note,
		Reason:      a.ReopenReason,
		ID:          a.ID,
		FindingID:   a.FindingID,
		Detection:   a.Detection,
		Severity:    a.Severity,
		FirstSeenAt: a.FirstSeenAt,
		LastSeenAt:  a.LastSeenAt,
	}
	if a.State == alertStateResolved {
		d.RunAt = a.ResolvedRun
	} else {
		d.RunAt = a.LastSeenRun
	}
	return d
}

func (s *apiServer) handleNCCDispositions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetNCCDispositions(w, r)
	case http.MethodPost:
		if !s.loadFeatureFlags().AlertHandling {
			writeFeatureDisabled(w, "Alert handling")
			return
		}
		s.handlePostNCCDispositions(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleGetNCCDispositions(w http.ResponseWriter, r *http.Request) {
	if n, err := s.syncNCCAlertLifecycle(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not update NCC alert history from the latest run: " + sanitizeErr(err)})
		return
	} else if n > 0 {
		s.audit(r, "alerts.ncc.sync", true, map[string]interface{}{"changes": n})
	}
	doc, err := s.alerts().read()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not read NCC alert marks: " + sanitizeErr(err)})
		return
	}
	permits := s.alertPermits(r)
	activeByKey := map[string]nccAlert{}
	for _, a := range doc.Alerts {
		if !a.Closed {
			activeByKey[a.CorrelationKey] = a
		}
	}
	items := []nccDisposition{}
	for _, item := range legacyDispositionsFromDoc(doc) {
		a, ok := activeByKey[alertCorrelationKey(item.Cluster, item.Check)]
		if permits != nil && !permits(item.Cluster, a.ClusterName) {
			continue
		}
		// Enrich with the lifecycle identity of the active instance.
		if ok {
			full := dispositionFromAlert(a)
			full.History = item.History
			item = full
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"items": items}})
}

func (s *apiServer) handlePostNCCDispositions(w http.ResponseWriter, r *http.Request) {
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req struct {
		ID      string `json:"id"`
		Cluster string `json:"cluster"`
		Check   string `json:"check"`
		Action  string `json:"action"`
		Note    string `json:"note"`
		RunAt   string `json:"run_at"`
		Items   []struct {
			ID      string `json:"id"`
			Cluster string `json:"cluster"`
			Check   string `json:"check"`
		} `json:"items"`
	}
	if err := decodeJSON(http.MaxBytesReader(w, r.Body, 1<<20), &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action != "acknowledge" && action != "resolve" && action != "reopen" {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Action must be acknowledge, resolve, or reopen."})
		return
	}
	note := strings.TrimSpace(req.Note)
	if utf8.RuneCountInString(note) > nccDispositionNoteMax {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "The note is too long. Keep it under 500 characters."})
		return
	}
	targets := make([]alertActionTarget, 0, len(req.Items)+1)
	if len(req.Items) > 0 {
		if len(req.Items) > nccDispositionBulkMax {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("Select at most %d alerts at a time.", nccDispositionBulkMax)})
			return
		}
		for _, item := range req.Items {
			t := alertActionTarget{ID: strings.TrimSpace(item.ID), Cluster: strings.TrimSpace(item.Cluster), Check: strings.TrimSpace(item.Check)}
			if t.ID == "" && (t.Cluster == "" || t.Check == "") {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Each selected alert needs an id, or a cluster and a check name."})
				return
			}
			targets = append(targets, t)
		}
	} else {
		t := alertActionTarget{ID: strings.TrimSpace(req.ID), Cluster: strings.TrimSpace(req.Cluster), Check: strings.TrimSpace(req.Check)}
		if t.ID == "" && (t.Cluster == "" || t.Check == "") {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Choose an NCC alert before marking it. Cluster and check name are both required."})
			return
		}
		targets = append(targets, t)
	}
	runAt := strings.TrimSpace(req.RunAt)
	if _, err := time.Parse(time.RFC3339, runAt); err != nil {
		runAt = s.currentNCCRunSnapshot().RunAt
	}
	// Bring the lifecycle up to date first so the mark lands on the current
	// instance rather than one a newer run is about to supersede.
	if _, err := s.syncNCCAlertLifecycle(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not update NCC alert history from the latest run: " + sanitizeErr(err)})
		return
	}
	actor := actorFromRequest(r)
	saved, err := s.alerts().applyAction(targets, action, actor, note, runAt, s.alertPermits(r))
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, errAlertForbidden):
			status = http.StatusForbidden
		case errors.Is(err, errAlertNotFound):
			status = http.StatusNotFound
		case strings.Contains(err.Error(), "is closed"):
			status = http.StatusConflict
		case errors.Is(err, errStateLockTimeout):
			status = http.StatusServiceUnavailable
		}
		s.audit(r, "alerts.ncc."+action, false, map[string]interface{}{"count": len(targets), "error": sanitizeErr(err)})
		writeJSON(w, status, envelope{Success: false, Error: "could not save the NCC alert mark: " + sanitizeErr(err)})
		return
	}
	auditFields := map[string]interface{}{
		"count":  len(targets),
		"status": map[string]string{"acknowledge": "acknowledged", "resolve": "resolved", "reopen": ""}[action],
		"by":     actor,
		"run_at": runAt,
	}
	ids := make([]string, 0, len(saved))
	for _, a := range saved {
		ids = append(ids, a.ID)
	}
	auditFields["alert_ids"] = ids
	if len(saved) == 1 {
		auditFields["cluster"] = saved[0].Cluster
		auditFields["check"] = saved[0].Check
	}
	s.audit(r, "alerts.ncc."+action, true, auditFields)
	out := make([]nccDisposition, 0, len(saved))
	for _, a := range saved {
		out = append(out, dispositionFromAlert(a))
	}
	if len(out) == 1 {
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "NCC alert updated", Data: out[0]})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "NCC alerts updated", Data: map[string]interface{}{
		"items": out,
		"count": len(out),
	}})
}

func actorFromRequest(r *http.Request) string {
	if p, ok := principalFromContext(r.Context()); ok && strings.TrimSpace(p.subject) != "" {
		return p.subject
	}
	return "unknown"
}

// sanitizeErr strips absolute filesystem paths from error text before it is
// returned to API clients; the full error is still logged server-side.
func sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	return redactPaths(msg)
}

var absPathPattern = regexp.MustCompile(`(?:[A-Za-z]:\\|/)(?:[^\s"':,;]+[/\\])+[^\s"':,;]*`)

// redactPaths replaces absolute POSIX or Windows paths with their base name.
func redactPaths(msg string) string {
	return absPathPattern.ReplaceAllStringFunc(msg, func(p string) string {
		base := filepath.Base(strings.ReplaceAll(p, "\\", "/"))
		if base == "" || base == "." || base == "/" {
			return "<path>"
		}
		return "<…>/" + base
	})
}

// ---- /api/v1/alerts/ncc (lifecycle + history API) -------------------------

// handleNCCAlerts serves:
//
//	GET  /api/v1/alerts/ncc                     list alert instances (filters + cursor paging)
//	GET  /api/v1/alerts/ncc/events              lifecycle event feed (cursor = seq)
//	GET  /api/v1/alerts/ncc/{id}                one instance with its events and chain
//	POST /api/v1/alerts/ncc/{id}/{acknowledge|resolve|reopen}
func (s *apiServer) handleNCCAlerts(w http.ResponseWriter, r *http.Request) {
	tail := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/alerts/ncc"), "/")
	switch tail {
	case "":
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
			return
		}
		s.handleListNCCAlerts(w, r)
	case "events":
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
			return
		}
		s.handleNCCAlertEvents(w, r)
	default:
		parts := strings.Split(tail, "/")
		if len(parts) > 2 || !alertIDPattern.MatchString(parts[0]) {
			writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "alert not found"})
			return
		}
		if len(parts) == 1 {
			if r.Method != http.MethodGet {
				writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
				return
			}
			s.handleGetNCCAlert(w, r, parts[0])
			return
		}
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
			return
		}
		if !s.loadFeatureFlags().AlertHandling {
			writeFeatureDisabled(w, "Alert handling")
			return
		}
		s.handleNCCAlertAction(w, r, parts[0], parts[1])
	}
}

var alertIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func parseAlertLimit(raw string, def, max int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, errors.New("invalid limit: must be a positive integer")
	}
	if n > max {
		n = max
	}
	return n, nil
}

func (s *apiServer) handleListNCCAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := parseAlertLimit(q.Get("limit"), 100, 1000)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	query := alertQuery{
		State:         strings.ToLower(strings.TrimSpace(q.Get("state"))),
		Detection:     strings.ToLower(strings.TrimSpace(q.Get("detection"))),
		Cluster:       strings.TrimSpace(q.Get("cluster")),
		Check:         strings.TrimSpace(q.Get("check")),
		FindingID:     strings.TrimSpace(q.Get("finding_id")),
		IncludeClosed: q.Get("include_closed") == "1" || strings.EqualFold(q.Get("include_closed"), "true"),
		Limit:         limit,
	}
	if query.State != "" && query.State != alertStateOpen && query.State != alertStateAcknowledged && query.State != alertStateResolved {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid state: must be open, acknowledged, or resolved"})
		return
	}
	if query.Detection != "" && query.Detection != alertDetected && query.Detection != alertNotDetected && query.Detection != alertUnknown {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid detection: must be detected, not_detected, or unknown"})
		return
	}
	if raw := strings.TrimSpace(q.Get("updated_since")); raw != "" {
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid updated_since: expected an RFC3339 timestamp"})
			return
		}
		query.UpdatedSince = t
	}
	cur, err := decodeAlertCursor(q.Get("cursor"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	query.Cursor = cur
	if _, err := s.syncNCCAlertLifecycle(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not update NCC alert history: " + sanitizeErr(err)})
		return
	}
	doc, err := s.alerts().read()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not read NCC alert history: " + sanitizeErr(err)})
		return
	}
	items, next := filterAlerts(doc.Alerts, query, s.alertPermits(r))
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items":       items,
		"count":       len(items),
		"next_cursor": next,
		"last_run":    doc.LastIngestRun,
	}})
}

func (s *apiServer) handleNCCAlertEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := parseAlertLimit(q.Get("limit"), 500, 5000)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	var after int64
	if raw := strings.TrimSpace(q.Get("cursor")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "invalid cursor: expected the next_cursor value from a previous response"})
			return
		}
		after = n
	}
	alertID := strings.TrimSpace(q.Get("alert_id"))
	if _, err := s.syncNCCAlertLifecycle(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not update NCC alert history: " + sanitizeErr(err)})
		return
	}
	doc, err := s.alerts().read()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not read NCC alert history: " + sanitizeErr(err)})
		return
	}
	permits := s.alertPermits(r)
	allowed := map[string]bool{}
	for _, a := range doc.Alerts {
		allowed[a.ID] = permits == nil || permits(a.Cluster, a.ClusterName)
	}
	out := make([]nccAlertEvent, 0, limit)
	next := after
	more := false
	for _, e := range doc.Events {
		if e.Seq <= after {
			continue
		}
		if alertID != "" && e.AlertID != alertID {
			continue
		}
		if !allowed[e.AlertID] {
			continue
		}
		if len(out) == limit {
			more = true
			break
		}
		out = append(out, e)
		next = e.Seq
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"items":       out,
		"count":       len(out),
		"next_cursor": strconv.FormatInt(next, 10),
		"has_more":    more,
	}})
}

func (s *apiServer) handleGetNCCAlert(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := s.syncNCCAlertLifecycle(); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not update NCC alert history: " + sanitizeErr(err)})
		return
	}
	doc, err := s.alerts().read()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not read NCC alert history: " + sanitizeErr(err)})
		return
	}
	idx := findAlertIndex(doc, id)
	permits := s.alertPermits(r)
	if idx < 0 || (permits != nil && !permits(doc.Alerts[idx].Cluster, doc.Alerts[idx].ClusterName)) {
		// Out-of-scope alerts are indistinguishable from missing ones.
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "alert not found"})
		return
	}
	a := doc.Alerts[idx]
	events := []nccAlertEvent{}
	for _, e := range doc.Events {
		if e.AlertID == id {
			events = append(events, e)
		}
	}
	// Chain of earlier/later instances of the same finding.
	chain := []map[string]string{}
	for _, other := range doc.Alerts {
		if other.CorrelationKey == a.CorrelationKey && other.ID != a.ID {
			chain = append(chain, map[string]string{"id": other.ID, "state": other.State, "created_at": other.CreatedAt, "closed_at": other.ClosedAt, "close_reason": other.CloseReason})
		}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
		"alert":     a,
		"events":    events,
		"instances": chain,
	}})
}

func (s *apiServer) handleNCCAlertAction(w http.ResponseWriter, r *http.Request, id, action string) {
	action = strings.ToLower(strings.TrimSpace(action))
	if action != "acknowledge" && action != "resolve" && action != "reopen" {
		writeJSON(w, http.StatusNotFound, envelope{Success: false, Error: "unknown action: use acknowledge, resolve, or reopen"})
		return
	}
	var body struct {
		Note string `json:"note"`
	}
	if r.ContentLength != 0 {
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		if err := decodeJSON(http.MaxBytesReader(w, r.Body, 16<<10), &body); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
	}
	note := strings.TrimSpace(body.Note)
	if utf8.RuneCountInString(note) > nccDispositionNoteMax {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "The note is too long. Keep it under 500 characters."})
		return
	}
	actor := actorFromRequest(r)
	runAt := s.currentNCCRunSnapshot().RunAt
	saved, err := s.alerts().applyAction([]alertActionTarget{{ID: id}}, action, actor, note, runAt, s.alertPermits(r))
	if err != nil {
		status := http.StatusInternalServerError
		msg := "could not update the alert: " + sanitizeErr(err)
		switch {
		case errors.Is(err, errAlertForbidden), errors.Is(err, errAlertNotFound):
			status, msg = http.StatusNotFound, "alert not found"
		case strings.Contains(err.Error(), "is closed"):
			status = http.StatusConflict
		case errors.Is(err, errStateLockTimeout):
			status = http.StatusServiceUnavailable
		}
		s.audit(r, "alerts.ncc."+action, false, map[string]interface{}{"alert_id": id, "error": msg})
		writeJSON(w, status, envelope{Success: false, Error: msg})
		return
	}
	s.audit(r, "alerts.ncc."+action, true, map[string]interface{}{"alert_id": id, "by": actor, "run_at": runAt, "cluster": saved[0].Cluster, "check": saved[0].Check})
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "NCC alert updated", Data: saved[0]})
}
