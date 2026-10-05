package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// nccDisposition is an operator mark on one NCC finding. Prism Central alerts
// keep their own acknowledged/resolved flags; this store is only for NCC rows.
// A resolved mark is cleared automatically when a later run still generates
// the same cluster and check.
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

type nccDispositionTarget struct {
	Cluster string
	Check   string
}

var (
	nccDispositionMu sync.Mutex
	nccDetailPrefix  = regexp.MustCompile(`(?i)^detailed information for\s*`)
)

const (
	nccDispositionNoteMax = 500
	nccDispositionBulkMax = 200
	nccDispositionHistMax = 20
	nccAutoActor          = "NCC Orchestrator"
)

func nccDispositionKey(cluster, check string) string {
	return strings.ToLower(strings.TrimSpace(cluster)) + "|" + strings.ToLower(strings.TrimSpace(check))
}

func normalizeNCCCheckTitle(check string) string {
	s := strings.TrimSpace(nccDetailPrefix.ReplaceAllString(check, ""))
	s = strings.TrimRight(s, ":")
	return strings.TrimSpace(s)
}

func (s *apiServer) nccDispositionPath() string {
	root := "."
	if s != nil && strings.TrimSpace(s.repoRoot) != "" {
		root = s.repoRoot
	}
	return filepath.Join(root, "outputfiles", "ncc-alert-dispositions.json")
}

func (s *apiServer) loadNCCDispositions() ([]nccDisposition, error) {
	b, err := os.ReadFile(s.nccDispositionPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var file nccDispositionFile
	if len(strings.TrimSpace(string(b))) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(b, &file); err != nil {
		return nil, err
	}
	return file.Items, nil
}

func (s *apiServer) saveNCCDispositions(items []nccDisposition) error {
	path := s.nccDispositionPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if items == nil {
		items = []nccDisposition{}
	}
	b, err := json.MarshalIndent(nccDispositionFile{Items: items}, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func capDispositionHistory(h []nccDispositionEvent) []nccDispositionEvent {
	if len(h) <= nccDispositionHistMax {
		return h
	}
	out := make([]nccDispositionEvent, nccDispositionHistMax)
	copy(out, h[len(h)-nccDispositionHistMax:])
	return out
}

func snapshotDisposition(item nccDisposition) nccDispositionEvent {
	return nccDispositionEvent{
		Status: item.Status,
		By:     item.By,
		At:     item.At,
		RunAt:  item.RunAt,
		Note:   item.Note,
		Reason: item.Reason,
	}
}

func runIsNewer(current, basis string) bool {
	current = strings.TrimSpace(current)
	basis = strings.TrimSpace(basis)
	if current == "" || basis == "" {
		return false
	}
	c, err1 := time.Parse(time.RFC3339, current)
	b, err2 := time.Parse(time.RFC3339, basis)
	if err1 == nil && err2 == nil {
		return c.After(b)
	}
	return current > basis
}

// applyResolvedRecurrence clears resolved marks when a newer run still
// contains the same cluster and check. The record stays, with status
// "reopened", so the UI can show why the mark disappeared.
func applyResolvedRecurrence(items []nccDisposition, currentRun string, present map[string]struct{}, now time.Time) ([]nccDisposition, int) {
	currentRun = strings.TrimSpace(currentRun)
	if currentRun == "" || len(present) == 0 || len(items) == 0 {
		return items, 0
	}
	changed := 0
	out := make([]nccDisposition, len(items))
	copy(out, items)
	at := now.UTC().Format(time.RFC3339)
	for i := range out {
		item := &out[i]
		if item.Status != "resolved" {
			continue
		}
		basis := strings.TrimSpace(item.RunAt)
		if basis == "" {
			basis = strings.TrimSpace(item.At)
		}
		if !runIsNewer(currentRun, basis) {
			continue
		}
		if _, ok := present[nccDispositionKey(item.Cluster, item.Check)]; !ok {
			continue
		}
		reason := "This check was generated again on the run at " + currentRun + ", so the resolved mark was cleared."
		hist := append([]nccDispositionEvent{}, item.History...)
		item.History = capDispositionHistory(append(hist, snapshotDisposition(*item), nccDispositionEvent{
			Status: "reopened",
			By:     nccAutoActor,
			At:     at,
			RunAt:  currentRun,
			Reason: reason,
		}))
		item.Status = "reopened"
		item.By = nccAutoActor
		item.At = at
		item.RunAt = currentRun
		item.Note = ""
		item.Reason = reason
		changed++
	}
	return out, changed
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

func addFindingKey(present map[string]struct{}, cluster, check string) {
	cluster = strings.TrimSpace(cluster)
	check = strings.TrimSpace(check)
	if cluster == "" || check == "" {
		return
	}
	present[nccDispositionKey(cluster, check)] = struct{}{}
	if normalized := normalizeNCCCheckTitle(check); normalized != "" && normalized != check {
		present[nccDispositionKey(cluster, normalized)] = struct{}{}
	}
}

func (s *apiServer) currentNCCRunFindings() (string, map[string]struct{}) {
	present := map[string]struct{}{}
	if s == nil {
		return "", present
	}
	outDir := s.selectBestReportOutDir()
	summary, _ := readJSONArtifact(filepath.Join(outDir, "run-summary.json"), map[string]interface{}{}).(map[string]interface{})
	runAt := ""
	if summary != nil {
		runAt = jsonFieldString(summary, "timestamp")
	}
	rows, _ := readInlineJSONVar(filepath.Join(outDir, "index.html"), "AGG", []interface{}{}).([]interface{})
	for _, raw := range rows {
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		check := jsonFieldString(m, "check", "check_name", "title", "alert")
		clusters := []string{
			jsonFieldString(m, "cluster"),
			jsonFieldString(m, "cluster_name"),
			jsonFieldString(m, "address"),
			jsonFieldString(m, "name"),
		}
		for _, cluster := range clusters {
			addFindingKey(present, cluster, check)
		}
	}
	return runAt, present
}

func (s *apiServer) reconcileNCCDispositions(items []nccDisposition, now time.Time) ([]nccDisposition, int) {
	runAt, present := s.currentNCCRunFindings()
	return applyResolvedRecurrence(items, runAt, present, now)
}

func (s *apiServer) handleNCCDispositions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetNCCDispositions(w, r)
	case http.MethodPost:
		s.handlePostNCCDispositions(w, r)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

func (s *apiServer) handleGetNCCDispositions(w http.ResponseWriter, r *http.Request) {
	nccDispositionMu.Lock()
	defer nccDispositionMu.Unlock()
	items, err := s.loadNCCDispositions()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not read NCC alert marks: " + err.Error()})
		return
	}
	next, reopened := s.reconcileNCCDispositions(items, time.Now())
	if reopened > 0 {
		if err := s.saveNCCDispositions(next); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not update NCC alert marks after a later run: " + err.Error()})
			return
		}
		s.audit(r, "alerts.ncc.auto-reopen", true, map[string]interface{}{
			"count": reopened,
		})
		items = next
	}
	if items == nil {
		items = []nccDisposition{}
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{"items": items}})
}

func (s *apiServer) handlePostNCCDispositions(w http.ResponseWriter, r *http.Request) {
	if err := requireJSONContentType(r); err != nil {
		writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
		return
	}
	var req struct {
		Cluster string `json:"cluster"`
		Check   string `json:"check"`
		Action  string `json:"action"`
		Note    string `json:"note"`
		RunAt   string `json:"run_at"`
		Items   []struct {
			Cluster string `json:"cluster"`
			Check   string `json:"check"`
		} `json:"items"`
	}
	if err := decodeJSON(http.MaxBytesReader(w, r.Body, 1<<20), &req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	status := ""
	switch action {
	case "acknowledge":
		status = "acknowledged"
	case "resolve":
		status = "resolved"
	case "reopen":
		status = ""
	default:
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Action must be acknowledge, resolve, or reopen."})
		return
	}
	note := strings.TrimSpace(req.Note)
	if utf8.RuneCountInString(note) > nccDispositionNoteMax {
		writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "The note is too long. Keep it under 500 characters."})
		return
	}
	targets := make([]nccDispositionTarget, 0, len(req.Items)+1)
	if len(req.Items) > 0 {
		if len(req.Items) > nccDispositionBulkMax {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("Select at most %d alerts at a time.", nccDispositionBulkMax)})
			return
		}
		for _, item := range req.Items {
			cluster := strings.TrimSpace(item.Cluster)
			check := strings.TrimSpace(item.Check)
			if cluster == "" || check == "" {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Each selected alert needs a cluster and a check name."})
				return
			}
			targets = append(targets, nccDispositionTarget{Cluster: cluster, Check: check})
		}
	} else {
		cluster := strings.TrimSpace(req.Cluster)
		check := strings.TrimSpace(req.Check)
		if cluster == "" || check == "" {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: "Choose an NCC alert before marking it. Cluster and check name are both required."})
			return
		}
		targets = append(targets, nccDispositionTarget{Cluster: cluster, Check: check})
	}
	runAt := strings.TrimSpace(req.RunAt)
	if _, err := time.Parse(time.RFC3339, runAt); err != nil {
		runAt, _ = s.currentNCCRunFindings()
	}
	actor := "unknown"
	if p, ok := principalFromContext(r.Context()); ok && strings.TrimSpace(p.subject) != "" {
		actor = p.subject
	}
	now := time.Now().UTC().Format(time.RFC3339)

	nccDispositionMu.Lock()
	defer nccDispositionMu.Unlock()
	items, err := s.loadNCCDispositions()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not read NCC alert marks: " + err.Error()})
		return
	}
	byKey := map[string]nccDisposition{}
	order := make([]string, 0, len(items))
	for _, item := range items {
		key := nccDispositionKey(item.Cluster, item.Check)
		if _, ok := byKey[key]; ok {
			continue
		}
		byKey[key] = item
		order = append(order, key)
	}
	saved := make([]nccDisposition, 0, len(targets))
	for _, target := range targets {
		key := nccDispositionKey(target.Cluster, target.Check)
		prev, existed := byKey[key]
		if status == "" {
			delete(byKey, key)
			saved = append(saved, nccDisposition{Cluster: target.Cluster, Check: target.Check, Status: ""})
			continue
		}
		next := nccDisposition{
			Cluster: target.Cluster,
			Check:   target.Check,
			Status:  status,
			By:      actor,
			At:      now,
			Note:    note,
			RunAt:   runAt,
		}
		if existed && (prev.Status != "" || len(prev.History) > 0) {
			next.History = capDispositionHistory(append(prev.History, snapshotDisposition(prev)))
		}
		byKey[key] = next
		if !existed {
			order = append(order, key)
		}
		saved = append(saved, next)
	}
	nextItems := make([]nccDisposition, 0, len(order))
	for _, key := range order {
		item, ok := byKey[key]
		if !ok || item.Status == "" {
			continue
		}
		nextItems = append(nextItems, item)
	}
	if err := s.saveNCCDispositions(nextItems); err != nil {
		writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: "could not save the NCC alert mark: " + err.Error()})
		return
	}
	auditFields := map[string]interface{}{
		"count":  len(targets),
		"status": status,
		"by":     actor,
		"run_at": runAt,
	}
	if len(targets) == 1 {
		auditFields["cluster"] = targets[0].Cluster
		auditFields["check"] = targets[0].Check
	}
	s.audit(r, "alerts.ncc."+action, true, auditFields)
	if len(saved) == 1 {
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "NCC alert updated", Data: saved[0]})
		return
	}
	writeJSON(w, http.StatusOK, envelope{Success: true, Message: "NCC alerts updated", Data: map[string]interface{}{
		"items": saved,
		"count": len(saved),
	}})
}
