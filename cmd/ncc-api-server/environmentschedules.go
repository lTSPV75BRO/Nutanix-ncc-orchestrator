package main

// Environment schedules: run an environment on a timetable (prod every day at
// 02:00, dev every week ...). They run inside the API server through the same
// run engine as manual runs, so results merge the same way and each run is
// tagged with its environment. The OS-level schedule (/api/v1/schedule) is
// unchanged and keeps running the main config.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	environmentScheduleMinInterval = 15 * time.Minute
	maxEnvironmentSchedules        = 50
)

var reClockTime = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

type environmentSchedule struct {
	ID          string `json:"id"`
	Environment string `json:"environment"`
	Enabled     bool   `json:"enabled"`
	// Every is an interval: 15m, 4h, 1d, 7d.
	Every string `json:"every"`
	// At pins day-based intervals to a time of day (HH:MM, server local time).
	At        string `json:"at,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`

	LastRunAt  string `json:"last_run_at,omitempty"`
	LastRunID  string `json:"last_run_id,omitempty"`
	LastResult string `json:"last_result,omitempty"`
	NextRunAt  string `json:"next_run_at,omitempty"`
}

var environmentScheduleMu sync.Mutex

func (s *apiServer) environmentSchedulesPath() string {
	return filepath.Join(s.environmentsDir(), ".schedules.json")
}

func (s *apiServer) loadEnvironmentSchedules() ([]environmentSchedule, error) {
	b, err := os.ReadFile(s.environmentSchedulesPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []environmentSchedule{}, nil
		}
		return nil, err
	}
	var out []environmentSchedule
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = []environmentSchedule{}
	}
	return out, nil
}

func (s *apiServer) saveEnvironmentSchedules(list []environmentSchedule) error {
	if err := os.MkdirAll(s.environmentsDir(), 0o755); err != nil {
		return err
	}
	for i := range list {
		list[i].NextRunAt = "" // derived on read
	}
	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.environmentSchedulesPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.environmentSchedulesPath())
}

func (s *apiServer) removeEnvironmentSchedules(env string) {
	environmentScheduleMu.Lock()
	defer environmentScheduleMu.Unlock()
	list, err := s.loadEnvironmentSchedules()
	if err != nil {
		return
	}
	kept := list[:0]
	for _, e := range list {
		if e.Environment != env {
			kept = append(kept, e)
		}
	}
	if len(kept) != len(list) {
		_ = s.saveEnvironmentSchedules(kept)
	}
}

func newScheduleID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// nextEnvironmentRun is when an entry next runs, or zero when disabled/invalid.
func nextEnvironmentRun(e environmentSchedule, loc *time.Location) time.Time {
	if !e.Enabled {
		return time.Time{}
	}
	every, err := parseEveryDuration(e.Every)
	if err != nil {
		return time.Time{}
	}
	var base time.Time
	if t, err := time.Parse(time.RFC3339, e.LastRunAt); err == nil {
		base = t
	} else if t, err := time.Parse(time.RFC3339, e.CreatedAt); err == nil {
		base = t
	} else {
		return time.Time{}
	}
	if e.At == "" || every%(24*time.Hour) != 0 {
		return base.Add(every)
	}
	hh, mm := 0, 0
	_, _ = fmt.Sscanf(e.At, "%d:%d", &hh, &mm) // At is validated as HH:MM on save
	local := base.In(loc)
	if e.LastRunAt == "" {
		// First run: the next occurrence of At after the entry was created.
		cand := time.Date(local.Year(), local.Month(), local.Day(), hh, mm, 0, 0, loc)
		if !cand.After(local) {
			cand = cand.AddDate(0, 0, 1)
		}
		return cand
	}
	next := local.Add(every)
	cand := time.Date(next.Year(), next.Month(), next.Day(), hh, mm, 0, 0, loc)
	// A late previous run must not push the next one a whole day out.
	if cand.Sub(local) < every-12*time.Hour {
		cand = cand.AddDate(0, 0, 1)
	}
	return cand
}

func validateEnvironmentSchedule(e environmentSchedule, envExists func(string) bool) error {
	if err := validEnvironmentName(e.Environment); err != nil {
		return err
	}
	if !envExists(e.Environment) {
		return fmt.Errorf("environment %q does not exist", e.Environment)
	}
	d, err := parseEveryDuration(e.Every)
	if err != nil {
		return err
	}
	if d < environmentScheduleMinInterval {
		return fmt.Errorf("runs can be at most every %s", environmentScheduleMinInterval)
	}
	if e.At != "" {
		if !reClockTime.MatchString(e.At) {
			return errors.New("at must be a time like 02:00")
		}
		if d%(24*time.Hour) != 0 {
			return errors.New("a time of day needs an interval in whole days (1d, 7d)")
		}
	}
	return nil
}

// handleEnvironmentSchedules serves GET (list with next run times) and PUT
// (replace the whole list) on /api/v1/environment-schedules.
func (s *apiServer) handleEnvironmentSchedules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		list, err := s.loadEnvironmentSchedules()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		for i := range list {
			if t := nextEnvironmentRun(list[i], time.Local); !t.IsZero() {
				list[i].NextRunAt = t.UTC().Format(time.RFC3339)
			}
		}
		_, offset := time.Now().Zone()
		writeJSON(w, http.StatusOK, envelope{Success: true, Data: map[string]interface{}{
			"items":           list,
			"server_timezone": time.Now().Format("MST"),
			"utc_offset_min":  offset / 60,
		}})
	case http.MethodPut:
		if err := requireJSONContentType(r); err != nil {
			writeJSON(w, http.StatusUnsupportedMediaType, envelope{Success: false, Error: err.Error()})
			return
		}
		var req struct {
			Items []environmentSchedule `json:"items"`
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: err.Error()})
			return
		}
		if len(req.Items) > maxEnvironmentSchedules {
			writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("at most %d schedules", maxEnvironmentSchedules)})
			return
		}
		environmentScheduleMu.Lock()
		defer environmentScheduleMu.Unlock()
		existing, _ := s.loadEnvironmentSchedules()
		prev := map[string]environmentSchedule{}
		for _, e := range existing {
			prev[e.ID] = e
		}
		now := time.Now().UTC().Format(time.RFC3339)
		out := make([]environmentSchedule, 0, len(req.Items))
		for i, e := range req.Items {
			e.Environment = strings.TrimSpace(e.Environment)
			e.Every = strings.TrimSpace(e.Every)
			e.At = strings.TrimSpace(e.At)
			if err := validateEnvironmentSchedule(e, s.environmentExists); err != nil {
				writeJSON(w, http.StatusBadRequest, envelope{Success: false, Error: fmt.Sprintf("schedule %d: %v", i+1, err)})
				return
			}
			old, known := prev[e.ID]
			if !known || e.ID == "" {
				e.ID = newScheduleID()
				e.CreatedAt = now
				e.LastRunAt, e.LastRunID, e.LastResult = "", "", ""
			} else {
				// Run history is the server's, not the caller's.
				e.CreatedAt, e.LastRunAt, e.LastRunID, e.LastResult = old.CreatedAt, old.LastRunAt, old.LastRunID, old.LastResult
				if old.Every != e.Every || old.At != e.At || (!old.Enabled && e.Enabled) {
					// A new timetable counts from now.
					e.CreatedAt, e.LastRunAt = now, ""
				}
			}
			out = append(out, e)
		}
		if err := s.saveEnvironmentSchedules(out); err != nil {
			writeJSON(w, http.StatusInternalServerError, envelope{Success: false, Error: err.Error()})
			return
		}
		s.audit(r, "schedule.environments.update", true, map[string]interface{}{"count": len(out)})
		for i := range out {
			if t := nextEnvironmentRun(out[i], time.Local); !t.IsZero() {
				out[i].NextRunAt = t.UTC().Format(time.RFC3339)
			}
		}
		writeJSON(w, http.StatusOK, envelope{Success: true, Message: "schedules saved", Data: map[string]interface{}{"items": out}})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, envelope{Success: false, Error: "method not allowed"})
	}
}

// startEnvironmentScheduleLoop checks once a minute for environment runs due.
func (s *apiServer) startEnvironmentScheduleLoop(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			s.runDueEnvironmentSchedules(time.Now())
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *apiServer) runDueEnvironmentSchedules(now time.Time) {
	environmentScheduleMu.Lock()
	defer environmentScheduleMu.Unlock()
	list, err := s.loadEnvironmentSchedules()
	if err != nil {
		serverLog.Error("environment schedules could not be read", "error", err.Error())
		return
	}
	if len(list) == 0 {
		return
	}
	changed := false
	for i := range list {
		next := nextEnvironmentRun(list[i], time.Local)
		if next.IsZero() || now.Before(next) {
			continue
		}
		list[i].LastRunAt = now.UTC().Format(time.RFC3339)
		changed = true
		choice, err := s.resolveRunConfig(nil, list[i].Environment, "")
		if err != nil {
			list[i].LastResult = "Not started: " + err.Error()
			list[i].LastRunID = ""
			serverLog.Error("environment schedule due but its environment can't be used", "schedule_id", list[i].ID, "environment", list[i].Environment, "error", err.Error())
			continue
		}
		res := s.submitRun(runStartParams{
			cfgPath:      choice.path,
			environment:  choice.environment,
			source:       "scheduled",
			unrestricted: true,
			auditSubject: "scheduler",
			auditRole:    "system",
		})
		switch {
		case res.noop:
			list[i].LastResult = "Skipped: its clusters were already running"
			list[i].LastRunID = ""
		case res.queued:
			list[i].LastResult = "Queued"
			list[i].LastRunID = res.rec.id
		default:
			list[i].LastResult = "Started"
			if res.rec != nil {
				list[i].LastRunID = res.rec.id
			}
		}
		serverLog.Info("environment schedule fired", "schedule_id", list[i].ID, "environment", list[i].Environment, "every", list[i].Every, "at", list[i].At, "result", list[i].LastResult, "run_id", list[i].LastRunID, "due_at", next.Format(time.RFC3339))
	}
	if changed {
		if err := s.saveEnvironmentSchedules(list); err != nil {
			serverLog.Error("environment schedules could not be saved", "error", err.Error())
		}
	}
}
