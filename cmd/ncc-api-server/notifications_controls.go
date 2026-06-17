package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

// This file adds delivery controls on top of the notification channels: quiet
// hours (a recurring daily mute), maintenance windows (absolute mute ranges),
// and throttling (per-event dedup + a global minimum interval). They are
// enforced for *event* notifications (run/operational alerts) through emit();
// manual test sends and the scheduled digest deliberately bypass them.

// failureEvents are the urgent classes that quiet-hours can be configured to
// let through (AllowFailures) and that maintenance windows still suppress.
func isFailureEvent(event string) bool {
	switch event {
	case "run_failure", "backup_failure", "selfheal_failure", "policy_violations":
		return true
	}
	return false
}

// parseHHMM returns minutes-since-midnight for "HH:MM" (24h), ok=false if bad.
func parseHHMM(s string) (int, bool) {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, false
	}
	h, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	m, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

// quietLocation resolves the configured timezone, defaulting to UTC.
func quietLocation(tz string) *time.Location {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	return time.UTC
}

// inQuietHours reports whether now (rendered in the configured tz) falls within
// the [Start, End) daily window. A window where End <= Start wraps past
// midnight (e.g. 22:00–07:00).
func inQuietHours(q quietHoursConfig, now time.Time) bool {
	if !q.Enabled {
		return false
	}
	start, ok1 := parseHHMM(q.Start)
	end, ok2 := parseHHMM(q.End)
	if !ok1 || !ok2 || start == end {
		return false
	}
	local := now.In(quietLocation(q.Timezone))
	cur := local.Hour()*60 + local.Minute()
	if start < end {
		return cur >= start && cur < end
	}
	// Wraps midnight: in window if after start OR before end.
	return cur >= start || cur < end
}

// inMaintenance reports whether now is inside any configured maintenance window.
func inMaintenance(windows []maintenanceWindow, now time.Time) (bool, string) {
	for _, w := range windows {
		start, err1 := time.Parse(time.RFC3339, strings.TrimSpace(w.Start))
		end, err2 := time.Parse(time.RFC3339, strings.TrimSpace(w.End))
		if err1 != nil || err2 != nil || !end.After(start) {
			continue
		}
		if !now.Before(start) && now.Before(end) {
			note := strings.TrimSpace(w.Note)
			if note == "" {
				note = "maintenance window"
			}
			return true, note
		}
	}
	return false, ""
}

// notificationSuppressed decides whether an event notification should be
// dropped right now and, when allowed, records the send time for throttling.
// It must be called while holding no external lock; it serializes its own
// throttle bookkeeping.
func (s *apiServer) notificationSuppressed(st *notificationState, event string, now time.Time) (bool, string) {
	// 1) Maintenance windows mute everything.
	if inMaint, note := inMaintenance(st.Maintenance, now); inMaint {
		return true, "maintenance: " + note
	}
	// 2) Quiet hours mute non-failures (and failures unless AllowFailures).
	if inQuietHours(st.Quiet, now) {
		if !(isFailureEvent(event) && st.Quiet.AllowFailures) {
			return true, "quiet hours"
		}
	}
	// 3) Throttle: per-event dedup + global minimum interval.
	s.notifThrottleMu.Lock()
	defer s.notifThrottleMu.Unlock()
	if s.notifLastSent == nil {
		s.notifLastSent = map[string]time.Time{}
	}
	if w := st.Throttle.DedupWindowSec; w > 0 {
		if last, ok := s.notifLastSent[event]; ok && now.Sub(last) < time.Duration(w)*time.Second {
			return true, fmt.Sprintf("deduped (within %ds)", w)
		}
	}
	if mi := st.Throttle.MinIntervalSec; mi > 0 && !s.notifLastAny.IsZero() {
		if now.Sub(s.notifLastAny) < time.Duration(mi)*time.Second {
			return true, fmt.Sprintf("rate-limited (min interval %ds)", mi)
		}
	}
	// Allowed: record send times for future throttle decisions.
	s.notifLastSent[event] = now
	s.notifLastAny = now
	return false, ""
}

// emit gates an event notification through the delivery controls, then
// dispatches it to the configured channels. Callers have already confirmed the
// per-event toggle (Events.*) and st.Enabled.
func (s *apiServer) emit(st *notificationState, event, title string, details map[string]interface{}) {
	if sup, reason := s.notificationSuppressed(st, event, time.Now()); sup {
		log.Printf("notifications: suppressed %s (%s)", event, reason)
		return
	}
	_ = s.dispatchNotifications(st, event, title, details, nil)
}

// validateNotificationControls checks the quiet-hours / maintenance / throttle
// fields so a bad config is rejected at save time rather than silently ignored.
func validateNotificationControls(st notificationState) error {
	if st.Quiet.Enabled {
		if _, ok := parseHHMM(st.Quiet.Start); !ok {
			return fmt.Errorf("quiet hours start must be HH:MM (got %q)", st.Quiet.Start)
		}
		if _, ok := parseHHMM(st.Quiet.End); !ok {
			return fmt.Errorf("quiet hours end must be HH:MM (got %q)", st.Quiet.End)
		}
		if tz := strings.TrimSpace(st.Quiet.Timezone); tz != "" {
			if _, err := time.LoadLocation(tz); err != nil {
				return fmt.Errorf("quiet hours timezone %q is not a valid IANA name", tz)
			}
		}
	}
	for i, w := range st.Maintenance {
		start, err1 := time.Parse(time.RFC3339, strings.TrimSpace(w.Start))
		end, err2 := time.Parse(time.RFC3339, strings.TrimSpace(w.End))
		if err1 != nil || err2 != nil {
			return fmt.Errorf("maintenance window %d: start/end must be RFC3339 timestamps", i+1)
		}
		if !end.After(start) {
			return fmt.Errorf("maintenance window %d: end must be after start", i+1)
		}
	}
	if st.Throttle.DedupWindowSec < 0 || st.Throttle.MinIntervalSec < 0 {
		return fmt.Errorf("throttle values must be >= 0")
	}
	if st.Digest.Enabled {
		if _, err := parseEveryDuration(st.Digest.Every); err != nil {
			return fmt.Errorf("digest interval: %w", err)
		}
	}
	return nil
}
