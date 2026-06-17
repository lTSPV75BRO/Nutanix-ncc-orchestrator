package main

import (
	"testing"
	"time"
)

func TestInQuietHours(t *testing.T) {
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	at := func(h, m int) time.Time { return base.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute) }

	// Same-day window 09:00-17:00.
	day := quietHoursConfig{Enabled: true, Start: "09:00", End: "17:00", Timezone: "UTC"}
	if !inQuietHours(day, at(10, 0)) {
		t.Error("10:00 should be inside 09:00-17:00")
	}
	if inQuietHours(day, at(8, 0)) {
		t.Error("08:00 should be outside 09:00-17:00")
	}
	if inQuietHours(day, at(17, 0)) {
		t.Error("17:00 is the exclusive end, should be outside")
	}

	// Overnight window 22:00-07:00 (wraps midnight).
	night := quietHoursConfig{Enabled: true, Start: "22:00", End: "07:00", Timezone: "UTC"}
	if !inQuietHours(night, at(23, 30)) {
		t.Error("23:30 should be inside overnight window")
	}
	if !inQuietHours(night, at(2, 0)) {
		t.Error("02:00 should be inside overnight window")
	}
	if inQuietHours(night, at(12, 0)) {
		t.Error("12:00 should be outside overnight window")
	}

	// Disabled or malformed never matches.
	if inQuietHours(quietHoursConfig{Enabled: false, Start: "09:00", End: "17:00"}, at(10, 0)) {
		t.Error("disabled quiet hours must not match")
	}
	if inQuietHours(quietHoursConfig{Enabled: true, Start: "bad", End: "17:00"}, at(10, 0)) {
		t.Error("malformed start must not match")
	}
}

func TestInMaintenance(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	windows := []maintenanceWindow{
		{Start: "2026-06-01T11:00:00Z", End: "2026-06-01T13:00:00Z", Note: "patching"},
	}
	in, note := inMaintenance(windows, now)
	if !in || note != "patching" {
		t.Fatalf("expected in maintenance with note, got in=%v note=%q", in, note)
	}
	if in, _ := inMaintenance(windows, now.Add(3*time.Hour)); in {
		t.Fatal("should be outside the window 3h later")
	}
	// Invalid window is ignored.
	if in, _ := inMaintenance([]maintenanceWindow{{Start: "x", End: "y"}}, now); in {
		t.Fatal("invalid window must not match")
	}
}

func TestNotificationSuppressed(t *testing.T) {
	s := &apiServer{}
	now := time.Date(2026, 6, 1, 23, 0, 0, 0, time.UTC)

	// Maintenance suppresses everything, including failures.
	st := &notificationState{
		Maintenance: []maintenanceWindow{{Start: "2026-06-01T22:00:00Z", End: "2026-06-02T02:00:00Z"}},
	}
	if sup, _ := s.notificationSuppressed(st, "run_failure", now); !sup {
		t.Fatal("maintenance window should suppress even failures")
	}

	// Quiet hours suppress non-failures; failures pass when AllowFailures.
	s2 := &apiServer{}
	st2 := &notificationState{Quiet: quietHoursConfig{Enabled: true, Start: "22:00", End: "07:00", Timezone: "UTC", AllowFailures: true}}
	if sup, _ := s2.notificationSuppressed(st2, "run_success", now); !sup {
		t.Fatal("quiet hours should suppress run_success")
	}
	if sup, _ := s2.notificationSuppressed(st2, "run_failure", now); sup {
		t.Fatal("quiet hours with AllowFailures should pass run_failure")
	}

	// Dedup: second identical event within the window is suppressed.
	s3 := &apiServer{}
	st3 := &notificationState{Throttle: notificationThrottle{DedupWindowSec: 300}}
	day := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if sup, _ := s3.notificationSuppressed(st3, "backup_failure", day); sup {
		t.Fatal("first event should pass")
	}
	if sup, _ := s3.notificationSuppressed(st3, "backup_failure", day.Add(time.Minute)); !sup {
		t.Fatal("second identical event within dedup window should be suppressed")
	}
	if sup, _ := s3.notificationSuppressed(st3, "backup_failure", day.Add(10*time.Minute)); sup {
		t.Fatal("event after dedup window should pass")
	}
}

func TestDigestDue(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if digestDue(digestConfig{Enabled: false, Every: "24h"}, now) {
		t.Fatal("disabled digest never due")
	}
	if !digestDue(digestConfig{Enabled: true, Every: "24h"}, now) {
		t.Fatal("never-sent digest should be due")
	}
	recent := now.Add(-2 * time.Hour).Format(time.RFC3339)
	if digestDue(digestConfig{Enabled: true, Every: "24h", LastSentAt: recent}, now) {
		t.Fatal("digest sent 2h ago with 24h interval should not be due")
	}
	old := now.Add(-25 * time.Hour).Format(time.RFC3339)
	if !digestDue(digestConfig{Enabled: true, Every: "24h", LastSentAt: old}, now) {
		t.Fatal("digest sent 25h ago with 24h interval should be due")
	}
}

func TestValidateNotificationControls(t *testing.T) {
	if err := validateNotificationControls(notificationState{Quiet: quietHoursConfig{Enabled: true, Start: "9:00", End: "17:00"}}); err != nil {
		t.Fatalf("9:00 should be valid HH:MM, got %v", err)
	}
	if err := validateNotificationControls(notificationState{Quiet: quietHoursConfig{Enabled: true, Start: "25:00", End: "17:00"}}); err == nil {
		t.Fatal("25:00 should be rejected")
	}
	if err := validateNotificationControls(notificationState{Quiet: quietHoursConfig{Enabled: true, Start: "09:00", End: "17:00", Timezone: "Not/AZone"}}); err == nil {
		t.Fatal("invalid timezone should be rejected")
	}
	if err := validateNotificationControls(notificationState{Maintenance: []maintenanceWindow{{Start: "2026-06-01T13:00:00Z", End: "2026-06-01T12:00:00Z"}}}); err == nil {
		t.Fatal("end before start should be rejected")
	}
	if err := validateNotificationControls(notificationState{Digest: digestConfig{Enabled: true, Every: "nope"}}); err == nil {
		t.Fatal("bad digest interval should be rejected")
	}
}
