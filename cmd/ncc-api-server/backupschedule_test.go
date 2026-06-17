package main

import (
	"testing"
	"time"
)

func TestParseEveryDuration(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"15m", 15 * time.Minute, false},
		{"4h", 4 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"", 0, true},
		{"0h", 0, true},
		{"5s", 0, true},
		{"abc", 0, true},
		{"-3h", 0, true},
	}
	for _, c := range cases {
		got, err := parseEveryDuration(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseEveryDuration(%q): expected error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseEveryDuration(%q): unexpected error %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseEveryDuration(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestScheduledBackupDue(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if scheduledBackupDue(backupScheduleState{Enabled: false, Every: "1h"}, now) {
		t.Fatal("disabled schedule should never be due")
	}
	if !scheduledBackupDue(backupScheduleState{Enabled: true, Every: "1h"}, now) {
		t.Fatal("never-run schedule should be due")
	}
	recent := now.Add(-30 * time.Minute).Format(time.RFC3339)
	if scheduledBackupDue(backupScheduleState{Enabled: true, Every: "1h", LastRunAt: recent}, now) {
		t.Fatal("schedule run 30m ago with 1h interval should not be due")
	}
	old := now.Add(-2 * time.Hour).Format(time.RFC3339)
	if !scheduledBackupDue(backupScheduleState{Enabled: true, Every: "1h", LastRunAt: old}, now) {
		t.Fatal("schedule run 2h ago with 1h interval should be due")
	}
}

func TestValidateBackupScheduleInput(t *testing.T) {
	if err := validateBackupScheduleInput(backupScheduleState{Enabled: false}); err != nil {
		t.Fatalf("disabled schedule should validate, got %v", err)
	}
	if err := validateBackupScheduleInput(backupScheduleState{Enabled: true, Every: "1m"}); err == nil {
		t.Fatal("interval below minimum should be rejected")
	}
	if err := validateBackupScheduleInput(backupScheduleState{Enabled: true, Every: "24h", Retain: -1}); err == nil {
		t.Fatal("negative retain should be rejected")
	}
	if err := validateBackupScheduleInput(backupScheduleState{Enabled: true, Every: "24h", Retain: 7}); err != nil {
		t.Fatalf("valid schedule should pass, got %v", err)
	}
}

func TestScheduledBackupFileFromOutput(t *testing.T) {
	out := "Asset layout: install-dir\nBackup written: /root/ncc-orchestrator/backups/ncc-backup-20260601T120000Z.tar.gz\nverified: 5 file(s)\n"
	if got := scheduledBackupFileFromOutput(out); got != "/root/ncc-orchestrator/backups/ncc-backup-20260601T120000Z.tar.gz" {
		t.Fatalf("unexpected file: %q", got)
	}
	if got := scheduledBackupFileFromOutput("no marker here"); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}
