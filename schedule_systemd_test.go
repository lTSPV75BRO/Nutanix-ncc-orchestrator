package main

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeScheduleTypeSystemd(t *testing.T) {
	for _, in := range []string{"systemd", "systemd-timer", "timer", "SystemD"} {
		got, err := normalizeScheduleType(in)
		if err != nil {
			t.Fatalf("normalizeScheduleType(%q) error: %v", in, err)
		}
		if got != "systemd" {
			t.Fatalf("normalizeScheduleType(%q) = %q, want systemd", in, got)
		}
	}
}

func TestCronToOnCalendar(t *testing.T) {
	cases := []struct {
		spec    string
		want    string
		wantErr bool
	}{
		{"*/15 * * * *", "*-*-* *:0/15:00", false},
		{"0 */4 * * *", "*-*-* 0/4:0:00", false},
		{"0 0 */2 * *", "*-*-1/2 0:0:00", false},
		{"15 3 * * *", "*-*-* 3:15:00", false},
		// day-of-week is enumerated to systemd weekday names (cron Sun=0..Sat=6).
		{"0 0 * * 1", "Mon *-*-* 0:0:00", false},
		{"0 0 * * 0", "Sun *-*-* 0:0:00", false},
		{"0 0 * * 7", "Sun *-*-* 0:0:00", false}, // cron 7 == Sunday
		{"0 0 * * 1-5", "Mon,Tue,Wed,Thu,Fri *-*-* 0:0:00", false},
		{"30 2 * * 0,6", "Sat,Sun *-*-* 2:30:00", false},
		{"0 0 * * mon,wed,fri", "Mon,Wed,Fri *-*-* 0:0:00", false},
		{"0 0 * * 5-0", "Fri,Sat,Sun *-*-* 0:0:00", false}, // wrap-around range
		{"0 0 * * 1-7", "*-*-* 0:0:00", false},             // every day -> no weekday prefix
		// ambiguous: cron ORs day-of-month and day-of-week; systemd ANDs them
		{"0 0 1 * 1", "", true},
		{"0 0 * * 9", "", true}, // out-of-range weekday
		{"bad spec", "", true},
		{"* * * *", "", true},
	}
	for _, c := range cases {
		got, err := cronToOnCalendar(c.spec)
		if c.wantErr {
			if err == nil {
				t.Fatalf("cronToOnCalendar(%q) expected error, got %q", c.spec, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("cronToOnCalendar(%q) error: %v", c.spec, err)
		}
		if got != c.want {
			t.Fatalf("cronToOnCalendar(%q) = %q, want %q", c.spec, got, c.want)
		}
	}
}

func TestOnCalendarFromSchedule(t *testing.T) {
	cases := []struct {
		cron  string
		every time.Duration
		want  string
	}{
		{"", 15 * time.Minute, "*-*-* *:0/15:00"},
		{"", 4 * time.Hour, "*-*-* 0/4:0:00"},
		{"", 24 * time.Hour, "*-*-1/1 0:0:00"},
		{"*/30 * * * *", time.Hour, "*-*-* *:0/30:00"}, // explicit cron wins over every
	}
	for _, c := range cases {
		got, err := onCalendarFromSchedule(c.cron, c.every)
		if err != nil {
			t.Fatalf("onCalendarFromSchedule(%q,%s) error: %v", c.cron, c.every, err)
		}
		if got != c.want {
			t.Fatalf("onCalendarFromSchedule(%q,%s) = %q, want %q", c.cron, c.every, got, c.want)
		}
	}
}

func TestSanitizeSystemdName(t *testing.T) {
	cases := map[string]string{
		"ncc-orchestrator": "ncc-orchestrator",
		"team:prod":        "team-prod",
		"a/b c":            "a-b-c",
		"":                 "default",
	}
	for in, want := range cases {
		if got := sanitizeSystemdName(in); got != want {
			t.Fatalf("sanitizeSystemdName(%q) = %q, want %q", in, got, want)
		}
	}
	if base := systemdUnitBase("prod"); base != "ncc-sched-prod" {
		t.Fatalf("systemdUnitBase(prod) = %q", base)
	}
}

func TestBuildSystemdScheduleUnits(t *testing.T) {
	marker := scheduleMarker("prod")
	svc, timer := buildSystemdScheduleUnits("prod", "*-*-* 0/4:00:00", "/opt/ncc/logs/ncc-sched-prod.sh", "/opt/ncc", marker)

	for _, want := range []string{"Type=oneshot", "WorkingDirectory=/opt/ncc", "ExecStart=/bin/sh /opt/ncc/logs/ncc-sched-prod.sh", "# " + marker} {
		if !strings.Contains(svc, want) {
			t.Fatalf("service unit missing %q:\n%s", want, svc)
		}
	}
	for _, want := range []string{"OnCalendar=*-*-* 0/4:00:00", "Persistent=true", "Unit=ncc-sched-prod.service", "WantedBy=timers.target", "# " + marker} {
		if !strings.Contains(timer, want) {
			t.Fatalf("timer unit missing %q:\n%s", want, timer)
		}
	}
}

func TestSystemdSupervisorUnit(t *testing.T) {
	unit := systemdSupervisorUnit(installServiceOptions{
		InstallDir:      "/root/ncc-orchestrator",
		ServiceName:     "ncc-orchestrator",
		OrchestratorBin: "/root/ncc-orchestrator/bin/ncc-orchestrator",
	})
	for _, want := range []string{
		"Type=simple",
		"Restart=always",
		"WorkingDirectory=/root/ncc-orchestrator",
		"ExecStart=/root/ncc-orchestrator/bin/ncc-orchestrator v2-supervise --install-dir /root/ncc-orchestrator",
		"WantedBy=multi-user.target",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("supervisor unit missing %q:\n%s", want, unit)
		}
	}
}
