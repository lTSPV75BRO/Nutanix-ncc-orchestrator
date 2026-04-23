package main

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNormalizeScheduleType(t *testing.T) {
	got, err := normalizeScheduleType("cron")
	if err != nil {
		t.Fatalf("normalizeScheduleType cron: %v", err)
	}
	if got != "cron" {
		t.Fatalf("got %q, want cron", got)
	}

	got, err = normalizeScheduleType("windows-task")
	if err != nil {
		t.Fatalf("normalizeScheduleType windows-task: %v", err)
	}
	if got != "windows" {
		t.Fatalf("got %q, want windows", got)
	}

	got, err = normalizeScheduleType("auto")
	if err != nil {
		t.Fatalf("normalizeScheduleType auto: %v", err)
	}
	if runtime.GOOS == "windows" && got != "windows" {
		t.Fatalf("got %q, want windows on Windows", got)
	}
	if runtime.GOOS != "windows" && got != "cron" {
		t.Fatalf("got %q, want cron on non-Windows", got)
	}

	if _, err := normalizeScheduleType("bogus"); err == nil {
		t.Fatal("expected error for invalid schedule type")
	}
}

func TestCronExprFromInterval(t *testing.T) {
	tests := []struct {
		every time.Duration
		want  string
	}{
		{15 * time.Minute, "*/15 * * * *"},
		{4 * time.Hour, "0 */4 * * *"},
		{24 * time.Hour, "0 0 */1 * *"},
	}
	for _, tt := range tests {
		got, err := cronExprFromInterval(tt.every)
		if err != nil {
			t.Fatalf("cronExprFromInterval(%s): %v", tt.every, err)
		}
		if got != tt.want {
			t.Fatalf("cronExprFromInterval(%s): got %q want %q", tt.every, got, tt.want)
		}
	}

	if _, err := cronExprFromInterval(90 * time.Second); err == nil {
		t.Fatal("expected error for non-minute interval")
	}
	if _, err := cronExprFromInterval(90 * time.Minute); err == nil {
		t.Fatal("expected error for unsupported 90-minute cadence")
	}
}

func TestUpsertScheduleLine(t *testing.T) {
	content := strings.Join([]string{
		"MAILTO=\"\"",
		"0 */2 * * * /usr/bin/old # ncc-orchestrator:nightly",
		"",
	}, "\n")
	newLine := "0 */4 * * * /usr/bin/new # ncc-orchestrator:nightly"

	got := upsertScheduleLine(content, "ncc-orchestrator:nightly", newLine)
	if strings.Count(got, "ncc-orchestrator:nightly") != 1 {
		t.Fatalf("expected exactly one marker line, got:\n%s", got)
	}
	if !strings.Contains(got, newLine) {
		t.Fatalf("missing new schedule line, got:\n%s", got)
	}
	if !strings.Contains(got, `MAILTO=""`) {
		t.Fatalf("lost existing unrelated line, got:\n%s", got)
	}
}

func TestRemoveScheduleLine(t *testing.T) {
	content := strings.Join([]string{
		"MAILTO=\"\"",
		"0 */2 * * * /usr/bin/old # ncc-orchestrator:nightly",
		"10 * * * * /usr/bin/other # other",
	}, "\n")
	got, removed := removeScheduleLine(content, "ncc-orchestrator:nightly")
	if !removed {
		t.Fatal("expected removeScheduleLine to remove target marker")
	}
	if strings.Contains(got, "ncc-orchestrator:nightly") {
		t.Fatalf("marker still present after removal:\n%s", got)
	}
	if !strings.Contains(got, "other # other") {
		t.Fatalf("expected unrelated entry to remain:\n%s", got)
	}

	got, removed = removeScheduleLine(content, "does-not-exist")
	if removed {
		t.Fatal("expected removed=false for missing marker")
	}
}

func TestValidateScheduleTaskName(t *testing.T) {
	if err := validateScheduleTaskName("ncc-orchestrator:nightly_01"); err != nil {
		t.Fatalf("expected valid task name, got error: %v", err)
	}
	if err := validateScheduleTaskName("bad task name with spaces"); err == nil {
		t.Fatal("expected invalid task name with spaces")
	}
	if err := validateScheduleTaskName(""); err == nil {
		t.Fatal("expected invalid empty task name")
	}
}

func TestSanitizeScheduleCommand(t *testing.T) {
	if _, err := sanitizeScheduleCommand(`"/usr/local/bin/ncc-orchestrator" --config "/tmp/c.yaml"`); err != nil {
		t.Fatalf("expected safe command, got error: %v", err)
	}
	if _, err := sanitizeScheduleCommand("ncc-orchestrator --config c.yaml; rm -rf /"); err == nil {
		t.Fatal("expected unsafe command to be rejected")
	}
}

func TestParseCommandLineStrict(t *testing.T) {
	name, args, err := parseCommandLineStrict(`"/usr/local/bin/ncc-orchestrator" --config "/tmp/config path.yaml"`)
	if err != nil {
		t.Fatalf("parseCommandLineStrict returned error: %v", err)
	}
	if name != "/usr/local/bin/ncc-orchestrator" {
		t.Fatalf("unexpected executable: %q", name)
	}
	if len(args) != 2 || args[0] != "--config" || args[1] != "/tmp/config path.yaml" {
		t.Fatalf("unexpected args: %#v", args)
	}
	if _, _, err := parseCommandLineStrict(`bad "unterminated`); err == nil {
		t.Fatal("expected unterminated quote error")
	}
}
