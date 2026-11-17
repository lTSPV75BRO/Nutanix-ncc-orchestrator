package main

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newHostScheduleServer(t *testing.T) *apiServer {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "config.yaml"), []byte("runner: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &apiServer{repoRoot: root, configPath: "config.yaml", scheduleStatePath: ".ncc-api-schedule.json"}
}

// Two admins open the host schedule; the second save must not silently undo
// the first, the same rule a Kubernetes CronJob enforces.
func TestHostScheduleSaveIsConditional(t *testing.T) {
	s := newHostScheduleServer(t)

	opened := getSchedule(t, s).ResourceVersion
	if opened != hostScheduleNone {
		t.Fatalf("unsaved schedule version = %q, want %q", opened, hostScheduleNone)
	}
	// Admin A saves from the version both opened.
	code, env := putSchedule(t, s, `{"type":"auto","action":"create","every":"1h","config":"config.yaml","print_only":false,"resource_version":"`+opened+`"}`)
	if code != http.StatusOK {
		t.Fatalf("first save: %d %+v", code, env)
	}
	fresh := getSchedule(t, s).ResourceVersion
	if fresh == "" || fresh == opened {
		t.Fatalf("version after save = %q", fresh)
	}
	// The saved file holds no version of its own.
	if b, _ := os.ReadFile(filepath.Join(s.repoRoot, ".ncc-api-schedule.json")); len(b) == 0 || strings.Contains(string(b), "resource_version") {
		t.Fatalf("schedule file: %s", b)
	}
	// Admin B, still on the old version, is refused and nothing changes.
	code, env = putSchedule(t, s, `{"type":"auto","action":"create","every":"4h","config":"config.yaml","print_only":false,"resource_version":"`+opened+`"}`)
	if code != http.StatusConflict || env.ErrorCode != "NCC_API_SCHEDULE_CONFLICT" || !strings.Contains(env.Error, "someone else") {
		t.Fatalf("stale save: %d %+v", code, env)
	}
	if st := getSchedule(t, s); st.Every != "1h" || st.ResourceVersion != fresh {
		t.Fatalf("stale save changed the schedule: %+v", st)
	}
	// After reloading, B's save goes through.
	if code, env := putSchedule(t, s, `{"type":"auto","action":"create","every":"4h","config":"config.yaml","print_only":false,"resource_version":"`+fresh+`"}`); code != http.StatusOK {
		t.Fatalf("save after reload: %d %+v", code, env)
	}
}

// Older clients send no version: their saves still work (last write wins).
func TestHostScheduleSaveWithoutVersion(t *testing.T) {
	s := newHostScheduleServer(t)
	for _, every := range []string{"1h", "4h"} {
		if code, env := putSchedule(t, s, `{"type":"auto","action":"create","every":"`+every+`","config":"config.yaml","print_only":false}`); code != http.StatusOK {
			t.Fatalf("save %s: %d %+v", every, code, env)
		}
	}
	if st := getSchedule(t, s); st.Every != "4h" {
		t.Fatalf("schedule = %+v", st)
	}
}
