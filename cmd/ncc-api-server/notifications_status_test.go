package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goncc/internal/notify"
)

func getNotificationStatus(t *testing.T, s *apiServer) notificationStatusResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleNotificationsStatus(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/notifications/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	}
	var env struct {
		Data notificationStatusResponse `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	return env.Data
}

func statusRow(t *testing.T, resp notificationStatusResponse, ch string) notificationChannelStatus {
	t.Helper()
	for _, r := range resp.Channels {
		if r.Channel == ch {
			return r
		}
	}
	t.Fatalf("no %s row in %+v", ch, resp.Channels)
	return notificationChannelStatus{}
}

// A test send updates the shared status file; the endpoint reports it per
// channel with the error redacted and the dead-letter count.
func TestNotificationStatusAfterTestSend(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	s := newNotifServer(t, "clusters: 10.0.0.1\nnotification-deadletter-dir: deadletter\n")
	s.outputDir = "outputfiles"
	dl := filepath.Join(s.repoRoot, "deadletter")
	if err := os.MkdirAll(dl, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"slack-20261108T100000Z-1.json", "slack-20261108T100001Z-2.json"} {
		if err := os.WriteFile(filepath.Join(dl, n), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	before := getNotificationStatus(t, s)
	if r := statusRow(t, before, "webhook"); r.State != "never" || r.Enabled {
		t.Fatalf("fresh webhook row = %+v", r)
	}
	if !before.DeadLetterConfigured {
		t.Fatal("dead-letter dir not detected")
	}

	st, _ := s.loadNotifications()
	st.Webhook = webhookNotificationConfig{Enabled: true, URL: ok.URL}
	st.Slack = slackNotificationConfig{Enabled: true, WebhookURL: bad.URL + "/services/T0/B0/SECRETTOKEN"}
	if err := s.saveNotifications(st); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/notifications/test", strings.NewReader(`{"channel":"all"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleNotificationsTest(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected partial failure, got %d %s", rec.Code, rec.Body.String())
	}

	resp := getNotificationStatus(t, s)
	wh := statusRow(t, resp, "webhook")
	if wh.State != "ok" || !wh.Enabled || wh.LastSuccessAt == "" || wh.Attempts != 1 || wh.Source != "api" || wh.Event != "manual_test" {
		t.Fatalf("webhook row = %+v", wh)
	}
	sl := statusRow(t, resp, "slack")
	if sl.State != "failing" || sl.LastError == "" || sl.LastSuccessAt != "" {
		t.Fatalf("slack row = %+v", sl)
	}
	if strings.Contains(sl.LastError, "SECRETTOKEN") {
		t.Fatalf("slack error leaked the webhook path: %q", sl.LastError)
	}
	if sl.DeadLetters != 2 {
		t.Fatalf("slack dead letters = %d, want 2", sl.DeadLetters)
	}
	if em := statusRow(t, resp, "email"); em.Enabled || em.State != "never" {
		t.Fatalf("email row = %+v", em)
	}
}

// The runner writes the same file after a run; the endpoint shows it, and
// re-redacts errors written by older builds.
func TestNotificationStatusShowsRunnerDelivery(t *testing.T) {
	s := newNotifServer(t, "clusters: 10.0.0.1\nemail-enabled: true\nsmtp-server: smtp.example.com\nemail-from: a@example.com\nemail-to: b@example.com\n")
	s.outputDir = "outputfiles"
	dir := s.notificationStatusDir()
	if err := notify.RecordDelivery(dir, notify.DeliveryOutcome{Channel: "email", Source: "runner", Event: "run_report", Attempts: 3, Err: os.ErrDeadlineExceeded}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Simulate an unredacted error from an older writer.
	raw, _ := json.Marshal(notify.StatusFile{Channels: map[string]notify.ChannelStatus{
		"email":   {LastAttemptAt: time.Now().UTC().Format(time.RFC3339), Attempts: 3, Source: "runner", LastError: "timeout"},
		"webhook": {LastAttemptAt: "2026-11-08T10:00:00Z", LastError: `Post "https://hooks.example.com/abc/SECRET": refused`},
	}})
	if err := os.WriteFile(filepath.Join(dir, notify.StatusFileName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	resp := getNotificationStatus(t, s)
	em := statusRow(t, resp, "email")
	if !em.Enabled || em.State != "failing" || em.Attempts != 3 || em.Source != "runner" {
		t.Fatalf("email row = %+v", em)
	}
	if wh := statusRow(t, resp, "webhook"); strings.Contains(wh.LastError, "SECRET") {
		t.Fatalf("not re-redacted: %q", wh.LastError)
	}
}

func TestNotificationStatusRouteRoleAndMethod(t *testing.T) {
	if got := routeMinRoleFor("/api/v1/settings/notifications/status", true); got != RoleOperator {
		t.Fatalf("GET role = %v, want operator", got)
	}
	if got := routeMinRoleFor("/api/v1/settings/notifications/status", false); got == RoleOperator || got == RoleViewer {
		t.Fatalf("write role = %v, want admin", got)
	}
	s := newNotifServer(t, "clusters: 10.0.0.1\n")
	rec := httptest.NewRecorder()
	s.handleNotificationsStatus(rec, httptest.NewRequest(http.MethodPost, "/api/v1/settings/notifications/status", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d", rec.Code)
	}
}

// Bare servers (no repo root / output dir) must not write status into the cwd.
func TestNotificationStatusBareServerNoop(t *testing.T) {
	s := &apiServer{}
	if s.notificationStatusDir() != "" {
		t.Fatal("bare server has a status dir")
	}
	s.recordSharedDelivery("email", "manual_test", nil)
	if _, err := os.Stat(notify.StatusFileName); err == nil {
		t.Fatal("status file written into the cwd")
	}
}
