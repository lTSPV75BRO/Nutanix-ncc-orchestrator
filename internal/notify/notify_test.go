package notify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"goncc/internal/model"
)

// TestSignWebhookBody verifies the HMAC-SHA256 signature header is deterministic
// and matches an independent computation.
func TestSignWebhookBody(t *testing.T) {
	body := []byte(`{"cluster":"c1","fail":2}`)
	got := signWebhookBody("topsecret", body)

	mac := hmac.New(sha256.New, []byte("topsecret"))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got != want {
		t.Fatalf("signature mismatch: got %q want %q", got, want)
	}
	if got != signWebhookBody("topsecret", body) {
		t.Fatalf("signature not deterministic")
	}
}

// TestWriteDeadLetter confirms a failed notification is persisted to disk with
// the channel and error captured.
func TestWriteDeadLetter(t *testing.T) {
	dir := t.TempDir()
	writeDeadLetter(dir, "webhook", "cluster-x", "", errors.New("boom"), []byte(`{"k":"v"}`))

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 dead-letter file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	for _, want := range []string{`"channel": "webhook"`, `"cluster": "cluster-x"`, "boom", `{\"k\":\"v\"}`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("dead-letter missing %q in:\n%s", want, data)
		}
	}

	// Empty dir is a no-op (must not panic or create files).
	writeDeadLetter("", "email", "", "subj", errors.New("x"), []byte("y"))
}

// TestMetrics covers the accumulator: attempts always increment, failures only
// on error, snapshot copies under lock, and reset clears.
func TestMetrics(t *testing.T) {
	m := NewMetrics()
	m.Record("email", nil)
	m.Record("email", errors.New("smtp down"))
	m.Record("webhook", errors.New("503"))
	m.Record("webhook", errors.New("503"))
	m.Record("slack", nil)

	attempts, failures := m.Snapshot()
	if attempts["email"] != 2 || failures["email"] != 1 {
		t.Errorf("email: got attempts=%d failures=%d, want 2/1", attempts["email"], failures["email"])
	}
	if attempts["webhook"] != 2 || failures["webhook"] != 2 {
		t.Errorf("webhook: got attempts=%d failures=%d, want 2/2", attempts["webhook"], failures["webhook"])
	}
	if attempts["slack"] != 1 || failures["slack"] != 0 {
		t.Errorf("slack: got attempts=%d failures=%d, want 1/0", attempts["slack"], failures["slack"])
	}

	m.Reset()
	attempts, failures = m.Snapshot()
	if len(attempts) != 0 || len(failures) != 0 {
		t.Errorf("reset should clear counts, got attempts=%v failures=%v", attempts, failures)
	}
	// nil receiver must be safe (no panic).
	var nilM *Metrics
	nilM.Record("email", nil)
	nilM.Reset()
}

// TestRenderTemplate covers operator-supplied template rendering: successful
// field substitution and a hard failure on an unknown field.
func TestRenderTemplate(t *testing.T) {
	data := model.NotificationSummary{Cluster: "10.0.0.1", FailCount: 3, WarnCount: 1, TotalChecks: 42, Overview: "ov"}

	got, err := RenderTemplate("email-subject", "NCC {{.Cluster}}: {{.FailCount}} FAIL / {{.WarnCount}} WARN", data)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got != "NCC 10.0.0.1: 3 FAIL / 1 WARN" {
		t.Fatalf("unexpected render: %q", got)
	}

	if _, err := RenderTemplate("email-subject", "{{.NoSuchField}}", data); err == nil {
		t.Fatalf("expected error for unknown template field, got nil")
	}
}

// TestApplyEmailTemplates checks override-when-set and fall-back-to-default
// (on empty template and on a broken template).
func TestApplyEmailTemplates(t *testing.T) {
	data := model.NotificationSummary{Cluster: "c1", FailCount: 2}
	logger := zerolog.Nop()

	s, b := ApplyEmailTemplates(model.Config{}, "defSubj", "defBody", data, logger)
	if s != "defSubj" || b != "defBody" {
		t.Fatalf("expected defaults, got subj=%q body=%q", s, b)
	}

	cfg := model.Config{EmailSubjectTemplate: "S {{.Cluster}}", EmailBodyTemplate: "B {{.FailCount}}"}
	s, b = ApplyEmailTemplates(cfg, "defSubj", "defBody", data, logger)
	if s != "S c1" || b != "B 2" {
		t.Fatalf("expected overrides, got subj=%q body=%q", s, b)
	}

	cfgBad := model.Config{EmailSubjectTemplate: "{{.Nope}}", EmailBodyTemplate: "B {{.FailCount}}"}
	s, b = ApplyEmailTemplates(cfgBad, "defSubj", "defBody", data, logger)
	if s != "defSubj" {
		t.Fatalf("broken subject should fall back to default, got %q", s)
	}
	if b != "B 2" {
		t.Fatalf("body should still render, got %q", b)
	}
}

// TestSendWebhook_TemplateBody verifies a configured webhook template controls
// the request body, and a broken template falls back to the default JSON.
func TestSendWebhook_TemplateBody(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	summary := model.NotificationSummary{Cluster: "c9", FailCount: 5}
	base := model.Config{WebhookEnabled: true, WebhookURL: srv.URL, RetryMaxAttempts: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}

	cfg := base
	cfg.WebhookTemplate = `{"cluster":"{{.Cluster}}","fail":{{.FailCount}}}`
	if err := SendWebhook(context.Background(), srv.Client(), cfg, summary); err != nil {
		t.Fatalf("SendWebhook (template): %v", err)
	}
	if len(bodies) != 1 || bodies[0] != `{"cluster":"c9","fail":5}` {
		t.Fatalf("template body not used, got: %v", bodies)
	}

	bodies = nil
	cfgBad := base
	cfgBad.WebhookTemplate = `{{.Nope}}`
	if err := SendWebhook(context.Background(), srv.Client(), cfgBad, summary); err != nil {
		t.Fatalf("SendWebhook (broken template): %v", err)
	}
	if len(bodies) != 1 || !strings.Contains(bodies[0], `"Cluster":"c9"`) {
		t.Fatalf("broken template should fall back to default JSON, got: %v", bodies)
	}
}

// TestWrappers_SkipDisabled verifies a disabled channel is not counted as an
// attempt (the senders return nil early when off), via the shared accumulator.
func TestWrappers_SkipDisabled(t *testing.T) {
	ResetMetrics()
	defer ResetMetrics()

	cfg := model.Config{} // all channels disabled
	if err := SendEmailWithRetry(cfg, "subj", "body", ""); err != nil {
		t.Fatalf("disabled email should no-op, got: %v", err)
	}
	if err := SendWebhookWithRetry(context.Background(), http.DefaultClient, cfg, model.NotificationSummary{}); err != nil {
		t.Fatalf("disabled webhook should no-op, got: %v", err)
	}
	if err := SendSlackWithRetry(context.Background(), http.DefaultClient, cfg, model.NotificationSummary{}); err != nil {
		t.Fatalf("disabled slack should no-op, got: %v", err)
	}
	attempts, failures := SnapshotMetrics()
	if len(attempts) != 0 || len(failures) != 0 {
		t.Errorf("disabled channels must not be recorded, got attempts=%v failures=%v", attempts, failures)
	}
}

// TestWebhookWithRetry_RecordsSuccess confirms a successful delivery records an
// attempt with no failure on the shared accumulator.
func TestWebhookWithRetry_RecordsSuccess(t *testing.T) {
	ResetMetrics()
	defer ResetMetrics()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := model.Config{WebhookEnabled: true, WebhookURL: srv.URL, RetryMaxAttempts: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}
	if err := SendWebhookWithRetry(context.Background(), srv.Client(), cfg, model.NotificationSummary{Cluster: "c1"}); err != nil {
		t.Fatalf("SendWebhookWithRetry: %v", err)
	}
	attempts, failures := SnapshotMetrics()
	if attempts["webhook"] != 1 || failures["webhook"] != 0 {
		t.Errorf("got attempts=%d failures=%d, want 1/0", attempts["webhook"], failures["webhook"])
	}
}
