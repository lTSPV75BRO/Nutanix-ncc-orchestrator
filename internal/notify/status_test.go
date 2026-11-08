package notify

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"goncc/internal/model"
)

func TestRecordDeliverySuccessThenFailure(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	if err := RecordDelivery(dir, DeliveryOutcome{Channel: "slack", Source: "api", Event: "manual_test", Attempts: 1}, t0); err != nil {
		t.Fatal(err)
	}
	t1 := t0.Add(time.Hour)
	secret := errors.New(`Post "https://hooks.slack.com/services/T000/B000/XXXXSECRET": dial tcp: i/o timeout`)
	if err := RecordDelivery(dir, DeliveryOutcome{Channel: "slack", Source: "runner", Attempts: 3, Err: secret}, t1); err != nil {
		t.Fatal(err)
	}
	st, err := LoadStatus(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := st.Channels["slack"]
	if got.Success || got.Attempts != 3 || got.Source != "runner" {
		t.Fatalf("unexpected status: %+v", got)
	}
	if got.LastSuccessAt != t0.Format(time.RFC3339) || got.LastAttemptAt != t1.Format(time.RFC3339) {
		t.Fatalf("times not kept: %+v", got)
	}
	if strings.Contains(got.LastError, "XXXXSECRET") || strings.Contains(got.LastError, "/services/") {
		t.Fatalf("error not redacted: %q", got.LastError)
	}
	if !strings.Contains(got.LastError, "https://hooks.slack.com/") || !strings.Contains(got.LastError, "i/o timeout") {
		t.Fatalf("redaction dropped useful context: %q", got.LastError)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, StatusFileName))
	if strings.Contains(string(raw), "XXXXSECRET") {
		t.Fatal("secret written to the status file")
	}
	// A later success clears the error.
	if err := RecordDelivery(dir, DeliveryOutcome{Channel: "slack", Attempts: 2}, t1.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	st, _ = LoadStatus(dir)
	if got := st.Channels["slack"]; !got.Success || got.LastError != "" || got.Attempts != 2 {
		t.Fatalf("success did not clear the error: %+v", got)
	}
}

func TestRecordDeliveryConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	channels := []string{"email", "webhook", "slack"}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := channels[i%len(channels)]
			var err error
			if i%2 == 0 {
				err = fmt.Errorf("attempt %d failed", i)
			}
			if rerr := RecordDelivery(dir, DeliveryOutcome{Channel: ch, Attempts: 1 + i%3, Err: err}, time.Now()); rerr != nil {
				t.Error(rerr)
			}
		}(i)
	}
	wg.Wait()
	st, err := LoadStatus(dir)
	if err != nil {
		t.Fatalf("status file corrupt after concurrent writes: %v", err)
	}
	for _, ch := range channels {
		if st.Channels[ch].LastAttemptAt == "" {
			t.Fatalf("lost update for %s: %+v", ch, st.Channels)
		}
	}
	left, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

func TestRecordDeliveryEmptyDirIsNoop(t *testing.T) {
	if err := RecordDelivery("", DeliveryOutcome{Channel: "email"}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestLoadStatusCorruptFileRecovers(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, StatusFileName), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadStatus(dir); err == nil {
		t.Fatal("expected a parse error")
	}
	if err := RecordDelivery(dir, DeliveryOutcome{Channel: "email", Attempts: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	st, err := LoadStatus(dir)
	if err != nil || !st.Channels["email"].Success {
		t.Fatalf("did not recover: %v %+v", err, st)
	}
}

func TestCountDeadLetters(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"slack-20261108T100000Z-1.json", "slack-20261108T100001Z-2.json", "webhook-20261108T100000Z-3.json", "slack-notes.txt"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := CountDeadLetters(dir, "slack"); got != 2 {
		t.Fatalf("slack = %d, want 2", got)
	}
	if got := CountDeadLetters(dir, "webhook"); got != 1 {
		t.Fatalf("webhook = %d, want 1", got)
	}
	if got := CountDeadLetters(filepath.Join(dir, "missing"), "slack"); got != 0 {
		t.Fatalf("missing dir = %d", got)
	}
}

type statusFailClient struct{}

func (statusFailClient) Do(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection refused")
}

func TestRunnerWebhookFailureRecordsStatus(t *testing.T) {
	out := t.TempDir()
	dl := filepath.Join(t.TempDir(), "deadletter")
	cfg := model.Config{
		WebhookEnabled:            true,
		WebhookURL:                "https://hooks.example.com/secret-path/abc",
		RetryMaxAttempts:          1,
		OutputDirFiltered:         out,
		NotificationDeadLetterDir: dl,
	}
	if err := SendWebhookWithRetry(context.Background(), statusFailClient{}, cfg, model.NotificationSummary{Cluster: "c1"}); err == nil {
		t.Fatal("expected failure")
	}
	st, err := LoadStatus(out)
	if err != nil {
		t.Fatal(err)
	}
	got := st.Channels["webhook"]
	if got.Success || got.Attempts != RetryAttempts || got.Source != "runner" {
		t.Fatalf("unexpected: %+v", got)
	}
	if strings.Contains(got.LastError, "secret-path") {
		t.Fatalf("not redacted: %q", got.LastError)
	}
	if got.DeadLetterDir == "" || CountDeadLetters(got.DeadLetterDir, "webhook") != 1 {
		t.Fatalf("dead letter not counted: %+v", got)
	}
}
