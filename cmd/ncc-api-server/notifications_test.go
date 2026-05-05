package main

import (
	"errors"
	"testing"
)

func TestValidateNotificationState(t *testing.T) {
	st := defaultNotificationState()
	st.Slack.Enabled = true
	st.Slack.WebhookURL = "https://hooks.slack.com/services/T000/B000/XXXX"
	if err := validateNotificationState(st); err != nil {
		t.Fatalf("expected valid slack config: %v", err)
	}

	st.Webhook.Enabled = true
	st.Webhook.URL = "https://example.com/webhook"
	if err := validateNotificationState(st); err != nil {
		t.Fatalf("expected valid webhook config: %v", err)
	}

	st.Email.Enabled = true
	st.Email.SMTPHost = "smtp.example.com"
	st.Email.SMTPPort = 587
	st.Email.From = "ncc@example.com"
	st.Email.To = "ops@example.com"
	if err := validateNotificationState(st); err != nil {
		t.Fatalf("expected valid email config: %v", err)
	}
}

func TestValidateNotificationStateRejectsInvalid(t *testing.T) {
	st := defaultNotificationState()
	st.Slack.Enabled = true
	if err := validateNotificationState(st); err == nil {
		t.Fatal("expected missing slack webhook URL to fail")
	}

	st = defaultNotificationState()
	st.Webhook.Enabled = true
	st.Webhook.URL = "://bad-url"
	if err := validateNotificationState(st); err == nil {
		t.Fatal("expected invalid webhook URL to fail")
	}

	st = defaultNotificationState()
	st.Email.Enabled = true
	st.Email.SMTPHost = "smtp.example.com"
	st.Email.SMTPPort = 587
	if err := validateNotificationState(st); err == nil {
		t.Fatal("expected missing from/to to fail")
	}
}

func TestRequestedChannels(t *testing.T) {
	all, err := requestedChannels("all")
	if err != nil {
		t.Fatalf("requestedChannels(all): %v", err)
	}
	if !(all["slack"] && all["webhook"] && all["email"]) {
		t.Fatal("expected all channels")
	}
	one, err := requestedChannels("slack")
	if err != nil {
		t.Fatalf("requestedChannels(slack): %v", err)
	}
	if len(one) != 1 || !one["slack"] {
		t.Fatal("expected only slack channel")
	}
	if _, err := requestedChannels("bad"); err == nil {
		t.Fatal("expected invalid channel to fail")
	}
}

func TestRecordDelivery(t *testing.T) {
	s := &apiServer{}
	st := defaultNotificationState()
	s.recordDelivery(&st, "webhook", "manual_test", nil)
	got := st.LastDelivery["webhook"]
	if !got.Success || got.TotalSuccess != 1 || got.TotalFailure != 0 {
		t.Fatalf("unexpected success status: %+v", got)
	}
	s.recordDelivery(&st, "webhook", "manual_test", errors.New("fail"))
	got = st.LastDelivery["webhook"]
	if got.Success || got.TotalSuccess != 1 || got.TotalFailure != 1 {
		t.Fatalf("unexpected failure status: %+v", got)
	}
	if got.LastError == "" {
		t.Fatal("expected last error to be recorded")
	}
}
