package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadNotificationsReadsConfigYAMLAndLegacyMetadata(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "config.yaml")
	cfg := strings.Join([]string{
		"email-enabled: true",
		"smtp-server: smtp.example.com",
		"smtp-port: 2525",
		"smtp-user: ncc",
		"smtp-password: cfg-secret",
		"email-from: ncc@example.com",
		"email-to: ops@example.com,sre@example.com",
		"webhook-enabled: true",
		"webhook-url: https://hooks.example.com/ncc",
		"slack-enabled: true",
		"slack-webhook-url: https://hooks.slack.com/services/T/B/C",
		"slack-channel: '#ncc-alerts'",
		"quiet-hours: 22:00-07:00",
		"maintenance-windows: 2026-06-01T10:00:00Z/2026-06-01T12:00:00Z",
		"notify-digest: true",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	legacyPath := filepath.Join(tmp, ".ncc-api-notifications.json")
	legacy := `{"enabled":false,"events":{"run_success":true,"run_failure":false,"policy_violations":false},"quiet":{"timezone":"Asia/Kolkata","allow_failures":true},"digest":{"last_sent_at":"2026-06-01T00:00:00Z"},"last_delivery":{"email":{"success":true}}}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	s := &apiServer{
		repoRoot:              tmp,
		configPath:            "config.yaml",
		notificationStatePath: ".ncc-api-notifications.json",
	}
	st, err := s.loadNotifications()
	if err != nil {
		t.Fatalf("loadNotifications: %v", err)
	}
	if !st.Email.Enabled || st.Email.SMTPHost != "smtp.example.com" || st.Email.SMTPPort != 2525 {
		t.Fatalf("email not loaded from config: %+v", st.Email)
	}
	if st.Email.Password != "cfg-secret" {
		t.Fatalf("expected config smtp-password, got %q", st.Email.Password)
	}
	if !st.Webhook.Enabled || st.Webhook.URL == "" {
		t.Fatalf("webhook not loaded from config: %+v", st.Webhook)
	}
	if !st.Slack.Enabled || st.Slack.WebhookURL == "" || st.Slack.Channel != "#ncc-alerts" {
		t.Fatalf("slack not loaded from config: %+v", st.Slack)
	}
	if !st.Quiet.Enabled || st.Quiet.Start != "22:00" || st.Quiet.End != "07:00" || st.Quiet.Timezone != "Asia/Kolkata" || !st.Quiet.AllowFailures {
		t.Fatalf("quiet settings not merged correctly: %+v", st.Quiet)
	}
	if len(st.Maintenance) != 1 {
		t.Fatalf("maintenance windows not loaded: %+v", st.Maintenance)
	}
	if !st.Digest.Enabled || st.Digest.LastSentAt == "" {
		t.Fatalf("digest settings not merged correctly: %+v", st.Digest)
	}
	if st.Enabled {
		t.Fatalf("expected legacy enabled=false to win for API-local toggle")
	}
	if st.Events.RunFailure {
		t.Fatalf("expected legacy events to be applied")
	}
}

func TestSaveNotificationsPersistsRuntimeFieldsToConfigYAML(t *testing.T) {
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte("clusters: 10.10.10.10\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	s := &apiServer{
		repoRoot:              tmp,
		configPath:            "config.yaml",
		notificationStatePath: ".ncc-api-notifications.json",
	}
	st := defaultNotificationState()
	st.Email.Enabled = true
	st.Email.SMTPHost = "smtp.example.com"
	st.Email.SMTPPort = 465
	st.Email.Username = "svc-ncc"
	st.Email.Password = "new-secret"
	st.Email.From = "ncc@example.com"
	st.Email.To = "ops@example.com, sre@example.com"
	st.Webhook.Enabled = true
	st.Webhook.URL = "https://hooks.example.com/ncc"
	st.Slack.Enabled = true
	st.Slack.WebhookURL = "https://hooks.slack.com/services/T/B/C"
	st.Slack.Channel = "#ncc"
	st.Quiet = quietHoursConfig{Enabled: true, Start: "21:00", End: "06:00", Timezone: "UTC"}
	st.Maintenance = []maintenanceWindow{{Start: "2026-06-01T10:00:00Z", End: "2026-06-01T12:00:00Z"}}
	st.Digest = digestConfig{Enabled: true, Every: "24h", LastSentAt: "2026-06-01T00:00:00Z"}
	st.LastDelivery = map[string]notificationDeliveryStatus{"email": {Success: true}}

	if err := s.saveNotifications(st); err != nil {
		t.Fatalf("saveNotifications: %v", err)
	}
	cfgRaw, err := os.ReadFile(filepath.Join(tmp, "config.yaml"))
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	cfgText := string(cfgRaw)
	for _, want := range []string{
		"email-enabled: true",
		"smtp-server: smtp.example.com",
		"smtp-port: 465",
		"smtp-user: svc-ncc",
		"smtp-password: new-secret",
		"email-from: ncc@example.com",
		"email-to: ops@example.com, sre@example.com",
		"webhook-enabled: true",
		"webhook-url: https://hooks.example.com/ncc",
		"slack-enabled: true",
		"slack-webhook-url: https://hooks.slack.com/services/T/B/C",
		"slack-channel: '#ncc'",
		"quiet-hours: 21:00-06:00",
		"maintenance-windows: 2026-06-01T10:00:00Z/2026-06-01T12:00:00Z",
		"notify-digest: true",
	} {
		if !strings.Contains(cfgText, want) {
			t.Fatalf("config missing %q\n%s", want, cfgText)
		}
	}
	legacyRaw, err := os.ReadFile(filepath.Join(tmp, ".ncc-api-notifications.json"))
	if err != nil {
		t.Fatalf("read legacy state: %v", err)
	}
	if !strings.Contains(string(legacyRaw), "\"last_delivery\"") {
		t.Fatalf("legacy state should keep delivery metadata: %s", string(legacyRaw))
	}
}

func TestLoadNotificationsMigratesLegacyRuntimeSettingsIntoConfigYAML(t *testing.T) {
	tmp := t.TempDir()
	// Intentionally omit notification runtime keys.
	if err := os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte("clusters: 10.10.10.10\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	legacyPath := filepath.Join(tmp, ".ncc-api-notifications.json")
	legacy := `{
		"email":{"enabled":true,"smtp_host":"smtp.legacy.example.com","smtp_port":2525,"username":"legacy-user","password":"legacy-pass","from":"legacy@example.com","to":"ops@example.com"},
		"webhook":{"enabled":true,"url":"https://hooks.legacy.example.com/ncc"},
		"slack":{"enabled":true,"webhook_url":"https://hooks.slack.com/services/L/E/G","channel":"#legacy"},
		"digest":{"enabled":true},
		"quiet":{"enabled":true,"start":"23:00","end":"06:00"}
	}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}
	s := &apiServer{
		repoRoot:              tmp,
		configPath:            "config.yaml",
		notificationStatePath: ".ncc-api-notifications.json",
	}
	st, err := s.loadNotifications()
	if err != nil {
		t.Fatalf("loadNotifications: %v", err)
	}
	if st.Email.SMTPHost != "smtp.legacy.example.com" || !st.Email.Enabled {
		t.Fatalf("expected legacy email settings to migrate, got %+v", st.Email)
	}
	cfgRaw, err := os.ReadFile(filepath.Join(tmp, "config.yaml"))
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	cfgText := string(cfgRaw)
	for _, want := range []string{
		"email-enabled: true",
		"smtp-server: smtp.legacy.example.com",
		"smtp-port: 2525",
		"smtp-user: legacy-user",
		"smtp-password: legacy-pass",
		"email-from: legacy@example.com",
		"email-to: ops@example.com",
		"webhook-enabled: true",
		"webhook-url: https://hooks.legacy.example.com/ncc",
		"slack-enabled: true",
		"slack-webhook-url: https://hooks.slack.com/services/L/E/G",
		"slack-channel: '#legacy'",
		"notify-digest: true",
		"quiet-hours: 23:00-06:00",
	} {
		if !strings.Contains(cfgText, want) {
			t.Fatalf("config missing %q after migration:\n%s", want, cfgText)
		}
	}
}
