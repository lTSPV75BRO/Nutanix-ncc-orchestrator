package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

const nestedNotifConfig = `schema-version: 1
runner:
  targets:
    clusters:
      - 10.0.0.1
  credentials:
    username: admin
    password: secret
notifications:
  email:
    enabled: false
`

func newNotifServer(t *testing.T, cfg string) *apiServer {
	t.Helper()
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return &apiServer{repoRoot: tmp, configPath: "config.yaml", notificationStatePath: ".ncc-api-notifications.json"}
}

func readConfigDoc(t *testing.T, s *apiServer) (string, map[string]interface{}) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(s.repoRoot, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	doc := map[string]interface{}{}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return string(b), doc
}

// Regression: reading the notification settings rewrote a nested config.yaml,
// and saving them copied credentials and clusters to top-level keys that then
// shadowed the nested values the Clusters page edits.
func TestNotificationsKeepNestedConfigLayout(t *testing.T) {
	s := newNotifServer(t, nestedNotifConfig)
	st, err := s.loadNotifications()
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := readConfigDoc(t, s); text != nestedNotifConfig {
		t.Fatalf("loading notifications rewrote config.yaml:\n%s", text)
	}

	st.Slack.Enabled = true
	st.Slack.WebhookURL = "https://hooks.example.com/x"
	st.RunReports.PerRun = true
	if err := s.saveNotifications(st); err != nil {
		t.Fatal(err)
	}
	text, doc := readConfigDoc(t, s)
	for _, k := range []string{"password", "username", "clusters", "slack-enabled", "email-enabled", "notify-digest"} {
		if _, ok := doc[k]; ok {
			t.Fatalf("top-level %q written into a nested config:\n%s", k, text)
		}
	}
	if v, _ := nestedConfigValue(doc, []string{"notifications", "slack", "webhook-url"}); v != "https://hooks.example.com/x" {
		t.Fatalf("slack URL not saved under notifications.slack:\n%s", text)
	}
	if v, _ := nestedConfigValue(doc, []string{"notifications", "email", "digest"}); v != true {
		t.Fatalf("per-run reports not saved as notifications.email.digest:\n%s", text)
	}
	if v, _ := nestedConfigValue(doc, []string{"runner", "credentials", "password"}); v != "secret" {
		t.Fatalf("credentials changed:\n%s", text)
	}
	again, err := s.loadNotifications()
	if err != nil {
		t.Fatal(err)
	}
	if !again.Slack.Enabled || again.Slack.WebhookURL != "https://hooks.example.com/x" || !again.RunReports.PerRun {
		t.Fatalf("settings did not round-trip: %+v", again)
	}
}

// Installs already hit by the bug have identical top-level copies; a save
// removes those (and keeps a copy that differs, which is what the runner used).
func TestNotificationsSaveDropsIdenticalShadowKeys(t *testing.T) {
	s := newNotifServer(t, nestedNotifConfig+"password: secret\nusername: someone-else\n")
	st, err := s.loadNotifications()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.saveNotifications(st); err != nil {
		t.Fatal(err)
	}
	text, doc := readConfigDoc(t, s)
	if _, ok := doc["password"]; ok {
		t.Fatalf("identical top-level password copy kept:\n%s", text)
	}
	if doc["username"] != "someone-else" {
		t.Fatalf("a differing top-level value must be kept:\n%s", text)
	}
}

func TestFlatConfigStaysFlat(t *testing.T) {
	s := newNotifServer(t, "clusters: 10.0.0.1\n")
	st, _ := s.loadNotifications()
	st.Webhook.Enabled = true
	st.Webhook.URL = "https://hooks.example.com/ncc"
	if err := s.saveNotifications(st); err != nil {
		t.Fatal(err)
	}
	text, doc := readConfigDoc(t, s)
	if doc["webhook-url"] != "https://hooks.example.com/ncc" {
		t.Fatalf("flat config should get flat keys:\n%s", text)
	}
	if _, ok := doc["notifications"]; ok {
		t.Fatalf("flat config gained a nested block:\n%s", text)
	}
}

// Regression: the digest interval was dropped on load, so an enabled digest
// never became due and the next save failed validation.
func TestDigestIntervalAndMaintenanceNotesSurviveReload(t *testing.T) {
	s := newNotifServer(t, "clusters: 10.0.0.1\n")
	st, _ := s.loadNotifications()
	st.Digest = digestConfig{Enabled: true, Every: "7d"}
	st.Maintenance = []maintenanceWindow{{Start: "2026-06-01T10:00:00Z", End: "2026-06-01T12:00:00Z", Note: "AOS upgrade"}}
	if err := s.saveNotifications(st); err != nil {
		t.Fatal(err)
	}
	got, err := s.loadNotifications()
	if err != nil {
		t.Fatal(err)
	}
	if !got.Digest.Enabled || got.Digest.Every != "7d" {
		t.Fatalf("digest = %+v, want enabled every 7d", got.Digest)
	}
	if len(got.Maintenance) != 1 || got.Maintenance[0].Note != "AOS upgrade" {
		t.Fatalf("maintenance note lost: %+v", got.Maintenance)
	}
}

func TestValidHTTPURL(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://hooks.slack.com/services/T/B/C": true,
		"http://10.0.0.5:8080/hook":              true,
		"/hooks/ncc":                             false,
		"mailto:ops@example.com":                 false,
		"ftp://example.com/x":                    false,
		"https://":                               false,
	} {
		if got := validHTTPURL(raw); got != want {
			t.Errorf("validHTTPURL(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestNotificationTextIsReadable(t *testing.T) {
	text := notificationText("manual_test", map[string]interface{}{"manual_test": true, "channel": "all"})
	if strings.Contains(text, "{") || !strings.Contains(text, "this channel works") {
		t.Fatalf("test message not plain text: %q", text)
	}
	text = notificationText("run_failure", map[string]interface{}{
		"error": "exit status 2", "started_at": "2026-11-03T10:00:00Z", "output_dir": "/opt/ncc/out", "last_output": "line1\nline2",
	})
	for _, want := range []string{"Error: exit status 2", "Started: 2026-11-03T10:00:00Z", "output dir: /opt/ncc/out", "Last output:\nline1\nline2"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

// The test endpoint's "all" used to require Slack, webhook and email to all be
// on; it now sends to the channels that are on, signed like the runner's
// webhook, and refuses when none is.
func TestNotificationTestAllUsesEnabledChannels(t *testing.T) {
	const secret = "s3cret"
	var mu sync.Mutex
	var gotBody []byte
	var gotSig, gotHeader string
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get("X-NCC-Signature")
		gotHeader = r.Header.Get("X-Team")
	}))
	defer hook.Close()

	s := newNotifServer(t, "clusters: 10.0.0.1\nwebhook-secret: "+secret+"\nwebhook-headers:\n  X-Team: sre\n")
	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/notifications/test", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.handleNotificationsTest(rec, req)
		return rec
	}
	if rec := post(`{"channel":"all"}`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "no notification channel") {
		t.Fatalf("nothing enabled: got %d %s", rec.Code, rec.Body.String())
	}

	st, _ := s.loadNotifications()
	st.Webhook = webhookNotificationConfig{Enabled: true, URL: hook.URL}
	if err := s.saveNotifications(st); err != nil {
		t.Fatal(err)
	}
	if rec := post(`{"channel":"all"}`); rec.Code != http.StatusOK {
		t.Fatalf("all with only webhook on: got %d %s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	body, sig, hdr := gotBody, gotSig, gotHeader
	mu.Unlock()
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil || payload["event"] != "manual_test" {
		t.Fatalf("webhook payload = %s (%v)", body, err)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	if want := "sha256=" + hex.EncodeToString(mac.Sum(nil)); sig != want {
		t.Fatalf("signature = %q, want %q", sig, want)
	}
	if hdr != "sre" {
		t.Fatalf("webhook-headers not sent: X-Team=%q", hdr)
	}

	if rec := post(`{"channel":"all","kind":"digest"}`); rec.Code != http.StatusOK {
		t.Fatalf("digest now: got %d %s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	body = gotBody
	mu.Unlock()
	if !strings.Contains(string(body), `"event":"digest"`) {
		t.Fatalf("digest not sent: %s", body)
	}
	if rec := post(`{"kind":"bogus"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad kind: got %d", rec.Code)
	}
}

// Regression: every alert sent (run results, self-heal) saved its delivery
// status by rewriting config.yaml, which reformatted it and dropped comments.
func TestDispatchLeavesConfigFileAlone(t *testing.T) {
	cfg := "# operator notes: keep this\nschema-version: 1\nrunner:\n  targets:\n    clusters: 10.0.0.1 # lab\n"
	s := newNotifServer(t, cfg)
	if err := os.WriteFile(filepath.Join(s.repoRoot, ".ncc-api-notifications.json"), []byte(`{"enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := s.loadNotifications()
	if err != nil {
		t.Fatal(err)
	}
	before, _ := readConfigDoc(t, s)
	if err := s.dispatchNotifications(&st, "selfheal", "Self-heal", map[string]interface{}{"failing": 1}, nil); err != nil {
		t.Fatal(err)
	}
	if after, _ := readConfigDoc(t, s); after != before {
		t.Fatalf("dispatch rewrote config.yaml:\n%s", after)
	}
}
