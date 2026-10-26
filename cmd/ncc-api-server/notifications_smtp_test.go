package main

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestSendSMTPMailRejectsHeaderInjectionInAddresses(t *testing.T) {
	cases := []emailNotificationConfig{
		{SMTPHost: "127.0.0.1", SMTPPort: 1, From: "ncc@example.com\r\nBcc: evil@example.com", To: "ops@example.com"},
		{SMTPHost: "127.0.0.1", SMTPPort: 1, From: "ncc@example.com", To: "ops@example.com\r\nBcc: evil@example.com"},
	}
	for i, cfg := range cases {
		err := sendSMTPMail(cfg, "subject", "body")
		if err == nil || !strings.Contains(err.Error(), "invalid email") {
			t.Fatalf("case %d: want address validation error before dialing, got %v", i, err)
		}
	}
}

func TestStripCRLF(t *testing.T) {
	if got := stripCRLF("a\r\nBcc: x\ny\rz"); strings.ContainsAny(got, "\r\n") {
		t.Fatalf("line break survived: %q", got)
	}
}

func TestBase64MIMEBodyRoundTripsAndWraps(t *testing.T) {
	body := strings.Repeat("cluster c1 FAIL\r\n.\r\nContent-Type: text/html\r\n", 20)
	enc := base64MIMEBody(body)
	for _, line := range strings.Split(enc, "\r\n") {
		if len(line) > 76 {
			t.Fatalf("line longer than 76 chars: %d", len(line))
		}
	}
	dec, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(enc, "\r\n", ""))
	if err != nil || string(dec) != body {
		t.Fatalf("round trip failed: err=%v", err)
	}
}

// Regression: an unparsable webhook URL left http.NewRequest's request nil
// and sendWebhookJSON dereferenced it (a panic in the notification path).
func TestSendWebhookJSONRejectsMalformedURL(t *testing.T) {
	if err := sendWebhookJSON("http://[::1", map[string]interface{}{"x": 1}); err == nil {
		t.Fatal("expected an error for a malformed webhook URL")
	}
}
