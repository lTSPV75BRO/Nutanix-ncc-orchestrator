package main

import (
	"strings"
	"testing"
)

// The redacted configuration view must not carry credentials under names the
// old line matcher did not know (webhook URLs, API keys, HMAC keys,
// passphrases, bind passwords), and must keep what is safe and useful.
func TestRedactSensitiveTextMasksCredentialsByKeyName(t *testing.T) {
	in := strings.Join([]string{
		"runner:",
		"  credentials:",
		"    username: admin",
		"    password: Sup3r-Secret-Value",
		"    api-key: abcd1234efgh5678",
		"notifications:",
		"  slack-webhook-url: https://hooks.slack.com/services/T000/B000/XXXXXXXX",
		"  webhook-hmac: 0123456789abcdef",
		"  smtp-password: 'hunter2'",
		"  smtp-host: smtp.example.com",
		"ldap:",
		"  bind-pw: \"cn=svc-secret\"",
		"  passphrase: correct horse battery",
		"mail: smtps://user:pa55word@smtp.example.com:465",
		"- token: eyJhbGciOi.payload.sig",
	}, "\n")
	out := redactSensitiveText(in)
	for _, leaked := range []string{"Sup3r-Secret-Value", "abcd1234efgh5678", "XXXXXXXX", "0123456789abcdef", "hunter2", "cn=svc-secret", "correct horse", "pa55word", "eyJhbGciOi"} {
		if strings.Contains(out, leaked) {
			t.Errorf("%q leaked:\n%s", leaked, out)
		}
	}
	for _, kept := range []string{"username: admin", "smtp-host: smtp.example.com", "credentials:"} {
		if !strings.Contains(out, kept) {
			t.Errorf("%q was lost:\n%s", kept, out)
		}
	}
}

func TestRedactSensitiveTextKeepsReferencesAndSwitches(t *testing.T) {
	in := strings.Join([]string{
		"password: secret://NCC_PASSWORD",
		"secrets-provider: env",
		"secrets-file: /data/secrets.yaml",
		"webhook-enabled: true",
		"webhook-timeout: 10s",
		"webhook-secret-file: /run/secrets/hook",
		"token-ttl: 90",
	}, "\r\n")
	out := redactSensitiveText(in)
	if out != in {
		t.Errorf("benign lines were changed:\n%s", out)
	}
}
