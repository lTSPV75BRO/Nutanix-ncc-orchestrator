package main

import (
	"regexp"
	"strings"
)

// Redaction of configuration text before it is shown outside the server
// (the "redacted" view of Settings, MCP tools that return configuration,
// diagnostics). The earlier version masked only lines containing
// "password:", "token:" or "secret:", so a Slack webhook URL, an API key, an
// HMAC key, a passphrase or an inline SMTP password under another name was
// returned in clear. Keys are now matched by name, and the value is masked
// unless it is clearly not a secret.

var (
	redactLineRe = regexp.MustCompile(`^(\s*(?:-\s+)?["']?)([A-Za-z0-9_.\-]+)(["']?\s*:\s*)(.*)$`)

	// redactSecretKeyRe matches key names that normally hold a credential.
	redactSecretKeyRe = regexp.MustCompile(`(?i)(pass(word|wd|phrase)?|pwd|secret|token|api[-_]?key|private[-_]?key|hmac|webhook|credential|authorization|cookie|bearer|dsn|bind[-_]?pw)`)

	// redactBenignSuffixRe matches key names that name a file, folder or
	// switch about a secret, not the secret itself.
	redactBenignSuffixRe = regexp.MustCompile(`(?i)([-_.](file|path|dir|provider|enabled|required|timeout|ttl|policy|header|name|id)|^secrets?$)$`)

	redactPlainValueRe = regexp.MustCompile(`^(true|false|null|nil|~|yes|no|on|off|[0-9]+(\.[0-9]+)?(ms|s|m|h|d)?)$`)

	redactURLUserinfoRe = regexp.MustCompile(`(?i)(\b[a-z][a-z0-9+.\-]*://)[^/\s:@"']+:[^/\s@"']+@`)
	redactHookURLRe     = regexp.MustCompile(`(?i)(hooks\.slack\.com/services/|discord(?:app)?\.com/api/webhooks/|outlook\.office\.com/webhook|webhook\.office\.com/webhook)[^\s"']*`)
)

func redactSensitiveText(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return raw
	}
	lines := strings.Split(raw, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		cr := ""
		if strings.HasSuffix(line, "\r") {
			cr = "\r"
			line = strings.TrimSuffix(line, "\r")
		}
		lines[i] = redactLine(line) + cr
	}
	return strings.Join(lines, "\n")
}

func redactLine(line string) string {
	if m := redactLineRe.FindStringSubmatch(line); m != nil {
		key, val := m[2], strings.TrimSpace(m[4])
		if redactSecretKeyRe.MatchString(key) && !redactBenignSuffixRe.MatchString(key) && val != "" {
			bare := strings.Trim(val, `"'`)
			// Keep references (secret://NAME) and plain switches, numbers and
			// durations; mask everything else, including empty-looking tails
			// such as `password: x # comment`.
			if !strings.HasPrefix(strings.ToLower(bare), "secret://") && !redactPlainValueRe.MatchString(strings.ToLower(bare)) && bare != "" {
				return m[1] + key + m[3] + `"***"`
			}
		}
	}
	line = redactURLUserinfoRe.ReplaceAllString(line, "${1}***:***@")
	line = redactHookURLRe.ReplaceAllString(line, "${1}***")
	return line
}
