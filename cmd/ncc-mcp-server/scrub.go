package main

import (
	"regexp"
	"strings"
)

// Everything an API response puts into a tool result is read by the model, and
// by anything the model is talking to. API responses are written for people who
// are signed in, so a few carry configuration or state with credentials in it
// (the configuration endpoint returns the raw file next to a redacted copy).
// scrubForModel is the last line of defence: it prefers the redacted copy of a
// field and masks values under credential-like key names, whatever the endpoint.

var (
	scrubSecretKeyRe = regexp.MustCompile(`(?i)(pass(word|wd|phrase)?|pwd|secret|token|api[-_]?key|private[-_]?key|hmac|webhook|credential|authorization|cookie|bearer|dsn|bind[-_]?pw)`)
	// A key that names a file, folder or switch about a secret is not secret.
	scrubBenignSuffixRe = regexp.MustCompile(`(?i)([-_.](file|path|dir|provider|enabled|required|timeout|ttl|policy|header|name|id|count|state|status|protection|source)|^secrets?$)$`)
	scrubURLUserinfoRe  = regexp.MustCompile(`(?i)(\b[a-z][a-z0-9+.\-]*://)[^/\s:@"']+:[^/\s@"']+@`)
	scrubHookURLRe      = regexp.MustCompile(`(?i)(hooks\.slack\.com/services/|discord(?:app)?\.com/api/webhooks/|outlook\.office\.com/webhook|webhook\.office\.com/webhook)[^\s"']*`)
)

const scrubMask = "***"

func scrubForModel(v any) any {
	switch t := v.(type) {
	case map[string]any:
		// Prefer the server's redacted copy over the raw one.
		if red, ok := t["content_redacted"]; ok {
			t["content"] = red
			delete(t, "content_redacted")
		}
		for k, val := range t {
			if scrubSecretKeyRe.MatchString(k) && !scrubBenignSuffixRe.MatchString(k) {
				if s, isStr := val.(string); isStr {
					if s != "" && !strings.HasPrefix(strings.ToLower(s), "secret://") {
						t[k] = scrubMask
					}
					continue
				}
				if val != nil {
					if _, isBool := val.(bool); !isBool {
						if _, isNum := val.(float64); !isNum {
							t[k] = scrubMask
							continue
						}
					}
				}
			}
			t[k] = scrubForModel(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = scrubForModel(t[i])
		}
		return t
	case string:
		t = scrubURLUserinfoRe.ReplaceAllString(t, "${1}***:***@")
		return scrubHookURLRe.ReplaceAllString(t, "${1}***")
	default:
		return v
	}
}
