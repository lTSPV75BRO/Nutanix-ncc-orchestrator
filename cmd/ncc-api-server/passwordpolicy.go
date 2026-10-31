package main

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Password policy for local accounts. Local passwords are a single
// authentication factor, so this follows NIST SP 800-63B rev. 4 §3.1.1.2:
//
//   - at least 15 characters (counted as Unicode code points);
//   - any printable characters and spaces, no composition rules;
//   - long passphrases accepted up to bcrypt's 72-byte input limit
//     (64 or more characters for ASCII text);
//   - rejected when found on a blocklist: repeated or sequential characters,
//     the username, and common words such as "password" or the product name,
//     alone, repeated, or padded with digits and symbols.
//
// No periodic expiry: SP 800-63B forbids forced rotation without evidence of
// compromise. Existing accounts keep working; the policy applies whenever a
// password is set.
const (
	minPasswordLen   = 15
	maxPasswordBytes = 72
)

// weakPasswordWords are words that, alone or repeated (with digits or symbols
// around them), make a guessable password. Includes context-specific terms
// (product, vendor) as SP 800-63B requires.
var weakPasswordWords = []string{
	"password", "passw0rd", "admin", "administrator", "welcome", "letmein",
	"changeme", "qwerty", "asdf", "zxcv", "iloveyou", "monkey", "dragon",
	"default", "secret", "login", "master", "root", "user", "test",
	"nutanix", "prism", "ncc", "orchestrator", "cluster",
}

// validatePassword returns a user-facing reason when password breaks the
// policy for the given username, or "" when it is acceptable.
func validatePassword(username, password string) string {
	if utf8.RuneCountInString(password) < minPasswordLen {
		return fmt.Sprintf("password must be at least %d characters; a few unrelated words make a strong, memorable one", minPasswordLen)
	}
	if len(password) > maxPasswordBytes {
		return fmt.Sprintf("password must be at most %d bytes", maxPasswordBytes)
	}
	for _, r := range password {
		if unicode.IsControl(r) {
			return "password must not contain control characters"
		}
	}
	lower := strings.ToLower(password)
	if u := strings.ToLower(strings.TrimSpace(username)); len(u) >= 3 && strings.Contains(lower, u) {
		return "password must not contain the username"
	}
	var b strings.Builder
	for _, r := range lower {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	core := b.String()
	if core == "" {
		core = lower // symbols only: still check for repetition
	}
	if isRepetitive(core) || isSequential(core) {
		return "password is too predictable (repeated or sequential characters)"
	}
	if words := strings.TrimFunc(core, unicode.IsDigit); words != "" && isCommonWords(words) {
		return "password is too common; avoid words like \"password\", \"admin\" or the product name on their own"
	}
	return ""
}

// isRepetitive reports whether s is one short unit (up to 4 runes) repeated,
// e.g. "aaaa…" or "abcabc…".
func isRepetitive(s string) bool {
	rs := []rune(s)
	if len(rs) < 2 {
		return len(rs) == 1
	}
	for period := 1; period <= 4 && period < len(rs); period++ {
		ok := true
		for i := period; i < len(rs); i++ {
			if rs[i] != rs[i-period] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// isSequential reports whether s steps by +1 or -1 throughout ("123456…",
// "abcdef…", "zyxw…"), wrapping 9→0 for digit runs.
func isSequential(s string) bool {
	rs := []rune(s)
	if len(rs) < 3 {
		return false
	}
	for _, step := range []rune{1, -1} {
		ok := true
		for i := 1; i < len(rs); i++ {
			d := rs[i] - rs[i-1]
			wrap := unicode.IsDigit(rs[i]) && unicode.IsDigit(rs[i-1]) && (d == -9*step)
			if d != step && !wrap {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// isCommonWords reports whether s (lower-case letters and digits) is made
// only of weakPasswordWords, each optionally followed by digits.
func isCommonWords(s string) bool {
	if s == "" {
		return true
	}
	memo := map[int]bool{}
	var from func(i int) bool
	from = func(i int) bool {
		if i == len(s) {
			return true
		}
		if v, ok := memo[i]; ok {
			return v
		}
		res := false
		for _, w := range weakPasswordWords {
			if !strings.HasPrefix(s[i:], w) {
				continue
			}
			j := i + len(w)
			for {
				if from(j) {
					res = true
					break
				}
				if j < len(s) && s[j] >= '0' && s[j] <= '9' {
					j++
					continue
				}
				break
			}
			if res {
				break
			}
		}
		memo[i] = res
		return res
	}
	return from(0)
}
