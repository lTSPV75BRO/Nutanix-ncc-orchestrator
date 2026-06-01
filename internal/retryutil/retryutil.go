// Package retryutil holds the small, dependency-free retry/backoff helpers
// shared by the NCC HTTP client and the notification senders. It is a leaf
// package (stdlib-only) so both package main and goncc/internal/notify can
// depend on it without an import cycle.
package retryutil

import (
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// JitteredBackoff returns a randomized backoff for the given attempt (1-based)
// using exponential growth capped at maxDelay, with full jitter.
func JitteredBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	capDelay := time.Duration(exp)
	if capDelay > maxDelay {
		capDelay = maxDelay
	}
	if capDelay <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(capDelay)))
}

// IsRetryableStatus reports whether an HTTP status code is worth retrying.
func IsRetryableStatus(code int) bool {
	switch code {
	case 408, 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

// RetryAfterDelay parses a Retry-After header (delta-seconds or HTTP-date) and
// returns the delay and whether the header was present and parseable.
func RetryAfterDelay(resp *http.Response) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(ra); err == nil {
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(ra); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
