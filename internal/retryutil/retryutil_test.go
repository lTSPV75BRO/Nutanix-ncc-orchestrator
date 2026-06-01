package retryutil

import (
	"net/http"
	"testing"
	"time"
)

func TestJitteredBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := time.Second
	for attempt := 1; attempt <= 6; attempt++ {
		for i := 0; i < 50; i++ {
			d := JitteredBackoff(base, maxDelay, attempt)
			if d < 0 || d > maxDelay {
				t.Fatalf("attempt %d: backoff %v out of [0,%v]", attempt, d, maxDelay)
			}
		}
	}
	if got := JitteredBackoff(0, 0, 1); got != 0 {
		t.Errorf("zero base/max should yield 0, got %v", got)
	}
}

func TestIsRetryableStatus(t *testing.T) {
	for _, c := range []int{408, 429, 500, 502, 503, 504} {
		if !IsRetryableStatus(c) {
			t.Errorf("%d should be retryable", c)
		}
	}
	for _, c := range []int{200, 301, 400, 401, 403, 404, 501} {
		if IsRetryableStatus(c) {
			t.Errorf("%d should not be retryable", c)
		}
	}
}

func TestRetryAfterDelay(t *testing.T) {
	if _, ok := RetryAfterDelay(nil); ok {
		t.Error("nil response should report ok=false")
	}
	resp := &http.Response{Header: http.Header{}}
	if _, ok := RetryAfterDelay(resp); ok {
		t.Error("absent header should report ok=false")
	}
	resp.Header.Set("Retry-After", "30")
	if d, ok := RetryAfterDelay(resp); !ok || d != 30*time.Second {
		t.Errorf("delta-seconds: got %v ok=%v, want 30s true", d, ok)
	}
	resp.Header.Set("Retry-After", "Mon, 02 Jan 2006 15:04:05 GMT") // past date -> 0
	if d, ok := RetryAfterDelay(resp); !ok || d != 0 {
		t.Errorf("past HTTP-date: got %v ok=%v, want 0 true", d, ok)
	}
	resp.Header.Set("Retry-After", "not-a-number")
	if _, ok := RetryAfterDelay(resp); ok {
		t.Error("garbage header should report ok=false")
	}
}
