package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"goncc/internal/httpclient"
	"goncc/internal/model"
	"goncc/internal/retryutil"

	"github.com/rs/zerolog/log"
)

// ==================== Retry Helpers ====================
// The shared retry/backoff helpers were extracted to goncc/internal/retryutil
// (so goncc/internal/notify can reuse them without an import cycle). These
// aliases keep the existing call sites in this package unchanged.
var (
	jitteredBackoff   = retryutil.JitteredBackoff
	isRetryableStatus = retryutil.IsRetryableStatus
	retryAfterDelay   = retryutil.RetryAfterDelay
)

// maxRateLimitBackoff caps Retry-After / computed backoff for HTTP 429 so a misbehaving server cannot sleep unbounded.
const maxRateLimitBackoff = 2 * time.Minute

const (
	maxRetryRequestBodyBytes  int64 = 2 << 20  // 2 MiB
	maxRetryResponseBodyBytes int64 = 64 << 20 // 64 MiB
)

var (
	adaptiveCurrentParallel int32
	adaptiveMaxParallel     int32
	adaptiveLastDecrease    int64 // unix nanos of the last back-off
	adaptiveNotifyMu        sync.Mutex
	adaptiveNotifyCh        chan struct{}
)

func setAdaptiveParallelismNotify(ch chan struct{}) {
	adaptiveNotifyMu.Lock()
	defer adaptiveNotifyMu.Unlock()
	adaptiveNotifyCh = ch
}

func notifyAdaptiveParallelismChanged() {
	adaptiveNotifyMu.Lock()
	ch := adaptiveNotifyCh
	adaptiveNotifyMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func resetAdaptiveParallelism(maxParallel int) {
	if maxParallel < 1 {
		maxParallel = 1
	}
	atomic.StoreInt32(&adaptiveMaxParallel, int32(maxParallel))
	atomic.StoreInt32(&adaptiveCurrentParallel, int32(maxParallel))
}

func currentAdaptiveParallel(maxParallel int) int {
	cur := int(atomic.LoadInt32(&adaptiveCurrentParallel))
	if cur < 1 {
		return 1
	}
	if cur > maxParallel {
		return maxParallel
	}
	return cur
}

func noteHTTPStatusForAdaptiveParallelism(status int, cfg Config) {
	if !cfg.AdaptiveParallelism {
		return
	}
	maxP := int(atomic.LoadInt32(&adaptiveMaxParallel))
	if maxP <= 1 {
		return
	}
	switch status {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		// Back off multiplicatively (to 3/4), at most once per cooldown so a
		// burst of rejections from in-flight requests counts once. Taking one
		// slot off per 429 barely registered at hundreds of clusters in flight.
		now := time.Now().UnixNano()
		last := atomic.LoadInt64(&adaptiveLastDecrease)
		if now-last < int64(adaptiveDecreaseCooldown) || !atomic.CompareAndSwapInt64(&adaptiveLastDecrease, last, now) {
			return
		}
		for {
			cur := atomic.LoadInt32(&adaptiveCurrentParallel)
			if cur <= 1 {
				return
			}
			next := cur * 3 / 4
			if next >= cur {
				next = cur - 1
			}
			if next < 1 {
				next = 1
			}
			if atomic.CompareAndSwapInt32(&adaptiveCurrentParallel, cur, next) {
				log.Warn().Int("status", status).Int("adaptive_parallel", int(next)).Int("max_parallel", maxP).Msg("adaptive parallelism reduced after the server pushed back")
				notifyAdaptiveParallelismChanged()
				return
			}
		}
	default:
		if status >= 200 && status < 300 {
			for {
				cur := atomic.LoadInt32(&adaptiveCurrentParallel)
				max := atomic.LoadInt32(&adaptiveMaxParallel)
				if cur >= max {
					return
				}
				next := cur + 1
				if atomic.CompareAndSwapInt32(&adaptiveCurrentParallel, cur, next) {
					notifyAdaptiveParallelismChanged()
					return
				}
			}
		}
	}
}

func capRateLimitWait(d time.Duration) time.Duration {
	if d > maxRateLimitBackoff {
		return maxRateLimitBackoff
	}
	return d
}

func logRateLimitHeaders(op string, resp *http.Response) {
	if resp == nil {
		return
	}
	rem := resp.Header.Get("X-RateLimit-Remaining")
	reset := resp.Header.Get("X-RateLimit-Reset")
	apiRem := resp.Header.Get("X-Api-Ratelimit-Remaining")
	if rem == "" && reset == "" && apiRem == "" {
		return
	}
	log.Warn().Str("op", op).Str("X-RateLimit-Remaining", rem).Str("X-RateLimit-Reset", reset).
		Str("X-Api-Ratelimit-Remaining", apiRem).Msg("rate limit headers")
}

func readBodyWithLimit(body io.ReadCloser, maxBytes int64, context string) ([]byte, error) {
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%s exceeds max size (%d bytes)", context, maxBytes)
	}
	return data, nil
}

// ==================== HTTP Client and File System ====================
// The HTTP client builder (connection pooling, TLS policy, redacted logging
// transport) was extracted to goncc/internal/httpclient. OSFS (the os-backed
// FS implementation) now lives in goncc/internal/model next to the FS
// interface. These aliases keep the existing call sites in this package
// unchanged.
var NewHTTPClient = httpclient.New

type OSFS = model.OSFS

// ==================== Retryable HTTP Wrappers ====================

func doWithRetry(ctx context.Context, client HTTPClient, req *http.Request, cfg Config, op string) (*http.Response, []byte, error) {
	attempts := cfg.RetryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	breaker := cfg.RetryCircuitBreaker
	if breaker <= 0 {
		breaker = defaultRetryCircuitBreaker
	}
	var lastErr error
	var resp *http.Response
	var body []byte
	consecutiveRetryableFailures := 0

	// Snapshot original body if present
	var origBody []byte
	var hasBody bool
	if req.Body != nil {
		b, err := readBodyWithLimit(req.Body, maxRetryRequestBodyBytes, "request body")
		if err != nil {
			return nil, nil, err
		}
		origBody = b
		hasBody = true
		req.Body = io.NopCloser(bytes.NewReader(origBody))
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		reqClone := req.Clone(reqCtx)
		if hasBody {
			reqClone.Body = io.NopCloser(bytes.NewReader(origBody))
		}

		resp, lastErr = client.Do(reqClone)
		if lastErr != nil {
			consecutiveRetryableFailures++
			cancel()
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if consecutiveRetryableFailures >= breaker {
				return nil, nil, fmt.Errorf("%s retry circuit breaker opened after %d consecutive transport failures: %w", op, consecutiveRetryableFailures, lastErr)
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("transport error, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(back):
				}
				continue
			}
			return nil, nil, lastErr
		}

		func() {
			defer cancel()
			var err error
			body, err = readBodyWithLimit(resp.Body, maxRetryResponseBodyBytes, "response body")
			if err != nil {
				lastErr = err
			} else {
				lastErr = nil
			}
		}()
		if lastErr != nil {
			consecutiveRetryableFailures++
			if consecutiveRetryableFailures >= breaker {
				return resp, nil, fmt.Errorf("%s retry circuit breaker opened after %d consecutive body read failures: %w", op, consecutiveRetryableFailures, lastErr)
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("read body failed, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-time.After(back):
				}
				continue
			}
			return resp, nil, lastErr
		}

		status := resp.StatusCode
		if status >= 200 && status < 300 {
			noteHTTPStatusForAdaptiveParallelism(status, cfg)
			log.Debug().Str("op", op).Int("status", status).Msg("request succeeded")
			return resp, body, nil
		}
		noteHTTPStatusForAdaptiveParallelism(status, cfg)

		retryable := isRetryableStatus(status)
		var back time.Duration
		if status == 429 {
			logRateLimitHeaders(op, resp)
			if ra, ok := retryAfterDelay(resp); ok {
				back = capRateLimitWait(ra)
				if ra > maxRateLimitBackoff {
					log.Warn().Str("op", op).Dur("requested", ra).Dur("capped", back).Msg("rate limit wait capped")
				}
			}
		}
		if back == 0 {
			back = jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		}

		if retryable && attempt < attempts {
			consecutiveRetryableFailures++
			if consecutiveRetryableFailures >= breaker {
				return resp, body, fmt.Errorf("%s retry circuit breaker opened after %d consecutive retryable failures (last HTTP %d)", op, consecutiveRetryableFailures, status)
			}
			log.Warn().Str("op", op).Int("attempt", attempt).Int("status", status).Dur("backoff", back).Msg("retryable status, retrying")
			select {
			case <-ctx.Done():
				return resp, body, ctx.Err()
			case <-time.After(back):
			}
			continue
		}

		log.Error().Str("op", op).Int("status", status).Int("attempts", attempt).Msg("request failed, not retrying")
		return resp, body, fmt.Errorf("%s HTTP %d", op, status)
	}

	if lastErr != nil {
		return nil, nil, lastErr
	}
	return resp, body, fmt.Errorf("%s exhausted retries", op)
}
