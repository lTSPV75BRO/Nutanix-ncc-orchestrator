package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"time"

	"goncc/pkg/config"
	"goncc/pkg/errors"
	"goncc/pkg/types"

	"github.com/rs/zerolog/log"
)

// LoggingTransport wraps an HTTP transport with logging capabilities
type LoggingTransport struct {
	Base    http.RoundTripper
	MaxBody int // bytes; 0 = unlimited
}

// RoundTrip implements the http.RoundTripper interface
func (t *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if d, err := httputil.DumpRequestOut(req, true); err == nil {
		dump := d
		if t.MaxBody > 0 && len(dump) > t.MaxBody {
			dump = append(dump[:t.MaxBody], []byte("...[truncated]")...)
		}
		log.Debug().
			Str("method", req.Method).
			Str("url", req.URL.String()).
			RawJSON("request_dump", dump).
			Msg("http request")
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		log.Error().Err(err).Str("url", req.URL.String()).Msg("http roundtrip error")
		return nil, err
	}
	if resp != nil {
		if d, err := httputil.DumpResponse(resp, true); err == nil {
			dump := d
			if t.MaxBody > 0 && len(dump) > t.MaxBody {
				dump = append(dump[:t.MaxBody], []byte("...[truncated]")...)
			}
			log.Debug().
				Int("status", resp.StatusCode).
				RawJSON("response_dump", dump).
				Msg("http response")
		}
	}
	return resp, nil
}

// NewHTTPClient creates a new HTTP client with the given configuration
func NewHTTPClient(cfg *config.Config) *http.Client {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify,
			MinVersion:         cfg.TLSMinVersion,
		},
		IdleConnTimeout: 90 * time.Second,
		MaxIdleConns:    100,
	}
	rt := http.RoundTripper(tr)
	if cfg.LogHTTP || os.Getenv("LOG_HTTP") == "1" {
		rt = &LoggingTransport{Base: tr, MaxBody: 64 * 1024}
	}
	return &http.Client{
		Timeout:   cfg.Timeout, // overall guard
		Transport: rt,
	}
}

// jitteredBackoff calculates a jittered backoff delay
func jitteredBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
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

// retryAfterDelay extracts retry delay from HTTP response headers
func retryAfterDelay(resp *http.Response) (time.Duration, bool) {
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

// DoWithRetry performs an HTTP request with retry logic
func DoWithRetry(ctx context.Context, client types.HTTPClient, req *http.Request, cfg *config.Config, op string) (*http.Response, []byte, error) {
	attempts := cfg.RetryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	var resp *http.Response
	var body []byte

	// Snapshot original body if present
	var origBody []byte
	var hasBody bool
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, nil, errors.Wrap(err, errors.ErrorTypeNetwork, "failed to read request body")
		}
		_ = req.Body.Close()
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
			cancel()
			if ctx.Err() != nil {
				return nil, nil, errors.Wrap(ctx.Err(), errors.ErrorTypeTimeout, "context cancelled")
			}
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("transport error, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, errors.Wrap(ctx.Err(), errors.ErrorTypeTimeout, "context cancelled during retry")
				case <-time.After(back):
				}
				continue
			}
			return nil, nil, errors.Wrap(lastErr, errors.ErrorTypeNetwork, "request failed after retries")
		}

		func() {
			defer cancel()
			defer resp.Body.Close()
			var err error
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				lastErr = err
			} else {
				lastErr = nil
			}
		}()
		if lastErr != nil {
			if attempt < attempts {
				back := jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
				log.Warn().Str("op", op).Int("attempt", attempt).Err(lastErr).Dur("backoff", back).Msg("read body failed, retrying")
				select {
				case <-ctx.Done():
					return nil, nil, errors.Wrap(ctx.Err(), errors.ErrorTypeTimeout, "context cancelled during retry")
				case <-time.After(back):
				}
				continue
			}
			return resp, nil, errors.Wrap(lastErr, errors.ErrorTypeNetwork, "failed to read response body")
		}

		status := resp.StatusCode
		if status >= 200 && status < 300 {
			log.Debug().Str("op", op).Int("status", status).Msg("request succeeded")
			return resp, body, nil
		}

		retryable := errors.IsRetryableStatus(status)
		var back time.Duration
		if status == 429 {
			if ra, ok := retryAfterDelay(resp); ok {
				back = ra
			}
		}
		if back == 0 {
			back = jitteredBackoff(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)
		}

		if retryable && attempt < attempts {
			log.Warn().Str("op", op).Int("attempt", attempt).Int("status", status).Dur("backoff", back).Msg("retryable status, retrying")
			select {
			case <-ctx.Done():
				return resp, body, errors.Wrap(ctx.Err(), errors.ErrorTypeTimeout, "context cancelled during retry")
			case <-time.After(back):
			}
			continue
		}

		log.Error().Str("op", op).Int("status", status).Int("attempts", attempt).Msg("request failed, not retrying")
		return resp, body, errors.NewHTTPError(status, req.URL.String(), fmt.Sprintf("%s HTTP %d", op, status))
	}

	if lastErr != nil {
		return nil, nil, errors.Wrap(lastErr, errors.ErrorTypeNetwork, "request failed")
	}
	return resp, body, errors.NetworkErrorf("%s exhausted retries", op)
}
