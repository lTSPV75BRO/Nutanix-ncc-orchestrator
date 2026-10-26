// Package graceful runs an http.Server until SIGINT/SIGTERM and then drains
// it. Without this the api/ui servers exited on SIGTERM (Kubernetes rolling
// update, systemd stop, v2-stop) mid-request: in-flight responses were cut,
// so a save, alert acknowledgement or backup could succeed server-side while
// the client saw a network error.
package graceful

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// DefaultGrace bounds how long in-flight requests may finish after a stop
// signal. It stays below the Kubernetes default terminationGracePeriodSeconds
// (30s); callers that stop the process themselves (v2-stop, the supervisor)
// escalate to a kill after their own grace, which still applies.
const DefaultGrace = 20 * time.Second

// Serve calls serve (e.g. srv.ListenAndServe) and returns its error. When the
// process receives SIGINT or SIGTERM first, it stops accepting connections,
// waits up to grace for active requests, and returns nil once drained
// (http.ErrServerClosed from serve is not an error here).
func Serve(srv *http.Server, serve func() error, grace time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveUntil(ctx, srv, serve, grace)
}

func serveUntil(ctx context.Context, srv *http.Server, serve func() error, grace time.Duration) error {
	errCh := make(chan error, 1)
	go func() { errCh <- serve() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	if grace <= 0 {
		grace = DefaultGrace
	}
	log.Printf("stop signal received; draining in-flight requests (up to %s)", grace)
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	shutdownErr := srv.Shutdown(sctx)
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if shutdownErr != nil {
		// Grace expired with requests still running; close them.
		_ = srv.Close()
		return shutdownErr
	}
	return nil
}
