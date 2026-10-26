package graceful

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// An in-flight request must complete when the stop signal arrives mid-request.
func TestServeUntilDrainsInFlightRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	})}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serveUntil(ctx, srv, func() error { return srv.Serve(ln) }, 5*time.Second) }()

	type result struct {
		body string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			resCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		resCh <- result{body: string(b), err: err}
	}()
	<-started
	cancel() // the "SIGTERM"
	res := <-resCh
	if res.err != nil || res.body != "done" {
		t.Fatalf("in-flight request was not drained: body=%q err=%v", res.body, res.err)
	}
	if err := <-served; err != nil {
		t.Fatalf("serveUntil after drain: %v", err)
	}
	if _, err := http.Get("http://" + ln.Addr().String()); err == nil {
		t.Fatal("server should stop accepting connections after shutdown")
	}
}

func TestServeUntilReturnsServeError(t *testing.T) {
	want := errors.New("bind failed")
	err := serveUntil(context.Background(), &http.Server{}, func() error { return want }, time.Second)
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}
