package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// This file forwards the JSONL audit stream to an external SIEM / log
// collector so security-relevant events (logins, settings changes, run
// triggers, self-heal, backups) are retained off-box for compliance and
// alerting. Two sinks are supported and may be enabled together:
//
//   - HTTP: POST each audit JSON object to a collector endpoint (Splunk HEC,
//     Elastic, Loki gateway, a generic webhook). Optional Authorization header
//     and optional Splunk-HEC event wrapping.
//   - Syslog: send each line as an RFC5424 message over UDP/TCP to a syslog/
//     rsyslog/SIEM relay.
//
// Forwarding is best-effort and fully decoupled from request handling: lines
// are dropped onto a bounded buffer and shipped by a background worker, so a
// slow or down collector never blocks an API request or the audit write. A
// dropped-line counter is exposed on /metrics.

type auditForwarder struct {
	ch         chan []byte
	httpURL    string
	httpAuth   string
	splunk     bool
	syslogAddr string
	syslogNet  string
	client     *http.Client
	hostname   string
	dropped    atomic.Int64
}

// startAuditForwarder builds the forwarder from the configured flags and
// launches its worker. Returns nil when no sink is configured.
func (s *apiServer) startAuditForwarder(ctx context.Context) *auditForwarder {
	httpURL := strings.TrimSpace(s.auditForwardHTTPURL)
	syslogAddr := strings.TrimSpace(s.auditForwardSyslog)
	if httpURL == "" && syslogAddr == "" {
		return nil
	}
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "ncc-orchestrator"
	}
	net := strings.ToLower(strings.TrimSpace(s.auditForwardSyslogNet))
	if net != "tcp" {
		net = "udp"
	}
	f := &auditForwarder{
		ch:         make(chan []byte, 1024),
		httpURL:    httpURL,
		httpAuth:   strings.TrimSpace(s.auditForwardHTTPAuth),
		splunk:     s.auditForwardHTTPSplunk,
		syslogAddr: syslogAddr,
		syslogNet:  net,
		client:     &http.Client{Timeout: 10 * time.Second},
		hostname:   host,
	}
	sinks := []string{}
	if httpURL != "" {
		sinks = append(sinks, "http")
	}
	if syslogAddr != "" {
		sinks = append(sinks, fmt.Sprintf("syslog(%s %s)", net, syslogAddr))
	}
	log.Printf("audit forwarding enabled: %s", strings.Join(sinks, ", "))
	go f.run(ctx)
	return f
}

// forward enqueues a copy of an audit line for asynchronous delivery. It never
// blocks: if the buffer is full (collector down/slow) the line is dropped and
// counted rather than stalling the caller.
func (f *auditForwarder) forward(line []byte) {
	if f == nil {
		return
	}
	cp := make([]byte, len(line))
	copy(cp, line)
	select {
	case f.ch <- cp:
	default:
		f.dropped.Add(1)
	}
}

func (f *auditForwarder) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case line := <-f.ch:
			if f.httpURL != "" {
				f.sendHTTP(ctx, line)
			}
			if f.syslogAddr != "" {
				f.sendSyslog(line)
			}
		}
	}
}

func (f *auditForwarder) sendHTTP(ctx context.Context, line []byte) {
	body := line
	if f.splunk {
		// Splunk HEC expects {"event": <payload>, ...}.
		var ev json.RawMessage = line
		wrapped, err := json.Marshal(map[string]interface{}{"event": ev, "sourcetype": "ncc:audit"})
		if err == nil {
			body = wrapped
		}
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, f.httpURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if f.httpAuth != "" {
		req.Header.Set("Authorization", f.httpAuth)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		f.dropped.Add(1)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		f.dropped.Add(1)
	}
}

// sendSyslog ships one RFC5424 message. UDP is fire-and-forget; TCP frames with
// octet-counting per RFC6587. A failed dial/write increments the drop counter.
func (f *auditForwarder) sendSyslog(line []byte) {
	// Facility local0 (16) * 8 + severity Informational (6) = 134.
	const pri = 134
	ts := time.Now().UTC().Format(time.RFC3339)
	msg := fmt.Sprintf("<%d>1 %s %s ncc-orchestrator - - - %s", pri, ts, f.hostname, bytes.TrimRight(line, "\n"))
	conn, err := net.DialTimeout(f.syslogNet, f.syslogAddr, 5*time.Second)
	if err != nil {
		f.dropped.Add(1)
		return
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	payload := msg
	if f.syslogNet == "tcp" {
		payload = fmt.Sprintf("%d %s", len(msg), msg) // octet-counted framing
	}
	if _, err := conn.Write([]byte(payload)); err != nil {
		f.dropped.Add(1)
	}
}
