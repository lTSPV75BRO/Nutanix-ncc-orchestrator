package notify

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"goncc/internal/model"
)

// fakeSMTP is a minimal plaintext SMTP server (no STARTTLS, no AUTH) that
// records the DATA section of one message.
func fakeSMTP(t *testing.T) (addr string, got <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	out := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		r := bufio.NewReader(conn)
		w := func(s string) { _, _ = conn.Write([]byte(s + "\r\n")) }
		w("220 fake ESMTP")
		var data strings.Builder
		inData := false
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if inData {
				if line == ".\r\n" {
					inData = false
					out <- data.String()
					w("250 queued")
					continue
				}
				data.WriteString(line)
				continue
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				w("250 fake")
			case cmd == "DATA":
				inData = true
				w("354 go ahead")
			case cmd == "QUIT":
				w("221 bye")
				return
			default:
				w("250 ok")
			}
		}
	}()
	return ln.Addr().String(), out
}

// Regression: a relay that accepts the TCP connection but never speaks used
// to block net/smtp forever. SendMail must give up within its timeout.
func TestSendMailTimesOutOnStalledServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		time.Sleep(5 * time.Second) // never send the 220 greeting
	}()
	start := time.Now()
	err = SendMail(SMTPMessage{Addr: ln.Addr().String(), From: "a@example.com", To: []string{"b@example.com"},
		Body: []byte("Subject: x\r\n\r\nbody"), Timeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("expected a timeout error from a stalled SMTP server")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("SendMail took %v; the deadline was not applied", elapsed)
	}
}

func TestSendMailRejectsCRLFInEnvelope(t *testing.T) {
	err := SendMail(SMTPMessage{Addr: "127.0.0.1:1", From: "a@example.com\r\nRCPT TO:<x@evil>", To: []string{"b@example.com"}})
	if err == nil || !strings.Contains(err.Error(), "CR or LF") {
		t.Fatalf("want CR/LF rejection before dialing, got %v", err)
	}
}

func TestSendMailRequireTLSWithoutSTARTTLS(t *testing.T) {
	addr, _ := fakeSMTP(t)
	err := SendMail(SMTPMessage{Addr: addr, From: "a@example.com", To: []string{"b@example.com"},
		Body: []byte("Subject: x\r\n\r\nbody"), RequireTLS: true, Timeout: 2 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Fatalf("want a STARTTLS-required error, got %v", err)
	}
}

// Regression: the runner's subject comes from templates over run data; a
// CR/LF in it used to start a new header line (e.g. an injected Bcc). Also
// covers sending through an unauthenticated relay with no SMTP user, which
// failed before because an empty PLAIN AUTH was always attempted.
func TestSendEmailFlattensSubjectAndSkipsAuthWithoutUser(t *testing.T) {
	addr, got := fakeSMTP(t)
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	cfg := model.Config{EmailEnabled: true, SMTPServer: host, SMTPPort: port,
		EmailFrom: "ncc@example.com", EmailTo: []string{"ops@example.com"}}
	if err := SendEmail(cfg, "NCC FAIL on c1\r\nBcc: evil@example.com", "body", ""); err != nil {
		t.Fatalf("SendEmail: %v", err)
	}
	select {
	case data := <-got:
		for _, line := range strings.Split(data, "\r\n") {
			if strings.HasPrefix(strings.ToLower(line), "bcc:") {
				t.Fatalf("injected header reached the message:\n%s", data)
			}
		}
		if !strings.Contains(data, "Subject: NCC FAIL on c1 Bcc: evil@example.com") {
			t.Fatalf("subject not flattened onto one line:\n%s", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake SMTP server received no message")
	}
}
