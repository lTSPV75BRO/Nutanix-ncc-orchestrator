package notify

import (
	"crypto/tls"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"strings"
	"time"
)

// SMTPTimeout bounds one complete SMTP exchange (dial, EHLO, STARTTLS, AUTH,
// DATA). net/smtp has no timeouts of its own, so a relay that accepted the
// TCP connection and then stalled used to hang the sender indefinitely: the
// api-server's test-notification request never returned, and a scheduled
// run never finished its notification stage.
const SMTPTimeout = 30 * time.Second

// SMTPMessage is one outgoing mail for SendMail.
type SMTPMessage struct {
	Addr string    // host:port
	Auth smtp.Auth // nil = no AUTH
	From string
	To   []string
	Body []byte // full RFC 5322 message (headers + body)
	// TLS overrides the STARTTLS config (default: verify against the host).
	TLS *tls.Config
	// RequireTLS fails instead of sending in plaintext when the server does
	// not offer STARTTLS.
	RequireTLS bool
	// Timeout for the whole exchange; <= 0 means SMTPTimeout.
	Timeout time.Duration
}

// SendMail delivers msg like smtp.SendMail (opportunistic STARTTLS, AUTH only
// when the server offers it and Auth is set) but under a single deadline.
func SendMail(msg SMTPMessage) error {
	timeout := msg.Timeout
	if timeout <= 0 {
		timeout = SMTPTimeout
	}
	if err := validateSMTPLine(msg.From); err != nil {
		return err
	}
	for _, r := range msg.To {
		if err := validateSMTPLine(r); err != nil {
			return err
		}
	}
	host, _, err := net.SplitHostPort(msg.Addr)
	if err != nil {
		return fmt.Errorf("smtp: invalid address %q: %w", msg.Addr, err)
	}
	conn, err := net.DialTimeout("tcp", msg.Addr, timeout)
	if err != nil {
		return err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return err
	}
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer c.Close()
	if ok, _ := c.Extension("STARTTLS"); ok {
		cfg := msg.TLS
		if cfg == nil {
			cfg = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		}
		if err := c.StartTLS(cfg); err != nil {
			return err
		}
	} else if msg.RequireTLS {
		return errors.New("smtp: server does not offer STARTTLS and TLS is required")
	}
	if msg.Auth != nil {
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("smtp: server doesn't support AUTH")
		}
		if err := c.Auth(msg.Auth); err != nil {
			return err
		}
	}
	if err := c.Mail(msg.From); err != nil {
		return err
	}
	for _, r := range msg.To {
		if err := c.Rcpt(r); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(msg.Body); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

func validateSMTPLine(s string) error {
	if strings.ContainsAny(s, "\r\n") {
		return errors.New("smtp: a line must not contain CR or LF")
	}
	return nil
}

// HeaderValue makes s safe for a single header line: CR/LF are flattened so
// run or cluster data cannot inject headers, and non-ASCII text is
// RFC 2047-encoded.
func HeaderValue(s string) string {
	s = strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(s)
	return mime.QEncoding.Encode("utf-8", s)
}
