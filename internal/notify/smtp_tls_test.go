package notify

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// SMTPS (port 465) starts TLS before the greeting. A plain dial used to wait
// for a 220 that never arrives until the deadline, so mail through relays such
// as smtp.gmail.com:465 always failed with a timeout.
func TestSendMailImplicitTLS(t *testing.T) {
	certSrv := httptest.NewUnstartedServer(nil)
	certSrv.StartTLS()
	defer certSrv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(certSrv.Certificate())

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := tls.NewListener(raw, &tls.Config{Certificates: certSrv.TLS.Certificates, MinVersion: tls.VersionTLS12})
	addr, got := fakeSMTPOn(t, ln)

	err = SendMail(SMTPMessage{
		Addr: addr, From: "a@example.com", To: []string{"b@example.com"},
		Body:        []byte("Subject: hi\r\n\r\nover tls"),
		TLS:         &tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS12},
		ImplicitTLS: true, RequireTLS: true, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("SendMail over implicit TLS: %v", err)
	}
	select {
	case data := <-got:
		if !strings.Contains(data, "over tls") {
			t.Fatalf("unexpected DATA: %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the message")
	}
}
