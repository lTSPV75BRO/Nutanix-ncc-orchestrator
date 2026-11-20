package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWatchFlagsRepeatedDenialsOncePerBurst(t *testing.T) {
	var w clientWatch
	now := time.Now()
	flagged := 0
	for i := 0; i < 30; i++ {
		if len(w.observe("api", "10.0.0.9", "", 403, now.Add(time.Duration(i)*time.Second))) > 0 {
			flagged++
		}
	}
	if flagged != 1 {
		t.Fatalf("expected one flag for the burst, got %d", flagged)
	}
	if got := w.observe("api", "10.0.0.8", "", 403, now); len(got) != 0 {
		t.Fatalf("another address must not inherit the count: %v", got)
	}
}

func TestWatchFlagsTokenUsedFromManyAddresses(t *testing.T) {
	var w clientWatch
	now := time.Now()
	var got []string
	for i, ip := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"} {
		got = w.observe("custom", ip, "tok", 200, now.Add(time.Duration(i)*time.Second))
	}
	if len(got) != 1 || got[0] != "token_many_sources" {
		t.Fatalf("got %v", got)
	}
}

func TestWatchIgnoresNormalTraffic(t *testing.T) {
	var w clientWatch
	now := time.Now()
	for i := 0; i < 200; i++ {
		if r := w.observe("api", "10.0.0.1", "tok", 200, now.Add(time.Duration(i)*time.Second)); len(r) > 0 {
			t.Fatalf("unexpected flag: %v", r)
		}
	}
}

func TestCustomClientIsNotTreatedAsOfficialUI(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
	r.Header.Set("X-Requested-With", "ncc-ui")
	if !isNCCUIRequest(r) {
		t.Fatal("legacy dashboard marker should still be treated as the dashboard")
	}
	r.Header.Set("X-NCC-Client", "Acme Console/1.2")
	if isNCCUIRequest(r) {
		t.Fatal("a self-declared custom client must not be version-gated")
	}
	if requestClientKind(r) != "custom" {
		t.Fatalf("kind = %s", requestClientKind(r))
	}
	if customClientName(r) != "Acme Console/1.2" {
		t.Fatalf("name = %q", customClientName(r))
	}
}

func TestCustomClientNeverBlockedEvenWithBlockedVersion(t *testing.T) {
	s := &apiServer{}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
	r.Header.Set("X-NCC-Client", "grafana-bridge")
	r.Header.Set("X-NCC-UI-Version", "2.2.1")
	w := httptest.NewRecorder()
	if s.rejectCompromisedUIClient(w, r) {
		t.Fatal("custom clients must not be refused")
	}
}
