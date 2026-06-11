package main

import (
	"testing"
	"time"
)

func TestCertExpiryStatus(t *testing.T) {
	cases := []struct {
		name      string
		remaining time.Duration
		want      diagStatus
	}{
		{"expired", -time.Hour, diagFail},
		{"expires-now", 0, diagFail},
		{"expiring-soon", 10 * 24 * time.Hour, diagWarn},
		{"just-under-30d", 29 * 24 * time.Hour, diagWarn},
		{"healthy", 90 * 24 * time.Hour, diagOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := certExpiryStatus(c.remaining); got != c.want {
				t.Errorf("certExpiryStatus(%s) = %q, want %q", c.remaining, got, c.want)
			}
		})
	}
}

func TestHumanShortDur(t *testing.T) {
	cases := map[time.Duration]string{
		30 * time.Second:    "30s",
		5 * time.Minute:     "5m",
		90 * time.Minute:    "1.5h",
		36 * time.Hour:      "1.5d",
		-2 * 24 * time.Hour: "2.0d", // negative normalized
	}
	for d, want := range cases {
		if got := humanShortDur(d); got != want {
			t.Errorf("humanShortDur(%s) = %q, want %q", d, got, want)
		}
	}
}
