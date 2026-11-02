package main

import "testing"

func TestKubernetesCron(t *testing.T) {
	tests := []struct {
		every string
		want  string
	}{
		{"15m", "*/15 * * * *"},
		{"1h", "0 */1 * * *"},
		{"4h", "0 */4 * * *"},
		{"24h", "0 0 * * *"},
		{"2d", "0 0 */2 * *"},
	}
	for _, tt := range tests {
		got, err := kubernetesCron(tt.every)
		if err != nil {
			t.Errorf("kubernetesCron(%q): %v", tt.every, err)
		} else if got != tt.want {
			t.Errorf("kubernetesCron(%q) = %q, want %q", tt.every, got, tt.want)
		}
	}
}

func TestKubernetesCronRejectsUnsupportedIntervals(t *testing.T) {
	for _, every := range []string{"0m", "60m", "25h", "32d", "weekly"} {
		if _, err := kubernetesCron(every); err == nil {
			t.Errorf("kubernetesCron(%q) unexpectedly succeeded", every)
		}
	}
}
