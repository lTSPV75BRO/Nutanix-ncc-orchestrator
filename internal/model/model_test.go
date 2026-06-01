package model

import "testing"

func TestClusterHealthScore(t *testing.T) {
	cases := []struct {
		name                          string
		fail, warn, errc, total, want int
	}{
		{"no checks is healthy", 0, 0, 0, 0, 100},
		{"all pass", 0, 0, 0, 10, 100},
		{"single fail in ten", 1, 0, 0, 10, 90},
		{"all fail clamps low", 10, 0, 0, 10, 0},
		{"warns penalize less than fails", 0, 10, 0, 10, 70},
		{"errs weighted between", 0, 0, 10, 10, 20},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClusterHealthScore(c.fail, c.warn, c.errc, c.total); got != c.want {
				t.Errorf("ClusterHealthScore(%d,%d,%d,%d) = %d, want %d", c.fail, c.warn, c.errc, c.total, got, c.want)
			}
		})
	}
}
