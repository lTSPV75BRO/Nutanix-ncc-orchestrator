package plural

import "testing"

func TestCount(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{Count(1, "cluster"), "1 cluster"},
		{Count(0, "cluster"), "0 clusters"},
		{Count(3, "backup"), "3 backups"},
		{Count(2, "is", "are"), "2 are"},
		{Word(1, "setting"), "setting"},
		{Word(5, "setting"), "settings"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}
