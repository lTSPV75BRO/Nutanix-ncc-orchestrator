package uiversion

import "testing"

func TestSanitize(t *testing.T) {
	if got := Sanitize(" v2.2.1 "); got != "2.2.1" {
		t.Fatalf("got %q", got)
	}
	if got := Sanitize("2.2.1<script>"); !stringsHasOnlyVersion(got) {
		t.Fatalf("unsafe %q", got)
	}
}

func stringsHasOnlyVersion(s string) bool {
	for _, r := range s {
		if r == '<' || r == '>' {
			return false
		}
	}
	return s != ""
}

func TestMatchBuiltin221(t *testing.T) {
	list := Builtin()
	if _, ok := Match("2.2.1", "ui", list); !ok {
		t.Fatal("2.2.1 ui must be blocked")
	}
	if _, ok := Match("v2.2.1", "ui-server", list); !ok {
		t.Fatal("v2.2.1 ui-server must be blocked")
	}
	if _, ok := Match("2.4.0", "ui", list); ok {
		t.Fatal("2.4.0 must not be builtin-blocked")
	}
}
