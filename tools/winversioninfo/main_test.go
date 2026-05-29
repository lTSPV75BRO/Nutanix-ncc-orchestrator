package main

import "testing"

// TestSplitVersion pins the version-string parser used to populate
// the Windows VERSIONINFO FixedFileInfo (Major.Minor.Patch.Build).
// Build defaults to 0; suffixes like "-rc1" / "+abc1234" are
// stripped because Windows resource compilers reject non-numeric
// version components.
func TestSplitVersion(t *testing.T) {
	cases := []struct {
		in        string
		wantMaj   int
		wantMin   int
		wantPatch int
	}{
		{"2.0.2", 2, 0, 2},
		{"v2.0.2", 2, 0, 2},
		{"2.0.2-rc1", 2, 0, 2},
		{"2.0.2+abc1234", 2, 0, 2},
		{"2.0", 2, 0, 0},
		{"2", 2, 0, 0},
		{"", 0, 0, 0},
		{"junk", 0, 0, 0},
		{"v9.10.11-dirty", 9, 10, 11},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			maj, min, patch := splitVersion(tc.in)
			if maj != tc.wantMaj || min != tc.wantMin || patch != tc.wantPatch {
				t.Fatalf("splitVersion(%q) = %d.%d.%d, want %d.%d.%d",
					tc.in, maj, min, patch, tc.wantMaj, tc.wantMin, tc.wantPatch)
			}
		})
	}
}
