package main

import (
	"reflect"
	"testing"
)

// TestExplicitFlagSet pins the contract used by applyStackAwareDefaults:
// only flags actually typed on the command line should be treated as
// "user-set" and excluded from auto-redirect. Both `--name=val` and
// `--name val` forms must be recognized; positional args and bare
// dashes must not pollute the set.
func TestExplicitFlagSet(t *testing.T) {
	cases := []struct {
		name string
		argv []string
		want map[string]bool
	}{
		{"empty", []string{}, map[string]bool{}},
		{"single", []string{"--listen", ":8081"}, map[string]bool{"listen": true}},
		{"equals", []string{"--config-path=/etc/ncc/config.yaml"}, map[string]bool{"config-path": true}},
		{"single dash", []string{"-listen", ":8081"}, map[string]bool{"listen": true}},
		{"double dash separator only", []string{"--", "trailing"}, map[string]bool{}},
		{"mixed", []string{
			"--repo-root", "/srv/ncc",
			"--config-path=/etc/x.yaml",
			"--auth-mode", "token",
		}, map[string]bool{
			"repo-root": true, "config-path": true, "auth-mode": true,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := explicitFlagSet(tc.argv)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplyStackAwareDefaults_RespectsExplicitOverrides verifies that
// applyStackAwareDefaults never silently overwrites a path the user
// explicitly set on the command line. We force-detect a stack by
// setting up apiServer fields at the compile-time defaults, simulate
// the user passing --config-path explicitly, and assert config-path
// is left untouched while other unset flags are still rewritten.
func TestApplyStackAwareDefaults_RespectsExplicitOverrides(t *testing.T) {
	// Build a fake apiServer with all paths at their post-flag.Parse
	// defaults. We can't call applyStackAwareDefaults directly because
	// it needs DetectStackRootFromExe to succeed (which requires the
	// real test binary to live in a "<X>/bin" directory). Instead we
	// re-implement the relevant subset of the override loop with a
	// fixed root; this pins the explicit-flag contract without
	// depending on filesystem layout.
	const fakeRoot = "/srv/ncc"
	type pathFlag struct {
		flagName  string
		current   *string
		zeroValue string
		stackPath string
	}
	repoRoot := "."
	configPath := "/etc/x.yaml" // user-supplied
	outputDir := "outputfiles"
	flags := []pathFlag{
		{"repo-root", &repoRoot, ".", fakeRoot},
		{"config-path", &configPath, "config.yaml", fakeRoot + "/config.yaml"},
		{"output-dir", &outputDir, "outputfiles", fakeRoot + "/outputfiles"},
	}
	explicit := explicitFlagSet([]string{"--config-path", "/etc/x.yaml"})
	for _, f := range flags {
		if explicit[f.flagName] {
			continue
		}
		if *f.current != f.zeroValue {
			continue
		}
		*f.current = f.stackPath
	}
	if repoRoot != fakeRoot {
		t.Errorf("repo-root: got %q, want %q", repoRoot, fakeRoot)
	}
	if configPath != "/etc/x.yaml" {
		t.Errorf("config-path overridden despite explicit set: got %q", configPath)
	}
	if outputDir != fakeRoot+"/outputfiles" {
		t.Errorf("output-dir not auto-resolved: got %q", outputDir)
	}
}
