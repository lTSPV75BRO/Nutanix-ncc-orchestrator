package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

// auto and automation-level used to be read from the command line only, and
// config.yaml rejected them as unknown keys.
func TestAutomationSettingsFromConfigFile(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		auto    bool
		level   string
		wantErr string
	}{
		{name: "flat", yaml: "auto: true\nautomation-level: full-auto\n", auto: true, level: "full-auto"},
		{name: "nested", yaml: "schema-version: 1\nrunner:\n  automation:\n    enabled: true\n    level: advisory\n", auto: true, level: "advisory"},
		{name: "bad level", yaml: "automation-level: yolo\n", wantErr: "invalid automation-level"},
		{name: "bad nested level", yaml: "schema-version: 1\nrunner:\n  automation:\n    level: yolo\n", wantErr: "invalid automation-level"},
		{name: "bad bool", yaml: "auto: maybe\n", wantErr: "auto: invalid boolean"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			defer viper.Reset()
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			viper.SetConfigFile(path)
			if err := viper.ReadInConfig(); err != nil {
				t.Fatal(err)
			}
			err := validateConfigFileRawTypes()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			normalizeCanonicalConfig()
			if got := viper.GetBool("auto"); got != tc.auto {
				t.Fatalf("auto = %v, want %v", got, tc.auto)
			}
			if got := viper.GetString("automation-level"); got != tc.level {
				t.Fatalf("automation-level = %q, want %q", got, tc.level)
			}
		})
	}
}
