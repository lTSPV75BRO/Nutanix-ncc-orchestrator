package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"goncc/pkg/cmd"
)

func main() {
	// Set version information from build info
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if cmd.Version == "" {
					cmd.Version = s.Value
				}
			case "vcs.time":
				if cmd.BuildDate == "" {
					cmd.BuildDate = s.Value
				}
			}
		}
		if cmd.GoVersion == "" {
			cmd.GoVersion = bi.GoVersion
		}
	}

	// Set defaults
	if cmd.Version == "" {
		cmd.Version = "unknown"
	}
	if cmd.BuildDate == "" {
		cmd.BuildDate = "unknown"
	}
	if cmd.Stream == "" {
		cmd.Stream = "dev"
	}

	if err := cmd.NewRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
