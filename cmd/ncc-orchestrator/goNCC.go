package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"goncc/internal/model"
	"goncc/internal/runtimetune"
)

// ==================== Configuration ====================

// The foundational shared types now live in goncc/internal/model so the
// extracted subsystem packages (promtext, notify, parser) can depend on them
// without importing package main. These aliases keep the thousands of
// existing references in this package compiling unchanged.
type (
	Config              = model.Config
	ClusterCredential   = model.ClusterCredential
	NotificationSummary = model.NotificationSummary
	ParsedBlock         = model.ParsedBlock
	FS                  = model.FS
	HTTPClient          = model.HTTPClient
)

type HTMLMeta struct {
	ClusterName    string
	ClusterVersion string
	NCCVersion     string
}

type HTMLData struct {
	Rows    []Row
	Now     string
	Meta    HTMLMeta
	Summary SummaryCounts
}

// SummaryCounts holds per-severity counts for the report header.
type SummaryCounts struct {
	FAIL, WARN, ERR, INFO int
}

const termsText = `
Nutanix NCC Orchestrator - Terms, Support, and Usage Notice

Project:
- Repository: https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator
- Maintainer: Prajwal Vernekar

What this tool does:
- Runs NCC checks across multiple clusters.
- Aggregates and filters results.
- Produces reports and automation artifacts.
- Optionally exposes v2 API and UI services.

Your responsibilities:
- Review and validate configuration before production use.
- Protect credentials, API tokens, and generated artifacts.
- Use secure defaults (TLS verification, least privilege, explicit allowlists).
- Test changes in non-production first, especially update and scheduling workflows.

Update and version policy:
- Use "ncc-orchestrator update --check" to check availability before replacing binaries.
- By default, updates stay on your current major track (v1.x stays on v1.x).
- Major upgrades (for example v1 -> v2) require explicit opt-in via "--allow-major-upgrade".
- Review v1 to v2 migration guidance before major upgrades:
  https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator#upgrading

Support and documentation:
- Start with: ncc-orchestrator --help
- Full usage and flags: README.md and docs/FEATURES_AND_CONFIG_FLAGS.md
- API and UI architecture: docs/API_AND_UI_ARCHITECTURE.md

Disclaimer:
- This software is provided "as is", without warranties of any kind.
- Use at your own risk; you are responsible for operational, security, and compliance outcomes.
- Contributors and affiliated organizations are not liable for direct or indirect damages.
`

// ==================== Constants ====================

const (
	// Default values
	defaultTimeout             = 15 * time.Minute
	defaultRequestTimeout      = 20 * time.Second
	defaultPollInterval        = 15 * time.Second
	defaultPollJitter          = 2 * time.Second
	defaultMaxParallel         = 4 // fallback for an invalid value; unset/0 = auto
	defaultRetryFailedClusters = 1
	defaultRetryAttempts       = 6
	defaultRetryBaseDelay      = 400 * time.Millisecond
	defaultRetryMaxDelay       = 8 * time.Second
	defaultRetryCircuitBreaker = 3
	defaultOutputDirLogs       = "nccfiles"
	defaultOutputDirFiltered   = "outputfiles"
	defaultPromDir             = "promfiles"
	defaultLogFile             = "logs/ncc-runner.log"
	defaultOutputFormat        = "html"
	defaultExcludeMatchMode    = "exact"
	maxSecretsFileBytes        = 1 << 20 // 1 MiB

	// HTTP connection pooling defaults live in goncc/internal/httpclient
	// (DefaultMaxIdleConns / DefaultMaxIdleConnsPerHost / DefaultIdleConnTimeout).

	// Graceful shutdown
	shutdownTimeout = 30 * time.Second

	// maxParallelLimit caps max-parallel (clusters processed at once).
	maxParallelLimit = 1000

	// adaptiveDecreaseCooldown spaces adaptive-parallelism back-offs.
	adaptiveDecreaseCooldown = 5 * time.Second

	// Security
	minPasswordLength = 1
	maxClusterNameLen = 255
	maxURLLength      = 2048

	// Prism Gateway port
	prismGatewayPort = 9440

	// defaultNutanixV4APIVersion is the default Nutanix v4 API path segment (clustermgmt / monitoring).
	defaultNutanixV4APIVersion = "v4.2"
	defaultDiscoverAPIVersion  = "v4"
	defaultClusterSourceMode   = "clusters"
	defaultFlakyLookbackRuns   = 6
	defaultFlakyTransitions    = 2
)

// ==================== Exit Codes ====================
// Exit code 0 = success, 1 = run/execution error, 2 = config/validation error (see README).

type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string { return e.err.Error() }
func (e *exitCodeError) Unwrap() error { return e.err }

func exitConfig(err error) error { return &exitCodeError{code: 2, err: err} }

// exitPartial indicates some clusters succeeded and some failed (exit code 3).
func exitPartial(err error) error { return &exitCodeError{code: 3, err: err} }

var (
	Version     string
	BuildDate   string
	GoVersion   string
	Stream      string // e.g., "prod", "dev", "beta"
	GitRevision string
)

func init() {
	// Defaults
	if Version == "" {
		Version = "2.4.0"
	}
	if BuildDate == "" {
		BuildDate = "unknown"
	}
	if Stream == "" {
		Stream = "dev"
	}

	// Optional build info enrichment
	if bi, ok := debug.ReadBuildInfo(); ok {
		if GoVersion == "" {
			GoVersion = bi.GoVersion
		}

		var gitRevision string
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if s.Value != "" {
					gitRevision = s.Value
					GitRevision = s.Value
				}
			case "vcs.time":
				if BuildDate == "unknown" && s.Value != "" {
					BuildDate = s.Value
				}
			}
		}
		if gitRevision != "" {
			Version = Version + "-" + gitRevision
		}
	}

	if GoVersion == "" {
		GoVersion = "unknown"
	}
}

func versionInfoString() string {
	return fmt.Sprintf(
		"Version: %s\nCommit: %s\nStream: %s\nBuild Date: %s\nGo Version: %s\nOS: %s\nArch: %s",
		Version, GitRevision, Stream, BuildDate, GoVersion, runtime.GOOS, runtime.GOARCH,
	)
}

func main() {
	// Size the Go heap to the container's memory limit (no-op on hosts).
	runtimetune.Apply()
	// Ensure logs are flushed on exit
	defer func() {
		// Give logger time to flush
		time.Sleep(100 * time.Millisecond)
	}()

	root := newRootCmd()
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", humanizeCLIError(root, os.Args[1:], err))
		code := 1
		var exitErr *exitCodeError
		if errors.As(err, &exitErr) {
			code = exitErr.code
		}
		os.Exit(code)
	}
	os.Exit(0)
}

// clusterStartedAt holds when each cluster's run began, for the durations in
// the per-cluster log lines.
var clusterStartedAt sync.Map

// clusterElapsed is seconds since the cluster's run began (0 if unknown).
func clusterElapsed(cluster string) float64 {
	if v, ok := clusterStartedAt.Load(cluster); ok {
		return math.Round(time.Since(v.(time.Time)).Seconds()*10) / 10
	}
	return 0
}
