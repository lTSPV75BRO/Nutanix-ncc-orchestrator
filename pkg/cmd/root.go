package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
	"golang.org/x/term"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"

	"goncc/pkg/client"
	"goncc/pkg/config"
	"goncc/pkg/errors"
	"goncc/pkg/metrics"
	"goncc/pkg/notifications"
	"goncc/pkg/parser"
	"goncc/pkg/renderer"
	"goncc/pkg/types"
)

var (
	Version   string
	BuildDate string
	GoVersion string
	Stream    string
)

// proxyDecorator is used for dynamic progress bar text
type proxyDecorator struct{ text string }

func (p *proxyDecorator) Decor(ctx decor.Statistics) string { return p.text }
func (p *proxyDecorator) Sync() (chan int, bool)            { return nil, false }
func (p *proxyDecorator) GetConf() decor.WC                 { return decor.WC{} }
func (p *proxyDecorator) SetConf(wc decor.WC)               {}
func (p *proxyDecorator) SetText(s string)                  { p.text = s }

// promptPasswordIfEmpty prompts for password if not provided
func promptPasswordIfEmpty(p string, username string) (string, error) {
	if p != "" {
		return p, nil
	}
	fmt.Printf("Prism Password (%s): ", username)
	bytePw, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", errors.Wrap(err, errors.ErrorTypeAuth, "failed to read password")
	}
	return strings.TrimSpace(string(bytePw)), nil
}

// setupFileLogger configures the file logger
func setupFileLogger(logPath string, lvl zerolog.Level) error {
	dir := filepath.Dir(logPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return errors.Wrap(err, errors.ErrorTypeFile, "failed to create log directory")
		}
	}
	fileWriter := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    20, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	log.Logger = zerolog.New(fileWriter).Level(lvl).With().
		Timestamp().
		Str("Version", Version).
		Str("Stream", Stream).
		Logger()
	return nil
}

// parseLogLevel converts string to zerolog level
func parseLogLevel(s string) zerolog.Level {
	if s == "" {
		if env := os.Getenv("LOG_LEVEL"); env != "" {
			s = env
		} else {
			return zerolog.InfoLevel
		}
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace", "0":
		return zerolog.TraceLevel
	case "debug", "1":
		return zerolog.DebugLevel
	case "info", "2":
		return zerolog.InfoLevel
	case "warn", "warning", "3":
		return zerolog.WarnLevel
	case "error", "4":
		return zerolog.ErrorLevel
	case "fatal", "5":
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

// NewRootCmd creates the root command
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ncc-orchestrator",
		Short: "Nutanix NCC Orchestrator",
		Long: `A tool to run NCC checks on multiple clusters, aggregate results, and generate reports.
Use --config for setup.

Examples:

  # Basic usage with configuration file
  ncc-orchestrator --config config.yaml

  # Specify clusters and username via flags
  ncc-orchestrator --clusters 10.0.1.1,10.0.2.1 --username admin

  # Show all available environment variables
  ncc-orchestrator --env-info

Run 'ncc-orchestrator --help' for a full list of options.
`,
		Version: fmt.Sprintf(`
Version: %s
Stream: %s
Build Date: %s
Go Version: %s`, Version, Stream, BuildDate, GoVersion),
		RunE: runCommand,
	}

	cmd.SilenceUsage = true
	setupFlags(cmd)
	bindFlags(cmd)

	return cmd
}

// setupFlags defines all command line flags
func setupFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("env-info", false, "Display possible environment variables and their current values")
	cmd.Flags().Bool("tc", false, "Display terms and conditions")
	cmd.Flags().String("config", "", "Config file path (yaml/json)")
	cmd.Flags().String("clusters", "", "Comma-separated cluster IPs or FQDNs")
	cmd.Flags().String("username", "admin", "Username for Prism Gateway")
	cmd.Flags().String("password", "", "Password (omit to be prompted)")
	cmd.Flags().Bool("insecure-skip-verify", false, "Skip TLS verify (only for trusted labs)")
	cmd.Flags().String("timeout", "15m", "Overall per-cluster timeout")
	cmd.Flags().String("request-timeout", "20s", "Per-request timeout")
	cmd.Flags().String("poll-interval", "15s", "Polling interval for task status")
	cmd.Flags().String("poll-jitter", "2s", "Additive jitter to polling interval")
	cmd.Flags().Int("max-parallel", 4, "Max concurrent clusters")
	cmd.Flags().String("outputs", "html,csv", "Comma-separated outputs: html,csv,json for per-cluster files")
	cmd.Flags().String("output-dir-logs", "nccfiles", "Directory for raw logs")
	cmd.Flags().String("output-dir-filtered", "outputfiles", "Directory for filtered and aggregated results")
	cmd.Flags().String("log-file", "logs/ncc-runner.log", "Path to log file (rotated)")
	cmd.Flags().String("log-level", "", "Log level (trace/debug/info/warn/error or 0..5)")
	cmd.Flags().Bool("log-http", false, "Enable HTTP request/response dump logs")
	cmd.Flags().Int("retry-max-attempts", 6, "Max retry attempts for HTTP calls")
	cmd.Flags().String("retry-base-delay", "400ms", "Base retry delay (with jitter, exponential)")
	cmd.Flags().String("retry-max-delay", "8s", "Max retry delay cap")
	cmd.Flags().Bool("replay", false, "Replay from existing logs without running NCC")
	cmd.Flags().Bool("skip-health-check", false, "Skip cluster health validation before running NCC")
	cmd.Flags().String("health-check-timeout", "30s", "Timeout for cluster health checks")
	cmd.Flags().String("filter-severity", "", "Filter results by severity (FAIL,WARN,INFO,ERR)")
	cmd.Flags().String("filter-check", "", "Filter results by check name pattern (regex supported)")
	cmd.Flags().String("filter-cluster", "", "Filter results by cluster pattern (regex supported)")
	cmd.Flags().Bool("email-enabled", false, "Enable email notifications")
	cmd.Flags().String("email-smtp-host", "", "SMTP server hostname")
	cmd.Flags().Int("email-smtp-port", 587, "SMTP server port")
	cmd.Flags().String("email-username", "", "SMTP username")
	cmd.Flags().String("email-password", "", "SMTP password")
	cmd.Flags().String("email-from", "", "Email sender address")
	cmd.Flags().String("email-to", "", "Comma-separated email recipients")
	cmd.Flags().String("email-subject", "NCC Orchestrator Report", "Email subject")
	cmd.Flags().Bool("email-tls", true, "Use TLS for SMTP")
	cmd.Flags().Bool("metrics-enabled", false, "Enable Prometheus metrics export")
	cmd.Flags().String("metrics-file", "metrics.prom", "Path to Prometheus metrics file")
	cmd.Flags().Bool("webhook-enabled", false, "Enable webhook notifications")
	cmd.Flags().String("webhook-url", "", "Webhook URL for notifications")
	cmd.Flags().String("webhook-method", "POST", "HTTP method for webhook")
	cmd.Flags().String("webhook-headers", "", "JSON string of additional headers")
	cmd.Flags().String("webhook-timeout", "30s", "Webhook request timeout")
}

// bindFlags binds command line flags to viper
func bindFlags(cmd *cobra.Command) {
	_ = viper.BindPFlag("config", cmd.Flags().Lookup("config"))
	_ = viper.BindPFlag("clusters", cmd.Flags().Lookup("clusters"))
	_ = viper.BindPFlag("username", cmd.Flags().Lookup("username"))
	_ = viper.BindPFlag("password", cmd.Flags().Lookup("password"))
	_ = viper.BindPFlag("insecure-skip-verify", cmd.Flags().Lookup("insecure-skip-verify"))
	_ = viper.BindPFlag("timeout", cmd.Flags().Lookup("timeout"))
	_ = viper.BindPFlag("request-timeout", cmd.Flags().Lookup("request-timeout"))
	_ = viper.BindPFlag("poll-interval", cmd.Flags().Lookup("poll-interval"))
	_ = viper.BindPFlag("poll-jitter", cmd.Flags().Lookup("poll-jitter"))
	_ = viper.BindPFlag("max-parallel", cmd.Flags().Lookup("max-parallel"))
	_ = viper.BindPFlag("outputs", cmd.Flags().Lookup("outputs"))
	_ = viper.BindPFlag("output-dir-logs", cmd.Flags().Lookup("output-dir-logs"))
	_ = viper.BindPFlag("output-dir-filtered", cmd.Flags().Lookup("output-dir-filtered"))
	_ = viper.BindPFlag("log-file", cmd.Flags().Lookup("log-file"))
	_ = viper.BindPFlag("log-level", cmd.Flags().Lookup("log-level"))
	_ = viper.BindPFlag("log-http", cmd.Flags().Lookup("log-http"))
	_ = viper.BindPFlag("retry-max-attempts", cmd.Flags().Lookup("retry-max-attempts"))
	_ = viper.BindPFlag("retry-base-delay", cmd.Flags().Lookup("retry-base-delay"))
	_ = viper.BindPFlag("retry-max-delay", cmd.Flags().Lookup("retry-max-delay"))
	_ = viper.BindPFlag("replay", cmd.Flags().Lookup("replay"))
	_ = viper.BindPFlag("skip-health-check", cmd.Flags().Lookup("skip-health-check"))
	_ = viper.BindPFlag("health-check-timeout", cmd.Flags().Lookup("health-check-timeout"))
	_ = viper.BindPFlag("filter-severity", cmd.Flags().Lookup("filter-severity"))
	_ = viper.BindPFlag("filter-check", cmd.Flags().Lookup("filter-check"))
	_ = viper.BindPFlag("filter-cluster", cmd.Flags().Lookup("filter-cluster"))
	_ = viper.BindPFlag("email-enabled", cmd.Flags().Lookup("email-enabled"))
	_ = viper.BindPFlag("email-smtp-host", cmd.Flags().Lookup("email-smtp-host"))
	_ = viper.BindPFlag("email-smtp-port", cmd.Flags().Lookup("email-smtp-port"))
	_ = viper.BindPFlag("email-username", cmd.Flags().Lookup("email-username"))
	_ = viper.BindPFlag("email-password", cmd.Flags().Lookup("email-password"))
	_ = viper.BindPFlag("email-from", cmd.Flags().Lookup("email-from"))
	_ = viper.BindPFlag("email-to", cmd.Flags().Lookup("email-to"))
	_ = viper.BindPFlag("email-subject", cmd.Flags().Lookup("email-subject"))
	_ = viper.BindPFlag("email-tls", cmd.Flags().Lookup("email-tls"))
	_ = viper.BindPFlag("metrics-enabled", cmd.Flags().Lookup("metrics-enabled"))
	_ = viper.BindPFlag("metrics-file", cmd.Flags().Lookup("metrics-file"))
	_ = viper.BindPFlag("webhook-enabled", cmd.Flags().Lookup("webhook-enabled"))
	_ = viper.BindPFlag("webhook-url", cmd.Flags().Lookup("webhook-url"))
	_ = viper.BindPFlag("webhook-method", cmd.Flags().Lookup("webhook-method"))
	_ = viper.BindPFlag("webhook-headers", cmd.Flags().Lookup("webhook-headers"))
	_ = viper.BindPFlag("webhook-timeout", cmd.Flags().Lookup("webhook-timeout"))
}

// runCommand is the main command execution function
func runCommand(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "configuration validation failed")
	}

	lvl := parseLogLevel(cfg.LogLevel)
	if err := setupFileLogger(cfg.LogFile, lvl); err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to setup logger")
	}

	log.Info().
		Strs("clusters", cfg.Clusters).
		Str("username", cfg.Username).
		Bool("insecureSkipVerify", cfg.InsecureSkipVerify).
		Dur("timeout", cfg.Timeout).
		Dur("requestTimeout", cfg.RequestTimeout).
		Dur("pollInterval", cfg.PollInterval).
		Dur("pollJitter", cfg.PollJitter).
		Int("maxParallel", cfg.MaxParallel).
		Strs("outputs", cfg.OutputFormats).
		Str("logsDir", cfg.OutputDirLogs).
		Str("filteredDir", cfg.OutputDirFiltered).
		Str("logFile", cfg.LogFile).
		Str("logLevel", lvl.String()).
		Bool("logHTTP", cfg.LogHTTP || os.Getenv("LOG_HTTP") == "1").
		Int("retryMaxAttempts", cfg.RetryMaxAttempts).
		Dur("retryBaseDelay", cfg.RetryBaseDelay).
		Dur("retryMaxDelay", cfg.RetryMaxDelay).
		Msg("starting NCC orchestrator")

	// Handle special flags
	if tc, _ := cmd.Flags().GetBool("tc"); tc {
		fmt.Println(getTermsText())
		return nil
	}

	if envInfo, _ := cmd.Flags().GetBool("env-info"); envInfo {
		printEnvInfo()
		return nil
	}

	cfg.Password, err = promptPasswordIfEmpty(cfg.Password, cfg.Username)
	if err != nil {
		return err
	}

	fs := types.OSFS{}
	httpc := client.NewHTTPClient(cfg)

	if err := fs.MkdirAll(cfg.OutputDirLogs, 0755); err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create logs directory")
	}
	if err := fs.MkdirAll(cfg.OutputDirFiltered, 0755); err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to create output directory")
	}

	// Handle replay mode
	if cmd.Flags().Changed("replay") && viper.GetBool("replay") {
		return handleReplayMode(cfg, fs)
	}

	fmt.Println("You have accepted T&C, Check using --tc flag")

	// Perform health checks if not skipped
	if !viper.GetBool("skip-health-check") {
		if err := performHealthChecks(cfg, httpc); err != nil {
			return err
		}
	}

	return runNCCChecks(cfg, fs, httpc)
}

// getTermsText returns the terms and conditions text
func getTermsText() string {
	return `This script is created by Prajwal Vernekar (prajwal.vernekar@nutanix.com).

Script Description:
Nutanix NCC Orchestrator is a CLI tool to run NCC checks across multiple clusters in parallel, aggregate results, and generate HTML/CSV reports.

How the Script Works:
- Reads configuration from config file, environment variables, or CLI flags.
- Starts NCC checks on each cluster via API.
- Polls for completion and fetches summaries.
- Generates per-cluster and aggregated reports in specified formats.

Usage:
./ncc-orchestrator [flags]
./ncc-orchestrator --help for more details.

Instructions for config.yaml File:
Create a config.yaml with keys like:
# Required
clusters: "10.0.XX.XX,10.1.XX.XX"      	  # Comma-separated list of Prism cluster IPs/hosts  
username: "admin"                         # Prism username  
password: ""                              # Prefer env NCC_PASSWORD in CI; leave empty here if using env  

# TLS and timeouts
insecure-skip-verify: false               # Set true only for lab/self-signed  
timeout: "15m"                            # Per-cluster overall timeout  
request-timeout: "30s"                    # Per HTTP request timeout  
poll-interval: "15s"                      # Polling interval for task status  
poll-jitter: "2s"                         # Random jitter to avoid herd behavior  

# Concurrency and outputs
max-parallel: 4                           # Parallel clusters processed  
outputs: "html,csv"                       # One or more: html,csv  
output-dir-logs: "nccfiles"               # Directory for raw NCC summary text  
output-dir-filtered: "outputfiles"        # Directory for generated HTML/CSV  

# Logging
log-file: "logs/ncc-runner.log"           # Rotated JSON logs path  
log-level: "2"                            # 0 trace, 1 debug, 2 info, 3 warn, 4 error  
log-http: false                           # Set true only for debugging; logs request/response dumps  
 
# Retry behavior
retry-max-attempts: 6                     # Max attempts per request  
retry-base-delay: "400ms"                 # Base backoff delay  
retry-max-delay: "8s"                     # Max jittered backoff delay  

Use --config to specify file path.

Nutanix APIs used:

1. POST https://{cluster_IP}:9440/PrismGateway/services/rest/v1/ncc/checks        -> Initiates NCC checks on the cluster. Returns a task UUID for polling.
2. GET  https://{cluster_IP}:9440/PrismGateway/services/rest/v2.0/tasks/{taskID}  -> Polls the status of the NCC task. Returns progress (percentage complete and status).
3. GET  https://{cluster_IP}:9440/PrismGateway/services/rest/v1/ncc/{taskID}      -> Fetches the NCC run summary once the task is complete. Returns the raw summary text.

Disclaimer:
     Use at your own risk. Running this program implies acceptance of associated risks.
     The developer or Nutanix shall not be held liable for any consequences resulting from its use.`
}

// printEnvInfo prints environment variable information
func printEnvInfo() {
	fmt.Println("Possible Environment Variables (prefix: NCC_) and Current Values:")
	envKeys := []string{
		"CLUSTERS",
		"USERNAME",
		"PASSWORD",
		"INSECURE_SKIP_VERIFY",
		"TIMEOUT",
		"REQUEST_TIMEOUT",
		"POLL_INTERVAL",
		"POLL_JITTER",
		"MAX_PARALLEL",
		"OUTPUTS",
		"OUTPUT_DIR_LOGS",
		"OUTPUT_DIR_FILTERED",
		"LOG_FILE",
		"LOG_LEVEL",
		"LOG_HTTP",
		"RETRY_MAX_ATTEMPTS",
		"RETRY_BASE_DELAY",
		"RETRY_MAX_DELAY",
	}
	for _, key := range envKeys {
		envVar := "NCC_" + key
		val := os.Getenv(envVar)
		if val != "" {
			fmt.Printf("%s = %s\n", envVar, val)
		} else {
			fmt.Printf("%s = (not set)\n", envVar)
		}
	}
}

// handleReplayMode processes existing logs without running NCC
func handleReplayMode(cfg *config.Config, fs types.FS) error {
	var agg []types.AggBlock
	var clusterFiles []struct{ Cluster, HTML, CSV string }

	renderer := renderer.NewHTMLRenderer(fs)

	for _, cluster := range cfg.Clusters {
		// Ensure filtered log exists
		filtered := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
		if _, err := os.Stat(filtered); err != nil {
			// Try to build it from raw ncc log
			raw := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
			if _, err2 := os.Stat(raw); err2 == nil {
				if err3 := buildFilteredLog(fs, raw, filtered); err3 != nil {
					log.Error().Str("cluster", cluster).Err(err3).Msg("replay: build filtered failed")
					continue
				}
				log.Info().Str("cluster", cluster).Str("filtered", filtered).Msg("replay: built filtered")
			} else {
				log.Warn().Str("cluster", cluster).Msg("replay: no filtered or raw log, skipping")
				continue
			}
		}

		// Parse filtered
		data, err := fs.ReadFile(filtered)
		if err != nil {
			log.Error().Str("cluster", cluster).Err(err).Msg("replay: read filtered failed")
			continue
		}
		blocks, err := parser.ParseSummary(string(data))
		if err != nil {
			log.Error().Str("cluster", cluster).Err(err).Msg("replay: parse filtered failed")
			continue
		}

		// Per-cluster outputs
		base := filtered
		for _, f := range cfg.OutputFormats {
			switch strings.ToLower(strings.TrimSpace(f)) {
			case "html":
				_ = renderer.GenerateHTML(parser.RowsFromBlocks(blocks), base+".html")
			case "csv":
				_ = renderer.GenerateCSV(blocks, base+".csv")
			case "json":
				_ = renderer.GenerateJSON(blocks, base+".json")
			}
		}

		clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
			Cluster: cluster,
			HTML:    filepath.Base(base + ".html"),
			CSV:     filepath.Base(base + ".csv"),
		})

		for _, b := range blocks {
			agg = append(agg, types.AggBlock{
				Cluster:  cluster,
				Severity: b.Severity,
				Check:    b.CheckName,
				Detail:   b.DetailRaw,
			})
		}
	}

	if err := renderer.WriteAggregatedHTML(cfg.OutputDirFiltered, agg, clusterFiles); err != nil {
		log.Error().Err(err).Msg("replay: write aggregated HTML failed")
		return err
	}
	log.Info().Int("clusters", len(clusterFiles)).Int("rows", len(agg)).Msg("replay: aggregated page generated")
	return nil
}

// buildFilteredLog creates a filtered log from raw NCC output
func buildFilteredLog(fs types.FS, inputPath, outputPath string) error {
	data, err := fs.ReadFile(inputPath)
	if err != nil {
		return err
	}

	blocks, err := parser.ParseSummary(string(data))
	if err != nil {
		return err
	}

	if err := fs.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	var b strings.Builder
	for _, pb := range blocks {
		b.WriteString(pb.CheckName)
		b.WriteString("\n")
		b.WriteString(pb.DetailRaw)
		b.WriteString("\n\n---------------------------------------\n")
	}

	return fs.WriteFile(outputPath, []byte(b.String()), 0644)
}

// runNCCChecks executes the main NCC checking workflow
func runNCCChecks(cfg *config.Config, fs types.FS, httpc *http.Client) error {
	p := mpb.New(mpb.WithWidth(80))

	ctx := context.Background()
	sem := make(chan struct{}, cfg.MaxParallel)
	var wg sync.WaitGroup
	results := make(chan types.ClusterResult, len(cfg.Clusters))

	for _, cluster := range cfg.Clusters {
		wg.Add(1)
		sem <- struct{}{}

		mainBar := p.New(
			100,
			mpb.BarStyle().Rbound("|"),
			mpb.PrependDecorators(
				decor.Name(fmt.Sprintf("%-18s", cluster), decor.WC{W: 20, C: decor.DidentRight}),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WC{W: 4}),
				decor.Name(" • "),
				decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
			),
		)

		phaseProxy := &proxyDecorator{text: "starting"}

		phaseBar := p.New(
			1,
			mpb.NopStyle(),
			mpb.PrependDecorators(decor.Name(strings.Repeat(" ", 20))),
			mpb.AppendDecorators(phaseProxy),
		)

		go func(cl string, b *mpb.Bar, phase *proxyDecorator, phaseBar *mpb.Bar) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					b.Abort(false)
					b.SetTotal(b.Current(), true)
					phaseBar.SetCurrent(1)
					phaseBar.SetTotal(1, true)
					log.Error().Interface("panic", r).Stack().Str("cluster", cl).Msg("cluster goroutine panic")
					results <- types.ClusterResult{Cluster: cl, Blocks: nil, Err: fmt.Errorf("panic: %v", r)}
				}
			}()

			reqCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()

			onPct := func(pct int) { b.SetCurrent(int64(pct)) }
			setPhase := func(text string) {
				phase.SetText(text)
				log.Info().Str("cluster", cl).Str("phase", text).Msg("phase change")
			}

			blocks, err := runClusterWithBars(reqCtx, cfg, fs, httpc, cl, onPct, setPhase)
			if err != nil {
				b.Abort(false)
				b.SetTotal(b.Current(), true)
				setPhase("failed")
				phaseBar.SetCurrent(1)
				phaseBar.SetTotal(1, true)
				log.Error().Str("cluster", cl).Err(err).Msg("cluster run failed")
				results <- types.ClusterResult{Cluster: cl, Blocks: nil, Err: err}
				return
			}

			b.SetCurrent(100)
			b.SetTotal(100, true)
			setPhase("done")
			phaseBar.SetCurrent(1)
			phaseBar.SetTotal(1, true)
			log.Info().Str("cluster", cl).Msg("cluster run completed")
			results <- types.ClusterResult{Cluster: cl, Blocks: blocks, Err: nil}
		}(cluster, mainBar, phaseProxy, phaseBar)
	}

	// Wait for workers, close and drain results
	wg.Wait()
	close(results)

	var failed []string
	var agg []types.AggBlock
	var clusterFiles []struct{ Cluster, HTML, CSV string }

	renderer := renderer.NewHTMLRenderer(fs)

	for r := range results {
		if r.Err != nil {
			failed = append(failed, r.Cluster)
			continue
		}
		for _, b := range r.Blocks {
			agg = append(agg, types.AggBlock{
				Cluster:  r.Cluster,
				Severity: b.Severity,
				Check:    b.CheckName,
				Detail:   b.DetailRaw,
			})
		}
		basePath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", r.Cluster))
		htmlPath := basePath + ".html"
		csvPath := basePath + ".csv"
		clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
			Cluster: r.Cluster,
			HTML:    filepath.Base(htmlPath),
			CSV:     filepath.Base(csvPath),
		})
	}

	// Write aggregated page
	if err := renderer.WriteAggregatedHTML(cfg.OutputDirFiltered, agg, clusterFiles); err != nil {
		log.Error().Err(err).Msg("write aggregated HTML failed")
	}

	// Send email notification if enabled
	if viper.GetBool("email-enabled") {
		if err := sendEmailNotification(agg, failed); err != nil {
			log.Error().Err(err).Msg("failed to send email notification")
			// Don't fail the entire process for email errors
		} else {
			log.Info().Msg("email notification sent successfully")
		}
	}

	// Export Prometheus metrics if enabled
	if viper.GetBool("metrics-enabled") {
		if err := exportPrometheusMetrics(agg, failed); err != nil {
			log.Error().Err(err).Msg("failed to export Prometheus metrics")
			// Don't fail the entire process for metrics errors
		} else {
			log.Info().Msg("Prometheus metrics exported successfully")
		}
	}

	// Send webhook notification if enabled
	if viper.GetBool("webhook-enabled") {
		if err := sendWebhookNotification(agg, failed); err != nil {
			log.Error().Err(err).Msg("failed to send webhook notification")
			// Don't fail the entire process for webhook errors
		} else {
			log.Info().Msg("webhook notification sent successfully")
		}
	}

	if len(failed) > 0 {
		log.Error().Strs("failedClusters", failed).Msg("some clusters failed")
		return fmt.Errorf("some clusters failed: %v", failed)
	}

	log.Info().Msg("all clusters processed successfully")
	fmt.Printf("All clusters processed successfully\n")
	return nil
}

// runClusterWithBars runs NCC checks on a single cluster with progress tracking
func runClusterWithBars(
	ctx context.Context,
	cfg *config.Config,
	fs types.FS,
	httpc *http.Client,
	cluster string,
	onPct func(int),
	setPhase func(string),
) ([]types.ParsedBlock, error) {
	l := log.With().Str("cluster", cluster).Logger()
	nccClient := client.NewNCCClient(cluster, cfg.Username, cfg.Password, httpc, cfg)

	setPhase("starting")
	l.Info().Msg("starting NCC checks")
	taskID, body, err := nccClient.StartChecks(ctx)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("start checks failed")
		return nil, errors.Wrap(err, errors.ErrorTypeNetwork, "start checks failed")
	}
	l.Info().Str("taskID", taskID).Msg("ncc task started")
	onPct(1)

	last := 1
	setPhase("polling")
	for {
		select {
		case <-ctx.Done():
			l.Error().Err(ctx.Err()).Msg("context done during polling")
			return nil, errors.Wrap(ctx.Err(), errors.ErrorTypeTimeout, "context cancelled during polling")
		case <-func() <-chan time.Time {
			jitter := time.Duration(rand.Int63n(int64(cfg.PollJitter)))
			return time.After(cfg.PollInterval + jitter)
		}():
			if dl, ok := ctx.Deadline(); ok {
				rem := time.Until(dl)
				if rem < 10*time.Second {
					l.Warn().Dur("remaining", rem).Msg("cluster deadline near")
				}
			}
			status, body, err := nccClient.GetTask(ctx, taskID)
			if err != nil {
				l.Error().Err(err).RawJSON("response_body", body).Msg("poll failed")
				return nil, errors.Wrap(err, errors.ErrorTypeNetwork, "poll failed")
			}
			pct := status.PercentageComplete
			if pct < last {
				pct = last
			}
			if pct > 100 {
				pct = 100
			}
			onPct(pct)
			l.Debug().Int("pct", pct).Str("progress", status.ProgressStatus).Msg("task status")
			last = pct

			if status.ProgressStatus == "Failed" {
				return nil, errors.New(errors.ErrorTypeNetwork, "ncc task failed")
			}
			if pct >= 100 {
				goto SUMMARY
			}
		}
	}

SUMMARY:
	setPhase("summary")
	summary, body, err := nccClient.GetRunSummary(ctx, taskID)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("get summary failed")
		return nil, errors.Wrap(err, errors.ErrorTypeNetwork, "get summary failed")
	}

	setPhase("writing")
	logPath, err := writeSummary(fs, cfg.OutputDirLogs, cluster, summary.RunSummary)
	if err != nil {
		l.Error().Err(err).Msg("write summary failed")
		return nil, err
	}
	l.Info().Str("logPath", logPath).Msg("summary written")

	filteredPath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
	if err := filterBlocksToFile(fs, logPath, filteredPath); err != nil {
		l.Error().Err(err).Msg("filter blocks failed")
		return nil, err
	}
	l.Info().Str("filteredPath", filteredPath).Msg("filtered written")

	data, err := fs.ReadFile(filteredPath)
	if err != nil {
		l.Error().Err(err).Msg("read filtered failed")
		return nil, err
	}
	l.Debug().Str("path", filteredPath).Int("bytes", len(data)).Msg("read filtered bytes")
	blocks, err := parser.ParseSummary(string(data))
	if err != nil {
		l.Error().Err(err).Msg("parse filtered failed")
		return nil, err
	}
	if len(blocks) == 0 {
		l.Warn().Str("path", filteredPath).Msg("no blocks parsed from summary")
	}

	// Apply filters
	blocks = applyFilters(blocks, cluster)

	renderer := renderer.NewHTMLRenderer(fs)
	base := filteredPath
	for _, f := range cfg.OutputFormats {
		switch strings.ToLower(strings.TrimSpace(f)) {
		case "html":
			htmlFile := base + ".html"
			if err := renderer.GenerateHTML(parser.RowsFromBlocks(blocks), htmlFile); err != nil {
				l.Error().Err(err).Str("file", htmlFile).Msg("write HTML failed")
				return nil, err
			}
			l.Info().Str("file", htmlFile).Msg("HTML generated")
		case "csv":
			csvFile := base + ".csv"
			if err := renderer.GenerateCSV(blocks, csvFile); err != nil {
				l.Error().Err(err).Str("file", csvFile).Msg("write CSV failed")
				return nil, err
			}
			l.Info().Str("file", csvFile).Msg("CSV generated")
		case "json":
			jsonFile := base + ".json"
			if err := renderer.GenerateJSON(blocks, jsonFile); err != nil {
				l.Error().Err(err).Str("file", jsonFile).Msg("write JSON failed")
				return nil, err
			}
			l.Info().Str("file", jsonFile).Msg("JSON generated")
		default:
			l.Warn().Str("format", f).Msg("unknown output format")
		}
	}

	setPhase("done")
	return blocks, nil
}

// writeSummary writes the NCC summary to a file
func writeSummary(fs types.FS, folder, cluster, summary string) (string, error) {
	if err := fs.MkdirAll(folder, 0755); err != nil {
		return "", errors.Wrap(err, errors.ErrorTypeFile, "failed to create directory")
	}
	outPath := filepath.Join(folder, fmt.Sprintf("%s.log", cluster))
	log.Debug().Str("path", outPath).Int("bytes", len(summary)).Msg("writing summary")
	if err := fs.WriteFile(outPath, []byte(parser.SanitizeSummary(summary)), 0644); err != nil {
		return "", errors.Wrap(err, errors.ErrorTypeFile, "failed to write summary")
	}
	return outPath, nil
}

// performHealthChecks validates all clusters before running NCC
func performHealthChecks(cfg *config.Config, httpc *http.Client) error {
	timeout, err := time.ParseDuration(viper.GetString("health-check-timeout"))
	if err != nil {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("Performing health checks on %d clusters...\n", len(cfg.Clusters))

	var failedClusters []string
	for _, cluster := range cfg.Clusters {
		fmt.Printf("Checking cluster %s... ", cluster)
		if err := checkClusterHealth(ctx, cluster, cfg.Username, cfg.Password, httpc); err != nil {
			fmt.Printf("FAILED: %v\n", err)
			failedClusters = append(failedClusters, cluster)
			log.Error().Str("cluster", cluster).Err(err).Msg("health check failed")
		} else {
			fmt.Printf("OK\n")
			log.Info().Str("cluster", cluster).Msg("health check passed")
		}
	}

	if len(failedClusters) > 0 {
		return fmt.Errorf("health checks failed for clusters: %v", failedClusters)
	}

	fmt.Println("All clusters passed health checks")
	return nil
}

// checkClusterHealth validates cluster connectivity and basic health
func checkClusterHealth(ctx context.Context, cluster, username, password string, httpc *http.Client) error {
	nccClient := client.NewNCCClient(cluster, username, password, httpc, &config.Config{})

	// Use the proper health check method that doesn't start NCC checks
	return nccClient.HealthCheck(ctx)
}

// applyFilters applies filtering to blocks based on configuration
func applyFilters(blocks []types.ParsedBlock, cluster string) []types.ParsedBlock {
	var filtered []types.ParsedBlock

	severityFilter := viper.GetString("filter-severity")
	checkFilter := viper.GetString("filter-check")
	clusterFilter := viper.GetString("filter-cluster")

	// Compile regex patterns if provided
	var checkRegex *regexp.Regexp
	var clusterRegex *regexp.Regexp
	var err error

	if checkFilter != "" {
		checkRegex, err = regexp.Compile(checkFilter)
		if err != nil {
			log.Warn().Str("pattern", checkFilter).Err(err).Msg("invalid check filter regex")
			checkRegex = nil
		}
	}

	if clusterFilter != "" {
		clusterRegex, err = regexp.Compile(clusterFilter)
		if err != nil {
			log.Warn().Str("pattern", clusterFilter).Err(err).Msg("invalid cluster filter regex")
			clusterRegex = nil
		}
	}

	// Apply cluster filter first
	if clusterRegex != nil && !clusterRegex.MatchString(cluster) {
		return filtered // Return empty if cluster doesn't match
	}

	// Apply other filters
	for _, block := range blocks {
		// Severity filter
		if severityFilter != "" {
			severities := strings.Split(severityFilter, ",")
			found := false
			for _, s := range severities {
				if strings.TrimSpace(s) == block.Severity {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Check name filter
		if checkRegex != nil && !checkRegex.MatchString(block.CheckName) {
			continue
		}

		filtered = append(filtered, block)
	}

	log.Info().
		Str("cluster", cluster).
		Int("original", len(blocks)).
		Int("filtered", len(filtered)).
		Msg("applied filters")

	return filtered
}

// sendEmailNotification sends email notification with results
func sendEmailNotification(results []types.AggBlock, failedClusters []string) error {
	emailConfig := notifications.EmailConfig{
		SMTPHost: viper.GetString("email-smtp-host"),
		SMTPPort: viper.GetInt("email-smtp-port"),
		Username: viper.GetString("email-username"),
		Password: viper.GetString("email-password"),
		From:     viper.GetString("email-from"),
		To:       strings.Split(viper.GetString("email-to"), ","),
		Subject:  viper.GetString("email-subject"),
		UseTLS:   viper.GetBool("email-tls"),
		UseAuth:  viper.GetString("email-username") != "",
	}

	// Clean up email addresses
	for i, addr := range emailConfig.To {
		emailConfig.To[i] = strings.TrimSpace(addr)
	}

	notifier := notifications.NewEmailNotifier(emailConfig)
	return notifier.SendReport(results, failedClusters)
}

// exportPrometheusMetrics exports metrics in Prometheus format
func exportPrometheusMetrics(results []types.AggBlock, failedClusters []string) error {
	exporter := metrics.NewPrometheusExporter()
	metricsData := exporter.ExportMetrics(results, failedClusters)

	metricsFile := viper.GetString("metrics-file")
	if err := os.WriteFile(metricsFile, []byte(metricsData), 0644); err != nil {
		return errors.Wrap(err, errors.ErrorTypeFile, "failed to write metrics file")
	}

	log.Info().Str("file", metricsFile).Msg("Prometheus metrics written")
	return nil
}

// sendWebhookNotification sends webhook notification with results
func sendWebhookNotification(results []types.AggBlock, failedClusters []string) error {
	timeout, err := time.ParseDuration(viper.GetString("webhook-timeout"))
	if err != nil {
		timeout = 30 * time.Second
	}

	// Parse additional headers
	headers := make(map[string]string)
	if headersStr := viper.GetString("webhook-headers"); headersStr != "" {
		if err := json.Unmarshal([]byte(headersStr), &headers); err != nil {
			log.Warn().Err(err).Msg("failed to parse webhook headers, ignoring")
		}
	}

	webhookConfig := notifications.WebhookConfig{
		URL:     viper.GetString("webhook-url"),
		Method:  viper.GetString("webhook-method"),
		Headers: headers,
		Timeout: timeout,
	}

	notifier := notifications.NewWebhookNotifier(webhookConfig)
	return notifier.SendReport(results, failedClusters)
}

// filterBlocksToFile parses summary and writes filtered blocks to file
func filterBlocksToFile(fs types.FS, inputPath, outputPath string) error {
	data, err := fs.ReadFile(inputPath)
	if err != nil {
		return err
	}

	blocks, err := parser.ParseSummary(string(data))
	if err != nil {
		return err
	}

	if err := fs.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	var b strings.Builder
	for _, pb := range blocks {
		b.WriteString(pb.CheckName)
		b.WriteString("\n")
		b.WriteString(pb.DetailRaw)
		b.WriteString("\n\n---------------------------------------\n")
	}

	return fs.WriteFile(outputPath, []byte(b.String()), 0644)
}
