package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"goncc/internal/notify"
	"goncc/internal/plural"
	"goncc/internal/trace"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
	"golang.org/x/term"
)

func newRootCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "ncc-orchestrator",
		Short: "Nutanix NCC Orchestrator",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Name() == "help" {
				return nil
			}
			return denyLegacyMCPClient()
		},
		Long: `A tool to run NCC checks on multiple clusters, aggregate results, and generate reports.
Use --config for setup.

Examples:

  # Basic usage with configuration file
  ncc-orchestrator --config config.yaml

  # Specify clusters and username via flags
  ncc-orchestrator --clusters 10.0.1.1,10.0.2.1 --username admin

  # Show all available environment variables
  ncc-orchestrator env-info

Run 'ncc-orchestrator --help' for a full list of options.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Opt-in profiling for large fleets: NCC_PROFILE_DIR=<dir> writes
			// cpu.pprof and heap.pprof there for `go tool pprof`.
			if stop := startRunProfiling(os.Getenv("NCC_PROFILE_DIR")); stop != nil {
				defer stop()
			}
			// Setup console logger first for early error visibility
			consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
			consoleLogger := zerolog.New(consoleWriter).With().Timestamp().Logger()
			zerolog.SetGlobalLevel(zerolog.InfoLevel)

			// Backward-compatible aliases for old root flags.
			if update, _ := cmd.Flags().GetBool("update"); update {
				if err := runUpdate(updateOptions{Repo: defaultGitHubRepo}); err != nil {
					consoleLogger.Error().Err(err).Msg("update failed")
					return fmt.Errorf("update: %w", err)
				}
				return nil
			}
			if genN, _ := cmd.Flags().GetInt("gen-test-agg"); genN > 0 {
				outDir := viper.GetString("output-dir-filtered")
				if outDir == "" {
					outDir = "outputfiles"
				}
				if err := generateTestAgg(genN, outDir); err != nil {
					consoleLogger.Error().Err(err).Int("clusters", genN).Msg("gen-test-agg failed")
					return fmt.Errorf("gen-test-agg: %w", err)
				}
				fmt.Printf("Generated test aggregated report: %d clusters, output in %s/index.html\n", genN, outDir)
				return nil
			}
			if showTC, _ := cmd.Flags().GetBool("tc"); showTC {
				fmt.Print(termsText)
				return nil
			}
			if showEnvInfo, _ := cmd.Flags().GetBool("env-info"); showEnvInfo {
				printEnvInfo()
				return nil
			}
			if showVersion, _ := cmd.Flags().GetBool("version"); showVersion {
				fmt.Println(versionInfoString())
				return nil
			}

			cfg, err := bindConfig()
			if err != nil {
				return exitConfig(fmt.Errorf("configuration: %w", err))
			}
			// viper, not cmd.Flags(): the flag still wins, but config.yaml and
			// NCC_AUTO / NCC_AUTOMATION_LEVEL now apply too.
			autoMode := viper.GetBool("auto")
			autoLevelRaw := viper.GetString("automation-level")
			autoLevel, err := parseAutomationLevel(autoLevelRaw)
			if err != nil {
				return exitConfig(err)
			}
			if autoMode && autoLevel == automationLevelFullAuto {
				changes := applyFullAutoRuntimeTuning(&cfg)
				for _, change := range changes {
					consoleLogger.Warn().Str("auto_tuning", change).Msg("full-auto runtime tuning applied")
				}
			}

			lvl := parseLogLevel(cfg.LogLevel)
			// Validate log level
			if lvl > zerolog.FatalLevel {
				consoleLogger.Warn().Int("level", int(lvl)).Msg("invalid log level, using info level")
				lvl = zerolog.InfoLevel
			}

			if err := checkOutputPermissions(&cfg); err != nil {
				consoleLogger.Error().Err(err).Msg("output permissions check failed (cannot open/create required files)")
				return exitConfig(fmt.Errorf("output permissions: %w", err))
			}

			if err := setupFileLogger(cfg.LogFile, lvl); err != nil {
				consoleLogger.Error().Err(err).Str("logFile", cfg.LogFile).Msg("failed to setup file logger")
				return exitConfig(fmt.Errorf("setup logger: %w", err))
			}

			// Set global log level after file logger is set up
			zerolog.SetGlobalLevel(lvl)
			log.Info().
				Str("clusterSourceMode", cfg.ClusterSourceMode).
				Strs("clusters", cfg.Clusters).
				Strs("pcs", cfg.PCs).
				Str("username", cfg.Username).
				Str("password", maskPassword(cfg.Password)).
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

			skipPreflight, _ := cmd.Flags().GetBool("skip-preflight-check")
			if !skipPreflight {
				preflightCfgPath := strings.TrimSpace(viper.GetString("config"))
				report := buildPreflightReport(preflightCfgPath)
				if report.Failed > 0 && autoMode && autoLevel != automationLevelAdvisory {
					fixed, actions := applySafePreflightFixes(cfg, preflightCfgPath, report, autoLevel)
					for _, action := range actions {
						log.Info().Str("auto_fix", action).Msg("automation applied safe fix")
					}
					if fixed > 0 {
						report = buildPreflightReport(preflightCfgPath)
					}
				}
				if report.Failed > 0 {
					printAutomationRunbook(report, autoLevel)
					hintMsg := ""
					if len(report.ActionableHints) > 0 {
						hintMsg = " hints: " + strings.Join(report.ActionableHints, "; ")
					}
					return exitConfig(fmt.Errorf("preflight-check failed (%d failures, %d warnings).%s", report.Failed, report.Warn, hintMsg))
				}
				log.Info().Int("preflight_failed", report.Failed).Int("preflight_warn", report.Warn).Msg("preflight-check passed")
			} else {
				log.Warn().Msg("preflight-check skipped by flag")
			}

			// Resolve targets by source mode.
			if cfg.ClusterSourceMode == "pc" {
				if strings.TrimSpace(cfg.Password) == "" {
					cfg.Password, err = promptPasswordIfEmpty("", cfg.Username)
					if err != nil {
						return err
					}
				}
				resolvedClusters, discoverErr := discoverClustersFromPCTargets(cfg)
				if discoverErr != nil {
					log.Error().Err(discoverErr).Msg("failed to discover clusters from PC targets")
					return exitConfig(discoverErr)
				}
				cfg.Clusters = resolvedClusters
				cfg.ClusterCredentials = map[string]ClusterCredential{}
				log.Info().
					Int("pcs", len(cfg.PCs)).
					Int("discovered_clusters", len(cfg.Clusters)).
					Str("discover_api_version", cfg.DiscoverAPIVersion).
					Msg("resolved clusters from pc mode")
			}

			// Validate required fields after target resolution.
			if len(cfg.Clusters) == 0 {
				err := errors.New("no clusters provided (--clusters, --clusters-file, env, or config)")
				log.Error().Msg(err.Error())
				return exitConfig(err)
			}
			if err := validateClusterCredentialCoverage(cfg); err != nil {
				log.Error().Msg(err.Error())
				return exitConfig(err)
			}

			// Dry-run mode: perform full validation and exit
			if cfg.DryRun {
				// Perform comprehensive validation for dry-run
				if err := validateConfig(cfg); err != nil {
					return fmt.Errorf("dry-run validation failed: %w", err)
				}

				log.Info().Msg("DRY-RUN MODE: Configuration validated, no checks will be executed")
				fmt.Println("✓ Configuration is valid")
				fmt.Printf("  Clusters: %d configured\n", len(cfg.Clusters))
				fmt.Printf("  Cluster source mode: %s\n", cfg.ClusterSourceMode)
				if cfg.ClusterSourceMode == "pc" {
					fmt.Printf("  Prism Central targets: %d\n", len(cfg.PCs))
				}
				if strings.TrimSpace(cfg.Username) != "" {
					fmt.Printf("  Username: %s\n", cfg.Username)
				} else {
					fmt.Println("  Username: per-cluster (clusters-file)")
				}
				fmt.Printf("  Output formats: %v\n", cfg.OutputFormats)
				if len(cfg.SeverityFilter) > 0 {
					fmt.Printf("  Severity filter: %v\n", cfg.SeverityFilter)
				}
				if len(cfg.ExcludeAlertTitles) > 0 {
					fmt.Printf("  Excluded alert titles: %v\n", cfg.ExcludeAlertTitles)
				}
				fmt.Printf("  Exclude alert match mode: %s\n", cfg.ExcludeAlertMatchMode)
				if cfg.ExcludeAlertTitlesFile != "" {
					fmt.Printf("  Exclude alert titles file: %s\n", cfg.ExcludeAlertTitlesFile)
				}
				if cfg.ArtifactRetainDays > 0 || cfg.ArtifactRetainMaxFiles > 0 {
					fmt.Printf("  Artifact retention: days=%d max_files=%d\n", cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles)
				}
				if cfg.InsecureSkipVerify {
					fmt.Println("  ⚠️  WARNING: TLS verification is disabled")
				}
				fmt.Println("  All settings validated successfully")
				return nil
			}

			if needPrompt, promptUser := needsPasswordPrompt(cfg); needPrompt {
				cfg.Password, err = promptPasswordIfEmpty(cfg.Password, promptUser)
				if err != nil {
					return err
				}
			}

			fs := OSFS{}
			httpc := NewHTTPClient(cfg)
			if err := fs.MkdirAll(cfg.OutputDirLogs, 0755); err != nil {
				return err
			}
			if err := fs.MkdirAll(cfg.OutputDirFiltered, 0755); err != nil {
				return err
			}
			if cfg.PromEnabled {
				if err := fs.MkdirAll(cfg.PromDir, 0755); err != nil {
					return err
				}
			}

			previousSummaryPath := filepath.Join(cfg.OutputDirFiltered, "run-summary.json")
			previousSummary, hasPreviousSummary, err := loadRunSummaryJSON(previousSummaryPath)
			if err != nil {
				log.Warn().Err(err).Str("path", previousSummaryPath).Msg("failed to read previous run-summary; regression baseline disabled")
			}
			previousChecksSnapshotPath := filepath.Join(cfg.OutputDirFiltered, "checks-snapshot.json")
			previousChecksSnapshot, hasPreviousChecksSnapshot, err := loadChecksSnapshotJSON(previousChecksSnapshotPath)
			if err != nil {
				log.Warn().Err(err).Str("path", previousChecksSnapshotPath).Msg("failed to read previous checks snapshot; drill-down baseline disabled")
			}
			if hasPreviousSummary {
				cfg.PreviousClusterFailCount = failCountByCluster(previousSummary)
				log.Info().
					Str("previous_timestamp", previousSummary.Timestamp).
					Int("previous_clusters", len(previousSummary.Clusters)).
					Msg("loaded previous run-summary baseline")
			} else {
				cfg.PreviousClusterFailCount = map[string]int{}
			}

			// Fast replay mode: skip API, parse existing logs and render everything
			if cmd.Flags().Changed("replay") && viper.GetBool("replay") {
				var agg []AggBlock
				var clusterFiles []struct{ Cluster, HTML, CSV string }
				excludedByCluster := make(map[string][]ExcludedAlert)

				for _, cluster := range cfg.Clusters {
					// Ensure filtered log exists
					filtered := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
					if _, err := os.Stat(filtered); err != nil {
						// Try to build it from raw ncc log
						raw := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
						if _, err2 := os.Stat(raw); err2 == nil {
							if _, err3 := filterBlocksToFile(OSFS{}, raw, filtered); err3 != nil {
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
					data, err := os.ReadFile(filtered)
					if err != nil {
						log.Error().Str("cluster", cluster).Err(err).Msg("replay: read filtered failed")
						continue
					}
					blocks, err := ParseSummary(string(data))
					if err != nil {
						log.Error().Str("cluster", cluster).Err(err).Msg("replay: parse filtered failed")
						continue
					}
					if len(cfg.SeverityFilter) > 0 {
						originalCount := len(blocks)
						blocks = filterBlocksBySeverity(blocks, cfg.SeverityFilter)
						log.Info().
							Str("cluster", cluster).
							Int("original", originalCount).
							Int("filtered", len(blocks)).
							Strs("severities", cfg.SeverityFilter).
							Msg("replay: applied severity filter")
					}
					if len(cfg.ExcludeAlertTitles) > 0 {
						originalCount := len(blocks)
						filteredBlocks, excludedAlerts, err := filterBlocksByTitle(blocks, cfg.ExcludeAlertTitles, cfg.ExcludeAlertMatchMode)
						if err != nil {
							log.Error().Str("cluster", cluster).Err(err).Msg("replay: apply alert title exclusion filter failed")
							continue
						}
						blocks = filteredBlocks
						excludedByCluster[cluster] = excludedAlerts
						log.Info().
							Str("cluster", cluster).
							Int("original", originalCount).
							Int("filtered", len(blocks)).
							Int("excluded", len(excludedAlerts)).
							Str("mode", cfg.ExcludeAlertMatchMode).
							Strs("titles", cfg.ExcludeAlertTitles).
							Msg("replay: applied alert title exclusion filter")
					}

					counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
					for _, b := range blocks {
						sev := b.Severity
						if sev == "" {
							sev = "INFO"
						}
						counts[sev]++
					}
					overviewReplay := fmt.Sprintf("NCC replay for cluster %s. FAIL: %d, WARN: %d, ERR: %d, INFO: %d. Total: %d checks (from existing log).",
						cluster, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], len(blocks))
					replaySummary := NotificationSummary{
						Cluster:     cluster,                           // from replay filename or param
						StartedAt:   time.Now().Add(-10 * time.Minute), // estimate
						FinishedAt:  time.Now(),
						FailCount:   counts["FAIL"],
						WarnCount:   counts["WARN"],
						ErrCount:    counts["ERR"],
						InfoCount:   counts["INFO"],
						TotalChecks: len(blocks),
						OutputFiles: []string{filtered},
						Overview:    overviewReplay,
					}

					subj := fmt.Sprintf("NCC REPLAY %s: FAIL=%d WARN=%d",
						replaySummary.Cluster, replaySummary.FailCount, replaySummary.WarnCount)
					body := overviewReplay + "\n\n" + fmt.Sprintf("REPLAY MODE - From existing log:\nFAIL: %d | WARN: %d | Total: %d\nLog: %s",
						replaySummary.FailCount, replaySummary.WarnCount, len(blocks), filtered)
					subj, body = applyEmailTemplates(cfg, subj, body, replaySummary, log.Logger)

					// Per-cluster outputs: generate HTML and CSV before notifications so we can attach HTML to email
					base := filtered
					var replayHTMLPath string
					rawPath := filepath.Join(cfg.OutputDirLogs, fmt.Sprintf("%s.log", cluster))
					var meta HTMLMeta
					metaLoaded := false
					loadMeta := func() HTMLMeta {
						if !metaLoaded {
							m, mErr := parseNCCHeader(rawPath)
							if mErr != nil {
								log.Warn().Err(mErr).Str("rawPath", rawPath).Msg("replay: parseNCCHeader failed, using empty meta")
								meta = HTMLMeta{}
							} else {
								meta = m
							}
							metaLoaded = true
						}
						return meta
					}
					if cfg.PromEnabled {
						if err := writePrometheusFile(OSFS{}, cfg.PromDir, cluster, blocks); err != nil {
							log.Error().Str("cluster", cluster).Err(err).Msg("replay write Prometheus .prom failed")
						} else {
							log.Info().Str("cluster", cluster).Str("prom_dir", cfg.PromDir).Msg("replay: Prometheus .prom written")
						}
					}
					for _, f := range cfg.OutputFormats {
						switch strings.ToLower(strings.TrimSpace(f)) {
						case "html":
							htmlFile := base + ".html"
							replayHTMLPath = htmlFile
							if err := generateHTML(OSFS{}, rowsFromBlocks(blocks), htmlFile, loadMeta()); err != nil {
								log.Error().Err(err).Str("file", htmlFile).Msg("replay: write HTML failed")
								replayHTMLPath = ""
							}
						case "csv":
							_ = generateCSV(OSFS{}, blocks, base+".csv")
						case "json":
							_ = generateJSON(OSFS{}, blocks, base+".json", loadMeta())
						case "markdown":
							_ = generateMarkdown(OSFS{}, blocks, base+".md", loadMeta())
						case "sarif":
							_ = generateSARIF(OSFS{}, blocks, base+".sarif")
						}
					}

					attachPath := ""
					if cfg.EmailAttachHTML && replayHTMLPath != "" {
						attachPath = replayHTMLPath
					}
					if cfg.WebhookIncludeHTML && replayHTMLPath != "" {
						if b, err := os.ReadFile(replayHTMLPath); err == nil {
							replaySummary.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
						}
					}
					ctx := context.Background()
					httpc := NewHTTPClient(cfg)
					if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
						log.Info().Str("cluster", cluster).Str("reason", reason).Msg("replay notifications suppressed")
					} else {
						if err := sendEmailWithRetry(cfg, subj, body, attachPath); err != nil {
							log.Error().Err(err).Str("cluster", cluster).Msg("replay email failed")
						}
						if err := sendWebhookWithRetry(ctx, httpc, cfg, replaySummary); err != nil {
							log.Error().Err(err).Str("cluster", cluster).Msg("replay webhook failed")
						}
						log.Info().Int("fail", replaySummary.FailCount).Int("warn", replaySummary.WarnCount).
							Str("cluster", cluster).Msg("replay notifications sent")
					}

					clusterFiles = append(clusterFiles, struct{ Cluster, HTML, CSV string }{
						Cluster: cluster,
						HTML:    filepath.Base(base + ".html"),
						CSV:     filepath.Base(base + ".csv"),
					})
					meta = loadMeta()

					for _, b := range blocks {
						agg = append(agg, AggBlock{
							Cluster:        cluster,
							Severity:       b.Severity,
							Check:          b.CheckName,
							Detail:         b.DetailRaw,
							ClusterName:    meta.ClusterName,
							ClusterVersion: meta.ClusterVersion,
							NCCVersion:     meta.NCCVersion,
						})
					}
				}

				if err := writeAggregatedHTMLSingle(OSFS{}, cfg.OutputDirFiltered, agg, clusterFiles, cfg); err != nil {
					log.Error().Err(err).Msg("replay: write aggregated HTML failed")
					return err
				}
				if len(cfg.ExcludeAlertTitles) > 0 {
					if err := writeExcludedAlertsAuditJSON(OSFS{}, cfg.OutputDirFiltered, cfg.ExcludeAlertMatchMode, cfg.ExcludeAlertTitles, excludedByCluster); err != nil {
						log.Error().Err(err).Msg("replay: write excluded-alerts.json failed (non-fatal)")
					}
				}
				if deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, time.Now()); err != nil {
					log.Error().Err(err).Msg("replay: artifact retention failed (non-fatal)")
				} else if deleted > 0 {
					log.Info().Int("deleted", deleted).Int("retain_days", cfg.ArtifactRetainDays).Int("retain_max_files", cfg.ArtifactRetainMaxFiles).Msg("replay: artifact retention applied")
				}
				log.Info().Int("clusters", len(clusterFiles)).Int("rows", len(agg)).Msg("replay: aggregated page generated")
				return nil
			}

			// Inside RunE, after setting up cfg, fs, httpc...
			fmt.Println("You have accepted T&C, Check using --tc flag")

			// Create root context with graceful shutdown support
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Setup graceful shutdown signal handling
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigChan)
			go func() {
				sig := <-sigChan
				log.Warn().Str("signal", sig.String()).Msg("received shutdown signal, initiating graceful shutdown")
				fmt.Fprintf(os.Stderr, "\n⚠️  Received %s signal. Initiating graceful shutdown...\n", sig.String())
				cancel() // Cancel root context to stop all operations
			}()

			// Live progress bars are for an interactive terminal only. When
			// stdout is redirected to a file (the systemd-timer scheduler and
			// the api-server both do `... >> ncc-runner.log 2>&1`), mpb's ANSI
			// cursor-control frames (\x1b[<n>A, \x1b[J) pile up and a viewer
			// collapses them to a single ~18-line final frame — making the
			// runner log look truncated. Discard the bar output off-TTY; the
			// structured zerolog lines below (phase change / cluster run
			// completed / cluster run failed) still record per-cluster progress.
			progressOpts := []mpb.ContainerOption{mpb.WithWidth(80)}
			onTTY := term.IsTerminal(int(os.Stdout.Fd()))
			if !onTTY {
				progressOpts = append(progressOpts, mpb.WithOutput(io.Discard))
			}
			// Drop finished bars when nobody watches them or there are too many
			// to show: every kept bar is redrawn on each refresh, which was ~15%
			// of a 7,000-cluster run's CPU with all 14,000 bars kept.
			dropDoneBars := !onTTY || len(cfg.Clusters) > 200
			var doneBarOpts []mpb.BarOption
			if dropDoneBars {
				doneBarOpts = append(doneBarOpts, mpb.BarRemoveOnComplete())
			}
			p := mpb.New(progressOpts...)
			defer func() {
				// Ensure progress bars are cleaned up on exit
				p.Wait()
			}()
			// max-parallel: 0 = auto, sized to the fleet now that it is known.
			if cfg.MaxParallel == 0 {
				cfg.MaxParallel = autoMaxParallel(len(cfg.Clusters))
				log.Info().Int("max_parallel", cfg.MaxParallel).Int("clusters", len(cfg.Clusters)).Msg("max-parallel auto")
			}
			resetAdaptiveParallelism(cfg.MaxParallel)
			gateChanged := make(chan struct{}, 1)
			setAdaptiveParallelismNotify(gateChanged)
			defer setAdaptiveParallelismNotify(nil)
			notifyGateChange := func() {
				select {
				case gateChanged <- struct{}{}:
				default:
				}
			}
			sem := make(chan struct{}, cfg.MaxParallel)
			var activeWorkers int32
			var wg sync.WaitGroup          // clusters of the current pass still talking to their cluster
			var procPending sync.WaitGroup // fetched summaries queued for or in the processing pool
			var results chan ClusterResult // final results, in cluster order (built after all passes)
			runStart := time.Now()
			notify.ResetMetrics()

			// Run-activity heartbeat: write a tiny per-pid marker into the output
			// dir for the lifetime of this run. The api-server reads these to
			// surface runs it did NOT spawn — chiefly systemd-timer/cron
			// *scheduled* runs — in its Active Runs view (it otherwise only knows
			// about runs it launched itself). Removed on exit; stale markers from a
			// crash are cleaned up by the reader once the pid is gone.
			if hbDir := strings.TrimSpace(cfg.OutputDirFiltered); hbDir != "" {
				hbSource := strings.TrimSpace(os.Getenv("NCC_RUN_SOURCE"))
				if hbSource == "" {
					hbSource = "manual"
				}
				hbPath := filepath.Join(hbDir, fmt.Sprintf(".ncc-run-active-%d.json", os.Getpid()))
				if hb, err := json.Marshal(map[string]interface{}{
					"pid":        os.Getpid(),
					"started_at": runStart.UTC().Format(time.RFC3339),
					"clusters":   cfg.Clusters,
					"source":     hbSource,
				}); err == nil {
					_ = os.WriteFile(hbPath, hb, 0o644)
					defer func() { _ = os.Remove(hbPath) }()
				}
			}

			// Opt-in OpenTelemetry tracing (no-op unless an OTLP endpoint is
			// configured). Spans are emitted per cluster below.
			otelShutdown, otelErr := trace.Init(ctx, Version)
			if otelErr != nil {
				log.Warn().Err(otelErr).Msg("opentelemetry tracing init failed; continuing without tracing")
			}
			defer func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = otelShutdown(shutdownCtx)
			}()

			// ---- Resume an interrupted run (--resume) ----
			// Clusters it already finished are rebuilt from their saved raw NCC
			// summaries instead of being run again.
			resumed := map[string]ClusterResult{}
			if cfg.Resume {
				done, why := readRunJournal(runJournalPath(cfg), cfg.Clusters, time.Now())
				if why != "" {
					log.Info().Str("reason", why).Msg("resume: starting a full run")
					fmt.Fprintf(os.Stderr, "Resume: %s; checking every cluster.\n", why)
				}
				for cl := range done {
					if r, ok := resumedClusterResult(cfg, cl); ok {
						resumed[cl] = r
					}
				}
				if len(resumed) > 0 {
					log.Info().Int("reused", len(resumed)).Int("clusters", len(cfg.Clusters)).Msg("resume: reusing clusters the interrupted run finished")
					fmt.Fprintf(os.Stderr, "Resume: reusing %d of %d clusters from the interrupted run.\n", len(resumed), len(cfg.Clusters))
				}
			}
			journal, jErr := openRunJournal(cfg, len(resumed) > 0, time.Now())
			if jErr != nil {
				log.Warn().Err(jErr).Msg("run journal unavailable; this run cannot be resumed if interrupted")
			}
			runCompleted := false
			defer func() { journal.close(runCompleted) }()

			// ---- Results ----
			// Recorded as each cluster finishes (synchronously, so a pass is fully
			// recorded when it returns); the latest result per cluster wins, so a
			// retry replaces a failure. Finished clusters go into the run journal.
			var collectedMu sync.Mutex
			collected := make(map[string]ClusterResult, len(cfg.Clusters))
			for cl, r := range resumed {
				collected[cl] = r
			}
			record := func(r ClusterResult) {
				collectedMu.Lock()
				collected[r.Cluster] = r
				collectedMu.Unlock()
				if r.Err == nil {
					journal.markDone(r.Cluster)
				}
			}

			// ---- Processing pool ----
			// Writing a cluster's files and sending its notifications needs no
			// cluster connection, so it runs here, off the max-parallel slots: a
			// slow SMTP server or webhook no longer holds a cluster slot.
			procJobs := make(chan clusterProcessJob, processingWorkers(cfg.MaxParallel))
			var procWorkers sync.WaitGroup
			for i := 0; i < cap(procJobs); i++ {
				procWorkers.Add(1)
				go func() {
					defer procWorkers.Done()
					for job := range procJobs {
						func() {
							defer procPending.Done()
							defer func() {
								if r := recover(); r != nil {
									job.finish(false)
									log.Error().Interface("panic", r).Stack().Str("cluster", job.cluster).Msg("cluster processing panic")
									panicErr := fmt.Errorf("panic: %v", r)
									record(ClusterResult{Cluster: job.cluster, Err: panicErr, ErrorClass: classifyClusterError(panicErr)})
								}
							}()
							runOut, err := processClusterSummary(ctx, cfg, fs, httpc, job.cluster, job.fetched, job.setPhase)
							if err != nil {
								job.setPhase("failed")
								job.finish(false)
								log.Error().Str("cluster", job.cluster).Err(err).Str("error_class", string(classifyClusterError(err))).Float64("duration_s", clusterElapsed(job.cluster)).Msg("cluster run failed")
								fmt.Fprintf(os.Stderr, "\n❌ Cluster %s failed: %v\n", job.cluster, err)
								record(ClusterResult{Cluster: job.cluster, Err: err, ErrorClass: classifyClusterError(err)})
								return
							}
							job.setPhase("done")
							job.finish(true)
							fails, warns := 0, 0
							for _, blk := range runOut.Blocks {
								switch strings.ToUpper(blk.Severity) {
								case "FAIL", "ERR":
									fails++
								case "WARN":
									warns++
								}
							}
							log.Info().Str("cluster", job.cluster).Int("findings", len(runOut.Blocks)).Int("fail_or_error", fails).Int("warn", warns).Int("excluded_by_title", len(runOut.ExcludedByTitle)).Float64("duration_s", clusterElapsed(job.cluster)).Msg("cluster run completed")
							record(ClusterResult{Cluster: job.cluster, Blocks: runOut.Blocks, ExcludedByTitle: runOut.ExcludedByTitle, Meta: runOut.Meta})
						}()
					}
				}()
			}

			// dispatch runs one pass over clusters: each holds a slot only while
			// it starts, polls and reads its NCC task, then hands the summary to
			// the processing pool. It returns once every cluster of the pass is
			// fully processed.
			dispatch := func(clusters []string) error {
				for _, cluster := range clusters {
					clusterUser, clusterPass := credentialsForCluster(cfg, cluster)
					wg.Add(1)
				waitForWorkerSlot:
					for {
						limit := currentAdaptiveParallel(cfg.MaxParallel)
						curActive := int(atomic.LoadInt32(&activeWorkers))
						if curActive < limit {
							break
						}
						select {
						case <-ctx.Done():
							break waitForWorkerSlot
						case <-gateChanged:
						}
					}
					if ctx.Err() != nil {
						wg.Done()
						break
					}
					sem <- struct{}{}
					atomic.AddInt32(&activeWorkers, 1)

					mainBar := p.New(
						100,
						mpb.BarStyle().Rbound("|"),
						append([]mpb.BarOption{
							mpb.PrependDecorators(
								decor.Name(fmt.Sprintf("%-18s", cluster), decor.WC{W: 20, C: decor.DidentRight}),
							),
							mpb.AppendDecorators(
								decor.Percentage(decor.WC{W: 4}),
								decor.Name(" • "),
								decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
							),
						}, doneBarOpts...)...,
					)

					phaseProxy := &proxyDecorator{text: "starting"}

					phaseBar := p.New(
						1,
						mpb.NopStyle(),
						append([]mpb.BarOption{
							mpb.PrependDecorators(decor.Name(strings.Repeat(" ", 20))),
							mpb.AppendDecorators(phaseProxy),
						}, doneBarOpts...)...,
					)

					go func(cl string, user string, pass string, b *mpb.Bar, phase *proxyDecorator, phaseBar *mpb.Bar) {
						defer wg.Done()
						slotHeld := true
						releaseSlot := func() {
							if slotHeld {
								slotHeld = false
								<-sem
								atomic.AddInt32(&activeWorkers, -1)
								notifyGateChange()
							}
						}
						defer releaseSlot()
						finish := func(ok bool) {
							if ok {
								b.SetCurrent(100)
								b.SetTotal(100, true)
							} else {
								b.Abort(dropDoneBars)
								b.SetTotal(b.Current(), true)
							}
							phaseBar.SetCurrent(1)     // Set current to match total
							phaseBar.SetTotal(1, true) // Complete phaseBar
						}
						defer func() {
							if r := recover(); r != nil {
								finish(false)
								log.Error().Interface("panic", r).Stack().Str("cluster", cl).Msg("cluster goroutine panic")
								panicErr := fmt.Errorf("panic: %v", r)
								record(ClusterResult{Cluster: cl, Blocks: nil, Err: panicErr, ErrorClass: classifyClusterError(panicErr)})
							}
						}()

						reqCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
						defer cancel()
						clusterStartedAt.Store(cl, time.Now())
						log.Info().Str("cluster", cl).
							Int("active_workers", int(atomic.LoadInt32(&activeWorkers))).
							Int("parallel_limit", currentAdaptiveParallel(cfg.MaxParallel)).
							Dur("timeout", cfg.Timeout).
							Bool("own_credentials", user != cfg.Username).
							Msg("cluster run started")

						reqCtx, span := trace.StartCluster(reqCtx, cl)
						defer span.End()

						onPct := func(pct int) { b.SetCurrent(int64(pct)) }
						setPhase := func(text string) {
							phase.SetText(text)
							log.Info().Str("cluster", cl).Str("phase", text).Msg("phase change")
						}

						fetched, err := fetchClusterSummary(reqCtx, cfg, httpc, cl, user, pass, onPct, setPhase)
						if err != nil {
							trace.RecordError(span, err)
							setPhase("failed")
							finish(false)
							// Log with detailed error information
							log.Error().
								Str("cluster", cl).
								Err(err).
								Str("error_type", fmt.Sprintf("%T", err)).
								Str("error_class", string(classifyClusterError(err))).
								Float64("duration_s", clusterElapsed(cl)).
								Msg("cluster run failed")
							// Also print to console for visibility
							fmt.Fprintf(os.Stderr, "\n❌ Cluster %s failed: %v\n", cl, err)
							if ctx.Err() != nil {
								err = fmt.Errorf("%w: %v", errRunCancelled, err)
							}
							record(ClusterResult{Cluster: cl, Blocks: nil, Err: err, ErrorClass: classifyClusterError(err)})
							return
						}
						log.Info().Str("cluster", cl).Float64("fetch_s", clusterElapsed(cl)).Msg("cluster results fetched; processing")
						setPhase("queued")
						procPending.Add(1)
						// Waits while the processing pool is busy, still holding the
						// slot: that caps how many fetched summaries sit in memory.
						procJobs <- clusterProcessJob{cluster: cl, fetched: fetched, setPhase: setPhase, finish: finish}
						releaseSlot()
					}(cluster, clusterUser, clusterPass, mainBar, phaseProxy, phaseBar) // Pass phaseBar and per-cluster credentials
				}

				// Wait for workers with context cancellation support
				done := make(chan struct{})
				go func() {
					wg.Wait()
					procPending.Wait()
					close(done)
				}()

				select {
				case <-done:
					// All workers completed normally
				case <-ctx.Done():
					// Context cancelled (shutdown signal received)
					log.Warn().Msg("context cancelled, waiting for workers to finish")
					fmt.Fprintln(os.Stderr, "⏳ Waiting for in-progress operations to complete...")
					// Wait with timeout for workers to finish
					waitCtx, waitCancel := context.WithTimeout(context.Background(), shutdownTimeout)
					defer waitCancel()
					select {
					case <-done:
						log.Info().Msg("all workers completed after cancellation")
					case <-waitCtx.Done():
						log.Error().Msg("timeout waiting for workers after cancellation")
						fmt.Fprintln(os.Stderr, "❌ Timeout waiting for operations to complete")
						return fmt.Errorf("graceful shutdown timeout: operations did not complete in time")
					}
				}
				return nil
			}

			pending := make([]string, 0, len(cfg.Clusters))
			for _, cl := range cfg.Clusters {
				if _, ok := resumed[cl]; !ok {
					pending = append(pending, cl)
				}
			}
			if err := dispatch(pending); err != nil {
				return err
			}

			// ---- Retry pass ----
			// At fleet scale some clusters always hit a timeout or a network blip;
			// give those another go in the same run rather than leaving them out
			// of the report until the next one.
			for attempt := 1; attempt <= cfg.RetryFailedClusters && ctx.Err() == nil; attempt++ {
				var retry []string
				collectedMu.Lock()
				for _, cl := range cfg.Clusters {
					if r, ok := collected[cl]; ok && retryableClusterError(r) {
						retry = append(retry, cl)
					}
				}
				collectedMu.Unlock()
				if len(retry) == 0 {
					break
				}
				log.Warn().Int("clusters", len(retry)).Int("attempt", attempt).Msg("retrying clusters that failed with a timeout, network error or rate limit")
				fmt.Fprintf(os.Stderr, "\n↻ Retrying %s that failed with a timeout, network error or rate limit (attempt %d of %d)\n", plural.Count(len(retry), "cluster"), attempt, cfg.RetryFailedClusters)
				select {
				case <-ctx.Done():
				case <-time.After(time.Duration(attempt) * 10 * time.Second):
				}
				if ctx.Err() != nil {
					break
				}
				if err := dispatch(retry); err != nil {
					return err
				}
			}

			close(procJobs)
			procWorkers.Wait()
			// Every cluster was attempted (a shutdown stops before that).
			runCompleted = ctx.Err() == nil

			// Hand the final results to the aggregation below in cluster order.
			ordered := make(chan ClusterResult, len(collected))
			for _, cl := range cfg.Clusters {
				if r, ok := collected[cl]; ok {
					ordered <- r
				}
			}
			close(ordered)
			results = ordered

			var failed []string
			var agg []AggBlock
			var clusterFiles []struct{ Cluster, HTML, CSV string }
			perCluster := make([]RunClusterSummary, 0, len(cfg.Clusters))
			checksSnapshot := ChecksSnapshotJSON{
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Clusters:  make([]ClusterChecksSnapshot, 0, len(cfg.Clusters)),
			}
			failureCounts := newFailureClassCounts()
			excludedByCluster := make(map[string][]ExcludedAlert)

			for r := range results {
				perCluster = append(perCluster, buildRunClusterSummary(r))
				checksSnapshot.Clusters = append(checksSnapshot.Clusters, buildClusterChecksSnapshotFromResult(r))
				incrementFailureClassCount(failureCounts, r)
				if r.Err != nil {
					failed = append(failed, r.Cluster)
					// Surface the failure as a real, visible UNKNOWN-severity row
					// rather than letting the cluster silently disappear from the
					// aggregated report (see clusterRunFailedCheckName doc comment).
					agg = append(agg, AggBlock{
						Cluster:  r.Cluster,
						Severity: "UNKNOWN",
						Check:    clusterRunFailedCheckName,
						Detail:   r.Err.Error() + " — " + runFailedRemediation(r.Err, r.ErrorClass),
					})
					continue
				}
				if len(r.ExcludedByTitle) > 0 {
					excludedByCluster[r.Cluster] = append(excludedByCluster[r.Cluster], r.ExcludedByTitle...)
				}

				meta := r.Meta

				for _, b := range r.Blocks {
					agg = append(agg, AggBlock{
						Cluster:        r.Cluster,
						Severity:       b.Severity,
						Check:          b.CheckName,
						Detail:         b.DetailRaw,
						ClusterName:    meta.ClusterName,
						ClusterVersion: meta.ClusterVersion,
						NCCVersion:     meta.NCCVersion,
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
				// Release per-cluster parsed blocks after aggregation to limit peak memory.
				r.Blocks = nil
			}
			sort.Slice(checksSnapshot.Clusters, func(i, j int) bool { return checksSnapshot.Clusters[i].Address < checksSnapshot.Clusters[j].Address })

			runDuration := time.Since(runStart)
			indexPath := filepath.Join(cfg.OutputDirFiltered, "index.html")
			log.Info().
				Int("clusters_ok", len(clusterFiles)).
				Int("clusters_failed", len(failed)).
				Float64("duration_s", runDuration.Seconds()).
				Str("index_html", indexPath).
				Msg("run summary")

			exitCode := 0
			if len(failed) > 0 {
				if len(clusterFiles) > 0 {
					exitCode = 3
				} else {
					exitCode = 1
				}
			}
			avgHealth := 0
			minHealth := 100
			healthClusters := 0
			for _, c := range perCluster {
				if !c.OK {
					continue
				}
				avgHealth += c.HealthScore
				healthClusters++
				if c.HealthScore < minHealth {
					minHealth = c.HealthScore
				}
			}
			if healthClusters > 0 {
				avgHealth = int(math.Round(float64(avgHealth) / float64(healthClusters)))
			} else {
				minHealth = 0
			}

			runSummary := RunSummaryJSON{
				SchemaVersion:  "3.0",
				RunID:          strings.TrimSpace(os.Getenv("NCC_RUN_ID")),
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
				DurationS:      runDuration.Seconds(),
				ClustersOK:     len(clusterFiles),
				ClustersFailed: len(failed),
				FailedClusters: failed,
				Clusters:       perCluster,
				ExitCode:       exitCode,
				Status:         runSummaryStatus(exitCode, len(clusterFiles)),
				IndexHTML:      indexPath,
				TotalChecks:    len(agg),
				AvgHealthScore: avgHealth,
				MinHealthScore: minHealth,
				FailureClasses: failureCounts,
				Source:         strings.TrimSpace(os.Getenv("NCC_RUN_SOURCE")),
			}
			if err := writeRunSummaryJSON(fs, cfg.OutputDirFiltered, runSummary); err != nil {
				log.Error().Err(err).Msg("write run-summary.json failed (non-fatal)")
			}
			if err := writeChecksSnapshotJSON(fs, cfg.OutputDirFiltered, checksSnapshot); err != nil {
				log.Error().Err(err).Msg("write checks-snapshot.json failed (non-fatal)")
			}
			if err := writeNCCRunRecordJSON(fs, cfg.OutputDirFiltered, runSummary); err != nil {
				log.Error().Err(err).Msg("write ncc-run-record.json failed (non-fatal)")
			}
			regression := computeRegressionSummary(previousSummary, hasPreviousSummary, runSummary)
			if err := writeRegressionSummaryJSON(fs, cfg.OutputDirFiltered, regression); err != nil {
				log.Error().Err(err).Msg("write regression-summary.json failed (non-fatal)")
			} else {
				log.Info().
					Bool("has_regression", regression.HasRegression).
					Int("delta_fail_total", regression.DeltaFailTotal).
					Msg("regression summary generated")
			}
			drillDownDiff := computeDrillDownDiff(previousChecksSnapshot, hasPreviousChecksSnapshot, checksSnapshot)
			if err := writeDrillDownDiffJSON(fs, cfg.OutputDirFiltered, drillDownDiff); err != nil {
				log.Error().Err(err).Msg("write drilldown-diff.json failed (non-fatal)")
			}
			if cfg.PromEnabled {
				if err := writeNotificationMetricsFile(fs, cfg.PromDir); err != nil {
					log.Error().Err(err).Msg("write notifications.prom failed (non-fatal)")
				}
			}
			historySnapshots, err := loadRecentCheckSnapshots(cfg.RunHistoryDir, cfg.FlakyLookbackRuns-1)
			if err != nil {
				log.Warn().Err(err).Str("history_dir", cfg.RunHistoryDir).Msg("load check snapshot history failed (non-fatal)")
				historySnapshots = nil
			}
			historySnapshots = append(historySnapshots, checksSnapshot)
			flaky := computeFlakyChecks(historySnapshots, cfg.FlakyMinTransitions)
			if err := writeFlakyChecksJSON(fs, cfg.OutputDirFiltered, flaky); err != nil {
				log.Error().Err(err).Msg("write flaky-checks.json failed (non-fatal)")
			}
			slo := buildSLODashboard(runSummary)
			if err := writeSLODashboardJSON(fs, cfg.OutputDirFiltered, slo); err != nil {
				log.Error().Err(err).Msg("write slo-dashboard.json failed (non-fatal)")
			}
			// Write index.html after run-summary/artifacts are written, so embedded RUN_SUMMARY
			// and artifact links in the page always represent the current run.
			if len(agg) > 0 {
				if err := writeAggregatedHTMLSingle(fs, cfg.OutputDirFiltered, agg, clusterFiles, cfg); err != nil {
					log.Error().Err(err).Msg("write aggregated HTML failed")
					return fmt.Errorf("write aggregated HTML: %w", err)
				}
			} else if len(failed) > 0 {
				if err := writeAllClustersFailedHTML(fs, cfg.OutputDirFiltered, failed); err != nil {
					log.Error().Err(err).Msg("write all-failed HTML failed (non-fatal)")
				}
			}
			if len(cfg.ExcludeAlertTitles) > 0 {
				if err := writeExcludedAlertsAuditJSON(fs, cfg.OutputDirFiltered, cfg.ExcludeAlertMatchMode, cfg.ExcludeAlertTitles, excludedByCluster); err != nil {
					log.Error().Err(err).Msg("write excluded-alerts.json failed (non-fatal)")
				}
			}
			if cfg.SingleReport {
				src := filepath.Join(cfg.OutputDirFiltered, "index.html")
				dst := filepath.Join(cfg.OutputDirFiltered, "ncc-report-single.html")
				if err := copyFile(src, dst); err != nil {
					log.Error().Err(err).Str("src", src).Str("dst", dst).Msg("single-report copy failed (non-fatal)")
				} else {
					log.Info().Str("file", dst).Msg("single-file report generated")
				}
			}
			if cfg.RunHistoryEnabled {
				if runDir, err := writeRunHistorySnapshot(cfg); err != nil {
					log.Error().Err(err).Str("history_dir", cfg.RunHistoryDir).Msg("write run history snapshot failed (non-fatal)")
				} else {
					log.Info().
						Str("history_dir", runDir).
						Int("retain_last", cfg.RetainLastRuns).
						Int("retain_days", cfg.RetainDays).
						Msg("run history snapshot created")
				}
			}
			if deleted, err := applyArtifactRetentionPolicies(cfg.OutputDirFiltered, cfg.ArtifactRetainDays, cfg.ArtifactRetainMaxFiles, time.Now()); err != nil {
				log.Error().Err(err).Msg("artifact retention failed (non-fatal)")
			} else if deleted > 0 {
				log.Info().Int("deleted", deleted).Int("retain_days", cfg.ArtifactRetainDays).Int("retain_max_files", cfg.ArtifactRetainMaxFiles).Msg("artifact retention applied")
			}
			if deleted, err := applyRawLogRetention(cfg.OutputDirLogs, cfg.OutputDirFiltered, cfg.RawLogRetainDays, time.Now()); err != nil {
				log.Error().Err(err).Msg("raw log retention failed (non-fatal)")
			} else if deleted > 0 {
				log.Info().Int("deleted", deleted).Int("retain_days", cfg.RawLogRetainDays).Msg("raw log retention applied")
			}

			if cfg.NotifyDigest {
				if cfg.NotifyOnRegression && !regression.HasRegression {
					log.Info().Msg("notify-on-regression enabled: skipping digest notification (no regression detected)")
				} else if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
					log.Info().Str("reason", reason).Msg("digest notifications suppressed by quiet-hours/maintenance-window")
				} else {
					digestCounts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
					for _, row := range agg {
						sev := strings.ToUpper(strings.TrimSpace(row.Severity))
						if sev == "" {
							sev = "INFO"
						}
						if _, ok := digestCounts[sev]; ok {
							digestCounts[sev]++
						} else {
							digestCounts["INFO"]++
						}
					}
					overview := fmt.Sprintf("Run completed in %s. Clusters OK: %d, Failed: %d. Alerts (filtered): FAIL=%d WARN=%d ERR=%d INFO=%d Total=%d.",
						runDuration.Round(time.Second), len(clusterFiles), len(failed),
						digestCounts["FAIL"], digestCounts["WARN"], digestCounts["ERR"], digestCounts["INFO"], len(agg))
					if len(failed) > 0 {
						overview += fmt.Sprintf(" Failed: %v.", failed)
					}
					subj := fmt.Sprintf("NCC Run: OK=%d FAIL=%d", len(clusterFiles), len(failed))
					bodyEmail := overview + "\n\nIndex report: " + indexPath
					// How old the data is for each cluster this run could not reach.
					if cfg.RunHistoryEnabled {
						if lines := unreachedClusterLines(cfg.RunHistoryDir, failed, time.Now()); len(lines) > 0 {
							bodyEmail += "\n\nData age:\n  - " + strings.Join(lines, "\n  - ")
						}
					}
					attachPath := ""
					if cfg.EmailAttachHTML {
						if _, err := os.Stat(indexPath); err == nil {
							attachPath = indexPath
						}
					}
					digestSummary := NotificationSummary{
						Cluster:     "run",
						StartedAt:   runStart,
						FinishedAt:  time.Now(),
						FailCount:   digestCounts["FAIL"],
						WarnCount:   digestCounts["WARN"],
						ErrCount:    digestCounts["ERR"],
						InfoCount:   digestCounts["INFO"],
						TotalChecks: len(agg),
						Overview:    overview,
					}
					subj, bodyEmail = applyEmailTemplates(cfg, subj, bodyEmail, digestSummary, log.Logger)
					if err := sendEmailWithRetry(cfg, subj, bodyEmail, attachPath); err != nil {
						log.Error().Err(err).Msg("digest email failed")
					} else if cfg.EmailEnabled && cfg.SMTPServer != "" && len(cfg.EmailTo) > 0 {
						log.Info().Int("recipients", len(cfg.EmailTo)).Bool("attachment", attachPath != "").Msg("digest email sent")
					}
					if cfg.WebhookIncludeHTML {
						if b, err := os.ReadFile(indexPath); err == nil {
							digestSummary.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
						}
					}
					if err := sendWebhookWithRetry(ctx, httpc, cfg, digestSummary); err != nil {
						log.Error().Err(err).Msg("digest webhook failed")
					}
					if err := sendSlackWithRetry(ctx, httpc, cfg, digestSummary); err != nil {
						log.Error().Err(err).Msg("digest slack failed")
					}
				}
			}

			// Check if context was cancelled during execution
			if ctx.Err() != nil {
				log.Warn().Err(ctx.Err()).Msg("operation cancelled during execution")
				if len(failed) > 0 {
					return fmt.Errorf("operation cancelled: %d clusters failed: %v", len(failed), failed)
				}
				return fmt.Errorf("operation cancelled: %w", ctx.Err())
			}
			policyMetrics := map[string]float64{
				"new-fails":            float64(drillDownDiff.NewFailCount),
				"resolved-fails":       float64(drillDownDiff.ResolvedFailCount),
				"fail-rate":            0,
				"clusters-failed":      float64(len(failed)),
				"max-cluster-failures": float64(len(failed)),
				"regressions":          0,
				"flaky-checks":         float64(flaky.TotalFlakyChecks),
				"min-health-score":     float64(runSummary.MinHealthScore),
				"avg-health-score":     float64(runSummary.AvgHealthScore),
				"timeout-clusters":     float64(failureCounts["timeout"]),
				"auth-failures":        float64(failureCounts["auth"]),
				"network-failures":     float64(failureCounts["network"]),
				"api-failures":         float64(failureCounts["api"]),
				"parser-failures":      float64(failureCounts["parser"]),
				"rate-limit-failures":  float64(failureCounts["rate_limit"]),
				"unknown-failures":     float64(failureCounts["unknown"]),
			}
			if regression.HasRegression {
				policyMetrics["regressions"] = 1
			}
			if runSummary.TotalChecks > 0 {
				policyMetrics["fail-rate"] = (float64(regression.CurrentFailTotal) * 100.0) / float64(runSummary.TotalChecks)
			}
			if len(cfg.PolicyGates) > 0 {
				violations, err := evaluatePolicyGates(cfg.PolicyGates, policyMetrics)
				if err != nil {
					return exitConfig(fmt.Errorf("policy-gates: %w", err))
				}
				if len(violations) > 0 {
					_ = fs.WriteFile(filepath.Join(cfg.OutputDirFiltered, "policy-gates.txt"), []byte(strings.Join(violations, "\n")+"\n"), 0644)
					return fmt.Errorf("policy gate violations: %s", strings.Join(violations, "; "))
				}
			}

			if len(failed) > 0 {
				printFailureResolutionHints(failureCounts)
				if len(clusterFiles) > 0 {
					log.Warn().Strs("failedClusters", failed).Int("succeeded", len(clusterFiles)).Msg("some clusters failed; aggregated report written for successful clusters")
					fmt.Fprintf(os.Stderr, "Some clusters failed: %v (report written for %s). Exit code 3.\n", failed, plural.Count(len(clusterFiles), "successful cluster"))
					return exitPartial(fmt.Errorf("some clusters failed: %v", failed))
				}
				log.Error().Strs("failedClusters", failed).Msg("all clusters failed")
				return fmt.Errorf("all clusters failed: %v", failed)
			}

			log.Info().Msg("all clusters processed successfully")
			fmt.Printf("All clusters processed successfully\n")
			return nil
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SuggestionsMinimumDistance = 2

	cmd.PersistentFlags().String("nutanix-v4-api-version", defaultNutanixV4APIVersion, "Nutanix v4 REST API path revision for clustermgmt and monitoring (e.g. v4.2, v4.1, v4.0.a1)")
	_ = viper.BindPFlag("nutanix-v4-api-version", cmd.PersistentFlags().Lookup("nutanix-v4-api-version"))
	cmd.PersistentFlags().String("config", "", "Config file path (yaml/json)")
	_ = viper.BindPFlag("config", cmd.PersistentFlags().Lookup("config"))
	cmd.PersistentFlags().Bool("insecure-skip-verify", false, "Skip TLS verify (only for trusted labs)")
	_ = viper.BindPFlag("insecure-skip-verify", cmd.PersistentFlags().Lookup("insecure-skip-verify"))
	cmd.PersistentFlags().String("ca-bundle", "", "Path to a PEM file of extra trusted CA certs (safer than --insecure-skip-verify)")
	_ = viper.BindPFlag("ca-bundle", cmd.PersistentFlags().Lookup("ca-bundle"))
	cmd.PersistentFlags().String("pin-sha256", "", "Comma-separated allowed server cert SHA-256 fingerprints (cert pinning; overrides system trust)")
	_ = viper.BindPFlag("pin-sha256", cmd.PersistentFlags().Lookup("pin-sha256"))
	cmd.PersistentFlags().String("request-timeout", "20s", "Per-request timeout")
	_ = viper.BindPFlag("request-timeout", cmd.PersistentFlags().Lookup("request-timeout"))

	// Deprecated root aliases (kept hidden for backward compatibility).
	cmd.Flags().BoolP("update", "u", false, "Fetch latest release from GitHub and update this binary if a matching asset exists")
	cmd.Flags().Bool("env-info", false, "Display possible environment variables and their current values")
	cmd.Flags().Bool("tc", false, "Display terms and conditions")
	cmd.Flags().BoolP("version", "v", false, "Display version/build metadata")
	_ = cmd.Flags().MarkDeprecated("update", "use `ncc-orchestrator update`")
	_ = cmd.Flags().MarkDeprecated("env-info", "use `ncc-orchestrator env-info`")
	_ = cmd.Flags().MarkDeprecated("tc", "use `ncc-orchestrator terms`")
	_ = cmd.Flags().MarkDeprecated("version", "use `ncc-orchestrator version`")
	_ = cmd.Flags().MarkHidden("update")
	_ = cmd.Flags().MarkHidden("env-info")
	_ = cmd.Flags().MarkHidden("tc")
	_ = cmd.Flags().MarkHidden("version")

	// flags
	cmd.Flags().Bool("skip-preflight-check", false, "Skip default preflight-check before run (not recommended)")
	cmd.Flags().Bool("auto", false, "Enable guided automation: apply safe self-healing fixes before failing")
	cmd.Flags().String("automation-level", "safe-fix", "Automation policy: advisory, safe-fix, full-auto")
	cmd.Flags().String("cluster-source-mode", defaultClusterSourceMode, "Cluster source mode: clusters (direct PE list) or pc (discover PEs from Prism Central targets)")
	cmd.Flags().String("clusters", "", "Comma-separated cluster IPs or FQDNs")
	cmd.Flags().String("clusters-file", "", "Path to cluster file (cluster or cluster,username[,password] per line; overrides clusters when set)")
	cmd.Flags().String("pcs", "", "Comma-separated Prism Central IPs/FQDNs/URLs (used when --cluster-source-mode=pc)")
	cmd.Flags().String("pcs-file", "", "Path to file with one Prism Central IP/FQDN/URL per line (used when --cluster-source-mode=pc)")
	cmd.Flags().String("prism-central-url", "", "Single Prism Central URL/IP/FQDN fallback target (used when --cluster-source-mode=pc and no pcs/pcs-file)")
	cmd.Flags().String("discover-api-version", defaultDiscoverAPIVersion, "Cluster discovery API for pc mode: v4 (GET clustermgmt) or v3 (legacy POST)")
	cmd.Flags().String("username", "admin", "Username for Prism Gateway")
	cmd.Flags().String("password", "", "Password (omit to be prompted)")
	cmd.Flags().String("ncc-api-version", "v4", "NCC API mode: v4 (default) or Legacy (Prism Gateway v1 start-checks only; v1 accepted as alias); use --nutanix-v4-api-version for v4.2 vs v4.0.a1 etc.")
	cmd.Flags().String("timeout", "15m", "Overall per-cluster timeout")
	cmd.Flags().String("poll-interval", "15s", "Polling interval for task status")
	cmd.Flags().String("poll-jitter", "2s", "Additive jitter to polling interval")
	cmd.Flags().Int("max-parallel", 0, "Max clusters checked at once; 0 = auto (one per cluster, up to 500)")
	cmd.Flags().Int("retry-failed-clusters", 1, "Extra passes that re-run clusters which failed with a timeout, network error or rate limit (0-5)")
	cmd.Flags().Bool("resume", false, "Reuse clusters an interrupted run already finished (same cluster list, within 24h)")
	cmd.Flags().String("outputs", "html,csv", "Comma-separated outputs: html,csv,json,markdown,sarif for per-cluster files")
	cmd.Flags().String("output-dir-logs", "nccfiles", "Directory for raw logs")
	cmd.Flags().String("output-dir-filtered", "outputfiles", "Directory for filtered and aggregated results")
	cmd.Flags().Bool("single-report", false, "Also write a single-file report copy at output-dir-filtered/ncc-report-single.html")
	cmd.Flags().String("severity-filter", "", "Comma-separated severities to include (FAIL,WARN,ERR,INFO). Empty = all")
	cmd.Flags().String("exclude-alert-titles", "", "Comma-separated NCC alert titles to exclude from generated reports/HTML")
	cmd.Flags().String("exclude-alert-titles-file", "", "Path to file containing alert titles to exclude (one title per line)")
	cmd.Flags().String("exclude-alert-match-mode", defaultExcludeMatchMode, "Alert title exclusion match mode: exact, contains, regex")
	cmd.Flags().Bool("dry-run", false, "Validate configuration without running checks")
	cmd.Flags().String("log-file", "logs/ncc-runner.log", "Path to log file (rotated)")
	cmd.Flags().String("log-level", "", "Log level (trace/debug/info/warn/error or 0..5)")
	cmd.Flags().Bool("log-http", false, "Enable HTTP request/response dump logs")
	cmd.Flags().Int("retry-max-attempts", 6, "Max retry attempts for HTTP calls")
	cmd.Flags().String("retry-base-delay", "400ms", "Base retry delay (with jitter, exponential)")
	cmd.Flags().String("retry-max-delay", "8s", "Max retry delay cap")
	cmd.Flags().Int("retry-circuit-breaker", defaultRetryCircuitBreaker, "Fail fast after N consecutive retryable failures")
	cmd.Flags().Bool("replay", false, "Replay from existing logs without running NCC")
	cmd.Flags().Int("gen-test-agg", 0, "Generate a test index.html with N clusters for scalability testing (no API calls)")
	_ = cmd.Flags().MarkDeprecated("gen-test-agg", "use `ncc-orchestrator gen-test-agg --clusters <N>`")
	_ = cmd.Flags().MarkHidden("gen-test-agg")
	cmd.Flags().Bool("prom-enabled", false, "Write Prometheus textfile metrics")
	cmd.Flags().Int("max-idle-conns", 0, "Max idle HTTP connections (0 = built-in default)")
	cmd.Flags().Int("max-idle-conns-per-host", 0, "Max idle HTTP connections per host (0 = built-in default)")
	cmd.Flags().Int("max-conns-per-host", 0, "Max HTTP connections per host (0 = unlimited)")
	cmd.Flags().String("idle-conn-timeout", "90s", "How long an idle HTTP connection stays in the pool")
	cmd.Flags().String("email-subject-template", "", "Go text/template for the email subject (empty = built-in)")
	cmd.Flags().String("email-body-template", "", "Go text/template for the email body (empty = built-in)")
	cmd.Flags().String("webhook-template", "", "Go text/template for the webhook body; must render JSON (empty = built-in JSON)")
	cmd.Flags().String("pc-alerts-cache-ttl", "5m", "Prism Central alert cache duration (0 disables the cache)")
	cmd.Flags().String("prom-dir", "promfiles", "Directory for Prometheus metrics (used when prom-enabled=true)")
	cmd.Flags().Bool("run-history", false, "Store each run snapshot in run-history-dir")
	cmd.Flags().String("run-history-dir", "", "Run history directory (default: <output-dir-filtered>/runs)")
	cmd.Flags().Int("retain-last", 0, "When run-history is enabled, keep only the last N runs (0 = unlimited)")
	cmd.Flags().Int("retain-days", 0, "When run-history is enabled, keep runs newer than N days (0 = unlimited)")
	cmd.Flags().Int("artifact-retain-days", 0, "Remove generated artifacts older than N days from output-dir-filtered (0 = unlimited)")
	cmd.Flags().Int("artifact-retain-max-files", 0, "Keep only N newest generated artifacts in output-dir-filtered (0 = unlimited)")
	cmd.Flags().Int("raw-log-retain-days", 0, "Remove raw NCC logs older than N days from output-dir-logs; the newest log and the latest report's clusters are kept (0 = unlimited)")
	cmd.Flags().Bool("notify-on-regression", false, "Only send notifications when FAIL count increases vs previous run-summary")
	cmd.Flags().Bool("adaptive-parallelism", true, "Dynamically reduce/increase effective concurrency based on HTTP 429 responses")
	cmd.Flags().String("policy-gates", "", "Comma-separated policy gates (e.g. new-fails>0,fail-rate>2,min-health-score<90,flaky-checks>0,timeout-clusters>0,auth-failures>0)")
	cmd.Flags().String("quiet-hours", "", "Suppress notifications during local quiet hours, format HH:MM-HH:MM")
	cmd.Flags().String("maintenance-windows", "", "Suppress notifications during RFC3339 windows: start/end[,start/end...]")
	cmd.Flags().Int("flaky-lookback-runs", defaultFlakyLookbackRuns, "Number of recent runs to inspect for flaky check detection")
	cmd.Flags().Int("flaky-min-transitions", defaultFlakyTransitions, "Minimum severity transitions to mark a check as flaky")
	cmd.Flags().Bool("email-enabled", false, "Enable email notifications")
	cmd.Flags().Bool("email-attach-html", false, "Attach per-cluster (or digest) HTML report to notification email")
	cmd.Flags().Bool("notify-digest", false, "Send one email/webhook/slack per run with run overview (and optional index.html attach) instead of per-cluster")
	cmd.Flags().String("smtp-server", "", "SMTP server (smtp.gmail.com)")
	cmd.Flags().String("smtp-port", "587", "SMTP port (587=STARTTLS, 465=SSL)")
	cmd.Flags().String("smtp-user", "", "SMTP username")
	cmd.Flags().String("smtp-password", "", "SMTP password (use env NCC_SMTP_PASSWORD)")
	cmd.Flags().String("email-from", "", "From email address")
	cmd.Flags().String("email-to", "", "Comma-separated recipient emails")
	cmd.Flags().Bool("email-use-tls", true, "Use STARTTLS (recommended)")
	cmd.Flags().Bool("smtp-insecure-skip-verify", false, "Skip SMTP STARTTLS certificate verification (independent of --insecure-skip-verify)")
	cmd.Flags().Bool("webhook-enabled", false, "Enable webhook notifications")
	cmd.Flags().Bool("webhook-include-html", false, "Include per-cluster HTML report as base64 in webhook JSON payload")
	cmd.Flags().String("webhook-url", "", "Webhook endpoint URL")
	cmd.Flags().StringToString("webhook-headers", map[string]string{}, "Webhook headers (key=value)")
	cmd.Flags().String("notification-deadletter-dir", "", "Directory to persist notification payloads that fail to deliver after retries")
	cmd.Flags().Bool("slack-enabled", false, "Enable Slack notifications")
	cmd.Flags().String("slack-webhook-url", "", "Slack webhook URL")
	cmd.Flags().String("slack-channel", "", "Slack channel (optional, uses webhook default if empty)")
	cmd.Flags().String("secrets-provider", "", "Secret source for secret:// refs: env or file")
	cmd.Flags().String("secrets-file", "", "Path to YAML/JSON secrets map when secrets-provider=file")

	// viper bindings
	_ = viper.BindPFlag("skip-preflight-check", cmd.Flags().Lookup("skip-preflight-check"))
	_ = viper.BindPFlag("auto", cmd.Flags().Lookup("auto"))
	_ = viper.BindPFlag("automation-level", cmd.Flags().Lookup("automation-level"))
	_ = viper.BindPFlag("config", cmd.Flags().Lookup("config"))
	_ = viper.BindPFlag("cluster-source-mode", cmd.Flags().Lookup("cluster-source-mode"))
	_ = viper.BindPFlag("clusters", cmd.Flags().Lookup("clusters"))
	_ = viper.BindPFlag("clusters-file", cmd.Flags().Lookup("clusters-file"))
	_ = viper.BindPFlag("pcs", cmd.Flags().Lookup("pcs"))
	_ = viper.BindPFlag("pcs-file", cmd.Flags().Lookup("pcs-file"))
	_ = viper.BindPFlag("prism-central-url", cmd.Flags().Lookup("prism-central-url"))
	_ = viper.BindPFlag("discover-api-version", cmd.Flags().Lookup("discover-api-version"))
	_ = viper.BindPFlag("username", cmd.Flags().Lookup("username"))
	_ = viper.BindPFlag("password", cmd.Flags().Lookup("password"))
	_ = viper.BindPFlag("ncc-api-version", cmd.Flags().Lookup("ncc-api-version"))
	_ = viper.BindPFlag("insecure-skip-verify", cmd.Flags().Lookup("insecure-skip-verify"))
	_ = viper.BindPFlag("timeout", cmd.Flags().Lookup("timeout"))
	_ = viper.BindPFlag("request-timeout", cmd.Flags().Lookup("request-timeout"))
	_ = viper.BindPFlag("poll-interval", cmd.Flags().Lookup("poll-interval"))
	_ = viper.BindPFlag("poll-jitter", cmd.Flags().Lookup("poll-jitter"))
	_ = viper.BindPFlag("max-parallel", cmd.Flags().Lookup("max-parallel"))
	_ = viper.BindPFlag("retry-failed-clusters", cmd.Flags().Lookup("retry-failed-clusters"))
	_ = viper.BindPFlag("resume", cmd.Flags().Lookup("resume"))
	_ = viper.BindPFlag("outputs", cmd.Flags().Lookup("outputs"))
	_ = viper.BindPFlag("output-dir-logs", cmd.Flags().Lookup("output-dir-logs"))
	_ = viper.BindPFlag("output-dir-filtered", cmd.Flags().Lookup("output-dir-filtered"))
	_ = viper.BindPFlag("single-report", cmd.Flags().Lookup("single-report"))
	_ = viper.BindPFlag("log-file", cmd.Flags().Lookup("log-file"))
	_ = viper.BindPFlag("log-level", cmd.Flags().Lookup("log-level"))
	_ = viper.BindPFlag("log-http", cmd.Flags().Lookup("log-http"))
	_ = viper.BindPFlag("retry-max-attempts", cmd.Flags().Lookup("retry-max-attempts"))
	_ = viper.BindPFlag("retry-base-delay", cmd.Flags().Lookup("retry-base-delay"))
	_ = viper.BindPFlag("retry-max-delay", cmd.Flags().Lookup("retry-max-delay"))
	_ = viper.BindPFlag("retry-circuit-breaker", cmd.Flags().Lookup("retry-circuit-breaker"))
	_ = viper.BindPFlag("replay", cmd.Flags().Lookup("replay"))
	_ = viper.BindPFlag("prom-enabled", cmd.Flags().Lookup("prom-enabled"))
	_ = viper.BindPFlag("max-idle-conns", cmd.Flags().Lookup("max-idle-conns"))
	_ = viper.BindPFlag("max-idle-conns-per-host", cmd.Flags().Lookup("max-idle-conns-per-host"))
	_ = viper.BindPFlag("max-conns-per-host", cmd.Flags().Lookup("max-conns-per-host"))
	_ = viper.BindPFlag("idle-conn-timeout", cmd.Flags().Lookup("idle-conn-timeout"))
	_ = viper.BindPFlag("email-subject-template", cmd.Flags().Lookup("email-subject-template"))
	_ = viper.BindPFlag("email-body-template", cmd.Flags().Lookup("email-body-template"))
	_ = viper.BindPFlag("webhook-template", cmd.Flags().Lookup("webhook-template"))
	_ = viper.BindPFlag("pc-alerts-cache-ttl", cmd.Flags().Lookup("pc-alerts-cache-ttl"))
	_ = viper.BindPFlag("gen-test-agg", cmd.Flags().Lookup("gen-test-agg"))
	_ = viper.BindPFlag("prom-dir", cmd.Flags().Lookup("prom-dir"))
	_ = viper.BindPFlag("run-history", cmd.Flags().Lookup("run-history"))
	_ = viper.BindPFlag("run-history-dir", cmd.Flags().Lookup("run-history-dir"))
	_ = viper.BindPFlag("retain-last", cmd.Flags().Lookup("retain-last"))
	_ = viper.BindPFlag("retain-days", cmd.Flags().Lookup("retain-days"))
	_ = viper.BindPFlag("artifact-retain-days", cmd.Flags().Lookup("artifact-retain-days"))
	_ = viper.BindPFlag("artifact-retain-max-files", cmd.Flags().Lookup("artifact-retain-max-files"))
	_ = viper.BindPFlag("raw-log-retain-days", cmd.Flags().Lookup("raw-log-retain-days"))
	_ = viper.BindPFlag("notify-on-regression", cmd.Flags().Lookup("notify-on-regression"))
	_ = viper.BindPFlag("adaptive-parallelism", cmd.Flags().Lookup("adaptive-parallelism"))
	_ = viper.BindPFlag("policy-gates", cmd.Flags().Lookup("policy-gates"))
	_ = viper.BindPFlag("quiet-hours", cmd.Flags().Lookup("quiet-hours"))
	_ = viper.BindPFlag("maintenance-windows", cmd.Flags().Lookup("maintenance-windows"))
	_ = viper.BindPFlag("flaky-lookback-runs", cmd.Flags().Lookup("flaky-lookback-runs"))
	_ = viper.BindPFlag("flaky-min-transitions", cmd.Flags().Lookup("flaky-min-transitions"))
	_ = viper.BindPFlag("email-enabled", cmd.Flags().Lookup("email-enabled"))
	_ = viper.BindPFlag("email-attach-html", cmd.Flags().Lookup("email-attach-html"))
	_ = viper.BindPFlag("notify-digest", cmd.Flags().Lookup("notify-digest"))
	_ = viper.BindPFlag("smtp-server", cmd.Flags().Lookup("smtp-server"))
	_ = viper.BindPFlag("smtp-port", cmd.Flags().Lookup("smtp-port"))
	_ = viper.BindPFlag("smtp-user", cmd.Flags().Lookup("smtp-user"))
	_ = viper.BindPFlag("smtp-password", cmd.Flags().Lookup("smtp-password"))
	_ = viper.BindPFlag("email-from", cmd.Flags().Lookup("email-from"))
	_ = viper.BindPFlag("email-to", cmd.Flags().Lookup("email-to"))
	_ = viper.BindPFlag("email-use-tls", cmd.Flags().Lookup("email-use-tls"))
	_ = viper.BindPFlag("smtp-insecure-skip-verify", cmd.Flags().Lookup("smtp-insecure-skip-verify"))
	_ = viper.BindPFlag("webhook-enabled", cmd.Flags().Lookup("webhook-enabled"))
	_ = viper.BindPFlag("webhook-include-html", cmd.Flags().Lookup("webhook-include-html"))
	_ = viper.BindPFlag("webhook-url", cmd.Flags().Lookup("webhook-url"))
	_ = viper.BindPFlag("webhook-headers", cmd.Flags().Lookup("webhook-headers"))
	_ = viper.BindPFlag("notification-deadletter-dir", cmd.Flags().Lookup("notification-deadletter-dir"))
	_ = viper.BindPFlag("severity-filter", cmd.Flags().Lookup("severity-filter"))
	_ = viper.BindPFlag("exclude-alert-titles", cmd.Flags().Lookup("exclude-alert-titles"))
	_ = viper.BindPFlag("exclude-alert-titles-file", cmd.Flags().Lookup("exclude-alert-titles-file"))
	_ = viper.BindPFlag("exclude-alert-match-mode", cmd.Flags().Lookup("exclude-alert-match-mode"))
	_ = viper.BindPFlag("dry-run", cmd.Flags().Lookup("dry-run"))
	_ = viper.BindPFlag("slack-enabled", cmd.Flags().Lookup("slack-enabled"))
	_ = viper.BindPFlag("slack-webhook-url", cmd.Flags().Lookup("slack-webhook-url"))
	_ = viper.BindPFlag("slack-channel", cmd.Flags().Lookup("slack-channel"))
	_ = viper.BindPFlag("secrets-provider", cmd.Flags().Lookup("secrets-provider"))
	_ = viper.BindPFlag("secrets-file", cmd.Flags().Lookup("secrets-file"))

	termsCmd := &cobra.Command{
		Use:   "terms",
		Short: "Display terms and conditions",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Print(termsText)
			return nil
		},
	}
	cmd.AddCommand(termsCmd)

	envInfoCmd := &cobra.Command{
		Use:   "env-info",
		Short: "Display supported NCC environment variables and current values",
		RunE: func(cmd *cobra.Command, args []string) error {
			printEnvInfo()
			return nil
		},
	}
	cmd.AddCommand(envInfoCmd)

	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "Check or update binary (track-aware, source configurable)",
		Long: `Checks or updates ncc-orchestrator binaries.

Default behavior follows the current major track (v1.x stays on latest v1.x).
To cross major versions (for example v1 -> v2), pass --allow-major-upgrade.

When a service manager runs the stack (or its update helper is installed),
update hands over to the update helper: the same as upgrade --to, with a
backup, health checks and an automatic rollback. Otherwise, or with
--programs-only, it replaces the programs only and keeps the previous ones for
upgrade --rollback (24 hours).

Use --check to only validate availability/version without downloading.
Use --binary-url to work with non-GitHub/custom binary repositories.
When using --binary-url for install, --binary-sha256 is required.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			checkOnly, _ := cmd.Flags().GetBool("check")
			allowMajorUpgrade, _ := cmd.Flags().GetBool("allow-major-upgrade")
			includePrerelease, _ := cmd.Flags().GetBool("include-prerelease")
			repo, _ := cmd.Flags().GetString("repo")
			binaryURL, _ := cmd.Flags().GetString("binary-url")
			binarySHA256, _ := cmd.Flags().GetString("binary-sha256")
			targetVersion, _ := cmd.Flags().GetString("target-version")
			skipChecksumVerify, _ := cmd.Flags().GetBool("skip-checksum-verify")
			jsonOut, _ := cmd.Flags().GetBool("json")
			programsOnly, _ := cmd.Flags().GetBool("programs-only")
			return runUpdateCommand(cmd.Context(), updateOptions{
				CheckOnly:          checkOnly,
				AllowMajorUpgrade:  allowMajorUpgrade,
				IncludePrerelease:  includePrerelease,
				Repo:               repo,
				BinaryURL:          binaryURL,
				BinarySHA256:       binarySHA256,
				TargetVersion:      targetVersion,
				SkipChecksumVerify: skipChecksumVerify,
				JSONOut:            jsonOut,
			}, programsOnly, cmd.OutOrStdout())
		},
	}
	updateCmd.Flags().Bool("programs-only", false, "Replace the programs only, even when the update helper could upgrade the whole stack (restart it yourself)")
	updateCmd.Flags().Bool("check", false, "Check update availability without downloading or replacing")
	updateCmd.Flags().Bool("json", false, "With --check, also emit a machine-readable result line (NCC_UPDATE_JSON {...}) on stdout")
	updateCmd.Flags().Bool("include-prerelease", false, "Also consider release candidates (an install that runs one always does)")
	updateCmd.Flags().Bool("allow-major-upgrade", false, "Allow major upgrades (for example v1.x to v2.x)")
	updateCmd.Flags().String("repo", defaultGitHubRepo, "GitHub repo in owner/repo or GitHub URL format")
	updateCmd.Flags().String("binary-url", "", "Direct binary URL for non-GitHub/custom repositories")
	updateCmd.Flags().String("binary-sha256", "", "Expected SHA256 for --binary-url (required for install)")
	updateCmd.Flags().String("target-version", "", "Target version hint (recommended with --binary-url)")
	updateCmd.Flags().Bool("skip-checksum-verify", false, "Skip SHA-256 verification of the downloaded asset against the release checksums.txt (not recommended; for air-gapped/mirrored installs)")
	cmd.AddCommand(updateCmd)
	cmd.AddCommand(newUpgradeCommands()...)
	cmd.AddCommand(newWatchdogCommand())

	v2BootstrapCmd := &cobra.Command{
		Use:   "v2-bootstrap",
		Short: "Download and prepare v2 API/UI/frontend binaries",
		Long: `Automates v2 stack setup using release artifacts (binary-first workflow).

Preferred artifact:
- ncc-v2-stack-<os>-<arch>.zip|tar.gz (single bundle)

Fallback (legacy layout):
- ncc-api-server binary (for current OS/arch)
- ncc-ui-server binary (for current OS/arch)
- frontend bundle archive

Then it writes startup scripts under --install-dir.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			version, _ := cmd.Flags().GetString("version")
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config-path")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			logDir, _ := cmd.Flags().GetString("log-dir")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			tokenFile, _ := cmd.Flags().GetString("token-file")
			checkOnly, _ := cmd.Flags().GetBool("check")
			skipChecksumVerify, _ := cmd.Flags().GetBool("skip-checksum-verify")
			return runV2Bootstrap(v2BootstrapOptions{
				Repo:               repo,
				Version:            version,
				InstallDir:         installDir,
				ConfigPath:         configPath,
				OutputDir:          outputDir,
				LogDir:             logDir,
				OrchestratorBin:    orchestratorBin,
				APIListen:          apiListen,
				UIListen:           uiListen,
				TokenFile:          tokenFile,
				CheckOnly:          checkOnly,
				SkipChecksumVerify: skipChecksumVerify,
			})
		},
	}
	v2BootstrapCmd.Flags().String("repo", defaultGitHubRepo, "GitHub repo in owner/repo or URL format")
	v2BootstrapCmd.Flags().String("version", "", "Release version (default: latest v2 stable)")
	v2BootstrapCmd.Flags().String("install-dir", ".ncc-v2", "Installation directory for downloaded binaries and scripts")
	v2BootstrapCmd.Flags().String("config-path", "config.yaml", "Config file path passed to API server")
	v2BootstrapCmd.Flags().String("output-dir", "outputfiles", "Output directory passed to API server")
	v2BootstrapCmd.Flags().String("log-dir", "nccfiles", "Log directory passed to API server")
	v2BootstrapCmd.Flags().String("orchestrator-bin", "./ncc-orchestrator", "Path to ncc-orchestrator binary used by API server")
	v2BootstrapCmd.Flags().String("api-listen", ":8081", "Listen address for API server")
	v2BootstrapCmd.Flags().String("ui-listen", ":8080", "Listen address for UI server")
	v2BootstrapCmd.Flags().String("token-file", ".ncc-api-token", "Token file path used by UI/API servers")
	v2BootstrapCmd.Flags().Bool("check", false, "Check required assets only; do not download")
	v2BootstrapCmd.Flags().Bool("skip-checksum-verify", false, "Skip SHA-256 verification of downloaded assets against the release checksums.txt (not recommended; for air-gapped/mirrored installs)")
	cmd.AddCommand(v2BootstrapCmd)

	v2StartCmd := &cobra.Command{
		Use:     "v2-start",
		Aliases: []string{"v2-supervise"},
		Short:   "Start v2 services (API + optional UI)",
		Long: `Starts ncc-api-server and, by default, ncc-ui-server from bootstrapped binaries.

Run "ncc-orchestrator v2-bootstrap" once before using this command.

Use --api-only to start only the backend API service (for custom UI integrations).

Use --detach to run services in the background.

Use --self-heal with --detach to auto-restart services on unexpected exits.

Use --supervise (or the "v2-supervise" alias) to run a single long-lived
foreground supervisor that owns and keeps the API/UI children alive (liveness +
health-probe restarts, exponential backoff, and cooldown-and-resume). Run it as
a Type=simple systemd service (Restart=always) so the OS keeps the supervisor
alive across reboots and the supervisor keeps the stack alive across crashes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config-path")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			logDir, _ := cmd.Flags().GetString("log-dir")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			apiAdvertiseURL, _ := cmd.Flags().GetString("api-advertise-url")
			uiAdvertiseURL, _ := cmd.Flags().GetString("ui-advertise-url")
			uiBackendURL, _ := cmd.Flags().GetString("ui-backend-url")
			apiCORSOrigins, _ := cmd.Flags().GetString("api-cors-origins")
			uiAllowedOrigins, _ := cmd.Flags().GetString("ui-allowed-origins")
			tokenFile, _ := cmd.Flags().GetString("token-file")
			usersDB, _ := cmd.Flags().GetString("users-db")
			apiAuthMode, _ := cmd.Flags().GetString("api-auth-mode")
			apiSessionTTL, _ := cmd.Flags().GetDuration("api-session-ttl")
			apiSessionSecret, _ := cmd.Flags().GetString("api-session-secret")
			apiSessionSecretFile, _ := cmd.Flags().GetString("api-session-secret-file")
			apiRunTimeout, _ := cmd.Flags().GetDuration("api-run-timeout")
			apiRateLimitPerMinute, _ := cmd.Flags().GetInt("api-rate-limit-per-minute")
			apiReadTimeout, _ := cmd.Flags().GetDuration("api-read-timeout")
			apiWriteTimeout, _ := cmd.Flags().GetDuration("api-write-timeout")
			apiIdleTimeout, _ := cmd.Flags().GetDuration("api-idle-timeout")
			apiTLSCertFile, _ := cmd.Flags().GetString("api-tls-cert-file")
			apiTLSKeyFile, _ := cmd.Flags().GetString("api-tls-key-file")
			apiTLSClientCAFile, _ := cmd.Flags().GetString("api-tls-client-ca-file")
			apiCookieInsecure, _ := cmd.Flags().GetBool("api-cookie-insecure")
			uiTLSCertFile, _ := cmd.Flags().GetString("ui-tls-cert-file")
			uiTLSKeyFile, _ := cmd.Flags().GetString("ui-tls-key-file")
			uiInsecureHTTP, _ := cmd.Flags().GetBool("ui-insecure-http")
			uiBackendCAFile, _ := cmd.Flags().GetString("ui-backend-ca-file")
			uiBackendClientCertFile, _ := cmd.Flags().GetString("ui-backend-client-cert-file")
			uiBackendClientKeyFile, _ := cmd.Flags().GetString("ui-backend-client-key-file")
			uiBackendInsecureSkipVerify, _ := cmd.Flags().GetBool("ui-backend-insecure-skip-verify")
			waitReady, _ := cmd.Flags().GetBool("wait-ready")
			readyTimeout, _ := cmd.Flags().GetDuration("ready-timeout")
			detach, _ := cmd.Flags().GetBool("detach")
			apiOnly, _ := cmd.Flags().GetBool("api-only")
			apiLogFile, _ := cmd.Flags().GetString("api-log-file")
			uiLogFile, _ := cmd.Flags().GetString("ui-log-file")
			apiPIDFile, _ := cmd.Flags().GetString("api-pid-file")
			uiPIDFile, _ := cmd.Flags().GetString("ui-pid-file")
			selfHeal, _ := cmd.Flags().GetBool("self-heal")
			selfHealMaxRestarts, _ := cmd.Flags().GetInt("self-heal-max-restarts")
			selfHealWindow, _ := cmd.Flags().GetDuration("self-heal-window")
			selfHealProbeInterval, _ := cmd.Flags().GetDuration("self-heal-probe-interval")
			selfHealUnhealthyThreshold, _ := cmd.Flags().GetInt("self-heal-unhealthy-threshold")
			supervise, _ := cmd.Flags().GetBool("supervise")
			// Invoking the command via its "v2-supervise" alias implies the
			// foreground supervisor without needing the flag.
			if cmd.CalledAs() == "v2-supervise" {
				supervise = true
			}
			if supervise && detach {
				fmt.Fprintln(os.Stderr, "note: --supervise runs in the foreground; ignoring --detach")
				detach = false
			}
			// Boot persistence: when run as the (systemd-managed) supervisor,
			// replay the persisted start settings for any flag the operator did
			// not explicitly pass. This keeps a reboot honoring settings that
			// were changed at runtime (e.g. TLS enabled from Settings → Access
			// rewrites .ncc-v2-start.json) without baking them into the unit.
			if supervise {
				effInstall := strings.TrimSpace(installDir)
				if effInstall == "" {
					effInstall = defaultV2InstallDir()
				}
				if st, ok := loadV2StartState(effInstall); ok {
					ovStr := func(name string, dst *string, src string) {
						if !cmd.Flags().Changed(name) && strings.TrimSpace(src) != "" {
							*dst = src
						}
					}
					ovDur := func(name string, dst *time.Duration, src time.Duration) {
						if !cmd.Flags().Changed(name) && src > 0 {
							*dst = src
						}
					}
					ovBool := func(name string, dst *bool, src bool) {
						if !cmd.Flags().Changed(name) {
							*dst = src
						}
					}
					ovStr("api-listen", &apiListen, st.APIListen)
					ovStr("ui-listen", &uiListen, st.UIListen)
					ovStr("api-advertise-url", &apiAdvertiseURL, st.APIAdvertiseURL)
					ovStr("ui-advertise-url", &uiAdvertiseURL, st.UIAdvertiseURL)
					ovStr("ui-backend-url", &uiBackendURL, st.UIBackendURL)
					ovStr("api-cors-origins", &apiCORSOrigins, st.APICORSOrigins)
					ovStr("ui-allowed-origins", &uiAllowedOrigins, st.UIAllowedOrigins)
					ovStr("api-auth-mode", &apiAuthMode, st.APIAuthMode)
					ovStr("api-session-secret", &apiSessionSecret, st.APISessionSecret)
					ovDur("api-session-ttl", &apiSessionTTL, st.APISessionTTL)
					ovDur("api-run-timeout", &apiRunTimeout, st.APIRunTimeout)
					ovDur("api-read-timeout", &apiReadTimeout, st.APIReadTimeout)
					ovDur("api-write-timeout", &apiWriteTimeout, st.APIWriteTimeout)
					ovDur("api-idle-timeout", &apiIdleTimeout, st.APIIdleTimeout)
					if !cmd.Flags().Changed("api-rate-limit-per-minute") {
						apiRateLimitPerMinute = st.APIRateLimitPerMinute
					}
					ovBool("ui-backend-insecure-skip-verify", &uiBackendInsecureSkipVerify, st.UIBackendInsecureSkipVerify)
					ovBool("api-cookie-insecure", &apiCookieInsecure, st.APICookieInsecure)
					ovStr("ui-tls-cert-file", &uiTLSCertFile, st.UITLSCertFile)
					ovStr("ui-tls-key-file", &uiTLSKeyFile, st.UITLSKeyFile)
					ovBool("ui-insecure-http", &uiInsecureHTTP, st.UIInsecureHTTP)
					ovBool("api-only", &apiOnly, st.APIOnly)
					if st.SelfHealMaxRestarts > 0 && !cmd.Flags().Changed("self-heal-max-restarts") {
						selfHealMaxRestarts = st.SelfHealMaxRestarts
					}
					ovDur("self-heal-window", &selfHealWindow, st.SelfHealWindow)
					ovDur("self-heal-probe-interval", &selfHealProbeInterval, st.SelfHealProbeInterval)
					if st.SelfHealUnhealthyThreshold > 0 && !cmd.Flags().Changed("self-heal-unhealthy-threshold") {
						selfHealUnhealthyThreshold = st.SelfHealUnhealthyThreshold
					}
				}
			}
			return runV2Start(v2StartOptions{
				InstallDir:                  installDir,
				ConfigPath:                  configPath,
				OutputDir:                   outputDir,
				LogDir:                      logDir,
				OrchestratorBin:             orchestratorBin,
				APIListen:                   apiListen,
				UIListen:                    uiListen,
				APIAdvertiseURL:             apiAdvertiseURL,
				UIAdvertiseURL:              uiAdvertiseURL,
				UIBackendURL:                uiBackendURL,
				APICORSOrigins:              apiCORSOrigins,
				UIAllowedOrigins:            uiAllowedOrigins,
				TokenFile:                   tokenFile,
				UsersDB:                     usersDB,
				APIAuthMode:                 apiAuthMode,
				APISessionTTL:               apiSessionTTL,
				APISessionSecret:            apiSessionSecret,
				APISessionSecretFile:        apiSessionSecretFile,
				APIRunTimeout:               apiRunTimeout,
				APIRateLimitPerMinute:       apiRateLimitPerMinute,
				APIReadTimeout:              apiReadTimeout,
				APIWriteTimeout:             apiWriteTimeout,
				APIIdleTimeout:              apiIdleTimeout,
				APITLSCertFile:              apiTLSCertFile,
				APITLSKeyFile:               apiTLSKeyFile,
				APITLSClientCAFile:          apiTLSClientCAFile,
				APICookieInsecure:           apiCookieInsecure,
				UITLSCertFile:               uiTLSCertFile,
				UITLSKeyFile:                uiTLSKeyFile,
				UIInsecureHTTP:              uiInsecureHTTP,
				UIBackendCAFile:             uiBackendCAFile,
				UIBackendClientCertFile:     uiBackendClientCertFile,
				UIBackendClientKeyFile:      uiBackendClientKeyFile,
				UIBackendInsecureSkipVerify: uiBackendInsecureSkipVerify,
				WaitReady:                   waitReady,
				ReadyTimeout:                readyTimeout,
				Detach:                      detach,
				APIOnly:                     apiOnly,
				APILogFile:                  apiLogFile,
				UILogFile:                   uiLogFile,
				APIPIDFile:                  apiPIDFile,
				UIPIDFile:                   uiPIDFile,
				SelfHeal:                    selfHeal,
				SelfHealMaxRestarts:         selfHealMaxRestarts,
				SelfHealWindow:              selfHealWindow,
				SelfHealProbeInterval:       selfHealProbeInterval,
				SelfHealUnhealthyThreshold:  selfHealUnhealthyThreshold,
				Supervise:                   supervise,
			})
		},
	}
	v2StartCmd.Flags().String("install-dir", "", "Installation directory used by v2-bootstrap (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2StartCmd.Flags().String("config-path", "", "Config file path passed to API server (default <install-dir>/config.yaml; falls back to <install-dir>/example_config.yaml with a warning when missing)")
	v2StartCmd.Flags().String("output-dir", "", "Output directory passed to API server (default <install-dir>/outputfiles)")
	v2StartCmd.Flags().String("log-dir", "", "Log directory passed to API server (default <install-dir>/nccfiles)")
	v2StartCmd.Flags().String("orchestrator-bin", "", "Path to ncc-orchestrator binary used by API server (default: running executable, then <install-dir>/bin/ncc-orchestrator)")
	v2StartCmd.Flags().String("api-listen", ":8081", "Listen address for API server")
	v2StartCmd.Flags().String("ui-listen", ":8080", "Listen address for UI server")
	v2StartCmd.Flags().String("api-advertise-url", "", "Optional externally reachable API URL printed in startup output")
	v2StartCmd.Flags().String("ui-advertise-url", "", "Optional externally reachable UI URL printed in startup output")
	v2StartCmd.Flags().String("ui-backend-url", "", "Backend URL for ncc-ui-server to proxy to (default derived from --api-listen)")
	v2StartCmd.Flags().String("api-cors-origins", "", "Comma-separated API CORS origins (default derived from UI origin + --ui-allowed-origins)")
	v2StartCmd.Flags().String("ui-allowed-origins", "", "Additional comma-separated UI origins to allow (e.g. http://192.168.1.50:8080); default localhost origin is always included")
	v2StartCmd.Flags().String("token-file", "", "Token file path used by UI/API servers (default <install-dir>/.ncc-api-token)")
	v2StartCmd.Flags().String("users-db", "", "Writable user-database path passed to ncc-api-server; persists local accounts, roles, and SSO config across restarts (default <install-dir>/.ncc-api-users.json)")
	v2StartCmd.Flags().String("api-auth-mode", "token", "API auth mode passed to ncc-api-server: token, session, hybrid")
	v2StartCmd.Flags().Duration("api-session-ttl", 6*time.Hour, "Session TTL passed to ncc-api-server")
	v2StartCmd.Flags().String("api-session-secret", "", "Session HMAC secret passed to ncc-api-server (use file flag in production)")
	v2StartCmd.Flags().String("api-session-secret-file", "", "Read API session secret from file and pass to ncc-api-server")
	v2StartCmd.Flags().Duration("api-run-timeout", 90*time.Minute, "Run timeout passed to ncc-api-server")
	v2StartCmd.Flags().Int("api-rate-limit-per-minute", defaultAPIRateLimitPerMinute, "Per-client API rate limit passed to ncc-api-server (0 disables)")
	v2StartCmd.Flags().Duration("api-read-timeout", 15*time.Second, "HTTP read timeout passed to ncc-api-server")
	v2StartCmd.Flags().Duration("api-write-timeout", 60*time.Second, "HTTP write timeout passed to ncc-api-server")
	v2StartCmd.Flags().Duration("api-idle-timeout", 60*time.Second, "HTTP idle timeout passed to ncc-api-server")
	v2StartCmd.Flags().String("api-tls-cert-file", "", "TLS cert file for ncc-api-server")
	v2StartCmd.Flags().String("api-tls-key-file", "", "TLS key file for ncc-api-server")
	v2StartCmd.Flags().String("api-tls-client-ca-file", "", "mTLS client CA bundle for ncc-api-server")
	v2StartCmd.Flags().Bool("api-cookie-insecure", false, "Drop the Secure attribute on session cookies so logins work when serving over plain HTTP from a non-localhost address (use TLS instead for anything exposed)")
	v2StartCmd.Flags().String("ui-tls-cert-file", "", "TLS cert file for ncc-ui-server (overrides the default self-signed certificate)")
	v2StartCmd.Flags().String("ui-tls-key-file", "", "TLS key file for ncc-ui-server (overrides the default self-signed certificate)")
	v2StartCmd.Flags().Bool("ui-insecure-http", false, "Serve the UI over plain HTTP instead of the default self-signed HTTPS (not recommended; only for trusted-loopback or TLS-terminating-proxy deployments)")
	v2StartCmd.Flags().String("ui-backend-ca-file", "", "Custom CA bundle for ncc-ui-server backend TLS")
	v2StartCmd.Flags().String("ui-backend-client-cert-file", "", "Client cert for ncc-ui-server backend mTLS")
	v2StartCmd.Flags().String("ui-backend-client-key-file", "", "Client key for ncc-ui-server backend mTLS")
	v2StartCmd.Flags().Bool("ui-backend-insecure-skip-verify", false, "Skip TLS verification for ncc-ui-server backend connection")
	v2StartCmd.Flags().Bool("wait-ready", false, "Wait for API health (and UI root when enabled) before returning")
	v2StartCmd.Flags().Duration("ready-timeout", 20*time.Second, "Timeout for wait-ready checks")
	v2StartCmd.Flags().Bool("api-only", false, "Start only ncc-api-server (skip ncc-ui-server/frontend); useful when building a custom frontend")
	v2StartCmd.Flags().Bool("detach", false, "Run started services in background; writes PID/log files under <install-dir>/run and <install-dir>/logs")
	v2StartCmd.Flags().String("api-log-file", "", "Detached mode API log file path (default <install-dir>/logs/v2-api.log)")
	v2StartCmd.Flags().String("ui-log-file", "", "Detached mode UI log file path (default <install-dir>/logs/v2-ui.log)")
	v2StartCmd.Flags().String("api-pid-file", "", "Detached mode API PID file path (default <install-dir>/run/v2-api.pid)")
	v2StartCmd.Flags().String("ui-pid-file", "", "Detached mode UI PID file path (default <install-dir>/run/v2-ui.pid)")
	v2StartCmd.Flags().Bool("self-heal", false, "Detached mode only: monitor and auto-restart API/UI if they exit unexpectedly (also restarts a hung-but-alive API via health probes, with exponential backoff and cooldown-and-resume after the restart budget is exhausted)")
	v2StartCmd.Flags().Int("self-heal-max-restarts", 3, "Maximum auto-restarts allowed within self-heal window before a cooldown")
	v2StartCmd.Flags().Duration("self-heal-window", 10*time.Minute, "Rolling window used for self-heal restart budget (also the cooldown duration after the budget is exhausted)")
	v2StartCmd.Flags().Duration("self-heal-probe-interval", 10*time.Second, "How often the self-heal supervisor health-probes a still-alive API server to detect hangs (api-server only; uses its built-in --health-check)")
	v2StartCmd.Flags().Int("self-heal-unhealthy-threshold", 3, "Consecutive failed health probes before the self-heal supervisor restarts a hung-but-alive API server")
	v2StartCmd.Flags().Bool("supervise", false, "Run a single long-lived foreground supervisor that owns and keeps the API/UI children alive (liveness + health-probe restarts, backoff, cooldown-and-resume). Run as a Type=simple systemd service for reboot persistence. Implied by the v2-supervise alias; reuses --self-heal-* tuning")
	cmd.AddCommand(v2StartCmd)

	v2InstallServiceCmd := &cobra.Command{
		Use:   "v2-install-service",
		Short: "Install the stack supervisor as a boot-persistent OS service",
		Long: `Registers "ncc-orchestrator v2-supervise" with the platform service manager so
the v2 stack starts automatically after an OS reboot and is kept alive by the
native supervisor:

  Linux   -> a Type=simple systemd service (Restart=always) under /etc/systemd/system
  Windows -> a Task Scheduler task triggered at system startup (run as SYSTEM),
             and one that starts it again when it has died
  macOS   -> a launchd LaunchDaemon (RunAtLoad + KeepAlive) under /Library/LaunchDaemons

Run with --print-only to preview the unit/task without applying it. Typically
requires root/Administrator.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			serviceName, _ := cmd.Flags().GetString("service-name")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			now, _ := cmd.Flags().GetBool("now")
			printOnly, _ := cmd.Flags().GetBool("print-only")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			return runV2InstallService(installServiceOptions{
				InstallDir:      installDir,
				ServiceName:     serviceName,
				OrchestratorBin: orchestratorBin,
				Now:             now,
				PrintOnly:       printOnly,
				APIListen:       strings.TrimSpace(apiListen),
				UIListen:        strings.TrimSpace(uiListen),
			})
		},
	}
	v2InstallServiceCmd.Flags().String("api-listen", "", "Listen address of the API server the service starts (default: the saved one, else :8081)")
	v2InstallServiceCmd.Flags().String("ui-listen", "", "Listen address of the UI server the service starts (default: the saved one, else :8080)")
	v2InstallServiceCmd.Flags().String("install-dir", "", "Installation directory the supervisor manages (default: auto-detect, fallback .ncc-v2)")
	v2InstallServiceCmd.Flags().String("service-name", "ncc-orchestrator", "Service/task name to create")
	v2InstallServiceCmd.Flags().String("orchestrator-bin", "", "Path to the ncc-orchestrator binary the service runs (default: this executable)")
	v2InstallServiceCmd.Flags().Bool("now", true, "Also enable+start the service immediately (Linux: enable --now; Windows: schtasks /Run; macOS: launchctl load -w)")
	v2InstallServiceCmd.Flags().Bool("print-only", false, "Preview the unit/task and the commands without applying them")
	cmd.AddCommand(v2InstallServiceCmd)

	v2InstallUpdateWatcherCmd := &cobra.Command{
		Use:   "v2-install-update-watcher",
		Short: "Install a systemd path unit that restarts the stack after binaries change",
		Long: `Registers a systemd .path watcher on the stack binaries so an in-app update
still restarts the service when the running API is too old to restart itself
(the 2.1.x then; restart script). The units live in /etc/systemd/system and
survive a binary downgrade. Linux/systemd only.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			serviceName, _ := cmd.Flags().GetString("service-name")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			printOnly, _ := cmd.Flags().GetBool("print-only")
			return installApplyUpdateWatcher(installServiceOptions{
				InstallDir:      installDir,
				ServiceName:     serviceName,
				OrchestratorBin: orchestratorBin,
				PrintOnly:       printOnly,
			})
		},
	}
	v2InstallUpdateWatcherCmd.Flags().String("install-dir", "", "Installation directory (default: auto-detect)")
	v2InstallUpdateWatcherCmd.Flags().String("service-name", "ncc-orchestrator", "Supervisor service the watcher restarts")
	v2InstallUpdateWatcherCmd.Flags().String("orchestrator-bin", "", "Path to ncc-orchestrator (default: this executable)")
	v2InstallUpdateWatcherCmd.Flags().Bool("print-only", false, "Preview the units without applying them")
	cmd.AddCommand(v2InstallUpdateWatcherCmd)

	v2UninstallServiceCmd := &cobra.Command{
		Use:   "v2-uninstall-service",
		Short: "Remove the boot-persistent supervisor service installed by v2-install-service",
		RunE: func(cmd *cobra.Command, args []string) error {
			serviceName, _ := cmd.Flags().GetString("service-name")
			installDir, _ := cmd.Flags().GetString("install-dir")
			printOnly, _ := cmd.Flags().GetBool("print-only")
			return runV2UninstallService(installServiceOptions{
				InstallDir:  installDir,
				ServiceName: serviceName,
				PrintOnly:   printOnly,
			})
		},
	}
	v2UninstallServiceCmd.Flags().String("service-name", "ncc-orchestrator", "Service/task name to remove")
	v2UninstallServiceCmd.Flags().String("install-dir", "", "Installation directory (used to locate runner artifacts; default: auto-detect)")
	v2UninstallServiceCmd.Flags().Bool("print-only", false, "Preview the removal commands without applying them")
	cmd.AddCommand(v2UninstallServiceCmd)

	v2CheckCmd := &cobra.Command{
		Use:   "v2-check",
		Short: "Run lightweight self-check for v2 paths and ports",
		Long:  `Validates v2 runtime prerequisites (binaries, config, dirs, and listen port availability) before running v2-start.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config-path")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			logDir, _ := cmd.Flags().GetString("log-dir")
			orchestratorBin, _ := cmd.Flags().GetString("orchestrator-bin")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			tokenFile, _ := cmd.Flags().GetString("token-file")
			apiOnly, _ := cmd.Flags().GetBool("api-only")
			return runV2Check(v2StartOptions{
				InstallDir:      installDir,
				ConfigPath:      configPath,
				OutputDir:       outputDir,
				LogDir:          logDir,
				OrchestratorBin: orchestratorBin,
				APIListen:       apiListen,
				UIListen:        uiListen,
				TokenFile:       tokenFile,
				APIOnly:         apiOnly,
			})
		},
	}
	v2CheckCmd.Flags().String("install-dir", "", "Installation directory used by v2-bootstrap (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2CheckCmd.Flags().String("config-path", "", "Config file path passed to API server (default <install-dir>/config.yaml; falls back to <install-dir>/example_config.yaml with a warning when missing)")
	v2CheckCmd.Flags().String("output-dir", "", "Output directory passed to API server (default <install-dir>/outputfiles)")
	v2CheckCmd.Flags().String("log-dir", "", "Log directory passed to API server (default <install-dir>/nccfiles)")
	v2CheckCmd.Flags().String("orchestrator-bin", "", "Path to ncc-orchestrator binary used by API server (default: running executable, then <install-dir>/bin/ncc-orchestrator)")
	v2CheckCmd.Flags().String("api-listen", ":8081", "Listen address for API server")
	v2CheckCmd.Flags().String("ui-listen", ":8080", "Listen address for UI server")
	v2CheckCmd.Flags().String("token-file", "", "Token file path used by UI/API servers (default <install-dir>/.ncc-api-token)")
	v2CheckCmd.Flags().Bool("api-only", false, "Validate only API prerequisites (skip UI/frontend checks)")
	cmd.AddCommand(v2CheckCmd)

	v2StopCmd := &cobra.Command{
		Use:   "v2-stop",
		Short: "Stop detached v2 API/UI services",
		Long: `Stops v2 services started with "v2-start --detach" using PID files under <install-dir>/run.

Use --force to send a hard kill signal.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			apiPIDFile, _ := cmd.Flags().GetString("api-pid-file")
			uiPIDFile, _ := cmd.Flags().GetString("ui-pid-file")
			force, _ := cmd.Flags().GetBool("force")
			stopTimeout, _ := cmd.Flags().GetDuration("stop-timeout")
			return runV2Stop(v2StopOptions{
				InstallDir:  installDir,
				APIPIDFile:  apiPIDFile,
				UIPIDFile:   uiPIDFile,
				Force:       force,
				StopTimeout: stopTimeout,
			})
		},
	}
	v2StopCmd.Flags().String("install-dir", "", "Installation directory used by v2-start --detach (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2StopCmd.Flags().String("api-pid-file", "", "Detached API PID file path override")
	v2StopCmd.Flags().String("ui-pid-file", "", "Detached UI PID file path override")
	v2StopCmd.Flags().Bool("force", false, "Force kill processes instead of graceful stop")
	v2StopCmd.Flags().Duration("stop-timeout", 5*time.Second, "Graceful stop timeout before force-kill (ignored when --force)")
	cmd.AddCommand(v2StopCmd)

	// status subcommand: report the live state of a v2 stack
	// (PIDs alive? api healthy? log paths?). Designed for support
	// triage and shell scripts — never modifies state, always
	// prints something useful even when no service is up.
	v2StatusCmd := &cobra.Command{
		Use:   "v2-status",
		Short: "Show live status of a running v2 stack (PIDs, listeners, health)",
		Long: `Reads <install-dir>/run/v2-{api,ui,api-supervisor,ui-supervisor}.pid,
checks each PID is still alive (signal 0), and probes
/api/v1/health on the API listener using the on-disk token.

Exits 0 even when services are down; the printed table (or --json
output) is the source of truth. Pair with awk/jq for scripted
liveness checks.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runV2Status(v2StatusOptions{
				InstallDir: installDir,
				APIListen:  apiListen,
				UIListen:   uiListen,
				JSON:       jsonOut,
			})
		},
	}
	v2StatusCmd.Flags().String("install-dir", "", "Installation directory (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	v2StatusCmd.Flags().String("api-listen", ":8081", "API listen address used to derive the health-probe URL (host:port; same value passed to v2-start)")
	v2StatusCmd.Flags().String("ui-listen", ":8080", "UI listen address used in the printed status row (host:port)")
	v2StatusCmd.Flags().Bool("json", false, "Emit JSON object {install_dir, services:[...]} instead of the text table")
	cmd.AddCommand(v2StatusCmd)

	// backup subcommand: capture the stateful parts of an install dir
	// (config + referenced files, user database, API token, scheduler /
	// notifications state) into one secrets-bearing tar.gz.
	v2BackupCmd := &cobra.Command{
		Use:   "v2-backup",
		Short: "Back up v2 config, local users/roles, tokens, and state into a tar.gz",
		Long: `Captures the stateful contents of a v2 install dir into a single
tar.gz so an install can be moved or recovered:

  - config.yaml and its referenced files (clusters-file,
    exclude-alert-titles-file, secrets-file)
  - the local user database (.ncc-api-users.json): accounts, bcrypt
    hashes, roles, runtime SAML config, and session policy
  - the API auth token (.ncc-api-token) and, if still present, the
    first-run admin password (.ncc-initial-admin-password)
  - scheduler and notifications state, plus any other .ncc-api-* state
    files at the install-dir root
  - the JSONL audit log (logs/ncc-audit.log) when present

The manifest records the exact ncc-orchestrator version (and stream /
build date) that created the backup, which 'v2-restore' reports and
checks. Regenerable artifacts (binaries, frontend bundle, run/ pid
files, output/ncc files) are excluded unless --include-stack is set.
--include-stack adds bin/ncc-* and frontend-dist so a restore can roll
the software version back (used by in-app pre-update snapshots). The
archive contains secrets, so it is written with 0600 permissions —
store it securely. Restore it with 'v2-restore'.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			outputFile, _ := cmd.Flags().GetString("output-file")
			outputDir, _ := cmd.Flags().GetString("output-dir")
			retain, _ := cmd.Flags().GetInt("retain")
			encrypt, _ := cmd.Flags().GetBool("encrypt")
			keyFile, _ := cmd.Flags().GetString("key-file")
			passphrase, _ := cmd.Flags().GetString("passphrase")
			includeStack, _ := cmd.Flags().GetBool("include-stack")
			// A supplied passphrase or key-file implies intent to encrypt.
			if strings.TrimSpace(keyFile) != "" || strings.TrimSpace(passphrase) != "" {
				encrypt = true
			}
			return runV2Backup(v2BackupOptions{
				InstallDir:   installDir,
				OutputFile:   outputFile,
				OutputDir:    outputDir,
				Retain:       retain,
				Encrypt:      encrypt,
				KeyFile:      keyFile,
				Passphrase:   passphrase,
				IncludeStack: includeStack,
			})
		},
	}
	v2BackupCmd.Flags().String("install-dir", "", "Installation directory to back up (default: auto-detect, fallback .ncc-v2)")
	v2BackupCmd.Flags().String("output-file", "", "Output archive path (default ./ncc-backup-<UTC-timestamp>.tar.gz)")
	v2BackupCmd.Flags().String("output-dir", "", "Directory to write the default timestamped archive into (used when --output-file is empty; e.g. <install>/backups for scheduled backups)")
	v2BackupCmd.Flags().Int("retain", 0, "Keep at most N newest ncc-backup-*.tar.gz files in the output directory, pruning older ones (0 = keep all)")
	v2BackupCmd.Flags().Bool("encrypt", false, "Encrypt the archive at rest with AES-256-GCM (key from --passphrase/NCC_BACKUP_PASSPHRASE or --key-file/NCC_BACKUP_KEY_FILE/NCC_BACKUP_KEY)")
	v2BackupCmd.Flags().String("passphrase", "", "Passphrase to derive the encryption key (scrypt); prefer NCC_BACKUP_PASSPHRASE to keep it out of the process list")
	v2BackupCmd.Flags().String("key-file", "", "File holding a 32-byte encryption key (base64/hex); alternative to --passphrase")
	v2BackupCmd.Flags().Bool("include-stack", false, "Also archive bin/ stack binaries and frontend-dist so restore can roll back the software version")
	cmd.AddCommand(v2BackupCmd)

	// restore subcommand: extract a v2-backup archive back into an install
	// dir. Confined to the install dir; refuses to overwrite a running
	// stack or pre-existing files unless --force.
	v2RestoreCmd := &cobra.Command{
		Use:   "v2-restore",
		Short: "Restore a v2 backup archive (config, users/roles, tokens, state) into an install dir",
		Long: `Restores an archive produced by 'v2-backup' into an install dir,
recreating config.yaml + referenced files, the local user database,
the API token, and scheduler/notifications state.

Safety:
  - refuses to run while the stack appears to be up (live pid files)
    unless --force; stop it first with 'v2-stop'
  - refuses to overwrite existing files unless --force
  - extraction is confined to the install dir (unsafe archive paths
    are rejected)

The restore reports the ncc-orchestrator version that created the
backup and warns if this binary is older than that.

Portability: the restore is OS- and version-agnostic. Backups taken on
Windows (drive letters / backslash paths) restore cleanly onto Linux/macOS —
file-reference paths under the backup's original install dir are rebased to
this install dir and separators are normalized automatically. Restoring a
backup from a different ncc-orchestrator version is allowed (a newer-than-this
binary backup only prints a warning).

Restart is automatic: when the stack is currently running it is stopped and
re-started for you (v2-stop then v2-start --detach, performed by this binary)
so the restored config/accounts/token load with no manual step. Pass --restart
to force a restart/start even if the stack looks stopped, or --no-restart to
suppress it (e.g. staging a host for later). The input archive may be given
with --input-file or as the first positional argument.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			inputFile, _ := cmd.Flags().GetString("input-file")
			force, _ := cmd.Flags().GetBool("force")
			restart, _ := cmd.Flags().GetBool("restart")
			noRestart, _ := cmd.Flags().GetBool("no-restart")
			verifyOnly, _ := cmd.Flags().GetBool("verify-only")
			asJSON, _ := cmd.Flags().GetBool("json")
			keyFile, _ := cmd.Flags().GetString("key-file")
			passphrase, _ := cmd.Flags().GetString("passphrase")
			trustStack, _ := cmd.Flags().GetBool("trust-stack")
			if strings.TrimSpace(inputFile) == "" && len(args) > 0 {
				inputFile = args[0]
			}
			return runV2Restore(v2RestoreOptions{
				TrustStack: trustStack,
				InstallDir: installDir,
				InputFile:  inputFile,
				Force:      force,
				Restart:    restart,
				NoRestart:  noRestart,
				VerifyOnly: verifyOnly,
				JSON:       asJSON,
				KeyFile:    keyFile,
				Passphrase: passphrase,
			})
		},
	}
	v2RestoreCmd.Flags().String("install-dir", "", "Installation directory to restore into (default: auto-detect, fallback .ncc-v2)")
	v2RestoreCmd.Flags().String("input-file", "", "Backup archive to restore (or pass as the first positional argument)")
	v2RestoreCmd.Flags().Bool("trust-stack", false, "Also restore the backup's program files (bin/ncc-*, frontend-dist) when this server has never installed them. Only for backups you trust: they run as the service account")
	v2RestoreCmd.Flags().Bool("verify-only", false, "Check the archive without restoring or writing to the install dir: decryption, every entry readable, safe paths, manifest and checksums, config files parse, secrets (names only), report paths, version compatibility. Exits non-zero when a check fails")
	v2RestoreCmd.Flags().Bool("json", false, "With --verify-only, print the check list as JSON")
	v2RestoreCmd.Flags().String("passphrase", "", "Passphrase to decrypt an encrypted backup (or NCC_BACKUP_PASSPHRASE)")
	v2RestoreCmd.Flags().String("key-file", "", "Key file to decrypt a key-encrypted backup (or NCC_BACKUP_KEY_FILE/NCC_BACKUP_KEY)")
	v2RestoreCmd.Flags().Bool("force", false, "Overwrite existing files and proceed even if the stack appears to be running")
	v2RestoreCmd.Flags().Bool("restart", false, "Force a stack restart/start after restore even if it appears stopped (default: auto-restart only when running)")
	v2RestoreCmd.Flags().Bool("no-restart", false, "Do not restart the stack after restore (overrides the default auto-restart when running)")
	cmd.AddCommand(v2RestoreCmd)

	v2RestartCmd := &cobra.Command{
		Use:   "v2-restart",
		Short: "Stop and re-start the v2 stack (v2-stop then v2-start --detach), performed by this binary",
		Long: `Restarts the v2 stack by stopping it and starting it again detached,
entirely from this orchestrator binary. Used internally by the UI/api-server to
apply a restored backup automatically, and available directly for operators.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			if strings.TrimSpace(installDir) == "" {
				installDir = defaultV2InstallDir()
			}
			if abs, err := filepath.Abs(installDir); err == nil {
				installDir = abs
			}
			return restartV2Stack(installDir)
		},
	}
	v2RestartCmd.Flags().String("install-dir", "", "Installation directory of the stack to restart (default: auto-detect, fallback .ncc-v2)")
	cmd.AddCommand(v2RestartCmd)

	// reset-password subcommand: offline recovery for a lost account password
	// (admin or any local user). Invokes ncc-api-server --reset-password against
	// the stack's user store, prints a new temporary password, and reminds the
	// operator to restart so it loads.
	v2ResetPasswordCmd := &cobra.Command{
		Use:   "v2-reset-password",
		Short: "Recover a lost local account password offline (admin or any user)",
		Long: `Resets a local account to a new random temporary password without
needing to log in — the recovery path when the admin (or any user)
password is lost.

It runs 'ncc-api-server --reset-password <user>' against the same user
store the stack uses (the <install-dir>/.ncc-api-users.json file, or a
Kubernetes Secret with --users-db-secret), prints a new temporary
password, and invalidates that account's existing sessions. The user is
forced to change the password at next login.

Because a running api-server caches accounts in memory, restart the
stack afterward ('v2-stop' then 'v2-start') for the new password to take
effect. Defaults to the 'admin' account.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			user, _ := cmd.Flags().GetString("user")
			usersDB, _ := cmd.Flags().GetString("users-db")
			usersDBSecret, _ := cmd.Flags().GetString("users-db-secret")
			usersDBSecretNS, _ := cmd.Flags().GetString("users-db-secret-namespace")
			return runV2ResetPassword(v2ResetPasswordOptions{
				InstallDir:   installDir,
				User:         user,
				UsersDB:      usersDB,
				UsersDBSec:   usersDBSecret,
				UsersDBSecNS: usersDBSecretNS,
			})
		},
	}
	v2ResetPasswordCmd.Flags().String("install-dir", "", "Installation directory whose user store to reset (default: auto-detect, fallback .ncc-v2)")
	v2ResetPasswordCmd.Flags().String("user", "admin", "Local account to reset")
	v2ResetPasswordCmd.Flags().String("users-db", "", "Override the user-database path (default <install-dir>/.ncc-api-users.json)")
	v2ResetPasswordCmd.Flags().String("users-db-secret", "", "Reset against a Kubernetes Secret store instead of a file (Secret name)")
	v2ResetPasswordCmd.Flags().String("users-db-secret-namespace", "", "Namespace of the Kubernetes Secret (defaults to the pod's own namespace)")
	cmd.AddCommand(v2ResetPasswordCmd)

	// doctor subcommand: single-command diagnostic for support
	// tickets. Runs verify + v2-check + v2-status, prints recent
	// log tails, and (by default) writes a redacted support tarball.
	v2DoctorCmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run all diagnostics + write a redacted support bundle (verify + v2-check + v2-status + log tails)",
		Long: `doctor is the "something's broken, give me everything" diagnostic.
It runs every read-only check the orchestrator ships and produces a
single tar.gz support bundle that can be attached to a support ticket.

Sections of the printed report (mirrored into report.txt inside the
bundle):
  1. verify   — embedded buildinfo + self SHA-256
  2. v2-check — install-dir layout + path readability
  3. v2-status — running services with PID/health/log-path
  4. environment summary — GOOS/GOARCH, NCC_* env var NAMES (values redacted)
  5. recent log tails — last 200 lines of each v2-*.log

The default bundle path is ./ncc-support-<UTC-timestamp>.tar.gz; pass
--output-file to override or --no-bundle to skip the tarball entirely.

Secrets in <install-dir>/config.yaml are redacted before they enter
the bundle: any key whose name contains password / secret / token /
credential / api-key / client-id (case insensitive) has its value
replaced by ***REDACTED***.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			apiListen, _ := cmd.Flags().GetString("api-listen")
			uiListen, _ := cmd.Flags().GetString("ui-listen")
			outputFile, _ := cmd.Flags().GetString("output-file")
			noBundle, _ := cmd.Flags().GetBool("no-bundle")
			configPath, _ := cmd.Flags().GetString("config")
			fix, _ := cmd.Flags().GetBool("fix")
			jsonOut, _ := cmd.Flags().GetBool("json")
			onlyChecks, _ := cmd.Flags().GetString("only-checks")
			noDisruptive, _ := cmd.Flags().GetBool("no-disruptive")
			return runV2Doctor(v2DoctorOptions{
				InstallDir:   installDir,
				APIListen:    apiListen,
				UIListen:     uiListen,
				OutputFile:   outputFile,
				NoBundle:     noBundle,
				ConfigPath:   configPath,
				Fix:          fix,
				JSON:         jsonOut,
				OnlyChecks:   onlyChecks,
				NoDisruptive: noDisruptive,
			})
		},
	}
	v2DoctorCmd.Flags().String("install-dir", "", "Installation directory (default: auto-detect, fallback .ncc-v2)")
	v2DoctorCmd.Flags().String("api-listen", ":8081", "API listen address used to derive the health-probe URL")
	v2DoctorCmd.Flags().String("ui-listen", ":8080", "UI listen address used in the printed status row")
	v2DoctorCmd.Flags().String("output-file", "", "Bundle path (default ./ncc-support-<UTC-timestamp>.tar.gz)")
	v2DoctorCmd.Flags().Bool("no-bundle", false, "Print the report only; do not write a tarball")
	v2DoctorCmd.Flags().String("config", "", "Config file for self-heal checks (default <install-dir>/config.yaml)")
	v2DoctorCmd.Flags().Bool("fix", false, "Apply safe self-heal remediations (create missing output dirs, anchor relative paths, tighten secret-file perms, repair config)")
	v2DoctorCmd.Flags().Bool("json", false, "Emit the self-heal report as JSON (skips the human report and support bundle)")
	v2DoctorCmd.Flags().String("only-checks", "", "Comma-separated self-heal check IDs to run (e.g. stale-pids,runtime-mode-drift)")
	v2DoctorCmd.Flags().Bool("no-disruptive", false, "Skip disruptive checks/fixes (e.g. service restarts)")
	cmd.AddCommand(v2DoctorCmd)

	migrateConfigCmd := &cobra.Command{
		Use:   "migrate-config",
		Short: "Convert config.yaml to the nested schema-version: 1 layout",
		Long: `migrate-config moves flat (legacy) keys to their nested schema-version: 1
paths and removes settings spelled twice, keeping the value the runner uses
now. Keys with no nested spelling stay at the top level.

The settings the runner reads must be identical before and after, or nothing
is written. The original file is kept as <config>.pre-migrate-<UTC>.bak.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			installDir, _ := cmd.Flags().GetString("install-dir")
			configPath, _ := cmd.Flags().GetString("config")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			jsonOut, _ := cmd.Flags().GetBool("json")
			if strings.TrimSpace(configPath) == "" {
				if strings.TrimSpace(installDir) == "" {
					installDir = defaultV2InstallDir()
				}
				configPath = resolveDoctorConfigPath(installDir)
			}
			res, err := migrateConfigFile(configPath, dryRun)
			if jsonOut {
				payload := map[string]interface{}{"result": res}
				if err != nil {
					payload["error"] = err.Error()
				}
				b, _ := json.MarshalIndent(payload, "", "  ")
				fmt.Println(string(b))
				return err
			}
			if err != nil {
				return fmt.Errorf("migrate-config: %w", err)
			}
			fmt.Printf("%s: %s layout\n", res.ConfigPath, res.Layout)
			for _, m := range res.Moved {
				fmt.Printf("  move    %s → %s\n", m.From, m.To)
			}
			for _, p := range res.Removed {
				fmt.Printf("  remove  %s (duplicate)\n", p)
			}
			switch {
			case !res.Changed:
				fmt.Println(res.Message)
			case dryRun:
				fmt.Printf("%s (dry run; nothing written)\n", res.Message)
			default:
				fmt.Printf("%s; original saved as %s\n", res.Message, res.Backup)
			}
			return nil
		},
	}
	migrateConfigCmd.Flags().String("config", "", "Config file to migrate (default <install-dir>/config.yaml)")
	migrateConfigCmd.Flags().String("install-dir", "", "Installation directory used to find config.yaml")
	migrateConfigCmd.Flags().Bool("dry-run", false, "Show what would change without writing")
	migrateConfigCmd.Flags().Bool("json", false, "Print the result as JSON (includes the new file content)")
	cmd.AddCommand(migrateConfigCmd)

	genTestAggCmd := &cobra.Command{
		Use:   "gen-test-agg",
		Short: "Generate synthetic aggregated index.html for load testing",
		RunE: func(cmd *cobra.Command, args []string) error {
			genN, _ := cmd.Flags().GetInt("clusters")
			if genN <= 0 {
				return errors.New("--clusters must be greater than 0")
			}
			outDir, _ := cmd.Flags().GetString("output-dir")
			if strings.TrimSpace(outDir) == "" {
				outDir = "outputfiles"
			}
			if err := generateTestAgg(genN, outDir); err != nil {
				return fmt.Errorf("gen-test-agg: %w", err)
			}
			fmt.Printf("Generated test aggregated report: %d clusters, output in %s/index.html\n", genN, outDir)
			return nil
		},
	}
	genTestAggCmd.Flags().Int("clusters", 0, "Number of synthetic clusters to generate")
	genTestAggCmd.Flags().String("output-dir", "outputfiles", "Directory for generated index.html")
	cmd.AddCommand(genTestAggCmd)

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Display version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(versionInfoString())
			return nil
		},
	}
	cmd.AddCommand(versionCmd)

	// verify subcommand: integrity / provenance check usable by anyone
	// who downloaded the binary without a code-signing certificate to
	// verify against. Prints:
	//   - injected ldflags (Version / BuildDate / Stream / GoVersion)
	//   - debug.ReadBuildInfo() output (commit, dirty, modules) — only
	//     the keys we care about, not the full module graph
	//   - SHA256 of the running executable — useful as the value an
	//     operator copy/pastes into checksums.txt comparison
	//   - a hint pointing at the published checksums.txt URL for the
	//     same release tag
	// Together this lets a user answer "is the file I'm running really
	// the one Nutanix published as v2.0.2?" using only the binary
	// itself and a hex string from the GitHub release page.
	verifyCmd := &cobra.Command{
		Use:   "verify",
		Short: "Print integrity + provenance metadata (self-hash, buildinfo, vendor)",
		Long: `verify prints information that can be used to confirm the binary
matches a known-good release without requiring an OS-level code signature:

  - The version, stream, and build date that were embedded at link time.
  - The git revision and dirty flag from Go's debug build info.
  - The SHA-256 of this executable on disk.
  - A reference URL where the matching SHA-256 is published.

Compare the printed SHA-256 against the value next to the same
ncc-orchestrator-<os>-<arch> filename in the release's checksums.txt
(or release-attestation.json) on GitHub. If both match, the binary
has not been tampered with in transit and originates from the build
that produced the release.

With --online, verify fetches the matching release's checksums.txt from
GitHub and reports MATCH / MISMATCH / NOT FOUND, exiting non-zero on a
mismatch (usable in CI / health checks). The checksum is fetched from the
same repo that serves the binary; on a build that embeds a release public
key (signed releases), --online also verifies an Ed25519 signature over
checksums.txt before trusting any hash. Add --require-signature to fail
when a valid signature is absent (e.g. an unsigned build or release).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			online, _ := cmd.Flags().GetBool("online")
			releaseTag, _ := cmd.Flags().GetString("release")
			repo, _ := cmd.Flags().GetString("repo")
			requireSig, _ := cmd.Flags().GetBool("require-signature")
			return runVerifyCommand(os.Stdout, verifyOptions{Online: online, ReleaseTag: releaseTag, Repo: repo, RequireSignature: requireSig})
		},
	}
	verifyCmd.Flags().Bool("online", false, "Fetch the matching release's checksums.txt from GitHub and compare (exits non-zero on mismatch)")
	verifyCmd.Flags().String("release", "", "Release tag to verify against (default: this binary's stamped version)")
	verifyCmd.Flags().String("repo", "", "GitHub owner/repo to fetch the release from (default: the project repo)")
	verifyCmd.Flags().Bool("require-signature", false, "With --online, fail unless the Ed25519 signature over checksums.txt verifies against the embedded release key")
	cmd.AddCommand(verifyCmd)

	// release-keygen / release-sign: maintainer-side tooling to produce signed
	// releases. Hidden from the normal command list (operators never need them).
	releaseKeygenCmd := &cobra.Command{
		Use:    "release-keygen",
		Short:  "Generate an Ed25519 release signing key (maintainers)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			out, _ := cmd.Flags().GetString("out")
			if strings.TrimSpace(out) == "" {
				out = "ncc-release-signing.key"
			}
			pub, err := generateReleaseSigningKey(out)
			if err != nil {
				return err
			}
			fmt.Printf("Wrote private key: %s (keep OFFLINE/secret)\n", out)
			fmt.Printf("Public key (embed via -ldflags \"-X main.releaseSigningPublicKeyB64=...\"):\n%s\n", pub)
			return nil
		},
	}
	releaseKeygenCmd.Flags().String("out", "ncc-release-signing.key", "Path to write the base64 Ed25519 private key")
	cmd.AddCommand(releaseKeygenCmd)

	releaseSignCmd := &cobra.Command{
		Use:    "release-sign",
		Short:  "Sign a file (e.g. checksums.txt) with the Ed25519 release key (maintainers)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, _ := cmd.Flags().GetString("key")
			in, _ := cmd.Flags().GetString("in")
			out, _ := cmd.Flags().GetString("out")
			if strings.TrimSpace(out) == "" {
				out = in + ".sig"
			}
			if err := signReleaseFile(key, in, out); err != nil {
				return err
			}
			fmt.Printf("Wrote signature: %s\n", out)
			return nil
		},
	}
	releaseSignCmd.Flags().String("key", "", "Path to the Ed25519 private key (from release-keygen, or PKCS#8 PEM)")
	releaseSignCmd.Flags().String("in", "checksums.txt", "File to sign")
	releaseSignCmd.Flags().String("out", "", "Signature output path (default <in>.sig)")
	_ = releaseSignCmd.MarkFlagRequired("key")
	cmd.AddCommand(releaseSignCmd)

	releasePubkeyCmd := &cobra.Command{
		Use:    "release-pubkey",
		Short:  "Print the release public key in the form a build embeds, and check that it belongs to the signing key (maintainers)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, _ := cmd.Flags().GetString("key")
			public, _ := cmd.Flags().GetString("public")
			pub, err := releasePublicKey(key, public)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), pub)
			return err
		},
	}
	releasePubkeyCmd.Flags().String("key", "", "Path to the Ed25519 private key")
	releasePubkeyCmd.Flags().String("public", "", "Public key: one line of base64, or PEM")
	cmd.AddCommand(releasePubkeyCmd)

	// completion subcommand: emit shell-completion scripts for bash,
	// zsh, fish, and powershell. Uses cobra's built-in generators so
	// the completions stay in lockstep with the actual subcommand /
	// flag set automatically — no hand-maintained completion files
	// to drift out of sync.
	completionCmd := &cobra.Command{
		Use:                   "completion [bash|zsh|fish|powershell]",
		Short:                 "Generate shell completion script",
		DisableFlagsInUseLine: true,
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Long: `Output a shell completion script for the specified shell.

To install completion permanently, append the output to the
appropriate location for your shell:

Bash (Linux):
  ncc-orchestrator completion bash | sudo tee /etc/bash_completion.d/ncc-orchestrator

Bash (macOS, Homebrew bash-completion@2):
  ncc-orchestrator completion bash > $(brew --prefix)/etc/bash_completion.d/ncc-orchestrator

Zsh (with compinit enabled):
  ncc-orchestrator completion zsh > "${fpath[1]}/_ncc-orchestrator"
  # restart your shell, or:  compinit -i

Fish:
  ncc-orchestrator completion fish > ~/.config/fish/completions/ncc-orchestrator.fish

PowerShell (current session):
  ncc-orchestrator completion powershell | Out-String | Invoke-Expression

PowerShell (persistent):
  ncc-orchestrator completion powershell > $PROFILE.ncc-orchestrator.ps1
  # then dot-source it from $PROFILE`,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletionV2(os.Stdout, true)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
			}
			return fmt.Errorf("unsupported shell %q", args[0])
		},
	}
	cmd.AddCommand(completionCmd)

	// discover-clusters subcommand: Prism Central cluster list (default v4 clustermgmt API)
	discoverCmd := &cobra.Command{
		Use:   "discover-clusters",
		Short: "List clusters from Prism Central (v4 API by default)",
		Long: `Lists registered clusters from Prism Central. Default output is one address per line (IPv4 preferred).

Use --format table for NAME, EXT_ID, ADDRESS, API columns; --format json for machine-readable rows.

Default: GET /api/clustermgmt/{nutanix-v4-api-version}/config/clusters with pagination ($page, $limit); use global --nutanix-v4-api-version (default v4.2) to match your PC API (e.g. v4.1, v4.0.a1).
Use --discover-api-version v3 for legacy POST /api/nutanix/v3/clusters/list.

If v4 returns HTTP 404, the command falls back to v3 automatically.

Use --output to write to a file (e.g. for --clusters-file).`,
		RunE: runDiscoverClusters,
	}
	discoverCmd.Flags().String("prism-central-url", "", "Prism Central URL (e.g. https://10.0.0.1:9440)")
	discoverCmd.Flags().String("username", "admin", "Prism username")
	discoverCmd.Flags().String("password", "", "Prism password (or NCC_PASSWORD)")
	discoverCmd.Flags().String("output", "", "Write cluster list to file (one per line)")
	discoverCmd.Flags().String("format", "lines", "Output format: lines (address per line), table (NAME EXT_ID ADDRESS API), or json")
	discoverCmd.Flags().String("discover-api-version", "v4", "Cluster list API: v4 (GET clustermgmt) or v3 (legacy POST); v4 path uses --nutanix-v4-api-version")
	_ = viper.BindPFlag("prism-central-url", discoverCmd.Flags().Lookup("prism-central-url"))
	// Do not BindPFlag username, password, or insecure-skip-verify here — they share keys
	// with the root command; the second bind would override viper and break the main run.
	_ = viper.BindPFlag("discover-api-version", discoverCmd.Flags().Lookup("discover-api-version"))
	cmd.AddCommand(discoverCmd)

	createScheduleCmd := &cobra.Command{
		Use:   "create-schedule",
		Short: "Create periodic scheduler for ncc-orchestrator",
		Long: `Creates a periodic schedule to run ncc-orchestrator.

On Linux/macOS, it installs (or replaces) a crontab entry by default, or a
systemd timer with --type systemd (better on systemd hosts: explicit working
directory/env, per-run logging, free overlap protection, and Persistent=true to
replay a run missed while the box was off).
On Windows, it creates or updates a Scheduled Task.

Examples:
  ncc-orchestrator create-schedule --type cron --cron "15 */4 * * *" --config config.yaml --print-only
  ncc-orchestrator create-schedule --type cron --every 4h --config config.yaml
  ncc-orchestrator create-schedule --type systemd --every 4h --config /root/ncc-orchestrator/config.yaml --print-only=false
  ncc-orchestrator create-schedule --type windows --every 4h --config C:\ncc\config.yaml
  ncc-orchestrator create-schedule --type auto --action list
  ncc-orchestrator create-schedule --type auto --action remove --print-only=false`,
		RunE: runCreateSchedule,
	}
	createScheduleCmd.Flags().String("type", "auto", "Scheduler type: auto, cron, systemd, or windows")
	createScheduleCmd.Flags().String("action", "create", "Action: create, list, remove, or run-now")
	createScheduleCmd.Flags().String("task-name", "ncc-orchestrator", "Schedule/task name marker")
	createScheduleCmd.Flags().String("command", "", "Override full command used by scheduler (advanced)")
	createScheduleCmd.Flags().String("cron", "", "Cron expression (for --type cron). If empty, derived from --every")
	createScheduleCmd.Flags().Duration("every", 4*time.Hour, "Periodic interval used for cron derivation or Windows schedule (e.g. 30m, 4h, 24h)")
	createScheduleCmd.Flags().String("log-path", "logs/ncc-scheduler.log", "Log file path for cron redirect")
	createScheduleCmd.Flags().Bool("with-lock", true, "Use flock lock file for cron runs to prevent overlap")
	createScheduleCmd.Flags().Bool("print-only", true, "Preview action without applying changes (used by create/remove)")
	cmd.AddCommand(createScheduleCmd)

	quickstartCmd := &cobra.Command{
		Use:   "quickstart",
		Short: "Guided setup and automated preflight",
		Long: `Quickstart bootstraps a starter config when missing, runs preflight checks,
and can apply safe self-healing actions for common setup issues.`,
		RunE: runQuickstartCommand,
	}
	quickstartCmd.Flags().Bool("auto-fix", true, "Apply safe auto-fixes during quickstart")
	quickstartCmd.Flags().Bool("interactive", false, "Prompt for common configuration values and write them to config")
	quickstartCmd.Flags().String("setup-v2", "ask", "v2 component setup mode: ask, download, skip")
	quickstartCmd.Flags().String("install-dir", ".ncc-v2", "Installation directory used by v2-bootstrap")
	quickstartCmd.Flags().String("repo", defaultGitHubRepo, "GitHub repo for v2 component downloads")
	quickstartCmd.Flags().Bool("assume-yes", false, "Auto-confirm quickstart prompts")
	quickstartCmd.Flags().String("automation-level", "safe-fix", "Automation policy: advisory, safe-fix, full-auto")
	cmd.AddCommand(quickstartCmd)

	configSchemaCmd := &cobra.Command{
		Use:   "config-schema",
		Short: "Print JSON schema for config.yaml",
		RunE:  runConfigSchema,
	}
	configSchemaCmd.Flags().String("output", "", "Write schema JSON to file (default: stdout)")
	cmd.AddCommand(configSchemaCmd)

	cmd.AddCommand(newRetentionCmd())

	validateConfigCmd := &cobra.Command{
		Use:   "validate-config",
		Short: "Validate configuration file and exit (use preflight-check for full guidance)",
		RunE:  runValidateConfigCommand,
	}
	cmd.AddCommand(validateConfigCmd)

	validateSecretsCmd := &cobra.Command{
		Use:   "validate-secrets",
		Short: "Validate secret:// references and secret source accessibility (use preflight-check for full guidance)",
		RunE:  runValidateSecretsCommand,
	}
	cmd.AddCommand(validateSecretsCmd)

	preflightCmd := &cobra.Command{
		Use:   "preflight-check",
		Short: "Run combined config/secrets/path preflight checks",
		RunE:  runPreflightCheckCommand,
	}
	preflightCmd.Flags().String("format", "json", "Output format (json)")
	cmd.AddCommand(preflightCmd)

	uninstallCmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall NCC-created local runtime artifacts/state",
		Long: `Uninstall removes local artifacts/state generated by ncc-orchestrator.
Kubernetes uninstall is intentionally script-only (use scripts/uninstall-v2-clean.sh).

Examples:
  ncc-orchestrator uninstall --dry-run
  ncc-orchestrator uninstall --config config.yaml --force`,
		RunE: runUninstallCommand,
	}
	uninstallCmd.Flags().String("install-dir", "", "v2 install directory created by bootstrap/start (default: auto-detect from running binary's stack layout, fallback .ncc-v2)")
	uninstallCmd.Flags().String("task-name", "ncc-orchestrator", "Scheduler task marker name to remove")
	uninstallCmd.Flags().Bool("remove-local", true, "Remove local NCC artifacts/state (outputfiles, nccfiles, promfiles, logs, token/state files)")
	uninstallCmd.Flags().Bool("remove-schedule", true, "Remove scheduler entry created by create-schedule")
	uninstallCmd.Flags().Bool("remove-v2-runtime", true, "Stop detached v2 services and remove install-dir")
	uninstallCmd.Flags().Bool("force", false, "Force stop behavior and non-interactive uninstall where applicable")
	uninstallCmd.Flags().Bool("dry-run", false, "Print actions without deleting")
	cmd.AddCommand(uninstallCmd)

	return cmd
}
