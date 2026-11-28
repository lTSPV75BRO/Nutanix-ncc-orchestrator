package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/vbauerster/mpb/v7/decor"
	"golang.org/x/term"
)

// ==================== Orchestration ====================

func sanitizeSummary(s string) string {
	return strings.ReplaceAll(s, "\\n", "\n")
}

// writeSummaryText saves the raw NCC summary and also returns the exact text
// written (sanitized), so callers can parse it without reading the file back.
func writeSummaryText(fs FS, folder, cluster, summary string) (string, string, error) {
	if err := fs.MkdirAll(folder, 0755); err != nil {
		return "", "", err
	}
	outPath := filepath.Join(folder, fmt.Sprintf("%s.log", cluster))
	log.Debug().Str("path", outPath).Int("bytes", len(summary)).Msg("writing summary")
	text := sanitizeSummary(summary)
	if err := fs.WriteFile(outPath, []byte(text), 0644); err != nil {
		return "", "", err
	}
	return outPath, text, nil
}

// clusterFetch is what a cluster hands back once its NCC run finished.
type clusterFetch struct {
	RunSummary string
	Started    time.Time
}

// fetchClusterSummary is the part of a cluster's run that needs the cluster:
// start NCC, poll the task, read the summary. A run's max-parallel slots cover
// only this; writing reports and sending notifications (processClusterSummary)
// happen in a separate pool so they never hold a cluster slot.
func fetchClusterSummary(
	ctx context.Context,
	cfg Config,
	httpc HTTPClient,
	cluster string,
	username string,
	password string,
	onPct func(int),
	setPhase func(string),
) (clusterFetch, error) {
	l := log.With().Str("cluster", cluster).Logger()
	client := NewNCCClient(cluster, username, password, httpc, cfg)
	var clusterStart = time.Now()
	setPhase("starting")
	l.Info().Msg("starting NCC checks")
	taskID, body, err := client.StartChecks(ctx)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("start checks failed")
		return clusterFetch{}, fmt.Errorf("start checks failed: %w", err)
	}
	l.Info().Str("taskID", taskID).Msg("ncc task started")
	taskStart := time.Now()
	onPct(1)

	last := 1
	setPhase("polling")
	for done := false; !done; {
		select {
		case <-ctx.Done():
			l.Warn().Err(ctx.Err()).Msg("context cancelled during polling, stopping gracefully")
			return clusterFetch{}, fmt.Errorf("operation cancelled: %w", ctx.Err())
		case <-time.After(nextPollDelay(cfg.PollInterval, cfg.PollJitter, time.Since(taskStart), last)):
			if dl, ok := ctx.Deadline(); ok {
				rem := time.Until(dl)
				if rem < 10*time.Second {
					l.Warn().Dur("remaining", rem).Msg("cluster deadline near")
				}
			}
			status, body, err := client.GetTask(ctx, taskID)
			if err != nil {
				l.Error().Err(err).RawJSON("response_body", body).Msg("poll failed")
				return clusterFetch{}, fmt.Errorf("poll failed: %w", err)
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
				return clusterFetch{}, fmt.Errorf("ncc task failed")
			}
			done = pct >= 100
		}
	}

	setPhase("summary")
	summary, body, err := client.GetRunSummary(ctx, taskID)
	if err != nil {
		l.Error().Err(err).RawJSON("response_body", body).Msg("get summary failed")
		return clusterFetch{}, fmt.Errorf("get summary failed: %w", err)
	}
	return clusterFetch{RunSummary: summary.RunSummary, Started: clusterStart}, nil
}

// nextPollDelay picks when to ask a running NCC task for its status again.
//
// A fixed interval finds a finished task on average half an interval late
// (~7.5 s per cluster at the 15 s default, ~4% of a 3-minute run, multiplied
// across thousands of clusters). The task reports its percentage, so the
// remaining time is estimated from it: early on it polls at the configured
// interval (never less often), and near the end it aims for just after the
// estimated finish — but never more often than interval/5, nor faster than
// once a second, so a cluster is not hammered when the estimate is off.
func nextPollDelay(interval, jitter, elapsed time.Duration, pct int) time.Duration {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	spread := func(max time.Duration) time.Duration {
		if max <= 0 {
			return 0
		}
		return time.Duration(rand.Int63n(int64(max)))
	}
	if pct <= 1 || pct >= 100 || elapsed <= 0 {
		return interval + spread(jitter)
	}
	remaining := time.Duration(float64(elapsed) * float64(100-pct) / float64(pct))
	if remaining >= interval {
		return interval + spread(jitter)
	}
	floor := interval / 5
	if floor < time.Second {
		floor = time.Second
	}
	if remaining < floor {
		remaining = floor
	}
	// Land just after the estimate rather than just before it.
	return remaining + remaining/10 + spread(jitter/4)
}

// processClusterSummary turns a fetched NCC summary into the cluster's
// files, findings and notifications. It needs no cluster connection.
func processClusterSummary(
	ctx context.Context,
	cfg Config,
	fs FS,
	httpc HTTPClient,
	cluster string,
	fetched clusterFetch,
	setPhase func(string),
) (ClusterRunOutput, error) {
	l := log.With().Str("cluster", cluster).Logger()
	clusterStart := fetched.Started
	setPhase("writing")
	logPath, rawText, err := writeSummaryText(fs, cfg.OutputDirLogs, cluster, fetched.RunSummary)
	if err != nil {
		l.Error().Err(err).Msg("write summary failed")
		return ClusterRunOutput{}, err
	}
	l.Info().Str("logPath", logPath).Msg("summary written")
	// Header metadata comes from the same text (used by the per-cluster
	// reports here and by the run aggregate, which no longer re-reads 7,000
	// raw logs one by one after the run).
	meta := parseNCCHeaderText(rawText)

	// Parse blocks and write filtered file in one operation to avoid duplicate parsing
	filteredPath := filepath.Join(cfg.OutputDirFiltered, fmt.Sprintf("%s.log", cluster))
	blocks, err := filterBlocksFromText(fs, rawText, logPath, filteredPath)
	if err != nil {
		l.Error().Err(err).Msg("filter blocks failed")
		return ClusterRunOutput{}, err
	}
	l.Info().Str("filteredPath", filteredPath).Msg("filtered written")

	blocks, excludedByTitle, err := applyBlockFilters(cfg, blocks, l)
	if err != nil {
		return ClusterRunOutput{}, err
	}

	counts := map[string]int{"FAIL": 0, "WARN": 0, "ERR": 0, "INFO": 0}
	for _, b := range blocks {
		sev := b.Severity
		if sev == "" {
			sev = "INFO"
		}
		counts[sev]++
	}
	// Write Prometheus file and output formats (HTML/CSV/JSON) before notifications so we can attach HTML
	if cfg.PromEnabled {
		if err := writePrometheusFile(fs, cfg.PromDir, cluster, blocks); err != nil {
			l.Error().Err(err).Msg("write Prometheus .prom failed")
		} else {
			log.Info().Str("cluster", cluster).Str("prom_dir", cfg.PromDir).Msg("Prometheus .prom written")
		}
	} else {
		log.Debug().Str("cluster", cluster).Msg("Prometheus metrics export disabled")
	}

	var htmlPathForNotify string
	base := filteredPath
	loadMeta := func() HTMLMeta { return meta }
	for _, f := range cfg.OutputFormats {
		switch strings.ToLower(strings.TrimSpace(f)) {
		case "html":
			htmlFile := base + ".html"
			rows := rowsFromBlocks(blocks)
			if err := generateHTML(fs, rows, htmlFile, loadMeta()); err != nil {
				l.Error().Err(err).Str("file", htmlFile).Msg("write HTML failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", htmlFile).Msg("HTML generated")
			htmlPathForNotify = htmlFile
		case "csv":
			csvFile := base + ".csv"
			if err := generateCSV(fs, blocks, csvFile); err != nil {
				l.Error().Err(err).Str("file", csvFile).Msg("write CSV failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", csvFile).Msg("CSV generated")
		case "json":
			jsonFile := base + ".json"
			if err := generateJSON(fs, blocks, jsonFile, loadMeta()); err != nil {
				l.Error().Err(err).Str("file", jsonFile).Msg("write JSON failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", jsonFile).Msg("JSON generated")
		case "markdown":
			mdFile := base + ".md"
			if err := generateMarkdown(fs, blocks, mdFile, loadMeta()); err != nil {
				l.Error().Err(err).Str("file", mdFile).Msg("write Markdown failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", mdFile).Msg("Markdown generated")
		case "sarif":
			sarifFile := base + ".sarif"
			if err := generateSARIF(fs, blocks, sarifFile); err != nil {
				l.Error().Err(err).Str("file", sarifFile).Msg("write SARIF failed")
				return ClusterRunOutput{}, err
			}
			l.Info().Str("file", sarifFile).Msg("SARIF generated")
		default:
			l.Warn().Str("format", f).Msg("unknown output format")
		}
	}

	if len(blocks) == 0 {
		l.Warn().Str("path", filteredPath).Msg("no blocks parsed from summary")
	}

	// Build notification summary with brief overview; optionally include HTML in webhook
	overview := fmt.Sprintf("NCC run completed for cluster %s. FAIL: %d, WARN: %d, ERR: %d, INFO: %d. Total: %d checks.",
		cluster, counts["FAIL"], counts["WARN"], counts["ERR"], counts["INFO"], len(blocks))
	summaryNotify := NotificationSummary{
		Cluster:     cluster,
		StartedAt:   clusterStart,
		FinishedAt:  time.Now(),
		FailCount:   counts["FAIL"],
		WarnCount:   counts["WARN"],
		ErrCount:    counts["ERR"],
		InfoCount:   counts["INFO"],
		TotalChecks: len(blocks),
		OutputFiles: []string{filteredPath},
		Overview:    overview,
	}
	if cfg.WebhookIncludeHTML && htmlPathForNotify != "" {
		if b, err := os.ReadFile(htmlPathForNotify); err == nil {
			summaryNotify.ReportHTMLBase64 = base64.StdEncoding.EncodeToString(b)
		}
	}

	subj := fmt.Sprintf("NCC %s: FAIL=%d WARN=%d", summaryNotify.Cluster,
		summaryNotify.FailCount, summaryNotify.WarnCount)
	bodyEmail := overview + "\n\n" + fmt.Sprintf("FAIL: %d | WARN: %d | Total: %d\nFiltered: %s",
		summaryNotify.FailCount, summaryNotify.WarnCount, len(blocks), filteredPath)
	subj, bodyEmail = applyEmailTemplates(cfg, subj, bodyEmail, summaryNotify, l)

	if !cfg.NotifyDigest {
		if suppressed, reason := notificationsSuppressedNow(cfg, time.Now()); suppressed {
			l.Info().Str("reason", reason).Msg("notifications suppressed by quiet-hours/maintenance-window")
			setPhase("done")
			return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle, Meta: meta}, nil
		}
		if cfg.NotifyOnRegression {
			prevFail := cfg.PreviousClusterFailCount[cluster]
			if summaryNotify.FailCount <= prevFail {
				l.Info().
					Int("current_fail", summaryNotify.FailCount).
					Int("previous_fail", prevFail).
					Msg("notify-on-regression enabled: skipping non-regression cluster notification")
				setPhase("done")
				return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle, Meta: meta}, nil
			}
		}
		if err := sendEmailWithRetry(cfg, subj, bodyEmail, htmlPathForNotify); err != nil {
			l.Error().Err(err).Msg("email failed")
		}
		if err := sendWebhookWithRetry(ctx, httpc, cfg, summaryNotify); err != nil {
			l.Error().Err(err).Msg("webhook failed")
		}
		if err := sendSlackWithRetry(ctx, httpc, cfg, summaryNotify); err != nil {
			l.Error().Err(err).Msg("slack notification failed")
		}
		l.Info().Int("fail", summaryNotify.FailCount).Int("warn", summaryNotify.WarnCount).Msg("notifications sent")
	}

	setPhase("done")
	if len(excludedByTitle) > 0 {
		l.Info().Int("excluded_alerts", len(excludedByTitle)).Msg("alerts excluded by title filters")
	}
	return ClusterRunOutput{Blocks: blocks, ExcludedByTitle: excludedByTitle, Meta: meta}, nil
}

// ==================== CLI ====================

type ClusterResult struct {
	Cluster         string
	Blocks          []ParsedBlock
	ExcludedByTitle []ExcludedAlert
	Meta            HTMLMeta // cluster name/versions from the NCC summary header
	Err             error
	ErrorClass      string
}

type ClusterRunOutput struct {
	Blocks          []ParsedBlock
	ExcludedByTitle []ExcludedAlert
	Meta            HTMLMeta
}

type proxyDecorator struct{ text string }

func (p *proxyDecorator) Decor(ctx decor.Statistics) string { return p.text }
func (p *proxyDecorator) Sync() (chan int, bool)            { return nil, false }
func (p *proxyDecorator) GetConf() decor.WC                 { return decor.WC{} }
func (p *proxyDecorator) SetConf(wc decor.WC)               {}
func (p *proxyDecorator) SetText(s string)                  { p.text = s }

func promptPasswordIfEmpty(p string, Username string) (string, error) {
	if p != "" {
		return p, nil
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", errors.New("password is empty and stdin is not interactive; set password in config/clusters-file or export NCC_PASSWORD")
	}
	fmt.Printf("Prism Password (%s): ", Username)
	bytePw, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", fmt.Errorf("read password from terminal: %w", err)
	}
	return strings.TrimSpace(string(bytePw)), nil
}
