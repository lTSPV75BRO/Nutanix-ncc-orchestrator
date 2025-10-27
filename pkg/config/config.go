package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"crypto/tls"

	"github.com/spf13/viper"
)

// Config holds all configuration for the NCC orchestrator
type Config struct {
	Clusters           []string
	Username           string
	Password           string
	InsecureSkipVerify bool
	Timeout            time.Duration // per-cluster overall timeout
	RequestTimeout     time.Duration // per HTTP request timeout
	PollInterval       time.Duration
	PollJitter         time.Duration
	OutputDirLogs      string
	OutputDirFiltered  string
	OutputFormats      []string // html,csv
	MaxParallel        int
	TLSMinVersion      uint16
	LogFile            string

	// Logging options
	LogLevel string // 0..5 or names
	LogHTTP  bool   // dump HTTP request/response

	// Retry tuning
	RetryMaxAttempts int
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Clusters:           []string{},
		Username:           "",
		Password:           "",
		InsecureSkipVerify: false,
		Timeout:            15 * time.Minute,
		RequestTimeout:     20 * time.Second,
		PollInterval:       15 * time.Second,
		PollJitter:         2 * time.Second,
		OutputDirLogs:      "nccfiles",
		OutputDirFiltered:  "outputfiles",
		OutputFormats:      []string{"html"},
		MaxParallel:        4,
		TLSMinVersion:      tls.VersionTLS12,
		LogFile:            "logs/ncc-runner.log",
		LogLevel:           "info",
		LogHTTP:            false,
		RetryMaxAttempts:   6,
		RetryBaseDelay:     400 * time.Millisecond,
		RetryMaxDelay:      8 * time.Second,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("no clusters provided")
	}
	if c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if c.MaxParallel <= 0 {
		return fmt.Errorf("max-parallel must be greater than 0")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request-timeout must be greater than 0")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll-interval must be greater than 0")
	}
	if c.RetryMaxAttempts <= 0 {
		return fmt.Errorf("retry-max-attempts must be greater than 0")
	}
	return nil
}

// Helper functions
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func mustParseDur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}

// LoadConfig loads configuration from various sources
func LoadConfig() (*Config, error) {
	cfg := DefaultConfig()

	// Load from config file if specified
	cfgFile := viper.GetString("config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
			if err := writeDummyConfig(cfgFile); err != nil {
				return nil, fmt.Errorf("failed to create dummy config at %s: %w", cfgFile, err)
			}
			return nil, fmt.Errorf("dummy config created at %s; edit and re-run", cfgFile)
		}
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	// Set up environment variable binding
	viper.SetEnvPrefix("ncc")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Load configuration values
	cfg.Clusters = splitCSV(viper.GetString("clusters"))
	cfg.Username = viper.GetString("username")
	cfg.Password = viper.GetString("password")
	cfg.InsecureSkipVerify = viper.GetBool("insecure-skip-verify")
	cfg.Timeout = mustParseDur(viper.GetString("timeout"), cfg.Timeout)
	cfg.RequestTimeout = mustParseDur(viper.GetString("request-timeout"), cfg.RequestTimeout)
	cfg.PollInterval = mustParseDur(viper.GetString("poll-interval"), cfg.PollInterval)
	cfg.PollJitter = mustParseDur(viper.GetString("poll-jitter"), cfg.PollJitter)
	cfg.OutputDirLogs = viper.GetString("output-dir-logs")
	cfg.OutputDirFiltered = viper.GetString("output-dir-filtered")
	cfg.OutputFormats = splitCSV(viper.GetString("outputs"))
	cfg.MaxParallel = viper.GetInt("max-parallel")
	cfg.LogFile = viper.GetString("log-file")
	cfg.LogLevel = viper.GetString("log-level")
	cfg.LogHTTP = viper.GetBool("log-http")
	cfg.RetryMaxAttempts = viper.GetInt("retry-max-attempts")
	cfg.RetryBaseDelay = mustParseDur(viper.GetString("retry-base-delay"), cfg.RetryBaseDelay)
	cfg.RetryMaxDelay = mustParseDur(viper.GetString("retry-max-delay"), cfg.RetryMaxDelay)

	// Apply defaults for empty values
	if cfg.OutputDirLogs == "" {
		cfg.OutputDirLogs = "nccfiles"
	}
	if cfg.OutputDirFiltered == "" {
		cfg.OutputDirFiltered = "outputfiles"
	}
	if len(cfg.OutputFormats) == 0 {
		cfg.OutputFormats = []string{"html"}
	}
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = 4
	}
	if cfg.LogFile == "" {
		cfg.LogFile = "logs/ncc-runner.log"
	}
	if cfg.RetryMaxAttempts <= 0 {
		cfg.RetryMaxAttempts = 6
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 400 * time.Millisecond
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = 8 * time.Second
	}

	return cfg, nil
}

// writeDummyConfig creates a dummy configuration file
func writeDummyConfig(path string) error {
	ext := strings.ToLower(filepath.Ext(path))
	dummy := ""
	switch ext {
	case ".yaml", ".yml":
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env

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
`
	case ".json":
		dummy = `{
  "clusters": ["10.0.0.1", "10.0.0.2"],
  "username": "admin",
  "password": "",
  "insecure-skip-verify": false,
  "timeout": "15m",
  "request-timeout": "30s",
  "poll-interval": "15s",
  "poll-jitter": "2s",
  "max-parallel": 4,
  "outputs": "html,csv",
  "output-dir-logs": "nccfiles",
  "output-dir-filtered": "outputfiles",
  "log-file": "logs/ncc-runner.log",
  "log-level": "2",
  "log-http": false,
  "retry-max-attempts": 6,
  "retry-base-delay": "400ms",
  "retry-max-delay": "8s"
}
`
	default:
		dummy = `# NCC Runner configuration (dummy values)

# Required
clusters: "10.2.XX.XX,10.0.XX.XX"      	  # Comma-separated list of Prism Element cluster IPs/cluster FQDNs
username: "admin"                         # Prism element username
password: ""                              # Prefer env NCC_PASSWORD in CLI; leave empty here if using env

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
`
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(dummy), 0644)
}
