package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Collector  CollectorConfig  `json:"collector"`
	Processor  ProcessorConfig  `json:"processor"`
	Output     OutputConfig     `json:"output"`
	StatsPort  int              `json:"stats_port"`
	DLQPath    string           `json:"dlq_path"`
	LogLevel   string           `json:"log_level"`
	BufferSize int              `json:"buffer_size"`
}

type CollectorConfig struct {
	Paths        []string `json:"paths"`
	BookmarkFile string   `json:"bookmark_file"`
}

type ProcessorConfig struct {
	Workers       int               `json:"workers"`
	RegexPatterns []string          `json:"regex_patterns"`
	MinLevel      string            `json:"min_level"`
	DropRegexes   []string          `json:"drop_regexes"`
	Labels        map[string]string `json:"labels"`
	LabelMode     string            `json:"label_mode"` // "nested" or "flattened"
}

type OutputConfig struct {
	Type          string     `json:"type"` // "elasticsearch", "opensearch", "stdout", "file"
	Elasticsearch ESConfig   `json:"elasticsearch"`
	OpenSearch    OSConfig   `json:"opensearch"`
	File          FileConfig `json:"file"`
}

type FileConfig struct {
	Path       string `json:"path"`
	MaxSizeMB  int64  `json:"max_size_mb"`
	MaxBackups int    `json:"max_backups"`
}

type OSConfig struct {
	URLs        []string      `json:"urls"`
	Index       string        `json:"index"`
	BatchSize   int           `json:"batch_size"`
	FlushPeriod time.Duration `json:"flush_period_ms"`
	Username    string        `json:"username"`
	Password    string        `json:"password"`
	CACertPath  string        `json:"ca_cert_path"`
}

type ESConfig struct {
	URLs        []string      `json:"urls"`
	Index       string        `json:"index"`
	BatchSize   int           `json:"batch_size"`
	FlushPeriod time.Duration `json:"flush_period_ms"`
	Username    string        `json:"username"`
	Password    string        `json:"password"`
	APIKey      string        `json:"api_key"`
	CACertPath  string        `json:"ca_cert_path"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, err
	}
	
	// Apply defaults
	if cfg.StatsPort == 0 {
		cfg.StatsPort = 8080
	}
	if cfg.DLQPath == "" {
		cfg.DLQPath = "dead_letters.jsonl"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "INFO"
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 1000
	}
	if cfg.Processor.Workers == 0 {
		cfg.Processor.Workers = 2
	}
	if cfg.Processor.LabelMode == "" {
		cfg.Processor.LabelMode = "nested"
	}
	if cfg.Collector.BookmarkFile == "" {
		cfg.Collector.BookmarkFile = "bookmarks.json"
	}
	if cfg.Output.Elasticsearch.BatchSize == 0 {
		cfg.Output.Elasticsearch.BatchSize = 100
	}
	if cfg.Output.Elasticsearch.FlushPeriod == 0 {
		cfg.Output.Elasticsearch.FlushPeriod = 5000 // default 5 seconds
	}
	if cfg.Output.OpenSearch.BatchSize == 0 {
		cfg.Output.OpenSearch.BatchSize = 100
	}
	if cfg.Output.OpenSearch.FlushPeriod == 0 {
		cfg.Output.OpenSearch.FlushPeriod = 5000 // default 5 seconds
	}
	// Convert milliseconds to duration
	cfg.Output.Elasticsearch.FlushPeriod = cfg.Output.Elasticsearch.FlushPeriod * time.Millisecond
	cfg.Output.OpenSearch.FlushPeriod = cfg.Output.OpenSearch.FlushPeriod * time.Millisecond

	// Apply file defaults
	if cfg.Output.File.Path == "" {
		cfg.Output.File.Path = "aggregator_output.jsonl"
	}
	if cfg.Output.File.MaxSizeMB == 0 {
		cfg.Output.File.MaxSizeMB = 10
	}
	if cfg.Output.File.MaxBackups == 0 {
		cfg.Output.File.MaxBackups = 5
	}

	// Normalize and validate MinLevel
	if cfg.Processor.MinLevel != "" {
		cfg.Processor.MinLevel = strings.ToUpper(strings.TrimSpace(cfg.Processor.MinLevel))
		validLevels := map[string]bool{"DEBUG": true, "INFO": true, "WARN": true, "ERROR": true, "FATAL": true}
		if !validLevels[cfg.Processor.MinLevel] {
			return nil, fmt.Errorf("invalid min_level in config: %q (valid: DEBUG, INFO, WARN, ERROR, FATAL)", cfg.Processor.MinLevel)
		}
	}

	// Validate LabelMode
	if cfg.Processor.LabelMode != "nested" && cfg.Processor.LabelMode != "flattened" {
		return nil, fmt.Errorf("invalid label_mode in config: %q (valid: nested, flattened)", cfg.Processor.LabelMode)
	}

	// Run structural validation
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks that the config is structurally valid beyond just defaults.
// It catches misconfigurations that would otherwise cause silent failures or panics at runtime.
func (c *Config) Validate() error {
	// Collector must have at least one path
	if len(c.Collector.Paths) == 0 {
		return fmt.Errorf("config validation: collector.paths must contain at least one path")
	}

	// Output type must be set
	if c.Output.Type == "" {
		return fmt.Errorf("config validation: output.type must be set (elasticsearch, opensearch, stdout, file)")
	}

	// Validate backend-specific fields
	switch c.Output.Type {
	case "elasticsearch":
		if len(c.Output.Elasticsearch.URLs) == 0 {
			return fmt.Errorf("config validation: elasticsearch.urls must not be empty when output type is 'elasticsearch'")
		}
		if c.Output.Elasticsearch.Index == "" {
			return fmt.Errorf("config validation: elasticsearch.index must not be empty when output type is 'elasticsearch'")
		}
	case "opensearch":
		if len(c.Output.OpenSearch.URLs) == 0 {
			return fmt.Errorf("config validation: opensearch.urls must not be empty when output type is 'opensearch'")
		}
		if c.Output.OpenSearch.Index == "" {
			return fmt.Errorf("config validation: opensearch.index must not be empty when output type is 'opensearch'")
		}
	case "stdout", "file":
		// No additional validation required
	default:
		return fmt.Errorf("config validation: unknown output type %q (valid: elasticsearch, opensearch, stdout, file)", c.Output.Type)
	}

	// Buffer size must be positive
	if c.BufferSize <= 0 {
		return fmt.Errorf("config validation: buffer_size must be positive, got %d", c.BufferSize)
	}

	return nil
}
