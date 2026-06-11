package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Collector CollectorConfig `json:"collector"`
	Processor ProcessorConfig `json:"processor"`
	Output    OutputConfig    `json:"output"`
	StatsPort int             `json:"stats_port"`
	DLQPath   string          `json:"dlq_path"`
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
	Type          string   `json:"type"` // "elasticsearch", "stdout"
	Elasticsearch ESConfig `json:"elasticsearch"`
}

type ESConfig struct {
	URLs        []string      `json:"urls"`
	Index       string        `json:"index"`
	BatchSize   int           `json:"batch_size"`
	FlushPeriod time.Duration `json:"flush_period_ms"`
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
	// Convert milliseconds to duration
	cfg.Output.Elasticsearch.FlushPeriod = cfg.Output.Elasticsearch.FlushPeriod * time.Millisecond

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

	return &cfg, nil
}
