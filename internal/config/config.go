package config

import (
	"encoding/json"
	"os"
	"time"
)

type Config struct {
	Collector CollectorConfig `json:"collector"`
	Processor ProcessorConfig `json:"processor"`
	Output    OutputConfig    `json:"output"`
}

type CollectorConfig struct {
	Paths        []string `json:"paths"`
	BookmarkFile string   `json:"bookmark_file"`
}

type ProcessorConfig struct {
	Workers       int      `json:"workers"`
	RegexPatterns []string `json:"regex_patterns"`
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
	if cfg.Processor.Workers == 0 {
		cfg.Processor.Workers = 2
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

	return &cfg, nil
}
