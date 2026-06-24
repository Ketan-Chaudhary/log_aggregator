package processor

import (
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

type ProcessorRuntime struct {
	MinSeverity    int
	DropRegexes    []*regexp.Regexp
	ExtractRegexes []*regexp.Regexp
	Labels         map[string]string
	FlattenLabels  bool
}

var levelMap = map[string]int{
	"UNKNOWN": 0,
	"DEBUG":   1,
	"INFO":    2,
	"WARN":    3,
	"ERROR":   4,
	"FATAL":   5,
}

func getSeverity(level string) int {
	level = strings.ToUpper(strings.TrimSpace(level))
	if val, ok := levelMap[level]; ok {
		return val
	}
	return 0 // UNKNOWN
}

func StartWorkerPool(cfg config.ProcessorConfig, in <-chan models.LogEntry, out chan<- models.LogEntry) *sync.WaitGroup {
	numWorkers := cfg.Workers
	if numWorkers < 1 {
		slog.Warn("Invalid worker count, defaulting to 1", "configured", numWorkers)
		numWorkers = 1
	}

	runtime := &ProcessorRuntime{
		MinSeverity:   getSeverity(cfg.MinLevel),
		Labels:        cfg.Labels,
		FlattenLabels: cfg.LabelMode == "flattened",
	}

	for _, pattern := range cfg.RegexPatterns {
		if re, err := regexp.Compile(pattern); err == nil {
			runtime.ExtractRegexes = append(runtime.ExtractRegexes, re)
		} else {
			slog.Error("Failed to compile extract regex", "pattern", pattern, "error", err)
		}
	}

	for _, pattern := range cfg.DropRegexes {
		if re, err := regexp.Compile(pattern); err == nil {
			runtime.DropRegexes = append(runtime.DropRegexes, re)
		} else {
			slog.Error("Failed to compile drop regex", "pattern", pattern, "error", err)
		}
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(wg, runtime, in, out)
	}
	return wg
}
