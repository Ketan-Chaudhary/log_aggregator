package processor

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func StartWorkerPool(cfg config.ProcessorConfig, in <-chan models.LogEntry, out chan<- models.LogEntry) *sync.WaitGroup {
	numWorkers := cfg.Workers
	if numWorkers < 1 {
		fmt.Println("Invalid Worker count: defaulting to 1")
		numWorkers = 1
	}

	var compiledRegexes []*regexp.Regexp
	for _, pattern := range cfg.RegexPatterns {
		if re, err := regexp.Compile(pattern); err == nil {
			compiledRegexes = append(compiledRegexes, re)
		} else {
			fmt.Printf("Failed to compile regex pattern %s: %v\n", pattern, err)
		}
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(wg, compiledRegexes, in, out)
	}
	return wg
}
