package output

import (
	"fmt"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

// Output defines the interface for all log destinations.
type Output interface {
	Run(in <-chan models.LogEntry)
}

// NewOutput creates an Output based on the configuration.
func NewOutput(cfg config.OutputConfig) (Output, error) {
	switch cfg.Type {
	case "elasticsearch":
		return NewElasticsearchOutput(
			cfg.Elasticsearch.URLs,
			cfg.Elasticsearch.Index,
			cfg.Elasticsearch.BatchSize,
			cfg.Elasticsearch.FlushPeriod,
		)
	case "stdout":
		return NewStdoutOutput(), nil
	default:
		return nil, fmt.Errorf("unknown output type: %s", cfg.Type)
	}
}
