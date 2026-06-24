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
func NewOutput(cfg config.OutputConfig, dlq *DLQ) (Output, error) {
	switch cfg.Type {
	case "elasticsearch":
		return NewElasticsearchOutput(cfg.Elasticsearch, dlq)
	case "opensearch":
		return NewOpenSearchOutput(cfg.OpenSearch, dlq)
	case "stdout":
		return NewStdoutOutput(), nil
	case "file":
		return NewFileOutput(cfg.File)
	default:
		return nil, fmt.Errorf("unknown output type: %s", cfg.Type)
	}
}
