package output

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
)

// ElasticsearchOutput sends logs to Elasticsearch using the Bulk API.
// It delegates batching and retry logic to a shared BatchSender.
type ElasticsearchOutput struct {
	client *elasticsearch.Client
	index  string
	sender *BatchSender
}

func NewElasticsearchOutput(
	cfg config.ESConfig,
	dlq *DLQ,
) (*ElasticsearchOutput, error) {

	esCfg := elasticsearch.Config{
		Addresses: cfg.URLs,
		Username:  cfg.Username,
		Password:  cfg.Password,
		APIKey:    cfg.APIKey,
	}

	if cfg.CACertPath != "" {
		cert, err := os.ReadFile(cfg.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert from %s: %w", cfg.CACertPath, err)
		}
		esCfg.CACert = cert
	}

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, err
	}

	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("error pinging ES: %s", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("ES responded with error: %s", res.String())
	}

	output := &ElasticsearchOutput{
		client: es,
		index:  cfg.Index,
	}

	output.sender = NewBatchSender(BatchSenderConfig{
		BatchSize:   cfg.BatchSize,
		FlushPeriod: cfg.FlushPeriod,
		NumWorkers:  3,
		DLQ:         dlq,
		Flusher:     output,
	})

	return output, nil
}

func (e *ElasticsearchOutput) Name() string {
	return "Elasticsearch"
}

func (e *ElasticsearchOutput) Run(ctx context.Context, in <-chan models.LogEntry) {
	e.sender.Run(ctx, in)
}

// FlushBulk implements the BulkFlusher interface.
func (e *ElasticsearchOutput) FlushBulk(ctx context.Context, logs []models.LogEntry) (retryable []models.LogEntry, dropped int, err error) {
	buf, buildErr := e.buildBulkBody(logs)
	if buildErr != nil {
		return nil, 0, fmt.Errorf("failed to build bulk request: %w", buildErr)
	}

	res, err := e.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		e.client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("bulk request network error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		status := res.StatusCode
		if isRetryableStatus(status) {
			return logs, 0, fmt.Errorf("bulk request failed with retryable status: %s", res.Status())
		}
		return nil, 0, fmt.Errorf("bulk request failed with non-retryable status: %s", res.Status())
	}

	var bulkResp BulkResponse
	if decodeErr := json.NewDecoder(res.Body).Decode(&bulkResp); decodeErr != nil {
		return nil, 0, fmt.Errorf("failed to decode bulk response: %w", decodeErr)
	}

	if !bulkResp.Errors {
		return nil, 0, nil // complete success
	}

	// Partial failures — separate retryable from non-retryable
	for idx, item := range bulkResp.Items {
		for _, result := range item {
			status := result.Status
			if status >= 200 && status < 300 {
				continue
			}

			slog.Warn("bulk item failed",
				"backend", "Elasticsearch",
				"status", status,
				"error", result.Error,
			)

			if isRetryableStatus(status) {
				if idx < len(logs) {
					retryable = append(retryable, logs[idx])
				}
			} else {
				if e.sender.dlq != nil && idx < len(logs) {
					e.sender.dlq.Write(logs[idx], fmt.Sprintf("non-retryable ES error: status=%d", status))
				}
				dropped++
			}
		}
	}

	return retryable, dropped, nil
}

func (e *ElasticsearchOutput) buildBulkBody(logs []models.LogEntry) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	for _, logEntry := range logs {
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": e.index,
			},
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		dataJSON, err := json.Marshal(logEntry)
		if err != nil {
			return nil, err
		}
		buf.Write(metaJSON)
		buf.WriteByte('\n')
		buf.Write(dataJSON)
		buf.WriteByte('\n')
	}
	return &buf, nil
}

// BulkResponse and BulkItem are shared types for bulk API responses.
type BulkResponse struct {
	Errors bool                  `json:"errors"`
	Items  []map[string]BulkItem `json:"items"`
}

type BulkItem struct {
	Status int         `json:"status"`
	Error  interface{} `json:"error,omitempty"`
}

func isRetryableStatus(status int) bool {
	switch status {
	case 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

func shouldRetry(res *esapi.Response, err error) bool {
	if err != nil {
		return true
	}
	if res == nil {
		return false
	}
	return isRetryableStatus(res.StatusCode)
}
