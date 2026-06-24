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
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// OpenSearchOutput sends logs to OpenSearch using the Bulk API.
// It delegates batching and retry logic to a shared BatchSender.
type OpenSearchOutput struct {
	client *opensearch.Client
	index  string
	sender *BatchSender
}

func NewOpenSearchOutput(
	cfg config.OSConfig,
	dlq *DLQ,
) (*OpenSearchOutput, error) {

	osCfg := opensearch.Config{
		Addresses: cfg.URLs,
		Username:  cfg.Username,
		Password:  cfg.Password,
	}

	if cfg.CACertPath != "" {
		cert, err := os.ReadFile(cfg.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert from %s: %w", cfg.CACertPath, err)
		}
		osCfg.CACert = cert
	}

	client, err := opensearch.NewClient(osCfg)
	if err != nil {
		return nil, err
	}

	res, err := client.Do(context.Background(), opensearchapi.InfoReq{}, nil)
	if err != nil {
		return nil, fmt.Errorf("error pinging OpenSearch: %s", err)
	}
	defer res.Body.Close()

	output := &OpenSearchOutput{
		client: client,
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

func (o *OpenSearchOutput) Name() string {
	return "OpenSearch"
}

func (o *OpenSearchOutput) Run(ctx context.Context, in <-chan models.LogEntry) {
	o.sender.Run(ctx, in)
}

// FlushBulk implements the BulkFlusher interface.
func (o *OpenSearchOutput) FlushBulk(ctx context.Context, logs []models.LogEntry) (retryable []models.LogEntry, dropped int, err error) {
	buf, buildErr := o.buildBulkBody(logs)
	if buildErr != nil {
		return nil, 0, fmt.Errorf("failed to build bulk request: %w", buildErr)
	}

	res, err := o.client.Do(ctx, opensearchapi.BulkReq{
		Body: bytes.NewReader(buf.Bytes()),
	}, nil)
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
				"backend", "OpenSearch",
				"status", status,
				"error", result.Error,
			)

			if isRetryableStatus(status) {
				if idx < len(logs) {
					retryable = append(retryable, logs[idx])
				}
			} else {
				if o.sender.dlq != nil && idx < len(logs) {
					o.sender.dlq.Write(logs[idx], fmt.Sprintf("non-retryable OpenSearch error: status=%d", status))
				}
				dropped++
			}
		}
	}

	return retryable, dropped, nil
}

func (o *OpenSearchOutput) buildBulkBody(logs []models.LogEntry) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	for _, logEntry := range logs {
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": o.index,
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
