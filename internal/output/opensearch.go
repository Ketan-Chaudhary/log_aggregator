package output

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/metrics"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type OpenSearchOutput struct {
	client      *opensearch.Client
	index       string
	batchSize   int
	flushPeriod time.Duration
	wg          sync.WaitGroup
	dlq         *DLQ

	sendQueue chan []models.LogEntry
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
		client:      client,
		index:       cfg.Index,
		batchSize:   cfg.BatchSize,
		flushPeriod: cfg.FlushPeriod,
		dlq:         dlq,
		sendQueue:   make(chan []models.LogEntry, 100),
	}

	for i := 0; i < 3; i++ {
		output.wg.Add(1)
		go output.senderWorker(i)
	}

	return output, nil
}

func (o *OpenSearchOutput) senderWorker(id int) {
	defer o.wg.Done()
	log.Printf("opensearch sender worker %d started", id)

	for batch := range o.sendQueue {
		log.Printf(
			"OpenSearch worker %d processing batch size=%d",
			id,
			len(batch),
		)
		o.flush(batch)
	}
}

func (o *OpenSearchOutput) Run(in <-chan models.LogEntry) {

	ticker := time.NewTicker(o.flushPeriod)
	defer ticker.Stop()

	batch := make([]models.LogEntry, 0, o.batchSize)

	for {
		select {

		case logEntry, ok := <-in:

			if !ok {
				if len(batch) > 0 {
					batchCopy := append([]models.LogEntry(nil), batch...)
					o.sendQueue <- batchCopy
				}
				close(o.sendQueue)
				o.wg.Wait()
				return
			}

			batch = append(batch, logEntry)

			if len(batch) >= o.batchSize {
				batchCopy := append([]models.LogEntry(nil), batch...)
				o.sendQueue <- batchCopy
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				batchCopy := append([]models.LogEntry(nil), batch...)
				o.sendQueue <- batchCopy
				batch = batch[:0]
			}
		}
	}
}

func osIsRetryableStatus(status int) bool {
	switch status {
	case 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

func (o *OpenSearchOutput) buildBulkBody(
	logs []models.LogEntry,
) (*bytes.Buffer, error) {

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

func (o *OpenSearchOutput) flush(logs []models.LogEntry) {

	if len(logs) == 0 {
		return
	}

	maxRetries := 3
	baseDelay := 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {

		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		buf, buildErr := o.buildBulkBody(logs)
		if buildErr != nil {
			log.Println("failed to build bulk request:", buildErr)
			cancel()
			return
		}

		res, err := o.client.Do(ctx, opensearchapi.BulkReq{
			Body: bytes.NewReader(buf.Bytes()),
		}, nil)

		// success or partial per-item failures on a non-error bulk response
		if err == nil && res != nil && !res.IsError() {
			var bulkResp BulkResponse

			if decodeErr := json.NewDecoder(res.Body).Decode(&bulkResp); decodeErr != nil {
				log.Println("failed to decode bulk response:", decodeErr)
				res.Body.Close()
				cancel()
				return
			}

			res.Body.Close()
			cancel()

			// complete success
			if !bulkResp.Errors {
				log.Printf(
					"successfully flushed batch of size %d to OpenSearch",
					len(logs),
				)
				return
			}

			// partial failures
			var retryLogs []models.LogEntry
			droppedCount := 0

			for idx, item := range bulkResp.Items {
				for _, result := range item {
					status := result.Status

					if status >= 200 && status < 300 {
						continue
					}

					log.Printf(
						"bulk items failed: status=%d error=%v",
						status,
						result.Error,
					)

					if osIsRetryableStatus(status) {
						if idx < len(logs) {
							retryLogs = append(retryLogs, logs[idx])
						}
					} else {
						// Non-retryable: send to DLQ
						if o.dlq != nil && idx < len(logs) {
							o.dlq.Write(logs[idx], fmt.Sprintf("non-retryable OpenSearch error: status=%d", status))
						}
						droppedCount++
					}
				}
			}

			if len(retryLogs) == 0 {
				msg := "no retryable documents left"
				if droppedCount > 0 {
					log.Printf("%s, permanently dropped %d documents", msg, droppedCount)
				} else {
					log.Println(msg)
				}
				return
			}

			if droppedCount > 0 {
				log.Printf(
					"permanently dropped %d documents with non-retryable failures",
					droppedCount,
				)
			}

			log.Printf(
				"retrying %d failed documents",
				len(retryLogs),
			)
			logs = retryLogs
			continue
		}

		// detailed OS error logging
		if res != nil && res.IsError() {
			log.Printf(
				"bulk request failed: status=%s body=%s",
				res.Status(),
				res.String(),
			)
		}

		if err != nil {
			log.Printf(
				"bulk request network error: %v",
				err,
			)
		}

		if res != nil {
			res.Body.Close()
		}
		cancel()

		if !osShouldRetry(res, err) {
			log.Println("non-retryable error, sending batch to DLQ")
			metrics.Global.ESFlushErrors.Add(1) // we can reuse the same counter or create OSFlushErrors
			o.sendToDLQ(logs, "non-retryable bulk request error")
			return
		}

		if attempt == maxRetries {
			log.Println("max retries reached, sending batch to DLQ")
			metrics.Global.ESFlushErrors.Add(1)
			o.sendToDLQ(logs, "max retries exhausted")
			return
		}

		jitter := time.Duration(rand.Intn(250)) * time.Millisecond
		delay := (baseDelay * time.Duration(1<<(attempt-1))) + jitter

		log.Printf(
			"retrying batch in %v (attempt %d/%d)",
			delay,
			attempt,
			maxRetries,
		)
		time.Sleep(delay)
	}
}

func osShouldRetry(res *opensearch.Response, err error) bool {
	if err != nil {
		return true
	}
	if res == nil {
		return false
	}
	switch res.StatusCode {
	case 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

func (o *OpenSearchOutput) sendToDLQ(logs []models.LogEntry, reason string) {
	if o.dlq == nil {
		log.Printf("DLQ not configured, permanently dropping %d logs", len(logs))
		return
	}
	for _, entry := range logs {
		o.dlq.Write(entry, reason)
	}
}
