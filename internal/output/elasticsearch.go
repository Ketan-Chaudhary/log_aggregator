package output

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
)

type ElasticsearchOutput struct {
	client      *elasticsearch.Client
	index       string
	batchSize   int
	flushPeriod time.Duration
	wg          sync.WaitGroup

	sendQueue chan []models.LogEntry
}

func NewElasticsearchOutput(
	index string,
	batchSize int,
	flushPeriod time.Duration,
) (*ElasticsearchOutput, error) {

	cfg := elasticsearch.Config{
		Addresses: []string{"http://127.0.0.1:9200"},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	output := &ElasticsearchOutput{
		client:      es,
		index:       index,
		batchSize:   batchSize,
		flushPeriod: flushPeriod,
		sendQueue:   make(chan []models.LogEntry, 100),
	}

	for i := 0; i < 3; i++ {
		output.wg.Add(1)
		go output.senderWorker(i)
	}

	return output, nil
}

func (e *ElasticsearchOutput) senderWorker(id int) {
	defer e.wg.Done()
	log.Printf("sender worker %d started", id)

	for batch := range e.sendQueue {
		log.Printf(
			"Sender worker %d processing batch size=%d",
			id,
			len(batch),
		)
		e.flush(batch)
	}
}

func (e *ElasticsearchOutput) Run(in <-chan models.LogEntry) {

	ticker := time.NewTicker(e.flushPeriod)
	defer ticker.Stop()

	batch := make([]models.LogEntry, 0, e.batchSize)

	for {
		select {

		case logEntry, ok := <-in:

			if !ok {
				if len(batch) > 0 {
					batchCopy := append([]models.LogEntry(nil), batch...)
					e.sendQueue <- batchCopy
				}
				close(e.sendQueue)
				e.wg.Wait()
				return
			}

			batch = append(batch, logEntry)

			log.Println("Received log")
			log.Println("Current batch size:", len(batch))

			if len(batch) >= e.batchSize {

				batchCopy := append([]models.LogEntry(nil), batch...)

				// async flush
				e.sendQueue <- batchCopy

				// reset batch
				batch = batch[:0]
			}

		case <-ticker.C:

			if len(batch) > 0 {

				batchCopy := append([]models.LogEntry(nil), batch...)

				// async flush
				e.sendQueue <- batchCopy

				// reset batch
				batch = batch[:0]
			}
		}
	}
}

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

func (e *ElasticsearchOutput) buildBulkBody(
	logs []models.LogEntry,
) (*bytes.Buffer, error) {

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

func (e *ElasticsearchOutput) flush(logs []models.LogEntry) {

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

		buf, buildErr := e.buildBulkBody(logs)
		if buildErr != nil {
			log.Println("failed to build bulk request:", buildErr)
			return
		}

		res, err := e.client.Bulk(
			bytes.NewReader(buf.Bytes()),
			e.client.Bulk.WithContext(ctx),
		)

		// success
		if err == nil && res != nil {
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
			if !bulkResp.Errors && !res.IsError() {
				log.Printf(
					"successfully flushed batch of size %d",
					len(logs),
				)
				return
			}

			// partial failures
			var retryLogs []models.LogEntry

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

					if isRetryableStatus(status) {
						if idx < len(logs) {
							retryLogs = append(retryLogs, logs[idx])
						}
					}
				}
			}

			// if everything failed
			if len(retryLogs) == 0 {
				log.Println("no retryable documents left")
				return
			}

			// retry only failed docs
			log.Printf(
				"retrying %d failed documents",
				len(retryLogs),
			)
			logs = retryLogs
			continue
		}

		// detailed ES error logging
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

		// cleanup
		if res != nil {
			res.Body.Close()
		}

		cancel()

		// retry decision
		if !shouldRetry(res, err) {
			log.Println("non-retryable error, dropping batch")
			return
		}

		// max retries reached
		if attempt == maxRetries {
			log.Println("dropping batch after max retries")
			return
		}

		// exponential backoff + jitter
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

func shouldRetry(res *esapi.Response, err error) bool {

	// retry network failures
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
