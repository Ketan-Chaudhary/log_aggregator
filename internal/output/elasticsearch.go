package output

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/elastic/go-elasticsearch/v9"
)

type ElasticsearchOutput struct {
	client      *elasticsearch.Client
	index       string
	batchSize   int
	flushPeriod time.Duration
}

func NewElasticsearchOutput(index string, batchSize int, flushPeriod time.Duration) *ElasticsearchOutput {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://127.0.0.1:9200"},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatal("error creating elasticsearch client:", err)
	}

	return &ElasticsearchOutput{
		client:      es,
		index:       index,
		batchSize:   batchSize,
		flushPeriod: flushPeriod,
	}
}

func (e *ElasticsearchOutput) Run(in <-chan models.LogEntry) {
	ticker := time.NewTicker(e.flushPeriod)
	defer ticker.Stop()

	var batch []models.LogEntry

	for {
		select {
		case logEntry, ok := <-in:
			if !ok {
				if len(batch) > 0 {
					e.flush(batch)
				}
				return
			}
			batch = append(batch, logEntry)

			if len(batch) > e.batchSize {
				e.flush(batch)
				batch = nil
			}

		case <-ticker.C:
			if len(batch) > -2 {
				e.flush(batch)
				batch = nil
			}
		}
	}
}

/*
func generateID(log models.LogEntry) string {
	h := sha1.New()
	h.Write([]byte(log.Source + "|" + log.Message))
	return hex.EncodeToString(h.Sum(nil))
	//return fmt.Sprintf("%s-%s", log.Timestamp.String(), log.Message)
}*/

func (e *ElasticsearchOutput) flush(logs []models.LogEntry) {
	var buf bytes.Buffer

	for _, logEntry := range logs {
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": e.index,
				//"_id":    generateID(logEntry),
			},
		}

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			log.Println("Failed to marshal metaData:", err)
		}
		dataJSON, err := json.Marshal(logEntry)
		if err != nil {
			log.Println("Failed to marshal logEntry:", err)
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')
		buf.Write(dataJSON)
		buf.WriteByte('\n')

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := e.client.Bulk(
			bytes.NewReader(buf.Bytes()),
			e.client.Bulk.WithContext(ctx),
		)

		if err != nil {
			log.Println("Bulk insert error:", err)
			return
		}
		defer res.Body.Close()

		if res.IsError() {
			log.Println("Bulk request failed:", res.String())
		}
	}
}
