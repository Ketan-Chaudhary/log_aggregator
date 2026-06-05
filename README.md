# Log Aggregator

A robust, concurrent Go application designed to continuously monitor log files, parse JSON-formatted log entries, and ship them to an Elasticsearch cluster. 

## Features

- **Robust File Tailing:** Safe monitoring of log files without race conditions, including buffering logic to avoid log corruption on partial system writes.
- **Concurrent Processing:** Configurable worker pool to parse incoming JSON logs efficiently.
- **Bulk Elasticsearch Shipping:** Log entries are batched and shipped to Elasticsearch asynchronously. Retries are implemented with exponential backoff for network/transient failures.
- **Graceful Shutdown:** Full support for `SIGINT` and `SIGTERM`. When stopping the service, it guarantees that all currently buffered logs are fully flushed to Elasticsearch before the application exits, preventing data loss.

## Installation

Ensure you have Go 1.20+ installed.

```bash
git clone <repository_url>
cd log_agg_goLang
go mod tidy
go build -o log_aggregator cmd/main.go
```

## Usage

You can start the log aggregator using command-line flags. Before running, ensure that your Elasticsearch instance is reachable.

```bash
./log_aggregator -workers 4 -file "app.log" -es "http://127.0.0.1:9200"
```

### Flags

- `-file` (default: `app.log`): Path to the log file to monitor.
- `-es` (default: `http://127.0.0.1:9200`): Comma-separated list of Elasticsearch URLs.
- `-workers` (default: `2`): Number of goroutines parsing and processing logs.

## Log Format

The processor is configured to parse JSON log strings. For example:
```json
{"level":"INFO","msg":"user login","request_id":"1","timestamp":"2023-10-27T10:00:00Z"}
```
If the timestamp or fields are not present or invalid, it will fallback gracefully to the current ingest time.

## Testing Log Generation

You can use the provided script to generate test logs:
```bash
./testlogs.sh
```
