package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {"type": "stdout"}
	}`
	path := writeTempConfig(t, cfg)

	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if c.StatsPort != 8080 {
		t.Errorf("expected stats_port=8080, got %d", c.StatsPort)
	}
	if c.BufferSize != 1000 {
		t.Errorf("expected buffer_size=1000, got %d", c.BufferSize)
	}
	if c.LogLevel != "INFO" {
		t.Errorf("expected log_level=INFO, got %s", c.LogLevel)
	}
	if c.Processor.Workers != 2 {
		t.Errorf("expected workers=2, got %d", c.Processor.Workers)
	}
	if c.Processor.LabelMode != "nested" {
		t.Errorf("expected label_mode=nested, got %s", c.Processor.LabelMode)
	}
}

func TestLoadConfig_ValidationNoCollectorPaths(t *testing.T) {
	cfg := `{
		"collector": {},
		"output": {"type": "stdout"}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected validation error for empty collector.paths")
	}
}

func TestLoadConfig_ValidationNoOutputType(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected validation error for empty output.type")
	}
}

func TestLoadConfig_ValidationElasticsearchNoURLs(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {
			"type": "elasticsearch",
			"elasticsearch": {"index": "logs"}
		}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected validation error for elasticsearch with no URLs")
	}
}

func TestLoadConfig_ValidationElasticsearchNoIndex(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {
			"type": "elasticsearch",
			"elasticsearch": {"urls": ["http://localhost:9200"]}
		}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected validation error for elasticsearch with no index")
	}
}

func TestLoadConfig_ValidationOpenSearchNoURLs(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {
			"type": "opensearch",
			"opensearch": {"index": "logs"}
		}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected validation error for opensearch with no URLs")
	}
}

func TestLoadConfig_InvalidMinLevel(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {"type": "stdout"},
		"processor": {"min_level": "INVALID"}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid min_level")
	}
}

func TestLoadConfig_InvalidLabelMode(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {"type": "stdout"},
		"processor": {"label_mode": "bad"}
	}`
	path := writeTempConfig(t, cfg)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid label_mode")
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	path := writeTempConfig(t, `{invalid json}`)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLoadConfig_ValidStdout(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {"type": "stdout"},
		"buffer_size": 500,
		"log_level": "DEBUG"
	}`
	path := writeTempConfig(t, cfg)

	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.BufferSize != 500 {
		t.Errorf("expected buffer_size=500, got %d", c.BufferSize)
	}
	if c.LogLevel != "DEBUG" {
		t.Errorf("expected log_level=DEBUG, got %s", c.LogLevel)
	}
}

func TestLoadConfig_ValidElasticsearch(t *testing.T) {
	cfg := `{
		"collector": {"paths": ["/var/log/*.log"]},
		"output": {
			"type": "elasticsearch",
			"elasticsearch": {
				"urls": ["http://localhost:9200"],
				"index": "logs",
				"batch_size": 50,
				"flush_period_ms": 2000
			}
		}
	}`
	path := writeTempConfig(t, cfg)

	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Output.Elasticsearch.BatchSize != 50 {
		t.Errorf("expected batch_size=50, got %d", c.Output.Elasticsearch.BatchSize)
	}
}
