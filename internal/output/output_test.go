package output

import (
	"testing"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
)

func TestNewOutput_Stdout(t *testing.T) {
	cfg := config.OutputConfig{Type: "stdout"}
	out, err := NewOutput(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out.(*StdoutOutput); !ok {
		t.Errorf("expected *StdoutOutput, got %T", out)
	}
}

func TestNewOutput_File(t *testing.T) {
	dir := t.TempDir()
	cfg := config.OutputConfig{
		Type: "file",
		File: config.FileConfig{
			Path:       dir + "/test_output.jsonl",
			MaxSizeMB:  1,
			MaxBackups: 3,
		},
	}
	out, err := NewOutput(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out.(*FileOutput); !ok {
		t.Errorf("expected *FileOutput, got %T", out)
	}
}

func TestNewOutput_UnknownType(t *testing.T) {
	cfg := config.OutputConfig{Type: "kafka"}
	_, err := NewOutput(cfg, nil)
	if err == nil {
		t.Fatal("expected error for unknown output type")
	}
}
