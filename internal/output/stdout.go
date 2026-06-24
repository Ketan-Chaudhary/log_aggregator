package output

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

type StdoutOutput struct{}

func NewStdoutOutput() *StdoutOutput {
	return &StdoutOutput{}
}

func (s *StdoutOutput) Run(_ context.Context, in <-chan models.LogEntry) {
	for logEntry := range in {
		data, err := json.Marshal(logEntry)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling log: %v\n", err)
			continue
		}
		fmt.Println(string(data))
	}
}
