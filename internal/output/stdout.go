package output

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func WriteLogs(in <-chan models.LogEntry) {
	for log := range in {
		data, err := json.Marshal(log)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to marshal logs:", err)
			continue
		}
		fmt.Println(string(data))
	}
}
