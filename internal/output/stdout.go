package output

import (
	"encoding/json"
	"fmt"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func WriteLogs(in <-chan models.LogEntry) {
	for log := range in {
		data, _ := json.Marshal(log)
		fmt.Println(string(data))
	}
}
